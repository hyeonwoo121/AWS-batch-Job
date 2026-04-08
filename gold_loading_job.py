import sys
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark.context import SparkConf, SparkContext # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.job import Job # type: ignore
from pyspark.sql import functions as F

from glue_libs.config import GlueJobConfig, SILVER_REQUIRED_ARGS
from glue_libs.db_utils import execute_jdbc_sql

# Gold 잡은 Silver와 동일한 파라미터 구조를 사용할 수 있습니다. (단일 TARGET_TABLE 파라미터는 무시됨)
args = getResolvedOptions(sys.argv, SILVER_REQUIRED_ARGS)
cfg  = GlueJobConfig(args)

# 1. Apache Iceberg 환경 세팅 (Silver 테이블 조회를 위함)
catalog_name = "glue_catalog"
conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", cfg.silver_s3_path)
conf.set(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(cfg.job_name, args)
logger = glueContext.get_logger()

logger.info("Gold 계층 적재 및 롤백용 트랜잭션 수집 잡을 시작합니다.")

# 2. 관리 테이블에서 Silver 계층 처리가 완료된(process_number = '02') 타겟들 획득
# 푸시다운 쿼리를 통해 DB에서부터 필터링하여 가져옵니다.
manage_query = "(SELECT * FROM data_ingestion_table WHERE process_number = '02') as tmp"
manage_df = spark.read.jdbc(
    url=cfg.aurora_jdbc_url,
    table=manage_query,
    properties=cfg.aurora_conn_properties
)

tasks = manage_df.collect()
if not tasks:
    logger.info("Gold 계층 적재 대상(process_number = '02')이 없어 Job을 종료합니다.")
    job.commit()
    sys.exit(0)

target_tables = [str(r['table_name']) for r in tasks]
logger.info(f"Gold 적재 대상 테이블 스캔 완료: {target_tables}")

# 3. 각 테이블별 Staging(임시) DB에 분산 적재
for t_name in target_tables:
    logger.info(f"[Staging Write Start] 테이블명: {t_name}")
    
    # Silver에서 Data 읽기
    iceberg_table_id = f"{catalog_name}.{cfg.iceberg_db_name}.{t_name}"
    silver_df = spark.table(iceberg_table_id)
    
    # 임시 테이블(예: stg_테이블명)에 전부 덮어쓰기로 적재
    staging_table = f"stg_{t_name}"
    
    silver_df.write.jdbc(
        url=cfg.aurora_jdbc_url,
        table=staging_table,
        mode="overwrite",
        properties=cfg.aurora_conn_properties
    )
    logger.info(f"[Staging Write Success] 임시 테이블 {staging_table} 저장 완료")

logger.info("모든 테이블의 Staging 적재가 성공하여 본 트랜잭션 스와핑(Swap)을 개시합니다.")

# 4. 일괄 Rollback 기반 통합 스와핑 트랜잭션 (JDBC Session Native DDL)
# 만약 여기까지 오는 과정에서 하나라도 실패했다면, Python Exception에 의해 이 구문은 아예 실행되지 않고 롤백(Job Failed)됩니다.
sql_commands = []
for t_name in target_tables:
    # 안전한 스와핑 전략
    old_table = f"{t_name}_old"
    stg_table = f"stg_{t_name}"
    
    sql_commands.append(f"DROP TABLE IF EXISTS {old_table}")
    # 원본 테이블이 없을 수도 있는 최초 실행 시를 대비한 분기
    sql_commands.append(f"DO $$ BEGIN IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = '{t_name}') THEN ALTER TABLE {t_name} RENAME TO {old_table}; END IF; END $$;")
    sql_commands.append(f"ALTER TABLE {stg_table} RENAME TO {t_name}")
    sql_commands.append(f"DROP TABLE IF EXISTS {old_table}")

# 5. 모든 테이블의 Swap이 끝나면 제어 상태(process_number)를 '00'으로 일괄 원복 (하나의 트랜잭션에 묶음)
table_list_str = ", ".join([f"'{t}'" for t in target_tables])
update_status_sql = f"UPDATE data_ingestion_table SET process_number = '00' WHERE process_number = '02' AND table_name IN ({table_list_str})"

sql_commands.append(update_status_sql)

logger.info("단일 트랜잭션 내에서 실행할 DDL/DML 조립 완료. JDBC 실행을 시도합니다.")
try:
    execute_jdbc_sql(spark, cfg.aurora_jdbc_url, cfg.aurora_conn_properties, sql_commands)
    logger.info(f"Gold Swap 및 상태 초기화(00) 트랜잭션이 완벽하게 커밋되었습니다. 반영된 테이블: {target_tables}")
except Exception as e:
    logger.error(f"테이블 스와핑 및 관리 플래그 업데이트 도중 치명적 에러가 발생했습니다. 트랜잭션은 롤백됩니다. 에러: {e}")
    raise e

logger.info("Gold 데이터 관리 파이프라인이 정상적으로 모두 종료되었습니다.")
job.commit()
