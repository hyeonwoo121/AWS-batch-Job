import sys
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark.context import SparkConf, SparkContext # type: ignore
from awsglue.context import GlueContext # type: ignore
from awsglue.job import Job # type: ignore
from pyspark.sql import functions as F
from glue_libs.db_utils import update_process_number
from glue_libs.config import GlueJobConfig, SILVER_REQUIRED_ARGS

# 1. 파라미터 파싱
args = getResolvedOptions(sys.argv, SILVER_REQUIRED_ARGS)
cfg  = GlueJobConfig(args)

# 2. Apache Iceberg를 위한 Spark 세션 환경 설정
# AWS Glue Data Catalog를 Iceberg 메타스토어로 사용하기 위한 필수 속성 주입
catalog_name = "glue_catalog"
conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", cfg.silver_s3_path) # 실버 계층 기준 경로
conf.set(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(cfg.job_name, args)

logger = glueContext.get_logger()
logger.info(f"Silver Iceberg Job 시작: {cfg}")

# 3. 관리 테이블(Gold) 단건 최적화 조회 (Pushdown)
manage_query = f"(SELECT * FROM data_ingestion_table WHERE table_name = '{cfg.target_table}') as tmp"
manage_df = spark.read.jdbc(
    url=cfg.aurora_jdbc_url,
    table=manage_query,
    properties=cfg.aurora_conn_properties
)

table_config = manage_df.collect()
if not table_config:
    raise ValueError(f"에러: 연계 타겟 테이블 '{cfg.target_table}' 이(가) 관리 테이블에 없습니다.")

config_row = table_config[0]
load_type = str(config_row['load_type']).upper()
# 테이블의 고유 식별자(PK). 파이프로 연결된 다중키(ex: user_id|event_id)도 커버 가능하도록 설계
pk_column_str = str(config_row.get('primary_keys', '')) 

# 4. Bronze 계층 Parquet 데이터 로드 시 최적화 (차분 필터링)
bronze_path = cfg.get_bronze_path(cfg.target_table)
logger.info(f"Bronze 저장소({bronze_path})에서 Parquet 데이터를 읽습니다.")

bronze_df = spark.read.parquet(bronze_path)

# INCREMENTAL 모드일 경우, Bronze 전체가 아닌 가장 최신에 적재된 파티션(오늘 자) 데이터만 읽도록 필터링하는 것을 권장합니다.
# 이 코드는 실행일 기준으로 당일 수집된 데이터에 한정해 Merge 속도를 극대화합니다.
if load_type == 'INCREMENTAL':
    logger.info("차분 업데이트를 위해 Bronze 데이터 중 최신(_ingestion_time) 파티션 영역만 필터링합니다.")
    # 간단한 예시: 가장 최신 year, month, day 파티션만 필터
    # 실제 운영 시에는 파라미터로 넘겨받은 수집 기준일(예: args['TARGET_DATE'])로 필터링하는 것이 안전합니다.
    current_ts = F.current_timestamp()
    bronze_df = bronze_df.filter(
        (F.col("year") == F.year(current_ts).cast("string")) &
        (F.col("month") == F.lpad(F.month(current_ts).cast("string"), 2, '0')) &
        (F.col("day") == F.lpad(F.dayofmonth(current_ts).cast("string"), 2, '0'))
    )

bronze_df.createOrReplaceTempView("bronze_updates")

# 5. Silver 계층 Iceberg 처리를 위한 타겟 식별
iceberg_table_id = f"{catalog_name}.{cfg.iceberg_db_name}.{cfg.target_table}"

# 데이터베이스(Database) 존재 유무 확인 후 미존재 시 생성 (스파크 SQL)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{cfg.iceberg_db_name}")

# Iceberg 테이블이 물리적으로 존재하는지 확인 (공식 Spark 3.x 카탈로그 API 활용)
table_exists = spark.catalog.tableExists(iceberg_table_id)

if not table_exists:
    logger.info(f"Iceberg 테이블 {iceberg_table_id} 이 존재하지 않습니다. 새로 생성합니다.")
    # CTAS (Create Table As Select) 방식으로 파티셔닝 유지하며 생성
    bronze_df.writeTo(iceberg_table_id) \
             .tableProperty('format-version', '2') \
             .partitionedBy("year", "month", "day") \
             .create()
else:
    logger.info(f"Iceberg 테이블 {iceberg_table_id} (이)가 존재합니다. {load_type} 을 수행합니다.")
    
    if load_type == 'FULL':
        logger.info("전건 업데이트 - 테이블 데이터를 전부 Overwrite 하여 새 스냅샷을 만듭니다.")
        bronze_df.writeTo(iceberg_table_id).overwritePartitions()

    elif load_type == 'INCREMENTAL':
        logger.info("차분 업데이트 (Merge/Upsert) 를 시작합니다.")
        
        if not pk_column_str or pk_column_str == 'None':
            # PK가 미지정된 경우 단순 Append
            logger.warn("Primary Key가 지정되지 않아 단순 Append를 수행합니다.")
            bronze_df.writeTo(iceberg_table_id).append()
        else:
            # PK 기반 MERGE INTO (Upsert) 동적 SQL 생성
            # 예: "t.id = s.id AND t.sub_id = s.sub_id"
            pk_list = [pk.strip() for pk in pk_column_str.split('|') if pk.strip()]
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_list])
            
            merge_sql = f"""
            MERGE INTO {iceberg_table_id} target
            USING bronze_updates source
            ON {merge_condition}
            WHEN MATCHED THEN 
                 UPDATE SET *
            WHEN NOT MATCHED THEN 
                 INSERT *
            """
            logger.info(f"MERGE INTO SQL 실행: {merge_sql}")
            spark.sql(merge_sql)
            
    else:
        raise ValueError(f"지원하지 않는 load_type: {load_type}")

# 6. 상태 플래그 갱신 (01 -> 02)
logger.info("Silver 병합 완료. 관리 테이블의 process_number 플래그를 '02' 로 갱신합니다.")
update_process_number(spark, cfg, current_status='01', new_status='02')

logger.info("Silver 데이터 병합(Merge) 프로세스가 정상 종료되었습니다.")
job.commit()
