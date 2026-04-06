import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────
# glue_libs.zip을 Glue Job의 "Python library path"에 등록해두면
# 아래처럼 외부 모듈을 import해서 사용할 수 있습니다.
# ─────────────────────────────────────────────────────────────────
from glue_libs.config import GlueJobConfig, BRONZE_REQUIRED_ARGS

# 1. 파라미터 파싱 → GlueJobConfig에 위임
args = getResolvedOptions(sys.argv, BRONZE_REQUIRED_ARGS)
cfg  = GlueJobConfig(args)   # 모든 커넥션/경로 정보는 cfg 객체가 관리

# 2. Spark & AWS Glue Context 초기화
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(cfg.job_name, args)

logger = glueContext.get_logger()
logger.info(f"Job 시작: {cfg}")

# 3. Gold 계층(Aurora) 관리 테이블(data_ingestion_table) 조회
# ※ 패스워드는 프로덕션에서 AWS Secrets Manager 연계를 권장합니다.
manage_df    = spark.read.jdbc(
    url        = cfg.aurora_jdbc_url,
    table      = "data_ingestion_table",
    properties = cfg.aurora_conn_properties
)
table_config = manage_df.filter(F.col("table_name") == cfg.target_table).collect()

# 관리 테이블 미등록 시 → 에러 로그 & Job 종료
if len(table_config) == 0:
    error_msg = (
        f"에러: 연계 타겟 테이블 '{cfg.target_table}' 이(가) "
        f"Gold 계층 관리 테이블에 존재하지 않습니다. 수집을 스킵합니다."
    )
    logger.error(error_msg)
    raise ValueError(error_msg)

# 테이블 구성 정보 매핑
config_row    = table_config[0]
load_type     = str(config_row['load_type']).upper()   # 'FULL' or 'INCREMENTAL'
ingest_column = str(config_row['ingest_column'])        # 최근 수집 기준 datetime
source_dt_col = str(config_row['source_dt_col'])        # 원천 DB 비교 datetime 컬럼명

logger.info(
    f"수집 지시 로드: 대상={cfg.target_table}, "
    f"타입={load_type}, 기준컬럼={source_dt_col}, 최근수집={ingest_column}"
)

# 4. 연계대상(PostgreSQL)에서 데이터 추출 — 전건/차분 분기
if load_type == 'FULL':
    logger.info(f"전건(FULL) 취득 모드. 현재 날짜 파티션을 Overwrite 합니다.")
    extract_query = cfg.target_table
    write_mode    = "overwrite"

elif load_type == 'INCREMENTAL':
    logger.info(f"차분(INCREMENTAL) 취득 모드.")
    if ingest_column in ("None", ""):
        logger.warn("최초 실행으로 간주 — 전체 데이터를 가져옵니다.")
        extract_query = cfg.target_table
    else:
        # 원천 DB에 Pushdown 쿼리 전달 → 최신 데이터만 반환
        extract_query = (
            f"(SELECT * FROM {cfg.target_table} "
            f"WHERE {source_dt_col} > '{ingest_column}') AS tmp_subquery"
        )
    write_mode = "append"

else:
    raise ValueError(f"알 수 없는 load_type: {load_type}")

# DataFrame 생성 (Postgres에서 분산 추출)
source_df = spark.read.jdbc(
    url        = cfg.source_jdbc_url,
    table      = extract_query,
    properties = cfg.source_conn_properties
)

if source_df.rdd.isEmpty():
    logger.info("수집할 신규 데이터가 없습니다. Job을 정상 조기 종료합니다.")
    job.commit()
    sys.exit(0)

# 5. 수집 메타데이터 + 파티셔닝 컬럼 추가
logger.info("파티셔닝 컬럼과 수집 시각 메타데이터를 추가합니다.")
current_ts = F.current_timestamp()

enriched_df = (
    source_df
    .withColumn("_ingestion_time", current_ts)
    .withColumn("year",  F.year(current_ts).cast("string"))
    .withColumn("month", F.lpad(F.month(current_ts).cast("string"),      2, "0"))
    .withColumn("day",   F.lpad(F.dayofmonth(current_ts).cast("string"), 2, "0"))
)

# S3 최종 경로 (예: s3://bucket/bronze/table_name/)
final_s3_path = cfg.get_s3_output_path(cfg.target_table)

# FULL 모드에서 오늘 날짜 파티션만 동적 Overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

logger.info(f"Target S3: {final_s3_path}  |  Write 모드: {write_mode}")

# 6. Parquet 형식으로 날짜별 파티셔닝 적재
(
    enriched_df.write
    .mode(write_mode)
    .partitionBy("year", "month", "day")
    .parquet(final_s3_path)
)

logger.info(f"{target_table} 수집 스크립트 실행이 정상 완료되었습니다.")
job.commit()
