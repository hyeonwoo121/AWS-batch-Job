import sys
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# 1. 파라미터 파싱 (AWS Glue에서 Job Parameter로 전달받을 인자들 지정)
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'TARGET_TABLE', 
    'BRONZE_S3_PATH', 
    'AURORA_JDBC_URL', 
    'AURORA_USER', 
    'AURORA_PASSWORD', 
    'SOURCE_JDBC_URL', 
    'SOURCE_USER', 
    'SOURCE_PASSWORD'
])

target_table = args['TARGET_TABLE']
bronze_s3_path = args['BRONZE_S3_PATH'] # ex) s3://my-medallion-lake/bronze/

# 2. Spark & AWS Glue Context 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()

# 3. Gold 계층(Aurora DB)의 관리 테이블(data_ingestion_table) 조회
# ※ 프로덕션 환경 시, 패스워드는 AWS Secrets Manager 등을 연계하는 것이 좋습니다.
aurora_conn_properties = {
    "user": args['AURORA_USER'],
    "password": args['AURORA_PASSWORD'],
    "driver": "org.postgresql.Driver"
}

manage_df = spark.read.jdbc(
    url=args['AURORA_JDBC_URL'],
    table="data_ingestion_table",
    properties=aurora_conn_properties
)

# 관리 테이블에서 해당 타겟 테이블의 레코드를 가져옵니다.
table_config = manage_df.filter(F.col("table_name") == target_table).collect()

# 등록 안된 경우 에러 로그 뱉고 프로세스 종료
if len(table_config) == 0:
    error_msg = f"에러: 연계 타겟 테이블 '{target_table}' 이(가) Gold 계층 관리 테이블에 존재하지 않습니다. 수집을 스킵합니다."
    logger.error(error_msg)
    # job의 상태를 Failed로 넘기기 위해 Exception
    raise ValueError(error_msg)

# 테이블 구성 정보 매핑
config_row = table_config[0]
load_type = str(config_row['load_type']).upper() # 'FULL' or 'INCREMENTAL'
ingest_column = str(config_row['ingest_column']) # 관리 테이블의 최신 ingest timestamp 혹은 datetime 문자열
source_dt_col = str(config_row['source_dt_col']) # 연계측 source DB의 비교대상 datetime 컬럼명

logger.info(f"수집 지시 로드: 대상={target_table}, 타입={load_type}, 기준={source_dt_col}, 최근수집={ingest_column}")

# 4. 연계대상(PostgreSQL) DB에서 데이터 추출
source_conn_properties = {
    "user": args['SOURCE_USER'],
    "password": args['SOURCE_PASSWORD'],
    "driver": "org.postgresql.Driver"
}

# 쿼리 조립 및 저장 모드 세팅
write_mode = ""
if load_type == 'FULL':
    logger.info(f"전건(FULL) 취득 모드. 기존 파티션 ({target_table}) 의 현재날짜 영역을 Overwrite 합니다.")
    # JDBC URL 옵션에 쿼리를 직접 태우거나 table 이름을 줍니다.
    extract_query = target_table
    write_mode = "overwrite" # 전건 취득이므로 완전히 엎어치기

elif load_type == 'INCREMENTAL':
    logger.info(f"차분(INCREMENTAL) 취득 모드. {target_table}에 조건을 걸어서 가져옵니다.")
    if ingest_column == "None" or not ingest_column:
         logger.warn("최근 수집 내역이 없습니다. (차분 모드이나 최초 실행으로 간주하여 전체를 가져옵니다)")
         extract_query = target_table
    else:
         # ingest_column 보다 연계대상의 source_dt_col이 큰 것만 취득하는 SubQuery
         extract_query = f"(SELECT * FROM {target_table} WHERE {source_dt_col} > '{ingest_column}') as tmp_subquery"
    
    # 차분일 경우, 오늘 자 파티션에 새로 추가된 데이터만 append
    write_mode = "append"
else:
    raise ValueError(f"알 수 없는 load_type 지원 안함: {load_type}")


# DataFrame 생성 (Postgres에서 분산 추출)
source_df = spark.read.jdbc(
    url=args['SOURCE_JDBC_URL'],
    table=extract_query,
    properties=source_conn_properties
)

if source_df.rdd.isEmpty():
    logger.info("수집할 신규 데이터가 존재하지 않습니다. Job을 성공적으로 조기 종료합니다.")
    job.commit()
    sys.exit(0)

# 5. S3 파티셔닝 식별자 및 메타데이터 추가
logger.info("데이터프레임에 수집 일자 파티셔닝 컬럼과 메타데이터 컬럼을 덧붙입니다.")
current_timestamp = F.current_timestamp()

enriched_df = source_df \
    .withColumn("_ingestion_time", current_timestamp) \
    .withColumn("year", F.year(current_timestamp).cast("string")) \
    .withColumn("month", F.lpad(F.month(current_timestamp).cast("string"), 2, '0')) \
    .withColumn("day", F.lpad(F.dayofmonth(current_timestamp).cast("string"), 2, '0'))

# s3 저장소 경로 구성
final_s3_path = f"{bronze_s3_path.rstrip('/')}/{target_table}/"

# (옵션) 동적 파티션 덮어쓰기 모드 - FULL 모드 시 오직 연/월/일 파티션 영역만 덮어쓰기하기 위함
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

logger.info(f"Target S3 경로: {final_s3_path}")
logger.info(f"Write 모드: {write_mode}")

# 6. S3에 파티션과 함께 Parquet 형식 격납 
enriched_df.write \
    .mode(write_mode) \
    .partitionBy("year", "month", "day") \
    .parquet(final_s3_path)

logger.info(f"{target_table} 수집 스크립트 실행이 정상 완료되었습니다.")
job.commit()
