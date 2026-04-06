"""
glue_libs/config.py
--------------------
AWS Glue Job 파라미터 파싱 및 DB 커넥션 프로퍼티 집중 관리 모듈.
"""

BRONZE_REQUIRED_ARGS = [
    'JOB_NAME',
    'TARGET_TABLE',
    'BRONZE_S3_PATH',
    'AURORA_JDBC_URL',
    'AURORA_USER',
    'AURORA_PASSWORD',
    'SOURCE_JDBC_URL',
    'SOURCE_USER',
    'SOURCE_PASSWORD',
]

SILVER_REQUIRED_ARGS = [
    'JOB_NAME',
    'TARGET_TABLE',
    'BRONZE_S3_PATH',
    'SILVER_S3_PATH',
    'ICEBERG_DB_NAME',   # AWS Glue Data Catalog상의 Database 이름
    'AURORA_JDBC_URL',
    'AURORA_USER',
    'AURORA_PASSWORD',
]

class GlueJobConfig:
    """
    Bronze 및 Silver Job을 모두 커버하는 파라미터/커넥션 통합 관리 객체
    """
    def __init__(self, args: dict):
        self.job_name       = args.get('JOB_NAME', 'unknown_job')
        self.target_table   = args.get('TARGET_TABLE')
        
        # S3 Paths
        _bronze = args.get('BRONZE_S3_PATH')
        self.bronze_s3_path = _bronze.rstrip('/') + '/' if _bronze else None
        
        _silver = args.get('SILVER_S3_PATH')
        self.silver_s3_path = _silver.rstrip('/') + '/' if _silver else None
        
        # Iceberg Config
        self.iceberg_db_name = args.get('ICEBERG_DB_NAME')

        # ── Aurora (Gold 계층 관리 테이블 DB) 접속 정보 ──────────────────────
        self.aurora_jdbc_url = args.get('AURORA_JDBC_URL')
        self.aurora_conn_properties = {
            "user":     args.get('AURORA_USER'),
            "password": args.get('AURORA_PASSWORD'),
            "driver":   "org.postgresql.Driver",
        } if self.aurora_jdbc_url else None

        # ── 연계 원천 PostgreSQL 접속 정보 (Bronze 잡 전용) ─────────
        self.source_jdbc_url = args.get('SOURCE_JDBC_URL')
        self.source_conn_properties = {
            "user":     args.get('SOURCE_USER'),
            "password": args.get('SOURCE_PASSWORD'),
            "driver":   "org.postgresql.Driver",
        } if self.source_jdbc_url else None

    def get_bronze_path(self, table_name: str) -> str:
        return f"{self.bronze_s3_path}{table_name}/"
        
    def get_silver_path(self, table_name: str) -> str:
        return f"{self.silver_s3_path}{table_name}/"

    def __repr__(self):
        return f"GlueJobConfig(job={self.job_name}, target={self.target_table})"
