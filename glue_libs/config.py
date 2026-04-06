"""
glue_libs/config.py
--------------------
AWS Glue Job 파라미터 파싱 및 DB 커넥션 프로퍼티 집중 관리 모듈.
메인 Job 스크립트에서 import하여 사용합니다.

사용법:
    from glue_libs.config import GlueJobConfig
    cfg = GlueJobConfig(args)
"""

# Glue Job에서 요구할 파라미터 키 목록 (getResolvedOptions에 전달)
REQUIRED_ARGS = [
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


class GlueJobConfig:
    """
    Glue Job 파라미터를 받아서 각 설정값 및
    JDBC 커넥션 프로퍼티를 한 곳에서 관리하는 설정 클래스.
    """

    def __init__(self, args: dict):
        """
        Args:
            args (dict): getResolvedOptions()로 파싱된 파라미터 딕셔너리
        """
        self.job_name       = args['JOB_NAME']
        self.target_table   = args['TARGET_TABLE']
        self.bronze_s3_path = args['BRONZE_S3_PATH'].rstrip('/') + '/'

        # ── Aurora (Gold 계층 관리 테이블 DB) 접속 정보 ──────────────────────
        self.aurora_jdbc_url = args['AURORA_JDBC_URL']
        self.aurora_conn_properties = {
            "user":     args['AURORA_USER'],
            "password": args['AURORA_PASSWORD'],
            "driver":   "org.postgresql.Driver",
        }

        # ── 연계 원천 PostgreSQL 접속 정보 ───────────────────────────────────
        self.source_jdbc_url = args['SOURCE_JDBC_URL']
        self.source_conn_properties = {
            "user":     args['SOURCE_USER'],
            "password": args['SOURCE_PASSWORD'],
            "driver":   "org.postgresql.Driver",
        }

    def get_s3_output_path(self, table_name: str) -> str:
        """테이블 이름을 받아 S3 최종 저장 경로를 반환합니다."""
        return f"{self.bronze_s3_path}{table_name}/"

    def __repr__(self):
        return (
            f"GlueJobConfig("
            f"job={self.job_name}, "
            f"target={self.target_table}, "
            f"s3={self.bronze_s3_path})"
        )
