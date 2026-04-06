"""
glue_libs/db_utils.py
---------------------
Spark JVM(Py4J)을 빌려와서, 복잡한 DataFrame 변환 없이
순수 JDBC 레벨에서 단일 트랜잭션 SQL문(UPDATE, DELETE, DDL 등)을 직접 실행하는 유틸리티입니다.
주로 관리 테이블의 상태 플래그 갱신이나, Gold 계층의 트랜잭셔널 스와핑(Swap)에 사용됩니다.
"""

def execute_jdbc_sql(spark, jdbc_url, properties, sql_queries):
    """
    여러 건의 SQL을 받아서 단일 트랜잭션(BEGIN ~ COMMIT)으로 실행합니다.
    
    Args:
        spark: SparkSession 객체
        jdbc_url: 접속 대상 DB의 JDBC URL
        properties: 접속 프로퍼티 (사용자, 암호, 드라이버 등)
        sql_queries: 실행할 SQL 문자열 (여러 개일 경우 리스트)
    """
    if isinstance(sql_queries, str):
        sql_queries = [sql_queries]
        
    jvm = spark._sc._gateway.jvm
    
    # 1. JDBC 드라이버 로드
    driver_class = properties.get("driver", "org.postgresql.Driver")
    jvm.java.lang.Class.forName(driver_class)
    
    # 2. 프로퍼티 속성 Java 호환 객체로 변환
    java_props = jvm.java.util.Properties()
    for k, v in properties.items():
        java_props.setProperty(k, str(v))
        
    # 3. 커넥션 획득
    driver_manager = jvm.java.sql.DriverManager
    conn = None
    stmt = None
    try:
        conn = driver_manager.getConnection(jdbc_url, java_props)
        # 트랜잭션 롤백 제어를 위해 오토커밋 Off
        conn.setAutoCommit(False)
        
        stmt = conn.createStatement()
        for sql in sql_queries:
            # 쿼리가 여러 개일 경우 순차적으로 실행
            # 주의: DML(Update/Insert) 혹은 DDL(Create/Alter)만 지원 (Select 결과 반환 X)
            stmt.executeUpdate(sql)
            
        conn.commit()  # 전부 에러 없이 돌았다면 일괄 커밋!
        
    except Exception as e:
        if conn:
            conn.rollback() # 중간에 에러 발생 시 롤백 수행
        raise RuntimeError(f"JDBC SQL 실행 중 에러가 발생하여 롤백되었습니다: {e}")
        
    finally:
        if stmt:
            stmt.close()
        if conn:
            # 커넥션 풀 반환 전 오토커밋 원상 복구
            conn.setAutoCommit(True) 
            conn.close()

def update_process_number(spark, cfg, current_status, new_status):
    """
    data_ingestion_table의 process_number를 특정 테이블 조건에 맞게 업데이트합니다.
    """
    sql = f"""
        UPDATE data_ingestion_table 
        SET process_number = '{new_status}' 
        WHERE table_name = '{cfg.target_table}' AND process_number = '{current_status}'
    """
    execute_jdbc_sql(spark, cfg.aurora_jdbc_url, cfg.aurora_conn_properties, sql)
