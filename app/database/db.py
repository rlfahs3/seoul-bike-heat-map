"""
MySQL 데이터베이스 연결 관리
SQLAlchemy를 사용한 동기식 MySQL 연결
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from typing import Generator
import logging

from app.config import settings

logger = logging.getLogger(__name__)

# MySQL 연결 URL
# 형식: mysql+pymysql://username:password@host:port/database
DATABASE_URL = (
    f"mysql+pymysql://{settings.mysql_user}:{settings.mysql_password}"
    f"@{settings.mysql_host}:{settings.mysql_port}/{settings.mysql_database}"
    f"?charset=utf8mb4"
)

# SQLAlchemy 엔진 생성
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,            # 커넥션 풀 크기
    max_overflow=20,         # 최대 추가 커넥션
    pool_pre_ping=True,      # 커넥션 유효성 체크
    pool_recycle=3600,       # 1시간마다 커넥션 재생성
    echo=settings.debug,     # SQL 로그 출력 (디버그 모드)
)

# 세션 팩토리 생성
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db() -> Generator[Session, None, None]:
    """
    데이터베이스 세션 의존성
    FastAPI의 Depends에서 사용
    
    Usage:
        @app.get("/api/stations")
        def get_stations(db: Session = Depends(get_db)):
            ...
    
    Yields:
        Session: SQLAlchemy 세션
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection() -> bool:
    """
    데이터베이스 연결 테스트
    
    Returns:
        bool: 연결 성공 여부
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        logger.info("✅ MySQL 연결 성공")
        return True
    except Exception as e:
        logger.error(f"❌ MySQL 연결 실패: {e}")
        return False


def get_db_info() -> dict:
    """
    데이터베이스 정보 조회
    
    Returns:
        dict: 데이터베이스 버전 및 상태 정보
    """
    try:
        with engine.connect() as conn:
            # MySQL 버전
            version_result = conn.execute(text("SELECT VERSION()"))
            version = version_result.fetchone()[0]
            
            # 데이터베이스 이름
            db_result = conn.execute(text("SELECT DATABASE()"))
            database = db_result.fetchone()[0]
            
            # 테이블 목록
            tables_result = conn.execute(text("SHOW TABLES"))
            tables = [row[0] for row in tables_result.fetchall()]
            
            return {
                "version": version,
                "database": database,
                "tables": tables,
                "connection_url": f"mysql://{settings.mysql_host}:{settings.mysql_port}/{settings.mysql_database}"
            }
    except Exception as e:
        logger.error(f"데이터베이스 정보 조회 실패: {e}")
        return {}


def check_tables_exist() -> dict:
    """
    필요한 테이블들이 존재하는지 확인
    
    Returns:
        dict: 테이블별 존재 여부
    """
    required_tables = [
        "bike_stations",
        "bike_status_history",
        "bike_availability_stats"
    ]
    
    result = {}
    try:
        with engine.connect() as conn:
            tables_result = conn.execute(text("SHOW TABLES"))
            existing_tables = [row[0] for row in tables_result.fetchall()]
            
            for table in required_tables:
                result[table] = table in existing_tables
                
        return result
    except Exception as e:
        logger.error(f"테이블 확인 실패: {e}")
        return {table: False for table in required_tables}


if __name__ == "__main__":
    # 직접 실행 시 연결 테스트
    print("=" * 60)
    print("MySQL 연결 테스트")
    print("=" * 60)
    
    if test_connection():
        print("✅ 연결 성공!")
        
        info = get_db_info()
        print(f"\n📊 데이터베이스 정보:")
        print(f"  - 버전: {info.get('version', 'N/A')}")
        print(f"  - 데이터베이스: {info.get('database', 'N/A')}")
        print(f"  - 테이블 수: {len(info.get('tables', []))}")
        print(f"  - 테이블: {', '.join(info.get('tables', []))}")
        
        tables_status = check_tables_exist()
        print(f"\n📋 필수 테이블 확인:")
        for table, exists in tables_status.items():
            status = "✅" if exists else "❌"
            print(f"  {status} {table}")
    else:
        print("❌ 연결 실패!")
