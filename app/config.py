"""
애플리케이션 설정 파일
환경 변수를 로드하고 관리합니다.
"""

from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    """애플리케이션 설정 클래스"""
    
    # 서버 설정
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = True
    reload: bool = True
    
    # 애플리케이션 정보
    app_name: str = "Seoul Bike Heat Map"
    app_version: str = "1.0.0"
    secret_key: str = "your-secret-key-change-in-production"
    
    # 서울 열린데이터 API
    seoul_api_key: Optional[str] = None
    seoul_api_base_url: str = "http://openapi.seoul.go.kr:8088"
    
    # Kafka 설정
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw: str = "raw-bike-data"
    kafka_topic_processed: str = "processed-bike-data"
    kafka_consumer_group: str = "bike-processor"
    
    # Redis 설정
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    
    # MySQL 데이터베이스 설정
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_database: str = "seoul_bike"
    mysql_user: str = "hch16"
    mysql_password: str = "cksgh970216!"
    
    # SQLAlchemy Database URL (자동 생성)
    @property
    def database_url(self) -> str:
        return f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
    
    # 데이터 수집 설정
    polling_interval_seconds: int = 60
    stats_days_lookback: int = 30
    
    # 가용성 임계값
    threshold_high: float = 0.6
    threshold_medium: float = 0.3
    threshold_low: float = 0.1
    # 10% 미만: critical
    
    # 캐시 TTL (초)
    cache_realtime_ttl: int = 60
    cache_stats_ttl: int = 3600
    
    # 로그 설정
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """
    설정 인스턴스를 반환합니다.
    lru_cache로 싱글톤 패턴 구현
    """
    return Settings()


# 전역 설정 인스턴스
settings = get_settings()


# 가용성 상태 계산 헬퍼
def get_availability_status(ratio: float) -> str:
    """
    비율에 따른 가용성 상태 반환
    
    Args:
        ratio: 잔여 자전거 비율 (0~1)
    
    Returns:
        상태 문자열 (high/medium/low/critical)
    """
    if ratio >= settings.threshold_high:
        return "high"
    elif ratio >= settings.threshold_medium:
        return "medium"
    elif ratio >= settings.threshold_low:
        return "low"
    else:
        return "critical"


def get_status_emoji(status: str) -> str:
    """상태에 해당하는 이모지 반환"""
    emojis = {
        "high": "🟩",
        "medium": "🟨",
        "low": "🟧",
        "critical": "🟥"
    }
    return emojis.get(status, "⬜")


def get_status_label(status: str) -> str:
    """상태에 해당하는 한글 라벨 반환"""
    labels = {
        "high": "여유",
        "medium": "보통",
        "low": "부족",
        "critical": "거의 불가능"
    }
    return labels.get(status, "알 수 없음")
