"""
따릉이 관련 Pydantic 스키마
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


# ===== 스테이션 스키마 =====

class StationBase(BaseModel):
    """스테이션 기본 스키마"""
    station_id: str = Field(..., description="스테이션 ID")
    station_name: str = Field(..., description="스테이션 이름")


class StationInfo(StationBase):
    """스테이션 상세 정보"""
    address: Optional[str] = Field(None, description="주소")
    lat: Optional[float] = Field(None, description="위도")
    lng: Optional[float] = Field(None, description="경도")
    capacity: int = Field(..., description="총 거치대 수")


class StationResponse(StationInfo):
    """스테이션 조회 응답"""
    pass


class StationListResponse(BaseModel):
    """스테이션 목록 응답"""
    total: int = Field(..., description="총 스테이션 수")
    stations: List[StationInfo]


# ===== 실시간 자전거 데이터 스키마 =====

class BikeRealtimeData(BaseModel):
    """실시간 자전거 데이터"""
    station_id: str = Field(..., description="스테이션 ID")
    station_name: str = Field(..., description="스테이션 이름")
    bikes_available: int = Field(..., description="잔여 자전거 수")
    capacity: int = Field(..., description="총 거치대 수")
    timestamp: datetime = Field(..., description="데이터 수집 시간")
    
    @property
    def ratio(self) -> float:
        """잔여 비율 계산"""
        if self.capacity == 0:
            return 0.0
        return self.bikes_available / self.capacity


class BikeRealtimeResponse(BikeRealtimeData):
    """실시간 데이터 응답"""
    ratio: float = Field(..., description="잔여 비율 (0~1)")
    status: str = Field(..., description="상태 (high/medium/low/critical)")
    status_label: str = Field(..., description="상태 라벨 (여유/보통/부족/거의 불가능)")
    status_emoji: str = Field(..., description="상태 이모지")


# ===== 시간대별 통계 스키마 =====

class HourlyStats(BaseModel):
    """시간대별 통계"""
    hour: int = Field(..., ge=0, le=23, description="시간 (0~23)")
    avg_available: float = Field(..., description="평균 잔여 대수")
    avg_ratio: float = Field(..., description="평균 비율 (0~1)")
    status: str = Field(..., description="상태 (high/medium/low/critical)")
    status_label: str = Field(..., description="상태 라벨")
    status_emoji: str = Field(..., description="상태 이모지")
    sample_count: int = Field(..., description="샘플 수")


class DailyPattern(BaseModel):
    """일별 패턴 (요일 포함)"""
    day_of_week: int = Field(..., ge=0, le=6, description="요일 (0=월요일)")
    day_name: str = Field(..., description="요일 이름")
    hourly_stats: List[HourlyStats] = Field(..., description="시간대별 통계")


# ===== 가용성 응답 스키마 =====

class AvailabilityHeatmapResponse(BaseModel):
    """시간대별 가용성 히트맵 응답"""
    station_id: str = Field(..., description="스테이션 ID")
    station_name: str = Field(..., description="스테이션 이름")
    capacity: int = Field(..., description="총 거치대 수")
    hourly_availability: List[HourlyStats] = Field(..., description="24시간 가용성")
    last_updated: Optional[datetime] = Field(None, description="마지막 업데이트 시간")


class AvailabilityRealtimeResponse(BaseModel):
    """실시간 가용성 응답"""
    station_id: str
    station_name: str
    bikes_available: int
    capacity: int
    ratio: float
    status: str
    status_label: str
    status_emoji: str
    timestamp: datetime


class StationAvailabilityFull(BaseModel):
    """스테이션 전체 가용성 정보 (실시간 + 히트맵)"""
    station_info: StationInfo
    realtime: AvailabilityRealtimeResponse
    heatmap: List[HourlyStats]
    recommendation: Optional[str] = Field(None, description="추천 메시지")


# ===== 추천 스키마 =====

class RecommendationResponse(BaseModel):
    """대여 시간 추천 응답"""
    station_id: str
    station_name: str
    current_status: str
    current_status_label: str
    recommended_hours: List[int] = Field(..., description="추천 시간대")
    avoid_hours: List[int] = Field(..., description="피해야 할 시간대")
    message: str = Field(..., description="추천 메시지")


# ===== Kafka 메시지 스키마 =====

class KafkaBikeMessage(BaseModel):
    """Kafka로 전송되는 자전거 데이터 메시지"""
    station_id: str
    station_name: str
    bikes_available: int
    capacity: int
    lat: Optional[float]
    lng: Optional[float]
    timestamp: datetime
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
