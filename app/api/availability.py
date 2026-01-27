from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime

from app.schemas.bike import (
    AvailabilityHeatmapResponse,
    AvailabilityRealtimeResponse,
    RecommendationResponse,
    StationAvailabilityFull,
    StationInfo,
    HourlyStats
)
from app.services.heatmap_service import heatmap_service
from app.services.bike_service import bike_service
from app.database.db import get_db

router = APIRouter()


@router.get("/{station_id}", response_model=AvailabilityHeatmapResponse)
async def get_availability_heatmap(
    station_id: str,
    day_of_week: Optional[int] = Query(None, ge=0, le=6, description="요일 (0=월요일, 6=일요일)"),
    db: Session = Depends(get_db)
):
    """
    시간대별 가용성 히트맵 조회

    - 🟩 여유 (60% 이상)
    - 🟨 보통 (30~60%)
    - 🟧 부족 (10~30%)
    - 🟥 거의 불가능 (10% 미만)
    """
    try:
        heatmap_data = heatmap_service.get_hourly_heatmap(db, station_id, day_of_week)
        
        return AvailabilityHeatmapResponse(
            station_id=heatmap_data["station_id"],
            station_name=heatmap_data["station_name"],
            capacity=heatmap_data["capacity"],
            hourly_availability=[
                HourlyStats(**h) for h in heatmap_data["hourly_availability"]
            ],
            last_updated=heatmap_data["last_updated"]
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"히트맵 조회 중 오류 발생: {str(e)}")


@router.get("/{station_id}/realtime", response_model=AvailabilityRealtimeResponse)
async def get_realtime_availability(
    station_id: str,
    db: Session = Depends(get_db)
):
    """
    실시간 가용성 조회
    
    현재 시점의 잔여 자전거 수와 상태를 반환합니다.
    """
    try:
        realtime_data = heatmap_service.get_realtime_availability(db, station_id)
        
        if not realtime_data:
            raise HTTPException(status_code=404, detail=f"스테이션을 찾을 수 없습니다: {station_id}")
        
        return AvailabilityRealtimeResponse(**realtime_data)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"실시간 데이터 조회 중 오류 발생: {str(e)}")


@router.get("/{station_id}/full", response_model=StationAvailabilityFull)
async def get_full_availability(
    station_id: str,
    day_of_week: Optional[int] = Query(None, ge=0, le=6, description="요일"),
    db: Session = Depends(get_db)
):
    """
    전체 가용성 정보 조회 (실시간 + 히트맵 + 추천)
    
    스테이션의 모든 가용성 정보를 한 번에 조회합니다.
    """
    try:
        # 스테이션 정보 조회
        station_data = bike_service.get_station_by_id(db, station_id)
        if not station_data:
            raise HTTPException(status_code=404, detail=f"스테이션을 찾을 수 없습니다: {station_id}")
        
        station_info = StationInfo(
            station_id=station_data["station_id"],
            station_name=station_data["station_name"],
            address=None,
            lat=station_data["lat"],
            lng=station_data["lng"],
            capacity=station_data["capacity"]
        )
        
        # 실시간 데이터 조회
        realtime_data = heatmap_service.get_realtime_availability(db, station_id)
        if not realtime_data:
            raise HTTPException(status_code=404, detail=f"실시간 데이터를 조회할 수 없습니다: {station_id}")
        
        realtime = AvailabilityRealtimeResponse(**realtime_data)
        
        # 히트맵 조회
        heatmap_data = heatmap_service.get_hourly_heatmap(db, station_id, day_of_week)
        hourly_stats = [HourlyStats(**h) for h in heatmap_data["hourly_availability"]]
        
        # 추천 메시지 생성
        recommendation = heatmap_service.generate_recommendation(
            heatmap_data["hourly_availability"],
            datetime.now().hour,
            realtime_data
        )
        
        return StationAvailabilityFull(
            station_info=station_info,
            realtime=realtime,
            heatmap=hourly_stats,
            recommendation=recommendation
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 중 오류 발생: {str(e)}")


@router.get("/{station_id}/recommend", response_model=RecommendationResponse)
async def get_recommendation(
    station_id: str,
    day_of_week: Optional[int] = Query(None, ge=0, le=6, description="요일"),
    db: Session = Depends(get_db)
):
    """
    최적 대여 시간 추천
    
    현재 시간 기준으로 최적의 대여 시간을 추천합니다.
    """
    try:
        # 히트맵 조회
        heatmap_data = heatmap_service.get_hourly_heatmap(db, station_id, day_of_week)
        
        # 실시간 조회
        realtime_data = heatmap_service.get_realtime_availability(db, station_id)
        if not realtime_data:
            raise HTTPException(status_code=404, detail=f"스테이션을 찾을 수 없습니다: {station_id}")
        
        # 추천 메시지 생성
        message = heatmap_service.generate_recommendation(
            heatmap_data["hourly_availability"],
            datetime.now().hour,
            realtime_data
        )
        
        # 피해야 할 시간대
        avoid_hours = [
            h["hour"] for h in heatmap_data["hourly_availability"]
            if h["status"] in ["critical", "low"] and h["sample_count"] > 0
        ]
        
        # 추천 시간대
        recommended_hours = [
            h["hour"] for h in heatmap_data["hourly_availability"]
            if h["status"] == "high" and h["sample_count"] > 0
        ]
        
        return RecommendationResponse(
            station_id=station_id,
            station_name=heatmap_data["station_name"],
            current_status=realtime_data["status"],
            current_status_label=realtime_data["status_label"],
            recommended_hours=recommended_hours,
            avoid_hours=avoid_hours,
            message=message
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"추천 생성 중 오류 발생: {str(e)}")


@router.get("/{station_id}/weekly")
async def get_weekly_heatmap(
    station_id: str,
    db: Session = Depends(get_db)
):
    """
    주간 히트맵 조회 (월~일, 24시간)
    
    특정 대여소의 요일별/시간별 전체 가용성 데이터를 조회합니다.
    1주간의 데이터를 기반으로 평균을 계산합니다.
    
    Returns:
        - station_id: 대여소 ID
        - station_name: 대여소 이름
        - capacity: 거치대 수
        - day_names: 요일 이름 목록 ["월요일", ..., "일요일"]
        - weekly_data: 요일별(0~6) 24시간 데이터
    """
    try:
        weekly_data = heatmap_service.get_weekly_heatmap(db, station_id)
        return weekly_data
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"주간 히트맵 조회 중 오류 발생: {str(e)}")


@router.get("/heatmap/all")
async def get_all_stations_heatmap(
    hour: int = Query(..., ge=0, le=23, description="시간 (0~23)"),
    day_of_week: Optional[int] = Query(None, ge=0, le=6, description="요일"),
    limit: int = Query(100, ge=1, le=1000, description="최대 조회 개수"),
    db: Session = Depends(get_db)
):
    """
    전체 대여소 히트맵 조회 (특정 시간대)
    
    모든 대여소의 특정 시간대 가용성을 한 번에 조회합니다.
    지도에 히트맵 오버레이로 표시하기 위한 API입니다.
    """
    try:
        heatmap_data = heatmap_service.get_all_stations_heatmap(
            db, hour, day_of_week, limit
        )
        
        return {
            "hour": hour,
            "day_of_week": day_of_week,
            "count": len(heatmap_data),
            "stations": heatmap_data
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"히트맵 조회 중 오류 발생: {str(e)}")
