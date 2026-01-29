"""
스테이션 API 라우터 - MySQL 연동
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import List, Optional

from app.schemas.bike import StationInfo, StationListResponse, StationResponse
from app.services.bike_service import bike_service
from app.database.db import get_db

router = APIRouter()


@router.get("", response_model=StationListResponse)
async def get_stations(
    limit: int = Query(100, ge=1, le=1000, description="조회할 스테이션 수"),
    offset: int = Query(0, ge=0, description="건너뛸 스테이션 수"),
    db: Session = Depends(get_db)
):
    """
    스테이션 목록 조회
    
    MySQL에서 전체 따릉이 스테이션 목록을 조회합니다.
    """
    try:
        stations_data, total = bike_service.get_all_stations(db, limit, offset)
        
        # Pydantic 모델로 변환
        stations = [
            StationInfo(
                station_id=s["station_id"],
                station_name=s["station_name"],
                address=None,  # stationName에 위치 정보 포함됨
                lat=s["lat"],
                lng=s["lng"],
                capacity=s["capacity"]
            )
            for s in stations_data
        ]
        
        return StationListResponse(
            total=total,
            stations=stations
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스테이션 조회 중 오류 발생: {str(e)}")


@router.get("/search")
async def search_stations(
    query: str = Query(..., min_length=1, description="검색어 (스테이션 이름)"),
    db: Session = Depends(get_db)
):
    """
    스테이션 검색
    
    스테이션 이름으로 검색합니다.
    """
    try:
        stations_data = bike_service.search_stations(db, query)
        
        return {
            "query": query,
            "count": len(stations_data),
            "stations": [
                {
                    "station_id": s["station_id"],
                    "station_name": s["station_name"],
                    "lat": s["lat"],
                    "lng": s["lng"],
                    "capacity": s["capacity"],
                    "current_bikes": s["current_bikes"]
                }
                for s in stations_data
            ]
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"검색 중 오류 발생: {str(e)}")


@router.get("/nearby")
async def get_nearby_stations(
    lat: float = Query(..., description="위도"),
    lng: float = Query(..., description="경도"),
    radius: float = Query(1.0, ge=0.1, le=5.0, description="검색 반경 (km)"),
    limit: int = Query(20, ge=1, le=50, description="최대 결과 수"),
    db: Session = Depends(get_db)
):
    """
    근처 스테이션 검색 (좌표 기반)
    
    주어진 좌표에서 반경 내의 따릉이 스테이션을 거리순으로 조회합니다.
    """
    try:
        stations_data = bike_service.get_nearby_stations(db, lat, lng, radius, limit)
        
        return {
            "lat": lat,
            "lng": lng,
            "radius_km": radius,
            "count": len(stations_data),
            "stations": stations_data
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"근처 스테이션 검색 중 오류 발생: {str(e)}")


@router.get("/map/all")
async def get_all_stations_for_map(
    db: Session = Depends(get_db)
):
    """
    지도 표시용 전체 스테이션 조회
    
    모든 따릉이 스테이션의 위치와 현재 자전거 수를 반환합니다.
    지도에 마커를 표시하기 위한 API입니다.
    """
    try:
        stations_data = bike_service.get_all_stations_for_map(db)
        
        return {
            "count": len(stations_data),
            "stations": stations_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스테이션 조회 중 오류 발생: {str(e)}")


@router.get("/{station_id}", response_model=StationResponse)
async def get_station(
    station_id: str,
    db: Session = Depends(get_db)
):
    """
    특정 스테이션 정보 조회
    
    스테이션 ID로 상세 정보를 조회합니다.
    """
    try:
        station_data = bike_service.get_station_by_id(db, station_id)
        
        if not station_data:
            raise HTTPException(status_code=404, detail=f"스테이션을 찾을 수 없습니다: {station_id}")
        
        return StationResponse(
            station_id=station_data["station_id"],
            station_name=station_data["station_name"],
            address=None,  # stationName에 위치 정보 포함됨
            lat=station_data["lat"],
            lng=station_data["lng"],
            capacity=station_data["capacity"]
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스테이션 조회 중 오류 발생: {str(e)}")


@router.get("/{station_id}/history")
async def get_station_history(
    station_id: str,
    hours: int = Query(24, ge=1, le=168, description="조회할 시간 (1~168시간)"),
    db: Session = Depends(get_db)
):
    """
    스테이션 이력 조회
    
    최근 N시간의 자전거 대여/반납 이력을 조회합니다.
    """
    try:
        history_data = bike_service.get_station_history(db, station_id, hours)
        
        if not history_data:
            raise HTTPException(status_code=404, detail=f"스테이션을 찾을 수 없습니다: {station_id}")
        
        return {
            "station_id": station_id,
            "hours": hours,
            "count": len(history_data),
            "history": history_data
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"이력 조회 중 오류 발생: {str(e)}")
