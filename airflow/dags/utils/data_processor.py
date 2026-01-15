"""
따릉이 데이터 전처리 유틸리티
"""
from typing import Dict, List, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def clean_station_data(raw_data: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    API 응답 데이터를 정제하여 DB 저장 형식으로 변환
    
    Args:
        raw_data: 서울 API에서 받은 원본 데이터
        
    Returns:
        (stations, status_history) 튜플
        - stations: bike_stations 테이블용 데이터
        - status_history: bike_status_history 테이블용 데이터
    """
    stations = []
    status_history = []
    recorded_at = datetime.now()
    
    for item in raw_data:
        try:
            # 필수 필드 확인
            station_id = item.get("stationId", "").strip()
            if not station_id:
                logger.warning(f"Missing stationId in item: {item}")
                continue
            
            station_name = item.get("stationName", "").strip()
            latitude = float(item.get("stationLatitude", 0))
            longitude = float(item.get("stationLongitude", 0))
            rack_total = int(item.get("rackTotCnt", 0))
            parking_bike = int(item.get("parkingBikeTotCnt", 0))
            
            # 위도/경도 유효성 검사
            if not (37.0 <= latitude <= 38.0 and 126.0 <= longitude <= 127.5):
                logger.warning(f"Invalid coordinates for {station_id}: ({latitude}, {longitude})")
                continue
            
            # 대여소 정보
            station = {
                "station_id": station_id,
                "station_name": station_name,
                "latitude": latitude,
                "longitude": longitude,
                "rack_total_count": rack_total
            }
            stations.append(station)
            
            # 상태 이력
            status = {
                "station_id": station_id,
                "parking_bike_count": parking_bike,
                "rack_total_count": rack_total,
                "recorded_at": recorded_at
            }
            status_history.append(status)
            
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to process item: {e}, data: {item}")
            continue
    
    logger.info(f"Processed {len(stations)} stations, {len(status_history)} status records")
    return stations, status_history


def calculate_availability(parking_count: int, total_count: int) -> float:
    """
    자전거 대여 가능율 계산
    
    Args:
        parking_count: 현재 주차된 자전거 수
        total_count: 총 거치대 수
        
    Returns:
        대여 가능율 (0-100%)
    """
    if total_count == 0:
        return 0.0
    return round((parking_count / total_count) * 100, 2)


def validate_station(station: Dict) -> bool:
    """
    대여소 데이터 유효성 검증
    
    Args:
        station: 대여소 정보 딕셔너리
        
    Returns:
        유효하면 True
    """
    required_fields = ["station_id", "station_name", "latitude", "longitude", "rack_total_count"]
    
    # 필수 필드 확인
    for field in required_fields:
        if field not in station:
            logger.warning(f"Missing field: {field}")
            return False
    
    # 위도/경도 범위 확인 (서울 지역)
    lat = station["latitude"]
    lng = station["longitude"]
    if not (37.0 <= lat <= 38.0 and 126.0 <= lng <= 127.5):
        logger.warning(f"Invalid coordinates: ({lat}, {lng})")
        return False
    
    # 거치대 수 확인
    if station["rack_total_count"] <= 0:
        logger.warning(f"Invalid rack_total_count: {station['rack_total_count']}")
        return False
    
    return True


if __name__ == "__main__":
    # 테스트용
    sample_data = [
        {
            "stationId": "ST-001",
            "stationName": "101. 테스트역 1번출구",
            "stationLatitude": "37.5665",
            "stationLongitude": "126.9780",
            "rackTotCnt": "20",
            "parkingBikeTotCnt": "15"
        }
    ]
    
    stations, history = clean_station_data(sample_data)
    print(f"Stations: {stations}")
    print(f"History: {history}")
