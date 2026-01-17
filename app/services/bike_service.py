"""
Bike Service - MySQL에서 대여소 및 자전거 데이터 조회
"""

from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from app.database.db import get_db

logger = logging.getLogger(__name__)


class BikeService:
    """자전거 대여소 데이터 서비스"""
    
    def get_all_stations(
        self, 
        db: Session, 
        limit: int = 100, 
        offset: int = 0
    ) -> tuple[List[Dict[str, Any]], int]:
        """
        모든 대여소 조회 (페이징)
        
        Args:
            db: 데이터베이스 세션
            limit: 조회할 개수
            offset: 건너뛸 개수
        
        Returns:
            tuple: (대여소 목록, 전체 개수)
        """
        try:
            # 전체 개수 조회
            count_query = text("SELECT COUNT(*) FROM bike_stations")
            total = db.execute(count_query).scalar()
            
            # 대여소 목록 조회 (최신 자전거 수 포함)
            query = text("""
                SELECT 
                    s.station_id,
                    s.station_name,
                    s.latitude,
                    s.longitude,
                    s.rack_total_count,
                    COALESCE(h.parking_bike_count, 0) as current_bikes,
                    h.recorded_at as last_updated
                FROM bike_stations s
                LEFT JOIN (
                    SELECT station_id, parking_bike_count, recorded_at
                    FROM bike_status_history
                    WHERE (station_id, recorded_at) IN (
                        SELECT station_id, MAX(recorded_at)
                        FROM bike_status_history
                        GROUP BY station_id
                    )
                ) h ON s.station_id = h.station_id
                ORDER BY s.station_name
                LIMIT :limit OFFSET :offset
            """)
            
            result = db.execute(query, {"limit": limit, "offset": offset})
            stations = [
                {
                    "station_id": row[0],
                    "station_name": row[1],
                    "lat": float(row[2]) if row[2] else None,
                    "lng": float(row[3]) if row[3] else None,
                    "capacity": int(row[4]),
                    "current_bikes": int(row[5]),
                    "last_updated": row[6]
                }
                for row in result.fetchall()
            ]
            
            return stations, total
            
        except Exception as e:
            logger.error(f"대여소 목록 조회 실패: {e}")
            raise
    
    
    def get_station_by_id(
        self, 
        db: Session, 
        station_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        특정 대여소 조회
        
        Args:
            db: 데이터베이스 세션
            station_id: 대여소 ID
        
        Returns:
            대여소 정보 또는 None
        """
        try:
            query = text("""
                SELECT 
                    s.station_id,
                    s.station_name,
                    s.latitude,
                    s.longitude,
                    s.rack_total_count,
                    COALESCE(h.parking_bike_count, 0) as current_bikes,
                    h.recorded_at as last_updated
                FROM bike_stations s
                LEFT JOIN (
                    SELECT station_id, parking_bike_count, recorded_at
                    FROM bike_status_history
                    WHERE (station_id, recorded_at) IN (
                        SELECT station_id, MAX(recorded_at)
                        FROM bike_status_history
                        WHERE station_id = :station_id
                        GROUP BY station_id
                    )
                ) h ON s.station_id = h.station_id
                WHERE s.station_id = :station_id
            """)
            
            result = db.execute(query, {"station_id": station_id})
            row = result.fetchone()
            
            if not row:
                return None
            
            return {
                "station_id": row[0],
                "station_name": row[1],
                "lat": float(row[2]) if row[2] else None,
                "lng": float(row[3]) if row[3] else None,
                "capacity": int(row[4]),
                "current_bikes": int(row[5]),
                "last_updated": row[6]
            }
            
        except Exception as e:
            logger.error(f"대여소 조회 실패 (station_id={station_id}): {e}")
            raise
    
    
    def search_stations(
        self, 
        db: Session, 
        query_text: str
    ) -> List[Dict[str, Any]]:
        """
        대여소 검색 (이름으로)
        공백을 무시하고 검색합니다.
        
        Args:
            db: 데이터베이스 세션
            query_text: 검색어
        
        Returns:
            검색 결과 목록
        """
        try:
            # 검색어에서 공백 제거
            search_normalized = query_text.replace(" ", "")
            
            query = text("""
                SELECT 
                    s.station_id,
                    s.station_name,
                    s.latitude,
                    s.longitude,
                    s.rack_total_count,
                    COALESCE(h.parking_bike_count, 0) as current_bikes,
                    h.recorded_at as last_updated
                FROM bike_stations s
                LEFT JOIN (
                    SELECT station_id, parking_bike_count, recorded_at
                    FROM bike_status_history
                    WHERE (station_id, recorded_at) IN (
                        SELECT station_id, MAX(recorded_at)
                        FROM bike_status_history
                        GROUP BY station_id
                    )
                ) h ON s.station_id = h.station_id
                WHERE REPLACE(s.station_name, ' ', '') LIKE :search
                   OR s.station_name LIKE :search_original
                ORDER BY s.station_name
                LIMIT 50
            """)
            
            result = db.execute(query, {
                "search": f"%{search_normalized}%",
                "search_original": f"%{query_text}%"
            })
            stations = [
                {
                    "station_id": row[0],
                    "station_name": row[1],
                    "lat": float(row[2]) if row[2] else None,
                    "lng": float(row[3]) if row[3] else None,
                    "capacity": int(row[4]),
                    "current_bikes": int(row[5]),
                    "last_updated": row[6]
                }
                for row in result.fetchall()
            ]
            
            return stations
            
        except Exception as e:
            logger.error(f"대여소 검색 실패 (query={query_text}): {e}")
            raise
    
    
    def get_station_history(
        self,
        db: Session,
        station_id: str,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        대여소 이력 조회 (최근 N시간)
        
        Args:
            db: 데이터베이스 세션
            station_id: 대여소 ID
            hours: 조회할 시간 (기본 24시간)
        
        Returns:
            이력 데이터 목록
        """
        try:
            query = text("""
                SELECT 
                    station_id,
                    parking_bike_count,
                    rack_total_count,
                    recorded_at
                FROM bike_status_history
                WHERE station_id = :station_id
                    AND recorded_at >= DATE_SUB(NOW(), INTERVAL :hours HOUR)
                ORDER BY recorded_at DESC
                LIMIT 500
            """)
            
            result = db.execute(query, {"station_id": station_id, "hours": hours})
            history = [
                {
                    "station_id": row[0],
                    "bikes_available": int(row[1]),
                    "capacity": int(row[2]),
                    "recorded_at": row[3],
                    "ratio": float(row[1]) / float(row[2]) if row[2] > 0 else 0.0
                }
                for row in result.fetchall()
            ]
            
            return history
            
        except Exception as e:
            logger.error(f"대여소 이력 조회 실패 (station_id={station_id}): {e}")
            raise


# 싱글톤 인스턴스
bike_service = BikeService()
