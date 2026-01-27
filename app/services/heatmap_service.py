"""
Heatmap Service - 시간대별 히트맵 데이터 생성
MySQL의 bike_availability_stats 테이블에서 통계 데이터 조회
"""

from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from app.config import settings, get_availability_status, get_status_emoji, get_status_label

logger = logging.getLogger(__name__)


class HeatmapService:
    """히트맵 데이터 서비스"""
    
    def get_hourly_heatmap(
        self, 
        db: Session, 
        station_id: str,
        day_of_week: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        시간대별 히트맵 데이터 조회
        
        Args:
            db: 데이터베이스 세션
            station_id: 대여소 ID
            day_of_week: 요일 (0=월요일, 6=일요일). None이면 전체 요일 평균
        
        Returns:
            히트맵 데이터 딕셔너리
        """
        try:
            # 대여소 기본 정보 조회
            station_query = text("""
                SELECT station_name, rack_total_count
                FROM bike_stations
                WHERE station_id = :station_id
            """)
            station_result = db.execute(station_query, {"station_id": station_id})
            station_row = station_result.fetchone()
            
            if not station_row:
                raise ValueError(f"대여소를 찾을 수 없습니다: {station_id}")
            
            station_name = station_row[0]
            capacity = int(station_row[1])
            
            # 시간대별 통계 조회
            if day_of_week is not None:
                # 특정 요일
                stats_query = text("""
                    SELECT 
                        hour_of_day,
                        avg_availability,
                        avg_parking_count,
                        sample_count,
                        last_updated
                    FROM bike_availability_stats
                    WHERE station_id = :station_id
                        AND day_of_week = :day_of_week
                    ORDER BY hour_of_day
                """)
                stats_result = db.execute(
                    stats_query, 
                    {"station_id": station_id, "day_of_week": day_of_week}
                )
            else:
                # 전체 요일 평균
                stats_query = text("""
                    SELECT 
                        hour_of_day,
                        AVG(avg_availability) as avg_availability,
                        AVG(avg_parking_count) as avg_parking_count,
                        SUM(sample_count) as sample_count,
                        MAX(last_updated) as last_updated
                    FROM bike_availability_stats
                    WHERE station_id = :station_id
                    GROUP BY hour_of_day
                    ORDER BY hour_of_day
                """)
                stats_result = db.execute(stats_query, {"station_id": station_id})
            
            # 24시간 데이터 생성
            hourly_data = []
            stats_dict = {
                row[0]: {
                    "avg_availability": float(row[1]),
                    "avg_parking_count": float(row[2]),
                    "sample_count": int(row[3]),
                    "last_updated": row[4]
                }
                for row in stats_result.fetchall()
            }
            
            for hour in range(24):
                if hour in stats_dict:
                    stats = stats_dict[hour]
                    avg_ratio = stats["avg_availability"] / 100.0  # 0~100 -> 0~1
                    status = get_availability_status(avg_ratio)
                    
                    hourly_data.append({
                        "hour": hour,
                        "avg_available": round(stats["avg_parking_count"], 1),
                        "avg_ratio": round(avg_ratio, 3),
                        "status": status,
                        "status_label": get_status_label(status),
                        "status_emoji": get_status_emoji(status),
                        "sample_count": stats["sample_count"]
                    })
                else:
                    # 데이터 없는 시간대는 기본값
                    hourly_data.append({
                        "hour": hour,
                        "avg_available": 0.0,
                        "avg_ratio": 0.0,
                        "status": "unknown",
                        "status_label": "데이터 없음",
                        "status_emoji": "⬜",
                        "sample_count": 0
                    })
            
            return {
                "station_id": station_id,
                "station_name": station_name,
                "capacity": capacity,
                "day_of_week": day_of_week,
                "hourly_availability": hourly_data,
                "last_updated": max(
                    (stats_dict[h]["last_updated"] for h in stats_dict if stats_dict[h]["last_updated"]),
                    default=None
                )
            }
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"히트맵 조회 실패 (station_id={station_id}): {e}")
            raise
    
    
    def get_realtime_availability(
        self, 
        db: Session, 
        station_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        실시간 가용성 조회
        
        Args:
            db: 데이터베이스 세션
            station_id: 대여소 ID
        
        Returns:
            실시간 가용성 데이터 또는 None
        """
        try:
            query = text("""
                SELECT 
                    s.station_id,
                    s.station_name,
                    s.rack_total_count,
                    COALESCE(h.parking_bike_count, 0) as current_bikes,
                    h.recorded_at
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
            
            capacity = int(row[2])
            current_bikes = int(row[3])
            ratio = current_bikes / capacity if capacity > 0 else 0.0
            status = get_availability_status(ratio)
            
            return {
                "station_id": row[0],
                "station_name": row[1],
                "bikes_available": current_bikes,
                "capacity": capacity,
                "ratio": round(ratio, 3),
                "status": status,
                "status_label": get_status_label(status),
                "status_emoji": get_status_emoji(status),
                "timestamp": row[4] or datetime.now()
            }
            
        except Exception as e:
            logger.error(f"실시간 가용성 조회 실패 (station_id={station_id}): {e}")
            raise
    
    
    def get_all_stations_heatmap(
        self,
        db: Session,
        hour: int,
        day_of_week: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        전체 대여소의 특정 시간대 히트맵 조회
        
        Args:
            db: 데이터베이스 세션
            hour: 시간 (0~23)
            day_of_week: 요일 (0=월요일, 6=일요일). None이면 전체 요일 평균
            limit: 최대 조회 개수
        
        Returns:
            대여소별 히트맵 데이터 목록
        """
        try:
            if day_of_week is not None:
                query = text("""
                    SELECT 
                        s.station_id,
                        s.station_name,
                        s.latitude,
                        s.longitude,
                        s.rack_total_count,
                        st.avg_availability,
                        st.avg_parking_count,
                        st.sample_count
                    FROM bike_stations s
                    LEFT JOIN bike_availability_stats st 
                        ON s.station_id = st.station_id
                        AND st.hour_of_day = :hour
                        AND st.day_of_week = :day_of_week
                    WHERE s.latitude IS NOT NULL 
                        AND s.longitude IS NOT NULL
                    ORDER BY s.station_name
                    LIMIT :limit
                """)
                result = db.execute(
                    query,
                    {"hour": hour, "day_of_week": day_of_week, "limit": limit}
                )
            else:
                query = text("""
                    SELECT 
                        s.station_id,
                        s.station_name,
                        s.latitude,
                        s.longitude,
                        s.rack_total_count,
                        AVG(st.avg_availability) as avg_availability,
                        AVG(st.avg_parking_count) as avg_parking_count,
                        SUM(st.sample_count) as sample_count
                    FROM bike_stations s
                    LEFT JOIN bike_availability_stats st 
                        ON s.station_id = st.station_id
                        AND st.hour_of_day = :hour
                    WHERE s.latitude IS NOT NULL 
                        AND s.longitude IS NOT NULL
                    GROUP BY s.station_id, s.station_name, s.latitude, s.longitude, s.rack_total_count
                    ORDER BY s.station_name
                    LIMIT :limit
                """)
                result = db.execute(query, {"hour": hour, "limit": limit})
            
            heatmap_data = []
            for row in result.fetchall():
                avg_availability = float(row[5]) if row[5] is not None else 0.0
                avg_ratio = avg_availability / 100.0
                status = get_availability_status(avg_ratio)
                
                heatmap_data.append({
                    "station_id": row[0],
                    "station_name": row[1],
                    "lat": float(row[2]),
                    "lng": float(row[3]),
                    "capacity": int(row[4]),
                    "avg_available": round(float(row[6] or 0), 1),
                    "avg_ratio": round(avg_ratio, 3),
                    "status": status,
                    "status_label": get_status_label(status),
                    "status_emoji": get_status_emoji(status),
                    "sample_count": int(row[7] or 0)
                })
            
            return heatmap_data
            
        except Exception as e:
            logger.error(f"전체 히트맵 조회 실패 (hour={hour}): {e}")
            raise
    
    
    def get_weekly_heatmap(
        self,
        db: Session,
        station_id: str
    ) -> Dict[str, Any]:
        """
        요일별 전체 히트맵 데이터 조회 (월~일, 24시간)
        
        Args:
            db: 데이터베이스 세션
            station_id: 대여소 ID
        
        Returns:
            요일별 히트맵 데이터 (7일 x 24시간)
        """
        try:
            # 대여소 기본 정보 조회
            station_query = text("""
                SELECT station_name, rack_total_count
                FROM bike_stations
                WHERE station_id = :station_id
            """)
            station_result = db.execute(station_query, {"station_id": station_id})
            station_row = station_result.fetchone()
            
            if not station_row:
                raise ValueError(f"대여소를 찾을 수 없습니다: {station_id}")
            
            station_name = station_row[0]
            capacity = int(station_row[1])
            
            # 모든 요일/시간대 통계 조회
            stats_query = text("""
                SELECT 
                    day_of_week,
                    hour_of_day,
                    avg_availability,
                    avg_parking_count,
                    sample_count,
                    last_updated
                FROM bike_availability_stats
                WHERE station_id = :station_id
                ORDER BY day_of_week, hour_of_day
            """)
            stats_result = db.execute(stats_query, {"station_id": station_id})
            
            # 요일별 데이터 구조 초기화
            day_names = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]
            weekly_data = {day: [] for day in range(7)}
            
            # 조회된 데이터를 딕셔너리로 변환
            stats_dict = {}
            for row in stats_result.fetchall():
                day = row[0]
                hour = row[1]
                if day not in stats_dict:
                    stats_dict[day] = {}
                stats_dict[day][hour] = {
                    "avg_availability": float(row[2]),
                    "avg_parking_count": float(row[3]),
                    "sample_count": int(row[4]),
                    "last_updated": row[5]
                }
            
            # 7일 x 24시간 데이터 생성
            for day in range(7):
                for hour in range(24):
                    if day in stats_dict and hour in stats_dict[day]:
                        stats = stats_dict[day][hour]
                        avg_ratio = stats["avg_availability"] / 100.0
                        status = get_availability_status(avg_ratio)
                        
                        weekly_data[day].append({
                            "hour": hour,
                            "avg_available": round(stats["avg_parking_count"], 1),
                            "avg_ratio": round(avg_ratio, 3),
                            "status": status,
                            "status_label": get_status_label(status),
                            "status_emoji": get_status_emoji(status),
                            "sample_count": stats["sample_count"]
                        })
                    else:
                        # 데이터 없는 시간대
                        weekly_data[day].append({
                            "hour": hour,
                            "avg_available": 0.0,
                            "avg_ratio": 0.0,
                            "status": "unknown",
                            "status_label": "데이터 없음",
                            "status_emoji": "⬜",
                            "sample_count": 0
                        })
            
            return {
                "station_id": station_id,
                "station_name": station_name,
                "capacity": capacity,
                "day_names": day_names,
                "weekly_data": weekly_data
            }
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"주간 히트맵 조회 실패 (station_id={station_id}): {e}")
            raise
    

    def generate_recommendation(
        self,
        hourly_data: List[Dict[str, Any]],
        current_hour: int,
        realtime_data: Dict[str, Any] = None
    ) -> str:
        """
        추천 메시지 생성 (실시간 + 통계 비교)
        
        Args:
            hourly_data: 시간대별 통계 데이터
            current_hour: 현재 시간
            realtime_data: 실시간 데이터
        
        Returns:
            추천 메시지
        """
        # 상태 우선순위
        STATUS_LEVEL = {"high": 3, "medium": 2, "low": 1, "critical": 0}
        
        # 주간 시간대 정의 (7시 ~ 22시)
        DAYTIME_HOURS = set(range(7, 23))
        
        # 가장 여유로운 시간대 찾기 (통계 기반)
        best_hours = []
        if hourly_data:
            best_hours = [
                d["hour"] for d in hourly_data 
                if d["status"] == "high" and d["sample_count"] > 0
            ]
        
        # 주간 시간대 중 여유로운 시간 필터링
        daytime_best_hours = [h for h in best_hours if h in DAYTIME_HOURS]
        
        # 추천 시간대가 모두 새벽/밤 시간대인지 확인
        def is_only_night_hours(hours: List[int]) -> bool:
            if not hours:
                return False
            return all(h not in DAYTIME_HOURS for h in hours)
        
        # 추천 시간 문자열 생성 (주간 시간대 우선)
        def get_recommend_hours_str(hours: List[int], limit: int = 3) -> str:
            # 주간 시간대 우선
            daytime = [h for h in hours if h in DAYTIME_HOURS]
            if daytime:
                return ', '.join(f'{h}시' for h in daytime[:limit])
            # 주간 시간대가 없으면 전체에서 선택
            return ', '.join(f'{h}시' for h in hours[:limit])
        
        # 통계 데이터에서 현재 시간대 상태 확인
        stats_status = None
        if hourly_data:
            current_stats = next((d for d in hourly_data if d["hour"] == current_hour), None)
            if current_stats and current_stats.get("sample_count", 0) > 0:
                stats_status = current_stats.get("status")
        
        # 주간 시간대에 여유로운 시간이 전혀 없는지 확인
        no_daytime_availability = best_hours and not daytime_best_hours
        
        # 실시간 데이터와 통계 데이터 비교
        if realtime_data:
            realtime_status = realtime_data.get("status", "unknown")
            realtime_ratio = realtime_data.get("ratio", 0)
            realtime_percentage = int(realtime_ratio * 100)
            
            realtime_level = STATUS_LEVEL.get(realtime_status, -1)
            stats_level = STATUS_LEVEL.get(stats_status, -1) if stats_status else -1
            
            # 실시간 여유 (high/medium)
            if realtime_level >= 2:
                if stats_level >= 2:
                    # 실시간 여유 + 통계 여유
                    message = f"현재 자전거가 여유롭습니다! ({realtime_percentage}%) 지금 대여하기 좋은 시간입니다."
                elif stats_level >= 0:
                    # 실시간 여유 + 통계 부족
                    message = f"현재는 여유롭지만 ({realtime_percentage}%), 평균적으로는 여유롭지 않은 시간대입니다. 서둘러 대여하세요!"
                else:
                    # 통계 데이터 없음
                    message = f"현재 자전거가 여유롭습니다! ({realtime_percentage}%) 지금 대여하기 좋은 시간입니다."
            
            # 실시간 부족 (low/critical)
            else:
                # 주간 시간대에 여유로운 시간이 없는 경우 특별 메시지
                if no_daytime_availability:
                    message = f"현재 대여가 어렵고 ({realtime_percentage}%), 해당 대여소는 주간 시간대(7~22시)에 거의 대여가 불가능해 보입니다. 실시간 현황을 확인하여 이용하거나 근처 다른 대여소 이용을 추천드립니다."
                elif stats_level >= 2:
                    # 실시간 부족 + 통계 여유
                    message = f"현재는 대여가 어렵지만 ({realtime_percentage}%), 평균적으로는 여유로운 시간대입니다. 잠시 후 다시 확인해보세요."
                elif stats_level >= 0:
                    # 실시간 부족 + 통계도 부족
                    message = f"현재도 부족하고 ({realtime_percentage}%), 평균적으로도 부족한 시간대입니다."
                    if daytime_best_hours:
                        message += f" 추천 시간: {get_recommend_hours_str(best_hours)}"
                    elif best_hours:
                        # 추천 시간이 새벽/밤뿐이면 다른 대여소 권장
                        message += " 주간 시간대에 여유로운 시간이 없어 다른 대여소를 고려하세요."
                    else:
                        message += " 다른 대여소를 고려하세요."
                else:
                    # 통계 데이터 없음
                    if realtime_status == "low":
                        message = f"현재 자전거가 부족합니다. ({realtime_percentage}%) 빨리 가거나 다른 대여소를 고려하세요."
                    else:
                        message = f"현재 자전거가 거의 없습니다. ({realtime_percentage}%) 다른 대여소를 이용하세요."
                    if daytime_best_hours:
                        message += f" 추천 시간: {get_recommend_hours_str(best_hours)}"
            
            return message
        
        # 실시간 데이터가 없으면 통계 기반으로만 판단
        if not hourly_data:
            return "데이터가 충분하지 않습니다."
        
        if not stats_status:
            return "현재 시간대 데이터가 없습니다."
        
        # 주간 시간대에 여유로운 시간이 없는 경우 특별 메시지
        if no_daytime_availability and stats_status in ["low", "critical"]:
            return "해당 대여소는 주간 시간대(7~22시)에 거의 대여가 불가능해 보입니다. 실시간 현황을 확인하여 이용하거나 근처 다른 대여소 이용을 추천드립니다."
        
        if stats_status == "high":
            message = "평균적으로 이 시간대는 여유롭습니다! 대여하기 좋은 시간입니다."
        elif stats_status == "medium":
            message = "평균적으로 이 시간대는 보통입니다. 대여 가능하지만 서두르는 것이 좋습니다."
            if daytime_best_hours:
                message += f" 더 여유로운 시간: {get_recommend_hours_str(best_hours)}"
        elif stats_status == "low":
            message = "평균적으로 이 시간대는 부족합니다. 빨리 가거나 다른 대여소를 고려하세요."
            if daytime_best_hours:
                message += f" 추천 시간: {get_recommend_hours_str(best_hours)}"
            elif best_hours:
                message += " 주간 시간대에 여유로운 시간이 없어 다른 대여소를 고려하세요."
        else:
            message = "평균적으로 이 시간대는 자전거가 거의 없습니다. 다른 대여소를 이용하세요."
            if daytime_best_hours:
                message += f" 여유로운 시간: {get_recommend_hours_str(best_hours)}"
        
        return message


# 싱글톤 인스턴스
heatmap_service = HeatmapService()
