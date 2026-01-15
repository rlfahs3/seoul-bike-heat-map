"""
Kafka Producer
따릉이 실시간 데이터를 Kafka로 전송
"""

import json
import asyncio
from datetime import datetime
from typing import List, Optional

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️ kafka-python이 설치되지 않았습니다. pip install kafka-python")

from app.config import settings
from app.services.seoul_bike_api import seoul_bike_api


class BikeDataProducer:
    """따릉이 데이터 Kafka Producer"""
    
    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self.topic = settings.kafka_topic_raw
        
    def connect(self) -> bool:
        """Kafka 연결"""
        if not KAFKA_AVAILABLE:
            print("❌ Kafka 라이브러리가 설치되지 않았습니다.")
            return False
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"✅ Kafka 연결 성공: {settings.kafka_bootstrap_servers}")
            return True
        except Exception as e:
            print(f"❌ Kafka 연결 실패: {e}")
            return False
    
    def disconnect(self):
        """Kafka 연결 해제"""
        if self.producer:
            self.producer.close()
            print("🔌 Kafka 연결 해제")
    
    async def fetch_and_send(self) -> int:
        """
        API에서 데이터를 가져와 Kafka로 전송
        
        Returns:
            전송된 메시지 수
        """
        if not self.producer:
            print("⚠️ Kafka Producer가 연결되지 않았습니다.")
            return 0
        
        try:
            # API에서 전체 스테이션 데이터 가져오기
            stations = await seoul_bike_api.get_all_stations()
            
            if not stations:
                print("⚠️ 가져올 데이터가 없습니다.")
                return 0
            
            timestamp = datetime.now().isoformat()
            sent_count = 0
            
            for station in stations:
                message = {
                    "station_id": station.get("stationId"),
                    "station_name": station.get("stationName"),
                    "bikes_available": int(station.get("parkingBikeTotCnt", 0)),
                    "capacity": int(station.get("rackTotCnt", 0)),
                    "lat": float(station.get("stationLatitude", 0)) if station.get("stationLatitude") else None,
                    "lng": float(station.get("stationLongitude", 0)) if station.get("stationLongitude") else None,
                    "timestamp": timestamp
                }
                
                # Kafka로 전송
                self.producer.send(
                    self.topic,
                    key=message["station_id"].encode('utf-8') if message["station_id"] else None,
                    value=message
                )
                sent_count += 1
            
            # 전송 완료 대기
            self.producer.flush()
            
            print(f"📤 {sent_count}개 메시지 전송 완료 (topic: {self.topic})")
            return sent_count
            
        except Exception as e:
            print(f"❌ 데이터 전송 오류: {e}")
            return 0


# 싱글톤 인스턴스
bike_producer = BikeDataProducer()


async def run_producer_once():
    """Producer 1회 실행 (테스트용)"""
    producer = BikeDataProducer()
    
    if producer.connect():
        count = await producer.fetch_and_send()
        producer.disconnect()
        return count
    
    return 0


if __name__ == "__main__":
    # 단독 실행 테스트
    print("🚲 따릉이 데이터 Kafka Producer 테스트")
    print("=" * 50)
    
    count = asyncio.run(run_producer_once())
    print(f"✅ 완료: {count}개 메시지 전송")
