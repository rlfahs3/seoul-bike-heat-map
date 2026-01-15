"""
API Poller
주기적으로 따릉이 API를 호출하여 데이터 수집
"""

import asyncio
import signal
import sys
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import settings
from pipeline.kafka_producer import bike_producer

# 종료 플래그
shutdown_flag = False


async def poll_bike_data():
    """따릉이 데이터 수집 및 전송"""
    print(f"\n⏰ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 시작...")
    
    try:
        count = await bike_producer.fetch_and_send()
        print(f"✅ 수집 완료: {count}개 스테이션 데이터")
    except Exception as e:
        print(f"❌ 수집 오류: {e}")


def signal_handler(signum, frame):
    """종료 시그널 핸들러"""
    global shutdown_flag
    print("\n🛑 종료 신호 수신. 안전하게 종료합니다...")
    shutdown_flag = True


async def main():
    """메인 함수"""
    global shutdown_flag
    
    print("=" * 60)
    print("🚲 따릉이 데이터 수집기 (API Poller)")
    print("=" * 60)
    print(f"📍 수집 주기: {settings.polling_interval_seconds}초")
    print(f"📡 Kafka Topic: {settings.kafka_topic_raw}")
    print(f"🔗 Kafka Server: {settings.kafka_bootstrap_servers}")
    print("=" * 60)
    
    # Kafka 연결
    if not bike_producer.connect():
        print("❌ Kafka 연결 실패. 프로그램을 종료합니다.")
        print("💡 Docker로 Kafka를 실행하세요: docker-compose up -d")
        return
    
    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 스케줄러 설정
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        poll_bike_data,
        trigger=IntervalTrigger(seconds=settings.polling_interval_seconds),
        id="bike_poller",
        name="따릉이 데이터 수집",
        replace_existing=True
    )
    
    # 스케줄러 시작
    scheduler.start()
    print("🚀 스케줄러 시작됨")
    
    # 최초 1회 즉시 실행
    await poll_bike_data()
    
    # 종료 신호 대기
    try:
        while not shutdown_flag:
            await asyncio.sleep(1)
    finally:
        print("\n🔌 정리 중...")
        scheduler.shutdown(wait=False)
        bike_producer.disconnect()
        print("✅ 종료 완료")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 프로그램 종료")
        sys.exit(0)
