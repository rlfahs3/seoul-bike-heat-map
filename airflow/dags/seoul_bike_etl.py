"""
Seoul Bike Heat Map - ETL DAG
서울시 따릉이 실시간 정보를 수집하여 MySQL에 저장하고 통계를 계산하는 DAG

실행 주기: 5분마다
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from typing import List, Dict
import logging
import os

# 로컬 모듈 import
from utils.seoul_api import fetch_bike_data
from utils.data_processor import clean_station_data

logger = logging.getLogger(__name__)

# ============================================
# DAG 기본 설정
# ============================================

default_args = {
    'owner': 'seoul-bike',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
}

dag = DAG(
    dag_id='seoul_bike_etl',
    default_args=default_args,
    description='서울 따릉이 데이터 수집 및 처리',
    schedule_interval='1 * * * *',  # 60분마다
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['seoul', 'bike', 'etl', 'heatmap'],
)

# ============================================
# Task 함수 정의
# ============================================

def task_fetch_bike_data(**context):
    """
    Task 1: 서울시 API에서 따릉이 데이터 가져오기
    """
    api_key = os.getenv("SEOUL_API_KEY")
    
    if not api_key:
        raise ValueError("SEOUL_API_KEY environment variable not set")
    
    logger.info("Fetching bike data from Seoul API...")
    raw_data = fetch_bike_data(api_key)
    
    if not raw_data:
        raise ValueError("No data fetched from API")
    
    logger.info(f"Fetched {len(raw_data)} stations")
    
    # XCom에 데이터 저장 (다음 Task로 전달)
    context['task_instance'].xcom_push(key='raw_bike_data', value=raw_data)
    
    return len(raw_data)


def task_process_bike_data(**context):
    """
    Task 2: 데이터 전처리 및 검증
    """
    # 이전 Task에서 데이터 가져오기
    task_instance = context['task_instance']
    raw_data = task_instance.xcom_pull(task_ids='fetch_bike_data', key='raw_bike_data')
    
    if not raw_data:
        raise ValueError("No raw data received from previous task")
    
    logger.info(f"Processing {len(raw_data)} stations...")
    stations, status_history = clean_station_data(raw_data)
    
    logger.info(f"Processed: {len(stations)} stations, {len(status_history)} status records")
    
    # 다음 Task로 전달
    task_instance.xcom_push(key='stations', value=stations)
    task_instance.xcom_push(key='status_history', value=status_history)
    
    return {
        'station_count': len(stations),
        'history_count': len(status_history)
    }


def task_save_to_mysql(**context):
    """
    Task 3: MySQL에 데이터 저장
    - bike_stations: UPSERT (INSERT ... ON DUPLICATE KEY UPDATE)
    - bike_status_history: INSERT
    """
    task_instance = context['task_instance']
    stations = task_instance.xcom_pull(task_ids='process_bike_data', key='stations')
    status_history = task_instance.xcom_pull(task_ids='process_bike_data', key='status_history')
    
    if not stations or not status_history:
        raise ValueError("No processed data received")
    
    # MySQL Hook 생성
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    # ===== 1. bike_stations 테이블 UPSERT =====
    logger.info(f"Upserting {len(stations)} stations...")
    
    station_sql = """
        INSERT INTO bike_stations (station_id, station_name, latitude, longitude, rack_total_count)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            station_name = VALUES(station_name),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            rack_total_count = VALUES(rack_total_count),
            updated_at = CURRENT_TIMESTAMP
    """
    
    # executemany로 배치 실행
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    
    station_rows = [(
        s['station_id'],
        s['station_name'],
        s['latitude'],
        s['longitude'],
        s['rack_total_count']
    ) for s in stations]
    
    cursor.executemany(station_sql, station_rows)
    conn.commit()
    cursor.close()
    
    logger.info(f"Stations upserted: {len(stations)}")
    
    # ===== 2. bike_status_history 테이블 INSERT =====
    logger.info(f"Inserting {len(status_history)} status records...")
    
    history_sql = """
        INSERT INTO bike_status_history 
        (station_id, parking_bike_count, rack_total_count, recorded_at)
        VALUES (%s, %s, %s, %s)
    """
    
    history_rows = [(
        h['station_id'],
        h['parking_bike_count'],
        h['rack_total_count'],
        h['recorded_at']
    ) for h in status_history]
    
    cursor = conn.cursor()
    cursor.executemany(history_sql, history_rows)
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Status history inserted: {len(status_history)}")
    
    return {
        'stations_saved': len(stations),
        'history_saved': len(status_history)
    }


def task_calculate_stats(**context):
    """
    Task 4: 히트맵용 통계 계산
    - bike_availability_stats 테이블 업데이트
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    logger.info("Calculating availability statistics...")
    
    # 스토어드 프로시저 호출 (전체 통계 재계산)
    # 주의: 데이터가 많으면 시간이 오래 걸릴 수 있음
    # 실제 운영에서는 증분 계산 로직 필요
    
    # 최근 업데이트된 대여소들만 통계 재계산
    sql = """
        INSERT INTO bike_availability_stats (
            station_id,
            hour_of_day,
            day_of_week,
            avg_availability,
            avg_parking_count,
            sample_count
        )
        SELECT 
            station_id,
            HOUR(recorded_at) as hour_of_day,
            WEEKDAY(recorded_at) as day_of_week,
            LEAST(GREATEST(AVG(availability_rate), 0), 100) as avg_availability,
            AVG(parking_bike_count) as avg_parking_count,
            COUNT(*) as sample_count
        FROM bike_status_history
        WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        GROUP BY station_id, HOUR(recorded_at), WEEKDAY(recorded_at)
        ON DUPLICATE KEY UPDATE
            avg_availability = VALUES(avg_availability),
            avg_parking_count = VALUES(avg_parking_count),
            sample_count = VALUES(sample_count),
            last_updated = CURRENT_TIMESTAMP
    """
    
    mysql_hook.run(sql)
    
    # 업데이트된 통계 개수 확인
    result = mysql_hook.get_first("SELECT COUNT(*) as cnt FROM bike_availability_stats")
    stats_count = result[0] if result else 0
    
    logger.info(f"Statistics updated: {stats_count} records")
    
    return {'stats_count': stats_count}


# ============================================
# Task 정의
# ============================================

fetch_task = PythonOperator(
    task_id='fetch_bike_data',
    python_callable=task_fetch_bike_data,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_bike_data',
    python_callable=task_process_bike_data,
    provide_context=True,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_to_mysql',
    python_callable=task_save_to_mysql,
    provide_context=True,
    dag=dag,
)

stats_task = PythonOperator(
    task_id='calculate_stats',
    python_callable=task_calculate_stats,
    provide_context=True,
    dag=dag,
)

# ============================================
# Task 의존성 설정
# ============================================

fetch_task >> process_task >> save_task >> stats_task

# DAG 구조:
# [API 호출] → [데이터 전처리] → [MySQL 저장] → [통계 계산]
