"""
Seoul Bike ETL DAG
따릉이 데이터 수집 → Kafka → Spark 전처리 → MySQL 저장
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
import os
import json
import httpx

# 환경 변수
SEOUL_API_KEY = os.getenv('SEOUL_API_KEY')
MYSQL_CONN_ID = 'mysql_default'
KAFKA_CONN_ID = 'kafka_default'

# DAG 기본 설정
default_args = {
    'owner': 'bikeadmin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'seoul_bike_etl',
    default_args=default_args,
    description='서울 따릉이 데이터 ETL 파이프라인',
    schedule_interval='1 * * * *',  # 60분마다 실행
    catchup=False,
    tags=['bike', 'etl', 'seoul'],
)


def fetch_bike_data(**context):
    """
    서울 API에서 따릉이 데이터 가져오기
    """
    if not SEOUL_API_KEY:
        raise ValueError("SEOUL_API_KEY 환경변수가 설정되지 않았습니다.")
    
    base_url = "http://openapi.seoul.go.kr:8088"
    url = f"{base_url}/{SEOUL_API_KEY}/json/bikeList/1/1000/"
    
    response = httpx.get(url, timeout=30)
    data = response.json()
    
    if not data.get("rentBikeStatus"):
        raise ValueError("API 응답 오류")
    
    stations = data["rentBikeStatus"].get("row", [])
    
    # XCom에 저장
    context['task_instance'].xcom_push(key='bike_data', value=stations)
    
    print(f"✅ {len(stations)}개 스테이션 데이터 수집 완료")
    return len(stations)


def transform_data(**context):
    """
    데이터 전처리 (간단한 변환)
    실제로는 Spark에서 처리하지만, 예시로 Python에서 처리
    """
    stations = context['task_instance'].xcom_pull(
        task_ids='fetch_data',
        key='bike_data'
    )
    
    transformed = []
    current_time = datetime.now().isoformat()
    
    for station in stations:
        transformed.append({
            'station_id': station.get('stationId'),
            'station_name': station.get('stationName'),
            'bikes_available': int(station.get('parkingBikeTotCnt', 0)),
            'capacity': int(station.get('rackTotCnt', 0)),
            'lat': float(station.get('stationLatitude', 0)) if station.get('stationLatitude') else None,
            'lng': float(station.get('stationLongitude', 0)) if station.get('stationLongitude') else None,
            'timestamp': current_time
        })
    
    # XCom에 저장
    context['task_instance'].xcom_push(key='transformed_data', value=transformed)
    
    print(f"✅ {len(transformed)}개 데이터 전처리 완료")
    return len(transformed)


def send_to_kafka(**context):
    """
    Kafka로 데이터 전송
    """
    from kafka import KafkaProducer
    
    transformed_data = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data'
    )
    
    producer = KafkaProducer(
        bootstrap_servers='kafka:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    topic = 'raw-bike-data'
    sent_count = 0
    
    for data in transformed_data:
        producer.send(topic, value=data)
        sent_count += 1
    
    producer.flush()
    producer.close()
    
    print(f"✅ Kafka로 {sent_count}개 메시지 전송 완료")
    return sent_count


def load_to_mysql(**context):
    """
    MySQL에 데이터 저장
    """
    import mysql.connector
    
    transformed_data = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_data'
    )
    
    conn = mysql.connector.connect(
        host='mysql',
        user='hch16',
        password='cksgh970216!',
        database='seoul_bike'
    )
    cursor = conn.cursor()
    
    # INSERT 쿼리 (UPSERT)
    query = """
    INSERT INTO bike_realtime 
    (station_id, station_name, bikes_available, capacity, lat, lng, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    bikes_available = VALUES(bikes_available),
    timestamp = VALUES(timestamp)
    """
    
    inserted = 0
    for data in transformed_data:
        cursor.execute(query, (
            data['station_id'],
            data['station_name'],
            data['bikes_available'],
            data['capacity'],
            data['lat'],
            data['lng'],
            data['timestamp']
        ))
        inserted += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ MySQL에 {inserted}개 레코드 저장 완료")
    return inserted


# Task 정의
task_fetch = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_bike_data,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

task_kafka = PythonOperator(
    task_id='send_to_kafka',
    python_callable=send_to_kafka,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_to_mysql',
    python_callable=load_to_mysql,
    dag=dag,
)

# Task 의존성 설정
task_fetch >> task_transform >> [task_kafka, task_load]
