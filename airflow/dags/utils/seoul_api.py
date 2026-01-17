"""
서울시 따릉이 Open API 호출 유틸리티
"""
import requests
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class SeoulBikeAPI:
    """서울시 따릉이 실시간 대여정보 API 클라이언트"""
    
    BASE_URL = "http://openapi.seoul.go.kr:8088"
    
    def __init__(self, api_key: str):
        """
        Args:
            api_key: 서울 열린데이터광장 API 키
        """
        self.api_key = api_key
        
    def fetch_bike_list(self, start_index: int = 1, end_index: int = 1000) -> Dict:
        """
        따릉이 대여소 정보 조회
        
        Args:
            start_index: 시작 인덱스 (기본 1)
            end_index: 종료 인덱스 (기본 1000, 최대 1000)
            
        Returns:
            API 응답 JSON
            
        Raises:
            requests.RequestException: API 호출 실패 시
        """
        url = f"{self.BASE_URL}/{self.api_key}/json/bikeList/{start_index}/{end_index}/"
        
        logger.info(f"Fetching bike data: {start_index}-{end_index}")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # API 에러 체크
            if "RESULT" in data and data["RESULT"]["CODE"] != "INFO-000":
                error_msg = data["RESULT"]["MESSAGE"]
                raise ValueError(f"Seoul API Error: {error_msg}")
            
            return data
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch bike data: {e}")
            raise
        except ValueError as e:
            logger.error(f"Invalid API response: {e}")
            raise
            
    def fetch_all_bikes(self, batch_size: int = 1000) -> List[Dict]:
        """
        모든 따릉이 대여소 정보 조회 (페이징 처리)

        Args:
            batch_size: 한 번에 가져올 개수 (최대 1000)
            
        Returns:
            전체 대여소 정보 리스트
        """
        import time
        
        all_bikes = []
        start_index = 1
        batch_num = 0
        
        logger.info("=" * 50)
        logger.info("Starting to fetch ALL bike stations with pagination")
        logger.info(f"Batch size: {batch_size}")
        logger.info("=" * 50)
        
        while True:
            batch_num += 1
            end_index = start_index + batch_size - 1
            
            try:
                logger.info(f"📥 Batch #{batch_num}: Fetching {start_index} ~ {end_index}")
                data = self.fetch_bike_list(start_index, end_index)
                
                # rentBikeStatus.row에 실제 데이터가 있음
                if "rentBikeStatus" not in data:
                    logger.warning("No rentBikeStatus in response")
                    break
                
                rent_bike_status = data["rentBikeStatus"]
                rows = rent_bike_status.get("row", [])
                fetched_count = len(rows)
                
                if not rows:
                    logger.info(f"📭 No more rows returned at batch #{batch_num}. Stopping.")
                    break
                    
                all_bikes.extend(rows)
                logger.info(f"✅ Batch #{batch_num}: Got {fetched_count} stations | Total so far: {len(all_bikes)}")
                
                if fetched_count < batch_size:
                    logger.info(f"🎉 Last batch detected (got {fetched_count} < {batch_size}). All stations fetched!")
                    break
                
                # 다음 배치로 이동
                start_index = end_index + 1
                
                # API 부하 방지를 위한 짧은 대기
                time.sleep(0.3)
                
            except Exception as e:
                logger.error(f"❌ Error fetching batch #{batch_num} ({start_index}-{end_index}): {e}")
                # 에러 발생해도 이미 가져온 데이터는 유지
                if all_bikes:
                    logger.warning(f"Returning partial data: {len(all_bikes)} stations")
                break
        
        logger.info("=" * 50)
        logger.info(f"📈 PAGINATION COMPLETE!")
        logger.info(f"   Total batches: {batch_num}")
        logger.info(f"   Total stations fetched: {len(all_bikes)}")
        logger.info("=" * 50)
        return all_bikes


def fetch_bike_data(api_key: str) -> List[Dict]:
    """
    Airflow Task에서 사용할 래퍼 함수
    
    Args:
        api_key: 서울 API 키
        
    Returns:
        대여소 정보 리스트
    """
    client = SeoulBikeAPI(api_key)
    return client.fetch_all_bikes()


if __name__ == "__main__":
    # 테스트용
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    api_key = os.getenv("SEOUL_API_KEY")
    
    if not api_key:
        print("Error: SEOUL_API_KEY not found in .env")
        exit(1)
    
    bikes = fetch_bike_data(api_key)
    print(f"Fetched {len(bikes)} stations")
    
    if bikes:
        print("\nSample station:")
        print(bikes[0])
