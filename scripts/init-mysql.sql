-- Seoul Bike Heat Map - MySQL 초기 스키마
-- 실행: Docker Compose가 자동으로 실행하거나 수동 실행

-- 데이터베이스 생성 (이미 docker-compose에서 생성됨)
CREATE DATABASE IF NOT EXISTS seoul_bike CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE seoul_bike;

-- ============================================
-- 테이블 1: bike_stations (대여소 정보)
-- ============================================
CREATE TABLE IF NOT EXISTS bike_stations (
    station_id VARCHAR(50) PRIMARY KEY COMMENT '대여소 고유 ID',
    station_name VARCHAR(200) NOT NULL COMMENT '대여소 이름',
    latitude DECIMAL(10, 7) NOT NULL COMMENT '위도',
    longitude DECIMAL(10, 7) NOT NULL COMMENT '경도',
    rack_total_count INT NOT NULL DEFAULT 0 COMMENT '거치대 총 개수',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
    
    INDEX idx_location (latitude, longitude) COMMENT '위치 기반 검색용 인덱스',
    INDEX idx_station_name (station_name) COMMENT '대여소명 검색용 인덱스'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='따릉이 대여소 기본 정보';

-- ============================================
-- 테이블 2: bike_status_history (시간대별 대여 현황 이력)
-- ============================================
CREATE TABLE IF NOT EXISTS bike_status_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '히스토리 고유 ID',
    station_id VARCHAR(50) NOT NULL COMMENT '대여소 ID',
    parking_bike_count INT NOT NULL DEFAULT 0 COMMENT '현재 주차된 자전거 수',
    rack_total_count INT NOT NULL DEFAULT 0 COMMENT '거치대 총 개수',
    availability_rate DECIMAL(7, 2) GENERATED ALWAYS AS (
        CASE 
            WHEN rack_total_count > 0 
            THEN (parking_bike_count / rack_total_count * 100)
            ELSE 0 
        END
    ) STORED COMMENT '대여 가능율 (자동 계산, 100% 초과 가능)',
    recorded_at TIMESTAMP NOT NULL COMMENT '데이터 수집 시각',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    
    INDEX idx_station_time (station_id, recorded_at) COMMENT '대여소별 시계열 조회용',
    INDEX idx_recorded_at (recorded_at) COMMENT '시간대별 조회용',
    
    FOREIGN KEY (station_id) REFERENCES bike_stations(station_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='따릉이 대여 현황 시계열 데이터';

-- ============================================
-- 테이블 3: bike_availability_stats (시간대별 통계 - 히트맵용)
-- ============================================
CREATE TABLE IF NOT EXISTS bike_availability_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '통계 고유 ID',
    station_id VARCHAR(50) NOT NULL COMMENT '대여소 ID',
    hour_of_day INT NOT NULL COMMENT '시간대 (0-23)',
    day_of_week INT NOT NULL COMMENT '요일 (0=월요일, 6=일요일)',
    avg_availability DECIMAL(7, 2) NOT NULL DEFAULT 0.00 COMMENT '평균 대여 가능율 (%, 100% 초과 가능)',
    avg_parking_count DECIMAL(5, 2) NOT NULL DEFAULT 0.00 COMMENT '평균 주차 자전거 수',
    sample_count INT NOT NULL DEFAULT 0 COMMENT '데이터 수집 횟수',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '마지막 업데이트',
    
    UNIQUE KEY uk_station_hour_dow (station_id, hour_of_day, day_of_week) COMMENT '대여소+시간+요일 유니크',
    INDEX idx_stats_lookup (station_id, day_of_week, hour_of_day) COMMENT '히트맵 조회용 복합 인덱스',
    INDEX idx_day_hour (day_of_week, hour_of_day) COMMENT '시간대별 전체 통계 조회용',
    
    FOREIGN KEY (station_id) REFERENCES bike_stations(station_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    
    CHECK (hour_of_day >= 0 AND hour_of_day <= 23),
    CHECK (day_of_week >= 0 AND day_of_week <= 6),
    CHECK (avg_availability >= 0)  -- 100% 초과 허용
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='시간대별 대여 가능율 통계 (히트맵 데이터)';

-- ============================================
-- 샘플 데이터 (개발/테스트용)
-- ============================================

-- 샘플 대여소 데이터 (서울 주요 위치)
INSERT INTO bike_stations (station_id, station_name, latitude, longitude, rack_total_count) VALUES
('ST-001', '101. 여의도역 1번출구 앞', 37.52164, 126.92422, 20),
('ST-002', '102. 강남역 2번출구 앞', 37.49794, 127.02762, 30),
('ST-003', '103. 홍대입구역 9번출구', 37.55698, 126.92308, 25),
('ST-004', '104. 서울역 광장', 37.55466, 126.97059, 40),
('ST-005', '105. 광화문광장', 37.57129, 126.97657, 35)
ON DUPLICATE KEY UPDATE 
    station_name = VALUES(station_name),
    latitude = VALUES(latitude),
    longitude = VALUES(longitude),
    rack_total_count = VALUES(rack_total_count);

-- 샘플 히스토리 데이터 (최근 1시간)
INSERT INTO bike_status_history (station_id, parking_bike_count, rack_total_count, recorded_at) VALUES
('ST-001', 15, 20, DATE_SUB(NOW(), INTERVAL 5 MINUTE)),
('ST-001', 14, 20, DATE_SUB(NOW(), INTERVAL 10 MINUTE)),
('ST-002', 25, 30, DATE_SUB(NOW(), INTERVAL 5 MINUTE)),
('ST-002', 22, 30, DATE_SUB(NOW(), INTERVAL 10 MINUTE)),
('ST-003', 18, 25, DATE_SUB(NOW(), INTERVAL 5 MINUTE)),
('ST-003', 20, 25, DATE_SUB(NOW(), INTERVAL 10 MINUTE));

-- ============================================
-- 뷰 생성 (편의 기능)
-- ============================================

-- 뷰 1: 최신 대여소 현황
CREATE OR REPLACE VIEW v_latest_bike_status AS
SELECT 
    s.station_id,
    s.station_name,
    s.latitude,
    s.longitude,
    s.rack_total_count,
    h.parking_bike_count,
    h.availability_rate,
    h.recorded_at
FROM bike_stations s
LEFT JOIN (
    SELECT 
        station_id,
        parking_bike_count,
        availability_rate,
        recorded_at,
        ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY recorded_at DESC) as rn
    FROM bike_status_history
) h ON s.station_id = h.station_id AND h.rn = 1
ORDER BY s.station_name;

-- 뷰 2: 시간대별 평균 대여율 (전체 대여소)
CREATE OR REPLACE VIEW v_hourly_avg_availability AS
SELECT 
    hour_of_day,
    day_of_week,
    AVG(avg_availability) as overall_avg_availability,
    COUNT(DISTINCT station_id) as station_count,
    SUM(sample_count) as total_samples
FROM bike_availability_stats
GROUP BY hour_of_day, day_of_week
ORDER BY day_of_week, hour_of_day;

-- ============================================
-- 스토어드 프로시저 (통계 계산용)
-- ============================================

DELIMITER //

-- 프로시저: 통계 재계산 (특정 대여소)
CREATE PROCEDURE IF NOT EXISTS sp_recalculate_stats(
    IN p_station_id VARCHAR(50)
)
BEGIN
    -- 각 시간대/요일별로 통계 계산 후 UPSERT
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
        AVG(availability_rate) as avg_availability,
        AVG(parking_bike_count) as avg_parking_count,
        COUNT(*) as sample_count
    FROM bike_status_history
    WHERE station_id = p_station_id
    GROUP BY station_id, HOUR(recorded_at), WEEKDAY(recorded_at)
    ON DUPLICATE KEY UPDATE
        avg_availability = VALUES(avg_availability),
        avg_parking_count = VALUES(avg_parking_count),
        sample_count = VALUES(sample_count),
        last_updated = CURRENT_TIMESTAMP;
END //

-- 프로시저: 전체 통계 재계산
CREATE PROCEDURE IF NOT EXISTS sp_recalculate_all_stats()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_station_id VARCHAR(50);
    
    DECLARE station_cursor CURSOR FOR 
        SELECT DISTINCT station_id FROM bike_stations;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN station_cursor;
    
    read_loop: LOOP
        FETCH station_cursor INTO v_station_id;
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        CALL sp_recalculate_stats(v_station_id);
    END LOOP;
    
    CLOSE station_cursor;
END //

-- 프로시저: 오래된 히스토리 삭제 (30일 이상)
CREATE PROCEDURE IF NOT EXISTS sp_cleanup_old_history(
    IN p_days_to_keep INT
)
BEGIN
    DELETE FROM bike_status_history
    WHERE recorded_at < DATE_SUB(NOW(), INTERVAL p_days_to_keep DAY);
    
    SELECT ROW_COUNT() as deleted_rows;
END //

DELIMITER ;

-- ============================================
-- 이벤트 스케줄러 (자동 정리)
-- ============================================

-- 이벤트 스케줄러 활성화
SET GLOBAL event_scheduler = ON;

-- 매일 새벽 3시에 30일 이상 된 데이터 삭제
CREATE EVENT IF NOT EXISTS evt_cleanup_old_data
ON SCHEDULE EVERY 1 DAY
STARTS TIMESTAMP(CURRENT_DATE + INTERVAL 1 DAY + INTERVAL 3 HOUR)
DO
    CALL sp_cleanup_old_history(30);

-- ============================================
-- 권한 설정
-- ============================================

-- hch16 사용자에게 필요한 권한 부여
GRANT SELECT, INSERT, UPDATE, DELETE ON seoul_bike.* TO 'hch16'@'%';
GRANT EXECUTE ON PROCEDURE seoul_bike.sp_recalculate_stats TO 'hch16'@'%';
GRANT EXECUTE ON PROCEDURE seoul_bike.sp_recalculate_all_stats TO 'hch16'@'%';
GRANT EXECUTE ON PROCEDURE seoul_bike.sp_cleanup_old_history TO 'hch16'@'%';
FLUSH PRIVILEGES;

-- ============================================
-- 초기화 완료 확인
-- ============================================

SELECT 'Database initialization completed!' as message;
SELECT COUNT(*) as station_count FROM bike_stations;
SELECT COUNT(*) as history_count FROM bike_status_history;
