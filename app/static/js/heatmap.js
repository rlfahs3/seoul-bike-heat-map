// Seoul Bike Heat Map - JavaScript

// 전역 변수
let currentStationId = null;
let currentDayOfWeek = null;  // null = 전체 평균, 0~6 = 요일별
let weeklyHeatmapData = null; // 주간 데이터 캐시
let originalRecommendation = ''; // 원본 추천 메시지 저장

// 지도 관련 전역 변수
let map = null;
let markers = [];
let clusterer = null;
let currentLocationMarker = null;
let clickedLocationMarker = null;
let allStations = [];

// 서울 중심 좌표
const SEOUL_CENTER = { lat: 37.5665, lng: 126.9780 };
const DEFAULT_ZOOM = 13;

// 요일 이름
const DAY_NAMES = ['월요일', '화요일', '수요일', '목요일', '금요일', '토요일', '일요일'];

// 페이지 로드 시 초기화
document.addEventListener('DOMContentLoaded', function() {
    initializeEventListeners();
    initializeDaySelector();
});

// 이벤트 리스너 초기화
function initializeEventListeners() {
    // 주소 검색 버튼 클릭
    const addressSearchBtn = document.getElementById('addressSearchBtn');
    if (addressSearchBtn) {
        addressSearchBtn.addEventListener('click', searchAddress);
    }
    
    // 주소 검색 입력 필드 엔터 키
    const addressSearch = document.getElementById('addressSearch');
    if (addressSearch) {
        addressSearch.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                searchAddress();
            }
        });
    }
    
    // 내 위치 버튼
    const myLocationBtn = document.getElementById('myLocationBtn');
    if (myLocationBtn) {
        myLocationBtn.addEventListener('click', moveToMyLocation);
    }
    
    // 지도 초기화 버튼
    const resetMapBtn = document.getElementById('resetMapBtn');
    if (resetMapBtn) {
        resetMapBtn.addEventListener('click', resetMap);
    }
}

// 지도 초기화
function initializeMap() {
    const mapContainer = document.getElementById('map');
    if (!mapContainer) return;
    
    console.log('지도 초기화 시작...');

    // 서울 전체가 보이는 줌 레벨
    const options = {
        center: new kakao.maps.LatLng(SEOUL_CENTER.lat, SEOUL_CENTER.lng),
        level: 7
    };
    
    map = new kakao.maps.Map(mapContainer, options);
    
    // 지도 컨트롤 추가
    const zoomControl = new kakao.maps.ZoomControl();
    map.addControl(zoomControl, kakao.maps.ControlPosition.RIGHT);
    
    // 클러스터러 생성
    clusterer = new kakao.maps.MarkerClusterer({
        map: map,
        averageCenter: true,
        minLevel: 6,
        disableClickZoom: false,
        styles: [{
            width: '50px',
            height: '50px',
            background: 'rgba(40, 167, 69, 0.9)',
            borderRadius: '25px',
            color: '#fff',
            textAlign: 'center',
            fontWeight: 'bold',
            lineHeight: '50px',
            fontSize: '14px'
        }]
    });
    
    // 지도 클릭 이벤트
    kakao.maps.event.addListener(map, 'click', function(mouseEvent) {
        const latlng = mouseEvent.latLng;
        handleMapClick(latlng.getLat(), latlng.getLng());
    });
    
    // 모든 스테이션 마커 로드
    loadAllStationMarkers();
}

// 모든 스테이션 마커 로드
async function loadAllStationMarkers() {
    try {
        const response = await fetch('/api/stations/map/all');
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.detail || '스테이션 로드 실패');
        }
        
        allStations = data.stations;
        createStationMarkers(allStations);
        
        console.log(`✅ ${allStations.length}개 스테이션 마커 로드 완료`);
        
    } catch (error) {
        console.error('스테이션 마커 로드 실패:', error);
    }
}

// 스테이션 마커 생성
function createStationMarkers(stations) {
    markers = [];
    
    stations.forEach(station => {
        if (!station.lat || !station.lng) return;
        
        const position = new kakao.maps.LatLng(station.lat, station.lng);
        
        // 자전거 상태에 따른 마커 색상
        const ratio = station.capacity > 0 ? station.bikes / station.capacity : 0;
        const markerColor = getMarkerColor(ratio);
        
        // 커스텀 마커 이미지
        const markerImage = createMarkerImage(markerColor);
        
        const marker = new kakao.maps.Marker({
            position: position,
            image: markerImage,
            title: station.name
        });
        
        // 마커 클릭 이벤트
        kakao.maps.event.addListener(marker, 'click', function() {
            selectStation(station.id, station.name);
            
            // 지도 중심 이동
            map.setCenter(position);
            map.setLevel(4);
        });
        
        // 인포윈도우
        const infoContent = `
            <div style="padding:8px;font-size:12px;min-width:150px;">
                <strong>${escapeHtml(station.name)}</strong><br>
                🚲 ${station.bikes}대 / ${station.capacity}대
            </div>
        `;
        const infowindow = new kakao.maps.InfoWindow({
            content: infoContent
        });
        
        kakao.maps.event.addListener(marker, 'mouseover', function() {
            infowindow.open(map, marker);
        });
        
        kakao.maps.event.addListener(marker, 'mouseout', function() {
            infowindow.close();
        });
        
        markers.push(marker);
    });
    
    // 클러스터러에 마커 추가
    clusterer.addMarkers(markers);
}

// 마커 색상 결정
function getMarkerColor(ratio) {
    if (ratio >= 0.6) return '#28a745';  // 여유 (녹색)
    if (ratio >= 0.3) return '#ffc107';  // 보통 (노랑)
    if (ratio >= 0.1) return '#fd7e14';  // 부족 (주황)
    return '#dc3545';  // 거의 불가능 (빨강)
}

// 커스텀 마커 이미지 생성
function createMarkerImage(color) {
    const svg = `
        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="32" viewBox="0 0 24 32">
            <path fill="${color}" stroke="#fff" stroke-width="1" d="M12 0C5.4 0 0 5.4 0 12c0 9 12 20 12 20s12-11 12-20c0-6.6-5.4-12-12-12z"/>
            <circle fill="#fff" cx="12" cy="12" r="6"/>
        </svg>
    `;
    const dataUrl = 'data:image/svg+xml;charset=UTF-8,' + encodeURIComponent(svg);
    
    return new kakao.maps.MarkerImage(
        dataUrl,
        new kakao.maps.Size(24, 32),
        { offset: new kakao.maps.Point(12, 32) }
    );
}

// 지도 클릭 처리
async function handleMapClick(lat, lng) {
    // 클릭 위치 마커 표시
    showClickedLocationMarker(lat, lng);
    
    // 근처 스테이션 검색
    await searchNearbyStations(lat, lng);
}

// 클릭 위치 마커 표시
function showClickedLocationMarker(lat, lng) {
    // 기존 마커 제거
    if (clickedLocationMarker) {
        clickedLocationMarker.setMap(null);
    }
    
    const position = new kakao.maps.LatLng(lat, lng);
    
    // 클릭 위치 마커 (파란색)
    const svg = `
        <svg xmlns="http://www.w3.org/2000/svg" width="30" height="40" viewBox="0 0 24 32">
            <path fill="#007bff" stroke="#fff" stroke-width="2" d="M12 0C5.4 0 0 5.4 0 12c0 9 12 20 12 20s12-11 12-20c0-6.6-5.4-12-12-12z"/>
            <circle fill="#fff" cx="12" cy="12" r="5"/>
        </svg>
    `;
    const dataUrl = 'data:image/svg+xml;charset=UTF-8,' + encodeURIComponent(svg);
    
    clickedLocationMarker = new kakao.maps.Marker({
        position: position,
        map: map,
        image: new kakao.maps.MarkerImage(
            dataUrl,
            new kakao.maps.Size(30, 40),
            { offset: new kakao.maps.Point(15, 40) }
        ),
        zIndex: 100
    });
}

// 근처 스테이션 검색
async function searchNearbyStations(lat, lng, radius = 1.0) {
    showLoading();
    
    try {
        const response = await fetch(`/api/stations/nearby?lat=${lat}&lng=${lng}&radius=${radius}&limit=20`);
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.detail || '근처 스테이션 검색 실패');
        }
        
        displayNearbyStations(data.stations);
        
    } catch (error) {
        console.error('근처 스테이션 검색 실패:', error);
        displayNearbyStations([]);
    } finally {
        hideLoading();
    }
}

// 근처 스테이션 목록 표시
function displayNearbyStations(stations) {
    const container = document.getElementById('nearbyStations');
    const countBadge = document.getElementById('nearbyCount');
    
    if (countBadge) {
        countBadge.textContent = stations.length;
    }
    
    if (stations.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted py-4">
                <i class="bi bi-emoji-frown fs-1"></i>
                <p class="mt-2">반경 1km 내에<br>스테이션이 없습니다</p>
            </div>
        `;
        return;
    }
    
    let html = '';
    stations.forEach(station => {
        const ratio = station.capacity > 0 ? station.current_bikes / station.capacity : 0;
        const statusColor = getMarkerColor(ratio);
        const statusEmoji = ratio >= 0.6 ? '🟢' : ratio >= 0.3 ? '🟡' : ratio >= 0.1 ? '🟠' : '🔴';
        
        html += `
            <div class="nearby-station-item" onclick="selectStationFromNearby('${station.station_id}', '${escapeHtml(station.station_name)}', ${station.lat}, ${station.lng})">
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <strong>${statusEmoji} ${escapeHtml(station.station_name)}</strong>
                        <div class="small text-muted">
                            🚲 ${station.current_bikes}대 / ${station.capacity}대
                        </div>
                    </div>
                    <div class="distance">
                        <span class="badge bg-light text-dark">${station.distance_m}m</span>
                    </div>
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
}

// 근처 스테이션 목록에서 선택
function selectStationFromNearby(stationId, stationName, lat, lng) {
    // 지도 이동
    if (map && lat && lng) {
        const position = new kakao.maps.LatLng(lat, lng);
        map.setCenter(position);
        map.setLevel(3);
    }
    
    // 스테이션 선택
    selectStation(stationId, stationName);
}

// 주소 검색
async function searchAddress() {
    const query = document.getElementById('addressSearch').value.trim();
    
    if (query.length < 2) {
        alert('검색어를 2글자 이상 입력해주세요.');
        return;
    }
    
    // Kakao map 장소 검색 서비스
    const ps = new kakao.maps.services.Places();
    
    showLoading();
    
    ps.keywordSearch(query, function(data, status, pagination) {
        hideLoading();
        
        if (status === kakao.maps.services.Status.OK) {
            // 첫 번째 결과로 이동
            const place = data[0];
            const lat = parseFloat(place.y);
            const lng = parseFloat(place.x);
            
            // 지도 이동
            const position = new kakao.maps.LatLng(lat, lng);
            map.setCenter(position);
            map.setLevel(4);
            
            // 클릭 위치 마커 표시
            showClickedLocationMarker(lat, lng);
            
            // 근처 스테이션 검색
            searchNearbyStations(lat, lng);
            
        } else if (status === kakao.maps.services.Status.ZERO_RESULT) {
            alert('검색 결과가 없습니다.');
        } else {
            alert('검색 중 오류가 발생했습니다.');
        }
    }, {
        location: new kakao.maps.LatLng(SEOUL_CENTER.lat, SEOUL_CENTER.lng),
        radius: 20000  // 서울 중심 20km 반경
    });
}

// 내 위치로 이동
function moveToMyLocation() {
    if (!navigator.geolocation) {
        alert('브라우저에서 위치 서비스를 지원하지 않습니다.');
        return;
    }
    
    showLoading();
    
    navigator.geolocation.getCurrentPosition(
        function(position) {
            const lat = position.coords.latitude;
            const lng = position.coords.longitude;
            
            // 지도 이동
            const pos = new kakao.maps.LatLng(lat, lng);
            map.setCenter(pos);
            map.setLevel(4);
            
            // 현재 위치 마커
            if (currentLocationMarker) {
                currentLocationMarker.setMap(null);
            }
            
            const svg = `
                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 20 20">
                    <circle fill="#4285f4" stroke="#fff" stroke-width="3" cx="10" cy="10" r="8"/>
                </svg>
            `;
            const dataUrl = 'data:image/svg+xml;charset=UTF-8,' + encodeURIComponent(svg);
            
            currentLocationMarker = new kakao.maps.Marker({
                position: pos,
                map: map,
                image: new kakao.maps.MarkerImage(
                    dataUrl,
                    new kakao.maps.Size(20, 20),
                    { offset: new kakao.maps.Point(10, 10) }
                ),
                zIndex: 100
            });
            
            // 근처 스테이션 검색
            searchNearbyStations(lat, lng);
            
            hideLoading();
        },
        function(error) {
            hideLoading();
            let message = '위치를 가져올 수 없습니다.';
            if (error.code === 1) {
                message = '위치 권한이 거부되었습니다. 브라우저 설정에서 위치 권한을 허용해주세요.';
            }
            alert(message);
        },
        {
            enableHighAccuracy: true,
            timeout: 10000,
            maximumAge: 0
        }
    );
}

// 지도 초기화
function resetMap() {
    if (!map) return;
    
    // 서울 중심으로 이동
    const position = new kakao.maps.LatLng(SEOUL_CENTER.lat, SEOUL_CENTER.lng);
    map.setCenter(position);
    map.setLevel(7);
    
    // 클릭 마커 제거
    if (clickedLocationMarker) {
        clickedLocationMarker.setMap(null);
        clickedLocationMarker = null;
    }
    
    // 근처 스테이션 목록 초기화
    const container = document.getElementById('nearbyStations');
    const countBadge = document.getElementById('nearbyCount');
    
    if (container) {
        container.innerHTML = `
            <div class="text-center text-muted py-5">
                <i class="bi bi-hand-index-thumb fs-1"></i>
                <p class="mt-2">지도를 클릭하거나<br>주소를 검색하세요</p>
            </div>
        `;
    }
    
    if (countBadge) {
        countBadge.textContent = '0';
    }
    
    // 결과 섹션 숨기기
    document.getElementById('resultSection').style.display = 'none';
}

// 요일 선택 버튼 초기화
function initializeDaySelector() {
    const selector = document.getElementById('daySelector');
    if (!selector) return;
    
    // 오늘 요일 표시 (0=일요일이지만, 우리는 0=월요일로 사용)
    const today = new Date().getDay();
    const todayIndex = today === 0 ? 6 : today - 1; // JavaScript: 0=일요일 → 우리: 6=일요일
    
    const dayBtns = selector.querySelectorAll('.day-btn');
    dayBtns.forEach(btn => {
        const day = btn.dataset.day;
        
        // 오늘 표시
        if (day !== 'all' && parseInt(day) === todayIndex) {
            btn.classList.add('today');
        }
        
        // 클릭 이벤트
        btn.addEventListener('click', () => handleDaySelect(btn, day));
    });
}

// 로딩 표시
function showLoading() {
    document.getElementById('loadingOverlay').classList.add('show');
}

function hideLoading() {
    document.getElementById('loadingOverlay').classList.remove('show');
}

// 스테이션 검색
async function searchStations() {
    const query = document.getElementById('stationSearch').value.trim();
    
    if (query.length < 1) {
        alert('검색어를 입력해주세요.');
        return;
    }
    
    showLoading();
    
    try {
        const response = await fetch(`/api/stations/search?query=${encodeURIComponent(query)}`);
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.detail || '검색 중 오류가 발생했습니다.');
        }
        
        displaySearchResults(data.stations);
        
    } catch (error) {
        console.error('검색 오류:', error);
        alert(error.message || '검색 중 오류가 발생했습니다.');
    } finally {
        hideLoading();
    }
}

// 검색 결과 표시
function displaySearchResults(stations) {
    const resultsSection = document.getElementById('searchResults');
    const stationList = document.getElementById('stationList');
    
    if (stations.length === 0) {
        stationList.innerHTML = `
            <div class="col-12">
                <div class="alert alert-info">
                    <i class="bi bi-info-circle"></i> 검색 결과가 없습니다.
                </div>
            </div>
        `;
        resultsSection.style.display = 'block';
        return;
    }
    
    let html = '';
    stations.forEach(station => {
        html += `
            <div class="col-md-6 col-lg-4 mb-3">
                <div class="card station-card h-100" onclick="selectStation('${station.station_id}', '${escapeHtml(station.station_name)}')">
                    <div class="card-body">
                        <h5 class="card-title">
                            <i class="bi bi-geo-alt text-success"></i>
                            ${escapeHtml(station.station_name)}
                        </h5>
                        <p class="card-text text-muted small">
                            <span class="badge bg-secondary">ID: ${station.station_id}</span>
                            <span class="badge bg-info ms-1">거치대: ${station.capacity}대</span>
                        </p>
                    </div>
                </div>
            </div>
        `;
    });
    
    stationList.innerHTML = html;
    resultsSection.style.display = 'block';
    
    // 결과 섹션 숨기기
    document.getElementById('resultSection').style.display = 'none';
}

// 요일 선택 핸들러
async function handleDaySelect(btn, day) {
    // 버튼 활성화 상태 변경
    document.querySelectorAll('.day-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    
    // 요일 설정
    currentDayOfWeek = day === 'all' ? null : parseInt(day);
    
    // 데이터가 있으면 히트맵 업데이트
    if (currentStationId) {
        await loadHeatmapByDay(currentStationId, currentDayOfWeek);
    }
}

// 요일별 히트맵 로드
async function loadHeatmapByDay(stationId, dayOfWeek) {
    showLoading();
    
    try {
        let url = `/api/availability/${stationId}`;
        if (dayOfWeek !== null) {
            url += `?day_of_week=${dayOfWeek}`;
        }
        
        const response = await fetch(url);
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.detail || '데이터를 불러올 수 없습니다.');
        }
        
        // 히트맵 업데이트
        updateHeatmap(data.hourly_availability);
        updateHourlyTable(data.hourly_availability);
        
        // 추천 메시지 업데이트 (요일 정보 포함)
        const dayText = dayOfWeek !== null ? DAY_NAMES[dayOfWeek] : '전체';
        updateDayInfo(dayText);
        
    } catch (error) {
        console.error('히트맵 조회 오류:', error);
    } finally {
        hideLoading();
    }
}

// 요일 정보 업데이트 (추천 카드에 표시)
function updateDayInfo(dayText) {
    const recommendText = document.getElementById('recommendationText');
    if (recommendText) {
        if (dayText !== '전체') {
            // 원본 추천 메시지에 요일 추가
            recommendText.textContent = `[${dayText}] ${originalRecommendation}`;
        } else {
            // 전체 평균일 때는 원본 메시지만 표시
            recommendText.textContent = originalRecommendation;
        }
    }
}

// 스테이션 선택
async function selectStation(stationId, stationName) {
    currentStationId = stationId;
    currentDayOfWeek = null; // 초기값은 전체 평균
    
    // 요일 선택 버튼 초기화
    document.querySelectorAll('.day-btn').forEach(b => b.classList.remove('active'));
    const allBtn = document.querySelector('.day-btn[data-day="all"]');
    if (allBtn) allBtn.classList.add('active');
    
    showLoading();
    
    try {
        // 전체 데이터 조회
        const response = await fetch(`/api/availability/${stationId}/full`);
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.detail || '데이터를 불러올 수 없습니다.');
        }
        
        // UI 업데이트
        updateStationInfo(data);
        updateRealtimeInfo(data.realtime);
        updateRecommendation(data.recommendation);
        updateHeatmap(data.heatmap);
        updateHourlyTable(data.heatmap);
        
        // 결과 섹션 표시
        document.getElementById('searchResults').style.display = 'none';
        document.getElementById('resultSection').style.display = 'block';
        document.getElementById('resultSection').classList.add('fade-in');
        
        // 스크롤
        document.getElementById('resultSection').scrollIntoView({ behavior: 'smooth' });
        
    } catch (error) {
        console.error('데이터 조회 오류:', error);
        alert(error.message || '데이터를 불러올 수 없습니다.');
    } finally {
        hideLoading();
    }
}

// 스테이션 정보 업데이트
function updateStationInfo(data) {
    document.getElementById('stationName').textContent = data.station_info.station_name;
}

// 실시간 정보 업데이트
function updateRealtimeInfo(realtime) {
    document.getElementById('realtimeEmoji').textContent = realtime.status_emoji;
    document.getElementById('realtimeStatus').textContent = realtime.status_label;
    document.getElementById('realtimeBikes').textContent = 
        `잔여 자전거: ${realtime.bikes_available}대 / ${realtime.capacity}대`;
    
    const progressBar = document.getElementById('realtimeProgress');
    const percentage = Math.round(realtime.ratio * 100);
    progressBar.style.width = `${Math.min(percentage, 100)}%`;
    progressBar.textContent = `${percentage}%`;
    
    // 색상 업데이트
    progressBar.className = 'progress-bar';
    
    // 100% 초과 시 특별 표시
    if (percentage > 100) {
        progressBar.classList.add('bg-info');
        progressBar.textContent = `${percentage}% (초과)`;
    } else {
        switch (realtime.status) {
            case 'high':
                progressBar.classList.add('bg-success');
                break;
            case 'medium':
                progressBar.classList.add('bg-warning');
                break;
            case 'low':
                progressBar.classList.add('bg-orange');
                progressBar.style.backgroundColor = '#fd7e14';
                break;
            case 'critical':
                progressBar.classList.add('bg-danger');
                break;
        }
    }
}

// 추천 업데이트
function updateRecommendation(recommendation) {
    originalRecommendation = recommendation || '추천 정보가 없습니다.';  // 원본 저장
    document.getElementById('recommendationText').textContent = originalRecommendation;
}

// 히트맵 업데이트
function updateHeatmap(hourlyData) {
    const container = document.getElementById('heatmapContainer');
    let html = '';
    
    hourlyData.forEach(hour => {
        html += `
            <div class="heatmap-cell status-${hour.status}" 
                 title="${hour.hour}시: ${hour.status_label} (${Math.round(hour.avg_ratio * 100)}%)">
                <span class="hour-label">${hour.hour}시</span>
                <span class="emoji">${hour.status_emoji}</span>
            </div>
        `;
    });
    
    container.innerHTML = html;
}

// 시간대별 테이블 업데이트
function updateHourlyTable(hourlyData) {
    const tbody = document.getElementById('hourlyTable');
    let html = '';
    
    hourlyData.forEach(hour => {
        const percentage = Math.round(hour.avg_ratio * 100);
        const percentageDisplay = percentage > 100 
            ? `<span class="text-info fw-bold">${percentage}%</span>` 
            : `${percentage}%`;
        
        html += `
            <tr>
                <td><strong>${hour.hour}시</strong></td>
                <td>
                    <span class="badge status-${hour.status}">
                        ${hour.status_emoji} ${hour.status_label}
                    </span>
                </td>
                <td>${hour.avg_available.toFixed(1)}대</td>
                <td>${percentageDisplay}</td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html;
}

// HTML 이스케이프
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// 새로고침
async function refreshData() {
    if (currentStationId) {
        await selectStation(currentStationId, '');
    }
}
