// Seoul Bike Heat Map - JavaScript

// 전역 변수
let currentStationId = null;
let currentDayOfWeek = null;  // null = 전체 평균, 0~6 = 요일별
let weeklyHeatmapData = null; // 주간 데이터 캐시
let originalRecommendation = ''; // 원본 추천 메시지 저장

// 요일 이름
const DAY_NAMES = ['월요일', '화요일', '수요일', '목요일', '금요일', '토요일', '일요일'];

// 페이지 로드 시 초기화
document.addEventListener('DOMContentLoaded', function() {
    initializeEventListeners();
    initializeDaySelector();
});

// 이벤트 리스너 초기화
function initializeEventListeners() {
    // 검색 버튼 클릭
    document.getElementById('searchBtn').addEventListener('click', searchStations);
    
    // 검색 입력 필드 엔터 키
    document.getElementById('stationSearch').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            searchStations();
        }
    });
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
