// Seoul Bike Heat Map - JavaScript

// 전역 변수
let currentStationId = null;

// 페이지 로드 시 초기화
document.addEventListener('DOMContentLoaded', function() {
    initializeEventListeners();
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
    
    // 자동 검색 기능 제거됨 - 엔터 또는 검색 버튼으로만 검색
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

// 스테이션 선택
async function selectStation(stationId, stationName) {
    currentStationId = stationId;
    
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
    progressBar.style.width = `${percentage}%`;
    progressBar.textContent = `${percentage}%`;
    
    // 색상 업데이트
    progressBar.className = 'progress-bar';
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

// 추천 업데이트
function updateRecommendation(recommendation) {
    document.getElementById('recommendationText').textContent = recommendation || '추천 정보가 없습니다.';
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
        html += `
            <tr>
                <td><strong>${hour.hour}시</strong></td>
                <td>
                    <span class="badge status-${hour.status}">
                        ${hour.status_emoji} ${hour.status_label}
                    </span>
                </td>
                <td>${hour.avg_available.toFixed(1)}대</td>
                <td>${Math.round(hour.avg_ratio * 100)}%</td>
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
