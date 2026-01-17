"""
Seoul Bike Heat Map - FastAPI 애플리케이션 메인 파일
"""

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pathlib import Path

from app.config import settings, get_status_emoji, get_status_label
from app.api import stations, availability

# FastAPI 앱 인스턴스 생성
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="따릉이 시간대별 대여 가능성 히트맵 서비스",
    debug=settings.debug,
)

# 정적 파일 마운트
BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# 템플릿 설정
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# 템플릿에서 사용할 전역 함수 등록
templates.env.globals["get_status_emoji"] = get_status_emoji
templates.env.globals["get_status_label"] = get_status_label


# 루트 라우트 (메인 페이지)
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """메인 페이지"""
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_name": settings.app_name,
        }
    )


# Health Check 엔드포인트
@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    from app.database.db import test_connection, check_tables_exist
    
    db_connected = test_connection()
    tables_status = check_tables_exist() if db_connected else {}
    
    return {
        "status": "healthy" if db_connected else "unhealthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "database": {
            "connected": db_connected,
            "tables": tables_status
        }
    }


# API 라우터 등록
app.include_router(stations.router, prefix="/api/stations", tags=["stations"])
app.include_router(availability.router, prefix="/api/availability", tags=["availability"])


# 시작 이벤트
@app.on_event("startup")
async def startup_event():
    """애플리케이션 시작 시 실행"""
    print("=" * 60)
    print(f"🚲 {settings.app_name} v{settings.app_version}")
    print("=" * 60)
    print(f"📍 서버: http://{settings.host}:{settings.port}")
    print(f"📚 API 문서: http://{settings.host}:{settings.port}/docs")
    print("=" * 60)
    
    # API 키 확인
    if not settings.seoul_api_key:
        print("⚠️  경고: SEOUL_API_KEY가 설정되지 않았습니다.")
        print("   서울 열린데이터광장에서 API 키를 발급받아 .env에 설정하세요.")


# 종료 이벤트
@app.on_event("shutdown")
async def shutdown_event():
    """애플리케이션 종료 시 실행"""
    print("🛑 애플리케이션 종료")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,
    )
