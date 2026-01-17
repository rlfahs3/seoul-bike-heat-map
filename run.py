import uvicorn
from app.config import settings

if __name__ == "__main__":
    print("=" * 60)
    print(f"🚲 {settings.app_name} v{settings.app_version}")
    print("=" * 60)
    print(f"- 서버 주소: http://{settings.host}:{settings.port}")
    print(f"- API 문서: http://{settings.host}:{settings.port}/docs")
    print("=" * 60)
    print("\n서버를 시작합니다...\n")
    
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,
        log_level=settings.log_level.lower(),
    )
