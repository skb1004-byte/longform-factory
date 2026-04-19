# LongForm Factory AI-MCP Service v2.0.0
# Python 3.11 기반 FastAPI 멀티 AI 프로바이더 서비스

FROM python:3.11-slim

# 메타데이터
LABEL maintainer="LongForm Factory"
LABEL service="lf_ai_mcp"
LABEL version="2.0.0"

# 환경 변수
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    SERVICE_NAME=lf_ai_mcp \
    SERVICE_VERSION=2.0.0 \
    SERVICE_PORT=8010

# 작업 디렉토리
WORKDIR /app

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# 애플리케이션 파일 복사
COPY app.py .

# 헬스 체크
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8010/health || exit 1

# 포트 노출
EXPOSE 8010

# 시작 명령
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8010", "--workers", "2"]
