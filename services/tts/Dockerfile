# FastAPI TTS 서비스 Docker 이미지
# Service: lf_tts:2.0.0
# Port: 8001

FROM python:3.11-slim

LABEL maintainer="LongForm Factory"
LABEL version="2.0.0"
LABEL description="ElevenLabs 기반 텍스트-음성 변환 FastAPI 서비스"

# ==================== 시스템 패키지 설치 ====================

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# ==================== 작업 디렉터리 설정 ====================

WORKDIR /app

# ==================== Python 의존성 설치 ====================

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# ==================== 애플리케이션 복사 ====================

COPY app.py .

# ==================== 데이터 디렉터리 생성 ====================

RUN mkdir -p /data/tmp

# ==================== 헬스 체크 ====================

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8001/health', timeout=5)" || exit 1

# ==================== 포트 노출 ====================

EXPOSE 8001

# ==================== 환경 변수 ====================

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# ==================== 실행 ====================

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]
