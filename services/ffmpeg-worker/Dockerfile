FROM python:3.11-slim

# FFmpeg 및 필수 라이브러리 설치
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libsm6 libxext6 libfontconfig1 \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY app.py .

# 데이터 디렉토리 생성
RUN mkdir -p /data/jobs /data/tmp /data/output/longform /data/output/shorts /data/output/thumbnails /data/bgm

# 환경 변수 설정
ENV TZ=Asia/Seoul
ENV PYTHONUNBUFFERED=1

# 포트 노출
EXPOSE 8002

# 애플리케이션 시작
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8002", "--workers", "2"]
