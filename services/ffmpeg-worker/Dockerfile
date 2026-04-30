FROM python:3.11-slim

# FFmpeg and required system libraries
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libsm6 libxext6 libfontconfig1 \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy v17 core modules
COPY app.py .
COPY pipelines.py .
COPY config.py .
COPY models.py .
COPY state.py .

# Copy pipeline sub-package
COPY pipeline/__init__.py pipeline/
COPY pipeline/script.py pipeline/
COPY pipeline/assets.py pipeline/
COPY pipeline/tts.py pipeline/
COPY pipeline/render.py pipeline/
COPY pipeline/render_utils.py pipeline/
COPY pipeline/audio.py pipeline/
COPY pipeline/subtitle.py pipeline/
COPY pipeline/thumbnail.py pipeline/

# Create data directories
RUN mkdir -p /data/jobs /data/tmp /data/output/longform /data/output/shorts /data/output/thumbnails /data/bgm

# Environment
ENV TZ=Asia/Seoul
ENV PYTHONUNBUFFERED=1

EXPOSE 8002

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8002", "--workers", "1"]
