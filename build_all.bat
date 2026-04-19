@echo off
echo [1/4] lf_tts:2.0.0 ??...
docker build -t lf_tts:2.0.0 "E:\longform_factory\services\tts"
if %errorlevel% neq 0 (echo TTS ?? ?? >> E:\longform_factory\build_log.txt) else (echo TTS OK >> E:\longform_factory\build_log.txt)

echo [2/4] lf_ai_mcp:2.0.0 ??...
docker build -t lf_ai_mcp:2.0.0 "E:\longform_factory\services\ai-mcp"
if %errorlevel% neq 0 (echo MCP ?? ?? >> E:\longform_factory\build_log.txt) else (echo MCP OK >> E:\longform_factory\build_log.txt)

echo [3/4] lf_uploader:3.2.0 ??...
docker build -t lf_uploader:3.2.0 "E:\longform_factory\services\uploader"
if %errorlevel% neq 0 (echo UPLOADER ?? ?? >> E:\longform_factory\build_log.txt) else (echo UPLOADER OK >> E:\longform_factory\build_log.txt)

echo [4/4] lf_ffmpeg_worker:15.0.0 ??...
docker build -t lf_ffmpeg_worker:15.0.0 "E:\longform_factory\services\ffmpeg-worker"
if %errorlevel% neq 0 (echo FFMPEG ?? ?? >> E:\longform_factory\build_log.txt) else (echo FFMPEG OK >> E:\longform_factory\build_log.txt)

echo ?? ?? ?? >> E:\longform_factory\build_log.txt
