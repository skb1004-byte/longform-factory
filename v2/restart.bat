@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo 재시작 중...
docker compose down
docker compose up -d
echo ✅ 재시작 완료 — http://localhost:8080
pause
