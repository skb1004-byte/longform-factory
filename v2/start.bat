@echo off
chcp 65001 >nul
echo.
echo ╔══════════════════════════════════════════╗
echo ║   Longform Factory v2.0  Starting...    ║
echo ╚══════════════════════════════════════════╝
echo.
cd /d "%~dp0"
docker compose up -d
echo.
echo ✅ 서비스 시작 완료
echo    n8n 대시보드 : http://localhost:8080
echo    ID: admin / PW: factory123
echo.
pause
