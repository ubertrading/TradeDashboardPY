@echo off
setlocal
cd /d "%~dp0"

echo ===================================================
echo Starting Trade Dashboard
echo ===================================================

if exist "var\watchdog.stop" del /f /q "var\watchdog.stop" 2>nul

powershell -NoProfile -Command "$svc = Get-Service 'TradeDashboard' -ErrorAction SilentlyContinue; if ($svc) { Start-Service 'TradeDashboard'; Write-Host '[OK] Started TradeDashboard Windows Service.' } else { exit 42 }"
set SVCERR=%ERRORLEVEL%

if "%SVCERR%"=="0" goto :svc_started

echo Starting Trade Dashboard directly via Python...
start "Trade Dashboard" py trade_dashboard.py
goto :end

:svc_started
echo Trade Dashboard started via Windows Service.

:end
echo ===================================================
