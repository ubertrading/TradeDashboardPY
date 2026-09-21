@echo off
setlocal
cd /d "%~dp0"

echo ===================================================
echo Starting Trade Dashboard
echo ===================================================

:: 1. Remove the watchdog.stop sentinel file to re-enable watchdog auto-restart
if exist "var\watchdog.stop" (
    del /f /q "var\watchdog.stop" 2>nul
    echo [1/2] Removed var\watchdog.stop sentinel (re-arming watchdog).
) else (
    echo [1/2] Sentinel clear.
)

:: 2. Check if running as Windows Service; if so, start the service
powershell -NoProfile -Command "$svc = Get-Service 'TradeDashboard' -ErrorAction SilentlyContinue; if ($svc) { Start-Service 'TradeDashboard'; Write-Host '[2/2] Started TradeDashboard Windows Service.' } else { exit 42 }"
if %ERRORLEVEL% EQU 0 (
    echo.
    echo Trade Dashboard started via Windows Service.
    goto :end
)

:: Otherwise start directly via Python
echo [2/2] Starting Trade Dashboard directly via Python...
start "Trade Dashboard" py trade_dashboard.py

:end
echo ===================================================
