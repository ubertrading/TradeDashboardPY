@echo off
setlocal
cd /d "%~dp0"

echo ===================================================
echo Stopping Trade Dashboard (Intentional Stop)
echo ===================================================

:: 1. Create the watchdog.stop sentinel file so watchdog won't auto-restart
if not exist "var" mkdir var
echo Intentional stop initiated on %date% %time% > "var\watchdog.stop"
echo [1/3] Created var\watchdog.stop sentinel.

:: 2. If running as a Windows Service, stop the service cleanly
powershell -NoProfile -Command "if (Get-Service 'TradeDashboard' -ErrorAction SilentlyContinue) { Stop-Service 'TradeDashboard' -Force -ErrorAction SilentlyContinue; Write-Host '[2/3] Stopped TradeDashboard Windows Service.' } else { Write-Host '[2/3] TradeDashboard Windows Service not running.' }"

:: 3. Kill running process by PID if PID file exists
if exist "var\dashboard.pid" (
    set /p DASH_PID=<"var\dashboard.pid"
    if defined DASH_PID (
        echo [3/3] Terminating dashboard process (PID %DASH_PID%)...
        taskkill /PID %DASH_PID% /F 2>nul
        del /f /q "var\dashboard.pid" 2>nul
    )
) else (
    echo [3/3] No var\dashboard.pid found.
)

echo.
echo Trade Dashboard stopped. Watchdog auto-restart is SUPPRESSED.
echo To restart the dashboard, run: start_dashboard.bat
echo ===================================================
