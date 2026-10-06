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

:: 2. If running as a Windows Service, stop the service cleanly (preserving MTBridgeService)
powershell -NoProfile -Command ^
  "if (Get-Service 'TradeDashboard' -ErrorAction SilentlyContinue) {" ^
  "  $nssm = if (Test-Path 'C:\nssm\nssm.exe') { 'C:\nssm\nssm.exe' } elseif (Get-Command 'nssm' -ErrorAction SilentlyContinue) { (Get-Command 'nssm').Source } else { $null };" ^
  "  if ($nssm) { & $nssm set TradeDashboard AppKillProcessTree 0 2>$null | Out-Null };" ^
  "  Stop-Service 'TradeDashboard' -Force -ErrorAction SilentlyContinue;" ^
  "  Write-Host '[2/3] Stopped TradeDashboard Windows Service (preserving background MTBridgeService).';" ^
  "} else { Write-Host '[2/3] TradeDashboard Windows Service not running.' }"

:: 3. Kill running process by PID if PID file exists
powershell -NoProfile -Command ^
  "$pidFile = 'var\dashboard.pid';" ^
  "if (Test-Path $pidFile) {" ^
  "  $pid = (Get-Content $pidFile -Raw).Trim();" ^
  "  if ($pid -match '^\d+$') {" ^
  "    Write-Host \"[3/3] Terminating dashboard process (PID $pid)...\";" ^
  "    taskkill /PID $pid /F 2>$null;" ^
  "  } else { Write-Host '[3/3] Invalid PID in dashboard.pid, skipping.'; }" ^
  "  Remove-Item $pidFile -Force -ErrorAction SilentlyContinue;" ^
  "} else { Write-Host '[3/3] No var\dashboard.pid found.'; }"

echo.
echo Trade Dashboard stopped. Watchdog auto-restart is SUPPRESSED.
echo To restart the dashboard, run: start_dashboard.bat
echo ===================================================
