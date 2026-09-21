<#
.SYNOPSIS
    Installs the Trade Dashboard and Watchdog as Windows Services using NSSM.

.DESCRIPTION
    Installs two services:
      1. TradeDashboard  -- runs trade_dashboard.py
      2. TradeDashboardWatchdog -- runs dashboard_watchdog.py

    Both services:
      - Run under the LOCAL SYSTEM account (or a user you specify)
      - Survive RDP session disconnects
      - Auto-restart after VPS reboots
      - Are configured to restart automatically on failure

    Prerequisites:
      - Run this script as Administrator
      - Python must be installed and accessible at the path below
      - nssm.exe (downloaded automatically if not found)

.EXAMPLE
    # Install with defaults:
    .\install_service.ps1

    # Uninstall:
    .\install_service.ps1 -Uninstall

    # Install with a custom port:
    .\install_service.ps1 -TradePort 5001
#>

param(
    [switch]$Uninstall,
    [string]$PythonExe   = "",          # Auto-detected if empty
    [int]   $TradePort   = 80,
    [string]$TradeHost   = "127.0.0.2",
    [string]$ServiceName = "TradeDashboard",
    [string]$WatchdogServiceName = "TradeDashboardWatchdog"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

# -- Helper: require Administrator ------------------------------------------
if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
        [Security.Principal.WindowsBuiltInRole]"Administrator")) {
    Write-Error "This script must be run as Administrator. Right-click -> 'Run as Administrator'."
    exit 1
}

# -- Locate Python -----------------------------------------------------------
if (-not $PythonExe) {
    $cmdPy = Get-Command python.exe -ErrorAction SilentlyContinue
    $cmdPySrc = if ($cmdPy) { $cmdPy.Source } else { $null }
    $cmdPyL = Get-Command py.exe -ErrorAction SilentlyContinue
    $cmdPyLSrc = if ($cmdPyL) { $cmdPyL.Source } else { $null }

    $candidates = @(
        "C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe",
        "C:\Python312\python.exe",
        $cmdPySrc,
        $cmdPyLSrc
    )
    foreach ($c in $candidates) {
        if ($c -and (Test-Path $c)) { $PythonExe = $c; break }
    }
}
if (-not $PythonExe -or -not (Test-Path $PythonExe)) {
    Write-Error "Cannot locate python.exe. Pass -PythonExe 'C:\path\to\python.exe' explicitly."
    exit 1
}
Write-Host "[install] Using Python: $PythonExe" -ForegroundColor Cyan

# -- Locate or download nssm -------------------------------------------------
$cmdNssm = Get-Command nssm.exe -ErrorAction SilentlyContinue
$NssmExe = if ($cmdNssm) { $cmdNssm.Source } else { $null }
if (-not $NssmExe) {
    $nssmDir  = Join-Path $ScriptDir "var\nssm"
    $NssmExe  = Join-Path $nssmDir "nssm.exe"
    if (-not (Test-Path $NssmExe)) {
        Write-Host "[install] nssm not found -- downloading..." -ForegroundColor Yellow
        $nssmZip = Join-Path $env:TEMP "nssm.zip"
        $nssmUrl = "https://nssm.cc/release/nssm-2.24.zip"
        New-Item -ItemType Directory -Force -Path $nssmDir | Out-Null
        try {
            [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
            Invoke-WebRequest -Uri $nssmUrl -OutFile $nssmZip -UseBasicParsing
            Expand-Archive -LiteralPath $nssmZip -DestinationPath $env:TEMP -Force
            $extracted = Get-Item "$env:TEMP\nssm-2.24\win64\nssm.exe" -ErrorAction Stop
            Copy-Item $extracted.FullName -Destination $NssmExe -Force
            Write-Host "[install] nssm downloaded to $NssmExe" -ForegroundColor Green
        } catch {
            Write-Error "Failed to download nssm: $_. Install manually from https://nssm.cc/ and add to PATH."
            exit 1
        }
    }
}
Write-Host "[install] Using nssm: $NssmExe" -ForegroundColor Cyan

# -- Common environment for both services ------------------------------------
$envBlock = "TRADE_PORT=$TradePort`nTRADE_HOST=$TradeHost"

function Install-NssmService {
    param(
        [string]$Name,
        [string]$Script,
        [string]$DisplayName,
        [string]$Description
    )

    # Remove existing service first (idempotent)
    $existing = Get-Service -Name $Name -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "[install] Removing existing service '$Name'..." -ForegroundColor Yellow
        & $NssmExe stop  $Name confirm 2>$null
        & $NssmExe remove $Name confirm
        Start-Sleep -Seconds 2
    }

    Write-Host "[install] Installing service '$Name'..." -ForegroundColor Cyan
    & $NssmExe install $Name $PythonExe $Script
    & $NssmExe set     $Name AppDirectory $ScriptDir
    & $NssmExe set     $Name DisplayName  $DisplayName
    & $NssmExe set     $Name Description  $Description

    # Stdout/stderr -> log files
    $logDir = Join-Path $ScriptDir "logs"
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    & $NssmExe set $Name AppStdout (Join-Path $logDir "$Name-stdout.log")
    & $NssmExe set $Name AppStderr (Join-Path $logDir "$Name-stderr.log")
    & $NssmExe set $Name AppRotateFiles 1
    & $NssmExe set $Name AppRotateBytes 5242880   # 5 MB rotate

    # Environment variables
    & $NssmExe set $Name AppEnvironmentExtra $envBlock

    # Auto-restart on failure: restart after 10s, up to 3 attempts per hour
    & $NssmExe set $Name AppExit   Default Restart
    & $NssmExe set $Name AppRestartDelay 10000   # 10 000 ms

    # Start type = Automatic
    & $NssmExe set $Name Start SERVICE_AUTO_START

    Write-Host "[install] Service '$Name' installed." -ForegroundColor Green
}

if ($Uninstall) {
    # -- Uninstall mode ---------------------------------------------------
    foreach ($svc in @($WatchdogServiceName, $ServiceName)) {
        $existing = Get-Service -Name $svc -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Host "[uninstall] Stopping and removing '$svc'..." -ForegroundColor Yellow
            & $NssmExe stop   $svc confirm 2>$null
            & $NssmExe remove $svc confirm
            Write-Host "[uninstall] '$svc' removed." -ForegroundColor Green
        } else {
            Write-Host "[uninstall] Service '$svc' not found -- skipping." -ForegroundColor Gray
        }
    }
    Write-Host "`n[uninstall] Done." -ForegroundColor Green
    exit 0
}

# -- Install Dashboard service ------------------------------------------------
Install-NssmService `
    -Name        $ServiceName `
    -Script      (Join-Path $ScriptDir "trade_dashboard.py") `
    -DisplayName "Trade Dashboard" `
    -Description "Trading execution dashboard (trade_dashboard.py)"

# -- Install Watchdog service -------------------------------------------------
Install-NssmService `
    -Name        $WatchdogServiceName `
    -Script      (Join-Path $ScriptDir "dashboard_watchdog.py") `
    -DisplayName "Trade Dashboard Watchdog" `
    -Description "Monitors the trade dashboard and auto-restarts on crash (dashboard_watchdog.py)"

# The watchdog's WATCHDOG_AUTO_RESTART should be OFF when both run as services
# (the service manager handles restart), but ON is safe because nssm's restart
# also fires -- just two restart mechanisms at once, which is harmless.
& $NssmExe set $WatchdogServiceName AppEnvironmentExtra "$envBlock`nWATCHDOG_AUTO_RESTART=0"

# -- Start both services ------------------------------------------------------
Write-Host "`n[install] Starting services..." -ForegroundColor Cyan
& $NssmExe start $ServiceName
Start-Sleep -Seconds 15   # Give dashboard time to bind the port before watchdog polls it
& $NssmExe start $WatchdogServiceName

Write-Host "`n[install] [OK] Done! Both services installed and started." -ForegroundColor Green
Write-Host ""
Write-Host "  Service status:"
Get-Service -Name $ServiceName, $WatchdogServiceName | Format-Table Name, Status, StartType -AutoSize
Write-Host ""
Write-Host "  Useful commands:"
Write-Host "    Restart dashboard:  Restart-Service $ServiceName"
Write-Host "    View logs:          Get-Content '$ScriptDir\logs\dashboard.log' -Tail 50 -Wait"
Write-Host "    Uninstall:          .\install_service.ps1 -Uninstall"
