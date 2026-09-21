# Trade Dashboard — Reliability, Watchdog & Service Guide

## Overview

The Trade Dashboard runs 24/7 on a Windows VPS to manage trade execution, hedge monitoring, and account synchronization across MT4/MT5, FIX, and iFOREX accounts.

This document explains the reliability architecture, how crashes are detected and handled, and how to run or stop the system.

---

## What Happened: The Silent Stop Issue

The dashboard had stopped unexpectedly without raising Python exceptions or logging errors. Investigation of the Windows Event Log revealed:

* **Event ID 1000 (Application Error)**: `python.exe` crashed in `python312.dll` at fault offset `0x1b9c45`.
* **Exception Code**: `0xc0000005` (Access Violation).
* **Exact Crash Point**: Python Garbage Collector (`PyObject_GC_Del` at `Modules/gcmodule.c:2410`).
* **Why it was silent**: A native memory access violation in C/C++ libraries or the Python runtime bypasses Python's `sys.excepthook` and `threading.excepthook`. The Windows OS terminates the process immediately without running Python-level catch blocks.

---

## Reliability & Crash Detection Architecture

To protect against this and ensure continuous uptime, four mechanisms have been implemented:

### 1. Native Crash Tracing (`faulthandler`)
* Initialized at startup in `trade_dashboard.py`.
* Listens directly at the OS level for faults (segfaults, access violations, aborts).
* When a native crash occurs, it dumps the full Python stack trace of all threads to:
  ```
  logs/faulthandler.log
  ```

### 2. PID-Based Fast Crash Detection
* On launch, `trade_dashboard.py` writes its process ID to `var/dashboard.pid`.
* The watchdog queries Windows directly (`tasklist`) to check if the PID is alive.
* If `python.exe` crashes natively, the watchdog catches the dead PID in the next cycle (~30s) instead of waiting for 3 consecutive HTTP timeouts (~90s).

### 3. Intentional Stop Sentinel (`var/watchdog.stop`)
* Prevents unwanted auto-restarts when you deliberately kill or stop the dashboard.
* If `var/watchdog.stop` exists, the watchdog skips auto-restart and suppresses crash alerts.
* When shutting down gracefully (`Ctrl+C` or closing console), `trade_dashboard.py` creates this sentinel automatically.
* When starting the dashboard again, the sentinel is automatically deleted to re-arm protection.

---

## How to Run the Dashboard

You have two ways to run the dashboard. **Windows Services is strongly recommended for VPS deployment.**

### Option A: Windows Services (Recommended for VPS 24/7)

Both the dashboard and the watchdog run as background Windows Services. They survive RDP disconnects, restart after VPS reboots, and manage process recovery.

#### Installation:
Open PowerShell as **Administrator** and run:
```powershell
cd E:\SWAP\trade_dashboard\
.\install_service.ps1
```
*(On local dev machines, use `d:\Documents\dev\TradeDashboard\TradeDashboardPY\`)*

This automatically installs two services via NSSM:
1. **`TradeDashboard`**: Runs `trade_dashboard.py` as a service. Auto-restarted by Windows/NSSM on failure.
2. **`TradeDashboardWatchdog`**: Runs `dashboard_watchdog.py`. Monitors health and sends Telegram/email alerts (with the last 40 lines of `faulthandler.log` attached).

#### Service Management:
```powershell
# Check status
Get-Service TradeDashboard, TradeDashboardWatchdog

# Restart dashboard service
Restart-Service TradeDashboard

# Uninstall services
.\install_service.ps1 -Uninstall
```

---

### Option B: Interactive Console (No Services)

If you prefer to run directly from the command prompt without installing Windows Services:

```cmd
py dashboard_watchdog.py
```

* The watchdog will automatically spawn `trade_dashboard.py`.
* It monitors HTTP endpoints (`/api/status`) and process PID.
* In the event of a crash, it restarts the dashboard and sends Telegram/Email notifications.

---

## Stopping and Starting the Dashboard

To avoid the watchdog auto-restarting the dashboard when you want it stopped, use the provided helper scripts:

### To Stop (Intentional):
Double-click or run:
```cmd
stop_dashboard.bat
```
* Creates `var/watchdog.stop` (telling watchdog **not** to auto-restart).
* Stops the Windows Service if running, or terminates the PID.
* No false crash alerts will be sent.

### To Start / Resume:
Double-click or run:
```cmd
start_dashboard.bat
```
* Deletes `var/watchdog.stop` (re-arming watchdog auto-restart).
* Starts the Windows Service (if installed), or launches `py trade_dashboard.py`.

---

## Log File Reference

| Log File | Purpose |
| :--- | :--- |
| `logs/dashboard.log` | Primary trading and system event log. |
| `logs/faulthandler.log` | Thread stack trace captured during OS/runtime crashes. |
| `logs/TradeDashboard-stdout.log` | Standard output when running as Windows Service. |
| `logs/TradeDashboard-stderr.log` | Error output when running as Windows Service. |
| `var/dashboard.pid` | Process ID of the active dashboard instance. |
| `var/watchdog.stop` | Flag indicating intentional stop; suppresses auto-restart. |
