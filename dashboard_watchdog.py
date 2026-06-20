#!/usr/bin/env python3
"""
Dashboard Watchdog Monitor
~~~~~~~~~~~~~~~~~~~~~~~~~~
Standalone process that monitors the trading dashboard and sends
crash / recovery alerts via the same email + Telegram channels
configured in dashboard_settings.json.

Can optionally auto-restart the dashboard on crash.

Usage:
    python dashboard_watchdog.py

Environment variables (all optional):
    TRADE_PORT           Dashboard port (default: 80)
    TRADE_HOST           Dashboard host (default: 0.0.0.0)
    WATCHDOG_INTERVAL    Seconds between checks (default: 30)
    WATCHDOG_FAILURES    Consecutive failures before alert (default: 3)
    WATCHDOG_AUTO_RESTART  Set to "1" to auto-restart on crash (default: 1)
    WATCHDOG_RESTART_CMD   Custom restart command (default: py trade_dashboard.py)
    WATCHDOG_RESTART_DELAY Seconds to wait before restart (default: 10)
    WATCHDOG_MAX_RESTARTS  Max restarts before giving up (default: 0, 0=unlimited)
    TRADE_SETTINGS_FILE  Path to dashboard_settings.json (default: configs/dashboard_settings.json)
"""

import json
import os
import smtplib
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime
from email.mime.text import MIMEText

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIGS_DIR = os.path.join(SCRIPT_DIR, "configs")
SETTINGS_FILE = os.environ.get("TRADE_SETTINGS_FILE", os.path.join(_CONFIGS_DIR, "dashboard_settings.json"))
TRADE_HOST = os.environ.get("TRADE_HOST", "127.0.0.2")
TRADE_PORT = int(os.environ.get("TRADE_PORT", "80"))
CHECK_INTERVAL = int(os.environ.get("WATCHDOG_INTERVAL", "30"))
FAILURE_THRESHOLD = int(os.environ.get("WATCHDOG_FAILURES", "3"))
AUTO_RESTART = os.environ.get("WATCHDOG_AUTO_RESTART", "1") == "1"
RESTART_CMD = os.environ.get("WATCHDOG_RESTART_CMD", "py trade_dashboard.py")
RESTART_DELAY = int(os.environ.get("WATCHDOG_RESTART_DELAY", "10"))
MAX_RESTARTS = int(os.environ.get("WATCHDOG_MAX_RESTARTS", "0"))

DASHBOARD_URL = f"http://{TRADE_HOST}:{TRADE_PORT}/api/status"


def _load_settings():
    """Load dashboard_settings.json from configs/ subdir (or explicit SETTINGS_FILE path)."""
    try:
        path = SETTINGS_FILE if os.path.isabs(SETTINGS_FILE) else os.path.join(SCRIPT_DIR, SETTINGS_FILE)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WATCHDOG] Warning: cannot read {SETTINGS_FILE}: {e}")
        return {}


def _send_email(settings, subject, body):
    """Send email using dashboard's SMTP config."""
    cfg = settings.get("email", {})
    if not cfg.get("enabled"):
        return False, "Email not enabled"
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = cfg.get("from_addr", cfg.get("smtp_user", ""))
        msg["To"] = cfg.get("to_addr", "")
        if not msg["To"]:
            return False, "No recipient address"
        port = int(cfg.get("smtp_port", 587))
        if port == 465:
            with smtplib.SMTP_SSL(cfg["smtp_host"], port, timeout=15) as srv:
                if cfg.get("smtp_user"):
                    srv.login(cfg["smtp_user"], cfg.get("smtp_pass", ""))
                srv.sendmail(msg["From"], [msg["To"]], msg.as_string())
        else:
            with smtplib.SMTP(cfg["smtp_host"], port, timeout=15) as srv:
                srv.ehlo()
                srv.starttls()
                srv.ehlo()
                if cfg.get("smtp_user"):
                    srv.login(cfg["smtp_user"], cfg.get("smtp_pass", ""))
                srv.sendmail(msg["From"], [msg["To"]], msg.as_string())
        return True, None
    except Exception as e:
        return False, str(e)


def _send_telegram(settings, message):
    """Send Telegram message using dashboard's bot config."""
    cfg = settings.get("telegram", {})
    if not cfg.get("enabled"):
        return False, "Telegram not enabled"
    token = cfg.get("bot_token", "")
    chat_id = cfg.get("chat_id", "")
    if not token or not chat_id:
        return False, "Bot token or chat ID not configured"
    try:
        import ssl
        ctx = ssl._create_unverified_context()
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({"chat_id": chat_id, "text": message, "parse_mode": "HTML"}).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            resp.read()
        return True, None
    except Exception as e:
        return False, str(e)


def _notify(settings, subject, body):
    """Send alert via all configured channels."""
    ok_e, err_e = _send_email(settings, subject, body)
    ok_t, err_t = _send_telegram(settings, body)
    results = []
    if ok_e:
        results.append("email ✓")
    elif err_e:
        results.append(f"email ✗ ({err_e})")
    if ok_t:
        results.append("telegram ✓")
    elif err_t:
        results.append(f"telegram ✗ ({err_t})")
    return ", ".join(results)


def _check_dashboard():
    """Check if dashboard is responding. Returns True if healthy."""
    try:
        req = urllib.request.Request(DASHBOARD_URL)
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return True
    except Exception:
        return False


def _restart_dashboard():
    """Launch the dashboard as a detached subprocess. Returns the process or None."""
    try:
        print(f"[WATCHDOG] Starting dashboard: {RESTART_CMD}")
        # Launch detached so the dashboard outlives the watchdog if needed
        proc = subprocess.Popen(
            RESTART_CMD,
            shell=True,
            cwd=SCRIPT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )
        print(f"[WATCHDOG] Dashboard process started (PID {proc.pid})")
        return proc
    except Exception as e:
        print(f"[WATCHDOG] Failed to restart dashboard: {e}")
        return None


def main():
    print(f"[WATCHDOG] Monitoring dashboard at {DASHBOARD_URL}")
    print(f"[WATCHDOG] Check interval: {CHECK_INTERVAL}s, failure threshold: {FAILURE_THRESHOLD}")
    print(f"[WATCHDOG] Auto-restart: {'ON' if AUTO_RESTART else 'OFF'}"
          + (f" (cmd: {RESTART_CMD}, max: {MAX_RESTARTS or 'unlimited'})" if AUTO_RESTART else ""))
    print(f"[WATCHDOG] Settings file: {SETTINGS_FILE}")

    consecutive_failures = 0
    alerted = False          # True = crash alert sent, waiting for recovery
    down_since = None
    restart_count = 0        # Total restarts since last successful recovery
    dashboard_proc = None    # Tracked subprocess (if we started it)

    while True:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        healthy = _check_dashboard()

        if healthy:
            if alerted:
                # Dashboard recovered — send recovery notification
                settings = _load_settings()
                downtime = ""
                if down_since:
                    delta = datetime.now() - down_since
                    mins = int(delta.total_seconds() // 60)
                    downtime = f"\nDowntime: ~{mins} min"
                restart_note = f"\nAuto-restarted: {restart_count} time(s)" if restart_count > 0 else ""
                msg = f"✅ <b>Dashboard RECOVERED</b>\nBack online at {now}{downtime}{restart_note}"
                result = _notify(settings, "✅ Dashboard Recovered", msg)
                print(f"[WATCHDOG] {now} — Dashboard recovered. Notified: {result}")
                alerted = False
                down_since = None
                restart_count = 0
            consecutive_failures = 0
            print(f"[WATCHDOG] {now} — Dashboard OK")
        else:
            consecutive_failures += 1
            print(f"[WATCHDOG] {now} — Dashboard UNREACHABLE ({consecutive_failures}/{FAILURE_THRESHOLD})")

            if consecutive_failures >= FAILURE_THRESHOLD and not alerted:
                # Confirmed down — send crash alert
                settings = _load_settings()
                down_since = datetime.now()
                restart_info = ""
                if AUTO_RESTART:
                    restart_info = "\nAuto-restart: ENABLED — attempting restart..."
                msg = (
                    f"🚨 <b>Dashboard CRASH DETECTED</b>\n"
                    f"Unreachable since {now}\n"
                    f"URL: {DASHBOARD_URL}\n"
                    f"Failed {consecutive_failures} consecutive health checks"
                    f"{restart_info}"
                )
                result = _notify(settings, "🚨 Dashboard Crash Detected", msg)
                print(f"[WATCHDOG] {now} — CRASH ALERT SENT. Notified: {result}")
                alerted = True

            # Auto-restart logic
            if alerted and AUTO_RESTART:
                can_restart = (MAX_RESTARTS == 0 or restart_count < MAX_RESTARTS)
                # Only attempt restart every FAILURE_THRESHOLD cycles after the initial alert
                # (avoids hammering restarts every 30s)
                cycles_since_alert = consecutive_failures - FAILURE_THRESHOLD
                if can_restart and cycles_since_alert >= 0 and cycles_since_alert % FAILURE_THRESHOLD == 0:
                    restart_count += 1
                    print(f"[WATCHDOG] {now} — Auto-restart attempt #{restart_count}"
                          + (f" (max {MAX_RESTARTS})" if MAX_RESTARTS else ""))
                    time.sleep(RESTART_DELAY)
                    dashboard_proc = _restart_dashboard()
                    if dashboard_proc:
                        # Give it time to start before next health check
                        print(f"[WATCHDOG] Waiting {RESTART_DELAY}s for dashboard to initialize...")
                        time.sleep(RESTART_DELAY)
                elif not can_restart and cycles_since_alert == 0:
                    settings = _load_settings()
                    msg = (
                        f"⛔ <b>Dashboard restart limit reached</b>\n"
                        f"Attempted {restart_count} restarts without recovery.\n"
                        f"Manual intervention required."
                    )
                    _notify(settings, "⛔ Dashboard Restart Limit Reached", msg)
                    print(f"[WATCHDOG] {now} — Max restarts ({MAX_RESTARTS}) reached. Manual intervention needed.")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[WATCHDOG] Stopped.")
        sys.exit(0)
