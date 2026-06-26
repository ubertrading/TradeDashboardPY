#!/usr/bin/env python3
"""
trade_dashboard.py — Trading Execution Dashboard

A standalone Flask application for opening and closing hedged positions
across MT4/MT5 accounts. EAs poll for trade commands and report results.

Features:
  - Create trading sessions (open/close) with configurable parameters
  - Spread validation (EA-side, server enforces max_spread in command)
  - Execution order: side1_first, side2_first, or simultaneous
  - Close: consecutive (one per poll), oldest-first, with count parameter
  - Time window scheduling (start/stop times)
  - Configurable polling interval
  - Session persistence (JSON file)
  - Real-time status dashboard with event log

Usage:
  python trade_dashboard.py
  # or with env vars:
  TRADE_PORT=5001 python trade_dashboard.py
"""

from flask import Flask, request, jsonify
import time
import json
import threading
import csv
import os
import re
import sys
import uuid
import logging
import subprocess
from datetime import datetime, timedelta
from collections import defaultdict

app = Flask(__name__)
app.json.compact = True  # Force compact JSON (no spaces after separators) for EA compatibility

# ─── Configuration ──────────────────────────────────────────────────────────
TRADE_PORT = int(os.environ.get("TRADE_PORT", "80"))
TRADE_HOST = os.environ.get("TRADE_HOST", "127.0.0.2")
DASHBOARD_BASE_URL = f"http://{TRADE_HOST}" if TRADE_PORT == 80 else f"http://{TRADE_HOST}:{TRADE_PORT}"
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ─── Subdirectory layout ─────────────────────────────────────────────────────
# configs/ holds all JSON config and data files
# logs/    holds rotating log files
# Both dirs are created automatically if missing.
_CONFIGS_DIR = os.path.join(_SCRIPT_DIR, "configs")
_LOGS_DIR    = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_CONFIGS_DIR, exist_ok=True)
os.makedirs(_LOGS_DIR,    exist_ok=True)

SESSIONS_FILE   = os.environ.get("TRADE_SESSIONS_FILE",   os.path.join(_CONFIGS_DIR, "trade_sessions.json"))
STRATEGIES_FILE = os.environ.get("TRADE_STRATEGIES_FILE", os.path.join(_CONFIGS_DIR, "trade_strategies.json"))
EVENT_LOG_MAX   = int(os.environ.get("TRADE_EVENT_LOG_MAX", "500"))
REPORTING_FILE  = os.environ.get("TRADE_REPORTING_FILE",  os.path.join(_CONFIGS_DIR, "reporting_data.json"))
SETTINGS_FILE   = os.environ.get("TRADE_SETTINGS_FILE",   os.path.join(_CONFIGS_DIR, "dashboard_settings.json"))
USE_MT_BRIDGE = True  # True = C# bridge service (stable) | False = pythonnet (old, crash-prone)

# Resolve config directory: env var → configs/ subdir → CWD → script dir
TRADE_CONFIG_DIR = os.environ.get("TRADE_CONFIG_DIR")
if not TRADE_CONFIG_DIR:
    # Prefer configs/ subdir if the key account files are there
    if os.path.exists(os.path.join(_CONFIGS_DIR, "mt_direct_accounts.json")) or \
       os.path.exists(os.path.join(_CONFIGS_DIR, "fix_accounts.json")):
        TRADE_CONFIG_DIR = _CONFIGS_DIR
    else:
        cwd = os.getcwd()
        if os.path.exists(os.path.join(cwd, "mt_direct_accounts.json")) or \
           os.path.exists(os.path.join(cwd, "fix_accounts.json")):
            TRADE_CONFIG_DIR = cwd
        else:
            TRADE_CONFIG_DIR = _SCRIPT_DIR

# ─── News Calendar (ForexFactory via free JSON feed) ─────────────────────────
import urllib.request

_news_cache = {"events": [], "fetched_at": 0}
_news_lock = threading.Lock()
_news_fetching = False  # Guard: prevent multiple concurrent fetch threads
_news_last_fail = 0     # Timestamp of last failed fetch (for backoff)
NEWS_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
NEWS_CACHE_TTL = 4 * 3600  # refresh every 4 hours
NEWS_FAIL_BACKOFF = 120    # wait 2 minutes before retrying after failure
NEWS_BLACKOUT_SECONDS = 60  # avoid trading ±60s around news

def _fetch_news_calendar():
    """Fetch this week's economic calendar from ForexFactory (via free JSON mirror)."""
    global _news_fetching, _news_last_fail
    try:
        import ssl
        ctx = ssl._create_unverified_context()
        req = urllib.request.Request(NEWS_CALENDAR_URL, headers={
            "User-Agent": "Mozilla/5.0"
        })
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            data = json.loads(resp.read().decode())
        # Parse dates into timestamps
        events = []
        for ev in data:
            try:
                # Dates come as ISO 8601 with timezone, e.g. "2026-02-27T08:30:00-05:00"
                dt_str = ev.get("date", "")
                if dt_str:
                    # Parse ISO date with timezone
                    from datetime import timezone
                    dt = datetime.fromisoformat(dt_str)
                    events.append({
                        "title": ev.get("title", ""),
                        "country": ev.get("country", ""),
                        "impact": ev.get("impact", ""),
                        "time": dt.timestamp(),
                        "dt_str": dt_str,
                    })
            except Exception:
                continue
        with _news_lock:
            _news_cache["events"] = events
            _news_cache["fetched_at"] = time.time()
        logging.info(f"[NEWS] Fetched {len(events)} events from calendar")
    except Exception as e:
        _news_last_fail = time.time()
        logging.error(f"[NEWS] Failed to fetch calendar: {e}")
    finally:
        _news_fetching = False


def _ensure_news_cache():
    """Ensure news cache is fresh; fetch in background if stale."""
    global _news_fetching
    if _news_fetching:
        return  # Already fetching in another thread
    with _news_lock:
        age = time.time() - _news_cache["fetched_at"]
    if age > NEWS_CACHE_TTL:
        # Don't retry too soon after a failure (backoff)
        if time.time() - _news_last_fail < NEWS_FAIL_BACKOFF:
            return
        _news_fetching = True
        threading.Thread(target=_fetch_news_calendar, daemon=True).start()


def is_news_blackout(impact_filter="High"):
    """Check if current time is within ±NEWS_BLACKOUT_SECONDS of any news event.
    impact_filter: only consider events with this impact level or higher.
    Returns (bool, str) - (is_blocked, reason)
    """
    # NEWS DISABLED — return immediately to avoid task queue flooding
    return (False, '')
    _ensure_news_cache()
    now = time.time()
    impact_levels = {"Holiday": 0, "Low": 1, "Medium": 2, "High": 3}
    min_impact = impact_levels.get(impact_filter, 3)
    with _news_lock:
        for ev in _news_cache["events"]:
            ev_impact = impact_levels.get(ev["impact"], 0)
            if ev_impact >= min_impact:
                if abs(now - ev["time"]) <= NEWS_BLACKOUT_SECONDS:
                    return True, f"{ev['country']} {ev['title']} ({ev['impact']})"
    return False, ""

# ─── Logging ────────────────────────────────────────────────────────────────
from logging.handlers import RotatingFileHandler

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_FILE = os.path.join(_LOGS_DIR, "dashboard.log")

# Root logger: console + rolling file
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
root_logger.addHandler(console_handler)

# Rolling file handler: 5 MB per file, keep 3 backups
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
root_logger.addHandler(file_handler)

app.logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# Suppress werkzeug's per-request HTTP log lines (very verbose with open_tickets params)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

# Redirect print() to logger so HEDGE-MON, HEDGE-TRACE etc. are captured in the log file
class _PrintLogger:
    def write(self, msg):
        msg = msg.rstrip()
        if msg:
            root_logger.info(msg)
    def flush(self):
        pass

sys.stdout = _PrintLogger()

# ─── In-memory state ────────────────────────────────────────────────────────
lock = threading.RLock()  # RLock: reentrant — _send_trade_alert re-acquires inside _log_event
sessions = {}           # session_id -> session dict
event_log = []          # list of {ts, session_id, account, event, detail}
ea_heartbeats = {}      # account -> last_poll_ts (track EA connectivity)
ea_account_info = {}    # account -> {balance, equity, bid, ask, spread, symbol, last_update}
_last_known_balances = {}   # account -> float
_last_known_positions = {}  # account -> int
_last_pos_change_ts = {}    # account -> float
_pending_fee_alerts = {}    # account -> {ts, prev_bal, new_bal}
_direct_quote_cache = {}  # (account, pair) -> {bid, ask} — last good get_symbol_info result
manual_accounts = {}    # name -> {conn_type, balance, equity}
strategies = {}         # strategy_id -> {id, name, account1, account2, created_at}
in_flight_commands = {}  # (session_id, account) -> timestamp — prevents duplicate commands
in_flight_retry_counts = {}  # (session_id, account) -> int — tracks close retry attempts
pending_position_reports = {}  # request_id -> {strategy_id, accounts: [acct1, acct2], comment_filter, pair, received: {acct: [positions]}, ts}
cycle_reminders = {}  # account_id -> {days_held, max_days, oldest_ts, message}
_last_fund_dist_ts = 0.0
_cached_fund_distributions = {}

# ─── Cycle Reminder Logic ────────────────────────────────────────────────────
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")  # Handles EST/EDT automatically
except ImportError:
    import pytz
    NY_TZ = pytz.timezone("America/New_York")

def _count_rollover_days(open_epoch, now_epoch=None):
    """Count rollover periods as calendar days since open,
    respecting the 5:00 PM Eastern Time rollover boundary."""
    if now_epoch is None:
        now_epoch = time.time()
    # Subtract 17 hours to align the 5:00 PM (17:00) EST rollover with midnight calendar boundaries.
    open_dt = datetime.fromtimestamp(open_epoch, tz=NY_TZ) - timedelta(hours=17)
    now_dt = datetime.fromtimestamp(now_epoch, tz=NY_TZ) - timedelta(hours=17)
    days = (now_dt.date() - open_dt.date()).days
    return max(0, days)

def _reminder_due_day(open_epoch, max_days):
    """Return the date when the reminder should fire, adjusted for weekends."""
    open_dt = datetime.fromtimestamp(open_epoch, tz=NY_TZ)
    open_rollover = open_dt.replace(hour=17, minute=0, second=0, microsecond=0)
    if open_dt >= open_rollover:
        open_rollover += timedelta(days=1)
    due_date = open_rollover + timedelta(days=max_days - 1)
    # If due on Saturday (5) or Sunday (6), move to Friday
    if due_date.weekday() == 5:   # Saturday
        due_date -= timedelta(days=1)
    elif due_date.weekday() == 6: # Sunday
        due_date -= timedelta(days=2)
    return due_date

def _get_cycle_reminder_thresholds(cfg):
    """Retrieve cycle reminder thresholds safely.
    Returns (remind_days, max_days) or (None, None) if neither is populated."""
    def to_int(v):
        if v is None or v == "" or str(v).strip() == "":
            return None
        try:
            val = int(v)
            return val if val > 0 else None
        except (ValueError, TypeError):
            return None

    r_days = to_int(cfg.get("cycle_reminder_days"))
    m_days = to_int(cfg.get("cycle_max_days"))
    
    if r_days is None and m_days is None:
        return None, None
        
    if r_days is None:
        r_days = max(1, m_days - 1)
    if m_days is None:
        m_days = r_days + 1
        
    return r_days, m_days

def _parse_broker_timestamp(ts_str, is_direct=True):
    """Parse a broker timestamp string to Unix epoch.
    If is_direct is True, converts MT4/MT5 naive broker time (EET/EEST)
    to UTC by adjusting for the 7-hour offset to New York time.
    Otherwise, parses as UTC/local aware."""
    if not ts_str:
        return None
    import re
    cleaned = str(ts_str).strip()
    has_tz = False
    if cleaned.endswith('Z') or re.search(r'[+-]\d{2}:\d{2}$', cleaned):
        has_tz = True
        
    if cleaned.endswith('Z'):
        cleaned = cleaned[:-1] + '+00:00'
        
    try:
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
            return dt.timestamp()
        else:
            if is_direct:
                dt_ny = dt - timedelta(hours=7)
                dt_ny = dt_ny.replace(tzinfo=NY_TZ)
                return dt_ny.timestamp()
            else:
                return dt.timestamp()
    except Exception:
        pass
        
    s = re.sub(r'[+-]\d{2}:\d{2}$', '', cleaned)
    s = re.sub(r'\.\d+', '', s)
    _FORMATS = ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p",
                "%Y.%m.%d %H:%M:%S", "%Y.%m.%d %H:%M",
                "%Y/%m/%d %H:%M:%S")
    for fmt in _FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            if is_direct:
                dt_ny = dt - timedelta(hours=7)
                dt_ny = dt_ny.replace(tzinfo=NY_TZ)
                return dt_ny.timestamp()
            else:
                return dt.timestamp()
        except (ValueError, TypeError):
            continue
    return None

def _check_cycle_reminders():
    """Scan all accounts with cycle_reminder_enabled and check position ages.
    Two thresholds: cycle_reminder_days (warning) and cycle_max_days (critical max).
    If auto_cycle_enabled, automatically trigger cycle action on sessions."""
    global cycle_reminders
    new_reminders = {}

    # Collect accounts from both MT Direct and FIX managers
    all_accounts = {}  # acct_id -> config dict
    if mt_direct_manager:
        for acct_id, acct in mt_direct_manager.accounts.items():
            all_accounts[acct_id] = acct.config
    if fix_manager:
        for acct_id, acct in fix_manager.accounts.items():
            all_accounts[acct_id] = acct.config

    for acct_id, cfg in all_accounts.items():
        if not cfg.get("cycle_reminder_enabled"):
            continue
        remind_days, max_days = _get_cycle_reminder_thresholds(cfg)
        if remind_days is None or max_days is None:
            continue
        # Get positions from ea_account_info
        # MT Direct stores position_details (list of dicts), EA poll stores positions (list of dicts)
        acct_info = ea_account_info.get(acct_id, {})
        positions = acct_info.get("position_details") or acct_info.get("positions", [])
        if not positions or not isinstance(positions, list):
            continue
        # Find oldest position open time
        oldest_epoch = None
        for pos in positions:
            oe = pos.get("open_epoch")
            if oe and (oldest_epoch is None or oe < oldest_epoch):
                oldest_epoch = oe
        if oldest_epoch is None:
            continue
        days_held = _count_rollover_days(oldest_epoch)
        due_dt = _reminder_due_day(oldest_epoch, remind_days)
        now_dt = datetime.now(NY_TZ)
        is_due = now_dt >= due_dt.replace(hour=17, minute=0, second=0)
        label = cfg.get("label") or acct_id
        is_critical = days_held >= max_days
        is_warning = is_due or days_held >= remind_days
        if is_critical:
            level = "CRITICAL"
        elif is_warning:
            level = "WARNING"
        else:
            continue  # Don't include OK-level accounts in reminders
        msg = (f"{label}: positions held {days_held} rollover days "
               f"(remind {remind_days} / max {max_days})")
        if is_critical:
            msg += " — CYCLE IMMEDIATELY"
        elif is_warning:
            msg += " — CYCLE SOON"
        new_reminders[acct_id] = {
            "days_held": days_held,
            "remind_days": remind_days,
            "max_days": max_days,
            "oldest_ts": oldest_epoch,
            "level": level,
            "message": msg,
        }

        # ── Auto-Cycle Trigger ──────────────────────────────────────
        if is_critical and cfg.get("auto_cycle_enabled"):
            _trigger_auto_cycle(acct_id, label, days_held, max_days)

    cycle_reminders = new_reminders



def _trigger_auto_cycle(acct_id, label, days_held, max_days):
    """Find active sessions containing acct_id and set them to cycle mode."""
    with lock:
        for sid, session in sessions.items():
            action = session.get("action", "")
            # Only trigger from open or monitor — never override close or existing cycle
            if action not in ("open", "monitor"):
                continue
            if session.get("status") not in ("active", "paused"):
                continue
            sides = session.get("sides", {})
            if acct_id not in sides:
                continue
            # Determine which cycle action to set based on side_number
            side_info = sides[acct_id]
            side_num = side_info.get("side_number", 1)
            cycle_action = f"cycle_acc{side_num}"
            session["action"] = cycle_action
            session["cycle_days"] = max_days  # so per-position age check passes
            session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _log_event(sid, acct_id, "auto_cycle_triggered",
                       f"Positions held {days_held} rollover days (max={max_days}). "
                       f"Auto-cycling {label} (side {side_num}).")
            print(f"[AUTO-CYCLE] sid={sid[:8]}: triggered {cycle_action} on {label} "
                  f"(held={days_held}d, max={max_days}d)")
            # Send notification
            tg_msg = (f"<b>🔄 Auto-Cycle Triggered</b>\n\n"
                      f"Account: {label}\n"
                      f"Session: {sid[:8]}\n"
                      f"Days held: {days_held} (max: {max_days})\n"
                      f"Action: {cycle_action}")
            threading.Thread(target=_send_telegram, args=(tg_msg,), daemon=True).start()
    _save_sessions()

def _friday_weekend_check():
    """On Friday start (after Thursday 5PM EST rollover), check if any positions
    can't survive the weekend without exceeding max_days.
    Formula: max_days - current_age - 3 < 0  (3 = Fri→Mon rollover days)."""
    # Collect accounts from both MT Direct and FIX managers
    all_accounts = {}
    if mt_direct_manager:
        for acct_id, acct in mt_direct_manager.accounts.items():
            all_accounts[acct_id] = acct.config
    if fix_manager:
        for acct_id, acct in fix_manager.accounts.items():
            all_accounts[acct_id] = acct.config
    if not all_accounts:
        return
    alerts = []
    for acct_id, cfg in all_accounts.items():
        if not cfg.get("cycle_reminder_enabled"):
            continue
        remind_days, max_days = _get_cycle_reminder_thresholds(cfg)
        if remind_days is None or max_days is None:
            continue
        acct_info = ea_account_info.get(acct_id, {})
        positions = acct_info.get("position_details") or acct_info.get("positions", [])
        if not positions or not isinstance(positions, list):
            continue
        oldest_epoch = None
        for pos in positions:
            oe = pos.get("open_epoch") or pos.get("ts_epoch")
            if oe and (oldest_epoch is None or oe < oldest_epoch):
                oldest_epoch = oe
        if oldest_epoch is None:
            continue
        age = _count_rollover_days(oldest_epoch)
        remaining = max_days - age - 3  # 3 rollover days over the weekend
        if remaining < 0:
            label = cfg.get("label") or acct_id
            alerts.append(f"{label}: age={age}d, max={max_days}d, "
                          f"won't survive weekend (need {abs(remaining)} more days than available)")
    if alerts:
        subject = "\u26a0\ufe0f URGENT: Cycle before weekend!"
        body = ("The following accounts will exceed max days over the weekend "
                "if not cycled before market close:\n\n" + "\n".join(alerts))
        tg_msg = ("<b>\u26a0\ufe0f URGENT: Cycle before weekend!</b>\n\n"
                  + "\n".join(alerts))
        print(f"[CYCLE-FRIDAY] Weekend alert: {len(alerts)} account(s) need cycling")
        for a in alerts:
            print(f"[CYCLE-FRIDAY]   {a}")
        def _send():
            _send_email(subject, body)
            _send_telegram(tg_msg)
        threading.Thread(target=_send, daemon=True, name="FridayCycleAlert").start()

def _cycle_reminder_loop():
    """Background thread: check reminders shortly after each 5PM EST rollover.
    Also runs Friday weekend check at start of Friday (Thursday 5PM EST)."""
    _friday_checked_this_week = None
    while True:
        try:
            now = datetime.now(NY_TZ)
            # Target: 5:01 PM EST
            target = now.replace(hour=17, minute=1, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait_secs = (target - now).total_seconds()
            # Don't wait more than 60 seconds — also check periodically for import triggers
            time.sleep(min(wait_secs, 60))
            # Run the check
            with lock:
                _check_cycle_reminders()
            # Friday weekend check: Thursday 5PM EST = start of Friday trading day
            now2 = datetime.now(NY_TZ)
            # weekday() 3 = Thursday; after 5PM Thursday = Friday trading day
            is_friday_start = (now2.weekday() == 3 and now2.hour >= 17) or \
                              (now2.weekday() == 4 and now2.hour < 17)
            week_id = now2.strftime("%Y-W%W")
            if is_friday_start and _friday_checked_this_week != week_id:
                _friday_checked_this_week = week_id
                with lock:
                    _friday_weekend_check()
        except Exception as e:
            logger.error("Cycle reminder loop error: %s", e)
            time.sleep(60)

threading.Thread(target=_cycle_reminder_loop, daemon=True, name="CycleReminder").start()

# ─── Swap Delta (rollover change) ─────────────────────────────────────────────
# Captures swap values before the 5 PM ET rollover; deltas are then computed
# live in get_status by comparing current swap values to the snapshot.
_swap_delta = {
    "pre": {},          # account_id -> total_swap before rollover
    "pre_by_instrument": {}, # account_id -> {symbol -> swap} before rollover
    "delta": {},        # account_id -> change in swap (computed live)
    "delta_by_instrument": {}, # account_id -> {symbol -> delta_swap} (computed live)
    "snapshot_date": None,  # date string of last pre-rollover snapshot
    "snapshot_ts": 0,   # epoch when snapshot was taken
}

def _get_all_swap_values():
    """Collect current total_swap for every known account."""
    result = {}
    try:
        # FIX accounts
        if 'fix_manager' in globals() and fix_manager:
            for aid, info in fix_manager.get_status().items():
                v = info.get("total_swap")
                if v is not None:
                    result[aid] = float(v)
        # MT Direct accounts
        if 'mt_direct_manager' in globals() and mt_direct_manager:
            for aid, info in mt_direct_manager.get_status().items():
                v = info.get("total_swap")
                if v is not None:
                    result[aid] = float(v)
        # EA-polled / manual accounts
        for aid, info in list(ea_account_info.items()):
            if aid not in result:
                v = info.get("total_swap")
                if v is not None:
                    result[aid] = float(v)
    except Exception as e:
        logger.error("[SWAP-DELTA] Snapshot collection error: %s", e, exc_info=True)
    return result

def _get_all_swap_breakdowns():
    """Collect current swap_by_instrument for every known account."""
    result = {}
    try:
        for aid, info in list(ea_account_info.items()):
            sbi = info.get("swap_by_instrument")
            if sbi:
                result[aid] = dict(sbi)
    except Exception as e:
        logger.error("[SWAP-DELTA] Snapshot detailed collection error: %s", e, exc_info=True)
    return result

def _compute_swap_deltas_live():
    """Compute swap deltas by comparing current swap values to the pre-rollover snapshot.
    Called from get_status on every poll after rollover."""
    if not _swap_delta["pre"]:
        return {}
    now_et = datetime.now(NY_TZ)
    # Only compute deltas after 5 PM ET and before the next snapshot (4:58 PM next day)
    if now_et.hour < 17:
        # Before 5 PM — show yesterday's computed delta if available
        return _swap_delta.get("delta", {})
    # After 5 PM: compute live delta = current - pre
    current = _get_all_swap_values()
    delta = {}
    delta_by_inst = {}
    for aid, cur_val in current.items():
        pre_val = _swap_delta["pre"].get(aid)
        if pre_val is not None:
            d = round(cur_val - pre_val, 2)
            if d != 0:
                delta[aid] = d

        # Calculate per-instrument delta for this account
        info = ea_account_info.get(aid, {})
        cur_sbi = info.get("swap_by_instrument", {})
        pre_sbi = _swap_delta["pre_by_instrument"].get(aid, {})
        all_syms = set(cur_sbi.keys()) | set(pre_sbi.keys())
        inst_deltas = {}
        for sym in all_syms:
            c_val = cur_sbi.get(sym, 0.0)
            p_val = pre_sbi.get(sym, 0.0)
            d_val = round(c_val - p_val, 2)
            if d_val != 0:
                inst_deltas[sym] = d_val
        if inst_deltas:
            delta_by_inst[aid] = inst_deltas

    _swap_delta["delta"] = delta
    _swap_delta["delta_by_instrument"] = delta_by_inst
    return delta

def _calculate_optimal_fund_distributions(all_accounts_info):
    """
    Groups accounts by AA-NN hedge code, calculates optimal equity distribution
    between Side A and Side B based on leverage and stopout levels, and then
    allocates that total per side based on margin used per account.
    """
    groups = {}  # group_id -> {"A": [acct_list], "B": [acct_list]}
    
    # 1. Parse accounts into hedge groups
    for aid, ainfo in all_accounts_info.items():
        # Try parsing from label first, fallback to account ID (aid)
        label = ainfo.get("label") or aid
        parts = label.split('-')
        if len(parts) < 3 or parts[2].upper() not in ('A', 'B'):
            # Fallback to splitting aid directly
            parts = aid.split('-')

        if len(parts) >= 3:
            aa = parts[0]
            nn = parts[1]
            side = parts[2].upper()
            if side in ('A', 'B'):
                group_id = f"{aa}-{nn}"
                groups.setdefault(group_id, {"A": [], "B": []})[side].append((aid, ainfo))
                
    results = {}
    
    for group_id, sides in groups.items():
        side_a_accts = sides["A"]
        side_b_accts = sides["B"]
        
        if not side_a_accts or not side_b_accts:
            # Must have at least one account on each side to calculate hedge distribution
            continue
            
        # 2. Gather total equity, margin used, and leverage/stopout for each side
        # For Side A
        total_equity_a = 0.0
        total_margin_a = 0.0
        
        # Stop-out level default: Side A (institutional, swap) -> 50% (0.5), Side B (retail, noswap) -> 20% (0.2)
        # Leverage default: 100
        first_a_id, first_a_info = side_a_accts[0]
        
        # Determine Leverage A
        lev_a = None
        if fix_manager and first_a_id in fix_manager.accounts:
            lev_a = fix_manager.accounts[first_a_id].config.get("leverage")
        if lev_a is None and mt_direct_manager and first_a_id in mt_direct_manager.accounts:
            lev_a = mt_direct_manager.accounts[first_a_id].config.get("leverage")
        if lev_a is None:
            lev_a = first_a_info.get("leverage")
        try:
            lev_a = float(lev_a) if lev_a else 100.0
        except (ValueError, TypeError):
            lev_a = 100.0
            
        # Determine Stop Out A
        stop_out_a = None
        if fix_manager and first_a_id in fix_manager.accounts:
            stop_out_a = fix_manager.accounts[first_a_id].config.get("stop_out_level")
        if stop_out_a is None and mt_direct_manager and first_a_id in mt_direct_manager.accounts:
            stop_out_a = mt_direct_manager.accounts[first_a_id].config.get("stop_out_level")
        if stop_out_a is None:
            stop_out_a = manual_accounts.get(first_a_id, {}).get("stop_out_level")
        if stop_out_a is None:
            stop_out_a = 0.5
        else:
            try:
                stop_out_a = float(stop_out_a)
                if stop_out_a > 1.0:
                    stop_out_a = stop_out_a / 100.0
            except (ValueError, TypeError):
                stop_out_a = 0.5
                
        for aid, ainfo in side_a_accts:
            total_equity_a += float(ainfo.get("equity") or 0.0)
            m = float(ainfo.get("margin") or ainfo.get("margin_used") or 0.0)
            total_margin_a += m
            
        # For Side B
        total_equity_b = 0.0
        total_margin_b = 0.0
        first_b_id, first_b_info = side_b_accts[0]
        
        # Determine Leverage B
        lev_b = None
        if fix_manager and first_b_id in fix_manager.accounts:
            lev_b = fix_manager.accounts[first_b_id].config.get("leverage")
        if lev_b is None and mt_direct_manager and first_b_id in mt_direct_manager.accounts:
            lev_b = mt_direct_manager.accounts[first_b_id].config.get("leverage")
        if lev_b is None:
            lev_b = first_b_info.get("leverage")
        try:
            lev_b = float(lev_b) if lev_b else 100.0
        except (ValueError, TypeError):
            lev_b = 100.0
            
        # Determine Stop Out B
        stop_out_b = None
        if fix_manager and first_b_id in fix_manager.accounts:
            stop_out_b = fix_manager.accounts[first_b_id].config.get("stop_out_level")
        if stop_out_b is None and mt_direct_manager and first_b_id in mt_direct_manager.accounts:
            stop_out_b = mt_direct_manager.accounts[first_b_id].config.get("stop_out_level")
        if stop_out_b is None:
            stop_out_b = manual_accounts.get(first_b_id, {}).get("stop_out_level")
        if stop_out_b is None:
            stop_out_b = 0.2
        else:
            try:
                stop_out_b = float(stop_out_b)
                if stop_out_b > 1.0:
                    stop_out_b = stop_out_b / 100.0
            except (ValueError, TypeError):
                stop_out_b = 0.2
                
        for aid, ainfo in side_b_accts:
            total_equity_b += float(ainfo.get("equity") or 0.0)
            m = float(ainfo.get("margin") or ainfo.get("margin_used") or 0.0)
            total_margin_b += m
            
        total_group_equity = total_equity_a + total_equity_b
        
        # 3. Apply optimal fund allocation formula based on Simulated Risk Buffer
        # We calculate theoretical margin for 1 unit of volume
        m_a = 1.0 / lev_a if lev_a > 0 else 0
        m_b = 1.0 / lev_b if lev_b > 0 else 0
        
        # Stop-out equity required for 1 unit
        so_eq_a = m_a * stop_out_a
        so_eq_b = m_b * stop_out_b
        
        # Apply a proportional risk buffer (0.35 yields roughly 3:1 for 100:1 @ 80% vs 1000:1 @ 50%)
        buffer_factor = 0.35
        risk_buffer = max(m_a, m_b) * buffer_factor
        
        target_eq_a = so_eq_a + risk_buffer
        target_eq_b = so_eq_b + risk_buffer
        
        if (target_eq_a + target_eq_b) > 0:
            alloc_pct_a = target_eq_a / (target_eq_a + target_eq_b)
            alloc_pct_b = 1.0 - alloc_pct_a
        else:
            alloc_pct_a = 0.5
            alloc_pct_b = 0.5
            
        optimal_equity_a = total_group_equity * alloc_pct_a
        optimal_equity_b = total_group_equity * alloc_pct_b
        
        # 4. Allocate within each side based on margin used per account
        for aid, ainfo in side_a_accts:
            if total_margin_a > 0:
                acct_m = float(ainfo.get("margin") or ainfo.get("margin_used") or 0.0)
                share = acct_m / total_margin_a
            else:
                share = 1.0 / len(side_a_accts)
                
            opt_eq = optimal_equity_a * share
            curr_eq = float(ainfo.get("equity") or 0.0)
            results[aid] = {
                "group_id": group_id,
                "side": "A",
                "optimal_equity": round(opt_eq, 2),
                "suggested_transfer": round(opt_eq - curr_eq, 2),
                "allocation_pct": round(alloc_pct_a * 100, 2),
                "stop_out_level": stop_out_a
            }
            
        for aid, ainfo in side_b_accts:
            if total_margin_b > 0:
                acct_m = float(ainfo.get("margin") or ainfo.get("margin_used") or 0.0)
                share = acct_m / total_margin_b
            else:
                share = 1.0 / len(side_b_accts)
                
            opt_eq = optimal_equity_b * share
            curr_eq = float(ainfo.get("equity") or 0.0)
            results[aid] = {
                "group_id": group_id,
                "side": "B",
                "optimal_equity": round(opt_eq, 2),
                "suggested_transfer": round(opt_eq - curr_eq, 2),
                "allocation_pct": round(alloc_pct_b * 100, 2),
                "stop_out_level": stop_out_b
            }
            
    return results

def _swap_delta_loop():
    """Background thread: snapshot swap at 4:58 PM ET daily (pre-rollover)."""
    while True:
        try:
            now = datetime.now(NY_TZ)
            # Target: 4:58 PM ET (2 min before rollover)
            pre_target = now.replace(hour=16, minute=58, second=0, microsecond=0)
            if now >= pre_target:
                pre_target += timedelta(days=1)
            wait_secs = (pre_target - now).total_seconds()
            logger.info("[SWAP-DELTA] Next snapshot at %s ET (waiting %.0fs / %.1fh)",
                        pre_target.strftime("%Y-%m-%d %H:%M"), wait_secs, wait_secs/3600)
            # Sleep in chunks (allows thread to be responsive)
            while wait_secs > 0:
                time.sleep(min(wait_secs, 30))
                wait_secs = (pre_target - datetime.now(NY_TZ)).total_seconds()

            # Take pre-rollover snapshot
            snap = _get_all_swap_values()
            snap_detailed = _get_all_swap_breakdowns()
            snap_date = datetime.now(NY_TZ).strftime("%Y-%m-%d")
            _swap_delta["pre"] = snap
            _swap_delta["pre_by_instrument"] = snap_detailed
            _swap_delta["snapshot_date"] = snap_date
            _swap_delta["snapshot_ts"] = time.time()
            _swap_delta["delta"] = {}  # Clear old deltas; live computation will repopulate after 5 PM
            _swap_delta["delta_by_instrument"] = {}
            logger.info("[SWAP-DELTA] Pre-rollover snapshot taken: %d accounts on %s | values: %s",
                        len(snap), snap_date,
                        {k: v for k, v in list(snap.items())[:5]})  # Log first 5 for diagnostics

        except Exception as e:
            logger.error("[SWAP-DELTA] Loop error: %s", e, exc_info=True)
            time.sleep(60)

threading.Thread(target=_swap_delta_loop, daemon=True, name="SwapDelta").start()

def _optimal_fund_email_loop():
    """Background thread: send daily optimal fund distribution summary."""
    while True:
        try:
            if not dashboard_settings.get("fund_email_enabled", True):
                time.sleep(60)
                continue

            now = datetime.now(NY_TZ)
            time_str = dashboard_settings.get("fund_email_time", "08:00")
            try:
                hour, minute = map(int, time_str.split(":"))
            except ValueError:
                hour, minute = 8, 0

            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait_secs = (target - now).total_seconds()
            
            logger.info("[FUND-EMAIL] Next daily summary email at %s (waiting %.0fs)",
                        target.strftime("%Y-%m-%d %H:%M"), wait_secs)
            
            while wait_secs > 0:
                time.sleep(min(wait_secs, 30))
                if not dashboard_settings.get("fund_email_enabled", True):
                    break
                if dashboard_settings.get("fund_email_time", "08:00") != time_str:
                    break
                wait_secs = (target - datetime.now(NY_TZ)).total_seconds()
            
            if not dashboard_settings.get("fund_email_enabled", True):
                continue
            if dashboard_settings.get("fund_email_time", "08:00") != time_str:
                continue
            
            all_info_for_dist = {}
            for aid, ainfo in ea_account_info.items():
                all_info_for_dist.setdefault(aid, {}).update(ainfo)
            if 'fix_manager' in globals() and fix_manager:
                for aid, info in fix_manager.get_status().items():
                    all_info_for_dist.setdefault(aid, {}).update(info)
            if 'mt_direct_manager' in globals() and mt_direct_manager:
                for aid, info in mt_direct_manager.get_status().items():
                    all_info_for_dist.setdefault(aid, {}).update(info)
                    
            distributions = _calculate_optimal_fund_distributions(all_info_for_dist)
            
            from collections import defaultdict
            email_groups = defaultdict(list)
            for aid, dist_info in distributions.items():
                acct_cfg = _get_account_config(aid)
                if acct_cfg and acct_cfg.get("alert_email"):
                    emails = [e.strip() for e in acct_cfg["alert_email"].split(",") if e.strip()]
                    for email in emails:
                        email_groups[email].append((aid, dist_info))
                        
            for email, items in email_groups.items():
                subject = f"Daily Optimal Fund Distribution Summary - {datetime.now(NY_TZ).strftime('%Y-%m-%d')}"
                
                body = "<html><head><style>"
                body += "table { border-collapse: collapse; width: 100%; font-family: sans-serif; }"
                body += "th, td { border: 1px solid #dddddd; text-align: left; padding: 8px; }"
                body += "th { background-color: #f2f2f2; }"
                body += "</style></head><body>"
                body += "<h2>Daily Optimal Fund Distribution Summary</h2>"
                body += "<table><tr><th>Account</th><th>Group</th><th>Side</th><th>Optimal Equity</th><th>Suggested Transfer</th><th>Allocation</th><th>Stop Out Level</th></tr>"
                
                items.sort(key=lambda x: x[1].get("group_id", ""))
                
                for aid, dist_info in items:
                    body += "<tr>"
                    body += f"<td>{aid}</td>"
                    body += f"<td>{dist_info.get('group_id', 'N/A')}</td>"
                    body += f"<td>{dist_info.get('side', 'N/A')}</td>"
                    body += f"<td>{dist_info.get('optimal_equity', 0.0):.2f}</td>"
                    body += f"<td>{dist_info.get('suggested_transfer', 0.0):.2f}</td>"
                    body += f"<td>{dist_info.get('allocation_pct', 0.0)}%</td>"
                    body += f"<td>{dist_info.get('stop_out_level', 0.0)}</td>"
                    body += "</tr>"
                
                body += "</table></body></html>"
                
                _send_email_direct([email], subject, body, is_html=True)
                logger.info(f"[FUND-EMAIL] Sent HTML summary to {email} ({len(items)} accounts)")
                
        except Exception as e:
            logger.error("[FUND-EMAIL] Loop error: %s", e, exc_info=True)
            time.sleep(60)

threading.Thread(target=_optimal_fund_email_loop, daemon=True, name="OptimalFundEmail").start()

# ─── Swap Rate Change Alert ──────────────────────────────────────────────────
_swap_rate_baseline = {}  # {f"{account_id}:{symbol}": {swap_long, swap_short, account, symbol, ts}}
SWAP_BASELINES_FILE = os.path.join(_CONFIGS_DIR, "swap_baselines.json")

def _load_swap_baselines():
    global _swap_rate_baseline
    try:
        if os.path.exists(SWAP_BASELINES_FILE):
            with open(SWAP_BASELINES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                _swap_rate_baseline.clear()
                _swap_rate_baseline.update(data)
            logger.info("[SWAP-ALERT] Loaded swap baselines for %d instruments", len(_swap_rate_baseline))
    except Exception as e:
        logger.error("[SWAP-ALERT] Failed loading swap baselines: %s", e)

def _save_swap_baselines():
    try:
        with open(SWAP_BASELINES_FILE, "w", encoding="utf-8") as f:
            json.dump(_swap_rate_baseline, f, indent=2, sort_keys=True)
    except Exception as e:
        logger.error("[SWAP-ALERT] Failed saving swap baselines: %s", e)

_load_swap_baselines()

def _swap_alert_loop():
    """Periodically check swap rates on tracked instruments and alert on change."""
    time.sleep(90)  # Initial delay to let accounts connect and populate _symbol_cache
    _empty_retries = 0
    while True:
        try:
            enabled = dashboard_settings.get("swap_alert_enabled", False)
            instruments_str = dashboard_settings.get("swap_alert_instruments", "")
            interval_min = dashboard_settings.get("swap_alert_interval_min", 60)
            pct_threshold = dashboard_settings.get("swap_alert_pct", 10)

            if not enabled or not instruments_str.strip():
                time.sleep(60)  # Check again in a minute if settings change
                _empty_retries = 0
                continue

            instruments = [s.strip() for s in instruments_str.split(",") if s.strip()]
            if not instruments:
                time.sleep(60)
                continue

            # Query swap rates from all connected MT Direct accounts
            current_rates = {}  # {f"{account_id}:{symbol}": {swap_long, swap_short, account, symbol}}
            if 'mt_direct_manager' in globals() and mt_direct_manager:
                acct_list = list(mt_direct_manager.accounts.keys())
                logger.info("[SWAP-ALERT] Checking %d MT Direct accounts: %s", len(acct_list), acct_list)
                for acct_id, acct in mt_direct_manager.accounts.items():
                    if not acct._connected:
                        logger.info("[SWAP-ALERT] Skipping %s (not connected)", acct_id)
                        continue
                    try:
                        rates = acct.get_swap_rates(instruments)
                        logger.info("[SWAP-ALERT] %s returned rates for %d/%d symbols: %s",
                                    acct_id, len(rates), len(instruments),
                                    {s: f"L={d['swap_long']:.5f} S={d['swap_short']:.5f}" for s, d in rates.items()})
                        for sym, data in rates.items():
                            key = f"{acct_id}:{sym}"
                            current_rates[key] = {
                                "swap_long": data["swap_long"],
                                "swap_short": data["swap_short"],
                                "account": acct_id,
                                "symbol": sym,
                            }
                    except Exception as e:
                        logger.error("[SWAP-ALERT] Error querying %s: %s", acct_id, e)
            else:
                logger.warning("[SWAP-ALERT] No mt_direct_manager available")

            if not current_rates:
                logger.info("[SWAP-ALERT] No swap rates obtained from any account (checked %d instruments: %s)",
                            len(instruments), instruments)
                # Retry quickly if caches are still empty (accounts may still be connecting)
                _empty_retries += 1
                if _empty_retries <= 5:
                    logger.info("[SWAP-ALERT] Retrying in 60s (attempt %d/5 — caches may still be populating)", _empty_retries)
                    time.sleep(60)
                else:
                    time.sleep(max(60, interval_min * 60))
                continue

            # Compare against baseline
            global _swap_rate_baseline
            alerts = []
            baselines_updated = False
            for key, curr in current_rates.items():
                prev = _swap_rate_baseline.get(key)
                if prev is None:
                    # First reading — store as baseline, no alert
                    _swap_rate_baseline[key] = {
                        **curr,
                        "ts": time.time(),
                    }
                    baselines_updated = True
                    logger.info("[SWAP-ALERT] Baseline set for %s (%s): long=%.5f short=%.5f",
                                curr["symbol"], curr["account"], curr["swap_long"], curr["swap_short"])
                    continue

                # Check percentage change for both long and short
                triggered = False
                for direction in ("swap_long", "swap_short"):
                    old_val = prev.get(direction, 0)
                    new_val = curr.get(direction, 0)
                    if old_val == 0 and new_val == 0:
                        continue
                    if old_val == 0:
                        pct_change = 100.0  # From zero to something
                    else:
                        pct_change = abs((new_val - old_val) / old_val) * 100

                    if pct_change >= pct_threshold:
                        triggered = True
                        label = "Long" if direction == "swap_long" else "Short"
                        alerts.append({
                            "symbol": curr["symbol"],
                            "direction": label,
                            "old": old_val,
                            "new": new_val,
                            "pct": pct_change,
                            "account": curr["account"],
                        })
                    elif pct_change > 0:
                        label = "Long" if direction == "swap_long" else "Short"
                        logger.debug("[SWAP-ALERT] %s (%s) %s changed %.2f%% (below %.1f%% threshold): %.5f → %.5f",
                                     curr["symbol"], curr["account"], label, pct_change, pct_threshold, old_val, new_val)

                # Update baseline
                if triggered:
                    _swap_rate_baseline[key] = {
                        **curr,
                        "ts": time.time(),
                    }
                    baselines_updated = True

            if baselines_updated:
                _save_swap_baselines()

            # Send consolidated alert if any changes detected
            if alerts:
                lines = []
                for a in alerts:
                    lines.append(f"  [{a['account']}] {a['symbol']} {a['direction']}: {a['old']:.5f} → {a['new']:.5f} ({a['pct']:+.1f}%)")
                body = "Swap rate changes detected:\n" + "\n".join(lines)
                subject = f"\U0001f4b1 Swap Rate Change Alert ({len(alerts)} change{'s' if len(alerts) != 1 else ''})"

                tg_lines = []
                for a in alerts:
                    tg_lines.append(f"  [{a['account']}] <code>{a['symbol']}</code> {a['direction']}: "
                                    f"<b>{a['old']:.5f}</b> → <b>{a['new']:.5f}</b> ({a['pct']:+.1f}%)")
                tg_msg = f"<b>\U0001f4b1 Swap Rate Change</b>\n" + "\n".join(tg_lines)

                def _send_swap_alert():
                    ok_e, _ = _send_email(subject, body)
                    ok_t, _ = _send_telegram(tg_msg)
                    if ok_e:
                        logger.info("[SWAP-ALERT] Email sent: %d changes", len(alerts))
                    if ok_t:
                        logger.info("[SWAP-ALERT] Telegram sent: %d changes", len(alerts))
                threading.Thread(target=_send_swap_alert, daemon=True, name="SwapAlertSend").start()

                logger.info("[SWAP-ALERT] %d swap rate change(s) detected", len(alerts))
            else:
                logger.info("[SWAP-ALERT] No changes above %.1f%% threshold for %d symbols across all connected accounts", pct_threshold, len(current_rates))

        except Exception as e:
            logger.error("[SWAP-ALERT] Loop error: %s", e, exc_info=True)

        # Sleep for the configured interval
        interval_min = dashboard_settings.get("swap_alert_interval_min", 60)
        time.sleep(max(60, interval_min * 60))

threading.Thread(target=_swap_alert_loop, daemon=True, name="SwapAlert").start()

# ─── Market stats CSV logging ────────────────────────────────────────────────
STATS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stats")
_stats_last_write = {}  # account -> last_write_ts (throttle to 1/sec)

def _log_market_stats(account, info):
    """Append one row of market data to a daily CSV for offline analysis."""
    now = time.time()
    # Throttle: max 1 write per second per account
    if now - _stats_last_write.get(account, 0) < 1.0:
        return
    _stats_last_write[account] = now

    # Only log opted-in accounts
    if account not in dashboard_settings.get("stats_log_accounts", []):
        return

    spread = info.get("spread")
    bid = info.get("bid")
    if spread is None or bid is None:
        return  # no useful data yet

    os.makedirs(STATS_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    csv_path = os.path.join(STATS_DIR, f"market_{account}_{date_str}.csv")
    write_header = not os.path.exists(csv_path)

    try:
        with open(csv_path, "a", newline="") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(["timestamp", "account", "pair", "spread", "ticks_5s",
                            "bid", "ask", "bid_delta", "ask_delta"])
            w.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                account,
                info.get("symbol", ""),
                spread,
                info.get("ticks_per_5s", 0),
                bid,
                info.get("ask", ""),
                info.get("last_bid_delta", 0),
                info.get("last_ask_delta", 0),
            ])
    except Exception:
        pass  # never crash the trading loop for stats

import_results = {}  # request_id -> result dict (kept for 60s after completion)
reporting_data = {"snapshots": [], "fees": [], "fee_keywords": ["Holding Fee"]}

# ─── FIX Account Manager ────────────────────────────────────────────────────
try:
    from fix_connector import FixAccountManager
    _fix_dashboard_data = {
        "ea_heartbeats": ea_heartbeats,
        "ea_account_info": ea_account_info,
        "sessions": sessions,
        "lock": lock,
        "in_flight_commands": in_flight_commands,
        "dashboard_url": DASHBOARD_BASE_URL,
    }
    fix_manager = FixAccountManager(
        _fix_dashboard_data,
        config_dir=TRADE_CONFIG_DIR
    )
except ImportError:
    fix_manager = None
    app.logger.warning("fix_connector not available — FIX accounts disabled")

# ─── MT Direct Account Manager ─────────────────────────────────────────────
try:
    if USE_MT_BRIDGE:
        from mt_bridge_client import MtBridgeManager as MTDirectManager
        app.logger.info("MT Direct mode: C# bridge service")
    else:
        from mt_direct_connector import MTDirectManager
        app.logger.info("MT Direct mode: pythonnet (in-process)")
    _mt_direct_dashboard_data = {
        "ea_heartbeats": ea_heartbeats,
        "ea_account_info": ea_account_info,
        "sessions": sessions,
        "lock": lock,
        "in_flight_commands": in_flight_commands,
    }
    mt_direct_manager = MTDirectManager(
        _mt_direct_dashboard_data,
        config_dir=TRADE_CONFIG_DIR
    )
except ImportError:
    mt_direct_manager = None
    app.logger.warning("MT Direct connector not available (bridge=%s) — MT Direct accounts disabled", USE_MT_BRIDGE)

# ─── Ticket normalization (MQL4 32-bit overflow fix) ────────────────────────
def _normalize_ticket(t):
    """Convert ticket to consistent unsigned value.
    MQL4 OrderTicket() returns signed 32-bit int. Brokers assigning tickets > 2^31
    cause overflow to negative in MQL4, but may appear positive in URL params.
    e.g. 4046502528 (unsigned) == -248464768 (signed 32-bit)"""
    try:
        v = int(t)
        if v < 0:
            v += (1 << 32)  # Convert negative 32-bit overflow to unsigned
        return v
    except (ValueError, TypeError):
        return t

# ─── Persistence ────────────────────────────────────────────────────────────
def _save_sessions():
    try:
        tmp = SESSIONS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sessions, f, indent=2, sort_keys=True, default=str)
        os.replace(tmp, SESSIONS_FILE)
    except Exception:
        app.logger.exception("Failed saving sessions")

def _load_sessions():
    global sessions
    try:
        if os.path.exists(SESSIONS_FILE):
            with open(SESSIONS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                sessions = data
                app.logger.info("Loaded %d sessions from %s", len(sessions), SESSIONS_FILE)
                # Migration: patch sides missing side_number
                for sid, s in sessions.items():
                    sides = s.get("sides", {})
                    if sides and not all("side_number" in info for info in sides.values()):
                        for idx, (acc, info) in enumerate(sides.items()):
                            if "side_number" not in info:
                                info["side_number"] = idx + 1
                        app.logger.info("Patched side_number for session %s", sid[:8])
                    # Reset rollback_start_ts on load — stale timestamps from
                    # previous sessions cause immediate 30s timeouts on restart
                    if s.get("rollback_start_ts"):
                        s["rollback_start_ts"] = {}
                        app.logger.info("Reset rollback_start_ts for session %s", sid[:8])
                    # Migration: normalize pair extensions to lowercase
                    # and use base pair (no extension) as global pair.
                    # e.g. global USDCHF.B -> USDCHF, side USDCHF.B -> USDCHF.b
                    def _fix_pair_ext(p):
                        if p and '.' in p:
                            base, ext = p.rsplit('.', 1)
                            return base + '.' + ext.lower()
                        return p
                    def _base_pair(p):
                        if p and '.' in p:
                            return p.rsplit('.', 1)[0]
                        return p
                    old_pair = s.get("pair", "")
                    # Global pair: strip extension to get base instrument
                    new_global = _base_pair(old_pair)
                    if new_global != old_pair:
                        s["pair"] = new_global
                    # Per-side: lowercase extensions, keep them for broker routing
                    for acc, info in sides.items():
                        sp = info.get("pair", "")
                        sp_new = _fix_pair_ext(sp)
                        if sp_new != sp:
                            info["pair"] = sp_new
                return
    except Exception:
        app.logger.exception("Failed loading sessions")
    sessions = {}

_load_sessions()

def _save_strategies():
    """Persist strategies and manual_accounts to disk."""
    try:
        tmp = STRATEGIES_FILE + ".tmp"
        payload = {
            "strategies": strategies,
            "manual_accounts": manual_accounts,
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True, default=str)
        os.replace(tmp, STRATEGIES_FILE)
    except Exception:
        app.logger.exception("Failed saving strategies")

def _load_strategies():
    global strategies, manual_accounts
    try:
        if os.path.exists(STRATEGIES_FILE):
            with open(STRATEGIES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                strategies = data.get("strategies", {})
                manual_accounts = data.get("manual_accounts", {})
                app.logger.info("Loaded %d strategies, %d accounts from %s",
                                len(strategies), len(manual_accounts), STRATEGIES_FILE)
                return
    except Exception:
        app.logger.exception("Failed loading strategies")
    strategies = {}
    manual_accounts = {}

_load_strategies()

# ─── Reporting persistence ──────────────────────────────────────────────────
def _save_reporting():
    try:
        tmp = REPORTING_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(reporting_data, f, indent=2, sort_keys=True, default=str)
        os.replace(tmp, REPORTING_FILE)
    except Exception:
        app.logger.exception("Failed saving reporting data")

def _load_reporting():
    global reporting_data
    try:
        if os.path.exists(REPORTING_FILE):
            with open(REPORTING_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                reporting_data = data
                reporting_data.setdefault("snapshots", [])
                reporting_data.setdefault("fees", [])
                reporting_data.setdefault("fee_keywords", ["Holding Fee"])
                app.logger.info("Loaded %d snapshots, %d fees from %s",
                                len(reporting_data["snapshots"]),
                                len(reporting_data["fees"]), REPORTING_FILE)
                return
    except Exception:
        app.logger.exception("Failed loading reporting data")
    reporting_data = {"snapshots": [], "fees": [], "fee_keywords": ["Holding Fee"]}

_load_reporting()

def _take_balance_snapshot():
    """Record a daily balance snapshot of all known accounts."""
    today = datetime.now().strftime("%Y-%m-%d")
    # Skip if already taken today
    if reporting_data["snapshots"] and reporting_data["snapshots"][-1].get("date") == today:
        return
    accounts = {}
    with lock:
        for acc, info in ea_account_info.items():
            grp = ""
            # Prefer MT Direct label (NAME field) for tree grouping
            if mt_direct_manager:
                mt_acct = mt_direct_manager.accounts.get(acc)
                if mt_acct:
                    grp = mt_acct.config.get("label", "")
            # Fall back to group_label from GROUP column
            if not grp:
                grp = manual_accounts.get(acc, {}).get("group_label", "")
            accounts[acc] = {
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "group_label": grp,
            }
        # Include FIX/manual accounts that may not be in ea_account_info
        for acc, info in manual_accounts.items():
            if acc not in accounts:
                accounts[acc] = {
                    "balance": info.get("balance"),
                    "equity": info.get("equity"),
                    "group_label": info.get("group_label", ""),
                }
    if not accounts:
        return
    # Build group totals: two levels
    # group_label format: NAME-HEDGEGROUP-SIDE (e.g. IRINA-6-A)
    # name_totals:  {"IRINA": {balance, equity}}  — all hedge groups under that name
    # hedge_group_totals: {"IRINA-6": {balance, equity}} — one hedge pair
    name_totals = {}
    hedge_group_totals = {}
    for acc, info in accounts.items():
        grp = info.get("group_label", "")
        parts = grp.split("-")
        bal = info.get("balance") or 0
        eq = info.get("equity") or 0
        if len(parts) >= 3:
            name = parts[0].strip()
            hedge_grp = f"{parts[0].strip()}-{parts[1].strip()}"  # e.g. IRINA-6
            nt = name_totals.setdefault(name, {"balance": 0, "equity": 0})
            nt["balance"] += bal
            nt["equity"] += eq
            ht = hedge_group_totals.setdefault(hedge_grp, {"balance": 0, "equity": 0})
            ht["balance"] += bal
            ht["equity"] += eq
        elif len(parts) == 2:
            # Fallback: 2-part label (GROUP-SIDE)
            prefix = parts[0].strip()
            ht = hedge_group_totals.setdefault(prefix, {"balance": 0, "equity": 0})
            ht["balance"] += bal
            ht["equity"] += eq
    snapshot = {"date": today, "ts": time.time(), "accounts": accounts,
                "name_totals": name_totals, "hedge_group_totals": hedge_group_totals,
                "group_totals": hedge_group_totals}  # backward compat
    reporting_data["snapshots"].append(snapshot)
    # Keep max 365 days
    if len(reporting_data["snapshots"]) > 365:
        reporting_data["snapshots"] = reporting_data["snapshots"][-365:]
    _save_reporting()
    app.logger.info("[REPORTING] Daily snapshot saved for %s (%d accounts)", today, len(accounts))

def _snapshot_scheduler():
    """Background thread: take a snapshot once per day at midnight."""
    import time as _time
    # On startup, take a snapshot if none exists for today
    _time.sleep(5)  # wait for EA data to arrive
    _take_balance_snapshot()
    while True:
        now = datetime.now()
        # Next midnight
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        sleep_sec = (tomorrow - now).total_seconds()
        _time.sleep(sleep_sec)
        _take_balance_snapshot()

_snapshot_thread = threading.Thread(target=_snapshot_scheduler, daemon=True, name="ReportingSnapshot")
_snapshot_thread.start()

# ─── Dashboard Settings (alerts, notifications) ────────────────────────────
_DEFAULT_SETTINGS = {
    "email": {"enabled": False, "smtp_host": "", "smtp_port": 587,
              "smtp_user": "", "smtp_pass": "", "from_addr": "", "to_addr": ""},
    "telegram": {"enabled": False, "bot_token": "", "chat_id": ""},
    "fee_thresholds": {},  # account_name -> threshold (default 0 = any fee)
    "fee_keywords_per_name": {},  # name -> "keyword1,keyword2" (overrides global)
    "stats_log_accounts": [],     # accounts opted in for market stats CSV logging
    "margin_alert_threshold": 85,  # global default margin use % to trigger alert
    "margin_alert_thresholds": {},  # account_name -> per-account override (%)
    "position_change_alert": False,  # alert when position count decreases (closures)
    "position_change_opened": True,
    "position_change_closed": True,
    "position_change_email": True,
    "position_change_telegram": True,
    "swap_alert_instruments": "",  # comma-separated instruments to track swaps (e.g. "USDJPY,USDCHF,XAUUSD")
    "swap_alert_enabled": False,  # enable/disable swap change alerts
    "swap_alert_pct": 10,  # percentage change threshold to trigger swap alert
    "swap_alert_interval_min": 60,  # how often to check swap rates (minutes)
    "theme_colors": {},  # CSS variable overrides for dashboard theme
    "rebalance_close_delay": 1,  # seconds between rebalance close commands (0 = no delay)
    "prompt_on_rollbacks": False,  # if True: pause rollback and require Yes/No confirmation in UI before closing
    "ea_poll_enabled": True,  # if False: ignore all /api/poll_command heartbeats from EAs
    "disbalance_alert_enabled": False,
    "disbalance_alert_telegram": True,
    "disbalance_alert_email": True,
    "disbalance_alert_period_sec": 30,
    # Trading Parameters (execution timeout / retry)
    "exec_timeout_sec": 60,
    "exec_alert_on_timeout": False,
    "exec_halt_on_timeout": False,
    "exec_retry_close": False,
    "exec_retry_max": 5,
    "fund_email_enabled": True,
    "fund_email_time": "08:00",
}

# ─── PnL Request State ──────────────────────────────────────────────────────
pnl_requests = {}  # request_id -> {id, name, accounts, from_date, to_date, fee_keywords, status, results, created_ts}
dashboard_settings = dict(_DEFAULT_SETTINGS)

# Rollback confirmation gate: (sid, account) -> None (pending) / True (approved) / False (denied)
# Populated by _should_issue_command when prompt_on_rollbacks=True.
_rollback_pending_confirmations = {}

def _save_settings():
    try:
        tmp = SETTINGS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(dashboard_settings, f, indent=2, sort_keys=True)
        os.replace(tmp, SETTINGS_FILE)
    except Exception:
        app.logger.exception("Failed saving dashboard settings")

def _load_settings():
    global dashboard_settings
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                dashboard_settings = data
                for k, v in _DEFAULT_SETTINGS.items():
                    dashboard_settings.setdefault(k, v)
                app.logger.info("Loaded settings from %s", SETTINGS_FILE)
                return
    except Exception:
        app.logger.exception("Failed loading dashboard settings")
    dashboard_settings = dict(_DEFAULT_SETTINGS)

_load_settings()

# ─── Alert Functions ────────────────────────────────────────────────────────
import smtplib
from email.mime.text import MIMEText

def _get_account_config(account_id):
    if not account_id:
        return None
    if mt_direct_manager and account_id in mt_direct_manager.accounts:
        return mt_direct_manager.accounts[account_id].config
    if fix_manager and account_id in fix_manager.accounts:
        return fix_manager.accounts[account_id].config
    if account_id in manual_accounts:
        return manual_accounts[account_id]
    return None

def _send_email(subject, body, account_id=None):
    """Send email via SMTP. Returns (success, error_msg)."""
    cfg = dashboard_settings.get("email", {})
    if not cfg.get("enabled"):
        return False, "Email not enabled"
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = cfg.get("from_addr", cfg.get("smtp_user", ""))
        
        # Collect recipients: start with global recipient(s) and append per-account overrides
        recipients = []
        global_to = cfg.get("to_addr", "")
        if global_to:
            recipients.extend([email.strip() for email in global_to.split(",") if email.strip()])
            
        if account_id:
            acct_cfg = _get_account_config(account_id)
            if acct_cfg:
                local_to = acct_cfg.get("alert_email")
                if local_to:
                    recipients.extend([email.strip() for email in local_to.split(",") if email.strip()])
                    
        # Deduplicate while preserving order
        seen = set()
        recipients = [r for r in recipients if not (r in seen or seen.add(r))]
        
        if not recipients:
            return False, "No recipient address configured"
            
        msg["To"] = ", ".join(recipients)
        
        port = int(cfg.get("smtp_port", 587))
        if port == 465:
            # Port 465 requires SSL from the start (implicit TLS)
            with smtplib.SMTP_SSL(cfg["smtp_host"], port, timeout=15) as srv:
                if cfg.get("smtp_user"):
                    srv.login(cfg["smtp_user"], cfg.get("smtp_pass", ""))
                srv.sendmail(msg["From"], recipients, msg.as_string())
        else:
            # Port 587 (or other) uses STARTTLS
            with smtplib.SMTP(cfg["smtp_host"], port, timeout=15) as srv:
                srv.ehlo()
                srv.starttls()
                srv.ehlo()
                if cfg.get("smtp_user"):
                    srv.login(cfg["smtp_user"], cfg.get("smtp_pass", ""))
                srv.sendmail(msg["From"], recipients, msg.as_string())
        return True, None
    except Exception as e:
        app.logger.exception("Email send failed")
        return False, str(e)

def _send_email_direct(recipients, subject, body, is_html=False):
    """Send email directly to a specific list of recipients, bypassing global 'to_addr'."""
    cfg = dashboard_settings.get("email", {})
    if not cfg.get("enabled"):
        return False, "Email not enabled"
    try:
        import smtplib
        msg_type = "html" if is_html else "plain"
        msg = MIMEText(body, msg_type, "utf-8")
        msg["Subject"] = subject
        msg["From"] = cfg.get("from_addr", cfg.get("smtp_user", ""))
        
        # Deduplicate while preserving order
        seen = set()
        clean_recipients = [r for r in recipients if not (r in seen or seen.add(r))]
        
        if not clean_recipients:
            return False, "No recipient address provided"
            
        msg["To"] = ", ".join(clean_recipients)
        
        port = int(cfg.get("smtp_port", 587))
        if port == 465:
            # Port 465 requires SSL from the start (implicit TLS)
            with smtplib.SMTP_SSL(cfg["smtp_host"], port, timeout=15) as srv:
                if cfg.get("smtp_user"):
                    srv.login(cfg["smtp_user"], cfg.get("smtp_pass", ""))
                srv.sendmail(msg["From"], clean_recipients, msg.as_string())
        else:
            # Port 587 (or other) uses STARTTLS
            with smtplib.SMTP(cfg["smtp_host"], port, timeout=15) as srv:
                srv.ehlo()
                srv.starttls()
                srv.ehlo()
                if cfg.get("smtp_user"):
                    srv.login(cfg["smtp_user"], cfg.get("smtp_pass", ""))
                srv.sendmail(msg["From"], clean_recipients, msg.as_string())
        return True, None
    except Exception as e:
        app.logger.exception("Direct email send failed")
        return False, str(e)

def _send_telegram(message, account_id=None):
    """Send Telegram message via Bot API. Returns (success, error_msg)."""
    cfg = dashboard_settings.get("telegram", {})
    if not cfg.get("enabled"):
        return False, "Telegram not enabled"
    token = cfg.get("bot_token", "")
    if not token:
        return False, "Bot token not configured"
        
    # Collect chat IDs: start with global chat ID(s) and append per-account overrides
    chat_ids = []
    global_chat = cfg.get("chat_id", "")
    if global_chat:
        chat_ids.extend([cid.strip() for cid in str(global_chat).split(",") if cid.strip()])
        
    if account_id:
        acct_cfg = _get_account_config(account_id)
        if acct_cfg:
            local_chat = acct_cfg.get("alert_telegram")
            if local_chat:
                chat_ids.extend([cid.strip() for cid in str(local_chat).split(",") if cid.strip()])
                
    # Deduplicate while preserving order
    seen = set()
    chat_ids = [cid for cid in chat_ids if not (cid in seen or seen.add(cid))]
    
    if not chat_ids:
        return False, "Chat ID not configured"
        
    errors = []
    success = False
    for cid in chat_ids:
        try:
            import ssl
            ctx = ssl._create_unverified_context()
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = json.dumps({"chat_id": cid, "text": message, "parse_mode": "HTML"}).encode("utf-8")
            req = urllib.request.Request(url, data=payload,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                resp.read()
            success = True
        except Exception as e:
            app.logger.exception(f"Telegram send failed for chat_id {cid}")
            errors.append(f"{cid}: {str(e)}")
            
    if success:
        return True, None
    else:
        return False, "; ".join(errors)

def _send_fee_alert(account, fee_entry):
    """Check threshold and send fee alert via enabled channels (in background)."""
    thresholds = dashboard_settings.get("fee_thresholds", {})
    threshold = float(thresholds.get(account, 0))  # default 0 = alert on any fee
    fee_amount = abs(fee_entry.get("amount", 0))
    if fee_amount < threshold:
        return  # below threshold, skip

    grp = manual_accounts.get(account, {}).get("group_label", "")
    subject = f"\u26a0\ufe0f Fee Alert: {account}"
    body = (f"Fee detected on account {account}\n"
            f"Group: {grp}\n"
            f"Amount: {fee_entry.get('amount', 0):.2f}\n"
            f"Balance: {fee_entry.get('balance_before', 0):.2f} → {fee_entry.get('balance_after', 0):.2f}\n"
            f"Time: {fee_entry.get('ts', '')}")
    tg_msg = (f"<b>\u26a0\ufe0f Fee Alert</b>\n"
              f"Account: <code>{account}</code>\n"
              f"Group: {grp}\n"
              f"Amount: <b>{fee_entry.get('amount', 0):.2f}</b>\n"
              f"Balance: {fee_entry.get('balance_before', 0):.2f} → {fee_entry.get('balance_after', 0):.2f}\n"
              f"Time: {fee_entry.get('ts', '')}")

    def _send():
        ok_e, err_e = _send_email(subject, body, account_id=account)
        ok_t, err_t = _send_telegram(tg_msg, account_id=account)
        if ok_e:
            app.logger.info("[ALERT] Email sent for fee on %s", account)
        if ok_t:
            app.logger.info("[ALERT] Telegram sent for fee on %s", account)
        if not ok_e and not ok_t and (dashboard_settings.get("email", {}).get("enabled") or
                                       dashboard_settings.get("telegram", {}).get("enabled")):
            app.logger.warning("[ALERT] Failed to send fee alert for %s: email=%s, tg=%s",
                               account, err_e, err_t)
    threading.Thread(target=_send, daemon=True, name=f"FeeAlert-{account}").start()

# ─── Margin Use Alert ────────────────────────────────────────────────────────
_margin_alert_cooldowns = {}  # account -> last alert timestamp

def _send_margin_alert(account, margin_pct, threshold, equity, margin_used,
                       all_accounts_info=None):
    """Send margin use alert via enabled channels (in background).
    all_accounts_info: optional dict used as fallback when the fund-distribution
    cache doesn't yet contain this account (e.g. alert fires before first poll).
    """
    grp = ""
    if manual_accounts.get(account):
        grp = manual_accounts[account].get("group_label", "")
    elif fix_manager and fix_manager.accounts.get(account):
        grp = fix_manager.accounts[account].config.get("group_label", "")

    # ── Rebalance / SHIFT section ──────────────────────────────────────────
    # Pull the SHIFT (suggested_transfer) for this account and its group siblings.
    # Prefer the hourly cache; fall back to an on-demand calculation when the
    # cache is empty or doesn't contain this account yet.
    rebalance_body = ""
    rebalance_tg = ""
    try:
        fund_dist = _cached_fund_distributions
        if not fund_dist or account not in fund_dist:
            # Cache miss — compute now from the live account snapshot
            if all_accounts_info:
                fund_dist = _calculate_optimal_fund_distributions(all_accounts_info)
                app.logger.info("[MARGIN-ALERT] Fund dist computed on-demand for %s: %s",
                                account, fund_dist)
            else:
                fund_dist = {}
        dist = fund_dist.get(account)
        if dist:
            acct_shift = dist.get("suggested_transfer")
            acct_opt_eq = dist.get("optimal_equity")
            group_id = dist.get("group_id", "")

            # Recommended deposit line for the alerting account
            if acct_shift is not None:
                sign = "+" if acct_shift >= 0 else ""
                rebalance_body += (
                    f"\nRecommended deposit: {sign}{acct_shift:,.2f}"
                    f"  (Opt Equity: {acct_opt_eq:,.2f})"
                )
                rebalance_tg += (
                    f"\n💰 <b>Recommended deposit:</b> {sign}{acct_shift:,.2f}"
                    f"  <i>(Opt Equity: {acct_opt_eq:,.2f})</i>"
                )

            # Collect all accounts in the same group for the rebalance table
            group_members = [
                (aid, d) for aid, d in _cached_fund_distributions.items()
                if d.get("group_id") == group_id
            ]
            # Sort: Side A first, then B; within side by account name
            group_members.sort(key=lambda x: (x[1].get("side", ""), x[0]))

            if len(group_members) > 1:
                rebalance_body += "\n\nRebalance as follows:"
                rebalance_tg += "\n\n📊 <b>Rebalance as follows:</b>"
                for aid, d in group_members:
                    shift_val = d.get("suggested_transfer")
                    opt_eq_val = d.get("optimal_equity")
                    side = d.get("side", "?")
                    marker = " ◄" if aid == account else ""
                    shift_str = (f"{'+' if shift_val >= 0 else ''}{shift_val:,.2f}"
                                 if shift_val is not None else "N/A")
                    opt_str = f"{opt_eq_val:,.2f}" if opt_eq_val is not None else "N/A"
                    rebalance_body += (
                        f"\n  [{side}] {aid}: shift {shift_str}  →  opt eq {opt_str}{marker}"
                    )
                    bold_open = "<b>" if aid == account else ""
                    bold_close = "</b>" if aid == account else ""
                    rebalance_tg += (
                        f"\n  {bold_open}[{side}] <code>{aid}</code>: "
                        f"shift {shift_str}  →  opt eq {opt_str}{bold_close}"
                    )
    except Exception as _enrich_err:
        app.logger.warning("[MARGIN-ALERT] Rebalance enrichment failed for %s: %s",
                           account, _enrich_err)

    subject = f"\u26a0\ufe0f Margin Alert: {account}"
    body = (f"Margin use alert on account {account}\n"
            f"Group: {grp}\n"
            f"Margin Use: {margin_pct:.1f}% (threshold: {threshold}%)\n"
            f"Equity: {equity:,.2f}\n"
            f"Margin Used: {margin_used:,.2f}"
            f"{rebalance_body}")
    tg_msg = (f"<b>\u26a0\ufe0f Margin Alert</b>\n"
              f"Account: <code>{account}</code>\n"
              f"Group: {grp}\n"
              f"Margin Use: <b>{margin_pct:.1f}%</b> (threshold: {threshold}%)\n"
              f"Equity: {equity:,.2f}\n"
              f"Margin Used: {margin_used:,.2f}"
              f"{rebalance_tg}")

    def _send():
        ok_e, err_e = _send_email(subject, body, account_id=account)
        ok_t, err_t = _send_telegram(tg_msg, account_id=account)
        if ok_e:
            app.logger.info("[MARGIN-ALERT] Email sent for %s (%.1f%%)", account, margin_pct)
        if ok_t:
            app.logger.info("[MARGIN-ALERT] Telegram sent for %s (%.1f%%)", account, margin_pct)
        if not ok_e and not ok_t and (dashboard_settings.get("email", {}).get("enabled") or
                                       dashboard_settings.get("telegram", {}).get("enabled")):
            app.logger.warning("[MARGIN-ALERT] Failed to send for %s: email=%s, tg=%s",
                               account, err_e, err_t)
    threading.Thread(target=_send, daemon=True, name=f"MarginAlert-{account}").start()

def _check_margin_alerts(all_accounts_info):
    """Check margin use % across all accounts and send alerts if threshold exceeded.
    all_accounts_info: dict of account_id -> {equity, margin, ...}
    """
    global_threshold = dashboard_settings.get("margin_alert_threshold", 85)
    per_account = dashboard_settings.get("margin_alert_thresholds", {})
    now = time.time()
    cooldown_secs = 300  # 5-minute cooldown

    for acct_id, info in all_accounts_info.items():
        try:
            equity = info.get("equity")
            margin = info.get("margin") or info.get("margin_used")
            if not equity or not margin or equity <= 0:
                continue
            margin_pct = (float(margin) / float(equity)) * 100
            # Get threshold: per-account override or global default
            threshold = per_account.get(acct_id)
            if threshold is None or threshold == "" or threshold == 0:
                threshold = global_threshold
            threshold = float(threshold)
            if threshold <= 0:
                continue  # disabled for this account
            if margin_pct >= threshold:
                last_alert = _margin_alert_cooldowns.get(acct_id, 0)
                if now - last_alert >= cooldown_secs:
                    _margin_alert_cooldowns[acct_id] = now
                    _send_margin_alert(acct_id, margin_pct, threshold, float(equity), float(margin),
                                       all_accounts_info=all_accounts_info)
        except Exception:
            pass


# ─── Position Change Alert ────────────────────────────────────────────────────
_last_position_counts = {}  # account -> last known position count

def _send_position_change_alert(account, old_count, new_count, margin_pct=None):
    """Send position change alert via enabled channels (in background)."""
    direction = "closed" if new_count < old_count else "opened"
    diff = abs(new_count - old_count)
    emoji = "\U0001f534" if direction == "closed" else "\U0001f7e2"
    subject = f"{emoji} Position {direction}: {account}"
    
    margin_str = f"{margin_pct:.1f}%" if margin_pct is not None else "N/A"
    
    body = (f"Position change on account {account}\n"
            f"Margin Use: {margin_str}\n"
            f"Positions {direction}: {diff}\n"
            f"Count: {old_count} \u2192 {new_count}")
    tg_msg = (f"<b>{emoji} Position {direction.title()}</b>\n"
              f"Account: <code>{account}</code>\n"
              f"Margin Use: {margin_str}\n"
              f"Positions {direction}: <b>{diff}</b>\n"
              f"Count: {old_count} \u2192 {new_count}")

    def _send():
        if dashboard_settings.get("position_change_email", True):
            ok_e, err_e = _send_email(subject, body, account_id=account)
            if ok_e:
                app.logger.info("[POS-ALERT] Email sent for %s (%d->%d)", account, old_count, new_count)
        if dashboard_settings.get("position_change_telegram", True):
            ok_t, err_t = _send_telegram(tg_msg, account_id=account)
            if ok_t:
                app.logger.info("[POS-ALERT] Telegram sent for %s (%d->%d)", account, old_count, new_count)
    threading.Thread(target=_send, daemon=True, name=f"PosAlert-{account}").start()

def _check_position_changes(all_accounts_info):
    """Detect position count changes and send alerts."""
    if not dashboard_settings.get("position_change_alert", False):
        return
    for acct_id, info in all_accounts_info.items():
        try:
            pos = info.get("positions")
            if pos is None:
                continue
            pos = int(pos)
            prev = _last_position_counts.get(acct_id)
            if prev is not None and pos != prev:
                is_open = pos > prev
                is_close = pos < prev
                should_alert = False
                if is_open and dashboard_settings.get("position_change_opened", True):
                    should_alert = True
                elif is_close and dashboard_settings.get("position_change_closed", True):
                    should_alert = True
                
                if should_alert:
                    margin_pct = None
                    try:
                        equity = info.get("equity")
                        margin = info.get("margin") or info.get("margin_used")
                        if equity is not None and margin is not None and float(equity) > 0:
                            margin_pct = (float(margin) / float(equity)) * 100
                    except Exception:
                        pass
                    _send_position_change_alert(acct_id, prev, pos, margin_pct=margin_pct)
            _last_position_counts[acct_id] = pos
        except Exception:
            pass

# ─── Hedge Disbalance Alert ──────────────────────────────────────────────────
_disbalance_start_ts = 0
_last_disbalance_alert_ts = {}  # subgroup -> last alert timestamp

def _disbalance_alert_loop():
    """Background thread to detect and alert on hedge disbalances."""
    import time as _time
    _time.sleep(10)  # Wait for initial data load

    while True:
        try:
            if not dashboard_settings.get("disbalance_alert_enabled", False):
                _time.sleep(5)
                continue

            # Compute global net lots
            global_net = 0.0
            with lock:
                for aid, info in ea_account_info.items():
                    lbi = info.get("lots_by_instrument", {})
                    for sym, vals in lbi.items():
                        global_net += (vals.get("buy", 0.0) - vals.get("sell", 0.0))

            if abs(global_net) < 0.001:
                # Balanced
                global _disbalance_start_ts
                _disbalance_start_ts = 0
                _time.sleep(2)
                continue

            # It is disbalanced
            now = _time.time()
            if _disbalance_start_ts == 0:
                _disbalance_start_ts = now
            
            period_sec = int(dashboard_settings.get("disbalance_alert_period_sec", 30))
            if now - _disbalance_start_ts < period_sec:
                _time.sleep(2)
                continue

            # Disbalance confirmed over period
            # 1. Gather all accounts and calculate net lots by subgroup
            # Also track connection status
            subgroups_net = {} # subgroup_id -> net lots
            subgroups_accts = {} # subgroup_id -> list of account dicts
            
            # Combine all known configured accounts
            all_cfg_accts = {}
            if mt_direct_manager:
                for a, acc in mt_direct_manager.accounts.items():
                    all_cfg_accts[a] = {"id": a, "manager": "mt"}
            if fix_manager:
                for a, acc in fix_manager.accounts.items():
                    all_cfg_accts[a] = {"id": a, "manager": "fix"}
            for a, info in manual_accounts.items():
                if a not in all_cfg_accts:
                    all_cfg_accts[a] = {"id": a, "manager": "manual"}
            
            for a, info in all_cfg_accts.items():
                parts = a.split("-")
                if len(parts) >= 2:
                    subgroup = f"{parts[0]}-{parts[1]}"
                elif len(parts) == 1:
                    subgroup = parts[0]
                else:
                    continue
                
                if subgroup not in subgroups_accts:
                    subgroups_accts[subgroup] = []
                    subgroups_net[subgroup] = 0.0
                
                # Check connection
                is_conn = False
                if a in ea_account_info:
                    # check heartbeat
                    hb = ea_heartbeats.get(a, 0)
                    if now - hb < 120:  # e.g., 2 minutes
                        is_conn = True
                else:
                    # check manager status directly if not EA
                    if info["manager"] == "mt" and mt_direct_manager:
                        st = mt_direct_manager.get_status().get(a, {})
                        is_conn = st.get("connected", False)
                    elif info["manager"] == "fix" and fix_manager:
                        st = fix_manager.get_status().get(a, {})
                        is_conn = st.get("connected", False)

                # Get lots
                a_net = 0.0
                with lock:
                    if a in ea_account_info:
                        lbi = ea_account_info[a].get("lots_by_instrument", {})
                        for sym, vals in lbi.items():
                            a_net += (vals.get("buy", 0.0) - vals.get("sell", 0.0))

                subgroups_net[subgroup] += a_net
                
                subgroups_accts[subgroup].append({
                    "id": a,
                    "connected": is_conn,
                    "net": a_net
                })

            # Check each subgroup for disbalance
            for sg, net in subgroups_net.items():
                if abs(net) > 0.001:
                    # Check cooldown
                    last_alert = _last_disbalance_alert_ts.get(sg, 0)
                    if now - last_alert < 300: # 5 min cooldown
                        continue
                    
                    _last_disbalance_alert_ts[sg] = now
                    
                    # Generate alert
                    accts = subgroups_accts[sg]
                    disconnected = [acc["id"] for acc in accts if not acc["connected"]]
                    all_ids = [acc["id"] for acc in accts]
                    
                    subject = "Disbalance Detected"
                    if disconnected:
                        subject = "Possible Disbalance Detected"
                    
                    body = f"{subject}: Hedge group {sg.split('-')[0]} and associated accounts {','.join(all_ids)} "
                    if disconnected:
                        body += f"possibly disbalanced by {net:.2f} lots.\n"
                        body += f"Accounts {','.join(disconnected)} are not connected. please connect to verify hedge\n"
                    else:
                        body += f"are disbalanced by {net:.2f} lots.\n"
                    
                    tg_msg = f"<b>{subject}</b>\n"
                    tg_msg += f"Hedge group {sg.split('-')[0]} and associated accounts {','.join(all_ids)} "
                    if disconnected:
                        tg_msg += f"possibly disbalanced by <b>{net:.2f}</b> lots.\n"
                        tg_msg += f"Accounts <code>{','.join(disconnected)}</code> are not connected. please connect to verify hedge\n"
                    else:
                        tg_msg += f"are disbalanced by <b>{net:.2f}</b> lots.\n"
                    
                    # Include breakdown per account
                    body += "\nAccount Breakdown:\n"
                    tg_msg += "\n<b>Account Breakdown:</b>\n"
                    with lock:
                        for acc in accts:
                            a = acc["id"]
                            a_net = acc["net"]
                            body += f" - {a}: {a_net:.2f} lots ("
                            tg_msg += f" - <code>{a}</code>: {a_net:.2f} lots ("
                            if a in ea_account_info:
                                lbi = ea_account_info[a].get("lots_by_instrument", {})
                                sym_strs = []
                                tg_sym_strs = []
                                for sym, vals in lbi.items():
                                    sn = vals.get("buy", 0.0) - vals.get("sell", 0.0)
                                    if abs(sn) > 0.001:
                                        sym_strs.append(f"{sym}: {sn:.2f}")
                                        tg_sym_strs.append(f"{sym}: {sn:.2f}")
                                body += ", ".join(sym_strs) + ")\n"
                                tg_msg += ", ".join(tg_sym_strs) + ")\n"
                            else:
                                body += "no data)\n"
                                tg_msg += "no data)\n"

                    # Send
                    def _send(s, b, tm):
                        if dashboard_settings.get("disbalance_alert_email", True):
                            _send_email(s, b)
                        if dashboard_settings.get("disbalance_alert_telegram", True):
                            _send_telegram(tm)
                    threading.Thread(target=_send, args=(subject, body, tg_msg), daemon=True).start()

            # Sleep briefly before next check
            _time.sleep(2)
        except Exception as e:
            app.logger.error("[DISBALANCE-ALERT] Loop error: %s", e, exc_info=True)
            _time.sleep(5)

threading.Thread(target=_disbalance_alert_loop, daemon=True, name="DisbalanceAlert").start()


def _log_event(session_id, account, event, detail=""):
    entry = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "session_id": session_id,
        "account": str(account) if account else "",
        "event": event,
        "detail": str(detail)[:300]
    }
    event_log.append(entry)
    if len(event_log) > EVENT_LOG_MAX:
        del event_log[:len(event_log) - EVENT_LOG_MAX]
    app.logger.info("Event: %s | %s | %s | %s", session_id[:8] if session_id else "", account, event, detail)
    # Send trade alerts if strategy has trade_alerts enabled
    _TRADE_ALERT_EVENTS = {"close_deal", "close_all_deals", "close_complete", "open_targets_reached",
                           "cycle_started", "hedge_rebalance", "session_created", "mode_changed"}
    if session_id and event in _TRADE_ALERT_EVENTS:
        _send_trade_alert(session_id, entry)

# ─── Shared cycle state machine helpers ─────────────────────────────────────
def _cycle_get_account(session, account):
    """Check if account is the cycling account for this session. Returns True if yes."""
    action = session.get("action", "")
    if not action.startswith("cycle_"):
        return False
    cyc_acc = session.get("cycle_account", "")
    if cyc_acc:
        return account == cyc_acc
    # Derive from sides
    sides = session.get("sides", {})
    accs = list(sides.keys())
    if len(accs) < 2:
        return False
    derived = accs[0] if action == "cycle_acc1" else accs[1]
    return account == derived


def _cycle_handle_close(session, account, data, session_id, cmd_sent_ts=None):
    """Handle a cycle close: transition close→open phase. Returns True if handled."""
    if not _cycle_get_account(session, account):
        return False

    ticket = data.get("ticket")
    progress = session.get("cycle_progress", {})

    # ── Deduplication guard: reject if this ticket's close was already processed ──
    # This prevents a duplicate close confirmation (from a retried close command) from
    # switching phase→open a second time and dispatching an extra open order.
    confirmed_set = progress.get("confirmed_closed_tickets", [])
    if ticket is not None and str(ticket) in confirmed_set:
        print(f"[CYCLE] DUPLICATE close REJECTED for {account} "
              f"(ticket={ticket} already processed — suppressing phase transition)")
        _log_event(session_id, account, "cycle_close_duplicate",
                   f"ticket={ticket} — duplicate close confirmation suppressed")
        return True  # Suppress: treat as handled so caller doesn't process it further

    progress["phase"] = "open"
    progress["cycle_close_ts"] = time.time()
    # Check if this is a Direct account (needs server to dispatch open) or EA (handles reopen natively)
    info = ea_account_info.get(account, {})
    is_direct = (info.get("direct_mode", False) or 
                 info.get("fix_account", False) or 
                 info.get("openapi_connected", False) or 
                 "direct" in info.get("conn_type", ""))

    if is_direct:
        # For Direct accounts, the server must dispatch the open command.
        # Clear open_dispatched to allow _should_issue_command to fire.
        progress.pop("open_dispatched", None)
    else:
        # EA handles reopen natively via cycle_reopen=True.
        # Pre-set open_dispatched to prevent server from dispatching a duplicate open.
        progress["open_dispatched"] = True

    progress.pop("open_fill_received", None)
    close_price_val = data.get("fill_price")
    if close_price_val is None or close_price_val == 0:
        close_price_val = data.get("close_price")
    if close_price_val is None or close_price_val == 0:
        close_price_val = data.get("price")
    if close_price_val is not None and float(close_price_val) > 0:
        progress["last_close_price"] = float(close_price_val)
    # Store the closed ticket so _cycle_handle_fill can replace the correct fill
    progress["closed_ticket"] = ticket
    session["cycle_progress"] = progress
    session.setdefault("last_trade_ts", {})[account] = time.time()
    session.setdefault("close_fills", []).append({
        "account": account,
        "ticket": ticket,
        "price": float(close_price_val) if close_price_val is not None else None,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ts_epoch": time.time(),
        "cmd_ts": cmd_sent_ts,
        "cycle": True,
    })
    # Record this ticket as confirmed-closed so any delayed duplicate close
    # confirmation (from a retried close command) is rejected by the guard above.
    if ticket is not None:
        confirmed_set = progress.setdefault("confirmed_closed_tickets", [])
        if str(ticket) not in confirmed_set:
            confirmed_set.append(str(ticket))
    session["cycle_progress"] = progress

    _log_event(session_id, account, "cycle_closed",
               f"ticket={ticket} price={close_price_val} — "
               f"fill #{progress.get('index', 0) + 1}, switching to open phase")
    print(f"[CYCLE] Closed ticket {ticket} on {account} "
          f"(fill #{progress.get('index', 0) + 1}), switching to open phase")
    _save_sessions()
    return True


def _cycle_handle_fill(session, account, data, cmd_sent_ts, session_id):
    """Handle a cycle reopen fill: transition open→close phase. Returns True if handled."""
    if not _cycle_get_account(session, account):
        return False

    progress = session.get("cycle_progress", {})
    if progress.get("phase") != "open":
        return False

    # Guard: reject duplicate fills for the same cycle open phase.
    # This prevents two concurrent open commands from both being recorded.
    if progress.get("open_fill_received"):
        print(f"[CYCLE] DUPLICATE fill rejected for {account} "
              f"(idx={progress.get('index', 0)}, ticket={data.get('ticket')})")
        return True  # Return True to prevent normal fill processing

    idx = progress.get("index", 0)
    ticket = data.get("ticket")
    fill_price = data.get("fill_price")
    quote_price = data.get("quote_price")
    spread = data.get("spread", 0)

    new_fill = {
        "account": account,
        "ticket": ticket,
        "price": float(fill_price) if fill_price is not None else None,
        "quote_price": float(quote_price) if quote_price is not None else None,
        "spread": int(spread) if spread else None,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ts_epoch": time.time(),
        "cmd_ts": cmd_sent_ts,
    }

    # Replace the closed fill with the new fill using the exact ticket stored by _cycle_handle_close
    closed_ticket = progress.pop("closed_ticket", None)
    replaced = False
    if closed_ticket is not None:
        for i, f in enumerate(session.get("fills", [])):
            if f.get("account") == account and str(f.get("ticket")) == str(closed_ticket):
                session["fills"][i] = new_fill
                replaced = True
                print(f"[CYCLE] Replaced fill ticket={closed_ticket} with new ticket={ticket} at fills[{i}]")
                break
    if not replaced:
        # Fallback: use active per-account fills by index
        closed_set = set(str(cf.get("ticket")) for cf in session.get("close_fills", []) if cf.get("account") == account)
        active_acct_fills = [f for f in session.get("fills", []) if f.get("account") == account and str(f.get("ticket")) not in closed_set]
        if idx < len(active_acct_fills):
            old_ticket = active_acct_fills[idx].get("ticket")
            for i, f in enumerate(session["fills"]):
                if f.get("account") == account and str(f.get("ticket")) == str(old_ticket):
                    session["fills"][i] = new_fill
                    print(f"[CYCLE] Fallback: replaced fill ticket={old_ticket} with new ticket={ticket}")
                    break
        else:
            # DO NOT append — growing fills list causes excess cycle iterations.
            # The position exists at the broker; log warning and continue.
            print(f"[CYCLE] WARNING: No fill to replace for ticket={ticket} "
                  f"(idx={idx}) — skipping append to prevent position count drift")

    # Mark this fill as received so duplicates are rejected
    progress["open_fill_received"] = True
    progress.pop("open_retries", None)  # Reset retry counter on success
    # Advance to next position
    progress["phase"] = "close"
    progress["index"] = idx + 1
    progress["cycled"] = progress.get("cycled", 0) + 1
    # Track spread cost
    reopen_price = float(fill_price) if fill_price is not None else None
    cycle_close_price = progress.pop("last_close_price", None)
    if reopen_price is not None and cycle_close_price is not None:
        spread_cost = abs(reopen_price - cycle_close_price)
        progress["total_spread_cost"] = progress.get("total_spread_cost", 0.0) + spread_cost
    session["cycle_progress"] = progress
    session.setdefault("last_trade_ts", {})[account] = time.time()

    _log_event(session_id, account, "cycle_reopened",
               f"ticket={ticket} price={fill_price} — replaced fill #{idx + 1}, "
               f"cycled {progress['cycled']} total")
    print(f"[CYCLE] Reopened on {account}: ticket={ticket} (fill #{idx + 1}), "
          f"cycled={progress['cycled']}")

    # Check if all positions have been cycled → auto-switch to monitor
    closed_set = set(str(cf.get("ticket")) for cf in session.get("close_fills", []) if cf.get("account") == account)
    acct_fill_count = len([f for f in session.get("fills", []) if f.get("account") == account and str(f.get("ticket")) not in closed_set])
    print(f"[CYCLE-COMPLETION-CHECK] acct={account}: idx={progress['index']} "
          f"active_fill_count={acct_fill_count} cycled={progress['cycled']}")
    target_cycles = progress.get("cycle_total", acct_fill_count)
    if progress.get("cycled", 0) >= target_cycles or progress["index"] >= acct_fill_count:
        session["action"] = "monitor"
        # Record cycle completion timestamp so hedge monitor stays suppressed
        # during the brief window where broker ticket data may still be stale
        session["cycle_complete_ts"] = time.time()
        avg_spread = 0
        total_sc = progress.get("total_spread_cost", 0)
        if progress["cycled"] > 0:
            avg_spread = total_sc / progress["cycled"]
        _log_event(session_id, account, "cycle_complete",
                   f"All {progress['cycled']} positions cycled — avg spread cost: {avg_spread:.5f} — switching to MONITOR")
        print(f"[CYCLE] Complete on {account}: {progress['cycled']} positions cycled, "
              f"avg spread cost={avg_spread:.5f}, auto-switching to MONITOR")

        # ── Post-cycle position count verification ──
        # Compare expected vs actual to detect duplicate opens or missed closes
        try:
            sides = session.get("sides", {})
            acct_list = list(sides.keys())
            expected_count = acct_fill_count  # Expected positions on the cycled side
            counts = {}
            for a in acct_list:
                ea_info = ea_account_info.get(a, {})
                counts[a] = ea_info.get("positions", -1)
            # Verify the cycled account matches expected
            actual = counts.get(account, -1)
            if actual >= 0 and actual != expected_count:
                mismatch_msg = (f"⚠ CYCLE POSITION MISMATCH on {account}: "
                                f"expected {expected_count} positions, broker reports {actual} "
                                f"(delta={actual - expected_count})")
                print(f"[CYCLE-VERIFY] {mismatch_msg}")
                _log_event(session_id, account, "cycle_mismatch", mismatch_msg)
                try:
                    _send_email(f"CYCLE MISMATCH: {account}", mismatch_msg, account_id=account)
                    _send_telegram(mismatch_msg, account_id=account)
                except Exception:
                    pass
            elif actual >= 0:
                print(f"[CYCLE-VERIFY] {account}: position count OK ({actual} == {expected_count})")
                _log_event(session_id, account, "cycle_verified",
                           f"Position count verified: {actual} positions match expected {expected_count}")
            # Also check cross-side balance
            other_accounts = [a for a in acct_list if a != account]
            if other_accounts:
                other = other_accounts[0]
                other_count = counts.get(other, -1)
                if actual >= 0 and other_count >= 0 and actual != other_count:
                    balance_msg = (f"⚠ HEDGE IMBALANCE after cycle: {account}={actual} vs "
                                   f"{other}={other_count} (delta={actual - other_count})")
                    print(f"[CYCLE-VERIFY] {balance_msg}")
                    _log_event(session_id, account, "cycle_imbalance", balance_msg)
                    try:
                        _send_email(f"HEDGE IMBALANCE: {account}", balance_msg, account_id=account)
                        _send_telegram(balance_msg, account_id=account)
                    except Exception:
                        pass
        except Exception as verify_err:
            print(f"[CYCLE-VERIFY] Error during verification: {verify_err}")
    _save_sessions()
    return True

# ─── Session helpers ────────────────────────────────────────────────────────
def _send_trade_alert(session_id, event_entry):
    """Send trade alert if the session's strategy has trade_alerts enabled."""
    with lock:
        session = sessions.get(session_id)
        if not session:
            return
        strat_id = session.get("strategy_id")
        if not strat_id:
            return
        strat = strategies.get(strat_id)
        if not strat or not strat.get("trade_alerts"):
            return
    strat_name = strat.get("name", strat_id[:8])
    ev = event_entry.get("event", "")
    detail = event_entry.get("detail", "")
    acct = event_entry.get("account", "")
    ts = event_entry.get("ts", "")
    subject = f"\U0001f4ca Trade Alert: {strat_name} — {ev}"
    body = (f"Strategy: {strat_name}\n"
            f"Event: {ev}\n"
            f"Account: {acct}\n"
            f"Detail: {detail}\n"
            f"Time: {ts}")
    tg_msg = (f"<b>\U0001f4ca Trade Alert</b>\n"
              f"Strategy: <b>{strat_name}</b>\n"
              f"Event: <code>{ev}</code>\n"
              f"Account: {acct}\n"
              f"Detail: {detail}\n"
              f"Time: {ts}")
    def _send():
        _send_email(subject, body, account_id=acct)
        _send_telegram(tg_msg, account_id=acct)
    threading.Thread(target=_send, daemon=True, name=f"TradeAlert-{ev}").start()
def _new_session(data):
    """Create a new session dict from request data."""
    sid = str(uuid.uuid4())
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Global defaults
    global_pair = data.get("pair", "EURUSD").strip()
    global_lot_size = float(data.get("lot_size", 0.01))

    sides = data.get("sides", {})
    accounts = list(sides.keys())

    global_max_spread = int(data.get("max_spread_points", 0))

    # Populate per-side pair, lot_size, and max_spread (default from global)
    for acc in accounts:
        side = sides[acc]
        if not side.get("pair") or not str(side["pair"]).strip():
            side["pair"] = global_pair
        else:
            side["pair"] = str(side["pair"]).strip()
        if side.get("lot_size") is not None and side["lot_size"] != "":
            side["lot_size"] = float(side["lot_size"])
        else:
            side["lot_size"] = global_lot_size
        if side.get("max_spread") is not None and side["max_spread"] != "":
            side["max_spread"] = int(side["max_spread"])
        else:
            side["max_spread"] = global_max_spread
        # Store per-side comment
        if "comment" not in side:
            side["comment"] = ""



    # Parse max_errors, trade_pause, diff, and accum limits
    max_errors_val = data.get("max_errors", 1)
    trade_pause_val = data.get("trade_pause", 0)
    diff_to_open_val = data.get("diff_to_open", None)
    diff_to_close_val = data.get("diff_to_close", 0)
    max_accum_lots_val = data.get("max_accum_lots", 0)
    max_accum_deals_val = data.get("max_accum_deals", 0)

    session = {
        "id": sid,
        "strategy_id": data.get("strategy_id"),
        "pair": global_pair,
        "lot_size": global_lot_size,
        "total_positions": int(data.get("total_positions", 1)),
        "max_spread_points": global_max_spread,

        "max_errors": int(max_errors_val) if max_errors_val else 1,
        "trade_pause": float(trade_pause_val) if trade_pause_val else 0.0,
        "diff_to_open": int(diff_to_open_val) if diff_to_open_val is not None and diff_to_open_val != "" else None,
        "diff_to_close": int(diff_to_close_val) if diff_to_close_val else 0,
        "max_accum_lots": float(max_accum_lots_val) if max_accum_lots_val else 0.0,
        "max_accum_deals": int(max_accum_deals_val) if max_accum_deals_val else 0,
        "comment": data.get("comment", ""),
        "execution_order": data.get("execution_order", "simultaneous"),
        "sides": sides,
        "status": "draft",
        "filled": {a: 0 for a in accounts},
        "closed": {a: 0 for a in accounts},
        "close_count": data.get("close_count"),
        "action": data.get("action", "monitor"),
        "created_at": now,
        "updated_at": now,
        "errors": {a: [] for a in accounts},
        "spread_rejects": {a: 0 for a in accounts},
        "rollback_needed": {},  # account -> number of positions to rollback-close
        "last_trade_ts": {},    # account -> timestamp of last trade (for trade_pause)
        # Execution filters (configurable via ⚙ Filters popup)
        "max_ticks_per_5s": int(data.get("max_ticks_per_5s", 0)),
        "max_price_jump": float(data.get("max_price_jump", 0)),
        "require_diff_skew_open": data.get("require_diff_skew_open", ""),
        "require_diff_skew_close": data.get("require_diff_skew_close", ""),
        "avoid_news": bool(data.get("avoid_news", False)),
    }

    # Auto-detect netting-mode accounts (e.g. Dukascopy FIX) and force lot-volume
    # matching. Netting brokers have one aggregate position per symbol — per-ticket
    # matching is meaningless for them.
    _any_netting_at_create = any(
        ea_account_info.get(a, {}).get("netting_mode", False)
        for a in accounts
    )
    if _any_netting_at_create:
        session["match_mode"] = "lots"
        app.logger.info("[SESSION] Netting-mode account detected — auto-set match_mode=lots")

    # Auto-generate comment if not provided
    if not session["comment"] and len(accounts) == 2:
        # Try to build from per-side comments if they exist
        side_comments = [sides[a].get("comment", "") for a in accounts]
        if any(side_comments):
            session["comment"] = " / ".join(c if c else a for c, a in zip(side_comments, accounts))
        else:
            session["comment"] = f"{accounts[0]}-{accounts[1]}"

    return session

# ─── Universal Hedge Monitor ────────────────────────────────────────────────
# Single implementation for ALL account types (EA poll, MT Direct, FIX).
# Reads open_tickets from ea_account_info (populated by all connectors).
# Runs in a background thread — no longer tied to any specific poll endpoint.

_hedge_monitor_last_run = [0.0]  # mutable container for closure

def _get_pos_count(pos_val):
    if pos_val is None:
        return 0
    if isinstance(pos_val, (dict, list)):
        return len(pos_val)
    if isinstance(pos_val, (int, float)):
        return int(pos_val)
    return 0

def _has_position_changed(prev_pos, new_pos):
    if prev_pos is None or new_pos is None:
        return False
    if _get_pos_count(prev_pos) != _get_pos_count(new_pos):
        return True
    if isinstance(prev_pos, dict) and isinstance(new_pos, dict):
        if set(prev_pos.keys()) != set(new_pos.keys()):
            return True
        # Check for MT5 partial closes where ticket remains the same but lots decrease
        for k in prev_pos:
            if isinstance(prev_pos[k], dict) and isinstance(new_pos[k], dict):
                if prev_pos[k].get("lots") != new_pos[k].get("lots"):
                    return True
        return False
    if isinstance(prev_pos, list) and isinstance(new_pos, list):
        try:
            def _extract(lst):
                res = []
                for x in lst:
                    if isinstance(x, dict) and "ticket" in x:
                        res.append(str(x["ticket"]))
                    else:
                        res.append(str(x))
                return set(res)
            return _extract(prev_pos) != _extract(new_pos)
        except Exception:
            pass
    return False

def _check_fee_alerts():
    """Universal fee detector: monitors all accounts in ea_account_info for balance drops."""
    try:
        now_ts = time.time()
        for account, info in list(ea_account_info.items()):
            new_bal = info.get("balance")
            if new_bal is None:
                continue
            
            # ── 1. Always track position changes independently of balance drops ──
            prev_pos = _last_known_positions.get(account)
            new_pos = info.get("positions")
            if _has_position_changed(prev_pos, new_pos):
                _last_pos_change_ts[account] = now_ts
                # We do NOT update _last_known_positions here yet, we wait until the end of the loop
                # so the rest of the logic can still compare prev_pos and new_pos if needed.

            prev_bal = _last_known_balances.get(account)
            if prev_bal is not None:
                delta = new_bal - prev_bal
                if delta < -0.001:  # balance decreased
                    
                    # ── 2. Check if a position changed recently (within 60s) ──
                    # This handles cases where the position changed BEFORE the balance dropped
                    # (broker latency) or AT THE SAME TIME.
                    last_change = _last_pos_change_ts.get(account, 0)
                    if (now_ts - last_change) < 60:
                        _last_known_balances[account] = new_bal
                        if new_pos is not None:
                            _last_known_positions[account] = new_pos
                        _pending_fee_alerts.pop(account, None)
                        continue

                    # For raw FIX accounts (e.g. Dukascopy) that have no OpenAPI companion,
                    # info["balance"] == equity, which fluctuates with unrealized P&L when
                    # positions are open. Suppress fee detection to avoid false positives.
                    if (info.get("fix_account") and not info.get("openapi_connected")
                            and (info.get("positions") or 0) > 0):
                        _last_known_balances[account] = new_bal
                        if new_pos is not None:
                            _last_known_positions[account] = new_pos
                        _pending_fee_alerts.pop(account, None)
                        continue

                    # ── 3. WAIT FOR CONFIRMATION ──
                    # Prevent race condition where MT4 updates balance BEFORE positions
                    pending = _pending_fee_alerts.get(account)
                    if not pending:
                        _pending_fee_alerts[account] = {
                            "ts": now_ts,
                            "prev_bal": prev_bal,
                            "new_bal": new_bal
                        }
                        # Do NOT update _last_known_balances yet, wait for positions to catch up
                        continue
                    elif (now_ts - pending["ts"]) < 15:
                        # Still in the 15-second waiting window
                        continue
                    else:
                        # 15 seconds passed, no position change arrived. It's a genuine fee.
                        _pending_fee_alerts.pop(account)
                        prev_bal = pending["prev_bal"]
                        delta = new_bal - prev_bal
                    
                    
                    # Cooldown check: only record if no close was just processed in sessions (cooldown 60s from last trade)
                    last_trade = 0
                    for _sid, _sess in list(sessions.items()):
                        if account in _sess.get("sides", {}):
                            lt = _sess.get("last_trade_ts", {}).get(account, 0)
                            if lt > last_trade:
                                last_trade = lt
                    if (now_ts - last_trade) > 60:  # no recent trade — likely a fee
                        fee_entry = {
                            "id": str(uuid.uuid4())[:8],
                            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "ts_epoch": now_ts,
                            "account": account,
                            "amount": round(delta, 2),
                            "balance_before": round(prev_bal, 2),
                            "balance_after": round(new_bal, 2),
                            "label": "auto-detected",
                        }
                        reporting_data["fees"].append(fee_entry)
                        _save_reporting()
                        app.logger.info("[FEE] Detected balance drop on %s: %.2f (%.2f -> %.2f)",
                                        account, delta, prev_bal, new_bal)
                        _send_fee_alert(account, fee_entry)
                        # Log event for frontend speech notification
                        for _sid, _sess in list(sessions.items()):
                            if account in _sess.get("sides", {}):
                                _log_event(_sid, account, "fee_detected",
                                           f"Fee {delta:.2f} on {account} ({prev_bal:.2f} -> {new_bal:.2f})")
                                break
                else:
                    # Balance did not decrease, or went up
                    _last_known_balances[account] = new_bal
                    if info.get("positions") is not None:
                        _last_known_positions[account] = info.get("positions")
                    _pending_fee_alerts.pop(account, None)
                    
            _last_known_balances[account] = new_bal
            if info.get("positions") is not None:
                _last_known_positions[account] = info.get("positions")
    except Exception as e:
        app.logger.error("Error in universal fee detection: %s", e, exc_info=True)


def _run_hedge_monitor_all():
    """Universal hedge monitor: detect externally closed positions and queue
    rollback closes on the paired account. Runs for ALL sessions/accounts
    regardless of connector type, using ea_account_info as the data source."""
    now_ts = time.time()
    # Throttle: only check every 3 seconds
    if (now_ts - _hedge_monitor_last_run[0]) < 3:
        return
    _hedge_monitor_last_run[0] = now_ts

    with lock:
        _check_fee_alerts()
        for sid, session in list(sessions.items()):
            if session.get("status") not in ("active", "partial_close"):
                continue

            # ── Skip if parent strategy is NOT running or NOT enabled ──
            _hm_strat_id = session.get("strategy_id")
            if _hm_strat_id:
                _hm_strat = strategies.get(_hm_strat_id)
                if _hm_strat and (not _hm_strat.get("running", False) or not _hm_strat.get("enabled", True)):
                    continue  # Strategy stopped or disabled — don't run hedge monitor

            # Note: imported sessions are NOT skipped — they need hedge monitoring too.
            # _normalize_ticket handles any ticket format differences.

            # ── Auto-correct stale partial_close ──
            if session.get("status") == "partial_close":
                sides = session.get("sides", {})
                closed_counts = [session.get("closed", {}).get(a, 0) for a in sides]
                rb_pending = any(session.get("rollback_needed", {}).get(a, 0) > 0 for a in sides)
                if len(set(closed_counts)) <= 1 and not rb_pending:
                    session["status"] = "active"
                    print(f"[AUTO-FIX] sid={sid[:8]}: partial_close auto-corrected to active "
                          f"(close counts balanced: {closed_counts})")
                    _save_sessions()

            # Skip during close phase — the normal close flow handles this
            sess_action = session.get("action", "")
            if sess_action == "close":
                continue
            # Cycle mode: skip hedge monitor entirely — cycle deliberately closes/reopens
            if sess_action.startswith("cycle_"):
                continue
            # Post-cycle cooldown: skip hedge monitor for 30s after cycle completes.
            # The cycle close/reopen replaces tickets, and the broker's position list
            # may briefly lag behind close_fills — causing false "missing ticket" detections.
            cycle_done_ts = session.get("cycle_complete_ts", 0)
            if cycle_done_ts > 0 and (now_ts - cycle_done_ts) < 30:
                continue

            # STARTUP COOLDOWN: Skip hedge monitor for the first 30s after session start
            hedge_start = session.get("hedge_monitor_start_ts", 0)
            if hedge_start > 0 and (now_ts - hedge_start) < 30:
                # Imported sessions skip this cooldown — positions already exist
                if not session.get("imported"):
                    continue

            sides = session.get("sides", {})
            if len(sides) < 2:
                continue

            # Only run after ALL sides have reached fill targets
            if sess_action == "open":
                all_sides_filled = all(
                    (session["filled"].get(acc, 0) - session["closed"].get(acc, 0)) >= session["total_positions"]
                    for acc in sides
                )
                if not all_sides_filled:
                    continue

            # COOLDOWN: Skip if any account had a recent trade (within 5s)
            cooldown_secs = 5
            last_ts_dict = session.get("last_trade_ts", {})
            most_recent_trade = max(last_ts_dict.values()) if last_ts_dict else 0
            if most_recent_trade > 0 and (now_ts - most_recent_trade) < cooldown_secs:
                continue

            # Skip if there are pending rollback closes for ANY side
            if any(session.get("rollback_needed", {}).get(a, 0) > 0 for a in sides):
                continue

            # ── IMBALANCE REBALANCE: close excess positions on the higher side ──
            # ONLY for structural imbalance (e.g., unbalanced import 22/43 where
            # fill counts differ). Transient imbalance from manual closes is handled
            # by the hedge monitor's ticket-level detection below — don't double-close.
            # SKIP for sessions that include a netting-mode account (e.g. Dukascopy):
            # netting brokers collapse all increments into one position so per-fill
            # counts cannot be compared to broker position counts.
            _any_netting = any(
                ea_account_info.get(a, {}).get("netting_mode", False)
                for a in sides
            )
            if sess_action == "monitor" and not _any_netting:
                accs = list(sides.keys())
                if len(accs) >= 2:
                    # Only run if fill counts differ (structural imbalance)
                    fill_count_1 = session.get("filled", {}).get(accs[0], 0) - session.get("closed", {}).get(accs[0], 0)
                    fill_count_2 = session.get("filled", {}).get(accs[1], 0) - session.get("closed", {}).get(accs[1], 0)
                    # Skip imbalance check if a close_deal is still in-flight
                    # (one side may close before the other, creating a transient imbalance)
                    close_deal_ts = session.get("close_deal_ts", 0)
                    if close_deal_ts and (now_ts - close_deal_ts) < 10:
                        pass  # Suppress — close_deal pair still settling
                    elif fill_count_1 != fill_count_2:
                        # Determine which side has excess from SESSION fill counts
                        # (not broker-level totals which include other symbols/sessions)
                        if fill_count_1 > fill_count_2:
                            max_acc, min_acc = accs[0], accs[1]
                            excess = fill_count_1 - fill_count_2
                        else:
                            max_acc, min_acc = accs[1], accs[0]
                            excess = fill_count_2 - fill_count_1

                        # Verify broker data is fresh for the excess side
                        max_info = ea_account_info.get(max_acc, {})
                        last_upd = max_info.get("last_update", 0)
                        if last_upd > 0 and (now_ts - last_upd) > 30:
                            pass  # Data too stale — skip rebalance this cycle
                        else:
                            # Get broker open tickets for verification
                            ea_open_tickets = set(
                                _normalize_ticket(t) for t in max_info.get("open_tickets", [])
                            )
                            # Find session fills on the excess side that are still open
                            close_tickets_set = set(
                                _normalize_ticket(f["ticket"])
                                for f in session.get("close_fills", [])
                                if f.get("account") == max_acc
                            )
                            open_session_fills = [
                                f for f in session.get("fills", [])
                                if f.get("account") == max_acc
                                and _normalize_ticket(f["ticket"]) not in close_tickets_set
                                and (not ea_open_tickets  # if no ticket data, trust session
                                     or _normalize_ticket(f["ticket"]) in ea_open_tickets)
                            ]
                            # Determine WHICH tickets to close based on match_mode
                            match_mode = session.get("match_mode", "ticket")
                            if match_mode == "ticket":
                                # Ticket-by-ticket matching: fills are paired by index
                                # Orphaned fills are those with no counterpart on the other side
                                min_close_set = set(
                                    _normalize_ticket(f["ticket"])
                                    for f in session.get("close_fills", [])
                                    if f.get("account") == min_acc
                                )
                                min_open_fills = [
                                    f for f in session.get("fills", [])
                                    if f.get("account") == min_acc
                                    and _normalize_ticket(f["ticket"]) not in min_close_set
                                ]
                                paired_count = len(min_open_fills)
                                # Fills at indices >= paired_count are orphaned (no pair on the other side)
                                orphaned = open_session_fills[paired_count:]
                                tickets_to_close = [
                                    _normalize_ticket(f["ticket"])
                                    for f in orphaned[:excess]
                                ]
                                print(f"[HEDGE-REBAL] ticket-match: paired={paired_count} orphaned={len(orphaned)} "
                                      f"closing={[f.get('ticket') for f in orphaned[:excess]]}")
                            else:
                                # Gross/lots matching: close the oldest fills (from the start)
                                tickets_to_close = [
                                    _normalize_ticket(f["ticket"])
                                    for f in open_session_fills[:excess]
                                ]
                                print(f"[HEDGE-REBAL] gross-match: closing oldest {excess} fills")
                            if tickets_to_close:
                                rb = session.setdefault("rollback_needed", {})
                                rb[max_acc] = rb.get(max_acc, 0) + len(tickets_to_close)
                                rb_tickets = session.setdefault("rollback_tickets", {})
                                rb_tickets.setdefault(max_acc, []).extend(tickets_to_close)
                                rebal_delay = dashboard_settings.get("rebalance_close_delay", 1)
                                # Set cooldown to block ticket-level detection from
                                # misinterpreting this intentional close as "external"
                                session["imbalance_rebal_ts"] = now_ts
                                print(f"[HEDGE-REBAL] IMBALANCE: {max_acc} has {excess} excess "
                                      f"(session fills {fill_count_1 if max_acc == accs[0] else fill_count_2} "
                                      f"vs {fill_count_2 if max_acc == accs[0] else fill_count_1}) — "
                                      f"queuing {len(tickets_to_close)} close(s) on {max_acc} "
                                      f"(delay={rebal_delay}s)")
                                _log_event(sid, max_acc, "hedge_rebalance",
                                           f"Structural imbalance: {fill_count_1} vs {fill_count_2} fills. "
                                           f"Queuing {len(tickets_to_close)} close(s) to rebalance "
                                           f"(excess={excess}, delay={rebal_delay}s)")
                                _save_sessions()
                                continue  # Skip normal hedge monitor for this iteration
                    else:
                        # Fills balanced — clear imbalance cooldown so ticket
                        # detection resumes for genuine external closes
                        session.pop("imbalance_rebal_ts", None)

            # ── Check each account using ea_account_info ──
            for account in sides:
                # Get open tickets from ea_account_info (populated by EA poll, MT Direct, FIX)
                info = ea_account_info.get(account, {})

                # NETTING-MODE SKIP: Dukascopy and similar netting brokers collapse all
                # fills for a symbol into a single aggregate position. Their open_tickets
                # list contains one synthetic entry per symbol regardless of how many
                # incremental fills were placed, so per-ticket comparison is meaningless
                # and will always produce false "missing" detections. Skip entirely —
                # the position is monitored at the lot/session level instead.
                if info.get("netting_mode", False):
                    mismatch_key = f"hedge_mismatch_{sid}_{account}"
                    session.pop(mismatch_key, None)  # clear any stale counter
                    continue

                ea_open_tickets_list = info.get("open_tickets")

                # If no data available for this account, skip
                if ea_open_tickets_list is None:
                    # If EA reports positions=0 explicitly, treat as empty set
                    if info.get("positions") == 0:
                        ea_open_tickets = set()
                    else:
                        continue
                else:
                    ea_open_tickets = set(_normalize_ticket(t) for t in ea_open_tickets_list)

                # Check staleness: only use data updated within last 30s
                last_update = info.get("last_update", 0)
                if last_update > 0 and (now_ts - last_update) > 30:
                    continue  # Data too stale

                # Step 1: Count — get tickets this account should have open
                acct_fill_tickets = [_normalize_ticket(f["ticket"]) for f in session.get("fills", [])
                                     if f.get("account") == account]
                acct_close_tickets = set(_normalize_ticket(f["ticket"]) for f in session.get("close_fills", [])
                                         if f.get("account") == account)
                # Also exclude tickets pending rollback close — broker may have
                # closed them before close_fills is recorded.
                pending_rb_tickets = set(
                    _normalize_ticket(t)
                    for t in session.get("rollback_tickets", {}).get(account, [])
                )
                expected_open = set(acct_fill_tickets) - acct_close_tickets - pending_rb_tickets

                if not expected_open:
                    continue  # No fills tracked, can't compare

                # Step 2: Detect — compare expected vs actual
                missing_tickets = expected_open - ea_open_tickets
                if not missing_tickets:
                    # Reset mismatch counter — everything matches
                    mismatch_key = f"hedge_mismatch_{sid}_{account}"
                    session.pop(mismatch_key, None)
                    continue

                # Log every detection
                print(f"[HEDGE-MON] acct={account} sid={sid[:8]}: "
                      f"expected={len(expected_open)} ea_has={len(ea_open_tickets)} "
                      f"missing={len(missing_tickets)}")

                # Require 3 consecutive detections to avoid glitches
                mismatch_key = f"hedge_mismatch_{sid}_{account}"
                prev_count = session.get(mismatch_key, 0)
                if prev_count < 2:
                    session[mismatch_key] = prev_count + 1
                    print(f"[HEDGE-REBAL] acct={account} sid={sid[:8]}: "
                          f"detected {len(missing_tickets)} missing ticket(s) "
                          f"({prev_count + 1}/3 consecutive), waiting...")
                    continue
                # Clear counter — taking action
                session.pop(mismatch_key, None)

                # ── SAFETY GUARD: block cascade on suspicious all-missing pattern ──
                # If ALL expected tickets are missing but the broker still shows >= as
                # many positions as expected, this is almost certainly a ticket ID
                # mismatch (shared account data race, EA poll overwriting MT Direct data)
                # rather than genuine external closes. Genuine mass-closes show
                # ea_has < expected (positions actually disappeared from the broker).
                # Seen in incident 2026-05-28: ALEX-ICM-7415899 ea_has=200, expected=100,
                # missing=100 → caused false cascade of 100 ALEX-YCM closes.
                if len(missing_tickets) == len(expected_open) and len(ea_open_tickets) >= len(expected_open):
                    app.logger.warning(
                        "[HEDGE-MON] CASCADE BLOCKED for %s sid=%s: ALL %d tickets missing "
                        "but broker has %d positions (>= expected=%d). "
                        "Likely ticket ID mismatch or data race — NOT cascading. "
                        "Manual verification required.",
                        account, sid[:8], len(missing_tickets), len(ea_open_tickets), len(expected_open)
                    )
                    try:
                        _send_telegram(
                            f"\u26a0\ufe0f <b>HEDGE CASCADE BLOCKED</b>: {account}\n"
                            f"ALL {len(missing_tickets)} session tickets missing but broker "
                            f"has {len(ea_open_tickets)} positions (\u2265 expected {len(expected_open)}).\n"
                            f"Possible ticket ID mismatch \u2014 NOT auto-closing.\n"
                            f"sid={sid[:8]} \u2014 manual check required.",
                            account_id=account
                        )
                    except Exception:
                        pass
                    continue

                print(f"[HEDGE-REBAL] acct={account} sid={sid[:8]}: "
                      f"CONFIRMED {len(missing_tickets)} externally closed ticket(s): {missing_tickets}")

                # *** CRITICAL: Record externally closed tickets in close_fills ***
                for missing_t in missing_tickets:
                    session.setdefault("close_fills", []).append({
                        "account": account,
                        "ticket": missing_t,
                        "price": None,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "ts_epoch": time.time(),
                        "external": True,
                    })
                    session["closed"][account] = session["closed"].get(account, 0) + 1
                    # Track closed lots for lot-mode
                    fill_lots = next((f.get("lots", 0) for f in session.get("fills", [])
                                      if f.get("account") == account and _normalize_ticket(f.get("ticket")) == missing_t), 0)
                    _update_closed_lots(session, account, fill_lots)

                other_accounts = [a for a in sides if a != account]
                tickets_to_close = []

                # Rebuild close tickets set now that we've added the external closes
                all_close_tickets = set(_normalize_ticket(f["ticket"]) for f in session.get("close_fills", []))

                # Step 3: Pair each missing ticket to a close target on the other side.
                # Non-netting accounts: match by pair_index (or positional fallback when
                #   pair_index is None — .get("pair_index", idx) doesn't work because the
                #   key exists with value None; use explicit coercion instead).
                # Netting-mode accounts (e.g. Dukascopy): skip per-ticket pairing entirely.
                #   Volume matching is used instead — the fallback below closes the oldest
                #   fill on the netting side for every missing ticket on the non-netting side.
                #   This correctly reduces the aggregate netting position by the right lot size.
                other_pair_maps = {}
                for other_acc in other_accounts:
                    if ea_account_info.get(other_acc, {}).get("netting_mode", False):
                        continue  # netting accounts handled by fallback below
                    other_acct_fills = [f for f in session.get("fills", [])
                                        if f.get("account") == other_acc]
                    other_pair_maps[other_acc] = {
                        (f.get("pair_index") if f.get("pair_index") is not None else i): f
                        for i, f in enumerate(other_acct_fills)
                    }

                acct_fills_ordered = [f for f in session.get("fills", [])
                                      if f.get("account") == account]
                unpaired_excess = 0  # fills with no counterpart on ANY other side
                for missing_t in missing_tickets:
                    pi_found = None
                    for idx, f in enumerate(acct_fills_ordered):
                        if _normalize_ticket(f.get("ticket")) == missing_t:
                            pi = f.get("pair_index")
                            pi_found = pi if pi is not None else idx
                            break

                    if pi_found is None:
                        # Fill not found in session — treat as unmatched
                        unpaired_excess += 1
                        print(f"[HEDGE-REBAL] Closed ticket {missing_t} on {account}: "
                              f"not found in session fills — skipping (untracked)")
                        continue

                    # Check non-netting accounts via pair_index/positional lookup
                    non_netting_accs = [a for a in other_accounts
                                        if not ea_account_info.get(a, {}).get("netting_mode", False)]
                    netting_accs = [a for a in other_accounts
                                    if ea_account_info.get(a, {}).get("netting_mode", False)]

                    found_pair = False
                    for other_acc in non_netting_accs:
                        paired_fill = other_pair_maps.get(other_acc, {}).get(pi_found)
                        if paired_fill is None:
                            unpaired_excess += 1
                            print(f"[HEDGE-REBAL] Closed ticket {missing_t} on {account} "
                                  f"(idx={pi_found}) has NO counterpart "
                                  f"on {other_acc} — skipping (unpaired excess)")
                        else:
                            already_queued = set(t for _, t in tickets_to_close)
                            if (_normalize_ticket(paired_fill["ticket"]) not in all_close_tickets
                                    and _normalize_ticket(paired_fill["ticket"]) not in already_queued):
                                tickets_to_close.append((other_acc, _normalize_ticket(paired_fill["ticket"])))
                                found_pair = True
                                print(f"[HEDGE-REBAL] Paired: closed {missing_t} on {account} "
                                      f"→ will close {paired_fill['ticket']} on {other_acc} (idx={pi_found})")

                    # Netting accounts: volume fallback will close oldest increment.
                    # Log intent but let Step 4 below handle the actual queuing.
                    for other_acc in netting_accs:
                        print(f"[HEDGE-REBAL] Netting close needed: {missing_t} on {account} "
                              f"→ will close 1 lot increment on {other_acc} (volume-match via fallback)")

                # Step 4: Fallback — for any paired missing ticket where direct lookup
                # didn't produce a close target (netting accounts, or non-netting with
                # no matching pair_index), close the oldest open fill on the other side.
                # Unpaired excess fills (no counterpart exists at all) are excluded.
                paired_missing = len(missing_tickets) - unpaired_excess
                if len(tickets_to_close) < paired_missing:
                    shortfall = paired_missing - len(tickets_to_close)
                    already_queued = set(t for _, t in tickets_to_close)
                    for other_acc in other_accounts:
                        other_fills = [f for f in session.get("fills", [])
                                       if f.get("account") == other_acc
                                       and _normalize_ticket(f["ticket"]) not in all_close_tickets
                                       and _normalize_ticket(f["ticket"]) not in already_queued]
                        other_fills.sort(key=lambda x: x.get("ts_epoch", 0))
                        for f in other_fills[:shortfall]:
                            tickets_to_close.append((other_acc, _normalize_ticket(f["ticket"])))
                            print(f"[HEDGE-REBAL] Fallback: will close oldest ticket {f['ticket']} on {other_acc}")

                # Queue the paired closes via rollback mechanism with specific tickets
                for other_acc, ticket in tickets_to_close:
                    rb = session.setdefault("rollback_needed", {})
                    rb[other_acc] = rb.get(other_acc, 0) + 1
                    rb_tickets = session.setdefault("rollback_tickets", {})
                    rb_tickets.setdefault(other_acc, []).append(ticket)

                _log_event(sid, account, "hedge_rebalance",
                           f"Detected {len(missing_tickets)} externally closed position(s). "
                           f"Missing tickets: {missing_tickets}. "
                           f"Queued {len(tickets_to_close)} close(s) on other side(s).")
                _save_sessions()
                _check_session_completion(session)


def _start_hedge_monitor_thread():
    """Start the background thread for the universal hedge monitor."""
    def _loop():
        while True:
            try:
                _run_hedge_monitor_all()
            except Exception as e:
                app.logger.error("Hedge monitor error: %s", e, exc_info=True)
            time.sleep(1)  # Check every second (function self-throttles to 3s)
    t = threading.Thread(target=_loop, daemon=True, name="HedgeMonitor")
    t.start()
    app.logger.info("Universal hedge monitor thread started")


def _is_within_time_window(session):
    """Check if current time is within the strategy's trade_start_time-trade_stop_time window."""
    try:
        strat_id = session.get("strategy_id")
        if not strat_id:
            return True  # No strategy, allow
        strat = strategies.get(strat_id)
        if not strat:
            return True
        start_str = strat.get("trade_start_time", "00:00")
        stop_str = strat.get("trade_stop_time", "23:59")
        now = datetime.now().time()
        start = datetime.strptime(start_str, "%H:%M").time()
        stop = datetime.strptime(stop_str, "%H:%M").time()

        if start <= stop:
            return start <= now <= stop
        else:
            # Overnight window (e.g., 22:00 - 06:00)
            return now >= start or now <= stop
    except Exception:
        return True  # If time parsing fails, allow

def _should_issue_command(session, account):
    """
    Determine if a command should be issued to this account based on
    session status, fill counts, execution order, time window, and per-account limits.
    Returns: True (normal command), False (skip), or "rollback" (issue close command for rollback).
    """
    # HARD BLOCK: paused/draft sessions must never issue commands
    if session.get("status") not in ("active", "partial_close"):
        return False

    # Rollback takes priority — but only if parent strategy is running
    rollback = session.get("rollback_needed", {})
    if rollback.get(account, 0) > 0:
        # ── Timeout: if rollback has been pending too long, clear and let hedge monitor re-detect ──
        rb_start = session.get("rollback_start_ts", {}).get(account, 0)
        if rb_start and (time.time() - rb_start) > 30:
            # Clear this rollback
            rb_tickets = session.get("rollback_tickets", {}).get(account, [])
            failed_ticket = rb_tickets[0] if rb_tickets else None
            rollback[account] = max(0, rollback.get(account, 0) - 1)
            session["rollback_needed"] = rollback
            if rb_tickets:
                rb_tickets.pop(0)
                if not rb_tickets:
                    session.get("rollback_tickets", {}).pop(account, None)
            session.get("rollback_start_ts", {}).pop(account, None)

            # Check if broker actually closed the position (close succeeded
            # but report_result may have failed to record it).
            if failed_ticket is not None:
                ea_info = ea_account_info.get(account, {})
                ea_open = set(
                    _normalize_ticket(t) for t in ea_info.get("open_tickets", [])
                )
                if _normalize_ticket(failed_ticket) not in ea_open:
                    # Position IS gone from broker — record close_fill to prevent
                    # hedge monitor from detecting it as "externally closed"
                    session["closed"][account] = session["closed"].get(account, 0) + 1
                    session.setdefault("close_fills", []).append({
                        "account": account,
                        "ticket": _normalize_ticket(failed_ticket),
                        "price": None,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "ts_epoch": time.time(),
                        "cmd_ts": None,
                        "timeout_recovery": True,
                    })
                    print(f"[ROLLBACK-TIMEOUT] acct={account}: ticket={failed_ticket} "
                          f"confirmed CLOSED on broker — recorded close_fill")
                else:
                    print(f"[ROLLBACK-TIMEOUT] acct={account}: ticket={failed_ticket} "
                          f"still OPEN on broker — genuine timeout")
            else:
                print(f"[ROLLBACK-TIMEOUT] acct={account}: rollback timed out after 30s "
                      f"(no ticket). Remaining={rollback.get(account, 0)}")
            if rollback.get(account, 0) > 0:
                # Still more rollbacks — apply rebalance_close_delay before next one
                rebal_delay = dashboard_settings.get("rebalance_close_delay", 1)
                if rebal_delay > 0:
                    session.setdefault("rollback_start_ts", {})[account] = time.time() + rebal_delay
                else:
                    session.setdefault("rollback_start_ts", {})[account] = time.time()
                return "rollback"
            return False  # No more rollbacks

        # PROMPT GATE: if prompt_on_rollbacks is enabled, pause and await UI confirmation
        if dashboard_settings.get("prompt_on_rollbacks"):
            key = (session.get("id", ""), account)
            confirmed = _rollback_pending_confirmations.get(key)  # True/False/None
            if confirmed is True:
                # User approved — clear the flag and proceed
                _rollback_pending_confirmations.pop(key, None)
            elif confirmed is False:
                # User denied — clear rollback entirely
                _rollback_pending_confirmations.pop(key, None)
                rollback[account] = 0
                session["rollback_needed"] = rollback
                session.get("rollback_tickets", {}).pop(account, None)
                app.logger.info("[ROLLBACK-PROMPT] Rollback DENIED by user for %s sid=%s",
                                account, session.get('id', '')[:8])
                return False
            else:
                # Not yet answered — register as pending and skip this cycle
                _rollback_pending_confirmations[key] = None
                return False  # Hold — command loop will retry next cycle

        # ── OTHER-SIDE CONNECTIVITY GUARD ────────────────────────────────────
        # Before executing a rollback on `account`, verify that at least ONE
        # of the other-side accounts has FRESH broker data (≤30s old).
        # Rationale: rollback_needed is persisted to disk. If the dashboard
        # restarts and the account that TRIGGERED the rollback was not set to
        # auto-connect (or hasn't connected yet), its ea_account_info is stale
        # or absent. In that case the rollback was queued on bad/old data and
        # must NOT fire until the triggering account reconnects and confirms
        # the imbalance is still real.
        # Seen in incident 2026-05-28: ALEX-ICM-7415899 (no auto-connect) →
        # stale data → persisted rollback fired on ALEX-YCM at restart.
        sides = session.get("sides", {})
        other_sides = [a for a in sides if a != account]
        now_for_check = time.time()
        any_other_fresh = False
        for other_acc in other_sides:
            other_info = ea_account_info.get(other_acc, {})
            other_last_update = other_info.get("last_update", 0)
            if other_last_update > 0 and (now_for_check - other_last_update) <= 30:
                any_other_fresh = True
                break
        if not any_other_fresh:
            # All other sides are disconnected or have stale data.
            # Suppress rollback and log — will retry next cycle once they connect.
            _rb_hold_key = f"_rb_connectivity_hold_{account}"
            _rb_hold_count = session.get(_rb_hold_key, 0)
            if _rb_hold_count == 0 or _rb_hold_count % 30 == 0:
                # Log and alert every 30 cycles (~30s) to avoid spam
                _hold_msg = (
                    f"[ROLLBACK-HOLD] acct={account} sid={session.get('id','')[:8]}: "
                    f"rollback_needed={rollback.get(account,0)} but ALL other sides "
                    f"{other_sides} have NO fresh broker data — holding until they connect. "
                    f"(count={_rb_hold_count})"
                )
                print(_hold_msg)
                app.logger.warning(_hold_msg)
                if _rb_hold_count == 0:
                    try:
                        _send_telegram(
                            f"\u26a0\ufe0f <b>ROLLBACK HELD</b>: {account}\n"
                            f"Rollback queued ({rollback.get(account,0)} positions) but "
                            f"trigger account(s) {', '.join(other_sides)} are not connected.\n"
                            f"Rollback will NOT execute until they reconnect.\n"
                            f"sid={session.get('id','')[:8]}",
                            account_id=account
                        )
                    except Exception:
                        pass
            session[_rb_hold_key] = _rb_hold_count + 1
            return False  # Hold — retry next cycle

        # Other side is fresh — clear any hold counter and proceed
        for other_acc in other_sides:
            session.pop(f"_rb_connectivity_hold_{other_acc}", None)

        # Set start timestamp if not already tracking
        if not rb_start:
            session.setdefault("rollback_start_ts", {})[account] = time.time()

        strat_id = session.get("strategy_id")
        strat = strategies.get(strat_id) if strat_id else None
        # Rollback is a safety mechanism — execute regardless of strategy running state
        return "rollback"

    _action_check = session.get("action", "")
    if session["status"] not in ("active", "partial_close"):
        if _action_check.startswith("cycle_"):
            print(f"[CYCLE-GATE] {account}: BLOCKED by status={session['status']}")
        return False

    # Check if the parent strategy is running
    # Bypass for: cycle operations (maintenance), monitor mode (rebalancing)
    strat_id = session.get("strategy_id")
    if strat_id and not _action_check.startswith("cycle_") and _action_check != "monitor":
        strat = strategies.get(strat_id)
        if strat and not strat.get("running", False):
            return False

    # Check time window — but bypass for cycle operations (must complete once started)
    if not _action_check.startswith("cycle_"):
        if not _is_within_time_window(session):
            return False

    # Check trade_pause: minimum delay between consecutive trades
    # Bypass for cycle operations — close→reopen must be instant
    trade_pause = session.get("trade_pause", 0)
    if trade_pause > 0 and not _action_check.startswith("cycle_"):
        last_ts = session.get("last_trade_ts", {}).get(account, 0)
        if last_ts > 0 and (time.time() - last_ts) < trade_pause:
            return False

    # Check in-flight: block if a command was recently sent and not yet confirmed
    # Uses configurable timeout from dashboard_settings (default 60s)
    # Resolve the session's dict key (sessions are keyed by session_id, not stored as session["id"])
    _sid = session.get("id", "")
    if not _sid:
        for _sk, _sv in sessions.items():
            if _sv is session:
                _sid = _sk
                break
    flight_key = (_sid, account)
    flight_ts = in_flight_commands.get(flight_key, 0)
    exec_timeout = dashboard_settings.get("exec_timeout_sec", 60)
    flight_timeout = exec_timeout  # Same timeout for all operations including cycles
    if flight_ts > 0:
        elapsed = time.time() - flight_ts
        if elapsed < flight_timeout:
            if _action_check.startswith("cycle_"):
                print(f"[CYCLE-INFLIGHT] {account}: BLOCKED by in-flight (sid={_sid[:8] if _sid else 'NONE'} elapsed={elapsed:.1f}s < timeout={flight_timeout}s)")
            return False
        # Timeout exceeded — command was sent but no fill received
        if elapsed >= flight_timeout and flight_ts > 0:
            acct_label = account
            if dashboard_settings.get("exec_alert_on_timeout"):
                msg = f"Execution timeout on {acct_label}: no fill received after {int(elapsed)}s"
                print(f"[EXEC-TIMEOUT] {msg}")
                _log_event(session.get('id', ''), account, 'exec_timeout', msg)
                try:
                    _send_email(f"EXEC TIMEOUT: {acct_label}", msg, account_id=account)
                    _send_telegram(msg, account_id=account)
                except Exception:
                    pass
            if dashboard_settings.get("exec_halt_on_timeout"):
                print(f"[EXEC-HALT] Halting {acct_label} — execution timeout after {int(elapsed)}s")
                _log_event(session.get('id', ''), account, 'exec_halt', f"Halted due to exec timeout ({int(elapsed)}s)")
                session["action"] = "monitor"
                _save_sessions()
                return False
            # Retry close commands (safe — closing an already-closed position is a no-op)
            if dashboard_settings.get("exec_retry_close") and _action_check in ("close", "cycle_close", "cycle_acc1", "cycle_acc2"):
                phase = session.get("cycle_progress", {}).get("phase", "")
                if _action_check == "close" or phase == "close":
                    retry_count = in_flight_retry_counts.get(flight_key, 0)
                    max_retries = dashboard_settings.get("exec_retry_max", 5)
                    if retry_count < max_retries:
                        in_flight_retry_counts[flight_key] = retry_count + 1
                        print(f"[EXEC-RETRY] Retrying close on {acct_label} (attempt {retry_count + 1}/{max_retries}) after {int(elapsed)}s timeout")
                        _log_event(session.get('id', ''), account, 'exec_retry_close', f"Retry {retry_count + 1}/{max_retries} after {int(elapsed)}s")
                    else:
                        print(f"[EXEC-RETRY-MAX] Max retries ({max_retries}) reached on {acct_label} — halting")
                        _log_event(session.get('id', ''), account, 'exec_retry_exhausted', f"Max retries ({max_retries}) reached, halting")
                        session["action"] = "monitor"
                        _save_sessions()
                        in_flight_retry_counts.pop(flight_key, None)
                        return False
            # Clear stale in-flight so system can retry
            in_flight_commands.pop(flight_key, None)

    action = session.get("action", "open")
    sides = session.get("sides", {})

    if account not in sides:
        return False

    # ── MONITOR mode: NEVER open positions, only rollback (close) is allowed ──
    # Rollback is already handled above. If we reach here, no rollback is needed.
    if action == "monitor":
        return False

    # ── News filter (applies to all actions) ──
    if session.get("avoid_news"):
        blocked, reason = is_news_blackout(impact_filter="High")
        if blocked:
            return False

    if action == "open":
        target = session["total_positions"]
        # Use NET open count (filled - closed) so re-opening after a full close works.
        # Raw 'filled' includes historical fills that were subsequently closed.
        current = session["filled"].get(account, 0) - session["closed"].get(account, 0)
        if current >= target:
            return False

        # Cross-account hedge sync: don't open next position until ALL sides
        # have completed their current fill. This prevents one fast side from
        # racing ahead while the other is still executing.
        for other_acc in sides:
            if other_acc != account:
                other_filled = session["filled"].get(other_acc, 0) - session["closed"].get(other_acc, 0)
                if current > other_filled:
                    return False



        # Check max_accum_deals (per-account net open position limit)
        # Each account is independently capped so one fast side can't block the other.
        max_deals = session.get("max_accum_deals", 0)
        if max_deals > 0:
            acct_net_open = session.get("filled", {}).get(account, 0) - session.get("closed", {}).get(account, 0)
            if acct_net_open >= max_deals:
                return False

        # Check max_accum_lots (per-account net open lots limit)
        max_accum = session.get("max_accum_lots", 0.0)
        if max_accum > 0:
            acct_net = max(0, session.get("filled", {}).get(account, 0) - session.get("closed", {}).get(account, 0))
            acct_lot = sides[account].get("lot_size", session.get("lot_size", 0.01))
            acct_lots = acct_net * acct_lot
            if acct_lots + acct_lot > max_accum + 1e-9:
                return False

        # Diff-to-open gating: only open when price diff >= threshold
        # None/blank = disabled — do NOT open (user must set a value to trade).
        # Any number including 0 is a valid threshold.
        diff_to_open = session.get("diff_to_open")
        if diff_to_open is None:
            return False  # Blank = don't trade
        curr_diff_val, _ = _calc_curr_diff(session, "open")
        if curr_diff_val is None or curr_diff_val < diff_to_open:
            return False

        # ── Execution Filters (open) ──
        # Quote rapidity: block if tick rate too high (fast market)
        max_ticks = session.get("max_ticks_per_5s", 0)
        if max_ticks > 0:
            for acc in sides:
                ei = ea_account_info.get(acc, {})
                if ei.get("ticks_per_5s", 0) > max_ticks:
                    return False

        # Price volatility: block if bid jumped too much
        max_jump = session.get("max_price_jump", 0)
        if max_jump > 0:
            for acc in sides:
                ei = ea_account_info.get(acc, {})
                if max(ei.get("last_bid_delta", 0), ei.get("last_ask_delta", 0)) > max_jump:
                    return False

        # DIFF skew filter
        skew_open = session.get("require_diff_skew_open", "")
        if skew_open:
            diff1, _ = _calc_curr_diff(session, "open")
            diff2, _ = _calc_curr_diff(session, "close")
            if diff1 is not None and diff2 is not None:
                if skew_open == "d1>d2" and diff1 <= diff2:
                    return False
                if skew_open == "d2>d1" and diff2 <= diff1:
                    return False

    elif action == "close":
        match_mode = session.get("match_mode", "ticket")
        if match_mode == "lots":
            # Lot-based close gating: compare closed_lots vs filled_lots
            filled_lots = session.get("filled_lots", {}).get(account, 0)
            closed_lots = session.get("closed_lots", {}).get(account, 0.0)
            if closed_lots >= filled_lots - 0.001:
                return False

            # Cross-account sync: let the side with more remaining lots close first
            for other_acc in sides:
                if other_acc != account:
                    other_filled = session.get("filled_lots", {}).get(other_acc, 0)
                    other_closed = session.get("closed_lots", {}).get(other_acc, 0.0)
                    my_remaining = filled_lots - closed_lots
                    other_remaining = other_filled - other_closed
                    if my_remaining < other_remaining - 0.001:
                        return False  # other side has more remaining lots, let them go first
        else:
            # Ticket-by-ticket (original logic)
            acct_filled = session["filled"].get(account, 0)
            close_cap = session.get("close_count")
            effective_target = min(close_cap, acct_filled) if close_cap is not None else acct_filled
            current = session["closed"].get(account, 0)
            if current >= effective_target:
                return False

            # Cross-account close sync: close one hedge pair at a time.
            # Don't close on this side if it's already ahead of the other side,
            # BUT only if the other side still has more positions left to close.
            # If the other side has finished ALL its closes (e.g. it had fewer
            # fills), allow this side to continue independently — otherwise a
            # fill-count asymmetry causes a permanent deadlock where this account
            # is blocked forever waiting for the other side to catch up when it
            # already finished.
            for other_acc in sides:
                if other_acc != account:
                    other_closed = session["closed"].get(other_acc, 0)
                    if current > other_closed:
                        # Only block if the other side is still actively closing
                        other_filled = session["filled"].get(other_acc, 0)
                        other_close_cap = session.get("close_count")
                        other_effective_target = min(other_close_cap, other_filled) if other_close_cap is not None else other_filled
                        if other_closed < other_effective_target:
                            # Other side is behind and still has work to do — wait for it
                            return False
                        # Other side is already done — run freely

        # Diff-to-close gating: only close when DIFF2 >= threshold.
        # Blank/None = close freely (no diff gating required).
        diff_to_close = session.get("diff_to_close")
        if diff_to_close is not None and diff_to_close != "":
            diff_to_close = int(diff_to_close)
            curr_diff_val, _ = _calc_curr_diff(session, "close")
            if curr_diff_val is None or curr_diff_val < diff_to_close:
                return False

        # ── Per-side spread gating (close) ──
        # MAX SPD1 >= SPD1 and MAX SPD2 >= SPD2 must BOTH be met
        for acc in sides:
            s_info = sides[acc]
            s_max_spread = s_info.get("max_spread") if s_info.get("max_spread") is not None else session.get("max_spread_points", 0)
            if s_max_spread is not None and s_max_spread != "":
                s_max_spread_int = int(float(s_max_spread))
                
                instrument = (s_info.get("pair") or session.get("pair", "")).upper()
                ei = ea_account_info.get(acc, {})
                bid = 0
                ask = 0
                stored_spread = None
                
                direct_acct = mt_direct_manager.accounts.get(acc) if mt_direct_manager else None
                if direct_acct and instrument:
                    try:
                        sq = direct_acct.get_quote_direct(instrument)
                        if sq and sq.get("bid") and sq.get("ask"):
                            q_bid = sq["bid"]
                            q_ask = sq["ask"]
                            is_jpy = "JPY" in instrument
                            if (is_jpy and q_bid > 10) or (not is_jpy and q_bid < 10):
                                bid = q_bid
                                ask = q_ask
                                stored_spread = sq.get("spread")
                    except Exception:
                        pass
                        
                if not (bid > 0 and ask > 0):
                    fix_acct = fix_manager.accounts.get(acc) if fix_manager else None
                    if fix_acct and hasattr(fix_acct, 'get_symbol_info') and instrument:
                        try:
                            sq = fix_acct.get_symbol_info(instrument)
                            if sq and sq.get("bid") and sq.get("ask"):
                                bid = sq["bid"]
                                ask = sq["ask"]
                                stored_spread = sq.get("spread")
                        except Exception:
                            pass
                            
                if not (bid > 0 and ask > 0):
                    bid = ei.get("bid", 0)
                    ask = ei.get("ask", 0)
                    stored_spread = ei.get("spread")
                    
                has_live_quotes = bid > 0 and ask > 0
                if has_live_quotes:
                    pip_mult = 1000 if "JPY" in session.get("pair", "").upper() else 100000
                    cur_spread = round((ask - bid) * pip_mult, 1)
                else:
                    cur_spread = stored_spread

                print(f"[CLOSE-SPREAD-GATE] {acc}: cur_spread={cur_spread} max_spread={s_max_spread_int} live={has_live_quotes}")
                
                if cur_spread is None:
                    print(f"[CLOSE-SPREAD-GATE] {acc}: BLOCKED — no quotes (cur_spread=None)")
                    return False  # No quotes — don't close blind
                if not has_live_quotes and cur_spread == 0:
                    print(f"[CLOSE-SPREAD-GATE] {acc}: BLOCKED — no live bid/ask (stale spread={cur_spread})")
                    return False  # Stale data — don't close blind
                # Always enforce max_spread: 0 = block all, N = allow up to N pips, None = no restriction
                if float(cur_spread) > s_max_spread_int:
                    print(f"[CLOSE-SPREAD-GATE] {acc}: BLOCKED — spread {cur_spread} > max {s_max_spread_int}")
                    return False

        # ── Execution Filters (close) ──
        max_ticks = session.get("max_ticks_per_5s", 0)
        if max_ticks > 0:
            for acc in sides:
                ei = ea_account_info.get(acc, {})
                if ei.get("ticks_per_5s", 0) > max_ticks:
                    return False

        max_jump = session.get("max_price_jump", 0)
        if max_jump > 0:
            for acc in sides:
                ei = ea_account_info.get(acc, {})
                if ei.get("last_bid_delta", 0) > max_jump:
                    return False

        # DIFF skew filter
        skew_close = session.get("require_diff_skew_close", "")
        if skew_close:
            diff1, _ = _calc_curr_diff(session, "open")
            diff2, _ = _calc_curr_diff(session, "close")
            if diff1 is not None and diff2 is not None:
                if skew_close == "d1>d2" and diff1 <= diff2:
                    return False
                if skew_close == "d2>d1" and diff2 <= diff1:
                    return False
    elif action.startswith("cycle_"):
        # CYCLE mode: close and reopen positions on ONE side, one at a time.
        # Derive cycling account from side_number, not dict key order
        target_side_num = 1 if action == "cycle_acc1" else 2
        cycle_account = ""
        for acc_key, side_info in sides.items():
            if side_info.get("side_number") == target_side_num:
                cycle_account = acc_key
                break
        if not cycle_account:
            print(f"[CYCLE-DBG] No account found with side_number={target_side_num} (sides: {[(a, s.get('side_number')) for a, s in sides.items()]})")
            return False
        if account != cycle_account:
            return False  # Only the cycling account gets commands

        if not session.get("cycle_progress"):
            # Count active fills (excluding manually closed)
            _init_closed = set(
                str(cf.get("ticket")) for cf in session.get("close_fills", [])
                if cf.get("account") == cycle_account
            )
            _init_fills = [
                f for f in session.get("fills", [])
                if f.get("account") == cycle_account
                and str(f.get("ticket")) not in _init_closed
            ]
            cycle_total = len(_init_fills)
            session["cycle_progress"] = {"phase": "close", "index": 0, "cycled": 0, "cycle_total": cycle_total}
            session["cycle_account"] = cycle_account
            print(f"[CYCLE-DBG] Auto-initialized cycle_progress for {cycle_account}, total={cycle_total}")

        progress = session.get("cycle_progress", {})
        phase = progress.get("phase", "close")
        idx = progress.get("index", 0)

        # Get fills for the cycling account, excluding already-closed tickets
        closed_tickets_set = set(
            str(f.get("ticket")) for f in session.get("close_fills", [])
            if f.get("account") == cycle_account
        )
        acct_fills = [
            f for f in session.get("fills", [])
            if f.get("account") == cycle_account
            and str(f.get("ticket")) not in closed_tickets_set
        ]
        # Sort oldest-first so cycle processes the oldest positions first
        def _fill_sort_key(f):
            ts_str = f.get("ts", "")
            if ts_str:
                is_direct = mt_direct_manager and cycle_account in mt_direct_manager.accounts
                epoch = _parse_broker_timestamp(ts_str, is_direct=is_direct)
                if epoch is not None:
                    return epoch
            return f.get("ts_epoch", 0) or 0
        acct_fills.sort(key=_fill_sort_key)
        # Use the filtered fill count as the total — it already excludes manually closed positions
        total_to_cycle = len(acct_fills)
        # IMPORTANT: Do NOT declare completion while phase=="open" — the last position
        # was just closed and still needs its reopen fill.  The close added the ticket
        # to close_fills, which shrinks acct_fills, making idx >= total_to_cycle true
        # Check if position is older than cycle_days (REQUIRED — blank = stop cycling)
        cycle_days = session.get("cycle_days")
        print(f"[CYCLE-DBG] acct={account}: cycle_days={repr(cycle_days)} idx={idx} acct_fills={len(acct_fills)} total_to_cycle={total_to_cycle} phase={phase}")
        if cycle_days is None or cycle_days == "":
            print(f"[CYCLE-DBG] acct={account}: cycle_days not set (value={repr(cycle_days)})")
            return False  # No cycle_days set — don't cycle
        try:
            cycle_days = float(cycle_days)
        except (ValueError, TypeError):
            print(f"[CYCLE-DBG] acct={account}: cycle_days invalid (value={repr(cycle_days)})")
            return False
        if cycle_days < 0:
            print(f"[CYCLE-DBG] acct={account}: cycle_days < 0 (value={cycle_days})")
            return False  # Negative = don't cycle

        found_old_enough = True  # Default: all positions qualify (cycle_days=0 or phase=open)
        if phase != "open":
            if cycle_days > 0:
                # BUG FIX: Always search from index 0 in the sorted list.
                # After each cycle, the replacement fill gets a fresh timestamp and
                # re-sorts to the end of the list, shifting all remaining fills down
                # by one position. If we start from progress["index"], we skip the
                # fill that shifted into the previous slot (every other position).
                # Starting from 0 ensures we always find the oldest uncycled position.
                search_idx = 0
                found_old_enough = False
                while search_idx < len(acct_fills):
                    fill_record = acct_fills[search_idx]
                    fill_epoch = None
                    fill_ts_str = fill_record.get("ts", "")
                    if fill_ts_str:
                        is_direct = mt_direct_manager and account in mt_direct_manager.accounts
                        fill_epoch = _parse_broker_timestamp(fill_ts_str, is_direct=is_direct)
                    if fill_epoch is None:
                        fill_epoch = fill_record.get("ts_epoch", 0)  # Fallback
                        
                    if fill_epoch:
                        age_days = _count_rollover_days(fill_epoch)
                        if age_days < cycle_days:
                            print(f"[CYCLE-DBG] acct={account}: position {search_idx} too new (age={age_days} rollover days < {cycle_days}d, ts={fill_ts_str}, epoch={fill_epoch}) - skipping")
                            search_idx += 1
                        else:
                            print(f"[CYCLE-DBG] acct={account}: position {search_idx} old enough (age={age_days} rollover days >= {cycle_days}d, ts={fill_ts_str}, epoch={fill_epoch})")
                            found_old_enough = True
                            break
                    else:
                        print(f"[CYCLE-DBG] acct={account}: position {search_idx} missing ts_epoch, allowing cycle")
                        found_old_enough = True
                        break
                # Update idx to the found position for downstream use
                idx = search_idx
                # CRITICAL: Store the age-filtered index back into progress so the
                # poll handler (which reads progress["index"]) closes the correct
                # position — not the stale/unfiltered one.
                progress["index"] = idx
                session["cycle_progress"] = progress
            else:
                print(f"[CYCLE-DBG] acct={account}: cycle_days=0 — skipping age check")
                # When cycle_days is 0, the oldest position is always at index 0 because
                # newly cycled positions sort to the end of the temporal list.
                # We enforce idx=0 to avoid the array drift that skips alternating positions.
                idx = 0
                progress["index"] = idx
                session["cycle_progress"] = progress

        # Guard: make sure fill index is in range (but NOT during open phase —
        # the close shrinks acct_fills and we still need the reopen to fire)
        if phase != "open":
            # When cycle_days > 0 and no old-enough position was found,
            # all remaining positions are too new → cycling is complete
            no_more_to_cycle = cycle_days > 0 and not found_old_enough
            target_cycles = progress.get("cycle_total", len(acct_fills))
            if progress.get("cycled", 0) >= target_cycles or idx >= len(acct_fills) or no_more_to_cycle:
                cycled_count = progress.get("cycled", 0)
                print(f"[CYCLE-DBG] acct={account}: No more positions to cycle (idx={idx}, acct_fills={len(acct_fills)}, found_old_enough={found_old_enough})")
                # Auto-switch to monitor when all positions are cycled
                if session.get("action", "").startswith("cycle_"):
                    session["action"] = "monitor"
                    _save_sessions()
                    avg_spread = 0
                    total_sc = progress.get("total_spread_cost", 0)
                    if cycled_count > 0:
                        avg_spread = total_sc / cycled_count
                    _log_event(session["id"], account, "cycle_complete",
                               f"All {cycled_count} positions processed — avg spread cost: {avg_spread:.5f} — switching to MONITOR")
                    print(f"[CYCLE] Complete: {cycled_count} cycled, avg spread cost={avg_spread:.5f}, auto-switching to MONITOR")
                return False  # All positions cycled or skipped

        if phase == "close":
            # Check spread gating for the cycling account
            side_info = sides.get(account, {})
            side_max_spread = side_info.get("max_spread") if side_info.get("max_spread") is not None else session.get("max_spread_points", 0)
            ea_info = ea_account_info.get(account, {})
            bid = 0
            ask = 0
            stored_spread = None
            # Use get_quote_direct for reliable instrument-specific quotes.
            # get_symbol_info reads from _symbol_cache which can return wrong-instrument
            # data (e.g. EURUSD values for a USDJPY lookup) due to cache keying issues.
            instrument = (side_info.get("pair") or session.get("pair", "")).upper()
            direct_acct = mt_direct_manager.accounts.get(account) if mt_direct_manager else None
            if direct_acct and instrument:
                try:
                    sq = direct_acct.get_quote_direct(instrument)
                    if sq and sq.get("bid") and sq.get("ask"):
                        q_bid = sq["bid"]
                        q_ask = sq["ask"]
                        # Sanity check: JPY pairs must have bid > 10 (e.g. 157.xx),
                        # non-JPY pairs must have bid < 10 (e.g. 1.169xx).
                        # Reject if the data looks like wrong-instrument contamination.
                        is_jpy = "JPY" in instrument
                        if (is_jpy and q_bid > 10) or (not is_jpy and q_bid < 10):
                            bid = q_bid
                            ask = q_ask
                            stored_spread = sq.get("spread")
                        else:
                            print(f"[CYCLE-SPREAD] acct={account}: REJECTED quote bid={q_bid} for {instrument} — wrong instrument range")
                except Exception:
                    pass
            if not (bid > 0 and ask > 0):
                # Fallback: try FIX connector
                fix_acct = fix_manager.accounts.get(account) if fix_manager else None
                if fix_acct and hasattr(fix_acct, 'get_symbol_info') and instrument:
                    try:
                        sq = fix_acct.get_symbol_info(instrument)
                        if sq and sq.get("bid") and sq.get("ask"):
                            bid = sq["bid"]
                            ask = sq["ask"]
                            stored_spread = sq.get("spread")
                    except Exception:
                        pass
            if not (bid > 0 and ask > 0):
                # Last fallback: ea_account_info (may be wrong instrument)
                bid = ea_info.get("bid", 0)
                ask = ea_info.get("ask", 0)
                stored_spread = ea_info.get("spread")
            # Always compute spread from bid/ask — the stored "spread" field can be
            # set by multiple code paths (heartbeat, _on_quote) with inconsistent values
            has_live_quotes = bid > 0 and ask > 0
            if has_live_quotes:
                pip_mult = 1000 if "JPY" in session.get("pair", "").upper() else 100000
                current_spread = round((ask - bid) * pip_mult, 1)
            else:
                current_spread = stored_spread  # fallback to stored if no bid/ask
            print(f"[CYCLE-SPREAD] acct={account}: instrument={instrument} max_spread={side_max_spread} current_spread={current_spread} stored={stored_spread} bid={bid} ask={ask} live={has_live_quotes}")
            if side_max_spread is not None and side_max_spread != "":
                side_max_spread = int(float(side_max_spread))
                if current_spread is None:
                    print(f"[CYCLE-SPREAD] acct={account}: BLOCKED — no quotes (current_spread=None)")
                    return False  # No quotes — don't close blind
                # BUG FIX: When no live bid/ask quotes are available, the stored
                # spread may be 0 or stale from a previous session / startup.
                # With max_spread=0, int(0) > 0 is False which PASSES the gate,
                # allowing cycles to fire on stale data. Block if we don't have
                # live bid/ask quotes — same as having no quotes at all.
                if not has_live_quotes:
                    print(f"[CYCLE-SPREAD] acct={account}: BLOCKED — no live bid/ask (stale spread={current_spread})")
                    return False  # Stale data — don't close blind
                if side_max_spread > 0 and float(current_spread) > side_max_spread:
                    print(f"[CYCLE-SPREAD] acct={account}: BLOCKED — spread {current_spread} (int={int(current_spread)}) > max {side_max_spread}")
                    return False  # Spread too wide
            print(f"[CYCLE-SPREAD] acct={account}: PASSED — returning cycle_close")
            return "cycle_close"
        elif phase == "open":
            # Guard: only dispatch ONE open per cycle step.
            # Without this, the command loop can re-enter before the fill
            # arrives and issue a duplicate open for the same index.
            if progress.get("open_dispatched"):
                # Safety valve: if open_dispatched has been set for >30s without
                # a fill or error callback (e.g. server restart, dropped response),
                # auto-clear it so the cycle can retry. The 30s window matches
                # the cycle_close_ts timeout used in the poll-loop recovery.
                close_ts = progress.get("cycle_close_ts", 0)
                if close_ts > 0 and (time.time() - close_ts) > 30:
                    print(f"[CYCLE-GUARD] open_dispatched stale (>{30}s) — clearing for retry")
                    progress.pop("open_dispatched", None)
                    session["cycle_progress"] = progress
                    _save_sessions()
                else:
                    return False
            progress["open_dispatched"] = True
            session["cycle_progress"] = progress
            return True  # Server manages reopen until EA supports atomic cycle_reopen
        return False
    else:
        return False

    # Check execution order
    exec_order = session.get("execution_order", "simultaneous")
    if exec_order == "simultaneous":
        return True

    # Find side numbers
    my_side = sides[account].get("side_number", 0)
    other_side_num = 1 if my_side == 2 else 2

    # Find the other account
    other_account = None
    for acc, info in sides.items():
        if info.get("side_number") == other_side_num:
            other_account = acc
            break

    if other_account is None:
        return True  # Only one side configured

    if action == "open":
        target = session["total_positions"]
        if exec_order == "side1_first":
            if my_side == 2:
                return session["filled"].get(other_account, 0) >= target
            return True
        elif exec_order == "side2_first":
            if my_side == 1:
                return session["filled"].get(other_account, 0) >= target
            return True
    elif action == "close":
        close_count = session.get("close_count", 0) or 0
        if exec_order == "side1_first":
            if my_side == 2:
                return session["closed"].get(other_account, 0) >= close_count
            return True
        elif exec_order == "side2_first":
            if my_side == 1:
                return session["closed"].get(other_account, 0) >= close_count
            return True

    return True

def _calc_curr_diff(session, direction):
    """
    Calculate current diff for a session based on live EA quotes.
    direction: 'open' or 'close'
    Returns (diff_value, reason_string).
    diff_value is a number or None. reason_string explains why it's None.
    """
    sides = session.get("sides", {})
    accounts = list(sides.keys())
    if len(accounts) != 2:
        return (None, "need 2 sides")

    acc1, acc2 = accounts[0], accounts[1]
    info1 = ea_account_info.get(acc1)
    info2 = ea_account_info.get(acc2)
    if not info1 and not info2:
        return (None, "no EA data")
    if not info1:
        return (None, f"{acc1}: offline")
    if not info2:
        return (None, f"{acc2}: offline")

    # Determine expected pair for each side
    pair1 = (sides[acc1].get("pair") or session.get("pair", "")).strip()
    pair2 = (sides[acc2].get("pair") or session.get("pair", "")).strip()

    # Verify EAs are reporting the correct symbol — if the EA is on a
    # different chart, its bid/ask would be for the wrong instrument.
    # Use lenient matching to handle broker suffixes (e.g. "USDJPY." or "USDJPYm")
    ea_sym1 = (info1.get("symbol") or "").upper()
    ea_sym2 = (info2.get("symbol") or "").upper()
    conn1 = info1.get("conn_type", "")
    conn2 = info2.get("conn_type", "")
    is_direct1 = conn1 in ("mt4_direct", "mt5_direct")
    is_direct2 = conn2 in ("mt4_direct", "mt5_direct")
    sym1_ok = not ea_sym1 or not pair1 or ea_sym1.startswith(pair1.upper()) or pair1.upper().startswith(ea_sym1)
    sym2_ok = not ea_sym2 or not pair2 or ea_sym2.startswith(pair2.upper()) or pair2.upper().startswith(ea_sym2)

    # For MT Direct accounts, get quotes directly for the session's pair.
    # Only BLOCK ea_account_info bid/ask if quote_symbol is present AND
    # doesn't match the pair (positive mismatch = wrong instrument).
    # Empty quote_symbol is permissive — trust the cached data.
    qs1 = (info1.get("quote_symbol") or "").upper()
    qs2 = (info2.get("quote_symbol") or "").upper()
    pair1_u = pair1.upper()
    pair2_u = pair2.upper()
    # For direct accounts: block only on positive symbol mismatch
    sym1_mismatch = is_direct1 and qs1 and not (qs1.startswith(pair1_u) or pair1_u.startswith(qs1))
    sym2_mismatch = is_direct2 and qs2 and not (qs2.startswith(pair2_u) or pair2_u.startswith(qs2))
    sym1_price_ok = sym1_ok and not sym1_mismatch
    sym2_price_ok = sym2_ok and not sym2_mismatch
    # For direct accounts, ea_account_info may cache bid/ask from whatever
    # chart the EA is on (e.g. EURUSD while session is USDCHF).  Only use
    # the cached prices when the symbol POSITIVELY matches the session pair.
    # Empty / missing symbol → can't verify → default to 0 (blank DIFF)
    # so we don't silently show cross-instrument garbage.
    def _direct_sym_matches(ea_sym, pair):
        """True only if both are non-empty and one is a prefix of the other (case-insensitive)."""
        if not ea_sym or not pair:
            return False
        eu, pu = ea_sym.upper(), pair.upper()
        return eu.startswith(pu) or pu.startswith(eu)

    if is_direct1:
        bid1 = info1.get("bid", 0) if _direct_sym_matches(ea_sym1, pair1) else 0
        ask1 = info1.get("ask", 0) if _direct_sym_matches(ea_sym1, pair1) else 0
    else:
        bid1 = info1.get("bid", 0) if sym1_price_ok else 0
        ask1 = info1.get("ask", 0) if sym1_price_ok else 0
    if is_direct2:
        bid2 = info2.get("bid", 0) if _direct_sym_matches(ea_sym2, pair2) else 0
        ask2 = info2.get("ask", 0) if _direct_sym_matches(ea_sym2, pair2) else 0
    else:
        bid2 = info2.get("bid", 0) if sym2_price_ok else 0
        ask2 = info2.get("ask", 0) if sym2_price_ok else 0


    # Try getting quotes directly from MT Direct or FIX account managers
    for i, (acc, pair_i) in enumerate([(acc1, pair1), (acc2, pair2)]):
        direct_acct = None
        if mt_direct_manager and acc in mt_direct_manager.accounts:
            direct_acct = mt_direct_manager.accounts.get(acc)
        elif fix_manager and acc in fix_manager.accounts:
            direct_acct = fix_manager.accounts.get(acc)
            
        if direct_acct:
            got_quote = False
            quote_src = "none"
            q_bid = q_ask = 0
            try:
                # 1) Try _symbol_cache if available
                if hasattr(direct_acct, 'get_symbol_info'):
                    sym_info = direct_acct.get_symbol_info(pair_i)
                    if sym_info and sym_info.get("bid") and sym_info.get("ask"):
                        _direct_quote_cache[(acc, pair_i)] = {
                            "bid": sym_info["bid"], "ask": sym_info["ask"]
                        }
                        q_bid, q_ask = sym_info["bid"], sym_info["ask"]
                        got_quote = True
                        quote_src = "symbol_cache"
                
                # 2) Try get_quote_direct
                if not got_quote and hasattr(direct_acct, 'get_quote_direct'):
                    dq = direct_acct.get_quote_direct(pair_i)
                    if dq and dq.get("bid") and dq.get("ask"):
                        _direct_quote_cache[(acc, pair_i)] = {
                            "bid": dq["bid"], "ask": dq["ask"]
                        }
                        q_bid, q_ask = dq["bid"], dq["ask"]
                        got_quote = True
                        quote_src = "get_quote_direct"
            except Exception:
                pass
                
            if got_quote:
                if i == 0:
                    bid1, ask1 = q_bid, q_ask
                    sym1_ok = True
                else:
                    bid2, ask2 = q_bid, q_ask
                    sym2_ok = True
            else:
                # 3) Fall back to last-known-good cache
                cached = _direct_quote_cache.get((acc, pair_i))
                if cached:
                    q_bid, q_ask = cached["bid"], cached["ask"]
                    if i == 0:
                        bid1, ask1 = q_bid, q_ask
                        sym1_ok = True
                    else:
                        bid2, ask2 = q_bid, q_ask
                        sym2_ok = True
                    quote_src = "direct_cache"
                    
            logger.debug("[DIFF-DIAG] side%d acc=%s pair=%s src=%s bid=%.6f ask=%.6f spd=%.1f",
                         i+1, acc, pair_i, quote_src, q_bid, q_ask,
                         (q_ask - q_bid) * (1000 if "JPY" in pair_i.upper() else 100000) if q_bid and q_ask else 0)


    # Original symbol checks for EA-polled accounts
    if not is_direct1 and not sym1_ok:
        if not sym2_ok and not is_direct2:
            return (None, f"S1:{ea_sym1}≠{pair1} S2:{ea_sym2}≠{pair2}")
        return (None, f"S1 on {ea_sym1} (need {pair1})")
    if not is_direct2 and not sym2_ok:
        return (None, f"S2 on {ea_sym2} (need {pair2})")

    if not bid1 or not ask1 or not bid2 or not ask2:
        return (None, None)

    # Determine which side buys and which sells
    s1_action = sides[acc1].get("action", "buy").lower()
    s2_action = sides[acc2].get("action", "sell").lower()

    # Determine pip multiplier from pair (e.g., USDJPY=1000 @ 0.001 pip, EURUSD=100000)
    pair = pair1 or pair2
    pip_mult = 1000 if "JPY" in pair else 100000

    if direction == "open":
        # DIFF1 = sell_side_bid - buy_side_ask
        # Negative = cost to open (normal), positive = arbitrage opportunity
        # E.g. ACCT1=BUY(ask1), ACCT2=SELL(bid2) → diff = bid2 - ask1
        if s1_action == "buy" and s2_action == "sell":
            diff = (bid2 - ask1) * pip_mult
        elif s1_action == "sell" and s2_action == "buy":
            diff = (bid1 - ask2) * pip_mult
        else:
            diff = (bid2 - ask1) * pip_mult
    else:  # close
        # DIFF2 = close_sell_bid - close_buy_ask
        # To close: buy side sells (gets bid), sell side buys (pays ask)
        # E.g. ACCT1=BUY→close=SELL(bid1), ACCT2=SELL→close=BUY(ask2)
        # diff2 = bid1 - ask2  (negative = cost to close, positive = profit/arb)
        if s1_action == "buy" and s2_action == "sell":
            diff = (bid1 - ask2) * pip_mult
        elif s1_action == "sell" and s2_action == "buy":
            diff = (bid2 - ask1) * pip_mult
        else:
            diff = (bid1 - ask2) * pip_mult

    return (round(diff, 1), None)


def _update_closed_lots(session, account, lots_closed):
    """Track closed lots for lot-mode sessions.
    Call this whenever a position is closed in a lot-mode session.
    lots_closed: the lot size of the position/partial that was just closed.
    """
    if session.get("match_mode") != "lots":
        return
    cl = session.setdefault("closed_lots", {})
    cl[account] = round(cl.get(account, 0.0) + lots_closed, 4)


def _check_session_completion(session):
    """Check if all sides have reached their targets.
    Sessions CYCLE: open -> close -> open -> close ...
    When close targets are met, reset counters and switch back to 'open'.
    """
    action = session.get("action", "open")
    all_done = True
    # DEBUG: trace completion check
    if action == "close":
        _dbg_filled = {a: session["filled"].get(a, 0) for a in session.get("sides", {})}
        _dbg_closed = {a: session["closed"].get(a, 0) for a in session.get("sides", {})}
        _dbg_cc = session.get("close_count")
        print(f"[CLOSE-CHECK] sid={session['id'][:8]} filled={_dbg_filled} closed={_dbg_closed} close_count={_dbg_cc} fills_len={len(session.get('fills', []))} close_fills_len={len(session.get('close_fills', []))}")

    for account in session.get("sides", {}):
        if action == "open":
            # Use NET open count (filled - closed) so re-opening after a full close works
            net_open = session["filled"].get(account, 0) - session["closed"].get(account, 0)
            if net_open < session["total_positions"]:
                all_done = False
                break
        elif action == "close":
            match_mode = session.get("match_mode", "ticket")
            if match_mode == "lots":
                filled_lots = session.get("filled_lots", {}).get(account, 0)
                closed_lots = session.get("closed_lots", {}).get(account, 0.0)
                if closed_lots < filled_lots - 0.001:
                    all_done = False
                    break
            else:
                acct_filled = session["filled"].get(account, 0)
                close_cap = session.get("close_count")
                effective = min(close_cap, acct_filled) if close_cap is not None else acct_filled
                if session["closed"].get(account, 0) < effective:
                    all_done = False
                    break

    if all_done and action == "close":
        # Safety check: only compare accounts that actually had positions to close.
        # Accounts with effective target = 0 (no fills) are excluded from the imbalance check.
        active_close_counts = []
        for acc in session.get("sides", {}):
            acct_filled = session["filled"].get(acc, 0)
            close_cap = session.get("close_count")
            effective = min(close_cap, acct_filled) if close_cap is not None else acct_filled
            if effective > 0:
                active_close_counts.append(session["closed"].get(acc, 0))
        if active_close_counts and max(active_close_counts) > 0 and min(active_close_counts) == 0:
            all_done = False
            print(f"[CYCLE-GUARD] Session {session['id'][:8]}: blocking — "
                  f"one active side has 0 closes while another has {max(active_close_counts)}.")

    if all_done and action == "close":
        print(f"[CLOSE-DONE-TRIGGER] sid={session['id'][:8]} ALL DONE filled={dict((a, session['filled'].get(a,0)) for a in session.get('sides',{}))} closed={dict((a, session['closed'].get(a,0)) for a in session.get('sides',{}))} close_count={session.get('close_count')}")
        # All close targets met — switch to monitor and pause.
        # Preserve fills/close_fills so the UI can still display position history.
        # The user can manually reset via the Reset Cycle button if needed.
        session["action"] = "monitor"
        session["status"] = "paused"
        session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _log_event(session["id"], None, "close_complete",
                   "All close targets met — switched to monitor/paused, fills preserved")
        print(f"[CLOSE-DONE] Session {session['id']}: all closes done, switched to monitor/paused")
        _save_sessions()
    elif all_done and action == "open":
        # All open targets met — check if all positions were also externally closed
        all_closed = all(
            session["closed"].get(acc, 0) >= session["filled"].get(acc, 0) > 0
            for acc in session.get("sides", {})
        )
        if all_closed:
            # All positions externally closed (e.g. margin call) — switch to MONITOR
            session["action"] = "monitor"
            session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _log_event(session["id"], None, "mode_monitor",
                       "All positions externally closed — switched to MONITOR mode")
            print(f"[MODE] Session {session['id']}: all positions externally closed, switching to MONITOR")
            _save_sessions()
        else:
            if session.get("status") == "partial_close":
                session["status"] = "active"
            session["action"] = "monitor"
            session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _log_event(session["id"], None, "open_targets_reached",
                       f"All sides reached open target ({session.get('total_positions')} each) — auto-switched to MONITOR")
            print(f"[MODE] Session {session['id']}: all open targets reached, auto-switching to MONITOR")
            _save_sessions()
    elif action == "close":
        # Check for partial close: one side done closing, other not
        # Only relevant when action is 'close' — during opening, imbalanced fills are normal.
        action = session.get("action", "open")
        if action == "close":
            close_count = session.get("close_count", 0) or 0
            sides = session.get("sides", {})
            if len(sides) >= 2:
                done_accounts = []
                pending_accounts = []
                for acc in sides:
                    acct_filled = session["filled"].get(acc, 0)
                    if close_count > 0:
                        effective = min(close_count, acct_filled) if acct_filled else close_count
                    else:
                        effective = acct_filled
                    if session["closed"].get(acc, 0) >= effective:
                        done_accounts.append(acc)
                    else:
                        pending_accounts.append(acc)
                if done_accounts and pending_accounts:
                    # One side closed, other still pending — flag as partial_close
                    if session.get("status") != "partial_close":
                        session["status"] = "partial_close"
                        session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        _log_event(session["id"], None, "partial_close",
                                   f"ALERT: Side(s) {done_accounts} closed but {pending_accounts} still open! Hedge is unbalanced.")
                        print(f"[ALERT] PARTIAL CLOSE: {done_accounts} closed, {pending_accounts} NOT closed. Hedge unbalanced!")
                        _save_sessions()


# ── Startup: check for sessions stuck in close mode after restart ───────────
for _sid, _s in sessions.items():
    if _s.get("status") in ("active", "partial_close") and _s.get("action") == "close":
        _check_session_completion(_s)

# ─── Prevent browser caching of API responses ──────────────────────────────
@app.after_request
def add_no_cache_headers(response):
    if request.path.startswith('/api/'):
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response

# ─── API Endpoints ──────────────────────────────────────────────────────────

@app.route('/api/sessions', methods=['GET'])
def list_sessions():
    with lock:
        return jsonify(list(sessions.values()))

@app.route('/api/sessions', methods=['POST'])
def create_session():
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "Invalid JSON"}), 400

        with lock:
            session = _new_session(data)
            sessions[session["id"]] = session
            _save_sessions()
            _log_event(session["id"], None, "session_created",
                       f"{session['action']} {session['pair']} x{session['total_positions']}")

        return jsonify(session), 201
    except Exception as e:
        app.logger.exception("Error creating session")
        return jsonify({"error": str(e)}), 500

@app.route('/api/sessions/<session_id>', methods=['GET'])
def get_session(session_id):
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        return jsonify(s)

@app.route('/api/sessions/<session_id>', methods=['PUT'])
def update_session(session_id):
    try:
        data = request.get_json(force=True)
        with lock:
            s = sessions.get(session_id)
            if not s:
                return jsonify({"error": "Session not found"}), 404
            # Fields that can be changed on-the-fly (even while active)
            hot_fields = ("diff_to_open", "diff_to_close", "max_spread_points",
                          "max_errors", "trade_pause",
                          "execution_order", "max_accum_lots", "max_accum_deals",
                          "side1_max_spread", "side2_max_spread",
                          "close_count", "action", "cycle_days",
                          "max_ticks_per_5s", "max_price_jump",
                          "require_diff_skew_open", "require_diff_skew_close",
                          "avoid_news")
            # Fields that require draft/paused to change
            structural_fields = ("pair", "lot_size", "total_positions",
                                 "comment", "sides")

            is_hot_only = all(f in hot_fields for f in data.keys())
            if s["status"] not in ("draft", "paused") and not is_hot_only:
                return jsonify({"error": "Can only edit structural fields on draft or paused sessions. Hot fields (diff, spread, timing) can be changed anytime."}), 400

            # Update allowed fields
            for field in (*hot_fields, *structural_fields):
                if field in data:
                    s[field] = data[field]

            # Re-type numeric fields
            s["lot_size"] = float(s["lot_size"])
            s["total_positions"] = int(s["total_positions"])
            s["max_spread_points"] = int(s["max_spread_points"])

            if s.get("max_errors") is not None:
                s["max_errors"] = int(s["max_errors"])
            if s.get("trade_pause") is not None:
                s["trade_pause"] = float(s["trade_pause"])
            if s.get("diff_to_open") is not None:
                s["diff_to_open"] = int(s["diff_to_open"])
            if s.get("diff_to_close") is not None:
                s["diff_to_close"] = int(s["diff_to_close"])
            if s.get("max_accum_lots") is not None:
                s["max_accum_lots"] = float(s["max_accum_lots"])
            if s.get("max_accum_deals") is not None:
                s["max_accum_deals"] = int(s["max_accum_deals"])
            if s.get("close_count") is not None:
                s["close_count"] = int(s["close_count"])
            if s.get("max_ticks_per_5s") is not None:
                s["max_ticks_per_5s"] = int(s["max_ticks_per_5s"])
            if s.get("max_price_jump") is not None:
                s["max_price_jump"] = float(s["max_price_jump"])
            if "avoid_news" in data:
                s["avoid_news"] = bool(data["avoid_news"])

            s["pair"] = s["pair"].strip()

            # Apply per-side pair, lot_size, and max_spread from edit modal
            # Map side1/side2 to accounts using side_number (not dict key order!)
            side_to_acc = {}
            fallback_idx = 1
            for acc, info in s["sides"].items():
                sn = info.get("side_number")
                if sn is None:
                    sn = fallback_idx
                    info["side_number"] = sn  # Backfill missing side_number
                side_to_acc[sn] = acc
                fallback_idx += 1
            for side_num in (1, 2):
                acc = side_to_acc.get(side_num)
                if not acc:
                    continue
                side_key = f"side{side_num}"
                sp = data.get(f"{side_key}_pair")
                sl = data.get(f"{side_key}_lot_size")
                sms = data.get(f"{side_key}_max_spread")
                sc = data.get(f"{side_key}_comment")
                sa = data.get(f"{side_key}_action")
                if sp is not None:
                    s["sides"][acc]["pair"] = sp.strip() if sp else s["pair"]
                if sa is not None:
                    s["sides"][acc]["action"] = sa
                if sl is not None:
                    s["sides"][acc]["lot_size"] = float(sl) if sl != "" else s["lot_size"]
                if sms is not None:
                    s["sides"][acc]["max_spread"] = int(sms) if sms != "" else s["max_spread_points"]
                if sc is not None:
                    s["sides"][acc]["comment"] = sc

            # Rebuild tracking dicts for any new accounts
            for acc in s["sides"]:
                s["filled"].setdefault(acc, 0)
                s["closed"].setdefault(acc, 0)
                s["errors"].setdefault(acc, [])
                s["spread_rejects"].setdefault(acc, 0)

            s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _save_sessions()
            _log_event(session_id, None, "session_updated", json.dumps(data, default=str)[:200])

        return jsonify(s)
    except Exception as e:
        app.logger.exception("Error updating session")
        return jsonify({"error": str(e)}), 500

@app.route('/api/sessions/<session_id>/start', methods=['POST'])
def start_session(session_id):
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        if s["status"] not in ("draft", "paused"):
            return jsonify({"error": f"Cannot start session in status '{s['status']}'"}), 400
        s["status"] = "active"
        s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        s["hedge_monitor_start_ts"] = time.time()  # Cooldown: hedge monitor waits 30s (non-imported only)
        # Clear stale rollback data from previous runs
        s["rollback_needed"] = {}
        s["rollback_tickets"] = {}
        s.pop("rollback_start_ts", None)
        s.pop("imbalance_rebal_ts", None)
        _save_sessions()
        _log_event(session_id, None, "session_started", f"Status -> active")
    return jsonify(s)

@app.route('/api/sessions/<session_id>/stop', methods=['POST'])
def stop_session(session_id):
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        s["status"] = "paused"
        s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_sessions()
        _log_event(session_id, None, "session_paused", f"Status -> paused")
    return jsonify(s)

@app.route('/api/sessions/<session_id>/clear_errors', methods=['POST'])
def clear_errors(session_id):
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        sides = s.get("sides", {})
        s["errors"] = {acc: [] for acc in sides}
        s["spread_rejects"] = {acc: 0 for acc in sides}
        _save_sessions()
        _log_event(session_id, None, "errors_cleared", "Errors and spread rejects cleared")
    return jsonify({"ok": True})

@app.route('/api/sessions/<session_id>', methods=['DELETE'])
def delete_session(session_id):
    with lock:
        s = sessions.pop(session_id, None)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        _save_sessions()
        _log_event(session_id, None, "session_deleted", "")
    return jsonify({"ok": True})

@app.route('/api/sessions/<session_id>/clone', methods=['POST'])
def clone_session(session_id):
    """Deep-copy a session's config into a new draft session."""
    import copy
    with lock:
        original = sessions.get(session_id)
        if not original:
            return jsonify({"error": "Session not found"}), 404
        cloned = copy.deepcopy(original)
        new_id = str(uuid.uuid4())[:8]
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cloned["id"] = new_id
        cloned["status"] = "draft"
        cloned["created_at"] = now
        cloned["updated_at"] = now
        # Reset all counters
        accounts = list(cloned.get("sides", {}).keys())
        cloned["filled"] = {a: 0 for a in accounts}
        cloned["closed"] = {a: 0 for a in accounts}
        cloned["errors"] = {a: [] for a in accounts}
        cloned["spread_rejects"] = {a: 0 for a in accounts}
        cloned["rollback_needed"] = {}
        cloned["last_trade_ts"] = {}
        sessions[new_id] = cloned
        _save_sessions()
        _log_event(new_id, None, "session_cloned", f"Cloned from {session_id}")
    return jsonify(cloned), 201

@app.route('/api/sessions/<session_id>/reset_errors', methods=['POST'])
def reset_errors(session_id):
    """Reset error and spread reject counters for all accounts."""
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        for acc in s.get("sides", {}):
            s["errors"][acc] = []
            s["spread_rejects"][acc] = 0
        s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_sessions()
        _log_event(session_id, None, "errors_reset", "All error/spread counters cleared")
    return jsonify(s)

@app.route('/api/sessions/<session_id>/unblock', methods=['POST'])
def unblock_session(session_id):
    """Clear rollback state, reset errors, and set to draft for review."""
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        s["rollback_needed"] = {}
        for acc in s.get("sides", {}):
            s["errors"][acc] = []
            s["spread_rejects"][acc] = 0
        s["status"] = "draft"
        s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_sessions()
        _log_event(session_id, None, "session_unblocked",
                   "Rollback cleared, errors reset, status -> draft")
    return jsonify(s)

@app.route('/api/sessions/<session_id>/reset_cycle', methods=['POST'])
def reset_cycle(session_id):
    """Reset all fill/close counters and switch to open mode so session can start fresh."""
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        for acc in s.get("sides", {}):
            s["filled"][acc] = 0
            s["closed"][acc] = 0
            s["errors"][acc] = []
            s["spread_rejects"][acc] = 0
        s["action"] = "open"
        s["close_count"] = None
        s["fills"] = []
        s["last_trade_ts"] = {}
        s["rollback_needed"] = {}
        s["rollback_tickets"] = {}
        s["status"] = "active"
        s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_sessions()
        _log_event(session_id, None, "cycle_reset",
                   "Manual cycle reset — counters cleared, mode -> open")
    return jsonify(s)

@app.route('/api/sessions/<session_id>/set_mode', methods=['POST'])
def set_session_mode(session_id):
    """Set session mode to open, close, monitor, cycle_acc1, or cycle_acc2."""
    data = request.get_json(force=True) or {}
    mode = data.get("mode", "").lower()
    valid_modes = ("open", "close", "monitor", "cycle_acc1", "cycle_acc2")
    if mode not in valid_modes:
        return jsonify({"error": f"Invalid mode. Must be one of: {', '.join(valid_modes)}"}), 400
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        old_mode = s.get("action", "monitor")
        s["action"] = mode
        if mode == "close":
            # Set close_count to None = close all filled positions
            # This works correctly even after partial closes via buttons,
            # since _should_issue_command uses min(close_count, filled) as the target.
            s["close_count"] = None
        elif mode.startswith("cycle_"):
            # Initialize cycle tracking — clear cycle_days so cycling
            # waits for the user to enter a value before starting
            s["cycle_days"] = ""
            accs = list(s.get("sides", {}).keys())
            target_side_num = 1 if mode == "cycle_acc1" else 2
            cycle_account = ""
            for acc_key, side_info in s.get("sides", {}).items():
                if side_info.get("side_number") == target_side_num:
                    cycle_account = acc_key
                    break
            if cycle_account:
                s["cycle_account"] = cycle_account
                # Count fills excluding already-closed tickets (from manual close_deal)
                closed_tickets_set = set(
                    str(f.get("ticket")) for f in s.get("close_fills", [])
                    if f.get("account") == cycle_account
                )
                active_fills = [
                    f for f in s.get("fills", [])
                    if f.get("account") == cycle_account
                    and str(f.get("ticket")) not in closed_tickets_set
                ]
                cycle_total = len(active_fills)
                # Trim excess ACTIVE fills for BOTH accounts to match the cycling account's active count
                # (fills list may have stale entries from before rebalancing)
                # CRITICAL: only count active fills (not in close_fills) — after close→reopen,
                # the fills list has both old closed fills and new active fills.
                if cycle_total > 0:
                    other_account = [a for a in accs if a != cycle_account]
                    for trim_acct in [cycle_account] + other_account:
                        trim_target = cycle_total  # Both sides should match the cycling account's active count
                        # Build closed set for THIS account (not just cycle_account)
                        trim_closed_set = set(
                            str(f.get("ticket")) for f in s.get("close_fills", [])
                            if f.get("account") == trim_acct
                        )
                        # Only count active fill indices (not in close_fills)
                        acct_active_indices = [
                            i for i, f in enumerate(s.get("fills", []))
                            if f.get("account") == trim_acct
                            and str(f.get("ticket")) not in trim_closed_set
                        ]
                        if len(acct_active_indices) > trim_target:
                            # Remove excess active fills from the end
                            for remove_idx in reversed(acct_active_indices[trim_target:]):
                                s["fills"].pop(remove_idx)
                            print(f"[CYCLE] Trimmed {len(acct_active_indices) - trim_target} excess active fills for {trim_acct}")
                s["cycle_progress"] = {"phase": "close", "index": 0, "cycled": 0, "cycle_total": cycle_total}
                _log_event(session_id, cycle_account, "cycle_started",
                           f"Cycling {cycle_total} positions on {cycle_account}")
        # Activate session so commands can be issued
        if s["status"] in ("draft", "paused"):
            s["status"] = "active"
        s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_sessions()
        _log_event(session_id, None, "mode_changed",
                   f"Mode changed: {old_mode} -> {mode}")
    return jsonify(s)

@app.route('/api/sessions/<session_id>/filters', methods=['GET', 'PUT'])
def session_filters(session_id):
    """Get or update execution filter settings for a session."""
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        if request.method == 'GET':
            return jsonify({
                "max_ticks_per_5s": s.get("max_ticks_per_5s", 0),
                "max_price_jump": s.get("max_price_jump", 0),
                "require_diff_skew_open": s.get("require_diff_skew_open", ""),
                "require_diff_skew_close": s.get("require_diff_skew_close", ""),
            })
        else:
            data = request.get_json(force=True) or {}
            if "max_ticks_per_5s" in data:
                s["max_ticks_per_5s"] = int(data["max_ticks_per_5s"]) if data["max_ticks_per_5s"] else 0
            if "max_price_jump" in data:
                s["max_price_jump"] = float(data["max_price_jump"]) if data["max_price_jump"] else 0
            if "require_diff_skew_open" in data:
                s["require_diff_skew_open"] = str(data["require_diff_skew_open"]) if data["require_diff_skew_open"] else ""
            if "require_diff_skew_close" in data:
                s["require_diff_skew_close"] = str(data["require_diff_skew_close"]) if data["require_diff_skew_close"] else ""
            s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _save_sessions()
            _log_event(session_id, None, "filters_updated",
                       f"Filters updated: ticks={s.get('max_ticks_per_5s')}, "
                       f"jump={s.get('max_price_jump')}, "
                       f"skew_open={s.get('require_diff_skew_open')}, "
                       f"skew_close={s.get('require_diff_skew_close')}")
            return jsonify(s)

@app.route('/api/sessions/<session_id>/close_deal', methods=['POST'])
def close_deal(session_id):
    """Close a specific deal pair by ticket numbers. Uses rollback mechanism for immediate close."""
    data = request.get_json(force=True) or {}
    tickets = data.get("tickets", {})  # {account: ticket}
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        already_closed = set(str(f["ticket"]) for f in s.get("close_fills", []))
        closed_tickets = []
        # Clear any stale rollback state — user's explicit close takes priority
        rb = s.setdefault("rollback_needed", {})
        rb_tickets_map = s.setdefault("rollback_tickets", {})
        for acc, ticket in tickets.items():
            if ticket and str(ticket) not in already_closed:
                # Replace (not accumulate) — user action overrides pending rebalances
                rb[acc] = 1
                rb_tickets_map[acc] = [ticket]
                closed_tickets.append(f"{acc}:{ticket}")
        # Set cooldown so hedge monitor doesn't misinterpret the transient
        # imbalance (one side closes before the other) as structural.
        s["close_deal_ts"] = time.time()
        if s["status"] in ("draft", "paused"):
            s["status"] = "active"
        s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_sessions()
        _log_event(session_id, None, "close_deal",
                   f"Queued close for tickets: {', '.join(closed_tickets)}")
    return jsonify(s)

@app.route('/api/sessions/<session_id>/close_all_deals', methods=['POST'])
def close_all_deals(session_id):
    """Close all open deal pairs. Uses rollback mechanism for immediate close."""
    with lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "Session not found"}), 404
        accs = list(s.get("sides", {}).keys())
        already_closed = set(f["ticket"] for f in s.get("close_fills", []))
        for acc in accs:
            acct_fills = [f for f in s.get("fills", []) if f.get("account") == acc]
            for f in acct_fills:
                ticket = f.get("ticket")
                if ticket and ticket not in already_closed:
                    rb = s.setdefault("rollback_needed", {})
                    rb[acc] = rb.get(acc, 0) + 1
                    rb_tickets = s.setdefault("rollback_tickets", {})
                    rb_tickets.setdefault(acc, []).append(ticket)
        s["close_deal_ts"] = time.time()
        if s["status"] in ("draft", "paused"):
            s["status"] = "active"
        s["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_sessions()
        _log_event(session_id, None, "close_all_deals",
                   "Queued close for all open deal pairs")
    return jsonify(s)

# ─── EA Polling Endpoint ────────────────────────────────────────────────────

@app.route('/api/poll_command', methods=['GET'])
def poll_command():
    """
    EAs poll this endpoint with ?account=<account_number>
    Returns the next trade command to execute, or empty {} if nothing to do.
    """
    account = request.args.get("account", "").strip()
    if not account:
        return jsonify({"error": "Missing account parameter"}), 400

    # Global EA poll gate — operator can disable all EA polling from Settings
    if not dashboard_settings.get("ea_poll_enabled", True):
        return jsonify({})  # Silent empty response — EA will retry next heartbeat

    now_ts = time.time()
    with lock:
        ea_heartbeats[account] = now_ts

        # Store account info (balance, equity, quotes) from EA
        bal = request.args.get("balance")
        eq = request.args.get("equity")
        bid = request.args.get("bid")
        ask = request.args.get("ask")
        spd = request.args.get("spread")
        sym = request.args.get("symbol", "")
        lev = request.args.get("leverage")
        pos_str = request.args.get("positions")
        open_tickets_raw = request.args.get("open_tickets", "")
        if bal or eq or bid or ask:
            info = ea_account_info.get(account, {})

            # ── Tick rate & price delta tracking (for execution filters) ──
            if bid:
                new_bid = float(bid)
                prev_bid = info.get("bid", 0)
                if prev_bid > 0 and new_bid != prev_bid:
                    # Track tick timestamps (rolling 5s window)
                    tick_ts = info.get("tick_timestamps", [])
                    tick_ts.append(now_ts)
                    tick_ts = [t for t in tick_ts if now_ts - t <= 5.0]
                    info["tick_timestamps"] = tick_ts
                    info["ticks_per_5s"] = len(tick_ts)
                    # Track price delta in pips
                    pair = info.get("symbol", sym or "")
                    pip_mult = 1000 if "JPY" in pair.upper() else 100000
                    info["last_bid_delta"] = round(abs(new_bid - prev_bid) * pip_mult, 2)
                info["bid"] = new_bid

            if ask:
                new_ask = float(ask)
                prev_ask = info.get("ask", 0)
                if prev_ask > 0 and new_ask != prev_ask:
                    pair = info.get("symbol", sym or "")
                    pip_mult = 1000 if "JPY" in pair.upper() else 100000
                    info["last_ask_delta"] = round(abs(new_ask - prev_ask) * pip_mult, 2)
                info["ask"] = new_ask

            # ── Fee detection: track balance drops ──
            prev_balance = info.get("balance")
            if bal: info["balance"] = float(bal)
            if eq: info["equity"] = float(eq)
            if spd: info["spread"] = float(spd)
            if sym: info["symbol"] = sym
            if lev: info["leverage"] = int(lev)
            if pos_str is not None:
                info["positions"] = int(pos_str)
            # Parse open ticket list from EA (comma-separated)
            # ── MT Direct / FIX source priority guard ──────────────────────────
            # If this account is already managed by MT Direct or FIX (conn_type
            # set and last_update fresh within 30s), do NOT overwrite open_tickets
            # with EA-polled data. The EA reports ALL positions on the account
            # (across multiple sessions) which corrupts the hedge monitor's
            # per-session ticket matching.
            # Seen in incident 2026-05-28: ALEX-ICM-7415899 EA poll sent 200
            # mixed-session tickets, overwriting MT Direct's 100 session tickets
            # → hedge monitor detected 100 missing → false cascade.
            _existing_conn = info.get("conn_type", "")
            _existing_last_update = info.get("last_update", 0)
            _mt_direct_fresh = (
                _existing_conn in ("mt5_direct", "mt4_direct", "fix")
                and _existing_last_update > 0
                and (now_ts - _existing_last_update) <= 30
            )
            if _mt_direct_fresh:
                # MT Direct / FIX owns this account — skip open_tickets overwrite
                # Still allow balance/equity/quote/spread updates from EA
                if open_tickets_raw:
                    app.logger.debug(
                        "[POLL] %s: skipping EA open_tickets overwrite "
                        "(MT Direct/FIX has fresh data, conn_type=%s)",
                        account, _existing_conn
                    )
            else:
                if open_tickets_raw:
                    info["open_tickets"] = [_normalize_ticket(t) for t in open_tickets_raw.split(",") if t.strip()]
                else:
                    info["open_tickets"] = []
            info["last_update"] = now_ts
            ea_account_info[account] = info
            _log_market_stats(account, info)

            # Sync imported session filled count with EA's actual position count
            # Skip during cycling — mid-cycle the EA briefly reports fewer positions
            if pos_str is not None:
                ea_pos = int(pos_str)
                for _sid, _sess in sessions.items():
                    if _sess.get("imported") and account in _sess.get("sides", {}):
                        sess_action = _sess.get("action", "")
                        if sess_action.startswith("cycle_") or sess_action == "close":
                            break  # Don't sync during cycling or closing
                        old_filled = _sess.get("filled", {}).get(account, 0)
                        if ea_pos != old_filled:
                            _sess["filled"][account] = ea_pos
                            _save_sessions()
                        break

        # ── Hedge monitor now runs universally in background thread ──
        # See _run_hedge_monitor_all() — handles ALL account types (EA, MT Direct, FIX)


        # ── Check if there's a pending position report request for this account ──
        if pending_position_reports:
            print(f"[IMPORT-DBG] Checking account={account} against {len(pending_position_reports)} pending import(s)")
        for req_id, req in list(pending_position_reports.items()):
            print(f"[IMPORT-DBG] req_id={req_id[:8]} accounts={req['accounts']} received={list(req.get('received', {}).keys())} checking '{account}' in accounts={account in req['accounts']}")
            if account in req["accounts"] and account not in req.get("received", {}):
                # Expire old requests (60 seconds)
                if time.time() - req.get("ts", 0) > 60:
                    print(f"[IMPORT] Expired request {req_id[:8]}")
                    del pending_position_reports[req_id]
                    continue
                print(f"[IMPORT] Sending report_positions to account {account} for request {req_id[:8]}")
                return jsonify({
                    "action": "report_positions",
                    "session_id": req_id,
                })

        # Find the first active session that needs this account
        for sid, session in sessions.items():
            should = _should_issue_command(session, account)
            if should is False:
                # Debug: log why this session was skipped
                if session.get("status") == "active" and account in session.get("sides", {}):
                    action = session.get("action", "open")
                    if action == "close":
                        cc = session.get("close_count")
                        cl = session.get("closed", {}).get(account, 0)
                        fi = session.get("filled", {}).get(account, 0)
                        print(f"[DEBUG] poll skip {sid[:8]} acct={account}: action={action} closed={cl} close_count={cc} filled={fi}")
                continue

            side_info = session["sides"].get(account, {})

            # Per-side pair, lot_size, and max_spread (fall back to session-level defaults)
            side_pair = side_info.get("pair") or session["pair"]
            side_lots = side_info.get("lot_size") if side_info.get("lot_size") is not None else session["lot_size"]
            side_max_spread = side_info.get("max_spread") if side_info.get("max_spread") is not None else session["max_spread_points"]

            # Handle rollback: server tells EA to close 1 position to unwind the failed hedge
            if should == "rollback":
                cmd = {
                    "session_id": sid,
                    "action": "close",
                    "pair": side_pair,
                    "lots": side_lots,
                    "side": side_info.get("action", "buy"),
                    "max_spread": 9999,  # No spread limit for rollback
                    "comment": session["comment"],
                    "close_count": 1,
                    "already_closed": 0,
                    "is_rollback": True
                }
                # Include specific ticket to close if set by hedge rebalance
                rb_tickets = session.get("rollback_tickets", {}).get(account, [])
                if rb_tickets:
                    cmd["close_ticket"] = rb_tickets[0]  # First ticket in queue
                print(f"[CMD-TRACE] ROLLBACK to acct={account}: {cmd}")
                return jsonify(cmd)

            action = session.get("action", "open")

            # Handle cycle_close: close the specific position AND immediately reopen
            if should == "cycle_close":
                progress = session.get("cycle_progress", {})
                idx = progress.get("index", 0)

                # SAFETY: Check if a previous cycle close is stuck (reopen never happened)
                if progress.get("phase") == "open":
                    close_ts = progress.get("cycle_close_ts", 0)
                    if close_ts > 0 and (now_ts - close_ts) > 30:
                        retries = progress.get("open_retries", 0) + 1
                        max_retries = 3
                        if retries >= max_retries:
                            session["action"] = "monitor"
                            session["cycle_progress"] = {}
                            _save_sessions()
                            msg = (f"Cycle reopen TIMEOUT after {retries} attempts on "
                                   f"{account} — reverting to MONITOR")
                            print(f"[CYCLE-FAIL] {msg}")
                            _log_event(sid, account, "cycle_failed", msg)
                        else:
                            progress["open_retries"] = retries
                            progress.pop("open_dispatched", None)
                            progress["cycle_close_ts"] = time.time()  # Reset timer
                            session["cycle_progress"] = progress
                            _save_sessions()
                            msg = (f"Cycle reopen TIMEOUT on {account} "
                                   f"(attempt {retries}/{max_retries}) — will retry")
                            print(f"[CYCLE-RETRY] {msg}")
                            _log_event(sid, account, "cycle_retry", msg)
                        continue

                cycle_account = session.get("cycle_account", "")
                closed_set = set(str(cf.get("ticket")) for cf in session.get("close_fills", []) if cf.get("account") == cycle_account)
                acct_fills = [f for f in session.get("fills", []) if f.get("account") == cycle_account and str(f.get("ticket")) not in closed_set]
                
                # Sort oldest-first to match _should_issue_command's exact array indices
                def _fill_sort_key(f):
                    ts_str = f.get("ts", "")
                    if ts_str:
                        import re
                        s = str(ts_str).strip().replace("T", " ").rstrip("Z")
                        s = re.sub(r'\.\d+', '', s)
                        for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%Y.%m.%d %H:%M:%S", "%Y.%m.%d %H:%M"):
                            try:
                                return datetime.strptime(s, fmt).timestamp()
                            except (ValueError, TypeError):
                                continue
                    return f.get("ts_epoch", 0) or 0
                acct_fills.sort(key=_fill_sort_key)
                
                if idx < len(acct_fills):
                    ticket = acct_fills[idx].get("ticket", 0)
                else:
                    continue
                if True:
                    print(f"[POLL-DEBUG] cycle_account={cycle_account} idx={idx} sending close_ticket={ticket}")
                    cmd = {
                        "session_id": sid,
                        "action": "close",
                        "pair": side_pair,
                        "lots": side_lots,
                        "side": side_info.get("action", "buy"),
                        "max_spread": 9999,  # Cycle close: bypass spread check
                        "comment": side_info.get("comment") or session["comment"],
                        "close_count": 1,
                        "already_closed": 0,
                        "close_ticket": ticket,
                        "is_cycle": True,
                        # Tell EA to immediately reopen after closing
                        "cycle_reopen": True,
                        "reopen_side": side_info.get("action", "buy"),
                        "reopen_lots": side_lots,
                        "reopen_pair": side_pair,
                        "reopen_comment": side_info.get("comment") or session["comment"],
                    }
                    print(f"[CMD-TRACE] CYCLE_CLOSE to acct={account}: {cmd}")
                    in_flight_commands[(sid, account)] = time.time()
                    return jsonify(cmd)
                continue

            # If _should_issue_command returned False, skip — no command to send
            if not should:
                continue

            # Handle cycle open phase — build a normal open command
            if action.startswith("cycle_"):
                action_for_cmd = "open"  # During cycle open phase, command is "open"
            else:
                action_for_cmd = action
            # During cycle open phase, bypass spread check — open immediately
            cmd_max_spread = 9999 if (action.startswith("cycle_") and should is True) else side_max_spread

            cmd = {
                "session_id": sid,
                "action": action_for_cmd,
                "pair": side_pair,
                "lots": side_lots,
                "side": side_info.get("action", "buy"),
                "max_spread": cmd_max_spread,
                "comment": side_info.get("comment") or session["comment"]
            }

            if action_for_cmd == "close":
                match_mode = session.get("match_mode", "ticket")
                if match_mode == "lots":
                    # Lot-based close: determine the lots to close for this command
                    filled_lots = session.get("filled_lots", {}).get(account, 0)
                    closed_lots = session.get("closed_lots", {}).get(account, 0.0)
                    remaining_lots = round(filled_lots - closed_lots, 4)
                    if remaining_lots <= 0.001:
                        pass  # shouldn't reach here due to gating, but safety
                    else:
                        # Find the next unclosed fill for this account (FIFO)
                        acct_fills = [f for f in session.get("fills", []) if f.get("account") == account]
                        already_closed_count = session["closed"].get(account, 0)
                        if already_closed_count < len(acct_fills):
                            next_fill = acct_fills[already_closed_count]
                            fill_lots = next_fill.get("lots", side_lots)
                            # Partial close: only close what's needed
                            close_lots_for_cmd = round(min(fill_lots, remaining_lots), 4)
                            cmd["lots"] = close_lots_for_cmd
                            cmd["close_ticket"] = next_fill.get("ticket", 0)
                        cmd["close_count"] = 1  # always close one ticket at a time
                        cmd["already_closed"] = already_closed_count
                else:
                    # Ticket-by-ticket (original logic)
                    acct_filled = session["filled"].get(account, 0)
                    close_cap = session.get("close_count")
                    effective_close = min(close_cap, acct_filled) if close_cap is not None else acct_filled
                    cmd["close_count"] = effective_close
                    cmd["already_closed"] = session["closed"].get(account, 0)

                    # Include ticket number for the NEXT position to close.
                    # Do NOT use closed count as a fill-list index — imbalance
                    # rebalance increments closed on only one side, which desyncs
                    # the index-based pairing and causes cascade closes.
                    # Instead, find the first fill whose ticket is NOT in close_fills.
                    acct_fills = [f for f in session.get("fills", []) if f.get("account") == account]
                    acct_closed_tickets = set(
                        _normalize_ticket(cf["ticket"])
                        for cf in session.get("close_fills", [])
                        if cf.get("account") == account
                    )
                    # Also exclude tickets pending in rollback_tickets
                    pending_rb = set(
                        _normalize_ticket(t)
                        for t in session.get("rollback_tickets", {}).get(account, [])
                    )
                    for af in acct_fills:
                        t = _normalize_ticket(af.get("ticket", 0))
                        if t not in acct_closed_tickets and t not in pending_rb:
                            cmd["close_ticket"] = t
                            break

            print(f"[CMD-TRACE] NORMAL to acct={account}: {cmd}")
            # Mark command as in-flight to prevent duplicate sends
            in_flight_commands[(sid, account)] = time.time()
            return jsonify(cmd)

    # ── Hedge rebalance: close excess positions when hedge is unbalanced ──
    # Works without active sessions — purely position-based.
    # Comment format "ACCT1-ACCT2" identifies paired accounts.
    # If one side has more positions, close the excess on that side.
    acct_info = ea_account_info.get(account, {})
    this_pos = acct_info.get("positions", -1)
    this_symbol = acct_info.get("symbol", "")

    # Determine the comment grouping for this account
    # Check any session (active, completed, partial_close) to find comment pairing
    rebal_comment = ""
    rebal_pair = ""
    paired_account = ""
    for sid, session in sessions.items():
        side_info = session.get("sides", {}).get(account)
        if side_info:
            rebal_comment = side_info.get("comment") or session.get("comment", "")
            rebal_pair = side_info.get("pair") or session.get("pair", "")
            # Find the other account in this session
            for other_acc in session.get("sides", {}):
                if other_acc != account:
                    paired_account = other_acc
                    break
            if paired_account:
                break

    # Also try parsing from comment format "ACCT1-ACCT2" even without session
    if not paired_account and rebal_comment and "-" in rebal_comment:
        parts = rebal_comment.split("-")
        if len(parts) == 2:
            paired_account = parts[1] if parts[0] == account else parts[0]

    # Also try inferring from ea_account_info if no session found
    if not paired_account:
        # Scan all ea heartbeats to find accounts with matching comments
        for other_acc, other_info in ea_account_info.items():
            if other_acc == account:
                continue
            # Check if any session references both accounts
            for sid, session in sessions.items():
                sides = session.get("sides", {})
                if account in sides and other_acc in sides:
                    paired_account = other_acc
                    rebal_comment = session.get("comment", "")
                    rebal_pair = session.get("pair", "")
                    break
            if paired_account:
                break

    if paired_account and this_pos > 0:
        # ── HARD BLOCK: NEVER rebalance during cycling ──
        # Cycling deliberately creates temporary imbalances. Skip entirely.
        _is_cycling = False
        _in_cycle_cooldown = False
        for _sid, _sess in sessions.items():
            _sides = _sess.get("sides", {})
            if account in _sides and paired_account in _sides:
                if _sess.get("action", "").startswith("cycle_"):
                    _is_cycling = True
                # Post-cycle cooldown: don't rebalance for 30s after cycle completes
                _cyc_ts = _sess.get("cycle_complete_ts", 0)
                if _cyc_ts > 0 and (now_ts - _cyc_ts) < 30:
                    _in_cycle_cooldown = True
                break
        if _is_cycling or _in_cycle_cooldown:
            pass  # Skip rebalance entirely during cycling
        else:
            # Skip rebalance if there's an ACTIVE session managing both accounts.
            has_active_session = False
            has_any_session = False
            for _sid, _sess in sessions.items():
                _sides = _sess.get("sides", {})
                if account in _sides and paired_account in _sides:
                    has_any_session = True
                    if _sess.get("status") in ("active", "partial_close"):
                        has_active_session = True
                        break

            # For imported sessions, allow rebalance ONLY if session is ACTIVE
            # and NOT in cycle mode (cycling creates intentional temporary imbalances)
            is_imported_active = False
            for _sid, _sess in sessions.items():
                _sides = _sess.get("sides", {})
                if account in _sides and paired_account in _sides:
                    sess_action = _sess.get("action", "")
                    # Rebalancing is a safety mechanism — allow regardless of
                    # strategy running state. Only block during cycling.
                    if (_sess.get("imported")
                            and _sess.get("status") in ("active", "partial_close")
                            and not sess_action.startswith("cycle_")):
                        is_imported_active = True
                    break

            if has_active_session or has_any_session:
                pass  # Session-level hedge monitor handles rebalance — skip poll-level
            else:
                other_info = ea_account_info.get(paired_account, {})
                other_pos = other_info.get("positions", -1)
                other_last_update = other_info.get("last_update", 0)

                if other_pos >= 0 and (now_ts - other_last_update) < 60:
                    # Only close if THIS polling account has more positions than paired
                    if this_pos > other_pos:
                        excess = this_pos - other_pos
                        # Use EA's open_tickets for ticket-based close
                        close_ticket = None
                        open_tix = acct_info.get("open_tickets", [])
                        if open_tix and isinstance(open_tix, list):
                            close_ticket = open_tix[0]
                        cmd = {
                            "session_id": "rebalance",
                            "action": "close",
                            "pair": rebal_pair or this_symbol,
                            "lots": 0,
                            "side": "",
                            "max_spread": 9999,
                            "comment": "",
                            "close_count": 1,
                            "already_closed": 0
                        }
                        if close_ticket:
                            cmd["close_ticket"] = close_ticket
                        print(f"[CMD-TRACE] REBALANCE to acct={account} has {this_pos} pos, "
                              f"paired={paired_account} has {other_pos} pos, "
                              f"excess={excess}, ticket={close_ticket}")
                        # Log event to imported session for frontend notifications
                        for _sid, _sess in sessions.items():
                            if _sess.get("imported") and account in _sess.get("sides", {}):
                                _log_event(_sid, account, "rebalance_close",
                                           f"Imbalance detected: {account}={this_pos} vs {paired_account}={other_pos}, closing excess")
                                break
                        return jsonify(cmd)

    # Nothing to do — but tell the EA which symbol to report quotes for
    # and which comment to monitor for position counting
    # Find any session (active or not) that involves this account
    quote_sym = ""
    mon_comment = ""
    for sid, session in sessions.items():
        side_info = session.get("sides", {}).get(account)
        if side_info:
            quote_sym = side_info.get("pair") or session.get("pair", "")
            mon_comment = side_info.get("comment") or session.get("comment", "")
            break
    idle_resp = {}
    if quote_sym:
        idle_resp["quote_symbol"] = quote_sym
    if mon_comment:
        idle_resp["monitor_comment"] = mon_comment

    # ── Inject pending PnL request for this account ──
    for rid, preq in pnl_requests.items():
        if preq.get("status") != "pending":
            continue
        if account in preq.get("accounts", []) and account not in preq.get("results", {}):
            idle_resp["pnl_request"] = {
                "request_id": rid,
                "from_ts": preq["from_ts"],
                "to_ts": preq["to_ts"],
                "fee_keywords": preq.get("fee_keywords", []),
            }
            break

    return jsonify(idle_resp) if idle_resp else jsonify({})

# ─── EA Trade Result Endpoint ───────────────────────────────────────────────

@app.route('/api/trade_result', methods=['POST'])
def trade_result():
    """
    EA reports trade execution result.
    JSON: {session_id, account, status: "filled"|"spread_too_wide"|"error"|"closed"|"cycle_failed",
           ticket, spread, detail}
    """
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "Invalid JSON"}), 400

        session_id = data.get("session_id", "")
        account = str(data.get("account", ""))
        status = data.get("status", "")
        ticket = data.get("ticket")
        spread = data.get("spread")
        detail = data.get("detail", "")

        with lock:
            session = sessions.get(session_id)
            # Capture and clear in-flight flag for this account+session
            cmd_sent_ts = in_flight_commands.pop((session_id, account), None)
            in_flight_retry_counts.pop((session_id, account), None)  # Reset retry counter on fill
            if not session:
                if session_id == "rebalance":
                    # Rebalance close — update imported session filled count
                    print(f"[REBALANCE] Result from {account}: status={status} ticket={ticket}")
                    if status == "closed":
                        # Find the imported session containing this account and decrement filled
                        for _sid, _sess in sessions.items():
                            if _sess.get("imported") and account in _sess.get("sides", {}):
                                old_filled = _sess["filled"].get(account, 0)
                                if old_filled > 0:
                                    _sess["filled"][account] = old_filled - 1
                                    _save_sessions()
                                    print(f"[REBALANCE] Updated filled for {account}: {old_filled} -> {old_filled - 1}")
                                break
                    return jsonify({"ok": True, "rebalance": True})
                return jsonify({"error": "Session not found"}), 404

            if status == "filled":
                if not _cycle_handle_fill(session, account, data, cmd_sent_ts, session_id):
                    # Normal fill (open mode or non-cycle)
                    # ── Deduplication Guard ──
                    # Check if this ticket is already in our fills list to prevent duplicate reporting
                    # from inflating the dashboard's position count.
                    existing_tickets = {str(f.get("ticket")) for f in session.get("fills", []) if f.get("account") == account}
                    if str(ticket) in existing_tickets:
                        print(f"[TRADE_RESULT] Suppressed duplicate fill report for ticket={ticket} on account={account}")
                        return jsonify({"ok": True, "suppressed_duplicate": True})

                    session["filled"][account] = session["filled"].get(account, 0) + 1
                    session.setdefault("last_trade_ts", {})[account] = time.time()
                    fill_price = data.get("fill_price")
                    quote_price = data.get("quote_price")
                    # Determine fill lots from the trade data or session lot_size
                    side_info = session.get("sides", {}).get(account, {})
                    fill_lot_size = float(data.get("lots", 0) or 0)
                    if fill_lot_size <= 0:
                        fill_lot_size = side_info.get("lot_size") or session.get("lot_size", 0)
                    fills_list = session.setdefault("fills", [])
                    fills_list.append({
                        "account": account,
                        "ticket": ticket,
                        "price": float(fill_price) if fill_price else None,
                        "quote_price": float(quote_price) if quote_price else None,
                        "spread": int(spread) if spread else None,
                        "lots": fill_lot_size,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "ts_epoch": time.time(),
                        "cmd_ts": cmd_sent_ts,
                    })
                    # Track filled_lots for lot-mode sessions
                    if session.get("match_mode") == "lots":
                        fl = session.setdefault("filled_lots", {})
                        fl[account] = round(fl.get(account, 0.0) + fill_lot_size, 4)
                    _log_event(session_id, account, "trade_filled",
                               f"ticket={ticket} spread={spread} price={fill_price} lots={fill_lot_size} filled={session['filled'][account]}/{session['total_positions']}")
                _check_session_completion(session)

            elif status == "rollback_closed":
                if not _cycle_handle_close(session, account, data, session_id, cmd_sent_ts):
                    # Normal rollback/rebalance close
                    rb = session.get("rollback_needed", {})
                    rb[account] = max(0, rb.get(account, 0) - 1)
                    session["rollback_needed"] = rb

                    rb_tickets = session.get("rollback_tickets", {}).get(account, [])
                    if rb_tickets:
                        rb_tickets.pop(0)
                        if not rb_tickets:
                            session.get("rollback_tickets", {}).pop(account, None)

                    session["closed"][account] = session["closed"].get(account, 0) + 1
                    # Track closed lots for lot-mode
                    lot_val = float(data.get("lots", 0) or 0)
                    if lot_val <= 0:
                        _rb_fills = [f for f in session.get("fills", []) if f.get("account") == account]
                        _rb_idx = session["closed"].get(account, 1) - 1
                        if 0 <= _rb_idx < len(_rb_fills):
                            lot_val = _rb_fills[_rb_idx].get("lots", session.get("lot_size", 0))
                    _update_closed_lots(session, account, lot_val)
                    session.setdefault("last_trade_ts", {})[account] = time.time()

                    close_price = data.get("fill_price") or data.get("close_price") or data.get("price")
                    # Look up the matching open fill to preserve its open price/time
                    # (the fill may be replaced later by cycling, losing this data)
                    orig_fill = next(
                        (f for f in session.get("fills", [])
                         if f.get("account") == account and f.get("ticket") == ticket),
                        None
                    )
                    session.setdefault("close_fills", []).append({
                        "account": account,
                        "ticket": ticket,
                        "price": float(close_price) if close_price else None,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "ts_epoch": time.time(),
                        "cmd_ts": cmd_sent_ts,
                        "open_price": orig_fill.get("price") if orig_fill else None,
                        "open_ts": orig_fill.get("ts") if orig_fill else None,
                        "open_ts_epoch": orig_fill.get("ts_epoch") if orig_fill else None,
                    })

                    _log_event(session_id, account, "rollback_closed",
                               f"ticket={ticket} — rebalance close, remaining rollback={rb.get(account, 0)}")
                    # Clear or reset rollback timeout timer
                    if rb.get(account, 0) <= 0:
                        session.get("rollback_start_ts", {}).pop(account, None)
                        _log_event(session_id, account, "rollback_complete",
                                   "Hedge rebalance complete for this account.")
                    else:
                        # Reset timer for the next rollback in queue
                        session.setdefault("rollback_start_ts", {})[account] = time.time()
                    _check_session_completion(session)

            elif status == "closed":
                if not _cycle_handle_close(session, account, data, session_id, cmd_sent_ts):
                    # Not a cycle close — handle as normal close
                    action = session.get("action", "open")
                    # Normal close
                    session["closed"][account] = session["closed"].get(account, 0) + 1
                    # Track closed lots for lot-mode
                    _cl_lot = float(data.get("lots", 0) or 0)
                    if _cl_lot <= 0:
                        _cl_fills = [f for f in session.get("fills", []) if f.get("account") == account]
                        _cl_idx = session["closed"].get(account, 1) - 1
                        if 0 <= _cl_idx < len(_cl_fills):
                            _cl_lot = _cl_fills[_cl_idx].get("lots", session.get("lot_size", 0))
                    _update_closed_lots(session, account, _cl_lot)
                    session.setdefault("last_trade_ts", {})[account] = time.time()
                    close_price = data.get("fill_price") or data.get("close_price") or data.get("price")
                    session.setdefault("close_fills", []).append({
                        "account": account,
                        "ticket": ticket,
                        "price": float(close_price) if close_price else None,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "ts_epoch": time.time(),
                        "cmd_ts": cmd_sent_ts,
                    })
                    _log_event(session_id, account, "position_closed",
                               f"ticket={ticket} price={close_price} closed={session['closed'][account]}/{session.get('close_count','?')}")
                    _check_session_completion(session)

            elif status == "spread_too_wide":
                session["spread_rejects"][account] = session["spread_rejects"].get(account, 0) + 1
                _log_event(session_id, account, "spread_rejected",
                           f"spread={spread} max={session['max_spread_points']} rejects={session['spread_rejects'][account]}")

            elif status == "cycle_failed":
                # EA reports: cycle close succeeded but reopen FAILED
                # Revert to MONITOR so hedge monitor auto-rebalances
                session["action"] = "monitor"
                session["cycle_progress"] = {}
                _save_sessions()
                msg = (f"EA reported cycle_failed on {account} (detail: {detail}) — "
                       f"reverting to MONITOR for auto-rebalance")
                print(f"[CYCLE-FAIL] {msg}")
                _log_event(session_id, account, "cycle_failed", msg)

            elif status == "error":
                errors = session["errors"].get(account, [])
                errors.append({"ts": datetime.now().strftime("%H:%M:%S"), "detail": str(detail)[:200], "ticket": ticket})
                if len(errors) > 50:
                    errors = errors[-50:]
                session["errors"][account] = errors
                _log_event(session_id, account, "trade_error", detail)

                # ── Error cooldown: re-set in-flight with a 30s backoff so we don't
                # immediately flood the broker with retries after a permanent rejection.
                # (in_flight_commands was already popped at the top of trade_result;
                # setting it again here imposes: block = now - flight_ts < 10 → blocked
                # for 30s by setting flight_ts = now + 20.)
                flight_key = (session_id, account)
                in_flight_commands[flight_key] = time.time() + 20  # unblocks after 30s

                # ── Rollback cleanup: clear the rollback tracking on error ──
                # Do NOT record a close_fill or increment closed count —
                # the position was NOT actually closed (e.g. market closed, broker error).
                # The hedge monitor will re-detect if the position is actually gone.
                rb = session.get("rollback_needed", {})
                if rb.get(account, 0) > 0:
                    rb[account] = max(0, rb.get(account, 0) - 1)
                    session["rollback_needed"] = rb
                    rb_tickets = session.get("rollback_tickets", {}).get(account, [])
                    failed_ticket = rb_tickets.pop(0) if rb_tickets else None
                    if not rb_tickets:
                        session.get("rollback_tickets", {}).pop(account, None)
                    session.get("rollback_start_ts", {}).pop(account, None)
                    print(f"[ROLLBACK-ERR] acct={account}: rollback close failed "
                          f"(ticket={failed_ticket}, detail={detail}). "
                          f"NOT marking as closed. Remaining rollback={rb.get(account, 0)}")
                    _log_event(session_id, account, "rollback_error_cleared",
                               f"Rollback close failed for ticket={failed_ticket} — "
                               f"NOT marking closed (will re-detect). Remaining={rb.get(account, 0)}")

                # ── Cycle safety: if error during reopen phase, retry or abort ──
                action = session.get("action", "")
                if action.startswith("cycle_"):
                    progress = session.get("cycle_progress", {})
                    if progress.get("phase") == "open":
                        retries = progress.get("open_retries", 0) + 1
                        max_retries = 3
                        if retries >= max_retries:
                            session["action"] = "monitor"
                            session["cycle_progress"] = {}
                            _save_sessions()
                            msg = (f"Cycle reopen FAILED after {retries} attempts on "
                                   f"{account} (error: {detail}) — reverting to MONITOR")
                            print(f"[CYCLE-FAIL] {msg}")
                            _log_event(session_id, account, "cycle_failed", msg)
                        else:
                            # Retry: clear dispatch flag so next loop re-sends open
                            progress["open_retries"] = retries
                            progress.pop("open_dispatched", None)
                            session["cycle_progress"] = progress
                            _save_sessions()
                            msg = (f"Cycle reopen timeout on {account} "
                                   f"(attempt {retries}/{max_retries}, "
                                   f"error: {detail}) — will retry")
                            print(f"[CYCLE-RETRY] {msg}")
                            _log_event(session_id, account, "cycle_retry", msg)

                # ── Max-errors + rollback logic ──
                max_errors = session.get("max_errors", 1)
                total_errors = sum(len(v) for v in session.get("errors", {}).values())
                # Don't let max_errors pause during cycle open-phase — retry logic handles that
                in_cycle_open = (
                    session.get("action", "").startswith("cycle_") and
                    session.get("cycle_progress", {}).get("phase") == "open"
                )
                should_pause = max_errors > 0 and total_errors >= max_errors and not in_cycle_open

                if should_pause and session.get("action") == "open":
                    sides = session.get("sides", {})
                    my_filled = session["filled"].get(account, 0)
                    for other_acc in sides:
                        if other_acc == account:
                            continue
                        other_filled = session["filled"].get(other_acc, 0)
                        if other_filled > my_filled:
                            # Other side has more fills — needs rollback
                            diff = other_filled - my_filled
                            rb = session.setdefault("rollback_needed", {})
                            rb[other_acc] = rb.get(other_acc, 0) + diff
                            _log_event(session_id, other_acc, "rollback_triggered",
                                       f"Side error on {account} — scheduling {diff} rollback close(s) on {other_acc}")
                    session["status"] = "paused"
                    _log_event(session_id, account, "session_paused_on_error",
                               f"Max errors ({max_errors}) reached — session paused. Errors: {total_errors}")

            elif status == "no_positions":
                # EA reports it cannot find matching positions to close
                close_count = session.get("close_count", 0) or 0
                current_closed = session["closed"].get(account, 0)
                _log_event(session_id, account, "no_positions_found",
                           f"EA found no matching positions (closed {current_closed}/{close_count}). "
                           f"Check comment match / symbol on EA side.")
                print(f"[WARN] {account}: no_positions but only {current_closed}/{close_count} closed")

                # ── Rollback cleanup: EA has ZERO matching positions, clear ALL remaining ──
                rb = session.get("rollback_needed", {})
                if rb.get(account, 0) > 0:
                    remaining = rb.get(account, 0)
                    rb_tickets = session.get("rollback_tickets", {}).get(account, [])
                    # Record ALL queued tickets as externally closed
                    for t in rb_tickets:
                        session.setdefault("close_fills", []).append({
                            "account": account,
                            "ticket": t,
                            "price": None,
                            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "ts_epoch": time.time(),
                            "external": True,
                        })
                        session["closed"][account] = session["closed"].get(account, 0) + 1
                        # Track closed lots for lot-mode
                        _np_fills = [f for f in session.get("fills", []) if f.get("account") == account]
                        _np_idx = session["closed"].get(account, 1) - 1
                        _np_lot = 0
                        if 0 <= _np_idx < len(_np_fills):
                            _np_lot = _np_fills[_np_idx].get("lots", session.get("lot_size", 0))
                        _update_closed_lots(session, account, _np_lot)
                    # Clear all rollback state for this account
                    rb[account] = 0
                    session["rollback_needed"] = rb
                    session.get("rollback_tickets", {}).pop(account, None)
                    session.get("rollback_start_ts", {}).pop(account, None)
                    print(f"[ROLLBACK-NOPOS] acct={account}: EA has no matching positions. "
                          f"Cleared ALL {remaining} remaining rollback(s) "
                          f"(tickets={rb_tickets})")
                    _log_event(session_id, account, "rollback_nopos_cleared",
                               f"EA has no matching positions — cleared ALL {remaining} "
                               f"remaining rollback(s). Tickets treated as gone: {rb_tickets}")

                # Re-read closed count after rollback cleanup (may have been incremented)
                current_closed = session["closed"].get(account, 0)

                # Check if other side already closed — this means hedge is partially closed
                sides = session.get("sides", {})
                for other_acc in sides:
                    if other_acc == account:
                        continue
                    other_closed = session["closed"].get(other_acc, 0)
                    if other_closed > current_closed:
                        # Other side closed more — hedge is unbalanced, pause and alert
                        session["status"] = "partial_close"
                        _log_event(session_id, account, "partial_close_alert",
                                   f"ALERT: {other_acc} closed {other_closed} but {account} closed {current_closed}. "
                                   f"Hedge is unbalanced! Check EA comment/symbol match.")
                        print(f"[ALERT] PARTIAL CLOSE: {other_acc} closed {other_closed}, "
                              f"{account} closed {current_closed}. HEDGE UNBALANCED!")
                        break

            else:
                _log_event(session_id, account, f"unknown_status:{status}", detail)

            session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _save_sessions()

        return jsonify({"ok": True})
    except Exception as e:
        app.logger.exception("Error in trade_result")
        return jsonify({"error": str(e)}), 500

@app.route('/api/recalculate_fund_distributions', methods=['POST'])
def api_recalculate_fund_distributions():
    global _last_fund_dist_ts, _cached_fund_distributions
    with lock:
        now_ts = time.time()
        fix_accts = fix_manager.get_status() if fix_manager else {}
        mt_accts = mt_direct_manager.get_status() if mt_direct_manager else {}
        ea_status = {}
        for acc, last_ts in ea_heartbeats.items():
            info = ea_account_info.get(acc, {})
            ea_status[acc] = {
                "last_poll": datetime.fromtimestamp(last_ts).strftime("%H:%M:%S"),
                "ago_sec": round(now_ts - last_ts, 1),
                "online": (now_ts - last_ts) < 45,
                "label": info.get("label") or acc,
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "margin": info.get("margin") or info.get("margin_used"),
                "leverage": info.get("leverage"),
                "bid": info.get("bid"),
                "ask": info.get("ask"),
                "spread": info.get("spread"),
                "symbol": info.get("symbol", ""),
                "stats_log": acc in dashboard_settings.get("stats_log_accounts", []),
            }

        all_info_for_dist = {}
        for name, info in manual_accounts.items():
            all_info_for_dist[name] = {
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "margin": 0.0,
                "leverage": 100.0,
            }
        for aid, ainfo in ea_status.items():
            all_info_for_dist.setdefault(aid, {}).update(ainfo)
        for aid, ainfo in fix_accts.items():
            all_info_for_dist.setdefault(aid, {}).update(ainfo)
        for aid, ainfo in mt_accts.items():
            all_info_for_dist.setdefault(aid, {}).update(ainfo)
        
        _cached_fund_distributions = _calculate_optimal_fund_distributions(all_info_for_dist)
        _last_fund_dist_ts = now_ts

    return jsonify({"status": "ok"})

# ─── Status Endpoint ────────────────────────────────────────────────────────

@app.route('/api/status', methods=['GET'])
def api_status():
    include_fills = request.args.get('include_fills', '0') == '1'
    with lock:
        now_ts = time.time()
        ea_status = {}
        for acc, last_ts in ea_heartbeats.items():
            info = ea_account_info.get(acc, {})
            ea_status[acc] = {
                "last_poll": datetime.fromtimestamp(last_ts).strftime("%H:%M:%S"),
                "ago_sec": round(now_ts - last_ts, 1),
                "online": (now_ts - last_ts) < 45,
                "label": info.get("label") or acc,
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "margin": info.get("margin") or info.get("margin_used"),
                "leverage": info.get("leverage"),
                "bid": info.get("bid"),
                "ask": info.get("ask"),
                "spread": info.get("spread"),
                "symbol": info.get("symbol", ""),
                "stats_log": acc in dashboard_settings.get("stats_log_accounts", []),
            }

        # Attach live diff/spread data to each session for UI display
        enriched_sessions = []
        for s in sessions.values():
            sc = dict(s)  # shallow copy

            # Backfill fills from event log for sessions that predate fill tracking
            total_filled = sum(sc.get("filled", {}).values())
            if total_filled > 0 and not sc.get("fills"):
                backfilled = []
                for evt in event_log:
                    if evt.get("session_id") == sc["id"] and evt.get("event") == "trade_filled":
                        detail = evt.get("detail", "")
                        # Parse "ticket=12345 spread=5 price=154.040 filled=1/10"
                        t_match = re.search(r'ticket=(\d+)', detail)
                        s_match = re.search(r'spread=(\d+)', detail)
                        p_match = re.search(r'price=([\d.]+)', detail)
                        backfilled.append({
                            "account": evt.get("account", ""),
                            "ticket": int(t_match.group(1)) if t_match else None,
                            "price": float(p_match.group(1)) if p_match else None,
                            "spread": int(s_match.group(1)) if s_match else None,
                            "ts": evt.get("ts", ""),
                            "ts_epoch": None,
                        })
                if backfilled:
                    sc["fills"] = backfilled

            diff_open, reason_open = _calc_curr_diff(s, "open")
            diff_close, reason_close = _calc_curr_diff(s, "close")
            sc["curr_diff_open"] = diff_open
            sc["curr_diff_close"] = diff_close
            sc["diff_reason"] = reason_open or reason_close  # same root cause for both
            # Attach per-side current spreads and EA symbol from live EA data
            for acc, side_info in s.get("sides", {}).items():
                sn = side_info.get("side_number", 0)
                if sn not in (1, 2):
                    continue
                ai = ea_account_info.get(acc, {})
                side_pair = (side_info.get("pair") or s.get("pair", "")).upper()

                # For MT Direct accounts, always show whatever quote data is available
                # (the command loop's subscribe_symbol already ensures the right data is pushed)
                conn_type = ai.get("conn_type", "")
                is_direct = conn_type in ("mt4_direct", "mt5_direct") or (
                    mt_direct_manager and acc in mt_direct_manager.accounts)
                if is_direct:
                    # ALWAYS query the correct instrument's quote directly
                    # (ea_account_info only caches ONE symbol — unreliable for multi-instrument)
                    direct_acct = mt_direct_manager.accounts.get(acc) if mt_direct_manager else None
                    got_direct = False
                    if direct_acct and side_pair:
                        try:
                            sym_quote = direct_acct.get_symbol_info(side_pair)
                            if sym_quote and sym_quote.get("bid") and sym_quote.get("ask"):
                                sc[f"curr_spread_{sn}"] = sym_quote.get("spread")
                                sc[f"curr_bid_{sn}"] = sym_quote.get("bid")
                                sc[f"curr_ask_{sn}"] = sym_quote.get("ask")
                                got_direct = True
                        except Exception:
                            pass
                        # Fallback: try direct CLR GetQuote
                        if not got_direct and hasattr(direct_acct, 'get_quote_direct'):
                            try:
                                dq = direct_acct.get_quote_direct(side_pair)
                                if dq and dq.get("bid") and dq.get("ask"):
                                    sc[f"curr_spread_{sn}"] = dq.get("spread")
                                    sc[f"curr_bid_{sn}"] = dq.get("bid")
                                    sc[f"curr_ask_{sn}"] = dq.get("ask")
                                    got_direct = True
                            except Exception:
                                pass
                    if not got_direct:
                        # Last resort: ea_account_info for SPD display (cosmetic)
                        sc[f"curr_spread_{sn}"] = ai.get("spread")
                        sc[f"curr_bid_{sn}"] = ai.get("bid")
                        sc[f"curr_ask_{sn}"] = ai.get("ask")
                    sc[f"ea_symbol_{sn}"] = side_pair
                else:
                    # Try to query the actual connector for the correct instrument
                    got_quote = False
                    fix_acct = fix_manager.accounts.get(acc) if fix_manager else None
                    if fix_acct and hasattr(fix_acct, 'get_symbol_info') and side_pair:
                        try:
                            sq = fix_acct.get_symbol_info(side_pair)
                            if sq and sq.get("bid") and sq.get("ask"):
                                sc[f"curr_spread_{sn}"] = sq.get("spread")
                                sc[f"curr_bid_{sn}"] = sq.get("bid")
                                sc[f"curr_ask_{sn}"] = sq.get("ask")
                                got_quote = True
                        except Exception:
                            pass
                    if not got_quote:
                        # Fallback to ea_account_info (may be wrong instrument)
                        ea_sym = (ai.get("symbol") or "").upper()
                        sym_match = not ea_sym or not side_pair or ea_sym.replace("/", "") == side_pair.replace("/", "")
                        if sym_match:
                            sc[f"curr_spread_{sn}"] = ai.get("spread")
                            sc[f"curr_bid_{sn}"] = ai.get("bid")
                            sc[f"curr_ask_{sn}"] = ai.get("ask")
                        else:
                            sc[f"curr_spread_{sn}"] = None
                            sc[f"curr_bid_{sn}"] = None
                            sc[f"curr_ask_{sn}"] = None
                    sc[f"ea_symbol_{sn}"] = side_pair
            # Strip heavy fills data unless explicitly requested (saves ~28KB+ per poll)
            if not include_fills:
                sc.pop('fills', None)
                sc.pop('close_fills', None)
            enriched_sessions.append(sc)

        # News blackout status
        news_blocked, news_reason = is_news_blackout(impact_filter="High")

        fix_accts = fix_manager.get_status() if fix_manager else {}
        mt_accts = mt_direct_manager.get_status() if mt_direct_manager else {}

        # Enrich account status with position age from cycle config + live positions
        def _enrich_age(accts, manager):
            if not manager:
                return
            try:
                for acct_id, entry in accts.items():
                    acct = manager.accounts.get(acct_id)
                    if not acct or not hasattr(acct, 'config'):
                        continue
                    cfg = acct.config or {}
                    
                    # Primary: get oldest position open time from ea_account_info
                    # (position_details has broker-reported open_epoch for each position)
                    oldest_epoch = None
                    acct_info = ea_account_info.get(acct_id, {})
                    positions = acct_info.get("position_details") or acct_info.get("positions", [])
                    open_tickets = set(acct_info.get("open_tickets", []))
                    if isinstance(positions, list):
                        for pos in positions:
                            if not isinstance(pos, dict):
                                continue
                            oe = pos.get("open_epoch")
                            if oe and (oldest_epoch is None or oe < oldest_epoch):
                                oldest_epoch = oe
                    # Fallback: check session fills if no position data available
                    # Only use fills whose tickets match currently open positions
                    # to avoid stale timestamps from historical fills.
                    # Skip entirely when open_tickets is empty — no positions means
                    # no age to compute.
                    if oldest_epoch is None and open_tickets:
                        for sid, sess in list(sessions.items()):
                            if sess.get("status") not in ("active", "paused", "partial_close"):
                                continue
                            if acct_id not in sess.get("sides", {}):
                                continue
                            for f in sess.get("fills", []):
                                if f.get("account") != acct_id:
                                    continue
                                # Only consider fills for currently open tickets
                                ft = f.get("ticket")
                                if ft and ft not in open_tickets:
                                    continue
                                fe = f.get("ts_epoch") or f.get("open_epoch")
                                if fe and (oldest_epoch is None or fe < oldest_epoch):
                                    oldest_epoch = fe
                    if oldest_epoch:
                        entry["oldest_position_age"] = _count_rollover_days(oldest_epoch)
                    else:
                        # No open positions — clear any stale age value
                        entry.pop("oldest_position_age", None)

                    if cfg.get("cycle_reminder_enabled"):
                        entry["cycle_remind_days"] = cfg.get("cycle_reminder_days")
                        entry["cycle_max_days"] = cfg.get("cycle_max_days")
                    else:
                        entry.pop("cycle_remind_days", None)
                        entry.pop("cycle_max_days", None)
            except Exception as e:
                print(f"[CYCLE-AGE] Error enriching age for accounts: {e}")

        _enrich_age(fix_accts, fix_manager)
        _enrich_age(mt_accts, mt_direct_manager)

        # ── Margin alert check (runs on every status poll) ──────────────
        try:
            _all_acct_info = {}
            for aid, ainfo in fix_accts.items():
                _all_acct_info[aid] = ainfo
            for aid, ainfo in mt_accts.items():
                _all_acct_info[aid] = ainfo
            for aid, ainfo in ea_status.items():
                if aid not in _all_acct_info:
                    _all_acct_info[aid] = ainfo
            _check_margin_alerts(_all_acct_info)
            _check_position_changes(_all_acct_info)
        except Exception:
            pass

        # Include margin alert thresholds in response for frontend
        margin_alert_data = {
            "global_threshold": dashboard_settings.get("margin_alert_threshold", 85),
            "per_account": dashboard_settings.get("margin_alert_thresholds", {}),
        }

        # Calculate optimal fund distributions
        global _last_fund_dist_ts, _cached_fund_distributions
        force_recalc = request.args.get('force_recalc', '0') == '1'
        # Treat an all-zero cache as invalid (startup race: accounts not yet connected)
        _cache_all_zero = _cached_fund_distributions and all(
            v.get("optimal_equity", 0) == 0 for v in _cached_fund_distributions.values()
        )
        if force_recalc or now_ts - _last_fund_dist_ts >= 3600 or not _cached_fund_distributions or _cache_all_zero:
            all_info_for_dist = {}
            for name, info in manual_accounts.items():
                all_info_for_dist[name] = {
                    "balance": info.get("balance"),
                    "equity": info.get("equity"),
                    "margin": 0.0,
                    "leverage": 100.0,
                }
            for aid, ainfo in ea_status.items():
                all_info_for_dist.setdefault(aid, {}).update(ainfo)
            for aid, ainfo in fix_accts.items():
                all_info_for_dist.setdefault(aid, {}).update(ainfo)
            for aid, ainfo in mt_accts.items():
                all_info_for_dist.setdefault(aid, {}).update(ainfo)
            
            _cached_fund_distributions = _calculate_optimal_fund_distributions(all_info_for_dist)
            _last_fund_dist_ts = now_ts

        dist_last_updated = datetime.fromtimestamp(_last_fund_dist_ts).strftime("%H:%M:%S") if _last_fund_dist_ts > 0 else "-"

        return jsonify({
            "sessions": enriched_sessions,
            "ea_heartbeats": ea_status,
            "event_log": event_log[-100:],
            "manual_accounts": manual_accounts,
            "strategies": list(strategies.values()),
            "fix_accounts": fix_accts,
            "mt_direct_accounts": mt_accts,
            "cycle_reminders": cycle_reminders,
            "news_blackout": {"blocked": news_blocked, "event": news_reason},
            "margin_alert": margin_alert_data,
            "swap_delta": _compute_swap_deltas_live(),
            "fund_distributions": _cached_fund_distributions,
            "fund_distributions_last_updated": dist_last_updated,
        })

# ─── Lots Breakdown by Instrument ──────────────────────────────────────────

@app.route('/api/lots_breakdown', methods=['GET'])
def api_lots_breakdown():
    """Aggregate lots by instrument across all accounts (or filtered accounts if ?account= is given).
    Supports comma-separated account IDs for group breakdown."""
    account_filter = request.args.get("account", "").strip()
    with lock:
        totals = {}  # symbol -> {"buy": x, "sell": y}
        seen = set()
        def _merge(acct_id):
            if acct_id in seen:
                return
            seen.add(acct_id)
            lbi = ea_account_info.get(acct_id, {}).get("lots_by_instrument", {})
            for sym, vals in lbi.items():
                if sym not in totals:
                    totals[sym] = {"buy": 0, "sell": 0}
                totals[sym]["buy"] = round(totals[sym]["buy"] + vals.get("buy", 0), 2)
                totals[sym]["sell"] = round(totals[sym]["sell"] + vals.get("sell", 0), 2)
        if account_filter:
            # Single or comma-separated accounts
            for aid in account_filter.split(","):
                aid = aid.strip()
                if aid:
                    _merge(aid)
        else:
            # Merge all account sources
            if fix_manager:
                for aid in fix_manager.get_status():
                    _merge(aid)
            if mt_direct_manager:
                for aid in mt_direct_manager.get_status():
                    _merge(aid)
            for aid in list(ea_account_info.keys()):
                _merge(aid)
        # Build sorted result
        result = []
        for sym in sorted(totals.keys()):
            v = totals[sym]
            net = round(v["buy"] - v["sell"], 2)
            result.append({"symbol": sym, "buy": v["buy"], "sell": v["sell"], "net": net})
        return jsonify(result)

# ─── Swap Breakdown by Instrument ──────────────────────────────────────────

@app.route('/api/swap_breakdown', methods=['GET'])
def api_swap_breakdown():
    """Aggregate swap delta by instrument across all accounts (or filtered accounts if ?account= is given).
    Supports comma-separated account IDs for group breakdown.
    """
    account_filter = request.args.get("account", "").strip()
    with lock:
        totals = {}  # symbol -> {"lots": 0.0, "delta_swap": 0.0}
        seen = set()
        
        # Get delta_by_instrument (computed live or cached from yesterday)
        delta_by_inst = _swap_delta.get("delta_by_instrument", {})
        
        def _merge(acct_id):
            if acct_id in seen:
                return
            seen.add(acct_id)
            
            # Current lots for this account
            info = ea_account_info.get(acct_id, {})
            lbi = info.get("lots_by_instrument", {})
            
            # Delta swap for this account
            inst_deltas = delta_by_inst.get(acct_id, {})
            
            # Union of symbols in lots or delta swap
            all_syms = set(lbi.keys()) | set(inst_deltas.keys())
            
            for sym in all_syms:
                # Sum buy + sell lots to get total lots open
                vals = lbi.get(sym, {})
                lots_val = round(vals.get("buy", 0.0) + vals.get("sell", 0.0), 2)
                
                d_swap = inst_deltas.get(sym, 0.0)
                
                if sym not in totals:
                    totals[sym] = {"lots": 0.0, "delta_swap": 0.0}
                totals[sym]["lots"] = round(totals[sym]["lots"] + lots_val, 2)
                totals[sym]["delta_swap"] = round(totals[sym]["delta_swap"] + d_swap, 2)
                
        if account_filter:
            for aid in account_filter.split(","):
                aid = aid.strip()
                if aid:
                    _merge(aid)
        else:
            # Merge all account sources
            if fix_manager:
                for aid in fix_manager.get_status():
                    _merge(aid)
            if mt_direct_manager:
                for aid in mt_direct_manager.get_status():
                    _merge(aid)
            for aid in list(ea_account_info.keys()):
                _merge(aid)
                
        # Build sorted result
        result = []
        for sym in sorted(totals.keys()):
            v = totals[sym]
            lots = v["lots"]
            d_swap = v["delta_swap"]
            # Per-lot swap delta: if lots > 0, compute. Else "-"
            if lots > 0:
                per_lot = round(d_swap / lots, 2)
            else:
                per_lot = "-"
            
            # Only include if there is some lots or non-zero delta_swap
            if lots > 0 or d_swap != 0:
                result.append({
                    "symbol": sym,
                    "lots": lots,
                    "total_delta_swap": d_swap,
                    "per_lot_delta_swap": per_lot
                })
        return jsonify(result)

# ─── Event Log Endpoint ────────────────────────────────────────────────────

@app.route('/api/events', methods=['GET'])
def api_events():
    limit = int(request.args.get("limit", "100"))
    with lock:
        return jsonify(event_log[-limit:])

# ─── Account Management ────────────────────────────────────────────────────

@app.route('/api/accounts', methods=['POST'])
def add_account():
    """Add a manual account (for FIX API, etc.)"""
    try:
        data = request.get_json(force=True)
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "Account name is required"}), 400

        conn_type = data.get("conn_type", "manual")  # manual, fix, poll
        group_label = str(data.get("group_label", "")).strip()
        with lock:
            manual_accounts[name] = {
                "conn_type": conn_type,
                "group_label": group_label,
                "balance": data.get("balance"),
                "equity": data.get("equity"),
            }
        _log_event(None, name, "account_added", f"type={conn_type}")
        _save_strategies()
        return jsonify({"ok": True, "name": name})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/accounts/<account_name>', methods=['PATCH'])
def update_account(account_name):
    """Update fields on a manual account (e.g. group_label)."""
    try:
        data = request.get_json(force=True)
        with lock:
            acct = manual_accounts.get(account_name)
            if not acct:
                # Auto-create entry for EA-discovered accounts
                acct = {"conn_type": "poll", "group_label": "", "balance": None, "equity": None}
                manual_accounts[account_name] = acct
            if "group_label" in data:
                acct["group_label"] = str(data["group_label"]).strip()
            if "stop_out_level" in data:
                try:
                    acct["stop_out_level"] = float(data["stop_out_level"]) if data["stop_out_level"] is not None and str(data["stop_out_level"]).strip() != "" else None
                except (ValueError, TypeError):
                    pass
            if "conn_type" in data:
                acct["conn_type"] = str(data["conn_type"]).strip()
            if "alert_email" in data:
                acct["alert_email"] = str(data["alert_email"]).strip() if data["alert_email"] else None
            if "alert_telegram" in data:
                acct["alert_telegram"] = str(data["alert_telegram"]).strip() if data["alert_telegram"] else None
            if "fee_threshold" in data:
                try:
                    acct["fee_threshold"] = float(data["fee_threshold"])
                    dashboard_settings.setdefault("fee_thresholds", {})[account_name] = acct["fee_threshold"]
                    _save_settings()
                except (ValueError, TypeError):
                    pass
            if "stats_log" in data:
                sla = dashboard_settings.setdefault("stats_log_accounts", [])
                if data["stats_log"]:
                    if account_name not in sla:
                        sla.append(account_name)
                else:
                    if account_name in sla:
                        sla.remove(account_name)
                _save_settings()
            if "margin_alert_threshold" in data:
                try:
                    val = data["margin_alert_threshold"]
                    if val is not None and val != "" and float(val) > 0:
                        dashboard_settings.setdefault("margin_alert_thresholds", {})[account_name] = float(val)
                    else:
                        dashboard_settings.get("margin_alert_thresholds", {}).pop(account_name, None)
                    _save_settings()
                except (ValueError, TypeError):
                    pass

            # Sync to FIX accounts config if it exists
            if fix_manager and account_name in fix_manager.accounts:
                fix_acct = fix_manager.accounts[account_name]
                changed = False
                if "group_label" in data:
                    fix_acct.config["group_label"] = str(data["group_label"]).strip()
                    changed = True
                if "stop_out_level" in data:
                    try:
                        fix_acct.config["stop_out_level"] = float(data["stop_out_level"]) if data["stop_out_level"] is not None and str(data["stop_out_level"]).strip() != "" else None
                        changed = True
                    except (ValueError, TypeError):
                        pass
                if "alert_email" in data:
                    fix_acct.config["alert_email"] = str(data["alert_email"]).strip() if data["alert_email"] else None
                    changed = True
                if "alert_telegram" in data:
                    fix_acct.config["alert_telegram"] = str(data["alert_telegram"]).strip() if data["alert_telegram"] else None
                    changed = True
                if "auto_connect_start" in data:
                    fix_acct.config["auto_connect_start"] = bool(data["auto_connect_start"])
                    changed = True
                if changed:
                    fix_manager.save_config()

            # Sync to MT Direct accounts config if it exists
            if mt_direct_manager and account_name in mt_direct_manager.accounts:
                mt_acct = mt_direct_manager.accounts[account_name]
                changed = False
                if "group_label" in data:
                    mt_acct.config["group_label"] = str(data["group_label"]).strip()
                    changed = True
                if "stop_out_level" in data:
                    try:
                        mt_acct.config["stop_out_level"] = float(data["stop_out_level"]) if data["stop_out_level"] is not None and str(data["stop_out_level"]).strip() != "" else None
                        changed = True
                    except (ValueError, TypeError):
                        pass
                if "alert_email" in data:
                    mt_acct.config["alert_email"] = str(data["alert_email"]).strip() if data["alert_email"] else None
                    changed = True
                if "alert_telegram" in data:
                    mt_acct.config["alert_telegram"] = str(data["alert_telegram"]).strip() if data["alert_telegram"] else None
                    changed = True
                if "auto_connect_start" in data:
                    mt_acct.config["auto_connect_start"] = bool(data["auto_connect_start"])
                    changed = True
                if changed:
                    from mt_bridge_client import normalize_mt_config
                    mt_acct.config = normalize_mt_config(mt_acct.config)
                    mt_direct_manager.save_config()

        # Handle connection type switching (EA ↔ MT Direct)
        # ONLY if conn_type was explicitly provided in the request
        if "conn_type" in data:
            new_conn = data["conn_type"]
            is_direct = new_conn in ("mt4_direct", "mt5_direct")
            was_direct = mt_direct_manager and account_name in mt_direct_manager.accounts

            if is_direct and mt_direct_manager and "mt_direct" in data:
                # Switching TO MT Direct (or updating Direct config)
                mt_cfg = data["mt_direct"]
                mt_cfg["label"] = mt_cfg.get("label", f"MT-{account_name}")
                if "alert_email" in data:
                    mt_cfg["alert_email"] = data["alert_email"]
                if "alert_telegram" in data:
                    mt_cfg["alert_telegram"] = data["alert_telegram"]
                if was_direct:
                    # Update existing: stop, remove, re-add
                    mt_direct_manager.remove_account(account_name)
                mt_direct_manager.add_account(account_name, mt_cfg)
                _log_event(None, account_name, "conn_type_switched",
                           f"→ {new_conn} server={mt_cfg.get('server')}")

            elif not is_direct and was_direct:
                # Switching AWAY from MT Direct (back to EA/Manual)
                mt_direct_manager.remove_account(account_name)
                _log_event(None, account_name, "conn_type_switched",
                           f"→ {new_conn} (Direct disconnected)")

        _save_strategies()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/accounts/<account_name>', methods=['DELETE'])
def delete_account(account_name):
    """Remove an account from the dashboard (manual, EA, or MT Direct)."""
    with lock:
        manual_accounts.pop(account_name, None)
        # Clean up stale heartbeat/account info data
        ea_account_info.pop(account_name, None)
        ea_heartbeats.pop(account_name, None)
    # Also remove from MT Direct if applicable
    if mt_direct_manager and account_name in mt_direct_manager.accounts:
        mt_direct_manager.remove_account(account_name)
    _log_event(None, account_name, "account_removed", "")
    _save_strategies()
    return jsonify({"ok": True})

# ─── Quote Stats Report Generation ─────────────────────────────────────────

_report_jobs = {}  # job_id -> {status, started, finished, args, error, filename}

def _run_report_job(job_id, args):
    """Background thread to run analyze_quote_stats.py."""
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analyze_quote_stats.py")
    cmd = [sys.executable, script_path, "--export"] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        _report_jobs[job_id]["stdout"] = result.stdout
        _report_jobs[job_id]["stderr"] = result.stderr
        if result.returncode == 0:
            match = re.search(r'analysis_[\d_-]+\.html', result.stdout)
            _report_jobs[job_id]["filename"] = match.group(0) if match else None
            _report_jobs[job_id]["status"] = "done"
        else:
            _report_jobs[job_id]["status"] = "error"
            _report_jobs[job_id]["error"] = result.stderr or result.stdout or "Unknown error"
    except subprocess.TimeoutExpired:
        _report_jobs[job_id]["status"] = "error"
        _report_jobs[job_id]["error"] = "Report generation timed out (120s)"
    except Exception as e:
        _report_jobs[job_id]["status"] = "error"
        _report_jobs[job_id]["error"] = str(e)
    _report_jobs[job_id]["finished"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@app.route('/api/reports/generate', methods=['POST'])
def generate_report():
    """Trigger quote stats report generation in background."""
    data = request.get_json(force=True) if request.is_json else {}
    args = []
    if data.get("account"):
        args.extend(["--account", str(data["account"])])
    if data.get("pair"):
        args.extend(["--pair", str(data["pair"])])
    if data.get("days"):
        args.extend(["--days", str(int(data["days"]))])
    if data.get("top"):
        args.extend(["--top", str(int(data["top"]))])

    job_id = str(uuid.uuid4())[:8]
    _report_jobs[job_id] = {
        "status": "running",
        "started": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "finished": None,
        "args": args,
        "error": None,
        "filename": None,
    }
    t = threading.Thread(target=_run_report_job, args=(job_id, args), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": job_id})

@app.route('/api/reports/status/<job_id>', methods=['GET'])
def report_job_status(job_id):
    """Check status of a report generation job."""
    job = _report_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)

@app.route('/api/reports', methods=['GET'])
def list_reports():
    """List available HTML reports."""
    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stats", "reports")
    if not os.path.isdir(reports_dir):
        return jsonify([])
    files = sorted(
        [f for f in os.listdir(reports_dir) if f.endswith(".html")],
        reverse=True
    )
    result = []
    for f in files[:20]:
        fp = os.path.join(reports_dir, f)
        result.append({
            "filename": f,
            "size": os.path.getsize(fp),
            "created": datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return jsonify(result)

@app.route('/api/reports/<filename>', methods=['GET', 'DELETE'])
def serve_report(filename):
    """Serve or delete a generated HTML report."""
    if '..' in filename or '/' in filename or '\\' in filename:
        return jsonify({"error": "Invalid filename"}), 400
    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stats", "reports")
    filepath = os.path.join(reports_dir, filename)
    if not os.path.isfile(filepath):
        return jsonify({"error": "Report not found"}), 404
    if request.method == 'DELETE':
        os.remove(filepath)
        return jsonify({"ok": True})
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read(), 200, {'Content-Type': 'text/html; charset=utf-8'}

# ─── FIX Account Management ────────────────────────────────────────────────

@app.route('/api/fix_accounts', methods=['GET'])
def list_fix_accounts():
    """List all configured FIX accounts and their connection status."""
    if not fix_manager:
        return jsonify({"error": "FIX connector not available"}), 501
    return jsonify(fix_manager.get_status())

@app.route('/api/fix_accounts', methods=['POST'])
def add_fix_account():
    """Add a new FIX account. JSON body = full config dict."""
    if not fix_manager:
        return jsonify({"error": "FIX connector not available"}), 501
    try:
        data = request.get_json(force=True)
        account_id = str(data.pop("account_id", "")).strip()
        auto_connect = data.pop("auto_connect", True)
        if not account_id:
            return jsonify({"error": "account_id is required"}), 400
        ok = fix_manager.add_account(account_id, data, auto_connect=auto_connect)
        if not ok:
            return jsonify({"error": f"Account {account_id} already exists"}), 409
        _log_event(None, account_id, "fix_account_added",
                   f"host={data.get('host')} label={data.get('label')}")
        return jsonify({"ok": True, "account_id": account_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/fix_accounts/<account_id>', methods=['DELETE'])
def remove_fix_account(account_id):
    """Stop and remove a FIX account."""
    if not fix_manager:
        return jsonify({"error": "FIX connector not available"}), 501
    ok = fix_manager.remove_account(account_id)
    if not ok:
        return jsonify({"error": "Account not found"}), 404
    _log_event(None, account_id, "fix_account_removed", "")
    return jsonify({"ok": True})

@app.route('/api/fix_accounts/<account_id>/reconnect', methods=['POST'])
def reconnect_fix_account(account_id):
    """Stop and restart a FIX account connection."""
    if not fix_manager:
        return jsonify({"error": "FIX connector not available"}), 501
    acct = fix_manager.accounts.get(account_id)
    if not acct:
        return jsonify({"error": "Account not found"}), 404
    acct.stop()
    threading.Thread(target=acct.start, daemon=True,
                     name=f"FIX-Reconnect-{account_id}").start()
    return jsonify({"ok": True})

@app.route('/api/fix_accounts/<account_id>/disconnect', methods=['POST'])
def disconnect_fix_account(account_id):
    """Stop a FIX account connection without removing it."""
    if not fix_manager:
        return jsonify({"error": "FIX connector not available"}), 501
    acct = fix_manager.accounts.get(account_id)
    if not acct:
        return jsonify({"error": "Account not found"}), 404
    acct.stop()
    _log_event(None, account_id, "fix_account_disconnected", "")
    return jsonify({"ok": True})

@app.route('/api/fix_accounts/<account_id>/config', methods=['GET'])
def get_fix_account_config(account_id):
    """Get the config of a FIX account for editing."""
    if not fix_manager:
        return jsonify({"error": "FIX connector not available"}), 501
    acct = fix_manager.accounts.get(account_id)
    if not acct:
        return jsonify({"error": "Account not found"}), 404
    # Return config with the password masked for safety — send actual to allow re-save
    cfg = dict(acct.config)
    cfg["account_id"] = account_id
    return jsonify(cfg)

@app.route('/api/fix_accounts/<account_id>', methods=['PUT'])
def update_fix_account(account_id):
    """Update a FIX account config (save only, no reconnect)."""
    if not fix_manager:
        return jsonify({"error": "FIX connector not available"}), 501
    acct = fix_manager.accounts.get(account_id)
    if not acct:
        return jsonify({"error": "Account not found"}), 404
    try:
        data = request.get_json(force=True)
        for key in ['label', 'host', 'trade_port', 'quote_port',
                     'sender_comp_id', 'sender_comp_id_quote', 'target_comp_id', 'username',
                     'password', 'heartbeat_interval', 'lot_multiplier',
                     'leverage', 'stop_out_level', 'use_ssl', 'symbol_file', 'implementation',
                     'openapi_client_id', 'openapi_client_secret',
                     'openapi_access_token', 'openapi_refresh_token',
                     'openapi_account_id', 'openapi_environment',
                     'auto_connect_start', 'cycle_reminder_enabled',
                     'cycle_reminder_days', 'cycle_max_days', 'auto_cycle_enabled',
                     'group_label', 'margin_alert_threshold', 'alert_email', 'alert_telegram']:
            if key in data:
                acct.config[key] = data[key]
        if "label" in data:
            acct.label = data["label"]
        # Sync margin_alert_threshold to dashboard_settings for alert logic
        if "margin_alert_threshold" in data:
            try:
                val = float(data["margin_alert_threshold"]) if data["margin_alert_threshold"] not in (None, "", 0) else None
                if val is not None:
                    dashboard_settings.setdefault("margin_alert_thresholds", {})[account_id] = val
                else:
                    dashboard_settings.get("margin_alert_thresholds", {}).pop(account_id, None)
                _save_settings()
            except (ValueError, TypeError):
                pass
        # Save config
        fix_manager.save_config()
        _log_event(None, account_id, "fix_account_updated",
                   f"host={acct.config.get('host')} label={acct.config.get('label')}")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── MT Direct Account Endpoints ───────────────────────────────────────────

@app.route('/api/mt_direct_accounts', methods=['GET'])
def list_mt_direct_accounts():
    """List all MT Direct connections and their status."""
    if not mt_direct_manager:
        return jsonify({})
    return jsonify(mt_direct_manager.get_status())

@app.route('/api/mt_direct_accounts', methods=['POST'])
def add_mt_direct_account():
    """Add a new MT4/MT5 Direct account."""
    if not mt_direct_manager:
        return jsonify({"error": "MT Direct connector not available"}), 503
    try:
        data = request.get_json(force=True)
        account_id = str(data.get("account_id", "")).strip()
        if not account_id:
            return jsonify({"error": "account_id is required"}), 400

        auto_connect = data.get("auto_connect", True)
        config = {
            "type": data.get("type", "mt4"),  # "mt4" or "mt5"
            "login": data.get("login"),
            "password": data.get("password"),
            "server": data.get("server"),
            "port": int(data.get("port", 443)),
            "label": data.get("label", f"MT-{account_id}"),
            "slippage": int(data.get("slippage", 3)),
            "magic_number": int(data.get("magic_number", 777888)),
            "auto_connect_start": auto_connect,
            "alert_email": data.get("alert_email"),
            "alert_telegram": data.get("alert_telegram"),
        }
        ok = mt_direct_manager.add_account(account_id, config, auto_connect=auto_connect)
        if not ok:
            return jsonify({"error": "Account already exists"}), 409
        _log_event(None, account_id, "mt_direct_added",
                   f"type={config['type']} server={config['server']}")
        return jsonify({"ok": True, "account_id": account_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/mt_direct_accounts/<account_id>', methods=['DELETE'])
def remove_mt_direct_account(account_id):
    """Remove an MT Direct account."""
    if not mt_direct_manager:
        return jsonify({"error": "MT Direct connector not available"}), 503
    ok = mt_direct_manager.remove_account(account_id)
    if not ok:
        return jsonify({"error": "Account not found"}), 404
    _log_event(None, account_id, "mt_direct_removed", "")
    return jsonify({"ok": True})

@app.route('/api/mt_direct_accounts/<account_id>/connect', methods=['POST'])
def connect_mt_direct_account(account_id):
    """Connect a specific MT Direct account."""
    if not mt_direct_manager:
        return jsonify({"error": "MT Direct connector not available"}), 503
    ok, error_msg = mt_direct_manager.connect_account(account_id)
    if not ok:
        return jsonify({"error": error_msg or "Connection failed"}), 400
    return jsonify({"ok": True})

@app.route('/api/mt_direct_accounts/<account_id>/disconnect', methods=['POST'])
def disconnect_mt_direct_account(account_id):
    """Disconnect a specific MT Direct account."""
    if not mt_direct_manager:
        return jsonify({"error": "MT Direct connector not available"}), 503
    ok = mt_direct_manager.disconnect_account(account_id)
    if not ok:
        return jsonify({"error": "Account not found"}), 404
    return jsonify({"ok": True})

@app.route('/api/mt_direct_accounts/<account_id>/config', methods=['GET'])
def get_mt_direct_config(account_id):
    """Get config for an MT Direct account (password masked)."""
    if not mt_direct_manager:
        return jsonify({"error": "MT Direct connector not available"}), 503
    acct = mt_direct_manager.accounts.get(account_id)
    if not acct:
        return jsonify({"error": "Account not found"}), 404
    cfg = dict(acct.config)
    # Password is included so the show/hide toggle in the edit modal works
    return jsonify(cfg)

@app.route('/api/mt_direct_accounts/<account_id>', methods=['PUT'])
def update_mt_direct_account(account_id):
    """Update an MT Direct account config (save only, no reconnect)."""
    if not mt_direct_manager:
        return jsonify({"error": "MT Direct connector not available"}), 503
    acct = mt_direct_manager.accounts.get(account_id)
    if not acct:
        return jsonify({"error": "Account not found"}), 404
    try:
        data = request.get_json(force=True)
        # Detect platform type change (mt4↔mt5) — requires recreating the
        # account object with the correct class so conn_type propagates.
        old_type = acct.config.get("type", "mt4")
        new_type = data.get("type", old_type)
        type_changed = old_type != new_type

        for key in ['login', 'server', 'port', 'label', 'slippage', 'magic_number', 'type', 'stop_out_level',
                     'auto_connect_start', 'cycle_reminder_enabled', 'cycle_reminder_days',
                     'cycle_max_days', 'auto_cycle_enabled', 'alert_email', 'alert_telegram']:
            if key in data:
                acct.config[key] = data[key]
        if "label" in data:
            acct.label = data["label"]
        # Only update password if not masked
        if data.get("password") and data["password"] != "********":
            acct.config["password"] = data["password"]

        if type_changed:
            # Recreate the account object with the correct class (MT4 vs MT5)
            updated_config = dict(acct.config)
            was_connected = acct.connected
            mt_direct_manager.remove_account(account_id)
            mt_direct_manager.add_account(account_id, updated_config,
                                          save=True, auto_connect=was_connected)
            _log_event(None, account_id, "mt_direct_updated",
                       f"type changed {old_type}→{new_type}")
        else:
            mt_direct_manager.save_config()
            _log_event(None, account_id, "mt_direct_updated",
                       f"server={acct.config.get('server')} label={acct.config.get('label')}")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─── cTrader Open API OAuth ────────────────────────────────────────────────

_oauth_pending = {}  # Stash client_id/secret between URL generation and callback

@app.route('/api/ctrader_oauth_url', methods=['POST'])
def ctrader_oauth_url():
    """Generate OAuth authorization URL for cTrader Open API."""
    try:
        data = request.get_json(force=True)
        client_id = data.get("client_id")
        client_secret = data.get("client_secret", "")
        redirect_uri = data.get("redirect_uri", f"{DASHBOARD_BASE_URL}/api/ctrader_oauth_callback")
        if not client_id:
            return jsonify({"error": "client_id required"}), 400
        # Stash credentials so the callback can retrieve them
        _oauth_pending["client_id"] = client_id
        _oauth_pending["client_secret"] = client_secret
        _oauth_pending["redirect_uri"] = redirect_uri
        # Build OAuth URL inline (no ctrader_open_api import needed)
        from urllib.parse import urlencode
        url = "https://openapi.ctrader.com/apps/auth?" + urlencode({
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": "trading",
        })
        return jsonify({"url": url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/ctrader_oauth_callback', methods=['GET'])
def ctrader_oauth_callback():
    """OAuth callback — exchanges auth code for access + refresh tokens."""
    try:
        import requests as _req
        code = request.args.get("code")
        client_id = _oauth_pending.get("client_id", "")
        client_secret = _oauth_pending.get("client_secret", "")
        redirect_uri = _oauth_pending.get("redirect_uri",
                         f"{DASHBOARD_BASE_URL}/api/ctrader_oauth_callback")
        if not code:
            return "<h2>Error: No authorization code received</h2>", 400
        if not client_id:
            return ("<h2>Error: No client_id found — please generate the OAuth URL "
                    "from the dashboard first (API Accounts → Edit → Open API section)</h2>"), 400
        # Exchange auth code for tokens (inline — no ctrader_open_api import needed)
        token_resp = _req.get("https://openapi.ctrader.com/apps/token", params={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        }, timeout=15)
        result = token_resp.json()
        if result and "accessToken" in result:
            access_token = result['accessToken']
            refresh_token = result.get('refreshToken', '')
            return f"""<html><body style='font-family:monospace;background:#1a1a2e;color:#eee;padding:40px;'>
            <h2 style='color:#7c3aed;'>✅ cTrader Open API Authorization Successful</h2>
            <p><strong>Access Token:</strong><br><textarea id='at' rows='3' cols='80' onclick='this.select()'>{access_token}</textarea></p>
            <p><strong>Refresh Token:</strong><br><textarea rows='3' cols='80' onclick='this.select()'>{refresh_token}</textarea></p>
            <div id='accountsArea'>
              <button onclick='fetchAccounts()' style='background:#7c3aed;color:#fff;border:none;padding:8px 20px;border-radius:6px;cursor:pointer;font-size:0.9rem;margin-top:12px;'>📋 Fetch Account IDs</button>
              <span style='color:#aaa;font-size:0.8rem;margin-left:8px;'>Click to retrieve your ctidTraderAccountId values</span>
            </div>
            <p style='color:#aaa;font-size:0.9rem;margin-top:16px;'>Copy the tokens and Account ID into the Edit API Account modal → Open API section.<br>
            The refresh token never expires. The access token expires in ~30 days and will be auto-refreshed.</p>
            <script>
            async function fetchAccounts() {{
              const btn = event.target;
              btn.disabled = true;
              btn.textContent = '⏳ Fetching...';
              try {{
                const res = await fetch('/api/ctrader_fetch_accounts?client_id={client_id}&client_secret={client_secret}&access_token=' + document.getElementById('at').value.trim());
                const data = await res.json();
                if (data.accounts && data.accounts.length > 0) {{
                  let html = '<h3 style="color:#10b981;margin-top:16px;">Authorized Accounts</h3>';
                  html += '<table style="border-collapse:collapse;border:1px solid #333;">';
                  html += '<tr style="color:#aaa;border-bottom:1px solid #333;"><th style="padding:6px 16px;text-align:left;">Account Login</th><th style="padding:6px 16px;text-align:left;">ctidTraderAccountId</th><th style="padding:6px 16px;text-align:left;">Type</th></tr>';
                  data.accounts.forEach(a => {{
                    html += '<tr><td style="padding:6px 16px;">' + (a.traderLogin || '-') + '</td>';
                    html += '<td style="padding:6px 16px;font-size:1.1em;"><strong>' + a.accountId + '</strong></td>';
                    html += '<td style="padding:6px 16px;">' + (a.isLive ? 'Live' : 'Demo') + '</td></tr>';
                  }});
                  html += '</table>';
                  html += '<p style="color:#aaa;font-size:0.85rem;margin-top:8px;">Use the <strong>ctidTraderAccountId</strong> value as the Account ID field.</p>';
                  document.getElementById('accountsArea').innerHTML = html;
                }} else {{
                  document.getElementById('accountsArea').innerHTML = '<p style="color:#f59e0b;">Error: ' + (data.error || 'No accounts found') + '</p>';
                }}
              }} catch(e) {{
                document.getElementById('accountsArea').innerHTML = '<p style="color:#f59e0b;">Fetch failed: ' + e + '</p>';
              }}
            }}
            </script>
            </body></html>"""
        else:
            error_msg = result.get("description", "Unknown error") if result else "No response"
            return f"<h2>Token exchange failed: {error_msg}</h2>", 400
    except Exception as e:
        return f"<h2>Error: {e}</h2>", 500

@app.route('/api/ctrader_fetch_accounts', methods=['GET'])
def ctrader_fetch_accounts():
    """Fetch cTrader accounts via subprocess (to avoid CLR conflicts)."""
    try:
        import subprocess, json as _json
        client_id = request.args.get("client_id", "")
        client_secret = request.args.get("client_secret", "")
        access_token = request.args.get("access_token", "")
        if not all([client_id, client_secret, access_token]):
            return jsonify({"error": "Missing parameters"}), 400
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "fetch_ctrader_accounts.py")
        proc = subprocess.run(
            [sys.executable, script_path, client_id, client_secret, access_token],
            capture_output=True, text=True, timeout=20
        )
        raw_out = proc.stdout.strip() if proc.stdout else ""
        raw_err = proc.stderr.strip() if proc.stderr else ""
        print(f"[OAUTH] fetch stdout: {raw_out[:500]}")
        if raw_err:
            print(f"[OAUTH] fetch stderr: {raw_err[:500]}")
        if raw_out:
            return jsonify(_json.loads(raw_out))
        return jsonify({"error": f"No output. stderr={raw_err[:200]}, rc={proc.returncode}"})
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timeout (20s) — cTrader server may be unreachable"})
    except Exception as e:
        return jsonify({"error": str(e)})

# ─── Strategy Management ───────────────────────────────────────────────────

@app.route('/api/strategies', methods=['GET'])
def list_strategies():
    with lock:
        return jsonify(list(strategies.values()))

@app.route('/api/strategies', methods=['POST'])
def create_strategy():
    """Create a named strategy (pair of accounts)."""
    try:
        data = request.get_json(force=True)
        name = str(data.get("name", "")).strip()
        account1 = str(data.get("account1", "")).strip()
        account2 = str(data.get("account2", "")).strip()
        if not name:
            return jsonify({"error": "Strategy name is required"}), 400
        if not account1 or not account2:
            return jsonify({"error": "Both accounts are required"}), 400
        sid = str(uuid.uuid4())
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        strat = {
            "id": sid,
            "name": name,
            "account1": account1,
            "account2": account2,
            "enabled": True,
            "running": False,
            "trade_start_time": "18:00",
            "trade_stop_time": "16:30",
            "trade_alerts": bool(data.get("trade_alerts", False)),
            "created_at": now,
        }
        with lock:
            strategies[sid] = strat
        _log_event(None, "", "strategy_created", f"{name} ({account1}/{account2})")
        _save_strategies()
        return jsonify(strat)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/strategies/<strategy_id>', methods=['PUT'])
def update_strategy(strategy_id):
    """Update strategy name."""
    with lock:
        strat = strategies.get(strategy_id)
        if not strat:
            return jsonify({"error": "Strategy not found"}), 404
        data = request.get_json(force=True)
        if "name" in data:
            strat["name"] = str(data["name"]).strip()
        if "account1" in data:
            strat["account1"] = str(data["account1"]).strip()
        if "account2" in data:
            strat["account2"] = str(data["account2"]).strip()
        if "enabled" in data:
            strat["enabled"] = bool(data["enabled"])
        if "running" in data:
            strat["running"] = bool(data["running"])
        if "trade_start_time" in data:
            strat["trade_start_time"] = str(data["trade_start_time"]).strip() or "00:00"
        if "trade_stop_time" in data:
            strat["trade_stop_time"] = str(data["trade_stop_time"]).strip() or "23:59"
        if "trade_alerts" in data:
            strat["trade_alerts"] = bool(data["trade_alerts"])
        _save_strategies()
        return jsonify(strat)

@app.route('/api/strategies/<strategy_id>', methods=['DELETE'])
def delete_strategy(strategy_id):
    """Delete a strategy and all its sessions."""
    with lock:
        strat = strategies.pop(strategy_id, None)
        if not strat:
            return jsonify({"error": "Strategy not found"}), 404
        # Remove sessions belonging to this strategy
        to_remove = [sid for sid, s in sessions.items() if s.get("strategy_id") == strategy_id]
        for sid in to_remove:
            del sessions[sid]
        _save_sessions()
        _save_strategies()
    _log_event(None, "", "strategy_deleted", f"{strat.get('name', '')} ({len(to_remove)} sessions removed)")
    return jsonify({"ok": True})

# ─── Position Import ───────────────────────────────────────────────────────

@app.route('/api/strategies/<strategy_id>/import_positions', methods=['POST'])
def import_positions(strategy_id):
    """Trigger position import for a strategy. Queues report_positions commands for both EAs."""
    with lock:
        strat = strategies.get(strategy_id)
        if not strat:
            return jsonify({"error": "Strategy not found"}), 404
        data = request.get_json(force=True) or {}
        comment_filter = data.get("comment_filter", "").strip()
        pair_filter = data.get("pair", "").strip()
        match_mode = data.get("match_mode", "ticket").strip()  # "ticket" or "lots"
        time_from = data.get("time_from", "").strip()  # ISO datetime string
        time_to = data.get("time_to", "").strip()      # ISO datetime string
        # Per-account ticket filters
        ticket_from_1_str = data.get("ticket_from_1", "").strip()
        ticket_to_1_str = data.get("ticket_to_1", "").strip()
        ticket_from_2_str = data.get("ticket_from_2", "").strip()
        ticket_to_2_str = data.get("ticket_to_2", "").strip()

        req_id = str(uuid.uuid4())
        pending_position_reports[req_id] = {
            "strategy_id": strategy_id,
            "accounts": [strat["account1"], strat["account2"]],
            "comment_filter": comment_filter,
            "pair": pair_filter,
            "match_mode": match_mode,
            "time_from": time_from,
            "time_to": time_to,
            "ticket_filters": {
                strat["account1"]: {
                    "from": int(ticket_from_1_str) if ticket_from_1_str else None,
                    "to": int(ticket_to_1_str) if ticket_to_1_str else None,
                },
                strat["account2"]: {
                    "from": int(ticket_from_2_str) if ticket_from_2_str else None,
                    "to": int(ticket_to_2_str) if ticket_to_2_str else None,
                },
            },
            "received": {},
            "ts": time.time(),
        }
        _log_event(None, "", "import_requested",
                   f"strategy={strat['name']} comment_filter='{comment_filter}' pair='{pair_filter}' time={time_from or '*'}→{time_to or '*'} ticket1={ticket_from_1_str or '*'}→{ticket_to_1_str or '*'} ticket2={ticket_from_2_str or '*'}→{ticket_to_2_str or '*'}")

        # Auto-fetch positions from MT Direct or FIX accounts
        req = pending_position_reports[req_id]
        waiting_for = []
        for acct_id in req["accounts"]:
            direct_acct = mt_direct_manager.accounts.get(acct_id) if mt_direct_manager else None
            fix_acct = fix_manager.accounts.get(acct_id) if fix_manager else None
            if direct_acct and direct_acct.connected and hasattr(direct_acct, 'get_positions_for_import'):
                try:
                    positions = direct_acct.get_positions_for_import(pair_filter, comment_filter)
                    req["received"][acct_id] = positions
                    app.logger.info("[IMPORT] Auto-fetched %d positions from MT Direct %s",
                                    len(positions), acct_id)
                except Exception as e:
                    app.logger.error("[IMPORT] Auto-fetch failed for %s: %s", acct_id, e)
                    waiting_for.append(acct_id)
            elif fix_acct and fix_acct.connected and hasattr(fix_acct, 'get_positions_for_import'):
                try:
                    positions = fix_acct.get_positions_for_import(pair_filter, comment_filter)
                    req["received"][acct_id] = positions
                    app.logger.info("[IMPORT] Auto-fetched %d positions from FIX %s",
                                    len(positions), acct_id)
                except Exception as e:
                    app.logger.error("[IMPORT] FIX Auto-fetch failed for %s: %s", acct_id, e)
                    waiting_for.append(acct_id)
            else:
                waiting_for.append(acct_id)

        # If all accounts responded (all Direct), process immediately
        if all(a in req["received"] for a in req["accounts"]):
            result = _process_position_import(req)
            result["_ts"] = time.time()
            import_results[req_id] = result
            del pending_position_reports[req_id]
            return jsonify({"ok": True, "request_id": req_id, "immediate": True,
                            "message": "Positions imported from MT Direct accounts.",
                            "result": result})

        return jsonify({"ok": True, "request_id": req_id,
                        "message": f"Waiting for position reports from: {', '.join(waiting_for)}"})

@app.route('/api/position_report', methods=['POST'])
def position_report():
    """EA posts detailed position data here in response to a report_positions command."""
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "Invalid JSON"}), 400

        req_id = data.get("request_id", "")
        account = str(data.get("account", "")).strip()
        positions = data.get("positions", [])

        with lock:
            req = pending_position_reports.get(req_id)
            if not req:
                return jsonify({"error": "Unknown or expired request_id"}), 404

            req["received"][account] = positions
            print(f"[IMPORT] Received {len(positions)} positions from account {account} for request {req_id[:8]}")

            # Check if both accounts have reported
            all_received = all(acct in req["received"] for acct in req["accounts"])
            if all_received:
                # Process the import
                result = _process_position_import(req)
                result["_ts"] = time.time()
                import_results[req_id] = result
                del pending_position_reports[req_id]
                return jsonify(result)

        return jsonify({"ok": True, "status": "waiting_for_other_account"})
    except Exception as e:
        app.logger.exception("Error processing position report")
        return jsonify({"error": str(e)}), 500

@app.route('/api/import_status/<request_id>', methods=['GET'])
def import_status(request_id):
    """Check the status of a pending import request."""
    with lock:
        # Check if result is available
        result = import_results.get(request_id)
        if result:
            # Clean up old results (>60s)
            if time.time() - result.get("_ts", 0) > 60:
                del import_results[request_id]
            return jsonify({"status": "completed", "result": result})

        req = pending_position_reports.get(request_id)
        if not req:
            return jsonify({"status": "completed_or_expired"})
        received = list(req["received"].keys())
        waiting = [a for a in req["accounts"] if a not in req["received"]]
        return jsonify({
            "status": "pending",
            "received_from": received,
            "waiting_for": waiting,
            "elapsed": round(time.time() - req["ts"], 1)
        })

def _process_position_import(req):
    """Process position data from both accounts and create an import session."""
    strategy_id = req["strategy_id"]
    strat = strategies.get(strategy_id)
    if not strat:
        return {"error": "Strategy not found"}

    acct1 = strat["account1"]
    acct2 = strat["account2"]
    comment_filter = req.get("comment_filter", "")
    pair_filter = req.get("pair", "").upper()
    if not pair_filter and strat:
        pair_filter = strat.get("pair", "").upper()
    time_from_str = req.get("time_from", "")
    time_to_str = req.get("time_to", "")

    # Parse time filter boundaries (ISO datetime → UTC epoch)
    # open_epoch is stored as UTC via calendar.timegm, so we must parse
    # the user's input the same way (treat as UTC, not local time).
    import calendar as _cal
    time_from_epoch = None
    time_to_epoch = None
    if time_from_str:
        try:
            time_from_epoch = _cal.timegm(datetime.fromisoformat(time_from_str).timetuple())
        except Exception:
            pass
    if time_to_str:
        try:
            time_to_epoch = _cal.timegm(datetime.fromisoformat(time_to_str).timetuple())
        except Exception:
            pass

    pos1_raw = req["received"].get(acct1, [])
    pos2_raw = req["received"].get(acct2, [])

    # Filter by time range if specified
    # Positions without a valid open_epoch are EXCLUDED when a time filter is active,
    # otherwise they'd default to 0 (epoch 1970) and slip through.
    if time_from_epoch is not None or time_to_epoch is not None:
        pre1, pre2 = len(pos1_raw), len(pos2_raw)
        def _time_ok(p):
            oe = p.get("open_epoch")
            if oe is None or oe == 0:
                return False  # no valid timestamp — exclude
            if time_from_epoch is not None and oe < time_from_epoch:
                return False
            if time_to_epoch is not None and oe > time_to_epoch:
                return False
            return True
        pos1_raw = [p for p in pos1_raw if _time_ok(p)]
        pos2_raw = [p for p in pos2_raw if _time_ok(p)]
        app.logger.info("[IMPORT] Time filter: acct1 %d→%d, acct2 %d→%d (from=%s to=%s)",
                        pre1, len(pos1_raw), pre2, len(pos2_raw), time_from_epoch, time_to_epoch)

    # Filter by ticket range — per-account
    ticket_filters = req.get("ticket_filters", {})
    tf1 = ticket_filters.get(acct1, {})
    tf2 = ticket_filters.get(acct2, {})
    ticket_from_1, ticket_to_1 = tf1.get("from"), tf1.get("to")
    ticket_from_2, ticket_to_2 = tf2.get("from"), tf2.get("to")

    def _make_ticket_filter(t_from, t_to):
        def _ticket_ok(p):
            try:
                t = int(p.get("ticket", 0))
            except (ValueError, TypeError):
                return False
            if t_from is not None and t < t_from:
                return False
            if t_to is not None and t > t_to:
                return False
            return True
        return _ticket_ok

    if ticket_from_1 is not None or ticket_to_1 is not None:
        pre1 = len(pos1_raw)
        pos1_raw = [p for p in pos1_raw if _make_ticket_filter(ticket_from_1, ticket_to_1)(p)]
        app.logger.info("[IMPORT] Ticket filter acct1 (%s): %d→%d (from=%s to=%s)",
                        acct1, pre1, len(pos1_raw), ticket_from_1, ticket_to_1)

    if ticket_from_2 is not None or ticket_to_2 is not None:
        pre2 = len(pos2_raw)
        pos2_raw = [p for p in pos2_raw if _make_ticket_filter(ticket_from_2, ticket_to_2)(p)]
        app.logger.info("[IMPORT] Ticket filter acct2 (%s): %d→%d (from=%s to=%s)",
                        acct2, pre2, len(pos2_raw), ticket_from_2, ticket_to_2)

    # Filter by pair if specified
    if pair_filter:
        pos1_raw = [p for p in pos1_raw if p.get("symbol", "").upper().startswith(pair_filter) or pair_filter.startswith(p.get("symbol", "").upper())]
        pos2_raw = [p for p in pos2_raw if p.get("symbol", "").upper().startswith(pair_filter) or pair_filter.startswith(p.get("symbol", "").upper())]

    # Filter by comment if specified (supports comma-separated list)
    if comment_filter:
        comment_parts = [c.strip() for c in comment_filter.split(",") if c.strip()]
        match_blank = any(cp.lower() == "<blank>" for cp in comment_parts)
        comment_parts = [cp for cp in comment_parts if cp.lower() != "<blank>"]
        
        is_fix_1 = (fix_manager and acct1 in fix_manager.accounts)
        is_fix_2 = (fix_manager and acct2 in fix_manager.accounts)
        
        if not is_fix_1:
            pos1_raw = [p for p in pos1_raw if (match_blank and not p.get("comment", "").strip()) or any(cp in p.get("comment", "") for cp in comment_parts)]
        if not is_fix_2:
            pos2_raw = [p for p in pos2_raw if (match_blank and not p.get("comment", "").strip()) or any(cp in p.get("comment", "") for cp in comment_parts)]

    # Sort both sides by open time DESCENDING (newest first).
    # This ensures that newest tickets (likely true hedge pairs) match at the
    # same fill index, while unpaired excess on the larger side ends up at the
    # END of the list — so imbalance rebalance closes the oldest unpaired
    # tickets that have NO counterpart, preventing cascade paired-closes.
    pos1 = sorted(pos1_raw, key=lambda p: p.get("open_epoch") or 0, reverse=True)
    pos2 = sorted(pos2_raw, key=lambda p: p.get("open_epoch") or 0, reverse=True)

    if not pos1 and not pos2:
        msg = "No matching positions found on either account"
        if comment_filter:
            msg += f" (comment filter: '{comment_filter}')"
        if pair_filter:
            msg += f" (pair filter: '{pair_filter}')"
        _log_event(None, "", "import_empty", msg)
        return {"error": msg, "acct1_total": len(req["received"].get(acct1, [])),
                "acct2_total": len(req["received"].get(acct2, []))}

    # Determine pair from positions
    pair = pair_filter
    if not pair:
        if pos1:
            pair = pos1[0].get("symbol", "UNKNOWN")
        elif pos2:
            pair = pos2[0].get("symbol", "UNKNOWN")

    # Determine lot size (use the first position's lot size as reference)
    lot_size = 0.01
    if pos1:
        lot_size = pos1[0].get("lots", 0.01)
    elif pos2:
        lot_size = pos2[0].get("lots", 0.01)

    # Determine sides (buy/sell) from actual positions
    side1_action = pos1[0].get("side", "buy") if pos1 else "buy"
    side2_action = pos2[0].get("side", "sell") if pos2 else "sell"

    # Determine match mode — auto-upgrade to 'lots' for netting-mode accounts.
    # Netting brokers (e.g. Dukascopy) expose ONE aggregate position per symbol
    # regardless of how many incremental orders were placed. Per-ticket matching
    # is meaningless; lot-volume comparison is the only valid reconciliation method.
    match_mode = req.get("match_mode", "ticket")  # "ticket" or "lots"
    _acct1_netting = ea_account_info.get(acct1, {}).get("netting_mode", False)
    _acct2_netting = ea_account_info.get(acct2, {}).get("netting_mode", False)
    if _acct1_netting or _acct2_netting:
        match_mode = "lots"
        app.logger.info("[IMPORT] Netting-mode account detected — forcing match_mode=lots")

    # For netting accounts with an aggregate position, split it into virtual fills
    # of lot_size each so the hedge monitor's virtual-ticket ledger is populated.
    # Example: Dukascopy shows 1 SELL position of 0.10 lots → 10 virtual fills of 0.01.
    # This lets the cascade close individual increments (each sends a 0.01-lot opposing order).
    def _split_netting_position(positions, lot_size_ref, other_count):
        """If positions is a single aggregate, split into virtual fills matching other_count."""
        if len(positions) != 1:
            return positions  # Already individual positions or empty — no split needed
        agg = positions[0]
        agg_lots = round(agg.get("lots", 0), 4)
        # Determine increment size: use lot_size_ref, fall back to agg_lots / other_count
        inc = round(lot_size_ref, 4) if lot_size_ref > 0 else round(agg_lots / max(other_count, 1), 4)
        if inc <= 0:
            return positions  # Can't split
        n = round(agg_lots / inc)
        if n <= 1:
            return positions
        app.logger.info("[IMPORT] Splitting netting aggregate %.4f lots → %d virtual fills of %.4f",
                        agg_lots, n, inc)
        virtual = []
        for vi in range(n):
            virtual.append({
                "ticket": f"{agg.get('ticket', 0)}_v{vi}",  # virtual ticket ID
                "symbol": agg.get("symbol", ""),
                "lots": inc,
                "side": agg.get("side", "sell"),
                "comment": agg.get("comment", ""),
                "open_price": agg.get("open_price", 0),
                "open_time": agg.get("open_time", ""),
                "open_epoch": agg.get("open_epoch"),
            })
        return virtual

    # Calculate total lots per side (needed for lot-mode, useful for display either way)
    total_lots_1 = round(sum(p.get("lots", 0) for p in pos1), 4)
    total_lots_2 = round(sum(p.get("lots", 0) for p in pos2), 4)

    # Determine lot_size before splitting so we can use it as the increment
    lot_size = 0.01
    if pos1 and not _acct1_netting:
        lot_size = pos1[0].get("lots", 0.01)
    elif pos2 and not _acct2_netting:
        lot_size = pos2[0].get("lots", 0.01)
    elif pos1:
        lot_size = pos1[0].get("lots", 0.01)
    elif pos2:
        lot_size = pos2[0].get("lots", 0.01)

    # Split netting aggregate positions into virtual fills
    if _acct1_netting:
        pos1 = _split_netting_position(pos1, lot_size, len(pos2))
    if _acct2_netting:
        pos2 = _split_netting_position(pos2, lot_size, len(pos1))

    # Match positions — logic depends on match mode
    if match_mode == "lots":
        # Lot-based matching: balanced when total lots match
        is_balanced = abs(total_lots_1 - total_lots_2) < 0.001
        matched = round(min(total_lots_1, total_lots_2), 4)  # matched lots
    else:
        # Ticket-by-ticket: balanced when position counts match
        matched = min(len(pos1), len(pos2))
        is_balanced = len(pos1) == len(pos2)
    total_positions = max(len(pos1), len(pos2))  # Use max — allow one-sided imports

    # Use short login numbers for comments (long account IDs get truncated by MT4/MT5)
    def _short_name(acc):
        if mt_direct_manager and acc in mt_direct_manager.accounts:
            return str(mt_direct_manager.accounts[acc].config.get('login', acc))
        # For EA Poll/Manual: extract trailing account number
        import re
        m = re.search(r'(\d+)$', acc)
        return m.group(1) if m else acc
    comment = comment_filter if comment_filter else f"{_short_name(acct1)}-{_short_name(acct2)}"
    sid = str(uuid.uuid4())
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Use actual symbol names from positions for per-side pairs (e.g., USDCHF.b vs USDCHF)
    pair1 = pos1[0].get("symbol", pair) if pos1 else pair
    pair2 = pos2[0].get("symbol", pair) if pos2 else pair
    sides = {
        acct1: {"action": side1_action, "pair": pair1, "lot_size": lot_size, "comment": comment, "side_number": 1, "max_spread": 0},
        acct2: {"action": side2_action, "pair": pair2, "lot_size": lot_size, "comment": comment, "side_number": 2, "max_spread": 0},
    }

    session = {
        "id": sid,
        "strategy_id": strategy_id,
        "pair": pair,
        "lot_size": lot_size,
        "total_positions": total_positions,
        "max_spread_points": 0,
        "max_errors": 1,
        "trade_pause": 0.0,
        "diff_to_open": None,
        "diff_to_close": 0,
        "max_accum_lots": 0.0,
        "max_accum_deals": 0,
        "comment": comment,
        "execution_order": "simultaneous",
        "sides": sides,
        "status": "paused",
        "filled": {acct1: len(pos1), acct2: len(pos2)},
        "closed": {acct1: 0, acct2: 0},
        "close_count": None,
        "action": "monitor",
        "created_at": now,
        "updated_at": now,
        "errors": {acct1: [], acct2: []},
        "spread_rejects": {acct1: 0, acct2: 0},
        "rollback_needed": {},
        "last_trade_ts": {},
        "max_ticks_per_5s": 0,
        "max_price_jump": 0,
        "require_diff_skew_open": "",
        "require_diff_skew_close": "",
        "avoid_news": False,
        "fills": [],
        "close_fills": [],
        "imported": True,  # Flag to indicate imported session
        "match_mode": match_mode,  # "ticket" or "lots"
    }

    # Lot-mode: track total lots and closed lots per side
    if match_mode == "lots":
        session["filled_lots"] = {acct1: total_lots_1, acct2: total_lots_2}
        session["closed_lots"] = {acct1: 0.0, acct2: 0.0}

    # Add fills in chronological order, interleaving accounts.
    # pair_index ties each acct1 fill to its acct2 counterpart so the
    # hedge monitor can find the correct paired ticket even after
    # imbalance rebalance shifts per-account indices.
    for i in range(total_positions):
        if i < len(pos1):
            p = pos1[i]
            session["fills"].append({
                "account": acct1,
                "ticket": _normalize_ticket(p.get("ticket", 0)),
                "price": p.get("open_price", p.get("price")),
                "quote_price": p.get("open_price", p.get("price")),
                "spread": None,
                "ts": p.get("open_time", now),
                "ts_epoch": p.get("open_epoch", time.time()),
                "cmd_ts": None,
                "imported": True,
                "pair_index": i,
            })
        if i < len(pos2):
            p = pos2[i]
            session["fills"].append({
                "account": acct2,
                "ticket": _normalize_ticket(p.get("ticket", 0)),
                "price": p.get("open_price", p.get("price")),
                "quote_price": p.get("open_price", p.get("price")),
                "spread": None,
                "ts": p.get("open_time", now),
                "ts_epoch": p.get("open_epoch", time.time()),
                "cmd_ts": None,
                "imported": True,
                "pair_index": i,
            })

    sessions[sid] = session
    _save_sessions()

    result = {
        "ok": True,
        "session_id": sid,
        "pair": pair,
        "acct1": acct1,
        "acct1_positions": len(pos1),
        "acct1_side": side1_action,
        "acct2": acct2,
        "acct2_positions": len(pos2),
        "acct2_side": side2_action,
        "matched_pairs": matched,
        "total_positions": total_positions,
        "balanced": is_balanced,
        "comment": comment,
    }

    status = "balanced" if is_balanced else "UNBALANCED"
    _log_event(sid, "", "import_complete",
               f"{pair} {status}: {acct1}={len(pos1)} {side1_action} / {acct2}={len(pos2)} {side2_action} "
               f"({matched} matched pairs) comment='{comment}'")
    print(f"[IMPORT] Completed: {result}")
    return result

# ─── Reporting API ──────────────────────────────────────────────────────────

@app.route('/api/reporting', methods=['GET'])
def api_reporting():
    """Return reporting data: group summary, snapshots, fees, fee_keywords."""
    with lock:
        now_ts = time.time()
        # Build live group summary from current EA + manual account data
        all_accounts = {}
        for acc, info in ea_account_info.items():
            grp = ""
            # Prefer MT Direct label (NAME field) for tree grouping
            if mt_direct_manager:
                mt_acct = mt_direct_manager.accounts.get(acc)
                if mt_acct:
                    grp = mt_acct.config.get("label", "")
            # Fall back to group_label from GROUP column
            if not grp:
                grp = manual_accounts.get(acc, {}).get("group_label", "")
            all_accounts[acc] = {
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "group_label": grp,
                "online": (now_ts - ea_heartbeats.get(acc, 0)) < 45 if acc in ea_heartbeats else False,
            }
        for acc, info in manual_accounts.items():
            if acc not in all_accounts:
                all_accounts[acc] = {
                    "balance": info.get("balance"),
                    "equity": info.get("equity"),
                    "group_label": info.get("group_label", ""),
                    "online": False,
                }

        # Group accounts by label: NAME-HEDGEGROUP-SIDE (e.g. IRINA-6-A)
        # Build two levels: hedge_groups (pairs) and name_totals (all pairs under a name)
        hedge_groups = {}   # key: "IRINA-6" -> {accounts, total_balance, total_equity}
        name_totals = {}    # key: "IRINA"   -> {total_balance, total_equity, hedge_groups: ["6","7"]}
        ungrouped = []
        for acc, info in all_accounts.items():
            grp = info.get("group_label", "")
            parts = grp.split("-")
            bal = info.get("balance") or 0
            eq = info.get("equity") or 0
            if len(parts) >= 3:
                name = parts[0].strip()
                hedge_num = parts[1].strip()
                side = parts[2].strip()
                hedge_key = f"{name}-{hedge_num}"
                hg = hedge_groups.setdefault(hedge_key, {
                    "name": name, "hedge_num": hedge_num,
                    "accounts": [], "total_balance": 0, "total_equity": 0
                })
                hg["accounts"].append({"name": acc, "side": side, **info})
                hg["total_balance"] += bal
                hg["total_equity"] += eq
                nt = name_totals.setdefault(name, {"total_balance": 0, "total_equity": 0, "hedge_groups": set()})
                nt["total_balance"] += bal
                nt["total_equity"] += eq
                nt["hedge_groups"].add(hedge_num)
            elif len(parts) == 2:
                # Fallback: 2-part (GROUP-SIDE)
                prefix = parts[0].strip()
                side = parts[1].strip()
                hg = hedge_groups.setdefault(prefix, {
                    "name": "", "hedge_num": prefix,
                    "accounts": [], "total_balance": 0, "total_equity": 0
                })
                hg["accounts"].append({"name": acc, "side": side, **info})
                hg["total_balance"] += bal
                hg["total_equity"] += eq
            else:
                ungrouped.append({"name": acc, **info})

        # Convert sets to lists for JSON serialization
        for nt in name_totals.values():
            nt["hedge_groups"] = sorted(nt["hedge_groups"])

    return jsonify({
        "hedge_groups": hedge_groups,
        "name_totals": name_totals,
        "ungrouped": ungrouped,
        "snapshots": reporting_data.get("snapshots", [])[-90:],
        "fees": reporting_data.get("fees", [])[-500:],
        "fee_keywords": reporting_data.get("fee_keywords", []),
    })


@app.route('/api/reporting/snapshot', methods=['POST'])
def take_snapshot():
    """Force a manual balance snapshot now."""
    # Clear today check by removing today's snapshot if exists
    today = datetime.now().strftime("%Y-%m-%d")
    reporting_data["snapshots"] = [s for s in reporting_data["snapshots"] if s.get("date") != today]
    _take_balance_snapshot()
    return jsonify({"ok": True, "date": today, "total_snapshots": len(reporting_data["snapshots"])})


@app.route('/api/reporting/fee_keywords', methods=['POST'])
def update_fee_keywords():
    """Update the list of fee keywords for detection."""
    try:
        data = request.get_json(force=True)
        keywords = data.get("keywords", [])
        if isinstance(keywords, str):
            keywords = [k.strip() for k in keywords.split(",") if k.strip()]
        reporting_data["fee_keywords"] = keywords
        _save_reporting()
        return jsonify({"ok": True, "keywords": keywords})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/reporting/fees', methods=['POST'])
def add_fee():
    """Manually add a fee entry."""
    try:
        data = request.get_json(force=True)
        fee_entry = {
            "id": str(uuid.uuid4())[:8],
            "ts": data.get("ts", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            "ts_epoch": time.time(),
            "account": data.get("account", ""),
            "amount": float(data.get("amount", 0)),
            "balance_before": data.get("balance_before"),
            "balance_after": data.get("balance_after"),
            "label": data.get("label", "manual"),
        }
        reporting_data["fees"].append(fee_entry)
        _save_reporting()
        return jsonify({"ok": True, "fee": fee_entry})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/reporting/fees/<fee_id>', methods=['DELETE'])
def delete_fee(fee_id):
    """Delete a fee entry by ID."""
    before = len(reporting_data["fees"])
    reporting_data["fees"] = [f for f in reporting_data["fees"] if f.get("id") != fee_id]
    after = len(reporting_data["fees"])
    if before != after:
        _save_reporting()
        return jsonify({"ok": True})
    return jsonify({"error": "Fee not found"}), 404


# ─── PnL Report API ─────────────────────────────────────────────────────────

@app.route('/api/pnl/request', methods=['POST'])
def pnl_request_create():
    """Create a PnL request for all accounts under a name group."""
    try:
        data = request.get_json(force=True)
        name = (data.get("name") or "").strip()
        from_date = data.get("from_date", "")
        to_date = data.get("to_date", "")
        fee_keywords_override = data.get("fee_keywords")  # optional override
        exclude_balance = data.get("exclude_balance", True)  # default: exclude deposits/withdrawals
        if not name or not from_date or not to_date:
            return jsonify({"error": "name, from_date, and to_date required"}), 400

        # Parse dates to timestamps
        try:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
            to_dt = datetime.strptime(to_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            from_ts = int(from_dt.timestamp())
            to_ts = int(to_dt.timestamp())
        except ValueError:
            return jsonify({"error": "Invalid date format, expected YYYY-MM-DD"}), 400

        # Find all accounts under this name from current account data
        # Match if the first segment of group_label OR account name (split by '-') equals the name
        # Account names follow: NAME-hedgenumber-side-accountnumber (e.g. HU-1-A-SQ2200508)
        acct_list = []
        seen = set()

        def _check_account(acc, grp=""):
            if acc in seen:
                return
            # Check group_label first
            if grp:
                parts = grp.split("-")
                if parts[0].strip().upper() == name.upper():
                    acct_list.append(acc)
                    seen.add(acc)
                    return
            # Then check account name itself
            acc_parts = acc.split("-")
            if acc_parts[0].strip().upper() == name.upper():
                acct_list.append(acc)
                seen.add(acc)

        # EA / heartbeat accounts
        for acc in list(ea_account_info.keys()):
            grp = manual_accounts.get(acc, {}).get("group_label", "")
            _check_account(acc, grp)
        # Manual accounts
        for acc, info in manual_accounts.items():
            _check_account(acc, info.get("group_label", ""))
        # FIX / OpenAPI accounts
        if fix_manager:
            for acc, acct in fix_manager.accounts.items():
                _check_account(acc, acct.config.get("group_label", ""))
        # MT Direct accounts — check config["label"] first (same as Reporting tab)
        if mt_direct_manager:
            for acc in mt_direct_manager.accounts:
                mt_acct = mt_direct_manager.accounts[acc]
                grp = mt_acct.config.get("label", "")
                if not grp:
                    grp = manual_accounts.get(acc, {}).get("group_label", "")
                _check_account(acc, grp)

        if not acct_list:
            return jsonify({"error": f"No accounts found for name '{name}'"}), 404

        # Determine fee keywords: override > per-name > global
        if fee_keywords_override is not None:
            if isinstance(fee_keywords_override, str):
                fee_kw = [k.strip() for k in fee_keywords_override.split(",") if k.strip()]
            else:
                fee_kw = fee_keywords_override
        else:
            per_name = dashboard_settings.get("fee_keywords_per_name", {}).get(name, "")
            if per_name:
                fee_kw = [k.strip() for k in per_name.split(",") if k.strip()]
            else:
                fee_kw = reporting_data.get("fee_keywords", [])

        # Capture current balance & equity for open PnL calculation
        current_states = {}
        try:
            fix_accts = fix_manager.get_status() if fix_manager else {}
            mt_accts = mt_direct_manager.get_status() if mt_direct_manager else {}
            for acc in acct_list:
                bal = None
                eq = None
                # Check mt_direct status
                if acc in mt_accts:
                    bal = mt_accts[acc].get("balance")
                    eq = mt_accts[acc].get("equity")
                # Check fix status
                elif acc in fix_accts:
                    bal = fix_accts[acc].get("balance")
                    eq = fix_accts[acc].get("equity")
                # Check ea_account_info
                elif acc in ea_account_info:
                    bal = ea_account_info[acc].get("balance")
                    eq = ea_account_info[acc].get("equity")
                # Check manual_accounts
                elif acc in manual_accounts:
                    bal = manual_accounts[acc].get("balance")
                    eq = manual_accounts[acc].get("equity")
                
                if bal is not None and eq is not None:
                    try:
                        open_pnl = float(eq) - float(bal)
                        current_states[acc] = {
                            "balance": float(bal),
                            "equity": float(eq),
                            "unrealized_pnl": open_pnl
                        }
                    except (ValueError, TypeError):
                        pass
        except Exception as ex:
            app.logger.warning("[PnL] Failed to capture current account states for open PnL: %s", ex)

        rid = str(uuid.uuid4())
        pnl_requests[rid] = {
            "id": rid,
            "name": name,
            "accounts": acct_list,
            "from_date": from_date,
            "to_date": to_date,
            "from_ts": from_ts,
            "to_ts": to_ts,
            "fee_keywords": fee_kw,
            "exclude_balance": exclude_balance,
            "status": "pending",
            "results": {},
            "created_ts": int(time.time()),
            "current_states": current_states,
        }
        app.logger.info("[PnL] Created request %s for name=%s accounts=%s range=%s->%s fee_kw=%s",
                        rid, name, acct_list, from_date, to_date, fee_kw)

        # ── Service server-side accounts (bypass EA polling) ──
        # Identify accounts that can report PnL directly via their connector:
        #   - MT Direct accounts (via .NET API)
        #   - cTrader OpenAPI accounts (via ProtoOADealListReq)
        server_side_accts = []  # list of (account_id, account_obj, source_label)

        if mt_direct_manager:
            for acc in acct_list:
                mt_acct = mt_direct_manager.accounts.get(acc)
                if mt_acct and mt_acct.connected:
                    server_side_accts.append((acc, mt_acct, "mt_direct"))

        if fix_manager:
            for acc in acct_list:
                # Skip if already handled by MT Direct
                if any(s[0] == acc for s in server_side_accts):
                    continue
                fix_acct = fix_manager.accounts.get(acc)
                if fix_acct and hasattr(fix_acct, 'get_deal_history') and getattr(fix_acct, 'connected', False):
                    server_side_accts.append((acc, fix_acct, "openapi"))

        if server_side_accts:
            labels = [f"{aid}({src})" for aid, _, src in server_side_accts]
            app.logger.info("[PnL] %d accounts will be serviced server-side: %s",
                            len(server_side_accts), labels)

            def _resolve_server_side(request_id, accts, f_ts, t_ts, fkw):
                """Background thread: fetch deal history for server-side accounts."""
                for acc_id, acct_obj, source in accts:
                    try:
                        if not getattr(acct_obj, 'connected', False):
                            app.logger.warning("[PnL] %s %s disconnected — marking offline", source, acc_id)
                            req = pnl_requests.get(request_id)
                            if req and req["status"] == "pending" and acc_id not in req["results"]:
                                req["results"][acc_id] = {
                                    "pnl": 0, "swap": 0, "fees": 0, "net": 0,
                                    "ts": int(time.time()), "source": "skipped_offline",
                                }
                            # Check completion
                            if len(req["results"]) >= len(req["accounts"]):
                                req["status"] = "complete"
                                req["completed_ts"] = int(time.time())
                            continue
                        hist = acct_obj.get_deal_history(f_ts, t_ts, fee_keywords=fkw,
                                                         exclude_balance=pnl_requests.get(request_id, {}).get("exclude_balance", True))
                        if hist is None:
                            app.logger.warning("[PnL] %s %s returned None for history — storing zero result", source, acc_id)
                            req = pnl_requests.get(request_id)
                            if req and req["status"] == "pending" and acc_id not in req["results"]:
                                req["results"][acc_id] = {
                                    "pnl": 0, "swap": 0, "fees": 0, "net": 0,
                                    "ts": int(time.time()), "source": "server_error",
                                }
                            # Check completion
                            if len(req["results"]) >= len(req["accounts"]):
                                req["status"] = "complete"
                                req["completed_ts"] = int(time.time())
                            continue

                        req = pnl_requests.get(request_id)
                        if not req or req["status"] != "pending":
                            break  # Request was cancelled or already completed

                        pnl_val = hist.get("pnl", 0.0)
                        swap_val = hist.get("swap", 0.0)
                        fees_val = hist.get("fees", 0.0)
                        by_sym  = hist.get("by_symbol", {})
                        req["results"][acc_id] = {
                            "pnl": round(pnl_val, 2),
                            "swap": round(swap_val, 2),
                            "fees": round(fees_val, 2),
                            "net": round(pnl_val + swap_val + fees_val, 2),
                            "ts": int(time.time()),
                            "source": source,
                            "by_symbol": by_sym,
                        }
                        app.logger.info("[PnL] %s result for req=%s acct=%s pnl=%.2f swap=%.2f fees=%.2f (deals=%d)",
                                        source, request_id, acc_id, pnl_val, swap_val, fees_val, hist.get("deal_count", 0))
                        # Log lot diagnostic so we can see which attribute holds lot size
                        lot_diag = hist.get("_lot_diag")
                        if lot_diag:
                            app.logger.info("[PnL] MT4 lots diag for %s: %s", acc_id, lot_diag)
                        else:
                            app.logger.info("[PnL] No lot diag (MT5 or diag missing) for %s", acc_id)
                        # Log ALL keys/values for each by_sym entry to reveal the dict format
                        if by_sym:
                            for _s, _v in list(by_sym.items())[:3]:  # first 3 symbols max
                                app.logger.info("[PnL] by_symbol[%s][%s] keys=%s val=%s",
                                                acc_id, _s, list(_v.keys()), _v)

                        # Auto-complete check
                        if len(req["results"]) >= len(req["accounts"]):
                            req["status"] = "complete"
                            req["completed_ts"] = int(time.time())
                            app.logger.info("[PnL] Request %s complete — all %d accounts reported",
                                            request_id, len(req["accounts"]))
                            break
                    except Exception as e:
                        app.logger.error("[PnL] %s %s history error: %s — storing zero result", source, acc_id, e)
                        req = pnl_requests.get(request_id)
                        if req and req["status"] == "pending" and acc_id not in req["results"]:
                            req["results"][acc_id] = {
                                "pnl": 0, "swap": 0, "fees": 0, "net": 0,
                                "ts": int(time.time()), "source": "server_error",
                            }

                # Final sweep: ensure all server-side accounts have a result
                req = pnl_requests.get(request_id)
                if req and req["status"] == "pending":
                    for acc_id, _, source in accts:
                        if acc_id not in req["results"]:
                            app.logger.warning("[PnL] %s %s had no result after loop — filling zero", source, acc_id)
                            req["results"][acc_id] = {
                                "pnl": 0, "swap": 0, "fees": 0, "net": 0,
                                "ts": int(time.time()), "source": "server_error",
                            }
                    if len(req["results"]) >= len(req["accounts"]):
                        req["status"] = "complete"
                        req["completed_ts"] = int(time.time())
                        app.logger.info("[PnL] Request %s complete after final sweep", request_id)

            # Run in a daemon thread so we don't block the HTTP response
            t = threading.Thread(
                target=_resolve_server_side,
                args=(rid, server_side_accts, from_ts, to_ts, fee_kw),
                daemon=True, name=f"PnL-ServerSide-{rid[:8]}")
            t.start()

        # ── Pre-fill disconnected accounts so the report doesn't hang ──
        now_ts = time.time()
        server_side_ids = {s[0] for s in server_side_accts}
        skipped = []
        for acc in acct_list:
            if acc in pnl_requests[rid]["results"]:
                continue  # already has a result
            if acc in server_side_ids:
                continue  # will be resolved by the background thread

            # Determine if online: EA heartbeat, MT Direct, FIX/cTrader
            online = False
            if acc in ea_heartbeats and (now_ts - ea_heartbeats[acc]) < 45:
                online = True
            if not online and mt_direct_manager:
                mt_acct = mt_direct_manager.accounts.get(acc)
                if mt_acct and mt_acct.connected:
                    online = True
            if not online and fix_manager:
                fix_acct = fix_manager.accounts.get(acc)
                if fix_acct and getattr(fix_acct, 'connected', False):
                    online = True

            if not online:
                pnl_requests[rid]["results"][acc] = {
                    "pnl": 0, "swap": 0, "fees": 0, "net": 0,
                    "ts": int(now_ts),
                    "source": "skipped_offline",
                }
                skipped.append(acc)

        if skipped:
            app.logger.warning("[PnL] Skipped %d offline account(s) for request %s: %s",
                               len(skipped), rid, skipped)

        # Auto-complete immediately if all accounts are now resolved
        req = pnl_requests[rid]
        if len(req["results"]) >= len(req["accounts"]) and req["status"] == "pending":
            req["status"] = "complete"
            req["completed_ts"] = int(time.time())
            app.logger.info("[PnL] Request %s complete — all %d accounts reported (some skipped offline)",
                            rid, len(req["accounts"]))

        return jsonify({"ok": True, "request_id": rid, "accounts": acct_list, "from_ts": from_ts, "to_ts": to_ts})
    except Exception as e:
        app.logger.exception("Error creating PnL request")
        return jsonify({"error": str(e)}), 500


@app.route('/api/pnl/status/<request_id>', methods=['GET'])
def pnl_request_status(request_id):
    """Check status of a PnL request. Returns results when all accounts have reported."""
    req = pnl_requests.get(request_id)
    if not req:
        return jsonify({"error": "Request not found"}), 404

    accounts_list = req.get("accounts", [])
    results = req.get("results", {})
    reported = list(results.keys())
    pending_accts = [a for a in accounts_list if a not in results]

    # Auto-complete if all accounts reported
    if len(reported) >= len(accounts_list) and req["status"] == "pending":
        req["status"] = "complete"
        req["completed_ts"] = int(time.time())

    # Compute aggregated totals (exclude offline/skipped accounts)
    totals = {"gross_pnl": 0, "swap": 0, "fees": 0, "net_pnl": 0}
    totals_by_symbol_raw = {}  # { sym: {"net": X, "lots": Y} } — raw sums across accounts
    for acc, r in results.items():
        if r.get("source") in ("skipped_offline", "server_error"):
            continue
        totals["gross_pnl"] += r.get("pnl", 0)
        totals["swap"]      += r.get("swap", 0)
        totals["fees"]      += r.get("fees", 0)
        for sym, sv in (r.get("by_symbol") or {}).items():
            entry = totals_by_symbol_raw.setdefault(sym, {"net": 0.0, "lots": 0.0})
            # Bridge accounts set 'net_pnl' (already gross+swap+fees); use it directly.
            # MT direct accounts have separate pnl/swap/fees fields — sum them here.
            if "net_pnl" in sv:
                sym_net = sv["net_pnl"]
            else:
                sym_net = (sv.get("pnl", 0) or 0) + (sv.get("swap", 0) or 0) + (sv.get("fees", 0) or 0)
            entry["net"] += sym_net
            if "hedge_lots" in sv:
                entry["lots"] += sv["hedge_lots"]  # already halved
                entry["_pre_halved"] = True
            else:
                entry["lots"] += sv.get("lots", 0)
    totals["net_pnl"] = totals["gross_pnl"] + totals["swap"] + totals["fees"]

    # Build final totals_by_symbol sorted by abs(net) descending
    totals_by_symbol = {}
    for sym, v in sorted(totals_by_symbol_raw.items(),
                         key=lambda kv: abs(kv[1].get("net", 0)), reverse=True):
        pre_halved = v.get("_pre_halved", False)
        hedge_lots = round(v["lots"] if pre_halved else v["lots"] / 2.0, 2)
        net_val    = round(v["net"], 2)
        totals_by_symbol[sym] = {
            "pnl":        net_val,
            "hedge_lots": hedge_lots,
            "pnl_per_lot": round(net_val / hedge_lots, 2) if hedge_lots > 0 else 0.0,
        }

    return jsonify({
        "id": request_id,
        "name": req.get("name"),
        "status": req["status"],
        "from_date": req.get("from_date"),
        "to_date": req.get("to_date"),
        "accounts": accounts_list,
        "reported": reported,
        "pending": pending_accts,
        "results": results,
        "totals": totals,
        "totals_by_symbol": totals_by_symbol,
        "fee_keywords": req.get("fee_keywords", []),
        "created_ts": req.get("created_ts"),
        "current_states": req.get("current_states", {}),
    })


@app.route('/api/pnl/result', methods=['POST'])
def pnl_result_submit():
    """EA submits PnL results for a request."""
    try:
        data = request.get_json(force=True)
        request_id = data.get("request_id", "")
        account = str(data.get("account", ""))
        pnl_val = float(data.get("pnl", 0.0))
        swap_val = float(data.get("swap", 0.0))
        fees_val = float(data.get("fees", 0.0))
        by_symbol = data.get("by_symbol", {})  # optional per-pair breakdown from EA

        if not request_id or not account:
            return jsonify({"error": "request_id and account required"}), 400

        req = pnl_requests.get(request_id)
        if not req:
            return jsonify({"error": "Unknown request_id"}), 404
        if req["status"] != "pending":
            return jsonify({"ok": True, "info": "Request already closed"})
        if account in req.get("results", {}):
            return jsonify({"ok": True, "info": "Already reported"})

        req["results"][account] = {
            "pnl": round(pnl_val, 2),
            "swap": round(swap_val, 2),
            "fees": round(fees_val, 2),
            "net": round(pnl_val + swap_val + fees_val, 2),
            "ts": int(time.time()),
            "by_symbol": by_symbol if isinstance(by_symbol, dict) else {},
        }
        app.logger.info("[PnL] Result for req=%s acct=%s pnl=%.2f swap=%.2f fees=%.2f",
                        request_id, account, pnl_val, swap_val, fees_val)

        # Auto-complete check
        if len(req["results"]) >= len(req["accounts"]):
            req["status"] = "complete"
            req["completed_ts"] = int(time.time())
            app.logger.info("[PnL] Request %s complete — all %d accounts reported", request_id, len(req["accounts"]))

        return jsonify({"ok": True})
    except Exception as e:
        app.logger.exception("Error processing PnL result")
        return jsonify({"error": str(e)}), 500


# ─── Settings API ───────────────────────────────────────────────────────────

@app.route('/api/settings', methods=['GET'])
def api_get_settings():
    """Return current settings (passwords masked)."""
    s = json.loads(json.dumps(dashboard_settings))  # deep copy
    # Mask sensitive fields
    if s.get("email", {}).get("smtp_pass"):
        s["email"]["smtp_pass"] = "••••••"
    if s.get("telegram", {}).get("bot_token"):
        tok = s["telegram"]["bot_token"]
        s["telegram"]["bot_token"] = tok[:8] + "••••••" if len(tok) > 8 else "••••••"
    return jsonify(s)


@app.route('/api/settings', methods=['POST'])
def api_update_settings():
    """Update dashboard settings."""
    try:
        data = request.get_json(force=True)
        if "email" in data:
            email_cfg = dashboard_settings.setdefault("email", {})
            for k in ["enabled", "smtp_host", "smtp_port", "smtp_user", "from_addr", "to_addr"]:
                if k in data["email"]:
                    email_cfg[k] = data["email"][k]
            # Only update password if not the masked placeholder
            if "smtp_pass" in data["email"] and data["email"]["smtp_pass"] != "••••••":
                email_cfg["smtp_pass"] = data["email"]["smtp_pass"]
        if "telegram" in data:
            tg_cfg = dashboard_settings.setdefault("telegram", {})
            for k in ["enabled", "chat_id"]:
                if k in data["telegram"]:
                    tg_cfg[k] = data["telegram"][k]
            if "bot_token" in data["telegram"] and "••••" not in data["telegram"]["bot_token"]:
                tg_cfg["bot_token"] = data["telegram"]["bot_token"]
        if "fee_thresholds" in data:
            dashboard_settings["fee_thresholds"] = data["fee_thresholds"]
        if "margin_alert_threshold" in data:
            try:
                dashboard_settings["margin_alert_threshold"] = float(data["margin_alert_threshold"])
            except (ValueError, TypeError):
                pass
        if "margin_alert_thresholds" in data:
            dashboard_settings["margin_alert_thresholds"] = data["margin_alert_thresholds"]
        if "position_change_alert" in data:
            dashboard_settings["position_change_alert"] = bool(data["position_change_alert"])
        if "position_change_opened" in data:
            dashboard_settings["position_change_opened"] = bool(data["position_change_opened"])
        if "position_change_closed" in data:
            dashboard_settings["position_change_closed"] = bool(data["position_change_closed"])
        if "position_change_email" in data:
            dashboard_settings["position_change_email"] = bool(data["position_change_email"])
        if "position_change_telegram" in data:
            dashboard_settings["position_change_telegram"] = bool(data["position_change_telegram"])
        if "swap_alert_instruments" in data:
            dashboard_settings["swap_alert_instruments"] = str(data["swap_alert_instruments"]).strip()
        if "swap_alert_enabled" in data:
            dashboard_settings["swap_alert_enabled"] = bool(data["swap_alert_enabled"])
        if "swap_alert_pct" in data:
            try:
                dashboard_settings["swap_alert_pct"] = max(0, float(data["swap_alert_pct"]))
            except (ValueError, TypeError):
                pass
        if "swap_alert_interval_min" in data:
            try:
                dashboard_settings["swap_alert_interval_min"] = max(1, float(data["swap_alert_interval_min"]))
            except (ValueError, TypeError):
                pass
        if "theme_colors" in data:
            dashboard_settings["theme_colors"] = data["theme_colors"]
        if "rebalance_close_delay" in data:
            try:
                dashboard_settings["rebalance_close_delay"] = max(0, float(data["rebalance_close_delay"]))
            except (ValueError, TypeError):
                pass
        if "prompt_on_rollbacks" in data:
            dashboard_settings["prompt_on_rollbacks"] = bool(data["prompt_on_rollbacks"])
        if "ea_poll_enabled" in data:
            dashboard_settings["ea_poll_enabled"] = bool(data["ea_poll_enabled"])
        if "disbalance_alert_enabled" in data:
            dashboard_settings["disbalance_alert_enabled"] = bool(data["disbalance_alert_enabled"])
        if "disbalance_alert_email" in data:
            dashboard_settings["disbalance_alert_email"] = bool(data["disbalance_alert_email"])
        if "disbalance_alert_telegram" in data:
            dashboard_settings["disbalance_alert_telegram"] = bool(data["disbalance_alert_telegram"])
        if "disbalance_alert_period_sec" in data:
            try:
                dashboard_settings["disbalance_alert_period_sec"] = max(5, int(data["disbalance_alert_period_sec"]))
            except (ValueError, TypeError):
                pass
        if "exec_timeout_sec" in data:
            try:
                dashboard_settings["exec_timeout_sec"] = max(5, float(data["exec_timeout_sec"]))
            except (ValueError, TypeError):
                pass
        if "exec_alert_on_timeout" in data:
            dashboard_settings["exec_alert_on_timeout"] = bool(data["exec_alert_on_timeout"])
        if "exec_halt_on_timeout" in data:
            dashboard_settings["exec_halt_on_timeout"] = bool(data["exec_halt_on_timeout"])
        if "exec_retry_close" in data:
            dashboard_settings["exec_retry_close"] = bool(data["exec_retry_close"])
        if "exec_retry_max" in data:
            try:
                dashboard_settings["exec_retry_max"] = max(1, int(data["exec_retry_max"]))
            except (ValueError, TypeError):
                pass
        if "fund_email_enabled" in data:
            dashboard_settings["fund_email_enabled"] = bool(data["fund_email_enabled"])
        if "fund_email_time" in data:
            dashboard_settings["fund_email_time"] = str(data["fund_email_time"]).strip()
        _save_settings()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/confirm_rollback', methods=['POST'])
def api_confirm_rollback():
    """Approve or deny a pending rollback confirmation (used by UI prompt)."""
    try:
        data = request.get_json(force=True)
        sid = data.get("sid", "")
        account = data.get("account", "")
        approved = bool(data.get("approved", False))
        key = (sid, account)
        if key in _rollback_pending_confirmations:
            _rollback_pending_confirmations[key] = approved
            app.logger.info("[ROLLBACK-PROMPT] User %s rollback for %s sid=%s",
                            'APPROVED' if approved else 'DENIED', account, sid[:8])
            return jsonify({"ok": True, "approved": approved})
        return jsonify({"ok": False, "error": "No pending rollback for this account"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/pending_rollbacks', methods=['GET'])
def api_pending_rollbacks():
    """Return list of rollbacks awaiting user confirmation."""
    pending = []
    with lock:
        for (sid, account), state in list(_rollback_pending_confirmations.items()):
            if state is None:  # awaiting answer
                session = sessions.get(sid, {})
                rb_tickets = session.get("rollback_tickets", {}).get(account, [])
                pending.append({
                    "sid": sid,
                    "account": account,
                    "count": session.get("rollback_needed", {}).get(account, 0),
                    "tickets": rb_tickets[:5],  # preview first 5
                    "pair": session.get("pair", ""),
                    "sid_short": sid[:8],
                })
    return jsonify({"pending": pending})


@app.route('/api/settings/test_email', methods=['POST'])
def test_email():
    """Send a test email."""
    ok, err = _send_email("Trade Dashboard — Test Email",
                          "This is a test email from the Trade Execution Dashboard.")
    return jsonify({"ok": ok, "error": err})


@app.route('/api/settings/test_telegram', methods=['POST'])
def test_telegram():
    """Send a test Telegram message."""
    ok, err = _send_telegram("<b>Trade Dashboard</b>\nThis is a test message.")
    return jsonify({"ok": ok, "error": err})


# ─── Dashboard UI ───────────────────────────────────────────────────────────

@app.route('/pwa-icon.png')
def pwa_icon():
    """Generate a simple 192x192 PNG icon dynamically for PWA install."""
    import struct, zlib
    size = 192
    # Build raw RGBA pixels: dark background (#0f1117) with a centered "T" in accent (#6c5ce7)
    bg = (15, 17, 23, 255)
    fg = (108, 92, 231, 255)
    rows = []
    for y in range(size):
        row = b'\x00'  # PNG filter byte
        for x in range(size):
            # Draw a "T" shape: top bar y 60-80, vertical bar x 86-106 y 80-140
            if (60 <= y <= 80 and 50 <= x <= 142) or (80 < y <= 140 and 86 <= x <= 106):
                row += bytes(fg)
            else:
                row += bytes(bg)
        rows.append(row)
    raw = b''.join(rows)
    # Build minimal PNG
    def chunk(ctype, data):
        c = ctype + data
        return struct.pack('>I', len(data)) + c + struct.pack('>I', zlib.crc32(c) & 0xffffffff)
    png = b'\x89PNG\r\n\x1a\n'
    png += chunk(b'IHDR', struct.pack('>IIBBBBB', size, size, 8, 6, 0, 0, 0))
    png += chunk(b'IDAT', zlib.compress(raw))
    png += chunk(b'IEND', b'')
    return app.response_class(png, mimetype='image/png')

@app.route('/manifest.json')
def pwa_manifest():
    manifest = {
        "name": "Trade Execution Dashboard",
        "short_name": "TradeDash",
        "description": "Hedged position trading dashboard",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#0f1117",
        "theme_color": "#0f1117",
        "icons": [
            {"src": "/pwa-icon.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/pwa-icon.png", "sizes": "512x512", "type": "image/png"}
        ]
    }
    return app.response_class(json.dumps(manifest), mimetype='application/manifest+json')

@app.route('/sw.js')
def service_worker():
    sw_js = "self.addEventListener('fetch', function(e) { e.respondWith(fetch(e.request)); });"
    return app.response_class(sw_js, mimetype='application/javascript')

@app.route('/')
def dashboard():
    return DASHBOARD_HTML

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#1a2640">
<title>Trade Execution Dashboard</title>
<link rel="manifest" href="/manifest.json">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #0f1117;
  --surface: #1a1d27;
  --surface2: #242836;
  --header-bg: #1a2640;
  --border: #2e3346;
  --text: #e4e6f0;
  --text2: #8b8fa3;
  --accent: #6c5ce7;
  --accent2: #a29bfe;
  --green: #00e676;
  --green-bg: rgba(0,230,118,0.1);
  --red: #ff5252;
  --red-bg: rgba(255,82,82,0.1);
  --orange: #ffa726;
  --orange-bg: rgba(255,167,38,0.1);
  --blue: #42a5f5;
  --blue-bg: rgba(66,165,245,0.1);
  --radius: 12px;
  --shadow: 0 4px 24px rgba(0,0,0,0.3);
}
* { margin:0; padding:0; box-sizing:border-box; }
body {
  font-family: 'Inter', -apple-system, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.5;
  min-height: 100vh;
}
.container { max-width: 1900px; margin: 0 auto; padding: 20px; }

/* Header */
.header {
  display: flex; align-items: center; justify-content: flex-end;
  padding: 6px 16px; margin-bottom: 8px;
  background: var(--header-bg); border-radius: var(--radius);
  border: 1px solid rgba(66,130,245,0.25); border-bottom: 2px solid rgba(66,130,245,0.4);
  box-shadow: var(--shadow), 0 2px 12px rgba(66,130,245,0.08); gap: 12px;
}

/* Cards */
.card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 20px; margin-bottom: 20px;
  box-shadow: var(--shadow);
}
.card h2 {
  font-size: 1rem; font-weight: 600; margin-bottom: 16px;
  color: var(--accent2); display: flex; align-items: center; gap: 8px;
}
.card h2::before {
  content: ''; width: 4px; height: 20px; border-radius: 2px;
  background: linear-gradient(180deg, var(--accent), var(--accent2));
}

/* Form grid */
.form-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 14px; margin-bottom: 16px;
}
.form-group { display: flex; flex-direction: column; }
.form-group label {
  font-size: 0.75rem; font-weight: 500; color: var(--text2);
  margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px;
}
.form-group input, .form-group select {
  padding: 9px 12px; border-radius: 8px;
  border: 1px solid var(--border); background: var(--surface2);
  color: var(--text); font-size: 0.9rem; font-family: inherit;
  transition: border-color 0.2s;
}
.form-group input:focus, .form-group select:focus {
  outline: none; border-color: var(--accent);
}

/* Sides config */
.sides-config {
  display: grid; grid-template-columns: 1fr 1fr; gap: 16px;
  margin-bottom: 16px;
}
.side-box {
  padding: 14px; border-radius: 10px; border: 1px solid var(--border);
  background: var(--surface2);
}
.side-box h3 {
  font-size: 0.85rem; font-weight: 600; margin-bottom: 10px;
  color: var(--text2);
}
.side-box .form-group { margin-bottom: 8px; }

/* Buttons */
.btn {
  padding: 9px 20px; border-radius: 8px; border: none;
  font-family: inherit; font-size: 0.85rem; font-weight: 600;
  cursor: pointer; transition: all 0.2s; display: inline-flex;
  align-items: center; gap: 6px;
}
.btn-primary {
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  color: white;
}
.btn-primary:hover { transform: translateY(-1px); box-shadow: 0 4px 16px rgba(108,92,231,0.4); }
.btn-success { background: var(--green); color: #000; }
.btn-success:hover { box-shadow: 0 4px 16px rgba(0,230,118,0.4); }
.btn-danger { background: var(--red); color: white; }
.btn-danger:hover { box-shadow: 0 4px 16px rgba(255,82,82,0.4); }
.btn-warning { background: var(--orange); color: #000; }
.btn-warning:hover { box-shadow: 0 4px 16px rgba(255,167,38,0.4); }
.btn-sm { padding: 5px 12px; font-size: 0.78rem; }
.btn-group { display: flex; gap: 8px; flex-wrap: wrap; }

/* Sessions table */
.sessions-table { width: auto; border-collapse: collapse; table-layout: auto; }
.sessions-table th {
  text-align: left; padding: 2px 4px; font-size: 0.72rem;
  font-weight: 600; text-transform: uppercase; letter-spacing: 0.3px;
  color: var(--text2); border-bottom: 2px solid var(--border); white-space: normal;
  overflow: hidden; position: relative; min-width: 10px; user-select: none;
  line-height: 1.15; vertical-align: bottom;
}
.sessions-table td {
  padding: 2px 4px; font-size: 0.82rem;
  border-bottom: 1px solid var(--border);
  vertical-align: middle; white-space: nowrap; overflow: hidden;
  text-overflow: ellipsis;
}
/* Let inline inputs/selects collapse during auto-size, JS expands after freeze */
.sessions-table .inl { width: 0; min-width: 0; box-sizing: border-box; }
/* Hide number input spinners for compact display */
.sessions-table input[type=number]::-webkit-inner-spin-button,
.sessions-table input[type=number]::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
.sessions-table input[type=number] { -moz-appearance: textfield; }
.sessions-table tr:hover td { background: var(--surface2); }
/* Hidden columns */
.sessions-table .col-hidden { display: none; }
/* Column toggle dropdown */
.col-toggle-wrap { position: relative; display: inline-block; }
.col-toggle-btn {
  background: var(--surface2); border: 1px solid var(--border); color: var(--text2);
  padding: 4px 10px; border-radius: 6px; font-size: 0.72rem; cursor: pointer;
  font-family: inherit;
}
.col-toggle-btn:hover { background: var(--surface); color: var(--text); }
.col-toggle-menu {
  display: none; position: fixed; z-index: 1020;
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 8px 0; min-width: 160px; box-shadow: 0 8px 24px rgba(0,0,0,0.3);
  max-height: 500px; overflow-y: auto;
}
.col-toggle-menu.open { display: block; }
.col-toggle-menu label {
  display: flex; align-items: center; gap: 8px; padding: 4px 14px;
  font-size: 0.78rem; color: var(--text); cursor: pointer; white-space: nowrap;
}
.col-toggle-menu label:hover { background: var(--surface2); }
.col-toggle-menu input[type="checkbox"] { width: 14px; height: 14px; cursor: pointer; }

/* Status badges */
.badge {
  display: inline-block; padding: 3px 10px; border-radius: 20px;
  font-size: 0.72rem; font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.5px;
}
.badge-draft { background: var(--blue-bg); color: var(--blue); }
.badge-active { background: var(--green-bg); color: var(--green); animation: pulse 2s infinite; }
.badge-paused { background: var(--orange-bg); color: var(--orange); }
.badge-completed { background: rgba(108,92,231,0.15); color: var(--accent2); }
.badge-partial_close { background: rgba(255,71,87,0.2); color: #ff4757; animation: pulse-alert 1s infinite; font-weight: 700; }
@keyframes pulse-alert { 0%,100% { opacity:1; box-shadow: 0 0 8px rgba(255,71,87,0.4); } 50% { opacity:0.8; box-shadow: 0 0 16px rgba(255,71,87,0.7); } }
@keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:0.7; } }

/* Progress bar */
.progress-wrap {
  width: 100%; height: 6px; background: var(--surface2);
  border-radius: 3px; overflow: hidden;
}
.progress-bar {
  height: 100%; border-radius: 3px; transition: width 0.5s ease;
  background: linear-gradient(90deg, var(--accent), var(--green));
}

/* Event log */
.event-log {
  max-height: 350px; overflow-y: auto; font-size: 0.78rem;
  font-family: 'Cascadia Code', 'Fira Code', monospace;
}
.event-log .entry {
  padding: 5px 8px; border-bottom: 1px solid rgba(46,51,70,0.5);
  display: flex; gap: 10px;
}
.event-log .entry:hover { background: var(--surface2); }
.event-log .ts { color: var(--text2); min-width: 70px; }
.event-log .acct { color: var(--blue); min-width: 80px; }
.event-log .evt { color: var(--accent2); min-width: 110px; }
.event-log .dtl { color: var(--text); flex: 1; word-break: break-all; }
.event-log .evt.filled { color: var(--green); }
.event-log .evt.error, .event-log .evt.spread_rejected { color: var(--red); }
.event-log .evt.closed { color: var(--orange); }

/* Refresh indicator */
.refresh-bar {
  position: fixed; top: 0; left: 0; height: 2px; width: 0;
  background: linear-gradient(90deg, var(--accent), var(--green));
  transition: width 0.3s; z-index: 999;
}

/* Scrollbar */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: var(--surface); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

/* Tab navigation */
.tab-nav {
  display: flex; gap: 4px; margin-bottom: 20px;
  background: var(--surface); padding: 4px; border-radius: 10px;
  border: 1px solid var(--border);
}
.tab-btn {
  padding: 7px 18px; border-radius: 8px; border: none;
  background: transparent; color: var(--text2); font-family: inherit;
  font-size: 0.82rem; font-weight: 600; cursor: pointer;
  transition: all 0.2s; flex: 1; text-align: center; white-space: nowrap;
}
.tab-btn.active {
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  color: white; box-shadow: 0 2px 12px rgba(108,92,231,0.3);
}
.tab-btn:not(.active):hover { background: var(--surface2); color: var(--text); }
.tab-panel { display: none; }
.tab-panel.active { display: block; }

/* Accounts table */
.accounts-table { width: 100%; border-collapse: collapse; }
.accounts-table th {
  text-align: left; padding: 4px 8px; font-size: 0.75rem;
  font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
  color: var(--text2); border-bottom: 2px solid var(--border);
}
.accounts-table td {
  padding: 5px 8px; font-size: 0.85rem;
  border-bottom: 1px solid var(--border); vertical-align: middle;
}
.accounts-table tr:hover td { background: var(--surface2); }
.conn-dot {
  width: 8px; height: 8px; border-radius: 50%; display: inline-block;
  margin-right: 6px; vertical-align: middle;
}
.conn-dot.online { background: var(--green); box-shadow: 0 0 6px var(--green); }
.conn-dot.offline { background: var(--red); }

/* Diff display in session table */
.diff-cell { font-size: 0.78rem; }
.diff-val { font-weight: 600; }
.diff-val.positive { color: var(--green); }
.diff-val.negative { color: var(--red); }
.diff-val.neutral { color: var(--text2); }

/* Responsive */
@media (max-width: 768px) {
  .sides-config { grid-template-columns: 1fr; }
  .form-grid { grid-template-columns: 1fr 1fr; }
  .tab-btn { padding: 8px 12px; font-size: 0.8rem; }
}

/* Modal overlay */
.modal-overlay {
  display: none; position: fixed; inset: 0;
  background: rgba(0,0,0,0.6); z-index: 1000;
  justify-content: center; align-items: center;
}
.modal-overlay.active { display: flex; }
/* Edit instrument modal needs higher z-index to appear above Edit Strategy */
#editModal { z-index: 1010; }
#newInstrumentModal { z-index: 1010; }
.modal {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 24px; width: 90%; max-width: 800px;
  max-height: 85vh; overflow-y: auto; box-shadow: 0 8px 48px rgba(0,0,0,0.5);
}
.modal h2 { margin-bottom: 20px; }
/* PnL Modal Custom Styles */
#pnlModalDialog {
  transition: width 0.15s ease, height 0.15s ease, max-width 0.15s ease, max-height 0.15s ease;
}
#pnlModalDialog.maximized {
  width: 98vw !important;
  height: 96vh !important;
  max-width: none !important;
  max-height: none !important;
  resize: none !important;
  border-radius: 0 !important;
}
/* Inline editable inputs in instruments table */
.inl {
  padding: 1px 4px; border-radius: 4px; border: 1px solid var(--border);
  background: var(--surface2); color: var(--text); font-size: 0.8rem;
  font-family: inherit; text-align: center;
  transition: border-color 0.2s;
}
.inl:focus { outline: none; border-color: var(--accent); background: var(--surface); }
.inl:hover { border-color: var(--accent2); }
.inl-saved { border-color: var(--green) !important; transition: border-color 0.1s; }

/* ─── Positions section ─── */
.positions-divider {
  border: none; border-top: 1px solid var(--border);
  margin: 10px 0 6px;
}
.positions-header {
  display: flex; align-items: center; gap: 12px;
  margin-bottom: 4px;
}
.positions-header h3 {
  font-size: 0.82rem; font-weight: 600; color: var(--text2);
  text-transform: uppercase; letter-spacing: 0.5px; margin: 0;
}
/* Positions sub-tabs (compact pill style) */
.pos-tab-nav {
  display: flex; gap: 2px; background: var(--surface2);
  padding: 2px; border-radius: 6px; border: 1px solid var(--border);
}
.pos-tab-btn {
  padding: 3px 10px; border-radius: 4px; border: none;
  background: transparent; color: var(--text2); font-family: inherit;
  font-size: 0.72rem; font-weight: 600; cursor: pointer;
  transition: all 0.2s; white-space: nowrap;
}
.pos-tab-btn.active {
  background: var(--accent); color: white;
}
.pos-tab-btn:not(.active):hover { background: var(--surface); color: var(--text); }
.pos-tab-panel { display: none; }
.pos-tab-panel.active { display: block; }
/* Positions pane collapse toggle */
.pos-collapse-btn {
  background: none; border: none; color: var(--text2); cursor: pointer;
  font-size: 0.9rem; padding: 2px 6px; border-radius: 4px; transition: all 0.2s;
  display: flex; align-items: center;
}
.pos-collapse-btn:hover { color: var(--text); background: var(--surface2); }
.pos-collapse-btn .chevron { transition: transform 0.25s ease; display: inline-block; }
.pos-collapse-btn.collapsed .chevron { transform: rotate(-90deg); }
#stab-instruments .pos-panels-wrapper.collapsed { display: none; }
/* Deals table */
.deals-table { width: 100%; border-collapse: collapse; table-layout: auto; font-size: 0.76rem; }
.deals-table th {
  text-align: left; padding: 2px 5px; font-size: 0.68rem;
  font-weight: 600; text-transform: uppercase; letter-spacing: 0.3px;
  color: var(--text2); border-bottom: 2px solid var(--border); white-space: nowrap;
}
.deals-table td {
  padding: 1px 5px; border-bottom: 1px solid rgba(46,51,70,0.3);
  vertical-align: middle; white-space: nowrap;
}
.deals-table tr.deal-row-top td { border-bottom: none; }
.deals-table tr.deal-row-bottom td { border-bottom: 1px solid var(--border); }
.deals-table .profit-pos { color: var(--green); font-weight: 600; }
.deals-table .profit-neg { color: var(--red); font-weight: 600; }

/* Reporting tab */
.reporting-section { margin-bottom: 24px; }
.reporting-section h3 {
  font-size: 1rem; font-weight: 600; margin-bottom: 10px;
  display: flex; align-items: center; gap: 8px;
}
.rpt-table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
.rpt-table th {
  text-align: left; padding: 10px 12px; font-size: 0.75rem;
  font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
  color: var(--text2); border-bottom: 2px solid var(--border);
}
.rpt-table td {
  padding: 8px 12px; border-bottom: 1px solid var(--border); vertical-align: middle;
}
.rpt-table tr:hover td { background: var(--surface2); }
.rpt-table .total-row td { font-weight: 700; border-top: 2px solid var(--accent); background: rgba(108,92,231,0.06); }
.rpt-table .neg { color: var(--red); }
.rpt-table .pos { color: var(--green); }
.rpt-chart-wrap { position: relative; background: var(--surface2); border-radius: 8px; padding: 16px; min-height: 200px; }
.rpt-chart-controls { display: flex; align-items: center; gap: 12px; margin-bottom: 10px; flex-wrap: wrap; }
.rpt-chart-controls select, .rpt-chart-controls input {
  padding: 6px 10px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--surface); color: var(--text); font-size: 0.82rem;
}
.fee-keywords-wrap { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
.fee-keywords-wrap input {
  padding: 6px 10px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--surface2); color: var(--text); font-size: 0.82rem; flex: 1; min-width: 200px;
}
/* Settings tab */
.settings-section { margin-bottom: 28px; }
.settings-section h3 { font-size: 1rem; font-weight: 600; margin-bottom: 12px; }
.settings-grid {
  display: grid; grid-template-columns: 160px 1fr; gap: 8px 16px; align-items: center;
  max-width: 600px;
}
.settings-grid label { font-size: 0.82rem; color: var(--text2); text-align: right; }
.settings-grid input, .settings-grid select {
  padding: 7px 10px; border-radius: 6px; border: 1px solid var(--border);
  background: var(--surface2); color: var(--text); font-size: 0.82rem;
}
.settings-grid input[type=checkbox] { width: 18px; height: 18px; justify-self: start; }
.settings-actions { margin-top: 12px; display: flex; gap: 8px; }
.theme-color-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 10px 18px;
  max-width: 900px;
}
.theme-color-item {
  display: flex; align-items: center; gap: 8px; font-size: 0.82rem; color: var(--text2);
}
.theme-color-item input[type=color] {
  width: 36px; height: 28px; border: 1px solid var(--border); border-radius: 6px;
  background: var(--surface2); cursor: pointer; padding: 1px;
}
.theme-color-item input[type=color]::-webkit-color-swatch-wrapper { padding: 2px; }
.theme-color-item input[type=color]::-webkit-color-swatch { border-radius: 3px; border: none; }
.theme-color-item label { cursor: pointer; user-select: none; white-space: nowrap; }
.threshold-table { width: 100%; max-width: 500px; border-collapse: collapse; font-size: 0.85rem; }
.threshold-table th { text-align: left; padding: 8px 12px; font-size: 0.75rem; font-weight: 600;
  text-transform: uppercase; color: var(--text2); border-bottom: 2px solid var(--border); }
.threshold-table td { padding: 6px 12px; border-bottom: 1px solid var(--border); }
.threshold-table input { width: 80px; padding: 4px 8px; border-radius: 4px; border: 1px solid var(--border);
  background: var(--surface2); color: var(--text); font-size: 0.82rem; text-align: right; }
</style>
</head>
<body>
<div class="refresh-bar" id="refreshBar"></div>
<div class="container">

<!-- Header -->
<div class="header">
  <span id="serverTime" style="font-size:0.85rem;color:var(--text2);font-weight:600;">--:--:--</span>
  <label style="font-size:0.78rem;color:var(--text2);">Refresh:
    <input type="number" id="refreshInterval" value="2" min="1" max="30"
           style="width:50px;padding:4px 6px;border-radius:6px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:0.8rem;text-align:center;">s
  </label>
  <button id="soundToggle" onclick="toggleSound()" style="background:none;border:none;font-size:1.1rem;cursor:pointer;padding:2px 4px;" title="Toggle trade sounds">🔊</button>
  <button id="speechToggle" onclick="toggleSpeech()" style="background:none;border:none;font-size:1.1rem;cursor:pointer;padding:2px 4px;" title="Toggle speech notifications">🗣️</button>
  <div id="eaIndicators" style="display:none;"></div>
</div>

<!-- Tab Navigation -->
<div class="tab-nav">
  <button class="tab-btn active" data-tab="accounts" onclick="switchTab('accounts')">📊 Accounts</button>
  <button class="tab-btn" data-tab="strategies" onclick="switchTab('strategies')">⚙ Strategies</button>
  <button class="tab-btn" data-tab="eventlog" onclick="switchTab('eventlog')">📋 Event Log</button>
  <button class="tab-btn" data-tab="reporting" onclick="switchTab('reporting')">📈 Reporting</button>
  <button class="tab-btn" data-tab="settings" onclick="switchTab('settings')">⚙️ Settings</button>
</div>

<!-- �?�?�?�?�?�?�?�?�?�?�? TAB 1: Accounts �?�?�?�?�?�?�?�?�?�?�? -->
<div id="cycleReminderBanner" style="display:none;margin-bottom:12px;"></div>
<div class="tab-panel active" id="tab-accounts">
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
      <div style="display:flex;align-items:center;gap:8px;">
        <h2 style="margin:0;">Connected Accounts</h2>
        <div class="col-toggle-wrap">
          <button class="col-toggle-btn" onclick="toggleAcctColMenu()" title="Show/hide columns">👁 Columns</button>
          <div class="col-toggle-menu" id="acctColToggleMenu"></div>
        </div>
        <label style="display:flex;align-items:center;gap:4px;font-size:0.78rem;color:var(--text2);cursor:pointer;user-select:none;" title="Group accounts by name prefix">
          <input type="checkbox" id="groupViewToggle" onchange="toggleGroupView(this.checked)">
          Group view
        </label>
        <span style="font-size:0.75rem;color:var(--text2);margin-left:12px;cursor:pointer;" id="fundDistUpdateBadge" onclick="triggerRecalculateFundDistributions()" title="Click to recalculate optimal fund distribution">
          Optimal Dist: -
        </span>
        <button class="btn" style="padding:2px 8px;font-size:0.7rem;margin-left:6px;background:var(--surface);border:1px solid var(--border);" onclick="triggerRecalculateFundDistributions()" title="Force recalculate optimal fund distribution">
          Recalc
        </button>
      </div>
      <div style="display:flex;gap:8px;">
        <button class="btn btn-primary" onclick="showAddAccountModal()" style="padding:6px 16px;font-size:0.8rem;">+ Add EA Account</button>
        <button class="btn" onclick="showAddFixAccountModal()" style="padding:6px 16px;font-size:0.8rem;background:var(--accent);color:white;">+ Add API Account</button>
        <button class="btn" onclick="showAddMTDirectModal()" style="padding:6px 16px;font-size:0.8rem;background:#6366f1;color:white;">+ Add MT Direct</button>
      </div>
    </div>
    <div style="overflow-x:auto;">
      <table class="accounts-table" id="accountsTable">
        <thead>
          <tr>
            <th data-acol="0">Name</th>
            <th data-acol="1">Group</th>
            <th data-acol="2">Connection</th>
            <th data-acol="3">Balance</th>
            <th data-acol="4">Equity</th>
            <th data-acol="5" title="Optimal suggested equity distribution based on leverage and stopout levels">Opt Eq</th>
            <th data-acol="6" title="Suggested fund transfer to reach optimal equity (Optimal Equity - Current Equity)">Shift</th>
            <th data-acol="7">PnL</th>
            <th data-acol="8">Leverage</th>
            <th data-acol="9">Positions</th>
            <th data-acol="10">Lots</th>
            <th data-acol="11">Margin Use</th>
            <th data-acol="12" title="Margin alert threshold (%)">Marg.Alrt%</th>
            <th data-acol="13">Swap</th>
            <th data-acol="14" title="Swap change at last 5 PM ET rollover">Δ Swap</th>
            <th data-acol="15" title="Oldest position age (rollover days)">Age</th>
            <th data-acol="16">Last Poll</th>
            <th data-acol="17" title="Auto connect account at start">Auto Conn</th>
            <th data-acol="18" title="Alert Email(s) Override">Email Alert</th>
            <th data-acol="19" title="Alert Telegram ID(s) Override">Telegram Alert</th>
            <th data-acol="20" title="Log market stats (spread, ticks, bid/ask) to CSV">📊</th>
            <th data-acol="21"></th>
          </tr>
        </thead>
        <tbody id="accountsBody">
          <tr><td colspan="22" style="text-align:center;color:var(--text2);padding:30px;">No accounts yet</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</div>

<!-- Quote Stats Report Modal -->
<div id="rptModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:1000;display:none;align-items:center;justify-content:center;">
  <div class="card" style="width:340px;max-width:90vw;">
    <h2 style="margin:0 0 12px 0;">📊 Quote Stats Report</h2>
    <input type="hidden" id="rptAccount">
    <p style="font-size:0.82rem;color:var(--text2);margin:0 0 10px 0;">Account: <strong id="rptAccLabel"></strong></p>
    <div style="display:flex;gap:10px;margin-bottom:10px;">
      <div style="flex:1">
        <label style="font-size:0.72rem;color:var(--text2);display:block;">Pair (optional)</label>
        <input id="rptPair" class="inl" placeholder="all" style="width:100%;">
      </div>
      <div style="flex:1">
        <label style="font-size:0.72rem;color:var(--text2);display:block;">Last N days</label>
        <input id="rptDays" class="inl" type="number" min="1" placeholder="all" style="width:100%;">
      </div>
      <div style="flex:1">
        <label style="font-size:0.72rem;color:var(--text2);display:block;">Best Slots</label>
        <input id="rptTop" class="inl" type="number" min="1" value="10" style="width:100%;" title="Number of best day+hour combinations to show, ranked by lowest spread + volatility">
      </div>
    </div>
    <div style="display:flex;gap:8px;align-items:center;">
      <button class="btn btn-primary" id="rptGenBtn" onclick="generateReport()" style="padding:6px 16px;font-size:0.82rem;">Generate</button>
      <button class="btn" onclick="closeRptModal()" style="padding:6px 16px;font-size:0.82rem;">Cancel</button>
      <span id="rptStatus" style="font-size:0.78rem;color:var(--text2);margin-left:8px;"></span>
    </div>
    <div id="rptList" style="font-size:0.82rem;color:var(--text2);margin-top:12px;max-height:200px;overflow-y:auto;"></div>
  </div>
</div>

<!-- �?�?�?�?�?�?�?�?�?�?�? TAB 2: Strategies �?�?�?�?�?�?�?�?�?�?�? -->
<div class="tab-panel" id="tab-strategies">
<div class="card">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
    <h2 style="margin:0;">Strategies</h2>
    <button class="btn btn-primary" onclick="showNewStrategyModal()" style="padding:6px 16px;font-size:0.8rem;">+ New Strategy</button>
  </div>
  <div style="overflow-x:auto;">
    <table class="sessions-table" id="strategiesTable">
      <thead>
        <tr>
          <th>Name</th>
          <th>Account 1</th>
          <th>Account 2</th>
          <th>Type</th>
          <th>Errors</th>
          <th>Positions</th>
          <th>Enabled</th>
          <th>Status</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="strategiesBody">
        <tr><td colspan="9" style="text-align:center;color:var(--text2);padding:30px;">No strategies yet</td></tr>
      </tbody>
    </table>
  </div>
</div>
</div> <!-- end tab-strategies -->

<!-- �?�?�?�?�?�?�?�?�?�?�? TAB 3: Event Log �?�?�?�?�?�?�?�?�?�?�? -->
<div class="tab-panel" id="tab-eventlog">
  <div class="card">
    <h2>Event Log</h2>
    <div class="event-log" id="eventLog">
      <div class="entry"><span class="ts">--</span><span class="dtl" style="color:var(--text2)">Waiting for events...</span></div>
    </div>
  </div>
</div>

<!-- TAB 4: Reporting -->
<div class="tab-panel" id="tab-reporting">
<div class="card">
  <!-- Group Summary -->
  <div class="reporting-section">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <h3>📊 Account Groups</h3>
      <button class="btn btn-primary btn-sm" onclick="takeSnapshot()">📸 Take Snapshot</button>
    </div>
    <div style="overflow-x:auto;">
      <table class="rpt-table" id="groupSummaryTable">
        <thead><tr>
          <th>Name</th><th>Hedge Grp</th><th>Side</th><th>Account</th><th>Balance</th><th>Equity</th>
          <th>Group Balance</th><th>Group Equity</th>
        </tr></thead>
        <tbody id="groupSummaryBody">
          <tr><td colspan="8" style="text-align:center;color:var(--text2);padding:20px;">Set group labels on accounts (e.g. IRINA-6-A, IRINA-6-B) to see groups</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- Balance & Equity History Chart -->
  <div class="reporting-section">
    <h3>📈 Balance & Equity History</h3>
    <div class="rpt-chart-wrap">
      <div class="rpt-chart-controls" style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;">
        <label style="font-size:0.78rem;color:var(--text2);">Group:
          <select id="chartGroupSelect" onchange="drawBalanceChart()"><option value="__all__">All Groups</option></select>
        </label>
        <label style="font-size:0.78rem;color:var(--text2);">Show:
          <select id="chartMetricSelect" onchange="drawBalanceChart()">
            <option value="both">Balance & Equity</option>
            <option value="balance">Balance Only</option>
            <option value="equity">Equity Only</option>
          </select>
        </label>
      </div>
      <canvas id="balanceChart" width="800" height="220" style="width:100%;height:220px;"></canvas>
      <div id="chartEmpty" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:var(--text2);font-size:0.9rem;">No snapshot data yet — click "Take Snapshot"</div>
    </div>
  </div>

  <!-- Fee Log -->
  <div class="reporting-section">
    <h3>💰 Fee Log</h3>
    <div class="fee-keywords-wrap">
      <label style="font-size:0.78rem;color:var(--text2);white-space:nowrap;">Fee Keywords:</label>
      <input type="text" id="feeKeywordsInput" placeholder="Holding Fee, Swap, Commission">
      <button class="btn btn-sm" onclick="saveFeeKeywords()">Save</button>
    </div>
    <div style="overflow-x:auto;">
      <table class="rpt-table" id="feeLogTable">
        <thead><tr>
          <th>Date</th><th>Account</th><th>Group</th><th>Amount</th>
          <th>Before</th><th>After</th><th>Label</th><th></th>
        </tr></thead>
        <tbody id="feeLogBody">
          <tr><td colspan="8" style="text-align:center;color:var(--text2);padding:20px;">No fees recorded yet</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</div>
</div>

<!-- TAB 5: Settings -->
<div class="tab-panel" id="tab-settings">
<div class="card">
  <!-- Email Settings -->
  <div class="settings-section">
    <h3>📧 Email Notifications</h3>
    <div class="settings-grid">
      <label>Enabled</label>
      <input type="checkbox" id="setEmailEnabled">
      <label>SMTP Host</label>
      <input type="text" id="setSmtpHost" placeholder="smtp.gmail.com">
      <label>SMTP Port</label>
      <input type="number" id="setSmtpPort" value="587">
      <label>Username</label>
      <input type="text" id="setSmtpUser" placeholder="user@gmail.com">
      <label>Password</label>
      <input type="password" id="setSmtpPass" placeholder="app password">
      <label>From Address</label>
      <input type="text" id="setFromAddr" placeholder="alerts@example.com">
      <label>To Address</label>
      <input type="text" id="setToAddr" placeholder="you@example.com">
    </div>
    
    <h4 style="margin-top:20px; border-top:1px solid var(--border); padding-top:15px; color:var(--text1);">Fund Distribution Report</h4>
    <div class="settings-grid">
      <label>Enabled</label>
      <input type="checkbox" id="setFundEmailEnabled">
      <label>Send Time (NY)</label>
      <input type="time" id="setFundEmailTime" value="08:00">
    </div>

    <div class="settings-actions">
      <button class="btn btn-primary btn-sm" onclick="saveSettings()">Save Email Settings</button>
      <button class="btn btn-sm" onclick="testEmail()" id="testEmailBtn">📨 Test Email</button>
    </div>
  </div>

  <!-- Telegram Settings -->
  <div class="settings-section">
    <h3>📢 Telegram Notifications</h3>
    <div class="settings-grid">
      <label>Enabled</label>
      <input type="checkbox" id="setTgEnabled">
      <label>Bot Token</label>
      <input type="password" id="setTgBotToken" placeholder="123456:ABC-DEF...">
      <label>Chat ID</label>
      <input type="text" id="setTgChatId" placeholder="-1001234567890">
    </div>
    <div class="settings-actions">
      <button class="btn btn-primary btn-sm" onclick="saveSettings()">Save Telegram Settings</button>
      <button class="btn btn-sm" onclick="testTelegram()" id="testTgBtn">📨 Test Message</button>
    </div>
  </div>

  <!-- Per-Account Fee Thresholds -->
  <div class="settings-section">
    <h3>💰 Fee Alert Thresholds</h3>
    <p style="font-size:0.8rem;color:var(--text2);margin-bottom:10px;">Set minimum fee amount to trigger alerts per account. Default 0 = alert on any fee.</p>
    <table class="threshold-table" id="thresholdTable">
      <thead><tr><th>Account</th><th>Group</th><th>Threshold ($)</th></tr></thead>
      <tbody id="thresholdBody">
        <tr><td colspan="3" style="text-align:center;color:var(--text2);padding:16px;">No accounts yet</td></tr>
      </tbody>
    </table>
  </div>
  <!-- Margin Use Threshold -->
  <div class="settings-section">
    <h3>📊 Margin Use Threshold</h3>
    <p style="font-size:0.8rem;color:var(--text2);margin-bottom:10px;">Alert when margin use % exceeds threshold. Per-account overrides can be set in the Accounts tab or below. Empty = use global default.</p>
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
      <label style="font-size:0.85rem;font-weight:600;">Global Default (%)</label>
      <input type="number" id="setMarginAlertThreshold" value="85" min="0" max="100" step="1" style="width:70px;text-align:center;" onchange="saveMarginAlertGlobal(this.value)">
    </div>
  </div>

  <!-- Position Change Alert -->
  <div class="settings-section">
    <h3>📋 Position Change Alert</h3>
    <p style="font-size:0.8rem;color:var(--text2);margin-bottom:10px;">Send alert when position count changes on any account (opens or closures).</p>
    <div style="display:flex;flex-direction:column;gap:8px;">
      <label style="display:flex;align-items:center;gap:8px;font-size:0.85rem;cursor:pointer;">
        <input type="checkbox" id="setPosChangeAlert" onchange="savePosChangeAlert(this.checked)">
        <span>Enable position change alerts</span>
      </label>
      <div id="posChangeSuboptions" style="display:flex;flex-direction:column;gap:6px;padding-left:24px;transition:opacity 0.2s ease;">
        <label style="display:flex;align-items:center;gap:8px;font-size:0.82rem;cursor:pointer;">
          <input type="checkbox" id="setPosChangeOpened" onchange="savePosChangeSuboption('position_change_opened', this.checked)">
          <span>Position opened</span>
        </label>
        <label style="display:flex;align-items:center;gap:8px;font-size:0.82rem;cursor:pointer;">
          <input type="checkbox" id="setPosChangeClosed" onchange="savePosChangeSuboption('position_change_closed', this.checked)">
          <span>Position closed</span>
        </label>
        <label style="display:flex;align-items:center;gap:8px;font-size:0.82rem;cursor:pointer;">
          <input type="checkbox" id="setPosChangeEmail" onchange="savePosChangeSuboption('position_change_email', this.checked)">
          <span>Email</span>
        </label>
        <label style="display:flex;align-items:center;gap:8px;font-size:0.82rem;cursor:pointer;">
          <input type="checkbox" id="setPosChangeTelegram" onchange="savePosChangeSuboption('position_change_telegram', this.checked)">
          <span>Telegram</span>
        </label>
      </div>
    </div>
  </div>

  <!-- Hedge Disbalance Alert -->
  <div class="settings-section">
    <h3>🚨 Hedge Disbalance Alert</h3>
    <p style="font-size:0.8rem;color:var(--text2);margin-bottom:10px;">Send alert when a hedge disbalance is detected over the confirmation period.</p>
    <div style="display:flex;flex-direction:column;gap:8px;">
      <label style="display:flex;align-items:center;gap:8px;font-size:0.85rem;cursor:pointer;">
        <input type="checkbox" id="setDisbalanceAlert" onchange="saveDisbalanceAlert(this.checked)">
        <span>Enable hedge disbalance alerts</span>
      </label>
      <div id="disbalanceSuboptions" style="display:flex;flex-direction:column;gap:6px;padding-left:24px;transition:opacity 0.2s ease;">
        <label style="display:flex;align-items:center;gap:8px;font-size:0.82rem;cursor:pointer;">
          <input type="checkbox" id="setDisbalanceEmail" onchange="saveDisbalanceSuboption('disbalance_alert_email', this.checked)">
          <span>Email</span>
        </label>
        <label style="display:flex;align-items:center;gap:8px;font-size:0.82rem;cursor:pointer;">
          <input type="checkbox" id="setDisbalanceTelegram" onchange="saveDisbalanceSuboption('disbalance_alert_telegram', this.checked)">
          <span>Telegram</span>
        </label>
        <label style="display:flex;align-items:center;gap:8px;font-size:0.82rem;cursor:pointer;">
          <span>Confirmation Period (sec):</span>
          <input type="number" id="setDisbalancePeriod" style="width:60px;padding:2px 4px;background:#111827;border:1px solid #374151;color:#fff;border-radius:4px;" 
            onchange="saveDisbalanceSuboption('disbalance_alert_period_sec', parseInt(this.value, 10))">
        </label>
      </div>
    </div>
  </div>

  <!-- Swap Change Alert -->
  <div class="settings-section">
    <h3>💱 Swap Change Alert</h3>
    <p style="font-size:0.8rem;color:var(--text2);margin-bottom:10px;">Track swap rates on specified instruments and alert when the rate changes by more than the configured percentage.</p>
    <div style="display:flex;flex-direction:column;gap:10px;">
      <div style="display:flex;align-items:center;gap:12px;">
        <label style="font-size:0.85rem;font-weight:600;min-width:140px;">Track swap on</label>
        <input type="text" id="setSwapAlertInstruments" placeholder="e.g. USDJPY,USDCHF,XAUUSD" style="flex:1;min-width:200px;" onchange="saveSwapAlertSetting('swap_alert_instruments', this.value)">
      </div>
      <label style="display:flex;align-items:center;gap:8px;font-size:0.85rem;cursor:pointer;">
        <input type="checkbox" id="setSwapAlertEnabled" onchange="saveSwapAlertSetting('swap_alert_enabled', this.checked)">
        <span>Enable swap change alerts</span>
      </label>
      <div style="display:flex;align-items:center;gap:12px;">
        <label style="font-size:0.85rem;font-weight:600;min-width:140px;">Swap % change</label>
        <input type="number" id="setSwapAlertPct" value="10" min="0" max="100" step="1" style="width:70px;text-align:center;" onchange="saveSwapAlertSetting('swap_alert_pct', parseFloat(this.value) || 10)">
        <span style="font-size:0.8rem;color:var(--text2);">% threshold</span>
      </div>
      <div style="display:flex;align-items:center;gap:12px;">
        <label style="font-size:0.85rem;font-weight:600;min-width:140px;">Check interval</label>
        <input type="number" id="setSwapAlertInterval" value="60" min="1" max="1440" step="1" style="width:70px;text-align:center;" onchange="saveSwapAlertSetting('swap_alert_interval_min', parseFloat(this.value) || 60)">
        <span style="font-size:0.8rem;color:var(--text2);">minutes</span>
      </div>
    </div>
  </div>

  <!-- Rebalance Close Delay -->
  <div class="settings-section">
    <h3>⚖️ Hedge Rebalance</h3>
    <p style="font-size:0.8rem;color:var(--text2);margin-bottom:10px;">Pause between individual close commands when rebalancing an imbalanced hedge. 0 = close all as fast as possible (batch). Default: 1 second.</p>
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
      <label style="font-size:0.85rem;font-weight:600;">Close Delay (seconds)</label>
      <input type="number" id="setRebalCloseDelay" value="1" min="0" max="60" step="0.5" style="width:70px;text-align:center;" onchange="saveRebalCloseDelay(this.value)">
    </div>
    <label style="display:flex;align-items:center;gap:8px;font-size:0.85rem;cursor:pointer;margin-top:4px;">
      <input type="checkbox" id="setPromptOnRollbacks" onchange="saveExecSetting('prompt_on_rollbacks', this.checked)">
      <span>Prompt for confirmation before executing rollback closes</span>
    </label>
    <p style="font-size:0.78rem;color:var(--text2);margin-top:6px;padding-left:24px;">When enabled, a Yes/No popup will appear before any hedge-rebalance rollback is executed. Use this to prevent accidental mass-closes on restart.</p>
  </div>

  <!-- EA Poll Enable/Disable -->
  <div class="settings-section">
    <h3>📡 EA Poll Accounts</h3>
    <p style="font-size:0.8rem;color:var(--text2);margin-bottom:10px;">Controls whether MetaTrader Expert Advisor heartbeat polls (<code>/api/poll_command</code>) are accepted. Disable to stop EAs from updating account data while using MT Direct connections.</p>
    <label style="display:flex;align-items:center;gap:8px;font-size:0.85rem;cursor:pointer;">
      <input type="checkbox" id="setEaPollEnabled" onchange="saveEaPollEnabled(this.checked)">
      <span>Enable EA Poll account heartbeats</span>
    </label>
    <p style="font-size:0.78rem;color:var(--text2);margin-top:6px;padding-left:24px;">When disabled, all EA poll requests return an empty response. Note: MT Direct connections manage their own data and are unaffected.</p>
  </div>

  <!-- Trading Parameters -->
  <div class="settings-section">
    <h3>⚡ Trading Parameters</h3>
    <p style="font-size:0.8rem;color:var(--text2);margin-bottom:10px;">Execution timeout controls for open/close/cycle commands. If the broker does not confirm a fill within the timeout, the system can alert and/or halt.</p>
    <div style="display:flex;flex-direction:column;gap:10px;">
      <div style="display:flex;align-items:center;gap:12px;">
        <label style="font-size:0.85rem;font-weight:600;min-width:240px;">Wait time for execution responses</label>
        <input type="number" id="setExecTimeout" value="60" min="5" max="300" step="5" style="width:70px;text-align:center;" onchange="saveExecSetting('exec_timeout_sec', parseFloat(this.value) || 60)">
        <span style="font-size:0.8rem;color:var(--text2);">seconds</span>
      </div>
      <label style="display:flex;align-items:center;gap:8px;font-size:0.85rem;cursor:pointer;">
        <input type="checkbox" id="setExecAlertOnTimeout" onchange="saveExecSetting('exec_alert_on_timeout', this.checked)">
        <span>Alert on no execution response</span>
      </label>
      <label style="display:flex;align-items:center;gap:8px;font-size:0.85rem;cursor:pointer;">
        <input type="checkbox" id="setExecHaltOnTimeout" onchange="saveExecSetting('exec_halt_on_timeout', this.checked)">
        <span>Halt on execution timeout <span style="font-size:0.72rem;color:var(--text2);">(stops cycle/open/close if no fill received)</span></span>
      </label>
      <label style="display:flex;align-items:center;gap:8px;font-size:0.85rem;cursor:pointer;">
        <input type="checkbox" id="setExecRetryClose" onchange="saveExecSetting('exec_retry_close', this.checked)">
        <span>Retry close on timeout <span style="font-size:0.72rem;color:var(--text2);">(safe — re-sends close if no fill; does NOT retry opens)</span></span>
      </label>
      <div style="display:flex;align-items:center;gap:12px;padding-left:26px;">
        <label style="font-size:0.85rem;font-weight:600;min-width:214px;">Max retry attempts</label>
        <input type="number" id="setExecRetryMax" value="5" min="1" max="50" step="1" style="width:70px;text-align:center;" onchange="saveExecSetting('exec_retry_max', parseInt(this.value) || 5)">
        <span style="font-size:0.8rem;color:var(--text2);">times (then halt)</span>
      </div>
    </div>
  </div>

  <!-- Theme Colors -->
  <div class="settings-section">
    <h3>🎨 Theme Colors</h3>
    <p style="font-size:0.8rem;color:var(--text2);margin-bottom:12px;">Customize the dashboard color scheme. Changes apply instantly.</p>
    <div class="theme-color-grid" id="themeColorGrid"></div>
    <div class="settings-actions">
      <button class="btn btn-primary btn-sm" onclick="saveThemeColors(this)">Save Theme</button>
      <button class="btn btn-sm" onclick="resetThemeColors()" style="background:var(--surface2);color:var(--text);">Reset to Defaults</button>
    </div>
  </div>

</div>
</div>

</div>

<!-- Edit Modal -->
<div class="modal-overlay" id="editModal">
  <div class="modal">
    <h2>Edit Instrument</h2>
    <input type="hidden" id="editSessionId">
    <div class="form-grid">
      <div class="form-group">
        <label>Pair (global default)</label>
        <input type="text" id="ePair">
      </div>
      <div class="form-group">
        <label>Lot Size (global default)</label>
        <input type="number" id="eLotSize" step="0.01" min="0.01">
      </div>
      <div class="form-group">
        <label>Total Positions</label>
        <input type="number" id="eTotalPositions" min="1">
      </div>
      <div class="form-group">
        <label>Max Spread (points)</label>
        <input type="number" id="eMaxSpread" min="1">
      </div>
      <div class="form-group">
        <label>Max Errors <span style="font-size:0.7rem;color:var(--text2)">(0=unlimited)</span></label>
        <input type="number" id="eMaxErrors" min="0">
      </div>
      <div class="form-group">
        <label>Trade Pause (s)</label>
        <input type="number" id="eTradePause" min="0" step="0.1">
      </div>
      <div class="form-group">
        <label>Diff to Open <span style="font-size:0.7rem;color:var(--text2)">(pts, 0=off)</span></label>
        <input type="number" id="eDiffToOpen" min="0" step="1">
      </div>
      <div class="form-group">
        <label>Diff to Close <span style="font-size:0.7rem;color:var(--text2)">(pts, 0=off)</span></label>
        <input type="number" id="eDiffToClose" min="0" step="1">
      </div>
      <div class="form-group">
        <label>Max Accum Lots <span style="font-size:0.7rem;color:var(--text2)">(0=off)</span></label>
        <input type="number" id="eMaxAccumLots" min="0" step="0.01">
      </div>
      <div class="form-group">
        <label>Max Accum Deals <span style="font-size:0.7rem;color:var(--text2)">(0=off)</span></label>
        <input type="number" id="eMaxAccumDeals" min="0" step="1">
      </div>

      <div class="form-group">
        <label>Execution Order</label>
        <select id="eExecOrder">
          <option value="simultaneous">Both Simultaneously</option>
          <option value="side1_first">Side 1 First</option>
          <option value="side2_first">Side 2 First</option>
        </select>
      </div>
      <div class="form-group" style="grid-column: 1 / -1;">
        <label>Comment Tag <span style="font-size:0.7rem;color:var(--text2)">(identifier for this instrument's trades)</span></label>
        <input type="text" id="eComment" placeholder="e.g. MT4-123-MT5-456">
      </div>
    </div>
    <div class="sides-config" style="margin-top:16px;">
      <div class="side-box" id="editSide1Box" style="display:none;">
        <h3 id="editSide1Title">Side 1</h3>
        <div class="form-group">
          <label>Action</label>
          <select id="eSide1Action">
            <option value="buy">Buy</option>
            <option value="sell">Sell</option>
          </select>
        </div>
        <div class="form-group">
          <label>Symbol</label>
          <input type="text" id="eSide1Pair" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Lot Size</label>
          <input type="number" id="eSide1Lots" step="0.01" min="0.01" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Max Spread</label>
          <input type="number" id="eSide1MaxSpread" min="1" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Comment <span style="font-size:0.7rem;color:var(--text2)">(for this acct's trades)</span></label>
          <input type="text" id="eSide1Comment" placeholder="e.g. hedge-A">
        </div>
      </div>
      <div class="side-box" id="editSide2Box" style="display:none;">
        <h3 id="editSide2Title">Side 2</h3>
        <div class="form-group">
          <label>Action</label>
          <select id="eSide2Action">
            <option value="buy">Buy</option>
            <option value="sell">Sell</option>
          </select>
        </div>
        <div class="form-group">
          <label>Symbol</label>
          <input type="text" id="eSide2Pair" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Lot Size</label>
          <input type="number" id="eSide2Lots" step="0.01" min="0.01" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Max Spread</label>
          <input type="number" id="eSide2MaxSpread" min="1" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Comment <span style="font-size:0.7rem;color:var(--text2)">(for this acct's trades)</span></label>
          <input type="text" id="eSide2Comment" placeholder="e.g. hedge-B">
        </div>
      </div>
    </div>
    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" onclick="saveEdit()">Save Changes</button>
      <button class="btn btn-danger" onclick="closeModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Add Account Modal -->
<div class="modal-overlay" id="addAccountModal">
  <div class="modal" style="max-width:450px;">
    <h2>Add EA Account</h2>
    <div class="form-grid" style="grid-template-columns:1fr;">
      <div class="form-group">
        <label>Account Name / Number</label>
        <input type="text" id="aAcctName" placeholder="e.g. 11271572 or MyFIXAccount">
      </div>
      <div class="form-group">
        <label>Group Label</label>
        <input type="text" id="aGroupLabel" placeholder="e.g. A">
      </div>

    </div>
    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" onclick="addAccount()">Add</button>
      <button class="btn btn-danger" onclick="closeAddAccountModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Add FIX Account Modal -->
<div class="modal-overlay" id="addFixAccountModal">
  <div class="modal" style="max-width:600px;">
    <h2>Add API Account</h2>
    <div class="form-grid" style="grid-template-columns:1fr 1fr;">
      <div class="form-group">
        <label>Implementation</label>
        <select id="fxImpl" onchange="toggleFixFields('fx')">
          <option value="ctrader" selected>cTrader FIX</option>
          <option value="swissquote">Swissquote CFXD</option>
          <option value="dukascopy">Dukascopy FIX</option>
          <option value="openapi">cTrader Open API</option>
        </select>
      </div>
      <div class="form-group">
        <label>Account ID <span style="font-size:0.7rem;color:var(--text2)">(unique key)</span></label>
        <input type="text" id="fxAcctId" placeholder="e.g. ct_demo">
      </div>
      <div class="form-group">
        <label>Label</label>
        <input type="text" id="fxLabel" placeholder="e.g. ICMarkets Demo">
      </div>
      <div class="form-group">
        <label>Group Label</label>
        <input type="text" id="fxGroupLabel" placeholder="e.g. A">
      </div>
      <div class="form-group">
        <label>Host</label>
        <input type="text" id="fxHost" placeholder="e.g. h43.p.ctrader.com">
      </div>
      <div class="form-group">
        <label>Trade Port</label>
        <input type="number" id="fxTradePort" value="5202">
      </div>
      <div class="form-group">
        <label>Quote Port</label>
        <input type="number" id="fxQuotePort" value="5201">
      </div>
      <div class="form-group">
        <label>SenderCompID</label>
        <input type="text" id="fxSenderCompId" placeholder="e.g. demo.icmarkets.9085248">
      </div>
      <div class="form-group" id="fxSenderCompIdQuoteGroup" style="display:none;">
        <label>SenderCompID (Quote)</label>
        <input type="text" id="fxSenderCompIdQuote" placeholder="e.g. DEMO2uotdK_DEMOQUOTE">
      </div>
      <div class="form-group">
        <label>TargetCompID</label>
        <input type="text" id="fxTargetCompId" value="cServer">
      </div>
      <div class="form-group">
        <label>Username</label>
        <input type="text" id="fxUsername" placeholder="e.g. 9085248">
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" id="fxPassword" placeholder="FIX API password">
      </div>
      <div class="form-group">
        <label>Heartbeat (sec)</label>
        <input type="number" id="fxHeartbeat" value="30">
      </div>
      <div class="form-group">
        <label>Lot Multiplier</label>
        <input type="number" id="fxLotMult" value="100000">
      </div>
      <div class="form-group">
        <label>Leverage <span style="font-size:0.7rem;color:var(--text2)">(e.g. 500)</span></label>
        <input type="number" id="fxLeverage" placeholder="e.g. 500">
      </div>
      <div class="form-group">
        <label>Stop Out Level (%) <span style="font-size:0.7rem;color:var(--text2)">(optional)</span></label>
        <input type="number" id="fxStopOutLevel" step="0.1" min="0" max="100" placeholder="e.g. 50">
      </div>
      <div class="form-group">
        <label>Use SSL</label>
        <select id="fxUseSSL">
          <option value="false" selected>No (cleartext)</option>
          <option value="true">Yes (SSL/TLS)</option>
        </select>
      </div>
      <div class="form-group">
        <label>Symbol File <span style="font-size:0.7rem;color:var(--text2)">(optional)</span></label>
        <input type="text" id="fxSymbolFile" placeholder="e.g. demo.icmarkets-ctraderid.txt">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Email(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="fxAlertEmails" placeholder="e.g. override1@example.com, override2@example.com">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Telegram ID(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="fxAlertTelegramIds" placeholder="e.g. 12345678, -987654321">
      </div>
    </div>
    <details style="margin-top:12px;">
      <summary style="cursor:pointer;color:var(--accent);font-size:0.82rem;font-weight:600;">▶ Open API (Balance/Equity)</summary>
      <div class="form-grid" style="grid-template-columns:1fr 1fr;margin-top:8px;">
        <div class="form-group">
          <label>Client ID</label>
          <input type="text" id="fxOaClientId" placeholder="App client ID from openapi.ctrader.com">
        </div>
        <div class="form-group">
          <label>Client Secret</label>
          <input type="password" id="fxOaClientSecret" placeholder="App secret">
        </div>
        <div class="form-group" style="grid-column:1/-1;">
          <button class="btn btn-sm" style="background:var(--accent);color:#fff;font-size:0.78rem;padding:4px 12px" onclick="oaAuthorize('fx')">🔗 Authorize via cTrader</button>
          <span style="font-size:0.72rem;color:var(--text2);margin-left:8px;">Fill Client ID & Secret first, then click to get tokens</span>
        </div>
        <div class="form-group">
          <label>Access Token</label>
          <input type="text" id="fxOaAccessToken" placeholder="OAuth access token">
        </div>
        <div class="form-group">
          <label>Refresh Token</label>
          <input type="text" id="fxOaRefreshToken" placeholder="OAuth refresh token">
        </div>
        <div class="form-group">
          <label>Account ID <span style="font-size:0.7rem;color:var(--text2)">(ctidTraderAccountId)</span></label>
          <input type="text" id="fxOaAccountId" placeholder="e.g. 30128348">
        </div>
        <div class="form-group">
          <label>Environment</label>
          <select id="fxOaEnv">
            <option value="demo" selected>Demo</option>
            <option value="live">Live</option>
          </select>
        </div>
      </div>
    </details>
    <div style="margin-top:8px; display: none;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="fxAutoConnect" checked> Auto Connect at Start</label>
    </div>
    <div style="margin-top:8px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="fxCycleReminder"> Cycle Reminder</label>
      <label style="font-size:0.78rem;color:var(--text2);">Remind <input type="number" id="fxCycleRemindDays" value="" min="0" max="30" style="width:50px;margin-left:4px;"></label>
      <label style="font-size:0.78rem;color:var(--text2);">Max Days <input type="number" id="fxCycleMaxDays" value="" min="0" max="30" style="width:50px;margin-left:4px;"></label>
    </div>
    <div style="margin-top:8px;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="fxAutoCycle"> Auto Cycle (close+reopen at max days)</label>
    </div>
    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" onclick="addFixAccount(true)">Connect</button>
      <button class="btn btn-success" onclick="addFixAccount(false)">Save</button>
      <button class="btn btn-danger" onclick="closeAddFixAccountModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Add MT Direct Account Modal -->
<div class="modal-overlay" id="addMTDirectModal">
  <div class="modal" style="max-width:500px;">
    <h2>Add MT Direct Account</h2>
    <div class="form-grid" style="grid-template-columns:1fr 1fr;">
      <div class="form-group">
        <label>Platform Type</label>
        <select id="mtdType">
          <option value="mt4" selected>MT4 Direct</option>
          <option value="mt5">MT5 Direct</option>
        </select>
      </div>
      <div class="form-group">
        <label>Name <span style="font-size:0.7rem;color:var(--text2)">(used as account identifier &amp; in trade comments)</span></label>
        <input type="text" id="mtdAcctId" placeholder="e.g. MT4-Broker1">
      </div>
      <div class="form-group">
        <label>Login</label>
        <input type="text" id="mtdLogin" placeholder="Trading account login">
      </div>
      <div class="form-group">
        <label>Password</label>
        <div style="display:flex;align-items:center;gap:4px;"><input type="password" id="mtdPassword" placeholder="Trading password" style="flex:1;min-width:0;"><button type="button" onclick="togglePwdVis('mtdPassword',this)" style="background:none;border:none;color:var(--text2);cursor:pointer;font-size:1rem;padding:2px 4px;flex-shrink:0;" title="Show/hide password">&#128065;</button></div>
      </div>
      <div class="form-group">
        <label>Server</label>
        <input type="text" id="mtdServer" placeholder="e.g. broker-live.com">
      </div>
      <div class="form-group">
        <label>Port</label>
        <input type="number" id="mtdPort" value="443">
      </div>
      <div class="form-group">
        <label>Slippage <span style="font-size:0.7rem;color:var(--text2)">(pts)</span></label>
        <input type="number" id="mtdSlippage" value="3">
      </div>
      <div class="form-group">
        <label>Stop Out Level (%) <span style="font-size:0.7rem;color:var(--text2)">(optional)</span></label>
        <input type="number" id="mtdStopOutLevel" step="0.1" min="0" max="100" placeholder="e.g. 20">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Email(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="mtdAlertEmails" placeholder="e.g. override1@example.com, override2@example.com">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Telegram ID(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="mtdAlertTelegramIds" placeholder="e.g. 12345678, -987654321">
      </div>
    </div>
    <div style="margin-top:8px; display: none;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="mtdAutoConnect" checked> Auto Connect at Start</label>
    </div>
    <div style="margin-top:8px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="mtdCycleReminder"> Cycle Reminder</label>
      <label style="font-size:0.78rem;color:var(--text2);">Remind <input type="number" id="mtdCycleRemindDays" value="" min="0" max="30" style="width:50px;margin-left:4px;"></label>
      <label style="font-size:0.78rem;color:var(--text2);">Max Days <input type="number" id="mtdCycleMaxDays" value="" min="0" max="30" style="width:50px;margin-left:4px;"></label>
    </div>
    <div style="margin-top:8px;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="mtdAutoCycle"> Auto Cycle (close+reopen at max days)</label>
    </div>
    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" onclick="addMTDirectAccount(true)">Connect</button>
      <button class="btn btn-success" onclick="addMTDirectAccount(false)">Save</button>
      <button class="btn btn-danger" onclick="closeAddMTDirectModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Edit EA/Manual Account Modal -->
<div class="modal-overlay" id="editEAAccountModal">
  <div class="modal" style="max-width:480px;">
    <h2>Edit Account</h2>
    <input type="hidden" id="eeaAcctName">
    <div class="form-grid" style="grid-template-columns:1fr 1fr;">
      <div class="form-group">
        <label>Connection Type</label>
        <select id="eeaConnType" onchange="toggleEADirectFields()">
          <option value="poll">EA Poll</option>
          <option value="manual">Manual</option>
          <option value="mt4_direct">MT4 Direct</option>
          <option value="mt5_direct">MT5 Direct</option>
        </select>
      </div>
      <div class="form-group">
        <label>Group Label</label>
        <input type="text" id="eeaGroupLabel" placeholder="e.g. TEST-1-A">
      </div>
      <div class="form-group">
        <label>Fee Threshold ($)</label>
        <input type="number" id="eeaFeeThreshold" step="0.01" placeholder="e.g. 5.00">
      </div>
      <div class="form-group">
        <label>Stop Out Level (%)</label>
        <input type="number" id="eeaStopOutLevel" step="0.1" min="0" max="100" placeholder="e.g. 50">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Email(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="eeaAlertEmails" placeholder="e.g. override1@example.com, override2@example.com">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Telegram ID(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="eeaAlertTelegramIds" placeholder="e.g. 12345678, -987654321">
      </div>
    </div>
    <!-- MT Direct fields (shown only when mt4_direct or mt5_direct selected) -->
    <div id="eeaDirectFields" style="display:none;margin-top:12px;padding:12px;border:1px solid var(--border);border-radius:8px;background:rgba(99,102,241,0.06);">
      <h3 style="margin:0 0 8px;font-size:0.85rem;color:#6366f1;">MT Direct Connection</h3>
      <div class="form-grid" style="grid-template-columns:1fr 1fr;">
        <div class="form-group">
          <label>Login</label>
          <input type="text" id="eeaLogin" placeholder="Trading account login">
        </div>
        <div class="form-group">
          <label>Password</label>
          <div style="display:flex;align-items:center;gap:4px;"><input type="password" id="eeaPassword" placeholder="Trading password" style="flex:1;min-width:0;"><button type="button" onclick="togglePwdVis('eeaPassword',this)" style="background:none;border:none;color:var(--text2);cursor:pointer;font-size:1rem;padding:2px 4px;flex-shrink:0;" title="Show/hide password">&#128065;</button></div>
        </div>
        <div class="form-group">
          <label>Server</label>
          <input type="text" id="eeaServer" placeholder="e.g. broker-live.com">
        </div>
        <div class="form-group">
          <label>Port</label>
          <input type="number" id="eeaPort" value="443">
        </div>
        <div class="form-group">
          <label>Slippage (pts)</label>
          <input type="number" id="eeaSlippage" value="3">
        </div>
        <div class="form-group" style="grid-column: 1 / -1;">
          <label>Name <span style="font-size:0.7rem;color:var(--text2)">(display name on dashboard)</span></label>
          <input type="text" id="eeaDirectLabel" placeholder="e.g. MyBroker-MT4">
        </div>
      </div>
    </div>
    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" onclick="saveEAAccountEdit()">Save</button>
      <button class="btn btn-danger" onclick="closeEditEAAccountModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Edit MT Direct Account Modal -->
<div class="modal-overlay" id="editMTDirectModal">
  <div class="modal" style="max-width:500px;">
    <h2>Edit MT Direct Account</h2>
    <input type="hidden" id="emtdAcctId">
    <div class="form-grid" style="grid-template-columns:1fr 1fr;">
      <div class="form-group">
        <label>Platform Type</label>
        <select id="emtdType">
          <option value="mt4">MT4 Direct</option>
          <option value="mt5">MT5 Direct</option>
        </select>
      </div>
      <div class="form-group">
        <label>Login</label>
        <input type="text" id="emtdLogin">
      </div>
      <div class="form-group">
        <label>Password</label>
        <div style="display:flex;align-items:center;gap:4px;"><input type="password" id="emtdPassword" placeholder="Leave as-is to keep current" style="flex:1;min-width:0;"><button type="button" onclick="togglePwdVis('emtdPassword',this)" style="background:none;border:none;color:var(--text2);cursor:pointer;font-size:1rem;padding:2px 4px;flex-shrink:0;" title="Show/hide password">&#128065;</button></div>
      </div>
      <div class="form-group">
        <label>Server</label>
        <input type="text" id="emtdServer">
      </div>
      <div class="form-group">
        <label>Port</label>
        <input type="number" id="emtdPort">
      </div>
      <div class="form-group">
        <label>Name</label>
        <input type="text" id="emtdLabel">
      </div>
      <div class="form-group">
        <label>Slippage (pts)</label>
        <input type="number" id="emtdSlippage">
      </div>
      <div class="form-group">
        <label>Magic Number</label>
        <input type="number" id="emtdMagic">
      </div>
      <div class="form-group">
        <label>Stop Out Level (%)</label>
        <input type="number" id="emtdStopOutLevel" step="0.1" min="0" max="100" placeholder="e.g. 20">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Email(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="emtdAlertEmails" placeholder="e.g. override1@example.com, override2@example.com">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Telegram ID(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="emtdAlertTelegramIds" placeholder="e.g. 12345678, -987654321">
      </div>
    </div>
    <div style="margin-top:8px; display: none;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="emtdAutoConnect"> Auto Connect at Start</label>
    </div>
    <div style="margin-top:8px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="emtdCycleReminder"> Cycle Reminder</label>
      <label style="font-size:0.78rem;color:var(--text2);">Remind <input type="number" id="emtdCycleRemindDays" value="" min="0" max="30" style="width:50px;margin-left:4px;"></label>
      <label style="font-size:0.78rem;color:var(--text2);">Max Days <input type="number" id="emtdCycleMaxDays" value="" min="0" max="30" style="width:50px;margin-left:4px;"></label>
    </div>
    <div style="margin-top:8px;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="emtdAutoCycle"> Auto Cycle (close+reopen at max days)</label>
    </div>
    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" onclick="saveMTDirectEdit()">Save</button>
      <button class="btn btn-danger" onclick="closeEditMTDirectModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Edit FIX Account Modal -->
<div class="modal-overlay" id="editFixAccountModal">
  <div class="modal" style="max-width:600px;">
    <h2>Edit API Account</h2>
    <input type="hidden" id="efxAcctId">
    <div class="form-grid" style="grid-template-columns:1fr 1fr;">
      <div class="form-group">
        <label>Implementation</label>
        <select id="efxImpl" onchange="toggleFixFields('efx')">
          <option value="ctrader" selected>cTrader FIX</option>
          <option value="swissquote">Swissquote CFXD</option>
          <option value="dukascopy">Dukascopy FIX</option>
          <option value="openapi">cTrader Open API</option>
        </select>
      </div>
      <div class="form-group">
        <label>Account ID</label>
        <input type="text" id="efxAcctIdDisplay" disabled style="opacity:0.6;">
      </div>
      <div class="form-group">
        <label>Label</label>
        <input type="text" id="efxLabel">
      </div>
      <div class="form-group">
        <label>Group Label</label>
        <input type="text" id="efxGroupLabel">
      </div>
      <div class="form-group">
        <label>Host</label>
        <input type="text" id="efxHost">
      </div>
      <div class="form-group">
        <label>Trade Port</label>
        <input type="number" id="efxTradePort">
      </div>
      <div class="form-group">
        <label>Quote Port</label>
        <input type="number" id="efxQuotePort">
      </div>
      <div class="form-group">
        <label>SenderCompID</label>
        <input type="text" id="efxSenderCompId">
      </div>
      <div class="form-group" id="efxSenderCompIdQuoteGroup" style="display:none;">
        <label>SenderCompID (Quote)</label>
        <input type="text" id="efxSenderCompIdQuote">
      </div>
      <div class="form-group">
        <label>TargetCompID</label>
        <input type="text" id="efxTargetCompId">
      </div>
      <div class="form-group">
        <label>Username</label>
        <input type="text" id="efxUsername">
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" id="efxPassword">
      </div>
      <div class="form-group">
        <label>Heartbeat (sec)</label>
        <input type="number" id="efxHeartbeat">
      </div>
      <div class="form-group">
        <label>Lot Multiplier</label>
        <input type="number" id="efxLotMult">
      </div>
      <div class="form-group">
        <label>Leverage <span style="font-size:0.7rem;color:var(--text2)">(e.g. 500)</span></label>
        <input type="number" id="efxLeverage">
      </div>
      <div class="form-group">
        <label>Stop Out Level (%)</label>
        <input type="number" id="efxStopOutLevel" step="0.1" min="0" max="100" placeholder="e.g. 50">
      </div>
      <div class="form-group">
        <label>Use SSL</label>
        <select id="efxUseSSL">
          <option value="false">No (cleartext)</option>
          <option value="true">Yes (SSL/TLS)</option>
        </select>
      </div>
      <div class="form-group">
        <label>Symbol File <span style="font-size:0.7rem;color:var(--text2)">(optional)</span></label>
        <input type="text" id="efxSymbolFile">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Email(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="efxAlertEmails" placeholder="e.g. override1@example.com, override2@example.com">
      </div>
      <div class="form-group" style="grid-column: 1 / -1; margin-top: 8px; display: none;">
        <label>Alert Telegram ID(s) Override <span style="font-size:0.7rem;color:var(--text2)">(comma-separated; optional)</span></label>
        <input type="text" id="efxAlertTelegramIds" placeholder="e.g. 12345678, -987654321">
      </div>
    </div>
    <details style="margin-top:12px;">
      <summary style="cursor:pointer;color:var(--accent);font-size:0.82rem;font-weight:600;">▶ Open API (Balance/Equity)</summary>
      <div class="form-grid" style="grid-template-columns:1fr 1fr;margin-top:8px;">
        <div class="form-group">
          <label>Client ID</label>
          <input type="text" id="efxOaClientId" placeholder="App client ID">
        </div>
        <div class="form-group">
          <label>Client Secret</label>
          <input type="password" id="efxOaClientSecret" placeholder="App secret">
        </div>
        <div class="form-group" style="grid-column:1/-1;">
          <button class="btn btn-sm" style="background:var(--accent);color:#fff;font-size:0.78rem;padding:4px 12px" onclick="oaAuthorize('efx')">🔗 Authorize via cTrader</button>
          <span style="font-size:0.72rem;color:var(--text2);margin-left:8px;">Fill Client ID & Secret first, then click to get tokens</span>
        </div>
        <div class="form-group">
          <label>Access Token</label>
          <input type="text" id="efxOaAccessToken" placeholder="OAuth access token">
        </div>
        <div class="form-group">
          <label>Refresh Token</label>
          <input type="text" id="efxOaRefreshToken" placeholder="OAuth refresh token">
        </div>
        <div class="form-group">
          <label>Account ID <span style="font-size:0.7rem;color:var(--text2)">(ctidTraderAccountId)</span></label>
          <input type="text" id="efxOaAccountId" placeholder="e.g. 30128348">
        </div>
        <div class="form-group">
          <label>Environment</label>
          <select id="efxOaEnv">
            <option value="demo">Demo</option>
            <option value="live">Live</option>
          </select>
        </div>
      </div>
    </details>
    <div style="margin-top:8px; display: none;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="efxAutoConnect" checked> Auto Connect at Start</label>
    </div>
    <div style="margin-top:8px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="efxCycleReminder"> Cycle Reminder</label>
      <label style="font-size:0.78rem;color:var(--text2);">Remind <input type="number" id="efxCycleRemindDays" value="" min="0" max="30" style="width:50px;margin-left:4px;"></label>
      <label style="font-size:0.78rem;color:var(--text2);">Max Days <input type="number" id="efxCycleMaxDays" value="" min="0" max="30" style="width:50px;margin-left:4px;"></label>
    </div>
    <div style="margin-top:8px;">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer;"><input type="checkbox" id="efxAutoCycle"> Auto Cycle (close+reopen at max days)</label>
    </div>
    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" onclick="saveFixAccountEdit()">Save</button>
      <button class="btn btn-danger" onclick="closeEditFixAccountModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- New Strategy Modal -->
<div class="modal-overlay" id="newStrategyModal">
  <div class="modal" style="max-width:500px;">
    <h2>New Strategy</h2>
    <div class="form-grid" style="grid-template-columns:1fr;">
      <div class="form-group">
        <label>Strategy Name</label>
        <input type="text" id="sStratName" placeholder="e.g. EURUSD Arb">
      </div>
      <div class="form-group">
        <label>Account 1</label>
        <select id="sAcct1"><option value="">— select account —</option></select>
      </div>
      <div class="form-group">
        <label>Account 2</label>
        <select id="sAcct2"><option value="">— select account —</option></select>
      </div>
    </div>
    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" onclick="createStrategy()">Create</button>
      <button class="btn btn-danger" onclick="closeNewStrategyModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Edit Strategy Modal (strategy details + instruments) -->
<div class="modal-overlay" id="editStrategyModal">
  <div class="modal" style="max-width:95vw;width:1200px;padding:8px 16px 16px;">
    <input type="hidden" id="esStratId">
    <span id="editStrategyTitle" style="display:none;"></span>

    <!-- Strategy Sub-Tabs -->
    <div class="tab-nav" id="stratTabNav" style="margin-bottom:8px;">
      <button class="tab-btn active" data-stab="stab-instruments" onclick="switchStratTab('stab-instruments')">📊 Instruments &amp; Orders</button>
      <button class="tab-btn" data-stab="stab-log" onclick="switchStratTab('stab-log')">📋 Log</button>
      <button class="tab-btn" data-stab="stab-settings" onclick="switchStratTab('stab-settings')">⚙ Settings</button>
    </div>

    <!-- Tab: Instruments & Orders -->
    <div class="tab-panel active" id="stab-instruments">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
        <div style="display:flex;align-items:center;gap:8px;">
          <button class="btn btn-primary" onclick="showNewInstrumentModal()" style="padding:4px 12px;font-size:0.78rem;">+ Add</button>
          <button class="btn" onclick="showImportModal()" style="padding:4px 12px;font-size:0.78rem;background:var(--accent);color:white;">⬇ Import</button>
          <div class="col-toggle-wrap">
            <button class="col-toggle-btn" onclick="toggleColMenu()" title="Show/hide columns">👁 Columns</button>
            <div class="col-toggle-menu" id="colToggleMenu"></div>
          </div>
        </div>
      </div>
      <div style="overflow-x:auto;">
        <table class="sessions-table" id="instrTable">
          <thead>
            <tr>
              <th data-col="0">Status</th>
              <th data-col="1">Pair</th>
              <th data-col="2">Lots</th>
              <th data-col="3">Side 1</th>
              <th data-col="4">Side 2</th>
              <th data-col="5">Diff<br>Open</th>
              <th data-col="6">Diff<br>Close</th>
              <th data-col="7">Max<br>Spd1</th>
              <th data-col="8">Max<br>Spd2</th>
              <th data-col="9">Diff 1</th>
              <th data-col="10">Diff 2</th>
              <th data-col="11">Spd 1</th>
              <th data-col="12">Spd 2</th>
              <th data-col="13">Exec<br>Order</th>
              <th data-col="14">Progress</th>
              <th data-col="15">Actions</th>
              <th data-col="16" title="Block trade if either side has more bid changes per 5 seconds than this value (0 = off)">Max<br>Ticks</th>
              <th data-col="17" title="Block trade if bid or ask jumped more than this many pips since last tick (0 = off)">Max<br>Jump</th>
              <th data-col="18">Skew<br>Open</th>
              <th data-col="19">Skew<br>Close</th>
              <th data-col="20" title="Avoid trading ±1 minute around high-impact news events (ForexFactory calendar)">Avoid<br>News</th>

            </tr>
          </thead>
          <tbody id="instrBody">
            <tr><td colspan="21" style="text-align:center;color:var(--text2);padding:30px;">No instruments yet</td></tr>
          </tbody>
        </table>
      </div>

      <!-- Positions divider & section -->
      <hr class="positions-divider">
      <div class="positions-header">
        <button class="pos-collapse-btn" id="posCollapseBtn" onclick="togglePositionsPane()" title="Collapse / Expand positions pane">
          <span class="chevron">▼</span>
        </button>
        <h3>📋 Positions</h3>
        <div class="pos-tab-nav" id="posTabNav">
          <button class="pos-tab-btn active" data-ptab="ptab-opened" onclick="switchPosTab('ptab-opened')">Opened Deals</button>
          <button class="pos-tab-btn" data-ptab="ptab-closed" onclick="switchPosTab('ptab-closed')">Closed Deals</button>
          <button class="pos-tab-btn" data-ptab="ptab-pending" onclick="switchPosTab('ptab-pending')">Pending</button>
          <button class="pos-tab-btn" data-ptab="ptab-rejected" onclick="switchPosTab('ptab-rejected')">Rejected</button>
        </div>
      </div>

      <div class="pos-panels-wrapper" id="posPanelsWrapper">
      <!-- Opened Deals -->
      <div class="pos-tab-panel active" id="ptab-opened">
        <div style="overflow-x:auto;">
          <table class="deals-table" id="openedDealsTable">
            <thead>
              <tr>
                <th>Pair Label</th>
                <th>Session</th>
                <th>Side</th>
                <th>Lots</th>
                <th>Time</th>
                <th>Ticket</th>
                <th>Price</th>
                <th>Open Diff</th>
                <th>Open ms</th>
                <th>Slippage</th>
                <th>Profit</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="openedDealsBody">
              <tr><td colspan="12" style="text-align:center;color:var(--text2);padding:20px;">No open deals</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Closed Deals -->
      <div class="pos-tab-panel" id="ptab-closed">
        <div style="overflow-x:auto;">
          <table class="deals-table" id="closedDealsTable">
            <thead>
              <tr>
                <th>Pair Label</th>
                <th>Session</th>
                <th>Side</th>
                <th>Lots</th>
                <th>Open Time</th>
                <th>Close Time</th>
                <th>Ticket</th>
                <th>Open Price</th>
                <th>Close Price</th>
                <th>Close MS</th>
                <th>Slippage</th>
                <th>Profit</th>
              </tr>
            </thead>
            <tbody id="closedDealsBody">
              <tr><td colspan="12" style="text-align:center;color:var(--text2);padding:20px;">No closed deals</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Pending -->
      <div class="pos-tab-panel" id="ptab-pending">
        <div style="text-align:center;color:var(--text2);padding:20px;font-size:0.82rem;">No pending orders</div>
      </div>

      <!-- Rejected -->
      <div class="pos-tab-panel" id="ptab-rejected">
        <div style="text-align:center;color:var(--text2);padding:20px;font-size:0.82rem;">No rejected orders</div>
      </div>
      </div> <!-- end pos-panels-wrapper -->
    </div>

    <!-- Tab: Log -->
    <div class="tab-panel" id="stab-log">
      <div id="strategyLogContainer" style="max-height:500px;overflow-y:auto;font-family:'Fira Mono',monospace;font-size:0.82rem;color:var(--text2);padding:12px;background:var(--surface);border-radius:8px;border:1px solid var(--border);">
        <p style="text-align:center;color:var(--text2);padding:30px 0;">No log entries yet</p>
      </div>
    </div>

    <!-- Tab: Settings -->
    <div class="tab-panel" id="stab-settings">
      <div class="form-grid" style="grid-template-columns:1fr 1fr 1fr;margin-bottom:16px;padding-bottom:16px;border-bottom:1px solid var(--border);">
        <div class="form-group">
          <label>Strategy Name</label>
          <input type="text" id="esStratName">
        </div>
        <div class="form-group">
          <label>Account 1</label>
          <input type="text" id="esAcct1">
        </div>
        <div class="form-group">
          <label>Account 2</label>
          <input type="text" id="esAcct2">
        </div>
      </div>
      <div class="form-grid" style="grid-template-columns:1fr 1fr 1fr;margin-bottom:16px;padding-bottom:16px;border-bottom:1px solid var(--border);">
        <div class="form-group">
          <label>Trade Start Time</label>
          <input type="time" id="esTradeStartTime" value="18:00">
        </div>
        <div class="form-group">
          <label>Trade Stop Time</label>
          <input type="time" id="esTradeStopTime" value="16:30">
        </div>
      </div>
      <div style="display:flex;align-items:center;gap:24px;margin-bottom:16px;padding-bottom:16px;border-bottom:1px solid var(--border);">
        <label style="display:flex;align-items:center;gap:8px;cursor:pointer;">
          <input type="checkbox" id="esEnabled" style="width:16px;height:16px;cursor:pointer;">
          <span>Enabled for Trading</span>
        </label>
        <label style="display:flex;align-items:center;gap:8px;cursor:pointer;">
          <input type="checkbox" id="esTradeAlerts" style="width:16px;height:16px;cursor:pointer;">
          <span>🔔 Trade Alerts</span>
        </label>
        <div id="esRunningControl" style="display:flex;align-items:center;gap:8px;">
          <span id="esRunningBadge"></span>
          <button id="esStartStopBtn" class="btn btn-sm" onclick="toggleRunningFromModal()"></button>
        </div>
      </div>
      <div style="margin-bottom:16px;">
        <button class="btn btn-primary" onclick="updateStrategy()" style="padding:6px 16px;font-size:0.8rem;">Save Strategy</button>
        <button class="btn btn-danger" onclick="deleteStrategyFromModal()" style="padding:6px 16px;font-size:0.8rem;margin-left:8px;">Delete Strategy</button>
      </div>
    </div>

  </div>
</div>

<!-- Import Positions Modal -->
<div class="modal-overlay" id="importPositionsModal">
  <div class="modal" style="max-width:500px;">
    <h2>⬇ Import Positions</h2>
    <p style="color:var(--text2);font-size:0.82rem;margin-bottom:12px;">
      Import existing hedged positions from both accounts. The accounts will be scanned for open positions
      and the dashboard will create an instrument with matched fills.
    </p>
    <div class="form-grid" style="grid-template-columns:1fr;">
      <div class="form-group">
        <label>Pair <span style="font-size:0.7rem;color:var(--text2)">(leave blank to import all pairs)</span></label>
        <input type="text" id="impPair" placeholder="e.g. EURUSD">
      </div>
      <div class="form-group">
        <label>
          <input type="checkbox" id="impUseComment" checked style="margin-right:6px;">
          Filter by Comment
        </label>
        <input type="text" id="impComment" placeholder="e.g. 12345-67890, 98765-43210">
      </div>
      <div class="form-group">
        <label>
          <input type="checkbox" id="impUseTime" style="margin-right:6px;">
          Filter by Open Time <span style="font-size:0.7rem;color:var(--text2)">(use broker/platform time as shown in MT4/MT5)</span>
        </label>
        <div id="impTimeFields" style="display:none;gap:8px;flex-direction:column;">
          <div style="display:flex;gap:8px;align-items:center;">
            <label style="min-width:40px;font-size:0.78rem;color:var(--text2)">From</label>
            <input type="datetime-local" id="impTimeFrom" style="flex:1;">
          </div>
          <div style="display:flex;gap:8px;align-items:center;">
            <label style="min-width:40px;font-size:0.78rem;color:var(--text2)">To</label>
            <input type="datetime-local" id="impTimeTo" style="flex:1;">
          </div>
        </div>
      </div>
      <div class="form-group">
        <label>
          <input type="checkbox" id="impUseTicket" style="margin-right:6px;">
          Filter by Ticket Range <span style="font-size:0.7rem;color:var(--text2)">(per account)</span>
        </label>
        <div id="impTicketFields" style="display:none;gap:10px;flex-direction:column;">
          <div style="border:1px solid var(--border);border-radius:6px;padding:8px 10px;">
            <div style="font-size:0.75rem;color:var(--accent);margin-bottom:4px;font-weight:600;" id="impTicketLabel1">Account 1</div>
            <div style="display:flex;gap:8px;align-items:center;margin-bottom:4px;">
              <label style="min-width:40px;font-size:0.78rem;color:var(--text2)">From</label>
              <input type="number" id="impTicketFrom1" placeholder="e.g. 22414492" style="flex:1;">
            </div>
            <div style="display:flex;gap:8px;align-items:center;">
              <label style="min-width:40px;font-size:0.78rem;color:var(--text2)">To</label>
              <input type="number" id="impTicketTo1" placeholder="e.g. 22414510" style="flex:1;">
            </div>
          </div>
          <div style="border:1px solid var(--border);border-radius:6px;padding:8px 10px;">
            <div style="font-size:0.75rem;color:var(--accent);margin-bottom:4px;font-weight:600;" id="impTicketLabel2">Account 2</div>
            <div style="display:flex;gap:8px;align-items:center;margin-bottom:4px;">
              <label style="min-width:40px;font-size:0.78rem;color:var(--text2)">From</label>
              <input type="number" id="impTicketFrom2" placeholder="e.g. 33525001" style="flex:1;">
            </div>
            <div style="display:flex;gap:8px;align-items:center;">
              <label style="min-width:40px;font-size:0.78rem;color:var(--text2)">To</label>
              <input type="number" id="impTicketTo2" placeholder="e.g. 33525020" style="flex:1;">
            </div>
          </div>
        </div>
      </div>
      <div class="form-group">
        <label>Matching Mode</label>
        <div style="display:flex;gap:16px;">
          <label style="cursor:pointer;display:flex;align-items:center;gap:4px;"><input type="radio" name="impMatchMode" value="ticket" checked> Ticket-by-Ticket</label>
          <label style="cursor:pointer;display:flex;align-items:center;gap:4px;"><input type="radio" name="impMatchMode" value="lots"> By Total Lots</label>
        </div>
        <span style="font-size:0.7rem;color:var(--text2)">Lot mode: balance by total lots, supports partial closes</span>
      </div>
    </div>
    <div id="importStatusArea" style="display:none;margin:12px 0;padding:12px;border-radius:8px;background:var(--surface);border:1px solid var(--border);font-size:0.82rem;">
      <div id="importStatusText" style="color:var(--text2);">Waiting for EAs to report positions...</div>
      <div style="margin-top:8px;height:4px;background:var(--border);border-radius:2px;overflow:hidden;">
        <div id="importProgressBar" style="height:100%;width:0;background:var(--accent);transition:width 0.3s;"></div>
      </div>
    </div>
    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" id="importStartBtn" onclick="startImport()">Import</button>
      <button class="btn btn-danger" onclick="closeImportModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Add Instrument Modal (session create form with accounts auto-filled) -->
<div class="modal-overlay" id="newInstrumentModal">
  <div class="modal" style="max-width:800px;">
    <h2>Add Instrument</h2>
    <input type="hidden" id="fStrategyId">
    <div class="form-grid">
      <div class="form-group">
        <label>Pair</label>
        <input type="text" id="fPair" value="EURUSD" placeholder="e.g. EURUSD">
      </div>
      <div class="form-group">
        <label>Lot Size</label>
        <input type="number" id="fLotSize" value="0.01" step="0.01" min="0.01">
      </div>
      <div class="form-group">
        <label>Total Positions</label>
        <input type="number" id="fTotalPositions" value="1" min="1">
      </div>
      <div class="form-group">
        <label>Max Spread (points)</label>
        <input type="number" id="fMaxSpread" value="0" min="0">
      </div>
      <div class="form-group">
        <label>Max Errors <span style="font-size:0.7rem;color:var(--text2)">(0=unlimited)</span></label>
        <input type="number" id="fMaxErrors" value="1" min="0" title="Pause after N errors. 0 = unlimited">
      </div>
      <div class="form-group">
        <label>Trade Pause (s)</label>
        <input type="number" id="fTradePause" value="0" min="0" step="0.1" title="Delay between consecutive trades in seconds">
      </div>
      <div class="form-group">
        <label>Diff to Open <span style="font-size:0.7rem;color:var(--text2)">(pts, 0=off)</span></label>
        <input type="number" id="fDiffToOpen" value="0" step="1" title="Min diff in points to open. 0 = disabled">
      </div>
      <div class="form-group">
        <label>Diff to Close <span style="font-size:0.7rem;color:var(--text2)">(pts, 0=off)</span></label>
        <input type="number" id="fDiffToClose" value="0" step="1" title="Min diff in points to close. 0 = disabled">
      </div>
      <div class="form-group">
        <label>Max Accum Lots <span style="font-size:0.7rem;color:var(--text2)">(0=off)</span></label>
        <input type="number" id="fMaxAccumLots" value="0" min="0" step="0.01" title="Max total lots across all accounts. 0 = unlimited">
      </div>
      <div class="form-group">
        <label>Max Accum Deals <span style="font-size:0.7rem;color:var(--text2)">(0=off)</span></label>
        <input type="number" id="fMaxAccumDeals" value="0" min="0" step="1" title="Max total deals across all accounts. 0 = unlimited">
      </div>


      <div class="form-group">
        <label>Execution Order</label>
        <select id="fExecOrder">
          <option value="simultaneous">Both Simultaneously</option>
          <option value="side1_first">Side 1 First</option>
          <option value="side2_first">Side 2 First</option>
        </select>
      </div>
    </div>

    <div class="sides-config">
      <div class="side-box">
        <h3 id="fSide1Title">Side 1</h3>
        <input type="hidden" id="fAcct1">
        <div class="form-group">
          <label>Action</label>
          <select id="fSide1Action">
            <option value="buy">Buy</option>
            <option value="sell">Sell</option>
          </select>
        </div>
        <div class="form-group">
          <label>Symbol <span style="font-size:0.7rem;color:var(--text2)">(blank = global)</span></label>
          <input type="text" id="fSide1Pair" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Lot Size <span style="font-size:0.7rem;color:var(--text2)">(blank = global)</span></label>
          <input type="number" id="fSide1Lots" step="0.01" min="0.01" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Max Spread <span style="font-size:0.7rem;color:var(--text2)">(blank = global)</span></label>
          <input type="number" id="fSide1MaxSpread" min="1" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Comment <span style="font-size:0.7rem;color:var(--text2)">(for this acct's trades)</span></label>
          <input type="text" id="fSide1Comment" placeholder="e.g. hedge-A">
        </div>
      </div>
      <div class="side-box">
        <h3 id="fSide2Title">Side 2</h3>
        <input type="hidden" id="fAcct2">
        <div class="form-group">
          <label>Action</label>
          <select id="fSide2Action">
            <option value="sell">Sell</option>
            <option value="buy">Buy</option>
          </select>
        </div>
        <div class="form-group">
          <label>Symbol <span style="font-size:0.7rem;color:var(--text2)">(blank = global)</span></label>
          <input type="text" id="fSide2Pair" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Lot Size <span style="font-size:0.7rem;color:var(--text2)">(blank = global)</span></label>
          <input type="number" id="fSide2Lots" step="0.01" min="0.01" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Max Spread <span style="font-size:0.7rem;color:var(--text2)">(blank = global)</span></label>
          <input type="number" id="fSide2MaxSpread" min="1" placeholder="same as global">
        </div>
        <div class="form-group">
          <label>Comment <span style="font-size:0.7rem;color:var(--text2)">(for this acct's trades)</span></label>
          <input type="text" id="fSide2Comment" placeholder="e.g. hedge-B">
        </div>
      </div>
    </div>

    <div class="btn-group" style="margin-top:16px;">
      <button class="btn btn-primary" onclick="createInstrument()">Create Instrument</button>
      <button class="btn btn-success" onclick="createInstrument(true)">Create & Start</button>
      <button class="btn btn-danger" onclick="closeNewInstrumentModal()">Cancel</button>
    </div>
  </div>
</div>

<script>
// ─── Theme Color Definitions & Startup ─────────────────────────────────
const THEME_DEFAULTS = {
  '--bg': '#0f1117',
  '--surface': '#1a1d27',
  '--surface2': '#242836',
  '--header-bg': '#1a2640',
  '--border': '#2e3346',
  '--text': '#e4e6f0',
  '--text2': '#8b8fa3',
  '--accent': '#6c5ce7',
  '--accent2': '#a29bfe',
  '--green': '#00e676',
  '--red': '#ff5252',
  '--orange': '#ffa726',
  '--blue': '#42a5f5',
};
const THEME_LABELS = {
  '--bg': 'Background',
  '--surface': 'Surface',
  '--surface2': 'Surface Alt',
  '--header-bg': 'Header Bar',
  '--border': 'Borders',
  '--text': 'Text Primary',
  '--text2': 'Text Secondary',
  '--accent': 'Accent',
  '--accent2': 'Accent Light',
  '--green': 'Green / Positive',
  '--red': 'Red / Negative',
  '--orange': 'Orange / Warning',
  '--blue': 'Blue / Info',
};
// Apply saved theme instantly on load (before first paint)
(function() {
  try {
    const saved = JSON.parse(localStorage.getItem('themeColors') || '{}');
    const root = document.documentElement;
    for (const [k, v] of Object.entries(saved)) {
      if (THEME_DEFAULTS.hasOwnProperty(k)) root.style.setProperty(k, v);
    }
    // Update theme-color meta tag if header-bg was customized
    if (saved['--header-bg']) {
      const meta = document.querySelector('meta[name="theme-color"]');
      if (meta) meta.setAttribute('content', saved['--header-bg']);
    }
  } catch(e) {}
})();

let refreshTimer = null;
let sessions_cache = [];
let strategies_cache = [];
let ea_heartbeats_cache = {};
let manual_accounts_cache = {};
let fix_accounts_cache = {};
let mt_direct_accounts_cache = {};
let swap_delta_cache = {};
let currentStrategyId = null;

function switchTab(tabId) {
  // Only toggle main dashboard tabs (id starts with 'tab-'), not strategy sub-tabs
  document.querySelectorAll('.tab-panel[id^="tab-"]').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-nav:not(#stratTabNav) .tab-btn').forEach(b => b.classList.remove('active'));
  const panel = document.getElementById('tab-' + tabId);
  if (panel) panel.classList.add('active');
  const btn = document.querySelector('.tab-nav:not(#stratTabNav) .tab-btn[data-tab="' + tabId + '"]');
  if (btn) btn.classList.add('active');
  // Tab-specific hooks
  if (tabId === 'settings') loadSettings();
  if (tabId === 'reporting') refreshReporting();
}

// Strategy sub-tab switching (inside Edit Strategy modal)
function switchStratTab(tabId) {
  document.querySelectorAll('#editStrategyModal .tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('#stratTabNav .tab-btn').forEach(b => b.classList.remove('active'));
  const panel = document.getElementById(tabId);
  if (panel) panel.classList.add('active');
  const btn = document.querySelector('#stratTabNav .tab-btn[data-stab="' + tabId + '"]');
  if (btn) btn.classList.add('active');
  localStorage.setItem('activeStratTab', tabId);
}

// ─── Column visibility toggle for instruments table ─────────────────────
let hiddenCols = JSON.parse(localStorage.getItem('instrHiddenCols') || '[]');

const INSTR_COLUMNS = [
  {idx:'0', name:'Status'}, {idx:'1', name:'Pair'}, {idx:'2', name:'Lots'},
  {idx:'3', name:'Side 1'}, {idx:'4', name:'Side 2'},
  {idx:'5', name:'Diff Open'}, {idx:'6', name:'Diff Close'},
  {idx:'7', name:'MaxSpd1'}, {idx:'8', name:'MaxSpd2'},
  {idx:'9', name:'Diff 1'}, {idx:'10', name:'Diff 2'},
  {idx:'11', name:'Spd 1'}, {idx:'12', name:'Spd 2'},
  {idx:'13', name:'Exec Order'}, {idx:'14', name:'Progress'}, {idx:'15', name:'Actions'},
  {idx:'16', name:'Max Ticks/5s'}, {idx:'17', name:'Max Jump'},
  {idx:'18', name:'Skew Open'}, {idx:'19', name:'Skew Close'},
  {idx:'20', name:'Avoid News'},

];

function initColToggleMenu() {
  const menu = document.getElementById('colToggleMenu');
  menu.innerHTML = '';
  INSTR_COLUMNS.forEach(col => {
    const checked = !hiddenCols.includes(col.idx);
    const label = document.createElement('label');
    label.innerHTML = `<input type="checkbox" ${checked ? 'checked' : ''} onchange="toggleInstrCol('${col.idx}', this.checked)"> ${col.name}`;
    menu.appendChild(label);
  });
}

function toggleColMenu() {
  const menu = document.getElementById('colToggleMenu');
  if (!menu.classList.contains('open')) {
    initColToggleMenu();
  }
  menu.classList.toggle('open');
  // Position the fixed menu relative to the button
  if (menu.classList.contains('open')) {
    const btn = document.querySelector('.col-toggle-btn');
    const rect = btn.getBoundingClientRect();
    menu.style.top = (rect.bottom + 2) + 'px';
    menu.style.left = Math.max(0, rect.right - menu.offsetWidth) + 'px';
  }
}
// Close menu when clicking outside
document.addEventListener('click', function(e) {
  const wrap = document.querySelector('.col-toggle-wrap');
  if (wrap && !wrap.contains(e.target)) {
    document.getElementById('colToggleMenu').classList.remove('open');
  }
});

function toggleInstrCol(colIdx, visible) {
  if (visible) {
    hiddenCols = hiddenCols.filter(c => c !== colIdx);
  } else {
    if (!hiddenCols.includes(colIdx)) hiddenCols.push(colIdx);
  }
  localStorage.setItem('instrHiddenCols', JSON.stringify(hiddenCols));
  applyColVisibility();
}

function applyColVisibility() {
  const table = document.getElementById('instrTable');
  if (!table) return;
  table.querySelectorAll('[data-col]').forEach(el => {
    const col = el.getAttribute('data-col');
    if (hiddenCols.includes(col)) {
      el.classList.add('col-hidden');
    } else {
      el.classList.remove('col-hidden');
    }
  });
}

// ─── Group view toggle ──────────────────────────────────────────────
let _groupViewEnabled = localStorage.getItem('acctGroupView') === '1';
(function() { const cb = document.getElementById('groupViewToggle'); if (cb) cb.checked = _groupViewEnabled; })();
function toggleGroupView(enabled) {
  _groupViewEnabled = enabled;
  localStorage.setItem('acctGroupView', enabled ? '1' : '0');
  // Force immediate re-render
  if (window._lastRenderAccountsArgs) {
    renderAccounts.apply(null, window._lastRenderAccountsArgs);
    applyAcctColVisibility();
  }
}

// ─── Account column toggle ───────────────────────────────────────────
let hiddenAcctCols = JSON.parse(localStorage.getItem('acctHiddenCols') || '["18", "19"]');
const ACCT_COLUMNS = [
  {idx:'0', name:'Name'}, {idx:'1', name:'Group'}, {idx:'2', name:'Connection'},
  {idx:'3', name:'Balance'}, {idx:'4', name:'Equity'},
  {idx:'5', name:'Opt Eq'}, {idx:'6', name:'Shift'},
  {idx:'7', name:'PnL'}, {idx:'8', name:'Leverage'}, {idx:'9', name:'Positions'}, {idx:'10', name:'Lots'},
  {idx:'11', name:'Margin Use'}, {idx:'12', name:'Marg.Alrt%'},
  {idx:'13', name:'Swap'}, {idx:'14', name:'Δ Swap'}, {idx:'15', name:'Age'}, {idx:'16', name:'Last Poll'},
  {idx:'17', name:'Auto Conn'}, {idx:'18', name:'Email Alert'}, {idx:'19', name:'Telegram Alert'},
  {idx:'20', name:'Stats'}, {idx:'21', name:'Actions'},
];
function initAcctColToggleMenu() {
  const menu = document.getElementById('acctColToggleMenu');
  menu.innerHTML = '';
  ACCT_COLUMNS.forEach(col => {
    const checked = !hiddenAcctCols.includes(col.idx);
    const label = document.createElement('label');
    label.innerHTML = `<input type="checkbox" ${checked ? 'checked' : ''} onchange="toggleAcctCol('${col.idx}', this.checked)"> ${col.name}`;
    menu.appendChild(label);
  });
}
function toggleAcctColMenu() {
  const menu = document.getElementById('acctColToggleMenu');
  if (!menu.classList.contains('open')) initAcctColToggleMenu();
  menu.classList.toggle('open');
  if (menu.classList.contains('open')) {
    const btn = menu.parentElement.querySelector('.col-toggle-btn');
    const rect = btn.getBoundingClientRect();
    menu.style.top = (rect.bottom + 2) + 'px';
    menu.style.left = Math.max(0, rect.right - menu.offsetWidth) + 'px';
  }
}
document.addEventListener('click', function(e) {
  const menu = document.getElementById('acctColToggleMenu');
  if (menu && menu.classList.contains('open')) {
    const wrap = menu.closest('.col-toggle-wrap');
    if (wrap && !wrap.contains(e.target)) menu.classList.remove('open');
  }
});
function toggleAcctCol(colIdx, visible) {
  if (visible) {
    hiddenAcctCols = hiddenAcctCols.filter(c => c !== colIdx);
  } else {
    if (!hiddenAcctCols.includes(colIdx)) hiddenAcctCols.push(colIdx);
  }
  localStorage.setItem('acctHiddenCols', JSON.stringify(hiddenAcctCols));
  applyAcctColVisibility();
}
function applyAcctColVisibility() {
  // Use injected CSS to hide nth-child columns (works on dynamically rendered rows)
  let style = document.getElementById('acctColStyle');
  if (!style) {
    style = document.createElement('style');
    style.id = 'acctColStyle';
    document.head.appendChild(style);
  }
  if (hiddenAcctCols.length === 0) {
    style.textContent = '';
    return;
  }
  // nth-child is 1-based, data-acol is 0-based
  const rules = hiddenAcctCols.map(c => {
    const n = parseInt(c) + 1;
    return `#accountsTable th:nth-child(${n}), #accountsTable td:nth-child(${n}) { display: none; }`;
  }).join('\n');
  style.textContent = rules;
}

// ─── Column width persistence ───────────────────────────────────────────
let _colWidthObserver = null;

function applyColWidths() {
  const table = document.getElementById('instrTable');
  if (!table) return;
  const ths = table.querySelectorAll('thead th[data-col]');
  if (!ths.length) return;
  console.log('[applyColWidths] running v2, ths=' + ths.length);
  // Invalidate old keys from previous versions
  localStorage.removeItem('instrColWidths');
  localStorage.removeItem('instrColWidths_v2');
  localStorage.removeItem('instrColWidths_v3');
  localStorage.removeItem('instrColWidths_v4');
  localStorage.removeItem('instrColWidths_v5');
  localStorage.removeItem('instrColWidths_v6');
  const STORAGE_KEY = 'instrColWidths_v7';
  const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || 'null');

  // Helper: expand inputs to fill cells after fixing layout
  function expandInputs() {
    table.querySelectorAll('.inl').forEach(el => { el.style.width = '100%'; });
  }

  // Helper: measure optimal column widths from actual content text
  function measureColWidths() {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    const thStyle = getComputedStyle(ths[0]);
    const hFont = thStyle.font;
    const hLetterSp = parseFloat(thStyle.letterSpacing) || 0;
    const firstTd = table.querySelector('tbody td');
    const cFont = firstTd ? getComputedStyle(firstTd).font : hFont;
    const PAD = 8;

    // Max pixel widths per column to keep table compact
    // Most columns auto-size from content; only cap the Actions column (buttons)
    const MAX_COL_W = {
      '15':160                                        // Actions (buttons wrap)
    };

    // Temporarily set auto layout to allow scrollWidth measurement
    table.style.tableLayout = 'auto';
    table.style.width = 'auto';
    ths.forEach(th => { th.style.width = ''; });
    // Collapse inputs so they don't inflate
    table.querySelectorAll('.inl').forEach(el => { el.style.width = '0'; });

    let totalW = 0;
    ths.forEach(th => {
      const col = th.getAttribute('data-col');
      // Measure header text (handle <br> wrapped headers — measure widest line)
      ctx.font = hFont;
      const hLines = th.textContent.trim().toUpperCase().split(/(?=[A-Z][a-z])/);
      // Actually use innerHTML to split on <br>
      const rawLines = th.innerHTML.split(/<br\s*\/?>/i).map(l => l.replace(/<[^>]+>/g,'').trim().toUpperCase());
      let maxW = 0;
      rawLines.forEach(line => {
        const lw = ctx.measureText(line).width + line.length * hLetterSp;
        if (lw > maxW) maxW = lw;
      });

      // Measure each cell: for multi-line or button cells use scrollWidth, for simple cells use measureText
      table.querySelectorAll('tbody td[data-col="' + col + '"]').forEach(td => {
        const btns = td.querySelectorAll('button');
        const hasBr = td.querySelector('br') || td.querySelector('span');
        if (btns.length > 0 || hasBr) {
          // For button/multi-line cells, measure actual rendered width
          const sw = td.scrollWidth;
          if (sw > maxW) maxW = sw;
        } else {
          ctx.font = cFont;
          let text = '';
          const inp = td.querySelector('input');
          const sel = td.querySelector('select');
          if (inp) text = inp.value || '';
          else if (sel) text = (sel.options[sel.selectedIndex] || {}).text || '';
          else text = td.textContent.trim();
          const tw = ctx.measureText(text).width;
          if (tw > maxW) maxW = tw;
        }
      });
      let colW = Math.ceil(maxW) + PAD;
      // Apply per-column max width cap
      if (MAX_COL_W[col] && colW > MAX_COL_W[col]) colW = MAX_COL_W[col];
      th.style.width = colW + 'px';
      if (!th.classList.contains('col-hidden')) totalW += colW;
    });
    table.style.tableLayout = 'fixed';
    table.style.width = totalW + 'px';
    expandInputs();
  }

  // Helper: save current th widths to localStorage
  function saveWidths() {
    const w = {};
    table.querySelectorAll('thead th[data-col]').forEach(th => {
      w[th.getAttribute('data-col')] = th.offsetWidth;
    });
    localStorage.setItem(STORAGE_KEY, JSON.stringify(w));
  }

  // Helper: recalculate table width from visible columns
  function syncTableWidth() {
    let tw = 0;
    table.querySelectorAll('thead th[data-col]').forEach(th => {
      if (!th.classList.contains('col-hidden')) tw += th.offsetWidth;
    });
    table.style.width = tw + 'px';
  }

  if (saved) {
    let totalW = 0;
    ths.forEach(th => {
      const col = th.getAttribute('data-col');
      const w = saved[col] || 60;
      th.style.width = w + 'px';
      if (!th.classList.contains('col-hidden')) totalW += w;
    });
    table.style.tableLayout = 'fixed';
    table.style.width = totalW + 'px';
    expandInputs();
  } else {
    measureColWidths();
  }

  // JS-based drag-to-resize on the right edge of each th
  const GRIP = 6; // px from right edge that activates resize
  ths.forEach(th => {
    if (th._dragResizeInit) return;
    th._dragResizeInit = true;

    // Change cursor when near right edge
    th.addEventListener('mousemove', e => {
      const rect = th.getBoundingClientRect();
      th.style.cursor = (e.clientX >= rect.right - GRIP) ? 'col-resize' : '';
    });
    th.addEventListener('mouseleave', () => { th.style.cursor = ''; });

    // Drag to resize
    th.addEventListener('mousedown', e => {
      const rect = th.getBoundingClientRect();
      if (e.clientX < rect.right - GRIP) return; // not in grip zone
      e.preventDefault();
      const startX = e.clientX;
      const startW = th.offsetWidth;

      function onMove(ev) {
        const newW = Math.max(10, startW + ev.clientX - startX);
        th.style.width = newW + 'px';
        syncTableWidth();
      }
      function onUp() {
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
        saveWidths();
      }
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });

    // Double-click to reset to auto-measured widths
    if (!th._dblReset) {
      th._dblReset = true;
      th.addEventListener('dblclick', () => {
        localStorage.removeItem(STORAGE_KEY);
        measureColWidths();
      });
    }
  });
}

// ─── Positions sub-tabs ─────────────────────────────────────────────────
function switchPosTab(tabId) {
  document.querySelectorAll('#stab-instruments .pos-tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('#posTabNav .pos-tab-btn').forEach(b => b.classList.remove('active'));
  const panel = document.getElementById(tabId);
  if (panel) panel.classList.add('active');
  const btn = document.querySelector(`#posTabNav .pos-tab-btn[data-ptab="${tabId}"]`);
  if (btn) btn.classList.add('active');
  localStorage.setItem('activePosTab', tabId);
}

// ─── Positions pane collapse ────────────────────────────────────────────
let positionsPaneCollapsed = localStorage.getItem('positionsPaneCollapsed') === 'true';
(function initPosCollapse() {
  // Apply saved state on load
  if (positionsPaneCollapsed) {
    const wrapper = document.getElementById('posPanelsWrapper');
    const btn = document.getElementById('posCollapseBtn');
    if (wrapper) wrapper.classList.add('collapsed');
    if (btn) btn.classList.add('collapsed');
  }
})();
function togglePositionsPane() {
  positionsPaneCollapsed = !positionsPaneCollapsed;
  const wrapper = document.getElementById('posPanelsWrapper');
  const btn = document.getElementById('posCollapseBtn');
  if (wrapper) wrapper.classList.toggle('collapsed', positionsPaneCollapsed);
  if (btn) btn.classList.toggle('collapsed', positionsPaneCollapsed);
  localStorage.setItem('positionsPaneCollapsed', positionsPaneCollapsed);
  // If expanding, force a re-render to show current data
  if (!positionsPaneCollapsed) {
    renderOpenedDeals();
    if (typeof renderClosedDeals === 'function') renderClosedDeals();
  }
}

// ─── Render Opened Deals ────────────────────────────────────────────────
function renderOpenedDeals() {
  // Skip rendering when pane is collapsed to save CPU
  if (positionsPaneCollapsed) return;
  const tbody = document.getElementById('openedDealsBody');
  if (!tbody) return;
  // Gather deals from sessions belonging to current strategy
  const stratSessions = sessions_cache.filter(s => s.strategy_id === currentStrategyId);
  // Build deal pairs from event log or session filled data
  // For now: display deals from sessions that have fills
  const dealPairs = [];
  // Only show sessions whose positions are still open (not closed/completed)
  stratSessions.filter(s => ['open','monitor','close','cycle_acc1','cycle_acc2'].includes(s.action) || s.status === 'active').forEach(s => {
    const accs = Object.keys(s.sides || {});
    if (accs.length < 2) return;
    const fills = s.fills || [];
    const closeFills = s.close_fills || [];
    const side1 = s.sides[accs[0]];
    const side2 = s.sides[accs[1]];
    const pair1 = side1.pair || s.pair;
    const pair2 = side2.pair || s.pair;
    // Build set of closed ticket IDs (as strings for comparison)
    const closedTickets = new Set(closeFills.map(cf => String(cf.ticket)));
    // Separate fills by account, filtering out closed tickets
    const fills1 = fills.filter(f => f.account === accs[0] && !closedTickets.has(String(f.ticket)));
    const fills2 = fills.filter(f => f.account === accs[1] && !closedTickets.has(String(f.ticket)));
    const totalPairs = Math.max(fills1.length, fills2.length);
    if (totalPairs <= 0) return;
    // If we have filled counts but fewer fill detail records, pad with empty placeholders
    while (fills1.length < totalPairs) fills1.push(null);
    while (fills2.length < totalPairs) fills2.push(null);
    for (let i = 0; i < totalPairs; i++) {
      const f1 = fills1[i];
      const f2 = fills2[i];
      const p1 = f1 && f1.price != null ? f1.price : null;
      const p2 = f2 && f2.price != null ? f2.price : null;
      const pipMult = s.pair.toUpperCase().includes('JPY') ? 1000 : 100000;
      // OPEN DIFF = SELL price - BUY price (negative = cost, positive = arb)
      let openDiff = '-';
      if (p1 != null && p2 != null) {
        const s1act = (side1.action || 'sell').toLowerCase();
        let sellPrice, buyPrice;
        if (s1act === 'buy') { buyPrice = p1; sellPrice = p2; }
        else { buyPrice = p2; sellPrice = p1; }
        openDiff = ((sellPrice - buyPrice) * pipMult).toFixed(1);
      }
      const t1epoch = f1 ? f1.ts_epoch : null;
      const t2epoch = f2 ? f2.ts_epoch : null;
      // Per-side execution latency: time from command sent → fill confirmed
      let openMs1 = '-', openMs2 = '-';
      if (f1 && f1.cmd_ts && t1epoch) {
        openMs1 = Math.round((t1epoch - f1.cmd_ts) * 1000) + 'ms';
      }
      if (f2 && f2.cmd_ts && t2epoch) {
        openMs2 = Math.round((t2epoch - f2.cmd_ts) * 1000) + 'ms';
      }
      // Fallback for older fills without cmd_ts: show inter-side delta
      // Skip for imported fills — execution timing is unknown
      const isImported = (f1 && f1.imported) || (f2 && f2.imported);
      if (!isImported && openMs1 === '-' && openMs2 === '-' && t1epoch && t2epoch) {
        const deltaMs = Math.round((t2epoch - t1epoch) * 1000);
        if (deltaMs >= 0) {
          openMs1 = '0ms';
          openMs2 = deltaMs + 'ms';
        } else {
          openMs1 = Math.abs(deltaMs) + 'ms';
          openMs2 = '0ms';
        }
      }
      // Calculate slippage in points
      // Skip for imported fills — no meaningful quote_price data
      let slip1 = '-';
      if (f1 && !f1.imported && f1.price != null && f1.quote_price != null && f1.quote_price != 0) {
        const ptMult1 = Math.pow(10, pair1.toUpperCase().includes('JPY') ? 3 : 5);
        const s1act = (side1.action || 'sell').toLowerCase();
        const diff1 = s1act === 'buy' ? (f1.price - f1.quote_price) : (f1.quote_price - f1.price);
        slip1 = Math.round(diff1 * ptMult1);
        if (slip1 > 0) slip1 = '+' + slip1;
      }
      let slip2 = '-';
      if (f2 && !f2.imported && f2.price != null && f2.quote_price != null && f2.quote_price != 0) {
        const ptMult2 = Math.pow(10, pair2.toUpperCase().includes('JPY') ? 3 : 5);
        const s2act = (side2.action || 'buy').toLowerCase();
        const diff2 = s2act === 'buy' ? (f2.price - f2.quote_price) : (f2.quote_price - f2.price);
        slip2 = Math.round(diff2 * ptMult2);
        if (slip2 > 0) slip2 = '+' + slip2;
      }
      // Calculate ACTUAL unrealized P&L using live bid/ask
      // BUY P&L = (current_bid - fill_price) * pipMult  (can sell at bid to close)
      // SELL P&L = (fill_price - current_ask) * pipMult  (pay ask to close)
      // Total profit = BUY P&L + SELL P&L
      let profit = '-';
      if (p1 != null && p2 != null) {
        const s1act = (side1.action || 'sell').toLowerCase();
        // Determine which fill is buy and which is sell
        let buyFillPrice, sellFillPrice, buyBid, sellAsk;
        if (s1act === 'buy') {
          buyFillPrice = p1;
          sellFillPrice = p2;
          buyBid = s.curr_bid_1;   // current bid for buy-side account
          sellAsk = s.curr_ask_2;  // current ask for sell-side account
        } else {
          buyFillPrice = p2;
          sellFillPrice = p1;
          buyBid = s.curr_bid_2;
          sellAsk = s.curr_ask_1;
        }
        if (buyBid != null && sellAsk != null) {
          const buyPnL = (buyBid - buyFillPrice) * pipMult;
          const sellPnL = (sellFillPrice - sellAsk) * pipMult;
          profit = Math.round((buyPnL + sellPnL) * 10) / 10;
        }
      }
      dealPairs.push({
        sessionId: s.id,
        ticket1: f1 ? f1.ticket : null,
        ticket2: f2 ? f2.ticket : null,
        acc1: accs[0],
        acc2: accs[1],
        pairLabel: s.pair,
        session1: accs[0],
        session2: accs[1],
        symbol1: pair1,
        symbol2: pair2,
        side1Action: (side1.action || 'sell').toUpperCase(),
        side2Action: (side2.action || 'buy').toUpperCase(),
        lots: s.lot_size,
        time1: f1 ? f1.ts : (s.updated_at || '-'),
        time2: f2 ? f2.ts : (s.updated_at || '-'),
        ticket1: f1 ? f1.ticket : '-',
        ticket2: f2 ? f2.ticket : '-',
        price1: p1 != null ? p1.toFixed(5) : '-',
        price2: p2 != null ? p2.toFixed(5) : '-',
        openDiff: openDiff,
        openMs1: openMs1,
        openMs2: openMs2,
        openSlip1: slip1,
        openSlip2: slip2,
        profit: profit
      });
    }
  });

  if (!dealPairs.length) {
    tbody.innerHTML = '<tr><td colspan="12" style="text-align:center;color:var(--text2);padding:20px;">No open deals</td></tr>';
    return;
  }

  tbody.innerHTML = dealPairs.map(d => {
    const profitClass = typeof d.profit === 'number' ? (d.profit >= 0 ? 'profit-pos' : 'profit-neg') : '';
    const profitVal = typeof d.profit === 'number' ? d.profit.toFixed(1) : d.profit;
    return `<tr class="deal-row-top">
      <td rowspan="2" style="vertical-align:middle;font-weight:600;">${d.pairLabel}</td>
      <td style="font-size:0.7rem;">${d.session1}</td>
      <td>${d.side1Action}</td>
      <td>${d.lots}</td>
      <td style="font-size:0.7rem;">${d.time1}</td>
      <td>${d.ticket1}</td>
      <td>${d.price1}</td>
      <td rowspan="2" style="vertical-align:middle;font-weight:600;">${d.openDiff}</td>
      <td>${d.openMs1}</td>
      <td>${d.openSlip1}</td>
      <td rowspan="2" class="${profitClass}" style="vertical-align:middle;">${profitVal}</td>
      <td rowspan="2" style="vertical-align:middle;"><button class="btn btn-danger btn-sm" onclick="closeDeal('${d.sessionId}', '${d.acc1}', ${d.ticket1 || 'null'}, '${d.acc2}', ${d.ticket2 || 'null'})" title="Close this deal pair" style="font-size:0.65rem;padding:2px 6px;">✕</button></td>
    </tr>
    <tr class="deal-row-bottom">
      <td style="font-size:0.7rem;">${d.session2}</td>
      <td>${d.side2Action}</td>
      <td>${d.lots}</td>
      <td style="font-size:0.7rem;">${d.time2}</td>
      <td>${d.ticket2}</td>
      <td>${d.price2}</td>
      <td>${d.openMs2}</td>
      <td>${d.openSlip2}</td>
    </tr>`;
  }).join('');
}

// ─── Render Closed Deals ────────────────────────────────────────────────
function renderClosedDeals() {
  // Skip rendering when pane is collapsed to save CPU
  if (positionsPaneCollapsed) return;
  const tbody = document.getElementById('closedDealsBody');
  if (!tbody) return;
  const stratSessions = sessions_cache.filter(s => s.strategy_id === currentStrategyId);
  const dealRows = [];
  // Show deals from sessions that have close_fills
  stratSessions.forEach(s => {
    const closeFills = s.close_fills || [];
    if (closeFills.length === 0) return;
    const accs = Object.keys(s.sides || {});
    if (accs.length < 2) return;
    const side1 = s.sides[accs[0]];
    const side2 = s.sides[accs[1]];
    const pipMult = s.pair.toUpperCase().includes('JPY') ? 1000 : 100000;
    // Group close fills by account, sort by time to align cycle closes properly
    const cf1 = closeFills.filter(f => f.account === accs[0]).sort((a, b) => (a.ts_epoch || 0) - (b.ts_epoch || 0));
    const cf2 = closeFills.filter(f => f.account === accs[1]).sort((a, b) => (a.ts_epoch || 0) - (b.ts_epoch || 0));
    // Also get open fills to pair for P&L calculation
    const openFills = s.fills || [];
    const of1 = openFills.filter(f => f.account === accs[0]);
    const of2 = openFills.filter(f => f.account === accs[1]);
    
    // Time-based pairing for closes (fixes misalignment when one side has cycle closes)
    const pairedCloses = [];
    let idx1 = 0, idx2 = 0;
    while (idx1 < cf1.length || idx2 < cf2.length) {
      if (idx1 < cf1.length && idx2 < cf2.length) {
        const t1 = cf1[idx1].ts_epoch || 0;
        const t2 = cf2[idx2].ts_epoch || 0;
        if (Math.abs(t1 - t2) < 60) {
          pairedCloses.push({c1: cf1[idx1], c2: cf2[idx2]});
          idx1++; idx2++;
        } else if (t1 < t2) {
          pairedCloses.push({c1: cf1[idx1], c2: null});
          idx1++;
        } else {
          pairedCloses.push({c1: null, c2: cf2[idx2]});
          idx2++;
        }
      } else if (idx1 < cf1.length) {
        pairedCloses.push({c1: cf1[idx1], c2: null});
        idx1++;
      } else {
        pairedCloses.push({c1: null, c2: cf2[idx2]});
        idx2++;
      }
    }

    if (pairedCloses.length === 0) return;
    for (let pIdx = 0; pIdx < pairedCloses.length; pIdx++) {
      const c1 = pairedCloses[pIdx].c1;
      const c2 = pairedCloses[pIdx].c2;
      const cp1 = c1 && c1.price != null ? c1.price : null;
      const cp2 = c2 && c2.price != null ? c2.price : null;
      // Find the matching open fill strictly by ticket (do not fallback to index which grabs wrong cycle ticket)
      const o1 = c1 ? (c1.open_price != null ? c1 : of1.find(f => f.ticket == c1.ticket)) : null;
      const o2 = c2 ? (c2.open_price != null ? c2 : of2.find(f => f.ticket == c2.ticket)) : null;
      const op1 = o1 && o1.open_price != null ? o1.open_price : (o1 && o1.price != null ? o1.price : null);
      const op2 = o2 && o2.open_price != null ? o2.open_price : (o2 && o2.price != null ? o2.price : null);
      const openTime1 = c1.open_ts || (o1 && o1 !== c1 ? o1.ts : null);
      const openTime2 = c2.open_ts || (o2 && o2 !== c2 ? o2.ts : null);
      // Calculate realized P&L: for each side, (close - open) for buy, (open - close) for sell
      let profit = '-';
      const s1act = (side1.action || 'sell').toLowerCase();
      if (cp1 != null && cp2 != null && op1 != null && op2 != null) {
        let buyOpenP, buyCloseP, sellOpenP, sellCloseP;
        if (s1act === 'buy') {
          buyOpenP = op1; buyCloseP = cp1;
          sellOpenP = op2; sellCloseP = cp2;
        } else {
          buyOpenP = op2; buyCloseP = cp2;
          sellOpenP = op1; sellCloseP = cp1;
        }
        const buyPnL = (buyCloseP - buyOpenP) * pipMult;
        const sellPnL = (sellOpenP - sellCloseP) * pipMult;
        profit = Math.round((buyPnL + sellPnL) * 10) / 10;
      }

      // Calculate close slippage in points
      let closeSlip1 = '-';
      const pair1 = side1.pair || s.pair;
      if (c1 && !c1.imported && c1.price != null && c1.quote_price != null && c1.quote_price != 0) {
        const ptMult1 = Math.pow(10, pair1.toUpperCase().includes('JPY') ? 3 : 5);
        const s1act = (side1.action || 'buy').toLowerCase(); // action was on open
        // Closing a buy means we sell. Sell slippage: quote - price
        // Closing a sell means we buy. Buy slippage: price - quote
        const diff1 = s1act === 'buy' ? (c1.quote_price - c1.price) : (c1.price - c1.quote_price);
        closeSlip1 = Math.round(diff1 * ptMult1);
        if (closeSlip1 > 0) closeSlip1 = '+' + closeSlip1;
      }

      let closeSlip2 = '-';
      const pair2 = side2.pair || s.pair;
      if (c2 && !c2.imported && c2.price != null && c2.quote_price != null && c2.quote_price != 0) {
        const ptMult2 = Math.pow(10, pair2.toUpperCase().includes('JPY') ? 3 : 5);
        const s2act = (side2.action || 'sell').toLowerCase();
        const diff2 = s2act === 'buy' ? (c2.quote_price - c2.price) : (c2.price - c2.quote_price);
        closeSlip2 = Math.round(diff2 * ptMult2);
        if (closeSlip2 > 0) closeSlip2 = '+' + closeSlip2;
      }

      let statusLabel = s.status === 'completed' ? 'CLOSED' :
                          s.status === 'partial_close' ? '<span style="color:#ff4757;font-weight:700;">PARTIAL</span>' : 'CLOSED';
      if (c1 == null || c2 == null) {
        statusLabel = '<span style="color:#f39c12;font-weight:700;">CYCLE</span>';
      }
      dealRows.push({
        pairLabel: s.pair,
        session1: accs[0], session2: accs[1],
        side1Action: (side1.action || 'buy').toUpperCase(),
        side2Action: (side2.action || 'sell').toUpperCase(),
        lots: s.lot_size,
        openTime1: openTime1 || '-', openTime2: openTime2 || '-',
        closeTime1: c1 ? c1.ts : '-', closeTime2: c2 ? c2.ts : '-',
        ticket1: c1 ? c1.ticket : '-', ticket2: c2 ? c2.ticket : '-',
        openPrice1: op1 != null ? op1.toFixed(5) : '-',
        openPrice2: op2 != null ? op2.toFixed(5) : '-',
        closePrice1: cp1 != null ? cp1.toFixed(5) : '-',
        closePrice2: cp2 != null ? cp2.toFixed(5) : '-',
        closeMs1: (c1 && c1.cmd_ts != null && c1.ts_epoch != null) ? Math.round((c1.ts_epoch - c1.cmd_ts) * 1000) : '-',
        closeMs2: (c2 && c2.cmd_ts != null && c2.ts_epoch != null) ? Math.round((c2.ts_epoch - c2.cmd_ts) * 1000) : '-',
        closeSlip1: closeSlip1,
        closeSlip2: closeSlip2,
        profit: profit,
        statusLabel: statusLabel
      });
    }
  });

  if (!dealRows.length) {
    tbody.innerHTML = '<tr><td colspan="12" style="text-align:center;color:var(--text2);padding:20px;">No closed deals</td></tr>';
    return;
  }

  tbody.innerHTML = dealRows.map(d => {
    const profitClass = typeof d.profit === 'number' ? (d.profit >= 0 ? 'profit-pos' : 'profit-neg') : '';
    const profitVal = typeof d.profit === 'number' ? d.profit.toFixed(1) : d.profit;
    return `<tr class="deal-row-top">
      <td rowspan="2" style="vertical-align:middle;font-weight:600;">${d.pairLabel}</td>
      <td style="font-size:0.7rem;">${d.session1}</td>
      <td>${d.side1Action}</td>
      <td>${d.lots}</td>
      <td style="font-size:0.7rem;">${d.openTime1}</td>
      <td style="font-size:0.7rem;">${d.closeTime1}</td>
      <td>${d.ticket1}</td>
      <td>${d.openPrice1}</td>
      <td>${d.closePrice1}</td>
      <td rowspan="2" style="vertical-align:middle;font-size:0.8rem;">${d.closeMs1 !== '-' ? d.closeMs1 + '<br>' + d.closeMs2 : '-'}</td>
      <td>${d.closeSlip1}</td>
      <td rowspan="2" class="${profitClass}" style="vertical-align:middle;">${profitVal}</td>
    </tr>
    <tr class="deal-row-bottom">
      <td style="font-size:0.7rem;">${d.session2}</td>
      <td>${d.side2Action}</td>
      <td>${d.lots}</td>
      <td style="font-size:0.7rem;">${d.openTime2}</td>
      <td style="font-size:0.7rem;">${d.closeTime2}</td>
      <td>${d.ticket2}</td>
      <td>${d.openPrice2}</td>
      <td>${d.closePrice2}</td>
      <td>${d.closeSlip2}</td>
    </tr>`;
  }).join('');
}

// ─── Strategy functions ─────────────────────────────────────────────────
function showNewStrategyModal() {
  document.getElementById('sStratName').value = '';
  // Populate account dropdowns from cached data
  const allAccounts = [];
  Object.keys(ea_heartbeats_cache).forEach(a => allAccounts.push(a));
  Object.keys(manual_accounts_cache).forEach(a => { if (!allAccounts.includes(a)) allAccounts.push(a); });
  Object.keys(mt_direct_accounts_cache).forEach(a => { if (!allAccounts.includes(a)) allAccounts.push(a); });
  allAccounts.sort();
  ['sAcct1','sAcct2'].forEach(selId => {
    const sel = document.getElementById(selId);
    sel.innerHTML = '<option value="">— select account —</option>' + allAccounts.map(a => {
      const lbl = (manual_accounts_cache[a] || {}).group_label || '';
      const display = lbl ? a + ' — ' + lbl : a;
      return `<option value="${a}">${display}</option>`;
    }).join('');
  });
  document.getElementById('newStrategyModal').classList.add('active');
}
function closeNewStrategyModal() {
  document.getElementById('newStrategyModal').classList.remove('active');
}

async function createStrategy() {
  const name = document.getElementById('sStratName').value.trim();
  const account1 = document.getElementById('sAcct1').value.trim();
  const account2 = document.getElementById('sAcct2').value.trim();
  if (!name) { alert('Strategy name is required'); return; }
  if (!account1 || !account2) { alert('Both accounts are required'); return; }
  try {
    const res = await fetch('/api/strategies', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({name, account1, account2})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    closeNewStrategyModal();
    refreshData();
  } catch(e) { alert('Failed to create strategy: ' + e); }
}

async function deleteStrategy(id) {
  showConfirmModal('Delete this strategy and ALL its instruments?', async () => {
    try {
      const res = await fetch('/api/strategies/' + id, {method:'DELETE'});
      const data = await res.json();
      if (data.error) { alert(data.error); return; }
      refreshData();
    } catch(e) { alert('Failed to delete strategy: ' + e); }
  });
}
function showConfirmModal(msg, onConfirm, confirmLabel) {
  const modal = document.getElementById('confirmModal');
  // Ensure modal is direct child of body (topmost stacking context)
  document.body.appendChild(modal);
  document.getElementById('confirmModalMsg').textContent = msg;
  const yesBtn = document.getElementById('confirmModalYes');
  modal.classList.add('active');
  // Clone+replace to remove old listeners
  const newBtn = yesBtn.cloneNode(true);
  yesBtn.parentNode.replaceChild(newBtn, yesBtn);
  newBtn.id = 'confirmModalYes';
  newBtn.textContent = confirmLabel || 'Delete';
  newBtn.onclick = () => { modal.classList.remove('active'); onConfirm(); };
}

function renderStrategies(strats, sessions) {
  strategies_cache = strats;
  const tbody = document.getElementById('strategiesBody');
  if (!strats || !strats.length) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--text2);padding:30px;">No strategies yet</td></tr>';
    return;
  }
  tbody.innerHTML = strats.map(st => {
    // Compute aggregates from sessions belonging to this strategy
    const instrSessions = (sessions || []).filter(s => s.strategy_id === st.id);
    // Errors: sum all errors across all sessions
    let totalErrors = 0;
    instrSessions.forEach(s => {
      if (s.errors) {
        Object.values(s.errors).forEach(errs => {
          totalErrors += Array.isArray(errs) ? errs.length : 0;
        });
      }
    });
    // Positions: show per-side counts (acc1 / acc2)
    let side1Pos = 0, side2Pos = 0;
    instrSessions.forEach(s => {
      if (s.status === 'completed') return;
      if (s.filled) {
        const accs = Object.keys(s.filled);
        if (accs.length >= 1) {
          const f1 = s.filled[accs[0]] || 0;
          const c1 = (s.closed && s.closed[accs[0]]) || 0;
          side1Pos += Math.max(0, f1 - c1);
        }
        if (accs.length >= 2) {
          const f2 = s.filled[accs[1]] || 0;
          const c2 = (s.closed && s.closed[accs[1]]) || 0;
          side2Pos += Math.max(0, f2 - c2);
        }
      }
    });
    const posDisplay = side1Pos + ' / ' + side2Pos;
    // Enabled checkbox
    const enabledChecked = st.enabled ? 'checked' : '';
    // Running status badge + start/stop button
    const isRunning = st.running;
    const statusBadge = isRunning
      ? `<span class="badge badge-active">Running</span>`
      : `<span class="badge badge-draft">Stopped</span>`;
    const startStopBtn = isRunning
      ? `<button class="btn btn-warning btn-sm" onclick="toggleStrategyRunning('${st.id}', false)" title="Stop">⏸ Stop</button>`
      : `<button class="btn btn-success btn-sm" onclick="toggleStrategyRunning('${st.id}', true)" title="Start">▶ Start</button>`;
    return `<tr style="${st.enabled ? '' : 'opacity:0.5;'}">
      <td><strong>${st.name}</strong></td>
      <td>${st.account1}</td>
      <td>${st.account2}</td>
      <td>Hedge</td>
      <td>${totalErrors > 0 ? '<span style="color:var(--red);font-weight:600">' + totalErrors + '</span>' : '0'}</td>
      <td>${posDisplay}</td>
      <td><label style="cursor:pointer;"><input type="checkbox" ${enabledChecked} onchange="toggleStrategyEnabled('${st.id}', this.checked)" style="width:16px;height:16px;cursor:pointer;"></label></td>
      <td>${statusBadge}</td>
      <td>
        ${startStopBtn}
        <button class="btn btn-primary btn-sm" onclick="popOutStrategy('${st.id}')">Edit</button>
        <button class="btn btn-danger btn-sm" onclick="deleteStrategy('${st.id}')" title="Delete strategy">✕</button>
      </td>
    </tr>`;
  }).join('');
}

// ─── Edit Strategy modal (strategy details + instruments) ───────────────
function editStrategy(stratId) {
  currentStrategyId = stratId;
  const strat = strategies_cache.find(s => s.id === stratId);
  if (!strat) return;
  document.getElementById('esStratId').value = strat.id;
  document.getElementById('esStratName').value = strat.name;
  document.getElementById('esAcct1').value = strat.account1;
  document.getElementById('esAcct2').value = strat.account2;
  document.getElementById('esTradeStartTime').value = strat.trade_start_time || '00:00';
  document.getElementById('esTradeStopTime').value = strat.trade_stop_time || '23:59';
  document.getElementById('esEnabled').checked = strat.enabled !== false;
  document.getElementById('esTradeAlerts').checked = strat.trade_alerts || false;
  updateRunningControls(strat.running || false);
  document.getElementById('editStrategyTitle').textContent = 'Edit Strategy — ' + strat.name;
  renderInstrumentsTable();
  document.getElementById('editStrategyModal').classList.add('active');
}
function closeEditStrategyModal() {
  currentStrategyId = null;
  document.getElementById('editStrategyModal').classList.remove('active');
}
// ─── Strategy Log Tab ───────────────────────────────────────────────────────
let _lastEventLogCache = [];
function renderStrategyLog(eventLog) {
  if (eventLog) _lastEventLogCache = eventLog;
  const container = document.getElementById('strategyLogContainer');
  if (!container || !currentStrategyId) return;
  // Find session IDs belonging to this strategy
  const stratSessionIds = new Set(
    (sessions_cache || []).filter(s => s.strategy_id === currentStrategyId).map(s => s.id)
  );
  // Filter events for these sessions
  const filtered = (_lastEventLogCache || []).filter(e => stratSessionIds.has(e.session_id));
  if (!filtered.length) {
    container.innerHTML = '<p style="text-align:center;color:var(--text2);padding:30px 0;">No log entries yet</p>';
    return;
  }
  // Render newest first
  const rows = filtered.slice().reverse().map(e => {
    const ts = e.ts || '';
    const acct = e.account || '';
    const evt = e.event || '';
    const detail = e.detail || '';
    // Color-code events
    let color = 'var(--text2)';
    if (evt.includes('complete') || evt.includes('opened')) color = 'var(--green)';
    else if (evt.includes('closed') || evt.includes('close')) color = 'var(--yellow)';
    else if (evt.includes('fail') || evt.includes('error')) color = 'var(--red)';
    else if (evt.includes('cycle')) color = 'var(--purple,#a78bfa)';
    else if (evt.includes('rebalance')) color = 'var(--orange,#f97316)';
    else if (evt.includes('fee')) color = 'var(--red)';
    return `<div style="padding:4px 0;border-bottom:1px solid var(--border);display:flex;gap:10px;">
      <span style="color:var(--text2);min-width:140px;flex-shrink:0;">${ts}</span>
      <span style="min-width:100px;flex-shrink:0;">${acct}</span>
      <span style="color:${color};min-width:130px;flex-shrink:0;font-weight:600;">${evt}</span>
      <span style="color:var(--text2);">${detail}</span>
    </div>`;
  }).join('');
  container.innerHTML = rows;
}

// ─── Pop-out Strategy Window ────────────────────────────────────────────────
function popOutStrategy(stratId) {
  // Restore saved window geometry or use defaults
  const saved = JSON.parse(localStorage.getItem('popoutGeo_' + stratId) || 'null');
  const w = (saved && saved.w) || 1920;
  const h = (saved && saved.h) || 860;
  const left = (saved && saved.x != null) ? saved.x : (screen.width - w) / 2;
  const top = (saved && saved.y != null) ? saved.y : (screen.height - h) / 2;
  window.open(
    window.location.pathname + '?strategy_id=' + encodeURIComponent(stratId),
    'strategy_' + stratId,
    `width=${w},height=${h},left=${left},top=${top},menubar=no,toolbar=no,location=no,status=no,resizable=yes,scrollbars=yes`
  );
}
async function updateStrategy() {
  const id = document.getElementById('esStratId').value;
  const name = document.getElementById('esStratName').value.trim();
  const account1 = document.getElementById('esAcct1').value.trim();
  const account2 = document.getElementById('esAcct2').value.trim();
  if (!name) { alert('Strategy name is required'); return; }
  if (!account1 || !account2) { alert('Both accounts are required'); return; }
  try {
    const res = await fetch('/api/strategies/' + id, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        name, account1, account2,
        enabled: document.getElementById('esEnabled').checked,
        trade_alerts: document.getElementById('esTradeAlerts').checked,
        trade_start_time: document.getElementById('esTradeStartTime').value,
        trade_stop_time: document.getElementById('esTradeStopTime').value
      })
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    await refreshData();
    // Update the title in the modal
    document.getElementById('editStrategyTitle').textContent = 'Edit Strategy — ' + name;
  } catch(e) { alert('Failed to update strategy: ' + e); }
}
async function deleteStrategyFromModal() {
  const id = document.getElementById('esStratId').value;
  showConfirmModal('Delete this strategy and ALL its instruments?', async () => {
    try {
      const res = await fetch('/api/strategies/' + id, {method:'DELETE'});
      const data = await res.json();
      if (data.error) { alert(data.error); return; }
      closeEditStrategyModal();
      refreshData();
    } catch(e) { alert('Failed to delete strategy: ' + e); }
  });
}

// ─── Toggle enabled / running from table ─────────────────────────────────
async function toggleStrategyEnabled(id, enabled) {
  try {
    const res = await fetch('/api/strategies/' + id, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({enabled})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    refreshData();
  } catch(e) { alert('Failed to toggle enabled: ' + e); }
}
async function toggleStrategyRunning(id, running) {
  try {
    const res = await fetch('/api/strategies/' + id, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({running})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    refreshData();
  } catch(e) { alert('Failed to toggle running: ' + e); }
}
function updateRunningControls(isRunning) {
  const badge = document.getElementById('esRunningBadge');
  const btn = document.getElementById('esStartStopBtn');
  if (isRunning) {
    badge.innerHTML = '<span class="badge badge-active">Running</span>';
    btn.className = 'btn btn-warning btn-sm';
    btn.textContent = '\u23f8 Stop';
  } else {
    badge.innerHTML = '<span class="badge badge-draft">Stopped</span>';
    btn.className = 'btn btn-success btn-sm';
    btn.textContent = '\u25b6 Start';
  }
}
async function toggleRunningFromModal() {
  const id = document.getElementById('esStratId').value;
  const strat = strategies_cache.find(s => s.id === id);
  const newRunning = !(strat && strat.running);
  try {
    const res = await fetch('/api/strategies/' + id, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({running: newRunning})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    updateRunningControls(newRunning);
    // Update local cache too
    if (strat) strat.running = newRunning;
    await refreshData();
  } catch(e) { alert('Failed to toggle running: ' + e); }
}

// Fast hash for change detection — avoids expensive DOM rebuilds when data is unchanged
let _instrLastHash = '';
function renderInstrumentsTable() {
  const tbody = document.getElementById('instrBody');
  // Skip re-render if user is actively editing an inline field or has a dropdown open
  const activeEl = document.activeElement;
  if (activeEl && (activeEl.tagName === 'INPUT' || activeEl.tagName === 'SELECT') && tbody.contains(activeEl)) {
    return;
  }
  // Change detection: skip DOM rebuild if session data hasn't changed
  const stratSessions = sessions_cache.filter(s => s.strategy_id === currentStrategyId);
  const hashInput = JSON.stringify(stratSessions.map(s => ({
    id: s.id, status: s.status, action: s.action, pair: s.pair, lot_size: s.lot_size,
    filled: s.filled, closed: s.closed, close_count: s.close_count,
    curr_diff_open: s.curr_diff_open, curr_diff_close: s.curr_diff_close,
    curr_spread_1: s.curr_spread_1, curr_spread_2: s.curr_spread_2,
    cycle_progress: s.cycle_progress, execution_order: s.execution_order,
    max_spread_points: s.max_spread_points, sides: s.sides,
    diff_to_open: s.diff_to_open, diff_to_close: s.diff_to_close,
    avoid_news: s.avoid_news, max_ticks_per_5s: s.max_ticks_per_5s
  })));
  if (hashInput === _instrLastHash) return; // No changes — skip rebuild
  _instrLastHash = hashInput;
  // Preserve horizontal scroll position across re-renders
  const scrollContainer = document.getElementById('instrTable')?.parentElement;
  const savedScrollLeft = scrollContainer ? scrollContainer.scrollLeft : 0;
  // stratSessions already computed above in hash check
  if (!stratSessions.length) {
    tbody.innerHTML = '<tr><td colspan="21" style="text-align:center;color:var(--text2);padding:30px;">No instruments yet — click "+ Add Instrument"</td></tr>';
    return;
  }
  const order = { partial_close:0, active:1, paused:2, draft:3, completed:4 };
  stratSessions.sort((a,b) => (order[a.status]||9) - (order[b.status]||9));
  tbody.innerHTML = stratSessions.map(s => {
    const isDraft = (s.status === 'draft' || s.status === 'paused');
    const ro = isDraft ? '' : 'disabled';
    const eo = s.execution_order || 'simultaneous';
    const accs = Object.keys(s.sides || {});
    // Find correct account for each side using side_number (Object.keys sorts numeric keys!)
    const side1Acc = accs.find(a => s.sides[a].side_number === 1) || accs[0];
    const side2Acc = accs.find(a => s.sides[a].side_number === 2) || accs[1];
    const ms1 = side1Acc && s.sides[side1Acc].max_spread != null ? s.sides[side1Acc].max_spread : s.max_spread_points;
    const ms2 = side2Acc && s.sides[side2Acc].max_spread != null ? s.sides[side2Acc].max_spread : s.max_spread_points;
    return `<tr>
    <td data-col="0"><span class="badge ${badgeClass(s.status)}">${s.status}</span></td>
    <td data-col="1"><input class="inl" value="${s.pair}" ${ro} onchange="inlineEditSession('${s.id}','pair',this.value.trim());this.value=this.value.trim()"></td>
    <td data-col="2"><input class="inl" type="number" step="0.01" min="0.01" value="${s.lot_size}" ${ro} onchange="inlineEditSession('${s.id}','lot_size',parseFloat(this.value))"></td>
    <td data-col="3">${renderSide(s, 1)}</td>
    <td data-col="4">${renderSide(s, 2)}</td>
    <td data-col="5"><input class="inl" type="number" step="1" value="${s.diff_to_open != null ? s.diff_to_open : ''}" onchange="inlineEditSession('${s.id}','diff_to_open',this.value===''?null:parseInt(this.value))" style="${(s.diff_to_open != null && s.curr_diff_open != null && s.curr_diff_open < s.diff_to_open && s.action === 'open') ? 'border-color:var(--red);color:var(--red)' : ''}"></td>
    <td data-col="6"><input class="inl" type="number" step="1" value="${s.diff_to_close != null ? s.diff_to_close : ''}" onchange="inlineEditSession('${s.id}','diff_to_close',this.value===''?null:parseInt(this.value))" style="${(s.diff_to_close != null && s.curr_diff_close != null && s.curr_diff_close < s.diff_to_close && s.action === 'close') ? 'border-color:var(--red);color:var(--red)' : ''}"></td>
    <td data-col="7"><input class="inl" type="number" min="0" value="${ms1}" onchange="inlineEditSession('${s.id}','side1_max_spread',parseInt(this.value))" style="${(ms1 > 0 && s.curr_spread_1 != null && s.curr_spread_1 > ms1) || ((s.action||'').startsWith('cycle_') && ms1 > 0 && s.curr_spread_1 != null && s.curr_spread_1 > ms1) ? 'border-color:var(--red);color:var(--red)' : ''}"></td>
    <td data-col="8"><input class="inl" type="number" min="0" value="${ms2}" onchange="inlineEditSession('${s.id}','side2_max_spread',parseInt(this.value))" style="${(ms2 > 0 && s.curr_spread_2 != null && s.curr_spread_2 > ms2) || ((s.action||'').startsWith('cycle_') && ms2 > 0 && s.curr_spread_2 != null && s.curr_spread_2 > ms2) ? 'border-color:var(--red);color:var(--red)' : ''}"></td>
    <td data-col="9">${renderCurrDiff(s, 'open')}</td>
    <td data-col="10">${renderCurrDiff(s, 'close')}</td>
    <td data-col="11" style="font-size:0.78rem">${s.curr_spread_1 != null ? s.curr_spread_1 : '-'}</td>
    <td data-col="12" style="font-size:0.78rem">${s.curr_spread_2 != null ? s.curr_spread_2 : '-'}</td>
    <td data-col="13"><select class="inl" style="font-size:0.7rem" onchange="inlineEditSession('${s.id}','execution_order',this.value)">
      <option value="simultaneous" ${eo==='simultaneous'?'selected':''}>Both</option>
      <option value="side1_first" ${eo==='side1_first'?'selected':''}>S1 1st</option>
      <option value="side2_first" ${eo==='side2_first'?'selected':''}>S2 1st</option>
    </select></td>
    <td data-col="14">${renderProgress(s)}</td>
    <td data-col="15" style="white-space:normal">${renderActions(s)}</td>
    <td data-col="16"><input class="inl" type="number" min="0" value="${s.max_ticks_per_5s||0}" style="width:50px" onchange="inlineEditSession('${s.id}','max_ticks_per_5s',parseInt(this.value)||0)" title="Block if either side has more ticks/5s than this (0=off)"></td>
    <td data-col="17"><input class="inl" type="number" min="0" step="0.1" value="${s.max_price_jump||0}" style="width:55px" onchange="inlineEditSession('${s.id}','max_price_jump',parseFloat(this.value)||0)" title="Block if bid jumped > X pips since last tick (0=off)"></td>
    <td data-col="18"><select class="inl" style="font-size:0.65rem" onchange="inlineEditSkew('${s.id}','require_diff_skew_open',this.value)" title="DIFF skew requirement for opening">
      <option value="" ${!s.require_diff_skew_open?'selected':''}>—</option>
      <option value="d1>d2" ${s.require_diff_skew_open==='d1>d2'?'selected':''}>D1>D2</option>
      <option value="d2>d1" ${s.require_diff_skew_open==='d2>d1'?'selected':''}>D2>D1</option>
    </select></td>
    <td data-col="19"><select class="inl" style="font-size:0.65rem" onchange="inlineEditSkew('${s.id}','require_diff_skew_close',this.value)" title="DIFF skew requirement for closing">
      <option value="" ${!s.require_diff_skew_close?'selected':''}>—</option>
      <option value="d1>d2" ${s.require_diff_skew_close==='d1>d2'?'selected':''}>D1>D2</option>
      <option value="d2>d1" ${s.require_diff_skew_close==='d2>d1'?'selected':''}>D2>D1</option>
    </select></td>
    <td data-col="20" class="news-cell-${s.id}">${(function(){
      const checked = s.avoid_news ? 'checked' : '';
      const nb = window._newsBlackout || {};
      if (s.avoid_news && nb.blocked) {
        return '<div style="background:var(--red);border-radius:4px;padding:2px 4px;text-align:center" title="' + (nb.event||'News blackout active') + '"><input type="checkbox" checked onchange="inlineEditSession(\'' + s.id + '\',\'avoid_news\',this.checked)"> <span style="font-size:0.65rem;color:#fff">⚠</span></div>';
      }
      return '<input type="checkbox" ' + checked + ' onchange="inlineEditSession(\'' + s.id + '\',\'avoid_news\',this.checked)" title="Avoid trading ±1min around high-impact news">';
    })()}</td>

  </tr>`;
  }).join('');
  applyColVisibility();
  applyColWidths();
  // Restore horizontal scroll position
  if (scrollContainer && savedScrollLeft) scrollContainer.scrollLeft = savedScrollLeft;
  renderOpenedDeals();
  renderClosedDeals();
}

// ─── Import Positions modal ─────────────────────────────────────────────
let importRequestId = null;
let importPollTimer = null;

function showImportModal() {
  if (!currentStrategyId) return;
  const strat = strategies_cache.find(s => s.id === currentStrategyId);
  if (!strat) return;
  document.getElementById('impPair').value = strat.pair || '';
  document.getElementById('impUseComment').checked = true;
  // Default comment: use login numbers (shorter, avoids MT4/MT5 truncation)
  function _shortName(acc) {
    const mtInfo = mt_direct_accounts_cache[acc];
    if (mtInfo) return mtInfo.login || acc;
    const m = acc.match(/(\d+)$/);
    return m ? m[1] : acc;
  }
  document.getElementById('impComment').value = _shortName(strat.account1) + '-' + _shortName(strat.account2);
  // Reset time filter
  document.getElementById('impUseTime').checked = false;
  document.getElementById('impTimeFields').style.display = 'none';
  document.getElementById('impTimeFrom').value = '';
  document.getElementById('impTimeTo').value = '';
  document.getElementById('impUseTime').onchange = function() {
    document.getElementById('impTimeFields').style.display = this.checked ? 'flex' : 'none';
  };
  // Reset ticket filter (per-account)
  document.getElementById('impUseTicket').checked = false;
  document.getElementById('impTicketFields').style.display = 'none';
  document.getElementById('impTicketFrom1').value = '';
  document.getElementById('impTicketTo1').value = '';
  document.getElementById('impTicketFrom2').value = '';
  document.getElementById('impTicketTo2').value = '';
  // Label ticket sections with account names
  document.getElementById('impTicketLabel1').textContent = 'Acct 1: ' + _shortName(strat.account1);
  document.getElementById('impTicketLabel2').textContent = 'Acct 2: ' + _shortName(strat.account2);
  document.getElementById('impUseTicket').onchange = function() {
    document.getElementById('impTicketFields').style.display = this.checked ? 'flex' : 'none';
  };
  document.getElementById('importStatusArea').style.display = 'none';
  document.getElementById('importStartBtn').disabled = false;
  document.getElementById('importStartBtn').textContent = 'Import';
  document.getElementById('importPositionsModal').classList.add('active');
}

function closeImportModal() {
  document.getElementById('importPositionsModal').classList.remove('active');
  if (importPollTimer) { clearInterval(importPollTimer); importPollTimer = null; }
  importRequestId = null;
}

async function startImport() {
  if (!currentStrategyId) return;
  const pair = document.getElementById('impPair').value.trim();
  const useComment = document.getElementById('impUseComment').checked;
  const comment = useComment ? document.getElementById('impComment').value.trim() : '';
  const matchMode = document.querySelector('input[name="impMatchMode"]:checked')?.value || 'ticket';
  const useTime = document.getElementById('impUseTime').checked;
  const timeFrom = useTime ? document.getElementById('impTimeFrom').value : '';
  const timeTo = useTime ? document.getElementById('impTimeTo').value : '';
  const useTicket = document.getElementById('impUseTicket').checked;
  const ticketFrom1 = useTicket ? document.getElementById('impTicketFrom1').value.trim() : '';
  const ticketTo1 = useTicket ? document.getElementById('impTicketTo1').value.trim() : '';
  const ticketFrom2 = useTicket ? document.getElementById('impTicketFrom2').value.trim() : '';
  const ticketTo2 = useTicket ? document.getElementById('impTicketTo2').value.trim() : '';

  const btn = document.getElementById('importStartBtn');
  btn.disabled = true;
  btn.textContent = 'Requesting...';

  const statusArea = document.getElementById('importStatusArea');
  const statusText = document.getElementById('importStatusText');
  const progressBar = document.getElementById('importProgressBar');
  statusArea.style.display = 'block';
  statusText.textContent = 'Requesting position reports...';
  statusText.style.color = 'var(--text2)';
  progressBar.style.width = '10%';

  try {
    const res = await fetch('/api/strategies/' + currentStrategyId + '/import_positions', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair, comment_filter: comment, match_mode: matchMode, time_from: timeFrom, time_to: timeTo, ticket_from_1: ticketFrom1, ticket_to_1: ticketTo1, ticket_from_2: ticketFrom2, ticket_to_2: ticketTo2 })
    });
    const data = await res.json();
    if (data.error) {
      statusText.textContent = '❌ ' + data.error;
      statusText.style.color = 'var(--red)';
      btn.disabled = false;
      btn.textContent = 'Import';
      return;
    }
    importRequestId = data.request_id;

    // If backend processed immediately (all MT Direct accounts)
    if (data.immediate && data.result) {
      progressBar.style.width = '100%';
      const r = data.result;
      if (r.error) {
        statusText.textContent = '❌ ' + r.error;
        statusText.style.color = 'var(--red)';
      } else {
        statusText.textContent = `✅ Imported ${r.session_id ? 'session' : ''} — ${r.matched_pairs || 0} matched positions`;
        statusText.style.color = 'var(--green)';
        await refreshData();
        closeImportModal();
        renderInstrumentsTable();
        // Show detailed result popup
        const balanceIcon = r.balanced ? '✅' : '⚠️';
        const balanceText = r.balanced ? 'BALANCED' : 'UNBALANCED';
        const msg = `${balanceIcon} Import ${balanceText}\n\n` +
          `Pair: ${r.pair}\n` +
          `Comment: ${r.comment}\n\n` +
          `Account ${r.acct1}: ${r.acct1_positions} positions (${r.acct1_side})\n` +
          `Account ${r.acct2}: ${r.acct2_positions} positions (${r.acct2_side})\n\n` +
          `Matched pairs: ${r.matched_pairs}\n` +
          `Total positions: ${r.total_positions}\n` +
          (r.balanced ? '' : `\n⚠️ Hedge is unbalanced! One side has more positions than the other.`) +
          `\n\nImport completed successfully. You can delete or edit it from the instruments table.`;
        alert(msg);
      }
      btn.disabled = false;
      btn.textContent = 'Import';
      return;
    }

    statusText.textContent = data.message || 'Waiting for accounts to report positions... (poll every 1s)';
    progressBar.style.width = '25%';

    // Start polling for results
    let pollCount = 0;
    importPollTimer = setInterval(async () => {
      pollCount++;
      if (pollCount > 60) {
        clearInterval(importPollTimer);
        importPollTimer = null;
        statusText.textContent = '⏱ Timeout — Accounts did not respond within 60 seconds. Make sure both accounts are running and connected.';
        statusText.style.color = 'var(--orange)';
        btn.disabled = false;
        btn.textContent = 'Retry';
        return;
      }
      try {
        const sRes = await fetch('/api/import_status/' + importRequestId);
        const sData = await sRes.json();
        if (sData.status === 'pending') {
          const recv = sData.received_from || [];
          const wait = sData.waiting_for || [];
          progressBar.style.width = recv.length >= 1 ? '50%' : '25%';
          statusText.textContent = `Received from: ${recv.length > 0 ? recv.join(', ') : 'none'} | Waiting for: ${wait.join(', ')} (${sData.elapsed}s)`;
        } else if (sData.status === 'completed' && sData.result) {
          clearInterval(importPollTimer);
          importPollTimer = null;
          progressBar.style.width = '100%';
          const r = sData.result;
          if (r.error) {
            statusText.textContent = '❌ ' + r.error;
            statusText.style.color = 'var(--red)';
            btn.disabled = false;
            btn.textContent = 'Retry';
          } else {
            statusText.textContent = '✅ Import completed!';
            statusText.style.color = 'var(--green)';
            await refreshData();
            closeImportModal();
            renderInstrumentsTable();
            // Show detailed result popup
            const balanceIcon = r.balanced ? '✅' : '⚠️';
            const balanceText = r.balanced ? 'BALANCED' : 'UNBALANCED';
            const msg = `${balanceIcon} Import ${balanceText}\n\n` +
              `Pair: ${r.pair}\n` +
              `Comment: ${r.comment}\n\n` +
              `Account ${r.acct1}: ${r.acct1_positions} positions (${r.acct1_side})\n` +
              `Account ${r.acct2}: ${r.acct2_positions} positions (${r.acct2_side})\n\n` +
              `Matched pairs: ${r.matched_pairs}\n` +
              `Total positions: ${r.total_positions}\n` +
              (r.balanced ? '' : `\n⚠️ Hedge is unbalanced! One side has more positions than the other.`) +
              `\n\nImport completed successfully. You can delete or edit it from the instruments table.`;
            alert(msg);
          }
        } else {
          // completed_or_expired without detailed result
          clearInterval(importPollTimer);
          importPollTimer = null;
          progressBar.style.width = '100%';
          statusText.textContent = 'Import completed! Refreshing...';
          statusText.style.color = 'var(--green)';
          await refreshData();
          closeImportModal();
          renderInstrumentsTable();
          alert('✅ Import completed. Check the instruments table for the imported session.');
        }
      } catch(e) {
        console.error('Import poll error:', e);
      }
    }, 1000);
  } catch(e) {
    statusText.textContent = '❌ Failed to start import: ' + e;
    statusText.style.color = 'var(--red)';
    btn.disabled = false;
    btn.textContent = 'Import';
  }
}

// ─── Add Instrument modal ───────────────────────────────────────────────
function showNewInstrumentModal() {
  if (!currentStrategyId) return;
  const strat = strategies_cache.find(s => s.id === currentStrategyId);
  if (!strat) return;
  document.getElementById('fStrategyId').value = currentStrategyId;
  document.getElementById('fAcct1').value = strat.account1;
  document.getElementById('fAcct2').value = strat.account2;
  document.getElementById('fSide1Title').textContent = 'Side 1 — ' + strat.account1;
  document.getElementById('fSide2Title').textContent = 'Side 2 — ' + strat.account2;
  // Reset form fields to defaults
  document.getElementById('fPair').value = 'EURUSD';
  document.getElementById('fLotSize').value = '0.01';
  document.getElementById('fTotalPositions').value = '1';
  document.getElementById('fMaxSpread').value = '0';
  document.getElementById('fMaxErrors').value = '1';
  document.getElementById('fTradePause').value = '0';
  document.getElementById('fDiffToOpen').value = '0';
  document.getElementById('fDiffToClose').value = '0';
  document.getElementById('fMaxAccumLots').value = '0';
  document.getElementById('fMaxAccumDeals').value = '0';
  document.getElementById('fExecOrder').value = 'simultaneous';
  document.getElementById('fSide1Action').value = 'buy';
  document.getElementById('fSide2Action').value = 'sell';
  document.getElementById('fSide1Pair').value = '';
  document.getElementById('fSide2Pair').value = '';
  document.getElementById('fSide1Lots').value = '';
  document.getElementById('fSide2Lots').value = '';
  document.getElementById('fSide1MaxSpread').value = '';
  document.getElementById('fSide2MaxSpread').value = '';
  // Default comments: use login numbers (shorter, avoids MT4/MT5 truncation)
  function _sn(acc) {
    const mtInfo = mt_direct_accounts_cache[acc];
    if (mtInfo) return mtInfo.login || acc;
    const m = acc.match(/(\d+)$/);
    return m ? m[1] : acc;
  }
  const defaultComment = _sn(strat.account1) + '-' + _sn(strat.account2);
  document.getElementById('fSide1Comment').value = defaultComment;
  document.getElementById('fSide2Comment').value = defaultComment;
  document.getElementById('newInstrumentModal').classList.add('active');
}
function closeNewInstrumentModal() {
  document.getElementById('newInstrumentModal').classList.remove('active');
}

function buildSessionPayload() {
  const acct1 = document.getElementById('fAcct1').value.trim();
  const acct2 = document.getElementById('fAcct2').value.trim();
  if (!acct1 || !acct2) { alert('Please enter both account numbers'); return null; }
  const payload = {
    action: 'open',
    strategy_id: document.getElementById('fStrategyId').value || null,
    pair: document.getElementById('fPair').value.trim(),
    lot_size: parseFloat(document.getElementById('fLotSize').value),
    total_positions: parseInt(document.getElementById('fTotalPositions').value),
    max_spread_points: parseInt(document.getElementById('fMaxSpread').value),
    max_errors: parseInt(document.getElementById('fMaxErrors').value) || 0,
    trade_pause: parseFloat(document.getElementById('fTradePause').value) || 0,
    diff_to_open: parseInt(document.getElementById('fDiffToOpen').value) || 0,
    diff_to_close: parseInt(document.getElementById('fDiffToClose').value) || 0,
    max_accum_lots: parseFloat(document.getElementById('fMaxAccumLots').value) || 0,
    max_accum_deals: parseInt(document.getElementById('fMaxAccumDeals').value) || 0,
    execution_order: document.getElementById('fExecOrder').value,
    sides: {}
  };
  payload.sides[acct1] = {
    action: document.getElementById('fSide1Action').value,
    comment: document.getElementById('fSide1Comment').value.trim(),
    side_number: 1,
    pair: document.getElementById('fSide1Pair').value.trim() || '',
    lot_size: document.getElementById('fSide1Lots').value ? parseFloat(document.getElementById('fSide1Lots').value) : '',
    max_spread: document.getElementById('fSide1MaxSpread').value ? parseInt(document.getElementById('fSide1MaxSpread').value) : ''
  };
  payload.sides[acct2] = {
    action: document.getElementById('fSide2Action').value,
    comment: document.getElementById('fSide2Comment').value.trim(),
    side_number: 2,
    pair: document.getElementById('fSide2Pair').value.trim() || '',
    lot_size: document.getElementById('fSide2Lots').value ? parseFloat(document.getElementById('fSide2Lots').value) : '',
    max_spread: document.getElementById('fSide2MaxSpread').value ? parseInt(document.getElementById('fSide2MaxSpread').value) : ''
  };
  return payload;
}

async function createInstrument(autoStart) {
  console.log('[DEBUG] createInstrument called, autoStart=', autoStart);
  const payload = buildSessionPayload();
  console.log('[DEBUG] payload=', payload);
  if (!payload) { console.log('[DEBUG] payload is null, returning'); return; }
  try {
    console.log('[DEBUG] POSTing to /api/sessions');
    const res = await fetch('/api/sessions', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
    console.log('[DEBUG] response status=', res.status);
    const data = await res.json();
    console.log('[DEBUG] response data=', data);
    if (!res.ok) { alert('Error: ' + (data.error || 'Unknown')); return; }
    if (autoStart) {
      await fetch('/api/sessions/' + data.id + '/start', { method: 'POST' });
    }
    closeNewInstrumentModal();
    await refreshData();
    // Re-render the instruments table
    renderInstrumentsTable();
  } catch(e) { console.error('[DEBUG] createInstrument error:', e); alert('Request failed: ' + e); }
}

// Keep createSession for backward compat (now essentially the same as createInstrument)
async function createSession(autoStart) {
  return createInstrument(autoStart);
}
function createAndStart() { createSession(true); }

async function startSession(id) {
  await fetch('/api/sessions/' + id + '/start', { method: 'POST' });
  await refreshData();
  renderInstrumentsTable();
}
async function stopSession(id) {
  await fetch('/api/sessions/' + id + '/stop', { method: 'POST' });
  await refreshData();
  renderInstrumentsTable();
}
async function deleteSession(id) {
  showConfirmModal('Delete this instrument?', async () => {
    await fetch('/api/sessions/' + id, { method: 'DELETE' });
    await refreshData();
    renderInstrumentsTable();
  });
}

async function inlineEditSession(id, field, value) {
  const payload = {};
  payload[field] = value;
  try {
    const res = await fetch('/api/sessions/' + id, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      const d = await res.json();
      alert('Error: ' + (d.error || 'Unknown'));
      return;
    }
    // Flash green border on the input that was just saved
    if (event && event.target) {
      event.target.classList.add('inl-saved');
      setTimeout(() => event.target.classList.remove('inl-saved'), 800);
    }
    await refreshData();
  } catch(e) { alert('Save failed: ' + e); }
}

// Auto-mirror helper: setting one skew dropdown auto-sets the other to the opposite
async function inlineEditSkew(id, field, value) {
  const mirror = {'d1>d2': 'd2>d1', 'd2>d1': 'd1>d2', '': ''};
  const otherField = field === 'require_diff_skew_open' ? 'require_diff_skew_close' : 'require_diff_skew_open';
  const payload = {};
  payload[field] = value;
  payload[otherField] = mirror[value] || '';
  try {
    const res = await fetch('/api/sessions/' + id, {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    if (!res.ok) { const d = await res.json(); alert('Error: ' + (d.error || 'Unknown')); return; }
    await refreshData();
    renderInstrumentsTable();
  } catch(e) { alert('Save failed: ' + e); }
}

function editSession(id) {
  const s = sessions_cache.find(x => x.id === id);
  if (!s) return;
  document.getElementById('editSessionId').value = id;
  document.getElementById('ePair').value = s.pair;
  document.getElementById('eLotSize').value = s.lot_size;
  document.getElementById('eTotalPositions').value = s.total_positions;
  document.getElementById('eMaxSpread').value = s.max_spread_points;
  document.getElementById('eMaxErrors').value = s.max_errors != null ? s.max_errors : 1;
  document.getElementById('eTradePause').value = s.trade_pause != null ? s.trade_pause : 0;

  document.getElementById('eExecOrder').value = s.execution_order || 'simultaneous';
  document.getElementById('eComment').value = s.comment || '';
  document.getElementById('eDiffToOpen').value = s.diff_to_open != null ? s.diff_to_open : 0;
  document.getElementById('eDiffToClose').value = s.diff_to_close != null ? s.diff_to_close : 0;
  document.getElementById('eMaxAccumLots').value = s.max_accum_lots != null ? s.max_accum_lots : 0;
  document.getElementById('eMaxAccumDeals').value = s.max_accum_deals != null ? s.max_accum_deals : 0;

  // Populate per-side pair/lots in edit modal
  const sides = s.sides || {};
  let fallbackIdx = 0;
  for (const [acc, info] of Object.entries(sides)) {
    fallbackIdx++;
    const sideNum = info.side_number || fallbackIdx;
    if (sideNum === 1) {
      document.getElementById('editSide1Box').style.display = '';
      document.getElementById('editSide1Title').textContent = 'Side 1 — ' + acc;
      document.getElementById('eSide1Action').value = info.action || 'buy';
      document.getElementById('eSide1Pair').value = (info.pair && info.pair !== s.pair) ? info.pair : '';
      document.getElementById('eSide1Lots').value = (info.lot_size != null && info.lot_size !== s.lot_size) ? info.lot_size : '';
      document.getElementById('eSide1MaxSpread').value = (info.max_spread != null && info.max_spread !== s.max_spread_points) ? info.max_spread : '';
      document.getElementById('eSide1Comment').value = info.comment || '';
    } else if (sideNum === 2) {
      document.getElementById('editSide2Box').style.display = '';
      document.getElementById('editSide2Title').textContent = 'Side 2 — ' + acc;
      document.getElementById('eSide2Action').value = info.action || 'sell';
      document.getElementById('eSide2Pair').value = (info.pair && info.pair !== s.pair) ? info.pair : '';
      document.getElementById('eSide2Lots').value = (info.lot_size != null && info.lot_size !== s.lot_size) ? info.lot_size : '';
      document.getElementById('eSide2MaxSpread').value = (info.max_spread != null && info.max_spread !== s.max_spread_points) ? info.max_spread : '';
      document.getElementById('eSide2Comment').value = info.comment || '';
    }
  }

  document.getElementById('editModal').classList.add('active');
}

async function saveEdit() {
  const id = document.getElementById('editSessionId').value;
  const payload = {
    pair: document.getElementById('ePair').value,
    lot_size: parseFloat(document.getElementById('eLotSize').value),
    total_positions: parseInt(document.getElementById('eTotalPositions').value),
    max_spread_points: parseInt(document.getElementById('eMaxSpread').value),
    max_errors: parseInt(document.getElementById('eMaxErrors').value) || 0,
    trade_pause: parseFloat(document.getElementById('eTradePause').value) || 0,
    diff_to_open: document.getElementById('eDiffToOpen').value === '' ? null : parseInt(document.getElementById('eDiffToOpen').value),
    diff_to_close: document.getElementById('eDiffToClose').value === '' ? null : parseInt(document.getElementById('eDiffToClose').value),
    max_accum_lots: parseFloat(document.getElementById('eMaxAccumLots').value) || 0,
    max_accum_deals: parseInt(document.getElementById('eMaxAccumDeals').value) || 0,

    execution_order: document.getElementById('eExecOrder').value,
    comment: document.getElementById('eComment').value.trim(),
    side1_action: document.getElementById('eSide1Action').value,
    side1_pair: document.getElementById('eSide1Pair').value.trim(),
    side1_lot_size: document.getElementById('eSide1Lots').value ? parseFloat(document.getElementById('eSide1Lots').value) : '',
    side1_max_spread: document.getElementById('eSide1MaxSpread').value ? parseInt(document.getElementById('eSide1MaxSpread').value) : '',
    side1_comment: document.getElementById('eSide1Comment').value.trim(),
    side2_action: document.getElementById('eSide2Action').value,
    side2_pair: document.getElementById('eSide2Pair').value.trim(),
    side2_lot_size: document.getElementById('eSide2Lots').value ? parseFloat(document.getElementById('eSide2Lots').value) : '',
    side2_max_spread: document.getElementById('eSide2MaxSpread').value ? parseInt(document.getElementById('eSide2MaxSpread').value) : '',
    side2_comment: document.getElementById('eSide2Comment').value.trim()
  };
  const res = await fetch('/api/sessions/' + id, { method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  if (!res.ok) { const d = await res.json(); alert('Error: ' + (d.error || 'Unknown')); return; }
  closeModal();
  await refreshData();
  renderInstrumentsTable();
}

function closeModal() { document.getElementById('editModal').classList.remove('active'); }

function badgeClass(status) {
  const m = { draft:'badge-draft', active:'badge-active', paused:'badge-paused', completed:'badge-completed', partial_close:'badge-partial_close' };
  return m[status] || 'badge-draft';
}

function renderSide(session, sideNum) {
  const sides = session.sides || {};
  for (const [acc, info] of Object.entries(sides)) {
    if (info.side_number === sideNum) {
      const action = session.action || 'open';
      const sidePair = info.pair || session.pair;
      const sideLots = (info.lot_size != null) ? info.lot_size : session.lot_size;
      const sideMaxSpread = (info.max_spread != null) ? info.max_spread : session.max_spread_points;
      const pairDiff = (info.pair && info.pair !== session.pair);
      const lotsDiff = (info.lot_size != null && info.lot_size !== session.lot_size);
      const spreadDiff = (info.max_spread != null && info.max_spread !== session.max_spread_points);
      const pairLabel = pairDiff ? `<span style="color:#fbbf24;font-size:0.72rem">${sidePair}</span>` : '';
      const lotsLabel = lotsDiff ? `<span style="color:#fbbf24;font-size:0.72rem">${sideLots}</span>` : '';
      const spreadLabel = spreadDiff ? `<span style="color:#fbbf24;font-size:0.72rem">spd:${sideMaxSpread}</span>` : '';
      const extras = (pairLabel || lotsLabel || spreadLabel) ? `<br>${[pairLabel, lotsLabel, spreadLabel].filter(Boolean).join(' | ')}` : '';
      // Error/spread reject counts
      const errCount = (session.errors && session.errors[acc]) ? session.errors[acc].length : 0;
      const srCount = (session.spread_rejects && session.spread_rejects[acc]) || 0;
      const errLabel = (errCount > 0 || srCount > 0) ? `<br><span style="font-size:0.7rem;color:var(--red);cursor:pointer;text-decoration:underline dotted" title="Click to clear errors" onclick="event.stopPropagation();fetch('/api/sessions/${session.id}/clear_errors',{method:'POST'}).then(()=>loadSessions())">err:${errCount} sr:${srCount} ✕</span>` : '';
      let count;
      if (action === 'close') {
        const matchMode = session.match_mode || 'ticket';
        if (matchMode === 'lots') {
          const closedLots = (session.closed_lots && session.closed_lots[acc]) || 0;
          const filledLots = (session.filled_lots && session.filled_lots[acc]) || 0;
          const groupLabel = info.group ? ` | ${info.group}` : '';
          return `<strong>${acc}</strong><br><span style="font-size:0.75rem;color:var(--text2)">${info.action.toUpperCase()}${groupLabel}</span>${extras}<br><span style="font-size:0.75rem">${closedLots}/${filledLots} lots closed</span>${errLabel}`;
        }
        count = (session.closed && session.closed[acc]) || 0;
        const target = session.close_count != null ? session.close_count : session.total_positions;
        const groupLabel = info.group ? ` | ${info.group}` : '';
        return `<strong>${acc}</strong><br><span style="font-size:0.75rem;color:var(--text2)">${info.action.toUpperCase()}${groupLabel}</span>${extras}<br><span style="font-size:0.75rem">${count}/${target} closed</span>${errLabel}`;
      } else if (action.startsWith('cycle_')) {
        // Derive cycling account using side_number, not Object.keys() order
        const cycleSideNum = action === 'cycle_acc1' ? 1 : 2;
        if (info.side_number === cycleSideNum) {
          const progress = session.cycle_progress || {};
          const groupLabel = info.group ? ` | ${info.group}` : '';
          return `<strong>${acc}</strong><br><span style="font-size:0.75rem;color:var(--orange)">CYCLING${groupLabel}</span>${extras}<br><span style="font-size:0.75rem">${progress.cycled||0} cycled (${progress.phase||'-'})</span>${errLabel}`;
        }
        // Non-cycling side: show net open positions
        const filled = (session.filled && session.filled[acc]) || 0;
        const closed = (session.closed && session.closed[acc]) || 0;
        count = Math.max(0, filled - closed);
        const groupLabel = info.group ? ` | ${info.group}` : '';
        return `<strong>${acc}</strong><br><span style="font-size:0.75rem;color:var(--text2)">${info.action.toUpperCase()}${groupLabel}</span>${extras}<br><span style="font-size:0.75rem">${count}/${session.total_positions} filled</span>${errLabel}`;
      } else {
        const filled = (session.filled && session.filled[acc]) || 0;
        const closed = (session.closed && session.closed[acc]) || 0;
        count = Math.max(0, filled - closed);
        const groupLabel = info.group ? ` | ${info.group}` : '';
        return `<strong>${acc}</strong><br><span style="font-size:0.75rem;color:var(--text2)">${info.action.toUpperCase()}${groupLabel}</span>${extras}<br><span style="font-size:0.75rem">${count}/${session.total_positions} filled</span>${errLabel}`;
      }
    }
  }
  return '-';
}

function renderProgress(session) {
  const action = session.action || 'monitor';

  // In cycle mode, show cycling progress
  if (action.startsWith('cycle_')) {
    const progress = session.cycle_progress || {};
    const cycled = progress.cycled || 0;
    // Use stored cycle_total (set at cycle start) so the total stays fixed
    let totalToCycle = progress.cycle_total || 0;
    if (!totalToCycle) {
      // Fallback: compute from filled count
      const cycleSideNum = action === 'cycle_acc1' ? 1 : 2;
      for (const [acc, info] of Object.entries(session.sides || {})) {
        if (info.side_number === cycleSideNum) {
          totalToCycle = (session.filled && session.filled[acc]) || 0;
          break;
        }
      }
    }
    // Each position has 2 steps: close then open. Progress = (cycled * 2 + current step) / (total * 2)
    const currentStep = progress.phase === 'open' ? 1 : 0;  // 'close' = step 0, 'open' = step 1
    const stepsCompleted = cycled * 2 + currentStep;
    const totalSteps = totalToCycle * 2;
    const pct = totalSteps > 0 ? Math.min(100, (stepsCompleted / totalSteps * 100)) : 0;
    return `<div class="progress-wrap"><div class="progress-bar" style="width:${pct}%;background:var(--orange)"></div></div>
            <span style="font-size:0.72rem;color:var(--orange)">${cycled}/${totalToCycle} cycled (${pct.toFixed(0)}%)</span>`;
  }

  // Default: show filled positions per account vs total_positions target
  const accs = Object.keys(session.sides || {});
  const totalTarget = session.total_positions || 0;
  let totalFilled = 0;
  for (const acc of accs) {
    const filled = (session.filled && session.filled[acc]) || 0;
    const closed = (session.closed && session.closed[acc]) || 0;
    totalFilled = Math.max(totalFilled, Math.max(0, filled - closed));
  }
  const pct = totalTarget > 0 ? Math.min(100, (totalFilled / totalTarget * 100)) : 0;
  return `<div class="progress-wrap"><div class="progress-bar" style="width:${pct}%"></div></div>
          <span style="font-size:0.72rem;color:var(--text2)">${totalFilled}/${totalTarget} (${pct.toFixed(0)}%)</span>`;
}

function renderActions(session) {
  const s = session.status;
  const mode = session.action || 'monitor';
  let html = '';
  // Get account labels by side_number to match backend cycle logic
  const accs = Object.keys(session.sides || {});
  let acc1Label = accs[0] || 'ACC1';
  let acc2Label = accs[1] || 'ACC2';
  // Use side_number to ensure labels match backend's cycle_acc1/cycle_acc2 mapping
  for (const [acc, info] of Object.entries(session.sides || {})) {
    if (info.side_number === 1) acc1Label = acc;
    if (info.side_number === 2) acc2Label = acc;
  }
  // Mode dropdown
  html += `<select class="btn btn-sm" style="background:var(--surface);color:var(--text);border:1px solid var(--border);padding:2px 4px;font-size:0.7rem;cursor:pointer" onchange="confirmSetMode('${session.id}', this, '${mode}')" title="Session Mode">`;
  html += `<option value="monitor"${mode==='monitor'?' selected':''}>MONITOR</option>`;
  html += `<option value="open"${mode==='open'?' selected':''}>OPEN</option>`;
  html += `<option value="close"${mode==='close'?' selected':''}>CLOSE</option>`;
  html += `<option value="cycle_acc1"${mode==='cycle_acc1'?' selected':''}>CYCLE ${acc1Label}</option>`;
  html += `<option value="cycle_acc2"${mode==='cycle_acc2'?' selected':''}>CYCLE ${acc2Label}</option>`;
  html += `</select> `;
  // Cycle date input — only show when in cycle mode
  if (mode.startsWith('cycle_')) {
    const cycleDays = session.cycle_days ?? '';
    const progress = session.cycle_progress || {};
    html += `<input type="number" min="0" step="0.5" placeholder="Days" style="font-size:0.68rem;width:55px;padding:2px 4px;border-radius:4px;border:1px solid var(--border);background:var(--surface2);color:var(--text);text-align:center" value="${cycleDays}" onchange="saveCycleDays('${session.id}', this.value)" title="Cycle positions older than X days"> `;
    html += `<span style="font-size:0.68rem;color:var(--text2)" title="Cycle progress">cycled:${progress.cycled||0} idx:${progress.index||0} ${progress.phase||'-'}</span> `;
  }
  if (s === 'draft' || s === 'paused') {
    html += `<button class="btn btn-success btn-sm" onclick="startSession('${session.id}')" title="Start">▶</button> `;
    html += `<button class="btn btn-primary btn-sm" onclick="editSession('${session.id}')" title="Edit">✏</button> `;
  }
  if (s === 'active') {
    html += `<button class="btn btn-warning btn-sm" onclick="stopSession('${session.id}')" title="Pause">⏸</button> `;
  }
  // Close All button — always show (netOpen may be 0 for imported sessions with untracked fills)
  html += `<button class="btn btn-sm" style="background:var(--surface);color:var(--red);border:1px solid var(--red);font-size:0.7rem" onclick="closeAllDeals('${session.id}')" title="Close All Positions">✕ Close All</button> `;
  // Reset Cycle button — show when filled > 0 and all positions closed
  const allFilled = Object.keys(session.sides||{}).every(a => (session.filled[a]||0) > 0);
  const allClosed = allFilled && Object.keys(session.sides||{}).every(a => (session.closed[a]||0) >= (session.filled[a]||0));
  if (allClosed) {
    html += `<button class="btn btn-sm" style="background:var(--surface);color:var(--blue);border:1px solid var(--blue);font-size:0.7rem" onclick="resetCycle('${session.id}')" title="Reset cycle counters to start opening again">↺ Reset</button> `;
  }
  html += `<button class="btn btn-sm" style="background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="cloneSession('${session.id}')" title="Clone">⧉</button> `;
  const totalErrors = Object.values(session.errors||{}).reduce((s,v)=>s+v.length,0);
  const totalSR = Object.values(session.spread_rejects||{}).reduce((s,v)=>s+v,0);
  if (totalErrors > 0 || totalSR > 0) {
    html += `<button class="btn btn-sm" style="background:var(--surface);color:var(--orange);border:1px solid var(--orange)" onclick="resetErrors('${session.id}')" title="Reset Errors">↺</button> `;
  }
  const hasRollback = session.rollback_needed && Object.values(session.rollback_needed).some(v=>v>0);
  if (s === 'paused' && (totalErrors > 0 || hasRollback)) {
    html += `<button class="btn btn-sm" style="background:var(--surface);color:var(--green);border:1px solid var(--green)" onclick="unblockSession('${session.id}')" title="Unblock">🔓</button> `;
  }
  html += `<button class="btn btn-danger btn-sm" onclick="deleteSession('${session.id}')" title="Delete">✕</button>`;
  return html;
}

async function cloneSession(id) {
  try {
    const res = await fetch('/api/sessions/' + id + '/clone', { method: 'POST' });
    if (!res.ok) { const d = await res.json(); alert('Clone failed: ' + (d.error||'Unknown')); return; }
    await refreshData();
    renderInstrumentsTable();
  } catch(e) { alert('Clone failed: ' + e); }
}

async function resetErrors(id) {
  try {
    await fetch('/api/sessions/' + id + '/reset_errors', { method: 'POST' });
    await refreshData();
    renderInstrumentsTable();
  } catch(e) { alert('Reset failed: ' + e); }
}

async function unblockSession(id) {
  if (!confirm('Unblock this session? This will clear all rollback states and errors, and set status to Draft.')) return;
  try {
    await fetch('/api/sessions/' + id + '/unblock', { method: 'POST' });
    await refreshData();
    renderInstrumentsTable();
  } catch(e) { alert('Unblock failed: ' + e); }
}

function confirmSetMode(id, selectEl, prevMode) {
  const newMode = selectEl.value;
  const labels = {monitor:'MONITOR', open:'OPEN', close:'CLOSE', cycle_acc1:'CYCLE ACC1', cycle_acc2:'CYCLE ACC2'};
  showConfirmModal('Switch mode to ' + (labels[newMode] || newMode.toUpperCase()) + '?', () => {
    setSessionMode(id, newMode);
  }, 'Confirm');
  // If modal is dismissed, revert dropdown
  const cancelBtn = document.getElementById('confirmModalNo');
  const origClick = cancelBtn.onclick;
  cancelBtn.onclick = () => { selectEl.value = prevMode; document.getElementById('confirmModal').classList.remove('active'); };
}

async function setSessionMode(id, mode) {
  try {
    // Blur the mode dropdown so the renderInstrumentsTable guard
    // doesn't block the re-render after the mode change
    if (document.activeElement) document.activeElement.blur();
    await fetch('/api/sessions/' + id + '/set_mode', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ mode: mode })
    });
    await refreshData();
    renderInstrumentsTable();
  } catch(e) { alert('Set mode failed: ' + e); }
}

async function saveCycleDays(id, val) {
  try {
    const cycleDays = (val !== '' && val !== null && val !== undefined) ? parseFloat(val) : '';
    await fetch('/api/sessions/' + id, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ cycle_days: cycleDays })
    });
  } catch(e) { alert('Save cycle days failed: ' + e); }
}
async function resetCycle(id) {
  showConfirmModal('Reset this session? This will clear all counters and switch to OPEN mode.', async () => {
    try {
      await fetch('/api/sessions/' + id + '/reset_cycle', { method: 'POST' });
      await refreshData();
      renderInstrumentsTable();
    } catch(e) { alert('Reset cycle failed: ' + e); }
  }, 'Reset');
}

async function closeDeal(sessionId, acc1, ticket1, acc2, ticket2) {
  showConfirmModal('Close this deal pair? Both sides will be closed.', async () => {
    try {
      const tickets = {};
      if (ticket1 != null) tickets[acc1] = ticket1;
      if (ticket2 != null) tickets[acc2] = ticket2;
      await fetch('/api/sessions/' + sessionId + '/close_deal', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ tickets: tickets })
      });
      await refreshData();
      renderInstrumentsTable();
      renderOpenedDeals();
    } catch(e) { alert('Close deal failed: ' + e); }
  }, 'OK');
}

async function closeAllDeals(sessionId) {
  showConfirmModal('Close ALL open positions for this session? Both sides will be closed.', async () => {
    try {
      await fetch('/api/sessions/' + sessionId + '/close_all_deals', { method: 'POST' });
      await refreshData();
      renderInstrumentsTable();
      renderOpenedDeals();
    } catch(e) { alert('Close all failed: ' + e); }
  }, 'OK');
}

function execOrderLabel(v) {
  const m = { simultaneous: 'Simultaneous', side1_first: 'Side 1 First', side2_first: 'Side 2 First' };
  return m[v] || v;
}

function renderCurrDiff(s, type) {
  const val = type === 'open' ? s.curr_diff_open : s.curr_diff_close;
  const threshold = type === 'open' ? s.diff_to_open : s.diff_to_close;
  if (typeof val === 'number') {
    const d = val.toFixed(1);
    return `<span class="diff-display">${d}</span>`;
  }
  if (val === '' || val === null || val === undefined) {
    if (!s.diff_reason) return '';  // no price data available — show blank, not warning
  }
  // Show reason why diff is unavailable
  const reason = s.diff_reason || 'no data';
  return `<span class="diff-display" style="color:var(--orange);font-size:0.68rem;cursor:help" title="${reason}">⚠</span>`;
}

function renderSessions(sessions) {
  sessions_cache = sessions;
}

function renderEvents(events) {
  const el = document.getElementById('eventLog');
  if (!events.length) {
    el.innerHTML = '<div class="entry"><span class="ts">--</span><span class="dtl" style="color:var(--text2)">Waiting for events...</span></div>';
    return;
  }
  el.innerHTML = events.reverse().map(e => {
    let evtClass = 'evt';
    if (e.event && e.event.includes('filled')) evtClass += ' filled';
    if (e.event && (e.event.includes('error') || e.event.includes('spread'))) evtClass += ' error';
    if (e.event && e.event.includes('closed')) evtClass += ' closed';
    return `<div class="entry">
      <span class="ts">${(e.ts||'').substring(11)}</span>
      <span class="acct">${e.account||''}</span>
      <span class="${evtClass}">${e.event||''}</span>
      <span class="dtl">${e.detail||''}</span>
    </div>`;
  }).join('');
}

function renderEAIndicators(heartbeats, fixAccounts, mtDirectAccounts) {
  const el = document.getElementById('eaIndicators');
  const items = [];
  const seen = new Set();
  // MT Direct accounts
  if (mtDirectAccounts) {
    Object.entries(mtDirectAccounts).forEach(([id, info]) => {
      const displayName = info.label || id;
      seen.add(id);
      items.push({ name: displayName, online: !!info.connected });
    });
  }
  // FIX accounts
  if (fixAccounts) {
    Object.entries(fixAccounts).forEach(([id, info]) => {
      if (seen.has(id)) return;
      seen.add(id);
      items.push({ name: id, online: !!(info.trade_connected && info.quote_connected) });
    });
  }
  // EA-polled accounts
  if (heartbeats) {
    Object.entries(heartbeats).forEach(([acc, info]) => {
      if (seen.has(acc)) return;
      seen.add(acc);
      items.push({ name: acc, online: !!info.online });
    });
  }
  if (!items.length) {
    el.innerHTML = '<span class="ea-label">No accounts</span>';
    return;
  }
  el.innerHTML = items.map(it =>
    `<span class="ea-label">${it.name}</span><span class="ea-dot ${it.online?'online':''}"></span>`
  ).join(' ');
}

// ── Sound notifications ──
let soundEnabled = localStorage.getItem('soundEnabled') !== 'false';  // default on
let prevFillCounts = {};  // {sessionId_account: count}
let prevCloseCounts = {};

function playTone(freq, duration, type) {
  if (!soundEnabled) return;
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.frequency.value = freq;
    osc.type = type || 'sine';
    gain.gain.setValueAtTime(0.15, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + duration);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + duration);
  } catch(e) {}
}

function playOpenSound() { playTone(880, 0.2, 'sine'); setTimeout(() => playTone(1100, 0.15, 'sine'), 120); }
function playCloseSound() { playTone(660, 0.2, 'triangle'); setTimeout(() => playTone(440, 0.2, 'triangle'), 150); }

function toggleSound() {
  soundEnabled = !soundEnabled;
  localStorage.setItem('soundEnabled', soundEnabled);
  document.getElementById('soundToggle').textContent = soundEnabled ? '🔊' : '🔇';
}

function checkSoundNotifications(sessionsList) {
  sessionsList.forEach(s => {
    for (const acc of Object.keys(s.sides || {})) {
      const fKey = s.id + '_' + acc + '_f';
      const cKey = s.id + '_' + acc + '_c';
      const cyKey = s.id + '_' + acc + '_cy';
      const filled = (s.filled && s.filled[acc]) || 0;
      const closed = (s.closed && s.closed[acc]) || 0;
      const cycled = (s.cycle_progress && s.cycle_progress.cycled) || 0;
      if (prevFillCounts[fKey] !== undefined && filled > prevFillCounts[fKey]) playOpenSound();
      if (prevCloseCounts[cKey] !== undefined && closed > prevCloseCounts[cKey]) playCloseSound();
      if (prevCloseCounts[cyKey] !== undefined && cycled > prevCloseCounts[cyKey]) playCloseSound();
      prevFillCounts[fKey] = filled;
      prevCloseCounts[cKey] = closed;
      prevCloseCounts[cyKey] = cycled;
    }
  });
}

// ── Speech notifications ──
let speechEnabled = localStorage.getItem('speechEnabled') === 'true';  // default off
let prevSessionStates = {};  // {sessionId: {action, allFilled, allClosed, cycleCompleted, rollbackTotal}}
let speechInitialized = false;  // skip first load to avoid announcing existing state

let _lastSpoken = '';  // dedup: last spoken text
let _lastSpokenAt = 0; // dedup: timestamp

function speak(text) {
  if (!speechEnabled) return;
  // Only speak from the main window, not pop-out strategy windows
  if (new URLSearchParams(window.location.search).get('strategy_id')) return;
  // Dedup: don't repeat the same message within 10 seconds
  const now = Date.now();
  if (text === _lastSpoken && (now - _lastSpokenAt) < 10000) return;
  _lastSpoken = text;
  _lastSpokenAt = now;
  try {
    const utter = new SpeechSynthesisUtterance(text);
    utter.rate = 1.1;
    utter.volume = 0.9;
    speechSynthesis.speak(utter);
  } catch(e) { console.error('Speech error:', e); }
}

function toggleSpeech() {
  speechEnabled = !speechEnabled;
  localStorage.setItem('speechEnabled', speechEnabled);
  document.getElementById('speechToggle').textContent = speechEnabled ? '🗣️' : '🤐';
  if (speechEnabled) speak('Speech notifications enabled');
}

function getStrategyName(session) {
  if (!session.strategy_id) return '';
  const strat = strategies_cache.find(s => s.id === session.strategy_id);
  return strat ? strat.name : '';
}

function checkSpeechNotifications(sessionsList, eventLog) {
  // Build session lookup for event-log scanning
  const sessionsById = {};
  sessionsList.forEach(s => { sessionsById[s.id] = s; });

  if (!speechInitialized) {
    // First load: snapshot state without speaking, record last event timestamp
    sessionsList.forEach(s => {
      const rollbackTotal = Object.values(s.rollback_needed || {}).reduce((sum, v) => sum + v, 0);
      prevSessionStates[s.id] = {
        action: s.action || 'monitor',
        rollbackTotal
      };
    });
    // Record the last event timestamp so we only announce events after this point
    prevSessionStates._lastEventTs = eventLog.length > 0 ? eventLog[eventLog.length - 1].ts : '';
    speechInitialized = true;
    document.getElementById('speechToggle').textContent = speechEnabled ? '\ud83d\udde3\ufe0f' : '\ud83e\udd10';
    return;
  }

  // ── 1. Detect MODE TRANSITIONS per session ──
  sessionsList.forEach(s => {
    const sid = s.id;
    const prev = prevSessionStates[sid] || {};
    const action = s.action || 'monitor';
    const stratName = getStrategyName(s);
    const symbol = s.pair || '';
    const label = stratName ? (stratName + ' ' + symbol) : symbol;
    const rollbackTotal = Object.values(s.rollback_needed || {}).reduce((sum, v) => sum + v, 0);

    if (prev.action !== action) {
      if (action.startsWith('cycle_')) {
        speak('Starting cycling ' + label);
      }
      if (action === 'open' && !prev.action?.startsWith?.('open')) {
        speak('Starting opening ' + label);
      }
      if (action === 'close') {
        speak('Started closing mode ' + label);
      }
    }

    // ALERT: orphaned trade detected (rollback count increased)
    if (rollbackTotal > (prev.rollbackTotal || 0)) {
      speak('Alert! Detected orphaned trade! Closing orphaned trade ' + label);
    }

    prevSessionStates[sid] = { action, rollbackTotal };
  });

  // ── 2. Detect COMPLETIONS via new events in the event log ──
  const lastTs = prevSessionStates._lastEventTs || '';
  // Find events newer than the last one we processed (timestamps are ISO-sortable strings)
  const newEvents = eventLog.filter(evt => evt.ts > lastTs);
  if (eventLog.length > 0) {
    prevSessionStates._lastEventTs = eventLog[eventLog.length - 1].ts;
  }

  newEvents.forEach(evt => {
    const s = sessionsById[evt.session_id];
    if (!s) return;
    const stratName = getStrategyName(s);
    const symbol = s.pair || '';
    const label = stratName ? (stratName + ' ' + symbol) : symbol;

    if (evt.event === 'close_complete') {
      speak('Completed closing ' + label);
    } else if (evt.event === 'open_targets_reached') {
      speak('Completed opening ' + label);
    } else if (evt.event === 'cycle_complete') {
      const detail = evt.detail || '';
      const avgMatch = detail.match(/avg spread cost: ([0-9.]+)/);
      const avgCost = avgMatch ? avgMatch[1] : '';
      const msg = avgCost
        ? 'Completed cycling ' + label + '. Average spread cost: ' + avgCost
        : 'Completed cycling ' + label;
      speak(msg);
    } else if (evt.event === 'rebalance_close') {
      speak('Rebalancing. Orphaned position detected on ' + label);
      playCloseSound();
    } else if (evt.event === 'fee_detected') {
      const detail = evt.detail || '';
      const amtMatch = detail.match(/Fee ([\-0-9.]+)/);
      const amt = amtMatch ? amtMatch[1] : '';
      speak('Fee detected on account ' + (evt.account || '') + '. Amount ' + amt);
      playTone(330, 0.4, 'sawtooth');
    }
  });
}

// ─── Reporting Tab ────────────────────────────────────────────────────────
let _reportingCache = null;

async function refreshReporting() {
  try {
    const res = await fetch('/api/reporting?_t=' + Date.now());
    _reportingCache = await res.json();
    renderGroupSummary(_reportingCache);
    renderFeeLog(_reportingCache);
    drawBalanceChart();
  } catch(e) { console.error('Reporting fetch error:', e); }
}

// Track which name/hedge groups are expanded (persists across re-renders)
const _grpExpanded = {};
function toggleGrp(key) {
  _grpExpanded[key] = !_grpExpanded[key];
  document.querySelectorAll('[data-parent="' + key + '"]').forEach(tr => {
    tr.style.display = _grpExpanded[key] ? '' : 'none';
    // If collapsing a name, also collapse its child hedge groups
    if (!_grpExpanded[key]) {
      const hgKey = tr.getAttribute('data-grp-hg');
      if (hgKey) {
        _grpExpanded[hgKey] = false;
        document.querySelectorAll('[data-parent="' + hgKey + '"]').forEach(r => r.style.display = 'none');
      }
    }
  });
  // Update arrow indicators
  document.querySelectorAll('[data-arrow="' + key + '"]').forEach(el => {
    el.textContent = _grpExpanded[key] ? '▾' : '▸';
  });
  // Also update child arrows when collapsing
  if (!_grpExpanded[key]) {
    document.querySelectorAll('[data-parent="' + key + '"]').forEach(tr => {
      const hgKey = tr.getAttribute('data-grp-hg');
      if (hgKey) {
        document.querySelectorAll('[data-arrow="' + hgKey + '"]').forEach(el => { el.textContent = '▸'; });
      }
    });
  }
}

function renderGroupSummary(data) {
  const tbody = document.getElementById('groupSummaryBody');
  const hg = data.hedge_groups || {};
  const nt = data.name_totals || {};
  if (Object.keys(hg).length === 0 && Object.keys(nt).length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text2);padding:20px;">Set group labels on accounts (e.g. IRINA-6-A, IRINA-6-B) to see groups</td></tr>';
    return;
  }
  const rows = [];
  const fmt = v => v != null ? v.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '\u2014';
  const dot = online => '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + (online ? 'var(--green)' : 'var(--red)') + ';margin-right:4px;"></span>';

  // Update chart group dropdown
  const sel = document.getElementById('chartGroupSelect');
  const curVal = sel.value;
  sel.innerHTML = '<option value="__all__">All Groups</option>';
  const nameKeys = Object.keys(nt).sort();
  nameKeys.forEach(n => { sel.innerHTML += '<option value="name:' + n + '">\ud83d\udc64 ' + n + ' (total)</option>'; });
  Object.keys(hg).sort().forEach(k => { sel.innerHTML += '<option value="hg:' + k + '">\ud83d\udd17 ' + k + '</option>'; });
  sel.value = curVal || '__all__';

  let grandBalance = 0, grandEquity = 0;
  const namesSeen = new Set();

  const sortedHgKeys = Object.keys(hg).sort();
  sortedHgKeys.forEach(key => {
    const group = hg[key];
    const name = group.name || '';
    const hedgeNum = group.hedge_num || key;
    const accts = group.accounts || [];
    const nameKey = 'name_' + name;
    const hgKey = 'hg_' + key;
    const nameOpen = !!_grpExpanded[nameKey];
    const hgOpen = !!_grpExpanded[hgKey];

    // Name total row (once per name) — clickable to expand hedge groups
    if (name && !namesSeen.has(name) && nt[name]) {
      namesSeen.add(name);
      const n = nt[name];
      const arrow = nameOpen ? '▾' : '▸';
      rows.push('<tr style="background:rgba(124,58,237,0.08);cursor:pointer;" onclick="toggleGrp(\'' + nameKey + '\')">'+
        '<td style="font-weight:700;font-size:0.95rem;" colspan="5"><span data-arrow="' + nameKey + '" style="display:inline-block;width:16px;font-size:0.8rem;color:var(--text2);">' + arrow + '</span> \ud83d\udc64 ' + name + '</td>' +
        '<td style="text-align:right;"><button class="btn btn-sm" style="padding:2px 10px;font-size:0.72rem;background:var(--accent);color:#fff;border:none;cursor:pointer;" onclick="event.stopPropagation();openPnlModal(\'' + name + '\')">📊 PnL</button></td>' +
        '<td style="font-weight:700;">' + fmt(n.total_balance) + '</td>' +
        '<td style="font-weight:700;">' + fmt(n.total_equity) + '</td></tr>');
    }

    // Hedge group header — child of name, clickable to expand accounts
    const numAccts = accts.length;
    const hgArrow = hgOpen ? '▾' : '▸';
    const hgDisplay = nameOpen ? '' : 'display:none;';
    rows.push('<tr data-parent="' + nameKey + '" data-grp-hg="' + hgKey + '" style="background:rgba(108,92,231,0.04);cursor:pointer;' + hgDisplay + '" onclick="toggleGrp(\'' + hgKey + '\')">' +
      '<td></td>' +
      '<td style="font-weight:600;padding-left:20px;"><span data-arrow="' + hgKey + '" style="display:inline-block;width:16px;font-size:0.8rem;color:var(--text2);">' + hgArrow + '</span> \ud83d\udd17 ' + hedgeNum + '</td>' +
      '<td colspan="4" style="color:var(--text2);font-size:0.78rem;">' + numAccts + ' account' + (numAccts !== 1 ? 's' : '') + '</td>' +
      '<td style="font-weight:600;">' + fmt(group.total_balance) + '</td>' +
      '<td style="font-weight:600;">' + fmt(group.total_equity) + '</td>' +
    '</tr>');

    // Sort accounts: A sides first, then B
    const sortedAccts = accts.slice().sort((a, b) => {
      const sa = (a.side || '').toUpperCase();
      const sb = (b.side || '').toUpperCase();
      return sa < sb ? -1 : sa > sb ? 1 : 0;
    });

    // Individual account rows — child of hedge group
    const acctDisplay = (nameOpen && hgOpen) ? '' : 'display:none;';
    sortedAccts.forEach(a => {
      const side = (a.side || '?').toUpperCase();
      const sideColor = side === 'A' ? 'var(--accent)' : side === 'B' ? 'var(--green)' : 'var(--text2)';
      rows.push('<tr data-parent="' + hgKey + '" style="' + acctDisplay + '">' +
        '<td></td><td></td>' +
        '<td style="color:' + sideColor + ';font-weight:600;padding-left:30px;">' + side + '</td>' +
        '<td>' + dot(a.online) + a.name + '</td>' +
        '<td>' + fmt(a.balance) + '</td>' +
        '<td>' + fmt(a.equity) + '</td>' +
        '<td></td><td></td>' +
      '</tr>');
    });

    grandBalance += (group.total_balance || 0);
    grandEquity += (group.total_equity || 0);
  });

  // Grand total
  rows.push('<tr class="total-row"><td colspan="6" style="text-align:right;">Grand Total</td>' +
    '<td>' + fmt(grandBalance) + '</td><td>' + fmt(grandEquity) + '</td></tr>');
  tbody.innerHTML = rows.join('');
}

function drawBalanceChart() {
  const canvas = document.getElementById('balanceChart');
  const emptyMsg = document.getElementById('chartEmpty');
  if (!_reportingCache || !_reportingCache.snapshots || _reportingCache.snapshots.length === 0) {
    emptyMsg.style.display = 'block';
    return;
  }
  emptyMsg.style.display = 'none';
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;

  const selectedGroup = document.getElementById('chartGroupSelect').value;
  const snapshots = _reportingCache.snapshots;

  const chartMetric = (document.getElementById('chartMetricSelect') || {}).value || 'both';
  const showBal = chartMetric === 'both' || chartMetric === 'balance';
  const showEq  = chartMetric === 'both' || chartMetric === 'equity';

  // Extract data points based on selection
  const points = snapshots.map(s => {
    let bal = 0, eq = 0;
    if (selectedGroup === '__all__') {
      for (const g of Object.values(s.hedge_group_totals || s.group_totals || {})) {
        bal += (g.balance || 0);
        eq  += (g.equity || 0);
      }
    } else if (selectedGroup.startsWith('name:')) {
      const name = selectedGroup.substring(5);
      const nt = (s.name_totals || {})[name];
      bal = nt ? (nt.balance || 0) : 0;
      eq  = nt ? (nt.equity || 0) : 0;
    } else if (selectedGroup.startsWith('hg:')) {
      const hgKey = selectedGroup.substring(3);
      const ht = (s.hedge_group_totals || s.group_totals || {})[hgKey];
      bal = ht ? (ht.balance || 0) : 0;
      eq  = ht ? (ht.equity || 0) : 0;
    } else {
      const gt = (s.group_totals || {})[selectedGroup];
      bal = gt ? (gt.balance || 0) : 0;
      eq  = gt ? (gt.equity || 0) : 0;
    }
    return { date: s.date, balance: bal, equity: eq };
  }).filter(p => p.balance !== 0 || p.equity !== 0);

  if (points.length === 0) {
    emptyMsg.style.display = 'block';
    emptyMsg.textContent = 'No data for selected group';
    return;
  }

  ctx.clearRect(0, 0, W, H);

  const pad = { top: 20, right: 20, bottom: 30, left: 70 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;

  // Compute min/max across all visible series
  let allVals = [];
  if (showBal) allVals = allVals.concat(points.map(p => p.balance));
  if (showEq)  allVals = allVals.concat(points.map(p => p.equity).filter(v => v !== 0));
  if (allVals.length === 0) allVals = points.map(p => p.balance);
  let minV = Math.min(...allVals);
  let maxV = Math.max(...allVals);
  if (minV === maxV) { minV -= 100; maxV += 100; }
  const rangeV = maxV - minV;

  // Grid lines
  ctx.strokeStyle = 'rgba(255,255,255,0.06)';
  ctx.lineWidth = 1;
  const gridCount = 4;
  for (let i = 0; i <= gridCount; i++) {
    const y = pad.top + (cH * i / gridCount);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke();
    const val = maxV - (rangeV * i / gridCount);
    ctx.fillStyle = 'rgba(255,255,255,0.4)';
    ctx.font = '11px Inter, sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(val.toLocaleString('en-US', {maximumFractionDigits: 0}), pad.left - 8, y + 4);
  }

  // Helper: draw a series line + gradient + dots
  function drawSeries(key, lineColor, fillColorTop, fillColorBot, dotColor) {
    ctx.strokeStyle = lineColor;
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.beginPath();
    points.forEach((p, i) => {
      const x = pad.left + (i / Math.max(1, points.length - 1)) * cW;
      const y = pad.top + cH - ((p[key] - minV) / rangeV) * cH;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();
    // Fill gradient
    const grad = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
    grad.addColorStop(0, fillColorTop);
    grad.addColorStop(1, fillColorBot);
    ctx.lineTo(pad.left + cW, pad.top + cH);
    ctx.lineTo(pad.left, pad.top + cH);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();
    // Dots
    points.forEach((p, i) => {
      const x = pad.left + (i / Math.max(1, points.length - 1)) * cW;
      const y = pad.top + cH - ((p[key] - minV) / rangeV) * cH;
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fillStyle = dotColor;
      ctx.fill();
    });
  }

  // Draw equity first (behind) so balance line is on top
  if (showEq)  drawSeries('equity',  '#10b981', 'rgba(16,185,129,0.18)', 'rgba(16,185,129,0.02)', '#6ee7b7');
  if (showBal) drawSeries('balance', '#7c3aed', 'rgba(124,58,237,0.25)', 'rgba(124,58,237,0.02)', '#a78bfa');

  // X-axis labels (show max 10)
  ctx.fillStyle = 'rgba(255,255,255,0.4)';
  ctx.font = '10px Inter, sans-serif';
  ctx.textAlign = 'center';
  const step = Math.max(1, Math.floor(points.length / 10));
  points.forEach((p, i) => {
    if (i % step === 0 || i === points.length - 1) {
      const x = pad.left + (i / Math.max(1, points.length - 1)) * cW;
      ctx.fillText(p.date.substring(5), x, H - 6);
    }
  });

  // Legend
  const legendY = pad.top + 4;
  let legendX = W - pad.right - 10;
  ctx.font = '11px Inter, sans-serif';
  ctx.textAlign = 'right';
  if (showEq) {
    ctx.fillStyle = '#10b981';
    ctx.fillText('Equity', legendX, legendY);
    ctx.fillRect(legendX - ctx.measureText('Equity').width - 14, legendY - 8, 10, 3);
    legendX -= ctx.measureText('Equity').width + 24;
  }
  if (showBal) {
    ctx.fillStyle = '#7c3aed';
    ctx.fillText('Balance', legendX, legendY);
    ctx.fillRect(legendX - ctx.measureText('Balance').width - 14, legendY - 8, 10, 3);
  }
}

function renderFeeLog(data) {
  const tbody = document.getElementById('feeLogBody');
  const fees = data.fees || [];
  if (fees.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text2);padding:20px;">No fees recorded yet</td></tr>';
    return;
  }
  // Update keywords input
  document.getElementById('feeKeywordsInput').value = (data.fee_keywords || []).join(', ');

  const fmt = v => v != null ? v.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '—';
  let totalFees = 0;
  const rows = fees.slice().reverse().map(f => {
    const grp = (manual_accounts_cache[f.account] || {}).group_label || '';
    totalFees += (f.amount || 0);
    return '<tr>' +
      '<td>' + (f.ts || '') + '</td>' +
      '<td>' + (f.account || '') + '</td>' +
      '<td>' + grp + '</td>' +
      '<td class="neg">' + fmt(f.amount) + '</td>' +
      '<td>' + fmt(f.balance_before) + '</td>' +
      '<td>' + fmt(f.balance_after) + '</td>' +
      '<td>' + (f.label || '') + '</td>' +
      '<td><button class="btn btn-danger btn-sm" onclick="deleteFee(\'' + f.id + '\')" style="padding:2px 8px;font-size:0.7rem;">✕</button></td>' +
    '</tr>';
  });
  rows.push('<tr class="total-row"><td colspan="3" style="text-align:right;">Total</td>' +
    '<td class="neg">' + fmt(totalFees) + '</td><td colspan="4"></td></tr>');
  tbody.innerHTML = rows.join('');
}

async function takeSnapshot() {
  try {
    await fetch('/api/reporting/snapshot', { method: 'POST' });
    await refreshReporting();
  } catch(e) { alert('Snapshot failed: ' + e); }
}

async function saveFeeKeywords() {
  const val = document.getElementById('feeKeywordsInput').value;
  try {
    await fetch('/api/reporting/fee_keywords', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ keywords: val })
    });
  } catch(e) { alert('Save failed: ' + e); }
}

async function deleteFee(feeId) {
  try {
    await fetch('/api/reporting/fees/' + feeId, { method: 'DELETE' });
    await refreshReporting();
  } catch(e) { alert('Delete failed: ' + e); }
}

// Settings Tab
let _settingsLoaded = false;

async function loadSettings() {
  try {
    const res = await fetch('/api/settings?_t=' + Date.now());
    const s = await res.json();
    document.getElementById('setEmailEnabled').checked = s.email && s.email.enabled;
    document.getElementById('setSmtpHost').value = (s.email && s.email.smtp_host) || '';
    document.getElementById('setSmtpPort').value = (s.email && s.email.smtp_port) || 587;
    document.getElementById('setSmtpUser').value = (s.email && s.email.smtp_user) || '';
    document.getElementById('setSmtpPass').value = (s.email && s.email.smtp_pass) || '';
    document.getElementById('setFromAddr').value = (s.email && s.email.from_addr) || '';
    document.getElementById('setToAddr').value = (s.email && s.email.to_addr) || '';
    if (document.getElementById('setFundEmailEnabled')) {
        document.getElementById('setFundEmailEnabled').checked = s.fund_email_enabled !== false;
        document.getElementById('setFundEmailTime').value = s.fund_email_time || '08:00';
    }
    document.getElementById('setTgEnabled').checked = s.telegram && s.telegram.enabled;
    document.getElementById('setTgBotToken').value = (s.telegram && s.telegram.bot_token) || '';
    document.getElementById('setTgChatId').value = (s.telegram && s.telegram.chat_id) || '';
    _settingsLoaded = true;
    renderThresholds(s.fee_thresholds || {});
    // Margin alert threshold
    document.getElementById('setMarginAlertThreshold').value = s.margin_alert_threshold != null ? s.margin_alert_threshold : 85;
    renderMarginThresholds(s.margin_alert_threshold || 85, s.margin_alert_thresholds || {});
    // Position change alert
    const posAlertEnabled = !!s.position_change_alert;
    document.getElementById('setPosChangeAlert').checked = posAlertEnabled;
    document.getElementById('setPosChangeOpened').checked = s.position_change_opened !== false;
    document.getElementById('setPosChangeClosed').checked = s.position_change_closed !== false;
    if (document.getElementById('setPosChangeEmail')) {
        document.getElementById('setPosChangeEmail').checked = s.position_change_email !== false;
    }
    if (document.getElementById('setPosChangeTelegram')) {
        document.getElementById('setPosChangeTelegram').checked = s.position_change_telegram !== false;
    }
    togglePosChangeSuboptions(posAlertEnabled);

    // Hedge Disbalance alert
    const disbalanceAlertEnabled = !!s.disbalance_alert_enabled;
    if (document.getElementById('setDisbalanceAlert')) {
        document.getElementById('setDisbalanceAlert').checked = disbalanceAlertEnabled;
        document.getElementById('setDisbalanceEmail').checked = s.disbalance_alert_email !== false;
        document.getElementById('setDisbalanceTelegram').checked = s.disbalance_alert_telegram !== false;
        document.getElementById('setDisbalancePeriod').value = s.disbalance_alert_period_sec != null ? s.disbalance_alert_period_sec : 30;
        toggleDisbalanceSuboptions(disbalanceAlertEnabled);
    }

    // Swap change alert
    document.getElementById('setSwapAlertInstruments').value = s.swap_alert_instruments || '';
    document.getElementById('setSwapAlertEnabled').checked = !!s.swap_alert_enabled;
    document.getElementById('setSwapAlertPct').value = s.swap_alert_pct != null ? s.swap_alert_pct : 10;
    document.getElementById('setSwapAlertInterval').value = s.swap_alert_interval_min != null ? s.swap_alert_interval_min : 60;
    // Theme colors
    loadThemeColorPickers(s.theme_colors || {});
    // Rebalance close delay
    document.getElementById('setRebalCloseDelay').value = s.rebalance_close_delay != null ? s.rebalance_close_delay : 1;
    // Prompt on rollbacks
    const porEl = document.getElementById('setPromptOnRollbacks');
    if (porEl) porEl.checked = !!s.prompt_on_rollbacks;
    // EA Poll enabled
    const eaPollEl = document.getElementById('setEaPollEnabled');
    if (eaPollEl) eaPollEl.checked = s.ea_poll_enabled !== false;  // default true
    // Trading parameters
    document.getElementById('setExecTimeout').value = s.exec_timeout_sec != null ? s.exec_timeout_sec : 60;
    document.getElementById('setExecAlertOnTimeout').checked = !!s.exec_alert_on_timeout;
    document.getElementById('setExecHaltOnTimeout').checked = !!s.exec_halt_on_timeout;
    document.getElementById('setExecRetryClose').checked = !!s.exec_retry_close;
    document.getElementById('setExecRetryMax').value = s.exec_retry_max != null ? s.exec_retry_max : 5;
  } catch(e) { console.error('Load settings error:', e); }
}

async function saveMarginAlertGlobal(value) {
  try {
    const val = parseFloat(value) || 85;
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({margin_alert_threshold: val})
    });
    if (window._marginAlertData) { window._marginAlertData.global_threshold = val; }
  } catch(e) { console.error('Failed to save global margin threshold:', e); }
}

async function saveRebalCloseDelay(value) {
  try {
    const val = Math.max(0, parseFloat(value) || 1);
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({rebalance_close_delay: val})
    });
  } catch(e) { console.error('Failed to save rebalance close delay:', e); }
}

async function saveExecSetting(key, value) {
  try {
    const payload = {};
    payload[key] = value;
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
  } catch(e) { console.error('Failed to save exec setting:', e); }
}

function togglePosChangeSuboptions(enabled) {
  const container = document.getElementById('posChangeSuboptions');
  if (container) {
    container.style.opacity = enabled ? '1' : '0.5';
    container.style.pointerEvents = enabled ? 'auto' : 'none';
    container.querySelectorAll('input').forEach(input => {
      input.disabled = !enabled;
    });
  }
}

async function savePosChangeAlert(enabled) {
  try {
    togglePosChangeSuboptions(enabled);
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({position_change_alert: enabled})
    });
  } catch(e) { console.error('Failed to save position change alert:', e); }
}

async function savePosChangeSuboption(key, enabled) {
  try {
    const payload = {};
    payload[key] = enabled;
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
  } catch(e) { console.error(`Failed to save ${key}:`, e); }
}

function toggleDisbalanceSuboptions(enabled) {
  const div = document.getElementById('disbalanceSuboptions');
  if(div) {
    div.style.opacity = enabled ? '1' : '0.4';
    div.style.pointerEvents = enabled ? 'auto' : 'none';
  }
}

async function saveDisbalanceAlert(enabled) {
  try {
    toggleDisbalanceSuboptions(enabled);
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({disbalance_alert_enabled: enabled})
    });
  } catch(e) { console.error('Failed to save disbalance alert:', e); }
}

async function saveDisbalanceSuboption(key, val) {
  try {
    const payload = {};
    payload[key] = val;
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
  } catch(e) { console.error(`Failed to save ${key}:`, e); }
}

async function saveSwapAlertSetting(key, value) {
  try {
    const payload = {};
    payload[key] = value;
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
  } catch(e) { console.error('Failed to save swap alert setting:', e); }
}

// ── Rollback confirmation polling & popup ────────────────────────────────────
let _rollbackPromptActive = false;
async function _checkPendingRollbacks() {
  if (_rollbackPromptActive) return;
  try {
    const res = await fetch('/api/pending_rollbacks?_t=' + Date.now());
    const data = await res.json();
    const pending = data.pending || [];
    if (pending.length === 0) return;
    // Show prompt for the first pending rollback
    _rollbackPromptActive = true;
    const rb = pending[0];
    const ticketPreview = rb.tickets && rb.tickets.length
      ? `\nFirst tickets: ${rb.tickets.join(', ')}${rb.count > rb.tickets.length ? ` ... (+${rb.count - rb.tickets.length} more)` : ''}`
      : '';
    const msg = `⚠️ ROLLBACK CONFIRMATION REQUIRED\n\nAccount: ${rb.account}\nSession: ${rb.sid_short}\nPair: ${rb.pair || 'N/A'}\nPositions to close: ${rb.count}${ticketPreview}\n\nApprove this rollback?`;
    const approved = confirm(msg);
    await fetch('/api/confirm_rollback', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ sid: rb.sid, account: rb.account, approved })
    });
    _rollbackPromptActive = false;
  } catch(e) {
    _rollbackPromptActive = false;
  }
}
// Poll for pending rollbacks every 2 seconds
setInterval(_checkPendingRollbacks, 2000);

async function saveEaPollEnabled(enabled) {
  try {
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ea_poll_enabled: enabled})
    });
  } catch(e) { console.error('Failed to save EA poll enabled:', e); }
}

function renderMarginThresholds(globalDefault, perAccount) {
  const tbody = document.getElementById('marginThresholdBody');
  if (!tbody) return;
  const allAccounts = new Set();
  for (const acc of Object.keys(ea_heartbeats_cache || {})) allAccounts.add(acc);
  for (const acc of Object.keys(manual_accounts_cache || {})) allAccounts.add(acc);
  for (const acc of Object.keys(mt_direct_accounts_cache || {})) allAccounts.add(acc);
  for (const acc of Object.keys(fix_accounts_cache || {})) allAccounts.add(acc);
  if (allAccounts.size === 0) {
    tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;color:var(--text2);padding:16px;">No accounts yet</td></tr>';
    return;
  }
  const rows = [];
  Array.from(allAccounts).sort().forEach(acc => {
    const grp = (manual_accounts_cache[acc] || {}).group_label ||
                (fix_accounts_cache[acc] || {}).group_label || '';
    const override = perAccount[acc] !== undefined && perAccount[acc] !== null && perAccount[acc] !== 0 ? perAccount[acc] : '';
    const isFix = !!(fix_accounts_cache || {})[acc];
    const saveFunc = isFix ? 'saveFixMarginAlert' : 'saveMarginAlert';
    rows.push('<tr>' +
      '<td>' + acc + '</td>' +
      '<td>' + grp + '</td>' +
      '<td><input type="number" min="0" max="100" step="1" value="' + override + '" placeholder="' + globalDefault + '" ' +
      'style="width:70px;text-align:center;' + (override === '' ? 'color:var(--text2);' : '') + '" ' +
      'onchange="' + saveFunc + '(\'' + acc + '\', this.value)" /></td>' +
    '</tr>');
  });
  tbody.innerHTML = rows.join('');
}

async function saveSettings(silent) {
  const payload = {
    email: {
      enabled: document.getElementById('setEmailEnabled').checked,
      smtp_host: document.getElementById('setSmtpHost').value,
      smtp_port: parseInt(document.getElementById('setSmtpPort').value) || 587,
      smtp_user: document.getElementById('setSmtpUser').value,
      smtp_pass: document.getElementById('setSmtpPass').value,
      from_addr: document.getElementById('setFromAddr').value,
      to_addr: document.getElementById('setToAddr').value,
    },
    telegram: {
      enabled: document.getElementById('setTgEnabled').checked,
      bot_token: document.getElementById('setTgBotToken').value,
      chat_id: document.getElementById('setTgChatId').value,
    },
    fund_email_enabled: document.getElementById('setFundEmailEnabled') ? document.getElementById('setFundEmailEnabled').checked : true,
    fund_email_time: document.getElementById('setFundEmailTime') ? document.getElementById('setFundEmailTime').value : "08:00"
  };
  try {
    const res = await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
    const r = await res.json();
    if (r.ok) { if (!silent) alert('Settings saved!'); } else { alert('Error: ' + (r.error || 'Unknown')); }
  } catch(e) { alert('Save failed: ' + e); }
}

async function testEmail() {
  const btn = document.getElementById('testEmailBtn');
  btn.textContent = 'Saving...';
  await saveSettings(true);
  btn.textContent = 'Sending...';
  try {
    const res = await fetch('/api/settings/test_email', { method: 'POST' });
    const r = await res.json();
    btn.textContent = r.ok ? '\u2705 Sent!' : '\u274c Failed';
    if (!r.ok && r.error) alert('Email test failed: ' + r.error);
  } catch(e) { btn.textContent = '\u274c Error'; alert(e); }
  setTimeout(() => { btn.textContent = '\ud83d\udce8 Test Email'; }, 3000);
}

async function testTelegram() {
  const btn = document.getElementById('testTgBtn');
  btn.textContent = 'Sending...';
  try {
    const res = await fetch('/api/settings/test_telegram', { method: 'POST' });
    const r = await res.json();
    btn.textContent = r.ok ? '\u2705 Sent!' : '\u274c Failed';
    if (!r.ok && r.error) alert('Telegram test failed: ' + r.error);
  } catch(e) { btn.textContent = '\u274c Error'; alert(e); }
  setTimeout(() => { btn.textContent = '\ud83d\udce8 Test Message'; }, 3000);
}

function renderThresholds(thresholds) {
  const tbody = document.getElementById('thresholdBody');
  // Build list of all known accounts
  const allAccounts = new Set();
  for (const acc of Object.keys(ea_heartbeats_cache || {})) allAccounts.add(acc);
  for (const acc of Object.keys(manual_accounts_cache || {})) allAccounts.add(acc);
  for (const acc of Object.keys(mt_direct_accounts_cache || {})) allAccounts.add(acc);
  if (allAccounts.size === 0) {
    tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;color:var(--text2);padding:16px;">No accounts yet</td></tr>';
    return;
  }
  const rows = [];
  Array.from(allAccounts).sort().forEach(acc => {
    const grp = (manual_accounts_cache[acc] || {}).group_label || '';
    const thresh = thresholds[acc] !== undefined ? thresholds[acc] : 0;
    rows.push('<tr>' +
      '<td>' + acc + '</td>' +
      '<td>' + grp + '</td>' +
      '<td><input type="number" min="0" step="0.01" value="' + thresh + '" ' +
      'onchange="updateThreshold(\'' + acc + '\', this.value)" /></td>' +
    '</tr>');
  });
  tbody.innerHTML = rows.join('');
}

async function updateThreshold(account, value) {
  try {
    await fetch('/api/accounts/' + encodeURIComponent(account), {
      method: 'PATCH', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ fee_threshold: parseFloat(value) || 0 })
    });
  } catch(e) { console.error('Threshold update failed:', e); }
}

async function refreshData() {
  const bar = document.getElementById('refreshBar');
  bar.style.width = '30%';
  try {
    const needFills = !positionsPaneCollapsed && currentStrategyId;
    const res = await fetch('/api/status?_t=' + Date.now() + (needFills ? '&include_fills=1' : ''));
    const data = await res.json();
    bar.style.width = '80%';

    // Always process notifications (even when tab is hidden — sounds/speech still work)
    checkSoundNotifications(data.sessions || []);
    checkSpeechNotifications(data.sessions || [], data.event_log || []);

    // Always update data caches (needed for notifications and next render)
    sessions_cache = data.sessions || [];
    ea_heartbeats_cache = data.ea_heartbeats || {};
    manual_accounts_cache = data.manual_accounts || {};
    fix_accounts_cache = data.fix_accounts || {};
    mt_direct_accounts_cache = data.mt_direct_accounts || {};
    swap_delta_cache = data.swap_delta || {};
    window._newsBlackout = data.news_blackout || {};
    window._marginAlertData = data.margin_alert || {global_threshold: 85, per_account: {}};
    window._statsAccounts = Object.entries(ea_heartbeats_cache)
      .filter(([_, info]) => info.stats_log)
      .map(([acc, _]) => acc);
    window._latestStrategies = data.strategies || [];
    window._latestEventLog = data.event_log || [];
    window._latestCycleReminders = data.cycle_reminders || {};
    window._fundDistributions = data.fund_distributions || {};
    window._fundDistributionsLastUpdated = data.fund_distributions_last_updated || "-";
    const badge = document.getElementById('fundDistUpdateBadge');
    if (badge) {
      badge.textContent = `Optimal Dist: ${window._fundDistributionsLastUpdated}`;
    }

    // Skip ALL DOM rendering when browser tab is hidden/minimized
    // (saves 100% of DOM work when user switches to another tab)
    if (document.hidden) {
      bar.style.width = '100%';
      setTimeout(() => { bar.style.width = '0'; }, 100);
      return;
    }

    // Sessions & strategies must always render (strategy modal depends on them)
    renderSessions(sessions_cache);
    renderStrategies(window._latestStrategies, sessions_cache);

    // Only render events/accounts when their tab is active (CPU savings)
    const eventsTab = document.getElementById('tab-events');
    const accountsTab = document.getElementById('tab-accounts');
    if (eventsTab && eventsTab.classList.contains('active')) {
      renderEvents(window._latestEventLog);
    }
    renderEAIndicators(ea_heartbeats_cache, fix_accounts_cache, mt_direct_accounts_cache);
    if (accountsTab && accountsTab.classList.contains('active')) {
      renderAccounts(ea_heartbeats_cache, manual_accounts_cache, fix_accounts_cache, mt_direct_accounts_cache, window._latestCycleReminders, swap_delta_cache);
      applyAcctColVisibility();
    }
    renderCycleReminders(window._latestCycleReminders);
    // Refresh reporting tab if active
    if (document.getElementById('tab-reporting') && document.getElementById('tab-reporting').classList.contains('active')) {
      refreshReporting();
    }
    // Load settings tab if active and not yet loaded
    if (document.getElementById('tab-settings') && document.getElementById('tab-settings').classList.contains('active')) {
      if (!_settingsLoaded) loadSettings();
    }
    // Re-render instruments table and position tabs if a strategy is currently open
    if (currentStrategyId) {
      renderInstrumentsTable();
      renderOpenedDeals();
      if (typeof renderClosedDeals === 'function') renderClosedDeals();
      // Only render log when its tab is active (avoid DOM churn when hidden)
      const logTab = document.getElementById('stab-log');
      if (logTab && logTab.classList.contains('active')) {
        renderStrategyLog(data.event_log || []);
      }
    }
    document.getElementById('serverTime').textContent = new Date().toLocaleTimeString();
  } catch(e) {
    console.error('Refresh failed:', e);
  }
  bar.style.width = '100%';
  setTimeout(() => { bar.style.width = '0'; }, 300);
}

const _dismissedReminders = new Set();
function renderCycleReminders(reminders) {
  const banner = document.getElementById('cycleReminderBanner');
  if (!banner) return;
  const entries = Object.entries(reminders)
    .filter(([acct, r]) => !_dismissedReminders.has(acct) && r.level !== 'OK');
  if (entries.length === 0) {
    banner.style.display = 'none';
    return;
  }
  banner.style.display = 'block';
  banner.innerHTML = entries.map(([acct, r]) => {
    const pct = Math.min(100, (r.days_held / r.max_days) * 100);
    const color = r.level === 'CRITICAL' ? '#ef4444' : '#f59e0b';
    const icon = r.level === 'CRITICAL' ? '🚨' : '⚠️';
    return `<div style="background:${color}22;border:1px solid ${color};border-radius:8px;padding:10px 16px;margin-bottom:6px;display:flex;align-items:center;gap:12px;">
      <span style="font-size:1.4rem;">${icon}</span>
      <div style="flex:1;">
        <strong style="color:${color};">${r.message}</strong>
        <div style="margin-top:4px;height:4px;background:var(--surface2);border-radius:2px;overflow:hidden;">
          <div style="width:${pct}%;height:100%;background:${color};border-radius:2px;transition:width 0.3s;"></div>
        </div>
      </div>
      <button onclick="_dismissedReminders.add('${acct}');this.parentElement.remove();if(!document.getElementById('cycleReminderBanner').children.length)document.getElementById('cycleReminderBanner').style.display='none';" style="background:none;border:none;color:${color};font-size:1.5rem;cursor:pointer;padding:4px 8px;line-height:1;opacity:1;font-weight:bold;" title="Dismiss" onmouseover="this.style.opacity='0.5'" onmouseout="this.style.opacity='1'">&times;</button>
    </div>`;
  }).join('');
}

function renderAccounts(heartbeats, manualAccounts, fixAccounts, mtDirectAccounts, cycleReminders, swapDelta) {
  // Stash args for re-render from toggleGroupView
  window._lastRenderAccountsArgs = [heartbeats, manualAccounts, fixAccounts, mtDirectAccounts, cycleReminders, swapDelta];
  const tbody = document.getElementById('accountsBody');
  // Skip re-render if user is editing a field inside the accounts table
  if (tbody.contains(document.activeElement) && document.activeElement.tagName === 'INPUT') return;

  // ── Grouped view ─────────────────────────────────────────────────
  if (_groupViewEnabled) {
    _renderGroupedAccounts(tbody, heartbeats, manualAccounts, fixAccounts, mtDirectAccounts, cycleReminders, swapDelta);
    return;
  }

  const rows = [];
  const shownAccounts = new Set();
  const fundDists = window._fundDistributions || {};
  cycleReminders = cycleReminders || {};
  swapDelta = swapDelta || {};
  function _swapDeltaCell(id) {
    const d = swapDelta[id];
    if (d == null) return '<td style="font-size:0.78rem;color:var(--text2)">-</td>';
    const color = d >= 0 ? 'var(--green)' : 'var(--red)';
    const sign = d > 0 ? '+' : '';
    return `<td style="font-size:0.78rem;color:${color}"><a href="#" onclick="showAccountSwapBreakdown('${id}');return false;" style="color:inherit;text-decoration:underline;text-decoration-style:dotted;cursor:pointer;" title="Click to see per-instrument swap breakdown">${sign}${d.toFixed(2)}</a></td>`;
  }
  function _lotsCell(id, lotsVal, style) {
    if (lotsVal == null || lotsVal === '-') return '<td>-</td>';
    return `<td style="${style}"><a href="#" onclick="showAccountLotsBreakdown('${id}');return false;" style="color:inherit;text-decoration:underline;text-decoration-style:dotted;cursor:pointer;" title="Click to see per-instrument breakdown">${lotsVal}</a></td>`;
  }
  function _ageCell(acctInfo, id) {
    // Try direct account info first (MT Direct embeds age in get_status)
    let d = acctInfo && acctInfo.oldest_position_age;
    let maxD = acctInfo && acctInfo.cycle_max_days;
    let remD = acctInfo && acctInfo.cycle_remind_days;
    // Fall back to cycle_reminders data
    if (d == null && cycleReminders[id]) {
      d = cycleReminders[id].days_held;
      maxD = cycleReminders[id].max_days;
      remD = cycleReminders[id].remind_days;
    }
    if (d == null) return '<td></td>';
    // Age should only show if Remind or Max Days is populated
    if ((remD == null || remD === 0 || remD === '') && (maxD == null || maxD === 0 || maxD === '')) {
      return '<td></td>';
    }
    let color = '';
    if (maxD != null && maxD > 0 && d >= maxD) color = 'var(--red)';
    else if (remD != null && remD > 0 && d >= remD) color = 'var(--orange)';
    const style = color ? `font-weight:600;color:${color};font-size:0.82rem;` : 'font-size:0.82rem;';
    return `<td style="${style}" title="${d} rollover days (remind: ${remD||'-'} / max: ${maxD||'-'})">${d}</td>`;
  }

  // Margin alert threshold cell — editable inline
  const _marginAlertData = window._marginAlertData || {global_threshold: 85, per_account: {}};
  function _marginAlertCell(id, isFix) {
    const pa = _marginAlertData.per_account || {};
    const globalT = _marginAlertData.global_threshold || 85;
    const acctT = pa[id];
    const hasCustom = acctT != null && acctT !== '' && acctT !== 0;
    const displayVal = hasCustom ? acctT : '';
    const placeholder = globalT;
    const saveFunc = isFix ? 'saveFixMarginAlert' : 'saveMarginAlert';
    return `<td><input type="number" class="inl" style="width:50px;text-align:center;${hasCustom ? '' : 'color:var(--text2);'}" value="${displayVal}" placeholder="${placeholder}" onchange="${saveFunc}('${id}', this.value)" onkeydown="if(event.key==='Enter')this.blur()" title="${hasCustom ? 'Custom: ' + acctT + '%' : 'Using global: ' + globalT + '%'}"></td>`;
  }

  // FIX accounts first — show with special styling
  if (fixAccounts) {
    Object.entries(fixAccounts).forEach(([id, info]) => {
      shownAccounts.add(id);
      const tConn = info.trade_connected;
      const qConn = info.quote_connected;
      const allConn = tConn && qConn;
      const tDot = `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${tConn?'var(--green)':'var(--red)'};margin-right:4px;"></span>T`;
      const qDot = `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${qConn?'var(--green)':'var(--red)'};margin-right:4px;"></span>Q`;
      const fixColor = allConn ? 'var(--green)' : 'var(--red)';
      const implLabel = (info.implementation === 'ctrader') ? 'cTrader FIX' : (info.implementation === 'openapi') ? 'cT OpenAPI' : (info.implementation === 'dukascopy') ? 'DK FIX' : (info.implementation === 'swissquote') ? 'SQ FIX' : 'FIX';
      const connDot = `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${fixColor};box-shadow:0 0 6px ${fixColor};margin-right:6px;"></span>${implLabel} &nbsp;${tDot}&nbsp;${qDot}`;
      const eaInfo = heartbeats ? heartbeats[id] : null;
      // Balance & equity: prefer FIX collateral data, fall back to EA data
      const rawBal = info.balance != null ? info.balance : (eaInfo && eaInfo.balance != null ? eaInfo.balance : null);
      const rawEq = info.equity != null ? info.equity : (eaInfo && eaInfo.equity != null ? eaInfo.equity : null);
      const bal = rawBal != null ? parseFloat(rawBal).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const eq = rawEq != null ? parseFloat(rawEq).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const lp = eaInfo && eaInfo.last_poll ? `${eaInfo.last_poll}` : '-';
      const lev = info.leverage ? ('1:' + info.leverage) : '-';
      const rawMargin1 = info.margin != null ? info.margin : (eaInfo && eaInfo.margin != null ? eaInfo.margin : null);
      const mu1 = (rawEq > 0 && rawMargin1 != null) ? ((rawMargin1 / rawEq) * 100).toFixed(1) + '%' : '-';
      const pnl1 = info.total_pnl != null ? parseFloat(info.total_pnl).toFixed(2) : '-';
      const pnl1Style = info.total_pnl != null ? (info.total_pnl >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';
      const swap1 = info.total_swap != null ? parseFloat(info.total_swap).toFixed(2) : '-';
      const pos1 = info.positions != null ? info.positions : '-';
      const lots1 = info.total_lots != null ? info.total_lots : '-';
      const lots1Style = info.total_lots != null ? (info.total_lots >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

      const dist = fundDists[id] || {};
      const optEqVal = dist.optimal_equity != null ? dist.optimal_equity.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const shiftVal = dist.suggested_transfer != null ? dist.suggested_transfer.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const shiftColor = dist.suggested_transfer != null ? (dist.suggested_transfer >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

      rows.push(`<tr>
        <td><strong>${id}</strong></td>
        <td><input class="inl" style="width:80px;" value="${info.group_label || ''}" onchange="saveFixGroupLabel('${id}', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td>${connDot}</td>
        <td>${bal}</td>
        <td>${eq}</td>
        <td>${optEqVal}</td>
        <td style="${shiftColor}">${shiftVal}</td>
        <td style="${pnl1Style}">${pnl1}</td>
        <td>${lev}</td>
        <td>${pos1}</td>
        ${_lotsCell(id, lots1, lots1Style)}
        <td>${mu1}</td>
        ${_marginAlertCell(id, true)}
        <td>${swap1}</td>
        ${_swapDeltaCell(id)}
        ${_ageCell(info, id)}
        <td style="font-size:0.78rem">${lp}</td>
        <td><input type="checkbox" ${info.auto_connect_start !== false ? 'checked' : ''} onchange="saveAccountField('${id}', 'auto_connect_start', this.checked)"></td>
        <td><input class="inl" style="width:120px;" value="${info.alert_email || ''}" placeholder="No override" onchange="saveAccountField('${id}', 'alert_email', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td><input class="inl" style="width:120px;" value="${info.alert_telegram || ''}" placeholder="No override" onchange="saveAccountField('${id}', 'alert_telegram', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td><input type="checkbox" ${(window._statsAccounts||[]).includes(id)?'checked':''} onchange="toggleStatsLog('${id}', this.checked)" title="Log market stats to CSV"></td>
        <td>
          <button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="editFixAccount('${id}')" title="Edit credentials">\u270e</button>
          <button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="reconnectFixAccount('${id}')" title="Reconnect">\u21bb</button>
          <button class="btn" style="padding:2px 8px;font-size:0.72rem;background:var(--surface);color:var(--orange);border:1px solid var(--orange);" onclick="disconnectFixAccount('${id}')" title="Disconnect (keep config)">\u23fb</button>
          <button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="openRptModal('${id}')" title="Generate quote stats report">📊</button>
          <button class="btn btn-danger" style="padding:2px 8px;font-size:0.72rem;" onclick="deleteFixAccount('${id}')" title="Delete account">x</button>
        </td>
      </tr>`);
    });
  }

  // MT Direct accounts
  if (mtDirectAccounts) {
    Object.entries(mtDirectAccounts).forEach(([id, info]) => {
      shownAccounts.add(id);
      const isConn = info.connected;
      const mtColor = isConn ? 'var(--green)' : 'var(--red)';
      const typeLabel = (info.type === 'mt5_direct' || info.type === 'mt5') ? 'MT5' : 'MT4';
      const errTip = (!isConn && info.last_error) ? ` <span style="font-size:0.65rem;color:var(--red)" title="${info.last_error}">⚠ ${info.last_error.substring(0,40)}${info.last_error.length>40?'…':''}</span>` : '';
      const connDot = `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${mtColor};box-shadow:0 0 6px ${mtColor};margin-right:6px;"></span>${typeLabel}${errTip}`;
      const eaInfo = heartbeats ? heartbeats[id] : null;
      const rawBal = info.balance != null ? info.balance : (eaInfo && eaInfo.balance != null ? eaInfo.balance : null);
      const rawEq = info.equity != null ? info.equity : (eaInfo && eaInfo.equity != null ? eaInfo.equity : null);
      const bal = rawBal != null ? parseFloat(rawBal).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const eq = rawEq != null ? parseFloat(rawEq).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const lev = info.leverage ? ('1:' + info.leverage) : '-';
      const rawMarginMt = info.margin != null ? info.margin : (eaInfo && eaInfo.margin != null ? eaInfo.margin : null);
      const muMt = (rawEq > 0 && rawMarginMt != null) ? ((rawMarginMt / rawEq) * 100).toFixed(1) + '%' : '-';
      const displayName = info.label || id;
      const pnlMt = info.total_pnl != null ? parseFloat(info.total_pnl).toFixed(2) : '-';
      const pnlMtStyle = info.total_pnl != null ? (info.total_pnl >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';
      const swapMt = info.total_swap != null ? parseFloat(info.total_swap).toFixed(2) : '-';
      const posMt = info.positions != null ? info.positions : '-';
      const lotsMt = info.total_lots != null ? info.total_lots : '-';
      const lotsMtStyle = info.total_lots != null ? (info.total_lots >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

      const distMt = fundDists[id] || {};
      const optEqMt = distMt.optimal_equity != null ? distMt.optimal_equity.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const shiftMt = distMt.suggested_transfer != null ? distMt.suggested_transfer.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const shiftMtColor = distMt.suggested_transfer != null ? (distMt.suggested_transfer >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

      rows.push(`<tr>
        <td><strong>${displayName}</strong></td>
        <td><input class="inl" style="width:80px;" value="${info.group_label || ''}" onchange="saveGroupLabel('${id}', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td>${connDot}</td>
        <td>${bal}</td>
        <td>${eq}</td>
        <td>${optEqMt}</td>
        <td style="${shiftMtColor}">${shiftMt}</td>
        <td style="${pnlMtStyle}">${pnlMt}</td>
        <td>${lev}</td>
        <td>${posMt}</td>
        ${_lotsCell(id, lotsMt, lotsMtStyle)}
        <td>${muMt}</td>
        ${_marginAlertCell(id, false)}
        <td>${swapMt}</td>
        ${_swapDeltaCell(id)}
        ${_ageCell(info, id)}
        <td style="font-size:0.78rem">-</td>
        <td><input type="checkbox" ${info.auto_connect_start !== false ? 'checked' : ''} onchange="saveAccountField('${id}', 'auto_connect_start', this.checked)"></td>
        <td><input class="inl" style="width:120px;" value="${info.alert_email || ''}" placeholder="No override" onchange="saveAccountField('${id}', 'alert_email', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td><input class="inl" style="width:120px;" value="${info.alert_telegram || ''}" placeholder="No override" onchange="saveAccountField('${id}', 'alert_telegram', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td><input type="checkbox" ${(window._statsAccounts||[]).includes(id)?'checked':''} onchange="toggleStatsLog('${id}', this.checked)" title="Log market stats to CSV"></td>
        <td>
          <button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="editMTDirect('${id}')" title="Edit credentials">\u270e</button>
          <button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="connectMTDirect('${id}')" title="Connect">\u21bb</button>
          <button class="btn" style="padding:2px 8px;font-size:0.72rem;background:var(--surface);color:var(--orange);border:1px solid var(--orange);" onclick="disconnectMTDirect('${id}')" title="Disconnect">\u23fb</button>
          <button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="openRptModal('${id}')" title="Generate quote stats report">📊</button>
          <button class="btn btn-danger" style="padding:2px 8px;font-size:0.72rem;" onclick="deleteMTDirect('${id}')" title="Delete account">x</button>
        </td>
      </tr>`);
    });
  }

  // Manual accounts — merge with EA heartbeat data if available
  if (manualAccounts) {
    Object.entries(manualAccounts).forEach(([name, info]) => {
      if (shownAccounts.has(name)) return; // Already rendered by FIX or MT Direct
      shownAccounts.add(name);
      const eaInfo = heartbeats ? heartbeats[name] : null;
      let connDot, bal, eq, lp;
      if (eaInfo) {
        connDot = eaInfo.online
          ? '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green);margin-right:6px;"></span>EA Online'
          : '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--red);box-shadow:0 0 6px var(--red);margin-right:6px;"></span>EA Offline';
        bal = eaInfo.balance != null ? parseFloat(eaInfo.balance).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
        eq = eaInfo.equity != null ? parseFloat(eaInfo.equity).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
        lp = eaInfo.last_poll ? `${eaInfo.last_poll} <span style="font-size:0.68rem;color:var(--text2)">(${eaInfo.ago_sec}s ago)</span>` : '-';
      } else {
        const connType = info.conn_type || 'manual';
        const connLabel = connType === 'fix' ? 'FIX API' : connType === 'poll' ? 'EA Poll' : 'Manual';
        connDot = '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--accent);box-shadow:0 0 6px var(--accent);margin-right:6px;"></span>' + connLabel;
        bal = info.balance != null ? parseFloat(info.balance).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
        eq = info.equity != null ? parseFloat(info.equity).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
        lp = '-';
      }
      const lev = eaInfo && eaInfo.leverage ? ('1:' + eaInfo.leverage) : '-';
      const rawMarginM = eaInfo && eaInfo.margin != null ? eaInfo.margin : (info.margin != null ? info.margin : null);
      const rawEqM = eaInfo && eaInfo.equity != null ? parseFloat(eaInfo.equity) : (info.equity != null ? parseFloat(info.equity) : 0);
      const muM = (rawEqM > 0 && rawMarginM != null) ? ((rawMarginM / rawEqM) * 100).toFixed(1) + '%' : '-';
      const distM = fundDists[name] || {};
      const optEqM = distM.optimal_equity != null ? distM.optimal_equity.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const shiftM = distM.suggested_transfer != null ? distM.suggested_transfer.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const shiftMColor = distM.suggested_transfer != null ? (distM.suggested_transfer >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

      rows.push(`<tr>
        <td><strong>${name}</strong></td>
        <td><input class="inl" style="width:80px;" value="${info.group_label || ''}" onchange="saveGroupLabel('${name}', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td>${connDot}</td>
        <td>${bal}</td>
        <td>${eq}</td>
        <td>${optEqM}</td>
        <td style="${shiftMColor}">${shiftM}</td>
        <td>-</td>
        <td>${lev}</td>
        <td>-</td>
        <td>-</td>
        <td>${muM}</td>
        ${_marginAlertCell(name, false)}
        <td>-</td>
        ${_swapDeltaCell(name)}
        ${_ageCell(null, name)}
        <td style="font-size:0.78rem">${lp}</td>
        <td style="text-align:center;color:var(--text3)">-</td>
        <td><input class="inl" style="width:120px;" value="${info.alert_email || ''}" placeholder="No override" onchange="saveAccountField('${name}', 'alert_email', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td><input class="inl" style="width:120px;" value="${info.alert_telegram || ''}" placeholder="No override" onchange="saveAccountField('${name}', 'alert_telegram', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td><input type="checkbox" ${(window._statsAccounts||[]).includes(name)?'checked':''} onchange="toggleStatsLog('${name}', this.checked)" title="Log market stats to CSV"></td>
        <td><button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="editEAAccount('${name}')" title="Edit account settings">\u270e</button> <button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="openRptModal('${name}')" title="Generate quote stats report">📊</button> <button class="btn btn-danger" style="padding:2px 8px;font-size:0.72rem;" onclick="deleteAccount('${name}')" title="Delete account">x</button></td>
      </tr>`);
    });
  }

  // EA-polled accounts that weren't manually added (auto-discovered)
  if (heartbeats) {
    Object.entries(heartbeats).forEach(([acc, info]) => {
      if (shownAccounts.has(acc)) return;
      const connDot = info.online
        ? '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green);margin-right:6px;"></span>EA Online'
        : '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:var(--red);box-shadow:0 0 6px var(--red);margin-right:6px;"></span>EA Offline';
      const bal = info.balance != null ? parseFloat(info.balance).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const eq = info.equity != null ? parseFloat(info.equity).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const lp = info.last_poll ? `${info.last_poll} <span style="font-size:0.68rem;color:var(--text2)">(${info.ago_sec}s ago)</span>` : '-';
      const lev = info.leverage ? ('1:' + info.leverage) : '-';
      const rawMarginE = info.margin != null ? parseFloat(info.margin) : null;
      const rawEqE = info.equity != null ? parseFloat(info.equity) : 0;
      const muE = (rawEqE > 0 && rawMarginE != null) ? ((rawMarginE / rawEqE) * 100).toFixed(1) + '%' : '-';
      const mConfig = manualAccounts ? (manualAccounts[acc] || {}) : {};

      const distE = fundDists[acc] || {};
      const optEqE = distE.optimal_equity != null ? distE.optimal_equity.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const shiftE = distE.suggested_transfer != null ? distE.suggested_transfer.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
      const shiftEColor = distE.suggested_transfer != null ? (distE.suggested_transfer >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

      rows.push(`<tr>
        <td><strong>${acc}</strong></td>
        <td><input class="inl" style="width:80px;" value="${mConfig.group_label || ''}" onchange="saveGroupLabel('${acc}', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td>${connDot}</td>
        <td>${bal}</td>
        <td>${eq}</td>
        <td>${optEqE}</td>
        <td style="${shiftEColor}">${shiftE}</td>
        <td>-</td>
        <td>${lev}</td>
        <td>-</td>
        <td>-</td>
        <td>${muE}</td>
        ${_marginAlertCell(acc, false)}
        <td>-</td>
        ${_swapDeltaCell(acc)}
        ${_ageCell(info, acc)}
        <td style="font-size:0.78rem">${lp}</td>
        <td style="text-align:center;color:var(--text3)">-</td>
        <td><input class="inl" style="width:120px;" value="${mConfig.alert_email || ''}" placeholder="No override" onchange="saveAccountField('${acc}', 'alert_email', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td><input class="inl" style="width:120px;" value="${mConfig.alert_telegram || ''}" placeholder="No override" onchange="saveAccountField('${acc}', 'alert_telegram', this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
        <td><input type="checkbox" ${(window._statsAccounts||[]).includes(acc)?'checked':''} onchange="toggleStatsLog('${acc}', this.checked)" title="Log market stats to CSV"></td>
        <td><button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="editEAAccount('${acc}')" title="Edit account settings">\u270e</button> <button class="btn" style="padding:2px 8px;font-size:0.72rem;" onclick="openRptModal('${acc}')" title="Generate quote stats report">📊</button> <button class="btn btn-danger" style="padding:2px 8px;font-size:0.72rem;" onclick="deleteAccount('${acc}')" title="Delete account">x</button></td>
      </tr>`);
    });
  }
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="20" style="text-align:center;color:var(--text2);padding:30px;">No accounts yet</td></tr>';
  } else {
    // ── Compute TOTALS across all accounts ──
    let totBal = 0, totEq = 0, totPnl = 0, totLots = 0, totSwap = 0;
    let totPosLots = 0, totNegLots = 0;
    let totOptEq = 0, totShift = 0;
    let hasBal = false, hasEq = false, hasPnl = false, hasLots = false, hasSwap = false;
    let hasOptEq = false, hasShift = false;
    const _seen = new Set();
    function _addAcct(id, info) {
      if (_seen.has(id)) return;
      _seen.add(id);
      // For accounts that appear in heartbeats too, prefer heartbeat balance/equity
      // (they overlay the same ea_account_info dict on the backend)
      const hb = heartbeats ? heartbeats[id] : null;
      const rb = info.balance != null ? parseFloat(info.balance) : (hb && hb.balance != null ? parseFloat(hb.balance) : NaN);
      if (!isNaN(rb)) { totBal += rb; hasBal = true; }
      const re = info.equity != null ? parseFloat(info.equity) : (hb && hb.equity != null ? parseFloat(hb.equity) : NaN);
      if (!isNaN(re)) { totEq += re; hasEq = true; }
      const rp = info.total_pnl != null ? parseFloat(info.total_pnl) : NaN;
      if (!isNaN(rp)) { totPnl += rp; hasPnl = true; }
      const rl = info.total_lots != null ? parseFloat(info.total_lots) : NaN;
      if (!isNaN(rl)) { totLots += rl; hasLots = true; if (rl >= 0) totPosLots += rl; else totNegLots += rl; }
      const rs = info.total_swap != null ? parseFloat(info.total_swap) : NaN;
      if (!isNaN(rs)) { totSwap += rs; hasSwap = true; }

      const dist = fundDists[id] || {};
      if (dist.optimal_equity != null) { totOptEq += dist.optimal_equity; hasOptEq = true; }
      if (dist.suggested_transfer != null) { totShift += dist.suggested_transfer; hasShift = true; }
    }
    // Add in render-order: FIX, MT Direct, Manual, EA-only
    if (fixAccounts) Object.entries(fixAccounts).forEach(([id, info]) => _addAcct(id, info));
    if (mtDirectAccounts) Object.entries(mtDirectAccounts).forEach(([id, info]) => _addAcct(id, info));
    if (manualAccounts) Object.entries(manualAccounts).forEach(([id, info]) => _addAcct(id, info));
    if (heartbeats) Object.entries(heartbeats).forEach(([id, info]) => _addAcct(id, info));
    const fmtBal = hasBal ? totBal.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
    const fmtEq  = hasEq  ? totEq.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
    const fmtPnl = hasPnl ? totPnl.toFixed(2) : '-';
    const pnlStyle = hasPnl ? (totPnl >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';
    const fmtLots = hasLots ? totLots.toFixed(2) : '-';
    const lotsStyle = hasLots ? (totLots >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';
    const lotsBreakdown = hasLots ? `<br><a href="#" onclick="showLotsBreakdown();return false;" style="font-size:0.7rem;font-weight:400;color:#e2e8f0;text-decoration:underline;cursor:pointer;">(${totPosLots.toFixed(2)} / ${totNegLots.toFixed(2)})</a>` : '';
    const fmtSwap = hasSwap ? totSwap.toFixed(2) : '-';

    const fmtOptEq = hasOptEq ? totOptEq.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
    const fmtShift = hasShift ? totShift.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
    const shiftStyle = hasShift ? (totShift >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

    // Sum swap deltas
    let totSwapDelta = 0; let hasSwapDelta = false;
    Object.values(swapDelta || {}).forEach(v => { totSwapDelta += v; hasSwapDelta = true; });
    const fmtSwapDelta = hasSwapDelta ? ((totSwapDelta > 0 ? '+' : '') + totSwapDelta.toFixed(2)) : '-';
    const swapDeltaStyle = hasSwapDelta ? (totSwapDelta >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';
    const totalsSwapDeltaCell = hasSwapDelta
      ? `<td style="${swapDeltaStyle};font-size:0.78rem"><a href="#" onclick="showSwapBreakdown();return false;" style="color:inherit;text-decoration:underline;text-decoration-style:dotted;cursor:pointer;" title="Click to see per-instrument swap breakdown (All Accounts)">${fmtSwapDelta}</a></td>`
      : `<td style="${swapDeltaStyle};font-size:0.78rem">${fmtSwapDelta}</td>`;
    rows.push(`<tr style="border-top:2px solid var(--accent);font-weight:700;background:var(--surface2);">
      <td>TOTALS</td>
      <td></td><td></td>
      <td>${fmtBal}</td>
      <td>${fmtEq}</td>
      <td>${fmtOptEq}</td>
      <td style="${shiftStyle}">${fmtShift}</td>
      <td style="${pnlStyle}">${fmtPnl}</td>
      <td></td><td></td>
      <td style="${lotsStyle}">${fmtLots}${lotsBreakdown}</td>
      <td></td><td></td>
      <td>${fmtSwap}</td>
      ${totalsSwapDeltaCell}
      <td></td><td></td><td></td><td></td><td></td><td></td><td></td>
    </tr>`);
    tbody.innerHTML = rows.join('');
  }
}

// ─── Grouped Accounts View ──────────────────────────────────────────────
function _renderGroupedAccounts(tbody, heartbeats, manualAccounts, fixAccounts, mtDirectAccounts, cycleReminders, swapDelta) {
  cycleReminders = cycleReminders || {};
  swapDelta = swapDelta || {};
  const fundDists = window._fundDistributions || {};

  // 1. Collect all unique accounts with their info (same dedup as normal view)
  const allAccts = {};  // id -> {info, source}
  const seen = new Set();
  function _collectAcct(id, info, source) {
    if (seen.has(id)) return;
    seen.add(id);
    allAccts[id] = { info, source };
  }
  if (fixAccounts) Object.entries(fixAccounts).forEach(([id, info]) => _collectAcct(id, info, 'fix'));
  if (mtDirectAccounts) Object.entries(mtDirectAccounts).forEach(([id, info]) => _collectAcct(id, info, 'mt'));
  if (manualAccounts) Object.entries(manualAccounts).forEach(([id, info]) => {
    if (!seen.has(id)) _collectAcct(id, info, 'manual');
  });
  if (heartbeats) Object.entries(heartbeats).forEach(([id, info]) => {
    if (!seen.has(id)) _collectAcct(id, info, 'ea');
  });

  if (Object.keys(allAccts).length === 0) {
    tbody.innerHTML = '<tr><td colspan="20" style="text-align:center;color:var(--text2);padding:30px;">No accounts yet</td></tr>';
    return;
  }

  // 2. Group by name prefix (before first hyphen)
  const groups = {};  // prefix -> [{id, info, source}]
  for (const [id, data] of Object.entries(allAccts)) {
    // Use display name: MT Direct uses label, FIX uses id, EA uses id
    let displayName = id;
    if (data.source === 'mt' && data.info.label) displayName = data.info.label;
    const dashIdx = displayName.indexOf('-');
    const prefix = dashIdx > 0 ? displayName.substring(0, dashIdx) : displayName;
    if (!groups[prefix]) groups[prefix] = [];
    groups[prefix].push({ id, info: data.info, source: data.source, displayName });
  }

  // 3. Render group rows
  const rows = [];
  // Grand totals for TOTALS row
  let gBal = 0, gEq = 0, gPnl = 0, gLots = 0, gSwap = 0, gPosLots = 0, gNegLots = 0;
  let gHasBal = false, gHasEq = false, gHasPnl = false, gHasLots = false, gHasSwap = false;
  let gSwapDelta = 0, gHasSwapDelta = false;
  let gMaxAge = null;
  let gMaxMu = null;
  let gOptEq = 0, gShift = 0;
  let gHasOptEq = false, gHasShift = false;

  const sortedGroups = Object.keys(groups).sort();
  for (const prefix of sortedGroups) {
    const members = groups[prefix];
    let sumBal = 0, sumEq = 0, sumPnl = 0, sumLots = 0, sumSwap = 0;
    let sumPos = 0, sumPosLots = 0, sumNegLots = 0;
    let hasBal = false, hasEq = false, hasPnl = false, hasLots = false, hasSwap = false, hasPos = false;
    let maxMu = null;  // highest margin use %
    let maxMuLev = null;  // leverage of the account with highest margin use
    let maxAge = null;
    let sumSwapDelta = 0, hasSwapDelta = false;
    let sumOptEq = 0, sumShift = 0;
    let hasOptEq = false, hasShift = false;
    const memberIds = [];
    let connectedCount = 0;  // Count of connected accounts in this group

    for (const m of members) {
      const info = m.info;
      const hb = heartbeats ? heartbeats[m.id] : null;
      memberIds.push(m.id);

      // Connection status
      let isConnected = false;
      if (m.source === 'fix') {
        isConnected = !!(info.trade_connected && info.quote_connected);
      } else if (m.source === 'mt') {
        isConnected = !!info.connected;
      } else if (m.source === 'ea') {
        isConnected = !!(info.online);
      } else {
        isConnected = true;  // Manual accounts treated as connected
      }
      if (isConnected) connectedCount++;

      // Balance
      const rb = info.balance != null ? parseFloat(info.balance) : (hb && hb.balance != null ? parseFloat(hb.balance) : NaN);
      if (!isNaN(rb)) { sumBal += rb; hasBal = true; }
      // Equity
      const re = info.equity != null ? parseFloat(info.equity) : (hb && hb.equity != null ? parseFloat(hb.equity) : NaN);
      if (!isNaN(re)) { sumEq += re; hasEq = true; }
      // PnL
      const rp = info.total_pnl != null ? parseFloat(info.total_pnl) : NaN;
      if (!isNaN(rp)) { sumPnl += rp; hasPnl = true; }
      // Positions
      const rpos = info.positions != null ? parseInt(info.positions) : NaN;
      if (!isNaN(rpos)) { sumPos += rpos; hasPos = true; }
      // Lots
      const rl = info.total_lots != null ? parseFloat(info.total_lots) : NaN;
      if (!isNaN(rl)) { sumLots += rl; hasLots = true; if (rl >= 0) sumPosLots += rl; else sumNegLots += rl; }
      // Swap
      const rs = info.total_swap != null ? parseFloat(info.total_swap) : NaN;
      if (!isNaN(rs)) { sumSwap += rs; hasSwap = true; }
      // Margin Use % — compute per account, take max; also track leverage of that account
      const rawMargin = info.margin != null ? parseFloat(info.margin) : (hb && hb.margin != null ? parseFloat(hb.margin) : null);
      const rawEqMu = !isNaN(re) ? re : 0;
      if (rawEqMu > 0 && rawMargin != null) {
        const mu = (rawMargin / rawEqMu) * 100;
        if (maxMu === null || mu > maxMu) {
          maxMu = mu;
          // Capture leverage of the account with highest margin use
          const acctLev = info.leverage || (hb && hb.leverage) || null;
          maxMuLev = acctLev ? ('1:' + acctLev) : null;
        }
      }
      // Δ Swap
      const sd = swapDelta[m.id];
      if (sd != null) { sumSwapDelta += sd; hasSwapDelta = true; }
      // Age — from direct info or cycle_reminders
      let age = info.oldest_position_age;
      let maxD = info.cycle_max_days;
      let remD = info.cycle_remind_days;
      if (age == null && cycleReminders[m.id]) {
        age = cycleReminders[m.id].days_held;
        maxD = cycleReminders[m.id].max_days;
        remD = cycleReminders[m.id].remind_days;
      }
      if (age != null && !((remD == null || remD === 0 || remD === '') && (maxD == null || maxD === 0 || maxD === ''))) {
        let numAge = Number(age);
        if (!isNaN(numAge)) {
          if (maxAge === null || numAge > maxAge) maxAge = numAge;
        }
      }
      // Optimal Equity & Shift
      const dist = fundDists[m.id] || {};
      if (dist.optimal_equity != null) { sumOptEq += dist.optimal_equity; hasOptEq = true; }
      if (dist.suggested_transfer != null) { sumShift += dist.suggested_transfer; hasShift = true; }
    }

    // Accumulate grand totals
    if (hasBal) { gBal += sumBal; gHasBal = true; }
    if (hasEq) { gEq += sumEq; gHasEq = true; }
    if (hasPnl) { gPnl += sumPnl; gHasPnl = true; }
    if (hasLots) { gLots += sumLots; gHasLots = true; gPosLots += sumPosLots; gNegLots += sumNegLots; }
    if (hasSwap) { gSwap += sumSwap; gHasSwap = true; }
    if (hasSwapDelta) { gSwapDelta += sumSwapDelta; gHasSwapDelta = true; }
    if (maxMu !== null && (gMaxMu === null || maxMu > gMaxMu)) gMaxMu = maxMu;
    if (maxAge !== null && (gMaxAge === null || maxAge > gMaxAge)) gMaxAge = maxAge;
    if (hasOptEq) { gOptEq += sumOptEq; gHasOptEq = true; }
    if (hasShift) { gShift += sumShift; gHasShift = true; }

    // Format values
    const fBal = hasBal ? sumBal.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
    const fEq = hasEq ? sumEq.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
    const fPnl = hasPnl ? sumPnl.toFixed(2) : '-';
    const pnlColor = hasPnl ? (sumPnl >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';
    const fPos = hasPos ? sumPos : '-';
    const fLots = hasLots ? sumLots.toFixed(2) : '-';
    const lotsColor = hasLots ? (sumLots >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';
    const lotsBreak = hasLots ? `<br><span style="font-size:0.7rem;font-weight:400;color:var(--text2);">(${sumPosLots.toFixed(2)} / ${sumNegLots.toFixed(2)})</span>` : '';
    const fMu = maxMu !== null ? maxMu.toFixed(1) + '%' : '-';
    const fSwap = hasSwap ? sumSwap.toFixed(2) : '-';
    const fSwapDelta = hasSwapDelta ? ((sumSwapDelta > 0 ? '+' : '') + sumSwapDelta.toFixed(2)) : '-';
    const sdColor = hasSwapDelta ? (sumSwapDelta >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

    const fOptEq = hasOptEq ? sumOptEq.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
    const fShift = hasShift ? sumShift.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
    const shiftColor = hasShift ? (sumShift >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

    let ageStr = '-';
    let ageStyle = 'font-size:0.82rem;';
    if (maxAge !== null) {
      ageStr = String(maxAge);
      // Check if any member is critical/warning based on their OWN thresholds
      let isRed = false, isOrange = false;
      for (const m of members) {
        let d = m.info.oldest_position_age;
        let md = m.info.cycle_max_days;
        let rd = m.info.cycle_remind_days;
        if (d == null && cycleReminders[m.id]) {
          d = cycleReminders[m.id].days_held;
          md = md != null ? md : cycleReminders[m.id].max_days;
          rd = rd != null ? rd : cycleReminders[m.id].remind_days;
        }
        if (d != null) {
          const numD = Number(d);
          if (md != null && md > 0 && numD >= md) isRed = true;
          else if (rd != null && rd > 0 && numD >= rd) isOrange = true;
        }
      }
      if (isRed) ageStyle += 'font-weight:600;color:var(--red);';
      else if (isOrange) ageStyle += 'font-weight:600;color:var(--orange);';
    }
    const memberCount = members.length;
    const memberList = members.map(m => m.displayName).join('\\n');

    const connTotal = members.length;
    const connColor = connectedCount < connTotal ? 'color:var(--red);font-weight:600;' : 'color:var(--green);';
    const connLabel = `<span style="${connColor}font-size:0.82rem;">${connectedCount}/${connTotal}</span>`;

    // Clickable LOTS cell for group — passes all member account IDs
    const groupMemberIdsStr = memberIds.map(id => encodeURIComponent(id)).join(',');
    const groupLotsCell = (hasLots && fLots !== '-')
      ? `<td style="${lotsColor}"><a href="#" onclick="showGroupLotsBreakdown('${prefix}', '${groupMemberIdsStr}');return false;" style="color:inherit;text-decoration:underline;text-decoration-style:dotted;cursor:pointer;" title="Click to see per-instrument breakdown for this group">${fLots}</a>${lotsBreak}</td>`
      : `<td style="${lotsColor}">${fLots}${lotsBreak}</td>`;

    const groupSwapDeltaCell = (hasSwapDelta && fSwapDelta !== '-')
      ? `<td style="${sdColor};font-size:0.78rem"><a href="#" onclick="showGroupSwapBreakdown('${prefix}', '${groupMemberIdsStr}');return false;" style="color:inherit;text-decoration:underline;text-decoration-style:dotted;cursor:pointer;" title="Click to see per-instrument swap breakdown for this group">${fSwapDelta}</a></td>`
      : `<td style="${sdColor};font-size:0.78rem">${fSwapDelta}</td>`;

    rows.push(`<tr style="background:var(--surface2);border-bottom:1px solid var(--border);">
      <td><strong title="${memberList}">${prefix}</strong> <span style="font-size:0.68rem;color:var(--text2);">(${memberCount})</span></td>
      <td></td><td>${connLabel}</td>
      <td>${fBal}</td>
      <td>${fEq}</td>
      <td>${fOptEq}</td>
      <td style="${shiftColor}">${fShift}</td>
      <td style="${pnlColor}">${fPnl}</td>
      <td>${maxMuLev || '-'}</td>
      <td>${fPos}</td>
      ${groupLotsCell}
      <td>${fMu}</td>
      <td></td>
      <td>${fSwap}</td>
      ${groupSwapDeltaCell}
      <td style="${ageStyle}" title="${maxAge != null ? maxAge + ' rollover days (highest in group)' : ''}">${ageStr}</td>
      <td></td><td></td><td></td><td></td><td></td><td></td>
    </tr>`);
  }

  // TOTALS row (same as normal view)
  const fGBal = gHasBal ? gBal.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
  const fGEq = gHasEq ? gEq.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
  const fGPnl = gHasPnl ? gPnl.toFixed(2) : '-';
  const gPnlStyle = gHasPnl ? (gPnl >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';
  const fGLots = gHasLots ? gLots.toFixed(2) : '-';
  const gLotsStyle = gHasLots ? (gLots >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';
  const gLotsBreak = gHasLots ? `<br><a href="#" onclick="showLotsBreakdown();return false;" style="font-size:0.7rem;font-weight:400;color:#e2e8f0;text-decoration:underline;cursor:pointer;">(${gPosLots.toFixed(2)} / ${gNegLots.toFixed(2)})</a>` : '';
  const fGSwap = gHasSwap ? gSwap.toFixed(2) : '-';
  const fGSwapDelta = gHasSwapDelta ? ((gSwapDelta > 0 ? '+' : '') + gSwapDelta.toFixed(2)) : '-';
  const gSdStyle = gHasSwapDelta ? (gSwapDelta >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

  const fGOptEq = gHasOptEq ? gOptEq.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
  const fGShift = gHasShift ? gShift.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : '-';
  const gShiftStyle = gHasShift ? (gShift >= 0 ? 'color:var(--green)' : 'color:var(--red)') : '';

  const totalsSwapDeltaCell = gHasSwapDelta
    ? `<td style="${gSdStyle};font-size:0.78rem"><a href="#" onclick="showSwapBreakdown();return false;" style="color:inherit;text-decoration:underline;text-decoration-style:dotted;cursor:pointer;" title="Click to see per-instrument swap breakdown (All Accounts)">${fGSwapDelta}</a></td>`
    : `<td style="${gSdStyle};font-size:0.78rem">${fGSwapDelta}</td>`;

  rows.push(`<tr style="border-top:2px solid var(--accent);font-weight:700;background:var(--surface2);">
    <td>TOTALS</td>
    <td></td><td></td>
    <td>${fGBal}</td>
    <td>${fGEq}</td>
    <td>${fGOptEq}</td>
    <td style="${gShiftStyle}">${fGShift}</td>
    <td style="${gPnlStyle}">${fGPnl}</td>
    <td></td><td></td>
    <td style="${gLotsStyle}">${fGLots}${gLotsBreak}</td>
    <td></td><td></td>
    <td>${fGSwap}</td>
    ${totalsSwapDeltaCell}
    <td></td><td></td><td></td><td></td><td></td><td></td><td></td>
  </tr>`);
  tbody.innerHTML = rows.join('');
}

async function showLotsBreakdown() {
  try {
    const res = await fetch('/api/lots_breakdown');
    const data = await res.json();
    if (!data.length) { alert('No position data available'); return; }
    _renderLotsBreakdownModal('Lots Breakdown by Instrument (All Accounts)', data);
  } catch(e) {
    console.error('Lots breakdown error:', e);
  }
}

async function showAccountLotsBreakdown(accountId) {
  try {
    const res = await fetch('/api/lots_breakdown?account=' + encodeURIComponent(accountId));
    const data = await res.json();
    if (!data.length) { alert('No instrument data for ' + accountId); return; }
    _renderLotsBreakdownModal('Lots Breakdown — ' + accountId, data);
  } catch(e) {
    console.error('Account lots breakdown error:', e);
  }
}

async function showGroupLotsBreakdown(groupName, commaSeparatedIds) {
  try {
    const res = await fetch('/api/lots_breakdown?account=' + commaSeparatedIds);
    const data = await res.json();
    if (!data.length) { alert('No instrument data for group ' + groupName); return; }
    _renderLotsBreakdownModal('Lots Breakdown — Group: ' + groupName, data);
  } catch(e) {
    console.error('Group lots breakdown error:', e);
  }
}

async function triggerRecalculateFundDistributions() {
  window.recalcFundDistributions = triggerRecalculateFundDistributions;
  const badge = document.getElementById('fundDistUpdateBadge');
  if (badge) {
    badge.textContent = 'Recalculating...';
  }
  try {
    const res = await fetch('/api/recalculate_fund_distributions', { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') {
      await refreshData();
    } else {
      alert('Recalculation error: ' + (data.error || 'unknown'));
      if (badge) {
        badge.textContent = `Optimal Dist: ${window._fundDistributionsLastUpdated || '-'}`;
      }
    }
  } catch (e) {
    alert('Recalculation failed: ' + e);
    if (badge) {
      badge.textContent = `Optimal Dist: ${window._fundDistributionsLastUpdated || '-'}`;
    }
  }
}

function _renderLotsBreakdownModal(title, data) {
    let html = `<div class="modal-overlay active" id="lotsBreakdownModal" onclick="if(event.target===this)this.remove()">
      <div class="modal" style="min-width:340px;max-width:500px;">
        <h3 style="margin:0 0 12px;font-size:1rem;">${title}</h3>
        <table style="width:100%;border-collapse:collapse;font-size:0.82rem;">
          <thead><tr style="border-bottom:1px solid var(--border);text-align:right;">
            <th style="text-align:left;padding:4px 8px;">Symbol</th>
            <th style="padding:4px 8px;color:var(--green)">Buy</th>
            <th style="padding:4px 8px;color:var(--red)">Sell</th>
            <th style="padding:4px 8px;">Net</th>
          </tr></thead><tbody>`;
    let gBuy = 0, gSell = 0, gNet = 0;
    data.forEach(r => {
      const netColor = r.net >= 0 ? 'var(--green)' : 'var(--red)';
      html += `<tr style="border-bottom:1px solid var(--border);">
        <td style="padding:4px 8px;font-weight:600;">${r.symbol}</td>
        <td style="padding:4px 8px;text-align:right;color:var(--green)">${r.buy.toFixed(2)}</td>
        <td style="padding:4px 8px;text-align:right;color:var(--red)">${r.sell.toFixed(2)}</td>
        <td style="padding:4px 8px;text-align:right;color:${netColor};font-weight:600;">${r.net.toFixed(2)}</td>
      </tr>`;
      gBuy += r.buy; gSell += r.sell; gNet += r.net;
    });
    html += `<tr style="border-top:2px solid var(--accent);font-weight:700;">
      <td style="padding:4px 8px;">TOTAL</td>
      <td style="padding:4px 8px;text-align:right;color:var(--green)">${gBuy.toFixed(2)}</td>
      <td style="padding:4px 8px;text-align:right;color:var(--red)">${gSell.toFixed(2)}</td>
      <td style="padding:4px 8px;text-align:right;color:${gNet >= 0 ? 'var(--green)' : 'var(--red)'}">${gNet.toFixed(2)}</td>
    </tr></tbody></table>
    <div style="text-align:right;margin-top:12px;">
      <button class="btn" onclick="document.getElementById('lotsBreakdownModal').remove()">Close</button>
    </div>
      </div>
    </div>`;
    // Remove existing if any
    const existing = document.getElementById('lotsBreakdownModal');
    if (existing) existing.remove();
    document.body.insertAdjacentHTML('beforeend', html);
}

async function showSwapBreakdown() {
  try {
    const res = await fetch('/api/swap_breakdown');
    const data = await res.json();
    if (!data.length) { alert('No swap delta data available'); return; }
    _renderSwapBreakdownModal('Swap Breakdown by Instrument (All Accounts)', data);
  } catch(e) {
    console.error('Swap breakdown error:', e);
  }
}

async function showAccountSwapBreakdown(accountId) {
  try {
    const res = await fetch('/api/swap_breakdown?account=' + encodeURIComponent(accountId));
    const data = await res.json();
    if (!data.length) { alert('No swap delta data for ' + accountId); return; }
    _renderSwapBreakdownModal('Swap Breakdown — ' + accountId, data);
  } catch(e) {
    console.error('Account swap breakdown error:', e);
  }
}

async function showGroupSwapBreakdown(groupName, commaSeparatedIds) {
  try {
    const res = await fetch('/api/swap_breakdown?account=' + commaSeparatedIds);
    const data = await res.json();
    if (!data.length) { alert('No swap delta data for group ' + groupName); return; }
    _renderSwapBreakdownModal('Swap Breakdown — Group: ' + groupName, data);
  } catch(e) {
    console.error('Group swap breakdown error:', e);
  }
}

function _renderSwapBreakdownModal(title, data) {
    let html = `<div class="modal-overlay active" id="swapBreakdownModal" onclick="if(event.target===this)this.remove()">
      <div class="modal" style="min-width:380px;max-width:550px;">
        <h3 style="margin:0 0 12px;font-size:1rem;">${title}</h3>
        <table style="width:100%;border-collapse:collapse;font-size:0.82rem;">
          <thead><tr style="border-bottom:1px solid var(--border);text-align:right;">
            <th style="text-align:left;padding:4px 8px;">Instrument</th>
            <th style="padding:4px 8px;">Lots</th>
            <th style="padding:4px 8px;">Total Δ Swap</th>
            <th style="padding:4px 8px;">Per Lot Δ Swap</th>
          </tr></thead><tbody>`;
    let gLots = 0, gDeltaSwap = 0;
    data.forEach(r => {
      const dsColor = r.total_delta_swap >= 0 ? 'var(--green)' : 'var(--red)';
      const dsSign = r.total_delta_swap > 0 ? '+' : '';
      const plColor = (r.per_lot_delta_swap !== '-' && r.per_lot_delta_swap >= 0) ? 'var(--green)' : ((r.per_lot_delta_swap !== '-' && r.per_lot_delta_swap < 0) ? 'var(--red)' : 'var(--text2)');
      const plSign = (r.per_lot_delta_swap !== '-' && r.per_lot_delta_swap > 0) ? '+' : '';
      const plVal = r.per_lot_delta_swap !== '-' ? plSign + r.per_lot_delta_swap.toFixed(2) : '-';
      
      html += `<tr style="border-bottom:1px solid var(--border);">
        <td style="padding:4px 8px;font-weight:600;">${r.symbol}</td>
        <td style="padding:4px 8px;text-align:right;">${r.lots.toFixed(2)}</td>
        <td style="padding:4px 8px;text-align:right;color:${dsColor};font-weight:600;">${dsSign}${r.total_delta_swap.toFixed(2)}</td>
        <td style="padding:4px 8px;text-align:right;color:${plColor}">${plVal}</td>
      </tr>`;
      gLots += r.lots;
      gDeltaSwap += r.total_delta_swap;
    });
    const gDsColor = gDeltaSwap >= 0 ? 'var(--green)' : 'var(--red)';
    const gDsSign = gDeltaSwap > 0 ? '+' : '';
    const gPerLot = gLots > 0 ? (gDeltaSwap / gLots) : null;
    const gPlColor = (gPerLot !== null && gPerLot >= 0) ? 'var(--green)' : ((gPerLot !== null && gPerLot < 0) ? 'var(--red)' : 'var(--text2)');
    const gPlSign = (gPerLot !== null && gPerLot > 0) ? '+' : '';
    const gPlVal = gPerLot !== null ? gPlSign + gPerLot.toFixed(2) : '-';

    html += `<tr style="border-top:2px solid var(--accent);font-weight:700;">
      <td style="padding:4px 8px;">TOTAL</td>
      <td style="padding:4px 8px;text-align:right;">${gLots.toFixed(2)}</td>
      <td style="padding:4px 8px;text-align:right;color:${gDsColor}">${gDsSign}${gDeltaSwap.toFixed(2)}</td>
      <td style="padding:4px 8px;text-align:right;color:${gPlColor}">${gPlVal}</td>
    </tr></tbody></table>
    <div style="text-align:right;margin-top:12px;">
      <button class="btn" onclick="document.getElementById('swapBreakdownModal').remove()">Close</button>
    </div>
      </div>
    </div>`;
    // Remove existing if any
    const existing = document.getElementById('swapBreakdownModal');
    if (existing) existing.remove();
    document.body.insertAdjacentHTML('beforeend', html);
}

function showAddAccountModal() {
  document.getElementById('aAcctName').value = '';

  document.getElementById('aGroupLabel').value = '';
  document.getElementById('addAccountModal').classList.add('active');
}
function closeAddAccountModal() {
  document.getElementById('addAccountModal').classList.remove('active');
}
async function addAccount() {
  const name = document.getElementById('aAcctName').value.trim();
  const connType = 'poll';
  const groupLabel = document.getElementById('aGroupLabel').value.trim();
  if (!name) { alert('Account name is required'); return; }
  try {
    const res = await fetch('/api/accounts', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({name, conn_type: connType, group_label: groupLabel})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    closeAddAccountModal();
    refreshData();
  } catch(e) { alert('Failed to add account: ' + e); }
}
async function saveGroupLabel(name, value) {
  try {
    const res = await fetch('/api/accounts/' + encodeURIComponent(name), {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({group_label: value})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); }
  } catch(e) { console.error('Failed to save group:', e); }
}
async function saveAccountField(name, field, value) {
  try {
    const res = await fetch('/api/accounts/' + encodeURIComponent(name), {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[field]: value})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); }
  } catch(e) { console.error(`Failed to save ${field}:`, e); }
}
async function toggleStatsLog(account, enabled) {
  try {
    await fetch('/api/accounts/' + encodeURIComponent(account), {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({stats_log: enabled})
    });
  } catch(e) { console.error(e); }
}

// ─── Quote Stats Report Functions ───────────────────────────
function openRptModal(account) {
  document.getElementById('rptAccount').value = account;
  document.getElementById('rptAccLabel').textContent = account;
  document.getElementById('rptPair').value = '';
  document.getElementById('rptDays').value = '';
  document.getElementById('rptTop').value = '10';
  document.getElementById('rptStatus').textContent = '';
  document.getElementById('rptGenBtn').disabled = false;
  document.getElementById('rptGenBtn').textContent = 'Generate';
  const modal = document.getElementById('rptModal');
  modal.style.display = 'flex';
  loadReportList();
}
function closeRptModal() {
  document.getElementById('rptModal').style.display = 'none';
}

async function generateReport() {
  const btn = document.getElementById('rptGenBtn');
  const status = document.getElementById('rptStatus');
  const account = document.getElementById('rptAccount').value.trim();
  const pair = document.getElementById('rptPair').value.trim();
  const days = document.getElementById('rptDays').value.trim();
  const top = document.getElementById('rptTop').value.trim();

  const payload = {};
  if (account) payload.account = account;
  if (pair) payload.pair = pair;
  if (days) payload.days = parseInt(days);
  if (top) payload.top = parseInt(top);

  btn.disabled = true;
  btn.textContent = '⏳ ...';
  status.textContent = 'Starting...';
  status.style.color = 'var(--text2)';

  try {
    const res = await fetch('/api/reports/generate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (!data.ok) {
      status.textContent = 'Error: ' + (data.error || 'Unknown');
      status.style.color = 'var(--red)';
      btn.disabled = false;
      btn.textContent = 'Generate';
      return;
    }
    const jobId = data.job_id;
    const poll = setInterval(async () => {
      try {
        const jr = await fetch('/api/reports/status/' + jobId);
        const job = await jr.json();
        if (job.status === 'done') {
          clearInterval(poll);
          status.textContent = '✓ Ready!';
          status.style.color = 'var(--green)';
          btn.disabled = false;
          btn.textContent = 'Generate';
          loadReportList();
          if (job.filename) {
            window.open('/api/reports/' + job.filename, '_blank');
          }
        } else if (job.status === 'error') {
          clearInterval(poll);
          status.textContent = '✗ ' + (job.error || 'Failed').substring(0, 80);
          status.style.color = 'var(--red)';
          btn.disabled = false;
          btn.textContent = 'Generate';
        } else {
          status.textContent = '⏳ Generating...';
        }
      } catch(e) {
        clearInterval(poll);
        status.textContent = '✗ Poll error';
        status.style.color = 'var(--red)';
        btn.disabled = false;
        btn.textContent = 'Generate';
      }
    }, 1000);
  } catch(e) {
    status.textContent = '✗ ' + e;
    status.style.color = 'var(--red)';
    btn.disabled = false;
    btn.textContent = 'Generate';
  }
}

async function loadReportList() {
  const el = document.getElementById('rptList');
  try {
    const res = await fetch('/api/reports');
    const reports = await res.json();
    if (!reports.length) {
      el.innerHTML = '<span style="color:var(--text2)">No reports yet.</span>';
      return;
    }
    el.innerHTML = reports.slice(0, 8).map(r => `<div style="display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-bottom:1px solid var(--border);">
      <a href="/api/reports/${r.filename}" target="_blank" style="color:var(--accent);text-decoration:underline;font-size:0.78rem;">${r.filename}</a>
      <span style="font-size:0.72rem;color:var(--text2);">${r.created} &nbsp; <button class="btn" style="padding:0 4px;font-size:0.68rem;" onclick="deleteReport('${r.filename}')">✕</button></span>
    </div>`).join('');
  } catch(e) {
    el.innerHTML = '<span style="color:var(--red)">Failed to load reports</span>';
  }
}

async function deleteReport(filename) {
  if (!confirm('Delete report ' + filename + '?')) return;
  try {
    await fetch('/api/reports/' + filename, {method: 'DELETE'});
    loadReportList();
  } catch(e) { console.error(e); }
}

async function saveFixGroupLabel(id, value) {
  try {
    const res = await fetch('/api/fix_accounts/' + encodeURIComponent(id), {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({group_label: value})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); }
  } catch(e) { console.error('Failed to save FIX group label:', e); }
}

async function saveFixMarginAlert(id, value) {
  try {
    const val = value === '' ? null : parseFloat(value);
    const res = await fetch('/api/fix_accounts/' + encodeURIComponent(id), {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({margin_alert_threshold: val})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); }
    // Update local cache so next render reflects the change
    if (window._marginAlertData) {
      if (val) { window._marginAlertData.per_account[id] = val; }
      else { delete window._marginAlertData.per_account[id]; }
    }
  } catch(e) { console.error('Failed to save margin alert:', e); }
}

async function saveMarginAlert(id, value) {
  try {
    const val = value === '' ? null : parseFloat(value);
    const res = await fetch('/api/accounts/' + encodeURIComponent(id), {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({margin_alert_threshold: val})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); }
    if (window._marginAlertData) {
      if (val) { window._marginAlertData.per_account[id] = val; }
      else { delete window._marginAlertData.per_account[id]; }
    }
  } catch(e) { console.error('Failed to save margin alert:', e); }
}

async function deleteAccount(name) {
  if (!confirm('Remove account "' + name + '"?')) return;
  try {
    const res = await fetch('/api/accounts/' + encodeURIComponent(name), {method:'DELETE'});
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    refreshData();
  } catch(e) { alert('Failed to delete account: ' + e); }
}
async function reconnectFixAccount(id) {
  try {
    await fetch('/api/fix_accounts/' + encodeURIComponent(id) + '/reconnect', {method:'POST'});
    refreshData();
  } catch(e) { alert('Failed: ' + e); }
}
async function disconnectFixAccount(id) {
  if (!confirm('Disconnect FIX account "' + id + '"? The account will remain configured.')) return;
  try {
    await fetch('/api/fix_accounts/' + encodeURIComponent(id) + '/disconnect', {method:'POST'});
    refreshData();
  } catch(e) { alert('Failed: ' + e); }
}
async function deleteFixAccount(id) {
  if (!confirm('Remove FIX account "' + id + '"? This will disconnect and delete the configuration.')) return;
  try {
    await fetch('/api/fix_accounts/' + encodeURIComponent(id), {method:'DELETE'});
    refreshData();
  } catch(e) { alert('Failed: ' + e); }
}

// -- Toggle FIX vs Open API fields in modals --------------------------------
function toggleFixFields(prefix) {
  // prefix is 'fx' (add modal) or 'efx' (edit modal)
  const impl = document.getElementById(prefix + 'Impl').value;
  const isOpenApi = (impl === 'openapi');
  const isSwissquote = (impl === 'swissquote');
  const isDukascopy = (impl === 'dukascopy');
  // FIX-specific field IDs to hide when openapi is selected
  const fixFieldIds = ['Host', 'TradePort', 'SenderCompId',
                       'TargetCompId', 'Username', 'Password', 'Heartbeat',
                       'UseSSL', 'SymbolFile'];
  fixFieldIds.forEach(fid => {
    const el = document.getElementById(prefix + fid);
    if (el) {
      const group = el.closest('.form-group');
      if (group) group.style.display = isOpenApi ? 'none' : '';
    }
  });
  // Quote Port: hide for openapi AND swissquote (single connection), show for dukascopy
  const qpEl = document.getElementById(prefix + 'QuotePort');
  if (qpEl) {
    const qpGroup = qpEl.closest('.form-group');
    if (qpGroup) qpGroup.style.display = (isOpenApi || isSwissquote) ? 'none' : '';
  }
  // Symbol File: hide for swissquote and dukascopy (symbols are text-based)
  const sfEl = document.getElementById(prefix + 'SymbolFile');
  if (sfEl) {
    const sfGroup = sfEl.closest('.form-group');
    if (sfGroup) sfGroup.style.display = (isOpenApi || isSwissquote || isDukascopy) ? 'none' : '';
  }
  // SenderCompID (Quote): only show for dukascopy
  const scqGroup = document.getElementById(prefix + 'SenderCompIdQuoteGroup');
  if (scqGroup) scqGroup.style.display = isDukascopy ? '' : 'none';
  // Auto-open the Open API details section when openapi is selected
  const modal = document.getElementById(prefix === 'fx' ? 'addFixAccountModal' : 'editFixAccountModal');
  if (modal) {
    const details = modal.querySelector('details');
    if (details) {
      // Open API section: show for openapi, hide for others
      details.open = isOpenApi;
      details.style.display = isOpenApi ? '' : ((isSwissquote || isDukascopy) ? 'none' : '');
    }
  }
}

// -- cTrader OAuth authorize from modal ------------------------------------
async function oaAuthorize(prefix) {
  const clientId = document.getElementById(prefix + 'OaClientId').value.trim();
  const clientSecret = document.getElementById(prefix + 'OaClientSecret').value.trim();
  if (!clientId) { alert('Please enter Client ID first'); return; }
  if (!clientSecret) { alert('Please enter Client Secret first'); return; }
  try {
    const res = await fetch('/api/ctrader_oauth_url', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ client_id: clientId, client_secret: clientSecret })
    });
    const data = await res.json();
    if (data.error) { alert('Error: ' + data.error); return; }
    if (data.url) {
      window.open(data.url, '_blank');
      alert('A cTrader authorization page has opened in a new tab.\\n\\n' +
            '1. Log in and authorize the app\\n' +
            '2. You will be redirected back here with your tokens\\n' +
            '3. Copy the Access Token and Refresh Token into the fields below');
    }
  } catch(e) { alert('OAuth URL generation failed: ' + e); }
}

// -- Edit FIX Account modal -----------------------------------------------
async function editFixAccount(id) {
  try {
    const res = await fetch('/api/fix_accounts/' + encodeURIComponent(id) + '/config');
    const cfg = await res.json();
    if (cfg.error) { alert(cfg.error); return; }
    document.getElementById('efxAcctId').value = id;
    document.getElementById('efxAcctIdDisplay').value = id;
    document.getElementById('efxImpl').value = cfg.implementation || 'ctrader';
    document.getElementById('efxLabel').value = cfg.label || '';
    document.getElementById('efxGroupLabel').value = cfg.group_label || '';
    document.getElementById('efxHost').value = cfg.host || '';
    document.getElementById('efxTradePort').value = cfg.trade_port || 5202;
    document.getElementById('efxQuotePort').value = cfg.quote_port || 5201;
    document.getElementById('efxSenderCompId').value = cfg.sender_comp_id || '';
    document.getElementById('efxSenderCompIdQuote').value = cfg.sender_comp_id_quote || '';
    document.getElementById('efxTargetCompId').value = cfg.target_comp_id || 'cServer';
    document.getElementById('efxUsername').value = cfg.username || '';
    document.getElementById('efxPassword').value = cfg.password || '';
    document.getElementById('efxHeartbeat').value = cfg.heartbeat_interval || 30;
    document.getElementById('efxLotMult').value = cfg.lot_multiplier || 100000;
    document.getElementById('efxLeverage').value = cfg.leverage || '';
    document.getElementById('efxUseSSL').value = cfg.use_ssl ? 'true' : 'false';
    document.getElementById('efxSymbolFile').value = cfg.symbol_file || '';
    document.getElementById('efxOaClientId').value = cfg.openapi_client_id || '';
    document.getElementById('efxOaClientSecret').value = cfg.openapi_client_secret || '';
    document.getElementById('efxOaAccessToken').value = cfg.openapi_access_token || '';
    document.getElementById('efxOaRefreshToken').value = cfg.openapi_refresh_token || '';
    document.getElementById('efxOaAccountId').value = cfg.openapi_account_id || '';
    document.getElementById('efxOaEnv').value = cfg.openapi_environment || 'demo';
    document.getElementById('efxAutoConnect').checked = cfg.auto_connect_start !== false;
    document.getElementById('efxCycleReminder').checked = !!cfg.cycle_reminder_enabled;
    document.getElementById('efxCycleRemindDays').value = cfg.cycle_reminder_days != null ? cfg.cycle_reminder_days : '';
    document.getElementById('efxCycleMaxDays').value = cfg.cycle_max_days != null ? cfg.cycle_max_days : '';
    document.getElementById('efxAutoCycle').checked = !!cfg.auto_cycle_enabled;
    document.getElementById('efxAlertEmails').value = cfg.alert_email || '';
    document.getElementById('efxAlertTelegramIds').value = cfg.alert_telegram || '';
    document.getElementById('efxStopOutLevel').value = cfg.stop_out_level != null ? cfg.stop_out_level : '';
    document.getElementById('editFixAccountModal').classList.add('active');
    toggleFixFields('efx');
  } catch(e) { alert('Failed to load config: ' + e); }
}
function closeEditFixAccountModal() {
  document.getElementById('editFixAccountModal').classList.remove('active');
}
async function saveFixAccountEdit() {
  const id = document.getElementById('efxAcctId').value;
  const host = document.getElementById('efxHost').value.trim();
  if (!host) { alert('Host is required'); return; }
  const payload = {
    label: document.getElementById('efxLabel').value.trim(),
    group_label: document.getElementById('efxGroupLabel').value.trim(),
    host: host,
    trade_port: parseInt(document.getElementById('efxTradePort').value) || 5202,
    quote_port: parseInt(document.getElementById('efxQuotePort').value) || 5201,
    sender_comp_id: document.getElementById('efxSenderCompId').value.trim(),
    sender_comp_id_quote: document.getElementById('efxSenderCompIdQuote').value.trim() || undefined,
    target_comp_id: document.getElementById('efxTargetCompId').value.trim() || 'cServer',
    username: document.getElementById('efxUsername').value.trim(),
    password: document.getElementById('efxPassword').value,
    heartbeat_interval: parseInt(document.getElementById('efxHeartbeat').value) || 30,
    lot_multiplier: parseInt(document.getElementById('efxLotMult').value) || 100000,
    leverage: parseInt(document.getElementById('efxLeverage').value) || undefined,
    use_ssl: document.getElementById('efxUseSSL').value === 'true',
    implementation: document.getElementById('efxImpl').value,
    symbol_file: document.getElementById('efxSymbolFile').value.trim() || undefined,
    openapi_client_id: document.getElementById('efxOaClientId').value.trim() || undefined,
    openapi_client_secret: document.getElementById('efxOaClientSecret').value.trim() || undefined,
    openapi_access_token: document.getElementById('efxOaAccessToken').value.trim() || undefined,
    openapi_refresh_token: document.getElementById('efxOaRefreshToken').value.trim() || undefined,
    openapi_account_id: document.getElementById('efxOaAccountId').value.trim() || undefined,
    openapi_environment: document.getElementById('efxOaEnv').value,
    auto_connect_start: document.getElementById('efxAutoConnect').checked,
    cycle_reminder_enabled: document.getElementById('efxCycleReminder').checked,
    cycle_reminder_days: document.getElementById('efxCycleRemindDays').value.trim() !== '' ? parseInt(document.getElementById('efxCycleRemindDays').value) : null,
    cycle_max_days: document.getElementById('efxCycleMaxDays').value.trim() !== '' ? parseInt(document.getElementById('efxCycleMaxDays').value) : null,
    auto_cycle_enabled: document.getElementById('efxAutoCycle').checked,
    alert_email: document.getElementById('efxAlertEmails').value.trim() || null,
    alert_telegram: document.getElementById('efxAlertTelegramIds').value.trim() || null,
    stop_out_level: document.getElementById('efxStopOutLevel').value.trim() !== '' ? parseFloat(document.getElementById('efxStopOutLevel').value) : null
  };
  try {
    const res = await fetch('/api/fix_accounts/' + encodeURIComponent(id), {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    closeEditFixAccountModal();
    refreshData();
  } catch(e) { alert('Failed to update FIX account: ' + e); }
}

// ── Add FIX Account modal ───────────────────────────────────────────────
function showAddFixAccountModal() {
  document.getElementById('fxImpl').value = 'ctrader';
  document.getElementById('fxAcctId').value = '';
  document.getElementById('fxLabel').value = '';
  document.getElementById('fxGroupLabel').value = '';
  document.getElementById('fxHost').value = '';
  document.getElementById('fxTradePort').value = '5202';
  document.getElementById('fxQuotePort').value = '5201';
  document.getElementById('fxSenderCompId').value = '';
  document.getElementById('fxSenderCompIdQuote').value = '';
  document.getElementById('fxTargetCompId').value = 'cServer';
  document.getElementById('fxUsername').value = '';
  document.getElementById('fxPassword').value = '';
  document.getElementById('fxHeartbeat').value = '30';
  document.getElementById('fxLotMult').value = '100000';
  document.getElementById('fxLeverage').value = '';
  document.getElementById('fxUseSSL').value = 'false';
  document.getElementById('fxSymbolFile').value = '';
  document.getElementById('fxAlertEmails').value = '';
  document.getElementById('fxAlertTelegramIds').value = '';
  document.getElementById('fxStopOutLevel').value = '';
  document.getElementById('addFixAccountModal').classList.add('active');
}
function closeAddFixAccountModal() {
  document.getElementById('addFixAccountModal').classList.remove('active');
}
async function addFixAccount(autoConnect = true) {
  const id = document.getElementById('fxAcctId').value.trim();
  if (!id) { alert('Account ID is required'); return; }
  const host = document.getElementById('fxHost').value.trim();
  const impl = document.getElementById('fxImpl').value;
  if (!host && impl !== 'openapi') { alert('Host is required'); return; }
  const payload = {
    account_id: id,
    label: document.getElementById('fxLabel').value.trim(),
    group_label: document.getElementById('fxGroupLabel').value.trim(),
    host: host || 'openapi',
    trade_port: parseInt(document.getElementById('fxTradePort').value) || 5202,
    quote_port: parseInt(document.getElementById('fxQuotePort').value) || 5201,
    sender_comp_id: document.getElementById('fxSenderCompId').value.trim() || (impl === 'openapi' ? 'openapi' : ''),
    sender_comp_id_quote: document.getElementById('fxSenderCompIdQuote').value.trim() || undefined,
    target_comp_id: document.getElementById('fxTargetCompId').value.trim() || 'cServer',
    username: document.getElementById('fxUsername').value.trim(),
    password: document.getElementById('fxPassword').value,
    heartbeat_interval: parseInt(document.getElementById('fxHeartbeat').value) || 30,
    lot_multiplier: parseInt(document.getElementById('fxLotMult').value) || 100000,
    leverage: parseInt(document.getElementById('fxLeverage').value) || undefined,
    use_ssl: document.getElementById('fxUseSSL').value === 'true',
    implementation: impl,
    symbol_file: document.getElementById('fxSymbolFile').value.trim() || undefined,
    openapi_client_id: document.getElementById('fxOaClientId').value.trim() || undefined,
    openapi_client_secret: document.getElementById('fxOaClientSecret').value.trim() || undefined,
    openapi_access_token: document.getElementById('fxOaAccessToken').value.trim() || undefined,
    openapi_refresh_token: document.getElementById('fxOaRefreshToken').value.trim() || undefined,
    openapi_account_id: document.getElementById('fxOaAccountId').value.trim() || undefined,
    openapi_environment: document.getElementById('fxOaEnv').value,
    auto_connect_start: document.getElementById('fxAutoConnect').checked,
    cycle_reminder_enabled: document.getElementById('fxCycleReminder').checked,
    cycle_reminder_days: document.getElementById('fxCycleRemindDays').value.trim() !== '' ? parseInt(document.getElementById('fxCycleRemindDays').value) : null,
    cycle_max_days: document.getElementById('fxCycleMaxDays').value.trim() !== '' ? parseInt(document.getElementById('fxCycleMaxDays').value) : null,
    auto_cycle_enabled: document.getElementById('fxAutoCycle').checked,
    auto_connect: autoConnect,
    alert_email: document.getElementById('fxAlertEmails').value.trim() || null,
    alert_telegram: document.getElementById('fxAlertTelegramIds').value.trim() || null,
    stop_out_level: document.getElementById('fxStopOutLevel').value.trim() !== '' ? parseFloat(document.getElementById('fxStopOutLevel').value) : null
  };
  try {
    const res = await fetch('/api/fix_accounts', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    closeAddFixAccountModal();
    refreshData();
  } catch(e) { alert('Failed to add FIX account: ' + e); }
}
// ── EA/Manual Account edit ──────────────────────────────────────────────
function toggleEADirectFields() {
  const ct = document.getElementById('eeaConnType').value;
  const directFields = document.getElementById('eeaDirectFields');
  directFields.style.display = (ct === 'mt4_direct' || ct === 'mt5_direct') ? 'block' : 'none';
}
function editEAAccount(name) {
  const info = manual_accounts_cache[name] || {};
  const eaInfo = ea_heartbeats_cache[name] || {};
  const mtInfo = mt_direct_accounts_cache[name] || null;
  document.getElementById('eeaAcctName').value = name;

  // Determine current conn_type
  let connType = info.conn_type || 'poll';
  if (mtInfo) {
    connType = mtInfo.type === 'mt5_direct' ? 'mt5_direct' : 'mt4_direct';
  }
  document.getElementById('eeaConnType').value = connType;
  document.getElementById('eeaGroupLabel').value = info.group_label || eaInfo.group_label || '';
  document.getElementById('eeaFeeThreshold').value = info.fee_threshold || '';
  document.getElementById('eeaStopOutLevel').value = info.stop_out_level != null ? info.stop_out_level : '';

  // Pre-populate Direct fields if this account is already an MT Direct account
  if (mtInfo) {
    // Fetch current config from API
    fetch('/api/mt_direct_accounts/' + encodeURIComponent(name) + '/config')
      .then(r => r.json())
      .then(cfg => {
        document.getElementById('eeaLogin').value = cfg.login || '';
        document.getElementById('eeaPassword').value = cfg.password || '';
        document.getElementById('eeaServer').value = cfg.server || '';
        document.getElementById('eeaPort').value = cfg.port || 443;
        document.getElementById('eeaSlippage').value = cfg.slippage || 3;
        document.getElementById('eeaDirectLabel').value = cfg.label || '';
        document.getElementById('eeaAlertEmails').value = cfg.alert_email || info.alert_email || '';
        document.getElementById('eeaAlertTelegramIds').value = cfg.alert_telegram || info.alert_telegram || '';
        document.getElementById('eeaStopOutLevel').value = cfg.stop_out_level != null ? cfg.stop_out_level : (info.stop_out_level != null ? info.stop_out_level : '');
      })
      .catch(() => {});
  } else {
    // Default values for a new switch
    document.getElementById('eeaLogin').value = name; // Use account name as login default
    document.getElementById('eeaPassword').value = '';
    document.getElementById('eeaServer').value = '';
    document.getElementById('eeaPort').value = '443';
    document.getElementById('eeaSlippage').value = '3';
  }

  document.getElementById('eeaAlertEmails').value = info.alert_email || '';
  document.getElementById('eeaAlertTelegramIds').value = info.alert_telegram || '';
  toggleEADirectFields();
  document.getElementById('editEAAccountModal').classList.add('active');
}
function closeEditEAAccountModal() {
  document.getElementById('editEAAccountModal').classList.remove('active');
}
async function saveEAAccountEdit() {
  const name = document.getElementById('eeaAcctName').value;
  const connType = document.getElementById('eeaConnType').value;
  const stopOut = document.getElementById('eeaStopOutLevel').value.trim();
  const payload = {
    conn_type: connType,
    group_label: document.getElementById('eeaGroupLabel').value.trim(),
    alert_email: document.getElementById('eeaAlertEmails').value.trim() || null,
    alert_telegram: document.getElementById('eeaAlertTelegramIds').value.trim() || null,
    stop_out_level: stopOut !== '' ? parseFloat(stopOut) : null
  };
  const fee = document.getElementById('eeaFeeThreshold').value.trim();
  if (fee) payload.fee_threshold = parseFloat(fee);

  // If switching to MT Direct, include credentials
  if (connType === 'mt4_direct' || connType === 'mt5_direct') {
    const server = document.getElementById('eeaServer').value.trim();
    const login = document.getElementById('eeaLogin').value.trim();
    const password = document.getElementById('eeaPassword').value;
    if (!server || !login || !password || password === '********') {
      if (!mt_direct_accounts_cache[name]) {
        alert('Server, Login, and Password are required for MT Direct');
        return;
      }
    }
    payload.mt_direct = {
      type: connType === 'mt5_direct' ? 'mt5' : 'mt4',
      login: login,
      password: password,
      server: server,
      port: parseInt(document.getElementById('eeaPort').value) || 443,
      slippage: parseInt(document.getElementById('eeaSlippage').value) || 3,
      label: document.getElementById('eeaDirectLabel').value.trim(),
      alert_email: document.getElementById('eeaAlertEmails').value.trim() || null,
      alert_telegram: document.getElementById('eeaAlertTelegramIds').value.trim() || null,
      stop_out_level: stopOut !== '' ? parseFloat(stopOut) : null
    };
  }

  try {
    const res = await fetch('/api/accounts/' + encodeURIComponent(name), {
      method: 'PATCH',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    closeEditEAAccountModal();
    refreshData();
  } catch(e) { alert('Failed to update account: ' + e); }
}

// ── MT Direct Account functions ─────────────────────────────────────────
function togglePwdVis(inputId, btn) {
  var inp = document.getElementById(inputId);
  if (!inp) return;
  if (inp.type === 'password') { inp.type = 'text'; btn.innerHTML = '&#x1F512;'; }
  else { inp.type = 'password'; btn.innerHTML = '&#x1F441;'; }
}

function showAddMTDirectModal() {
  document.getElementById('mtdType').value = 'mt4';
  document.getElementById('mtdAcctId').value = '';
  document.getElementById('mtdLogin').value = '';
  document.getElementById('mtdPassword').value = '';
  document.getElementById('mtdServer').value = '';
  document.getElementById('mtdPort').value = '443';
  document.getElementById('mtdSlippage').value = '3';
  document.getElementById('mtdAlertEmails').value = '';
  document.getElementById('mtdAlertTelegramIds').value = '';
  document.getElementById('mtdStopOutLevel').value = '';
  document.getElementById('addMTDirectModal').classList.add('active');
}
function closeAddMTDirectModal() {
  document.getElementById('addMTDirectModal').classList.remove('active');
}
async function addMTDirectAccount(autoConnect = true) {
  const id = document.getElementById('mtdAcctId').value.trim();
  if (!id) { alert('Account ID is required'); return; }
  const server = document.getElementById('mtdServer').value.trim();
  if (!server) { alert('Server is required'); return; }
  const login = document.getElementById('mtdLogin').value.trim();
  if (!login) { alert('Login is required'); return; }
  const password = document.getElementById('mtdPassword').value;
  if (!password) { alert('Password is required'); return; }
  const payload = {
    account_id: id,
    type: document.getElementById('mtdType').value,
    login: login,
    password: password,
    server: server,
    port: parseInt(document.getElementById('mtdPort').value) || 443,
    auto_connect_start: document.getElementById('mtdAutoConnect').checked,
    cycle_reminder_enabled: document.getElementById('mtdCycleReminder').checked,
    cycle_reminder_days: document.getElementById('mtdCycleRemindDays').value.trim() !== '' ? parseInt(document.getElementById('mtdCycleRemindDays').value) : null,
    cycle_max_days: document.getElementById('mtdCycleMaxDays').value.trim() !== '' ? parseInt(document.getElementById('mtdCycleMaxDays').value) : null,
    auto_cycle_enabled: document.getElementById('mtdAutoCycle').checked,
    label: id,
    slippage: parseInt(document.getElementById('mtdSlippage').value) || 3,
    auto_connect: autoConnect,
    alert_email: document.getElementById('mtdAlertEmails').value.trim() || null,
    alert_telegram: document.getElementById('mtdAlertTelegramIds').value.trim() || null,
    stop_out_level: document.getElementById('mtdStopOutLevel').value.trim() !== '' ? parseFloat(document.getElementById('mtdStopOutLevel').value) : null
  };
  try {
    const res = await fetch('/api/mt_direct_accounts', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    closeAddMTDirectModal();
    refreshData();
  } catch(e) { alert('Failed to add MT Direct account: ' + e); }
}
async function connectMTDirect(id) {
  // Pause background timer during connect — MT5 connect takes several seconds
  // and the interval firing mid-connect would read an intermediate PnL=0 state.
  if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = null; }
  try {
    const res = await fetch('/api/mt_direct_accounts/' + encodeURIComponent(id) + '/connect', {method:'POST'});
    const data = await res.json();
    if (data.error) { alert('Connection failed: ' + data.error); }
    await refreshData();
  } finally {
    // Always restart the timer, even if connect threw
    startRefreshLoop();
  }
}
async function disconnectMTDirect(id) {
  await fetch('/api/mt_direct_accounts/' + encodeURIComponent(id) + '/disconnect', {method:'POST'});
  refreshData();
}
async function deleteMTDirect(id) {
  if (!confirm('Delete MT Direct account ' + id + '?')) return;
  await fetch('/api/mt_direct_accounts/' + encodeURIComponent(id), {method:'DELETE'});
  refreshData();
}
async function editMTDirect(id) {
  try {
    const res = await fetch('/api/mt_direct_accounts/' + encodeURIComponent(id) + '/config');
    const cfg = await res.json();
    if (cfg.error) { alert(cfg.error); return; }
    document.getElementById('emtdAcctId').value = id;
    document.getElementById('emtdType').value = cfg.type || 'mt4';
    document.getElementById('emtdLogin').value = cfg.login || '';
    document.getElementById('emtdPassword').value = cfg.password || '';
    document.getElementById('emtdServer').value = cfg.server || '';
    document.getElementById('emtdPort').value = cfg.port || 443;
    document.getElementById('emtdLabel').value = cfg.label || '';
    document.getElementById('emtdSlippage').value = cfg.slippage || 3;
    document.getElementById('emtdMagic').value = cfg.magic_number || 777888;
    document.getElementById('emtdAutoConnect').checked = cfg.auto_connect_start !== false;
    document.getElementById('emtdCycleReminder').checked = !!cfg.cycle_reminder_enabled;
    document.getElementById('emtdCycleRemindDays').value = cfg.cycle_reminder_days != null ? cfg.cycle_reminder_days : '';
    document.getElementById('emtdCycleMaxDays').value = cfg.cycle_max_days != null ? cfg.cycle_max_days : '';
    document.getElementById('emtdAutoCycle').checked = !!cfg.auto_cycle_enabled;
    document.getElementById('emtdAlertEmails').value = cfg.alert_email || '';
    document.getElementById('emtdAlertTelegramIds').value = cfg.alert_telegram || '';
    document.getElementById('emtdStopOutLevel').value = cfg.stop_out_level != null ? cfg.stop_out_level : '';
    document.getElementById('editMTDirectModal').classList.add('active');
  } catch(e) { alert('Failed to load MT Direct config: ' + e); }
}
function closeEditMTDirectModal() {
  document.getElementById('editMTDirectModal').classList.remove('active');
}
async function saveMTDirectEdit() {
  const id = document.getElementById('emtdAcctId').value;
  const payload = {
    type: document.getElementById('emtdType').value,
    login: document.getElementById('emtdLogin').value.trim(),
    password: document.getElementById('emtdPassword').value,
    server: document.getElementById('emtdServer').value.trim(),
    port: parseInt(document.getElementById('emtdPort').value) || 443,
    label: document.getElementById('emtdLabel').value.trim(),
    slippage: parseInt(document.getElementById('emtdSlippage').value) || 3,
    magic_number: parseInt(document.getElementById('emtdMagic').value) || 777888,
    auto_connect_start: document.getElementById('emtdAutoConnect').checked,
    cycle_reminder_enabled: document.getElementById('emtdCycleReminder').checked,
    cycle_reminder_days: document.getElementById('emtdCycleRemindDays').value.trim() !== '' ? parseInt(document.getElementById('emtdCycleRemindDays').value) : null,
    cycle_max_days: document.getElementById('emtdCycleMaxDays').value.trim() !== '' ? parseInt(document.getElementById('emtdCycleMaxDays').value) : null,
    auto_cycle_enabled: document.getElementById('emtdAutoCycle').checked,
    alert_email: document.getElementById('emtdAlertEmails').value.trim() || null,
    alert_telegram: document.getElementById('emtdAlertTelegramIds').value.trim() || null,
    stop_out_level: document.getElementById('emtdStopOutLevel').value.trim() !== '' ? parseFloat(document.getElementById('emtdStopOutLevel').value) : null
  };
  try {
    const res = await fetch('/api/mt_direct_accounts/' + encodeURIComponent(id), {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    closeEditMTDirectModal();
    refreshData();
  } catch(e) { alert('Failed to update MT Direct account: ' + e); }
}

function startRefreshLoop() {
  if (refreshTimer) clearInterval(refreshTimer);
  const sec = Math.max(1, parseInt(document.getElementById('refreshInterval').value) || 2);
  refreshTimer = setInterval(refreshData, sec * 1000);
}

document.getElementById('refreshInterval').addEventListener('change', startRefreshLoop);

// Pop-out window support: detect ?strategy_id= parameter BEFORE first data load
(function checkPopoutParam() {
  const params = new URLSearchParams(window.location.search);
  const popoutStratId = params.get('strategy_id');
  if (popoutStratId) {
    document.title = 'Strategy \u2014 Loading...';
    // ─── Save window geometry on resize/move/close ───
    function savePopoutGeo() {
      const geo = { w: window.outerWidth, h: window.outerHeight, x: window.screenX, y: window.screenY };
      localStorage.setItem('popoutGeo_' + popoutStratId, JSON.stringify(geo));
    }
    window.addEventListener('resize', savePopoutGeo);
    // Browsers don't fire 'move' events, so we poll for position changes
    let lastX = window.screenX, lastY = window.screenY;
    setInterval(function() {
      if (window.screenX !== lastX || window.screenY !== lastY) {
        lastX = window.screenX; lastY = window.screenY;
        savePopoutGeo();
      }
    }, 500);
    window.addEventListener('beforeunload', savePopoutGeo);

    const origRefresh = refreshData;
    let firstLoad = true;
    refreshData = async function() {
      await origRefresh();
      if (firstLoad) {
        firstLoad = false;
        // Hide header, main tab-nav, footer — but NOT strategy sub-tab nav
        document.querySelectorAll('.header, .tab-nav:not(#stratTabNav), .refresh-bar-wrap').forEach(el => el.style.display = 'none');
        // Hide only main dashboard tab panels (not strategy sub-tabs)
        document.querySelectorAll('.tab-panel[id^="tab-"]').forEach(p => p.style.display = 'none');
        // Auto-open the strategy
        editStrategy(popoutStratId);
        // Restyle the modal to act as inline content (not overlay)
        const modal = document.getElementById('editStrategyModal');
        if (modal) {
          modal.style.position = 'relative';
          modal.style.background = 'transparent';
          modal.style.padding = '0';
          modal.style.display = 'block';
          modal.classList.add('active');
          const innerModal = modal.querySelector('.modal');
          if (innerModal) {
            innerModal.style.maxWidth = '100%';
            innerModal.style.width = '100%';
            innerModal.style.maxHeight = 'none';
            innerModal.style.borderRadius = '0';
            innerModal.style.boxShadow = 'none';
            innerModal.style.padding = '2px 4px 4px';
            innerModal.style.border = 'none';
          }
        }
        // Remove container padding in pop-out mode
        const container = document.querySelector('.container');
        if (container) container.style.padding = '0';
        const strat = strategies_cache.find(s => s.id === popoutStratId);
        if (strat) document.title = 'Strategy \u2014 ' + strat.name;
        // Restore saved strategy sub-tab
        const savedStratTab = localStorage.getItem('activeStratTab');
        if (savedStratTab) switchStratTab(savedStratTab);
        // Restore saved positions sub-tab
        const savedPosTab = localStorage.getItem('activePosTab');
        if (savedPosTab) switchPosTab(savedPosTab);
      }
      // Always re-render instruments if this strategy is open
      if (currentStrategyId === popoutStratId) {
        renderInstrumentsTable();
      }
    };
  }
})();

// Init
refreshData().then(() => applyColVisibility());
startRefreshLoop();

// Close modals on click outside
document.getElementById('editModal').addEventListener('click', function(e) {
  if (e.target === this) closeModal();
});
document.getElementById('newStrategyModal').addEventListener('click', function(e) {
  if (e.target === this) closeNewStrategyModal();
});
document.getElementById('editStrategyModal').addEventListener('click', function(e) {
  if (e.target === this) closeEditStrategyModal();
});
document.getElementById('newInstrumentModal').addEventListener('click', function(e) {
  if (e.target === this) closeNewInstrumentModal();
});
document.getElementById('addAccountModal').addEventListener('click', function(e) {
  if (e.target === this) closeAddAccountModal();
});
document.getElementById('addFixAccountModal').addEventListener('click', function(e) {
  if (e.target === this) closeAddFixAccountModal();
});
document.getElementById('addMTDirectModal').addEventListener('click', function(e) {
  if (e.target === this) closeAddMTDirectModal();
});
document.getElementById('editEAAccountModal').addEventListener('click', function(e) {
  if (e.target === this) closeEditEAAccountModal();
});
document.getElementById('editMTDirectModal').addEventListener('click', function(e) {
  if (e.target === this) closeEditMTDirectModal();
});
document.getElementById('editFixAccountModal').addEventListener('click', function(e) {
  if (e.target === this) closeEditFixAccountModal();
});
// Register PWA service worker
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('/sw.js').catch(function() {});
}
// ── PnL Report Functions ──
let _pnlCurrentName = '';
let _pnlRequestId = '';
let _pnlPollTimer = null;
let _pnlLastData = null;  // cached last completed PnL response for pair breakdown popup

function openPnlModal(name) {
  _pnlCurrentName = name;
  document.getElementById('pnlModalTitle').textContent = '📊 PnL Report — ' + name;
  // Default dates: Sunday of current week to today
  const now = new Date();
  const sunday = new Date(now);
  sunday.setDate(now.getDate() - now.getDay()); // getDay() returns 0=Sun..6=Sat
  document.getElementById('pnlToDate').value = now.toISOString().split('T')[0];
  document.getElementById('pnlFromDate').value = sunday.toISOString().split('T')[0];
  const savedFeeKw = localStorage.getItem('tradeDash_pnlFeeKeywords');
  document.getElementById('pnlFeeKeywords').value = savedFeeKw !== null ? savedFeeKw : 'fee,fees';
  document.getElementById('pnlForm').style.display = '';
  document.getElementById('pnlStatus').style.display = 'none';
  document.getElementById('pnlResults').style.display = 'none';
  document.getElementById('pnlModal').style.display = 'flex';
}

function closePnlModal() {
  document.getElementById('pnlModal').style.display = 'none';
  const dialog = document.getElementById('pnlModalDialog');
  if (dialog && dialog.classList.contains('maximized')) {
    dialog.classList.remove('maximized');
    const btn = document.getElementById('pnlMaximizeBtn');
    if (btn) {
      btn.textContent = '🗖';
      btn.title = 'Maximize';
    }
  }
  if (_pnlPollTimer) { clearInterval(_pnlPollTimer); _pnlPollTimer = null; }
}

function togglePnlMaximize() {
  const dialog = document.getElementById('pnlModalDialog');
  const btn = document.getElementById('pnlMaximizeBtn');
  if (!dialog || !btn) return;
  if (dialog.classList.contains('maximized')) {
    dialog.classList.remove('maximized');
    btn.textContent = '🗖';
    btn.title = 'Maximize';
  } else {
    dialog.classList.add('maximized');
    btn.textContent = '🗗';
    btn.title = 'Restore';
  }
}

function exportPnlHtmlReport() {
  const data = _pnlLastData;
  if (!data) { alert('No report data to export.'); return; }
  const name = data.name || 'Report';
  const fromDate = data.from_date || '';
  const toDate = data.to_date || '';
  const accounts = data.accounts || [];
  const results = data.results || {};
  const totals = data.totals || {};
  const fmt = v => (v || 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  const clrStyle = v => v > 0 ? 'color:#00e676;' : v < 0 ? 'color:#ff5252;' : '';
  const isSkipped = a => results[a] && (results[a].source === 'skipped_offline' || results[a].source === 'server_error');

  // Header
  let tableHeader = '<tr><th>Metric</th>';
  accounts.forEach(a => {
    if (isSkipped(a)) {
      const label = results[a].source === 'server_error' ? 'ERROR' : 'OFFLINE';
      const color = results[a].source === 'server_error' ? '#ff5252' : '#ffa726';
      tableHeader += `<th>${a}<br><span style="font-size:0.65rem;color:${color};font-weight:700;">${label}</span></th>`;
    } else {
      tableHeader += `<th>${a}</th>`;
    }
  });
  tableHeader += '<th style="font-weight:700;">Total</th></tr>';

  const includeUnrealized = document.getElementById('pnlIncludeUnrealized').checked;

  // Rows
  const metrics = [
    {key: 'pnl', label: 'Realized PnL'},
    {key: 'swap', label: 'Swap'},
    {key: 'fees', label: 'Fees'},
  ];
  if (includeUnrealized) {
    metrics.push({key: 'unrealized', label: 'Unrealized PnL'});
  }
  let tableBody = '';
  metrics.forEach(m => {
    let totalVal = 0;
    tableBody += `<tr><td style="font-weight:600;">${m.label}</td>`;
    accounts.forEach(a => {
      if (isSkipped(a)) {
        tableBody += '<td style="color:#8b8fa3;text-align:center;">—</td>';
      } else {
        let v = 0;
        if (m.key === 'unrealized') {
          v = (data.current_states && data.current_states[a]) ? data.current_states[a].unrealized_pnl || 0 : 0;
        } else {
          v = results[a] ? results[a][m.key] || 0 : 0;
        }
        totalVal += v;
        tableBody += `<td style="${clrStyle(v)}">${fmt(v)}</td>`;
      }
    });
    let tv = 0;
    if (m.key === 'unrealized') {
      tv = totalVal;
    } else {
      const tKey = m.key === 'pnl' ? 'gross_pnl' : m.key;
      tv = totals[tKey] || totalVal;
    }
    tableBody += `<td style="font-weight:700;${clrStyle(tv)}">${fmt(tv)}</td></tr>`;
  });

  // Net PnL row
  tableBody += '<tr style="border-top:2px solid #6c5ce7;font-weight:700;background:#1e212f;"><td>Net PnL</td>';
  let netTotal = 0;
  accounts.forEach(a => {
    if (isSkipped(a)) {
      tableBody += '<td style="color:#8b8fa3;text-align:center;">—</td>';
    } else {
      let v = results[a] ? results[a].net || 0 : 0;
      if (includeUnrealized) {
        v += (data.current_states && data.current_states[a]) ? data.current_states[a].unrealized_pnl || 0 : 0;
      }
      netTotal += v;
      tableBody += `<td style="${clrStyle(v)}font-size:1.05rem;">${fmt(v)}</td>`;
    }
  });
  let nt = totals.net_pnl || netTotal;
  if (includeUnrealized) {
    let totUnrealized = 0;
    accounts.forEach(a => {
      if (!isSkipped(a)) {
        totUnrealized += (data.current_states && data.current_states[a]) ? data.current_states[a].unrealized_pnl || 0 : 0;
      }
    });
    nt += totUnrealized;
  }
  tableBody += `<td style="font-weight:700;${clrStyle(nt)}font-size:1.05rem;">${fmt(nt)}</td></tr>`;

  // Symbol Breakdown table
  const bySym = data.totals_by_symbol || {};
  const symKeys = Object.keys(bySym);
  let symbolSection = '';
  if (symKeys.length > 0) {
    symbolSection = `
    <div class="section-title">📊 PnL Breakdown by Symbol</div>
    <table>
      <thead>
        <tr>
          <th>Symbol</th>
          <th class="text-right">Net PnL</th>
          <th class="text-right">Hedge Lots</th>
          <th class="text-right">PnL / Lot</th>
        </tr>
      </thead>
      <tbody>
    `;
    let totPnl = 0, totLots = 0;
    symKeys.forEach(sym => {
      const s = bySym[sym];
      totPnl += s.pnl || 0;
      totLots += s.hedge_lots || 0;
      symbolSection += `
        <tr>
          <td style="font-weight:600;color:#a29bfe;">${sym}</td>
          <td class="text-right" style="${clrStyle(s.pnl)}">${fmt(s.pnl)}</td>
          <td class="text-right" style="color:#8b8fa3;">${(s.hedge_lots || 0).toFixed(2)}</td>
          <td class="text-right" style="font-weight:600;${clrStyle(s.pnl_per_lot)}">${fmt(s.pnl_per_lot)}</td>
        </tr>
      `;
    });
    const totalPpl = totLots > 0 ? totPnl / totLots : 0;
    symbolSection += `
        <tr style="border-top:2px solid #6c5ce7;font-weight:700;background:#1e212f;">
          <td>TOTAL</td>
          <td class="text-right" style="${clrStyle(totPnl)}">${fmt(totPnl)}</td>
          <td class="text-right" style="color:#8b8fa3;">${totLots.toFixed(2)}</td>
          <td class="text-right" style="${clrStyle(totalPpl)}">${fmt(totalPpl)}</td>
        </tr>
      </tbody>
    </table>
    `;
  }

  const generatedTime = new Date().toLocaleString();
  const htmlContent = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>PnL Report - ${name}</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    body {
      font-family: 'Inter', sans-serif;
      background: #0f1117;
      color: #e4e6f0;
      margin: 0;
      padding: 40px 20px;
      display: flex;
      justify-content: center;
    }
    .report-container {
      width: 100%;
      max-width: 1200px;
      background: #1a1d27;
      border: 1px solid #2e3346;
      border-radius: 12px;
      padding: 32px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.5);
    }
    .header-section {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      border-bottom: 1px solid #2e3346;
      padding-bottom: 20px;
      margin-bottom: 24px;
    }
    h1 {
      margin: 0;
      font-size: 1.8rem;
      color: #a29bfe;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .meta-info {
      text-align: right;
      font-size: 0.9rem;
      color: #8b8fa3;
      line-height: 1.6;
    }
    .meta-info strong {
      color: #e4e6f0;
    }
    .section-title {
      font-size: 1.2rem;
      font-weight: 600;
      margin: 28px 0 16px;
      color: #6c5ce7;
      border-left: 4px solid #6c5ce7;
      padding-left: 10px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-bottom: 24px;
      font-size: 0.9rem;
    }
    th {
      background: #242836;
      color: #8b8fa3;
      text-transform: uppercase;
      font-size: 0.75rem;
      font-weight: 600;
      letter-spacing: 0.5px;
      padding: 12px 16px;
      text-align: left;
      border-bottom: 2px solid #2e3346;
    }
    td {
      padding: 12px 16px;
      border-bottom: 1px solid #2e3346;
    }
    tr:hover td {
      background: #242836;
    }
    .text-right { text-align: right; }
    .text-center { text-align: center; }
    .footer {
      margin-top: 40px;
      text-align: center;
      font-size: 0.8rem;
      color: #8b8fa3;
      border-top: 1px solid #2e3346;
      padding-top: 20px;
    }
  </style>
</head>
<body>
  <div class="report-container">
    <div class="header-section">
      <div>
        <h1>📊 PnL Report — ${name}</h1>
        <div style="margin-top: 8px; color: #8b8fa3;">
          Period: <strong>${fromDate}</strong> &rarr; <strong>${toDate}</strong>
        </div>
      </div>
      <div class="meta-info">
        Generated: <strong>${generatedTime}</strong><br>
        Status: <strong>Report Complete</strong>
      </div>
    </div>

    <div class="section-title">💼 Account Breakdown</div>
    <table>
      <thead>
        ${tableHeader}
      </thead>
      <tbody>
        ${tableBody}
      </tbody>
    </table>

    ${symbolSection}

    <div class="footer">
      Trade Execution Dashboard &bull; PnL Reporting Suite
    </div>
  </div>
</body>
</html>`;

  const blob = new Blob([htmlContent], { type: 'text/html;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.setAttribute('download', `PnL_Report_${name}_${fromDate}_${toDate}.html`);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

async function requestPnl() {
  const fromDate = document.getElementById('pnlFromDate').value;
  const toDate = document.getElementById('pnlToDate').value;
  const feeKw = document.getElementById('pnlFeeKeywords').value.trim();
  localStorage.setItem('tradeDash_pnlFeeKeywords', document.getElementById('pnlFeeKeywords').value);
  if (!fromDate || !toDate) { alert('Please select both dates'); return; }
  document.getElementById('pnlSubmitBtn').disabled = true;
  try {
    const body = { name: _pnlCurrentName, from_date: fromDate, to_date: toDate,
                   exclude_balance: document.getElementById('pnlExcludeBalance').checked };
    if (feeKw) body.fee_keywords = feeKw;
    const resp = await fetch('/api/pnl/request', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body) });
    const data = await resp.json();
    if (!resp.ok) { alert(data.error || 'Failed'); return; }
    _pnlRequestId = data.request_id;
    document.getElementById('pnlForm').style.display = 'none';
    document.getElementById('pnlStatus').style.display = '';
    document.getElementById('pnlResults').style.display = 'none';
    document.getElementById('pnlSpinner').textContent = '⏳ Waiting for accounts to report...';
    document.getElementById('pnlProgress').textContent = '0 / ' + data.accounts.length + ' accounts reported';
    // Start polling
    _pnlPollTimer = setInterval(() => pollPnlStatus(), 2000);
  } catch(e) { alert('Error: ' + e.message); } finally {
    document.getElementById('pnlSubmitBtn').disabled = false;
  }
}

async function pollPnlStatus() {
  if (!_pnlRequestId) return;
  try {
    const resp = await fetch('/api/pnl/status/' + _pnlRequestId);
    const data = await resp.json();
    if (!resp.ok) return;
    const total = data.accounts ? data.accounts.length : 0;
    const done = data.reported ? data.reported.length : 0;
    const results = data.results || {};
    const skippedCount = Object.values(results).filter(r => r.source === 'skipped_offline' || r.source === 'server_error').length;
    const liveCount = done - skippedCount;
    const liveTotal = total - skippedCount;
    let progressText = liveCount + ' / ' + liveTotal + ' accounts reported';
    if (skippedCount > 0) progressText += ' (' + skippedCount + ' offline/error)';
    document.getElementById('pnlProgress').textContent = progressText;
    if (data.status === 'complete') {
      clearInterval(_pnlPollTimer); _pnlPollTimer = null;
      document.getElementById('pnlSpinner').textContent = '✅ Report complete';
      renderPnlResults(data);
    }
  } catch(e) { console.error('PnL poll error:', e); }
}

function renderPnlResults(data) {
  _pnlLastData = data;  // cache for pair breakdown popup
  const results = data.results || {};
  const totals = data.totals || {};
  const accounts = data.accounts || [];
  const fmt = v => (v || 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  const clr = v => v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--text)';
  // Detect offline accounts
  const isSkipped = a => results[a] && (results[a].source === 'skipped_offline' || results[a].source === 'server_error');
  // Header
  let hdr = '<th>Metric</th>';
  accounts.forEach(a => {
    if (isSkipped(a)) {
      const label = results[a].source === 'server_error' ? 'ERROR' : 'OFFLINE';
      const color = results[a].source === 'server_error' ? 'var(--red)' : 'var(--orange)';
      hdr += '<th style="color:var(--text2);">' + a + '<br><span style="font-size:0.65rem;color:' + color + ';font-weight:700;">' + label + '</span></th>';
    } else {
      hdr += '<th>' + a + '</th>';
    }
  });
  hdr += '<th style="font-weight:700;">Total</th>';
  document.getElementById('pnlResultsHeader').innerHTML = hdr;
  const includeUnrealized = document.getElementById('pnlIncludeUnrealized').checked;

  // Rows
  const metrics = [
    {key: 'pnl', label: 'Realized PnL'},
    {key: 'swap', label: 'Swap'},
    {key: 'fees', label: 'Fees'},
  ];
  if (includeUnrealized) {
    metrics.push({key: 'unrealized', label: 'Unrealized PnL'});
  }
  let tbody = '';
  metrics.forEach(m => {
    let totalVal = 0;
    tbody += '<tr><td style="font-weight:600;">' + m.label + '</td>';
    accounts.forEach(a => {
      if (isSkipped(a)) {
        tbody += '<td style="color:var(--text2);text-align:center;" title="Account was offline">—</td>';
      } else {
        let v = 0;
        if (m.key === 'unrealized') {
          v = (data.current_states && data.current_states[a]) ? data.current_states[a].unrealized_pnl || 0 : 0;
        } else {
          v = results[a] ? results[a][m.key] || 0 : 0;
        }
        totalVal += v;
        tbody += '<td style="color:' + clr(v) + ';">' + fmt(v) + '</td>';
      }
    });
    let tv = 0;
    if (m.key === 'unrealized') {
      tv = totalVal;
    } else {
      const tKey = m.key === 'pnl' ? 'gross_pnl' : m.key;
      tv = totals[tKey] || totalVal;
    }
    tbody += '<td style="font-weight:700;color:' + clr(tv) + ';">' + fmt(tv) + '</td></tr>';
  });
  // Net PnL row — total cell always clickable to open pair breakdown popup
  tbody += '<tr style="border-top:2px solid var(--accent);font-weight:700;"><td>Net PnL</td>';
  let netTotal = 0;
  accounts.forEach(a => {
    if (isSkipped(a)) {
      tbody += '<td style="color:var(--text2);text-align:center;" title="Account was offline">—</td>';
    } else {
      let v = results[a] ? results[a].net || 0 : 0;
      if (includeUnrealized) {
        v += (data.current_states && data.current_states[a]) ? data.current_states[a].unrealized_pnl || 0 : 0;
      }
      netTotal += v;
      tbody += '<td style="color:' + clr(v) + ';font-size:1.05rem;">' + fmt(v) + '</td>';
    }
  });
  let nt = totals.net_pnl || netTotal;
  if (includeUnrealized) {
    let totUnrealized = 0;
    accounts.forEach(a => {
      if (!isSkipped(a)) {
        totUnrealized += (data.current_states && data.current_states[a]) ? data.current_states[a].unrealized_pnl || 0 : 0;
      }
    });
    nt += totUnrealized;
  }
  tbody += '<td style="color:' + clr(nt) + ';font-size:1.1rem;cursor:pointer;border-bottom:2px dotted currentColor;" '
         + 'onclick="openPairBreakdown()" title="Click to see PnL breakdown by pair">'
         + fmt(nt) + ' 📊</td></tr>';
  document.getElementById('pnlResultsBody').innerHTML = tbody;
  document.getElementById('pnlResults').style.display = '';
}

// ─── PnL Pair Breakdown Popup ─────────────────────────────────────────────
function openPairBreakdown() {
  const data = _pnlLastData;
  if (!data) return;
  const bySym = data.totals_by_symbol || {};
  const symKeys = Object.keys(bySym);

  const fromDate = data.from_date || '';
  const toDate   = data.to_date   || '';
  document.getElementById('pairBreakdownTitle').textContent =
    '\uD83D\uDCC8 PnL by Pair \u2014 ' + (data.name || '') + (fromDate ? '  (' + fromDate + ' \u2192 ' + toDate + ')' : '');

  if (!symKeys.length) {
    // Show a helpful empty state instead of hiding the popup
    document.getElementById('pairBreakdownBody').innerHTML =
      '<tr><td colspan="4" style="text-align:center;padding:24px 12px;color:var(--text2);font-size:0.85rem;">'
      + '<div style="font-size:1.4rem;margin-bottom:8px;">\uD83D\uDCCA</div>'
      + '<strong style="color:var(--text);">No per-pair data available for this report.</strong><br><br>'
      + 'Per-pair breakdown is populated automatically for accounts connected via <strong>MT Direct</strong> '
      + '(the .NET MT4/MT5 bridge). EA-polled accounts do not yet submit per-symbol history.<br><br>'
      + '<span style="color:var(--orange);">\u26A0 If you\'re using MT Direct accounts and see this message, '
      + 'please ensure the server was restarted after the latest update.</span>'
      + '</td></tr>';
    document.getElementById('pairBreakdownModal').style.display = 'flex';
    return;
  }

  const fmt = v => (v || 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  const clr = v => v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--text)';
  let totPnl = 0, totLots = 0;
  let rows = '';
  symKeys.forEach(sym => {
    const s = bySym[sym];
    totPnl += s.pnl || 0;
    totLots += s.hedge_lots || 0;
    rows += '<tr>'
          + '<td style="font-weight:600;color:var(--accent2);padding:7px 10px;">' + sym + '</td>'
          + '<td style="color:' + clr(s.pnl) + ';text-align:right;padding:7px 10px;">' + fmt(s.pnl) + '</td>'
          + '<td style="text-align:right;color:var(--text2);padding:7px 10px;">' + (s.hedge_lots || 0).toFixed(2) + '</td>'
          + '<td style="color:' + clr(s.pnl_per_lot) + ';text-align:right;font-weight:600;padding:7px 10px;">' + fmt(s.pnl_per_lot) + '</td>'
          + '</tr>';
  });
  const totalPpl = totLots > 0 ? totPnl / totLots : 0;
  rows += '<tr style="border-top:2px solid var(--accent);font-weight:700;">'
        + '<td style="padding:8px 10px;">TOTAL</td>'
        + '<td style="color:' + clr(totPnl) + ';text-align:right;padding:8px 10px;">' + fmt(totPnl) + '</td>'
        + '<td style="text-align:right;color:var(--text2);padding:8px 10px;">' + totLots.toFixed(2) + '</td>'
        + '<td style="color:' + clr(totalPpl) + ';text-align:right;padding:8px 10px;">' + fmt(totalPpl) + '</td>'
        + '</tr>';

  document.getElementById('pairBreakdownBody').innerHTML = rows;
  document.getElementById('pairBreakdownModal').style.display = 'flex';
}

function closePairBreakdown() {
  document.getElementById('pairBreakdownModal').style.display = 'none';
}

// ─── Theme Color Settings ─────────────────────────────────────────────
function loadThemeColorPickers(serverColors) {
  const grid = document.getElementById('themeColorGrid');
  if (!grid) return;
  // Merge: localStorage takes priority, then server, then defaults
  const saved = JSON.parse(localStorage.getItem('themeColors') || '{}');
  const merged = Object.assign({}, THEME_DEFAULTS, serverColors, saved);
  grid.innerHTML = '';
  for (const [varName, defaultVal] of Object.entries(THEME_DEFAULTS)) {
    const current = merged[varName] || defaultVal;
    const item = document.createElement('div');
    item.className = 'theme-color-item';
    const input = document.createElement('input');
    input.type = 'color';
    input.value = current;
    input.id = 'tc_' + varName.replace(/--/g, '');
    input.dataset.varName = varName;
    input.addEventListener('input', function() {
      document.documentElement.style.setProperty(this.dataset.varName, this.value);
      // Live-update theme-color meta for header-bg
      if (this.dataset.varName === '--header-bg') {
        const meta = document.querySelector('meta[name="theme-color"]');
        if (meta) meta.setAttribute('content', this.value);
      }
    });
    const label = document.createElement('label');
    label.setAttribute('for', input.id);
    label.textContent = THEME_LABELS[varName] || varName;
    item.appendChild(input);
    item.appendChild(label);
    grid.appendChild(item);
  }
}

async function saveThemeColors(btn) {
  const colors = {};
  document.querySelectorAll('#themeColorGrid input[type=color]').forEach(inp => {
    colors[inp.dataset.varName] = inp.value;
  });
  // Save to localStorage for instant load next time
  localStorage.setItem('themeColors', JSON.stringify(colors));
  // Persist to backend settings
  try {
    await fetch('/api/settings', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({theme_colors: colors})
    });
    // Brief visual feedback
    if (btn) {
      const orig = btn.textContent;
      btn.textContent = '\u2705 Saved!';
      setTimeout(() => { btn.textContent = orig; }, 1500);
    }
  } catch(e) { alert('Failed to save theme: ' + e); }
}

function resetThemeColors() {
  // Reset all CSS variables to defaults
  const root = document.documentElement;
  for (const [varName, val] of Object.entries(THEME_DEFAULTS)) {
    root.style.setProperty(varName, val);
  }
  // Update color picker inputs
  document.querySelectorAll('#themeColorGrid input[type=color]').forEach(inp => {
    inp.value = THEME_DEFAULTS[inp.dataset.varName] || inp.value;
  });
  // Update meta theme-color
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) meta.setAttribute('content', THEME_DEFAULTS['--header-bg']);
  // Clear localStorage
  localStorage.removeItem('themeColors');
  // Persist reset to backend
  fetch('/api/settings', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({theme_colors: {}})
  }).catch(() => {});
}

</script>
<!-- PnL Report Modal -->
<div id="pnlModal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.7);z-index:9999;justify-content:center;align-items:center;" onclick="if(event.target===this)closePnlModal()">
  <div id="pnlModalDialog" style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;width:75%;min-width:480px;max-width:960px;max-height:90vh;overflow:auto;box-shadow:0 20px 40px rgba(0,0,0,0.5);resize:both;position:relative;">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
      <h3 id="pnlModalTitle" style="margin:0;">📊 PnL Report</h3>
      <div style="display:flex;gap:8px;align-items:center;">
        <button class="btn btn-sm" id="pnlMaximizeBtn" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:2px 8px;" onclick="togglePnlMaximize()" title="Maximize">🗖</button>
        <button class="btn btn-sm" style="background:var(--red);color:#fff;border:none;" onclick="closePnlModal()">✕</button>
      </div>
    </div>
    <div id="pnlForm">
      <div style="display:flex;gap:12px;margin-bottom:12px;">
        <div style="flex:1"><label style="font-size:0.75rem;color:var(--text2);">FROM DATE</label><input type="date" id="pnlFromDate" style="width:100%;padding:8px;border-radius:6px;border:1px solid var(--border);background:var(--bg2);color:var(--text);"></div>
        <div style="flex:1"><label style="font-size:0.75rem;color:var(--text2);">TO DATE</label><input type="date" id="pnlToDate" style="width:100%;padding:8px;border-radius:6px;border:1px solid var(--border);background:var(--bg2);color:var(--text);"></div>
      </div>
      <div style="margin-bottom:12px;">
        <label style="font-size:0.75rem;color:var(--text2);">FEE KEYWORDS <span style="opacity:0.6">(optional override, comma-separated)</span></label>
        <input type="text" id="pnlFeeKeywords" placeholder="Leave empty for defaults" style="width:100%;padding:8px;border-radius:6px;border:1px solid var(--border);background:var(--bg2);color:var(--text);">
      </div>
      <div style="margin-bottom:12px;display:flex;flex-direction:column;gap:8px;">
        <label style="font-size:0.85rem;color:var(--text);display:flex;align-items:center;gap:8px;cursor:pointer;">
          <input type="checkbox" id="pnlExcludeBalance" checked style="width:16px;height:16px;cursor:pointer;">
          Exclude deposits &amp; withdrawals
        </label>
        <label style="font-size:0.85rem;color:var(--text);display:flex;align-items:center;gap:8px;cursor:pointer;">
          <input type="checkbox" id="pnlIncludeUnrealized" checked style="width:16px;height:16px;cursor:pointer;">
          Include Unrealized PNL
        </label>
      </div>
      <button class="btn btn-primary" id="pnlSubmitBtn" onclick="requestPnl()" style="width:100%;">Generate Report</button>
    </div>
    <div id="pnlStatus" style="display:none;text-align:center;padding:20px;">
      <div id="pnlSpinner" style="font-size:1.2rem;">⏳ Waiting for accounts to report...</div>
      <div id="pnlProgress" style="color:var(--text2);margin-top:8px;"></div>
    </div>
    <div id="pnlResults" style="display:none;margin-top:16px;">
      <div style="display:flex;justify-content:flex-end;margin-bottom:12px;">
        <button class="btn btn-sm btn-primary" onclick="exportPnlHtmlReport()" style="display:flex;align-items:center;gap:6px;">
          📥 Export HTML Report
        </button>
      </div>
      <table class="rpt-table" style="width:100%;">
        <thead><tr id="pnlResultsHeader"></tr></thead>
        <tbody id="pnlResultsBody"></tbody>
      </table>
    </div>
  </div>
</div>
<!-- Pair Breakdown Modal -->
<div id="pairBreakdownModal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.75);z-index:10000;justify-content:center;align-items:center;" onclick="if(event.target===this)closePairBreakdown()">
  <div style="background:var(--card);border:1px solid var(--border);border-radius:12px;padding:24px;min-width:420px;max-width:640px;max-height:80vh;overflow-y:auto;box-shadow:0 20px 40px rgba(0,0,0,0.6);">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
      <h3 id="pairBreakdownTitle" style="margin:0;font-size:1rem;">📈 PnL by Pair</h3>
      <button class="btn btn-sm" style="background:var(--red);color:#fff;border:none;" onclick="closePairBreakdown()">✕</button>
    </div>
    <table class="rpt-table" style="width:100%;border-collapse:collapse;">
      <thead>
        <tr>
          <th style="text-align:left;padding:8px 10px;border-bottom:1px solid var(--border);color:var(--text2);font-size:0.72rem;letter-spacing:.05em;">PAIR</th>
          <th style="text-align:right;padding:8px 10px;border-bottom:1px solid var(--border);color:var(--text2);font-size:0.72rem;letter-spacing:.05em;">NET PNL</th>
          <th style="text-align:right;padding:8px 10px;border-bottom:1px solid var(--border);color:var(--text2);font-size:0.72rem;letter-spacing:.05em;">HEDGE LOTS</th>
          <th style="text-align:right;padding:8px 10px;border-bottom:1px solid var(--border);color:var(--text2);font-size:0.72rem;letter-spacing:.05em;">PNL / LOT</th>
        </tr>
      </thead>
      <tbody id="pairBreakdownBody"></tbody>
    </table>
    <p id="pairBreakdownFooter" style="margin-top:10px;font-size:0.7rem;color:var(--text2);">Hedge Lots = deal count &divide; 2 (1 buy + 1 sell = 1 lot). Click outside to close.</p>
  </div>
</div>
<!-- Custom Confirm Modal (must be last in DOM for z-order on popout page) -->
<div class="modal-overlay" id="confirmModal" style="z-index:9999;">
  <div class="modal" style="max-width:400px;text-align:center;padding:24px;">
    <p id="confirmModalMsg" style="font-size:1rem;margin-bottom:20px;"></p>
    <div class="btn-group" style="justify-content:center;gap:12px;">
      <button class="btn btn-danger" id="confirmModalYes" style="min-width:80px;">Delete</button>
      <button class="btn" id="confirmModalNo" style="min-width:80px;background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="document.getElementById('confirmModal').classList.remove('active')">Cancel</button>
    </div>
  </div>
</div>
</body>
</html>"""

# ─── Main ───────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # Wire up references for FIX command loop (must be done AFTER _load_sessions)
    if fix_manager:
        _fix_dashboard_data["sessions"] = sessions  # re-set after _load_sessions replaced it
        _fix_dashboard_data["should_issue_command"] = _should_issue_command
        fix_manager.start()
        app.logger.info("FIX Account Manager started")

    # Wire up MT Direct manager
    if mt_direct_manager:
        _mt_direct_dashboard_data["sessions"] = sessions
        _mt_direct_dashboard_data["should_issue_command"] = _should_issue_command
        _mt_direct_dashboard_data["log_event"] = _log_event
        _mt_direct_dashboard_data["save_sessions"] = _save_sessions
        _mt_direct_dashboard_data["normalize_ticket"] = _normalize_ticket
        _mt_direct_dashboard_data["strategies"] = strategies
        # Provide a callback for Direct accounts to report trade results
        def _mt_direct_report_result(data):
            """Internal callback: process trade result from MT Direct connector."""
            with lock:
                session = sessions.get(data.get("session_id"))
                if not session:
                    return
                # Reuse the existing trade_result logic
                from flask import Request as _FReq
                # Directly call into the session update logic
                account = str(data.get("account", ""))
                status = data.get("status", "")
                ticket = data.get("ticket")
                spread = data.get("spread", 0)
                detail = data.get("detail", "")
                session_id = data.get("session_id", "")
                cmd_sent_ts = in_flight_commands.pop((session_id, account), None)
                in_flight_retry_counts.pop((session_id, account), None)  # Reset retry counter on fill


                if status == "filled":
                    if _cycle_handle_fill(session, account, data, cmd_sent_ts, session_id):
                        pass  # Handled by shared cycle function
                    else:
                        # ── Ghost-fill guard (Fix 2 — defence-in-depth) ──────────────────────
                        # If this session is in cycle mode and _cycle_handle_fill returned
                        # False, it means the phase is NOT "open" (already advanced to
                        # "close" or beyond).  This happens when the background OrderSend
                        # thread delivers a late fill callback AFTER the 15-second poll
                        # already timed out, an error was reported, a retry was dispatched,
                        # the retry succeeded, and the phase moved on.  Appending the ghost
                        # ticket here would grow the fills list and cause one extra cycle
                        # close + reopen on a position that shouldn't exist in the session.
                        _action_now = session.get("action", "")
                        if _action_now.startswith("cycle_"):
                            _cyc_acc = session.get("cycle_account", "")
                            if not _cyc_acc:
                                _sides_k = list(session.get("sides", {}).keys())
                                _cyc_acc = _sides_k[0] if _action_now == "cycle_acc1" else (_sides_k[1] if len(_sides_k) > 1 else "")
                            if account == _cyc_acc:
                                _cyc_phase = session.get("cycle_progress", {}).get("phase", "")
                                print(f"[CYCLE-GHOST] Discarding late fill for {account} "
                                      f"ticket={ticket} — cycle phase is already '{_cyc_phase}' "
                                      f"(open step already completed). This was a duplicate bg-thread callback.")
                                _log_event(session_id, account, "cycle_ghost_fill_discarded",
                                           f"[MT-DIRECT] Late duplicate fill discarded: ticket={ticket} "
                                           f"phase={_cyc_phase} — would have caused extra position")
                                _check_session_completion(session)
                                # Skip the normal fill recording entirely
                                session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                _save_sessions()
                                return  # early-exit the report_result callback
                        # Normal (non-cycle) fill path
                        session["filled"][account] = session["filled"].get(account, 0) + 1
                        session.setdefault("last_trade_ts", {})[account] = time.time()
                        fill_price = data.get("fill_price")
                        quote_price = data.get("quote_price")
                        session.setdefault("fills", []).append({
                            "account": account,
                            "ticket": ticket,
                            "price": float(fill_price) if fill_price else None,
                            "quote_price": float(quote_price) if quote_price else None,
                            "spread": int(spread) if spread else None,
                            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "ts_epoch": time.time(),
                            "cmd_ts": cmd_sent_ts,
                        })
                        _log_event(session_id, account, "trade_filled",
                                   f"[MT-DIRECT] ticket={ticket} price={fill_price} "
                                   f"filled={session['filled'][account]}/{session['total_positions']}")
                    _check_session_completion(session)

                elif status in ("rollback_closed", "closed"):
                    if not _cycle_handle_close(session, account, data, session_id, cmd_sent_ts):
                        # Normal rollback/close
                        if status == "rollback_closed":
                            rb = session.get("rollback_needed", {})
                            rb[account] = max(0, rb.get(account, 0) - 1)
                            session["rollback_needed"] = rb
                            rb_tickets = session.get("rollback_tickets", {}).get(account, [])
                            if rb_tickets:
                                rb_tickets.pop(0)
                                if not rb_tickets:
                                    session.get("rollback_tickets", {}).pop(account, None)
                            if rb.get(account, 0) <= 0:
                                session.get("rollback_start_ts", {}).pop(account, None)
                            else:
                                session.setdefault("rollback_start_ts", {})[account] = time.time()

                        session["closed"][account] = session["closed"].get(account, 0) + 1
                        session.setdefault("last_trade_ts", {})[account] = time.time()
                        close_price = data.get("fill_price")
                        session.setdefault("close_fills", []).append({
                            "account": account,
                            "ticket": ticket,
                            "price": float(close_price) if close_price else None,
                            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "ts_epoch": time.time(),
                            "cmd_ts": cmd_sent_ts,
                        })
                        _log_event(session_id, account, "position_closed",
                                   f"[MT-DIRECT] ticket={ticket} price={close_price}")
                    _check_session_completion(session)

                elif status == "error":
                    errors = session["errors"].get(account, [])
                    errors.append({"ts": datetime.now().strftime("%H:%M:%S"),
                                   "detail": str(detail)[:200], "ticket": ticket})
                    if len(errors) > 50:
                        errors = errors[-50:]
                    session["errors"][account] = errors
                    _log_event(session_id, account, "trade_error", f"[MT-DIRECT] {detail}")

                    # Rollback cleanup on error — clear the rollback tracking
                    # but do NOT record a close_fill or increment closed count
                    # because the position was NOT actually closed.
                    rb = session.get("rollback_needed", {})
                    if rb.get(account, 0) > 0:
                        rb[account] = max(0, rb.get(account, 0) - 1)
                        session["rollback_needed"] = rb
                        rb_tickets = session.get("rollback_tickets", {}).get(account, [])
                        if rb_tickets:
                            rb_tickets.pop(0)
                            if not rb_tickets:
                                session.get("rollback_tickets", {}).pop(account, None)
                        session.get("rollback_start_ts", {}).pop(account, None)

                    # ── Cycle safety: if error during reopen phase, retry or abort ──
                    # Mirrors the same logic in the EA-poll trade_result error handler.
                    # Without this, open_dispatched stays True and the cycle freezes.
                    _mt_action = session.get("action", "")
                    if _mt_action.startswith("cycle_"):
                        _mt_progress = session.get("cycle_progress", {})
                        if _mt_progress.get("phase") == "open":
                            _mt_retries = _mt_progress.get("open_retries", 0) + 1
                            _mt_max_retries = 3
                            if _mt_retries >= _mt_max_retries:
                                session["action"] = "monitor"
                                session["cycle_progress"] = {}
                                _save_sessions()
                                _mt_msg = (f"Cycle reopen FAILED after {_mt_retries} attempts on "
                                           f"{account} (MT-DIRECT error: {detail}) — reverting to MONITOR")
                                print(f"[CYCLE-FAIL] {_mt_msg}")
                                _log_event(session_id, account, "cycle_failed", _mt_msg)
                            else:
                                # Retry: clear dispatch flag so next poll re-sends open
                                _mt_progress["open_retries"] = _mt_retries
                                _mt_progress.pop("open_dispatched", None)
                                session["cycle_progress"] = _mt_progress
                                _save_sessions()
                                _mt_msg = (f"Cycle reopen timeout on {account} "
                                           f"(MT-DIRECT, attempt {_mt_retries}/{_mt_max_retries}, "
                                           f"error: {detail}) — will retry")
                                print(f"[CYCLE-RETRY] {_mt_msg}")
                                _log_event(session_id, account, "cycle_retry", _mt_msg)

                session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _save_sessions()

        _mt_direct_dashboard_data["report_trade_result"] = _mt_direct_report_result
        mt_direct_manager.start()
        app.logger.info("MT Direct Account Manager started")

    # Start universal hedge monitor (works for EA poll, MT Direct, FIX — all account types)
    _start_hedge_monitor_thread()

    app.logger.info("Starting Trade Dashboard on %s:%d", TRADE_HOST, TRADE_PORT)
    try:
        from waitress import serve
        serve(app, host=TRADE_HOST, port=TRADE_PORT, threads=16)
    except Exception as e:
        app.logger.warning("Waitress not available: %s — falling back to Flask dev server", e)
        app.run(host=TRADE_HOST, port=TRADE_PORT, threaded=True)
