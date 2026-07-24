#!/usr/bin/env python3
"""
mt_bridge_client.py â€” Python bridge client for MtBridgeService

Replaces pythonnet-based MT4/MT5 Direct connections with HTTP calls
to the standalone C# MtBridgeService. Same interface as
MT4DirectAccount / MT5DirectAccount / MTDirectManager.
"""

import json
import logging
import os
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error
import urllib.parse

logger = logging.getLogger(__name__)

# Global wakeup event for command loop (similar to MTDirect)
_quote_wakeup = threading.Event()

def _normalize_ticket(t):
    """Handle 32-bit overflow for MT4 tickets."""
    try:
        v = int(t)
        if v < 0:
            v = v + (1 << 32)
        return v
    except (ValueError, TypeError):
        return t

def normalize_mt_config(cfg):
    """Normalize a config dict from mt_direct_accounts.json to lowercase keys."""
    if not isinstance(cfg, dict):
        return {}
    
    # 1. First extract nested Extra if it exists
    extra_dict = {}
    for extra_key in ['Extra', 'extra']:
        if extra_key in cfg and isinstance(cfg[extra_key], dict):
            for k, v in cfg[extra_key].items():
                extra_dict[k.lower()] = v
                
    # 2. Build normalized dictionary from Extra contents first
    normalized = {}
    for k, v in extra_dict.items():
        normalized[k] = v
        
    # 3. Overwrite/add root keys (converted to lowercase)
    for k, v in cfg.items():
        if k not in ('Extra', 'extra'):
            normalized[k.lower()] = v
            
    # 4. Resolve the type correctly (using Platform/Extra first to avoid type: mt4 mangling)
    resolved_type = "mt5"
    for type_source in [cfg.get("Platform"), extra_dict.get("type"), extra_dict.get("platform"), cfg.get("type"), cfg.get("platform")]:
        if type_source:
            resolved_type = str(type_source).lower()
            break
    normalized['type'] = resolved_type
    
    # 5. Ensure standard keys are lowercase and clean
    login_val = None
    for k in ['Login', 'login']:
        if k in cfg:
            login_val = cfg[k]
            break
    if login_val is None:
        for k in ['login']:
            if k in extra_dict:
                login_val = extra_dict[k]
                break
    normalized['login'] = str(login_val) if login_val is not None else ''

    password_val = None
    for k in ['Password', 'password']:
        if k in cfg:
            password_val = cfg[k]
            break
    if password_val is None:
        for k in ['password']:
            if k in extra_dict:
                password_val = extra_dict[k]
                break
    normalized['password'] = str(password_val) if password_val is not None else ''

    server_val = None
    for k in ['Server', 'server']:
        if k in cfg:
            server_val = cfg[k]
            break
    if server_val is None:
        for k in ['server']:
            if k in extra_dict:
                server_val = extra_dict[k]
                break
    normalized['server'] = str(server_val) if server_val is not None else ''

    port_val = None
    for k in ['Port', 'port']:
        if k in cfg:
            port_val = cfg[k]
            break
    if port_val is None:
        for k in ['port']:
            if k in extra_dict:
                port_val = extra_dict[k]
                break
    try:
        normalized['port'] = int(port_val) if port_val is not None else 443
    except (ValueError, TypeError):
        normalized['port'] = 443
        
    # 6. Standardise auto_connect_start
    auto_conn = None
    for k in ['AutoConnect', 'auto_connect_start']:
        if k in cfg:
            auto_conn = cfg[k]
            break
    if auto_conn is None:
        for k in ['auto_connect_start', 'autoconnect']:
            if k in extra_dict:
                auto_conn = extra_dict[k]
                break
    normalized['auto_connect_start'] = bool(auto_conn) if auto_conn is not None else True

    # Clean up any leftover uppercase keys or nested Extra/platform/etc.
    normalized.pop('extra', None)
    normalized.pop('platform', None)
    normalized.pop('id', None)
    normalized.pop('autoconnect', None)
    normalized.pop('lotmultiplier', None)
    
    return normalized


# â”€â”€â”€ Configuration â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
BRIDGE_URL = os.environ.get("MT_BRIDGE_URL", "http://localhost:5090")
BRIDGE_EXE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "MtBridgeService", "bin", "Debug", "net8.0", "MtBridgeService.exe")

# â”€â”€â”€ HTTP Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _api(method, path, data=None, timeout=30):
    """Make an HTTP request to the bridge service."""
    url = f"{BRIDGE_URL}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            err_body = json.loads(e.read().decode())
        except Exception:
            err_body = {"error": str(e)}
        return err_body
    except urllib.error.URLError as e:
        return {"error": f"Bridge unreachable: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}


def _get(path, timeout=30):
    return _api("GET", path, timeout=timeout)

def _post(path, data=None, timeout=30):
    return _api("POST", path, data=data, timeout=timeout)

def _put(path, data=None, timeout=30):
    return _api("PUT", path, data=data, timeout=timeout)

def _delete(path, timeout=30):
    return _api("DELETE", path, timeout=timeout)


# â”€â”€â”€ Bridge Process Management â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_bridge_process = None

def ensure_bridge_running():
    """Start the bridge service if not already running."""
    global _bridge_process
    # Check if already running
    try:
        result = _get("/api/status", timeout=2)
        if result.get("status") == "ok":
            logger.info("MtBridgeService already running")
            return True
    except Exception:
        pass

    if not os.path.exists(BRIDGE_EXE):
        logger.error("MtBridgeService not found at %s â€” run 'dotnet build' first", BRIDGE_EXE)
        return False

    logger.info("Starting MtBridgeService from %s", BRIDGE_EXE)
    _bridge_process = subprocess.Popen(
        [BRIDGE_EXE],
        cwd=os.path.dirname(BRIDGE_EXE),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    )

    # Wait for it to become ready
    for i in range(15):
        time.sleep(1)
        try:
            result = _get("/api/status", timeout=2)
            if result.get("status") == "ok":
                logger.info("MtBridgeService started (PID %d)", _bridge_process.pid)
                return True
        except Exception:
            pass

    logger.error("MtBridgeService failed to start within 15s")
    return False


def stop_bridge():
    """Stop the bridge service subprocess."""
    global _bridge_process
    if _bridge_process:
        _bridge_process.terminate()
        _bridge_process.wait(timeout=5)
        _bridge_process = None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# MtBridgeAccount â€” drop-in replacement for MT4DirectAccount/MT5DirectAccount
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class MtBridgeAccount:
    """
    Bridge account that forwards all calls to MtBridgeService via HTTP.
    Same interface as MT4DirectAccount / MT5DirectAccount.
    """

    def __init__(self, account_id, config, dashboard_data):
        self.account_id = str(account_id)
        self.config = normalize_mt_config(config)
        self.dd = dashboard_data
        self._connected = False
        self._running = False
        self._last_error = None
        self._heartbeat_thread = None
        self._reconnect_attempt = 0
        self._reconnect_delay = 5
        self._empty_reads = 0

    @property
    def connected(self):
        return self._connected

    @property
    def label(self):
        return self.config.get("label", "")

    @label.setter
    def label(self, value):
        self.config["label"] = value

    @property
    def conn_type(self):
        t = self.config.get("type", self.config.get("platform", "mt5")).lower()
        # Normalise to canonical 'mt4_direct' / 'mt5_direct' â€” same as mt_direct_connector
        if t in ("mt5", "mt5_direct"):
            return "mt5_direct"
        return "mt4_direct"

    def start(self):
        """Connect to broker via bridge service. Returns True/False."""
        # Register account with bridge
        cfg = {
            "id": self.account_id,
            "platform": self.config.get("type", self.config.get("platform", "mt5")),
            "login": int(self.config.get("login", 0)),
            "password": str(self.config.get("password", "")),
            "server": str(self.config.get("server", "")),
            "port": int(self.config.get("port", 443)),
            "label": self.label,
        }

        # Add account (may already exist)
        _post("/api/accounts", cfg)

        # Connect
        result = _post(f"/api/accounts/{self.account_id}/connect", timeout=45)
        if result.get("connected"):
            self._connected = True
            self._last_error = None
            self._running = True
            self._start_heartbeat()
            self._push_account_info()
            self._push_positions()
            return True
        else:
            self._last_error = result.get("error", "Unknown error")
            logger.error("[%s] Bridge connect failed: %s", self.account_id, self._last_error)
            return False

    def stop(self):
        """Disconnect from broker."""
        self._running = False
        self._connected = False
        _post(f"/api/accounts/{self.account_id}/disconnect")

    def _start_heartbeat(self):
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        self._running = True
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True,
            name=f"BridgeHB-{self.account_id}")
        self._heartbeat_thread.start()

    def _heartbeat_loop(self):
        """Periodically sync account info and positions from bridge."""
        while self._running:
            try:
                info = _get(f"/api/accounts/{self.account_id}/info", timeout=10)
                if info and "error" not in info:
                    self._connected = info.get("connected", False)
                    self._last_error = info.get("last_error")
                    if self._connected:
                        self._push_account_info(info)
                        self._push_positions()
                        self._reconnect_attempt = 0
                        self._reconnect_delay = 5
                        _quote_wakeup.set()  # Wake command loop immediately
                    else:
                        # Not connected yet (async stagger still in progress or
                        # reconnecting) â€” do NOT write positions so stale data
                        # from a previous session is never injected.
                        logger.debug("[%s] Bridge heartbeat: account not connected, skipping position push",
                                     self.account_id)
                else:
                    self._connected = False
            except Exception as e:
                logger.error("[%s] Bridge heartbeat error: %s", self.account_id, e)
                self._connected = False

            # Poll more frequently (0.5s) to quickly detect externally closed positions
            # for hedge rebalancing. Sleep in short increments to allow quick exit.
            for _ in range(2):
                if not self._running:
                    break
                time.sleep(0.25)

    def _push_account_info(self, info=None):
        """Push account data into ea_account_info."""
        if info is None:
            info = _get(f"/api/accounts/{self.account_id}/info", timeout=10)
        if not info or "error" in info:
            return

        dd = self.dd
        aid = self.account_id

        if aid not in dd.get("ea_account_info", {}):
            dd.setdefault("ea_account_info", {})[aid] = {}

        acct = dd["ea_account_info"][aid]
        credit = info.get("credit", 0)
        acct["balance"] = info.get("balance", 0)
        acct["equity"] = info.get("equity", 0) - credit
        acct["margin"] = info.get("margin", 0)
        acct["free_margin"] = info.get("free_margin", 0)
        acct["profit"] = info.get("profit", 0)
        
        # Safely compute fallback PNL (avoid 0 - 112000 = -112000 glitches)
        eq = info.get("equity", 0)
        bal = info.get("balance", 0)
        credit = info.get("credit", 0)
        if eq > 0 and bal > 0:
            acct["total_pnl"] = round(eq - bal - credit, 2)
        else:
            acct["total_pnl"] = round(acct["profit"], 2)
            
        acct["leverage"] = info.get("leverage", 0)
        acct["mt_direct"] = True
        acct["conn_type"] = self.conn_type
        acct["direct_mode"] = True
        acct["connected"] = info.get("connected", False)
        acct["last_update"] = time.time()

    @staticmethod
    def _parse_open_time(ot_str):
        """Parse an open_time string into a Unix epoch float, or None."""
        if not ot_str:
            return None
        from datetime import datetime, timedelta
        import re
        try:
            from zoneinfo import ZoneInfo
            NY_TZ = ZoneInfo("America/New_York")
        except ImportError:
            import pytz
            NY_TZ = pytz.timezone("America/New_York")

        cleaned = str(ot_str).strip()
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
                dt_ny = dt - timedelta(hours=7)
                dt_ny = dt_ny.replace(tzinfo=NY_TZ)
                return dt_ny.timestamp()
        except Exception:
            pass

        s = re.sub(r'[+-]\d{2}:\d{2}$', '', cleaned)
        s = re.sub(r'\.\d+', '', s)
        for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p",
                    "%Y.%m.%d %H:%M:%S", "%Y.%m.%d %H:%M",
                    "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                dt_ny = dt - timedelta(hours=7)
                dt_ny = dt_ny.replace(tzinfo=NY_TZ)
                return dt_ny.timestamp()
            except (ValueError, TypeError):
                continue
        return None

    def _push_positions(self):
        """Push open positions into ea_account_info."""
        positions = _get(f"/api/accounts/{self.account_id}/positions", timeout=10)
        if positions is None or isinstance(positions, dict) and "error" in positions:
            return
        # If the bridge returns an empty list but reports not connected, this is
        # likely the stagger window before the live handshake completes.  Skip
        # writing so stale positions from a previous session are not overwritten
        # with an empty dict (which would then look like "no positions" briefly
        # before the real data arrives â€” or worse, old positions if the bridge
        # wasn't restarted and still has its own stale in-memory cache).
        if isinstance(positions, list) and len(positions) == 0:
            if not self._connected:
                logger.debug("[%s] _push_positions: skipping empty list while not connected",
                             self.account_id)
                return
            
            # If connected but equity != balance, the broker is still syncing positions.
            info = self.dd.get("ea_account_info", {}).get(self.account_id, {})
            eq = info.get("equity", 0)
            bal = info.get("balance", 0)
            credit = info.get("credit", 0)
            if eq > 0 and bal > 0 and abs(eq - bal - credit) > 1.0:
                logger.warning("[%s] _push_positions: rejecting empty positions array because equity (%.2f) != balance (%.2f) + credit (%.2f). Broker is likely still syncing.", self.account_id, eq, bal, credit)
                info["_positions_desync"] = True
                return

            # Debounce: MT4/MT5 can briefly report 0 positions and eq==bal during terminal restart
            # before it finishes syncing trade history. We require 3 consecutive polls (1.5s) to confirm.
            # Only debounce if the account previously had positions to prevent startup delays.
            prev_count = info.get("open_count", 0)
            if prev_count > 0:
                self._empty_reads += 1
                if self._empty_reads < 3:
                    logger.info("[%s] _push_positions: debouncing empty positions array (%d/3) to prevent restart glitch.", self.account_id, self._empty_reads)
                    return
            else:
                self._empty_reads = 3 # Already confirmed empty or never had positions
        else:
            self._empty_reads = 0

        dd = self.dd
        aid = self.account_id

        if aid not in dd.get("ea_account_info", {}):
            dd.setdefault("ea_account_info", {})[aid] = {}

        acct = dd["ea_account_info"][aid]
        acct["_positions_desync"] = False
        pos_dict = {}
        total_swap = 0.0
        tickets = []
        pos_details = []
        _lbi = {}  # lots_by_instrument: symbol -> {"buy": x, "sell": y}
        if positions and not getattr(self, '_bridge_pos_logged', False):
            self._bridge_pos_logged = True
            logger.warning("[%s] Bridge positions count: %d active positions", self.account_id, len(positions))

        for p in positions:
            side = p.get("side", "").lower()
            # Filter out pending limit/stop orders (we only want active market positions)
            if side not in ("buy", "sell", "0", "1", "op_buy", "op_sell", "position_type_buy", "position_type_sell"):
                continue
                
            ticket = p.get("ticket", 0)
            symbol = p.get("symbol", "")
            lots = p.get("lots", 0)
            open_time_str = p.get("open_time", p.get("openTime", ""))
            open_epoch = self._parse_open_time(open_time_str)

            pos_dict[ticket] = {
                "symbol": symbol,
                "type": 0 if side == "buy" else 1,
                "lots": lots,
                "open_price": p.get("open_price", p.get("openPrice", 0)),
                "open_time": open_time_str,
                "profit": p.get("profit", 0),
                "swap": p.get("swap", 0),
                "comment": p.get("comment", ""),
            }
            total_swap += p.get("swap", 0)
            tickets.append(ticket)

            # Position details for cycle age tracking
            pos_details.append({
                "ticket": ticket,
                "symbol": symbol,
                "comment": p.get("comment", ""),
                "open_epoch": open_epoch,
            })

            # Per-instrument lots breakdown
            sym_key = symbol.upper() if symbol else "Unknown"
            if sym_key not in _lbi:
                _lbi[sym_key] = {"buy": 0, "sell": 0}
            if side == "buy":
                _lbi[sym_key]["buy"] = round(_lbi[sym_key]["buy"] + lots, 2)
            else:
                _lbi[sym_key]["sell"] = round(_lbi[sym_key]["sell"] + lots, 2)

        acct["positions"] = pos_dict
        acct["open_count"] = len(pos_dict)
        acct["total_swap"] = round(total_swap, 2)
        acct["total_pnl"] = round(
            sum(p.get("profit", 0) + p.get("swap", 0)
                for p in pos_dict.values()), 2)
        # Signed lots: buy = positive, sell = negative (matches direct connector)
        acct["total_lots"] = round(
            sum(p["lots"] if p.get("type") == 0 else -p["lots"]
                for p in pos_dict.values()), 2)
        acct["open_tickets"] = tickets
        acct["position_details"] = pos_details
        acct["lots_by_instrument"] = _lbi

        # Per-instrument swap breakdown
        _sbi = {}
        for p in pos_dict.values():
            sym = p.get("symbol", "")
            sym_key = sym.upper() if sym else "Unknown"
            swap_val = p.get("swap", 0.0)
            _sbi[sym_key] = round(_sbi.get(sym_key, 0.0) + swap_val, 2)
        acct["swap_by_instrument"] = _sbi

        acct["last_update"] = time.time()

        # Sync imported session filled count with the broker-confirmed position count.
        # This mirrors the same logic in the EA heartbeat route (trade_dashboard.py) which
        # only runs for EA poll-mode accounts. Bridge accounts bypass that route, so we
        # must do the sync here after every successful position push.
        # Skip during cycling/closing/opening — mid-execution the broker async delivery
        # can lag behind fill callbacks, causing a false rollback of the filled count.
        ea_pos = len(pos_dict)
        _needs_save = False
        sessions_dict = dd.get("sessions", {})
        for _sid, _sess in sessions_dict.items():
            if not _sess.get("imported"):
                continue
            if aid not in _sess.get("sides", {}):
                continue
            sess_action = _sess.get("action", "")
            # Skip when the session is actively opening, cycling, or closing.
            # During "open" the broker count can lag behind fill callbacks by 1-2 polls
            # (500ms each) causing this sync to decrease filled and retrigger extra orders.
            if sess_action in ("open", "close", "close_limit") or sess_action.startswith("cycle_"):
                continue
            old_filled = _sess.get("filled", {}).get(aid, 0)
            if ea_pos == old_filled:
                continue
            # Safety: never decrease the filled count via auto-sync.
            # A decrease means the broker hasn't fully propagated the new position yet.
            # Only upward corrections (recovering from a missed fill callback) are safe.
            if ea_pos < old_filled:
                logger.debug("[%s] Bridge auto-sync sid=%s: suppressing downward correction %d -> %d (broker lag)",
                             aid, _sid[:8], old_filled, ea_pos)
                continue
            logger.info("[%s] Bridge auto-sync sid=%s: filled %d -> %d (broker confirmed)", aid, _sid[:8], old_filled, ea_pos)
            _sess.setdefault("filled", {})[aid] = ea_pos
            _needs_save = True
        if _needs_save:
            save_fn = dd.get("save_sessions")
            if save_fn:
                save_fn()

    def get_positions_for_import(self, pair_filter="", comment_filter=""):
        """Get open positions in import-compatible format."""
        params = []
        if pair_filter:
            params.append(f"pair={pair_filter}")
        if comment_filter:
            params.append(f"comment={comment_filter}")
        qs = "&".join(params)
        path = f"/api/accounts/{self.account_id}/import"
        if qs:
            path += f"?{qs}"
        return _get(path, timeout=10) or []

    def get_deal_history(self, from_ts, to_ts, fee_keywords=None, exclude_balance=True):
        """Get deal history PnL totals."""
        eb = "true" if exclude_balance else "false"
        url = f"/api/accounts/{self.account_id}/history?from={int(from_ts)}&to={int(to_ts)}&exclude_balance={eb}"
        if fee_keywords:
            kw_str = ",".join(fee_keywords) if isinstance(fee_keywords, (list, tuple)) else str(fee_keywords)
            if kw_str.strip():
                url += f"&fee_keywords={urllib.parse.quote(kw_str)}"
        result = _get(url, timeout=30)
        if result and "error" in result:
            if "404" in str(result['error']):
                logger.warning(f"[{self.account_id}] Bridge returned 404 for deal history (not connected?)")
            else:
                logger.error(f"Bridge returned error: {result['error']}")
            return None
        if result:
            # Post-process by_symbol: bridge returns deal 'count' per symbol.
            # Use net PnL (pnl + swap + fees) so per-pair figures match the parent report's Net PnL.
            # hedge_lots = count / 2  (1 buy deal + 1 sell deal = 1 hedge lot)
            by_sym = result.get("by_symbol")
            if isinstance(by_sym, dict):
                for sym, sv in by_sym.items():
                    if not isinstance(sv, dict):
                        continue
                    count    = sv.get("count", 0) or 0
                    gross    = float(sv.get("pnl",  0) or 0)
                    swap     = float(sv.get("swap", 0) or 0)
                    fees     = float(sv.get("fees", 0) or 0)
                    net      = gross + swap + fees          # true net per pair
                    hedge_lots = round(count / 2.0, 2)
                    sv["net_pnl"]    = round(net, 2)       # net field â€” do NOT overwrite pnl (avoids double-count in aggregation)
                    sv["hedge_lots"] = hedge_lots
                    sv["pnl_per_lot"] = round(net / hedge_lots, 2) if hedge_lots > 0 else 0.0
            return result
        return None

    def get_symbol_info(self, symbol):
        """Get bid/ask/spread for a symbol."""
        result = _get(f"/api/accounts/{self.account_id}/quote/{symbol}", timeout=5)
        if result and "error" in result:
            if "404" not in str(result['error']):
                logger.error(f"Bridge returned error: {result['error']}")
            return None
        if result:
            return result
        return None

    def get_quote_direct(self, symbol):
        """Get live bid/ask â€” same as get_symbol_info via bridge."""
        return self.get_symbol_info(symbol)

    def get_swap_rates(self, symbols):
        """Get swap rates for symbols."""
        sym_str = ",".join(symbols) if isinstance(symbols, (list, tuple)) else str(symbols)
        result = _get(f"/api/accounts/{self.account_id}/swaps?symbols={sym_str}", timeout=10)
        if result and "error" in result:
            if "404" in str(result['error']):
                logger.warning(f"[{self.account_id}] Bridge returned 404 for swap rates (not connected?)")
            else:
                logger.error(f"Bridge returned error: {result['error']}")
            return {}
        if result:
            return result
        return {}

    def _get_open_orders(self):
        """Get open positions from bridge in the expected dict format."""
        try:
            positions = _get(f"/api/accounts/{self.account_id}/positions", timeout=10)
            if not positions or not isinstance(positions, list):
                return []
            result = []
            for p in positions:
                result.append({
                    'Ticket': int(p.get("ticket", 0)),
                    'Symbol': str(p.get("symbol", "")),
                    'Type': str(p.get("side", "")),
                    'Lots': float(p.get("lots", 0)),
                    'Comment': str(p.get("comment", "")),
                    'OpenPrice': float(p.get("open_price", p.get("openPrice", 0))),
                    'OpenTime': str(p.get("open_time", p.get("openTime", ""))),
                    'Profit': float(p.get("profit", 0)),
                    'Swap': float(p.get("swap", 0)),
                })
            return result
        except Exception as e:
            logger.error("[%s] Bridge _get_open_orders error: %s", self.account_id, e)
            return []

    def _report_result(self, session_id, status, ticket, detail="", fill_price=0, quote_price=0):
        """Report trade result back to dashboard."""
        try:
            report_fn = self.dd.get("report_trade_result")
            if report_fn:
                report_fn({
                    "session_id": session_id,
                    "account": self.account_id,
                    "status": status,
                    "ticket": _normalize_ticket(ticket) if ticket else 0,
                    "spread": 0,
                    "detail": detail,
                    "fill_price": fill_price,
                    "quote_price": quote_price,
                })
        except Exception as e:
            logger.error("[%s] Bridge report result error: %s", self.account_id, e)

    def send_market_order(self, symbol, side, lots, session_id="", comment=""):
        """Send a market order. Returns (success, ticket, price)."""
        data = {
            "symbol": symbol,
            "side": side,
            "lots": lots,
            "sessionId": session_id,
            "comment": comment
        }
        result = _post(f"/api/accounts/{self.account_id}/order", data, timeout=120)
        if result and result.get("success"):
            ticket = result.get("ticket", 0)
            price = result.get("open_price", 0)
            quote_price = result.get("quote_price", 0)
            self._report_result(session_id, "filled", ticket, fill_price=price, quote_price=quote_price)
            return (True, ticket, price)
        else:
            detail = result.get("error", "Unknown error") if result else "Connection failed"
            self._report_result(session_id, "error", 0, detail=detail)
            return (False, 0, 0)

    def close_position(self, ticket, symbol, side, lots, session_id="", comment=""):
        """Close a position by ticket. Returns (success, ticket, close_price)."""
        data = {
            "ticket": ticket,
            "symbol": symbol,
            "side": side,
            "lots": lots,
            "sessionId": session_id,
            "comment": comment
        }
        result = _post(f"/api/accounts/{self.account_id}/close", data, timeout=120)
        if result and result.get("success"):
            close_ticket = result.get("ticket", 0)
            price = result.get("close_price", 0)
            quote_price = result.get("quote_price", 0)
            
            # Determine rollback status
            status = "closed"
            if session_id:
                sessions_dict = self.dd.get("sessions", {})
                session = sessions_dict.get(session_id)
                if session and session.get("rollback_needed", {}).get(self.account_id, 0) > 0:
                    status = "rollback_closed"
            
            self._report_result(session_id, status, close_ticket, fill_price=price, quote_price=quote_price)
            return (True, close_ticket, price)
        else:
            detail = result.get("error", "Unknown error") if result else "Connection failed"
            self._report_result(session_id, "error", ticket, detail=detail)
            return (False, 0, 0)

    def modify_position_tp(self, ticket, symbol, side, lots, tp, sl=None, price=None):
        """Modify TakeProfit/StopLoss for an existing position."""
        data = {
            "ticket": ticket,
            "symbol": symbol,
            "side": side,
            "lots": lots,
            "sl": sl,
            "tp": tp,
            "price": price
        }
        result = _post(f"/api/accounts/{self.account_id}/modify", data, timeout=60)
        if result and result.get("success"):
            return True, ticket
        return False, result.get("error", "Unknown error") if result else "Connection failed"

    def modify_limit_price(self, ticket, symbol, side, lots, price):
        """Modify the entry price of a pending limit order."""
        return self.modify_position_tp(ticket, symbol, side, lots, tp=None, sl=None, price=price)

    def send_limit_order(self, symbol, side, lots, price, limit_type, session_id="", comment=""):
        """Send a limit/pending order."""
        # Capture current positions to know what's new
        _prev_positions = set(
            int(p.get("Ticket", p.get("ticket", 0)))
            for p in self._get_open_orders()
        )

        data = {
            "symbol": symbol,
            "side": side,
            "lots": lots,
            "price": price,
            "type": limit_type,
            "sessionId": session_id,
            "comment": comment
        }
        result = _post(f"/api/accounts/{self.account_id}/order", data, timeout=120)
        if result and result.get("success"):
            ticket = result.get("ticket", 0)
            open_price = result.get("open_price", 0)
            quote_price = result.get("quote_price", 0)
            self._report_result(session_id, "limit_placed", ticket, fill_price=open_price, quote_price=quote_price)

            # ── Background fill-watcher (Bridge Version) ─────────────────────
            _watch_symbol = symbol
            _watch_lots = float(lots)
            
            def _watch_limit_fill():
                import time as _time
                _timeout = 86400  # watch for up to 24h
                _start = _time.time()
                _poll_interval = 2.0
                logger.info("[%s] LIMIT-WATCH: watching ticket=%d for fill (symbol=%s lots=%.2f)",
                            self.account_id, ticket, _watch_symbol, _watch_lots)
                
                sym_upper = _watch_symbol.upper().replace(".", "")
                while _time.time() - _start < _timeout:
                    _time.sleep(_poll_interval)
                    if not self._running:
                        return
                    try:
                        orders = self._get_open_orders()
                        new_ticket = 0
                        new_price = price
                        
                        # Find new position matching symbol and lots
                        for o in orders:
                            t = int(o.get('Ticket', 0))
                            if t == 0 or t in _prev_positions:
                                continue
                            o_sym = str(o.get('Symbol', '')).upper().replace(".", "")
                            if sym_upper not in o_sym and o_sym not in sym_upper:
                                continue
                            
                            # Match by lots (within 1% tolerance)
                            o_lots = float(o.get('Lots', 0))
                            if abs(o_lots - _watch_lots) > _watch_lots * 0.01 + 0.001:
                                continue
                            
                            new_ticket = t
                            new_price = float(o.get('OpenPrice', price))
                            break
                            
                        # Fallback: any new position on this symbol
                        if new_ticket == 0:
                            for o in orders:
                                t = int(o.get('Ticket', 0))
                                if t == 0 or t in _prev_positions:
                                    continue
                                o_sym = str(o.get('Symbol', '')).upper().replace(".", "")
                                if sym_upper in o_sym or o_sym in sym_upper:
                                    new_ticket = t
                                    new_price = float(o.get('OpenPrice', price))
                                    break
                                    
                        if new_ticket > 0:
                            logger.info("[%s] LIMIT-WATCH: FILLED! pending=%d -> position=%d @ %.5f",
                                        self.account_id, ticket, new_ticket, new_price)
                            self._report_result(session_id, "filled", new_ticket,
                                                fill_price=new_price, quote_price=new_price)
                            return
                            
                    except Exception as _e:
                        logger.debug("[%s] LIMIT-WATCH poll error: %s", self.account_id, _e)
                logger.warning("[%s] LIMIT-WATCH: timed out watching ticket=%d", self.account_id, ticket)

            import threading as _threading
            _t = _threading.Thread(target=_watch_limit_fill, daemon=True,
                                   name=f"BridgeLimitWatch-{self.account_id}-{ticket}")
            _t.start()
            # ─────────────────────────────────────────────────────────────────

            return (True, ticket, open_price)
        else:
            detail = result.get("error", "Unknown error") if result else "Connection failed"
            self._report_result(session_id, "error", 0, detail=detail)
            return (False, 0, 0)

    def subscribe_symbol(self, symbol):
        """Ensure the symbol is watched (update ea_account_info with it)."""
        info = self.dd["ea_account_info"].get(self.account_id, {})
        info["symbol"] = symbol
        self.dd["ea_account_info"][self.account_id] = info
        sym_info = self.get_symbol_info(symbol)
        if sym_info:
            for k, v in sym_info.items():
                if v is not None:
                    info[k] = v


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# MtBridgeManager â€” drop-in replacement for MTDirectManager
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class MtBridgeManager:
    """
    Manages MT4/MT5 accounts via the MtBridgeService HTTP API.
    Same interface as MTDirectManager.
    """

    CONFIG_FILE = "mt_direct_accounts.json"

    def __init__(self, dashboard_data, config_dir="."):
        self.dd = dashboard_data
        self.config_dir = config_dir
        self.accounts = {}
        self._running = False
        self._command_thread = None
        # Don't auto-start in __init__ â€” dashboard calls .start() explicitly
    
    def start(self):
        """Start the bridge and load config."""
        if self._running:
            return  # already started
        if not ensure_bridge_running():
            logger.error("MtBridgeService failed to start â€” MT Direct disabled")
            return
        self._running = True
        self._load_config()

        # Start the Python-side background command processor thread (same as MTDirectManager)
        self._command_thread = threading.Thread(
            target=self._command_loop, daemon=True, name="BridgeCommandLoop"
        )
        self._command_thread.start()

    def _command_loop(self):
        """Background thread executing orders from dashboard shared data (dd)."""
        logger.info("Bridge background command processor thread started")
        while self._running:
            try:
                # Wait for wake signal or timeout
                _quote_wakeup.wait(timeout=1.0)
                _quote_wakeup.clear()
                
                # Check and process commands
                had_cycle = self._process_commands()
                if had_cycle:
                    # Immediately re-run command check to process next step without delay
                    _quote_wakeup.set()
            except Exception as e:
                logger.error("Bridge command loop error: %s", e)
                time.sleep(1.0)

    def _process_commands(self):
        """
        Scan active sessions and execute actions.
        Matches MTDirectManager._process_commands logic and layout.
        """
        should_issue = self.dd.get("should_issue_command")
        if not should_issue:
            return False

        # Phase 1: Collect commands under the dashboard lock (fast â€” no broker calls)
        pending_commands = []  # list of (direct_acct, session, session_id, account_id, pair, lot_size, comment, result, action, side_info)

        with self.dd["lock"]:
            for session_id, session in self.dd["sessions"].items():
                if session.get("status") not in ("active", "partial_close"):
                    continue
                sides = session.get("sides", {})
                for account_id in sides:
                    # Only process accounts managed by this manager
                    direct_acct = self.accounts.get(account_id)
                    if not direct_acct:
                        continue
                    if not direct_acct.connected:
                        continue

                    # Ensure symbol is subscribed
                    side_info = sides[account_id]
                    pair = (side_info.get("pair") or session.get("pair", "")).strip()
                    if pair:
                        direct_acct.subscribe_symbol(pair)

                    # Mirror ea_account_info from Direct account's internal key to the session's raw account_id
                    internal_id = direct_acct.account_id
                    if internal_id != account_id:
                        src = self.dd["ea_account_info"].get(internal_id, {})
                        if src:
                            dst = self.dd["ea_account_info"].get(account_id, {})
                            dst.update(src)
                            self.dd["ea_account_info"][account_id] = dst
                            self.dd["ea_heartbeats"][account_id] = time.time()

                    # Debug: log rollback state before calling should_issue
                    rb = session.get("rollback_needed", {})
                    if rb.get(account_id, 0) > 0:
                        logger.info("[%s] ROLLBACK PENDING: rb_needed=%s rb_tickets=%s",
                                    account_id, rb, session.get("rollback_tickets", {}))

                    result = should_issue(session, account_id)

                    # Debug: log should_issue result for rollback cases
                    if rb.get(account_id, 0) > 0:
                        logger.info("[%s] should_issue returned: %s (status=%s action=%s)",
                                    account_id, result, session.get("status"), session.get("action"))

                    # Debug: log should_issue result for cycle actions
                    action_dbg = session.get("action", "")
                    if action_dbg.startswith("cycle_"):
                        logger.info("[%s] CYCLE-TRACE: should_issue=%s action=%s phase=%s idx=%s",
                                    account_id, result, action_dbg,
                                    session.get("cycle_progress", {}).get("phase"),
                                    session.get("cycle_progress", {}).get("index"))

                    if result is False:
                        continue

                    lot_size = side_info.get("lot_size") or session.get("lot_size", 0.01)
                    comment = side_info.get("comment", "") or session.get("comment", "")
                    max_spread = side_info.get("max_spread") if side_info.get("max_spread") is not None else session.get("max_spread_points")
                    try:
                        max_spread = float(max_spread) if max_spread is not None else None
                    except (ValueError, TypeError):
                        max_spread = None

                    # Check spread gating — bypass for rollback, cycle close, and cycle_limit_open
                    is_cycle_reopen = (session.get("action", "").startswith("cycle_") and
                                       session.get("cycle_progress", {}).get("phase") == "open")
                    if result not in ("rollback", "cycle_close", "cycle_limit_close", "cycle_limit_open") and not is_cycle_reopen:
                        current_spread = None
                        session_pair = pair
                        acct_obj = self.accounts.get(account_id)

                        # Query real-time quote via bridge quote cache
                        if acct_obj and session_pair:
                            try:
                                quote = acct_obj.get_quote_direct(session_pair)
                                if quote:
                                    current_spread = quote.get("spread")
                            except Exception:
                                pass

                        # Fallback to ea_account_info
                        if current_spread is None:
                            ea_info = self.dd["ea_account_info"].get(account_id, {})
                            current_spread = ea_info.get("spread")

                        if current_spread is None:
                            logger.info("[%s] Spread gate: no quotes for %s, skipping", account_id, pair)
                            continue
                        if max_spread is not None and current_spread > max_spread:
                            logger.info("[%s] Spread gate: spread %.1f > max %s for %s", account_id, current_spread, max_spread, pair)
                            session.setdefault("spread_rejects", {})[account_id] = session.get("spread_rejects", {}).get(account_id, 0) + 1
                            continue

                    logger.info("[%s] PASSED all gates for %s — executing order", account_id, pair)

                    # Mark in-flight
                    self.dd["in_flight_commands"][(session_id, account_id)] = time.time()

                    action = session.get("action", "open")

                    # Queue the command for execution outside the lock
                    pending_commands.append((
                        direct_acct, session, session_id, account_id,
                        pair, lot_size, comment, result, action, side_info
                    ))

        # Phase 2: Execute broker commands OUTSIDE the lock
        had_cycle = False
        for (direct_acct, session, session_id, account_id,
             pair, lot_size, comment, result, action, side_info) in pending_commands:
            try:
                if result == "rollback":
                    self._send_close_command(direct_acct, session, account_id, pair, lot_size, comment)
                elif result == "cycle_close":
                    self._send_close_command(direct_acct, session, account_id, pair, lot_size, comment)
                elif result == "cycle_limit_close":
                    limit_dist = session.get("cycle_limit_distance")
                    limit_dist = 10 if limit_dist is None or limit_dist == "" else float(limit_dist)
                    self._send_close_command(direct_acct, session, account_id, pair, lot_size, comment,
                                             is_limit=True, limit_dist=limit_dist, limit_batch=1, limit_days=0)
                elif result == "cycle_limit_open":
                    # Cycle-limit REOPEN: send batch_size passive limit orders to re-enter positions
                    trade_side = side_info.get("action", "buy")
                    limit_dist = session.get("cycle_limit_distance")
                    limit_dist = 10 if limit_dist is None or limit_dist == "" else float(limit_dist)
                    closed_tickets = session.get("cycle_progress", {}).get("closed_tickets", [])
                    batch_size = len(closed_tickets) if closed_tickets else int(session.get("cycle_limit_batch_size", 1))
                    
                    prog = session.get("cycle_progress", {})
                    already_placed = prog.get("limit_placed_this_batch", 0)
                    to_place = batch_size - already_placed
                    
                    if to_place <= 0:
                        logger.info("[%s] cycle_limit_open: target %d already placed in this batch, skipping", account_id, batch_size)
                        had_cycle = True
                        continue

                    # Fetch initial quote for sanity check before loop
                    quote = direct_acct.get_quote_direct(pair)
                    if not quote:
                        logger.warning("[%s] cycle_limit_open: no quote for %s — clearing in-flight", account_id, pair)
                        self.dd["in_flight_commands"].pop((session_id, account_id), None)
                    else:
                        is_jpy = "JPY" in pair.upper()
                        pip_mult = 1000.0 if is_jpy else 100000.0
                        limit_type = "BuyLimit" if trade_side == "buy" else "SellLimit"
                        any_placed = False
                        placed_now = 0
                        if "MT5" in account_id.upper():
                            import threading
                            threads = []
                            results = [False] * to_place
                            def _place_init_limit(i, lim_pr, b_pr):
                                logger.info("[%s] cycle_limit_open [%d/%d]: placing %s at %.5f (dist=%s pips, base=%.5f)",
                                            account_id, already_placed + i + 1, batch_size, limit_type, lim_pr, limit_dist, b_pr)
                                order_result = direct_acct.send_limit_order(
                                    pair, trade_side, lot_size, lim_pr, limit_type,
                                    session_id=session_id, comment=comment
                                )
                                if isinstance(order_result, tuple) and not order_result[0]:
                                    logger.warning("[%s] cycle_limit_open [%d/%d]: limit order failed", account_id, already_placed + i + 1, batch_size)
                                else:
                                    results[i] = True
                            for i in range(to_place):
                                fresh_quote = direct_acct.get_quote_direct(pair) or quote
                                base_price = fresh_quote.get("ask", 0) if trade_side == "buy" else fresh_quote.get("bid", 0)
                                lim_price = base_price - (limit_dist / pip_mult) if trade_side == "buy" else base_price + (limit_dist / pip_mult)
                                lim_price = round(lim_price, 3 if is_jpy else 5)
                                t = threading.Thread(target=_place_init_limit, args=(i, lim_price, base_price))
                                threads.append(t)
                                t.start()
                            for t in threads:
                                t.join()
                            placed_now = sum(results)
                            any_placed = placed_now > 0
                        else:
                            for i in range(to_place):
                                # Re-fetch quote on EVERY order — price can tick between placements
                                # and stale prices cause "Invalid price in the request" broker rejection
                                fresh_quote = direct_acct.get_quote_direct(pair) or quote
                                base_price = fresh_quote.get("ask", 0) if trade_side == "buy" else fresh_quote.get("bid", 0)
                                limit_price = base_price - (limit_dist / pip_mult) if trade_side == "buy" else base_price + (limit_dist / pip_mult)
                                limit_price = round(limit_price, 3 if is_jpy else 5)
                                logger.info("[%s] cycle_limit_open [%d/%d]: placing %s at %.5f (dist=%s pips, base=%.5f)",
                                            account_id, already_placed + i + 1, batch_size, limit_type, limit_price, limit_dist, base_price)
                                order_result = direct_acct.send_limit_order(
                                    pair, trade_side, lot_size, limit_price, limit_type,
                                    session_id=session_id, comment=comment
                                )
                                if isinstance(order_result, tuple) and not order_result[0]:
                                    logger.warning("[%s] cycle_limit_open [%d/%d]: limit order failed", account_id, already_placed + i + 1, batch_size)
                                else:
                                    any_placed = True
                                    placed_now += 1
                                
                        if placed_now > 0:
                            prog["limit_placed_this_batch"] = already_placed + placed_now
                            session["cycle_progress"] = prog
                            
                        if not any_placed:
                            logger.warning("[%s] cycle_limit_open: all %d limit orders failed — clearing in-flight", account_id, to_place)
                            self.dd["in_flight_commands"].pop((session_id, account_id), None)
                        else:
                            had_cycle = True
                elif action == "close" or action.startswith("close_limit"):
                    limit_dist = session.get("limit_distance")
                    limit_dist = 100 if limit_dist is None or limit_dist == "" else float(limit_dist)
                    limit_batch = session.get("limit_batch_size") or 1
                    limit_days = session.get("limit_days") or 0
                    
                    target_side_num = 1 if "acc1" in action else (2 if "acc2" in action else 0)
                    my_side_num = side_info.get("side_number", 0)
                    is_limit_side = action.startswith("close_limit") and ((target_side_num == 0) or (target_side_num == my_side_num))
                    
                    self._send_close_command(direct_acct, session, account_id, pair, lot_size, comment, is_limit=is_limit_side, limit_dist=limit_dist, limit_batch=limit_batch, limit_days=limit_days)
                elif action == "open" or (action.startswith("cycle_") and result is True) or action.startswith("open_limit"):
                    # Normal open OR cycle reopen phase OR open_limit
                    trade_side = side_info.get("action", "buy")
                    
                    target_side_num = 1 if "acc1" in action else (2 if "acc2" in action else 0)
                    my_side_num = side_info.get("side_number", 0)
                    is_limit_side = action.startswith("open_limit") and ((target_side_num == 0) or (target_side_num == my_side_num))
                    
                    if is_limit_side:
                        limit_dist = session.get("limit_distance")
                        limit_dist = 100 if limit_dist is None or limit_dist == "" else float(limit_dist)
                        quote = direct_acct.get_quote_direct(pair)
                        base_price = (quote.get("ask", 0) if trade_side == "buy" else quote.get("bid", 0)) if quote else 0
                        is_jpy = "JPY" in pair.upper()
                        pip_mult = 1000.0 if is_jpy else 100000.0
                        limit_price = base_price - (limit_dist / pip_mult) if trade_side == "buy" else base_price + (limit_dist / pip_mult)
                        limit_price = round(limit_price, 3 if is_jpy else 5)
                        limit_type = "BuyLimit" if trade_side == "buy" else "SellLimit"
                        order_result = direct_acct.send_limit_order(
                            pair, trade_side, lot_size, limit_price, limit_type,
                            session_id=session_id, comment=comment
                        )
                    else:
                        order_result = direct_acct.send_market_order(
                            pair, trade_side, lot_size,
                            session_id=session_id, comment=comment
                        )

                    if isinstance(order_result, tuple) and not order_result[0]:
                        logger.warning("[%s] Order failed (returned False) â€” clearing in-flight", account_id)
                        self.dd["in_flight_commands"].pop((session_id, account_id), None)
                    elif action.startswith("cycle_"):
                        had_cycle = True
                else:
                    logger.warning("[%s] Unknown action=%s result=%s â€” skipping",
                                   account_id, action, result)
            except Exception as e:
                logger.error("[%s] Command execution error: %s", account_id, e)
                # Clear in-flight so the command can be retried on next loop
                self.dd["in_flight_commands"].pop((session_id, account_id), None)
        return had_cycle

    def _send_close_command(self, direct_acct, session, account_id, pair, lot_size, comment, is_limit=False, limit_dist=0, limit_batch=1, limit_days=0):
        """Send a close order for the oldest open position tracked in fills."""
        fills = session.get("fills", [])
        close_fills = session.get("close_fills", [])
        closed_tickets = {f["ticket"] for f in close_fills if f.get("account") == account_id}

        # Check if there's a specific rollback ticket to close
        rb_tickets = session.get("rollback_tickets", {}).get(account_id, [])
        if rb_tickets:
            first_ticket = rb_tickets[0]
            if isinstance(first_ticket, dict):
                ticket = first_ticket.get("ticket")
                custom_lots = first_ticket.get("lots")
                if custom_lots:
                    lot_size = custom_lots
            else:
                ticket = first_ticket

            side_info = session.get("sides", {}).get(account_id, {})
            original_side = side_info.get("action", "buy")

            # Look up actual position volume from broker â€” MT5 rejects mismatched lots
            actual_lots = lot_size
            try:
                orders = direct_acct._get_open_orders()
                for o in orders:
                    # Don't try to parse "missing_X" dummy tickets
                    if not str(ticket).startswith("missing_") and o.get('Ticket') and int(o['Ticket']) == int(ticket):
                        if o.get('Lots'):
                            actual_lots = float(o['Lots'])
                        elif o.get('Volume'):
                            actual_lots = float(o['Volume'])
                        break
            except Exception as e:
                logger.error("[%s] Could not look up actual lots for ticket %s: %s",
                             account_id, ticket, e)

            logger.info("[%s] ROLLBACK CLOSE: ticket=%s pair=%s side=%s lots=%s (session_lots=%s)",
                        account_id, ticket, pair, original_side, actual_lots, lot_size)
            direct_acct.close_position(
                ticket, pair, original_side, actual_lots,
                session_id=session.get("id", ""), comment=comment
            )
            return

        # For cycle_close: use cycle_progress index to close the correct fill (oldest first)
        action = session.get("action", "")
        if action.startswith("cycle_"):
            progress = session.get("cycle_progress", {})
            idx = progress.get("index", 0)
            cycle_account = session.get("cycle_account", "")
            if cycle_account == account_id:
                closed_tickets_cycle = {
                    str(f["ticket"]) for f in close_fills
                    if f.get("account") == account_id
                }
                acct_fills = [
                    f for f in fills
                    if f.get("account") == account_id
                    and str(f.get("ticket")) not in closed_tickets_cycle
                ]
                # Sort oldest-first to match _should_issue_command's sorted order —
                # progress["index"] was set based on sorted acct_fills, so we must
                # use the same sort here to close the correct position.
                def _fill_sort_key(f):
                    # Sort by (ts_epoch, ticket) — must match _should_issue_command's sort
                    # exactly so that progress["index"] refers to the same fill in both places.
                    # Do NOT parse the ts string here — ts_epoch is set at import/fill time
                    # and is reliable. Using time.mktime() for string fallback would apply
                    # local machine timezone instead of the broker's EET offset, causing
                    # the two sorted lists to diverge.
                    ep = f.get("ts_epoch", 0) or 0
                    ticket = int(f.get("ticket") or 0)
                    return (ep, ticket)
                acct_fills.sort(key=_fill_sort_key)
                if idx < len(acct_fills):
                    side_info = session.get("sides", {}).get(account_id, {})
                    original_side = side_info.get("action", "buy")
                    batch_size = int(session.get("cycle_limit_batch_size", 1))

                    if is_limit:
                        # CYCLE-LIMIT: modify TakeProfit to passive limit price for all batch tickets
                        quote = direct_acct.get_quote_direct(pair)
                        base_price = (quote.get("bid", 0) if original_side == "buy" else quote.get("ask", 0)) if quote else 0
                        pip_mult = 1000.0 if "JPY" in pair.upper() else 100000.0
                        limit_price = base_price + (limit_dist / pip_mult) if original_side == "buy" else base_price - (limit_dist / pip_mult)

                        any_tp_ok = False
                        all_gone = True  # True if every ticket in batch is gone (already closed)
                        for i in range(batch_size):
                            if idx + i >= len(acct_fills):
                                break
                            fill = acct_fills[idx + i]
                            ticket = fill.get("ticket")

                            # Look up actual position volume from broker
                            actual_lots = lot_size
                            try:
                                orders = direct_acct._get_open_orders()
                                for o in orders:
                                    if o.get('Ticket') and int(o['Ticket']) == int(ticket):
                                        if o.get('Lots'):
                                            actual_lots = float(o['Lots'])
                                        elif o.get('Volume'):
                                            actual_lots = float(o['Volume'])
                                        break
                            except Exception as e:
                                logger.error("[%s] Could not look up lots for cycle ticket %s: %s",
                                             account_id, ticket, e)

                            logger.info("[%s] CYCLE-LIMIT CLOSE: modifying TP ticket=%s pair=%s side=%s lots=%s TP=%.5f (batch item %d, fill #%d of %d)",
                                        account_id, ticket, pair, original_side, actual_lots, limit_price,
                                        i + 1, idx + i + 1, len(acct_fills))
                            tp_result = direct_acct.modify_position_tp(ticket, pair, original_side, actual_lots, limit_price)
                            tp_ok = isinstance(tp_result, tuple) and tp_result[0]
                            msg = str(tp_result[1]) if isinstance(tp_result, tuple) and len(tp_result) > 1 else str(tp_result)

                            if not tp_ok and ("No changes" in msg or "no changes" in msg.lower()):
                                logger.info("[%s] CYCLE-LIMIT TP already at %.5f for ticket %s - treating as success", account_id, limit_price, ticket)
                                tp_ok = True

                            if tp_ok:
                                any_tp_ok = True
                                all_gone = False
                            elif "Ticket not found" in msg or "Invalid ticket" in msg:
                                logger.info("[%s] CYCLE-LIMIT ticket %s is gone — already closed", account_id, ticket)
                                # Ticket is gone — already closed, that's OK
                            else:
                                logger.warning("[%s] CYCLE-LIMIT: TP modify failed for ticket %s (result=%s)",
                                               account_id, ticket, tp_result)
                                all_gone = False

                        if all_gone and not any_tp_ok:
                            # All tickets in this batch are already gone — advance to open phase
                            logger.info("[%s] CYCLE-LIMIT: all batch tickets gone — advancing to open phase", account_id)
                            with self.dd["lock"]:
                                prog = session.get("cycle_progress", {})
                                prog["phase"] = "open"
                                prog["cycle_close_ts"] = time.time()
                                prog.pop("close_tp_set", None)
                                prog.pop("close_tp_set_ts", None)
                                prog.pop("close_tp_confirmed", None)
                                prog.pop("open_dispatched", None)
                                prog.pop("open_fill_received", None)
                                batch_tickets = [acct_fills[idx + i].get("ticket") for i in range(batch_size) if idx + i < len(acct_fills)]
                                prog["closed_tickets"] = batch_tickets
                                for ticket in batch_tickets:
                                    session.setdefault("close_fills", []).append({
                                        "account": account_id,
                                        "ticket": ticket,
                                        "price": limit_price,
                                        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                                        "ts_epoch": time.time(),
                                        "external": True,
                                    })
                                session["cycle_progress"] = prog
                            self.dd["in_flight_commands"].pop((session.get("id", ""), account_id), None)
                        elif any_tp_ok:
                            # At least one TP set successfully — mark as broker-confirmed
                            with self.dd["lock"]:
                                prog = session.get("cycle_progress", {})
                                prog["close_tp_set"] = True
                                prog["close_tp_set_ts"] = time.time()
                                prog["close_tp_confirmed"] = True
                                prog["close_tp_price"] = limit_price
                                
                                batch_tickets = [acct_fills[idx + i].get("ticket") for i in range(batch_size) if idx + i < len(acct_fills)]
                                prog["closed_tickets"] = batch_tickets
                                
                                session["cycle_progress"] = prog
                            self.dd["in_flight_commands"].pop((session.get("id", ""), account_id), None)
                            save_fn = self.dd.get("save_sessions")
                            if save_fn:
                                save_fn()
                            logger.info("[%s] CYCLE-LIMIT: TP confirmed at %.5f for batch — in-flight cleared, watchdog active",
                                        account_id, limit_price)
                        else:
                            # All failed for non-gone reasons — clear in-flight for retry
                            logger.warning("[%s] CYCLE-LIMIT: all TP modifies failed — clearing in-flight for retry", account_id)
                            self.dd["in_flight_commands"].pop((session.get("id", ""), account_id), None)
                        # We don't append to close_limit_fills because cycle tracking handles state
                    else:
                        fill = acct_fills[idx]
                        ticket = fill.get("ticket")
                        actual_lots = lot_size
                        try:
                            orders = direct_acct._get_open_orders()
                            for o in orders:
                                if o.get('Ticket') and int(o['Ticket']) == int(ticket):
                                    if o.get('Lots'):
                                        actual_lots = float(o['Lots'])
                                    elif o.get('Volume'):
                                        actual_lots = float(o['Volume'])
                                    break
                        except Exception as e:
                            logger.error("[%s] Could not look up lots for cycle ticket %s: %s",
                                         account_id, ticket, e)
                        logger.info("[%s] CYCLE CLOSE: ticket=%s pair=%s side=%s lots=%s (fill #%d of %d)",
                                    account_id, ticket, pair, original_side, actual_lots,
                                    idx + 1, len(acct_fills))
                        direct_acct.close_position(
                            ticket, pair, original_side, actual_lots,
                            session_id=session.get("id", ""), comment=comment
                        )
                    return
                else:
                    logger.warning("[%s] CYCLE: idx=%d >= acct_fills=%d â€” nothing to close",
                                   account_id, idx, len(acct_fills))
                    return

        # Otherwise close oldest open fill (or limit batch)
        # Filter for batch closing
        eligible_fills = []
        now_epoch = time.time()
        for fill in fills:
            if fill.get("account") != account_id:
                continue
            ticket = fill.get("ticket")
            if ticket in closed_tickets:
                continue
            
            # Age filter for limit orders
            if is_limit and limit_days > 0:
                ts_epoch = fill.get("ts_epoch", 0)
                if ts_epoch:
                    days_held = (now_epoch - ts_epoch) / (24 * 3600)
                    if days_held < limit_days:
                        continue
                        
            eligible_fills.append(fill)

        batch_count = int(limit_batch) if is_limit else 1
        processed_count = 0

        # Calculate single limit price for the whole batch
        batch_limit_price = None
        if is_limit and eligible_fills:
            quote = direct_acct.get_quote_direct(pair)
            side_info = session.get("sides", {}).get(account_id, {})
            original_side = side_info.get("action", "buy")
            base_price = (quote.get("bid", 0) if original_side == "buy" else quote.get("ask", 0)) if quote else 0
            is_jpy = "JPY" in pair.upper()
            pip_mult = 1000.0 if is_jpy else 100000.0
            limit_price = base_price + (limit_dist / pip_mult) if original_side == "buy" else base_price - (limit_dist / pip_mult)
            batch_limit_price = round(limit_price, 3 if is_jpy else 5)

        all_placed = True
        any_placed = False

        for fill in eligible_fills:
            if processed_count >= batch_count:
                break

            ticket = fill.get("ticket")
            side_info = session.get("sides", {}).get(account_id, {})
            original_side = side_info.get("action", "buy")

            # Look up actual position volume from broker — MT5 rejects mismatched lots
            actual_lots = lot_size
            ticket_found = False
            try:
                orders = direct_acct._get_open_orders()
                for o in orders:
                    if o.get('Ticket') and int(o['Ticket']) == int(ticket):
                        ticket_found = True
                        if o.get('Lots'):
                            actual_lots = float(o['Lots'])
                        elif o.get('Volume'):
                            actual_lots = float(o['Volume'])
                        break
            except Exception as e:
                logger.error("[%s] Could not look up actual lots for ticket %s: %s",
                             account_id, ticket, e)
                # If we can't check, try anyway
                ticket_found = True

            if not ticket_found:
                # Safety: if broker returned zero open orders, don't auto-skip
                try:
                    orders = direct_acct._get_open_orders()
                    if len(orders) <= 0:
                        logger.warning("[%s] SKIP ABORTED: broker has 0 open orders — likely disconnected, "
                                       "not marking ticket %s as closed", account_id, ticket)
                        break
                except Exception:
                    pass

                # Ticket genuinely gone — mark as closed, but only ONE per call
                session.setdefault("close_fills", []).append({
                    "ticket": ticket, "account": account_id,
                    "time": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "note": "auto-skipped: not in broker open orders"
                })
                session["closed"][account_id] = session.get("closed", {}).get(account_id, 0) + 1
                break  # Only handle ONE auto-skip per call

            if is_limit:
                logger.info("[%s] CYCLE-LIMIT CLOSE: modifying TP ticket=%s pair=%s side=%s lots=%s TP=%s (batch item %d)",
                            account_id, ticket, pair, original_side, actual_lots, batch_limit_price, processed_count + 1)
                
                res = direct_acct.modify_position_tp(
                    ticket, pair, original_side, actual_lots, batch_limit_price
                )
                
                if isinstance(res, tuple) and not res[0]:
                    logger.warning("[%s] CYCLE-LIMIT: TP modify failed for ticket %s (result=%s)", account_id, ticket, res)
                    all_placed = False
                else:
                    any_placed = True
                    # Mark as 'close_limit_placed' so we don't keep modifying it
                    session.setdefault("close_limit_fills", []).append({
                        "account": account_id,
                        "ticket": ticket,
                        "tp": batch_limit_price,
                        "lots": actual_lots,
                        "ts_epoch": time.time()
                    })
            else:
                logger.info("[%s] CLOSE: ticket=%s pair=%s side=%s lots=%s",
                            account_id, ticket, pair, original_side, actual_lots)
                direct_acct.close_position(
                    ticket, pair, original_side, actual_lots,
                    session_id=session.get("id", ""), comment=comment
                )

            processed_count += 1

        if is_limit:
            if any_placed and all_placed:
                # CYCLE-LIMIT: Mark TP as confirmed so watchdog can take over
                if session.get("action", "").startswith("cycle_limit_"):
                    with self.dd["lock"]:
                        prog = session.get("cycle_progress", {})
                        prog["close_tp_set"] = True
                        prog["close_tp_set_ts"] = time.time()
                        prog["close_tp_confirmed"] = True
                        prog["close_tp_price"] = batch_limit_price
                        session["cycle_progress"] = prog
                    save_fn = self.dd.get("save_sessions")
                    if save_fn:
                        save_fn()
                self.dd["in_flight_commands"].pop((session.get("id", ""), account_id), None)
            else:
                # All failed or partial success - clear in-flight so it retries with fresh price
                logger.warning("[%s] CYCLE-LIMIT: Not all TPs placed successfully, clearing in-flight for retry", account_id)
                self.dd["in_flight_commands"].pop((session.get("id", ""), account_id), None)

    def _load_config(self):
        """Load Direct accounts from config file and connect via bridge."""
        config_path = os.path.join(self.config_dir, self.CONFIG_FILE)
        if not os.path.exists(config_path):
            logger.warning("Config file not found: %s", config_path)
            return

        with open(config_path, "r") as f:
            configs = json.load(f)

        logger.info("Loading %d MT Direct accounts via bridge", len(configs))

        # Tell bridge to load config directly (it handles staggered connects)
        result = _post("/api/config/load", {"path": os.path.abspath(config_path)})
        loaded = result.get("loaded", 0)
        logger.info("Bridge loaded %d accounts", loaded)

        # Create local MtBridgeAccount wrappers for each
        for account_id, cfg in configs.items():
            acct = MtBridgeAccount(account_id, cfg, self.dd)
            # Bridge handles staggered async connects; we start the Python-side
            # heartbeat immediately so it will pick up the connection once the
            # bridge finishes the handshake.  The heartbeat skips position pushes
            # until connected=True, so no stale data from a previous session
            # will be injected during the startup window.
            acct._running = True
            acct._start_heartbeat()
            self.accounts[account_id] = acct

    def save_config(self):
        """Save config â€” delegates to bridge."""
        # 1. Write JSON directly from Python -- authoritative on restart
        config_path = os.path.join(self.config_dir, self.CONFIG_FILE)
        _configs = {aid: a.config for aid, a in self.accounts.items()}
        try:
            import tempfile as _tmpfile
            _dir = os.path.dirname(os.path.abspath(config_path))
            with _tmpfile.NamedTemporaryFile("w", dir=_dir, delete=False, suffix=".tmp") as _tf:
                json.dump(_configs, _tf, indent=2)
                _tmp = _tf.name
            try:
                os.replace(_tmp, config_path)
            except Exception:
                if os.path.exists(_tmp): os.unlink(_tmp)
                raise
            logger.info("MtBridgeManager: saved %d accounts to %s", len(_configs), config_path)
        except Exception as _e:
            logger.error("Failed to save MT Bridge config: %s", _e)
        # 2. Also sync to bridge in-memory state
        for account_id, acct in self.accounts.items():
            cfg = {
                "id": account_id,
                "platform": acct.config.get("type", acct.config.get("platform", "mt5")),
                "login": int(acct.config.get("login", 0)),
                "password": str(acct.config.get("password", "")),
                "server": str(acct.config.get("server", "")),
                "port": int(acct.config.get("port", 443)),
                "label": acct.config.get("label", ""),
                "autoConnect": acct.config.get("auto_connect_start", True),
                "lotMultiplier": acct.config.get("lot_multiplier", 100000),
                "extra": acct.config,
            }
            _put(f"/api/accounts/{account_id}", cfg)
            

        _post("/api/config/save", {"path": os.path.abspath(config_path)})

    def add_account(self, account_id, config, save=True, auto_connect=True):
        """Add a new MT Direct account. Returns True on success."""
        if account_id in self.accounts:
            return False

        acct = MtBridgeAccount(account_id, config, self.dd)
        self.accounts[account_id] = acct

        cfg = {
            "id": account_id,
            "platform": config.get("type", config.get("platform", "mt5")),
            "login": int(config.get("login", 0)),
            "password": str(config.get("password", "")),
            "server": str(config.get("server", "")),
            "port": int(config.get("port", 443)),
            "label": config.get("label", ""),
            "autoConnect": config.get("auto_connect_start", True),
            "lotMultiplier": config.get("lot_multiplier", 100000),
            "extra": config,
        }
        _post("/api/accounts", cfg)

        if auto_connect:
            acct.start()
        if save:
            self.save_config()
        return True

    def remove_account(self, account_id):
        """Stop and remove a Direct account. Returns True on success."""
        if account_id in self.accounts:
            self.accounts[account_id].stop()
            del self.accounts[account_id]
        _delete(f"/api/accounts/{account_id}")
        # Persist the removal so the account does not reappear on restart
        self.save_config()
        # Clean up stale dashboard data
        self.dd.get("ea_account_info", {}).pop(account_id, None)
        self.dd.get("ea_heartbeats", {}).pop(account_id, None)
        return True

    def connect_account(self, account_id):
        """Connect a specific account. Returns (ok, error_msg) tuple."""
        acct = self.accounts.get(account_id)
        if not acct:
            return False, "Account not found"
        ok = acct.start()
        if ok:
            return True, None
        return False, acct._last_error or "Connection failed (unknown error)"

    def disconnect_account(self, account_id):
        """Disconnect a specific account. Returns True on success."""
        if account_id in self.accounts:
            self.accounts[account_id].stop()
            return True
        return False

    def get_status(self):
        """Get status of all Direct accounts. Same shape as MTDirectManager."""
        result = {}
        for acct_id, acct in self.accounts.items():
            info = self.dd.get("ea_account_info", {}).get(acct_id, {})
            pos = info.get("positions", {})
            pos_count = len(pos) if isinstance(pos, dict) else 0
            # Compute totals from position data
            t_pnl = info.get("total_pnl")
            t_swap = info.get("total_swap")
            t_lots = info.get("total_lots")
            if t_pnl is None and isinstance(pos, dict):
                t_pnl = sum(p.get("profit", 0) + p.get("swap", 0) for p in pos.values())
            if t_swap is None and isinstance(pos, dict):
                t_swap = sum(p.get("swap", 0) for p in pos.values())
            if t_lots is None and isinstance(pos, dict):
                # Signed lots: type 0 (buy) = positive, type 1 (sell) = negative
                t_lots = sum(p.get("lots", 0) if p.get("type") == 0 else -p.get("lots", 0)
                             for p in pos.values())
            result[acct_id] = {
                "label": acct.label,
                "type": acct.conn_type,
                "connected": acct.connected,
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "margin": info.get("margin"),
                "leverage": info.get("leverage"),
                "total_pnl": round(t_pnl, 2) if t_pnl is not None else None,
                "total_swap": round(t_swap, 2) if t_swap is not None else None,
                "total_lots": round(t_lots, 2) if t_lots is not None else None,
                "positions": pos_count,
                "server": acct.config.get("server", ""),
                "login": acct.config.get("login", ""),
                "last_error": acct._last_error,
                "group_label": acct.config.get("group_label"),
                "alert_email": acct.config.get("alert_email"),
                "alert_telegram": acct.config.get("alert_telegram"),
                "auto_connect_start": acct.config.get("auto_connect_start", True),
            }
        return result

    def stop(self):
        """Stop all accounts."""
        self._running = False
        for acct in self.accounts.values():
            acct.stop()

