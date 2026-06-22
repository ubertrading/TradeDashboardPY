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

            time.sleep(15)  # 15s sync interval

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
        acct["balance"] = info.get("balance", 0)
        acct["equity"] = info.get("equity", 0)
        acct["margin"] = info.get("margin", 0)
        acct["free_margin"] = info.get("free_margin", 0)
        acct["profit"] = info.get("profit", 0)
        acct["total_pnl"] = round(info.get("equity", 0) - info.get("balance", 0), 2)
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
        if isinstance(positions, list) and len(positions) == 0 and not self._connected:
            logger.debug("[%s] _push_positions: skipping empty list while not connected",
                         self.account_id)
            return

        dd = self.dd
        aid = self.account_id

        if aid not in dd.get("ea_account_info", {}):
            dd.setdefault("ea_account_info", {})[aid] = {}

        acct = dd["ea_account_info"][aid]
        pos_dict = {}
        total_swap = 0.0
        tickets = []
        pos_details = []
        _lbi = {}  # lots_by_instrument: symbol -> {"buy": x, "sell": y}
        for p in positions:
            ticket = p.get("ticket", 0)
            side = p.get("side", "").lower()
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
            self._report_result(session_id, "filled", ticket, fill_price=price)
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
            
            # Determine rollback status
            status = "closed"
            if session_id:
                sessions_dict = self.dd.get("sessions", {})
                session = sessions_dict.get(session_id)
                if session and session.get("rollback_needed", {}).get(self.account_id, 0) > 0:
                    status = "rollback_closed"
            
            self._report_result(session_id, status, close_ticket, fill_price=price)
            return (True, close_ticket, price)
        else:
            detail = result.get("error", "Unknown error") if result else "Connection failed"
            self._report_result(session_id, "error", ticket, detail=detail)
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
                    max_spread = side_info.get("max_spread") if side_info.get("max_spread") is not None else session.get("max_spread_points", 999)
                    try:
                        max_spread = float(max_spread) if max_spread is not None else 999
                    except (ValueError, TypeError):
                        max_spread = 999

                    # Check spread gating â€“ bypass for rollback and cycle reopen
                    is_cycle_reopen = (session.get("action", "").startswith("cycle_") and
                                       session.get("cycle_progress", {}).get("phase") == "open")
                    if result != "rollback" and result != "cycle_close" and not is_cycle_reopen:
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
                        if max_spread > 0 and current_spread > max_spread:
                            logger.info("[%s] Spread gate: spread %.1f > max %s for %s", account_id, current_spread, max_spread, pair)
                            session.setdefault("spread_rejects", {})[account_id] = session.get("spread_rejects", {}).get(account_id, 0) + 1
                            continue

                    logger.info("[%s] PASSED all gates for %s â€” executing order", account_id, pair)

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
                elif action == "close":
                    self._send_close_command(direct_acct, session, account_id, pair, lot_size, comment)
                elif action == "open" or (action.startswith("cycle_") and result is True):
                    # Normal open OR cycle reopen phase
                    trade_side = side_info.get("action", "buy")
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

    def _send_close_command(self, direct_acct, session, account_id, pair, lot_size, comment):
        """Send a close order for the oldest open position tracked in fills."""
        fills = session.get("fills", [])
        close_fills = session.get("close_fills", [])
        closed_tickets = {f["ticket"] for f in close_fills if f.get("account") == account_id}

        # Check if there's a specific rollback ticket to close
        rb_tickets = session.get("rollback_tickets", {}).get(account_id, [])
        if rb_tickets:
            ticket = rb_tickets[0]
            side_info = session.get("sides", {}).get(account_id, {})
            original_side = side_info.get("action", "buy")

            # Look up actual position volume from broker â€” MT5 rejects mismatched lots
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
                # Sort oldest-first to match _should_issue_command's sorted order
                def _fill_sort_key(f):
                    ts_str = f.get("ts", "")
                    if ts_str:
                        for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S", "%Y.%m.%d %H:%M:%S", "%Y.%m.%d %H:%M"):
                            try:
                                return time.mktime(time.strptime(ts_str, fmt))
                            except (ValueError, TypeError):
                                continue
                    return f.get("ts_epoch", 0) or 0
                acct_fills.sort(key=_fill_sort_key)
                if idx < len(acct_fills):
                    fill = acct_fills[idx]
                    ticket = fill.get("ticket")
                    side_info = session.get("sides", {}).get(account_id, {})
                    original_side = side_info.get("action", "buy")

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

        # Otherwise close oldest open fill
        for fill in fills:
            if fill.get("account") != account_id:
                continue
            ticket = fill.get("ticket")
            if ticket in closed_tickets:
                continue
            side_info = session.get("sides", {}).get(account_id, {})
            original_side = side_info.get("action", "buy")

            # Look up actual position volume from broker â€” MT5 rejects mismatched lots
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
                if len(orders) <= 0:
                    logger.warning("[%s] SKIP ABORTED: broker has 0 open orders â€” likely disconnected, "
                                   "not marking ticket %s as closed", account_id, ticket)
                    break

                # Ticket genuinely gone â€” mark as closed, but only ONE per call
                order_tickets = sorted([o.get('Ticket', 0) for o in orders])
                t_min = order_tickets[0] if order_tickets else '?'
                t_max = order_tickets[-1] if order_tickets else '?'
                logger.info("[%s] SKIP: ticket=%s not found in %d broker orders "
                            "(ticket range %s..%s) â€” marking as closed",
                            account_id, ticket, len(orders), t_min, t_max)
                print(f"[AUTO-SKIP] {account_id}: ticket {ticket} not on broker, auto-marking closed")
                session.setdefault("close_fills", []).append({
                    "ticket": ticket, "account": account_id,
                    "time": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "note": "auto-skipped: not in broker open orders"
                })
                session["closed"][account_id] = session.get("closed", {}).get(account_id, 0) + 1
                break  # Only handle ONE auto-skip per call

            logger.info("[%s] CLOSE: ticket=%s pair=%s side=%s lots=%s",
                        account_id, ticket, pair, original_side, actual_lots)
            direct_acct.close_position(
                ticket, pair, original_side, actual_lots,
                session_id=session.get("id", ""), comment=comment
            )
            break

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
            
        config_path = os.path.join(self.config_dir, self.CONFIG_FILE)
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

