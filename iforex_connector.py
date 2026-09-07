#!/usr/bin/env python3
"""
iforex_connector.py — iFOREX WebPL4 Direct API Connector

Connects directly to iFOREX WebPL4 endpoints (trader.iforex.com) using pure HTTP:
- Feeds quotes/account data into ea_account_info + ea_heartbeats for TradeDashboardPY
- Provides open_order() and close_order() mapped to /webpl4/Deals/OpenDeal and /webpl4/Deals/CloseDeals
- Queries margin requirements and active positions
- Implements the standard connector interface matching MTDirectManager & FixAccountManager
"""

import os
import sys
import json
import time
import logging
import threading
import requests
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone

logger = logging.getLogger("iforex_connector")

# ─── Instrument ID Mappings ──────────────────────────────────────────────────
INSTRUMENT_MAP = {
    "EURUSD": 3631,
    "EUR/USD": 3631,
    "USDJPY": 12055,
    "USD/JPY": 12055,
    "GBPUSD": 4143,
    "GBP/USD": 4143,
    "AUDUSD": 303,
    "AUD/USD": 303,
    "NZDUSD": 7727,
    "NZD/USD": 7727,
    "USDCAD": 12035,
    "USD/CAD": 12035,
    "USDCHF": 12036,
    "USD/CHF": 12036,
    "GOLD": 13103,
    "XAUUSD": 13103,
    "GOLD/USD": 13103,
    "OIL": 24623,
    "OIL/USD": 24623,
}

REVERSE_INSTRUMENT_MAP = {
    3631: "EURUSD",
    12055: "USDJPY",
    4143: "GBPUSD",
    303: "AUDUSD",
    7727: "NZDUSD",
    12035: "USDCAD",
    12036: "USDCHF",
    13103: "GOLD",
    24623: "OIL"
}


# ─── Notional Quantity & Lot Conversion Helpers ──────────────────────────────
FOREX_CURRENCIES = {
    "USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF",
    "NOK", "SEK", "SGD", "HKD", "TRY", "ZAR", "MXN", "PLN",
    "CZK", "DKK", "HUF", "CNH", "ILS"
}

def is_forex_pair(symbol: str) -> bool:
    """Check if an instrument symbol represents a Forex currency pair."""
    sym_clean = str(symbol).upper().replace("/", "").replace(" ", "").replace("-", "").replace(".", "")
    if any(k in sym_clean for k in ("GOLD", "XAU", "SILVER", "XAG", "OIL", "WTI", "BRENT", "US30", "US500", "NAS100", "DE30", "DE40", "UK100", "BTC", "ETH")):
        return False
    if len(sym_clean) == 6 and sym_clean[:3] in FOREX_CURRENCIES and sym_clean[3:] in FOREX_CURRENCIES:
        return True
    if "/" in str(symbol):
        parts = str(symbol).upper().split("/")
        if len(parts) == 2 and parts[0] in FOREX_CURRENCIES and parts[1] in FOREX_CURRENCIES:
            return True
    if sym_clean in ("EURUSD", "USDJPY", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF"):
        return True
    return True

def lot_to_notional(symbol: str, lot_or_amount: float) -> int:
    """
    Convert lot size or amount to notional quantity for iFOREX orders.
    For Forex pairs: 1 lot = 100,000 units of base currency (e.g. 1 lot USD/JPY = 100,000 USD).
    If lot_or_amount < 500, it is treated as standard lot size (e.g. 1.0 -> 100,000, 0.1 -> 10,000).
    If lot_or_amount >= 500, it is assumed to be already in notional units.
    """
    if lot_or_amount <= 0:
        return 0
    if is_forex_pair(symbol):
        if lot_or_amount < 500.0:
            return int(round(lot_or_amount * 100000.0))
        return int(round(lot_or_amount))
    else:
        if lot_or_amount < 500.0:
            return int(round(lot_or_amount * 100.0))
        return int(round(lot_or_amount))

def notional_to_lots(symbol: str, amount: float) -> float:
    """
    Convert notional unit amount back to lot size for display and dashboard accounting.
    For Forex pairs: 100,000 units = 1.0 lot.
    """
    if amount <= 0:
        return 0.0
    if is_forex_pair(symbol):
        if amount >= 500.0:
            return round(amount / 100000.0, 4)
        return round(amount, 4)
    return round(amount, 4)


# ─── Account Class ──────────────────────────────────────────────────────────
class IForexAccount:
    def __init__(self, config: Dict[str, Any], dashboard_data: Dict[str, Any]):
        self.config = config
        self.account_id = str(config.get("account_id", "IFOREX_01"))
        self.label = config.get("label", self.account_id)
        self.account_number = str(config.get("account_number", "12279333"))
        self.cookie = config.get("cookie", "")
        self.security_token = config.get("security_token", 0)
        self.base_url = config.get("base_url", "https://trader.iforex.com/webpl4")
        self.conn_type = "iforex_direct"
        self.connected = False
        self.dd = dashboard_data
        
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36 Edg/152.0.0.0",
            "Referer": "https://trader.iforex.com/webpl4/trading/new-transaction",
            "Origin": "https://trader.iforex.com",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "sec-ch-ua": '"Chromium";v="152", "Not?A_Brand";v="24", "Microsoft Edge";v="152"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin"
        })
        if self.cookie:
            self.session.headers["Cookie"] = self.cookie

        self._quotes_cache: Dict[str, Dict[str, float]] = {}
        self._margin_cache: Dict[int, float] = {}
        self._open_orders: List[Dict[str, Any]] = []
        self._seen_ticket_ids: set = set()
        self._account_summary: Dict[str, float] = {}
        self.is_market_closed: bool = False
        self._last_401_ts = 0.0
        self._running = False
        self._poll_thread = None
        self._pos_sync_thread = None
        self._lock = threading.Lock()

    def start(self):
        """Start the background polling loop and position sync loop."""
        self._running = True
        self.connected = True
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True, name=f"iforex_poll_{self.account_id}")
        self._poll_thread.start()
        self._pos_sync_thread = threading.Thread(target=self._position_sync_loop, daemon=True, name=f"iforex_possync_{self.account_id}")
        self._pos_sync_thread.start()
        logger.info("[%s] iFOREX Direct polling and position sync threads started", self.account_id)

    def stop(self):
        """Stop background polling and position sync."""
        self._running = False
        self.connected = False
        with self._lock:
            self._quotes_cache.clear()
        if "ea_account_info" in self.dd and self.account_id in self.dd["ea_account_info"]:
            self.dd["ea_account_info"][self.account_id]["connected"] = False

    def fetch_quote_live(self, symbol: str) -> Optional[Tuple[float, float]]:
        """Actively fetch live bid/ask from iFOREX WebPL4 for symbol."""
        if not self.connected and self._last_401_ts > 0 and (time.time() - self._last_401_ts) < 30:
            return None

        sym_clean = symbol.upper().replace("/", "").replace(" ", "").replace("-", "").replace(".", "")
        inst_id = INSTRUMENT_MAP.get(sym_clean)
        if not inst_id:
            return None

        # 1. Primary: FeedsHistory/GetTicks (real live ticks: rateType 1 = Bid, rateType 0 = Ask)
        url_ticks = f"{self.base_url}/FeedsHistory/GetTicks"
        try:
            r_bid = self.session.get(url_ticks, params={"instrumentId": inst_id, "numTicks": 1, "rateType": 1}, timeout=3.0)
            r_ask = self.session.get(url_ticks, params={"instrumentId": inst_id, "numTicks": 1, "rateType": 0}, timeout=3.0)
            if r_bid.status_code == 200 and r_ask.status_code == 200:
                self.connected = True
                b_ticks = r_bid.json().get("Ticks", [])
                a_ticks = r_ask.json().get("Ticks", [])
                if b_ticks and a_ticks:
                    bid = float(b_ticks[-1][1])
                    ask = float(a_ticks[-1][1])
                    if bid > 0 and ask > 0:
                        with self._lock:
                            self._quotes_cache[sym_clean] = {"bid": bid, "ask": ask, "ts": time.time()}
                        return (bid, ask)
            elif r_bid.status_code == 401 or r_ask.status_code == 401:
                self.connected = False
                now_ts = time.time()
                if now_ts - self._last_401_ts > 45:
                    self._last_401_ts = now_ts
                    logger.warning("[%s] iFOREX session expired (401 Unauthorized) — initiating auto-recovery...", self.account_id)
                    self._trigger_auto_relogin()
                return None
        except Exception as e:
            logger.debug("[%s] FeedsHistory/GetTicks error for %s: %s", self.account_id, symbol, e)

        # 2. Fallback to GetDealMarginDetails
        url_margin = f"{self.base_url}/Deals/GetDealMarginDetails"
        try:
            resp = self.session.get(url_margin, params={"instrumentId": inst_id}, timeout=3.0)
            if resp.status_code == 200:
                self.connected = True
                try:
                    data = resp.json()
                    res = data.get("Result", {}) or data
                    rate = float(res.get("SpotRate") or res.get("Rate") or res.get("MarketRate") or 0.0)
                    if rate > 0:
                        spread = 0.00015 if "JPY" not in sym_clean and "GOLD" not in sym_clean else (0.015 if "JPY" in sym_clean else 0.3)
                        bid = round(rate - spread / 2, 5)
                        ask = round(rate + spread / 2, 5)
                        with self._lock:
                            self._quotes_cache[sym_clean] = {"bid": bid, "ask": ask, "ts": time.time()}
                        return (bid, ask)
                except Exception:
                    pass
            elif resp.status_code == 401:
                self.connected = False
                now_ts = time.time()
                if now_ts - self._last_401_ts > 45:
                    self._last_401_ts = now_ts
                    logger.warning("[%s] iFOREX session expired (401) on margin check — initiating auto-recovery...", self.account_id)
                    self._trigger_auto_relogin()
        except Exception:
            pass

        return None

    def _trigger_auto_relogin(self):
        """Trigger background Playwright auto-login if credentials or profile exist."""
        if getattr(self, "_relogin_in_progress", False):
            return
        self._relogin_in_progress = True

        def _worker():
            try:
                from iforex_auto_login import refresh_iforex_session
                logger.info("[%s] Triggering automatic iFOREX session recovery in background...", self.account_id)
                res = refresh_iforex_session(
                    account_id=self.account_id,
                    username=self.config.get("username"),
                    password=self.config.get("password"),
                    timeout_sec=90,
                    headless=True
                )
                if res.get("status") == "ok":
                    self.cookie = res["cookie"]
                    self.session.headers["Cookie"] = res["cookie"]
                    self.security_token = res["security_token"]
                    self.connected = True
                    self._last_401_ts = 0.0
                    with self._lock:
                        self._quotes_cache.clear()
                    if "ea_account_info" in self.dd:
                        self.dd["ea_account_info"].setdefault(self.account_id, {})["connected"] = True
                    logger.info("[%s] Automatic session recovery succeeded! Reconnected to iFOREX.", self.account_id)
                else:
                    logger.warning("[%s] Headless auto-recovery did not complete: %s (click 'Re-Auth' in UI)",
                                   self.account_id, res.get("message"))
            except Exception as e:
                logger.error("[%s] Background auto-login error: %s", self.account_id, e)
            finally:
                self._relogin_in_progress = False

        threading.Thread(target=_worker, daemon=True, name=f"iforex_reauth_{self.account_id}").start()

    def get_quote(self, symbol: str, allow_live: bool = True) -> Optional[Tuple[float, float]]:
        """Return (bid, ask) for symbol from cache or live fetch if allow_live=True."""
        sym_clean = symbol.upper().replace("/", "").replace(" ", "").replace("-", "").replace(".", "")
        with self._lock:
            q = self._quotes_cache.get(sym_clean)
            if q and (time.time() - q.get("ts", 0)) < 10.0:
                return (q["bid"], q["ask"])
        if allow_live:
            return self.fetch_quote_live(symbol)
        return None

    def get_deal_margin_details(self, symbol_or_id: Any) -> Optional[Dict[str, Any]]:
        """Query margin requirements for an instrument."""
        if not self.connected and self._last_401_ts > 0 and (time.time() - self._last_401_ts) < 30:
            return None

        inst_id = INSTRUMENT_MAP.get(str(symbol_or_id).upper().replace("/", "").replace(" ", ""), symbol_or_id)
        url = f"{self.base_url}/Deals/GetDealMarginDetails"
        try:
            resp = self.session.get(url, params={"instrumentId": inst_id}, timeout=3.0)
            if resp.status_code == 200:
                self.connected = True
                try:
                    data = resp.json()
                    res = data.get("Result", {})
                    if "MarginPercentage" in res:
                        with self._lock:
                            self._margin_cache[inst_id] = float(res["MarginPercentage"])
                    return data
                except Exception:
                    return {"raw": resp.text}
            elif resp.status_code == 401:
                self.connected = False
                now_ts = time.time()
                if now_ts - self._last_401_ts > 45:
                    self._last_401_ts = now_ts
                    logger.warning("[%s] iFOREX session expired (401 Unauthorized) — initiating auto-recovery...", self.account_id)
                    self._trigger_auto_relogin()
        except Exception as e:
            self.connected = False
            logger.error("[%s] GetDealMarginDetails error: %s", self.account_id, e)
        return None

    def get_deal_risk_details(self, symbol_or_id: Any) -> Optional[Dict[str, Any]]:
        """Query risk and max exposure details."""
        inst_id = INSTRUMENT_MAP.get(str(symbol_or_id).upper().replace("/", ""), symbol_or_id)
        url = f"{self.base_url}/Deals/GetDealRiskDetails"
        try:
            resp = self.session.get(url, params={"instrumentId": inst_id}, timeout=3.0)
            if resp.status_code == 200:
                self.connected = True
                return resp.json()
        except Exception as e:
            logger.error("[%s] GetDealRiskDetails error: %s", self.account_id, e)
        return None

    def get_overnight_financing(self, symbol_or_id: Any, amount: float = 1.0) -> Optional[Dict[str, Any]]:
        """Query long/short overnight swap/financing rates."""
        inst_id = INSTRUMENT_MAP.get(str(symbol_or_id).upper().replace("/", ""), symbol_or_id)
        notional_amount = lot_to_notional(str(symbol_or_id), amount)
        url = f"{self.base_url}/Deals/GetOvernightFinancing"
        try:
            resp = self.session.get(url, params={"instrumentId": inst_id, "amount": int(notional_amount)}, timeout=3.0)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error("[%s] GetOvernightFinancing error: %s", self.account_id, e)
        return None

    def open_order(self, symbol: str, direction: str, amount: float, market_rate: float, 
                   other_rate: Optional[float] = None, tp_rate: float = 0.0, sl_rate: float = 0.0) -> Dict[str, Any]:
        """
        Open a new position directly via HTTP POST.
        amount: lot size or notional quantity (for Forex pairs, 1 lot = 100,000 units of base currency)
        """
        sym_clean = symbol.upper().replace("/", "")
        inst_id = INSTRUMENT_MAP.get(sym_clean, 3631)
        dir_code = 1 if str(direction).upper() in ("BUY", "1", "OP_BUY") else 0
        if other_rate is None:
            other_rate = market_rate

        notional_amount = lot_to_notional(symbol, amount)

        url = f"{self.base_url}/Deals/OpenDeal"
        payload = {
            "DealType": 2,  # Spot
            "InstrumentId": inst_id,
            "Amount": int(notional_amount),
            "MarketRate": market_rate,
            "OtherRateSeen": other_rate,
            "OrderDirection": dir_code,
            "TakeProfitRate": tp_rate,
            "StopLossRate": sl_rate,
            "SecurityToken": self.security_token
        }
        
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        logger.info("[%s] Submitting OpenDeal (%s %s, input=%s -> notional=%d): %s", 
                    self.account_id, symbol, direction, amount, notional_amount, payload)
        try:
            resp = self.session.post(url, data=payload, headers=headers, timeout=5.0)
            logger.info("[%s] OpenDeal response (%d): %s", self.account_id, resp.status_code, resp.text)
            if resp.status_code in (401, 403, 503) or "session has been terminated" in resp.text.lower():
                self.connected = False
                now_ts = time.time()
                if now_ts - getattr(self, "_last_401_ts", 0) > 30:
                    self._last_401_ts = now_ts
                    logger.warning("[%s] iFOREX session terminated on OpenDeal (%d) — triggering auto-relogin...", self.account_id, resp.status_code)
                    self._trigger_auto_relogin()
                return {"status": "error", "message": "iFOREX session terminated — auto-relogin triggered", "raw": resp.text}
            try:
                res_json = resp.json()
                is_ok = (resp.status_code == 200 and
                         isinstance(res_json, dict) and
                         res_json.get("status") == 1 and
                         (res_json.get("itemId") or 0) > 0)
                if is_ok:
                    return {"status": "ok", "data": res_json, "raw": resp.text}
                else:
                    err_msg = res_json.get("result") if isinstance(res_json, dict) else resp.text
                    if "OrderError103" in str(err_msg) or "market is closed" in str(resp.text).lower():
                        self.is_market_closed = True
                        if "ea_account_info" in self.dd:
                            self.dd["ea_account_info"].setdefault(self.account_id, {})["market_closed"] = True
                    return {"status": "error", "message": str(err_msg), "data": res_json, "raw": resp.text}
            except Exception:
                return {"status": "error", "message": "Invalid JSON response", "raw": resp.text}
        except Exception as e:
            logger.error("[%s] OpenDeal exception: %s", self.account_id, e)
            return {"status": "error", "message": str(e)}

    def close_order(self, position_number: Any, spot_rate: float, fw_pips: float = 0.0) -> Dict[str, Any]:
        """
        Close an active deal directly via HTTP POST.
        position_number: Deal position ticket ID
        spot_rate: Current closing market rate
        """
        url = f"{self.base_url}/Deals/CloseDeals"
        positions_str = f"{position_number}#{spot_rate}#{fw_pips}"
        payload = {
            "positions": positions_str,
            "SecurityToken": self.security_token
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        logger.info("[%s] Submitting CloseDeals: %s", self.account_id, payload)
        try:
            resp = self.session.post(url, data=payload, headers=headers, timeout=5.0)
            logger.info("[%s] CloseDeals response (%d): %s", self.account_id, resp.status_code, resp.text)
            if resp.status_code in (401, 403, 503) or "session has been terminated" in resp.text.lower():
                self.connected = False
                now_ts = time.time()
                if now_ts - getattr(self, "_last_401_ts", 0) > 30:
                    self._last_401_ts = now_ts
                    logger.warning("[%s] iFOREX session terminated (%d) — triggering auto-relogin...", self.account_id, resp.status_code)
                    self._trigger_auto_relogin()
                return {"status": "error", "message": "iFOREX session terminated — auto-relogin triggered", "raw": resp.text}
            try:
                res_json = resp.json()
                # iFOREX returns HTTP 200 even for business-logic errors (e.g.
                # OrderError8 = "position already closed").  Check the response
                # body for status:0 / OrderError* to avoid treating duplicate
                # close attempts as successful closes.
                iforex_ok = resp.status_code == 200
                if iforex_ok and isinstance(res_json, list) and len(res_json) > 0:
                    item = res_json[0] if isinstance(res_json[0], dict) else {}
                    item_status = item.get("status")
                    item_result = str(item.get("result", ""))
                    if item_result == "OrderError8":
                        logger.info("[%s] CloseDeals ticket %s was already closed on broker (OrderError8) — treating as closed",
                                    self.account_id, position_number)
                        return {"status": "ok", "already_closed": True, "data": res_json, "raw": resp.text}
                    if item_status == 0 or item_result.startswith("OrderError"):
                        logger.warning("[%s] CloseDeals returned iFOREX error: status=%s result=%s (ticket=%s)",
                                        self.account_id, item_status, item_result, position_number)
                        return {"status": "error", "data": res_json, "raw": resp.text,
                                "iforex_error": item_result}
                return {"status": "ok" if iforex_ok else "error", "data": res_json, "raw": resp.text}
            except Exception:
                return {"status": "ok" if resp.status_code == 200 else "error", "raw": resp.text}
        except Exception as e:
            logger.error("[%s] CloseDeals exception: %s", self.account_id, e)
            return {"status": "error", "message": str(e)}

    def _get_open_orders(self) -> List[Dict[str, Any]]:
        """Return list of open orders in standard dictionary format."""
        with self._lock:
            return list(self._open_orders)

    def get_positions_for_import(self, pair_filter: str = "", comment_filter: str = "") -> List[Dict[str, Any]]:
        """
        Get open positions in import-compatible format for TradeDashboardPY.
        Matches MTDirectManager and FixAccountManager interface.
        iFOREX positions have no comments, so comment_filter is not enforced unless blank matching.
        """
        positions = []
        try:
            # 1. Check cached orders or wait briefly if background sync is actively populating
            orders = self._get_open_orders()
            if not orders:
                for _ in range(12):
                    time.sleep(0.5)
                    orders = self._get_open_orders()
                    if orders:
                        break
            # 2. If still empty, attempt direct fetch
            if not orders:
                try:
                    from iforex_auto_login import fetch_active_deals_and_summary
                    deals, summary = fetch_active_deals_and_summary(timeout_sec=20)
                    if deals is not None:
                        with self._lock:
                            self._open_orders = deals
                            if summary:
                                self._account_summary = summary
                        orders = deals
                except Exception as fe:
                    logger.warning("[%s] On-demand position fetch in get_positions_for_import: %s", self.account_id, fe)

            def _clean(s):
                return str(s or "").upper().replace("/", "").replace(".", "").replace(" ", "").replace("-", "")

            clean_pair = _clean(pair_filter)

            for o in orders:
                sym = str(o.get("Symbol") or o.get("symbol") or "").upper()
                clean_sym = _clean(sym)
                if clean_pair and not (clean_sym.startswith(clean_pair) or clean_pair.startswith(clean_sym)):
                    continue

                t = str(o.get("Ticket") or o.get("ticket") or "")
                direction = str(o.get("Type") or o.get("direction") or "buy").lower()
                side = "buy" if direction in ("buy", "1", "op_buy") else "sell"
                lots = float(o.get("Lots") or o.get("lots") or 0.01)
                open_price = float(o.get("OpenPrice") or o.get("open_price") or 0.0)
                open_time = str(o.get("OpenTime") or o.get("open_time") or "")
                open_epoch = o.get("open_epoch")

                positions.append({
                    "ticket": t,
                    "symbol": sym,
                    "lots": lots,
                    "side": side,
                    "comment": "",
                    "open_price": open_price,
                    "open_time": open_time,
                    "open_epoch": open_epoch,
                })
            logger.info("[%s] Import: found %d positions (pair=%s)", self.account_id, len(positions), pair_filter)
        except Exception as e:
            logger.error("[%s] get_positions_for_import error: %s", self.account_id, e)
        return positions

    def _poll_loop(self):
        """Periodic background poll for account data & heartbeats."""
        while self._running:
            try:
                # 1. Update Heartbeat
                if "ea_heartbeats" in self.dd:
                    self.dd["ea_heartbeats"][self.account_id] = time.time()
                
                # 2. Query EUR/USD margin to check connection & maintain session keep-alive
                self.get_deal_margin_details(3631)

                # 3. Poll active symbols from sessions for live quotes (non-blocking for dashboard lock)
                sessions = self.dd.get("sessions", {})
                active_pairs = set()
                for sess in sessions.values():
                    sides = sess.get("sides", {})
                    if self.account_id in sides:
                        pair = (sides[self.account_id].get("pair") or sess.get("pair", "")).strip()
                        if pair:
                            active_pairs.add(pair)
                
                if not active_pairs:
                    active_pairs.add("EUR/USD")

                for pair in active_pairs:
                    self.fetch_quote_live(pair)

                # 4. Update ea_account_info dictionary
                if "ea_account_info" in self.dd:
                    info = self.dd["ea_account_info"].setdefault(self.account_id, {})
                    info["conn_type"] = "iforex_direct"
                    info["account_number"] = self.account_number
                    info["connected"] = self.connected
                    
                    # Check weekend market hours (Friday >= 20:00 UTC through Sunday < 21:00 UTC)
                    now_utc = datetime.now(timezone.utc)
                    is_weekend = (now_utc.weekday() == 4 and now_utc.hour >= 20) or (now_utc.weekday() == 5) or (now_utc.weekday() == 6 and now_utc.hour < 21)
                    if is_weekend:
                        self.is_market_closed = True
                        self._market_closed_by_weekend = True
                    elif getattr(self, '_market_closed_by_weekend', False):
                        self.is_market_closed = False
                        self._market_closed_by_weekend = False
                    info["market_closed"] = self.is_market_closed
                    
                    # Margin & Balance
                    summ = getattr(self, "_account_summary", {})
                    if summ.get("balance", 0.0) > 0:
                        info["balance"] = summ["balance"]
                    else:
                        info.setdefault("balance", float(self.config.get("balance", 0.0)))

                    if summ.get("equity", 0.0) > 0:
                        info["equity"] = summ["equity"]
                    else:
                        info.setdefault("equity", float(self.config.get("equity", 0.0)))

                    if "margin" in summ and summ["margin"] is not None:
                        info["margin"] = summ["margin"]
                    else:
                        info.setdefault("margin", float(self.config.get("margin", 0.0)))

                    if "free_margin" in summ and summ["free_margin"] is not None:
                        info["free_margin"] = summ["free_margin"]
                    else:
                        info.setdefault("free_margin", float(self.config.get("free_margin", 0.0)))

                    info.setdefault("leverage", int(self.config.get("leverage", 400)))

                    # Update spread / bid / ask in info
                    syms_dict = info.setdefault("symbols", {})
                    for pair in active_pairs:
                        q = self.get_quote(pair, allow_live=False)
                        if q:
                            pip_mult = 100.0 if "JPY" in pair.upper() else 10000.0
                            spread_pts = round((q[1] - q[0]) * pip_mult, 1)
                            sym_clean = pair.upper().replace("/", "").replace(" ", "")
                            syms_dict[pair] = {"bid": q[0], "ask": q[1], "spread": spread_pts}
                            syms_dict[sym_clean] = {"bid": q[0], "ask": q[1], "spread": spread_pts}
                            if "symbol" not in info or info.get("symbol") == pair or not info.get("bid"):
                                info["bid"] = q[0]
                                info["ask"] = q[1]
                                info["spread"] = spread_pts
                                info["symbol"] = pair
                    
                    # Position tracking for hedge balancing
                    orders = self._get_open_orders()
                    tickets = [o.get("Ticket") for o in orders if o.get("Ticket")]
                    info["open_tickets"] = tickets
                    info["positions"] = len(tickets)
                    info["pos_details"] = orders
                    info["position_details"] = [
                        {
                            "ticket": str(o.get("Ticket")),
                            "symbol": o.get("Symbol", "EURUSD"),
                            "type": str(o.get("Type", "buy")),
                            "lots": float(o.get("Lots", 0.01)),
                            "open_price": float(o.get("OpenPrice", 0.0)),
                            "profit": float(o.get("Profit", 0.0))
                        }
                        for o in orders if o.get("Ticket")
                    ]

                    # Lots calculation
                    _lbi = {}
                    tot_lots = 0.0
                    for o in orders:
                        sym = o.get("Symbol", "Unknown")
                        lots = o.get("Lots")
                        if lots is None or lots == 0.0:
                            amt = float(o.get("Amount") or o.get("amount") or 0.0)
                            lots = notional_to_lots(sym, amt)
                        if sym not in _lbi:
                            _lbi[sym] = {"buy": 0.0, "sell": 0.0}
                        if str(o.get("Type", "")).lower() in ("buy", "0", "op_buy"):
                            _lbi[sym]["buy"] = round(_lbi[sym]["buy"] + lots, 2)
                            tot_lots += lots
                        else:
                            _lbi[sym]["sell"] = round(_lbi[sym]["sell"] + lots, 2)
                            tot_lots -= lots
                    info["lots_by_instrument"] = _lbi
                    info["total_lots"] = round(tot_lots, 2)
                    now_ts = time.time()
                    info["last_update"] = now_ts
                    if "ea_heartbeats" in self.dd:
                        self.dd["ea_heartbeats"][self.account_id] = now_ts

            except Exception as e:
                logger.debug("[%s] Poll loop error: %s", self.account_id, e)
            time.sleep(3.0)

    def _position_sync_loop(self):
        """
        Background thread that continuously synchronizes active positions from the iFOREX platform.
        Uses headless Playwright to inspect React DOM for live positions.
        If a position was closed externally in iFOREX GUI, this updates _open_orders,
        which immediately triggers ea_account_info update and hedge monitor rebalancing!
        """
        time.sleep(5)  # Initial wait for platform settlement
        _consecutive_empty = 0  # Debounce: require confirmation only for ambiguous empty reads
        _EMPTY_THRESHOLD = 2    # Only for ambiguous reads without clear summary margin
        _consecutive_sync_failures = 0
        while self._running:
            try:
                from iforex_auto_login import fetch_active_deals_and_summary
                deals, summary = fetch_active_deals_and_summary(timeout_sec=25)
                if not self._running:
                    break
                if deals is None:
                    _consecutive_sync_failures += 1
                    if _consecutive_sync_failures >= 3:
                        logger.warning("[%s] Position sync failed %d consecutive times — triggering auto-relogin",
                                       self.account_id, _consecutive_sync_failures)
                        self._trigger_auto_relogin()
                        _consecutive_sync_failures = 0
                else:
                    _consecutive_sync_failures = 0

                if summary:
                    with self._lock:
                        self._account_summary = summary
                    if summary.get("balance") is not None and summary.get("balance", 0.0) > 0:
                        self.config["balance"] = summary["balance"]
                    if summary.get("equity") is not None and summary.get("equity", 0.0) > 0:
                        self.config["equity"] = summary["equity"]
                    # Propagate market_closed from Playwright DOM check
                    if "market_closed" in summary:
                        self.is_market_closed = bool(summary["market_closed"])
                        if "ea_account_info" in self.dd:
                            self.dd["ea_account_info"].setdefault(self.account_id, {})["market_closed"] = self.is_market_closed
                if deals is not None:
                    # --- Sanity guard: never trust deals=[] if margin/open_pl says we're still in positions ---
                    _margin = summary.get("margin")
                    _open_pl = summary.get("open_pl")
                    _balance = summary.get("balance")
                    _has_valid_summary = (_balance is not None and _balance > 0 and _margin is not None)

                    _suspicious_empty = (len(deals) == 0 and _margin is not None and (abs(_margin) > 1.0 or (_open_pl is not None and abs(_open_pl) > 0.01)))
                    if _suspicious_empty:
                        _consecutive_empty = 0  # Reset — margin says we still have positions
                        logger.warning("[%s] deals=[] but margin=%s open_pl=%s — skipping update (likely render lag)",
                                       self.account_id, _margin, _open_pl)
                    elif len(deals) == 0 and self._open_orders:
                        # If summary margin and open_pl are zero AND summary is verified valid with positive balance,
                        # broker account confirms 0 positions — accept IMMEDIATELY!
                        if _has_valid_summary and abs(_margin) <= 0.01 and (_open_pl is None or abs(_open_pl) <= 0.01):
                            logger.info("[%s] deals=[] confirmed by margin=%.2f open_pl=%s balance=%.2f — accepting closure immediately",
                                        self.account_id, _margin, _open_pl, _balance)
                            _consecutive_empty = 0
                        else:
                            _consecutive_empty += 1
                            if _consecutive_empty < _EMPTY_THRESHOLD:
                                logger.warning("[%s] deals=[] (empty read %d/%d) — deferring position wipe",
                                               self.account_id, _consecutive_empty, _EMPTY_THRESHOLD)
                                deals = None  # Treat as None to skip update this cycle
                            else:
                                logger.info("[%s] deals=[] confirmed after %d consecutive reads — accepting closure",
                                            self.account_id, _consecutive_empty)
                                _consecutive_empty = 0
                    else:
                        _consecutive_empty = 0  # Non-empty result — reset counter

                if deals is not None:
                    with self._lock:
                        now_epoch = time.time()
                        deal_tickets = set(str(d.get("Ticket")) for d in deals if d.get("Ticket"))
                        # Track all tickets seen in broker deals
                        self._seen_ticket_ids.update(deal_tickets)

                        # Only preserve unrendered fresh orders that have NEVER appeared in broker deals yet,
                        # to allow brief DOM rendering lag right after OpenDeal returns (max 8s).
                        # Once an order has appeared in broker deals at least once, it is NEVER preserved if missing!
                        for fo in list(self._open_orders):
                            fo_t = str(fo.get("Ticket"))
                            fo_time = fo.get("open_epoch") or fo.get("OpenTime") or 0
                            if not isinstance(fo_time, (int, float)):
                                fo_time = 0
                            if (fo_t and fo_t not in deal_tickets and 
                                fo_t not in self._seen_ticket_ids and 
                                (now_epoch - fo_time) < 8.0):
                                deals.append(fo)
                                deal_tickets.add(fo_t)
                                logger.info("[%s] Preserving unrendered fresh local order ticket=%s in position sync (age=%.1fs < 8s)",
                                            self.account_id, fo_t, now_epoch - fo_time)

                        prev_tickets = set(str(o.get("Ticket")) for o in self._open_orders if o.get("Ticket"))
                        new_tickets = set(str(d.get("Ticket")) for d in deals if d.get("Ticket"))

                        # Check if any ticket disappeared (e.g. manual closure on iFOREX)
                        closed_externally = prev_tickets - new_tickets
                        if closed_externally:
                            logger.info("[%s] Detected %d position(s) closed externally in iFOREX: %s",
                                        self.account_id, len(closed_externally), closed_externally)

                        # Update open orders list
                        self._open_orders = deals
                        
                        # Also refresh ea_account_info right away so hedge monitor sees change immediately
                        if "ea_account_info" in self.dd:
                            info = self.dd["ea_account_info"].setdefault(self.account_id, {})
                            if summary:
                                if summary.get("balance", 0.0) > 0:
                                    info["balance"] = summary["balance"]
                                if summary.get("equity", 0.0) > 0:
                                    info["equity"] = summary["equity"]
                                if "margin" in summary and summary["margin"] is not None:
                                    info["margin"] = summary["margin"]
                                if "free_margin" in summary and summary["free_margin"] is not None:
                                    info["free_margin"] = summary["free_margin"]
                                if "open_pl" in summary and summary["open_pl"] is not None:
                                    info["profit"] = summary["open_pl"]
                            tickets = [o.get("Ticket") for o in deals if o.get("Ticket")]
                            info["open_tickets"] = tickets
                            info["positions"] = len(tickets)
                            info["pos_details"] = deals
                            info["position_details"] = [
                                {
                                    "ticket": str(o.get("Ticket")),
                                    "symbol": o.get("Symbol", "EURUSD"),
                                    "type": str(o.get("Type", "buy")),
                                    "lots": float(o.get("Lots", 0.01)),
                                    "open_price": float(o.get("OpenPrice", 0.0)),
                                    "profit": float(o.get("Profit", 0.0))
                                }
                                for o in deals if o.get("Ticket")
                            ]
                            _lbi = {}
                            tot_lots = 0.0
                            for o in deals:
                                sym = o.get("Symbol", "Unknown")
                                lots = float(o.get("Lots") or 0.01)
                                if sym not in _lbi:
                                    _lbi[sym] = {"buy": 0.0, "sell": 0.0}
                                if str(o.get("Type", "")).lower() in ("buy", "0", "op_buy"):
                                    _lbi[sym]["buy"] = round(_lbi[sym]["buy"] + lots, 2)
                                    tot_lots += lots
                                else:
                                    _lbi[sym]["sell"] = round(_lbi[sym]["sell"] + lots, 2)
                                    tot_lots -= lots
                            info["lots_by_instrument"] = _lbi
                            info["total_lots"] = round(tot_lots, 2)
                            now_t = time.time()
                            info["last_update"] = now_t
                            if "ea_heartbeats" in self.dd:
                                self.dd["ea_heartbeats"][self.account_id] = now_t
                        logger.info("[%s] Position sync: updated %d open positions (tickets: %s)",
                                    self.account_id, len(deals), [d.get("Ticket") for d in deals])

            except Exception as e:
                logger.warning("[%s] Position sync error: %s", self.account_id, e)

            # Sleep between position synchronization passes (0.5s for prompt detection)
            time.sleep(0.5)


# ─── Account Manager ────────────────────────────────────────────────────────
class IForexAccountManager:
    def __init__(self, dashboard_data: Dict[str, Any], config_dir: str = "."):
        self.dd = dashboard_data
        self.config_dir = config_dir
        self.config_file = os.path.join(config_dir, "iforex_accounts.json")
        self.accounts: Dict[str, IForexAccount] = {}
        self._running = False
        self._cmd_thread = None
        self.load_accounts()

    def load_accounts(self):
        if not os.path.exists(self.config_file):
            return
        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            for acc_id, conf in data.items():
                if conf.get("enabled", True):
                    acct = IForexAccount(conf, self.dd)
                    self.accounts[acc_id] = acct
                    acct.start()
                    logger.info("Loaded iFOREX direct account: %s", acc_id)
        except Exception as e:
            logger.error("Failed loading iforex_accounts.json: %s", e)

    def get_account(self, account_id: str) -> Optional[IForexAccount]:
        return self.accounts.get(account_id)

    def save_config(self):
        """Save all active account configs to iforex_accounts.json."""
        out = {}
        for acc_id, acct in self.accounts.items():
            out[acc_id] = acct.config
        try:
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2)
            logger.info("Saved %d accounts to %s", len(out), self.config_file)
            return True
        except Exception as e:
            logger.error("Failed saving iforex_accounts.json: %s", e)
            return False

    def add_account(self, account_id: str, config: Dict[str, Any]) -> bool:
        """Add or update an iFOREX account and start background polling."""
        config["account_id"] = account_id
        if account_id in self.accounts:
            self.accounts[account_id].stop()
        acct = IForexAccount(config, self.dd)
        self.accounts[account_id] = acct
        acct.start()
        self.save_config()
        return True

    def remove_account(self, account_id: str) -> bool:
        """Stop and remove an iFOREX account."""
        if account_id in self.accounts:
            self.accounts[account_id].stop()
            del self.accounts[account_id]
            self.dd.get("ea_account_info", {}).pop(account_id, None)
            self.dd.get("ea_heartbeats", {}).pop(account_id, None)
            self.save_config()
            return True
        return False

    def get_positions_for_import(self, account_id: str, pair_filter: str = "", comment_filter: str = "") -> List[Dict[str, Any]]:
        """Get open positions for a specific account in import format."""
        acct = self.accounts.get(account_id)
        if acct:
            return acct.get_positions_for_import(pair_filter, comment_filter)
        return []

    def start(self):
        """Start all account poll threads and the command loop."""
        self._running = True
        self._cmd_thread = threading.Thread(
            target=self._command_loop, daemon=True, name="IForex-CmdLoop"
        )
        self._cmd_thread.start()
        logger.info("IForexAccountManager command loop started")

    def stop(self):
        """Stop the command loop and all account threads."""
        self._running = False
        for acct in self.accounts.values():
            acct.stop()

    def _command_loop(self):
        """Poll active sessions and dispatch orders for iFOREX accounts."""
        while self._running:
            try:
                self._process_commands()
            except Exception as e:
                logger.error("IForex command loop error: %s", e)
            time.sleep(0.25)

    def _process_commands(self):
        """Check each active session for iFOREX accounts that need orders sent."""
        should_issue = self.dd.get("should_issue_command")
        if not should_issue:
            return

        with self.dd.get("lock", threading.Lock()):
            sessions = self.dd.get("sessions", {})
            in_flight = self.dd.get("in_flight_commands", {})

            for session_id, session in list(sessions.items()):
                if session.get("status") not in ("active", "partial_close"):
                    continue

                sides = session.get("sides", {})
                action = session.get("action", "open")

                for account_id, side_info in sides.items():
                    acct = self.accounts.get(account_id)
                    if not acct or not acct.connected:
                        continue

                    # Spread gate using iFOREX native cached get_quote (non-blocking)
                    pair = (side_info.get("pair") or session.get("pair", "")).strip()
                    max_spread = side_info.get("max_spread") if side_info.get("max_spread") is not None else session.get("max_spread_points")
                    try:
                        max_spread = float(max_spread) if max_spread is not None else None
                    except (ValueError, TypeError):
                        max_spread = None

                    if action in ("open", "open_limit") and max_spread is not None and pair:
                        q = acct.get_quote(pair, allow_live=False)
                        if q is None:
                            _gate_key = ("iforex_gate_noquote", account_id, session_id)
                            _gate_last = getattr(self, '_gate_log_ts', {})
                            if time.time() - _gate_last.get(_gate_key, 0) > 30:
                                logger.info("[%s] IFOREX SPREAD GATE: no cached quote for %s — blocking session %s",
                                            account_id, pair, session_id[:8])
                                _gate_last[_gate_key] = time.time()
                                self._gate_log_ts = _gate_last
                            continue
                        pip_mult = 100.0 if "JPY" in pair.upper() else 10000.0
                        cur_sp = round((q[1] - q[0]) * pip_mult, 1)
                        if cur_sp > max_spread:
                            _gate_key = ("iforex_gate_highspread", account_id, session_id)
                            _gate_last = getattr(self, '_gate_log_ts', {})
                            if time.time() - _gate_last.get(_gate_key, 0) > 30:
                                logger.info("[%s] IFOREX SPREAD GATE: spread %.1f > max %.1f for %s — blocking session %s",
                                            account_id, cur_sp, max_spread, pair, session_id[:8])
                                _gate_last[_gate_key] = time.time()
                                self._gate_log_ts = _gate_last
                            session.setdefault("spread_rejects", {})[account_id] = \
                                session.get("spread_rejects", {}).get(account_id, 0) + 1
                            continue

                    result = should_issue(session, account_id)
                    if result is False:
                        continue

                    lot_size = side_info.get("lot_size") or session.get("lot_size", 0.01)
                    trade_side = side_info.get("action", "buy")

                    in_flight[(session_id, account_id)] = time.time()

                    # Execute outside the lock in a thread
                    def _exec(acct_=acct, sid_=session_id, aid_=account_id,
                               pair_=pair, ls_=lot_size, side_=trade_side,
                               sess_=session, act_=action, res_=result):
                        try:
                            report = self.dd.get("report_trade_result")
                            q2 = acct_.get_quote(pair_, allow_live=True)
                            if not q2:
                                logger.warning("[%s] iFOREX open aborted — no live quote for %s", aid_, pair_)
                                in_flight.pop((sid_, aid_), None)
                                return
                            market_rate = q2[1] if side_ == "buy" else q2[0]

                            is_cycle_close = (res_ == "cycle_close")
                            is_close_op = act_ in ("close", "rollback") or res_ in ("rollback", "cycle_close")

                            if is_close_op:
                                # Close: find the iFOREX position ticket to close
                                open_pos = acct_._get_open_orders()
                                closed = {str(f.get("ticket")) for f in sess_.get("close_fills", []) if f.get("account") == aid_ and f.get("ticket") is not None}

                                target = None
                                # 1. If cycle_close, select the specific fill at the cycle index (oldest first)
                                if is_cycle_close:
                                    progress = sess_.get("cycle_progress", {})
                                    idx = progress.get("index", 0)
                                    closed_tickets_cycle = {
                                        str(f["ticket"]) for f in sess_.get("close_fills", [])
                                        if f.get("account") == aid_ and f.get("ticket") is not None
                                    }
                                    new_cycle_tks = set(str(t) for t in progress.get("new_cycle_tickets", []))
                                    acct_fills = [
                                        f for f in sess_.get("fills", [])
                                        if f.get("account") == aid_
                                        and str(f.get("ticket")) not in closed_tickets_cycle
                                        and str(f.get("ticket")) not in new_cycle_tks
                                    ]
                                    acct_fills.sort(key=lambda f: (f.get("ts_epoch", 0) or 0, int(f.get("ticket") or 0)))
                                    if idx < len(acct_fills):
                                        target = {"Ticket": str(acct_fills[idx].get("ticket"))}
                                        logger.info("[%s] iFOREX cycle_close: selected ticket %s at index %d", aid_, target["Ticket"], idx)

                                # 2. If rollback specifically nominated a ticket, try that first
                                if not target:
                                    rb_specific = sess_.get("rollback_tickets", {}).get(aid_, [])
                                    if rb_specific:
                                        for rbt in rb_specific:
                                            if str(rbt) not in closed:
                                                target = {"Ticket": str(rbt)}
                                                logger.info("[%s] iFOREX rollback: using specifically queued ticket %s", aid_, rbt)
                                                break

                                # 3. Find unclosed ticket from session fills for this account
                                if not target:
                                    fills = sess_.get("fills", [])
                                    for fill in fills:
                                        t = fill.get("ticket")
                                        if fill.get("account") == aid_ and t is not None and str(t) not in closed:
                                            target = {"Ticket": t}
                                            logger.info("[%s] iFOREX close: found ticket %s from session fills", aid_, t)
                                            break

                                # 4. Fallback: match open_orders that match the session pair and are not closed
                                if not target:
                                    clean_pair = pair_.upper().replace("/", "").replace(" ", "").replace("-", "")
                                    for o in open_pos:
                                        sym = str(o.get("Symbol", "")).upper().replace("/", "").replace(" ", "").replace("-", "")
                                        ot = o.get("Ticket")
                                        if (clean_pair.startswith(sym) or sym.startswith(clean_pair)) and ot and str(ot) not in closed:
                                            target = o
                                            logger.info("[%s] iFOREX close: found position ticket %s from open_orders for %s", aid_, ot, pair_)
                                            break

                                if not target:
                                    logger.warning("[%s] iFOREX close: no open position found to close for session %s "
                                                   "(open_orders=%d, fills=%d, closed=%d)",
                                                   aid_, sid_[:8], len(open_pos),
                                                   len([f for f in sess_.get("fills", []) if f.get("account") == aid_]),
                                                   len(closed))
                                    in_flight.pop((sid_, aid_), None)
                                    return
                                ticket = target.get("Ticket")
                                logger.info("[%s] iFOREX closing position ticket=%s rate=%s", aid_, ticket, market_rate)
                                close_res = acct_.close_order(ticket, market_rate)
                                status = "cycle_closed" if is_cycle_close else ("rollback_closed" if res_ == "rollback" else "closed")
                                if close_res.get("status") == "ok":
                                    # Remove from _open_orders and ea_account_info immediately
                                    with acct_._lock:
                                        acct_._open_orders = [o for o in acct_._open_orders if str(o.get("Ticket")) != str(ticket)]
                                        if "ea_account_info" in acct_.dd:
                                            info = acct_.dd["ea_account_info"].setdefault(aid_, {})
                                            tks = [str(t) for t in (info.get("open_tickets") or []) if str(t) != str(ticket)]
                                            info["open_tickets"] = tks
                                            info["positions"] = len(tks)
                                            info["position_details"] = [p for p in (info.get("position_details") or []) if str(p.get("ticket")) != str(ticket)]
                                            info["last_update"] = time.time()

                                    # Extract executed rate from iFOREX CloseDeals response
                                    exec_price = market_rate
                                    res_data = close_res.get("data")
                                    close_item = None
                                    if isinstance(res_data, list) and len(res_data) > 0:
                                        close_item = res_data[0]
                                    elif isinstance(res_data, dict):
                                        close_item = res_data.get("Result") or res_data
                                    if isinstance(close_item, dict):
                                        sr = close_item.get("serverRate") or close_item.get("rateCalc")
                                        if sr:
                                            try:
                                                exec_price = float(str(sr).replace(",", ""))
                                            except Exception:
                                                pass

                                    if report:
                                        report({"session_id": sid_, "account": aid_, "status": status,
                                                "ticket": str(ticket), "fill_price": exec_price,
                                                "quote_price": market_rate, "lots": ls_})
                                else:
                                    logger.error("[%s] iFOREX close_order failed: %s", aid_, close_res)
                                    if report:
                                        report({"session_id": sid_, "account": aid_, "status": "error",
                                                "detail": str(close_res.get("raw", close_res)), "ticket": str(ticket)})
                                    in_flight.pop((sid_, aid_), None)
                            else:
                                if act_.startswith("cycle_"):
                                    progress = sess_.get("cycle_progress", {})
                                    last_lots = progress.get("last_closed_lots")
                                    if last_lots:
                                        try:
                                            ls_ = float(last_lots)
                                        except (ValueError, TypeError):
                                            pass
                                open_res = acct_.open_order(pair_, side_, ls_, market_rate)
                                if open_res.get("status") == "ok":
                                    data = open_res.get("data", {})
                                    res_inner = data
                                    if isinstance(data, list) and len(data) > 0:
                                        res_inner = data[0]
                                    elif isinstance(data, dict):
                                        res_inner = data.get("Result") or data

                                    # itemId is the iFOREX position number needed for CloseDeals
                                    ticket = res_inner.get("itemId") or res_inner.get("DealId") or res_inner.get("Id") or res_inner.get("PositionId")
                                    if not ticket:
                                        logger.error("[%s] iFOREX OpenDeal response missing valid ticket: %s", aid_, open_res)
                                        in_flight.pop((sid_, aid_), None)
                                        if report:
                                            report({"session_id": sid_, "account": aid_, "status": "error",
                                                    "detail": f"Missing ticket in response: {res_inner}", "ticket": None})
                                        return

                                    # Extract executed rate from iFOREX OpenDeal response
                                    exec_price = market_rate
                                    sr = res_inner.get("serverRate") or res_inner.get("rateCalc")
                                    if sr:
                                        try:
                                            exec_price = float(str(sr).replace(",", ""))
                                        except Exception:
                                            pass

                                    logger.info("[%s] iFOREX order filled: ticket=%s pair=%s side=%s lots=%s quoted=%s exec=%s",
                                                aid_, ticket, pair_, side_, ls_, market_rate, exec_price)
                                    # Track in _open_orders for close lookups and update ea_account_info immediately
                                    with acct_._lock:
                                        now_fill_epoch = time.time()
                                        new_order_entry = {
                                            "Ticket": str(ticket), "Symbol": pair_,
                                            "Type": side_, "Lots": ls_,
                                            "OpenPrice": exec_price, "OpenTime": now_fill_epoch,
                                            "open_epoch": now_fill_epoch
                                        }
                                        if not any(str(o.get("Ticket")) == str(ticket) for o in acct_._open_orders):
                                            acct_._open_orders.append(new_order_entry)
                                        if "ea_account_info" in acct_.dd:
                                            info = acct_.dd["ea_account_info"].setdefault(aid_, {})
                                            tks = [str(t) for t in (info.get("open_tickets") or [])]
                                            if str(ticket) not in tks:
                                                tks.append(str(ticket))
                                                info["open_tickets"] = tks
                                                info["positions"] = len(tks)
                                                pos_list = list(info.get("position_details") or [])
                                                pos_list.append({
                                                    "ticket": str(ticket),
                                                    "symbol": pair_,
                                                    "type": side_,
                                                    "lots": ls_,
                                                    "open_price": exec_price,
                                                    "profit": 0.0
                                                })
                                                info["position_details"] = pos_list
                                            info["last_update"] = now_fill_epoch

                                    pip_mult = 100.0 if "JPY" in pair_.upper() else 10000.0
                                    spread = round((q2[1] - q2[0]) * pip_mult, 1) if (q2 and len(q2) >= 2) else 0

                                    if report:
                                        report({"session_id": sid_, "account": aid_, "status": "filled",
                                                "ticket": str(ticket), "fill_price": exec_price,
                                                "quote_price": market_rate, "spread": spread, "lots": ls_})
                                else:
                                    logger.error("[%s] iFOREX open_order failed: %s", aid_, open_res)
                                    in_flight.pop((sid_, aid_), None)
                                    if report:
                                        report({"session_id": sid_, "account": aid_, "status": "error",
                                                "detail": str(open_res.get("raw", open_res)), "ticket": None})
                        except Exception as ex:
                            logger.error("[%s] iFOREX _exec error: %s", aid_, ex)
                            in_flight.pop((sid_, aid_), None)

                    threading.Thread(target=_exec, daemon=True,
                                     name=f"iforex-exec-{account_id[:12]}").start()

    def get_status(self) -> Dict[str, Any]:
        """Get status of all iFOREX direct accounts."""
        result = {}
        for acct_id, acct in self.accounts.items():
            info = self.dd.get("ea_account_info", {}).get(acct_id, {})
            tot_l = info.get("total_lots")
            if tot_l is None or isinstance(tot_l, dict):
                lbi = info.get("lots_by_instrument", {})
                if isinstance(lbi, dict) and lbi:
                    tot_l = round(sum((v.get("buy", 0) - v.get("sell", 0)) for v in lbi.values()), 2)
                else:
                    tot_l = 0.0
            result[acct_id] = {
                "label": acct.label,
                "group_label": acct.config.get("group_label", acct.label),
                "type": acct.conn_type,
                "connected": bool(acct.connected and acct._running),
                "account_number": acct.account_number,
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "margin": info.get("margin"),
                "margin_used": info.get("margin"),
                "free_margin": info.get("free_margin"),
                "total_pnl": info.get("profit", 0.0),
                "positions": info.get("positions", 0),
                "leverage": acct.config.get("leverage", 400),
                "total_lots": tot_l,
                "swapfree": acct.config.get("swapfree", False),
                "stop_out_level": acct.config.get("stop_out_level"),
                "alert_email": acct.config.get("alert_email"),
                "alert_telegram": acct.config.get("alert_telegram"),
                "auto_connect_start": acct.config.get("auto_connect_start", True),
                "market_closed": bool(acct.is_market_closed),
            }
        return result

