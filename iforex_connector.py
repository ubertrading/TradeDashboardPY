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
        self._running = False
        self._poll_thread = None
        self._lock = threading.Lock()

    def start(self):
        """Start the background polling loop."""
        self._running = True
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True, name=f"iforex_poll_{self.account_id}")
        self._poll_thread.start()
        logger.info("[%s] iFOREX Direct polling thread started", self.account_id)

    def stop(self):
        """Stop background polling."""
        self._running = False

    def fetch_quote_live(self, symbol: str) -> Optional[Tuple[float, float]]:
        """Actively fetch live bid/ask from iFOREX WebPL4 for symbol."""
        sym_clean = symbol.upper().replace("/", "").replace(" ", "").replace("-", "").replace(".", "")
        inst_id = INSTRUMENT_MAP.get(sym_clean)
        if not inst_id:
            return None

        # 1. Primary: FeedsHistory/GetTicks (real live ticks: rateType 1 = Bid, rateType 0 = Ask)
        url_ticks = f"{self.base_url}/FeedsHistory/GetTicks"
        try:
            r_bid = self.session.get(url_ticks, params={"instrumentId": inst_id, "numTicks": 1, "rateType": 1}, timeout=5)
            r_ask = self.session.get(url_ticks, params={"instrumentId": inst_id, "numTicks": 1, "rateType": 0}, timeout=5)
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
                logger.warning("[%s] iFOREX session expired (401 Unauthorized) — please update Cookie in Account Edit", self.account_id)
                return None
        except Exception as e:
            logger.debug("[%s] FeedsHistory/GetTicks error for %s: %s", self.account_id, symbol, e)

        # 2. Fallback to GetDealMarginDetails
        url_margin = f"{self.base_url}/Deals/GetDealMarginDetails"
        try:
            resp = self.session.get(url_margin, params={"instrumentId": inst_id}, timeout=8)
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
        except Exception:
            pass

        return None

    def get_quote(self, symbol: str) -> Optional[Tuple[float, float]]:
        """Return (bid, ask) for symbol from cache or live fetch."""
        sym_clean = symbol.upper().replace("/", "").replace(" ", "").replace("-", "").replace(".", "")
        with self._lock:
            q = self._quotes_cache.get(sym_clean)
            if q and (time.time() - q.get("ts", 0)) < 3.0:
                return (q["bid"], q["ask"])
        return self.fetch_quote_live(symbol)

    def get_deal_margin_details(self, symbol_or_id: Any) -> Optional[Dict[str, Any]]:
        """Query margin requirements for an instrument."""
        inst_id = INSTRUMENT_MAP.get(str(symbol_or_id).upper().replace("/", "").replace(" ", ""), symbol_or_id)
        url = f"{self.base_url}/Deals/GetDealMarginDetails"
        try:
            resp = self.session.get(url, params={"instrumentId": inst_id}, timeout=10)
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
                logger.warning("[%s] iFOREX session expired (401 Unauthorized)", self.account_id)
        except Exception as e:
            self.connected = False
            logger.error("[%s] GetDealMarginDetails error: %s", self.account_id, e)
        return None

    def get_deal_risk_details(self, symbol_or_id: Any) -> Optional[Dict[str, Any]]:
        """Query risk and max exposure details."""
        inst_id = INSTRUMENT_MAP.get(str(symbol_or_id).upper().replace("/", ""), symbol_or_id)
        url = f"{self.base_url}/Deals/GetDealRiskDetails"
        try:
            resp = self.session.get(url, params={"instrumentId": inst_id}, timeout=10)
            if resp.status_code == 200:
                self.connected = True
                return resp.json()
        except Exception as e:
            logger.error("[%s] GetDealRiskDetails error: %s", self.account_id, e)
        return None

    def get_overnight_financing(self, symbol_or_id: Any, amount: float = 10000.0) -> Optional[Dict[str, Any]]:
        """Query long/short overnight swap/financing rates."""
        inst_id = INSTRUMENT_MAP.get(str(symbol_or_id).upper().replace("/", ""), symbol_or_id)
        url = f"{self.base_url}/Deals/GetOvernightFinancing"
        try:
            resp = self.session.get(url, params={"instrumentId": inst_id, "amount": int(amount)}, timeout=10)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error("[%s] GetOvernightFinancing error: %s", self.account_id, e)
        return None

    def open_order(self, symbol: str, direction: str, amount: float, market_rate: float, 
                   other_rate: Optional[float] = None, tp_rate: float = 0.0, sl_rate: float = 0.0) -> Dict[str, Any]:
        """
        Open a new position directly via HTTP POST.
        amount: units (e.g. 100,000 for 1.0 lot, 10,000 for 0.1 lot, 1,000 for 0.01 lot)
        """
        sym_clean = symbol.upper().replace("/", "")
        inst_id = INSTRUMENT_MAP.get(sym_clean, 3631)
        dir_code = 1 if str(direction).upper() in ("BUY", "1", "OP_BUY") else 0
        if other_rate is None:
            other_rate = market_rate

        url = f"{self.base_url}/Deals/OpenDeal"
        payload = {
            "DealType": 2,  # Spot
            "InstrumentId": inst_id,
            "Amount": int(amount),
            "MarketRate": market_rate,
            "OtherRateSeen": other_rate,
            "OrderDirection": dir_code,
            "TakeProfitRate": tp_rate,
            "StopLossRate": sl_rate,
            "SecurityToken": self.security_token
        }
        
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        logger.info("[%s] Submitting OpenDeal: %s", self.account_id, payload)
        try:
            resp = self.session.post(url, data=payload, headers=headers, timeout=15)
            logger.info("[%s] OpenDeal response (%d): %s", self.account_id, resp.status_code, resp.text)
            try:
                res_json = resp.json()
                return {"status": "ok" if resp.status_code == 200 else "error", "data": res_json, "raw": resp.text}
            except Exception:
                return {"status": "ok" if resp.status_code == 200 else "error", "raw": resp.text}
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
            resp = self.session.post(url, data=payload, headers=headers, timeout=15)
            logger.info("[%s] CloseDeals response (%d): %s", self.account_id, resp.status_code, resp.text)
            try:
                res_json = resp.json()
                return {"status": "ok" if resp.status_code == 200 else "error", "data": res_json, "raw": resp.text}
            except Exception:
                return {"status": "ok" if resp.status_code == 200 else "error", "raw": resp.text}
        except Exception as e:
            logger.error("[%s] CloseDeals exception: %s", self.account_id, e)
            return {"status": "error", "message": str(e)}

    def _get_open_orders(self) -> List[Dict[str, Any]]:
        """Return list of open orders in standard dictionary format."""
        with self._lock:
            return list(self._open_orders)

    def _poll_loop(self):
        """Periodic background poll for account data & heartbeats."""
        while self._running:
            try:
                # 1. Update Heartbeat
                if "ea_heartbeats" in self.dd:
                    self.dd["ea_heartbeats"][self.account_id] = time.time()
                
                # 2. Query EUR/USD margin to check connection & maintain session keep-alive
                self.get_deal_margin_details(3631)

                # 3. Update ea_account_info dictionary
                if "ea_account_info" in self.dd:
                    info = self.dd["ea_account_info"].setdefault(self.account_id, {})
                    info["conn_type"] = "iforex_direct"
                    info["account_number"] = self.account_number
                    info["connected"] = self.connected
                    
                    # Margin & Balance
                    info.setdefault("balance", float(self.config.get("balance", 0.0)))
                    info.setdefault("equity", float(self.config.get("equity", 0.0)))
                    info.setdefault("margin", float(self.config.get("margin", 0.0)))
                    info.setdefault("free_margin", float(self.config.get("free_margin", 0.0)))
                    info.setdefault("leverage", int(self.config.get("leverage", 400)))
                    
                    # Position tracking for hedge balancing
                    orders = self._get_open_orders()
                    tickets = [o.get("Ticket") for o in orders if o.get("Ticket")]
                    info["open_tickets"] = tickets
                    info["positions"] = len(tickets)
                    info["pos_details"] = orders

                    # Lots calculation
                    _lbi = {}
                    for o in orders:
                        sym = o.get("Symbol", "Unknown")
                        lots = o.get("Lots", 0.0)
                        if sym not in _lbi:
                            _lbi[sym] = {"buy": 0.0, "sell": 0.0}
                        if str(o.get("Type", "")).lower() in ("buy", "0", "op_buy"):
                            _lbi[sym]["buy"] = round(_lbi[sym]["buy"] + lots, 2)
                        else:
                            _lbi[sym]["sell"] = round(_lbi[sym]["sell"] + lots, 2)
                    info["lots_by_instrument"] = _lbi

            except Exception as e:
                logger.debug("[%s] Poll loop error: %s", self.account_id, e)
            time.sleep(5.0)


# ─── Account Manager ────────────────────────────────────────────────────────
class IForexAccountManager:
    def __init__(self, dashboard_data: Dict[str, Any], config_dir: str = "."):
        self.dd = dashboard_data
        self.config_dir = config_dir
        self.config_file = os.path.join(config_dir, "iforex_accounts.json")
        self.accounts: Dict[str, IForexAccount] = {}
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
            self.save_config()
            return True
        return False

    def get_status(self) -> Dict[str, Any]:
        """Get status of all iFOREX direct accounts."""
        result = {}
        for acct_id, acct in self.accounts.items():
            info = self.dd.get("ea_account_info", {}).get(acct_id, {})
            result[acct_id] = {
                "label": acct.label,
                "group_label": acct.config.get("group_label", acct.label),
                "type": acct.conn_type,
                "connected": acct.connected,
                "account_number": acct.account_number,
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "free_margin": info.get("free_margin"),
                "positions": info.get("positions", 0),
                "leverage": acct.config.get("leverage", 400),
                "total_lots": info.get("lots_by_instrument"),
                "swapfree": acct.config.get("swapfree", False),
                "stop_out_level": acct.config.get("stop_out_level"),
                "alert_email": acct.config.get("alert_email"),
                "alert_telegram": acct.config.get("alert_telegram"),
            }
        return result

