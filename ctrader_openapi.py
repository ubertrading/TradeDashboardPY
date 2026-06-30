"""
cTrader Open API client — two modes:
1. Adjunct mode (CTraderOpenApiClient): supplements FIX with balance/equity/leverage
2. Standalone mode (CTraderOpenApiAccount): full trading — orders, market data, positions

Uses the ctrader-open-api package (Twisted-based) running in a dedicated thread.
Feeds data into the shared ea_account_info dict used by the dashboard.

Auth flow:
1. User registers app at https://openapi.ctrader.com → gets client_id + client_secret
2. Dashboard provides OAuth redirect URL → user grants access → gets access_token + refresh_token
3. This module connects via TCP+SSL to cTrader backend
"""

import json
import logging
import os
import threading
import time
import requests as _requests_mod
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
except ImportError:
    import pytz
    NY_TZ = pytz.timezone("America/New_York")

logger = logging.getLogger("ctrader_openapi")

# ─── Lazy imports (Twisted is heavy) ────────────────────────────────────────
_twisted_imported = False
_import_error = None

# ─── Startup serialization ───────────────────────────────────────────────────
# Prevents ReactorNotRestartable race and connection storms when multiple
# OpenAPI clients start simultaneously at dashboard boot.
_reactor_start_lock = threading.Lock()   # only one thread calls reactor.run()
_openapi_start_lock = threading.Lock()   # serialises stagger index assignment
_openapi_start_index = 0                 # increments per client; delay = index * 7s


def _ensure_imports():
    """Import Twisted and ctrader-open-api modules on first use."""
    global _twisted_imported, _import_error
    if _twisted_imported:
        return True
    if _import_error:
        print(f"[OPENAPI] Skipping imports — previous error: {_import_error}")
        return False
    print("[OPENAPI] Importing ctrader-open-api + Twisted...")
    try:
        global Client, Protobuf, Auth, EndPoints, TcpProtocol
        global ProtoOAApplicationAuthReq, ProtoOAAccountAuthReq
        global ProtoOAGetAccountListByAccessTokenReq, ProtoOATraderReq
        global ProtoOAGetAccountListByAccessTokenRes, ProtoOATraderRes
        global ProtoOAApplicationAuthRes, ProtoOAAccountAuthRes
        global ProtoHeartbeatEvent
        global ProtoOANewOrderReq, ProtoOAClosePositionReq
        global ProtoOAExecutionEvent, ProtoOASubscribeSpotsReq
        global ProtoOASubscribeSpotsRes, ProtoOASpotEvent
        global ProtoOASymbolsListReq, ProtoOASymbolsListRes
        global ProtoOAReconcileReq, ProtoOAReconcileRes
        global ProtoOAErrorRes, ProtoOAOrderErrorEvent
        global ProtoOAGetPositionUnrealizedPnLReq, ProtoOAGetPositionUnrealizedPnLRes
        global ProtoOAMarginChangedEvent
        global ProtoOADealListReq, ProtoOADealListRes

        from ctrader_open_api import Client, Protobuf, Auth, EndPoints, TcpProtocol
        from ctrader_open_api.messages.OpenApiMessages_pb2 import (
            ProtoOAApplicationAuthReq,
            ProtoOAApplicationAuthRes,
            ProtoOAAccountAuthReq,
            ProtoOAAccountAuthRes,
            ProtoOAGetAccountListByAccessTokenReq,
            ProtoOAGetAccountListByAccessTokenRes,
            ProtoOATraderReq,
            ProtoOATraderRes,
            ProtoOANewOrderReq,
            ProtoOAClosePositionReq,
            ProtoOAExecutionEvent,
            ProtoOASubscribeSpotsReq,
            ProtoOASubscribeSpotsRes,
            ProtoOASpotEvent,
            ProtoOASymbolsListReq,
            ProtoOASymbolsListRes,
            ProtoOAReconcileReq,
            ProtoOAReconcileRes,
            ProtoOAErrorRes,
            ProtoOAOrderErrorEvent,
            ProtoOAGetPositionUnrealizedPnLReq,
            ProtoOAGetPositionUnrealizedPnLRes,
            ProtoOAMarginChangedEvent,
            ProtoOADealListReq,
            ProtoOADealListRes,
        )
        from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import (
            ProtoHeartbeatEvent,
        )
        # Thread-safe send wrapper for Client
        original_send = Client.send
        def thread_safe_send(self, message, **kwargs):
            from twisted.internet import reactor
            from twisted.internet import defer
            import threading
            is_reactor_thread = getattr(reactor, "_reactor_thread_id", None) == threading.get_ident()
            if not reactor.running or is_reactor_thread:
                return original_send(self, message, **kwargs)
            external_d = defer.Deferred()
            def _do_send():
                try:
                    d = original_send(self, message, **kwargs)
                    d.chainDeferred(external_d)
                except Exception as e:
                    external_d.errback(e)
            reactor.callFromThread(_do_send)
            return external_d
        Client.send = thread_safe_send

        # BUG FIX: ctrader_open_api's TcpProtocol uses CLASS variables for queue/task!
        # This causes traffic to mix across accounts. We patch __init__ to use instance vars.
        original_protocol_init = getattr(TcpProtocol, '__init__', object.__init__)
        def _tcp_protocol_init(self, *args, **kwargs):
            from collections import deque
            self._send_queue = deque([])
            self._send_task = None
            self._lastSendMessageTime = None
            if original_protocol_init is not object.__init__:
                original_protocol_init(self, *args, **kwargs)
        TcpProtocol.__init__ = _tcp_protocol_init

        _twisted_imported = True
        print("[OPENAPI] All imports successful")
        return True
    except Exception as e:
        _import_error = str(e)
        print(f"[OPENAPI] IMPORT FAILED: {e}")
        logger.error("Failed to import ctrader-open-api: %s", e)
        return False


# ═══════════════════════════════════════════════════════════════════════════
#  ADJUNCT CLIENT — balance/equity supplement for FIX accounts
# ═══════════════════════════════════════════════════════════════════════════

class CTraderOpenApiClient:
    """
    Connects to cTrader Open API, authenticates, and periodically
    retrieves account balance/equity/leverage via ProtoOATraderReq.
    Used as an adjunct to FIX accounts (not for trading).
    """

    def __init__(self, account_id, config, dd):
        self.account_id = account_id
        self.config = config
        self.dd = dd
        self._running = False
        self._connected = False
        self._authenticated_app = False
        self._authenticated_account = False
        self._client = None
        self._reactor_thread = None
        self._poll_thread = None
        self._last_trader_request = 0
        self._last_pnl_request = 0
        self._last_reconcile_request = 0
        self._poll_interval = 15
        self._money_digits = 2
        self.balance = None
        self.equity = None
        self.leverage = None

        # Reconciled data for supplemental details
        self._positions = {}
        self._position_swaps = {}
        self._position_margins = {}
        self._unrealized_pnl = {}
        self._total_unrealized_pnl = 0.0
        self._pt_pnl_res = True

        # Auto-reconnect state
        self._reconnect_delay = 5
        self._reconnect_attempt = 0

        # API error tracking / backoff
        self._api_error_count = 0
        self._api_backoff_until = 0

    @property
    def is_available(self):
        return bool(
            self.config.get("openapi_client_id")
            and self.config.get("openapi_client_secret")
            and self.config.get("openapi_access_token")
            and self.config.get("openapi_account_id")
        )

    def start(self):
        global _openapi_start_index
        if not _ensure_imports():
            logger.error("[%s] Cannot start Open API — missing deps: %s",
                         self.account_id, _import_error)
            return False
        if not self.is_available:
            logger.info("[%s] Open API not configured — skipping", self.account_id)
            return False
        with _openapi_start_lock:
            self._startup_delay = _openapi_start_index * 7
            _openapi_start_index += 1
        self._running = True
        self._reactor_thread = threading.Thread(
            target=self._run_reactor, daemon=True,
            name=f"OpenAPI-{self.account_id}"
        )
        self._reactor_thread.start()
        logger.info("[%s] Open API adjunct client starting", self.account_id)
        return True

    def stop(self):
        self._running = False
        if self._client:
            try:
                from twisted.internet import reactor
                if reactor.running:
                    reactor.callFromThread(self._client.stopService)
                else:
                    self._client.stopService()
            except Exception:
                pass
        logger.info("[%s] Open API adjunct client stopped", self.account_id)

    def _run_reactor(self):
        # Stagger so each adjunct client connects 7s after the previous one,
        # avoiding a simultaneous auth burst on dashboard restart.
        _startup_delay = getattr(self, '_startup_delay', 0)
        if _startup_delay > 0:
            logger.info("[%s] Open API adjunct staggered start — waiting %ds",
                        self.account_id, _startup_delay)
            time.sleep(_startup_delay)
        try:
            from twisted.internet import reactor as _reactor
            env = self.config.get("openapi_environment", "demo")
            host = EndPoints.PROTOBUF_LIVE_HOST if env == "live" else EndPoints.PROTOBUF_DEMO_HOST
            self._client = Client(host, 5035, TcpProtocol)
            self._client.setConnectedCallback(self._on_connected)
            self._client.setDisconnectedCallback(self._on_disconnected)
            self._client.setMessageReceivedCallback(self._on_message)
            self._poll_thread = threading.Thread(
                target=self._poll_loop, daemon=True,
                name=f"OpenAPI-Poll-{self.account_id}"
            )
            self._poll_thread.start()
            with _reactor_start_lock:
                if _reactor.running:
                    _reactor.callFromThread(self._client.startService)
                    _should_run_reactor = False
                else:
                    self._client.startService()
                    _reactor._reactor_thread_id = threading.get_ident()
                    _should_run_reactor = True
            if _should_run_reactor:
                _reactor.run(installSignalHandlers=False)
        except Exception as e:
            logger.error("[%s] Open API reactor error: %s", self.account_id, e)

    def _on_connected(self, client=None):
        logger.info("[%s] Open API connected", self.account_id)
        self._connected = True
        self._authenticated_app = False
        self._authenticated_account = False
        # Skip auth if we're in an error backoff — let the connection sit idle
        # until the backoff expires, then the poll_loop will re-enable auth.
        if time.time() < self._api_backoff_until:
            remaining = int(self._api_backoff_until - time.time())
            logger.info("[%s] In error backoff (%ds remaining) — deferring auth",
                        self.account_id, remaining)
            return
        self._send_app_auth()

    def _on_disconnected(self, client=None, reason=None):
        logger.warning("[%s] Open API disconnected: %s — will auto-reconnect", self.account_id, reason)
        self._connected = False
        self._authenticated_app = False
        self._authenticated_account = False

    def _on_message(self, client, msg):
        try:
            pt = msg.payloadType
            if pt == ProtoOAApplicationAuthRes().payloadType:
                self._authenticated_app = True
                self._send_account_auth()
            elif pt == ProtoOAAccountAuthRes().payloadType:
                self._authenticated_account = True
                self._request_trader_info()
                self._reconcile_positions()
            elif pt == ProtoOATraderRes().payloadType:
                self._handle_trader_response(msg)
            elif pt == ProtoOAReconcileRes().payloadType:
                self._handle_reconcile(msg)
            elif pt == ProtoOAGetPositionUnrealizedPnLRes().payloadType:
                self._handle_unrealized_pnl(msg)
            elif pt == ProtoOAMarginChangedEvent().payloadType:
                self._handle_margin_changed(msg)
            elif pt == ProtoOAExecutionEvent().payloadType:
                # Trigger a reconciliation on any fill or close event to keep positions in sync
                self._reconcile_positions()
            elif pt == ProtoHeartbeatEvent().payloadType:
                pass
        except Exception as e:
            logger.error("[%s] Open API message error: %s", self.account_id, e)

    def _send_app_auth(self):
        req = ProtoOAApplicationAuthReq()
        req.clientId = self.config["openapi_client_id"]
        req.clientSecret = self.config["openapi_client_secret"]
        d = self._client.send(req)
        d.addErrback(lambda f: self._on_request_error(f, "App auth"))

    def _send_account_auth(self):
        req = ProtoOAAccountAuthReq()
        req.ctidTraderAccountId = int(self.config["openapi_account_id"])
        req.accessToken = self.config["openapi_access_token"]
        d = self._client.send(req)
        d.addErrback(lambda f: self._on_request_error(f, "Account auth"))

    def _request_trader_info(self):
        if not self._authenticated_account:
            return
        req = ProtoOATraderReq()
        req.ctidTraderAccountId = int(self.config["openapi_account_id"])
        self._last_trader_request = time.time()
        d = self._client.send(req)
        d.addErrback(lambda f: self._on_request_error(f, "Trader req"))

    def _handle_trader_response(self, msg):
        try:
            self._api_error_count = 0
            trader_res = Protobuf.extract(msg)
            trader = trader_res.trader
            md = trader.moneyDigits if trader.moneyDigits else 2
            self._money_digits = md
            self.balance = trader.balance / (10 ** md)
            if trader.maxLeverage and trader.maxLeverage > 0:
                self.leverage = trader.maxLeverage
            elif trader.leverageInCents and trader.leverageInCents > 0:
                self.leverage = trader.leverageInCents // 100
            self._update_account_info()
        except Exception as e:
            logger.error("[%s] Error parsing trader response: %s", self.account_id, e)

    def _reconcile_positions(self):
        if not self._authenticated_account:
            return
        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = int(self.config["openapi_account_id"])
        d = self._client.send(req)
        d.addErrback(lambda f: self._on_request_error(f, "Reconcile"))

    def _handle_reconcile(self, msg):
        try:
            self._api_error_count = 0
            res = Protobuf.extract(msg)
            self._positions.clear()
            self._position_swaps.clear()
            self._position_margins.clear()
            for pos in res.position:
                pid = pos.positionId
                open_ts = None
                try:
                    if hasattr(pos.tradeData, 'openTimestamp') and pos.tradeData.openTimestamp:
                        open_ts = int(pos.tradeData.openTimestamp)
                except Exception:
                    pass
                self._positions[pid] = {
                    "side": "buy" if pos.tradeData.tradeSide == 1 else "sell",
                    "volume": pos.tradeData.volume,
                    "price": pos.price if pos.price else 0,
                    "open_ts": open_ts,
                }
                try:
                    if hasattr(pos, 'swap') and pos.swap:
                        self._position_swaps[pid] = pos.swap
                except Exception:
                    pass
                try:
                    if hasattr(pos, 'usedMargin') and pos.usedMargin:
                        self._position_margins[pid] = pos.usedMargin
                except Exception:
                    pass
            logger.info("[%s] Open API adjunct reconciled %d positions (swaps: %d, margins: %d)",
                        self.account_id, len(self._positions),
                        len(self._position_swaps), len(self._position_margins))
        except Exception as e:
            logger.error("[%s] Reconcile parse error: %s", self.account_id, e)

    def _request_unrealized_pnl(self):
        if not self._authenticated_account:
            return
        if not self._pt_pnl_res:
            return
        try:
            req = ProtoOAGetPositionUnrealizedPnLReq()
            req.ctidTraderAccountId = int(self.config["openapi_account_id"])
            self._last_pnl_request = time.time()
            d = self._client.send(req)
            d.addErrback(lambda f: self._on_request_error(f, "PnL req"))
        except Exception as e:
            logger.error("[%s] PnL request error: %s", self.account_id, e)
            self._last_pnl_request = time.time()

    def _handle_unrealized_pnl(self, msg):
        try:
            self._api_error_count = 0
            res = Protobuf.extract(msg)
            md = self._money_digits
            self._unrealized_pnl.clear()
            total = 0
            for entry in res.positionUnrealizedPnL:
                net_pnl_raw = 0
                if hasattr(entry, 'netUnrealizedPnL') and entry.netUnrealizedPnL:
                    net_pnl_raw = entry.netUnrealizedPnL
                elif hasattr(entry, 'grossUnrealizedPnL') and entry.grossUnrealizedPnL:
                    net_pnl_raw = entry.grossUnrealizedPnL
                net_pnl = net_pnl_raw / (10 ** md) if net_pnl_raw else 0
                self._unrealized_pnl[entry.positionId] = net_pnl
                total += net_pnl
            self._total_unrealized_pnl = round(total, 2)
        except Exception as e:
            logger.error("[%s] PnL response error: %s", self.account_id, e)

    def _handle_margin_changed(self, msg):
        try:
            evt = Protobuf.extract(msg)
            if evt.positionId and evt.usedMargin is not None:
                self._position_margins[evt.positionId] = evt.usedMargin
        except Exception as e:
            logger.error("[%s] Margin changed error: %s", self.account_id, e)

    def _update_account_info(self):
        info = self.dd.get("ea_account_info", {}).get(self.account_id, {})
        if self.balance is not None:
            info["balance"] = self.balance
        if self.leverage is not None:
            info["leverage"] = self.leverage
        info["openapi_connected"] = True
        info["last_update"] = time.time()
        self.dd.setdefault("ea_account_info", {})[self.account_id] = info

    def _on_request_error(self, failure, label):
        """Track consecutive API errors and pause polling after 3 failures.
        Does NOT reset auth state — timeouts on data requests don't mean the
        session is broken, only that the server was slow. Auth is reset only
        by _handle_error when INVALID_REQUEST is received."""
        self._api_error_count += 1
        logger.error("[%s] %s failed: %s", self.account_id, label, failure)
        if self._api_error_count >= 3:
            logger.warning("[%s] %d consecutive API errors — pausing polling for 60s",
                           self.account_id, self._api_error_count)
            self._api_backoff_until = time.time() + 60
            self._api_error_count = 0  # reset so next 3 failures trigger again

    def _poll_loop(self):
        while self._running:
            try:
                if time.time() < self._api_backoff_until:
                    time.sleep(2)
                    continue
                if self._connected and self._authenticated_account:
                    now = time.time()
                    if now - self._last_trader_request > self._poll_interval:
                        self._request_trader_info()
                    if now - self._last_pnl_request > 5 and len(self._positions) > 0:
                        self._request_unrealized_pnl()
                    if now - self._last_reconcile_request > 60:
                        self._last_reconcile_request = now
                        self._reconcile_positions()
                    # Reset reconnect state on successful operation
                    if self._reconnect_attempt > 0:
                        logger.info("[%s] Open API adjunct reconnected after %d attempt(s)",
                                    self.account_id, self._reconnect_attempt)
                        self._reconnect_delay = 5
                        self._reconnect_attempt = 0

                elif not self._connected and self._running:
                    # Disconnected — Twisted's reconnecting factory handles TCP,
                    # but if client startup failed entirely, we log and track
                    self._reconnect_attempt += 1
                    if self._reconnect_attempt <= 3 or self._reconnect_attempt % 10 == 0:
                        logger.info("[%s] Open API adjunct waiting for reconnect (attempt #%d)...",
                                    self.account_id, self._reconnect_attempt)
                time.sleep(2)
            except Exception as e:
                logger.error("[%s] Poll error: %s", self.account_id, e)
                time.sleep(5)

    def refresh_access_token(self):
        if not _ensure_imports():
            return None
        try:
            rt = self.config.get("openapi_refresh_token")
            if not rt:
                return None
            auth = Auth(self.config["openapi_client_id"],
                        self.config["openapi_client_secret"], "")
            result = auth.refreshToken(rt)
            if result and "accessToken" in result:
                self.config["openapi_access_token"] = result["accessToken"]
                if "refreshToken" in result:
                    self.config["openapi_refresh_token"] = result["refreshToken"]
                return result["accessToken"]
            return None
        except Exception as e:
            logger.error("[%s] Token refresh error: %s", self.account_id, e)
            return None


# ═══════════════════════════════════════════════════════════════════════════
#  STANDALONE TRADING ACCOUNT — full order execution via Open API
# ═══════════════════════════════════════════════════════════════════════════

class CTraderOpenApiAccount:
    """
    Standalone cTrader Open API account for full trading.
    Same interface as CTraderFixAccount so FixAccountManager can use both.
    """

    def __init__(self, account_id, config, dd):
        """
        Args:
            account_id: Dashboard account identifier
            config: dict with openapi_* fields
            dd: shared dashboard data dict
        """
        self.account_id = account_id
        self.config = config
        self.dd = dd
        self.label = config.get("label", account_id)

        # Connection state
        self._running = False
        self._connected = False
        self._authenticated_app = False
        self._authenticated_account = False
        self._client = None
        self._reactor_thread = None
        self._poll_thread = None

        # Symbol mapping: name → id, id → name
        self._symbols_by_name = {}   # {"EURUSD": 1}
        self._symbols_by_id = {}     # {1: "EURUSD"}
        self._symbol_digits = {}     # {1: 5}  (price decimal digits)
        self._symbols_loaded = False

        # Market data
        self._bid = {}    # {symbol_name: float}
        self._ask = {}    # {symbol_name: float}
        self._subscribed_symbols = set()

        # Account data
        self._money_digits = 2
        self._balance = None
        self._equity = None
        self._leverage = None
        self._last_trader_request = 0
        self._poll_interval = 15

        # Order tracking: maps client_order_id → {session_id, comment, ...}
        self._pending_orders = {}

        # Position tracking from reconcile
        self._positions = {}  # positionId → position info dict
        self._position_swaps = {}  # positionId → swap (raw, moneyDigits-encoded)
        self._position_margins = {}  # positionId → usedMargin (raw, moneyDigits-encoded)
        self._unrealized_pnl = {}  # positionId → net PnL (deposit currency)
        self._total_unrealized_pnl = None  # total unrealized PnL in deposit currency
        self._last_pnl_request = 0
        self._last_reconcile_request = 0

        # Deal history (PnL report) — pending request tracking
        self._deal_history_result = None
        self._deal_history_event = threading.Event()

        # Will be populated in start() after imports
        self._pt_pnl_res = None
        self._pt_margin_evt = None
        self._pt_deal_list_res = None

        # API error tracking / backoff
        self._api_error_count = 0
        self._api_backoff_until = 0

    @property
    def connected(self):
        """Compatible with CTraderFixAccount interface."""
        return self._connected and self._authenticated_account

    @property
    def quote_connected(self):
        """Open API uses a single connection for everything."""
        return self.connected

    def start(self):
        """Start the Open API account."""
        global _openapi_start_index
        # Idempotent: if already running, don't create duplicate reactor/client
        if self._running:
            logger.info("[%s] Open API account already running — skipping duplicate start", self.account_id)
            return
        print(f"[OPENAPI] Starting account {self.account_id}...")
        if not _ensure_imports():
            msg = f"[OPENAPI] CANNOT START {self.account_id} — import failed: {_import_error}"
            print(msg)
            logger.error(msg)
            return
        with _openapi_start_lock:
            self._startup_delay = _openapi_start_index * 7
            _openapi_start_index += 1
        self._running = True
        # Cache protobuf payload type IDs now that imports are available
        try:
            self._pt_pnl_res = ProtoOAGetPositionUnrealizedPnLRes().payloadType
            logger.info("[%s] PnL response payloadType=%s", self.account_id, self._pt_pnl_res)
        except Exception:
            logger.warning("[%s] ProtoOAGetPositionUnrealizedPnLRes not available", self.account_id)
        try:
            self._pt_margin_evt = ProtoOAMarginChangedEvent().payloadType
            logger.info("[%s] MarginChanged payloadType=%s", self.account_id, self._pt_margin_evt)
        except Exception:
            logger.warning("[%s] ProtoOAMarginChangedEvent not available", self.account_id)
        try:
            self._pt_deal_list_res = ProtoOADealListRes().payloadType
            logger.info("[%s] DealListRes payloadType=%s", self.account_id, self._pt_deal_list_res)
        except Exception:
            logger.warning("[%s] ProtoOAMarginChangedEvent not available", self.account_id)
        self._reactor_thread = threading.Thread(
            target=self._run_reactor, daemon=True,
            name=f"OA-{self.account_id}"
        )
        self._reactor_thread.start()
        print(f"[OPENAPI] Account {self.account_id} reactor thread started")
        logger.info("[%s] Open API account starting", self.account_id)

    def stop(self):
        """Stop the account."""
        self._running = False
        if self._client:
            try:
                from twisted.internet import reactor
                if reactor.running:
                    reactor.callFromThread(self._client.stopService)
                else:
                    self._client.stopService()
            except Exception:
                pass
        logger.info("[%s] Open API account stopped", self.account_id)

    # ─── Reactor / Connection ───────────────────────────────────────────────

    def _run_reactor(self):
        # Stagger so each client connects 7s after the previous one.
        _startup_delay = getattr(self, '_startup_delay', 0)
        if _startup_delay > 0:
            logger.info("[%s] Open API account staggered start — waiting %ds",
                        self.account_id, _startup_delay)
            time.sleep(_startup_delay)
        try:
            from twisted.internet import reactor as _reactor
            env = self.config.get("openapi_environment", "demo")
            host = EndPoints.PROTOBUF_LIVE_HOST if env == "live" else EndPoints.PROTOBUF_DEMO_HOST
            self._client = Client(host, 5035, TcpProtocol)
            self._client.setConnectedCallback(self._on_connected)
            self._client.setDisconnectedCallback(self._on_disconnected)
            self._client.setMessageReceivedCallback(self._on_message)

            # Start data feed thread
            self._poll_thread = threading.Thread(
                target=self._heartbeat_loop, daemon=True,
                name=f"OA-HB-{self.account_id}"
            )
            self._poll_thread.start()

            with _reactor_start_lock:
                if _reactor.running:
                    _reactor.callFromThread(self._client.startService)
                    _should_run_reactor = False
                else:
                    self._client.startService()
                    _reactor._reactor_thread_id = threading.get_ident()
                    _should_run_reactor = True
            if _should_run_reactor:
                _reactor.run(installSignalHandlers=False)
        except Exception as e:
            logger.error("[%s] Reactor error: %s", self.account_id, e)

    def _on_connected(self, client=None):
        logger.info("[%s] TCP connected", self.account_id)
        self._connected = True
        self._authenticated_app = False
        self._authenticated_account = False
        self._symbols_loaded = False
        # Step 1: App auth
        req = ProtoOAApplicationAuthReq()
        req.clientId = self.config["openapi_client_id"]
        req.clientSecret = self.config["openapi_client_secret"]
        d = self._client.send(req)
        d.addErrback(lambda f: logger.error("[%s] App auth send failed: %s", self.account_id, f))

    def _on_disconnected(self, client=None, reason=None):
        logger.warning("[%s] Disconnected: %s — will auto-reconnect", self.account_id, reason)
        self._connected = False
        self._authenticated_app = False
        self._authenticated_account = False

    # ─── Message Router ─────────────────────────────────────────────────────

    def _on_message(self, client, msg):
        try:
            pt = msg.payloadType

            # Auth responses
            if pt == ProtoOAApplicationAuthRes().payloadType:
                logger.info("[%s] App authenticated", self.account_id)
                self._authenticated_app = True
                # Step 2: Account auth
                req = ProtoOAAccountAuthReq()
                req.ctidTraderAccountId = int(self.config["openapi_account_id"])
                req.accessToken = self.config["openapi_access_token"]
                d = self._client.send(req)
                d.addErrback(lambda f: logger.error("[%s] Acct auth send failed", self.account_id))

            elif pt == ProtoOAAccountAuthRes().payloadType:
                logger.info("[%s] Account authenticated", self.account_id)
                self._authenticated_account = True
                # Step 3: Fetch symbols, trader info, reconcile
                # Stagger requests by 1s each to avoid overwhelming the server
                # with 3 simultaneous deferreds that all race the same 5s timeout
                from twisted.internet import reactor as _r
                self._fetch_symbol_list()
                _r.callLater(1.0, self._request_trader_info)
                _r.callLater(2.0, self._reconcile_positions)

            # Symbol list
            elif pt == ProtoOASymbolsListRes().payloadType:
                self._handle_symbol_list(msg)

            # Spot price events
            elif pt == ProtoOASpotEvent().payloadType:
                self._handle_spot_event(msg)

            elif pt == ProtoOASubscribeSpotsRes().payloadType:
                pass  # ack

            # Trader info
            elif pt == ProtoOATraderRes().payloadType:
                self._handle_trader_response(msg)

            # Execution events (fills, accepts, rejects)
            elif pt == ProtoOAExecutionEvent().payloadType:
                self._handle_execution_event(msg)

            # Order errors
            elif pt == ProtoOAOrderErrorEvent().payloadType:
                self._handle_order_error(msg)

            # Error response
            elif pt == ProtoOAErrorRes().payloadType:
                self._handle_error(msg)

            # Reconcile (existing positions)
            elif pt == ProtoOAReconcileRes().payloadType:
                self._handle_reconcile(msg)

            # Unrealized PnL response
            elif self._pt_pnl_res and pt == self._pt_pnl_res:
                self._handle_unrealized_pnl(msg)

            # Deal list response (PnL history)
            elif self._pt_deal_list_res and pt == self._pt_deal_list_res:
                self._handle_deal_list_response(msg)

            # Margin changed event
            elif self._pt_margin_evt and pt == self._pt_margin_evt:
                self._handle_margin_changed(msg)

            # Heartbeat
            elif pt == ProtoHeartbeatEvent().payloadType:
                pass

            else:
                logger.debug("[%s] Unhandled msg type=%s", self.account_id, pt)

        except Exception as e:
            logger.error("[%s] Message error: %s", self.account_id, e, exc_info=True)

    # ─── Symbol Management ──────────────────────────────────────────────────

    def _fetch_symbol_list(self):
        """Request available symbols for this account."""
        req = ProtoOASymbolsListReq()
        req.ctidTraderAccountId = int(self.config["openapi_account_id"])
        d = self._client.send(req)
        d.addErrback(lambda f: self._on_request_error(f, "Symbol list"))

    def _handle_symbol_list(self, msg):
        """Cache symbol name ↔ ID mapping."""
        try:
            res = Protobuf.extract(msg)
            for sym in res.symbol:
                name = sym.symbolName if sym.symbolName else f"SYM_{sym.symbolId}"
                self._symbols_by_name[name] = sym.symbolId
                self._symbols_by_id[sym.symbolId] = name
            self._symbols_loaded = True
            logger.info("[%s] Loaded %d symbols", self.account_id, len(self._symbols_by_name))
        except Exception as e:
            logger.error("[%s] Symbol list parse error: %s", self.account_id, e)

    def _get_symbol_id(self, symbol_name):
        """Resolve symbol name to ID. Tries exact match, then case-insensitive."""
        sid = self._symbols_by_name.get(symbol_name)
        if sid:
            return sid
        # Case-insensitive fallback
        upper = symbol_name.upper()
        for name, sid in self._symbols_by_name.items():
            if name.upper() == upper:
                return sid
        return None

    def _get_symbol_name(self, symbol_id):
        """Resolve symbol ID to name."""
        return self._symbols_by_id.get(symbol_id, f"SYM_{symbol_id}")

    # ─── Market Data ────────────────────────────────────────────────────────

    def subscribe_symbol(self, symbol_name):
        """Subscribe to spot prices for a symbol."""
        if not self.connected:
            return
        sid = self._get_symbol_id(symbol_name)
        if not sid:
            logger.warning("[%s] Cannot subscribe — unknown symbol: %s",
                           self.account_id, symbol_name)
            return
        if sid in self._subscribed_symbols:
            return
        req = ProtoOASubscribeSpotsReq()
        req.ctidTraderAccountId = int(self.config["openapi_account_id"])
        req.symbolId.append(sid)
        d = self._client.send(req)
        d.addErrback(lambda f: logger.error("[%s] Spot sub failed: %s", self.account_id, f))
        self._subscribed_symbols.add(sid)
        logger.info("[%s] Subscribed to %s (id=%d)", self.account_id, symbol_name, sid)

    def _handle_spot_event(self, msg):
        """Update bid/ask from spot price event."""
        try:
            evt = Protobuf.extract(msg)
            sym_name = self._get_symbol_name(evt.symbolId)
            if evt.bid:
                self._bid[sym_name] = evt.bid
            if evt.ask:
                self._ask[sym_name] = evt.ask
        except Exception as e:
            logger.error("[%s] Spot event error: %s", self.account_id, e)

    def get_symbol_info(self, symbol):
        """Get bid/ask/spread for a specific symbol from internal price cache."""
        sym_upper = symbol.upper()
        # Try exact match first, then case-insensitive
        bid = self._bid.get(symbol) or self._bid.get(sym_upper)
        ask = self._ask.get(symbol) or self._ask.get(sym_upper)
        if not bid and not ask:
            # Case-insensitive fallback
            for k in self._bid.keys():
                if k.upper() == sym_upper:
                    bid = self._bid.get(k)
                    ask = self._ask.get(k)
                    break
        if bid and ask:
            pip_mult = 1000 if "JPY" in sym_upper else 100000
            spread = round((ask - bid) * pip_mult, 1)
            return {"bid": bid, "ask": ask, "spread": spread}
        return None

    # ─── Order Execution ────────────────────────────────────────────────────

    def send_market_order(self, symbol, side, lot_size,
                          session_id=None, comment="", slippage=10):
        """
        Send a market order. Compatible with CTraderFixAccount interface.

        Args:
            symbol: e.g. "EURUSD"
            side: "buy" or "sell"
            lot_size: e.g. 0.01
            session_id: dashboard session ID for tracking
            comment: order comment
            slippage: slippage in points
        """
        if not self.connected:
            logger.error("[%s] Cannot send order — not connected", self.account_id)
            self._report_error(session_id, "Not connected")
            return

        sid = self._get_symbol_id(symbol)
        if not sid:
            logger.error("[%s] Unknown symbol: %s", self.account_id, symbol)
            self._report_error(session_id, f"Unknown symbol: {symbol}")
            return

        # Volume in cTrader units (lots * multiplier, default 100_000 for FX)
        lot_mult = self.config.get("lot_multiplier", 100000)
        volume = int(round(lot_size * lot_mult))

        req = ProtoOANewOrderReq()
        req.ctidTraderAccountId = int(self.config["openapi_account_id"])
        req.symbolId = sid
        req.orderType = 1  # MARKET
        req.tradeSide = 1 if side.lower() == "buy" else 2  # BUY=1, SELL=2
        req.volume = volume
        if comment:
            req.comment = comment
        if slippage:
            req.slippageInPoints = slippage

        # Generate a client order ID for tracking
        client_oid = f"OA_{self.account_id}_{int(time.time()*1000)}"
        req.clientOrderId = client_oid

        # Track pending order
        self._pending_orders[client_oid] = {
            "session_id": session_id,
            "symbol": symbol,
            "side": side,
            "lot_size": lot_size,
            "comment": comment,
            "sent_at": time.time(),
        }

        logger.info("[%s] Sending MARKET %s %s %.2f lots (comment=%s, coid=%s)",
                     self.account_id, side.upper(), symbol, lot_size,
                     comment, client_oid)

        d = self._client.send(req)
        d.addErrback(lambda f: self._on_order_send_failed(f, client_oid, session_id))

        # Also ensure we're subscribed to this symbol's market data
        self.subscribe_symbol(symbol)

    def close_position(self, pos_id, symbol, side, lot_size,
                       session_id=None, comment="", is_rollback=False):
        """
        Close a position. Compatible with CTraderFixAccount interface.

        Args:
            pos_id: position ID (string or int)
            symbol: e.g. "EURUSD" (for reference only)
            side: original side of the position
            lot_size: lots to close
            session_id: dashboard session ID
            comment: for tracking
            is_rollback: whether this is a rollback close
        """
        if not self.connected:
            logger.error("[%s] Cannot close — not connected", self.account_id)
            self._report_error(session_id, "Not connected")
            return

        lot_mult = self.config.get("lot_multiplier", 100000)
        volume = int(round(lot_size * lot_mult))

        req = ProtoOAClosePositionReq()
        req.ctidTraderAccountId = int(self.config["openapi_account_id"])
        req.positionId = int(pos_id)
        req.volume = volume

        # Track as pending
        client_oid = f"CL_{self.account_id}_{pos_id}_{int(time.time()*1000)}"
        self._pending_orders[client_oid] = {
            "session_id": session_id,
            "symbol": symbol,
            "side": side,
            "lot_size": lot_size,
            "comment": comment,
            "pos_id": pos_id,
            "is_close": True,
            "is_rollback": is_rollback,
            "sent_at": time.time(),
        }

        logger.info("[%s] Closing position %s (%s %.2f lots)",
                     self.account_id, pos_id, symbol, lot_size)

        d = self._client.send(req)
        d.addErrback(lambda f: self._on_order_send_failed(f, client_oid, session_id))

    def _on_order_send_failed(self, failure, client_oid, session_id):
        logger.error("[%s] Order send failed: %s", self.account_id, failure)
        self._pending_orders.pop(client_oid, None)
        self._report_error(session_id, f"Send failed: {failure}")

    # ─── Execution Event Handling ───────────────────────────────────────────

    def _handle_execution_event(self, msg):
        """Handle ProtoOAExecutionEvent — fills, accepts, rejects."""
        try:
            evt = Protobuf.extract(msg)
            exec_type = evt.executionType
            # exec_type: 2=ACCEPTED, 3=FILLED, 7=REJECTED, 11=PARTIAL_FILL

            position = evt.position if evt.HasField("position") else None
            order = evt.order if evt.HasField("order") else None
            deal = evt.deal if evt.HasField("deal") else None

            # Find the pending order by clientOrderId
            client_oid = order.clientOrderId if order and order.clientOrderId else None
            pending = self._pending_orders.get(client_oid) if client_oid else None

            # Try to match by positionId if no clientOrderId match
            if not pending and position:
                for coid, p in list(self._pending_orders.items()):
                    if p.get("is_close") and str(p.get("pos_id")) == str(position.positionId):
                        pending = p
                        client_oid = coid
                        break

            if exec_type == 3:  # ORDER_FILLED
                self._on_order_filled(evt, pending, client_oid)
            elif exec_type == 11:  # ORDER_PARTIAL_FILL
                self._on_order_filled(evt, pending, client_oid)
            elif exec_type == 2:  # ORDER_ACCEPTED
                logger.info("[%s] Order accepted (coid=%s)", self.account_id, client_oid)
            elif exec_type == 7:  # ORDER_REJECTED
                error_code = evt.errorCode if evt.errorCode else "unknown"
                logger.error("[%s] Order REJECTED: %s (coid=%s)",
                             self.account_id, error_code, client_oid)
                if pending:
                    self._report_error(pending.get("session_id"),
                                       f"Order rejected: {error_code}")
                    self._pending_orders.pop(client_oid, None)
            else:
                logger.debug("[%s] Execution type=%d", self.account_id, exec_type)

        except Exception as e:
            logger.error("[%s] Execution event error: %s", self.account_id, e, exc_info=True)

    def _on_order_filled(self, evt, pending, client_oid):
        """Process a filled order — report back to dashboard."""
        try:
            position = evt.position if evt.HasField("position") else None
            order = evt.order if evt.HasField("order") else None
            deal = evt.deal if evt.HasField("deal") else None

            pos_id = position.positionId if position else None
            exec_price = order.executionPrice if order and order.executionPrice else 0
            filled_vol = deal.filledVolume if deal and deal.filledVolume else 0
            lot_mult = self.config.get("lot_multiplier", 100000)
            filled_lots = filled_vol / lot_mult if filled_vol else 0

            sym_id = position.tradeData.symbolId if position else None
            symbol = self._get_symbol_name(sym_id) if sym_id else "?"

            is_close = (order and order.closingOrder) if order else False
            if pending and pending.get("is_close"):
                is_close = True

            session_id = pending.get("session_id") if pending else None

            logger.info("[%s] FILL: pos=%s %s price=%.5f vol=%.2f lots close=%s session=%s",
                        self.account_id, pos_id, symbol, exec_price, filled_lots,
                        is_close, session_id)

            # Report to dashboard via trade_result callback
            if session_id:
                self._report_fill(session_id, pos_id, exec_price, filled_lots,
                                  symbol, is_close, pending)

            # Update position tracking
            if position:
                from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAPositionStatus
                if position.positionStatus == ProtoOAPositionStatus.Value("POSITION_STATUS_CLOSED"):
                    self._positions.pop(pos_id, None)
                else:
                    self._positions[pos_id] = {
                        "symbol": symbol,
                        "side": "buy" if position.tradeData.tradeSide == 1 else "sell",
                        "volume": position.tradeData.volume,
                        "price": position.price if position.price else exec_price,
                    }

            # Clean up pending
            if client_oid:
                self._pending_orders.pop(client_oid, None)

        except Exception as e:
            logger.error("[%s] Fill processing error: %s", self.account_id, e, exc_info=True)

    def _report_fill(self, session_id, pos_id, price, lots, symbol, is_close, pending):
        """Report a fill/close to the dashboard (same as FIX execution report)."""
        try:
            trade_result_url = self.dd.get("dashboard_url", "http://127.0.0.1")
            url = f"{trade_result_url}/api/trade_result"
            comment = pending.get("comment", "") if pending else ""
            side = pending.get("side", "buy") if pending else "buy"
            spread = None
            bid = self._bid.get(symbol)
            ask = self._ask.get(symbol)
            if bid and ask:
                spread = round(abs(ask - bid) * 100000, 1)  # approx

            is_rollback = pending.get("is_rollback", False) if pending else False
            status = "rollback_closed" if is_rollback else ("closed" if is_close else "filled")

            payload = {
                "session_id": session_id,
                "account": self.account_id,
                "status": status,
                "ticket": str(pos_id),
                "pos_id": str(pos_id),
                "detail": f"OA {side} {symbol} {lots:.2f} @ {price:.5f}",
                "spread": spread,
                "price": price,
            }
            resp = _requests_mod.post(url, json=payload, timeout=5)
            logger.info("[%s] Reported %s to dashboard: HTTP %d",
                        self.account_id, "close" if is_close else "fill", resp.status_code)
        except Exception as e:
            logger.error("[%s] Failed to report fill: %s", self.account_id, e)

    def _report_error(self, session_id, detail):
        """Report an error to the dashboard."""
        if not session_id:
            return
        try:
            url = f"{self.dd.get('dashboard_url', 'http://127.0.0.1')}/api/trade_result"
            payload = {
                "session_id": session_id,
                "account": self.account_id,
                "status": "error",
                "detail": detail,
            }
            _requests_mod.post(url, json=payload, timeout=5)
        except Exception:
            pass

    def _handle_order_error(self, msg):
        """Handle ProtoOAOrderErrorEvent."""
        try:
            evt = Protobuf.extract(msg)
            logger.error("[%s] Order error: %s (code=%s)",
                         self.account_id, evt.description if hasattr(evt, 'description') else '?',
                         evt.errorCode if hasattr(evt, 'errorCode') else '?')
        except Exception as e:
            logger.error("[%s] Order error parse: %s", self.account_id, e)

    def _handle_error(self, msg):
        """Handle ProtoOAErrorRes."""
        try:
            evt = Protobuf.extract(msg)
            error_code = str(evt.errorCode) if hasattr(evt, 'errorCode') else ''
            description = str(evt.description) if hasattr(evt, 'description') else '?'

            # ALREADY_LOGGED_IN means app/account is already authenticated
            # on this TCP session — treat as success and proceed
            if 'ALREADY_LOGGED_IN' in error_code:
                logger.info("[%s] Already authenticated — proceeding to account auth",
                            self.account_id)
                self._authenticated_app = True
                if not self._authenticated_account:
                    # Send account auth
                    req = ProtoOAAccountAuthReq()
                    req.ctidTraderAccountId = int(self.config["openapi_account_id"])
                    req.accessToken = self.config["openapi_access_token"]
                    d = self._client.send(req)
                    d.addErrback(lambda f: logger.error("[%s] Acct auth send failed", self.account_id))
                return

            # ACCESS_TOKEN_INVALID — refresh the token and retry
            if 'ACCESS_TOKEN_INVALID' in error_code or 'TOKEN_EXPIRED' in error_code:
                logger.warning("[%s] Access token expired — attempting refresh...", self.account_id)
                new_token = self._refresh_access_token()
                if new_token:
                    logger.info("[%s] Token refreshed successfully — retrying account auth", self.account_id)
                    req = ProtoOAAccountAuthReq()
                    req.ctidTraderAccountId = int(self.config["openapi_account_id"])
                    req.accessToken = new_token
                    d = self._client.send(req)
                    d.addErrback(lambda f: logger.error("[%s] Acct auth after refresh failed: %s", self.account_id, f))
                else:
                    logger.error("[%s] Token refresh failed — manual re-auth may be required", self.account_id)
                return

            if 'INVALID_REQUEST' in error_code and 'not authorized' in description.lower():
                logger.warning("[%s] Session invalidated (INVALID_REQUEST: not authorized) — resetting auth state",
                               self.account_id)
                self._authenticated_account = False
                self._authenticated_app = False
                self._api_error_count = 0  # re-auth will be triggered by heartbeat
                return

            logger.error("[%s] API error: %s (code=%s)",
                         self.account_id, description, error_code)
        except Exception as e:
            logger.error("[%s] Error response parse: %s", self.account_id, e)

    def _refresh_access_token(self):
        """Refresh the OAuth2 access token using the refresh token.
        Returns the new access token string, or None on failure."""
        try:
            rt = self.config.get("openapi_refresh_token")
            if not rt:
                logger.error("[%s] No refresh token configured — cannot refresh", self.account_id)
                return None
            auth = Auth(self.config["openapi_client_id"],
                        self.config["openapi_client_secret"], "")
            result = auth.refreshToken(rt)
            if result and "accessToken" in result:
                self.config["openapi_access_token"] = result["accessToken"]
                if "refreshToken" in result:
                    self.config["openapi_refresh_token"] = result["refreshToken"]
                logger.info("[%s] Token refreshed — new token stored", self.account_id)
                # Persist to disk if save callback is available
                if hasattr(self, '_save_config_callback') and self._save_config_callback:
                    try:
                        self._save_config_callback()
                    except Exception as e:
                        logger.warning("[%s] Could not persist refreshed token: %s", self.account_id, e)
                return result["accessToken"]
            logger.error("[%s] Token refresh returned no accessToken: %s", self.account_id, result)
            return None
        except Exception as e:
            logger.error("[%s] Token refresh error: %s", self.account_id, e)
            return None

    # ─── Position Reconciliation ────────────────────────────────────────────

    def _reconcile_positions(self):
        """Fetch existing positions on connect."""
        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = int(self.config["openapi_account_id"])
        d = self._client.send(req)
        d.addErrback(lambda f: self._on_request_error(f, "Reconcile"))

    def _handle_reconcile(self, msg):
        """Process reconcile response — cache existing positions, swap, and open timestamps."""
        try:
            self._api_error_count = 0
            res = Protobuf.extract(msg)
            self._positions.clear()
            self._position_swaps.clear()
            self._position_margins.clear()
            for pos in res.position:
                sym_name = self._get_symbol_name(pos.tradeData.symbolId)
                pid = pos.positionId
                # Extract open timestamp (milliseconds since epoch)
                open_ts = None
                try:
                    if hasattr(pos.tradeData, 'openTimestamp') and pos.tradeData.openTimestamp:
                        open_ts = int(pos.tradeData.openTimestamp)
                except Exception:
                    pass
                self._positions[pid] = {
                    "symbol": sym_name,
                    "side": "buy" if pos.tradeData.tradeSide == 1 else "sell",
                    "volume": pos.tradeData.volume,
                    "price": pos.price if pos.price else 0,
                    "comment": pos.tradeData.comment if pos.tradeData.comment else "",
                    "open_ts": open_ts,
                }
                # Store swap per position (raw moneyDigits-encoded int64)
                try:
                    if hasattr(pos, 'swap') and pos.swap:
                        self._position_swaps[pid] = pos.swap
                except Exception:
                    pass
                # Store used margin per position (raw moneyDigits-encoded uint64)
                try:
                    if hasattr(pos, 'usedMargin') and pos.usedMargin:
                        self._position_margins[pid] = pos.usedMargin
                except Exception:
                    pass
            logger.info("[%s] Reconciled %d positions (swaps: %d, margins: %d)",
                        self.account_id, len(self._positions),
                        len(self._position_swaps), len(self._position_margins))
        except Exception as e:
            logger.error("[%s] Reconcile parse error: %s", self.account_id, e)

    # ─── Unrealized PnL / Margin Tracking ────────────────────────────────────

    def _request_unrealized_pnl(self):
        """Request unrealized PnL for all open positions."""
        if not self._authenticated_account:
            return
        if not self._pt_pnl_res:
            return  # PnL request type not available in this package version
        try:
            req = ProtoOAGetPositionUnrealizedPnLReq()
            req.ctidTraderAccountId = int(self.config["openapi_account_id"])
            self._last_pnl_request = time.time()
            d = self._client.send(req)
            d.addErrback(lambda f: self._on_request_error(f, "PnL req"))
        except Exception as e:
            logger.error("[%s] PnL request error: %s", self.account_id, e)
            self._last_pnl_request = time.time()  # prevent rapid retries

    def _handle_unrealized_pnl(self, msg):
        """Handle ProtoOAGetPositionUnrealizedPnLRes — update PnL per position."""
        try:
            self._api_error_count = 0
            res = Protobuf.extract(msg)
            md = self._money_digits
            logger.info("[%s] PnL response received, fields: %s", self.account_id,
                        [f.name for f in res.DESCRIPTOR.fields])
            self._unrealized_pnl.clear()
            total = 0
            for entry in res.positionUnrealizedPnL:
                # Try multiple field names for compatibility
                net_pnl_raw = 0
                if hasattr(entry, 'netUnrealizedPnL') and entry.netUnrealizedPnL:
                    net_pnl_raw = entry.netUnrealizedPnL
                elif hasattr(entry, 'grossUnrealizedPnL') and entry.grossUnrealizedPnL:
                    net_pnl_raw = entry.grossUnrealizedPnL
                net_pnl = net_pnl_raw / (10 ** md) if net_pnl_raw else 0
                self._unrealized_pnl[entry.positionId] = net_pnl
                total += net_pnl
            self._total_unrealized_pnl = round(total, 2)
            logger.info("[%s] PnL computed: total=%.2f from %d positions",
                        self.account_id, self._total_unrealized_pnl, len(self._unrealized_pnl))
        except Exception as e:
            logger.error("[%s] PnL response error: %s", self.account_id, e, exc_info=True)

    def _handle_margin_changed(self, msg):
        """Handle ProtoOAMarginChangedEvent — track used margin per position."""
        try:
            evt = Protobuf.extract(msg)
            if evt.positionId and evt.usedMargin is not None:
                self._position_margins[evt.positionId] = evt.usedMargin
        except Exception as e:
            logger.error("[%s] Margin changed error: %s", self.account_id, e)

    # ─── Trader Info (Balance/Equity/Leverage) ──────────────────────────────

    def _request_trader_info(self):
        if not self._authenticated_account:
            return
        req = ProtoOATraderReq()
        req.ctidTraderAccountId = int(self.config["openapi_account_id"])
        self._last_trader_request = time.time()
        d = self._client.send(req)
        d.addErrback(lambda f: self._on_request_error(f, "Trader req"))

    def _handle_trader_response(self, msg):
        try:
            self._api_error_count = 0
            res = Protobuf.extract(msg)
            trader = res.trader
            md = trader.moneyDigits if trader.moneyDigits else 2
            self._money_digits = md
            self._balance = trader.balance / (10 ** md)
            if trader.maxLeverage and trader.maxLeverage > 0:
                self._leverage = trader.maxLeverage
            elif trader.leverageInCents and trader.leverageInCents > 0:
                self._leverage = trader.leverageInCents // 100
            logger.info("[%s] Balance=%.2f Leverage=%s", self.account_id,
                        self._balance, self._leverage)
        except Exception as e:
            logger.error("[%s] Trader response error: %s", self.account_id, e)

    def _on_request_error(self, failure, label):
        """Track consecutive API errors and pause polling after 3 failures.
        Does NOT reset auth state — timeouts on data requests don't mean the
        session is broken. Auth is reset only by _handle_error on INVALID_REQUEST."""
        self._api_error_count += 1
        logger.error("[%s] %s failed: %s", self.account_id, label, failure)
        if self._api_error_count >= 3:
            logger.warning("[%s] %d consecutive API errors — pausing polling for 60s",
                           self.account_id, self._api_error_count)
            self._api_backoff_until = time.time() + 60
            self._api_error_count = 0  # reset so next 3 failures trigger again

    # ─── Heartbeat / Data Feed ─────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        """Feed bid/ask/balance/leverage into ea_account_info periodically.
        Also monitors connection and triggers re-auth on reconnect."""
        _reconnect_logged = 0  # suppress repeated log spam
        _startup_ts = time.time()  # grace period to let TCP connect first
        while self._running:
            try:
                now = time.time()
                self.dd["ea_heartbeats"][self.account_id] = now

                info = self.dd["ea_account_info"].get(self.account_id, {})

                # Feed price data: find first symbol with bid/ask
                for sym_name in list(self._bid.keys()):
                    bid = self._bid.get(sym_name)
                    ask = self._ask.get(sym_name)
                    if bid and ask:
                        info["bid"] = bid
                        info["ask"] = ask
                        info["spread"] = round((ask - bid) * 100000, 1)  # approx 5-digit spread
                        info["symbol"] = sym_name
                        break

                # Feed balance/equity/leverage
                if self._balance is not None:
                    info["balance"] = self._balance
                if self._leverage is not None:
                    info["leverage"] = self._leverage

                # Equity = balance + unrealized PnL (if no PnL data yet, equity = balance)
                if self._balance is not None:
                    pnl = self._total_unrealized_pnl if self._total_unrealized_pnl is not None else 0
                    info["equity"] = round(self._balance + pnl, 2)

                # PnL from unrealized PnL polling
                if self._total_unrealized_pnl is not None:
                    info["total_pnl"] = self._total_unrealized_pnl

                # Swap (from reconcile position data, raw values are moneyDigits-encoded)
                md = self._money_digits
                if self._position_swaps:
                    info["total_swap"] = round(sum(self._position_swaps.values()) / (10 ** md), 2)

                # Margin (from reconcile usedMargin or ProtoOAMarginChangedEvent)
                if self._position_margins:
                    info["margin"] = round(sum(self._position_margins.values()) / (10 ** md), 2)
                elif self._leverage and self._positions and len(self._positions) > 0:
                    # Fallback: estimate margin from volume/leverage
                    # margin ≈ total_volume_notional / leverage
                    # For simplicity, use volume * avg open price / leverage
                    total_margin = 0
                    for p in self._positions.values():
                        vol = p.get("volume", 0)  # in units (e.g. 100000 for 1 lot)
                        opx = p.get("price", 0)
                        if vol and opx and self._leverage:
                            total_margin += (vol * opx) / self._leverage
                    if total_margin > 0:
                        info["margin"] = round(total_margin, 2)

                # Position age (days since oldest open position)
                oldest_ts = None
                for p in self._positions.values():
                    ots = p.get("open_ts")
                    if ots and (oldest_ts is None or ots < oldest_ts):
                        oldest_ts = ots
                if oldest_ts:
                    # open_ts is in milliseconds since epoch
                    open_dt = datetime.fromtimestamp(oldest_ts / 1000.0, tz=NY_TZ) - timedelta(hours=17)
                    now_dt = datetime.fromtimestamp(now, tz=NY_TZ) - timedelta(hours=17)
                    info["oldest_position_age"] = max(0, (now_dt.date() - open_dt.date()).days)
                else:
                    info["oldest_position_age"] = 0

                info["last_update"] = now
                info["fix_account"] = True  # so dashboard treats it like a managed account
                info["trade_connected"] = self.connected
                info["quote_connected"] = self.connected
                info["openapi_connected"] = self.connected
                info["positions"] = len(self._positions)
                lot_mult = self.config.get("lot_multiplier", 100000)
                _vol_div = lot_mult * 100  # cTrader volume to lots divisor
                # Signed lots: buy = positive, sell = negative
                # cTrader OA volumes include a 100x centigram factor on top of lot_mult
                if self._positions:
                    _bv = sum(p.get("volume", 0) for p in self._positions.values() if p.get("side") == "buy")
                    _sv = sum(p.get("volume", 0) for p in self._positions.values() if p.get("side") == "sell")
                    info["total_lots"] = round((_bv - _sv) / _vol_div, 2)
                    # Per-instrument lots breakdown
                    _lbi = {}
                    for p in self._positions.values():
                        sym = p.get("symbol", "Unknown")
                        lots = round(p.get("volume", 0) / _vol_div, 2)
                        if sym not in _lbi:
                            _lbi[sym] = {"buy": 0, "sell": 0}
                        if p.get("side") == "buy":
                            _lbi[sym]["buy"] = round(_lbi[sym]["buy"] + lots, 2)
                        else:
                            _lbi[sym]["sell"] = round(_lbi[sym]["sell"] + lots, 2)
                    info["lots_by_instrument"] = _lbi

                    # Per-instrument swap breakdown
                    _sbi = {}
                    for pid, p in self._positions.items():
                        sym = p.get("symbol", "Unknown")
                        raw_swap = self._position_swaps.get(pid, 0)
                        swap_val = round(raw_swap / (10 ** md), 2)
                        _sbi[sym] = round(_sbi.get(sym, 0.0) + swap_val, 2)
                    info["swap_by_instrument"] = _sbi
                else:
                    info["total_lots"] = 0
                    info["lots_by_instrument"] = {}
                    info["swap_by_instrument"] = {}
                self.dd["ea_account_info"][self.account_id] = info

                # API error backoff gate — skip polling during cooldown
                _in_backoff = time.time() < self._api_backoff_until

                # Periodically request unrealized PnL (every 5s)
                if (not _in_backoff and self._authenticated_account and
                    now - self._last_pnl_request > 5 and
                    len(self._positions) > 0):
                    self._request_unrealized_pnl()

                # Periodically re-reconcile for swap/position freshness (every 60s)
                if (not _in_backoff and self._authenticated_account and
                    now - self._last_reconcile_request > 60):
                    self._last_reconcile_request = now
                    self._reconcile_positions()

                # Periodically re-request trader info
                if (not _in_backoff and self._authenticated_account and
                    now - self._last_trader_request > self._poll_interval):
                    self._request_trader_info()

                # ── Auto-reconnect monitoring ──
                # Skip reconnect logic during startup grace period (10s)
                if now - _startup_ts < 10:
                    pass  # let _on_connected handle initial auth
                elif not self._connected and self._running:
                    _reconnect_logged += 1
                    if _reconnect_logged <= 3 or _reconnect_logged % 30 == 0:
                        logger.info("[%s] Open API account disconnected — Twisted handles TCP reconnect",
                                    self.account_id)
                elif self._connected and not self._authenticated_app:
                    # TCP reconnected but not yet authenticated — send app auth
                    # Guard: only retry if we haven't recently sent an auth request
                    # Do NOT call _on_connected() which destructively resets all state
                    if now - getattr(self, '_last_auth_attempt', 0) > 30:
                        self._last_auth_attempt = now
                        logger.info("[%s] Open API sending app auth (recovery)", self.account_id)
                        try:
                            req = ProtoOAApplicationAuthReq()
                            req.clientId = self.config["openapi_client_id"]
                            req.clientSecret = self.config["openapi_client_secret"]
                            d = self._client.send(req)
                            d.addErrback(lambda f: logger.error("[%s] App auth retry failed: %s", self.account_id, f))
                        except Exception as e:
                            logger.error("[%s] App auth retry error: %s", self.account_id, e)
                    _reconnect_logged = 0
                elif self._connected and self._authenticated_account:
                    if _reconnect_logged > 0:
                        logger.info("[%s] Open API account fully reconnected", self.account_id)
                        _reconnect_logged = 0

            except Exception as e:
                logger.error("[%s] Heartbeat error: %s", self.account_id, e)
            time.sleep(1)

    # ─── Deal History (PnL Report) ───────────────────────────────────────────

    def get_deal_history(self, from_ts, to_ts, fee_keywords=None):
        """Retrieve closed deal history from cTrader and compute PnL totals.

        Uses ProtoOADealListReq. Sends the request via Twisted reactor and
        waits for the response on a threading.Event (max 15s timeout).

        Args:
            from_ts: Start timestamp (Unix epoch seconds, UTC).
            to_ts:   End timestamp (Unix epoch seconds, UTC).
            fee_keywords: List of strings for fee comment matching (not used
                          for cTrader — fees are captured as commission).

        Returns:
            dict with keys: pnl (float), swap (float), fees (float), deal_count (int),
            or None on failure.
        """
        if not self.connected or not self._client:
            logger.warning("[%s] get_deal_history: not connected", self.account_id)
            return None

        try:
            from twisted.internet import reactor as _reactor

            # Prepare the request
            req = ProtoOADealListReq()
            req.ctidTraderAccountId = int(self.config["openapi_account_id"])
            req.fromTimestamp = int(from_ts * 1000)  # milliseconds
            req.toTimestamp = int(to_ts * 1000)       # milliseconds
            req.maxRows = 10000  # generous limit

            # Reset the event and result
            self._deal_history_result = None
            self._deal_history_event.clear()

            logger.info("[%s] Requesting cTrader deal history %s → %s",
                        self.account_id,
                        time.strftime("%Y-%m-%d", time.gmtime(from_ts)),
                        time.strftime("%Y-%m-%d", time.gmtime(to_ts)))

            # Send from Twisted reactor thread
            def _send():
                try:
                    d = self._client.send(req)
                    d.addErrback(lambda f: self._deal_history_err(f))
                except Exception as e:
                    logger.error("[%s] Deal list send error: %s", self.account_id, e)
                    self._deal_history_event.set()  # unblock waiter

            _reactor.callFromThread(_send)

            # Wait for response (up to 15s)
            got_it = self._deal_history_event.wait(timeout=15.0)
            if not got_it:
                logger.warning("[%s] Deal history request timed out", self.account_id)
                return None

            return self._deal_history_result

        except Exception as e:
            logger.error("[%s] get_deal_history error: %s", self.account_id, e)
            return None

    def _deal_history_err(self, failure):
        """Errback for deal list request."""
        logger.error("[%s] Deal list request failed: %s", self.account_id, failure)
        self._deal_history_event.set()

    def _handle_deal_list_response(self, msg):
        """Handle ProtoOADealListRes — compute PnL from closed deals."""
        try:
            res = Protobuf.extract(msg)
            total_pnl = 0.0
            total_swap = 0.0
            total_commission = 0.0
            deal_count = 0

            for deal in res.deal:
                if not deal.HasField('closePositionDetail'):
                    continue  # Only interested in closing deals

                cpd = deal.closePositionDetail
                md = cpd.moneyDigits if cpd.moneyDigits else self._money_digits
                divisor = 10 ** md

                gross = cpd.grossProfit / divisor if cpd.grossProfit else 0
                swap = cpd.swap / divisor if cpd.swap else 0
                comm = cpd.commission / divisor if cpd.commission else 0

                total_pnl += gross
                total_swap += swap
                total_commission += comm
                deal_count += 1

            self._deal_history_result = {
                "pnl": round(total_pnl, 2),
                "swap": round(total_swap, 2),
                "fees": round(total_commission, 2),  # commission = fees for cTrader
                "deal_count": deal_count,
            }
            logger.info("[%s] cTrader deal history: %d closing deals → pnl=%.2f swap=%.2f fees=%.2f",
                        self.account_id, deal_count,
                        self._deal_history_result["pnl"],
                        self._deal_history_result["swap"],
                        self._deal_history_result["fees"])
        except Exception as e:
            logger.error("[%s] Deal list response error: %s", self.account_id, e, exc_info=True)
            self._deal_history_result = None
        finally:
            self._deal_history_event.set()  # unblock the waiting thread

    # ─── Helpers for FixAccountManager compatibility ────────────────────────

    def _get_digits(self, symbol_name):
        """Get decimal digits for a symbol (placeholder — returns 5)."""
        return 5

    def _subscribe_market_data(self):
        """No-op — market data subscription is handled per-symbol."""
        pass

    def _request_security_list(self):
        """No-op — symbols loaded via ProtoOASymbolsListReq."""
        pass


# ─── Helper: generate OAuth URL ────────────────────────────────────────────
# These use lightweight imports (Auth only) to avoid loading Twisted/Protobuf
# which conflicts with pythonnet's .NET CLR used by MT Direct.

def get_oauth_url(client_id, redirect_uri):
    """Generate the OAuth authorization URL for cTrader Open API."""
    try:
        from ctrader_open_api import Auth
        auth = Auth(client_id, "", redirect_uri)
        return auth.getAuthUri()
    except ImportError as e:
        logger.error("Cannot import ctrader_open_api.Auth: %s", e)
        return None


def exchange_auth_code(client_id, client_secret, redirect_uri, auth_code):
    """Exchange OAuth authorization code for access + refresh tokens."""
    try:
        from ctrader_open_api import Auth
        auth = Auth(client_id, client_secret, redirect_uri)
        return auth.getToken(auth_code)
    except ImportError as e:
        logger.error("Cannot import ctrader_open_api.Auth: %s", e)
        return None
