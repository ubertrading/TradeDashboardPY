#!/usr/bin/env python3
"""
fix_connector.py — cTrader FIX 4.4 Protocol Connector

Manages FIX connections to cTrader for trading and market data.
Designed to integrate with trade_dashboard.py, feeding data into
the same ea_heartbeats/ea_account_info dicts so the entire dashboard
works identically for FIX accounts.

Architecture:
  FixSession       — Low-level TCP/SSL FIX 4.4 connection (heartbeat, logon, messages)
  CTraderFixAccount — One cTrader account (TRADE + QUOTE sessions)
  FixAccountManager — Manages all FIX accounts, runs command loop
"""

import json
import logging
import os
import queue
import re
import socket
import ssl
import struct
import threading
import time
import urllib.request
from ctrader_openapi import CTraderOpenApiClient, CTraderOpenApiAccount
import uuid
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
except ImportError:
    import pytz
    NY_TZ = pytz.timezone("America/New_York")

import simplefix

logger = logging.getLogger("fix_connector")

# ─── FIX Tag Constants ──────────────────────────────────────────────────────
TAG_BEGINSTRING    = 8
TAG_BODYLENGTH     = 9
TAG_MSGTYPE        = 35
TAG_SENDERCOMPID   = 49
TAG_TARGETCOMPID   = 56
TAG_MSGSEQNUM      = 34
TAG_SENDINGTIME    = 52
TAG_SENDERSUBID    = 50
TAG_TARGETSUBID    = 57
TAG_CHECKSUM       = 10

TAG_ENCRYPTMETHOD  = 98
TAG_HEARTBTINT     = 108
TAG_RESETSEQNUMFLAG = 141
TAG_USERNAME       = 553
TAG_PASSWORD       = 554
TAG_TESTREQID      = 112
TAG_TEXT            = 58

# Application tags
TAG_CLORDID        = 11
TAG_ORDERID        = 37
TAG_ORIGCLORDID    = 41
TAG_SYMBOL         = 55
TAG_SIDE           = 54
TAG_TRANSACTTIME   = 60
TAG_ORDERQTY       = 38
TAG_ORDTYPE        = 40
TAG_TIMEINFORCE    = 59
TAG_PRICE          = 44
TAG_STOPPX         = 99
TAG_EXECTYPE       = 150
TAG_ORDSTATUS      = 39
TAG_AVGPX          = 6
TAG_CUMQTY         = 14
TAG_LASTQTY        = 32
TAG_LEAVESQTY      = 151
TAG_POSMAINTRPTID  = 721
TAG_DESIGNATION    = 5765

# Market data tags
TAG_MDREQID        = 262
TAG_SUBSCRIPTIONREQUESTTYPE = 263
TAG_MARKETDEPTH    = 264
TAG_MDUPDATETYPE   = 265
TAG_NOMDENTRIES    = 268
TAG_NOMDENTRTYPES  = 267
TAG_MDENTRYTYPE    = 269
TAG_MDENTRYPX      = 270
TAG_MDENTRYSIZE    = 271
TAG_MDENTRYID      = 278
TAG_MDUPDATEACTION = 279
TAG_NORELATEDSYM   = 146

# Security list tags
TAG_SECURITYREQID  = 320
TAG_SECURITYLISTTYPE = 559
TAG_SECURITYRESPID = 322
TAG_SECURITYREQRESULT = 560
TAG_SYMBOLNAME     = 1007
TAG_SYMBOLDIGITS   = 1008

# Position report tags
TAG_POSREQID       = 710
TAG_POSREQRESULT   = 728
TAG_TOTALNUMPOS    = 727
TAG_NOPOSITIONS    = 702
TAG_LONGQTY        = 704
TAG_SHORTQTY       = 705
TAG_SETTLPRICE     = 730

# Mass status
TAG_MASSSTATUSREQID   = 584
TAG_MASSSTATUSREQTYPE = 585
TAG_ISSUEDATE         = 225

# Reject
TAG_REFSEQNUM          = 45
TAG_REFMSGTYPE         = 372
TAG_BUSINESSREJECTREFID = 379
TAG_BUSINESSREJECTREASON = 380

# Collateral / Account info tags (FIX 4.4 standard)
TAG_COLLREQID          = 894
TAG_COLLSTATUS         = 910
TAG_COLLINQUIRYID      = 909
TAG_ACCOUNT            = 1
TAG_CURRENCY           = 15
TAG_TOTALNETVALUE      = 900     # Equity / Net Liquidation Value
TAG_MARGINEXCESS       = 899     # Free margin
TAG_STARTCASH          = 921     # Balance (starting cash)
TAG_ENDCASH            = 922     # Balance (ending cash / current balance)
TAG_CASHOUTSTANDING    = 901     # Margin used

# ExecType values
EXECTYPE_NEW      = b'0'
EXECTYPE_CANCELED = b'4'
EXECTYPE_REPLACED = b'5'
EXECTYPE_REJECTED = b'8'
EXECTYPE_EXPIRED  = b'C'
EXECTYPE_FILL     = b'F'
EXECTYPE_STATUS   = b'I'

# Swissquote ExecType (uses FIX 4.4 standard values, different from cTrader)
SQ_EXECTYPE_NEW      = b'0'
SQ_EXECTYPE_FILLED    = b'2'
SQ_EXECTYPE_CANCELED  = b'4'
SQ_EXECTYPE_REJECTED  = b'8'
SQ_EXECTYPE_CALCULATED = b'B'

# OrdStatus values
ORDSTATUS_NEW           = b'0'
ORDSTATUS_PARTIALLY     = b'1'
ORDSTATUS_FILLED        = b'2'
ORDSTATUS_REJECTED      = b'8'
ORDSTATUS_CANCELED      = b'4'

# Side
SIDE_BUY  = b'1'
SIDE_SELL = b'2'

# OrdType
ORDTYPE_MARKET = b'1'
ORDTYPE_LIMIT  = b'2'
ORDTYPE_STOP   = b'3'

# Swissquote custom tags
TAG_CLORDLINKID        = 583      # PositionID to close/reduce
TAG_SQ_SENDMISSED      = 10104    # SendMissedMessages (in Logon)
TAG_SQ_BALANCE         = 10103    # AccountBalance (in ExecReport / PosReqAck)
TAG_SQ_EQUITY          = 30005    # Equity
TAG_SQ_USEDMARGIN      = 30006    # UsedMargin
TAG_SQ_MAINTMARGIN     = 30007    # MaintenanceMargin
TAG_SQ_LINKEDPOSITIONS = 10105    # Comma-separated PositionIDs in ExecReport
TAG_SQ_CLIENTSLIPPAGE  = 10106    # Slippage for Limit FOK orders
TAG_QUOTECONDITION     = 276      # A=Open/Tradable, B=Closed/Indicative
TAG_POSREQTYPE         = 724
TAG_ACCOUNTTYPE        = 581
TAG_CLEARINGBIZDATE    = 715
TAG_UNSOLIND           = 325


# ─── FixSession ─────────────────────────────────────────────────────────────
class FixSession:
    """
    Low-level FIX 4.4 session over TCP/SSL.
    Handles connection, heartbeat, logon/logout, sequence numbers, and message I/O.
    """
    def __init__(self, host, port, sender_comp_id, target_comp_id,
                 sender_sub_id="TRADE", target_sub_id="TRADE",
                 username="", password="", heartbeat_interval=30,
                 use_ssl=True, extra_logon_fields=None):
        self.host = host
        self.port = int(port)
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.sender_sub_id = sender_sub_id
        self.target_sub_id = target_sub_id
        self.username = username
        self.password = password
        self.heartbeat_interval = int(heartbeat_interval)
        self.use_ssl = use_ssl
        self._extra_logon_fields = extra_logon_fields or []

        self._sock = None
        self._seq_num = 0
        self._connected = False
        self._logged_in = False
        self._lock = threading.Lock()
        self._recv_buffer = b""
        self._parser = simplefix.FixParser()
        self._last_sent = 0
        self._last_recv = 0
        self._callbacks = {}  # msgtype -> callback function
        self._running = False
        self._thread = None

    @property
    def connected(self):
        return self._connected and self._logged_in

    def register_callback(self, msg_type, callback):
        """Register a callback for a specific MsgType (e.g. b'8' for Execution Report)."""
        self._callbacks[msg_type] = callback

    def _next_seq(self):
        self._seq_num += 1
        return self._seq_num

    def _build_message(self, msg_type, fields):
        """Build a FIX message with standard header/trailer."""
        msg = simplefix.FixMessage()
        msg.append_pair(TAG_BEGINSTRING, "FIX.4.4")
        msg.append_pair(TAG_MSGTYPE, msg_type)
        msg.append_pair(TAG_SENDERCOMPID, self.sender_comp_id)
        msg.append_pair(TAG_TARGETCOMPID, self.target_comp_id)
        msg.append_pair(TAG_MSGSEQNUM, self._next_seq())
        msg.append_pair(TAG_SENDINGTIME, datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        if self.sender_sub_id:
            msg.append_pair(TAG_SENDERSUBID, self.sender_sub_id)
        if self.target_sub_id:
            msg.append_pair(TAG_TARGETSUBID, self.target_sub_id)
        for tag, value in fields:
            msg.append_pair(tag, value)
        return msg

    def connect(self):
        """Establish TCP/SSL connection and send Logon."""
        try:
            raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_sock.settimeout(10)
            if self.use_ssl:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                self._sock = ctx.wrap_socket(raw_sock, server_hostname=self.host)
            else:
                self._sock = raw_sock
            self._sock.connect((self.host, self.port))
            self._connected = True
            self._seq_num = 0
            self._recv_buffer = b""
            self._parser = simplefix.FixParser()
            logger.info("Connected to %s:%d (%s)", self.host, self.port, self.sender_sub_id)
            self._send_logon()
        except Exception as e:
            logger.error("Connection failed to %s:%d: %s", self.host, self.port, e)
            self._connected = False
            raise

    def _send_logon(self):
        """Send Logon message with ResetSeqNumFlag=Y."""
        fields = [
            (TAG_ENCRYPTMETHOD, "0"),
            (TAG_HEARTBTINT, str(self.heartbeat_interval)),
            (TAG_RESETSEQNUMFLAG, "Y"),
            (TAG_USERNAME, self.username),
            (TAG_PASSWORD, self.password),
        ]
        # Append any extra fields (e.g. Swissquote SendMissedMessages)
        fields.extend(self._extra_logon_fields)
        msg = self._build_message("A", fields)
        self._send_raw(msg)
        logger.info("Logon sent to %s:%d (%s)", self.host, self.port, self.sender_sub_id)

    def _send_raw(self, msg):
        """Send a FixMessage over the socket."""
        with self._lock:
            if not self._sock:
                logger.debug("Cannot send FIX message (socket is None): %s", msg)
                return
            try:
                data = msg.encode()
                self._sock.sendall(data)
                self._last_sent = time.time()
            except Exception as e:
                logger.debug("Error sending FIX message: %s", e)

    def send_message(self, msg_type, fields):
        """Build and send an application message."""
        msg = self._build_message(msg_type, fields)
        self._send_raw(msg)
        return msg

    def _send_heartbeat(self, test_req_id=None):
        fields = []
        if test_req_id:
            fields.append((TAG_TESTREQID, test_req_id))
        msg = self._build_message("0", fields)
        self._send_raw(msg)

    def _send_test_request(self):
        test_id = str(int(time.time()))
        fields = [(TAG_TESTREQID, test_id)]
        msg = self._build_message("1", fields)
        self._send_raw(msg)

    def _send_logout(self):
        msg = self._build_message("5", [])
        self._send_raw(msg)
        logger.info("Logout sent")

    def _recv_data(self):
        """Read data from socket, non-blocking with timeout."""
        try:
            self._sock.settimeout(0.5)
            data = self._sock.recv(65536)
            if not data:
                raise ConnectionError("Socket closed by remote")
            self._last_recv = time.time()
            self._parser.append_buffer(data)
        except socket.timeout:
            pass
        except Exception as e:
            logger.error("Recv error: %s", e)
            self._connected = False
            raise

    def _process_messages(self):
        """Parse buffered data and dispatch messages."""
        while True:
            msg = self._parser.get_message()
            if msg is None:
                break
            self._handle_message(msg)

    def _handle_message(self, msg):
        """Dispatch a received FIX message."""
        msg_type = msg.get(TAG_MSGTYPE)
        if msg_type == b'A':
            # Logon response
            self._logged_in = True
            pairs_str = ", ".join(f"{tag.decode() if isinstance(tag, bytes) else tag}={val.decode(errors='ignore') if isinstance(val, bytes) else val}" for tag, val in msg.pairs)
            logger.info("Logon accepted (%s) fields: %s", self.sender_sub_id, pairs_str)
        elif msg_type == b'5':
            # Logout
            text = msg.get(TAG_TEXT)
            logger.warning("Logout received: %s", text.decode() if text else "no reason")
            self._logged_in = False
        elif msg_type == b'0':
            # Heartbeat — no action needed
            pass
        elif msg_type == b'1':
            # Test Request — respond with Heartbeat
            test_id = msg.get(TAG_TESTREQID)
            self._send_heartbeat(test_id.decode() if test_id else None)
        elif msg_type == b'3':
            # Session Reject
            text = msg.get(TAG_TEXT)
            logger.error("Session Reject: %s", text.decode() if text else "unknown")
            # Forward to callback if registered (e.g. to detect unsupported MsgType)
            if msg_type in self._callbacks:
                self._callbacks[msg_type](msg)
        elif msg_type == b'j':
            # Business Message Reject
            text = msg.get(TAG_TEXT)
            logger.error("Business Reject: %s", text.decode() if text else "unknown")
            # Call callback if registered
            if msg_type in self._callbacks:
                self._callbacks[msg_type](msg)
        else:
            # Application message — dispatch to callback
            if msg_type in self._callbacks:
                try:
                    self._callbacks[msg_type](msg)
                except Exception as e:
                    logger.error("Callback error for MsgType %s: %s", msg_type, e)
            else:
                logger.debug("Unhandled MsgType: %s", msg_type)

    def _check_heartbeat(self):
        """Send heartbeat if idle, or test request if no data received."""
        now = time.time()
        if now - self._last_sent > self.heartbeat_interval:
            self._send_heartbeat()
        if self._last_recv and now - self._last_recv > self.heartbeat_interval * 2:
            logger.warning("No data for %ds, sending test request", self.heartbeat_interval * 2)
            self._send_test_request()

    def run_loop(self):
        """Main I/O loop — call from a thread.
        Auto-reconnects with exponential backoff on connection loss."""
        self._running = True
        reconnect_delay = 5
        while self._running:
            if not self._connected:
                # Attempt reconnect with backoff
                logger.info("FIX session %s:%d (%s) — reconnecting in %ds...",
                            self.host, self.port, self.sender_sub_id, reconnect_delay)
                for _ in range(int(reconnect_delay * 2)):
                    if not self._running:
                        return
                    time.sleep(0.5)
                if not self._running:
                    return
                try:
                    # Close old socket cleanly
                    try:
                        if self._sock:
                            self._sock.close()
                    except Exception:
                        pass
                    self._sock = None
                    self._logged_in = False
                    self.connect()
                    if self._connected:
                        logger.info("FIX session %s:%d (%s) — reconnected successfully",
                                    self.host, self.port, self.sender_sub_id)
                        reconnect_delay = 5  # reset backoff
                    else:
                        reconnect_delay = min(reconnect_delay * 2, 60)
                except Exception as e:
                    logger.error("FIX session reconnect failed (%s): %s", self.sender_sub_id, e)
                    reconnect_delay = min(reconnect_delay * 2, 60)
                continue
            try:
                self._recv_data()
                self._process_messages()
                self._check_heartbeat()
            except ConnectionError:
                logger.error("Connection lost (%s) — will auto-reconnect", self.sender_sub_id)
                self._connected = False
                self._logged_in = False
            except Exception as e:
                logger.error("Session loop error: %s", e)
                time.sleep(1)

    def start(self):
        """Connect and start the I/O loop in a background thread."""
        self.connect()
        self._thread = threading.Thread(target=self.run_loop, daemon=True,
                                        name=f"FIX-{self.sender_sub_id}")
        self._thread.start()

    def stop(self):
        """Send Logout and stop the loop."""
        self._running = False
        try:
            if self._logged_in:
                self._send_logout()
        except Exception:
            pass
        try:
            if self._sock:
                self._sock.close()
        except Exception:
            pass
        self._connected = False
        self._logged_in = False


# ─── CTraderFixAccount ───────────────────────────────────────────────────────
class CTraderFixAccount:
    """
    Manages one cTrader account: TRADE session + QUOTE session.
    Feeds price/heartbeat data into the dashboard's data structures.
    """
    def __init__(self, account_id, config, dashboard_data):
        """
        Args:
            account_id: Unique string identifier for this FIX account
            config: Dict with host, trade_port, quote_port, sender_comp_id,
                    target_comp_id, username, password, heartbeat_interval,
                    lot_multiplier, symbols
            dashboard_data: Dict with references to dashboard's shared data:
                - ea_heartbeats, ea_account_info, sessions, lock,
                  event_log, _log_event, _save_sessions, trade_result_handler
        """
        self.account_id = account_id
        self.config = config
        self.dd = dashboard_data
        self.label = config.get("label", account_id)

        self.lot_multiplier = config.get("lot_multiplier", 100000)

        # Symbol mapping: name -> numeric ID and reverse
        self._symbols_by_name = {}   # {"EUR/USD": 1, "EURUSD": 1}
        self._symbols_by_id = {}     # {1: "EUR/USD"}
        self._symbol_digits = {}     # symbol_id -> digits

        # Load symbols from file first, then overlay config symbols
        symbol_file = config.get("symbol_file", "")
        if symbol_file:
            self._load_symbol_file(symbol_file)
        # Config symbols override/supplement file
        for name, sid in config.get("symbols", {}).items():
            self._register_symbol(name, int(sid))

        # Position tracking: order ClOrdID -> {session_id, side, ...}
        self._pending_orders = {}  # clordid -> order info
        self._position_ids = {}    # posmaintrptid -> {symbol, side, qty, ...}
        self._last_pos_request = 0
        self._current_pos_req_id = ""
        self._current_pos_req_ts = 0
        self._temp_position_ids = {}

        # Market data
        self._bid = {}  # symbol_name -> bid price
        self._ask = {}  # symbol_name -> ask price

        # Sessions
        sender = config.get("sender_comp_id", "")
        target = config.get("target_comp_id", "CSERVER")
        username = config.get("username", "")
        password = config.get("password", "")
        hb = config.get("heartbeat_interval", 30)
        host = config.get("host", "")
        use_ssl = config.get("use_ssl", True)

        self.trade_session = FixSession(
            host=host, port=config.get("trade_port", 5201),
            sender_comp_id=sender, target_comp_id=target,
            sender_sub_id="TRADE", target_sub_id="TRADE",
            username=username, password=password,
            heartbeat_interval=hb, use_ssl=use_ssl
        )
        self.quote_session = FixSession(
            host=host, port=config.get("quote_port", 5202),
            sender_comp_id=sender, target_comp_id=target,
            sender_sub_id="QUOTE", target_sub_id="QUOTE",
            username=username, password=password,
            heartbeat_interval=hb, use_ssl=use_ssl
        )

        # Register callbacks
        self.trade_session.register_callback(b'8', self._on_execution_report)
        self.trade_session.register_callback(b'y', self._on_security_list)
        self.trade_session.register_callback(b'AP', self._on_position_report)
        self.trade_session.register_callback(b'BA', self._on_collateral_report)
        self.trade_session.register_callback(b'j', self._on_business_reject)
        self.trade_session.register_callback(b'3', self._on_session_reject)
        self.quote_session.register_callback(b'W', self._on_market_data_snapshot)
        self.quote_session.register_callback(b'X', self._on_market_data_incremental)
        self.quote_session.register_callback(b'Y', self._on_market_data_reject)

        # Account info from collateral reports
        self._balance = None
        self._equity = None
        self._margin_used = None
        self._free_margin = None
        self._leverage = config.get("leverage", None)  # Manual override from config
        self._total_pnl = 0.0
        self._total_swap = 0.0
        self._collateral_supported = True  # Assume supported until proven otherwise
        self._last_collateral_request = 0

        # Open API client for balance/equity/leverage (if configured)
        self._openapi_client = CTraderOpenApiClient(account_id, config, self.dd)

        self._running = False
        self._heartbeat_thread = None

        # Auto-reconnect state
        self._reconnect_delay = 5
        self._last_reconnect_check = 0

    @property
    def connected(self):
        return self.trade_session.connected

    @property
    def quote_connected(self):
        return self.quote_session.connected

    def get_quote_direct(self, symbol):
        """Get current bid/ask for a symbol (case-insensitive, slash-lenient)."""
        sym_clean = symbol.upper()
        sym_no_slash = sym_clean.replace("/", "")
        for s in (sym_clean, sym_no_slash):
            if s in self._bid and s in self._ask:
                return {"bid": self._bid[s], "ask": self._ask[s]}
        for k in list(self._bid.keys()):
            if k.upper().replace("/", "") == sym_no_slash:
                return {"bid": self._bid[k], "ask": self._ask[k]}
        return None

    def get_positions_for_import(self, pair_filter="", comment_filter=""):
        """Get open positions in import-compatible format."""
        positions = []
        try:
            for pid, p in self._position_ids.items():
                symbol = p["symbol"].upper()
                sym_clean = symbol.replace("/", "")
                pf_clean = pair_filter.upper().replace("/", "")
                if pf_clean and not (sym_clean.startswith(pf_clean) or pf_clean.startswith(sym_clean)):
                    continue
                comment = ""
                
                qty = p.get("qty") or 0.0
                lots = round(qty / self.lot_multiplier, 2)
                
                try:
                    ticket_int = int(pid)
                except ValueError:
                    ticket_int = pid
                
                oe = p.get("open_epoch")
                ot = time.strftime('%Y.%m.%d %H:%M:%S', time.gmtime(oe)) if oe else ""
                
                positions.append({
                    "ticket": ticket_int,
                    "symbol": symbol,
                    "lots": lots,
                    "side": p["side"],
                    "comment": comment,
                    "open_price": p.get("settle_price") or 0.0,
                    "open_time": ot,
                    "open_epoch": oe,
                })
            logger.info("[%s] Import: found %d positions (pair=%s comment=%s)",
                        self.account_id, len(positions), pair_filter, comment_filter)
        except Exception as e:
            logger.error("[%s] get_positions_for_import error: %s", self.account_id, e)
        return positions

    def start(self):
        """Start both sessions and begin feeding data."""
        self._running = True
        try:
            self.trade_session.start()
            # Wait for logon
            for _ in range(20):
                if self.trade_session.connected:
                    break
                time.sleep(0.5)
            if self.trade_session.connected:
                logger.info("[%s] TRADE session logged in", self.account_id)
                # Fetch security list
                time.sleep(0.5)
                self._request_security_list()
                # Try to fetch balance/equity via CollateralInquiry
                time.sleep(0.5)
                self._request_collateral_info()
                # Request initial positions
                time.sleep(0.5)
                self.request_positions()
            else:
                logger.error("[%s] TRADE session failed to logon", self.account_id)
        except Exception as e:
            logger.error("[%s] TRADE session error: %s", self.account_id, e)

        try:
            self.quote_session.start()
            for _ in range(20):
                if self.quote_session.connected:
                    break
                time.sleep(0.5)
            if self.quote_session.connected:
                logger.info("[%s] QUOTE session logged in", self.account_id)
                # Subscribe to market data after a brief delay
                time.sleep(1)
                self._subscribe_market_data()
            else:
                logger.error("[%s] QUOTE session failed to logon", self.account_id)
        except Exception as e:
            logger.error("[%s] QUOTE session error: %s", self.account_id, e)

        # Start heartbeat/data feed thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True,
            name=f"FIX-HB-{self.account_id}"
        )
        self._heartbeat_thread.start()

        # Start Open API client if configured
        if self._openapi_client.is_available:
            try:
                self._openapi_client.start()
            except Exception as e:
                logger.error("[%s] Open API start error: %s", self.account_id, e)

    def stop(self):
        """Stop both sessions and Open API client."""
        self._running = False
        self.trade_session.stop()
        self.quote_session.stop()
        if self._openapi_client:
            self._openapi_client.stop()
        logger.info("[%s] Stopped", self.account_id)

    # ─── Heartbeat / Data Feed ──────────────────────────────────────────────

    def _heartbeat_loop(self):
        """Periodically update dashboard data structures with FIX data.
        Also detects dead sessions and triggers reconnect."""
        while self._running:
            try:
                now = time.time()
                # Feed heartbeat into ea_heartbeats
                self.dd["ea_heartbeats"][self.account_id] = now

                # Feed price data into ea_account_info
                info = self.dd["ea_account_info"].get(self.account_id, {})
                # Find the first symbol with data for bid/ask/spread
                for sym_name, prices in list(self._bid.items()):
                    bid = self._bid.get(sym_name)
                    ask = self._ask.get(sym_name)
                    if bid and ask:
                        info["bid"] = bid
                        info["ask"] = ask
                        info["spread"] = round((ask - bid) * 10 ** self._get_digits(sym_name), 1)
                        info["symbol"] = sym_name
                        break

                # 1. Update supplement data from openapi client if available & connected
                if self._openapi_client and self._openapi_client.is_available:
                    if self._openapi_client.balance is not None:
                        self._balance = self._openapi_client.balance
                    if self._openapi_client.leverage is not None:
                        self._leverage = self._openapi_client.leverage
                    
                    md = self._openapi_client._money_digits
                    
                    # Swaps from openapi
                    if self._openapi_client._position_swaps:
                        self._total_swap = round(sum(self._openapi_client._position_swaps.values()) / (10 ** md), 2)
                    else:
                        self._total_swap = 0.0
                    
                    # Margin from openapi
                    if self._openapi_client._position_margins:
                        self._margin_used = round(sum(self._openapi_client._position_margins.values()) / (10 ** md), 2)
                    else:
                        self._margin_used = 0.0
                    
                    # PNL from openapi
                    self._total_pnl = self._openapi_client._total_unrealized_pnl
                else:
                    # Fallback: calculate PNL locally from FIX positions and current prices
                    total_pnl = 0.0
                    for p in self._position_ids.values():
                        total_pnl += self._calculate_position_pnl(p)
                    self._total_pnl = round(total_pnl, 2)
                    self._total_swap = 0.0

                # Calculate equity
                if self._balance is not None:
                    self._equity = round(self._balance + (self._total_pnl or 0.0), 2)

                # Feed balance/equity/leverage
                if self._balance is not None:
                    info["balance"] = self._balance
                if self._equity is not None:
                    info["equity"] = self._equity
                if self._margin_used is not None:
                    info["margin_used"] = self._margin_used
                    info["margin"] = self._margin_used
                if self._free_margin is not None:
                    info["free_margin"] = self._free_margin
                if self._leverage is not None:
                    info["leverage"] = self._leverage
                if getattr(self, "_total_swap", None) is not None:
                    info["total_swap"] = self._total_swap
                if getattr(self, "_total_pnl", None) is not None:
                    info["total_pnl"] = self._total_pnl

                # Check if position request timed out (fallback for brokers without TotalNumPos, or 0 positions)
                req_ts = getattr(self, '_current_pos_req_ts', 0)
                if req_ts > 0 and (now - req_ts) > 3.0:
                    self._position_ids = getattr(self, '_temp_position_ids', {}).copy()
                    self._current_pos_req_ts = 0
                    logger.info("[%s] Position snapshot timeout sync: %d positions",
                                self.account_id, len(self._position_ids))

                # Supplement position open_epoch from openapi if matching positionId is found
                if self._openapi_client and self._openapi_client.is_available and self._openapi_client._positions:
                    for pid, p in self._position_ids.items():
                        try:
                            pid_key = int(pid)
                        except ValueError:
                            pid_key = pid
                        oa_pos = self._openapi_client._positions.get(pid_key)
                        if oa_pos and oa_pos.get("open_ts"):
                            p["open_epoch"] = oa_pos["open_ts"] / 1000.0

                info["positions"] = len(self._position_ids)
                # Signed lots: buy = positive, sell = negative
                if self._position_ids:
                    _bl = sum(p.get("qty", 0) for p in self._position_ids.values() if p.get("side") == "buy")
                    _sl = sum(p.get("qty", 0) for p in self._position_ids.values() if p.get("side") == "sell")
                    info["total_lots"] = round((_bl - _sl) / self.lot_multiplier, 2)
                    
                    # Build open_tickets for hedge matching
                    tickets = []
                    for k in self._position_ids.keys():
                        try:
                            tickets.append(int(k))
                        except ValueError:
                            pass
                    info["open_tickets"] = tickets

                    # Per-instrument lots breakdown
                    _lbi = {}
                    pos_details = []
                    for k, p in self._position_ids.items():
                        sym = p.get("symbol", "Unknown")
                        lots = round(p.get("qty", 0) / self.lot_multiplier, 2)
                        if sym not in _lbi:
                            _lbi[sym] = {"buy": 0, "sell": 0}
                        if p.get("side") == "buy":
                            _lbi[sym]["buy"] = round(_lbi[sym]["buy"] + lots, 2)
                        else:
                            _lbi[sym]["sell"] = round(_lbi[sym]["sell"] + lots, 2)
                        
                        ticket_int = 0
                        try:
                            ticket_int = int(k)
                        except ValueError:
                            pass
                        pos_details.append({
                            "ticket": ticket_int,
                            "symbol": sym,
                            "comment": "",
                            "open_epoch": p.get("open_epoch") or now,
                        })
                    info["lots_by_instrument"] = _lbi
                    info["position_details"] = pos_details
                else:
                    info["total_lots"] = 0
                    info["lots_by_instrument"] = {}
                    info["open_tickets"] = []
                    info["position_details"] = []

                # Calculate oldest position age
                oldest_epoch = None
                for p in self._position_ids.values():
                    oep = p.get("open_epoch")
                    if oep and (oldest_epoch is None or oep < oldest_epoch):
                        oldest_epoch = oep
                if oldest_epoch:
                    open_dt = datetime.fromtimestamp(oldest_epoch, tz=NY_TZ) - timedelta(hours=17)
                    now_dt = datetime.fromtimestamp(now, tz=NY_TZ) - timedelta(hours=17)
                    info["oldest_position_age"] = max(0, (now_dt.date() - open_dt.date()).days)
                else:
                    info["oldest_position_age"] = 0

                info["last_update"] = now
                info["fix_account"] = True
                info["netting_mode"] = False  # cTrader: per-position tickets, not netting
                info["trade_connected"] = self.trade_session.connected
                info["quote_connected"] = self.quote_session.connected
                self.dd["ea_account_info"][self.account_id] = info

                # Periodically re-request collateral info (every 30s)
                if (self._collateral_supported and
                    self.trade_session.connected and
                    now - self._last_collateral_request > 30):
                    self._request_collateral_info()

                # Periodically re-request positions (every 60s)
                if (self.trade_session.connected and
                    now - getattr(self, '_last_pos_request', 0) > 60):
                    self.request_positions()
                    self._last_pos_request = now

                # ── Auto-reconnect: detect dead sessions ──
                if now - self._last_reconnect_check > 10:
                    self._last_reconnect_check = now
                    self._check_fix_sessions()

            except Exception as e:
                logger.error("[%s] Heartbeat error: %s", self.account_id, e)
            time.sleep(1)

    def _check_fix_sessions(self):
        """Detect dead FIX sessions and trigger reconnect via FixSession.run_loop.
        FixSession.run_loop already has its own reconnect loop; if the run_loop thread
        has died (shouldn't happen, but safety net), restart it."""
        # Check TRADE session
        if not self.trade_session.connected and self.trade_session._running:
            # run_loop handles its own reconnect; just log
            pass
        elif not self.trade_session._running and self._running:
            # Thread died unexpectedly — restart it
            logger.warning("[%s] TRADE session thread died — restarting", self.account_id)
            try:
                self.trade_session.start()
                for _ in range(20):
                    if self.trade_session.connected:
                        break
                    time.sleep(0.5)
                if self.trade_session.connected:
                    logger.info("[%s] TRADE session re-established", self.account_id)
                    time.sleep(0.5)
                    self._request_security_list()
                    time.sleep(0.5)
                    self._request_collateral_info()
                    time.sleep(0.5)
                    self.request_positions()
            except Exception as e:
                logger.error("[%s] TRADE session restart error: %s", self.account_id, e)

        # Check QUOTE session
        if not self.quote_session.connected and self.quote_session._running:
            pass  # run_loop handles reconnect
        elif not self.quote_session._running and self._running:
            logger.warning("[%s] QUOTE session thread died — restarting", self.account_id)
            try:
                self.quote_session.start()
                for _ in range(20):
                    if self.quote_session.connected:
                        break
                    time.sleep(0.5)
                if self.quote_session.connected:
                    logger.info("[%s] QUOTE session re-established — re-subscribing", self.account_id)
                    time.sleep(1)
                    self._subscribe_market_data()
            except Exception as e:
                logger.error("[%s] QUOTE session restart error: %s", self.account_id, e)

    def _get_digits(self, symbol_name):
        """Get decimal digits for a symbol."""
        sym_id = self._symbols_by_name.get(symbol_name)
        if sym_id and sym_id in self._symbol_digits:
            return self._symbol_digits[sym_id]
        return 5  # default

    # ─── Collateral / Account Info ───────────────────────────────────────────

    def _request_collateral_info(self):
        """Send CollateralInquiry (BB) to request balance/equity info."""
        if not self._collateral_supported:
            return
        try:
            req_id = str(uuid.uuid4())[:12]
            fields = [
                (TAG_COLLREQID, req_id),
            ]
            self.trade_session.send_message("BB", fields)
            self._last_collateral_request = time.time()
            logger.debug("[%s] Collateral inquiry sent (id=%s)", self.account_id, req_id)
        except Exception as e:
            logger.error("[%s] Failed to send collateral inquiry: %s", self.account_id, e)

    def _on_collateral_report(self, msg):
        """Handle CollateralReport (MsgType=BA) with balance/equity info."""
        try:
            # Parse the report for relevant financial fields
            for raw_tag, value in msg.pairs:
                tag = int(raw_tag) if isinstance(raw_tag, bytes) else raw_tag
                val_str = value.decode() if isinstance(value, bytes) else str(value)
                if tag == TAG_ENDCASH or tag == TAG_STARTCASH:
                    # EndCash (922) = current balance; StartCash (921) = starting balance
                    self._balance = float(val_str)
                elif tag == TAG_TOTALNETVALUE:
                    # TotalNetValue (900) = equity / net asset value
                    self._equity = float(val_str)
                elif tag == TAG_MARGINEXCESS:
                    # MarginExcess (899) = free margin
                    self._free_margin = float(val_str)
                elif tag == TAG_CASHOUTSTANDING:
                    # CashOutstanding (901) = margin used
                    self._margin_used = float(val_str)
                elif tag == TAG_ACCOUNT:
                    pass  # Account identifier, skip

            logger.info("[%s] Collateral report: balance=%.2f equity=%.2f free_margin=%.2f",
                        self.account_id,
                        self._balance or 0, self._equity or 0, self._free_margin or 0)
        except Exception as e:
            logger.error("[%s] Error parsing collateral report: %s", self.account_id, e)


    def _register_symbol(self, name, sym_id):
        """Register a symbol with both slash and no-slash lookups."""
        name = name.strip()
        self._symbols_by_name[name] = sym_id
        self._symbols_by_id[sym_id] = name
        # Also register without slashes (EUR/USD -> EURUSD) for dashboard compat
        no_slash = name.replace("/", "")
        if no_slash != name:
            self._symbols_by_name[no_slash] = sym_id

    def _load_symbol_file(self, filepath):
        """Load symbol mappings from a semicolon-delimited file (NAME;ID per line)."""
        try:
            paths_to_try = [
                filepath,
                os.path.join(os.getcwd(), filepath),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), filepath)
            ]
            tcd = os.environ.get("TRADE_CONFIG_DIR")
            if tcd:
                paths_to_try.append(os.path.join(tcd, filepath))
                
            actual_path = None
            for p in paths_to_try:
                if os.path.exists(p):
                    actual_path = p
                    break
            
            if not actual_path:
                raise FileNotFoundError(f"Could not locate symbol file {filepath}")

            with open(actual_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or ";" not in line:
                        continue
                    parts = line.split(";", 1)
                    name = parts[0].strip()
                    try:
                        sym_id = int(parts[1].strip())
                    except ValueError:
                        continue
                    self._register_symbol(name, sym_id)
            logger.info("[%s] Loaded %d symbols from %s (actual path: %s)",
                        self.account_id, len(self._symbols_by_id), filepath, actual_path)
        except Exception as e:
            logger.error("[%s] Failed to load symbol file %s: %s",
                         self.account_id, filepath, e)

    # ─── Security List ──────────────────────────────────────────────────────

    def _request_security_list(self):
        """Request full security list to build symbol ID ↔ name mapping."""
        req_id = str(uuid.uuid4())[:12]
        fields = [
            (TAG_SECURITYREQID, req_id),
            (TAG_SECURITYLISTTYPE, "0"),
        ]
        self.trade_session.send_message("x", fields)
        logger.info("[%s] Security List requested", self.account_id)

    def _on_security_list(self, msg):
        """Handle Security List response (MsgType=y)."""
        # Parse all symbol entries: 55=id, 1007=name, 1008=digits
        pairs = msg.pairs
        i = 0
        current_id = None
        while i < len(pairs):
            tag, value = pairs[i]
            if tag == TAG_SYMBOL:
                current_id = int(value)
            elif tag == TAG_SYMBOLNAME and current_id is not None:
                name = value.decode() if isinstance(value, bytes) else str(value)
                self._register_symbol(name, current_id)
            elif tag == TAG_SYMBOLDIGITS and current_id is not None:
                self._symbol_digits[current_id] = int(value)
            i += 1
        logger.info("[%s] Security List: %d symbols loaded", self.account_id, len(self._symbols_by_id))

    # ─── Market Data ────────────────────────────────────────────────────────

    def _subscribe_market_data(self):
        """Subscribe to bid/ask for configured symbols."""
        needed_pairs = set()
        
        # Extract pairs from live sessions where this account is a side
        try:
            sessions = self.dd.get("sessions", {})
            for sess in sessions.values():
                if isinstance(sess, dict):
                    sides = sess.get("sides", {})
                    if self.account_id in sides:
                        pair = sess.get("pair")
                        if pair:
                            clean_pair = pair.upper().replace("/", "").replace("_", "").replace("-", "").replace(".", "")
                            needed_pairs.add(clean_pair)
        except Exception as e:
            logger.error("[%s] Failed to extract needed pairs from sessions: %s", self.account_id, e)

        logger.info("[%s] Determining symbols to subscribe from needed pairs: %s", self.account_id, sorted(list(needed_pairs)))

        subscribed_ids = set()
        subscribed_names = []
        for sym_name, sym_id in self._symbols_by_name.items():
            if sym_id in subscribed_ids:
                continue
            
            # Clean symbol name for matching (e.g., "EUR/USD" -> "EURUSD")
            clean_sym = sym_name.upper().replace("/", "").replace("_", "").replace("-", "").replace(".", "")
            
            # Match if clean symbol name is in needed_pairs or matches with suffix (e.g. EURUSDm)
            matched = False
            if clean_sym in needed_pairs:
                matched = True
            else:
                for np in needed_pairs:
                    if clean_sym.startswith(np) or np.startswith(clean_sym):
                        matched = True
                        break
            
            if matched:
                subscribed_ids.add(sym_id)
                subscribed_names.append(sym_name)
                req_id = f"{sym_id}_{uuid.uuid4().hex[:8]}"
                fields = [
                    (TAG_MDREQID, req_id),
                    (TAG_SUBSCRIPTIONREQUESTTYPE, "1"),  # Subscribe
                    (TAG_MARKETDEPTH, "1"),               # Top of book
                    (TAG_MDUPDATETYPE, "1"),               # Incremental
                    (TAG_NORELATEDSYM, "1"),
                    (TAG_SYMBOL, str(sym_id)),
                    (TAG_NOMDENTRTYPES, "2"),
                    (TAG_MDENTRYTYPE, "0"),               # Bid
                    (TAG_MDENTRYTYPE, "1"),               # Ask
                ]
                self.quote_session.send_message("V", fields)
                
        logger.info("[%s] Subscribed to %d symbols: %s", self.account_id, len(subscribed_ids), ", ".join(subscribed_names))

    def _on_market_data_snapshot(self, msg):
        """Handle Market Data Snapshot/Full Refresh (MsgType=W)."""
        pairs_str = ", ".join(f"{tag}={val.decode(errors='ignore') if isinstance(val, bytes) else val}" for tag, val in msg.pairs)
        logger.debug("[%s] Market Data Snapshot (W) fields: %s", self.account_id, pairs_str)
        sym_id_raw = msg.get(TAG_SYMBOL)
        if not sym_id_raw:
            return
        sym_id = int(sym_id_raw)
        sym_name = self._symbols_by_id.get(sym_id, str(sym_id))

        # Parse MD entries
        for raw_tag, value in msg.pairs:
            tag = int(raw_tag) if isinstance(raw_tag, bytes) else raw_tag
            if tag == TAG_MDENTRYTYPE:
                entry_type = value  # b'0'=bid, b'1'=ask
            elif tag == TAG_MDENTRYPX:
                price = float(value)
                if entry_type == b'0':
                    self._bid[sym_name] = price
                elif entry_type == b'1':
                    self._ask[sym_name] = price

    def _on_market_data_incremental(self, msg):
        """Handle Market Data Incremental Refresh (MsgType=X)."""
        pairs_str = ", ".join(f"{tag.decode() if isinstance(tag, bytes) else tag}={val.decode(errors='ignore') if isinstance(val, bytes) else val}" for tag, val in msg.pairs)
        logger.debug("[%s] Market Data Incremental (X) fields: %s", self.account_id, pairs_str)
        entry_type = None
        sym_id = None
        for raw_tag, value in msg.pairs:
            tag = int(raw_tag) if isinstance(raw_tag, bytes) else raw_tag
            if tag == TAG_MDENTRYTYPE:
                entry_type = value
            elif tag == TAG_SYMBOL:
                sym_id = int(value)
            elif tag == TAG_MDENTRYPX:
                if sym_id is not None and entry_type is not None:
                    sym_name = self._symbols_by_id.get(sym_id, str(sym_id))
                    price = float(value)
                    if entry_type == b'0':
                        self._bid[sym_name] = price
                    elif entry_type == b'1':
                        self._ask[sym_name] = price

    def _on_market_data_reject(self, msg):
        """Handle Market Data Request Reject (MsgType=Y)."""
        text = msg.get(TAG_TEXT)
        req_id = msg.get(TAG_MDREQID)
        logger.error("[%s] Market data reject: %s (req=%s)", self.account_id,
                     text.decode() if text else "unknown",
                     req_id.decode() if req_id else "?")

    def get_symbol_info(self, symbol):
        """Get bid/ask/spread for a specific symbol from internal price cache."""
        sym_clean = symbol.upper().replace("/", "")
        bid = ask = None
        for k in self._bid.keys():
            if k.upper().replace("/", "") == sym_clean:
                bid = self._bid.get(k)
                ask = self._ask.get(k)
                break
        if bid and ask:
            pip_mult = 1000 if "JPY" in sym_clean else 100000
            spread = round((ask - bid) * pip_mult, 1)
            return {"bid": bid, "ask": ask, "spread": spread}
        return None

    def subscribe_symbol(self, symbol_name):
        """Subscribe to market data for a symbol if not already subscribed."""
        if not self.quote_session.connected:
            return
        sym_clean = symbol_name.upper().replace("/", "")
        sym_id = None
        for name, i in self._symbols_by_name.items():
            if name.upper().replace("/", "") == sym_clean:
                sym_id = i
                break
        if not sym_id:
            logger.warning("[%s] Cannot subscribe: unknown symbol %s", self.account_id, symbol_name)
            return
        # Check if we already have bid/ask for any representation of this symbol
        for k in self._bid.keys():
            if k.upper().replace("/", "") == sym_clean:
                return  # already subscribed
        req_id = f"{sym_id}_{uuid.uuid4().hex[:8]}"
        fields = [
            (TAG_MDREQID, req_id),
            (TAG_SUBSCRIPTIONREQUESTTYPE, "1"),  # Subscribe
            (TAG_MARKETDEPTH, "1"),               # Top of book
            (TAG_MDUPDATETYPE, "1"),               # Incremental
            (TAG_NORELATEDSYM, "1"),
            (TAG_SYMBOL, str(sym_id)),
            (TAG_NOMDENTRTYPES, "2"),
            (TAG_MDENTRYTYPE, "0"),               # Bid
            (TAG_MDENTRYTYPE, "1"),               # Ask
        ]
        self.quote_session.send_message("V", fields)
        logger.info("[%s] Dynamically subscribed to %s (ID=%d)", self.account_id, symbol_name, sym_id)

    def _get_conversion_to_usd(self, quote_currency):
        quote_currency = quote_currency.upper()
        if quote_currency == "USD":
            return 1.0
        
        # Try XYZUSD
        for k in self._bid.keys():
            k_upper = k.upper().replace("/", "")
            if k_upper == f"{quote_currency}USD":
                return self._bid[k]
        
        # Try USDXYZ
        for k in self._ask.keys():
            k_upper = k.upper().replace("/", "")
            if k_upper == f"USD{quote_currency}":
                ask_val = self._ask[k]
                return 1.0 / ask_val if ask_val > 0 else 1.0
                
        return 1.0  # fallback

    def _calculate_position_pnl(self, p):
        symbol = p.get("symbol")
        qty = p.get("qty", 0)
        side = p.get("side")
        entry_price = p.get("settle_price")
        if not symbol or not entry_price:
            return 0.0
            
        bid = self._bid.get(symbol)
        ask = self._ask.get(symbol)
        if not bid or not ask:
            # Case-insensitive lookup
            sym_upper = symbol.upper()
            for k in self._bid.keys():
                if k.upper() == sym_upper:
                    bid = self._bid.get(k)
                    ask = self._ask.get(k)
                    break
        
        if not bid or not ask:
            return 0.0
            
        # PnL in quote currency
        if side == "buy":
            pnl_quote = (bid - entry_price) * qty
        else:
            pnl_quote = (entry_price - ask) * qty
            
        # Parse quote currency
        parts = symbol.split("/")
        if len(parts) == 2:
            quote = parts[1]
        else:
            clean_sym = symbol.split(".")[0]
            if len(clean_sym) >= 6:
                quote = clean_sym[-3:]
            else:
                quote = "USD"
                
        rate = self._get_conversion_to_usd(quote)
        return pnl_quote * rate

    # ─── Order Management ───────────────────────────────────────────────────

    def send_market_order(self, symbol_name, side, lot_size, session_id=None,
                          position_id=None, comment="", is_rollback=False):
        """
        Send a market order.
        Args:
            symbol_name: e.g. "EURUSD"
            side: "buy" or "sell"
            lot_size: In lots (e.g. 0.01)
            session_id: Dashboard session ID for fill tracking
            position_id: PosMaintRptID for closing existing positions
            comment: Order comment/designation
            is_rollback: Whether this is a rollback close
        Returns: ClOrdID string
        """
        sym_id = self._symbols_by_name.get(symbol_name.upper())
        if sym_id is None:
            logger.error("[%s] Unknown symbol: %s", self.account_id, symbol_name)
            return None

        clordid = str(uuid.uuid4())[:12]
        qty = int(lot_size * self.lot_multiplier)
        fix_side = "1" if side.lower() == "buy" else "2"
        now = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S")

        fields = [
            (TAG_CLORDID, clordid),
            (TAG_SYMBOL, str(sym_id)),
            (TAG_SIDE, fix_side),
            (TAG_TRANSACTTIME, now),
            (TAG_ORDTYPE, "1"),     # Market
            (TAG_ORDERQTY, str(qty)),
        ]
        if position_id:
            fields.append((TAG_POSMAINTRPTID, str(position_id)))
        # cTrader FIX does not support Tag 5765 (Designation) for comments
        # if comment:
        #     fields.append((TAG_DESIGNATION, comment))

        # Snapshot quote at the moment the order is dispatched
        sym_clean = symbol_name.upper().replace("/", "")
        quote_at_order = None
        for k in self._bid.keys():
            if k.upper().replace("/", "") == sym_clean:
                quote_at_order = (self._bid[k], self._ask[k])
                break

        # Track this order
        self._pending_orders[clordid] = {
            "session_id": session_id,
            "symbol": symbol_name,
            "side": side,
            "lot_size": lot_size,
            "qty": qty,
            "position_id": position_id,
            "is_close": position_id is not None,
            "is_rollback": is_rollback,
            "ts": time.time(),
            "quote_at_order": quote_at_order,
        }

        self.trade_session.send_message("D", fields)
        logger.info("[%s] Market order sent: %s %s %.2f lots (clordid=%s, pos=%s)",
                    self.account_id, side.upper(), symbol_name, lot_size,
                    clordid, position_id or "new")
        return clordid

    def close_position(self, position_id, symbol_name, side, lot_size,
                       session_id=None, comment="", is_rollback=False):
        """
        Close an existing position by sending opposite-side order with PosMaintRptID.
        """
        # Opposite side
        close_side = "sell" if side.lower() == "buy" else "buy"
        return self.send_market_order(
            symbol_name, close_side, lot_size,
            session_id=session_id, position_id=position_id, comment=comment,
            is_rollback=is_rollback
        )

    # ─── Position Management ────────────────────────────────────────────────

    def request_positions(self):
        """Request all open positions."""
        req_id = str(uuid.uuid4())[:12]
        self._current_pos_req_id = req_id
        self._current_pos_req_ts = time.time()
        self._temp_position_ids = {}
        fields = [(TAG_POSREQID, req_id)]
        self.trade_session.send_message("AN", fields)
        logger.info("[%s] Position report requested (req_id=%s)", self.account_id, req_id)

    def _on_position_report(self, msg):
        """Handle Position Report (MsgType=AP)."""
        pos_id = msg.get(TAG_POSMAINTRPTID)
        sym_id_raw = msg.get(TAG_SYMBOL)
        long_qty = msg.get(TAG_LONGQTY)
        short_qty = msg.get(TAG_SHORTQTY)
        settle_price = msg.get(TAG_SETTLPRICE)
        pos_req_id_raw = msg.get(TAG_POSREQID)

        pos_id_str = pos_id.decode() if pos_id else ""
        pos_req_id = pos_req_id_raw.decode() if pos_req_id_raw else ""

        if pos_id_str and sym_id_raw:
            sym_id = int(sym_id_raw)
            sym_name = self._symbols_by_id.get(sym_id, str(sym_id))
            lq = int(float(long_qty)) if long_qty else 0
            sq = int(float(short_qty)) if short_qty else 0

            # If this is response to our active request, collect in temp
            if pos_req_id and pos_req_id == getattr(self, '_current_pos_req_id', ''):
                if lq > 0 or sq > 0:
                    existing = self._position_ids.get(pos_id_str, {})
                    open_epoch = existing.get("open_epoch") or time.time()
                    self._temp_position_ids[pos_id_str] = {
                        "symbol": sym_name,
                        "symbol_id": sym_id,
                        "long_qty": lq,
                        "short_qty": sq,
                        "side": "buy" if lq > 0 else "sell",
                        "qty": lq or sq,
                        "settle_price": float(settle_price) if settle_price else None,
                        "open_epoch": open_epoch,
                    }
                    self.subscribe_symbol(sym_name)
                
                # Check TotalNumPos if present
                total_num_pos_raw = msg.get(TAG_TOTALNUMPOS)
                total_num_pos = int(total_num_pos_raw) if total_num_pos_raw else None
                if total_num_pos == 0:
                    self._position_ids = {}
                    self._current_pos_req_ts = 0
                    logger.info("[%s] Position snapshot: 0 positions", self.account_id)
                elif total_num_pos is not None and len(self._temp_position_ids) >= total_num_pos:
                    self._position_ids = self._temp_position_ids.copy()
                    self._current_pos_req_ts = 0
                    logger.info("[%s] Position snapshot updated: %d positions",
                                self.account_id, len(self._position_ids))
            else:
                # Unsolicited report or other request, update directly
                if lq > 0 or sq > 0:
                    existing = self._position_ids.get(pos_id_str, {})
                    open_epoch = existing.get("open_epoch") or time.time()
                    self._position_ids[pos_id_str] = {
                        "symbol": sym_name,
                        "symbol_id": sym_id,
                        "long_qty": lq,
                        "short_qty": sq,
                        "side": "buy" if lq > 0 else "sell",
                        "qty": lq or sq,
                        "settle_price": float(settle_price) if settle_price else None,
                        "open_epoch": open_epoch,
                    }
                    self.subscribe_symbol(sym_name)
                else:
                    self._position_ids.pop(pos_id_str, None)
                logger.debug("[%s] Position updated directly: %s %s L=%d S=%d",
                             self.account_id, pos_id_str, sym_name, lq, sq)

    # ─── Execution Reports ──────────────────────────────────────────────────

    def _on_execution_report(self, msg):
        """Handle Execution Report (MsgType=8)."""
        exec_type = msg.get(TAG_EXECTYPE)
        ord_status = msg.get(TAG_ORDSTATUS)
        clordid = msg.get(TAG_CLORDID)
        order_id = msg.get(TAG_ORDERID)
        pos_id = msg.get(TAG_POSMAINTRPTID)
        avg_px = msg.get(TAG_AVGPX)
        cum_qty = msg.get(TAG_CUMQTY)
        last_qty = msg.get(TAG_LASTQTY)
        sym_id_raw = msg.get(TAG_SYMBOL)
        side_raw = msg.get(TAG_SIDE)
        text = msg.get(TAG_TEXT)

        clordid_str = clordid.decode() if clordid else ""
        order_info = self._pending_orders.get(clordid_str, {})

        if exec_type == EXECTYPE_NEW:
            # Order accepted — waiting for fill
            logger.info("[%s] Order accepted: clordid=%s orderid=%s",
                        self.account_id, clordid_str,
                        order_id.decode() if order_id else "?")

        elif exec_type == EXECTYPE_FILL:
            # Order filled!
            fill_price = float(avg_px) if avg_px else 0
            fill_qty = int(cum_qty) if cum_qty else 0
            pos_id_str = pos_id.decode() if pos_id else ""

            sym_id = int(sym_id_raw) if sym_id_raw else 0
            sym_name = self._symbols_by_id.get(sym_id, order_info.get("symbol", ""))

            logger.info("[%s] FILL: %s %s @ %.5f qty=%d pos=%s",
                        self.account_id, order_info.get("side", "?").upper(),
                        sym_name, fill_price, fill_qty, pos_id_str)

            # Track the position ID for future closes
            if pos_id_str:
                if order_info.get("is_close"):
                    self._position_ids.pop(pos_id_str, None)
                    logger.info("[%s] Position removed on close execution: %s", self.account_id, pos_id_str)
                else:
                    side = "buy" if side_raw == SIDE_BUY else "sell"
                    self._position_ids[pos_id_str] = {
                        "symbol": sym_name,
                        "symbol_id": sym_id,
                        "side": side,
                        "qty": fill_qty,
                        "settle_price": fill_price,
                        "open_epoch": time.time(),
                    }
                    logger.info("[%s] Position added on fill execution: %s %s %.2f lots",
                                self.account_id, pos_id_str, sym_name, fill_qty / self.lot_multiplier)

            # Feed into dashboard via HTTP POST to /api/trade_result
            session_id = order_info.get("session_id")
            if session_id:
                is_close = order_info.get("is_close", False)
                is_rollback = order_info.get("is_rollback", False)
                status = "rollback_closed" if is_rollback else ("closed" if is_close else "filled")
                if is_close and order_info.get("position_id"):
                    try:
                        ticket = int(order_info["position_id"])
                    except ValueError:
                        ticket = order_info["position_id"]
                else:
                    ticket = int(order_id.decode()) if order_id else int(time.time() * 1000)
                
                order_side = order_info.get("side", "buy").lower()
                quote_at_order = order_info.get("quote_at_order")
                quote_price = 0.0
                if quote_at_order:
                    quote_price = quote_at_order[1] if order_side == "buy" else quote_at_order[0]
                self._post_trade_result({
                    "session_id": session_id,
                    "account": self.account_id,
                    "status": status,
                    "ticket": ticket,
                    "fill_price": fill_price,
                    "quote_price": quote_price,
                    "spread": None,
                    "detail": f"FIX fill: {sym_name} @ {fill_price}",
                })

            # Clean up pending order
            self._pending_orders.pop(clordid_str, None)

        elif exec_type == EXECTYPE_REJECTED:
            reason = text.decode() if text else "unknown"
            logger.error("[%s] Order REJECTED: %s (clordid=%s)",
                         self.account_id, reason, clordid_str)

            # Feed rejection into dashboard as error
            session_id = order_info.get("session_id")
            if session_id:
                self._post_trade_result({
                    "session_id": session_id,
                    "account": self.account_id,
                    "status": "error",
                    "detail": f"FIX reject: {reason}",
                })
            self._pending_orders.pop(clordid_str, None)

        elif exec_type == EXECTYPE_CANCELED:
            logger.info("[%s] Order canceled: clordid=%s", self.account_id, clordid_str)
            self._pending_orders.pop(clordid_str, None)

        elif exec_type == EXECTYPE_STATUS:
            # Status report response — informational only
            pass

    def _on_business_reject(self, msg):
        """Handle Business Message Reject (MsgType=j)."""
        text = msg.get(TAG_TEXT)
        ref_msg = msg.get(TAG_REFMSGTYPE)
        ref_id = msg.get(TAG_BUSINESSREJECTREFID)
        text_str = text.decode() if text else "unknown"
        ref_msg_str = ref_msg.decode() if ref_msg else "?"
        logger.error("[%s] Business reject: %s (ref_msg=%s ref=%s)", self.account_id,
                     text_str, ref_msg_str,
                     ref_id.decode() if ref_id else "?")
        # If collateral inquiry was rejected, disable future polling
        if ref_msg_str == "BB":
            logger.warning("[%s] CollateralInquiry not supported — disabling balance polling",
                           self.account_id)
            self._collateral_supported = False

    def _on_session_reject(self, msg):
        """Handle Session Reject (MsgType=3) — detect unsupported CollateralInquiry."""
        text = msg.get(TAG_TEXT)
        ref_msg = msg.get(TAG_REFMSGTYPE)
        text_str = text.decode() if text else ""
        ref_msg_str = ref_msg.decode() if ref_msg else ""
        # Check if the rejected message was our CollateralInquiry (BB)
        if ref_msg_str == "BB" or "BB" in text_str or "Invalid MsgType" in text_str:
            if self._collateral_supported:
                logger.warning("[%s] CollateralInquiry rejected at session level — disabling",
                               self.account_id)
                self._collateral_supported = False

    def _post_trade_result(self, data):
        """POST trade result to the dashboard's /api/trade_result endpoint with retry/backoff."""
        url = self.dd.get("dashboard_url", "http://127.0.0.1:5000")
        url = f"{url}/api/trade_result"
        payload = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=payload,
                                    headers={"Content-Type": "application/json"})
        
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    resp.read()
                return  # Success!
            except Exception as e:
                if attempt < max_attempts - 1:
                    sleep_time = 0.1 * (2 ** attempt)
                    logger.warning("[%s] Failed to post trade result to %s (attempt %d/%d): %s. Retrying in %.1fs...",
                                   self.account_id, url, attempt + 1, max_attempts, e, sleep_time)
                    time.sleep(sleep_time)
                else:
                    logger.error("[%s] Failed to post trade result to %s after %d attempts: %s",
                                 self.account_id, url, max_attempts, e)


# ─── SwissquoteFixAccount ───────────────────────────────────────────────────
class SwissquoteFixAccount:
    """
    Manages one Swissquote CFXD account: TRADE + QUOTE sessions on same port.
    Separating sessions prevents quote floods from blocking order execution.
    Feeds price/heartbeat/balance data into the dashboard's data structures.
    """
    def __init__(self, account_id, config, dashboard_data):
        self.account_id = account_id
        self.config = config
        self.dd = dashboard_data
        self.label = config.get("label", account_id)
        self.lot_multiplier = config.get("lot_multiplier", 100000)

        # Swissquote uses text-based symbols (EUR/USD) — no numeric IDs needed.
        # We store a mapping from dashboard format (EURUSD) to SQ format (EUR/USD)
        self._sym_to_sq = {}   # EURUSD -> EUR/USD
        self._sym_from_sq = {} # EUR/USD -> EURUSD
        self._subscribed_symbols = set()

        # Build initial symbol mapping from config
        for sym in config.get("symbols", []):
            self._register_sq_symbol(sym)

        # Position tracking
        self._pending_orders = {}    # clordid -> order info
        self._positions = {}         # position_id -> {symbol, side, qty, price}

        # Market data
        self._bid = {}    # symbol_name -> bid price
        self._ask = {}    # symbol_name -> ask price

        # Account data from ExecutionReport / RequestForPositionsAck
        self._balance = None
        self._equity = None
        self._margin_used = None
        self._leverage = config.get("leverage", None)
        self._last_pos_request = 0

        # Dual FIX sessions on same port — TRADE for orders, QUOTE for market data
        sender = config.get("sender_comp_id", "")
        target = config.get("target_comp_id", "")
        username = config.get("username", "")
        password = config.get("password", "")
        hb = config.get("heartbeat_interval", 30)
        host = config.get("host", "")
        port = config.get("trade_port", 443)

        self.trade_session = FixSession(
            host=host, port=port,
            sender_comp_id=sender, target_comp_id=target,
            sender_sub_id="TRADE", target_sub_id="TRADE",
            username=username, password=password,
            heartbeat_interval=hb, use_ssl=True,
            extra_logon_fields=[(TAG_SQ_SENDMISSED, "1")]
        )
        self.quote_session = FixSession(
            host=host, port=port,
            sender_comp_id=sender, target_comp_id=target,
            sender_sub_id="QUOTE", target_sub_id="QUOTE",
            username=username, password=password,
            heartbeat_interval=hb, use_ssl=True
        )

        # TRADE session: orders, execution reports, positions, balance
        self.trade_session.register_callback(b'8', self._on_execution_report)
        self.trade_session.register_callback(b'AP', self._on_position_report)
        self.trade_session.register_callback(b'AO', self._on_position_report_ack)
        self.trade_session.register_callback(b'3', self._on_session_reject)
        self.trade_session.register_callback(b'j', self._on_business_reject)
        self.trade_session.register_callback(b'9', self._on_order_cancel_reject)
        # QUOTE session: market data
        self.quote_session.register_callback(b'W', self._on_market_data_snapshot)
        self.quote_session.register_callback(b'Y', self._on_market_data_reject)

        self._running = False
        self._heartbeat_thread = None

        # Auto-reconnect state
        self._last_reconnect_check = 0

    # ─── Symbol Mapping ─────────────────────────────────────────────────────

    def _register_sq_symbol(self, sq_name):
        """Register a Swissquote symbol (EUR/USD) and map to dashboard format (EURUSD)."""
        sq_name = sq_name.strip()
        dashboard_name = sq_name.replace("/", "")
        self._sym_to_sq[dashboard_name] = sq_name
        self._sym_to_sq[sq_name] = sq_name  # direct lookup too
        self._sym_from_sq[sq_name] = dashboard_name

    def _to_sq_symbol(self, name):
        """Convert dashboard symbol (EURUSD) to SQ format (EUR/USD)."""
        sq = self._sym_to_sq.get(name)
        if sq:
            return sq
        # Auto-detect: if 6-char currency pair, try inserting slash
        name_upper = name.upper()
        if len(name_upper) == 6 and name_upper.isalpha():
            sq = f"{name_upper[:3]}/{name_upper[3:]}"
            self._register_sq_symbol(sq)
            return sq
        # Index-style symbols (#DE40)
        if name.startswith("#"):
            return name
        return name

    def _from_sq_symbol(self, sq_name):
        """Convert SQ symbol (EUR/USD) to dashboard format (EURUSD)."""
        dash = self._sym_from_sq.get(sq_name)
        if dash:
            return dash
        dash = sq_name.replace("/", "")
        self._sym_from_sq[sq_name] = dash
        self._sym_to_sq[dash] = sq_name
        return dash

    # ─── Connection ─────────────────────────────────────────────────────────

    @property
    def connected(self):
        return self.trade_session.connected

    @property
    def quote_connected(self):
        return self.quote_session.connected

    def start(self):
        """Start TRADE and QUOTE sessions and begin feeding data."""
        self._running = True
        # Start TRADE session
        try:
            self.trade_session.start()
            for _ in range(20):
                if self.trade_session.connected:
                    break
                time.sleep(0.5)
            if self.trade_session.connected:
                logger.info("[%s] SQ TRADE session logged in", self.account_id)
                time.sleep(1)
                self._request_positions()
            else:
                logger.error("[%s] SQ TRADE session failed to logon", self.account_id)
        except Exception as e:
            logger.error("[%s] SQ TRADE session error: %s", self.account_id, e)

        # Start QUOTE session
        try:
            self.quote_session.start()
            for _ in range(20):
                if self.quote_session.connected:
                    break
                time.sleep(0.5)
            if self.quote_session.connected:
                logger.info("[%s] SQ QUOTE session logged in", self.account_id)
            else:
                logger.error("[%s] SQ QUOTE session failed to logon", self.account_id)
        except Exception as e:
            logger.error("[%s] SQ QUOTE session error: %s", self.account_id, e)

        # Start heartbeat/data feed thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True,
            name=f"SQ-HB-{self.account_id}"
        )
        self._heartbeat_thread.start()

    def stop(self):
        """Stop both sessions."""
        self._running = False
        self.trade_session.stop()
        self.quote_session.stop()
        logger.info("[%s] Swissquote stopped", self.account_id)

    # ─── Market Data ────────────────────────────────────────────────────────

    def subscribe_symbol(self, symbol_name):
        """Subscribe to market data for a symbol."""
        if not self.connected:
            return
        sq_sym = self._to_sq_symbol(symbol_name)
        if sq_sym in self._subscribed_symbols:
            return
        req_id = f"MD_{sq_sym}_{uuid.uuid4().hex[:6]}"
        fields = [
            (TAG_MDREQID, req_id),
            (TAG_SUBSCRIPTIONREQUESTTYPE, "1"),   # Snapshot + Updates
            (TAG_MARKETDEPTH, "1"),                # Top of book
            (TAG_MDUPDATETYPE, "0"),               # Full refresh
            (TAG_NOMDENTRTYPES, "2"),
            (TAG_MDENTRYTYPE, "0"),                # Bid
            (TAG_MDENTRYTYPE, "1"),                # Ask
            (TAG_NORELATEDSYM, "1"),
            (TAG_SYMBOL, sq_sym),
        ]
        self.quote_session.send_message("V", fields)
        self._subscribed_symbols.add(sq_sym)
        logger.info("[%s] Subscribed to %s", self.account_id, sq_sym)

    def _on_market_data_snapshot(self, msg):
        """Handle MarketDataSnapshotFullRefresh (MsgType=W)."""
        sq_sym = msg.get(TAG_SYMBOL)
        if not sq_sym:
            return
        sq_sym_str = sq_sym.decode() if isinstance(sq_sym, bytes) else str(sq_sym)
        dash_sym = self._from_sq_symbol(sq_sym_str)

        entry_type = None
        for raw_tag, value in msg.pairs:
            tag = int(raw_tag) if isinstance(raw_tag, bytes) else raw_tag
            if tag == TAG_MDENTRYTYPE:
                entry_type = value
            elif tag == TAG_MDENTRYPX:
                price = float(value)
                if entry_type == b'0':  # Bid
                    self._bid[dash_sym] = price
                elif entry_type == b'1':  # Ask
                    self._ask[dash_sym] = price

    def _on_market_data_reject(self, msg):
        """Handle MarketDataRequestReject (MsgType=Y)."""
        text = msg.get(TAG_TEXT)
        req_id = msg.get(TAG_MDREQID)
        logger.error("[%s] MD reject: %s (req=%s)", self.account_id,
                     text.decode() if text else "unknown",
                     req_id.decode() if req_id else "?")

    def get_symbol_info(self, symbol):
        """Get bid/ask/spread for a specific symbol from internal price cache."""
        sym_clean = symbol.upper().replace("/", "")
        bid = ask = None
        for k in self._bid.keys():
            if k.upper().replace("/", "") == sym_clean:
                bid = self._bid.get(k)
                ask = self._ask.get(k)
                break
        if bid and ask:
            pip_mult = 1000 if "JPY" in sym_clean else 100000
            spread = round((ask - bid) * pip_mult, 1)
            return {"bid": bid, "ask": ask, "spread": spread}
        return None

    # ─── Order Execution ────────────────────────────────────────────────────

    def send_market_order(self, symbol_name, side, lot_size, session_id=None,
                          position_id=None, comment="", is_rollback=False):
        """
        Send a market order.
        Args:
            symbol_name: e.g. "EURUSD" (will be converted to EUR/USD)
            side: "buy" or "sell"
            lot_size: In lots (e.g. 0.01)
            session_id: Dashboard session ID for fill tracking
            position_id: SQ PositionID for closing/reducing (ClOrdLinkID)
            comment: Order comment (tag 58 Text)
            is_rollback: Whether this is a rollback close
        Returns: ClOrdID string
        """
        sq_sym = self._to_sq_symbol(symbol_name)
        clordid = str(uuid.uuid4())[:12]
        qty = int(lot_size * self.lot_multiplier)
        fix_side = "1" if side.lower() == "buy" else "2"
        now = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S")

        fields = [
            (TAG_CLORDID, clordid),
            (TAG_SYMBOL, sq_sym),
            (TAG_SIDE, fix_side),
            (TAG_TRANSACTTIME, now),
            (TAG_ORDTYPE, "1"),       # Market
            (TAG_ORDERQTY, str(qty)),
            (TAG_TIMEINFORCE, "4"),   # Fill or Kill
        ]
        if position_id:
            fields.append((TAG_CLORDLINKID, str(position_id)))
        if comment:
            fields.append((TAG_TEXT, comment))

        # Snapshot quote at the moment the order is dispatched
        sym_clean = symbol_name.upper().replace("/", "")
        quote_at_order = None
        for k in self._bid.keys():
            if k.upper().replace("/", "") == sym_clean:
                quote_at_order = (self._bid[k], self._ask[k])
                break

        # Track this order
        self._pending_orders[clordid] = {
            "session_id": session_id,
            "symbol": symbol_name,
            "side": side,
            "lot_size": lot_size,
            "qty": qty,
            "position_id": position_id,
            "is_close": position_id is not None,
            "is_rollback": is_rollback,
            "ts": time.time(),
            "quote_at_order": quote_at_order,
        }

        self.trade_session.send_message("D", fields)
        logger.info("[%s] SQ Market order: %s %s %.2f lots (clordid=%s, pos=%s)",
                    self.account_id, side.upper(), sq_sym, lot_size,
                    clordid, position_id or "new")
        return clordid

    def close_position(self, position_id, symbol_name, side, lot_size,
                       session_id=None, comment="", is_rollback=False):
        """
        Close a position. Sends opposite-side order with ClOrdLinkID = PositionID.
        """
        close_side = "sell" if side.lower() == "buy" else "buy"
        return self.send_market_order(
            symbol_name, close_side, lot_size,
            session_id=session_id, position_id=position_id, comment=comment,
            is_rollback=is_rollback
        )

    # ─── Execution Reports ──────────────────────────────────────────────────

    def _on_execution_report(self, msg):
        """Handle ExecutionReport (MsgType=8) — SQ specific."""
        exec_type = msg.get(TAG_EXECTYPE)
        ord_status = msg.get(TAG_ORDSTATUS)
        clordid = msg.get(TAG_CLORDID)
        order_id = msg.get(TAG_ORDERID)
        avg_px = msg.get(TAG_AVGPX)
        cum_qty = msg.get(TAG_CUMQTY)
        sym_raw = msg.get(TAG_SYMBOL)
        side_raw = msg.get(TAG_SIDE)
        text = msg.get(TAG_TEXT)

        clordid_str = clordid.decode() if clordid else ""
        order_info = self._pending_orders.get(clordid_str, {})

        # Extract balance/equity from SQ custom tags (present in all ExecReports)
        self._parse_sq_account_tags(msg)

        # Parse SQ linked positions
        linked_pos_raw = None
        for raw_tag, value in msg.pairs:
            tag = int(raw_tag) if isinstance(raw_tag, bytes) else raw_tag
            if tag == TAG_SQ_LINKEDPOSITIONS:
                linked_pos_raw = value.decode() if isinstance(value, bytes) else str(value)

        if exec_type == SQ_EXECTYPE_NEW:
            # Order acknowledged
            logger.info("[%s] SQ order accepted: clordid=%s",
                        self.account_id, clordid_str)

        elif exec_type == SQ_EXECTYPE_FILLED or exec_type == SQ_EXECTYPE_CALCULATED:
            # Order filled
            fill_price = float(avg_px) if avg_px else 0
            fill_qty = int(float(cum_qty)) if cum_qty else 0
            order_id_str = order_id.decode() if order_id else ""

            sq_sym = sym_raw.decode() if sym_raw else order_info.get("symbol", "")
            sym_name = self._from_sq_symbol(sq_sym) if "/" in sq_sym else sq_sym

            is_close = order_info.get("is_close", False)

            logger.info("[%s] SQ FILL: %s %s @ %.5f qty=%d close=%s linked=%s",
                        self.account_id, order_info.get("side", "?").upper(),
                        sym_name, fill_price, fill_qty, is_close, linked_pos_raw)

            # Track position from linked positions
            if linked_pos_raw and not is_close:
                for pos_id in linked_pos_raw.split(","):
                    pos_id = pos_id.strip()
                    if pos_id:
                        side = "buy" if side_raw == SIDE_BUY else "sell"
                        self._positions[pos_id] = {
                            "symbol": sym_name,
                            "side": side,
                            "qty": fill_qty,
                            "price": fill_price,
                        }

            # Feed into dashboard
            session_id = order_info.get("session_id")
            if session_id:
                is_rollback = order_info.get("is_rollback", False)
                status = "rollback_closed" if is_rollback else ("closed" if is_close else "filled")
                if is_close and order_info.get("position_id"):
                    try:
                        ticket = int(order_info["position_id"])
                    except ValueError:
                        ticket = order_info["position_id"]
                else:
                    ticket = int(order_id_str) if order_id_str.isdigit() else int(time.time() * 1000)
                
                order_side = order_info.get("side", "buy").lower()
                quote_at_order = order_info.get("quote_at_order")
                quote_price = 0.0
                if quote_at_order:
                    quote_price = quote_at_order[1] if order_side == "buy" else quote_at_order[0]
                self._post_trade_result({
                    "session_id": session_id,
                    "account": self.account_id,
                    "status": status,
                    "ticket": ticket,
                    "fill_price": fill_price,
                    "quote_price": quote_price,
                    "spread": None,
                    "detail": f"SQ fill: {sym_name} @ {fill_price}",
                })

            self._pending_orders.pop(clordid_str, None)

        elif exec_type == SQ_EXECTYPE_REJECTED:
            reason = text.decode() if text else "unknown"
            logger.error("[%s] SQ order REJECTED: %s (clordid=%s)",
                         self.account_id, reason, clordid_str)
            session_id = order_info.get("session_id")
            if session_id:
                self._post_trade_result({
                    "session_id": session_id,
                    "account": self.account_id,
                    "status": "error",
                    "detail": f"SQ reject: {reason}",
                })
            self._pending_orders.pop(clordid_str, None)

        elif exec_type == SQ_EXECTYPE_CANCELED:
            logger.info("[%s] SQ order canceled: clordid=%s", self.account_id, clordid_str)
            self._pending_orders.pop(clordid_str, None)

    def _parse_sq_account_tags(self, msg):
        """Extract balance/equity/margin from SQ custom tags in any message."""
        for raw_tag, value in msg.pairs:
            tag = int(raw_tag) if isinstance(raw_tag, bytes) else raw_tag
            try:
                val = float(value)
                if tag == TAG_SQ_BALANCE:
                    self._balance = val
                elif tag == TAG_SQ_EQUITY:
                    self._equity = val
                elif tag == TAG_SQ_USEDMARGIN:
                    self._margin_used = val
            except (ValueError, TypeError):
                pass

    # ─── Position Management ────────────────────────────────────────────────

    def _request_positions(self):
        """Send RequestForPositions (35=AN) — also returns balance/equity."""
        req_id = str(uuid.uuid4())[:12]
        now = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S")
        today = datetime.utcnow().strftime("%Y%m%d")
        fields = [
            (TAG_POSREQID, req_id),
            (TAG_POSREQTYPE, "0"),
            (453, "0"),                    # NoPartyIDs (empty group)
            (TAG_ACCOUNT, "0"),
            (TAG_ACCOUNTTYPE, "1"),
            (TAG_TRANSACTTIME, now),
            (TAG_CLEARINGBIZDATE, today),
            (TAG_SUBSCRIPTIONREQUESTTYPE, "1"),  # Snapshot + Updates
        ]
        self.trade_session.send_message("AN", fields)
        self._last_pos_request = time.time()
        logger.info("[%s] SQ position request sent", self.account_id)

    def _on_position_report_ack(self, msg):
        """Handle RequestForPositionsAck (MsgType=AO) — contains balance/equity."""
        self._parse_sq_account_tags(msg)
        total_reports = msg.get(TAG_TOTALNUMPOS)
        result = msg.get(TAG_POSREQRESULT)
        logger.info("[%s] SQ PosReqAck: result=%s reports=%s balance=%.2f equity=%.2f",
                    self.account_id,
                    result.decode() if result else "?",
                    total_reports.decode() if total_reports else "?",
                    self._balance or 0, self._equity or 0)

    def _on_position_report(self, msg):
        """Handle PositionReport (MsgType=AP) — each one is an open position."""
        pos_id = msg.get(TAG_POSMAINTRPTID)
        sq_sym = msg.get(TAG_SYMBOL)
        long_qty = msg.get(TAG_LONGQTY)
        short_qty = msg.get(TAG_SHORTQTY)
        settle_price = msg.get(TAG_SETTLPRICE)

        if pos_id:
            pos_id_str = pos_id.decode() if isinstance(pos_id, bytes) else str(pos_id)
            sq_sym_str = sq_sym.decode() if sq_sym else "?"
            sym_name = self._from_sq_symbol(sq_sym_str)

            lq = int(float(long_qty)) if long_qty else 0
            sq_val = int(float(short_qty)) if short_qty else 0

            if lq > 0 or sq_val > 0:
                self._positions[pos_id_str] = {
                    "symbol": sym_name,
                    "side": "buy" if lq > 0 else "sell",
                    "qty": lq or sq_val,
                    "price": float(settle_price) if settle_price else 0,
                }
                # Auto-subscribe to this symbol's market data
                self.subscribe_symbol(sym_name)
            else:
                # Position closed
                self._positions.pop(pos_id_str, None)

            logger.debug("[%s] SQ Position: %s %s L=%d S=%d",
                         self.account_id, pos_id_str, sym_name, lq, sq_val)

    # ─── Rejects ────────────────────────────────────────────────────────────

    def _on_session_reject(self, msg):
        text = msg.get(TAG_TEXT)
        logger.error("[%s] SQ session reject: %s", self.account_id,
                     text.decode() if text else "unknown")

    def _on_business_reject(self, msg):
        text = msg.get(TAG_TEXT)
        ref_msg = msg.get(TAG_REFMSGTYPE)
        logger.error("[%s] SQ business reject: %s (ref=%s)", self.account_id,
                     text.decode() if text else "?",
                     ref_msg.decode() if ref_msg else "?")

    def _on_order_cancel_reject(self, msg):
        text = msg.get(TAG_TEXT)
        logger.error("[%s] SQ cancel reject: %s", self.account_id,
                     text.decode() if text else "?")

    # ─── Heartbeat / Data Feed ──────────────────────────────────────────────

    def _heartbeat_loop(self):
        """Periodically update dashboard data and handle auto-reconnect."""
        while self._running:
            try:
                now = time.time()
                self.dd["ea_heartbeats"][self.account_id] = now

                info = self.dd["ea_account_info"].get(self.account_id, {})

                # Feed price data
                for sym_name in list(self._bid.keys()):
                    bid = self._bid.get(sym_name)
                    ask = self._ask.get(sym_name)
                    if bid and ask:
                        info["bid"] = bid
                        info["ask"] = ask
                        pip_mult = 1000 if "JPY" in sym_name.upper() else 100000
                        info["spread"] = round((ask - bid) * pip_mult, 1)
                        info["symbol"] = sym_name
                        break

                # Feed balance/equity/leverage
                if self._balance is not None:
                    info["balance"] = self._balance
                if self._equity is not None:
                    info["equity"] = self._equity
                if self._margin_used is not None:
                    info["margin_used"] = self._margin_used
                if self._leverage is not None:
                    info["leverage"] = self._leverage

                info["last_update"] = now
                info["fix_account"] = True
                info["trade_connected"] = self.trade_session.connected
                info["quote_connected"] = self.quote_session.connected
                info["positions"] = len(self._positions)
                # Signed lots: buy = positive, sell = negative
                if self._positions:
                    _bl = sum(p.get("qty", 0) for p in self._positions.values() if p.get("side") == "buy")
                    _sl = sum(p.get("qty", 0) for p in self._positions.values() if p.get("side") == "sell")
                    info["total_lots"] = round((_bl - _sl) / 100000.0, 2)
                    # Per-instrument lots breakdown
                    _lbi = {}
                    for p in self._positions.values():
                        sym = p.get("symbol", "Unknown")
                        lots = round(p.get("qty", 0) / 100000.0, 2)
                        if sym not in _lbi:
                            _lbi[sym] = {"buy": 0, "sell": 0}
                        if p.get("side") == "buy":
                            _lbi[sym]["buy"] = round(_lbi[sym]["buy"] + lots, 2)
                        else:
                            _lbi[sym]["sell"] = round(_lbi[sym]["sell"] + lots, 2)
                    info["lots_by_instrument"] = _lbi
                    
                    # Per-instrument swap breakdown
                    _sbi = {}
                    if self._openapi_client and self._openapi_client.is_available:
                        md = self._openapi_client._money_digits
                        if self._openapi_client._position_swaps:
                            for pid, p in self._openapi_client._positions.items():
                                sym = p.get("symbol", "Unknown")
                                raw_swap = self._openapi_client._position_swaps.get(pid, 0)
                                swap_val = round(raw_swap / (10 ** md), 2)
                                _sbi[sym] = round(_sbi.get(sym, 0.0) + swap_val, 2)
                    info["swap_by_instrument"] = _sbi
                else:
                    info["total_lots"] = 0
                    info["lots_by_instrument"] = {}
                    info["swap_by_instrument"] = {}
                self.dd["ea_account_info"][self.account_id] = info

                # Periodically re-request positions (every 60s) for balance updates
                if (self.trade_session.connected and
                    now - self._last_pos_request > 60):
                    self._request_positions()

                # ── Auto-reconnect: detect dead sessions ──
                if now - self._last_reconnect_check > 10:
                    self._last_reconnect_check = now
                    self._check_fix_sessions()

            except Exception as e:
                logger.error("[%s] SQ heartbeat error: %s", self.account_id, e)
            time.sleep(1)

    def _check_fix_sessions(self):
        """Detect dead FIX sessions and restart if thread died."""
        # Check TRADE session
        if not self.trade_session._running and self._running:
            logger.warning("[%s] SQ TRADE session thread died — restarting", self.account_id)
            try:
                self.trade_session.start()
                for _ in range(20):
                    if self.trade_session.connected:
                        break
                    time.sleep(0.5)
                if self.trade_session.connected:
                    logger.info("[%s] SQ TRADE session re-established", self.account_id)
                    time.sleep(1)
                    self._request_positions()
            except Exception as e:
                logger.error("[%s] SQ TRADE session restart error: %s", self.account_id, e)

        # Check QUOTE session
        if not self.quote_session._running and self._running:
            logger.warning("[%s] SQ QUOTE session thread died — restarting", self.account_id)
            try:
                self.quote_session.start()
                for _ in range(20):
                    if self.quote_session.connected:
                        break
                    time.sleep(0.5)
                if self.quote_session.connected:
                    logger.info("[%s] SQ QUOTE session re-established — re-subscribing", self.account_id)
                    time.sleep(1)
                    for sym in self._subscribed_symbols:
                        self.subscribe_symbol(sym)
            except Exception as e:
                logger.error("[%s] SQ QUOTE session restart error: %s", self.account_id, e)

    def _post_trade_result(self, data):
        """POST trade result to the dashboard's /api/trade_result endpoint with retry/backoff."""
        url = self.dd.get("dashboard_url", "http://127.0.0.1:5000")
        url = f"{url}/api/trade_result"
        payload = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=payload,
                                    headers={"Content-Type": "application/json"})
        
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    resp.read()
                return  # Success!
            except Exception as e:
                if attempt < max_attempts - 1:
                    sleep_time = 0.1 * (2 ** attempt)
                    logger.warning("[%s] Failed to post trade result to %s (attempt %d/%d): %s. Retrying in %.1fs...",
                                   self.account_id, url, attempt + 1, max_attempts, e, sleep_time)
                    time.sleep(sleep_time)
                else:
                    logger.error("[%s] Failed to post trade result to %s after %d attempts: %s",
                                 self.account_id, url, max_attempts, e)

    def request_positions(self):
        """Public method to request positions."""
        self._request_positions()


# ─── Dukascopy custom tags ──────────────────────────────────────────────────
TAG_DK_LEVERAGE       = 7005    # Leverage (in AccountInfoResponse U2)
TAG_DK_CURRENTMARGIN  = 7006    # CurrentMargin (in AccountInfoResponse U2)
TAG_DK_CURRENTEQUITY  = 7007    # CurrentEquity (in AccountInfoResponse U2)
TAG_DK_AMOUNT         = 7008    # Swap amount (in OvernightReport U4)

# Dukascopy ExecType values (standard FIX 4.4)
DK_EXECTYPE_NEW        = b'0'   # New ack
DK_EXECTYPE_FILL       = b'F'   # Filled
DK_EXECTYPE_CANCELED   = b'4'   # Canceled
DK_EXECTYPE_REJECTED   = b'8'   # Rejected
DK_EXECTYPE_PENDINGNEW = b'A'   # Pending New
DK_EXECTYPE_CALCULATED = b'B'   # Calculated (quote orders)


class DukascopyFixAccount:
    """
    Manages one Dukascopy FIX API account: TRADE + QUOTE sessions.
    
    Dukascopy FIX API 8.0.1 specifics:
    - Two FIX connections: data feed (QUOTE) and trading (TRADE)
    - Custom MsgType 'U2' for AccountInfoResponse (leverage, margin, equity)
    - Custom MsgType 'U4' for OvernightReport (swap data)
    - Symbols use slash format: EUR/USD
    - Standard FIX 4.4 order flow (NewOrderSingle D, ExecutionReport 8)
    - Position reports via RequestForPositions (AN) / PositionReport (AP)
    """

    def __init__(self, account_id, config, dashboard_data):
        self.account_id = account_id
        self.config = config
        self.dd = dashboard_data
        self.label = config.get("label", account_id)
        self.lot_multiplier = config.get("lot_multiplier", 1000000)  # Dukascopy uses base units

        # Symbol mapping: EUR/USD ↔ EURUSD (same as Swissquote)
        self._sym_to_dk = {}     # EURUSD -> EUR/USD
        self._sym_from_dk = {}   # EUR/USD -> EURUSD
        self._subscribed_symbols = set()

        # Build initial symbol mapping from config
        for sym in config.get("symbols", []):
            self._register_dk_symbol(sym)

        # Position tracking
        self._pending_orders = {}    # clordid -> order info
        self._positions = {}         # position_id -> {symbol, side, qty, price}

        # Market data
        self._bid = {}    # symbol_name -> bid price
        self._ask = {}    # symbol_name -> ask price

        # Account data from AccountInfoResponse (U2) and position reports
        self._balance = None
        self._equity = None
        self._free_margin = None
        self._margin_used = None
        self._leverage = config.get("leverage", None)
        self._total_pnl = 0.0
        self._total_swap = 0.0
        self._last_pos_request = 0
        self._last_acct_info_request = 0
        sender_trade = config.get("sender_comp_id", "")
        sender_quote = config.get("sender_comp_id_quote", sender_trade)
        target = config.get("target_comp_id", "")
        username = config.get("username", "")
        self.username = username
        # External account ID for Dukascopy API — use config or default to username
        self._external_account = config.get("external_account_id") or username
        # Determine if single connection (retail/default)
        self.single_connection = config.get("single_connection", True)
        password = config.get("password", "")
        hb = config.get("heartbeat_interval", 30)
        trade_host = config.get("host", "")
        trade_port = config.get("trade_port", 443)
        quote_host = config.get("quote_host", trade_host)
        quote_port = config.get("quote_port", trade_port)
        use_ssl = config.get("use_ssl", True)

        self.trade_session = FixSession(
            host=trade_host, port=trade_port,
            sender_comp_id=sender_trade, target_comp_id=target,
            sender_sub_id="", target_sub_id="",
            username=username, password=password,
            heartbeat_interval=hb, use_ssl=use_ssl
        )
        self.quote_session = FixSession(
            host=quote_host, port=quote_port,
            sender_comp_id=sender_quote, target_comp_id=target,
            sender_sub_id="", target_sub_id="",
            username=username, password=password,
            heartbeat_interval=hb, use_ssl=use_ssl
        )

        # TRADE session callbacks
        self.trade_session.register_callback(b'8', self._on_execution_report)
        self.trade_session.register_callback(b'U2', self._on_account_info)
        self.trade_session.register_callback(b'U3', self._on_instrument_position_info)
        self.trade_session.register_callback(b'U4', self._on_overnight_report)
        self.trade_session.register_callback(b'U1', self._on_notification)
        self.trade_session.register_callback(b'h', self._on_trading_session_status)
        self.trade_session.register_callback(b'3', self._on_session_reject)
        self.trade_session.register_callback(b'j', self._on_business_reject)
        self.trade_session.register_callback(b'9', self._on_order_cancel_reject)
        # QUOTE session callbacks
        self.quote_session.register_callback(b'W', self._on_market_data_snapshot)
        self.quote_session.register_callback(b'Y', self._on_market_data_reject)

        self._running = False
        self._heartbeat_thread = None

        # Auto-reconnect state
        self._last_reconnect_check = 0

    # ─── Symbol Mapping ─────────────────────────────────────────────────────

    def _register_dk_symbol(self, dk_name):
        """Register a Dukascopy symbol (EUR/USD) and map to dashboard format (EURUSD)."""
        dk_name = dk_name.strip()
        dashboard_name = dk_name.replace("/", "")
        self._sym_to_dk[dashboard_name] = dk_name
        self._sym_to_dk[dk_name] = dk_name
        self._sym_from_dk[dk_name] = dashboard_name

    def _to_dk_symbol(self, name):
        """Convert dashboard symbol (EURUSD) to Dukascopy format (EUR/USD)."""
        dk = self._sym_to_dk.get(name)
        if dk:
            return dk
        name_upper = name.upper()
        if len(name_upper) == 6 and name_upper.isalpha():
            dk = f"{name_upper[:3]}/{name_upper[3:]}"
            self._register_dk_symbol(dk)
            return dk
        return name

    def _from_dk_symbol(self, dk_name):
        """Convert Dukascopy symbol (EUR/USD) to dashboard format (EURUSD)."""
        dash = self._sym_from_dk.get(dk_name)
        if dash:
            return dash
        dash = dk_name.replace("/", "")
        self._sym_from_dk[dk_name] = dash
        self._sym_to_dk[dash] = dk_name
        return dash

    # ─── Connection ─────────────────────────────────────────────────────────

    @property
    def connected(self):
        return self.trade_session.connected

    @property
    def quote_connected(self):
        return self.quote_session.connected

    def get_quote_direct(self, symbol):
        """Get current bid/ask for a symbol (case-insensitive, slash-lenient)."""
        sym_clean = symbol.upper()
        sym_no_slash = sym_clean.replace("/", "")
        for s in (sym_clean, sym_no_slash):
            if s in self._bid and s in self._ask:
                return {"bid": self._bid[s], "ask": self._ask[s]}
        for k in list(self._bid.keys()):
            if k.upper().replace("/", "") == sym_no_slash:
                return {"bid": self._bid[k], "ask": self._ask[k]}
        return None

    def get_positions_for_import(self, pair_filter="", comment_filter=""):
        """Get open positions in import-compatible format."""
        import hashlib
        positions = []
        try:
            for pid, p in self._positions.items():
                symbol = p["symbol"].upper()
                sym_clean = symbol.replace("/", "")
                pf_clean = pair_filter.upper().replace("/", "")
                if pf_clean and not (sym_clean.startswith(pf_clean) or pf_clean.startswith(sym_clean)):
                    continue
                # Generate deterministic 32-bit integer ticket from symbol name
                ticket_int = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16)
                
                qty = p.get("qty") or 0.0
                lots = round(qty / self.lot_multiplier, 2)
                
                oe = p.get("open_epoch")
                ot = time.strftime('%Y.%m.%d %H:%M:%S', time.gmtime(oe)) if oe else ""
                
                positions.append({
                    "ticket": ticket_int,
                    "symbol": symbol,
                    "lots": lots,
                    "side": p["side"],
                    "comment": "",
                    "open_price": p.get("price") or 0.0,
                    "open_time": ot,
                    "open_epoch": oe,
                })
            logger.info("[%s] Import: found %d positions (pair=%s comment=%s)",
                        self.account_id, len(positions), pair_filter, comment_filter)
        except Exception as e:
            logger.error("[%s] get_positions_for_import error: %s", self.account_id, e)
        return positions

    def start(self):
        """Start TRADE and QUOTE sessions and begin feeding data."""
        self._running = True
        # Start TRADE session
        try:
            self.trade_session.start()
            for _ in range(20):
                if self.trade_session.connected:
                    break
                time.sleep(0.5)
            if self.trade_session.connected:
                logger.info("[%s] DK TRADE session logged in", self.account_id)
                time.sleep(1)
                self._subscribe_trading_session()
                if self._external_account and self._external_account != self.username:
                    self._request_account_info()
            else:
                logger.error("[%s] DK TRADE session failed to logon", self.account_id)
        except Exception as e:
            logger.error("[%s] DK TRADE session error: %s", self.account_id, e)

        # Start QUOTE session
        try:
            self.quote_session.start()
            for _ in range(20):
                if self.quote_session.connected:
                    break
                time.sleep(0.5)
            if self.quote_session.connected:
                logger.info("[%s] DK QUOTE session logged in", self.account_id)
                # Auto-subscribe to symbols from dashboard sessions
                time.sleep(1)
                self._auto_subscribe_from_sessions()
            else:
                logger.error("[%s] DK QUOTE session failed to logon", self.account_id)
        except Exception as e:
            logger.error("[%s] DK QUOTE session error: %s", self.account_id, e)

        # Start heartbeat/data feed thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True,
            name=f"DK-HB-{self.account_id}"
        )
        self._heartbeat_thread.start()

    def stop(self):
        """Stop both sessions."""
        self._running = False
        self.trade_session.stop()
        self.quote_session.stop()
        logger.info("[%s] Dukascopy stopped", self.account_id)

    # ─── Market Data ────────────────────────────────────────────────────────

    def _auto_subscribe_from_sessions(self):
        """Scan dashboard sessions and subscribe to symbols for this account."""
        try:
            sessions = self.dd.get("sessions", {})
            for sess_name, sess in sessions.items():
                sides = sess.get("sides", {})
                if self.account_id in sides:
                    pair = (sides[self.account_id].get("pair") or sess.get("pair", "")).upper()
                    if pair:
                        self.subscribe_symbol(pair)
        except Exception as e:
            logger.error("[%s] DK auto-subscribe error: %s", self.account_id, e)

    def subscribe_symbol(self, symbol_name):
        """Subscribe to market data for a symbol."""
        if not self.quote_session.connected:
            return
        dk_sym = self._to_dk_symbol(symbol_name)
        if dk_sym in self._subscribed_symbols:
            return
        # Use simple incrementing ID (matching working Dukascopy implementation)
        req_id = str(len(self._subscribed_symbols) + 1)
        # Tag order matches working Dukascopy log exactly
        fields = [
            (TAG_NORELATEDSYM, "1"),
            (TAG_SYMBOL, dk_sym),
            (TAG_MDREQID, req_id),
            (TAG_SUBSCRIPTIONREQUESTTYPE, "1"),   # Snapshot + Updates
            (TAG_MARKETDEPTH, "1"),                # Top of book
            (TAG_MDUPDATETYPE, "0"),               # Full refresh
            (TAG_NOMDENTRTYPES, "2"),
            (TAG_MDENTRYTYPE, "0"),                # Bid
            (TAG_MDENTRYTYPE, "1"),                # Ask
        ]
        self.quote_session.send_message("V", fields)
        self._subscribed_symbols.add(dk_sym)
        logger.info("[%s] DK subscribed to %s", self.account_id, dk_sym)

    def _on_market_data_snapshot(self, msg):
        """Handle MarketDataSnapshotFullRefresh (MsgType=W)."""
        try:
            dk_sym = msg.get(TAG_SYMBOL)
            if not dk_sym:
                return
            dk_sym_str = dk_sym.decode() if isinstance(dk_sym, bytes) else str(dk_sym)
            dash_sym = self._from_dk_symbol(dk_sym_str)

            # Note: simplefix stores tags as bytes (b'269'), convert to int
            entry_type = None
            pairs = getattr(msg, 'pairs', None)
            if pairs is None:
                return
            for raw_tag, value in pairs:
                tag = int(raw_tag) if isinstance(raw_tag, bytes) else raw_tag
                if tag == TAG_MDENTRYTYPE:
                    entry_type = value
                elif tag == TAG_MDENTRYPX:
                    price = float(value)
                    if entry_type == b'0':  # Bid
                        self._bid[dash_sym] = price
                    elif entry_type == b'1':  # Ask
                        self._ask[dash_sym] = price
            bid = self._bid.get(dash_sym)
            ask = self._ask.get(dash_sym)
            if bid and ask:
                pip_mult = 1000 if "JPY" in dash_sym.upper() else 100000
                spread = round((ask - bid) * pip_mult, 1)
                acct_info = self.dd["ea_account_info"].get(self.account_id, {})
                acct_info["spread"] = spread
                acct_info["bid"] = bid
                acct_info["ask"] = ask
                acct_info["symbol"] = dash_sym
                self.dd["ea_account_info"][self.account_id] = acct_info
                logger.debug("[%s] DK Quote: %s bid=%.5f ask=%.5f spread=%.1f",
                            self.account_id, dash_sym, bid, ask, spread)
        except Exception as e:
            import traceback
            logger.error("[%s] DK W handler error: %s\n%s", self.account_id, e, traceback.format_exc())


    def _on_market_data_reject(self, msg):
        """Handle MarketDataRequestReject (MsgType=Y)."""
        text = msg.get(TAG_TEXT)
        req_id = msg.get(TAG_MDREQID)
        logger.error("[%s] DK MD reject: %s (req=%s)", self.account_id,
                     text.decode() if text else "unknown",
                     req_id.decode() if req_id else "?")

    # ─── Account Info (Custom MsgType U7/U2) ────────────────────────────────

    def _subscribe_trading_session(self):
        """Send TradingSessionStatusRequest (MsgType=g) to subscribe to account updates.
        This triggers the server to push U2 (AccountInfo) and U3 (InstrumentPositionInfo)."""
        req_id = f"TSS_{uuid.uuid4().hex[:8]}"
        fields = [
            (335, req_id),   # TradSesReqID
            (TAG_SUBSCRIPTIONREQUESTTYPE, "1"),  # Snapshot + Updates
        ]
        if self._external_account:
            fields.append((TAG_ACCOUNT, self._external_account))
        self.trade_session.send_message("g", fields)
        logger.info("[%s] DK TradingSessionStatusRequest sent (subscribe)", self.account_id)

    def _request_account_info(self):
        """Send AccountInfoRequest (MsgType=U7) to get leverage, margin, equity."""
        fields = []
        if not self.single_connection and self._external_account:
            fields.append((TAG_ACCOUNT, self._external_account))
        self.trade_session.send_message("U7", fields)
        self._last_acct_info_request = time.time()
        logger.info("[%s] DK account info request sent (U7)", self.account_id)

    def _on_account_info(self, msg):
        """Handle AccountInfo (MsgType=U2) — Dukascopy custom.
        Fields: AccountName(7004), Currency(15), Leverage(7005),
                UsableMargin(7006), Equity(7007)."""
        for raw_tag, value in msg.pairs:
            try:
                tag = int(raw_tag) if isinstance(raw_tag, bytes) else raw_tag
                if tag == TAG_DK_LEVERAGE:
                    self._leverage = int(float(value))
                elif tag == TAG_DK_CURRENTMARGIN:  # 7006 is UsableMargin
                    self._free_margin = float(value)
                elif tag == TAG_DK_CURRENTEQUITY:  # 7007 is Equity
                    self._equity = float(value)
                elif tag == 7004:  # AccountName — the numeric trader account
                    acct_name = value.decode() if isinstance(value, bytes) else str(value)
                    if acct_name and acct_name != self._external_account:
                        logger.info("[%s] DK trader account discovered: %s (was: %s)",
                                    self.account_id, acct_name, self._external_account)
                        self._external_account = acct_name
                elif tag == TAG_ACCOUNT:
                    acct_val = value.decode() if isinstance(value, bytes) else str(value)
                    if acct_val and acct_val != self._external_account:
                        logger.info("[%s] DK account tag(1) discovered: %s", self.account_id, acct_val)
                        self._external_account = acct_val
            except (ValueError, TypeError):
                pass
        
        # Calculate Used Margin if we have both Equity and Free Margin
        if self._equity is not None and self._free_margin is not None:
            self._margin_used = round(self._equity - self._free_margin, 2)

        logger.info("[%s] DK AccountInfo(U2): leverage=%s equity=%.2f margin_used=%.2f free_margin=%.2f acct=%s",
                    self.account_id, self._leverage,
                    self._equity or 0, self._margin_used or 0, self._free_margin or 0, self._external_account)

    def _on_notification(self, msg):
        """Handle Notification (MsgType=U1) — server push notifications."""
        text = msg.get(TAG_TEXT)
        priority = msg.get(7003)
        text_str = text.decode(errors='ignore') if text else "?"
        prio_str = priority.decode(errors='ignore') if priority else "?"
        logger.info("[%s] DK Notification: priority=%s text=%s",
                    self.account_id, prio_str, text_str)

        # Parse order execution status from notification message
        clordid_str = None
        for pid in list(self._pending_orders.keys()):
            if f"({pid})" in text_str:
                clordid_str = pid
                break

        if clordid_str:
            order_info = self._pending_orders.get(clordid_str)
            if "FILLED" in text_str:
                # Parse price and ticket from notification
                import re
                match = re.search(r'FILLED at ([\d\.]+)\s+\(#(\d+)', text_str)
                if match:
                    fill_price = float(match.group(1))
                    order_id_str = match.group(2)
                else:
                    fill_price = 0.0
                    order_id_str = ""

                if not order_id_str:
                    order_id_str = str(int(time.time() * 1000))

                fill_qty = int(order_info.get("qty", 0))
                sym_name = order_info.get("symbol", "")
                is_close = order_info.get("is_close", False)

                logger.info("[%s] DK FILL via U1 Notification: %s %s @ %.5f qty=%d close=%s (clordid=%s)",
                            self.account_id, order_info.get("side", "?").upper(),
                            sym_name, fill_price, fill_qty, is_close, clordid_str)

                # Netting mode: do not track individual fill tickets as positions.
                # Position tracking is handled via U3 InstrumentPositionInfo messages.
                pass

                # Feed into dashboard
                session_id = order_info.get("session_id")
                if session_id:
                    is_rollback = order_info.get("is_rollback", False)
                    status = "rollback_closed" if is_rollback else ("closed" if is_close else "filled")
                    if is_close and order_info.get("position_id"):
                        pos_id_val = order_info["position_id"]
                        if str(pos_id_val).startswith("dk_"):
                            import hashlib
                            symbol = str(pos_id_val)[3:]  # strip "dk_"
                            try:
                                ticket = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16)
                            except Exception:
                                ticket = pos_id_val
                        else:
                            try:
                                ticket = int(pos_id_val)
                            except ValueError:
                                ticket = pos_id_val
                    else:
                        ticket = int(order_id_str) if order_id_str.isdigit() else int(time.time() * 1000)
                    # Compute quote_price at time of order dispatch
                    order_side = order_info.get("side", "buy").lower()
                    quote_at_order = order_info.get("quote_at_order")
                    quote_price = 0.0
                    if quote_at_order:
                        quote_price = quote_at_order[1] if order_side == "buy" else quote_at_order[0]
                    self._post_trade_result({
                        "session_id": session_id,
                        "account": self.account_id,
                        "status": status,
                        "ticket": ticket,
                        "fill_price": fill_price,
                        "quote_price": quote_price,
                        "spread": None,
                        "detail": f"DK fill: {sym_name} @ {fill_price}",
                    })

                self._pending_orders.pop(clordid_str, None)

            elif "REJECT" in text_str.upper() or "DECLINED" in text_str.upper() or "FAILED" in text_str.upper():
                logger.error("[%s] DK order REJECTED via U1 Notification: %s (clordid=%s)",
                             self.account_id, text_str, clordid_str)
                session_id = order_info.get("session_id")
                if session_id:
                    self._post_trade_result({
                        "session_id": session_id,
                        "account": self.account_id,
                        "status": "error",
                        "detail": f"DK reject: {text_str}",
                    })
                self._pending_orders.pop(clordid_str, None)

    def _on_instrument_position_info(self, msg):
        """Handle InstrumentPositionInfo (MsgType=U3) — per-instrument position.
        Fields: AccountName(7004), Symbol(55), Amount(7008), Price(44)."""
        dk_sym = msg.get(TAG_SYMBOL)
        amount_raw = msg.get(TAG_DK_AMOUNT)
        price_raw = msg.get(TAG_PRICE)

        if not dk_sym:
            return
        dk_sym_str = dk_sym.decode() if isinstance(dk_sym, bytes) else str(dk_sym)
        sym_name = self._from_dk_symbol(dk_sym_str)
        amount = float(amount_raw) if amount_raw else 0
        price = float(price_raw) if price_raw else 0

        if amount != 0:
            pos_id = f"dk_{sym_name}"
            self._positions[pos_id] = {
                "symbol": sym_name,
                "side": "buy" if amount > 0 else "sell",
                "qty": abs(amount),
                "price": price,
            }
            self.subscribe_symbol(sym_name)
            logger.info("[%s] DK Position(U3): %s amount=%.0f price=%.5f",
                        self.account_id, sym_name, amount, price)
        else:
            pos_id = f"dk_{sym_name}"
            prev = self._positions.pop(pos_id, None)
            if prev:
                logger.info("[%s] DK Position CLOSED(U3): %s", self.account_id, sym_name)

    def _on_trading_session_status(self, msg):
        """Handle TradingSessionStatus (MsgType=h) — response to g request."""
        status = msg.get(340)  # TradSesStatus
        status_str = status.decode() if status else "?"
        
        # Log all tags in TradingSessionStatus for diagnostic purposes
        pairs_str = ", ".join(f"{tag}={val.decode(errors='ignore') if isinstance(val, bytes) else val}" for tag, val in msg.pairs)
        logger.info("[%s] DK TradingSessionStatus fields: %s", self.account_id, pairs_str)

        # Check if Account tag (1) is present
        acct_val = msg.get(TAG_ACCOUNT)
        if acct_val:
            acct_str = acct_val.decode() if isinstance(acct_val, bytes) else str(acct_val)
            if acct_str and acct_str != self._external_account:
                logger.info("[%s] DK trader account discovered in TradingSessionStatus: %s (was: %s)",
                            self.account_id, acct_str, self._external_account)
                self._external_account = acct_str
                # Re-request account info immediately with the discovered ID
                self._request_account_info()

        logger.info("[%s] DK TradingSessionStatus: status=%s", self.account_id, status_str)

    # ─── Overnight Report (Custom MsgType U4) ───────────────────────────────

    def _on_overnight_report(self, msg):
        """Handle OvernightReport (MsgType=U4) — sent daily after settlement."""
        account = msg.get(TAG_ACCOUNT)
        amount = None
        for raw_tag, value in msg.pairs:
            tag = int(raw_tag) if isinstance(raw_tag, bytes) else raw_tag
            if tag == TAG_DK_AMOUNT:
                try:
                    amount = float(value)
                except (ValueError, TypeError):
                    pass
        if amount is not None:
            self._total_swap = round(self._total_swap + amount, 2)

        acct_str = account.decode() if account else "?"
        logger.info("[%s] DK OvernightReport: account=%s swap_amount=%s total_swap=%.2f",
                    self.account_id, acct_str, amount, self._total_swap)

    def _get_conversion_to_usd(self, quote_currency):
        quote_currency = quote_currency.upper()
        if quote_currency == "USD":
            return 1.0
        
        # Try XYZUSD
        for k in self._bid.keys():
            k_upper = k.upper().replace("/", "")
            if k_upper == f"{quote_currency}USD":
                return self._bid[k]
        
        # Try USDXYZ
        for k in self._ask.keys():
            k_upper = k.upper().replace("/", "")
            if k_upper == f"USD{quote_currency}":
                ask_val = self._ask[k]
                return 1.0 / ask_val if ask_val > 0 else 1.0
                
        return 1.0  # fallback

    def _calculate_position_pnl(self, p):
        symbol = p.get("symbol")
        qty = p.get("qty", 0)
        side = p.get("side")
        entry_price = p.get("price")
        if not symbol or not entry_price:
            return 0.0
            
        bid = self._bid.get(symbol)
        ask = self._ask.get(symbol)
        if not bid or not ask:
            # Case-insensitive lookup
            sym_upper = symbol.upper()
            for k in self._bid.keys():
                if k.upper() == sym_upper:
                    bid = self._bid.get(k)
                    ask = self._ask.get(k)
                    break
        
        if not bid or not ask:
            return 0.0
            
        # PnL in quote currency
        if side == "buy":
            pnl_quote = (bid - entry_price) * qty
        else:
            pnl_quote = (entry_price - ask) * qty
            
        # Parse quote currency
        parts = symbol.split("/")
        if len(parts) == 2:
            quote = parts[1]
        else:
            clean_sym = symbol.split(".")[0]
            if len(clean_sym) >= 6:
                quote = clean_sym[-3:]
            else:
                quote = "USD"
                
        rate = self._get_conversion_to_usd(quote)
        return pnl_quote * rate

    def get_symbol_info(self, symbol):
        """Get bid/ask/spread for a specific symbol from internal price cache."""
        sym_clean = symbol.upper().replace("/", "")
        bid = ask = None
        for k in self._bid.keys():
            if k.upper().replace("/", "") == sym_clean:
                bid = self._bid.get(k)
                ask = self._ask.get(k)
                break
        if bid and ask:
            pip_mult = 1000 if "JPY" in sym_clean else 100000
            spread = round((ask - bid) * pip_mult, 1)
            return {"bid": bid, "ask": ask, "spread": spread}
        return None

    # ─── Order Execution ────────────────────────────────────────────────────

    def send_market_order(self, symbol_name, side, lot_size, session_id=None,
                          position_id=None, comment="", is_rollback=False):
        """
        Send a market order (NewOrderSingle MsgType=D).
        Args:
            symbol_name: e.g. "EURUSD" (will be converted to EUR/USD)
            side: "buy" or "sell"
            lot_size: In lots (e.g. 0.01)
            session_id: Dashboard session ID for fill tracking
            position_id: Position ID for closing (used in ClOrdLinkID)
            comment: Order comment (tag 58 Text)
            is_rollback: Whether this is a rollback close
        Returns: ClOrdID string
        """
        dk_sym = self._to_dk_symbol(symbol_name)
        clordid = str(uuid.uuid4())[:12]
        qty = int(lot_size * self.lot_multiplier)
        fix_side = "1" if side.lower() == "buy" else "2"
        now = datetime.utcnow().strftime("%Y%m%d-%H:%M:%S")

        fields = [
            (TAG_CLORDID, clordid),
            (TAG_SYMBOL, dk_sym),
            (TAG_SIDE, fix_side),
            (TAG_TRANSACTTIME, now),
            (TAG_ORDTYPE, "1"),    # MARKET — Dukascopy netting mode auto-reduces/closes exposure
            (TAG_ORDERQTY, str(qty)),
            (TAG_TIMEINFORCE, "3"),    # IOC (Immediate or Cancel) — Dukascopy standard
        ]
        if not self.single_connection and self._external_account and self._external_account != self.username:
            fields.append((TAG_ACCOUNT, self._external_account))
        if comment:
            fields.append((TAG_TEXT, comment))

        # Snapshot quote at the moment the order is dispatched
        sym_clean = symbol_name.upper().replace("/", "")
        quote_at_order = None
        for k in self._bid.keys():
            if k.upper().replace("/", "") == sym_clean:
                quote_at_order = (self._bid[k], self._ask[k])
                break

        # Track this order
        self._pending_orders[clordid] = {
            "session_id": session_id,
            "symbol": symbol_name,
            "side": side,
            "lot_size": lot_size,
            "qty": qty,
            "position_id": position_id,
            "is_close": position_id is not None,
            "is_rollback": is_rollback,
            "ts": time.time(),
            "quote_at_order": quote_at_order,  # (bid, ask) snapshot
        }

        self.trade_session.send_message("D", fields)
        logger.info("[%s] DK Market order: %s %s %.2f lots (clordid=%s, pos=%s)",
                    self.account_id, side.upper(), dk_sym, lot_size,
                    clordid, position_id or "new")
        return clordid

    def close_position(self, position_id, symbol_name, side, lot_size,
                       session_id=None, comment="", is_rollback=False):
        """Close a position via opposite-side order with ClOrdLinkID."""
        close_side = "sell" if side.lower() == "buy" else "buy"
        return self.send_market_order(
            symbol_name, close_side, lot_size,
            session_id=session_id, position_id=position_id, comment=comment,
            is_rollback=is_rollback
        )

    # ─── Execution Reports ──────────────────────────────────────────────────

    def _on_execution_report(self, msg):
        """Handle ExecutionReport (MsgType=8) — Dukascopy FIX 4.4."""
        exec_type = msg.get(TAG_EXECTYPE)
        ord_status = msg.get(TAG_ORDSTATUS)
        clordid = msg.get(TAG_CLORDID)
        order_id = msg.get(TAG_ORDERID)
        avg_px = msg.get(TAG_AVGPX)
        cum_qty = msg.get(TAG_CUMQTY)
        sym_raw = msg.get(TAG_SYMBOL)
        side_raw = msg.get(TAG_SIDE)
        text = msg.get(TAG_TEXT)

        clordid_str = clordid.decode() if clordid else ""
        order_info = self._pending_orders.get(clordid_str, {})

        if exec_type == DK_EXECTYPE_NEW or exec_type == DK_EXECTYPE_PENDINGNEW:
            # Order acknowledged
            logger.info("[%s] DK order accepted: clordid=%s",
                        self.account_id, clordid_str)

        elif exec_type == DK_EXECTYPE_FILL or exec_type == DK_EXECTYPE_CALCULATED:
            # Order filled
            fill_price = float(avg_px) if avg_px else 0
            fill_qty = int(float(cum_qty)) if cum_qty else 0
            order_id_str = order_id.decode() if order_id else ""

            dk_sym = sym_raw.decode() if sym_raw else order_info.get("symbol", "")
            sym_name = self._from_dk_symbol(dk_sym) if "/" in dk_sym else dk_sym

            is_close = order_info.get("is_close", False)

            logger.info("[%s] DK FILL: %s %s @ %.5f qty=%d close=%s",
                        self.account_id, order_info.get("side", "?").upper(),
                        sym_name, fill_price, fill_qty, is_close)

            # Netting mode: do not track individual fill tickets as positions.
            # Position tracking is handled via U3 InstrumentPositionInfo messages.
            pass

            # Feed into dashboard
            session_id = order_info.get("session_id")
            if session_id:
                is_rollback = order_info.get("is_rollback", False)
                status = "rollback_closed" if is_rollback else ("closed" if is_close else "filled")
                if is_close and order_info.get("position_id"):
                    pos_id_val = order_info["position_id"]
                    if str(pos_id_val).startswith("dk_"):
                        import hashlib
                        symbol = str(pos_id_val)[3:]  # strip "dk_"
                        try:
                            ticket = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16)
                        except Exception:
                            ticket = pos_id_val
                    else:
                        try:
                            ticket = int(pos_id_val)
                        except ValueError:
                            ticket = pos_id_val
                else:
                    ticket = int(order_id_str) if order_id_str.isdigit() else int(time.time() * 1000)
                # Compute quote_price: for buy = ask, for sell = bid at time of order
                order_side = order_info.get("side", "buy").lower()
                quote_at_order = order_info.get("quote_at_order")
                quote_price = 0.0
                if quote_at_order:
                    quote_price = quote_at_order[1] if order_side == "buy" else quote_at_order[0]
                self._post_trade_result({
                    "session_id": session_id,
                    "account": self.account_id,
                    "status": status,
                    "ticket": ticket,
                    "fill_price": fill_price,
                    "quote_price": quote_price,
                    "spread": None,
                    "detail": f"DK fill: {sym_name} @ {fill_price}",
                })

            self._pending_orders.pop(clordid_str, None)

        elif exec_type == DK_EXECTYPE_REJECTED:
            reason = text.decode() if text else "unknown"
            logger.error("[%s] DK order REJECTED: %s (clordid=%s)",
                         self.account_id, reason, clordid_str)
            session_id = order_info.get("session_id")
            if session_id:
                self._post_trade_result({
                    "session_id": session_id,
                    "account": self.account_id,
                    "status": "error",
                    "detail": f"DK reject: {reason}",
                })
            self._pending_orders.pop(clordid_str, None)

        elif exec_type == DK_EXECTYPE_CANCELED:
            logger.info("[%s] DK order canceled: clordid=%s", self.account_id, clordid_str)
            self._pending_orders.pop(clordid_str, None)

    # ─── Position Management ────────────────────────────────────────────────

    def request_positions(self):
        """Public method — re-subscribe to trading session to get fresh U3 position pushes."""
        self._subscribe_trading_session()

    # ─── Rejects ────────────────────────────────────────────────────────────

    def _on_session_reject(self, msg):
        text = msg.get(TAG_TEXT)
        ref_tag = msg.get(371)
        ref_msg = msg.get(372)
        logger.error("[%s] DK session reject: %s (ref_msg=%s, ref_tag=%s) fields: %s",
                     self.account_id,
                     text.decode() if text else "unknown",
                     ref_msg.decode() if ref_msg else "?",
                     ref_tag.decode() if ref_tag else "?",
                     {int(k) if k.isdigit() else k: (v.decode(errors='ignore') if isinstance(v, bytes) else v) for k, v in msg.pairs})

    def _on_business_reject(self, msg):
        text = msg.get(TAG_TEXT)
        ref_msg = msg.get(TAG_REFMSGTYPE)
        logger.error("[%s] DK business reject: %s (ref=%s) fields: %s", self.account_id,
                     text.decode() if text else "?",
                     ref_msg.decode() if ref_msg else "?",
                     {int(k) if k.isdigit() else k: (v.decode(errors='ignore') if isinstance(v, bytes) else v) for k, v in msg.pairs})

    def _on_order_cancel_reject(self, msg):
        text = msg.get(TAG_TEXT)
        logger.error("[%s] DK cancel reject: %s", self.account_id,
                     text.decode() if text else "?")

    # ─── Heartbeat / Data Feed ──────────────────────────────────────────────

    def _heartbeat_loop(self):
        """Periodically update dashboard data and handle auto-reconnect."""
        while self._running:
            try:
                now = time.time()
                self.dd["ea_heartbeats"][self.account_id] = now

                info = self.dd["ea_account_info"].get(self.account_id, {})

                # Feed price data
                for sym_name in list(self._bid.keys()):
                    bid = self._bid.get(sym_name)
                    ask = self._ask.get(sym_name)
                    if bid and ask:
                        info["bid"] = bid
                        info["ask"] = ask
                        pip_mult = 1000 if "JPY" in sym_name.upper() else 100000
                        info["spread"] = round((ask - bid) * pip_mult, 1)
                        info["symbol"] = sym_name
                        break

                # Calculate PNL locally from positions
                total_pnl = 0.0
                for p in self._positions.values():
                    total_pnl += self._calculate_position_pnl(p)
                self._total_pnl = round(total_pnl, 2)

                # Calculate balance
                if self._equity is not None:
                    self._balance = round(self._equity - self._total_pnl, 2)

                # Calculate margin used if we have equity and free margin
                if self._equity is not None and self._free_margin is not None:
                    self._margin_used = round(self._equity - self._free_margin, 2)

                # Feed balance/equity/leverage/margin/swaps
                if self._balance is not None:
                    info["balance"] = self._balance
                if self._equity is not None:
                    info["equity"] = self._equity
                if self._margin_used is not None:
                    info["margin_used"] = self._margin_used
                    info["margin"] = self._margin_used
                if self._free_margin is not None:
                    info["free_margin"] = self._free_margin
                if self._leverage is not None:
                    info["leverage"] = self._leverage
                if getattr(self, "_total_swap", None) is not None:
                    info["total_swap"] = self._total_swap
                if getattr(self, "_total_pnl", None) is not None:
                    info["total_pnl"] = self._total_pnl

                info["last_update"] = now
                info["fix_account"] = True
                # Dukascopy uses netting mode: all fills for one symbol collapse into
                # a single position. Individual fill tickets are never visible as
                # separate open positions — the hedge monitor must NOT do per-ticket
                # comparison for this account type.
                info["netting_mode"] = True
                info["trade_connected"] = self.trade_session.connected
                info["quote_connected"] = self.quote_session.connected
                info["positions"] = len(self._positions)
                info["open_tickets"] = []
                import hashlib
                for pid, p in self._positions.items():
                    symbol = p["symbol"].upper()
                    ticket_int = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16)
                    info["open_tickets"].append(ticket_int)
                # Signed lots: buy = positive, sell = negative
                if self._positions:
                    _bl = sum(p.get("qty", 0) for p in self._positions.values() if p.get("side") == "buy")
                    _sl = sum(p.get("qty", 0) for p in self._positions.values() if p.get("side") == "sell")
                    info["total_lots"] = round((_bl - _sl) / 100000.0, 2)
                    # Per-instrument lots breakdown
                    _lbi = {}
                    for p in self._positions.values():
                        sym = p.get("symbol", "Unknown")
                        lots = round(p.get("qty", 0) / 100000.0, 2)
                        if sym not in _lbi:
                            _lbi[sym] = {"buy": 0, "sell": 0}
                        if p.get("side") == "buy":
                            _lbi[sym]["buy"] = round(_lbi[sym]["buy"] + lots, 2)
                        else:
                            _lbi[sym]["sell"] = round(_lbi[sym]["sell"] + lots, 2)
                    info["lots_by_instrument"] = _lbi
                    info["swap_by_instrument"] = {}
                else:
                    info["total_lots"] = 0
                    info["lots_by_instrument"] = {}
                    info["swap_by_instrument"] = {}
                self.dd["ea_account_info"][self.account_id] = info

                # Periodically request account info and positions
                if self.trade_session.connected:
                    if now - self._last_acct_info_request > 60:
                        self._request_account_info()

                # ── Auto-reconnect: detect dead sessions ──
                if now - self._last_reconnect_check > 10:
                    self._last_reconnect_check = now
                    self._check_fix_sessions()

            except Exception as e:
                logger.error("[%s] DK heartbeat error: %s", self.account_id, e)
            time.sleep(1)

    def _check_fix_sessions(self):
        """Detect dead FIX sessions and restart if thread died."""
        # Check TRADE session
        if not self.trade_session._running and self._running:
            logger.warning("[%s] DK TRADE session thread died — restarting", self.account_id)
            try:
                self.trade_session.start()
                for _ in range(20):
                    if self.trade_session.connected:
                        break
                    time.sleep(0.5)
                if self.trade_session.connected:
                    logger.info("[%s] DK TRADE session re-established", self.account_id)
                    time.sleep(1)
                    self._subscribe_trading_session()
                    if self._external_account and self._external_account != self.username:
                        self._request_account_info()
            except Exception as e:
                logger.error("[%s] DK TRADE session restart error: %s", self.account_id, e)

        # Check QUOTE session
        if not self.quote_session._running and self._running:
            logger.warning("[%s] DK QUOTE session thread died — restarting", self.account_id)
            try:
                self.quote_session.start()
                for _ in range(20):
                    if self.quote_session.connected:
                        break
                    time.sleep(0.5)
                if self.quote_session.connected:
                    logger.info("[%s] DK QUOTE session re-established — re-subscribing", self.account_id)
                    time.sleep(1)
                    old_syms = list(self._subscribed_symbols)
                    self._subscribed_symbols.clear()
                    for sym in old_syms:
                        self.subscribe_symbol(sym)
            except Exception as e:
                logger.error("[%s] DK QUOTE session restart error: %s", self.account_id, e)

    def _post_trade_result(self, data):
        """POST trade result to the dashboard's /api/trade_result endpoint with retry/backoff."""
        url = self.dd.get("dashboard_url", "http://127.0.0.1:5000")
        url = f"{url}/api/trade_result"
        payload = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=payload,
                                    headers={"Content-Type": "application/json"})
        
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    resp.read()
                return  # Success!
            except Exception as e:
                if attempt < max_attempts - 1:
                    sleep_time = 0.1 * (2 ** attempt)
                    logger.warning("[%s] Failed to post trade result to %s (attempt %d/%d): %s. Retrying in %.1fs...",
                                   self.account_id, url, attempt + 1, max_attempts, e, sleep_time)
                    time.sleep(sleep_time)
                else:
                    logger.error("[%s] Failed to post trade result to %s after %d attempts: %s",
                                 self.account_id, url, max_attempts, e)


# ─── FixAccountManager ──────────────────────────────────────────────────────
class FixAccountManager:
    """
    Manages all FIX accounts. Loads config, starts/stops accounts,
    and runs the command loop that pushes orders based on dashboard session state.
    """
    FIX_CONFIG_FILE = "fix_accounts.json"

    def __init__(self, dashboard_data, config_dir="."):
        """
        Args:
            dashboard_data: Dict with references to dashboard shared data
            config_dir: Directory containing fix_accounts.json
        """
        self.dd = dashboard_data
        self.config_path = os.path.join(config_dir, self.FIX_CONFIG_FILE)
        self.accounts = {}  # account_id -> CTraderFixAccount
        self._running = False
        self._command_thread = None
        self._lock = threading.Lock()

    def load_config(self):
        """Load FIX accounts from config file."""
        if not os.path.exists(self.config_path):
            logger.info("No FIX config file found at %s", self.config_path)
            return
        try:
            with open(self.config_path, "r") as f:
                configs = json.load(f)
            for acct_id, config in configs.items():
                self.add_account(acct_id, config, save=False, auto_connect=False)
            logger.info("Loaded %d FIX account(s) from config", len(configs))
            # Auto-connect accounts with auto_connect_start enabled
            # Uses retry loop with backoff so transient failures don't leave accounts dead
            def _auto_connect_fix(aid, acct):
                delay = 5  # base delay seconds
                attempt = 0
                while True:
                    try:
                        attempt += 1
                        logger.info("[%s] FIX auto-connecting at startup (attempt #%d)...", aid, attempt)
                        # If already running but not connected, stop first to reset state
                        if hasattr(acct, '_running') and acct._running and not acct.connected:
                            logger.info("[%s] Stopping stale connection before retry", aid)
                            acct.stop()
                            time.sleep(1)
                        acct.start()
                        # For async accounts (OpenAPI, etc.), start() returns before
                        # TCP is established. Poll for up to 15s.
                        for _w in range(30):
                            if acct.connected:
                                break
                            time.sleep(0.5)
                        if acct.connected:
                            logger.info("[%s] FIX auto-connect succeeded on attempt #%d", aid, attempt)
                            return
                        logger.error("[%s] FIX auto-connect failed — retrying in %ds", aid, delay)
                    except Exception as e:
                        logger.error("[%s] FIX auto-connect error: %s — retrying in %ds", aid, e, delay)
                    # Sleep in small increments
                    for _ in range(int(delay * 2)):
                        time.sleep(0.5)
                    delay = min(delay * 2.0, 60)

            for acct_id, config in configs.items():
                if config.get("auto_connect_start", True):
                    account = self.accounts.get(acct_id)
                    if account:
                        threading.Thread(target=_auto_connect_fix, args=(acct_id, account),
                                         daemon=True, name=f"Acct-AutoStart-{acct_id}").start()
        except Exception as e:
            logger.error("Failed to load FIX config: %s", e)

    def save_config(self):
        """Save FIX accounts to config file."""
        configs = {}
        for acct_id, account in self.accounts.items():
            configs[acct_id] = account.config
        try:
            with open(self.config_path, "w") as f:
                json.dump(configs, f, indent=2)
        except Exception as e:
            logger.error("Failed to save FIX config: %s", e)

    def add_account(self, account_id, config, save=True, auto_connect=True):
        """Add a new account (cTrader FIX, Swissquote FIX, or Open API). Optionally connect."""
        with self._lock:
            if account_id in self.accounts:
                logger.warning("Account %s already exists", account_id)
                return False
            impl = config.get("implementation", "ctrader")
            if impl == "openapi":
                account = CTraderOpenApiAccount(account_id, config, self.dd)
                account._save_config_callback = self.save_config  # persist refreshed tokens
            elif impl == "swissquote":
                account = SwissquoteFixAccount(account_id, config, self.dd)
            elif impl == "dukascopy":
                account = DukascopyFixAccount(account_id, config, self.dd)
            else:
                account = CTraderFixAccount(account_id, config, self.dd)
            self.accounts[account_id] = account
        if save:
            self.save_config()
        # Start in background only if auto_connect is True
        if auto_connect:
            threading.Thread(target=account.start, daemon=True,
                             name=f"Acct-Start-{account_id}").start()
        return True

    def remove_account(self, account_id):
        """Stop and remove a FIX account."""
        with self._lock:
            account = self.accounts.pop(account_id, None)
        if account:
            account.stop()
            self.save_config()
            return True
        return False

    def get_status(self):
        """Get status of all FIX accounts."""
        result = {}
        for acct_id, acct in self.accounts.items():
            info = self.dd["ea_account_info"].get(acct_id, {})
            result[acct_id] = {
                "label": acct.label,
                "trade_connected": acct.connected,
                "quote_connected": acct.quote_connected,
                "symbols": list(getattr(acct, '_symbols_by_name', getattr(acct, '_subscribed_symbols', {})).keys()) if hasattr(acct, '_symbols_by_name') else list(getattr(acct, '_subscribed_symbols', set())),
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "leverage": info.get("leverage") or acct.config.get("leverage"),
                "margin": info.get("margin") or info.get("margin_used"),
                "margin_used": info.get("margin_used") or info.get("margin"),
                "free_margin": info.get("free_margin"),
                "total_pnl": info.get("total_pnl"),
                "total_swap": info.get("total_swap"),
                "total_lots": info.get("total_lots"),
                "positions": info.get("positions"),
                "oldest_position_age": info.get("oldest_position_age"),
                "implementation": acct.config.get("implementation", "ctrader"),
                "openapi_connected": info.get("openapi_connected", False),
                "group_label": acct.config.get("group_label"),
                "alert_email": acct.config.get("alert_email"),
                "alert_telegram": acct.config.get("alert_telegram"),
                "auto_connect_start": acct.config.get("auto_connect_start", True),
            }
        return result

    def start(self):
        """Load config and start all accounts + command loop."""
        self.load_config()
        self._running = True
        self._command_thread = threading.Thread(
            target=self._command_loop, daemon=True, name="FIX-CmdLoop"
        )
        self._command_thread.start()

    def stop(self):
        """Stop all accounts and command loop."""
        self._running = False
        for account in self.accounts.values():
            account.stop()

    def _command_loop(self):
        """
        Periodically check dashboard sessions for commands that should be
        sent via FIX (instead of waiting for EA polling).
        """
        while self._running:
            try:
                self._process_commands()
            except Exception as e:
                logger.exception("FIX command loop error")
            time.sleep(0.25)  # Fast polling for cycle responsiveness

    def _process_commands(self):
        """
        Check each active session for FIX accounts that need commands.
        Uses the same _should_issue_command() logic as EA polling.
        """
        should_issue = self.dd.get("should_issue_command")
        if not should_issue:
            return

        with self.dd["lock"]:
            for session_id, session in self.dd["sessions"].items():
                if session.get("status") not in ("active", "partial_close"):
                    continue
                sides = session.get("sides", {})
                for account_id in sides:
                    # Only process FIX accounts
                    if account_id not in self.accounts:
                        continue
                    fix_acct = self.accounts[account_id]
                    if not fix_acct.connected:
                        continue

                    # Ensure symbol is subscribed for dynamic quote feed
                    if isinstance(fix_acct, (CTraderFixAccount, CTraderOpenApiAccount, SwissquoteFixAccount, DukascopyFixAccount)):
                        side_info_sub = sides[account_id]
                        pair_sub = (side_info_sub.get("pair") or session.get("pair", "")).upper()
                        if pair_sub:
                            fix_acct.subscribe_symbol(pair_sub)

                    result = should_issue(session, account_id)
                    if result is False:
                        continue

                    action = session.get("action", "open")
                    side_info = sides[account_id]
                    pair = (side_info.get("pair") or session.get("pair", "")).upper()
                    lot_size = side_info.get("lot_size") or session.get("lot_size", 0.01)
                    comment = side_info.get("comment", "")
                    max_spread = side_info.get("max_spread") if side_info.get("max_spread") is not None else session.get("max_spread_points", 999)
                    try:
                        max_spread = float(max_spread) if max_spread is not None else 999
                    except (ValueError, TypeError):
                        max_spread = 999

                    # Check spread gating using FIX market data.
                    # Skip spread check for: rollback (emergency close), cycle reopen phase
                    # (reopen must be immediate after close — no spread wait).
                    _bypass_spread = (result == "rollback" or
                                      (action.startswith("cycle_") and result is True))
                    current_spread = None
                    ea_info = self.dd["ea_account_info"].get(account_id, {})
                    try:
                        current_spread = float(ea_info.get("spread")) if ea_info.get("spread") is not None else None
                    except (ValueError, TypeError):
                        current_spread = None
                    if not _bypass_spread:
                        # MAXSPD=0 means block all; MAXSPD=N means allow up to N pips;
                        # missing/999 means no restriction. Block if no live spread data
                        # and a strict spread limit is configured.
                        if current_spread is None and max_spread < 999:
                            continue  # No spread data — don't trade blind
                        if current_spread is not None and current_spread > max_spread:
                            # Spread too wide
                            session["spread_rejects"][account_id] = session.get("spread_rejects", {}).get(account_id, 0) + 1
                            continue

                    # Mark in-flight
                    self.dd["in_flight_commands"][(session_id, account_id)] = time.time()

                    if result == "rollback":
                        # Rollback close — close the most recent position
                        self._send_close_command(fix_acct, session, account_id, pair, lot_size, comment, is_rollback=True)
                    elif result == "cycle_close":
                        # Cycle close
                        self._send_close_command(fix_acct, session, account_id, pair, lot_size, comment, is_rollback=False)
                    elif action == "close":
                        # Normal close
                        self._send_close_command(fix_acct, session, account_id, pair, lot_size, comment, is_rollback=False)
                    elif action == "open" or result is True:
                        # Open new position
                        side_num = side_info.get("side_number", 1)
                        # Determine buy/sell based on side_number convention
                        # Side 1 = buy, Side 2 = sell (standard convention)
                        trade_side = side_info.get("action", "buy") if side_info.get("action") else ("buy" if side_num == 1 else "sell")
                        fix_acct.send_market_order(
                            pair, trade_side, lot_size,
                            session_id=session_id, comment=comment
                        )

    def _send_close_command(self, fix_acct, session, account_id, pair, lot_size, comment, is_rollback=False):
        """Send a close order for the oldest open position."""
        # Find the position to close from fills
        fills = session.get("fills", [])
        close_fills = session.get("close_fills", [])
        closed_tickets = {f["ticket"] for f in close_fills if f.get("account") == account_id}

        rb_tickets = session.get("rollback_tickets", {}).get(account_id, [])
        custom_lot_size = None
        target_ticket = None

        if is_rollback and rb_tickets:
            first_ticket = rb_tickets[0]
            if isinstance(first_ticket, dict):
                target_ticket = first_ticket.get("ticket")
                custom_lot_size = first_ticket.get("lots")
            else:
                target_ticket = first_ticket

            # Immediately execute close for synthetic tickets without matching against fills
            if target_ticket and (str(target_ticket).startswith("rebal_") or str(target_ticket).startswith("partial_close_")):
                fill_lot_size = custom_lot_size or lot_size
                side_info = session.get("sides", {}).get(account_id, {})
                original_side = side_info.get("action", "buy")
                
                # For netting mode, dynamically override original_side based on actual broker exposure
                ea_info = self.dd["ea_account_info"].get(account_id, {})
                if ea_info.get("netting_mode", False):
                    lots_info = ea_info.get("lots_by_instrument", {}).get(pair, {})
                    buy_lots = lots_info.get("buy", 0.0)
                    sell_lots = lots_info.get("sell", 0.0)
                    if buy_lots > sell_lots:
                        original_side = "buy"  # forces close_position to send a SELL
                    elif sell_lots > buy_lots:
                        original_side = "sell" # forces close_position to send a BUY

                logger.info("[%s] Closing synthetic ticket=%s lot_size=%.4f (directed_close_from_side=%s)",
                            account_id, target_ticket, fill_lot_size, original_side)
                self.accounts[account_id].close_position(
                    target_ticket, pair, original_side, fill_lot_size,
                    session_id=session.get("id"), comment=comment,
                    is_rollback=is_rollback
                )
                return

        for fill in fills:
            if fill.get("account") != account_id:
                continue
            ticket = fill.get("ticket")
            if ticket in closed_tickets:
                continue
            
            # If a specific ticket was requested for rollback, skip others
            if target_ticket and str(ticket) != str(target_ticket) and not str(target_ticket).startswith("missing_"):
                continue

            # Found an open position to close.
            # Use custom_lot_size if provided, otherwise the fill's own lot size,
            # falling back to the side/session lot_size if not recorded.
            fill_lot_size = custom_lot_size or fill.get("lots") or fill.get("lot_size") or lot_size
            pos_id = fill.get("pos_id") or str(ticket)
            side_info = session.get("sides", {}).get(account_id, {})
            original_side = side_info.get("action", "buy")
            
            # For netting mode, dynamically override original_side based on actual broker exposure
            ea_info = self.dd["ea_account_info"].get(account_id, {})
            if ea_info.get("netting_mode", False):
                lots_info = ea_info.get("lots_by_instrument", {}).get(pair, {})
                buy_lots = lots_info.get("buy", 0.0)
                sell_lots = lots_info.get("sell", 0.0)
                if buy_lots > sell_lots:
                    original_side = "buy"  # forces close_position to send a SELL
                elif sell_lots > buy_lots:
                    original_side = "sell" # forces close_position to send a BUY

            logger.info("[%s] Closing position ticket=%s lot_size=%.4f (fill lots=%.4f session lots=%.4f custom=%.4f, directed_close_from_side=%s)",
                        account_id, ticket, fill_lot_size,
                        fill.get("lots") or 0, lot_size, custom_lot_size or 0.0, original_side)
            self.accounts[account_id].close_position(
                pos_id, pair, original_side, fill_lot_size,
                session_id=session.get("id"), comment=comment,
                is_rollback=is_rollback
            )
            break
