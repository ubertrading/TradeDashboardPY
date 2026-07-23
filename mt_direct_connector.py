#!/usr/bin/env python3
"""
mt_direct_connector.py — MT4/MT5 Direct API Connector

Connects directly to MetaTrader broker servers using .NET DLLs
(MT4ServerAPI.dll / mt5api.dll) via pythonnet, bypassing the terminal.

Follows the same integration pattern as fix_connector.py:
- Feeds quotes/account data into ea_account_info + ea_heartbeats
- Uses _should_issue_command() for trade decisions
- Reports fills/closes through the standard trade_result endpoint
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime

logger = logging.getLogger("mt_direct")

# ─── .NET interop via pythonnet ─────────────────────────────────────────────
_clr_loaded = False
_mt4_asm = None
_mt5_asm = None
_clr_lock = threading.Lock()  # Global lock: pythonnet is NOT thread-safe for .NET iteration
# Single lock for connection serialization: even though MT4ServerAPI.dll and
# mt5api.dll have separate static buffers, they share the CLR thread pool and
# async infrastructure.  Parallel Connect() calls overwhelm the shared .NET
# thread pool causing CLR corruption (0x80131506).
_mt_connect_lock = threading.Lock()
# Per-platform locks for deferred event subscription only (lighter operations
# that don't stress the shared thread pool).
_mt4_connect_lock = threading.Lock()
_mt5_connect_lock = threading.Lock()
DLL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MT-DLLS")

# ─── Per-platform callback suppression gates ────────────────────────────────
# Default OPEN (set). Cleared during connection init to prevent .NET callbacks
# from dispatching through pythonnet while CLR reflection is in progress.
# Split per-platform: MT4 callbacks don't need suppression during MT5 connects.
_mt4_init_gate = threading.Event()
_mt4_init_gate.set()
_mt5_init_gate = threading.Event()
_mt5_init_gate.set()

# ─── Quote-driven command loop wakeup ────────────────────────────────────
# Signaled by any MT4/MT5 _on_quote callback so the command loop wakes
# immediately on tick arrival instead of blind polling.
_quote_wakeup = threading.Event()

# ─── Heartbeat throttle ─────────────────────────────────────────────────────
# Fully serializes Python→.NET API calls (GetQuote / GetOpenedOrders) across
# all heartbeat threads.  With 25+ accounts, even 2-3 concurrent calls cause
# CLR corruption: the library's internal CalcProfit task runs unsafe memcpy
# on order objects (OrderProfit.UpdateSymbolTick) concurrently with our
# GetOpenedOrders iteration of the same collection.  Using a mutex (sem=1)
# eliminates the race at the cost of slightly slower heartbeat throughput.
import random as _random
_heartbeat_sem = threading.Semaphore(1)

# C# QuoteBuffer shim instance — initialized in _load_clr() if QuoteBuffer.dll exists
_quote_buffer = None


# ─── Active account registry for connection-time callback pause/resume ──────
# Tracks all accounts with active .NET event subscriptions.  Before each new
# connection init, _pause_all_callbacks() physically unsubscribes every
# registered account's .NET event handlers.  After init completes,
# _resume_all_callbacks() re-subscribes them.  This prevents .NET from
# dispatching ANY callbacks through pythonnet during type/assembly loading.
_registered_accounts = []
_registry_lock = threading.Lock()


def _pause_all_callbacks():
    """Unsubscribe .NET event handlers from ALL registered accounts."""
    with _registry_lock:
        for acct in _registered_accounts:
            try:
                acct._unsubscribe_events()
            except Exception:
                pass
    logger.info("Paused .NET callbacks on %d account(s)", len(_registered_accounts))


def _resume_all_callbacks():
    """Re-subscribe .NET event handlers on ALL registered accounts.
    Uses staggered re-subscription (0.5s between each) to prevent a
    thundering herd of .NET thread pool work items that crash the CLR
    when ManualResetEvent objects are created concurrently."""
    with _registry_lock:
        count = len(_registered_accounts)
        for i, acct in enumerate(_registered_accounts):
            try:
                acct._subscribe_events()
            except Exception:
                pass
            # Stagger re-subscription to avoid overwhelming .NET thread pool
            # 3s per account — 0.5s was too aggressive, causing CLR corruption
            # when ManualResetEvent + RoundBidAsk collide on the thread pool
            if i < count - 1:
                time.sleep(3)
    logger.info("Resumed .NET callbacks on %d account(s)", count)


def _register_account(acct):
    with _registry_lock:
        if acct not in _registered_accounts:
            _registered_accounts.append(acct)


def _unregister_account(acct):
    with _registry_lock:
        try:
            _registered_accounts.remove(acct)
        except ValueError:
            pass

# ─── Auto-reconnect constants ───────────────────────────────────────────────
RECONNECT_BASE_DELAY = 5      # initial wait (seconds)
RECONNECT_MAX_DELAY  = 120    # cap (2min — keeps retrying steadily during outages)
RECONNECT_BACKOFF    = 2.0    # multiplier per failed attempt


def _parse_open_time(ot):
    """Robustly parse a .NET DateTime (or its string representation) into a UTC epoch,
    adjusting naive broker time (EET/EEST) by 7 hours to align with New York rollover calculations.
    Returns epoch (int) or None on failure."""
    from datetime import datetime as _dt, timedelta
    try:
        from zoneinfo import ZoneInfo
        NY_TZ = ZoneInfo("America/New_York")
    except ImportError:
        import pytz
        NY_TZ = pytz.timezone("America/New_York")

    # If it's a .NET DateTime object, try extracting Ticks directly
    if not isinstance(ot, str):
        try:
            ticks = getattr(ot, 'Ticks', None)
            if ticks:
                broker_epoch = int((int(ticks) - 621355968000000000) / 10000000)
                dt = _dt.utcfromtimestamp(broker_epoch)
                dt_ny = dt - timedelta(hours=7)
                dt_ny = dt_ny.replace(tzinfo=NY_TZ)
                return int(dt_ny.timestamp())
        except Exception:
            pass

    s = str(ot).strip().replace("T", " ").rstrip("Z")
    if not s:
        return None

    import re
    # Strip fractional seconds like .123456 at the end of the time string
    s = re.sub(r'\.\d+(?=\s*$|\s*[a-zA-Z]+$)', '', s)

    # MT5 APIs sometimes return TimeCreate as an integer (Unix timestamp in broker time)
    if s.isdigit() and len(s) >= 9:
        try:
            broker_epoch = int(s)
            dt = _dt.utcfromtimestamp(broker_epoch)
            dt_ny = dt - timedelta(hours=7)
            dt_ny = dt_ny.replace(tzinfo=NY_TZ)
            return int(dt_ny.timestamp())
        except Exception:
            pass

    # Try multiple date formats
    _FORMATS = [
        '%Y-%m-%d %H:%M:%S',    # ISO: 2026-03-30 01:43:22
        '%Y.%m.%d %H:%M:%S',    # Dot: 2026.03.30 01:43:22
        '%Y/%m/%d %H:%M:%S',    # Slash: 2026/03/30 01:43:22
        '%m/%d/%Y %I:%M:%S %p', # US 12h: 3/30/2026 1:43:22 AM
        '%m/%d/%Y %H:%M:%S',    # US 24h: 03/30/2026 01:43:22
        '%d/%m/%Y %H:%M:%S',    # EU: 30/03/2026 01:43:22
        '%d.%m.%Y %H:%M:%S',    # EU dot: 30.03.2026 01:43:22
    ]
    for fmt in _FORMATS:
        try:
            parsed = _dt.strptime(s, fmt)
            dt_ny = parsed - timedelta(hours=7)
            dt_ny = dt_ny.replace(tzinfo=NY_TZ)
            return int(dt_ny.timestamp())
        except ValueError:
            continue
    logger.warning("_parse_open_time: could not parse %r", s)
    return None


def _load_clr():
    """Load pythonnet and the MT DLLs. Call once at startup."""
    global _clr_loaded, _mt4_asm, _mt5_asm
    if _clr_loaded:
        return True
    try:
        # pythonnet 3.x requires explicit runtime load before import clr
        try:
            from pythonnet import load as _pn_load
            _pn_load("coreclr")  # or "netfx" for .NET Framework
            logger.info("pythonnet 3.x runtime loaded (coreclr)")
        except ImportError:
            pass  # pythonnet 2.x doesn't need this
        except RuntimeError as e:
            if "already" in str(e).lower():
                pass  # Runtime already loaded, that's fine
            else:
                # Try .NET Framework runtime instead
                try:
                    from pythonnet import load as _pn_load2
                    _pn_load2("netfx")
                    logger.info("pythonnet 3.x runtime loaded (netfx)")
                except Exception as e2:
                    logger.warning("pythonnet runtime load warning: %s / %s", e, e2)
        except Exception as e:
            # Try netfx as fallback
            try:
                from pythonnet import load as _pn_load3
                _pn_load3("netfx")
                logger.info("pythonnet 3.x runtime loaded (netfx fallback)")
            except Exception as e2:
                logger.warning("pythonnet runtime load warning: %s / %s", e, e2)

        import clr  # pythonnet
        logger.info("pythonnet clr imported successfully")

        # Add DLL directory to path
        import sys as _sys
        if DLL_DIR not in _sys.path:
            _sys.path.append(DLL_DIR)

        mt4_path = os.path.join(DLL_DIR, "MT4ServerAPI.dll")
        mt5_path = os.path.join(DLL_DIR, "mt5api.dll")

        if os.path.exists(mt4_path):
            try:
                clr.AddReference(mt4_path)
                _mt4_asm = True
                logger.info("Loaded MT4ServerAPI.dll from %s", mt4_path)
            except Exception as e:
                logger.error("Failed to load MT4ServerAPI.dll: %s", e)
        else:
            logger.warning("MT4ServerAPI.dll not found at %s", mt4_path)

        if os.path.exists(mt5_path):
            try:
                clr.AddReference(mt5_path)
                _mt5_asm = True
                logger.info("Loaded mt5api.dll from %s", mt5_path)
            except Exception as e:
                logger.error("Failed to load mt5api.dll: %s", e)
        else:
            logger.warning("mt5api.dll not found at %s", mt5_path)

        # Load QuoteBuffer shim — handles .NET events natively in C#
        # to avoid CLR corruption (0x80131506) from pythonnet's event dispatch
        qb_path = os.path.join(DLL_DIR, "QuoteBuffer.dll")
        if os.path.exists(qb_path):
            try:
                clr.AddReference(qb_path)
                from QuoteBufferLib import QuoteBuffer as _QBClass
                global _quote_buffer
                _quote_buffer = _QBClass()
                logger.info("QuoteBuffer.dll loaded — C# event shim active")
            except Exception as e:
                logger.error("Failed to load QuoteBuffer.dll: %s", e)
        else:
            logger.warning("QuoteBuffer.dll not found at %s — running in polling-only mode", qb_path)

        _clr_loaded = True
        return True
    except ImportError as e:
        logger.error("pythonnet not installed: %s. Install with: pip install pythonnet", e)
        return False
    except Exception as e:
        logger.error("Failed to load MT DLLs: %s (%s)", e, type(e).__name__)
        return False


# ─── Normalize ticket (same as dashboard) ───────────────────────────────────
def _normalize_ticket(t):
    """Handle 32-bit overflow for MT4 tickets."""
    try:
        v = int(t)
        if v < 0:
            v = v + (1 << 32)
        return v
    except (ValueError, TypeError):
        return t


# ─── MT4 Direct Account ────────────────────────────────────────────────────
class MT4DirectAccount:
    """
    Wraps TradingAPI.MT4Server.QuoteClient + OrderClient.
    Provides connect, quotes, positions, and trade execution.
    """

    def __init__(self, account_id, config, dashboard_data):
        self.account_id = str(account_id)
        self.config = config
        self.dd = dashboard_data
        self.label = config.get("label", f"MT4-{account_id}")
        self.conn_type = "mt4_direct"
        self.lot_divisor = config.get("lot_divisor", 1.0)

        self._client = None  # QuoteClient instance
        self._order_client = None  # OrderClient instance
        self._connected = False
        self._running = False
        self._lock = threading.Lock()
        self._quote_thread = None
        self._last_error = None
        self._events_subscribed = False
        self._order_update_pending = threading.Event()  # Flag for deferred order refresh

        # Auto-reconnect state
        self._reconnect_delay = RECONNECT_BASE_DELAY
        self._reconnect_attempt = 0

        # Per-symbol quote cache — populated by _push_account_info (CLR-safe thread),
        # read by get_symbol_info (Flask thread, no CLR calls)
        self._symbol_cache = {}  # {"USDCHF": {"bid": ..., "ask": ..., "spread": ...}}

    @property
    def connected(self):
        return self._connected

    def _subscribe_events(self):
        """Subscribe to .NET events via C# QuoteBuffer shim.
        All handlers run as pure .NET delegates — no pythonnet dispatch."""
        if self._events_subscribed or not self._client or not self._connected:
            return
        global _quote_buffer
        if _quote_buffer is not None:
            try:
                target = (self.dd.get("ea_account_info", {}).get(self.account_id, {}).get("symbol", "") or "")
                _quote_buffer.SubscribeMT4(self._client, self.account_id, target)
                self._events_subscribed = True
                logger.info("[%s] Events subscribed via C# QuoteBuffer (symbol=%s)", self.account_id, target)
            except Exception as e:
                logger.warning("[%s] Failed to subscribe via QuoteBuffer: %s", self.account_id, e)
        else:
            logger.info("[%s] QuoteBuffer not available — polling-only mode", self.account_id)

    def _unsubscribe_events(self):
        """Unsubscribe .NET event handlers. Safe to call multiple times."""
        if not self._events_subscribed or not self._client:
            return
        try:
            self._client.OnQuote -= self._on_quote
        except Exception:
            pass
        try:
            self._client.OnOrderUpdate -= self._on_order_update
        except Exception:
            pass
        try:
            self._client.OnDisconnect -= self._on_disconnect
        except Exception:
            pass
        self._events_subscribed = False

    def _deferred_subscribe(self):
        """Subscribe .NET events via C# QuoteBuffer.
        With QuoteBuffer handling events natively in C#, no deferral needed —
        subscribe immediately. The C# handlers are thread-safe and don't
        involve pythonnet dispatch."""
        if self._connected and self._running:
            self._subscribe_events()
            logger.info("[%s] Events subscribed (immediate via QuoteBuffer)", self.account_id)

    def start(self):
        """Connect to the MT4 server."""
        self._last_error = None
        # Mark running BEFORE attempting connect so the heartbeat thread
        # always starts — it handles auto-reconnect on failure.
        self._running = True
        if not _load_clr():
            self._last_error = "pythonnet CLR failed to initialize — check server logs for details. Ensure .NET runtime is installed."
            logger.error("[%s] Cannot start — %s", self.account_id, self._last_error)
            return False

        try:
            from TradingAPI.MT4Server import QuoteClient, Op, PlacedType
            import System
            import hashlib
            import subprocess

            login = int(self.config["login"])
            password = str(self.config["password"])
            server = str(self.config["server"])
            port = int(self.config.get("port", 443))

            # Ensure LoginId.dll is findable: add DLL_DIR to system PATH
            if DLL_DIR not in os.environ.get("PATH", ""):
                os.environ["PATH"] = DLL_DIR + os.pathsep + os.environ.get("PATH", "")

            # Step 1: Use 4-arg constructor to initialize internal state
            # (will fail to connect because HardwareId can't be generated from
            # the dead loginid-mt4.mtapi.io service, but internal state is set up)
            logger.info("[%s] Connecting to MT4 server %s:%d ...", self.account_id, server, port)
            # Serialize connection init — .NET MT API has shared static buffers
            # Gate suppresses heartbeat + callback processing on the Python side
            # during connect. We do NOT pause/resume other accounts' .NET event
            # handlers — doing so corrupts their IO completion ports.
            with _mt_connect_lock:
                _mt4_init_gate.clear()
                logger.info("[%s] Gate closed for connection init", self.account_id)

                # Brief settle — QuoteBuffer handles events in C# so no
                # callback collision risk, but serialized connect still needed
                # for MT4 API's shared static buffers.
                time.sleep(0.5)

                self._client = QuoteClient(login, password, server, port)

                if self._client is None:
                    self._last_error = "QuoteClient constructor returned None — .NET resources may not have been fully released. Try again."
                    logger.error("[%s] %s", self.account_id, self._last_error)
                    _mt4_init_gate.set()
                    return False

                if not self._client.Connected:
                    # Step 2: Generate HardwareId unique per account
                    # Mix machine UUID + account login so each instance gets its own ID
                    try:
                        r = subprocess.run(
                            ['powershell', '-c', '(Get-CimInstance Win32_ComputerSystemProduct).UUID'],
                            capture_output=True, text=True, timeout=5)
                        machine_uuid = r.stdout.strip()
                        unique_seed = f"{machine_uuid}:{self.account_id}"
                        hwid_bytes = hashlib.md5(unique_seed.encode()).digest()
                        arr = System.Array.CreateInstance(System.Byte, len(hwid_bytes))
                        for i, b in enumerate(hwid_bytes):
                            arr[i] = System.Byte(b)
                        self._client.HardwareId = arr
                        logger.info("[%s] Generated unique HardwareId (machine+account)", self.account_id)
                    except Exception as e:
                        logger.warning("[%s] Could not generate HardwareId: %s", self.account_id, e)

                    # Step 3: Re-set credentials and reconnect with valid HardwareId
                    self._client.User = login
                    self._client.Password = password
                    self._client.Host = server
                    self._client.Port = port
                    try:
                        self._client.Disconnect()
                        time.sleep(0.5)
                    except Exception:
                        pass
                    self._client.Connect()

                # Try to get OrderClient — multiple approaches
                self._order_client = self._client.OrderClient
                if not self._order_client:
                    # Try OrderClientSafe
                    self._order_client = getattr(self._client, 'OrderClientSafe', None)
                if not self._order_client:
                    # Try creating OrderClient from the namespace
                    try:
                        from TradingAPI.MT4Server import OrderClient as MT4OrderClient
                        self._order_client = MT4OrderClient(self._client)
                        logger.info("[%s] Created OrderClient from constructor", self.account_id)
                    except Exception as oe:
                        logger.warning("[%s] OrderClient constructor failed: %s", self.account_id, oe)
                if not self._order_client:
                    # Log all properties containing 'Order' or 'Client'
                    try:
                        order_attrs = [a for a in dir(self._client) if 'Order' in a or 'Client' in a or 'Trade' in a]
                        logger.info("[%s] Available order-related attrs: %s", self.account_id, order_attrs)
                    except Exception:
                        pass
                self._connected = self._client.Connected
                self._running = True
                logger.info("[%s] OrderClient=%s connected=%s", self.account_id, 
                            type(self._order_client).__name__ if self._order_client else None,
                            self._connected)

                if self._connected:
                    logger.info("[%s] Connected! Account: %s Balance: %.2f Equity: %.2f Leverage: 1:%d",
                                self.account_id, self._client.AccountName,
                                self._client.AccountBalance, self._client.AccountEquity,
                                self._client.AccountLeverage)

                    # Initial data push (no events yet — safe)
                    self._push_account_info()
                    self._push_positions()

                    # Register for callback management, then subscribe events
                    _register_account(self)
                else:
                    # Try to extract more details from the client object
                    err_detail = ""
                    try:
                        for attr in dir(self._client):
                            if 'error' in attr.lower() or 'message' in attr.lower() or 'reason' in attr.lower():
                                try:
                                    val = getattr(self._client, attr)
                                    if val is not None and not callable(val):
                                        err_detail += f" {attr}={val}"
                                except Exception:
                                    pass
                    except Exception:
                        pass
                    self._last_error = f"Connection failed (Connected=False){err_detail} — check server/login/password"
                    logger.error("[%s] %s", self.account_id, self._last_error)
                    # Log all available properties for debugging
                    try:
                        diag_attrs = ['ServerName', 'ServerBuild', 'HardwareId', 'SoftId',
                                      'LoginIdExPath', 'LoginIdTimeoutMs', 'IsInvestor',
                                      'User', 'AccountName', 'ServerTime', 'ConnectTime',
                                      'Host', 'Port', 'Id']
                        for a in diag_attrs:
                            try:
                                val = getattr(self._client, a, '???')
                                logger.info("[%s] %s = %s", self.account_id, a, val)
                            except Exception:
                                pass
                    except Exception:
                        pass

                # Brief settle — QuoteBuffer eliminates callback collision risk
                time.sleep(0.5)

                # DO NOT subscribe events inside the lock — deferring to a
                # background thread prevents callback traffic from colliding
                # with the next account's ConnectThread.LoadSymbols on the
                # shared .NET thread pool (deterministic crash prevention).

                # Open gate — settle complete, safe for heartbeats
                _mt4_init_gate.set()
                logger.info("[%s] Gate opened (heartbeats may resume)", self.account_id)

            # Subscribe events immediately (via C# QuoteBuffer — no deferral needed)
            if self._connected:
                # Clear any stale disconnect flag from the OLD client's OnDisconnect
                # event — otherwise the heartbeat loop picks it up immediately and
                # triggers another reconnect (infinite loop).
                if _quote_buffer is not None:
                    try:
                        _quote_buffer.CheckAndClearDisconnect(self.account_id)
                    except Exception:
                        pass
                self._deferred_subscribe()

            # Always ensure heartbeat thread is running (handles reconnect on failure)
            if self._quote_thread is None or not self._quote_thread.is_alive():
                self._quote_thread = threading.Thread(
                    target=self._heartbeat_loop, daemon=True,
                    name=f"MT4Direct-{self.account_id}")
                self._quote_thread.start()

            return self._connected
        except Exception as e:
            self._last_error = str(e)
            logger.error("[%s] Connection error: %s", self.account_id, e)
            self._connected = False
            # Ensure heartbeat thread runs for auto-reconnect even after exceptions
            self._running = True
            if self._quote_thread is None or not self._quote_thread.is_alive():
                self._quote_thread = threading.Thread(
                    target=self._heartbeat_loop, daemon=True,
                    name=f"MT4Direct-{self.account_id}")
                self._quote_thread.start()
            # Make sure gate is reopened even on error
            try:
                _mt4_init_gate.set()
            except Exception:
                pass
            return False

    def stop(self):
        """Disconnect from the MT4 server."""
        self._running = False
        self._connected = False
        _unregister_account(self)
        # Wait for heartbeat thread to exit before tearing down .NET objects
        if self._quote_thread and self._quote_thread.is_alive():
            self._quote_thread.join(timeout=5)
        self._quote_thread = None
        # Properly tear down .NET client to prevent CLR corruption on reconnect
        self._cleanup_for_reconnect()
        # Brief settle delay to let .NET GC release resources before any reconnect
        time.sleep(1)
        logger.info("[%s] Disconnected", self.account_id)

    def _heartbeat_loop(self):
        """Safety-net sync: re-push account info and positions every 30s.
        Persists through disconnections — triggers reconnect with backoff."""
        # Brief random stagger (0-2s) to desynchronize heartbeat threads
        time.sleep(_random.uniform(0, 2))
        while self._running:
            try:
                if self._connected:
                    # Skip .NET calls if another account is mid-Connect()
                    # to prevent CLR corruption (0x80131506)
                    if not _mt4_init_gate.is_set():
                        time.sleep(2)
                        continue
                    # Check if an order update callback set the pending flag
                    # — do an immediate refresh so position data stays current.
                    # This replaces doing .NET iteration on the callback thread
                    # which crashes tp_iternext (CLR 0x80131506).
                    if self._order_update_pending.is_set():
                        self._order_update_pending.clear()
                        _heartbeat_sem.acquire()
                        try:
                            self._push_account_info()
                            self._push_positions()
                        finally:
                            _heartbeat_sem.release()
                        continue  # re-check immediately in case more updates queued
                    # Throttle concurrent .NET API calls across all accounts
                    _heartbeat_sem.acquire()
                    try:
                        self._push_account_info()
                        self._push_positions()
                    finally:
                        _heartbeat_sem.release()
                    # Check if connection silently died
                    if not self._check_connection():
                        continue  # will enter reconnect on next iteration
                    # Sleep in short increments — compensates for disabled
                    # .NET events by polling every ~3s instead of 30s
                    for _ in range(2):
                        if self._order_update_pending.is_set() or not self._running:
                            break
                        # Check C# QuoteBuffer for order updates + disconnects
                        if _quote_buffer is not None:
                            try:
                                if _quote_buffer.CheckAndClearOrderUpdate(self.account_id):
                                    self._order_update_pending.set()
                                    break
                                if _quote_buffer.CheckAndClearDisconnect(self.account_id):
                                    logger.warning("[%s] Disconnect detected via QuoteBuffer", self.account_id)
                                    self._connected = False
                                    break
                            except Exception:
                                pass
                        time.sleep(2)
                else:
                    # Disconnected — attempt reconnect with backoff
                    self._attempt_reconnect()
            except Exception as e:
                logger.error("[%s] Heartbeat error: %s", self.account_id, e)
                if not self._check_connection():
                    continue
                time.sleep(30)

    def _check_connection(self):
        """Check if still connected; if not, mark as disconnected."""
        try:
            if self._client and self._client.Connected:
                return True
        except Exception:
            pass
        if self._connected:
            logger.warning("[%s] Connection lost (detected by heartbeat)", self.account_id)
            self._connected = False
        return False

    def _attempt_reconnect(self):
        """Try to reconnect with exponential backoff."""
        if not self._running:
            return
        self._reconnect_attempt += 1
        delay = self._reconnect_delay
        logger.info("[%s] Attempting reconnect (attempt #%d, waiting %ds)...",
                    self.account_id, self._reconnect_attempt, delay)
        # Sleep in small increments so we can exit quickly if stopped
        for _ in range(int(delay * 2)):
            if not self._running:
                return
            time.sleep(0.5)
        if not self._running:
            return
        try:
            self._cleanup_for_reconnect()
            # start() already serializes via _mt_connect_lock internally,
            # so we do NOT acquire the lock here (would deadlock — Lock is non-reentrant).
            if not self._running:
                return
            ok = self.start()
            if ok:
                # Belt-and-suspenders: clear disconnect flag again in case
                # the old client's OnDisconnect fired during start().
                if _quote_buffer is not None:
                    try:
                        _quote_buffer.CheckAndClearDisconnect(self.account_id)
                    except Exception:
                        pass
                logger.info("[%s] Reconnected successfully after %d attempt(s)",
                            self.account_id, self._reconnect_attempt)
                self._reconnect_delay = RECONNECT_BASE_DELAY
                self._reconnect_attempt = 0
            else:
                logger.warning("[%s] Reconnect failed: %s", self.account_id, self._last_error)
                self._reconnect_delay = min(delay * RECONNECT_BACKOFF, RECONNECT_MAX_DELAY)
        except Exception as e:
            logger.error("[%s] Reconnect error: %s", self.account_id, e)
            self._reconnect_delay = min(delay * RECONNECT_BACKOFF, RECONNECT_MAX_DELAY)

    def _cleanup_for_reconnect(self):
        """Tear down stale .NET client objects before reconnecting."""
        self._unsubscribe_events()
        try:
            if self._client:
                try:
                    self._client.Disconnect()
                except Exception:
                    pass
                try:
                    if hasattr(self._client, 'Dispose'):
                        self._client.Dispose()
                except Exception:
                    pass
        except Exception:
            pass
        self._client = None
        self._order_client = None

    def _push_account_info(self):
        """Push account data into ea_account_info (same shape as EA polls)."""
        if not self._client:
            return
        # Get or create info dict — store immediately so partial updates aren't lost
        info = self.dd["ea_account_info"].get(self.account_id, {})
        self.dd["ea_account_info"][self.account_id] = info
        info["conn_type"] = "mt5_direct" if self.config.get("type") == "mt5" else "mt4_direct"

        # Debug: log available account properties (once) to find MT5 equivalents
        if not getattr(self, '_acct_props_logged', False):
            try:
                acct_attrs = {a: str(getattr(self._client, a, '?'))[:40] for a in dir(self._client)
                              if not a.startswith('_') and ('alance' in a.lower() or 'quity' in a.lower()
                                  or 'argin' in a.lower() or 'everage' in a.lower() or 'account' in a.lower())}
                logger.info("[%s] Account attrs: %s", self.account_id, acct_attrs)
                self._acct_props_logged = True
            except Exception:
                pass

        # Account properties — each wrapped individually so one failure doesn't kill all
        for prop, attr in [("balance", "AccountBalance"), ("equity", "AccountEquity"),
                           ("margin", "AccountMargin"), ("free_margin", "AccountFreeMargin"),
                           ("leverage", "AccountLeverage"), ("credit", "AccountCredit")]:
            try:
                val = getattr(self._client, attr, None)
                if val is not None:
                    info[prop] = int(val) if prop == "leverage" else float(val)
            except Exception:
                pass

        info["last_update"] = time.time()

        # Push symbol quotes — prefer C# QuoteBuffer (real-time),
        # fall back to GetQuote / SymbolsInfo polling
        target_symbol = info.get("symbol", "")
        if target_symbol:
            quote_ok = False
            # Method 0: C# QuoteBuffer (real-time, zero .NET calls from Python)
            if _quote_buffer is not None:
                try:
                    qb_quote = _quote_buffer.GetQuote(self.account_id)
                    if qb_quote is not None:
                        info["bid"] = float(qb_quote.Bid)
                        info["ask"] = float(qb_quote.Ask)
                        info["quote_symbol"] = target_symbol.upper()
                        if info["bid"] > 0 and info["ask"] > 0:
                            pair = target_symbol.upper()
                            pip_mult = 1000 if "JPY" in pair else 100000
                            info["spread"] = round((info["ask"] - info["bid"]) * pip_mult, 1)
                        quote_ok = True
                except Exception:
                    pass
            # Method 1: GetQuote (direct, reliable)
            if not quote_ok:
                try:
                    if hasattr(self._client, 'GetQuote'):
                        quote = self._client.GetQuote(target_symbol)
                        if quote:
                            info["bid"] = float(quote.Bid)
                            info["ask"] = float(quote.Ask)
                            info["quote_symbol"] = target_symbol.upper()
                            if info["bid"] > 0 and info["ask"] > 0:
                                pair = target_symbol.upper()
                                pip_mult = 1000 if "JPY" in pair else 100000
                                info["spread"] = round((info["ask"] - info["bid"]) * pip_mult, 1)
                            quote_ok = True
                except Exception:
                    pass
            # Method 2: SymbolsInfo iteration (fallback for quote ONLY if needed)
            if not quote_ok:
                try:
                    with _clr_lock:
                        symbols_info = self._client.SymbolsInfo
                        if symbols_info:
                            for sym in symbols_info:
                                try:
                                    sym_name = getattr(sym, 'Name', None) or getattr(sym, 'Symbol', None) or str(sym)
                                    sn_upper = str(sym_name).upper()
                                    if sn_upper == target_symbol.upper():
                                        bid_val = float(sym.Bid)
                                        ask_val = float(sym.Ask)
                                        info["bid"] = bid_val
                                        info["ask"] = ask_val
                                        info["quote_symbol"] = sn_upper
                                        if bid_val > 0 and ask_val > 0:
                                            pair = target_symbol.upper()
                                            pip_mult = 1000 if "JPY" in pair else 100000
                                            info["spread"] = round((ask_val - bid_val) * pip_mult, 1)
                                        break
                                except Exception:
                                    continue
                except Exception:
                    pass

        # Always cache ALL symbols from SymbolsInfo for swap data
        # (runs every poll, NOT gated by quote_ok)
        try:
            with _clr_lock:
                symbols_info = self._client.SymbolsInfo
                # One-time SymbolsInfo diagnostic
                if not getattr(self, '_syminfo_logged', False):
                    try:
                        si_count = 0
                        sample_props = None
                        if symbols_info:
                            for si_sym in symbols_info:
                                si_count += 1
                                if sample_props is None:
                                    sample_props = [a for a in dir(si_sym) if not a.startswith('_')]
                                if si_count >= 3:
                                    break
                        logger.info("[%s] SymbolsInfo: count=%s, sample_props=%s",
                                    self.account_id, si_count if symbols_info else 'None', sample_props)
                        self._syminfo_logged = True
                    except Exception as si_err:
                        logger.info("[%s] SymbolsInfo diagnostic error: %s", self.account_id, si_err)
                        self._syminfo_logged = True
                if symbols_info:
                    new_cache = {}
                    for sym in symbols_info:
                        try:
                            # SymbolInfo ToString() returns garbage like '0 0 MARKET'
                            # The real name is in Ex sub-object or Currency
                            sym_name = None
                            ex = getattr(sym, 'Ex', None)
                            if ex:
                                # One-time: log Ex properties
                                if not getattr(self, '_ex_props_logged', False):
                                    try:
                                        ex_props = [a for a in dir(ex) if not a.startswith('_')]
                                        # Log actual VALUES of name-related properties
                                        name_vals = {}
                                        for prop in ['symbol', 'SymbolAsString', 'description', 'currency', 'margin_currency', 'source']:
                                            try:
                                                name_vals[prop] = str(getattr(ex, prop, '?'))
                                            except Exception:
                                                pass
                                        try:
                                            name_vals['get_SymbolAsString()'] = str(ex.get_SymbolAsString())
                                        except Exception:
                                            pass
                                        logger.info("[%s] SymbolInfo.Ex name values: %s", self.account_id, name_vals)
                                    except Exception:
                                        pass
                                    self._ex_props_logged = True
                                sym_name = (
                                    getattr(ex, 'SymbolAsString', None) or
                                    getattr(ex, 'symbol', None) or
                                    getattr(ex, 'Symbol', None) or
                                    getattr(ex, 'Name', None) or
                                    getattr(ex, 'description', None)
                                )
                            if not sym_name:
                                sym_name = getattr(sym, 'Currency', None)
                            if not sym_name:
                                try:
                                    sym_name = sym.ToString()
                                except Exception:
                                    sym_name = str(sym)
                            sym_name = str(sym_name).strip().upper()
                            if not sym_name or ' ' in sym_name:
                                continue  # Skip garbage names like '0 0 MARKET'
                            entry = {
                                "swap_long": float(getattr(sym, 'SwapLong', 0)),
                                "swap_short": float(getattr(sym, 'SwapShort', 0)),
                            }
                            # Bid/Ask may not exist on SymbolInfo — only add if available
                            try:
                                bid_val = float(sym.Bid)
                                ask_val = float(sym.Ask)
                                if bid_val > 0 and ask_val > 0:
                                    pip_mult = 1000 if "JPY" in sym_name else 100000
                                    spd = round((ask_val - bid_val) * pip_mult, 1)
                                    entry.update({"bid": bid_val, "ask": ask_val, "spread": spd})
                            except (AttributeError, TypeError):
                                pass
                            new_cache[sym_name] = entry
                        except Exception:
                            continue
                    if new_cache:
                        self._symbol_cache = new_cache
                        if not getattr(self, '_cache_sample_logged', False):
                            sample = {k: v for i, (k, v) in enumerate(new_cache.items()) if i < 3}
                            logger.info("[%s] _symbol_cache populated: %d symbols, sample: %s",
                                        self.account_id, len(new_cache), sample)
                            self._cache_sample_logged = True
        except Exception as e:
            logger.warning("[%s] Error in MT4 _push_account_info: %s", self.account_id, e, exc_info=True)


        self.dd["ea_heartbeats"][self.account_id] = time.time()

    def _push_positions(self):
        """Push open position tickets into ea_account_info."""
        if not self._client:
            return
        try:
            info = self.dd["ea_account_info"].get(self.account_id, {})
            # Get open orders from the client
            orders = self._get_open_orders()
            # Filter to only include active market positions (exclude pending limit/stop orders)
            active_types = ('buy', 'sell', '0', '1', 'op_buy', 'op_sell', 'position_type_buy', 'position_type_sell')
            
            raw_types = [str(o.get('Type', '')) for o in orders]
            logger.warning("[%s] MT4 _push_positions raw_types: %s", self.account_id, raw_types)
            
            tickets = [o['Ticket'] for o in orders if str(o.get('Type', '')).lower() in active_types]
            logger.warning("[%s] MT4 _push_positions filtered %d orders down to %d tickets", self.account_id, len(orders), len(tickets))
            
            info["open_tickets"] = tickets
            info["positions"] = len(tickets)
            # Aggregate PnL, swap, and lots for Accounts tab display
            # Use equity - balance as PnL (always in deposit currency,
            # matching what the terminal shows).  Summing individual position
            # Profit fields is unreliable: the .NET API can return them in
            # the quote currency (e.g. CHF for USDCHF on a EUR account).
            bal = info.get("balance")
            eq  = info.get("equity")
            if bal is not None and eq is not None and eq > 0 and bal > 0:
                # equity - balance matches what the MT4 terminal displays as floating P&L
                # (already includes swap). Do NOT subtract credit — the terminal does not.
                info["total_pnl"] = round(eq - bal, 2)
            else:
                # Fallback: try AccountProfit, then sum of position Profits
                try:
                    ap = getattr(self._client, 'AccountProfit', None)
                    info["total_pnl"] = float(ap) if ap is not None else sum(o.get('Profit', 0) for o in orders)
                except Exception:
                    info["total_pnl"] = sum(o.get('Profit', 0) for o in orders)
            info["total_swap"] = sum(o.get('Swap', 0) for o in orders)
            # Signed lots: buy = positive, sell = negative
            _buy_types = ('buy', '0', 'op_buy')
            info["total_lots"] = round(
                sum(o.get('Lots', 0) if str(o.get('Type', '')).lower() in _buy_types
                    else -o.get('Lots', 0)
                    for o in orders), 2)
            # Per-instrument lots breakdown
            _lbi = {}
            for o in orders:
                sym = o.get('Symbol', 'Unknown')
                lots = o.get('Lots', 0)
                if sym not in _lbi:
                    _lbi[sym] = {"buy": 0, "sell": 0}
                if str(o.get('Type', '')).lower() in _buy_types:
                    _lbi[sym]["buy"] = round(_lbi[sym]["buy"] + lots, 2)
                else:
                    _lbi[sym]["sell"] = round(_lbi[sym]["sell"] + lots, 2)
            info["lots_by_instrument"] = _lbi

            # Per-instrument swap breakdown
            _sbi = {}
            for o in orders:
                sym = o.get('Symbol', 'Unknown')
                swap = o.get('Swap', 0.0)
                _sbi[sym] = round(_sbi.get(sym, 0.0) + swap, 2)
            info["swap_by_instrument"] = _sbi
            # Store position details for cycle age tracking
            pos_details = []
            for o in orders:
                oe = None
                ot = o.get('OpenTime', '')
                if ot:
                    oe = _parse_open_time(ot)
                pos_details.append({
                    "ticket": o['Ticket'],
                    "symbol": o.get('Symbol', ''),
                    "comment": o.get('Comment', ''),
                    "open_epoch": oe,
                })
            info["position_details"] = pos_details
            info["last_update"] = time.time()
            self.dd["ea_account_info"][self.account_id] = info
        except Exception as e:
            logger.error("[%s] Push positions error: %s", self.account_id, e)

    def _get_open_orders(self):
        """Get list of open orders from QuoteClient as plain Python dicts.
        All .NET attribute access happens inside _clr_lock to prevent
        concurrent pythonnet CLR corruption."""
        if not self._client:
            return []
        try:
            if hasattr(self._client, 'GetOpenedOrders'):
                with _clr_lock:
                    raw_orders = list(self._client.GetOpenedOrders())
                    # Materialize all .NET attributes into pure Python dicts
                    result = []
                    for o in raw_orders:
                        result.append({
                            'Ticket': _normalize_ticket(getattr(o, 'Ticket', 0)),
                            'Symbol': str(getattr(o, 'Symbol', '')),
                            'Type': str(getattr(o, 'Type', '')),
                            'Lots': float(getattr(o, 'Lots', 0)) / self.lot_divisor,
                            'Comment': str(getattr(o, 'Comment', '')),
                            'OpenPrice': float(getattr(o, 'OpenPrice', 0)),
                            'OpenTimeRaw': getattr(o, 'OpenTime', None),
                            'OpenTime': str(getattr(o, 'OpenTime', '')),
                            'Profit': float(getattr(o, 'Profit', 0)),
                            'Swap': float(getattr(o, 'Swap', 0)),
                        })
                    return result
            return []
        except Exception as e:
            logger.error("[%s] Get open orders error: %s", self.account_id, e)
            return []

    def get_positions_for_import(self, pair_filter="", comment_filter=""):
        """Get open positions in import-compatible format."""
        positions = []
        try:
            orders = self._get_open_orders()
            if orders and not getattr(self, '_import_attrs_logged', False):
                self._import_attrs_logged = True
                logger.info("[%s] MT4 order keys: %s", self.account_id, list(orders[0].keys()))
            for o in orders:
                symbol = o['Symbol'].upper()
                if pair_filter and not (symbol.startswith(pair_filter.upper()) or pair_filter.upper().startswith(symbol)):
                    continue
                comment = o['Comment']
                if comment_filter:
                    comment_parts = [c.strip() for c in comment_filter.split(",") if c.strip()]
                    match_blank = any(cp.lower() == "<blank>" for cp in comment_parts)
                    parts = [cp for cp in comment_parts if cp.lower() != "<blank>"]
                    if not ((match_blank and not comment.strip()) or any(cp in comment for cp in parts)):
                        continue
                side = "buy" if o['Type'].lower() in ('buy', '0', 'op_buy') else "sell"
                oe = _parse_open_time(o.get('OpenTimeRaw') or o.get('OpenTime')) if (o.get('OpenTimeRaw') or o.get('OpenTime')) else None
                positions.append({
                    "ticket": o['Ticket'],
                    "symbol": symbol,
                    "lots": o['Lots'],
                    "side": side,
                    "comment": comment,
                    "open_price": o['OpenPrice'],
                    "open_time": o['OpenTime'],
                    "open_epoch": oe,
                })
            logger.info("[%s] Import: found %d positions (pair=%s comment=%s)",
                        self.account_id, len(positions), pair_filter, comment_filter)
            if positions:
                sample = positions[0]
                logger.info("[%s] Import sample pos: ticket=%s lots=%s side=%s open_time=%r open_epoch=%s",
                            self.account_id, sample.get('ticket'), sample.get('lots'), sample.get('side'), sample.get('open_time'), sample.get('open_epoch'))
        except Exception as e:
            logger.error("[%s] get_positions_for_import error: %s", self.account_id, e)
        return positions

    def get_deal_history(self, from_ts, to_ts, fee_keywords=None, **kwargs):
        """Retrieve closed deal history from the MT4 server and compute PnL totals.

        Uses QuoteClient.DownloadOrderHistory(from, to) to fetch closed orders
        directly from the broker, bypassing the need for an EA.

        Args:
            from_ts: Start timestamp (Unix epoch, UTC).
            to_ts:   End timestamp (Unix epoch, UTC).
            fee_keywords: List of strings to match against order Comment
                          for fee identification (e.g. ["Holding Fee"]).

        Returns:
            dict with keys: pnl (float), swap (float), fees (float), deal_count (int),
            or None on failure.
        """
        if not self._connected or not self._client:
            logger.warning("[%s] get_deal_history: not connected", self.account_id)
            return None
        if fee_keywords is None:
            fee_keywords = []
        try:
            import System
            from datetime import datetime as _dt, timezone as _tz

            # Convert Unix timestamps to .NET DateTime (server time).
            # Pad ±3 hours as recommended by mtapi docs — servers may not
            # interpret the range precisely.
            from_dt = _dt.fromtimestamp(from_ts, tz=_tz.utc)
            to_dt = _dt.fromtimestamp(to_ts, tz=_tz.utc)
            pad_h = 3
            net_from = System.DateTime(from_dt.year, from_dt.month, from_dt.day,
                                       from_dt.hour, from_dt.minute, from_dt.second).AddHours(-pad_h)
            net_to = System.DateTime(to_dt.year, to_dt.month, to_dt.day,
                                     to_dt.hour, to_dt.minute, to_dt.second).AddHours(pad_h)

            logger.info("[%s] Downloading MT4 order history %s → %s (padded ±%dh)",
                        self.account_id, from_dt.strftime("%Y-%m-%d"), to_dt.strftime("%Y-%m-%d"), pad_h)

            # Serialize .NET access — prevent CLR corruption
            _heartbeat_sem.acquire()
            try:
                with _clr_lock:
                    raw_orders = list(self._client.DownloadOrderHistory(net_from, net_to))
            finally:
                _heartbeat_sem.release()

            # Materialize into Python and compute totals
            total_pnl = 0.0
            total_swap = 0.0
            total_fees = 0.0
            deal_count = 0
            by_symbol = {}  # { "EURUSD": {"pnl": 0.0, "lots": 0.0} }
            _lot_diag = None  # first-order lots attribute dump, routed back via app.logger

            # Uppercase fee keywords for case-insensitive matching
            fee_kw_upper = [kw.upper() for kw in fee_keywords if kw]

            for o in raw_orders:
                try:
                    with _clr_lock:
                        ticket = _normalize_ticket(getattr(o, 'Ticket', 0))
                        profit = float(getattr(o, 'Profit', 0))
                        swap = float(getattr(o, 'Swap', 0))
                        commission = float(getattr(o, 'Commission', 0) or 0)
                        taxes = float(getattr(o, 'Taxes', 0) or 0)
                        fee = float(getattr(o, 'Fee', 0) or 0)
                        comment = str(getattr(o, 'Comment', ''))
                        otype = str(getattr(o, 'Type', '')).lower()
                        close_time_raw = getattr(o, 'CloseTime', None)
                        sym = str(getattr(o, 'Symbol', '') or '').upper().strip()
                        # Try every common attribute name for lot size on closed MT4 orders.
                        lots = float(getattr(o, 'Lots', getattr(o, 'Volume', getattr(o, 'CloseVolume', getattr(o, 'OpenVolume', 0)))) or 0) / self.lot_divisor
                        
                        if lots == 0 and otype in ('buy', 'sell', '0', '1', 'op_buy', 'op_sell'):
                            try:
                                diag = {'Lots': str(getattr(o, 'Lots', '__N/A')),
                                        'Volume': str(getattr(o, 'Volume', '__N/A')),
                                        'CloseVolume': str(getattr(o, 'CloseVolume', '__N/A')),
                                        'OpenVolume': str(getattr(o, 'OpenVolume', '__N/A')),
                                        'Symbol': str(getattr(o, 'Symbol', '?')),
                                        'Profit': str(getattr(o, 'Profit', '?')),
                                        'Type': str(getattr(o, 'Type', '?')),
                                        '__all__': ','.join(a for a in dir(o) if not a.startswith('_'))[:300],
                                        }
                                _lot_diag = diag
                            except Exception:
                                _lot_diag = {'error': 'diag_failed'}

                    # Filter by close time within exact [from_ts, to_ts] range
                    # (we padded the download range, so filter precisely here)
                    if close_time_raw:
                        close_epoch = _parse_open_time(close_time_raw)
                        if close_epoch and (close_epoch < from_ts or close_epoch > to_ts):
                            continue  # Outside requested range

                    # Identify type of operation
                    is_trade = otype in ('buy', 'sell', '0', '1', 'op_buy', 'op_sell')
                    is_balance = otype in ('balance', '6', 'op_balance', 'credit', '7', 'op_credit')

                    if is_trade:
                        total_pnl += profit
                        total_swap += swap
                        total_fees += commission + taxes + fee
                        deal_count += 1
                        
                        # Per-symbol accumulation
                        if sym:
                            entry = by_symbol.setdefault(sym, {"pnl": 0.0, "lots": 0.0})
                            entry["pnl"] += profit
                            entry["lots"] += lots
                    elif not is_balance:
                        # Non-trade deal (e.g. charge, storage fee)
                        total_fees += profit + commission + taxes + fee
                        deal_count += 1

                except Exception as e:
                    logger.warning("[%s] Error processing history order: %s", self.account_id, e)
                    continue

            # Build per-symbol breakdown: hedge_lots = raw_lots / 2 (1 buy + 1 sell = 1 hedge lot)
            by_symbol_final = {}
            for sym, v in by_symbol.items():
                hedge_lots = round(v["lots"] / 2.0, 2)
                by_symbol_final[sym] = {
                    "pnl": round(v["pnl"], 2),
                    "hedge_lots": hedge_lots,
                    "pnl_per_lot": round(v["pnl"] / hedge_lots, 2) if hedge_lots > 0 else 0.0,
                }

            result = {
                "pnl": round(total_pnl, 2),
                "swap": round(total_swap, 2),
                "fees": round(total_fees, 2),
                "deal_count": deal_count,
                "by_symbol": by_symbol_final,
                "_lot_diag": _lot_diag,  # routed to app.logger by trade_dashboard.py
            }
            logger.info("[%s] MT4 deal history: %d raw orders, %d closed deals → pnl=%.2f swap=%.2f fees=%.2f pairs=%d",
                        self.account_id, len(raw_orders), deal_count,
                        result["pnl"], result["swap"], result["fees"], len(by_symbol_final))
            return result

        except Exception as e:
            logger.error("[%s] get_deal_history error: %s", self.account_id, e)
            return None

    def _on_quote(self, sender, args):
        """Handle incoming quote from MT4 server."""
        if not _mt4_init_gate.is_set():
            return  # Suppress during connection init to prevent CLR crash
        try:
            info = self.dd["ea_account_info"].get(self.account_id, {})
            symbol = str(args.Symbol).upper() if hasattr(args, 'Symbol') else ""
            target = (info.get("symbol") or "").upper()
            # Case-insensitive match, also handle broker suffixes
            if symbol and target and (symbol.startswith(target) or target.startswith(symbol)):
                info["bid"] = float(args.Bid)
                info["ask"] = float(args.Ask)
                info["quote_symbol"] = target
                if info["bid"] > 0 and info["ask"] > 0:
                    pip_mult = 1000 if "JPY" in target else 100000
                    info["spread"] = round((info["ask"] - info["bid"]) * pip_mult, 1)
                info["last_update"] = time.time()
                self.dd["ea_account_info"][self.account_id] = info
                self.dd["ea_heartbeats"][self.account_id] = time.time()
                _quote_wakeup.set()  # Wake command loop immediately
        except Exception as e:
            logger.error("[%s] Quote event error: %s", self.account_id, e)

    def _on_order_update(self, sender, args):
        """Handle order update from MT4 server.
        IMPORTANT: This runs on a .NET thread pool thread. Do NOT call
        any .NET iteration (GetOpenedOrders, SymbolsInfo, etc.) here —
        pythonnet's tp_iternext corrupts the CLR (0x80131506).
        Instead, set a flag so the heartbeat loop does the refresh."""
        if not _mt4_init_gate.is_set():
            return
        self._order_update_pending.set()

    def _on_disconnect(self, sender, args):
        """Handle disconnection from MT4 server.
        Sets flag so the heartbeat loop will trigger reconnect."""
        if not _mt4_init_gate.is_set():
            return  # Suppress during connection init to prevent CLR crash
        logger.warning("[%s] Disconnected from MT4 server (event callback) — will auto-reconnect", self.account_id)
        self._connected = False

    def send_market_order(self, symbol, side, lots, session_id="", comment=""):
        """Send a market order. Returns (success, ticket, price) or (False, 0, 0)."""
        if not self._connected or not self._order_client:
            logger.error("[%s] MT4 send_market_order blocked: connected=%s order_client=%s",
                         self.account_id, self._connected, 
                         type(self._order_client).__name__ if self._order_client else None)
            return False, 0, 0

        try:
            from TradingAPI.MT4Server import Op, PlacedType
            import System

            op = Op.Buy if side.lower() == "buy" else Op.Sell

            # Get current price — prefer GetQuote for the EXACT symbol
            # (ea_account_info bid/ask can be stale or from a different symbol)
            price = 0.0
            try:
                quote = self._client.GetQuote(symbol)
                if quote:
                    price = float(quote.Ask) if side.lower() == "buy" else float(quote.Bid)
            except Exception:
                pass
            if price <= 0:
                ea_info = self.dd["ea_account_info"].get(self.account_id, {})
                if side.lower() == "buy":
                    price = ea_info.get("ask", 0.0) or 0.0
                else:
                    price = ea_info.get("bid", 0.0) or 0.0

            if price <= 0:
                logger.error("[%s] Cannot get price for %s (no cached quotes)", self.account_id, symbol)
                return False, 0, 0

            slippage = int(self.config.get("slippage", 3))
            magic = int(self.config.get("magic_number", 777888))

            logger.info("[%s] Sending %s %s %.2f lots @ %.5f comment=%s",
                        self.account_id, side, symbol, lots, price, comment)

            order = self._order_client.OrderSend(
                symbol, op, lots, price, slippage,
                0.0,  # SL
                0.0,  # TP
                comment,
                magic,
                System.DateTime.MaxValue,  # Expiration
                PlacedType.Default
            )

            if order and order.Ticket > 0:
                ticket = _normalize_ticket(order.Ticket)
                fill_price = float(order.OpenPrice)
                logger.info("[%s] FILLED: ticket=%d %s %s %.2f @ %.5f",
                            self.account_id, ticket, side, symbol, lots, fill_price)

                # Report fill to dashboard
                self._report_result(session_id, "filled", ticket,
                                    fill_price=fill_price, quote_price=price)
                return True, ticket, fill_price
            else:
                logger.error("[%s] OrderSend returned no ticket", self.account_id)
                self._report_result(session_id, "error", 0, detail="OrderSend returned no ticket")
                return False, 0, 0

        except Exception as e:
            logger.error("[%s] OrderSend error: %s", self.account_id, e)
            self._report_result(session_id, "error", 0, detail=str(e))
            return False, 0, 0

    def close_position(self, ticket, symbol, side, lots, session_id="", comment=""):
        """Close a specific position by ticket."""
        if not self._connected or not self._order_client:
            return False

        try:
            from TradingAPI.MT4Server import Op

            ticket = int(ticket)

            # Get current price for close — prefer GetQuote for the EXACT symbol
            # (ea_account_info bid/ask can be stale or from a different symbol)
            price = 0.0
            try:
                quote = self._client.GetQuote(symbol)
                if quote:
                    price = float(quote.Bid) if side.lower() == "buy" else float(quote.Ask)
            except Exception:
                pass
            if price <= 0:
                ea_info = self.dd["ea_account_info"].get(self.account_id, {})
                if side.lower() == "buy":
                    price = ea_info.get("bid", 0.0) or 0.0
                else:
                    price = ea_info.get("ask", 0.0) or 0.0

            slippage = int(self.config.get("slippage", 3))

            logger.info("[%s] Closing ticket=%d %s %.2f lots @ %.5f",
                        self.account_id, ticket, symbol, lots, price)

            # OrderClose(symbol, ticket, lots, price, slippage)
            result = self._order_client.OrderClose(symbol, ticket, lots, price, slippage)

            if result:
                # Try to get actual close price from the closed order
                actual_close_price = price
                try:
                    orders = self._order_client.GetClosedOrders(symbol)
                    if orders:
                        for o in orders:
                            if hasattr(o, 'Ticket') and int(o.Ticket) == ticket:
                                if hasattr(o, 'ClosePrice'):
                                    actual_close_price = float(o.ClosePrice)
                                break
                except Exception:
                    pass  # Fallback to request price
                logger.info("[%s] CLOSED: ticket=%d @ %.5f (quote=%.5f)", self.account_id, ticket, actual_close_price, price)
                self._report_result(session_id, "rollback_closed" if session_id else "closed",
                                    ticket, fill_price=actual_close_price, quote_price=price)
                return True
            else:
                logger.error("[%s] OrderClose failed for ticket=%d", self.account_id, ticket)
                self._report_result(session_id, "error", ticket, detail="OrderClose failed")
                return False

        except Exception as e:
            logger.error("[%s] OrderClose error: %s", self.account_id, e)
            self._report_result(session_id, "error", ticket, detail=str(e))
            return False

    def modify_position_tp(self, ticket, symbol, side, lots, tp, sl=None, price=None):
        """Modify the TakeProfit (and optionally StopLoss) of an open position.
        Used by CLOSE-LIMIT mode to set a passive TP instead of a market close."""
        if not self._connected or not self._order_client:
            logger.error("[%s] MT4 modify_position_tp blocked: not connected", self.account_id)
            return False, "Not connected"
        try:
            from TradingAPI.MT4Server import Op
            import System
            ticket = int(ticket)
            op = Op.Buy if side.lower() == "buy" else Op.Sell
            sl_val = float(sl) if sl is not None else 0.0
            tp_val = float(tp) if tp is not None else 0.0
            target_price = float(price) if price is not None else 0.0
            # For MT4 OrderModify: price is the open price of the position (0 = use current)
            result = self._order_client.OrderModify(
                ticket, symbol, op, lots, target_price,
                sl_val, tp_val,
                System.DateTime.MaxValue
            )
            if result:
                logger.info("[%s] MT4 position modified: ticket=%d price=%.5f tp=%.5f", self.account_id, ticket, target_price, tp_val)
                return True, ticket
            else:
                logger.error("[%s] MT4 OrderModify returned False for ticket=%d", self.account_id, ticket)
                return False, "OrderModify returned False"
        except Exception as e:
            logger.error("[%s] MT4 modify_position_tp error: %s", self.account_id, e)
            return False, str(e)

    def modify_limit_price(self, ticket, symbol, side, lots, price):
        """Modify the entry price of a pending limit order."""
        return self.modify_position_tp(ticket, symbol, side, lots, tp=None, sl=None, price=price)

    def send_limit_order(self, symbol, side, lots, price, limit_type, session_id="", comment=""):
        """Send a pending limit order (BuyLimit/SellLimit).
        Used by OPEN-LIMIT mode."""
        if not self._connected or not self._order_client:
            logger.error("[%s] MT4 send_limit_order blocked: not connected", self.account_id)
            return False, 0, 0
        try:
            from TradingAPI.MT4Server import Op
            import System
            if limit_type.lower() in ("buylimit", "buy_limit"):
                op = Op.BuyLimit
            else:
                op = Op.SellLimit
            slippage = int(self.config.get("slippage", 3))
            magic = int(self.config.get("magic_number", 777888))
            logger.info("[%s] MT4 Sending %s %s %.2f lots @ %.5f (limit)", self.account_id, limit_type, symbol, lots, price)
            order = self._order_client.OrderSend(
                symbol, op, lots, float(price), slippage,
                0.0, 0.0, comment, magic,
                System.DateTime.MinValue, None
            )
            if order and order.Ticket > 0:
                ticket = _normalize_ticket(order.Ticket)
                logger.info("[%s] MT4 LIMIT PLACED: ticket=%d %s %s %.2f @ %.5f", self.account_id, ticket, limit_type, symbol, lots, price)
                self._report_result(session_id, "limit_placed", ticket, fill_price=float(price), quote_price=float(price))

                # ── Background fill-watcher (MT4 Direct Version) ─────────────────
                _pending_ticket = ticket
                _watch_symbol = symbol
                _watch_lots = float(lots)
                _watch_side = side.lower()

                def _watch_limit_fill():
                    import time as _time
                    _timeout = 86400  # watch for up to 24h
                    _start = _time.time()
                    _poll_interval = 2.0
                    logger.info("[%s] LIMIT-WATCH: watching ticket=%d for fill (symbol=%s side=%s lots=%.2f)",
                                self.account_id, _pending_ticket, _watch_symbol, _watch_side, _watch_lots)
                    _prev_positions = set()
                    while _time.time() - _start < _timeout:
                        _time.sleep(_poll_interval)
                        if not self._connected or not self._client:
                            return
                        try:
                            with _clr_lock:
                                orders = self._get_open_orders()
                            still_pending = any(
                                o.get('Ticket') and int(o['Ticket']) == _pending_ticket
                                for o in orders
                            )
                            if still_pending:
                                _prev_positions = set(
                                    int(o['Ticket']) for o in orders
                                    if o.get('Ticket') and o.get('Type') in ('Buy', 'Sell', 0, 1)
                                )
                                continue

                            # Pending order gone — find the new filled position
                            sym_upper = _watch_symbol.upper().replace(".", "")
                            new_ticket = 0
                            new_price = price
                            for o in orders:
                                if not o.get('Ticket'):
                                    continue
                                t = int(o['Ticket'])
                                if t == _pending_ticket:
                                    continue
                                o_sym = str(o.get('Symbol', '') or o.get('symbol', '')).upper().replace(".", "")
                                if sym_upper not in o_sym and o_sym not in sym_upper:
                                    continue
                                o_lots = float(o.get('Lots') or o.get('Volume') or 0)
                                if abs(o_lots - _watch_lots) > _watch_lots * 0.01 + 0.001:
                                    continue
                                new_ticket = t
                                new_price = float(o.get('OpenPrice') or o.get('PriceOpen') or price)
                                break

                            if new_ticket == 0:
                                for o in orders:
                                    if not o.get('Ticket'):
                                        continue
                                    o_sym = str(o.get('Symbol', '') or o.get('symbol', '')).upper().replace(".", "")
                                    if sym_upper in o_sym or o_sym in sym_upper:
                                        t = int(o['Ticket'])
                                        if t != _pending_ticket and t not in _prev_positions:
                                            new_ticket = t
                                            new_price = float(o.get('OpenPrice') or o.get('PriceOpen') or price)
                                            break

                            if new_ticket > 0:
                                logger.info("[%s] LIMIT-WATCH: FILLED! pending=%d -> position=%d @ %.5f",
                                            self.account_id, _pending_ticket, new_ticket, new_price)
                                self._report_result(session_id, "filled", new_ticket,
                                                    fill_price=new_price, quote_price=new_price)
                            else:
                                logger.warning("[%s] LIMIT-WATCH: pending=%d gone but no new position found",
                                               self.account_id, _pending_ticket)
                                self._report_result(session_id, "filled", _pending_ticket,
                                                    fill_price=price, quote_price=price)
                            return
                        except Exception as _e:
                            logger.debug("[%s] LIMIT-WATCH poll error: %s", self.account_id, _e)
                            continue

                import threading as _threading
                _t = _threading.Thread(target=_watch_limit_fill, daemon=True,
                                       name=f"LimitWatch-{self.account_id}-{ticket}")
                _t.start()
                # ─────────────────────────────────────────────────────────────────

                return True, ticket, float(price)
            else:
                logger.error("[%s] MT4 limit OrderSend returned no ticket", self.account_id)
                self._report_result(session_id, "error", 0, detail="MT4 limit OrderSend returned no ticket")
                return False, 0, 0
        except Exception as e:
            logger.error("[%s] MT4 send_limit_order error: %s", self.account_id, e)
            self._report_result(session_id, "error", 0, detail=str(e))
            return False, 0, 0

    def _report_result(self, session_id, status, ticket, detail="", fill_price=0, quote_price=0):
        """Report trade result back to dashboard (same as EA's trade_result)."""
        try:
            import requests
            # Post to dashboard's trade_result endpoint internally
            # Instead of HTTP, we can call the handler directly via dashboard_data
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
            else:
                logger.warning("[%s] No report_trade_result function available", self.account_id)
        except Exception as e:
            logger.error("[%s] Report result error: %s", self.account_id, e)

    def get_symbol_info(self, symbol):
        """Get bid/ask/spread for a symbol.
        Reads from _symbol_cache (polled from SymbolsInfo during _push_account_info).
        CLR-safe: pure Python dict read."""
        sym_upper = symbol.upper()
        cached = self._symbol_cache.get(sym_upper)
        if cached:
            return dict(cached)
        return None

    def get_quote_direct(self, symbol):
        """Get a live bid/ask for *any* symbol via CLR GetQuote.
        Slower than get_symbol_info (hits the .NET API) but works even
        if the symbol is not in SymbolsInfo / Market Watch.
        Returns {bid, ask, spread} or None."""
        if not self._connected or not self._client:
            return None
        # Try original symbol, then with lowercase extension (e.g. USDCHF.B -> USDCHF.b)
        variants = [symbol]
        if '.' in symbol:
            base, ext = symbol.rsplit('.', 1)
            lc = base + '.' + ext.lower()
            if lc != symbol:
                variants.append(lc)
        for sym_try in variants:
            try:
                with _clr_lock:
                    quote = self._client.GetQuote(sym_try)
                    if quote:
                        bid = float(quote.Bid)
                        ask = float(quote.Ask)
                        if not getattr(self, '_quote_direct_logged', False):
                            q_attrs = {}
                            for attr in dir(quote):
                                if not attr.startswith('_'):
                                    try:
                                        q_attrs[attr] = str(getattr(quote, attr, '?'))[:60]
                                    except Exception:
                                        pass
                            logger.info("[%s] get_quote_direct(%s) bid=%.6f ask=%.6f attrs=%s",
                                        self.account_id, sym_try, bid, ask, q_attrs)
                            self._quote_direct_logged = True
                        if bid > 0 and ask > 0:
                            pip_mult = 1000 if "JPY" in symbol.upper() else 100000
                            spd = round((ask - bid) * pip_mult, 1)
                            return {"bid": bid, "ask": ask, "spread": spd}
                    else:
                        if sym_try == variants[-1]:
                            logger.warning("[%s] get_quote_direct(%s) returned None/empty", self.account_id, sym_try)
            except Exception as e:
                if sym_try == variants[-1]:
                    logger.error("[%s] get_quote_direct(%s) error: %s", self.account_id, sym_try, e)
        return None

    def get_swap_rates(self, symbols):
        """Get swap long/short rates for a list of symbols.
        Returns dict: {symbol: {swap_long, swap_short}} or empty dict."""
        result = {}
        if not self._client or not self._connected:
            return result
        # Try _symbol_cache first (populated by _push_account_info on command loop thread)
        cache = getattr(self, '_symbol_cache', {})
        if cache:
            symbols_lookup = {s.upper(): s for s in symbols}
            for sym_upper, original_name in symbols_lookup.items():
                cached = cache.get(sym_upper)
                if cached:
                    result[original_name] = {
                        "swap_long": cached.get("swap_long", 0),
                        "swap_short": cached.get("swap_short", 0),
                    }
            if result:
                return result
        # Fallback: try SymbolsInfo with CLR lock (only works from some threads)
        try:
            with _clr_lock:
                symbols_info = self._client.SymbolsInfo
                if symbols_info:
                    symbols_upper = {s.upper(): s for s in symbols}
                    for sym_obj in symbols_info:
                        try:
                            sym_name = str(getattr(sym_obj, 'Name', None) or getattr(sym_obj, 'Symbol', None) or '').upper()
                            if sym_name in symbols_upper:
                                original = symbols_upper[sym_name]
                                sl = float(getattr(sym_obj, 'SwapLong', 0))
                                ss = float(getattr(sym_obj, 'SwapShort', 0))
                                result[original] = {"swap_long": sl, "swap_short": ss}
                        except Exception:
                            continue
                    if result:
                        return result
        except Exception:
            pass
        # Last resort: GetQuote per symbol (no swap data, returns 0s)
        try:
            with _clr_lock:
                for sym in symbols:
                    try:
                        quote = self._client.GetQuote(sym)
                        if quote:
                            result[sym] = {"swap_long": 0, "swap_short": 0}
                    except Exception:
                        continue
        except Exception as e:
            logger.error("[%s] get_swap_rates CLR error: %s", self.account_id, e)
        return result

    def subscribe_symbol(self, symbol):
        """Ensure the symbol is watched (update ea_account_info with it)."""
        info = self.dd["ea_account_info"].get(self.account_id, {})
        info["symbol"] = symbol
        self.dd["ea_account_info"][self.account_id] = info
        # Update QuoteBuffer target symbol so C# filters correctly
        if _quote_buffer is not None:
            try:
                _quote_buffer.SetTargetSymbol(self.account_id, symbol)
            except Exception:
                pass
        # Push latest quote for this symbol
        sym_info = self.get_symbol_info(symbol)
        if sym_info:
            # Only update non-None values to avoid clearing existing data
            for k, v in sym_info.items():
                if v is not None:
                    info[k] = v


# ─── MT5 Direct Account ────────────────────────────────────────────────────
class MT5DirectAccount:
    """
    Wraps mtapi.mt5.MT5API.
    Same interface as MT4DirectAccount but for MT5 brokers.
    """

    def __init__(self, account_id, config, dashboard_data):
        self.account_id = str(account_id)
        self.config = config
        self.dd = dashboard_data
        self.label = config.get("label", f"MT5-{account_id}")
        self.conn_type = "mt5_direct"
        self.lot_divisor = config.get("lot_divisor", 1.0)

        self._client = None  # MT5API instance
        self._connected = False
        self._running = False
        self._lock = threading.Lock()
        self._quote_thread = None
        self._last_error = None
        self._events_subscribed = False
        self._order_update_pending = threading.Event()  # Flag for deferred order refresh

        # Batch fill tracking: prevents multiple concurrent fill-watchers from
        # claiming the same new position ticket when a batch of limits fills at once.
        self._claimed_fill_tickets = set()
        self._claimed_fill_lock = threading.Lock()

        # Auto-reconnect state
        self._reconnect_delay = RECONNECT_BASE_DELAY
        self._reconnect_attempt = 0

        # Per-symbol quote cache — populated by _push_account_info (CLR-safe thread),
        # read by get_symbol_info (Flask thread, no CLR calls)
        self._symbol_cache = {}  # {"USDCHF": {"bid": ..., "ask": ..., "spread": ...}}

    @property
    def connected(self):
        return self._connected

    def _subscribe_events(self):
        """Subscribe to .NET events via C# QuoteBuffer shim.
        All handlers run as pure .NET delegates — no pythonnet dispatch."""
        if self._events_subscribed or not self._client or not self._connected:
            return
        global _quote_buffer
        if _quote_buffer is not None:
            try:
                target = (self.dd.get("ea_account_info", {}).get(self.account_id, {}).get("symbol", "") or "")
                _quote_buffer.SubscribeMT5(self._client, self.account_id, target)
                self._events_subscribed = True
                logger.info("[%s] MT5 Events subscribed via C# QuoteBuffer (symbol=%s)", self.account_id, target)
            except Exception as e:
                logger.warning("[%s] Failed to subscribe via QuoteBuffer: %s", self.account_id, e)
        else:
            logger.info("[%s] QuoteBuffer not available — polling-only mode", self.account_id)

    def _unsubscribe_events(self):
        """Unsubscribe .NET event handlers. Safe to call multiple times."""
        if not self._events_subscribed or not self._client:
            return
        try:
            self._client.OnQuote -= self._on_quote
        except Exception:
            pass
        try:
            self._client.OnOrderUpdate -= self._on_order_update
        except Exception:
            pass
        self._events_subscribed = False

    def _deferred_subscribe(self):
        """Subscribe .NET events via C# QuoteBuffer.
        With QuoteBuffer handling events natively in C#, no deferral needed."""
        if self._connected and self._running:
            self._subscribe_events()
            logger.info("[%s] MT5 Events subscribed (immediate via QuoteBuffer)", self.account_id)

    def start(self):
        """Connect to the MT5 server."""
        self._last_error = None
        # Mark running BEFORE attempting connect so the heartbeat thread
        # always starts — it handles auto-reconnect on failure.
        self._running = True
        if not _load_clr():
            self._last_error = "pythonnet CLR failed to initialize — check server logs for details. Ensure .NET runtime is installed."
            logger.error("[%s] Cannot start — %s", self.account_id, self._last_error)
            return False

        try:
            from mtapi.mt5 import MT5API

            login = int(self.config["login"])
            password = str(self.config["password"])
            server = str(self.config["server"])
            port = int(self.config.get("port", 443))

            logger.info("[%s] Connecting to MT5 server %s:%d ...", self.account_id, server, port)
            # Serialize connection init — .NET MT API has shared static buffers.
            # Both the constructor AND Connect() must be serialized.
            # Gate suppresses heartbeat + callback processing on the Python side
            # during connect. We do NOT pause/resume other accounts' .NET event
            # handlers — doing so corrupts their IO completion ports.
            with _mt_connect_lock:
                _mt5_init_gate.clear()
                logger.info("[%s] Gate closed for connection init", self.account_id)

                # REMOVED: System.GC.Collect() — calling GC.Collect() from Python
                # via pythonnet causes fatal CLR corruption (0x80131506) when .NET
                # background threads (IO completion, event handlers) are active.
                # Let the .NET runtime manage its own GC.
                time.sleep(1)  # Settle for .NET thread pool to drain callbacks

                logger.info("[%s] Creating MT5API object (under lock)...", self.account_id)
                self._client = MT5API(login, password, server, port)
                logger.info("[%s] MT5API object created", self.account_id)

                # Enable background thread for processing server messages.
                # Without this, MarketOpenWaiter/MarketCloseWaiter never receive
                # trade responses and time out after 30s.
                try:
                    cur_val = self._client.ProcessServerMessagesInThread
                    logger.info("[%s] ProcessServerMessagesInThread was: %s", self.account_id, cur_val)
                    self._client.ProcessServerMessagesInThread = True
                    logger.info("[%s] ProcessServerMessagesInThread set to True", self.account_id)
                except Exception as pmt_err:
                    logger.warning("[%s] Could not set ProcessServerMessagesInThread: %s", self.account_id, pmt_err)

                if self._client is None:
                    self._last_error = "MT5API constructor returned None — .NET resources may not have been fully released. Try again."
                    logger.error("[%s] %s", self.account_id, self._last_error)
                    _mt5_init_gate.set()
                    return False

                # Run Connect() with a timeout — it sometimes hangs forever.
                # Use a daemon thread so a hung Connect() doesn't block all
                # other accounts from proceeding through the serialization lock.
                connect_result = [None]  # [exception_or_None]
                def _do_connect():
                    try:
                        self._client.Connect()
                    except Exception as ex:
                        connect_result[0] = ex

                logger.info("[%s] Calling Connect() (15s timeout)...", self.account_id)
                ct = threading.Thread(target=_do_connect, daemon=True)
                ct.start()
                ct.join(timeout=15)

                if ct.is_alive():
                    logger.warning("[%s] Connect() timed out after 15s — will retry later", self.account_id)
                    self._last_error = "MT5 Connect() timed out after 15s"
                    self._connected = False
                    # Tear down the stuck client's .NET resources to prevent
                    # orphaned thread pool work items from causing AccessViolationException
                    stuck = self._client
                    self._client = None
                    try:
                        stuck.Disconnect()
                    except Exception:
                        pass
                    try:
                        if hasattr(stuck, 'Dispose'):
                            stuck.Dispose()
                    except Exception:
                        pass
                elif connect_result[0] is not None:
                    logger.error("[%s] Connect() raised: %s", self.account_id, connect_result[0])
                    self._last_error = f"MT5 Connect() error: {connect_result[0]}"
                    self._connected = False
                else:
                    _is_connected = self._client.Connected

                    if _is_connected:
                        _bal = 0.0
                        try:
                            acct = getattr(self._client, 'Account', None)
                            if acct:
                                _bal = float(getattr(acct, 'Balance', 0) or 0)
                        except Exception:
                            pass
                        logger.info("[%s] Connected! Balance: %.2f Equity: %.2f",
                                    self.account_id,
                                    _bal,
                                    float(self._client.AccountEquity) if hasattr(self._client, 'AccountEquity') else 0)

                        # Register for callback management first, then do the
                        # definitive data push.
                        _register_account(self)

                        # Active-wait for BOTH Account.Balance AND AccountEquity
                        # to be non-zero before pushing.  ProcessServerMessagesInThread
                        # makes Connect() return before all data is synced — Balance
                        # arrives quickly but AccountEquity (used for PnL) may lag
                        # behind by 0.5-2s.  Exiting the wait before Equity is ready
                        # causes pnl = equity(0) - balance = 0 on first push.
                        _acct_wait_deadline = time.time() + 5.0
                        while time.time() < _acct_wait_deadline:
                            try:
                                _acct_obj = getattr(self._client, 'Account', None)
                                _acct_bal = float(getattr(_acct_obj, 'Balance', 0) or 0) if _acct_obj else 0.0
                                _acct_eq  = float(getattr(self._client, 'AccountEquity', 0) or 0)
                                _open_pos_count = len(self._get_open_orders() or [])
                                if _acct_bal != 0.0 and _acct_eq != 0.0:
                                    if _open_pos_count > 0 and _acct_eq == _acct_bal:
                                        # Stale equity (equals balance but has open positions) — keep waiting
                                        pass
                                    else:
                                        break  # Both balance and equity received
                            except Exception:
                                pass
                            time.sleep(0.1)
                        _elapsed = round(5.0 - (_acct_wait_deadline - time.time()), 2)
                        logger.info("[%s] Account data ready after ~%.1fs (bal=%.2f eq=%.2f) — pushing",
                                    self.account_id, _elapsed, _acct_bal, _acct_eq)

                        # Single definitive push — Account.Balance is now populated
                        self._push_account_info()
                        self._push_positions()

                        # NOW mark as connected — get_status() will only report
                        # this account after PnL data is fully populated.
                        # This prevents the browser from ever seeing
                        # connected=true with total_pnl=null.
                        self._connected = True

                        logger.info("[%s] Post-connect push done (balance=%.2f equity=%.2f pnl=%.2f)",
                                    self.account_id,
                                    self.dd["ea_account_info"].get(self.account_id, {}).get("balance") or 0,
                                    self.dd["ea_account_info"].get(self.account_id, {}).get("equity") or 0,
                                    self.dd["ea_account_info"].get(self.account_id, {}).get("total_pnl") or 0)
                    else:
                        self._last_error = "MT5 Connection failed — check server/login/password"
                        logger.error("[%s] %s", self.account_id, self._last_error)

                # Brief settle before releasing lock
                time.sleep(0.5)

                # DO NOT subscribe events inside the lock — deferring to a
                # background thread prevents callback traffic from colliding
                # with the next account's ConnectThread.LoadSymbols on the
                # shared .NET thread pool (deterministic crash prevention).

                # Open gate — settle complete, safe for heartbeats
                _mt5_init_gate.set()
                logger.info("[%s] Gate opened (heartbeats may resume)", self.account_id)

            # Subscribe events immediately (via C# QuoteBuffer — no deferral needed)
            if self._connected:
                self._deferred_subscribe()

            # Always ensure heartbeat thread is running (handles reconnect on failure)
            if self._quote_thread is None or not self._quote_thread.is_alive():
                self._quote_thread = threading.Thread(
                    target=self._heartbeat_loop, daemon=True,
                    name=f"MT5Direct-{self.account_id}")
                self._quote_thread.start()

            return self._connected
        except Exception as e:
            self._last_error = str(e)
            logger.error("[%s] MT5 Connection error: %s", self.account_id, e)
            self._connected = False
            # Ensure heartbeat thread runs for auto-reconnect even after exceptions
            self._running = True
            if self._quote_thread is None or not self._quote_thread.is_alive():
                self._quote_thread = threading.Thread(
                    target=self._heartbeat_loop, daemon=True,
                    name=f"MT5Direct-{self.account_id}")
                self._quote_thread.start()
            # Make sure gate is reopened even on error
            try:
                _mt5_init_gate.set()
            except Exception:
                pass
            return False

    def stop(self):
        """Disconnect from the MT5 server."""
        self._running = False
        self._connected = False
        _unregister_account(self)
        # Wait for heartbeat thread to exit before tearing down .NET objects
        if self._quote_thread and self._quote_thread.is_alive():
            self._quote_thread.join(timeout=5)
        self._quote_thread = None
        # Properly tear down .NET client to prevent CLR corruption on reconnect
        self._cleanup_for_reconnect()
        # Brief settle delay to let .NET GC release resources before any reconnect
        time.sleep(1)
        logger.info("[%s] MT5 Disconnected", self.account_id)

    def _heartbeat_loop(self):
        """Safety-net sync: re-push account info and positions every 30s.
        Persists through disconnections — triggers reconnect with backoff."""
        # Brief random stagger (0-2s) to desynchronize heartbeat threads
        time.sleep(_random.uniform(0, 2))
        while self._running:
            try:
                if self._connected:
                    # Skip .NET calls if another account is mid-Connect()
                    # to prevent CLR corruption (0x80131506)
                    if not _mt5_init_gate.is_set():
                        time.sleep(2)
                        continue
                    # Check if an order update callback set the pending flag
                    if self._order_update_pending.is_set():
                        self._order_update_pending.clear()
                        _heartbeat_sem.acquire()
                        try:
                            self._push_account_info()
                            self._push_positions()
                        finally:
                            _heartbeat_sem.release()
                        continue
                    # Throttle concurrent .NET API calls across all accounts
                    _heartbeat_sem.acquire()
                    try:
                        self._push_account_info()
                        self._push_positions()
                    finally:
                        _heartbeat_sem.release()
                    # Check if connection silently died
                    if not self._check_connection():
                        continue
                    # Sleep in short increments — check buffer for events
                    for _ in range(2):
                        if self._order_update_pending.is_set() or not self._running:
                            break
                        # Check C# QuoteBuffer for order updates
                        if _quote_buffer is not None:
                            try:
                                if _quote_buffer.CheckAndClearOrderUpdate(self.account_id):
                                    self._order_update_pending.set()
                                    break
                            except Exception:
                                pass
                        time.sleep(2)
                else:
                    # Disconnected — attempt reconnect with backoff
                    self._attempt_reconnect()
            except Exception as e:
                logger.error("[%s] MT5 Heartbeat error: %s", self.account_id, e)
                if not self._check_connection():
                    continue
                time.sleep(30)

    def _check_connection(self):
        """Check if still connected; if not, mark as disconnected."""
        try:
            if self._client and self._client.Connected:
                return True
        except Exception:
            pass
        if self._connected:
            logger.warning("[%s] MT5 Connection lost (detected by heartbeat)", self.account_id)
            self._connected = False
        return False

    def _attempt_reconnect(self):
        """Try to reconnect with exponential backoff."""
        if not self._running:
            return
        self._reconnect_attempt += 1
        delay = self._reconnect_delay
        logger.info("[%s] MT5 Attempting reconnect (attempt #%d, waiting %ds)...",
                    self.account_id, self._reconnect_attempt, delay)
        # Sleep in small increments so we can exit quickly if stopped
        for _ in range(int(delay * 2)):
            if not self._running:
                return
            time.sleep(0.5)
        if not self._running:
            return
        try:
            self._cleanup_for_reconnect()
            # start() already serializes via _mt_connect_lock internally,
            # so we do NOT acquire the lock here (would deadlock — Lock is non-reentrant).
            if not self._running:
                return
            ok = self.start()
            if ok:
                logger.info("[%s] MT5 Reconnected successfully after %d attempt(s)",
                            self.account_id, self._reconnect_attempt)
                self._reconnect_delay = RECONNECT_BASE_DELAY
                self._reconnect_attempt = 0
            else:
                logger.warning("[%s] MT5 Reconnect failed: %s", self.account_id, self._last_error)
                self._reconnect_delay = min(delay * RECONNECT_BACKOFF, RECONNECT_MAX_DELAY)
        except Exception as e:
            logger.error("[%s] MT5 Reconnect error: %s", self.account_id, e)
            self._reconnect_delay = min(delay * RECONNECT_BACKOFF, RECONNECT_MAX_DELAY)

    def _cleanup_for_reconnect(self):
        """Tear down stale .NET client objects before reconnecting."""
        self._unsubscribe_events()
        try:
            if self._client:
                try:
                    self._client.Disconnect()
                except Exception:
                    pass
                try:
                    if hasattr(self._client, 'Dispose'):
                        self._client.Dispose()
                except Exception:
                    pass
        except Exception:
            pass
        self._client = None

    def _push_account_info(self):
        """Push account data into ea_account_info."""
        if not self._client:
            return
        # Get or create info dict — store immediately so partial updates aren't lost
        info = self.dd["ea_account_info"].get(self.account_id, {})
        self.dd["ea_account_info"][self.account_id] = info
        info["conn_type"] = "mt5_direct"

        # Account properties — MT5 API uses different property names than MT4
        # Direct client properties: AccountEquity, AccountFreeMargin, AccountMargin, AccountProfit
        for prop, attr in [("equity", "AccountEquity"), ("free_margin", "AccountFreeMargin"),
                           ("margin", "AccountMargin")]:
            try:
                val = getattr(self._client, attr, None)
                if val is not None:
                    info[prop] = float(val)
            except Exception:
                pass

        # Try direct AccountBalance / AccountProfit properties first — these are
        # available immediately after Connect() returns, unlike the Account
        # sub-object (Account.Balance) which is populated asynchronously via a
        # server data packet that arrives with a short delay.  This is the root
        # cause of MT5 not showing PnL immediately on connect while MT4 does.
        for prop, attr in [("balance", "AccountBalance"), ("leverage", "AccountLeverage"),
                           ("credit", "AccountCredit")]:
            try:
                val = getattr(self._client, attr, None)
                if val is not None:
                    info[prop] = int(val) if prop == "leverage" else float(val)
            except Exception:
                pass

        # Account sub-object properties: Balance, Leverage (may be populated with a
        # delay after connect — only overwrite if we have a non-zero value so we
        # don't clobber the direct-property values with zeros)
        try:
            acct = self._client.Account if hasattr(self._client, 'Account') else None
            if acct:
                # Debug: log account sub-object properties once
                if not getattr(self, '_acct_props_logged', False):
                    try:
                        acct_attrs = [a for a in dir(acct) if not a.startswith('_')]
                        logger.info("[%s] MT5 Account sub-object attrs: %s", self.account_id, acct_attrs)
                        self._acct_props_logged = True
                    except Exception:
                        pass
                for prop, attr in [("balance", "Balance"), ("leverage", "Leverage")]:
                    try:
                        val = getattr(acct, attr, None)
                        # Only overwrite if sub-object has a meaningful (non-zero) value
                        # — avoids clobbering the direct-property value with a
                        # transient zero while the account packet is in-flight.
                        if val is not None and float(val) != 0.0:
                            info[prop] = int(val) if prop == "leverage" else float(val)
                    except Exception:
                        pass

                # Detect netting mode via Account.TradeMode.
                # MT5 TradeMode: 0=Demo hedge, 1=Contest/Netting, 2=Real hedge, etc.
                # The canonical netting value is ACCOUNT_TRADE_MODE_NETTING = 1,
                # but some brokers use 0 for netting on demo — so we also check
                # the string representation for 'netting'.
                try:
                    trade_mode = getattr(acct, 'TradeMode', None)
                    if trade_mode is not None:
                        tm_str = str(trade_mode).lower()
                        is_netting = ('netting' in tm_str) or (str(trade_mode) == '1')
                        prev_netting = info.get('netting_mode', None)
                        info['netting_mode'] = is_netting
                        if prev_netting != is_netting:
                            logger.info("[%s] MT5 netting_mode=%s (TradeMode=%s)",
                                        self.account_id, is_netting, trade_mode)
                except Exception:
                    pass

        except Exception:
            pass

        # Push quote data for subscribed symbol — prefer C# buffer
        target_symbol = info.get("symbol", "")
        if target_symbol:
            quote_ok = False
            # Method 0: C# QuoteBuffer (real-time, zero .NET calls from Python)
            if _quote_buffer is not None:
                try:
                    qb_quote = _quote_buffer.GetQuote(self.account_id)
                    if qb_quote is not None:
                        info["bid"] = float(qb_quote.Bid)
                        info["ask"] = float(qb_quote.Ask)
                        info["quote_symbol"] = target_symbol.upper()
                        info["spread"] = float(qb_quote.Spread)
                        quote_ok = True
                except Exception:
                    pass
            # Method 1: GetQuote polling (fallback)
            if not quote_ok:
                try:
                    quote = self._client.GetQuote(target_symbol)
                    if quote:
                        info["bid"] = float(quote.Bid)
                        info["ask"] = float(quote.Ask)
                        info["quote_symbol"] = target_symbol.upper()
                        if info["bid"] > 0 and info["ask"] > 0:
                            pair = target_symbol.upper()
                            pip_mult = 1000 if "JPY" in pair else 100000
                            info["spread"] = round((info["ask"] - info["bid"]) * pip_mult, 1)
                except Exception:
                    pass  # Quote not available yet

        # Cache ALL symbols from MT5 Symbols.Infos (dictionary keyed by symbol name)
        try:
            symbols_obj = getattr(self._client, 'Symbols', None)
            if symbols_obj:
                infos = getattr(symbols_obj, 'Infos', None)
                # One-time diagnostic
                if not getattr(self, '_syminfo_logged', False):
                    try:
                        if infos:
                            keys_list = list(infos.Keys)[:5] if hasattr(infos, 'Keys') else []
                            si_count = len(list(infos.Keys)) if hasattr(infos, 'Keys') else 0
                            # Log first value's properties
                            sample_props = None
                            group_props = None
                            si_props = None
                            if keys_list:
                                first_val = infos[keys_list[0]]
                                if first_val:
                                    sample_props = [a for a in dir(first_val) if not a.startswith('_')]
                            # Also try GetGroup and SymbolsInfo for swap data
                            try:
                                grp = symbols_obj.GetGroup('USDJPY')
                                if grp:
                                    group_props = [a for a in dir(grp) if not a.startswith('_')]
                            except Exception:
                                pass
                            try:
                                si_old = getattr(self._client, 'SymbolsInfo', None)
                                if si_old:
                                    for si_item in si_old:
                                        si_props = [a for a in dir(si_item) if not a.startswith('_')]
                                        break
                            except Exception:
                                pass
                            logger.info("[%s] MT5 Symbols.Infos: count=%d, first_keys=%s, Infos_props=%s",
                                        self.account_id, si_count, keys_list, sample_props)
                            logger.info("[%s] MT5 GetGroup('USDJPY') props=%s", self.account_id, group_props)
                            logger.info("[%s] MT5 SymbolsInfo[0] props=%s", self.account_id, si_props)
                        else:
                            logger.info("[%s] MT5 Symbols.Infos is None/empty", self.account_id)
                    except Exception as si_err:
                        logger.info("[%s] MT5 Symbols.Infos diagnostic error: %s", self.account_id, si_err)
                    self._syminfo_logged = True
                if infos and hasattr(infos, 'Keys'):
                    new_cache = {}
                    try:
                        for sym_name in infos.Keys:
                            try:
                                sn_upper = str(sym_name).strip().upper()
                                if not sn_upper:
                                    continue
                                # Swap data is in GetGroup, NOT in Infos values
                                sl = 0.0
                                ss = 0.0
                                try:
                                    grp = symbols_obj.GetGroup(sym_name)
                                    if grp:
                                        sl = float(getattr(grp, 'SwapLong', 0) or 0)
                                        ss = float(getattr(grp, 'SwapShort', 0) or 0)
                                except Exception:
                                    pass
                                entry = {"swap_long": sl, "swap_short": ss}
                                new_cache[sn_upper] = entry
                            except Exception:
                                continue
                    except Exception:
                        pass
                    if new_cache:
                        self._symbol_cache = new_cache
                        if not getattr(self, '_cache_sample_logged', False):
                            # Show FX pairs in sample, not stocks
                            fx_sample = {k: v for k, v in new_cache.items()
                                         if len(k) == 6 and k.isalpha() and (v['swap_long'] != 0 or v['swap_short'] != 0)}
                            sample = dict(list(fx_sample.items())[:3]) if fx_sample else dict(list(new_cache.items())[:3])
                            logger.info("[%s] MT5 _symbol_cache populated: %d symbols, fx_swap_sample: %s",
                                        self.account_id, len(new_cache), sample)
                            self._cache_sample_logged = True
        except Exception as e:
            logger.warning("[%s] Error in MT5 _push_account_info: %s", self.account_id, e, exc_info=True)


        # Check if direct properties are stale (async update delay after connection)
        # If equity equals balance, but there are open positions, override with calculated values
        try:
            _eq = info.get("equity", 0.0)
            _bal = info.get("balance", 0.0)
            if _eq == _bal and _eq > 0:
                _orders = self._get_open_orders()
                if _orders:
                    _calc_profit = sum(o.get('Profit', 0.0) + o.get('Swap', 0.0) for o in _orders)
                    if _calc_profit != 0.0:
                        info["equity"] = _bal + _calc_profit
                        # If we have margin, free margin is equity - margin
                        _margin = info.get("margin", 0.0)
                        info["free_margin"] = info["equity"] - _margin
        except Exception:
            pass

        info["last_update"] = time.time()
        self.dd["ea_heartbeats"][self.account_id] = time.time()

    def _push_positions(self):
        """Push open position tickets into ea_account_info."""
        if not self._client:
            return
        try:
            info = self.dd["ea_account_info"].get(self.account_id, {})
            # Get open orders from the client
            orders = self._get_open_orders()
            # Filter to only include active market positions (exclude pending limit/stop orders)
            active_types = ('buy', 'sell', '0', '1', 'op_buy', 'op_sell', 'position_type_buy', 'position_type_sell')
            
            raw_types = [str(o.get('Type', '')) for o in orders]
            logger.warning("[%s] MT5 _push_positions raw_types: %s", self.account_id, raw_types)
            
            tickets = [o['Ticket'] for o in orders if str(o.get('Type', '')).lower() in active_types]
            logger.warning("[%s] MT5 _push_positions filtered %d orders down to %d tickets", self.account_id, len(orders), len(tickets))
            
            # Zero-drop guard: if we previously had N positions and now see 0,
            # this is almost certainly a transient API error (connection hiccup,
            # mid-reconnect race, order-update callback firing too early).
            # Skip writing open_tickets=[] with a fresh last_update — that
            # combination would defeat the hedge monitor's 30s staleness check
            # and trigger a false cascade. Log prominently so it's visible.
            prev_tickets = info.get("open_tickets")
            if prev_tickets and len(prev_tickets) > 0 and len(tickets) == 0:
                logger.warning(
                    "[%s] _push_positions: 0 orders returned but had %d open — "
                    "skipping update to prevent false hedge cascade (transient read)",
                    self.account_id, len(prev_tickets)
                )
                return
            info["open_tickets"] = tickets
            info["positions"] = len(tickets)
            # Aggregate PnL — use AccountProfit as the PRIMARY source.
            # It is a live direct property on MT5API, always in deposit currency,
            # and is correct as soon as the server delivers account data.
            # equity-balance is kept as fallback only: AccountEquity can lag
            # behind on first push (ProcessServerMessagesInThread async delivery)
            # causing a transient pnl=0 if equity hasn't been updated yet.
            try:
                ap = getattr(self._client, 'AccountProfit', None)
                if ap is not None:
                    info["total_pnl"] = round(float(ap), 2)
                else:
                    bal = info.get("balance")
                    eq  = info.get("equity")
                    if bal is not None and eq is not None and eq > 0 and bal > 0:
                        info["total_pnl"] = round(eq - bal, 2)
                    else:
                        info["total_pnl"] = round(sum(o.get('Profit', 0) for o in orders), 2)
            except Exception:
                info["total_pnl"] = round(sum(o.get('Profit', 0) for o in orders), 2)
            info["total_swap"] = sum(o.get('Swap', 0) for o in orders)
            # Signed lots: buy = positive, sell = negative
            _buy_types = ('buy', '0', 'op_buy')
            info["total_lots"] = round(
                sum(o.get('Lots', 0) if str(o.get('Type', '')).lower() in _buy_types
                    else -o.get('Lots', 0)
                    for o in orders), 2)
            # Per-instrument lots breakdown
            _lbi = {}
            for o in orders:
                sym = o.get('Symbol', 'Unknown')
                lots = o.get('Lots', 0)
                if sym not in _lbi:
                    _lbi[sym] = {"buy": 0, "sell": 0}
                if str(o.get('Type', '')).lower() in _buy_types:
                    _lbi[sym]["buy"] = round(_lbi[sym]["buy"] + lots, 2)
                else:
                    _lbi[sym]["sell"] = round(_lbi[sym]["sell"] + lots, 2)
            info["lots_by_instrument"] = _lbi

            # Per-instrument swap breakdown
            _sbi = {}
            for o in orders:
                sym = o.get('Symbol', 'Unknown')
                swap = o.get('Swap', 0.0)
                _sbi[sym] = round(_sbi.get(sym, 0.0) + swap, 2)
            info["swap_by_instrument"] = _sbi
            # Store position details for cycle age tracking
            pos_details = []
            for o in orders:
                oe = None
                ot = o.get('OpenTime', '')
                if ot:
                    oe = _parse_open_time(ot)
                pos_details.append({
                    "ticket": o['Ticket'],
                    "symbol": o.get('Symbol', ''),
                    "comment": o.get('Comment', ''),
                    "open_epoch": oe,
                })
            info["position_details"] = pos_details
            info["last_update"] = time.time()
            self.dd["ea_account_info"][self.account_id] = info
        except Exception as e:
            logger.error("[%s] MT5 Push positions error: %s", self.account_id, e)

    def _get_open_orders(self):
        """Get list of open positions from MT5API as plain Python dicts.
        All .NET attribute access happens inside _clr_lock to prevent
        concurrent pythonnet CLR corruption."""
        if not self._client:
            return []
        try:
            # Log available attrs once for diagnostics
            if not getattr(self, '_client_attrs_logged', False):
                self._client_attrs_logged = True
                try:
                    with _clr_lock:
                        relevant = [a for a in dir(self._client)
                                    if any(k in a.lower() for k in ('order', 'position', 'trade', 'open'))]
                    logger.info("[%s] MT5 client attrs: %s", self.account_id, relevant)
                except Exception:
                    pass

            # MT5: try Positions / OpenedPositions / GetPositions first (open trades)
            for attr in ('Positions', 'OpenedPositions', 'GetPositions',
                         'GetOpenedOrders', 'Orders'):
                if not hasattr(self._client, attr):
                    continue
                try:
                    with _clr_lock:
                        raw = getattr(self._client, attr)
                        # pythonnet MethodObject: try calling first, then iterating
                        raw_list = None
                        try:
                            raw_list = list(raw())
                        except Exception:
                            try:
                                raw_list = list(raw)
                            except Exception:
                                pass
                        if raw_list is not None:  # [] is valid (0 positions) — do NOT fall through
                            # Materialize all .NET attributes into pure Python dicts
                            result = []
                            for o in raw_list:
                                result.append({
                                    'Ticket': _normalize_ticket(getattr(o, 'Ticket', getattr(o, 'Id', 0))),
                                    'Symbol': str(getattr(o, 'Symbol', '')),
                                    'Type': str(getattr(o, 'Type', getattr(o, 'PositionType', getattr(o, 'OrderType', '')))),
                                    'Lots': float(getattr(o, 'Lots', getattr(o, 'Volume', 0))) / self.lot_divisor,
                                    'Comment': str(getattr(o, 'Comment', '')),
                                    'OpenPrice': float(getattr(o, 'OpenPrice', getattr(o, 'PriceOpen', 0))),
                                    'OpenTimeRaw': getattr(o, 'OpenTime', getattr(o, 'TimeCreate', None)),
                                    'OpenTime': str(getattr(o, 'OpenTime', getattr(o, 'TimeCreate', ''))),
                                    'Profit': float(getattr(o, 'Profit', 0)),
                                    'Swap': float(getattr(o, 'Swap', 0)),
                                })
                            logger.info("[%s] MT5 %s returned %d items", self.account_id, attr, len(result))
                            return result
                except Exception as e:
                    logger.warning("[%s] MT5 %s failed: %s", self.account_id, attr, e)
            return []
        except Exception as e:
            logger.error("[%s] MT5 Get open orders error: %s", self.account_id, e)
            return []

    def _get_pending_orders(self):
        """Get list of pending orders (limits/stops) from MT5API.
        In MT5, pending orders are separate from open positions."""
        if not self._client:
            return []
        try:
            # 'Orders' typically contains the pending orders in MT5API
            for attr in ('Orders', 'GetOpenedOrders'):
                if not hasattr(self._client, attr):
                    continue
                try:
                    with _clr_lock:
                        raw = getattr(self._client, attr)
                        raw_list = None
                        try:
                            raw_list = list(raw())
                        except Exception:
                            try:
                                raw_list = list(raw)
                            except Exception:
                                pass
                        if raw_list is not None:
                            result = []
                            for o in raw_list:
                                d = {}
                                for p in dir(o):
                                    if not p.startswith('_'):
                                        try:
                                            d[p] = getattr(o, p)
                                        except Exception:
                                            pass
                                if d.get('Ticket'):
                                    result.append(d)
                            return result
                except Exception as e:
                    logger.debug("[%s] MT5 _get_pending_orders attr %s failed: %s", self.account_id, attr, e)
            return []
        except Exception as e:
            logger.error("[%s] MT5 Get pending orders error: %s", self.account_id, e)
            return []

    def get_positions_for_import(self, pair_filter="", comment_filter=""):
        """Get open positions in import-compatible format."""
        positions = []
        try:
            orders = self._get_open_orders()
            if orders and not getattr(self, '_import_attrs_logged', False):
                self._import_attrs_logged = True
                logger.info("[%s] MT5 order keys: %s", self.account_id, list(orders[0].keys()))
            for o in orders:
                symbol = o['Symbol'].upper()
                if pair_filter and not (symbol.startswith(pair_filter.upper()) or pair_filter.upper().startswith(symbol)):
                    continue
                comment = o['Comment']
                if comment_filter:
                    comment_parts = [c.strip() for c in comment_filter.split(",") if c.strip()]
                    match_blank = any(cp.lower() == "<blank>" for cp in comment_parts)
                    parts = [cp for cp in comment_parts if cp.lower() != "<blank>"]
                    if not ((match_blank and not comment.strip()) or any(cp in comment for cp in parts)):
                        continue
                otype = o['Type'].lower()
                side = "buy" if otype in ('buy', '0', 'position_type_buy') else "sell"
                oe = _parse_open_time(o.get('OpenTimeRaw') or o.get('OpenTime')) if o.get('OpenTimeRaw') or o.get('OpenTime') else None
                positions.append({
                    "ticket": o['Ticket'],
                    "symbol": symbol,
                    "lots": o['Lots'],
                    "side": side,
                    "comment": comment,
                    "open_price": o['OpenPrice'],
                    "open_time": o['OpenTime'],
                    "open_epoch": oe,
                })
            logger.info("[%s] MT5 Import: found %d positions (pair=%s comment=%s)",
                        self.account_id, len(positions), pair_filter, comment_filter)
            if positions:
                sample = positions[0]
                logger.info("[%s] MT5 Import sample pos: ticket=%s lots=%s side=%s open_time=%r open_epoch=%s",
                            self.account_id, sample.get('ticket'), sample.get('lots'), sample.get('side'), sample.get('open_time'), sample.get('open_epoch'))
        except Exception as e:
            logger.error("[%s] MT5 get_positions_for_import error: %s", self.account_id, e)
        return positions

    def get_deal_history(self, from_ts, to_ts, fee_keywords=None, **kwargs):
        """Retrieve closed deal history from the MT5 server and compute PnL totals.

        Uses the two-step MT5 flow:
          1. MT5API.RequestHistory(from, to)  — triggers async download on server
          2. MT5API.GetHistoryOrders(from, to) — retrieves cached results

        Uses a poll-retry approach between the two calls for responsiveness.

        Args:
            from_ts: Start timestamp (Unix epoch, UTC).
            to_ts:   End timestamp (Unix epoch, UTC).
            fee_keywords: List of strings to match against deal Comment
                          for fee identification (e.g. ["Holding Fee"]).

        Returns:
            dict with keys: pnl (float), swap (float), fees (float), deal_count (int),
            or None on failure.
        """
        if not self._connected or not self._client:
            logger.warning("[%s] MT5 get_deal_history: not connected", self.account_id)
            return None
        if fee_keywords is None:
            fee_keywords = []
        try:
            import System
            from datetime import datetime as _dt, timezone as _tz

            # Convert Unix timestamps to .NET DateTime.
            # Pad ±3 hours for reliability (same as MT4).
            from_dt = _dt.fromtimestamp(from_ts, tz=_tz.utc)
            to_dt = _dt.fromtimestamp(to_ts, tz=_tz.utc)
            pad_h = 3
            net_from = System.DateTime(from_dt.year, from_dt.month, from_dt.day,
                                       from_dt.hour, from_dt.minute, from_dt.second).AddHours(-pad_h)
            net_to = System.DateTime(to_dt.year, to_dt.month, to_dt.day,
                                     to_dt.hour, to_dt.minute, to_dt.second).AddHours(pad_h)

            logger.info("[%s] Requesting MT5 order history %s → %s (padded ±%dh)",
                        self.account_id, from_dt.strftime("%Y-%m-%d"), to_dt.strftime("%Y-%m-%d"), pad_h)

            # DownloadOrderHistory returns an OrderHistoryEventArgs object.
            # Access .Orders on the result to get the actual collection.
            raw_orders = []

            logger.info("[%s] MT5 using DownloadOrderHistory → Orders", self.account_id)
            _heartbeat_sem.acquire()
            try:
                with _clr_lock:
                    # Set timeout if available (default may be too short)
                    if hasattr(self._client, 'DownloadOrderHistoryTimeout'):
                        try:
                            self._client.DownloadOrderHistoryTimeout = 15000  # 15s
                        except Exception:
                            pass
                    result = self._client.DownloadOrderHistory(net_from, net_to)
                    # result is OrderHistoryEventArgs — extract .Orders
                    if result is not None:
                        if hasattr(result, 'Orders') and result.Orders is not None:
                            raw_orders = list(result.Orders)
                            logger.info("[%s] MT5 DownloadOrderHistory: %d orders from .Orders",
                                        self.account_id, len(raw_orders))
                        else:
                            # Log all attributes for diagnostics
                            result_attrs = [a for a in dir(result) if not a.startswith('_')]
                            logger.warning("[%s] MT5 DownloadOrderHistory result has no .Orders — attrs: %s",
                                           self.account_id, result_attrs[:20])
                    else:
                        logger.warning("[%s] MT5 DownloadOrderHistory returned None", self.account_id)
            finally:
                _heartbeat_sem.release()

            logger.info("[%s] MT5 history: got %d orders", self.account_id, len(raw_orders))

            if not raw_orders:
                logger.warning("[%s] MT5 history: no orders returned", self.account_id)
                return {"pnl": 0.0, "swap": 0.0, "fees": 0.0, "deal_count": 0}

            # Compute totals
            total_pnl = 0.0
            total_swap = 0.0
            total_fees = 0.0
            deal_count = 0
            fee_kw_upper = [kw.upper() for kw in fee_keywords if kw]
            by_symbol = {}  # { "EURUSD": {"pnl": 0.0, "lots": 0.0} }

            # Diagnostic: dump first order's attributes to debug filtering
            if raw_orders:
                o0 = raw_orders[0]
                with _clr_lock:
                    attrs = {}
                    for attr in ('Ticket', 'Id', 'OrderId', 'Profit', 'Swap', 'Commission',
                                 'Comment', 'Type', 'DealType', 'OrderType', 'State',
                                 'CloseTime', 'TimeCreate', 'OpenTime', 'Symbol',
                                 'Volume', 'Lots', 'ClosePrice', 'OpenPrice'):
                        try:
                            v = getattr(o0, attr, '__MISSING__')
                            if v != '__MISSING__':
                                attrs[attr] = str(v)
                        except Exception:
                            pass
                    # Also dump all .NET properties
                    try:
                        all_attrs = [a for a in dir(o0) if not a.startswith('_')]
                        attrs['__all_attrs__'] = ','.join(all_attrs[:40])
                    except Exception:
                        pass
                logger.info("[%s] MT5 history order[0] diagnostic: %s", self.account_id, attrs)

            for o in raw_orders:
                try:
                    with _clr_lock:
                        ticket = _normalize_ticket(getattr(o, 'Ticket', getattr(o, 'Id', 0)))
                        profit = float(getattr(o, 'Profit', 0))
                        swap = float(getattr(o, 'Swap', 0))
                        # Commission: some brokers use 'Commission', others use 'Fee'
                        commission = float(getattr(o, 'Commission', 0) or 0)
                        fee = float(getattr(o, 'Fee', 0) or 0)
                        comment = str(getattr(o, 'Comment', ''))
                        deal_type = str(getattr(o, 'DealType', getattr(o, 'Type', ''))).lower()
                        close_time_raw = getattr(o, 'CloseTime', getattr(o, 'TimeCreate', None))
                        sym = str(getattr(o, 'Symbol', '') or '').upper().strip()
                        # MT5 uses Volume (in lots); fall back to Lots attribute
                        lots = float(getattr(o, 'Volume', None) or getattr(o, 'Lots', 0) or 0) / self.lot_divisor

                    # Filter by close time within exact [from_ts, to_ts] range
                    if close_time_raw:
                        close_epoch = _parse_open_time(close_time_raw)
                        if close_epoch and (close_epoch < from_ts or close_epoch > to_ts):
                            continue

                    # Identify type of operation
                    is_trade = ('buy' in deal_type) or ('sell' in deal_type)
                    is_balance = ('balance' in deal_type) or ('credit' in deal_type) or deal_type in ('2', '3')

                    if is_trade:
                        total_pnl += profit
                        total_swap += swap
                        total_fees += commission + fee
                        deal_count += 1
                        
                        if sym:
                            entry = by_symbol.setdefault(sym, {"pnl": 0.0, "lots": 0.0})
                            entry["pnl"] += profit
                            entry["lots"] += lots
                    elif not is_balance:
                        # Non-trade deal (e.g. charge, storage fee)
                        total_fees += profit + commission + fee
                        deal_count += 1
                except Exception as e:
                    logger.warning("[%s] Error processing MT5 history deal: %s", self.account_id, e)
                    continue

            # Build per-symbol breakdown: hedge_lots = raw_lots / 2 (1 buy + 1 sell = 1 hedge lot)
            by_symbol_final = {}
            for sym, v in by_symbol.items():
                hedge_lots = round(v["lots"] / 2.0, 2)
                by_symbol_final[sym] = {
                    "pnl": round(v["pnl"], 2),
                    "hedge_lots": hedge_lots,
                    "pnl_per_lot": round(v["pnl"] / hedge_lots, 2) if hedge_lots > 0 else 0.0,
                }

            result = {
                "pnl": round(total_pnl, 2),
                "swap": round(total_swap, 2),
                "fees": round(total_fees, 2),
                "deal_count": deal_count,
                "by_symbol": by_symbol_final,
            }
            logger.info("[%s] MT5 deal history: %d raw, %d closed deals → pnl=%.2f swap=%.2f fees=%.2f pairs=%d",
                        self.account_id, len(raw_orders), deal_count,
                        result["pnl"], result["swap"], result["fees"], len(by_symbol_final))
            return result

        except Exception as e:
            logger.error("[%s] MT5 get_deal_history error: %s", self.account_id, e)
            return None

    def _on_quote(self, quote):
        """Handle incoming quote from MT5 server."""
        if not _mt5_init_gate.is_set():
            return  # Suppress during connection init to prevent CLR crash
        try:
            info = self.dd["ea_account_info"].get(self.account_id, {})
            symbol = str(quote.Symbol).upper() if hasattr(quote, 'Symbol') else ""
            target = (info.get("symbol") or "").upper()
            # Case-insensitive match, handle broker suffixes
            if symbol and target and (symbol.startswith(target) or target.startswith(symbol)):
                bid = float(quote.Bid) if hasattr(quote, 'Bid') else None
                ask = float(quote.Ask) if hasattr(quote, 'Ask') else None
                if bid and ask:
                    info["bid"] = bid
                    info["ask"] = ask
                    info["quote_symbol"] = target
                    pip_mult = 1000 if "JPY" in target else 100000
                    info["spread"] = round((ask - bid) * pip_mult, 1)
                    info["last_update"] = time.time()
                    self.dd["ea_account_info"][self.account_id] = info
                    self.dd["ea_heartbeats"][self.account_id] = time.time()
                    _quote_wakeup.set()  # Wake command loop immediately
        except Exception as e:
            logger.error("[%s] MT5 Quote event error: %s", self.account_id, e)

    def _on_order_update(self, update):
        """Handle order update from MT5 server.
        IMPORTANT: This runs on a .NET thread pool thread. Do NOT call
        any .NET iteration here — set flag for heartbeat loop instead."""
        if not _mt5_init_gate.is_set():
            return
        self._order_update_pending.set()

    # ── Async trade helpers ──────────────────────────────────────────────
    _request_id_counter = 0
    _request_id_lock = threading.Lock()

    def _next_request_id(self):
        """Generate a unique request ID for OrderSendAsync / OrderCloseAsync."""
        with MT5DirectAccount._request_id_lock:
            MT5DirectAccount._request_id_counter += 1
            return MT5DirectAccount._request_id_counter

    def _discover_async_methods(self):
        """One-time .NET introspection of async trade methods.
        Logs all available OrderSendAsync / OrderCloseAsync overloads
        so we can diagnose signature mismatches from the logs."""
        if getattr(self, '_async_methods_logged', False):
            return
        self._async_methods_logged = True
        try:
            with _clr_lock:
                client_type = self._client.GetType()
                methods = client_type.GetMethods()
                for m in methods:
                    name = str(m.Name)
                    if 'ordersend' in name.lower() or 'orderclose' in name.lower() or 'getid' in name.lower():
                        params = m.GetParameters()
                        param_str = ", ".join(f"{p.ParameterType.Name} {p.Name}" for p in params)
                        logger.info("[%s] MT5-API method: %s(%s) -> %s",
                                    self.account_id, name, param_str, m.ReturnType.Name)
        except Exception as e:
            logger.warning("[%s] MT5 introspection failed: %s", self.account_id, e)

    def send_market_order(self, symbol, side, lots, session_id="", comment=""):
        """Send a market order. Returns (success, ticket, price).

        Strategy:
          1. Fire-and-forget OrderSendAsync (dispatches immediately)
          2. Simultaneously: threaded OrderSend (sync, captures exact error)
          3. Poll _get_open_orders() for up to 60s to detect the fill
        """
        if not self._connected or not self._client:
            return False, 0, 0

        try:
            from mtapi.mt5 import OrderType, PlacedType, Expiration, FillPolicy, TradeType
            import System
            import threading

            # ── Pre-trade diagnostics ──
            try:
                diag = {}
                for attr in ('IsTradeAllowed', 'IsTradeDisableOnServer', 'IsTradeSession',
                             'ExecutionTimeout', 'MarginLevel', 'AccountEquity',
                             'AccountFreeMargin', 'AccountMargin', 'AccountProfit'):
                    try:
                        v = getattr(self._client, attr, '__N/A__')
                        if v != '__N/A__':
                            diag[attr] = str(v)
                    except Exception:
                        pass
                logger.info("[%s] MT5 PRE-TRADE diagnostics: %s", self.account_id, diag)
            except Exception as diag_err:
                logger.warning("[%s] MT5 diagnostics error: %s", self.account_id, diag_err)

            # ── Increase ExecutionTimeout ──
            # Default is 30000ms which causes "Trade timeout" on opens.
            # The server responds to closes within ~7s but opens take longer.
            try:
                old_timeout = getattr(self._client, 'ExecutionTimeout', 0)
                self._client.ExecutionTimeout = 120000  # 120s
                logger.info("[%s] MT5 ExecutionTimeout: %s -> 120000ms",
                            self.account_id, old_timeout)
            except Exception as to_err:
                logger.warning("[%s] Could not set ExecutionTimeout: %s",
                               self.account_id, to_err)

            # ── Get price (real-time first, fallback to cache) ──
            price = 0.0
            # Method 1: CLR GetQuote — real-time broker query (CLR-safe: command loop thread)
            try:
                quote = self._client.GetQuote(symbol)
                if quote:
                    price = float(quote.Ask) if side.lower() == "buy" else float(quote.Bid)
            except Exception:
                pass

            # Method 2: _symbol_cache — polled from SymbolsInfo (fallback)
            if price <= 0:
                sym_info = self.get_symbol_info(symbol)
                if sym_info:
                    price = sym_info["ask"] if side.lower() == "buy" else sym_info["bid"]

            # Method 3: ea_account_info — subscribed symbol data (last resort)
            if price <= 0:
                ea_info = self.dd["ea_account_info"].get(self.account_id, {})
                price = ea_info.get("ask", 0.0) if side.lower() == "buy" else ea_info.get("bid", 0.0)
                price = price or 0.0

            if price <= 0:
                logger.error("[%s] Cannot get price for %s", self.account_id, symbol)
                return False, 0, 0

            order_type = OrderType.Buy if side.lower() == "buy" else OrderType.Sell
            magic = int(self.config.get("magic_number", 777888))
            slippage = int(self.config.get("slippage", 3))
            fill_val = int(self.config.get("fill_policy", 3))

            logger.info("[%s] MT5 Sending %s %s %.2f lots @ %.5f comment=%s",
                        self.account_id, side, symbol, lots, price, comment)

            # ── Snapshot existing tickets so we can detect new fills ──
            existing_tickets = set()
            try:
                orders = self._get_open_orders()
                existing_tickets = {o['Ticket'] for o in orders if o.get('Ticket')}
            except Exception:
                pass

            # ── Event callbacks (diagnostic only — do NOT break poll) ──
            def _on_progress(*args):
                try:
                    logger.info("[%s] OnOrderProgress (open): %s", self.account_id, args)
                except Exception:
                    pass

            def _on_order_update(*args):
                try:
                    logger.info("[%s] OnOrderUpdate (open): %s", self.account_id, args)
                except Exception:
                    pass

            try:
                self._client.OnOrderProgress += _on_progress
            except Exception:
                pass
            try:
                self._client.OnOrderUpdate += _on_order_update
            except Exception:
                pass

            # ── Introspect once to log available signatures ──
            self._discover_async_methods()

            # ── Build order parameters ──
            fp = FillPolicy(fill_val)
            # DLL default is TradeType.Transfer — NOT MarketExecution!
            # Using MarketExecution(3) caused silent broker rejection.
            try:
                tt = TradeType.Transfer
                logger.info("[%s] Using TradeType.Transfer (DLL default)", self.account_id)
            except Exception:
                trade_val = int(self.config.get("trade_type", 0))
                tt = TradeType(trade_val)
                logger.info("[%s] TradeType.Transfer not found, using TradeType(%d)",
                            self.account_id, trade_val)
            try:
                exp = Expiration(System.Int32(0))
            except Exception:
                exp = Expiration()

            # One-time enum introspection
            if not getattr(self, '_enums_logged', False):
                self._enums_logged = True
                try:
                    fp_members = {str(k): int(v) for k, v in
                                  {n: FillPolicy.__dict__.get(n)
                                   for n in dir(FillPolicy)
                                   if not n.startswith('_')}.items()
                                  if isinstance(v, int)}
                except Exception:
                    fp_members = {}
                try:
                    # Try to get all FillPolicy values
                    fp_members2 = {}
                    for i in range(10):
                        try:
                            fp_members2[str(FillPolicy(i))] = i
                        except Exception:
                            pass
                    if fp_members2:
                        fp_members = fp_members2
                except Exception:
                    pass
                try:
                    tt_members = {}
                    for i in range(10):
                        try:
                            tt_members[str(TradeType(i))] = i
                        except Exception:
                            pass
                except Exception:
                    tt_members = {}
                logger.info("[%s] MT5 FillPolicy enum: %s", self.account_id, fp_members)
                logger.info("[%s] MT5 TradeType enum: %s", self.account_id, tt_members)

                # Try to get symbol execution info from GetMarketWatch
                try:
                    mw = self._client.GetMarketWatch()
                    if mw:
                        for sym in mw:
                            if str(getattr(sym, 'Name', '')).upper() == symbol.upper():
                                sym_attrs = {}
                                for a in dir(sym):
                                    if not a.startswith('_'):
                                        try:
                                            sym_attrs[a] = str(getattr(sym, a))
                                        except Exception:
                                            pass
                                logger.info("[%s] MT5 Symbol %s info: %s",
                                            self.account_id, symbol, sym_attrs)
                                break
                except Exception as mw_err:
                    logger.warning("[%s] GetMarketWatch failed: %s",
                                   self.account_id, mw_err)

            logger.info("[%s] MT5 order params: type=%s fill=%s(%d) trade=%s "
                        "dev=%d magic=%d",
                        self.account_id, order_type, fp, fill_val, tt,
                        slippage, magic)

            # ── Threaded sync OrderSend (with 120s ExecutionTimeout) ──
            sync_result = [None]  # [Order or None]
            sync_error = [None]   # [str or None]
            sync_done = threading.Event()
            # Cancellation flag: set True when the poll loop gives up (timeout or
            # early fill detected via GetOpenedOrders). The bg thread checks this
            # before storing sync_result so a late-arriving OrderSend response
            # cannot race with a retry and produce a ghost position.
            _bg_cancelled = [False]

            def _bg_sync_open():
                try:
                    logger.info("[%s] OrderSend (sync thread) starting...",
                                self.account_id)
                    result = self._client.OrderSend(
                        symbol, float(lots), float(price), order_type,
                        0.0, 0.0, System.UInt64(slippage), comment or "",
                        System.Int64(magic), fp, tt,
                        0.0, exp, System.Int64(0), PlacedType.Manually
                    )
                    if result:
                        ticket_val = 0
                        result_attrs = {}
                        try:
                            for a in dir(result):
                                if not a.startswith('_'):
                                    try:
                                        result_attrs[a] = str(getattr(result, a))
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                        try:
                            ticket_val = int(getattr(result, 'Ticket',
                                                     getattr(result, 'Id', 0)))
                        except Exception:
                            pass
                        logger.info("[%s] OrderSend (sync) returned: ticket=%s attrs=%s",
                                    self.account_id, ticket_val, result_attrs)
                        if ticket_val > 0:
                            if _bg_cancelled[0]:
                                # Poll loop already timed out and reported error/filled via
                                # GetOpenedOrders — suppress this late callback to prevent
                                # a ghost position being opened for the SAME cycle step.
                                logger.warning(
                                    "[%s] OrderSend (sync) late response SUPPRESSED "
                                    "(poll loop already resolved): ticket=%s",
                                    self.account_id, ticket_val)
                            else:
                                sync_result[0] = result
                    else:
                        logger.warning("[%s] OrderSend (sync) returned None/empty",
                                       self.account_id)
                except Exception as sync_err:
                    err_str = str(sync_err)
                    sync_error[0] = err_str
                    logger.error("[%s] OrderSend (sync) FAILED: %s",
                                 self.account_id, err_str)
                    # Dump full .NET exception chain
                    try:
                        if hasattr(sync_err, 'InnerException') and sync_err.InnerException:
                            logger.error("[%s] OrderSend InnerException: %s",
                                         self.account_id, sync_err.InnerException)
                        if hasattr(sync_err, 'clsException'):
                            logger.error("[%s] OrderSend .NET Exception: %s",
                                         self.account_id, sync_err.clsException)
                    except Exception:
                        pass
                finally:
                    sync_done.set()

            threading.Thread(target=_bg_sync_open, daemon=True,
                             name=f"MT5-SyncOpen-{self.account_id}").start()

            # ── Poll for new position (event-driven, up to 60s) ──
            _confirm_start = time.time()
            _confirm_timeout = 60  # total seconds
            _poll_count = 0
            while (time.time() - _confirm_start) < _confirm_timeout:
                # Wait for OnOrderUpdate event instead of blind sleep
                self._order_update_pending.clear()
                self._order_update_pending.wait(timeout=0.5)
                _poll_count += 1

                # Check if sync thread got a result
                if sync_done.is_set() and sync_result[0]:
                    try:
                        ticket_val = int(getattr(sync_result[0], 'Ticket',
                                                 getattr(sync_result[0], 'Id', 0)))
                        if ticket_val > 0:
                            fill_price = float(getattr(sync_result[0], 'OpenPrice',
                                                       getattr(sync_result[0], 'PriceOpen', price)))
                            logger.info("[%s] MT5 FILLED (sync): ticket=%d %s %s "
                                        "%.2f @ %.5f after %.1fs",
                                        self.account_id, ticket_val, side, symbol,
                                        lots, fill_price, time.time() - _confirm_start)
                            _bg_cancelled[0] = True  # prevent bg thread double-report
                            self._unsubscribe_open_events(_on_progress, _on_order_update)
                            self._report_result(session_id, "filled", ticket_val,
                                                fill_price=fill_price, quote_price=price)
                            return True, ticket_val, fill_price
                    except Exception:
                        pass

                # Check if sync thread failed with a meaningful error
                if sync_done.is_set() and sync_error[0] and (time.time() - _confirm_start) > 5:
                    pass

                # Poll GetOpenedOrders for new tickets
                try:
                    orders = self._get_open_orders()
                    current_tickets = {o['Ticket'] for o in orders if o.get('Ticket')}
                    new_tickets = current_tickets - existing_tickets
                    if new_tickets:
                        new_ticket = list(new_tickets)[0]
                        new_order = next(
                            (o for o in orders if o['Ticket'] == new_ticket), None)
                        fill_price = (new_order.get('OpenPrice', price)
                                      if new_order else price)
                        logger.info("[%s] MT5 FILLED (event-driven): ticket=%d %s %s "
                                    "%.2f @ %.5f after %d polls (%.1fs)",
                                    self.account_id, new_ticket, side, symbol,
                                    lots, fill_price, _poll_count,
                                    time.time() - _confirm_start)
                        _bg_cancelled[0] = True  # prevent bg thread double-report
                        self._unsubscribe_open_events(_on_progress, _on_order_update)
                        self._report_result(session_id, "filled", new_ticket,
                                            fill_price=fill_price, quote_price=price)
                        return True, new_ticket, fill_price
                except Exception:
                    pass

            # ── Unsubscribe callbacks ──
            self._unsubscribe_open_events(_on_progress, _on_order_update)

            # ── Final check ──
            try:
                orders = self._get_open_orders()
                current_tickets = {o['Ticket'] for o in orders if o.get('Ticket')}
                new_tickets = current_tickets - existing_tickets
                if new_tickets:
                    new_ticket = list(new_tickets)[0]
                    new_order = next(
                        (o for o in orders if o['Ticket'] == new_ticket), None)
                    fill_price = (new_order.get('OpenPrice', price)
                                  if new_order else price)
                    logger.info("[%s] MT5 FILLED (final check): ticket=%d",
                                self.account_id, new_ticket)
                    _bg_cancelled[0] = True  # prevent bg thread double-report
                    self._report_result(session_id, "filled", new_ticket,
                                        fill_price=fill_price, quote_price=price)
                    return True, new_ticket, fill_price
            except Exception:
                pass

            # Log the sync error if we have one
            if sync_error[0]:
                logger.error("[%s] MT5 Open FAILED — sync error was: %s",
                             self.account_id, sync_error[0])

            logger.error("[%s] MT5 Open: no new position after 60s", self.account_id)
            # Set cancelled BEFORE reporting error so the bg thread (which may still
            # be running its blocking OrderSend) cannot race and call _report_result
            # a second time after the retry is already dispatched.
            _bg_cancelled[0] = True
            self._report_result(session_id, "error", 0,
                                detail="OrderSend timed out — no new position")
            return False, 0, 0

        except Exception as e:
            logger.error("[%s] MT5 send_market_order error: %s", self.account_id, e)
            self._report_result(session_id, "error", 0, detail=str(e))
            return False, 0, 0

    def _unsubscribe_open_events(self, on_progress, on_order_update):
        """Helper to unsubscribe event callbacks."""
        try:
            self._client.OnOrderProgress -= on_progress
        except Exception:
            pass
        try:
            self._client.OnOrderUpdate -= on_order_update
        except Exception:
            pass

    def close_position(self, ticket, symbol, side, lots, session_id="", comment=""):
        """Close a specific position by ticket.

        Uses OrderCloseAsync (non-blocking) as the primary path to bypass
        MarketCloseWaiter timeouts. Falls back to threaded sync OrderSend
        (with closeByTicket) if async is unavailable.
        Confirms the close by polling _get_open_orders().
        """
        if not self._connected or not self._client:
            return False

        try:
            import System
            import threading
            import time as _time
            from mtapi.mt5 import OrderType, PlacedType, FillPolicy, TradeType, Expiration

            ticket = int(ticket)
            slippage = int(self.config.get("slippage", 3))
            magic = int(self.config.get("magic_number", 0))
            fill_val = int(self.config.get("fill_policy", 3))
            trade_val = int(self.config.get("trade_type", 3))

            # Close direction = OPPOSITE of position direction
            if side.lower() == "buy":
                close_type = OrderType.Sell
            else:
                close_type = OrderType.Buy

            # Position type (the type we're closing, NOT the close direction)
            pos_type = OrderType.Buy if side.lower() == "buy" else OrderType.Sell

            # Get price for close — MUST use the actual instrument's quote,
            # not ea_account_info which only tracks the subscribed symbol.
            close_price = 0.0
            try:
                sym_info = self.get_symbol_info(symbol)
                if sym_info:
                    close_price = (sym_info["bid"] if side.lower() == "buy"
                                   else sym_info["ask"])
            except Exception:
                pass
            if close_price <= 0:
                # Fallback: ea_info ONLY if the position symbol matches subscribed symbol
                ea_info = self.dd["ea_account_info"].get(self.account_id, {})
                subscribed = ea_info.get("symbol", "").upper()
                if subscribed and subscribed == symbol.upper():
                    if side.lower() == "buy":
                        close_price = ea_info.get("bid", 0.0) or 0.0
                    else:
                        close_price = ea_info.get("ask", 0.0) or 0.0

            fp = FillPolicy(fill_val)

            logger.info("[%s] MT5 Closing: ticket=%d %s %s %.2f lots @ %.5f "
                        "(close_type=%s, fill=%s)",
                        self.account_id, ticket, symbol, side, lots,
                        close_price, close_type, fp)

            # ── OnOrderProgress callback (diagnostic only) ──
            def _on_progress(*args):
                """Callback fired by mtapi when order execution progresses."""
                try:
                    logger.info("[%s] OnOrderProgress (close): %s",
                                self.account_id, args)
                except Exception:
                    pass

            try:
                self._client.OnOrderProgress += _on_progress
            except Exception as sub_err:
                logger.warning("[%s] Could not subscribe to OnOrderProgress: %s",
                               self.account_id, sub_err)

            # ── Introspect once ──
            self._discover_async_methods()

            # ── Close via OrderCloseAsync (fire-and-forget) ──
            # Switched from OrderCloseAsyncTask (which blocks on MarketCloseWaiter)
            # to OrderCloseAsync for robustness. Confirmation via position poll.
            # Signature (from .NET reflection):
            #   OrderCloseAsync(Int32 requestId, Int64 ticket, String symbol,
            #                   Double price, Double lots, OrderType type,
            #                   UInt64 deviation, FillPolicy fillPolicy,
            #                   Int64 expertId, String comment,
            #                   Int64 closeByTicket, PlacedType placedType) -> Void
            req_id = self._next_request_id()
            logger.info("[%s] OrderCloseAsync: reqId=%d ticket=%d symbol=%s "
                        "price=%.5f lots=%.2f type=%s dev=%d fill=%s",
                        self.account_id, req_id, ticket, symbol,
                        close_price, lots, pos_type, slippage, fp)

            try:
                self._client.OrderCloseAsync(
                    System.Int32(req_id),
                    System.Int64(ticket),
                    symbol,
                    float(close_price),
                    float(lots),
                    pos_type,
                    System.UInt64(slippage),
                    fp,
                    System.Int64(magic),
                    comment or "",
                    System.Int64(0),
                    PlacedType.Manually,
                )
                logger.info("[%s] OrderCloseAsync dispatched OK (reqId=%d)",
                            self.account_id, req_id)
            except Exception as async_err:
                logger.error("[%s] OrderCloseAsync failed: %s", self.account_id, async_err)
                # Fallback: threaded sync OrderClose
                def _bg_sync_close():
                    try:
                        result = self._client.OrderClose(
                            System.Int64(ticket),
                            symbol,
                            float(close_price),
                            float(lots),
                            pos_type,
                            System.UInt64(slippage),
                            fp,
                            System.Int64(magic),
                            comment or "",
                            System.Int64(0),
                            PlacedType.Manually,
                        )
                        logger.info("[%s] OrderClose (sync fallback) result: %s",
                                    self.account_id, result)
                    except Exception as sync_err:
                        logger.error("[%s] OrderClose (sync fallback) error: %s",
                                     self.account_id, sync_err)
                threading.Thread(target=_bg_sync_close, daemon=True,
                                 name=f"MT5-SyncClose-{self.account_id}").start()

            # ── Poll for position disappearance (event-driven, up to 30s) ──
            # 30s gives slow brokers enough time to confirm without timing out and
            # triggering the external exec_retry_close path, which can race with a
            # delayed close confirmation and produce a duplicate open order.
            _confirm_start = time.time()
            _confirm_timeout = 30  # total seconds (was 15)
            _poll_count = 0
            while (time.time() - _confirm_start) < _confirm_timeout:
                # Wait for OnOrderUpdate event instead of blind sleep
                # Clear first so we catch the NEXT event, not a stale one
                self._order_update_pending.clear()
                self._order_update_pending.wait(timeout=0.5)
                _poll_count += 1

                # Poll GetOpenedOrders to check if position gone
                try:
                    orders = self._get_open_orders()
                    still_open = any(
                        o.get('Ticket') and int(o['Ticket']) == ticket
                        for o in orders
                    )
                    if not still_open:
                        logger.info("[%s] MT5 CLOSED (event-driven): "
                                    "ticket=%d after %d polls (%.1fs)",
                                    self.account_id, ticket, _poll_count,
                                    time.time() - _confirm_start)
                        self._report_result(
                            session_id,
                            "rollback_closed" if session_id else "closed",
                            ticket, fill_price=close_price, quote_price=close_price)
                        try:
                            self._client.OnOrderProgress -= _on_progress
                        except Exception:
                            pass
                        return True
                except Exception:
                    pass

            # ── Unsubscribe callback ──
            try:
                self._client.OnOrderProgress -= _on_progress
            except Exception:
                pass

            # ── Final check ──
            try:
                orders = self._get_open_orders()
                still_open = any(
                    o.get('Ticket') and int(o['Ticket']) == ticket
                    for o in orders)
                if not still_open:
                    logger.info("[%s] MT5 CLOSED (final check): ticket=%d",
                                self.account_id, ticket)
                    self._report_result(
                        session_id,
                        "rollback_closed" if session_id else "closed",
                        ticket, fill_price=close_price, quote_price=close_price)
                    return True
            except Exception:
                pass

            logger.error("[%s] MT5 Close: ticket=%d still open after 45s",
                         self.account_id, ticket)
            self._report_result(session_id, "error", ticket,
                                detail="Close sent but position still open after 45s")
            return False

        except Exception as e:
            err_str = str(e)
            logger.error("[%s] MT5 Close error: %s", self.account_id, err_str)
            if "already been closed" in err_str.lower():
                logger.info("[%s] Position %d already closed",
                            self.account_id, ticket)
                self._report_result(session_id, "closed", ticket, fill_price=0)
                return True
            self._report_result(session_id, "error", ticket, detail=err_str)
            return False

    def modify_position_tp(self, ticket, symbol, side, lots, tp, sl=None, price=None):
        """Modify the TakeProfit (and optionally StopLoss) of an open position.
        Used by CLOSE-LIMIT mode to set a passive TP instead of a market close."""
        if not self._connected or not self._client:
            logger.error("[%s] MT5 modify_position_tp blocked: not connected", self.account_id)
            return False, "Not connected"
        try:
            from mtapi.mt5 import OrderType, Expiration
            import System
            ticket = int(ticket)
            order_type = OrderType.Buy if side.lower() == "buy" else OrderType.Sell
            sl_val = float(sl) if sl is not None else 0.0
            tp_val = float(tp) if tp is not None else 0.0
            
            # Get current price for the modify call if a new price wasn't specified
            if price is not None:
                target_price = float(price)
            else:
                quote = self._client.GetQuote(symbol)
                target_price = float(quote.Bid if order_type == OrderType.Buy else quote.Ask) if quote else 0.0
                
            self._client.OrderModify(
                System.Int64(ticket), symbol, float(lots), target_price, order_type,
                sl_val, tp_val, System.Int64(0), 0.0, Expiration(), ""
            )
            logger.info("[%s] MT5 position modified: ticket=%d price=%.5f tp=%.5f", self.account_id, ticket, target_price, tp_val)
            return True, ticket
        except Exception as e:
            logger.error("[%s] MT5 modify_position_tp error: %s", self.account_id, e)
            return False, str(e)

    def modify_limit_price(self, ticket, symbol, side, lots, price):
        """Modify the entry price of a pending limit order."""
        return self.modify_position_tp(ticket, symbol, side, lots, tp=None, sl=None, price=price)

    def send_limit_order(self, symbol, side, lots, price, limit_type, session_id="", comment=""):
        """Send a pending limit order (BuyLimit/SellLimit).
        Used by OPEN-LIMIT mode."""
        if not self._connected or not self._client:
            logger.error("[%s] MT5 send_limit_order blocked: not connected", self.account_id)
            return False, 0, 0
        try:
            from mtapi.mt5 import OrderType, FillPolicy
            import System
            if limit_type.lower() in ("buylimit", "buy_limit"):
                order_type = OrderType.BuyLimit
            else:
                order_type = OrderType.SellLimit
            magic = int(self.config.get("magic_number", 777888))
            logger.info("[%s] MT5 Sending %s %s %.2f lots @ %.5f (limit)", self.account_id, limit_type, symbol, lots, price)
            policies = [FillPolicy.ImmediateOrCancel, FillPolicy.FillOrKill, FillPolicy.FlashFill, FillPolicy.Any]

            # Snapshot all current open/pending tickets BEFORE this limit is placed.
            # The watcher uses this to identify which tickets are genuinely new, even if
            # the limit order fills instantly before the first watcher poll.
            try:
                with _clr_lock:
                    _snap = self._get_open_orders()
                _pre_batch_tickets = set(int(o['Ticket']) for o in _snap if o.get('Ticket'))
            except Exception:
                _pre_batch_tickets = set()

            order = None
            last_ex = None
            for fp in policies:
                try:
                    order = self._client.OrderSend(
                        symbol, float(lots), float(price), order_type,
                        0.0, 0.0, System.Int64(1000), comment, System.Int64(magic), fp
                    )
                    break
                except Exception as ex:
                    last_ex = ex
                    continue
            if order and order.Ticket > 0:
                ticket = _normalize_ticket(order.Ticket)
                logger.info("[%s] MT5 LIMIT PLACED: ticket=%d %s %s %.2f @ %.5f", self.account_id, ticket, limit_type, symbol, lots, price)
                self._report_result(session_id, "limit_placed", ticket, fill_price=float(price), quote_price=float(price))

                # ── Background fill-watcher ──────────────────────────────────────
                # MT5 executes the pending order asynchronously when market hits it.
                # Poll every 2s for the pending order to disappear, then report "filled"
                # with the new position ticket. This is the ONLY reliable detection path
                # for cycle_limit fills — broker strips comments so auto-heal can't match.
                _pending_ticket = ticket
                _watch_symbol = symbol
                _watch_lots = float(lots)
                _watch_side = side.lower()

                def _watch_limit_fill():
                    import time as _time
                    _timeout = 86400  # watch for up to 24h
                    _start = _time.time()
                    _poll_interval = 2.0
                    logger.info("[%s] LIMIT-WATCH: watching ticket=%d for fill (symbol=%s side=%s lots=%.2f) pre_snap=%d tickets",
                                self.account_id, _pending_ticket, _watch_symbol, _watch_side, _watch_lots, len(_pre_batch_tickets))
                    _prev_positions = set()
                    while _time.time() - _start < _timeout:
                        _time.sleep(_poll_interval)
                        if not self._connected or not self._client:
                            logger.warning("[%s] LIMIT-WATCH: disconnected — stopping watcher for ticket=%d",
                                           self.account_id, _pending_ticket)
                            return
                        try:
                            with _clr_lock:
                                orders = self._get_open_orders()
                                pending_orders = self._get_pending_orders()
                            # Check if pending order is still there (in the pending orders list)
                            still_pending = any(
                                o.get('Ticket') and int(o['Ticket']) == _pending_ticket
                                for o in pending_orders
                            )
                            if still_pending:
                                # Still waiting — continue polling
                                _prev_positions = set(
                                    int(o['Ticket']) for o in orders
                                    if o.get('Ticket') and o.get('Type') in (None, 'Buy', 'Sell', 0, 1)
                                )
                                continue

                            # Pending order gone — find the new filled position
                            logger.info("[%s] LIMIT-WATCH: pending ticket=%d gone — searching for filled position",
                                        self.account_id, _pending_ticket)
                            # Get current positions and find new ones matching symbol/lots/side
                            sym_upper = _watch_symbol.upper().replace(".", "")
                            new_ticket = 0
                            new_price = price

                            # Use a lock-guarded claimed-ticket set to atomically prevent
                            # multiple concurrent watchers from claiming the same position
                            # when a batch of limits all fill at the same time.
                            new_ticket = 0
                            new_price = price
                            with self._claimed_fill_lock:
                                for o in orders:
                                    if not o.get('Ticket'):
                                        continue
                                    t = int(o['Ticket'])
                                    if t == _pending_ticket:
                                        continue
                                    if t in self._claimed_fill_tickets:
                                        continue  # Already claimed by another watcher
                                    o_sym = str(o.get('Symbol', '') or o.get('symbol', '')).upper().replace(".", "")
                                    if sym_upper not in o_sym and o_sym not in sym_upper:
                                        continue
                                    o_lots = float(o.get('Lots') or o.get('Volume') or 0)
                                    if abs(o_lots - _watch_lots) > _watch_lots * 0.01 + 0.001:
                                        continue
                                    # Atomically claim this ticket
                                    new_ticket = t
                                    new_price = float(o.get('OpenPrice') or o.get('PriceOpen') or price)
                                    self._claimed_fill_tickets.add(t)
                                    break

                                if new_ticket == 0:
                                    # Fallback: newest unclaimed position on this symbol that didn't exist before the batch
                                    for o in orders:
                                        if not o.get('Ticket'):
                                            continue
                                        o_sym = str(o.get('Symbol', '') or o.get('symbol', '')).upper().replace(".", "")
                                        if sym_upper in o_sym or o_sym in sym_upper:
                                            t = int(o['Ticket'])
                                            if t != _pending_ticket and t not in _pre_batch_tickets and t not in self._claimed_fill_tickets:
                                                new_ticket = t
                                                new_price = float(o.get('OpenPrice') or o.get('PriceOpen') or price)
                                                self._claimed_fill_tickets.add(t)
                                                break

                            if new_ticket > 0:
                                logger.info("[%s] LIMIT-WATCH: FILLED! pending=%d -> position=%d @ %.5f",
                                            self.account_id, _pending_ticket, new_ticket, new_price)
                                self._report_result(session_id, "filled", new_ticket,
                                                    fill_price=new_price, quote_price=new_price)
                            else:
                                # Order filled but couldn't find unclaimed position — report with original ticket
                                logger.warning("[%s] LIMIT-WATCH: pending=%d gone but no unclaimed position found — reporting fill with original ticket",
                                               self.account_id, _pending_ticket)
                                self._report_result(session_id, "filled", _pending_ticket,
                                                    fill_price=price, quote_price=price)
                            return  # Done watching
                        except Exception as _e:
                            logger.debug("[%s] LIMIT-WATCH poll error: %s", self.account_id, _e)
                            continue
                    logger.warning("[%s] LIMIT-WATCH: timed out watching ticket=%d", self.account_id, _pending_ticket)

                import threading as _threading
                _t = _threading.Thread(target=_watch_limit_fill, daemon=True,
                                       name=f"LimitWatch-{self.account_id}-{ticket}")
                _t.start()
                # ─────────────────────────────────────────────────────────────────

                return True, ticket, float(price)
            else:
                err = str(last_ex) if last_ex else "OrderSend returned no ticket"
                logger.error("[%s] MT5 limit OrderSend failed: %s", self.account_id, err)
                self._report_result(session_id, "error", 0, detail=err)
                return False, 0, 0
        except Exception as e:
            logger.error("[%s] MT5 send_limit_order error: %s", self.account_id, e)
            self._report_result(session_id, "error", 0, detail=str(e))
            return False, 0, 0


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
            logger.error("[%s] MT5 Report result error: %s", self.account_id, e)

    def get_symbol_info(self, symbol):
        """Get bid/ask/spread for a symbol.
        Reads from _symbol_cache (polled from SymbolsInfo during _push_account_info).
        CLR-safe: pure Python dict read."""
        sym_upper = symbol.upper()
        cached = self._symbol_cache.get(sym_upper)
        if cached:
            return dict(cached)
        return None

    def get_quote_direct(self, symbol):
        """Get a live bid/ask for *any* symbol via CLR GetQuote.
        Slower than get_symbol_info (hits the .NET API) but works even
        if the symbol is not in SymbolsInfo / Market Watch.
        Returns {bid, ask, spread} or None."""
        if not self._connected or not self._client:
            return None
        try:
            with _clr_lock:
                quote = self._client.GetQuote(symbol)
                if quote:
                    bid = float(quote.Bid)
                    ask = float(quote.Ask)
                    if bid > 0 and ask > 0:
                        pip_mult = 1000 if "JPY" in symbol.upper() else 100000
                        spd = round((ask - bid) * pip_mult, 1)
                        return {"bid": bid, "ask": ask, "spread": spd}
        except Exception:
            pass
        return None

    def get_swap_rates(self, symbols):
        """Get swap long/short rates for a list of symbols.
        Returns dict: {symbol: {swap_long, swap_short}} or empty dict."""
        result = {}
        if not self._client or not self._connected:
            return result
        # Try _symbol_cache first
        cache = getattr(self, '_symbol_cache', {})
        if cache:
            symbols_lookup = {s.upper(): s for s in symbols}
            for sym_upper, original_name in symbols_lookup.items():
                cached = cache.get(sym_upper)
                if cached:
                    result[original_name] = {
                        "swap_long": cached.get("swap_long", 0),
                        "swap_short": cached.get("swap_short", 0),
                    }
            if result:
                return result
        # Fallback: query CLR directly per symbol
        try:
            for sym in symbols:
                try:
                    quote = self._client.GetQuote(sym)
                    if not quote:
                        continue
                    if not getattr(self, '_swap_props_logged', False):
                        props = [a for a in dir(quote) if not a.startswith('_')]
                        logger.info("[%s] MT5 GetQuote(%s) properties: %s", self.account_id, sym, props)
                        self._swap_props_logged = True
                    sl = getattr(quote, 'SwapLong', None) or getattr(quote, 'SwapBuy', None)
                    ss = getattr(quote, 'SwapShort', None) or getattr(quote, 'SwapSell', None)
                    if sl is not None or ss is not None:
                        result[sym] = {
                            "swap_long": float(sl) if sl is not None else 0,
                            "swap_short": float(ss) if ss is not None else 0,
                        }
                    else:
                        result[sym] = {"swap_long": 0, "swap_short": 0}
                except Exception as e:
                    logger.debug("[%s] MT5 GetQuote(%s) swap failed: %s", self.account_id, sym, e)
        except Exception as e:
            logger.error("[%s] MT5 get_swap_rates CLR error: %s", self.account_id, e)
        return result

    def subscribe_symbol(self, symbol):
        """Subscribe to a symbol for quote updates."""
        info = self.dd["ea_account_info"].get(self.account_id, {})
        info["symbol"] = symbol
        self.dd["ea_account_info"][self.account_id] = info
        # Update QuoteBuffer target symbol so C# filters correctly
        global _quote_buffer
        if _quote_buffer is not None:
            try:
                _quote_buffer.SetTargetSymbol(self.account_id, symbol)
            except Exception:
                pass
        sym_info = self.get_symbol_info(symbol)
        if sym_info:
            # Only update non-None values to avoid clearing existing data
            for k, v in sym_info.items():
                if v is not None:
                    info[k] = v


# ─── MT Direct Manager ─────────────────────────────────────────────────────
class MTDirectManager:
    """
    Manages all MT4/MT5 Direct connections.
    Follows the same pattern as FixAccountManager.
    """

    CONFIG_FILE = "mt_direct_accounts.json"

    def __init__(self, dashboard_data, config_dir="."):
        self.dd = dashboard_data
        self.config_path = os.path.join(config_dir, self.CONFIG_FILE)
        self.accounts = {}  # account_id -> MT4DirectAccount | MT5DirectAccount
        self._running = False
        self._command_thread = None
        self._lock = threading.Lock()

    def load_config(self):
        """Load Direct accounts from config file."""
        if not os.path.exists(self.config_path):
            logger.info("No MT Direct config file found at %s", self.config_path)
            return
        try:
            with open(self.config_path, "r") as f:
                configs = json.load(f)
            for acct_id, config in configs.items():
                self.add_account(acct_id, config, save=False, auto_connect=False)
            # Persist any inferred types (e.g. mt5 from account_id) back to config
            self.save_config()
            logger.info("Loaded %d MT Direct account(s) from config", len(configs))
            # Serial batch connect — MT4 and MT5 share the CLR thread pool,
            # so connections must be serialized to prevent thread pool
            # exhaustion and CLR corruption (0x80131506).
            # Failed accounts still auto-reconnect via their heartbeat loop.
            def _batch_connect():
                MAX_ROUNDS = 3
                RETRY_DELAY = 15
                to_connect = [aid for aid, cfg in configs.items()
                              if cfg.get("auto_connect_start", True)]
                logger.info("Batch connecting %d account(s) (max %d rounds)...",
                            len(to_connect), MAX_ROUNDS)
                failed = list(to_connect)
                for round_num in range(1, MAX_ROUNDS + 1):
                    if not failed:
                        break
                    if round_num > 1:
                        logger.info("Batch connect round %d/%d — retrying %d failed account(s) in %ds...",
                                    round_num, MAX_ROUNDS, len(failed), RETRY_DELAY)
                        time.sleep(RETRY_DELAY)
                    still_failed = []
                    for i, aid in enumerate(failed):
                        try:
                            logger.info("[%s] Batch connect round %d — %d/%d...",
                                        aid, round_num, i + 1, len(failed))
                            ok, err = self.connect_account(aid)
                            if ok:
                                logger.info("[%s] Batch connect succeeded (round %d)",
                                            aid, round_num)
                            else:
                                logger.warning("[%s] Batch connect failed (round %d): %s",
                                               aid, round_num, err)
                                still_failed.append(aid)
                        except Exception as e:
                            logger.error("[%s] Batch connect error (round %d): %s", aid, round_num, e)
                            still_failed.append(aid)
                    failed = still_failed

                if failed:
                    logger.warning("Batch connect: %d account(s) still failed after %d rounds "
                                   "— heartbeat threads will keep retrying: %s",
                                   len(failed), MAX_ROUNDS, failed)
                else:
                    logger.info("Batch connect complete — all %d account(s) connected",
                                len(to_connect))

            threading.Thread(target=_batch_connect, daemon=True,
                             name="BatchConnect").start()
        except Exception as e:
            logger.error("Failed to load MT Direct config: %s", e)

    def save_config(self):
        """Save Direct accounts to config file atomically."""
        configs = {}
        for acct_id, account in self.accounts.items():
            configs[acct_id] = account.config
        try:
            import tempfile
            dir_name = os.path.dirname(self.config_path)
            # Create a temp file in the same directory to ensure it's on the same drive (required for atomic rename)
            with tempfile.NamedTemporaryFile('w', dir=dir_name, delete=False, suffix='.tmp') as tf:
                json.dump(configs, tf, indent=2)
                temp_name = tf.name
            try:
                os.replace(temp_name, self.config_path)
            except Exception:
                if os.path.exists(temp_name):
                    os.unlink(temp_name)
                raise
        except Exception as e:
            logger.error("Failed to save MT Direct config: %s", e)

    def add_account(self, account_id, config, save=True, auto_connect=True):
        """Add a new MT Direct account."""
        with self._lock:
            if account_id in self.accounts:
                logger.warning("MT Direct account %s already exists", account_id)
                return False

            mt_type = config.get("type", "").lower()
            if not mt_type:
                # Infer from account_id (e.g. "DEMO-MT5-52769010")
                mt_type = "mt5" if "MT5" in account_id.upper() else "mt4"
                config["type"] = mt_type  # Persist so it's saved to config
            if mt_type == "mt5":
                account = MT5DirectAccount(account_id, config, self.dd)
            else:
                account = MT4DirectAccount(account_id, config, self.dd)

            self.accounts[account_id] = account

        if save:
            self.save_config()

        if auto_connect:
            threading.Thread(target=account.start, daemon=True,
                             name=f"MTDirect-Start-{account_id}").start()
        return True

    def remove_account(self, account_id):
        """Stop and remove a Direct account."""
        with self._lock:
            account = self.accounts.pop(account_id, None)
        if account:
            account.stop()
            # Clean up stale data from dashboard dicts
            self.dd.get("ea_account_info", {}).pop(account_id, None)
            self.dd.get("ea_heartbeats", {}).pop(account_id, None)
            self.save_config()
            return True
        return False

    def connect_account(self, account_id):
        """Connect a specific account. Synchronous — waits for result."""
        account = self.accounts.get(account_id)
        if not account:
            return False, "Account not found"
        ok = account.start()
        if ok:
            return True, None
        return False, account._last_error or "Connection failed (unknown error)"

    def disconnect_account(self, account_id):
        """Disconnect a specific account."""
        account = self.accounts.get(account_id)
        if not account:
            return False
        account.stop()
        return True

    def get_status(self):
        """Get status of all Direct accounts."""
        result = {}
        for acct_id, acct in self.accounts.items():
            info = self.dd["ea_account_info"].get(acct_id, {})
            result[acct_id] = {
                "label": acct.label,
                "type": acct.conn_type,
                "connected": acct.connected,
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "margin": info.get("margin"),
                "leverage": info.get("leverage"),
                "total_pnl": info.get("total_pnl"),
                "total_swap": info.get("total_swap"),
                "total_lots": info.get("total_lots"),
                "positions": info.get("positions"),
                "server": acct.config.get("server", ""),
                "login": acct.config.get("login", ""),
                "last_error": acct._last_error,
            }
        return result

    def start(self):
        """Load config and start command loop."""
        self.load_config()
        self._running = True
        self._command_thread = threading.Thread(
            target=self._command_loop, daemon=True, name="MTDirect-CmdLoop"
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
        sent via Direct API (instead of waiting for EA polling).
        """
        while self._running:
            try:
                had_cycle = self._process_commands()
            except Exception as e:
                logger.error("MT Direct command loop error: %s", e)
                had_cycle = False
            # Hedge monitor now runs universally in trade_dashboard.py background thread
            if had_cycle:
                continue  # Skip sleep — immediately check for next cycle step
            # Wait for a quote tick instead of blind sleep — wakes instantly
            # when any MT4/MT5 account receives a quote update
            _quote_wakeup.wait(timeout=0.1)
            _quote_wakeup.clear()

    # _monitor_hedge removed — hedge monitor is now universal in trade_dashboard.py
    # See _run_hedge_monitor_all() which handles ALL account types via ea_account_info


    def _process_commands(self):
        """
        Check each active session for Direct accounts that need commands.
        Uses the same _should_issue_command() logic as EA polling/FIX.
        
        Two-phase approach: collect commands under the dashboard lock (fast),
        then execute broker calls OUTSIDE the lock to avoid blocking the GUI.
        """
        should_issue = self.dd.get("should_issue_command")
        if not should_issue:
            return False

        # Phase 1: Collect commands under the dashboard lock (fast — no broker calls)
        pending_commands = []  # list of (direct_acct, session, session_id, account_id, pair, lot_size, comment, result, side_info)

        with self.dd["lock"]:
            # Track sessions that already dispatched a close this cycle to enforce
            # sequential hedge-pair closing: once one side closes, the other must
            # wait for the next loop iteration (by which time session["closed"] will
            # reflect the first close and _should_issue_command will correctly block it).
            _sessions_with_close_dispatched = set()

            for session_id, session in self.dd["sessions"].items():
                if session.get("status") not in ("active", "partial_close"):
                    continue
                sides = session.get("sides", {})
                for account_id in sides:
                    # Only process Direct accounts — require direct key match
                    # (prefixed like "MT4-DIRECT-1218954455").
                    # Do NOT fall back to raw login IDs — old sessions with raw IDs
                    # would overwrite the symbol subscription for new sessions.
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

                    # Mirror ea_account_info from Direct account's internal key
                    # to the session's raw account_id (they may differ, e.g.
                    # "MT4-DIRECT-1218954455" vs "1218954455")
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

                    action = session.get("action", "open")

                    # ── Close sequencing guard ────────────────────────────────────────
                    # _should_issue_command uses session["closed"] to gate cross-account
                    # sequencing. But session["closed"] is only incremented AFTER the
                    # broker confirms the close (via _report_result). In Phase 1 we
                    # evaluate ALL accounts before any broker call is made, so BOTH sides
                    # can pass the gate simultaneously (both see closed==0, other_closed==0).
                    # Solution: once we queue a close for any account in this session,
                    # skip all remaining accounts in this session for this cycle so that
                    # on the next loop the first close will be in-flight and the gate will
                    # correctly block the second side.
                    if (action == "close" or action.startswith("close_limit")) and result not in ("rollback",):
                        if session_id in _sessions_with_close_dispatched:
                            logger.info("[%s] CLOSE-SEQ: skipping — another account in session already queued a close this cycle", account_id)
                            continue
                    # ─────────────────────────────────────────────────────────────────

                    # Check spread gating — but bypass for rollback (safety rebalancing must execute)
                    # and bypass for cycle reopen (once closed, must reopen immediately)
                    is_cycle_reopen = (session.get("action", "").startswith("cycle_") and
                                       session.get("cycle_progress", {}).get("phase") == "open")
                    if result not in ("rollback", "cycle_close", "cycle_limit_close", "cycle_limit_open") and not is_cycle_reopen:
                        current_spread = None
                        session_pair = pair
                        acct_obj = self.accounts.get(account_id)

                        # Method 1: CLR GetQuote — real-time spread (CLR-safe: command loop thread)
                        if acct_obj and session_pair:
                            try:
                                quote = acct_obj._client.GetQuote(session_pair)
                                if quote:
                                    bid = float(quote.Bid)
                                    ask = float(quote.Ask)
                                    if bid > 0 and ask > 0:
                                        pip_mult = 1000 if "JPY" in session_pair else 100000
                                        current_spread = round((ask - bid) * pip_mult, 1)
                            except Exception:
                                pass

                        # Method 2: _symbol_cache (polled, fallback)
                        if current_spread is None and acct_obj and session_pair:
                            try:
                                sym_quote = acct_obj.get_symbol_info(session_pair)
                                if sym_quote:
                                    current_spread = sym_quote.get("spread")
                            except Exception:
                                pass

                        # Method 3: ea_account_info (last resort)
                        if current_spread is None:
                            ea_info = self.dd["ea_account_info"].get(account_id, {})
                            current_spread = ea_info.get("spread")

                        if current_spread is None:
                            # No quotes yet — do not send orders blind
                            logger.info("[%s] Spread gate: no quotes for %s, skipping", account_id, pair)
                            continue
                        if max_spread is not None and current_spread > max_spread:
                            logger.info("[%s] Spread gate: spread %.1f > max %s for %s", account_id, current_spread, max_spread, pair)
                            session.setdefault("spread_rejects", {})[account_id] = \
                                session.get("spread_rejects", {}).get(account_id, 0) + 1
                            continue
                    logger.info("[%s] PASSED all gates for %s — executing order", account_id, pair)

                    # Mark in-flight
                    self.dd["in_flight_commands"][(session_id, account_id)] = time.time()

                    # Track that this session dispatched a close this cycle so the
                    # close-sequencing guard above can block the other side of the hedge.
                    if action == "close" or action.startswith("close_limit"):
                        _sessions_with_close_dispatched.add(session_id)

                    # Queue the command for execution outside the lock
                    pending_commands.append((
                        direct_acct, session, session_id, account_id,
                        pair, lot_size, comment, result, action, side_info
                    ))

        # Phase 2: Execute broker commands OUTSIDE the lock (may block on broker)
        had_cycle = False
        for (direct_acct, session, session_id, account_id,
             pair, lot_size, comment, result, action, side_info) in pending_commands:
            try:
                if result == "rollback":
                    self._send_close_command(direct_acct, session, account_id, pair, lot_size, comment)
                elif result == "cycle_close":
                    self._send_close_command(direct_acct, session, account_id, pair, lot_size, comment)
                    # Do NOT set had_cycle=True here: _send_close_command calls _report_result
                    # synchronously, which already transitions phase→open and clears open_dispatched.
                    # If we return had_cycle=True the outer loop re-enters immediately and fires
                    # a SECOND open for the same step before the first one is even dispatched.
                elif result == "cycle_limit_close":
                    logger.info("[%s] ENTERED elif cycle_limit_close block! action=%s", account_id, action)
                    limit_dist = session.get("cycle_limit_distance") or 10
                    self._send_close_command(direct_acct, session, account_id, pair, lot_size, comment,
                                             is_limit=True, limit_dist=limit_dist, limit_batch=1, limit_days=0)
                elif action == "close" or action.startswith("close_limit"):
                    limit_dist = session.get("limit_distance") or 100
                    limit_batch = session.get("limit_batch_size") or 1
                    limit_days = session.get("limit_days") or 0
                    self._send_close_command(direct_acct, session, account_id, pair, lot_size, comment,
                                             is_limit=(action == "close_limit"),
                                             limit_dist=limit_dist, limit_batch=limit_batch, limit_days=limit_days)
                elif action == "open" or (action.startswith("cycle_") and result is True) or action == "open_limit" or result == "cycle_limit_open":
                    # Normal open OR cycle reopen phase OR open_limit OR cycle_limit open
                    trade_side = side_info.get("action", "buy")
                    if result == "cycle_limit_open":
                        # Place batch_size limit orders for cycle reopen
                        limit_dist = session.get("cycle_limit_distance") or 10
                        quote = direct_acct.get_quote_direct(pair)
                        base_price = (quote.get("ask", 0) if trade_side == "buy" else quote.get("bid", 0)) if quote else 0
                        pip_mult = 1000.0 if "JPY" in pair.upper() else 100000.0
                        digits = 3 if "JPY" in pair.upper() else 5
                        raw_limit = base_price - (limit_dist / pip_mult) if trade_side == "buy" else base_price + (limit_dist / pip_mult)
                        limit_price = round(raw_limit, digits)
                        limit_type = "BuyLimit" if trade_side == "buy" else "SellLimit"
                        closed_tickets = session.get("cycle_progress", {}).get("closed_tickets", [])
                        batch_size = len(closed_tickets) if closed_tickets else int(session.get("cycle_limit_batch_size", 1))
                        
                        prog = session.get("cycle_progress", {})
                        already_placed = prog.get("limit_placed_this_batch", 0)
                        to_place = batch_size - already_placed
                        
                        if to_place <= 0:
                            logger.info("[%s] cycle_limit_open: target %d already placed in this batch, skipping", account_id, batch_size)
                            had_cycle = True
                            continue
                            
                        any_placed = False
                        placed_now = 0
                        for i in range(to_place):
                            logger.info("[%s] cycle_limit_open [%d/%d]: placing %s at %.5f",
                                        account_id, already_placed + i + 1, batch_size, limit_type, limit_price)
                            order_result = direct_acct.send_limit_order(
                                pair, trade_side, lot_size, limit_price, limit_type,
                                session_id=session_id, comment=comment
                            )
                            if isinstance(order_result, tuple) and not order_result[0]:
                                logger.warning("[%s] cycle_limit_open [%d/%d]: limit order failed",
                                               account_id, already_placed + i + 1, batch_size)
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
                    elif action == "open_limit":
                        limit_dist = session.get("limit_distance") or 100
                        quote = direct_acct.get_quote_direct(pair)
                        base_price = (quote.get("ask", 0) if trade_side == "buy" else quote.get("bid", 0)) if quote else 0
                        pip_mult = 1000.0 if "JPY" in pair.upper() else 100000.0
                        digits = 3 if "JPY" in pair.upper() else 5
                        raw_limit = base_price - (limit_dist / pip_mult) if trade_side == "buy" else base_price + (limit_dist / pip_mult)
                        limit_price = round(raw_limit, digits)
                        limit_type = "BuyLimit" if trade_side == "buy" else "SellLimit"
                        order_result = direct_acct.send_limit_order(
                            pair, trade_side, lot_size, limit_price, limit_type,
                            session_id=session_id, comment=comment
                        )
                        if isinstance(order_result, tuple) and not order_result[0]:
                            logger.warning("[%s] Order failed (returned False) — clearing in-flight", account_id)
                            self.dd["in_flight_commands"].pop((session_id, account_id), None)
                    else:
                        order_result = direct_acct.send_market_order(
                            pair, trade_side, lot_size,
                            session_id=session_id, comment=comment
                        )
                        # send_market_order returns (success, ticket, price) or raises
                        if isinstance(order_result, tuple) and not order_result[0]:
                            logger.warning("[%s] Order failed (returned False) — clearing in-flight", account_id)
                            self.dd["in_flight_commands"].pop((session_id, account_id), None)
                        elif action.startswith("cycle_"):
                            had_cycle = True
                else:
                    logger.warning("[%s] Unknown action=%s result=%s — skipping",
                                   account_id, action, repr(result))
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

            # Look up actual position volume from broker — MT5 rejects mismatched lots
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
                    # Prefer ts_epoch (reliably set at import/fill time as broker open epoch)
                    ep = f.get("ts_epoch", 0) or 0
                    if ep == 0:
                        # Fallback: parse ts string
                        ts_str = f.get("ts", "")
                        if ts_str:
                            import re
                            s = str(ts_str).strip().replace("T", " ").rstrip("Z")
                            s = re.sub(r'\.\d+', '', s)
                            for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%Y.%m.%d %H:%M:%S", "%Y.%m.%d %H:%M"):
                                try:
                                    ep = time.mktime(time.strptime(s, fmt))
                                    break
                                except (ValueError, TypeError):
                                    continue
                    # Tie-breaker: ticket number
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
                    logger.warning("[%s] CYCLE: idx=%d >= acct_fills=%d — nothing to close",
                                   account_id, idx, len(acct_fills))
                    return


        # Otherwise close oldest open fill (or limit batch)
        # Filter eligible fills (apply age filter for limit mode)
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

        for fill in eligible_fills:
            if processed_count >= batch_count:
                break

            ticket = fill.get("ticket")
            side_info = session.get("sides", {}).get(account_id, {})
            original_side = side_info.get("action", "buy")

            # Look up actual position volume from broker — MT5 rejects mismatched lots
            actual_lots = lot_size
            ticket_found = False
            orders = []
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
                    logger.warning("[%s] SKIP ABORTED: broker has 0 open orders — likely disconnected, "
                                   "not marking ticket %s as closed", account_id, ticket)
                    break

                # Ticket genuinely gone — mark as closed
                order_tickets = sorted([o.get('Ticket', 0) for o in orders])
                t_min = order_tickets[0] if order_tickets else '?'
                t_max = order_tickets[-1] if order_tickets else '?'
                logger.info("[%s] SKIP: ticket=%s not found in %d broker orders "
                            "(ticket range %s..%s) — marking as closed",
                            account_id, ticket, len(orders), t_min, t_max)
                print(f"[AUTO-SKIP] {account_id}: ticket {ticket} not on broker, auto-marking closed")
                session.setdefault("close_fills", []).append({
                    "ticket": ticket, "account": account_id,
                    "time": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "note": "auto-skipped: not in broker open orders"
                })
                session["closed"][account_id] = session.get("closed", {}).get(account_id, 0) + 1
                break  # Only handle ONE auto-skip per call

            if is_limit:
                # CLOSE-LIMIT: modify TakeProfit to passive limit price
                quote = direct_acct.get_quote_direct(pair)
                base_price = (quote.get("bid", 0) if original_side == "buy" else quote.get("ask", 0)) if quote else 0
                pip_mult = 1000.0 if "JPY" in pair.upper() else 100000.0
                limit_price = base_price + (limit_dist / pip_mult) if original_side == "buy" else base_price - (limit_dist / pip_mult)
                logger.info("[%s] LIMIT CLOSE: modifying TP ticket=%s pair=%s side=%s lots=%s TP=%.5f",
                            account_id, ticket, pair, original_side, actual_lots, limit_price)
                direct_acct.modify_position_tp(ticket, pair, original_side, actual_lots, limit_price)
                # Track that TP has been placed (prevents re-applying on every loop)
                session.setdefault("close_limit_fills", []).append({
                    "account": account_id,
                    "ticket": ticket,
                    "tp": limit_price,
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
