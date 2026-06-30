#!/usr/bin/env python3
"""
trading_logic.py — Universal Trading Logic

Extracted from trade_dashboard.py for modularity and safety.
Contains hedge monitoring, command routing, session completion, and
diff calculation logic used across all connector types.

Initialize with init(ctx) before use.
"""

import time
import threading
from datetime import datetime

# ─── Module-level shared state (set via init()) ─────────────────────────────
_ctx = {}


def init(ctx):
    """Initialize module with shared state references from trade_dashboard.

    ctx keys:
        sessions        - dict of session_id -> session dict
        strategies      - dict of strategy_id -> strategy dict
        ea_account_info - dict of account_id -> {balance, equity, bid, ask, ...}
        lock            - threading.RLock for session access
        in_flight_commands - dict of (session_id, account) -> timestamp
        save_sessions   - callable to persist sessions to disk
        log_event       - callable(session_id, account, event, detail)
        logger          - logging.Logger instance
        is_news_blackout - callable(impact_filter) -> (bool, str)
        mt_direct_manager - MTDirectManager instance or None
    """
    global _ctx
    _ctx = ctx


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


# ─── Time Window ────────────────────────────────────────────────────────────
def _is_within_time_window(session):
    """Check if current time is within the strategy's trade_start_time-trade_stop_time window."""
    try:
        strat_id = session.get("strategy_id")
        if not strat_id:
            return True  # No strategy, allow
        strat = _ctx["strategies"].get(strat_id)
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


# ─── Diff Calculation ───────────────────────────────────────────────────────
def _calc_curr_diff(session, direction):
    """
    Calculate current diff for a session based on live EA quotes.
    direction: 'open' or 'close'
    Returns (diff_value, reason_string).
    diff_value is a number or None. reason_string explains why it's None.
    """
    ea_account_info = _ctx["ea_account_info"]
    mt_direct_manager = _ctx.get("mt_direct_manager")

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
    pair1 = (sides[acc1].get("pair") or session.get("pair", "")).upper()
    pair2 = (sides[acc2].get("pair") or session.get("pair", "")).upper()

    # Verify EAs are reporting the correct symbol
    ea_sym1 = (info1.get("symbol") or "").upper()
    ea_sym2 = (info2.get("symbol") or "").upper()
    conn1 = info1.get("conn_type", "")
    conn2 = info2.get("conn_type", "")
    is_direct1 = conn1 in ("mt4_direct", "mt5_direct")
    is_direct2 = conn2 in ("mt4_direct", "mt5_direct")
    sym1_ok = not ea_sym1 or not pair1 or ea_sym1.startswith(pair1) or pair1.startswith(ea_sym1)
    sym2_ok = not ea_sym2 or not pair2 or ea_sym2.startswith(pair2) or pair2.startswith(ea_sym2)

    # For MT Direct accounts, get quotes directly for the session's pair
    bid1 = info1.get("bid", 0) if sym1_ok else 0
    ask1 = info1.get("ask", 0) if sym1_ok else 0
    bid2 = info2.get("bid", 0) if sym2_ok else 0
    ask2 = info2.get("ask", 0) if sym2_ok else 0

    if mt_direct_manager:
        for i, (acc, pair_i, is_dir, s_ok) in enumerate([
            (acc1, pair1, is_direct1, sym1_ok),
            (acc2, pair2, is_direct2, sym2_ok)
        ]):
            if not is_dir:
                continue
            direct_acct = mt_direct_manager.accounts.get(acc)
            if direct_acct and direct_acct.connected:
                try:
                    sym_info = direct_acct.get_symbol_info(pair_i)
                    if sym_info and sym_info.get("bid") and sym_info.get("ask"):
                        if i == 0:
                            bid1, ask1 = sym_info["bid"], sym_info["ask"]
                        else:
                            bid2, ask2 = sym_info["bid"], sym_info["ask"]
                    elif not s_ok:
                        return (None, f"S{i+1}: fetching {pair_i}")
                except Exception:
                    if not s_ok:
                        return (None, f"S{i+1}: fetching {pair_i}")

    # Original symbol checks for EA-polled accounts
    if not is_direct1 and not sym1_ok:
        if not sym2_ok and not is_direct2:
            return (None, f"S1:{ea_sym1}≠{pair1} S2:{ea_sym2}≠{pair2}")
        return (None, f"S1 on {ea_sym1} (need {pair1})")
    if not is_direct2 and not sym2_ok:
        return (None, f"S2 on {ea_sym2} (need {pair2})")

    if not bid1 or not ask1 or not bid2 or not ask2:
        return (None, "waiting for quotes")

    # Determine which side buys and which sells
    s1_action = sides[acc1].get("action", "buy").lower()
    s2_action = sides[acc2].get("action", "sell").lower()

    # Determine pip multiplier from pair (e.g., USDJPY=100, EURUSD=100000)
    pair = pair1 or pair2
    pip_mult = 100 if "JPY" in pair else 100000

    if direction == "open":
        if s1_action == "buy" and s2_action == "sell":
            diff = (bid2 - ask1) * pip_mult
        elif s1_action == "sell" and s2_action == "buy":
            diff = (bid1 - ask2) * pip_mult
        else:
            diff = (bid2 - ask1) * pip_mult
    else:  # close
        if s1_action == "buy" and s2_action == "sell":
            diff = (bid1 - ask2) * pip_mult
        elif s1_action == "sell" and s2_action == "buy":
            diff = (bid2 - ask1) * pip_mult
        else:
            diff = (bid1 - ask2) * pip_mult

    return (round(diff, 1), None)


# ─── Command Routing ────────────────────────────────────────────────────────
def _should_issue_command(session, account):
    """
    Determine if a command should be issued to this account based on
    session status, fill counts, execution order, time window, and per-account limits.
    Returns: True (normal command), False (skip), or "rollback" (issue close command for rollback).
    """
    strategies = _ctx["strategies"]
    ea_account_info = _ctx["ea_account_info"]
    in_flight_commands = _ctx["in_flight_commands"]
    is_news_blackout = _ctx["is_news_blackout"]
    _save_sessions = _ctx["save_sessions"]
    _log_event = _ctx["log_event"]

    # Rollback takes priority — but only if parent strategy is running
    rollback = session.get("rollback_needed", {})
    if rollback.get(account, 0) > 0:
        # ── Timeout: if rollback has been pending too long, clear and let hedge monitor re-detect ──
        rb_start = session.get("rollback_start_ts", {}).get(account, 0)
        if rb_start and (time.time() - rb_start) > 30:
            # Clear this rollback — do NOT create phantom close_fills
            rb_tickets = session.get("rollback_tickets", {}).get(account, [])
            failed_ticket = rb_tickets[0] if rb_tickets else None
            rollback[account] = max(0, rollback.get(account, 0) - 1)
            session["rollback_needed"] = rollback
            if rb_tickets:
                rb_tickets.pop(0)
                if not rb_tickets:
                    session.get("rollback_tickets", {}).pop(account, None)
            session.get("rollback_start_ts", {}).pop(account, None)
            print(f"[ROLLBACK-TIMEOUT] acct={account}: rollback timed out after 30s "
                  f"(ticket={failed_ticket}). Clearing — hedge monitor will re-detect. "
                  f"Remaining={rollback.get(account, 0)}")
            if rollback.get(account, 0) > 0:
                # Still more rollbacks — reset the timer for the next one
                session.setdefault("rollback_start_ts", {})[account] = time.time()
                return "rollback"
            return False  # No more rollbacks

        # Set start timestamp if not already tracking
        if not rb_start:
            session.setdefault("rollback_start_ts", {})[account] = time.time()

        strat_id = session.get("strategy_id")
        strat = strategies.get(strat_id) if strat_id else None
        # Rollback is a safety mechanism — execute regardless of strategy running state
        return "rollback"

    if session["status"] not in ("active", "partial_close"):
        return False

    # Check if the parent strategy is running
    strat_id = session.get("strategy_id")
    if strat_id:
        strat = strategies.get(strat_id)
        if strat and not strat.get("running", False):
            return False

    if not _is_within_time_window(session):
        return False

    # Check trade_pause: minimum delay between consecutive trades
    trade_pause = session.get("trade_pause", 0)
    if trade_pause > 0:
        last_ts = session.get("last_trade_ts", {}).get(account, 0)
        if last_ts > 0 and (time.time() - last_ts) < trade_pause:
            return False

    # Check in-flight: block if a command was recently sent and not yet confirmed
    flight_key = (session.get("id", ""), account)
    flight_ts = in_flight_commands.get(flight_key, 0)
    if flight_ts > 0 and (time.time() - flight_ts) < 10:  # 10s timeout for lost responses
        return False

    action = session.get("action", "open")
    sides = session.get("sides", {})

    if account not in sides:
        return False

    # ── MONITOR mode: NEVER open positions, only rollback (close) is allowed ──
    if action == "monitor":
        return False

    # ── News filter (applies to all actions) ──
    if session.get("avoid_news"):
        blocked, reason = is_news_blackout(impact_filter="High")
        if blocked:
            return False

    if action == "open":
        target = session["total_positions"]
        current = session["filled"].get(account, 0)
        if current >= target:
            return False

        # Cross-account hedge sync
        for other_acc in sides:
            if other_acc != account:
                other_filled = session["filled"].get(other_acc, 0)
                if current > other_filled:
                    return False

        # Check max_accum_deals
        max_deals = session.get("max_accum_deals", 0)
        if max_deals > 0:
            acct_net_open = session.get("filled", {}).get(account, 0) - session.get("closed", {}).get(account, 0)
            if acct_net_open >= max_deals:
                return False

        # Check max_accum_lots
        max_accum = session.get("max_accum_lots", 0.0)
        if max_accum > 0:
            acct_net = max(0, session.get("filled", {}).get(account, 0) - session.get("closed", {}).get(account, 0))
            acct_lot = sides[account].get("lot_size", session.get("lot_size", 0.01))
            acct_lots = acct_net * acct_lot
            if acct_lots + acct_lot > max_accum + 1e-9:
                return False

        # Diff-to-open gating
        diff_to_open = session.get("diff_to_open")
        if diff_to_open is None:
            return False  # Blank = don't trade
        curr_diff_val, _ = _calc_curr_diff(session, "open")
        if curr_diff_val is None or curr_diff_val < diff_to_open:
            return False

        # ── Execution Filters (open) ──
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
        acct_filled = session["filled"].get(account, 0)
        close_cap = session.get("close_count")
        effective_target = min(close_cap, acct_filled) if close_cap is not None else acct_filled
        current = session["closed"].get(account, 0)
        if current >= effective_target:
            return False

        # Check if we are in the middle of an incomplete batch close.
        # This prevents the system from getting stuck with an unbalanced hedge
        # if the price ticks away while closing multiple positions (e.g. virtual fills).
        mid_close = sum(session.get("closed", {}).values()) > 0

        # Diff-to-close gating: only close when DIFF2 >= threshold.
        # Blank/None = don't close (mirrors diff_to_open: None = don't open).
        diff_to_close = session.get("diff_to_close")
        if diff_to_close is None or diff_to_close == "":
            return False  # Blank = don't close
        
        if not mid_close:
            diff_to_close = int(diff_to_close)
            curr_diff_val, _ = _calc_curr_diff(session, "close")
            if curr_diff_val is None or curr_diff_val < diff_to_close:
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

        # Auto-initialize cycle tracking if missing
        if not session.get("cycle_progress"):
            cycle_total = session.get("filled", {}).get(cycle_account, len([f for f in session.get("fills", []) if f.get("account") == cycle_account]))
            session["cycle_progress"] = {"phase": "close", "index": 0, "cycled": 0, "cycle_total": cycle_total}
            session["cycle_account"] = cycle_account
            print(f"[CYCLE-DBG] Auto-initialized cycle_progress for {cycle_account}, total={cycle_total}")

        progress = session.get("cycle_progress", {})
        phase = progress.get("phase", "close")
        idx = progress.get("index", 0)

        # Get active fills for the cycling account (exclude externally closed positions)
        closed_set = set(str(cf.get("ticket")) for cf in session.get("close_fills", []) if cf.get("account") == cycle_account)
        acct_fills = [f for f in session.get("fills", []) if f.get("account") == cycle_account and str(f.get("ticket")) not in closed_set]
        
        if session.get("imported"):
            total_to_cycle = progress.get("cycle_total", session.get("filled", {}).get(cycle_account, len(acct_fills)))
        else:
            total_to_cycle = progress.get("cycle_total", len(acct_fills))
        # Check if position is older than cycle_days
        cycle_days = session.get("cycle_days")
        if cycle_days is None or cycle_days == "":
            print(f"[CYCLE-DBG] acct={account}: cycle_days not set (value={repr(cycle_days)})")
            return False
        try:
            cycle_days = float(cycle_days)
        except (ValueError, TypeError):
            print(f"[CYCLE-DBG] acct={account}: cycle_days invalid (value={repr(cycle_days)})")
            return False
        if cycle_days <= 0:
            print(f"[CYCLE-DBG] acct={account}: cycle_days <= 0 (value={cycle_days})")
            return False

        # BUG FIX: Always search from index 0 to avoid skipping positions.
        # When fills are replaced in-place during cycling, starting from
        # progress["index"] can skip the position that shifted into a previous slot.
        found_old_enough = True  # Default for cycle_days=0
        if cycle_days > 0:
            search_idx = 0
            found_old_enough = False
            while search_idx < len(acct_fills):
                fill_epoch = acct_fills[search_idx].get("ts_epoch", 0)
                if fill_epoch:
                    age_days = abs(time.time() - fill_epoch) / 86400
                    if age_days < cycle_days:
                        print(f"[CYCLE-DBG] acct={account}: position {search_idx} too new (age={age_days:.2f}d < {cycle_days}d) - skipping")
                        search_idx += 1
                    else:
                        print(f"[CYCLE-DBG] acct={account}: position {search_idx} old enough (age={age_days:.2f}d >= {cycle_days}d)")
                        found_old_enough = True
                        break
                else:
                    print(f"[CYCLE-DBG] acct={account}: position {search_idx} missing ts_epoch, allowing cycle")
                    found_old_enough = True
                    break
            idx = search_idx
            # CRITICAL: Store the age-filtered index back into progress so the
            # downstream poll handler closes the correct age-qualified position.
            progress["index"] = idx
            session["cycle_progress"] = progress

        if idx >= total_to_cycle:
            print(f"[CYCLE-DBG] acct={account}: All positions cycled/skipped (idx={idx} >= total={total_to_cycle})")
            if session.get("action", "").startswith("cycle_"):
                session["action"] = "monitor"
                _save_sessions()
                avg_spread = 0
                total_sc = progress.get("total_spread_cost", 0)
                if idx > 0:
                    avg_spread = total_sc / idx
                _log_event(session["id"], account, "cycle_complete",
                           f"All {idx} positions processed — avg spread cost: {avg_spread:.5f} — switching to MONITOR")
                print(f"[CYCLE] Complete: avg spread cost={avg_spread:.5f}, auto-switching to MONITOR")
            return False  # All positions cycled or skipped

        if phase == "close":
            side_info = sides.get(account, {})
            side_max_spread = side_info.get("max_spread") if side_info.get("max_spread") is not None else session["max_spread_points"]
            try:
                side_max_spread = float(side_max_spread) if side_max_spread is not None else 0.0
            except (ValueError, TypeError):
                side_max_spread = 0.0
            ea_info = ea_account_info.get(account, {})
            current_spread = ea_info.get("spread")
            if side_max_spread > 0 and current_spread is not None and current_spread > side_max_spread:
                return False  # Spread too wide
            return "cycle_close"
        elif phase == "open":
            return True
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


# ─── Session Completion ─────────────────────────────────────────────────────
def _check_session_completion(session):
    """Check if all sides have reached their targets.
    Sessions CYCLE: open -> close -> open -> close ...
    When close targets are met, reset counters and switch back to 'open'.
    """
    _save_sessions = _ctx["save_sessions"]
    _log_event = _ctx["log_event"]

    action = session.get("action", "open")
    all_done = True

    for account in session.get("sides", {}):
        if action == "open":
            if session["filled"].get(account, 0) < session["total_positions"]:
                all_done = False
                break
        elif action == "close":
            acct_filled = session["filled"].get(account, 0)
            close_cap = session.get("close_count")
            effective = min(close_cap, acct_filled) if close_cap is not None else acct_filled
            if session["closed"].get(account, 0) < effective:
                all_done = False
                break

    if all_done and action == "close":
        # Safety check: if one side has 0 closes but another has > 0,
        # something went wrong — don't cycle back.
        # Only consider accounts that actually had positions to close (effective target > 0).
        active_close_counts = []
        for acc in session.get("sides", {}):
            acct_filled = session["filled"].get(acc, 0)
            close_cap = session.get("close_count")
            effective = min(close_cap, acct_filled) if close_cap is not None else acct_filled
            if effective > 0:
                active_close_counts.append(session["closed"].get(acc, 0))
        if active_close_counts and max(active_close_counts) > 0 and min(active_close_counts) == 0:
            all_done = False
            print(f"[CYCLE-GUARD] Session {session['id'][:8]}: blocking cycle — "
                  f"one side has 0 closes while other has {max(active_close_counts)}. "
                  f"close_counts={dict(zip(session.get('sides', {}).keys(), [session['closed'].get(acc, 0) for acc in session.get('sides', {})]))}")

    if all_done and action == "close":
        # Before resetting counters, check whether any raw FIX account (e.g. Dukascopy)
        # still has a live broker position. This can happen when duplicate close orders
        # accidentally create a new opposite position instead of just closing the original.
        # Example: 2× SELL closes on a LONG +15M → first closes it, second opens SHORT -15M.
        # In that case decrement closed[acc] to force one corrective close before the reset.
        ea_account_info = _ctx.get("ea_account_info", {})
        residual_found = False
        for acc in list(session.get("sides", {})):
            acct_info = ea_account_info.get(acc, {})
            residual_positions = acct_info.get("positions") or 0
            acct_filled = session["filled"].get(acc, 0)
            if (acct_info.get("fix_account") and not acct_info.get("openapi_connected")
                    and residual_positions > 0 and acct_filled > 0):
                new_closed = max(0, acct_filled - 1)
                print(f"[RESIDUAL-CLOSE] Session {session['id'][:8]}: {acc} still has "
                      f"{residual_positions} open FIX position(s) after apparent close completion "
                      f"(filled={acct_filled}, closed={session['closed'].get(acc,0)}). "
                      f"Decrementing closed to {new_closed} to force corrective close.")
                session["closed"][acc] = new_closed
                _log_event(session["id"], acc, "residual_close_detected",
                           f"FIX account still has {residual_positions} open position(s) — "
                           f"forcing corrective close (closed→{new_closed})")
                residual_found = True
                break  # Handle one account at a time
        if residual_found:
            pass  # Loop will re-check; _should_issue_command will dispatch the corrective close
        else:
            for acc in session.get("sides", {}):
                session["filled"][acc] = 0
                session["closed"][acc] = 0
                session["errors"][acc] = []
                session["spread_rejects"][acc] = 0
            session["close_count"] = None
            session["fills"] = []
            session["last_trade_ts"] = {}
            session["status"] = "active"
            session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _log_event(session["id"], None, "close_complete",
                       "All close targets met — counters reset, mode unchanged")
            print(f"[CLOSE-DONE] Session {session['id']}: all closes done, counters reset")
            _save_sessions()
    elif all_done and action == "open":
        all_closed = all(
            session["closed"].get(acc, 0) >= session["filled"].get(acc, 0) > 0
            for acc in session.get("sides", {})
        )
        if all_closed:
            session["action"] = "monitor"
            session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _log_event(session["id"], None, "mode_monitor",
                       "All positions externally closed — switched to MONITOR mode")
            print(f"[MODE] Session {session['id']}: all positions externally closed, switching to MONITOR")
            _save_sessions()
        else:
            if session.get("status") == "partial_close":
                session["status"] = "active"
                session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _save_sessions()
            _log_event(session["id"], None, "open_targets_reached",
                       f"All sides reached open target ({session.get('total_positions')} each)")
    elif action == "close":
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
                    if session.get("status") != "partial_close":
                        session["status"] = "partial_close"
                        session["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        _log_event(session["id"], None, "partial_close",
                                   f"ALERT: Side(s) {done_accounts} closed but {pending_accounts} still open! Hedge is unbalanced.")
                        print(f"[ALERT] PARTIAL CLOSE: {done_accounts} closed, {pending_accounts} NOT closed. Hedge unbalanced!")
                        _save_sessions()


# ─── Universal Hedge Monitor ────────────────────────────────────────────────
# Single implementation for ALL account types (EA poll, MT Direct, FIX).
# Reads open_tickets from ea_account_info (populated by all connectors).

_hedge_monitor_last_run = [0.0]  # mutable container for closure


def _run_hedge_monitor_all():
    """Universal hedge monitor: detect externally closed positions and queue
    rollback closes on the paired account. Runs for ALL sessions/accounts
    regardless of connector type, using ea_account_info as the data source."""
    sessions = _ctx["sessions"]
    strategies = _ctx["strategies"]
    ea_account_info = _ctx["ea_account_info"]
    lock = _ctx["lock"]
    _save_sessions = _ctx["save_sessions"]
    _log_event = _ctx["log_event"]

    now_ts = time.time()
    # Throttle: only check every 0.5 seconds
    if (now_ts - _hedge_monitor_last_run[0]) < 0.5:
        return
    _hedge_monitor_last_run[0] = now_ts

    with lock:
        for sid, session in list(sessions.items()):
            if session.get("status") not in ("active", "paused", "partial_close"):
                continue

            # ── Skip if parent strategy is NOT running ──
            _hm_strat_id = session.get("strategy_id")
            if _hm_strat_id:
                _hm_strat = strategies.get(_hm_strat_id)
                if _hm_strat and not _hm_strat.get("running", False):
                    continue  # Strategy not started — don't run hedge monitor

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

            # STARTUP COOLDOWN: Skip hedge monitor for the first 30s after session start
            hedge_start = session.get("hedge_monitor_start_ts", 0)
            if hedge_start > 0 and (now_ts - hedge_start) < 30:
                continue

            sides = session.get("sides", {})
            if len(sides) < 2:
                continue

            # Only run after ALL sides have reached fill targets
            if sess_action == "open":
                all_sides_filled = all(
                    session["filled"].get(acc, 0) >= session["total_positions"]
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

            # ── Check each account using ea_account_info ──
            for account in sides:
                # Get open tickets from ea_account_info (populated by EA poll, MT Direct, FIX)
                info = ea_account_info.get(account, {})
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
                expected_open = set(acct_fill_tickets) - acct_close_tickets

                if not expected_open:
                    # If we have no expected open tickets, we might still have a permanent shortfall
                    # We'll check that below, so we don't immediately continue yet.
                    pass

                missing_tickets = set()
                
                # Check for permanent import imbalances (e.g. imported 0 vs N)
                if sess_action != "open" and session.get("imported"):
                    total_pos = session.get("total_positions", 0)
                    if len(acct_fill_tickets) < total_pos:
                        shortfall = total_pos - len(acct_fill_tickets)
                        for i in range(shortfall):
                            fake_t = f"MISSING_IMPORT_{account}_{i}"
                            if fake_t not in acct_close_tickets:
                                missing_tickets.add(fake_t)

                # ── NETTING MODE BYPASS ──────────────────────────────────────
                # Brokers like Dukascopy use netting mode: all fills for one
                # symbol collapse into a single broker-side position. Per-ticket
                # comparison will always show "N missing, ea_has=1". We instead
                # compare the expected total lot volume against the broker's net lots.
                if info.get("netting_mode"):
                    mismatch_key = f"hedge_mismatch_{sid}_{account}"
                    
                    pair = (sides[account].get("pair") or session.get("pair", "")).upper()
                    sess_action = sides[account].get("action")
                    if not sess_action:
                        side_num = sides[account].get("side_number", 1)
                        sess_action = "buy" if side_num == 1 else "sell"
                    sess_action = sess_action.lower()

                    actual_lots = 0.0
                    broker_positions = info.get("positions", 0)
                    if broker_positions > 0:
                        lots_info = info.get("lots_by_instrument", {}).get(pair, {})
                        actual_lots = lots_info.get(sess_action, 0.0)

                    # Calculate expected lots for this session
                    expected_lots = 0.0
                    expected_open_sorted = []
                    acct_fills_dict = {str(f["ticket"]): f for f in session.get("fills", []) if f.get("account") == account}
                    
                    for t in expected_open:
                        fill = acct_fills_dict.get(str(t), {})
                        ts = fill.get("ts_epoch", 0)
                        lot_val = float(fill.get("lots") or fill.get("lot_size") or session.get("lot_size", 0.01))
                        expected_open_sorted.append((t, lot_val, ts))
                        expected_lots += lot_val
                        
                    expected_lots = round(expected_lots, 4)
                    
                    if actual_lots >= expected_lots and not missing_tickets:
                        # Still open and no permanent shortfall — reset counter and move on
                        session.pop(mismatch_key, None)
                        continue
                    else:
                        missing_lots = round(expected_lots - actual_lots, 4)
                        if missing_lots > 0:
                            # Treat some tickets as missing to balance the lots.
                            expected_open_sorted.sort(key=lambda x: x[2]) # oldest first
                            lots_to_remove = missing_lots
                            for t, lot_val, _ in expected_open_sorted:
                                if t in missing_tickets:
                                    continue # skip if already missing
                                if lots_to_remove > 1e-5:
                                    missing_tickets.add(t)
                                    lots_to_remove -= lot_val
                                if lots_to_remove <= 1e-5:
                                    break
                            
                            if missing_tickets:
                                print(f"[HEDGE-MON] acct={account} sid={sid[:8]}: "
                                      f"netting_mode — expected {expected_lots} lots, broker has {actual_lots} lots. "
                                      f"Treating {len(missing_tickets)} ticket(s) as externally closed.")
                else:
                    # Step 2: Detect — compare expected vs actual (per-ticket path)
                    missing_tickets.update(expected_open - ea_open_tickets)
                    
                if not missing_tickets:
                    # Reset mismatch counter — everything matches
                    mismatch_key = f"hedge_mismatch_{sid}_{account}"
                    session.pop(mismatch_key, None)
                    continue

                # Log every detection
                print(f"[HEDGE-MON] acct={account} sid={sid[:8]}: "
                      f"expected={len(expected_open)} ea_has={len(ea_open_tickets)} "
                      f"missing={len(missing_tickets)}")
                mismatch_key = f"hedge_mismatch_{sid}_{account}"
                prev_count = session.get(mismatch_key, 0)
                
                # Direct connections (FIX, MT4/MT5 Direct) are reliable and don't need 
                # the 3-tick debounce that EA HTTP polling requires for stability.
                threshold = 0 if info.get("direct_mode") else 2
                
                if prev_count < threshold:
                    session[mismatch_key] = prev_count + 1
                    print(f"[HEDGE-REBAL] acct={account} sid={sid[:8]}: "
                          f"detected {len(missing_tickets)} missing ticket(s) "
                          f"({prev_count + 1}/{threshold + 1} consecutive), waiting...")
                    continue
                # Clear counter — taking action
                session.pop(mismatch_key, None)

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

                other_accounts = [a for a in sides if a != account]
                tickets_to_close = []

                # Rebuild close tickets set now that we've added the external closes
                all_close_tickets = set(_normalize_ticket(f["ticket"]) for f in session.get("close_fills", []))

                # Build per-account fill lists (chronological order)
                acct_fills_ordered = [f for f in session.get("fills", [])
                                      if f.get("account") == account]
                for missing_t in missing_tickets:
                    missing_idx = None
                    for idx, f in enumerate(acct_fills_ordered):
                        if _normalize_ticket(f.get("ticket")) == missing_t:
                            missing_idx = idx
                            break

                    if missing_idx is not None:
                        for other_acc in other_accounts:
                            other_fills_ordered = [f for f in session.get("fills", [])
                                                   if f.get("account") == other_acc]
                            if missing_idx < len(other_fills_ordered):
                                paired_fill = other_fills_ordered[missing_idx]
                                already_queued = set(t for _, t in tickets_to_close)
                                if (_normalize_ticket(paired_fill["ticket"]) not in all_close_tickets
                                        and _normalize_ticket(paired_fill["ticket"]) not in already_queued):
                                    tickets_to_close.append((other_acc, _normalize_ticket(paired_fill["ticket"])))
                                    print(f"[HEDGE-REBAL] Paired: closed ticket {missing_t} on {account} "
                                          f"(fill #{missing_idx + 1}) "
                                          f"→ will close ticket {paired_fill['ticket']} on {other_acc} "
                                          f"(fill #{missing_idx + 1})")

                # Step 4: Fallback — close oldest on other side if pairing didn't find all
                if len(tickets_to_close) < len(missing_tickets):
                    shortfall = len(missing_tickets) - len(tickets_to_close)
                    already_queued = set(t for _, t in tickets_to_close)
                    for other_acc in other_accounts:
                        other_fills = [f for f in session.get("fills", [])
                                       if f.get("account") == other_acc
                                       and _normalize_ticket(f["ticket"]) not in all_close_tickets
                                       and _normalize_ticket(f["ticket"]) not in already_queued]
                        other_fills.sort(key=lambda x: (x.get("ts_epoch") or 0))
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
    logger = _ctx["logger"]

    def _loop():
        while True:
            try:
                _run_hedge_monitor_all()
            except Exception as e:
                logger.error("Hedge monitor error: %s", e, exc_info=True)
            time.sleep(0.5)  # Check every 0.5s
    t = threading.Thread(target=_loop, daemon=True, name="HedgeMonitor")
    t.start()
    logger.info("Universal hedge monitor thread started")
