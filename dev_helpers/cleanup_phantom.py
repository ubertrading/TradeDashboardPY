"""
One-time cleanup: Remove ALL external close_fills and reset closed counters.
This forces the hedge monitor to re-detect the imbalance from scratch.
"""
import json, os

SESSIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trade_sessions.json")

with open(SESSIONS_FILE, "r", encoding="utf-8") as f:
    sessions = json.load(f)

for sid, session in sessions.items():
    sides = session.get("sides", {})
    if not sides:
        continue

    close_fills = session.get("close_fills", [])
    # Remove ALL external close_fills (both MT4 and MT5)
    phantom = [cf for cf in close_fills if cf.get("external")]
    real = [cf for cf in close_fills if not cf.get("external")]

    if phantom:
        print(f"Session {sid[:8]}:")
        print(f"  Removing {len(phantom)} external close_fills:")
        for p in phantom:
            print(f"    acct={p.get('account')} ticket={p.get('ticket')} ts={p.get('ts')}")
        session["close_fills"] = real

        # Reset closed counters to match real close_fills count per account
        for acc in sides:
            real_count = len([cf for cf in real if cf.get("account") == acc])
            old_count = session.get("closed", {}).get(acc, 0)
            session["closed"][acc] = real_count
            print(f"  closed[{acc}]: {old_count} -> {real_count}")

    # Clear stale rollback state
    session.pop("rollback_start_ts", None)
    rb = session.get("rollback_needed", {})
    for acc in list(rb.keys()):
        if rb[acc] > 0:
            print(f"  Clearing rollback_needed[{acc}]={rb[acc]}")
            rb[acc] = 0
            session.get("rollback_tickets", {}).pop(acc, None)

tmp = SESSIONS_FILE + ".tmp"
with open(tmp, "w", encoding="utf-8") as f:
    json.dump(sessions, f, indent=2, sort_keys=True, default=str)
os.replace(tmp, SESSIONS_FILE)
print("\nSaved. Restart dashboard — hedge monitor will re-detect the imbalance.")
