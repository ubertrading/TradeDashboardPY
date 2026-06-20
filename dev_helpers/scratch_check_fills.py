"""Verify fix: reset index to 0 after each cycle"""
import json, time
from datetime import datetime, timezone, timedelta
from copy import deepcopy

try:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
except:
    NY_TZ = timezone(timedelta(hours=-4))

def _count_rollover_days(open_epoch, now_epoch=None):
    if now_epoch is None:
        now_epoch = time.time()
    open_dt = datetime.fromtimestamp(open_epoch, tz=NY_TZ)
    now_dt = datetime.fromtimestamp(now_epoch, tz=NY_TZ)
    days = (now_dt.date() - open_dt.date()).days
    return max(0, days)

def _fill_sort_key(f):
    ts_str = f.get("ts", "")
    if ts_str:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p"):
            try:
                return datetime.strptime(ts_str, fmt).timestamp()
            except:
                continue
    return f.get("ts_epoch", 0) or 0

# Test with 10 positions, all old
fills = []
close_fills = []
for i in range(10):
    fills.append({
        "account": "TEST",
        "ticket": f"T{i}",
        "ts": "2026-04-13 12:00:00",
        "ts_epoch": datetime.strptime("2026-04-13 12:00:00", "%Y-%m-%d %H:%M:%S").timestamp()
    })

cycle_days = 4.0
idx = 0
cycled_tickets = []
now = time.time()

print("=== FIX: Always start search from index 0 ===")
print(f"Starting: {len(fills)} fills, cycle_days={cycle_days}")
print()

for step in range(25):
    closed_set = set(str(cf["ticket"]) for cf in close_fills)
    acct_fills = [f for f in fills if str(f["ticket"]) not in closed_set]
    acct_fills.sort(key=_fill_sort_key)
    total_to_cycle = len(acct_fills)
    
    # FIX: Always search from 0 (the oldest)
    search_idx = 0
    found = False
    while search_idx < len(acct_fills):
        fill_record = acct_fills[search_idx]
        fill_epoch = fill_record.get("ts_epoch", 0)
        age = _count_rollover_days(fill_epoch) if fill_epoch else 0
        
        if age < cycle_days:
            search_idx += 1
            continue
        
        # Found an old enough position
        found = True
        break
    
    if not found:
        print(f"  Step {step}: No positions old enough — DONE")
        break
    
    ticket = acct_fills[search_idx]["ticket"]
    print(f"  Step {step}: found ticket={ticket} at sorted_idx={search_idx} age={age}d — CYCLING")
    cycled_tickets.append(ticket)
    
    close_fills.append({"ticket": ticket, "account": "TEST", "cycle": True})
    for i, f in enumerate(fills):
        if str(f["ticket"]) == str(ticket):
            fills[i] = {
                "account": "TEST",
                "ticket": f"NEW_{ticket}",
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ts_epoch": now
            }
            break

print()
print(f"Cycled {len(cycled_tickets)} tickets: {cycled_tickets}")
original_tickets = [f"T{i}" for i in range(10)]
missed = [t for t in original_tickets if t not in cycled_tickets]
print(f"Missed {len(missed)} tickets: {missed}")
