import json
import os
import re
from datetime import datetime

paths = [
    "trade_sessions.json",
    "JSON-demo/trade_sessions.json",
    "JSON-demo/1/trade_sessions.json",
    "JSON-demo/2/trade_sessions.json",
]

def parse_ts(ts_str):
    if not ts_str:
        return None
    s = str(ts_str).strip().replace("T", " ").rstrip("Z")
    s = re.sub(r'\.\d+', '', s)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%Y.%m.%d %H:%M:%S", "%Y.%m.%d %H:%M"):
        try:
            return datetime.strptime(s, fmt).timestamp()
        except ValueError:
            continue
    return None

for p in paths:
    if os.path.exists(p):
        print(f"Processing {p}...")
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        updated = 0
        for sid, session in data.items():
            fills = session.get("fills", [])
            for f_rec in fills:
                ts = f_rec.get("ts")
                if ts:
                    epoch = parse_ts(ts)
                    if epoch:
                        # Check if it differs significantly
                        old_epoch = f_rec.get("ts_epoch")
                        if old_epoch is None or abs(old_epoch - epoch) > 3600:
                            f_rec["ts_epoch"] = epoch
                            updated += 1
            
            close_fills = session.get("close_fills", [])
            for f_rec in close_fills:
                ts = f_rec.get("ts")
                if ts:
                    epoch = parse_ts(ts)
                    if epoch:
                        old_epoch = f_rec.get("ts_epoch")
                        if old_epoch is None or abs(old_epoch - epoch) > 3600:
                            f_rec["ts_epoch"] = epoch
                            updated += 1

        if updated > 0:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"Saved {p}: updated {updated} fill timestamps.")
        else:
            print(f"No updates needed for {p}.")
