import json
import os

p1 = r"d:\Documents\dev\temp\configs\trade_sessions.json"
p2 = r"d:\Documents\dev\TradeDashboard\TradeDashboardPY\configs\trade_sessions.json"

print("--- dev/temp/configs/trade_sessions.json ---")
if os.path.exists(p1):
    with open(p1) as f:
        data = json.load(f)
        for sid, s in data.items():
            print(f"Session {sid[:8]}: pair={s.get('pair')}, action={s.get('action')}, status={s.get('status')}, diff_to_open={s.get('diff_to_open')}")

print("\n--- TradeDashboardPY/configs/trade_sessions.json ---")
if os.path.exists(p2):
    with open(p2) as f:
        data = json.load(f)
        for sid, s in data.items():
            print(f"Session {sid[:8]}: pair={s.get('pair')}, action={s.get('action')}, status={s.get('status')}, diff_to_open={s.get('diff_to_open')}")
