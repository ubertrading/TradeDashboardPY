import json

with open(r"d:\Documents\dev\temp\configs\trade_sessions.json") as f:
    sessions = json.load(f)

s = sessions["54c3438e-0b3b-44f0-933c-18bebcbd9fbd"]
print("Status:", s.get("status"))
print("Action:", s.get("action"))
print("Trade Pause:", s.get("trade_pause"))
print("Last Trade TS:", s.get("last_trade_ts"))
print("Hedge Monitor Start TS:", s.get("hedge_monitor_start_ts"))
print("Spread Rejects:", s.get("spread_rejects"))
print("Errors:", s.get("errors"))
print("Rollback Needed:", s.get("rollback_needed"))
