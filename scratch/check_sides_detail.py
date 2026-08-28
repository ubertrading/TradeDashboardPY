import json
import sys
sys.path.insert(0, r"d:\Documents\dev\TradeDashboard\TradeDashboardPY")

# Read trade_sessions.json
with open(r"d:\Documents\dev\temp\configs\trade_sessions.json") as f:
    sessions = json.load(f)

session_id = "54c3438e-0b3b-44f0-933c-18bebcbd9fbd"
s = sessions[session_id]

print("Session sides:")
for k, v in s["sides"].items():
    print(f"  Account: {k}, Pair: '{v.get('pair')}', Max Spread: {v.get('max_spread')}")
