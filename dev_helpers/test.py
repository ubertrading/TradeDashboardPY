
import json
with open("d:/Documents/dev/TradeDashboard/TradeDashboardPY/configs/trade_sessions.json", "r") as f:
    sessions = json.load(f)
for sid, s in sessions.items():
    print(f"Session {sid}: sides={list(s.get(chr(34) + str(chr(115)) + chr(105) + chr(100) + chr(101) + chr(115) + chr(34), {}).keys())}")
    for acc in s.get("sides", {}).keys():
        fills = [f["ticket"] for f in s.get("fills", []) if f.get("account") == acc]
        close_fills = [f["ticket"] for f in s.get("close_fills", []) if f.get("account") == acc]
        print(f"  {acc}: fills={fills} close_fills={close_fills}")

