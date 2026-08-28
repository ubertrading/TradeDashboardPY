import urllib.request
import json

resp = urllib.request.urlopen("http://127.0.0.2/api/status", timeout=5)
data = json.loads(resp.read().decode())

mt_direct = data.get("mt_direct_accounts", {})
print("--- Connected MT Direct Accounts ---")
for aid, info in mt_direct.items():
    print(f"Account: {aid}, label: {info.get('label')}, connected: {info.get('connected')}, type: {info.get('type')}")

ea_info = data.get("ea_account_info", {})
print("\n--- EA Account Info Keys ---")
print(list(ea_info.keys()))
for aid in ["HUGO-1-A-SQ649159", "HUGO-1-B-ORB91033"]:
    if aid in ea_info:
        print(f"\n{aid}:")
        print("  connected:", ea_info[aid].get("connected"))
        print("  spread:", ea_info[aid].get("spread"))
        print("  bid/ask:", ea_info[aid].get("bid"), "/", ea_info[aid].get("ask"))
