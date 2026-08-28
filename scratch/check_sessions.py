import json

path = r"d:\Documents\dev\temp\configs\trade_sessions.json"
with open(path, "r") as f:
    data = json.load(f)

print(f"Total sessions: {len(data)}")
for sid, s in data.items():
    if s.get("status") == "active":
        print("="*60)
        print(f"ID: {sid}")
        print(f"  Status: {s.get('status')}")
        print(f"  Action: {s.get('action')}")
        print(f"  Pair: {s.get('pair')}")
        print(f"  Total Positions: {s.get('total_positions')}")
        print(f"  Diff to Open: {s.get('diff_to_open')}")
        print(f"  Diff to Close: {s.get('diff_to_close')}")
        print(f"  Filled: {s.get('filled')}")
        print(f"  Closed: {s.get('closed')}")
        print(f"  Sides: {list(s.get('sides', {}).keys())}")
        for acc, side in s.get('sides', {}).items():
            print(f"    Account {acc}: max_spread={side.get('max_spread')}")
        print(f"  Spread Rejects: {s.get('spread_rejects')}")
        print(f"  Rollback Needed: {s.get('rollback_needed')}")
