import json

p_sess = r"d:\Documents\dev\temp\configs\trade_sessions.json"
p_dir = r"d:\Documents\dev\temp\configs\mt_direct_accounts.json"
p_fix = r"d:\Documents\dev\temp\configs\fix_accounts.json"

with open(p_sess) as f:
    sessions = json.load(f)

for sid, s in sessions.items():
    if "SQ649159" in str(s):
        print(f"Session {sid}: pair={s.get('pair')}, action={s.get('action')}, status={s.get('status')}")
        print("  sides:", list(s.get("sides", {}).keys()))

with open(p_dir) as f:
    direct = json.load(f)

print("\nMT Direct Accounts matching SQ649159 or ORB:")
for aid in direct:
    if "SQ649159" in aid or "ORB" in aid:
        print(f"  {aid}: type={direct[aid].get('type')}, label={direct[aid].get('label')}")

with open(p_fix) as f:
    fix = json.load(f)

print("\nFIX Accounts matching SQ649159 or ORB:")
for aid in fix:
    if "SQ649159" in aid or "ORB" in aid:
        print(f"  {aid}: type={fix[aid].get('type')}")
