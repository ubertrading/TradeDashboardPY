import json

p = r"d:\Documents\dev\temp\configs\trade_sessions.json"
with open(p) as f:
    data = json.load(f)

s = data["54c3438e-0b3b-44f0-933c-18bebcbd9fbd"]
print(json.dumps(s, indent=2))
