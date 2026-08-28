import json

with open(r"d:\Documents\dev\temp\configs\trade_sessions.json") as f:
    s = json.load(f)["54c3438e-0b3b-44f0-933c-18bebcbd9fbd"]

print(json.dumps(s, indent=2))
