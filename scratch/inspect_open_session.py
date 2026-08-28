import json
import os

path = r"d:\Documents\dev\temp\configs\trade_sessions.json"
with open(path, "r") as f:
    sessions = json.load(f)

session_id = "54c3438e-0b3b-44f0-933c-18bebcbd9fbd"
s = sessions.get(session_id)
print("Session 54c3438e:")
print(json.dumps(s, indent=2))
