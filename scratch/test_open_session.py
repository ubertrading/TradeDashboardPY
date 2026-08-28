import json
import os
import sys

# Add directory to sys.path
sys.path.insert(0, r"d:\Documents\dev\TradeDashboard\TradeDashboardPY")

# Import trade_dashboard modules
import trade_dashboard

# Load dev/temp session data into trade_dashboard.sessions
session_path = r"d:\Documents\dev\temp\configs\trade_sessions.json"
with open(session_path, "r") as f:
    sessions = json.load(f)

trade_dashboard.sessions = sessions
session_id = "54c3438e-0b3b-44f0-933c-18bebcbd9fbd"
sess = sessions[session_id]

print("Testing _calc_curr_diff for open phase:")
curr_diff, quote_str = trade_dashboard._calc_curr_diff(sess, "open")
print(f"  curr_diff: {curr_diff}, quote_str: {quote_str}")
print(f"  diff_to_open: {sess.get('diff_to_open')}")

for acc in sess["sides"]:
    print(f"\nTesting _should_issue_command for account {acc}:")
    res = trade_dashboard._should_issue_command(sess, acc)
    print(f"  Result: {res}")
