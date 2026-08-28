import json
import sys
sys.path.insert(0, r"d:\Documents\dev\TradeDashboard\TradeDashboardPY")
import trade_dashboard

# Test _calc_curr_diff with mock quotes for GBPCHF
session_path = r"d:\Documents\dev\temp\configs\trade_sessions.json"
with open(session_path, "r") as f:
    sessions = json.load(f)

session_id = "54c3438e-0b3b-44f0-933c-18bebcbd9fbd"
sess = sessions[session_id]

# Set mock ea_account_info for the 2 accounts
# HUGO-1-A-SQ649159 (Side 1: Buy GBPCHF)
# HUGO-1-B-ORB91033 (Side 2: Sell GBPCHF)

trade_dashboard.ea_account_info["HUGO-1-A-SQ649159"] = {
    "conn_type": "mt4_direct",
    "bid": 1.1000,
    "ask": 1.1002,
    "spread": 20,
    "symbol": "GBPCHF",
    "symbols": {"GBPCHF": {"bid": 1.1000, "ask": 1.1002}}
}

trade_dashboard.ea_account_info["HUGO-1-B-ORB91033"] = {
    "conn_type": "mt5_direct",
    "bid": 1.1017,
    "ask": 1.1018,
    "spread": 10,
    "symbol": "GBPCHF.",
    "symbols": {"GBPCHF.": {"bid": 1.1017, "ask": 1.1018}}
}

diff_open, reason_open = trade_dashboard._calc_curr_diff(sess, "open")
print(f"Diff Open: {diff_open} (Reason: {reason_open})")
print(f"Session diff_to_open: {sess.get('diff_to_open')}")

res1 = trade_dashboard._should_issue_command(sess, "HUGO-1-A-SQ649159")
res2 = trade_dashboard._should_issue_command(sess, "HUGO-1-B-ORB91033")
print(f"Should issue SQ649159: {res1}")
print(f"Should issue ORB91033: {res2}")
