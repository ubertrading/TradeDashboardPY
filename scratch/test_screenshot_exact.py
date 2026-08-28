import json
import sys
sys.path.insert(0, r"d:\Documents\dev\TradeDashboard\TradeDashboardPY")
import trade_dashboard

session_path = r"d:\Documents\dev\temp\configs\trade_sessions.json"
with open(session_path, "r") as f:
    sessions = json.load(f)

trade_dashboard.sessions = sessions
session_id = "54c3438e-0b3b-44f0-933c-18bebcbd9fbd"
sess = sessions[session_id]

# Set exact values from screenshot:
# Side 1: SQ649159 -> Ask = 1.10025, Bid = 1.10000 (spread = 25.0)
# Side 2: ORB91033 -> Ask = 1.09992, Bid = 1.09984 (spread = 8.0)
# DIFF1 = Bid2 - Ask1 = 1.09984 - 1.10025 = -0.00041 * 100000 = -41.0 ???
# Wait! In the screenshot:
# DIFF 1 is -16.0!
# If DIFF 1 is -16.0, then Bid2 - Ask1 = -0.00016
# So if Ask1 = 1.10025, then Bid2 = 1.10009!

trade_dashboard.ea_account_info["HUGO-1-A-SQ649159"] = {
    "conn_type": "mt4_direct",
    "bid": 1.10000,
    "ask": 1.10025, # spread = 25
    "spread": 25,
    "symbol": "GBPCHF",
    "symbols": {"GBPCHF": {"bid": 1.10000, "ask": 1.10025}}
}

trade_dashboard.ea_account_info["HUGO-1-B-ORB91033"] = {
    "conn_type": "mt5_direct",
    "bid": 1.10009,
    "ask": 1.10017, # spread = 8
    "spread": 8,
    "symbol": "GBPCHF.",
    "symbols": {"GBPCHF.": {"bid": 1.10009, "ask": 1.10017}}
}

curr_diff, reason = trade_dashboard._calc_curr_diff(sess, "open")
print(f"Calculated curr_diff (DIFF 1): {curr_diff} (Reason: {reason})")
print(f"diff_to_open: {sess.get('diff_to_open')}")

res1 = trade_dashboard._should_issue_command(sess, "HUGO-1-A-SQ649159")
res2 = trade_dashboard._should_issue_command(sess, "HUGO-1-B-ORB91033")

print(f"Result for SQ649159: {res1}")
print(f"Result for ORB91033: {res2}")
