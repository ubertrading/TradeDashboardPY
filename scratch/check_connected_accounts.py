import json
import sys
sys.path.insert(0, r"d:\Documents\dev\TradeDashboard\TradeDashboardPY")

# Check mt_direct_accounts.json, fix_accounts.json, etc.
import os

print("mt_direct_accounts.json exists:", os.path.exists(r"d:\Documents\dev\temp\configs\mt_direct_accounts.json"))
if os.path.exists(r"d:\Documents\dev\temp\configs\mt_direct_accounts.json"):
    with open(r"d:\Documents\dev\temp\configs\mt_direct_accounts.json") as f:
        print("MT Direct accounts:", list(json.load(f).keys()))

print("fix_accounts.json exists:", os.path.exists(r"d:\Documents\dev\temp\configs\fix_accounts.json"))
if os.path.exists(r"d:\Documents\dev\temp\configs\fix_accounts.json"):
    with open(r"d:\Documents\dev\temp\configs\fix_accounts.json") as f:
        print("FIX accounts:", list(json.load(f).keys()))
