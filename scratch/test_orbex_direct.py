import json
import sys
sys.path.insert(0, r"d:\Documents\dev\TradeDashboard\TradeDashboardPY")

from mt_direct_connector import MTDirectManager

# Read mt_direct_accounts.json
with open(r"d:\Documents\dev\temp\configs\mt_direct_accounts.json") as f:
    accounts_cfg = json.load(f)

mgr = MTDirectManager({"lock": None, "sessions": {}, "ea_account_info": {}})
for aid, cfg in accounts_cfg.items():
    mgr.add_account(aid, cfg, auto_connect=False, save=False)

print("Direct accounts loaded:", list(mgr.accounts.keys()))

orb_acct = mgr.accounts.get("HUGO-1-B-ORB91033")
if orb_acct:
    print(f"Orbex account type: {type(orb_acct).__name__}")
    print(f"Orbex config: login={orb_acct.login}, server={orb_acct.server}")
