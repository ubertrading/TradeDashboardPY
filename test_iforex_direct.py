#!/usr/bin/env python3
import json
import logging
from iforex_connector import IForexAccount

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    try:
        with open("iforex_accounts.json", "r", encoding="utf-8") as f:
            config = json.load(f)
    except FileNotFoundError:
        print("[ERROR] iforex_accounts.json not found!")
        return

    acct_conf = list(config.values())[0]
    print(f"Testing iFOREX Direct Connection for account: {acct_conf.get('account_number')}...")
    
    mock_dd = {"ea_heartbeats": {}, "ea_account_info": {}}
    acct = IForexAccount(acct_conf, mock_dd)
    
    # 1. Test Margin Requirements Query for EUR/USD (3631)
    print("\n--- [1] Testing GetDealMarginDetails (EUR/USD, ID: 3631) ---")
    margin_res = acct.get_deal_margin_details(3631)
    print("Margin Response:", json.dumps(margin_res, indent=2) if margin_res else "No response")

    # 2. Test Margin Requirements Query for Gold (14896)
    print("\n--- [2] Testing GetDealMarginDetails (Gold, ID: 14896) ---")
    gold_margin = acct.get_deal_margin_details(14896)
    print("Gold Margin Response:", json.dumps(gold_margin, indent=2) if gold_margin else "No response")

if __name__ == "__main__":
    main()
