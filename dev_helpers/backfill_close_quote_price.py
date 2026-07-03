"""
Backfill quote_price on existing close_fills that are missing it.

When quote_price is absent (old records), we set quote_price = price (fill price),
which means slippage will show as 0 pts — indicating "data unavailable, assumed zero".

This is better than showing '-' for every existing closed deal.

Usage: python dev_helpers/backfill_close_quote_price.py
"""

import json
import sys
import os

SESSIONS_FILE = os.path.join(os.path.dirname(__file__), '..', 'accts', 'trade_sessions.json')

def main():
    with open(SESSIONS_FILE, 'r') as f:
        sessions = json.load(f)

    # Support both list format and dict-keyed format
    if isinstance(sessions, dict):
        session_list = list(sessions.values())
    elif isinstance(sessions, list):
        session_list = sessions
    else:
        print("Unexpected sessions format")
        sys.exit(1)

    total_patched = 0
    for s in session_list:
        if not isinstance(s, dict):
            continue
        for cf in s.get('close_fills', []):
            if not isinstance(cf, dict):
                continue
            # Only patch real fills (have 'price') that are missing 'quote_price'
            if 'price' in cf and cf['price'] is not None and 'quote_price' not in cf:
                cf['quote_price'] = cf['price']
                total_patched += 1

    with open(SESSIONS_FILE, 'w') as f:
        json.dump(sessions, f, indent=2)

    print(f"Done. Patched {total_patched} close_fills with quote_price = price (slippage = 0).")

if __name__ == '__main__':
    main()
