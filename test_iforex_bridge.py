#!/usr/bin/env python3
import time
import json
import logging
from iforex_connector import IForexAccountManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    print("=" * 60)
    print("      iFOREX BRIDGE SERVER LISTENING (port 8765)      ")
    print("=" * 60)
    print("Waiting for Tampermonkey sync from your iFOREX browser tab...")
    
    mock_dd = {
        "ea_heartbeats": {},
        "ea_account_info": {}
    }
    
    mgr = IForexAccountManager(mock_dd)
    
    # Watch for updates from the browser
    last_print = 0
    try:
        while True:
            time.sleep(1)
            info = mock_dd["ea_account_info"].get("IFOREX_01")
            hb = mock_dd["ea_heartbeats"].get("IFOREX_01")
            
            if info and (time.time() - last_print > 2):
                last_print = time.time()
                print("\n[LIVE SYNC RECEIVED from iFOREX]")
                print(f"  Balance:            ${info.get('balance', 0.0):,.2f}")
                print(f"  Total Equity:       ${info.get('equity', 0.0):,.2f}")
                print(f"  Available Margin:   ${info.get('free_margin', 0.0):,.2f}")
                print(f"  Used Margin:        ${info.get('margin', 0.0):,.2f}")
                print(f"  Open PnL:           ${info.get('open_pl', 0.0):,.2f}")
                print(f"  EUR/USD Live Rate:  Bid {info.get('bid', 0.0)} / Ask {info.get('ask', 0.0)}")
                print(f"  Heartbeat Age:      {round(time.time() - hb, 1)}s ago")
    except KeyboardInterrupt:
        print("\nStopping bridge test...")

if __name__ == "__main__":
    main()
