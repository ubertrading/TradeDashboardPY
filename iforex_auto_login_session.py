#!/usr/bin/env python3
"""
iforex_auto_login_session.py — Automated Session & Cookie Extractor

Uses Playwright with Microsoft Edge to:
1. Open the iFOREX platform in a persistent profile.
2. Wait for login completion.
3. Automatically extract ALL cookies (including HttpOnly cookies) & securityToken.
4. Save credentials to iforex_accounts.json for pure standalone Option B execution.
"""

import os
import json
import time
import logging
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("iforex_auto_session")

PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "iforex_edge_profile")

def main():
    print("=" * 60)
    print("      AUTOMATIC iFOREX CREDENTIAL & COOKIE EXTRACTOR      ")
    print("=" * 60)
    print("Opening Microsoft Edge window...")
    
    with sync_playwright() as p:
        from iforex_auto_login import launch_browser_context
        context = launch_browser_context(
            p,
            user_data_dir=PROFILE_DIR,
            headless=False,
            viewport=None,
            args=["--start-maximized"]
        )
        
        page = context.pages[0] if context.pages else context.new_page()
        page.goto("https://trader.iforex.com/webpl4/trading", wait_until="commit")
        
        print("\n>>> Please log into your iFOREX account in the browser window that just opened <<<")
        print("Waiting for login detection...")
        
        account_number = None
        security_token = None
        
        for _ in range(300):  # Wait up to 5 minutes
            time.sleep(1.0)
            try:
                # Check if systemInfo and customer objects exist
                is_logged_in = page.evaluate("""
                    (() => {
                        if (window.systemInfo && window.systemInfo.securityToken && window.$customer && window.$customer.prop) {
                            return {
                                loggedIn: true,
                                accountNumber: window.$customer.prop.accountNumber,
                                securityToken: window.systemInfo.securityToken
                            };
                        }
                        return { loggedIn: false };
                    })()
                """)
                
                if is_logged_in.get("loggedIn"):
                    account_number = str(is_logged_in.get("accountNumber"))
                    security_token = is_logged_in.get("securityToken")
                    print(f"\n[+] Login detected successfully! Account #{account_number}")
                    break
            except Exception:
                pass
        
        if not account_number:
            print("[ERROR] Login timeout. Please try again.")
            context.close()
            return

        # Extract ALL cookies (including HttpOnly ones) from the browser context
        all_cookies = context.cookies(["https://trader.iforex.com"])
        cookie_header = "; ".join([f"{c['name']}={c['value']}" for c in all_cookies])
        
        config = {
            "IFOREX_01": {
                "account_id": "IFOREX_01",
                "account_number": account_number,
                "cookie": cookie_header,
                "security_token": security_token,
                "base_url": "https://trader.iforex.com/webpl4",
                "enabled": True
            }
        }
        
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "iforex_accounts.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
            
        print(f"[+] Complete cookies (including HttpOnly) saved to {config_path}")
        print(f"[+] SecurityToken: {security_token}")
        
        context.close()
        print("\n>>> Extraction complete! Now running pure direct Option B test...")

if __name__ == "__main__":
    main()
