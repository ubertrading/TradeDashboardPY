#!/usr/bin/env python3
"""
iforex_auto_login.py — Fully Automated Credentials & Session Extractor for iFOREX
Launches Microsoft Edge with persistent user profile, auto-fills Username & Password
(if provided), handles cookie consent and auto-login, waits for platform initialization,
extracts ALL cookies (including HttpOnly FXnetWeb_identity and .AspNetCore.Session)
plus securityToken, and updates configs/iforex_accounts.json & running instances.
"""

import os
import json
import time
import logging
import re
from typing import Dict, Any, Optional, Tuple, List
from playwright.sync_api import sync_playwright

logger = logging.getLogger("iforex_auto_login")

PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "iforex_edge_profile")

def refresh_iforex_session(account_id: str = "12141021",
                           config_dir: str = "configs",
                           username: Optional[str] = None,
                           password: Optional[str] = None,
                           timeout_sec: int = 120,
                           headless: bool = False) -> Dict[str, Any]:
    """
    Automates login and session extraction for an iFOREX account.
    If username/password are provided or present in config, fills the form and submits.
    If 2FA is required, keeps browser window open for user to complete.
    Saves new cookies & securityToken to config files.
    """
    cfg_paths = [
        os.path.join(config_dir, "iforex_accounts.json"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "iforex_accounts.json")
    ]
    
    # Load stored credentials from config if not passed
    if not username or not password:
        for cpath in cfg_paths:
            if os.path.exists(cpath):
                try:
                    with open(cpath, "r", encoding="utf-8") as f:
                        cfg_data = json.load(f)
                    acct_data = cfg_data.get(account_id) or (list(cfg_data.values())[0] if cfg_data else {})
                    if acct_data:
                        if not username:
                            username = acct_data.get("username") or acct_data.get("account_number")
                        if not password:
                            password = acct_data.get("password")
                except Exception:
                    pass
                if username and password:
                    break

    logger.info("Starting iFOREX session refresh for account %s (user=%s, headless=%s)...",
                account_id, username or "None", headless)
    
    with sync_playwright() as p:
        try:
            args = ["--start-maximized", "--disable-blink-features=AutomationControlled"] if not headless else ["--disable-blink-features=AutomationControlled"]
            context = p.chromium.launch_persistent_context(
                user_data_dir=PROFILE_DIR,
                channel="msedge",
                headless=headless,
                viewport=None,
                args=args
            )
        except Exception as e:
            logger.error("Failed to launch Edge persistent context: %s", e)
            return {"status": "error", "message": f"Failed to launch Edge: {e}"}

        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://trader.iforex.com/webpl4/trading", wait_until="domcontentloaded", timeout=25000)
            time.sleep(2.0)

            # 1. Accept cookie banner if present
            try:
                cookie_btn = page.query_selector("#cookies_ok")
                if cookie_btn and cookie_btn.is_visible():
                    cookie_btn.click()
                    time.sleep(0.5)
            except Exception:
                pass

            # 2. Check if login form is displayed
            uname_input = page.query_selector("#txtUName")
            pass_input = page.query_selector("#txtPass")

            if uname_input and pass_input and username and password:
                logger.info("Login form detected — entering username and password automatically...")
                try:
                    uname_input.fill(str(username))
                    pass_input.fill(str(password))
                    
                    # Check autologin checkbox
                    autologin_chk = page.query_selector("#autologin")
                    if autologin_chk and not autologin_chk.is_checked():
                        autologin_chk.check()

                    time.sleep(0.5)
                    login_btn = page.query_selector("#btnOkLogin")
                    if login_btn:
                        login_btn.click()
                        logger.info("Login form submitted, waiting for platform session...")
                except Exception as ex:
                    logger.warning("Error submitting login form: %s", ex)

            # 3. Wait for login completion and securityToken detection
            account_number = None
            security_token = None
            start_t = time.time()

            while time.time() - start_t < timeout_sec:
                time.sleep(1.0)
                try:
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
                        security_token = int(is_logged_in.get("securityToken"))
                        logger.info("Login confirmed! Account #%s, SecurityToken: %s", account_number, security_token)
                        break
                except Exception:
                    pass

            if not security_token:
                context.close()
                return {"status": "error", "message": "Login timeout: security token not detected. Please verify credentials or complete 2FA."}

            # 4. Extract all cookies including HttpOnly cookies (must include /webpl4 path)
            all_cookies = context.cookies(["https://trader.iforex.com/webpl4", "https://trader.iforex.com"])
            cookie_header = "; ".join([f"{c['name']}={c['value']}" for c in all_cookies])
            cookie_names = [c['name'] for c in all_cookies]
            logger.info("Extracted %d cookies (has FXnetWeb_identity: %s): %s",
                        len(cookie_names), "FXnetWeb_identity" in cookie_names, cookie_names)

            context.close()

            # 5. Update configuration files
            saved = False
            for cpath in cfg_paths:
                try:
                    if os.path.exists(cpath):
                        with open(cpath, "r", encoding="utf-8") as f:
                            data = json.load(f)
                    else:
                        data = {}
                    acct_cfg = data.get(account_id) or (list(data.values())[0] if data else {})
                    if not acct_cfg:
                        acct_cfg = {"account_id": account_id, "label": f"iFOREX-{account_id}", "enabled": True}
                    acct_cfg["cookie"] = cookie_header
                    acct_cfg["security_token"] = security_token
                    if username:
                        acct_cfg["username"] = username
                    if password:
                        acct_cfg["password"] = password
                    if account_number:
                        acct_cfg["account_number"] = account_number
                    data[account_id] = acct_cfg
                    with open(cpath, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2)
                    saved = True
                    logger.info("Saved fresh credentials to %s", cpath)
                except Exception as ex:
                    logger.warning("Could not save to %s: %s", cpath, ex)

            return {
                "status": "ok",
                "account_id": account_id,
                "account_number": account_number,
                "security_token": security_token,
                "cookie": cookie_header,
                "cookies_count": len(all_cookies),
                "has_identity": "FXnetWeb_identity" in cookie_names,
                "message": f"Successfully authenticated account #{account_number}!"
            }
        except Exception as e:
            try:
                context.close()
            except Exception:
                pass
            logger.error("Error during session extraction: %s", e)
            return {"status": "error", "message": str(e)}


def _parse_currency(val_str: Any) -> float:
    """Parse currency strings like 'Fr. 8,997.83' or '-Fr. 0.08' to float."""
    if not val_str:
        return 0.0
    s = str(val_str).replace(chr(160), " ").replace(",", "").strip()
    is_neg = "-" in s
    m = re.search(r'\d+(?:\.\d+)?', s)
    if m:
        try:
            val = float(m.group(0))
            return -val if is_neg else val
        except ValueError:
            return 0.0
    return 0.0


def fetch_active_deals_and_summary(timeout_sec: int = 25) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    """
    Launch headless Edge with persistent profile to read active positions AND live
    account summary (balance, equity, margin, free_margin, open_pl) directly from the
    iFOREX WebPL4 interface (React DOM).
    Returns: (deals_list, summary_dict)
    """
    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                user_data_dir=PROFILE_DIR,
                headless=True,
                channel="msedge",
                args=["--disable-blink-features=AutomationControlled"]
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://trader.iforex.com/webpl4/trading/new-transaction", wait_until="domcontentloaded", timeout=timeout_sec * 1000)
            try:
                page.wait_for_selector("#accSummaryAccountBalance", timeout=12000)
            except Exception:
                pass  # Best-effort wait; proceed anyway
            time.sleep(1.0)  # Allow deal rows to render after summary appears

            raw_data = page.evaluate("""
                (() => {
                    const rows = Array.from(document.querySelectorAll('[data-automation^="deal-details-"]'));
                    const deals = [];
                    for (let el of rows) {
                        const reactKey = Object.keys(el).find(k => k.startsWith('__reactFiber') || k.startsWith('__reactInternalInstance'));
                        if (reactKey && el[reactKey]) {
                            let fiber = el[reactKey];
                            let depth = 0;
                            while (fiber && depth < 20) {
                                if (fiber.memoizedProps && fiber.memoizedProps.deal) {
                                    const d = fiber.memoizedProps.deal;
                                    deals.push({
                                        ticket: String(d.positionNumber || d.orderID || ''),
                                        exeTime: d.exeTime,
                                        instrumentID: d.instrumentID,
                                        orderDir: d.orderDir,
                                        dealAmount: d.dealAmount,
                                        orderRate: d.orderRateNumeric || d.orderRate,
                                        plNumeric: d.plNumeric
                                    });
                                    break;
                                }
                                fiber = fiber.return;
                                depth++;
                            }
                        }
                    }

                    const summary = {};
                    const elBalance = document.getElementById('accSummaryAccountBalance');
                    if (elBalance) summary.balance = elBalance.innerText;
                    
                    const elEquity = document.getElementById('accSummaryEquity');
                    if (elEquity) summary.equity = elEquity.innerText;
                    
                    const elMargin = document.getElementById('accSummaryUsedMargin');
                    if (elMargin) summary.margin = elMargin.innerText;
                    
                    const elFreeMargin = document.getElementById('accSummaryMargin');
                    if (elFreeMargin) summary.free_margin = elFreeMargin.innerText;
                    
                    const elOpenPL = document.getElementById('accSummaryOpenPL');
                    if (elOpenPL) summary.open_pl = elOpenPL.innerText;

                    const elUtilization = document.getElementById('accSummaryMarginUtilization');
                    if (elUtilization) summary.margin_utilization = elUtilization.innerText;

                    const elExposure = document.getElementById('accSummaryExposureCoverage');
                    if (elExposure) summary.exposure_coverage = elExposure.innerText;

                    const bodyText = document.body ? document.body.innerText : '';
                    summary.market_closed = bodyText.includes('Market is closed') || bodyText.includes('Market Closed');

                    return { deals, summary };
                })()
            """)
            context.close()

            raw_deals = raw_data.get("deals", []) if isinstance(raw_data, dict) else []
            raw_summary = raw_data.get("summary", {}) if isinstance(raw_data, dict) else {}

            parsed_summary = {
                "balance": _parse_currency(raw_summary.get("balance")),
                "equity": _parse_currency(raw_summary.get("equity")),
                "margin": _parse_currency(raw_summary.get("margin")),
                "free_margin": _parse_currency(raw_summary.get("free_margin")),
                "open_pl": _parse_currency(raw_summary.get("open_pl")),
                "market_closed": bool(raw_summary.get("market_closed", False)),
            }

            # Map to standard format
            out = []
            for d in (raw_deals or []):
                t = str(d.get("ticket", "")).strip()
                if not t:
                    continue
                inst_id = d.get("instrumentID")
                # Instrument reverse map
                from iforex_connector import REVERSE_INSTRUMENT_MAP, notional_to_lots
                sym = REVERSE_INSTRUMENT_MAP.get(inst_id, "EURUSD")
                direction = "buy" if d.get("orderDir") == 1 else "sell"
                # amount clean
                amt_str = str(d.get("dealAmount", "1000")).replace(",", "").replace(" ", "")
                try:
                    amt = float(amt_str)
                except ValueError:
                    amt = 1000.0
                lots = notional_to_lots(sym, amt)
                # parse open time
                raw_time = str(d.get("exeTime") or "").strip()
                open_time_str = raw_time
                open_epoch = None
                if raw_time:
                    for fmt in ("%d/%m/%y %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%m/%d/%y %H:%M:%S"):
                        try:
                            from datetime import datetime, timezone
                            dt = datetime.strptime(raw_time, fmt)
                            open_epoch = dt.replace(tzinfo=timezone.utc).timestamp()
                            open_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            break
                        except Exception:
                            continue

                order_rate = float(d.get("orderRate", 0.0) or 0.0)
                out.append({
                    "Ticket": t,
                    "ticket": t,
                    "Symbol": sym,
                    "symbol": sym,
                    "Type": direction,
                    "direction": direction,
                    "Lots": lots,
                    "lots": lots,
                    "Amount": amt,
                    "OpenPrice": order_rate,
                    "open_price": order_rate,
                    "Profit": float(d.get("plNumeric", 0.0) or 0.0),
                    "OpenTime": open_time_str,
                    "open_time": open_time_str,
                    "open_epoch": open_epoch,
                    "comment": "",
                })
            return out, parsed_summary
        except Exception as e:
            logger.debug("fetch_active_deals_and_summary error: %s", e)
            return None, {}


def fetch_active_deals(timeout_sec: int = 25) -> list:
    """
    Launch headless Edge with persistent profile to read active positions directly
    from the iFOREX WebPL4 interface (React DOM).
    Maintains backward compatibility with callers expecting a list of deals.
    """
    deals, _ = fetch_active_deals_and_summary(timeout_sec=timeout_sec)
    return deals


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    res = refresh_iforex_session()
    print("Result:", json.dumps(res, indent=2))

