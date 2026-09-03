================================================================================
                    iFOREX INTEGRATION GUIDE (TradeDashboardPY)
================================================================================

This document explains the two connection architectures available for iFOREX:
  - Type A: Browser Proxy (Tampermonkey / DevTools Bridge)
  - Type B: Pure Direct HTTP (Headless Connector - Currently Integrated)

--------------------------------------------------------------------------------
1. OVERVIEW & ARCHITECTURE COMPARISON
--------------------------------------------------------------------------------

+----------------------+---------------------------+---------------------------+
| Feature              | Type A: Browser Proxy     | Type B: Direct HTTP       |
+----------------------+---------------------------+---------------------------+
| Underlying Engine    | Real browser session      | Pure Python requests      |
| WAF / Bot Bypass     | 100% Native (in browser)  | Uses exported cookies     |
| Session Persistence  | Auto-refreshes by browser | Requires manual cookie    |
|                      |                           | renewal when expired      |
| Speed / Latency      | ~10-25ms (via WebSocket)  | ~2-10ms (Direct HTTP)     |
| Headless Execution   | Requires browser open     | Fully headless            |
| Configuration File   | Tampermonkey script       | `iforex_accounts.json`    |
+----------------------+---------------------------+---------------------------+


================================================================================
2. TYPE B: PURE DIRECT HTTP (INTEGRATED INTO DASHBOARD)
================================================================================

Type B communicates directly with the iFOREX ASP.NET trading backend at:
  https://trader.iforex.com/webpl4

Endpoints used:
  - Open Deal:   POST /webpl4/Deals/OpenDeal
  - Close Deal:  POST /webpl4/Deals/CloseDeals
  - Margin Info: GET  /webpl4/Deals/GetDealMarginDetails?instrumentId={id}
  - Risk Info:   GET  /webpl4/Deals/GetDealRiskDetails?instrumentId={id}
  - Swap Info:   GET  /webpl4/Deals/GetOvernightFinancing?instrumentId={id}&amount={amt}

--------------------------------------------------------------------------------
HOW TO SET UP & CONFIGURE TYPE B:
--------------------------------------------------------------------------------

1. Log in to iFOREX in your browser (Chrome or Edge):
   https://trader.iforex.com/webpl4/trading

2. Open Browser Developer Tools (F12) -> Console tab:
   Type:
     window.systemInfo.securityToken
   Note down this number (e.g. 1054717647).

3. Go to Developer Tools (F12) -> Network tab:
   - Filter by "Fetch/XHR"
   - Click on any active network request (e.g. `GetDealMarginDetails`, `GetDealRiskDetails`, or `Ping`)
   - In Request Headers, locate the `Cookie:` header.
   - Right-click the Cookie value -> "Copy value".
   (Ensure it includes HttpOnly cookies: `FXnetWeb_identity`, `.AspNetCore.Session`, `RmData`, `TS01d34e12`, `rbzid`, `rbzsessionid`).

4. Open `iforex_accounts.json` in `TradeDashboardPY`:
   Configure as follows:

   {
     "IFOREX_01": {
       "account_id": "IFOREX_01",
       "account_number": "YOUR_ACCOUNT_NUMBER",
       "label": "iFOREX-12279333",
       "cookie": "PASTE_ENTIRE_COOKIE_HEADER_HERE",
       "security_token": 1054717647,
       "base_url": "https://trader.iforex.com/webpl4",
       "leverage": 400,
       "enabled": true
     }
   }

5. Start the dashboard:
   python trade_dashboard.py

   - `IFOREX_01` will appear in the Accounts tab with live heartbeat and margin monitoring.
   - If session expires (returns 401 Unauthorized), simply grab the fresh Cookie header from browser F12 and paste it into `iforex_accounts.json`.


================================================================================
3. TYPE A: BROWSER PROXY (TAMPERMONKEY / USERSCRIPT BRIDGE)
================================================================================

Type A runs a local WebSocket server (`iforex_bridge.py` on port 8765). A Tampermonkey
userscript running inside your logged-in iFOREX tab connects to this WebSocket and executes
commands natively in the page context.

Advantages:
  - You never have to manually copy or refresh cookies.
  - Browser automatically handles 2FA, session refresh, WAF token renewal.

--------------------------------------------------------------------------------
HOW TO SET UP TYPE A:
--------------------------------------------------------------------------------

1. Install Tampermonkey extension in Chrome/Edge:
   https://www.tampermonkey.net/

2. Create a new Tampermonkey Userscript with this code:

// ==UserScript==
// @name         iFOREX TradeDashboard Bridge
// @namespace    http://tampermonkey.net/
// @version      1.0
// @description  Connects iFOREX browser session to TradeDashboardPY
// @match        https://trader.iforex.com/*
// @grant        none
// @run-at       document-end
// ==/UserScript==

(function() {
    'use strict';
    console.log('[iFOREX-Bridge] Initializing bridge client...');

    let ws = null;
    function connect() {
        ws = new WebSocket('ws://127.0.0.1:8765');
        ws.onopen = function() {
            console.log('[iFOREX-Bridge] Connected to Python Bridge server!');
        };
        ws.onmessage = async function(evt) {
            let msg;
            try {
                msg = JSON.parse(evt.data);
            } catch(e) { return; }

            const reqId = msg.id;
            const code = msg.code;
            try {
                let result = await eval(code);
                ws.send(JSON.stringify({ id: reqId, result: result }));
            } catch(err) {
                ws.send(JSON.stringify({ id: reqId, error: err.toString() }));
            }
        };
        ws.onclose = function() {
            console.warn('[iFOREX-Bridge] WebSocket closed. Reconnecting in 3s...');
            setTimeout(connect, 3000);
        };
        ws.onerror = function() {
            ws.close();
        };
    }
    setTimeout(connect, 2000);
})();

3. Start the bridge server on your machine:
   python iforex_bridge.py

4. Open https://trader.iforex.com/webpl4/trading in your browser.
   The browser tab will connect to `ws://127.0.0.1:8765` and bridge commands automatically.


================================================================================
4. INSTRUMENT ID REFERENCE
================================================================================

Below are the mapped internal instrument IDs used by iFOREX WebPL4:

| Symbol     | Instrument ID | Default Margin |
|------------|---------------|----------------|
| EUR/USD    | 3631          | 0.25% (1:400)  |
| USD/JPY    | 12055         | 0.25% (1:400)  |
| GBP/USD    | 4143          | 0.25% (1:400)  |
| AUD/USD    | 303           | 0.25% (1:400)  |
| NZD/USD    | 7727          | 0.25% (1:400)  |
| USD/CAD    | 12035         | 0.25% (1:400)  |
| USD/CHF    | 12036         | 0.25% (1:400)  |
| GOLD / XAU | 13103         | 0.50% (1:200)  |
| OIL / CL   | 24623         | 1.00% (1:100)  |

================================================================================
