How to Copy the Complete Cookie (Including HttpOnly)
Option 1: Copy via Network Tab (Recommended - 30 seconds)
In the browser (Edge or Chrome) where you are logged into https://trader.iforex.com/webpl4/trading:
Press F12 to open Developer Tools and select the Network tab.
Click the Fetch/XHR filter button at the top.
Click on any instrument in iFOREX (or click anywhere to make a request like GetDealMarginDetails, GetTicks, or Ping appear in the list).
Click on that request in the list.
In the right-hand panel, under Headers → Request Headers, look for Cookie:.
Right-click on the cookie value → select Copy value (this guarantees FXnetWeb_identity and .AspNetCore.Session are included).
Go to your Trade Dashboard at http://127.0.0.2, open the Accounts tab, click ✎ (Edit) on the iFOREX account, paste the new Cookie, and click Save.
(Also check the Console tab by typing window.systemInfo.securityToken and make sure the Security Token field in the edit modal matches).

Option 2: Automatic Extraction via Playwright
You can also run the automated extractor script in a PowerShell terminal:

powershell


python iforex_auto_login_session.py
This opens Edge, captures all cookies (including HttpOnly ones) and the security_token automatically upon login, and saves them directly to configs/iforex_accounts.json.




How to Add an iFOREX Account via GUI
Open the Dashboard: Navigate to the Accounts tab. At the top right, you will see the new amber + Add iFOREX button:

[+ Add EA Account]  [+ Add API Account]  [+ Add MT Direct]  [+ Add iFOREX]
Click + Add iFOREX to open the modal:

Account ID: Enter a unique identifier (e.g., IFOREX_01).
Account Number: Your iFOREX trading account login (e.g., 12279333).
Display / Group Label: Name or group prefix (e.g., iFOREX-12279333).
Leverage: Default is 400 (1:400).
Security Token:
In your logged-in browser tab at https://trader.iforex.com/webpl4/trading, open F12 Console and type:
javascript


window.systemInfo.securityToken
Paste that number into the field (e.g., 1054717647).
Cookie Header:
In browser F12 Network tab, click any request (e.g., Ping or GetDealMarginDetails), find Cookie: under Request Headers, right-click → Copy value, and paste it into the textarea.
Base URL: Defaults to https://trader.iforex.com/webpl4.
Click Add Account:

The account will be saved to iforex_accounts.json and immediately start background polling.
It will appear in the Accounts table with live status (green ● iFOREX), balance, equity, leverage, margin use %, and PnL.
Managing the Account
Edit / Refresh Session: Click the ✎ (Edit) button on the account row to update its settings or paste a fresh Cookie when the browser session expires.
Delete: Click the x button to remove the account from the dashboard.
Strategy Selection: The iFOREX account will automatically be available in the dropdown when creating or editing trading strategies.