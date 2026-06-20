import os, sys, time

DLL_DIR = r"d:\Documents\dev\trade_dashboard-BADMARGINALERT\MT-DLLS"
os.environ["PATH"] = DLL_DIR + os.pathsep + os.environ.get("PATH", "")

from pythonnet import load as _pn_load
try: _pn_load("coreclr")
except RuntimeError: pass

import clr
sys.path.append(DLL_DIR)
clr.AddReference(os.path.join(DLL_DIR, "mt5api.dll"))
from mtapi.mt5 import MT5API

# Credentials for JU-3-A-MEX950059
login = 950059
password = "8sJbrV)3"
host = "192.109.15.176"
port = 443

print(f"Connecting to {host}:{port} as {login}...")
api = MT5API(login, password, host, port)

# Enable connection progress logging
def on_progress(sender, args):
    print(f"Progress: {args.Progress}")
api.OnConnectProgress += on_progress

try:
    api.Connect()
    print("Connect called.")
except Exception as e:
    print(f"Connect failed to call: {e}")

# Wait up to 10 seconds for connection
for i in range(10):
    if api.Connected:
        break
    time.sleep(1)

print(f"Connected: {api.Connected}")
if api.Connected:
    # Print direct properties immediately
    print("\n--- Direct Properties (Immediate) ---")
    print(f"AccountBalance: {getattr(api, 'AccountBalance', 'N/A')}")
    print(f"AccountEquity: {getattr(api, 'AccountEquity', 'N/A')}")
    print(f"AccountFreeMargin: {getattr(api, 'AccountFreeMargin', 'N/A')}")
    print(f"AccountMargin: {getattr(api, 'AccountMargin', 'N/A')}")
    print(f"AccountProfit: {getattr(api, 'AccountProfit', 'N/A')}")
    print(f"MarginLevel: {getattr(api, 'MarginLevel', 'N/A')}")

    # Check Account sub-object
    acct = getattr(api, 'Account', None)
    if acct:
        print("\n--- Account Sub-Object ---")
        for attr in dir(acct):
            if not attr.startswith('_'):
                try:
                    print(f"  {attr}: {getattr(acct, attr)}")
                except Exception as e:
                    print(f"  {attr}: Error: {e}")
    else:
        print("\nAccount sub-object is None")

    # Sleep to allow async data to arrive and check again
    print("\nSleeping 5 seconds for async updates...")
    time.sleep(5)

    print("\n--- Direct Properties (After 5s) ---")
    print(f"AccountBalance: {getattr(api, 'AccountBalance', 'N/A')}")
    print(f"AccountEquity: {getattr(api, 'AccountEquity', 'N/A')}")
    print(f"AccountFreeMargin: {getattr(api, 'AccountFreeMargin', 'N/A')}")
    print(f"AccountMargin: {getattr(api, 'AccountMargin', 'N/A')}")
    print(f"AccountProfit: {getattr(api, 'AccountProfit', 'N/A')}")
    print(f"MarginLevel: {getattr(api, 'MarginLevel', 'N/A')}")

    if acct:
        print("\n--- Account Sub-Object (After 5s) ---")
        for attr in dir(acct):
            if not attr.startswith('_'):
                try:
                    print(f"  {attr}: {getattr(acct, attr)}")
                except Exception as e:
                    print(f"  {attr}: Error: {e}")

    # Check positions
    try:
        orders = api.GetOpenedOrders()
        print(f"\nOpen positions count: {len(orders)}")
        for idx, o in enumerate(orders):
            print(f"  Position {idx}: Ticket={o.Ticket}, Symbol={o.Symbol}, Lots={o.Lots}, Profit={o.Profit}, Swap={o.Swap}")
    except Exception as e:
        print(f"Error getting open positions: {e}")

    try:
        api.Disconnect()
        print("Disconnected.")
    except Exception as e:
        print(f"Disconnect error: {e}")
