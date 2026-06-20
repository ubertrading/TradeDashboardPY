"""Quick test: parameterless QuoteClient + LoginIdExPath + Connect()"""
import os, sys, time

DLL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MT-DLLS")
os.environ["PATH"] = DLL_DIR + os.pathsep + os.environ.get("PATH", "")

from pythonnet import load as _pn_load
try: _pn_load("coreclr")
except RuntimeError: pass

import clr
sys.path.append(DLL_DIR)
clr.AddReference(os.path.join(DLL_DIR, "MT4ServerAPI.dll"))
from TradingAPI.MT4Server import QuoteClient

client = QuoteClient()
print(f"Default LoginIdExPath: {client.LoginIdExPath}")
print(f"Default HardwareId: {client.HardwareId}")

# Set local LoginId.dll
login_id = os.path.join(DLL_DIR, "LoginId.dll")
client.LoginIdExPath = login_id
print(f"After set LoginIdExPath: {client.LoginIdExPath}")
print(f"After set HardwareId: {client.HardwareId}")

# Set credentials
client.User = 31336621
client.Password = "np4usgg"
client.Host = "98.158.104.79"
client.Port = 443

print(f"\nConnecting to {client.Host}:{client.Port} as {client.User}...")
print(f"HardwareId before Connect: {client.HardwareId}")

try:
    client.Connect()
    time.sleep(2)
except Exception as e:
    print(f"Connect() threw: {e}")

print(f"\nConnected = {client.Connected}")
print(f"HardwareId = {client.HardwareId}")
print(f"ServerName = {client.ServerName}")
print(f"ServerTime = {client.ServerTime}")
print(f"AccountName = {client.AccountName}")
print(f"AccountBalance = {client.AccountBalance}")

if not client.Connected:
    print("\n=== FAILED - diagnostics ===")
    for attr in ['ServerBuild', 'ClientBuild', 'SoftId', 'ApiKey', 
                 'IsInvestor', 'ConnectTime', 'LoginIdTimeoutMs']:
        try: print(f"  {attr} = {getattr(client, attr)}")
        except: pass
    
    # Try with different LoginIdExPath (directory instead of file)
    print("\n=== Trying LoginIdExPath as directory ===")
    client2 = QuoteClient()
    client2.LoginIdExPath = DLL_DIR  # directory, not file
    client2.User = 31336621
    client2.Password = "np4usgg"
    client2.Host = "98.158.104.79"
    client2.Port = 443
    print(f"LoginIdExPath = {client2.LoginIdExPath}")
    try:
        client2.Connect()
        time.sleep(2)
    except Exception as e:
        print(f"Connect() threw: {e}")
    print(f"Connected = {client2.Connected}")
    print(f"HardwareId = {client2.HardwareId}")
    print(f"AccountName = {client2.AccountName}")
else:
    print("\n=== SUCCESS! ===")
    try: client.Disconnect()
    except: pass
