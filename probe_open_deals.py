"""
probe_open_deals.py  —  Run on the server to find the correct open-deals endpoint.

Usage:
  python probe_open_deals.py

Reads configs/iforex_accounts.json, picks the iforex.eu account, and probes
a list of candidate endpoints.  Prints response status + structure for each.
"""
import json, os, sys, time, requests
from urllib.parse import urlparse

# ── locate the config ───────────────────────────────────────────────────────
SEARCH_PATHS = [
    r"C:\Users\admin\Documents\dev\2\TradeDashboardPY\configs\iforex_accounts.json",
    r"C:\Users\admin\Documents\dev\2\TradeDashboardPY\iforex_accounts.json",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs", "iforex_accounts.json"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "iforex_accounts.json"),
]

cfg_data = {}
for p in SEARCH_PATHS:
    if os.path.exists(p):
        with open(p) as f:
            cfg_data = json.load(f)
        print(f"[+] Loaded config from {p}")
        break

if not cfg_data:
    print("[ERROR] iforex_accounts.json not found in any search path")
    sys.exit(1)

# pick the .eu account (or first account)
acct_id, acct_cfg = None, None
for aid, cfg in cfg_data.items():
    if "iforex.eu" in cfg.get("base_url", ""):
        acct_id, acct_cfg = aid, cfg
        break
if not acct_cfg:
    acct_id, acct_cfg = next(iter(cfg_data.items()))

base_url = acct_cfg.get("base_url", "https://trader.iforex.com/webpl4").rstrip("/")
cookie   = acct_cfg.get("cookie", "")
parsed   = urlparse(base_url)
origin   = f"{parsed.scheme}://{parsed.netloc}"

print(f"[+] Account : {acct_id}")
print(f"[+] Base URL: {base_url}")
print()

session = requests.Session()
session.headers.update({
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":          f"{base_url}/trading/new-transaction",
    "Origin":           origin,
    "X-Requested-With": "XMLHttpRequest",
    "Accept":           "application/json, */*",
    "Cookie":           cookie,
})

CANDIDATES = [
    # closed deals (known-working — sanity check)
    "/api/closeddeals/LoadOrderedClosedDealSummary?page=1&pageSize=5",
    # open deals — most likely candidates
    "/api/opendeals/LoadOrderedOpenDealSummary",
    "/api/opendeals/GetData",
    "/api/opendeals/GetOpenDeals",
    "/Deals/GetOpenDeals",
    "/api/deals/GetOpenDeals",
    "/Portfolio/GetOpenDeals",
    "/Portfolio/GetData",
    "/api/portfolio/GetData",
    "/trading/GetOpenPositions",
    "/api/positions/GetOpen",
    "/Deals/GetActiveDeals",
    "/api/activedeals/GetData",
    # InitialData (known — but check what personalInfo / clientParams contain)
    "/InitialData/GetData?rnd=1",
]

for ep in CANDIDATES:
    url = f"{base_url}{ep}"
    try:
        r = session.get(url, timeout=8)
        status = r.status_code
        if status == 404:
            print(f"  404  {ep}")
            continue
        if status in (401, 403):
            print(f"  {status}  {ep}  ← AUTH ERROR")
            continue
        if status != 200:
            print(f"  {status}  {ep}")
            continue

        try:
            data = r.json()
        except Exception:
            print(f"  200  {ep}  ← non-JSON response")
            continue

        # summarise structure
        if isinstance(data, list):
            print(f"  200  {ep}  → list of {len(data)}")
            if data and isinstance(data[0], dict):
                print(f"       first item keys: {sorted(data[0].keys())[:20]}")
                if len(data[0]) < 30:
                    print(f"       first item: {json.dumps(data[0])[:400]}")
        elif isinstance(data, dict):
            top_keys = list(data.keys())
            print(f"  200  {ep}  → dict keys: {top_keys[:20]}")
            # look for any lists inside that could be deals
            for k, v in data.items():
                if isinstance(v, list) and v:
                    first = v[0]
                    if isinstance(first, dict) and len(first) > 1:
                        print(f"       .{k} = list[{len(v)}], first keys: {sorted(first.keys())[:15]}")
                        if len(v) > 0 and len(first) < 30:
                            print(f"       first: {json.dumps(first)[:400]}")
        else:
            print(f"  200  {ep}  → {type(data).__name__}: {str(data)[:200]}")

    except requests.exceptions.ConnectionError as e:
        print(f"  ERR  {ep}  ← {e}")
    except Exception as e:
        print(f"  ERR  {ep}  ← {e}")

print()
print("[done]")
