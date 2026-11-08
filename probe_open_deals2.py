"""
probe_open_deals2.py  —  Dump InitialData sub-keys + closed deals structure.
Run on the server:  python probe_open_deals2.py
"""
import json, os, sys, time, requests
from urllib.parse import urlparse

SEARCH_PATHS = [
    r"C:\Users\admin\Documents\dev\2\TradeDashboardPY\configs\iforex_accounts.json",
    r"C:\Users\admin\Documents\dev\2\TradeDashboardPY\iforex_accounts.json",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs", "iforex_accounts.json"),
]
cfg_data = {}
for p in SEARCH_PATHS:
    if os.path.exists(p):
        with open(p) as f: cfg_data = json.load(f)
        print(f"[+] Config: {p}"); break

acct_id, acct_cfg = None, None
for aid, cfg in cfg_data.items():
    if "iforex.eu" in cfg.get("base_url", ""):
        acct_id, acct_cfg = aid, cfg; break
if not acct_cfg:
    acct_id, acct_cfg = next(iter(cfg_data.items()))

base_url = acct_cfg.get("base_url", "").rstrip("/")
cookie   = acct_cfg.get("cookie", "")
parsed   = urlparse(base_url)
origin   = f"{parsed.scheme}://{parsed.netloc}"
print(f"[+] Account: {acct_id}  base: {base_url}\n")

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Referer":    f"{base_url}/trading/new-transaction",
    "Origin":     origin,
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, */*",
    "Cookie": cookie,
})

# ── 1. Dump InitialData sub-keys in full ────────────────────────────────────
print("=" * 60)
print("1. InitialData/GetData — full dump of each top-level value")
print("=" * 60)
r = session.get(f"{base_url}/InitialData/GetData?rnd={time.time()}", timeout=10)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    for k, v in data.items():
        print(f"\n--- {k!r} ({type(v).__name__}) ---")
        print(json.dumps(v, indent=2)[:2000])

# ── 2. Closed deals structure for reference ──────────────────────────────────
print("\n" + "=" * 60)
print("2. Closed deals (reference — to understand array format)")
print("=" * 60)
r2 = session.get(f"{base_url}/api/closeddeals/LoadOrderedClosedDealSummary",
                 params={"page": 1, "pageSize": 3}, timeout=10)
print(f"Status: {r2.status_code}")
if r2.status_code == 200:
    d2 = r2.json()
    print(f"Keys: {list(d2.keys())}")
    rows = d2.get("closedDealSummary", [])
    print(f"closedDealSummary: {len(rows)} rows")
    if rows:
        print(f"First row: {rows[0]}")

# ── 3. Try raw body of the non-JSON 200s to see what they really are ─────────
print("\n" + "=" * 60)
print("3. Raw body snippet of 'non-JSON' endpoints")
print("=" * 60)
for ep in ["/api/opendeals/LoadOrderedOpenDealSummary",
           "/Deals/GetOpenDeals",
           "/api/activedeals/GetData"]:
    r3 = session.get(f"{base_url}{ep}", timeout=8)
    body_start = r3.text[:200].replace("\n", " ").strip()
    print(f"  {ep}  [{r3.status_code}]  {body_start[:150]}")

# ── 4. Try POST variants of open deals ───────────────────────────────────────
print("\n" + "=" * 60)
print("4. POST variants")
print("=" * 60)
for ep, payload in [
    ("/api/opendeals/LoadOrderedOpenDealSummary", {"page": 1, "pageSize": 50}),
    ("/api/opendeals/GetData", {}),
]:
    r4 = session.post(f"{base_url}{ep}", json=payload, timeout=8)
    try:
        d4 = r4.json()
        print(f"  POST {ep}  [{r4.status_code}]  keys={list(d4.keys())[:10]}")
        for k,v in d4.items():
            if isinstance(v, list) and v:
                print(f"    .{k} list[{len(v)}] first={v[0]}")
    except Exception:
        print(f"  POST {ep}  [{r4.status_code}]  non-JSON: {r4.text[:100]}")

print("\n[done]")
