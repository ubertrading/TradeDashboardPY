import urllib.request
import json

resp = urllib.request.urlopen("http://127.0.0.2/api/status", timeout=5)
data = json.loads(resp.read().decode())

sessions = data.get("sessions", [])
print("Session 54c3438e in /api/status:")
for s in sessions:
    if isinstance(s, dict) and s.get("id", "").startswith("54c3438e"):
        print(json.dumps(s, indent=2))

direct_accts = data.get("mt_direct_accounts", {})
print("\nmt_direct_accounts:", json.dumps(direct_accts, indent=2))
