import urllib.request
import json

try:
    with urllib.request.urlopen("http://127.0.0.2/api/status", timeout=5) as response:
        status_data = json.loads(response.read().decode())
    
    print("mt_direct_accounts:")
    print(json.dumps(status_data.get("mt_direct_accounts"), indent=2))
    print("\nea_heartbeats:")
    print(json.dumps(status_data.get("ea_heartbeats"), indent=2))
except Exception as e:
    print(f"Error calling API: {e}")
