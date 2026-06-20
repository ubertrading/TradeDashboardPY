import urllib.request, json
req = urllib.request.Request("http://127.0.0.2/api/pnl/request", method="POST")
req.add_header('Content-Type', 'application/json')
data = json.dumps({"name": "HUGO", "from_date": "2026-06-03", "to_date": "2026-06-17", "exclude_balance": True})
try:
    with urllib.request.urlopen(req, data=data.encode('utf-8')) as f:
        print("Status:", f.status)
        print(f.read().decode('utf-8'))
        resp = json.loads(f.read().decode('utf-8'))
        req_id = resp.get("request_id")
except Exception as e:
    print("Error:", e)
    if hasattr(e, 'read'):
        print(e.read().decode('utf-8'))
