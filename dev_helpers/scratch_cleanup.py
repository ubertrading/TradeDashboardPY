import json

with open("trade_sessions.json", "r", encoding="utf-8") as f:
    data = json.load(f)

s = data["5af8f7ce-6368-489b-a8c4-b115f058f3c3"]
s["filled"] = {
    "DEMO-CT-FIX-HDBCH2-9040256": 3,
    "DUKA-DEMO-DEMO2uotdK": 0
}
s["closed"] = {
    "DEMO-CT-FIX-HDBCH2-9040256": 1,
    "DUKA-DEMO-DEMO2uotdK": 0
}
s["filled_lots"] = {
    "DEMO-CT-FIX-HDBCH2-9040256": 300.0,
    "DUKA-DEMO-DEMO2uotdK": 0.0
}
s["closed_lots"] = {
    "DEMO-CT-FIX-HDBCH2-9040256": 100.0,
    "DUKA-DEMO-DEMO2uotdK": 0.0
}
s["close_fills"] = [
    {
      "account": "DEMO-CT-FIX-HDBCH2-9040256",
      "ticket": 604179075,
      "price": 159.902,
      "ts": "2026-06-02 20:30:21",
      "ts_epoch": 1780446621.445,
      "open_price": 158.694,
      "open_ts": "2026.04.09 00:16:23",
      "open_ts_epoch": 1775693783.736
    }
]
s["rollback_needed"] = {}
s["rollback_tickets"] = {}
s["rollback_start_ts"] = {}
s["spread_rejects"] = {
    "DEMO-CT-FIX-HDBCH2-9040256": 0,
    "DUKA-DEMO-DEMO2uotdK": 0
}

with open("trade_sessions.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print("trade_sessions.json cleaned up successfully!")
