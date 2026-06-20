import sys, json
sys.path.append('d:/Documents/dev/TradeDashboard/TradeDashboardPY')

# Simulate exactly what _calculate_optimal_fund_distributions receives on production
# based on screenshot values
dummy_data = {
    "GO-1-A-SQ651938":     {"label": "GO-1-A-SQ651938",  "equity": 92268.28,  "margin": 1000.0,  "leverage": 400},
    "GO-1-B-ICM7991931":   {"label": "GO-1-B-ICM7991931","equity": 28404.85,  "margin": 500.0,   "leverage": 500},
    "HUGO-1-A-SQ649159":   {"label": "HUGO-1-A-SQ649159","equity": 51234.30,  "margin": 0.0,     "leverage": 400},
    "HUGO-1-B-ORB808723":  {"label": "HUGO-1-B-ORB808723","equity": 28563.96, "margin": 0.0,     "leverage": 500},
    "JU-1-A-SQ651189":     {"label": "JU-1-A-SQ651189",  "equity": 200779.94, "margin": 0.0,     "leverage": 400},
    "JU-1-B-ICM11609373":  {"label": "JU-1-B-ICM11609373","equity": 209497.82,"margin": 0.0,     "leverage": 1000},
    "JU-1-B-ICM7905211":   {"label": "JU-1-B-ICM7905211","equity": 0.83,      "margin": 0.0,     "leverage": 1000},
    "JU-1-B-ICM7969844":   {"label": "JU-1-B-ICM7969844","equity": 204845.00, "margin": 0.0,     "leverage": 1000},
    "JU-2-A-SQ651572":     {"label": "JU-2-A-SQ651572",  "equity": 201516.82, "margin": 0.0,     "leverage": 400},
    "JU-2-B-ICM7992910":   {"label": "JU-2-B-ICM7992910","equity": 1.10,      "margin": 0.0,     "leverage": 1000},
    "JU-2-B-ICM8024646":   {"label": "JU-2-B-ICM8024646","equity": None,      "margin": None,    "leverage": None},
}

# --- Patch: simulate no fix_manager / mt_direct_manager (like the function sees at global scope)
import trade_dashboard as td
td.fix_manager = None
td.mt_direct_manager = None
td.manual_accounts = {}

result = td._calculate_optimal_fund_distributions(dummy_data)
print("=== Groups formed and results ===")
print(json.dumps(result, indent=2))

# Also test what happens with label=None (ea_status path — no label key in ainfo)
print("\n=== Test: accounts WITHOUT label key in ainfo ===")
dummy_no_label = {
    "GO-1-A-SQ651938":     {"equity": 92268.28,  "margin": 1000.0,  "leverage": 400},
    "GO-1-B-ICM7991931":   {"equity": 28404.85,  "margin": 500.0,   "leverage": 500},
}
result2 = td._calculate_optimal_fund_distributions(dummy_no_label)
print(json.dumps(result2, indent=2))
