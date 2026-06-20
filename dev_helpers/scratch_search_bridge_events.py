import os, sys

log_path = r"d:\Documents\dev\trade_dashboard-BADMARGINALERT\bridge_log.txt"

if not os.path.exists(log_path):
    print("Log file not found")
    sys.exit(1)

print("Reading log...")
with open(log_path, "r", encoding="utf-16") as f:
    lines = f.readlines()

print(f"Total lines: {len(lines)}")
keywords = ["connecting", "connected", "disconnect", "connection loss", "reconnect", "attempt", "exception", "error"]

for idx, line in enumerate(lines):
    line_lower = line.lower()
    if any(kw in line_lower for kw in keywords):
        # Skip routine HTTP requests/responses logs that might contain these keywords
        if "Request starting" in line or "Request finished" in line or "Executing endpoint" in line or "Executed endpoint" in line:
            continue
        print(f"Line {idx+1}: {line.strip()}")
