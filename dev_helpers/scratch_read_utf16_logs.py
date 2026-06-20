import os

log_path = r"d:\Documents\dev\trade_dashboard-BADMARGINALERT\bridge_log.txt"

if not os.path.exists(log_path):
    print("Log file not found")
    sys.exit(1)

print("Reading log...")
with open(log_path, "r", encoding="utf-16") as f:
    lines = f.readlines()

print(f"Total lines: {len(lines)}")
# Find lines with JU-3-A-MEX950059 and print them with context
for idx, line in enumerate(lines):
    if "JU-3-A-MEX950059" in line:
        print(f"--- Line {idx+1} ---")
        # Print 2 lines before and 2 lines after
        start = max(0, idx - 2)
        end = min(len(lines), idx + 5)
        for i in range(start, end):
            prefix = " > " if i == idx else "   "
            print(f"{prefix}{i+1}: {lines[i].strip()}")
