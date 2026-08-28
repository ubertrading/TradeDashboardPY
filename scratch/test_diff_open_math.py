import json
import sys
sys.path.insert(0, r"d:\Documents\dev\TradeDashboard\TradeDashboardPY")
import trade_dashboard

# Test diff_to_open check with realistic prices
diff_to_open = -17 # Threshold set in session 54c3438e

# Case A: Current diff is -25 (e.g. Orbex Bid = 1.09975, Swissquote Ask = 1.10000)
bid2_a = 1.09975
ask1_a = 1.10000
diff_a = round((bid2_a - ask1_a) * 100000, 1) # -25.0

print(f"Case A: diff = {diff_a}, threshold = {diff_to_open}")
print(f"  diff < threshold: {diff_a < diff_to_open} -> BLOCKED by diff_to_open gate!")

# Case B: Current diff is -10 (e.g. Orbex Bid = 1.09990, Swissquote Ask = 1.10000)
bid2_b = 1.09990
ask1_b = 1.10000
diff_b = round((bid2_b - ask1_b) * 100000, 1) # -10.0

print(f"Case B: diff = {diff_b}, threshold = {diff_to_open}")
print(f"  diff >= threshold: {diff_b >= diff_to_open} -> PASSED diff_to_open gate!")
