from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
except ImportError:
    import pytz
    NY_TZ = pytz.timezone("America/New_York")

# Position opened: epoch from log = 1786636817.0
# Opened 2026-08-13 19:00:17 broker time (EET = UTC+3)
open_epoch = 1786636817.0

# Time of log entry: 2026-08-17 09:14:18 EST
now_epoch_log = datetime(2026, 8, 17, 9, 14, 18, tzinfo=NY_TZ).timestamp()

sched = [1.0, 1.0, 3.0, 1.0, 1.0, 0.0, 0.0]  # Standard 3x Wednesday (Mon..Sun)
DAY_NAMES = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

open_dt = datetime.fromtimestamp(open_epoch, tz=NY_TZ)
now_dt  = datetime.fromtimestamp(now_epoch_log, tz=NY_TZ)

print(f"Open epoch : {open_epoch}")
print(f"Open  (NY) : {open_dt}  ({DAY_NAMES[open_dt.weekday()]})")
print(f"Now   (NY) : {now_dt}  ({DAY_NAMES[now_dt.weekday()]})")

first_rollover = open_dt.replace(hour=17, minute=0, second=0, microsecond=0)
if open_dt >= first_rollover:
    first_rollover += timedelta(days=1)

print(f"First rollover after open: {first_rollover}  ({DAY_NAMES[first_rollover.weekday()]})")
print()

cur = first_rollover
total = 0.0
print("Rollover crossings counted:")
while cur <= now_dt:
    w = cur.weekday()
    wt = sched[w]
    print(f"  {cur.strftime('%Y-%m-%d %H:%M %Z')} ({DAY_NAMES[w]}) weight={wt}  running={total+wt}")
    total += wt
    cur += timedelta(days=1)

print()
print(f"Total rollover days = {total}")
print()
print("=== DIAGNOSIS ===")
print(f"Positions opened on: {open_dt.strftime('%A %Y-%m-%d %H:%M %Z')}")
print(f"Schedule: Mon=1 Tue=1 Wed=3 Thu=1 Fri=1 Sat=0 Sun=0")
print(f"Config:   remind_days=4 / max_days=5")
print()

# Key question: What weekday is the first rollover AFTER the open?
print(f"First rollover falls on a {DAY_NAMES[first_rollover.weekday()]}")
print("If the first rollover is Wednesday, it counts 3 — so 3 days immediately after Wed 5PM.")
print("But if opened AFTER Wed 5PM, the first rollover is THURSDAY which only counts 1.")
print()
print("=== Broker timestamp analysis ===")
print(f"Broker raw time in log: 2026-08-13 19:00:17")
print(f"Assuming EET (UTC+3): that's 2026-08-13 16:00:17 UTC")
utc_dt = datetime(2026, 8, 13, 16, 0, 17)
from datetime import timezone
utc_aware = utc_dt.replace(tzinfo=timezone.utc)
ny_aware = utc_aware.astimezone(NY_TZ)
print(f"In New York time: {ny_aware}  ({DAY_NAMES[ny_aware.weekday()]})")
print()
print(f"5PM rollover on that day (NY): {ny_aware.replace(hour=17,minute=0,second=0)}")
print(f"Was position opened BEFORE 5PM NY? {ny_aware.hour < 17}")
