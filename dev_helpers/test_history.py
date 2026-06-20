import sys, time
from mt_direct_connector import MTDirectManager
manager = MTDirectManager({})
manager.start()
time.sleep(4)
for acc_id, acc in manager.accounts.items():
    if 'ORB' in acc_id:
        print(f"Testing {acc_id}")
        try:
            # 6/3 to 6/17
            from_ts = 1717372800 # 2026-06-03
            to_ts = 1718668799 # 2026-06-17
            hist = acc.get_deal_history(from_ts, to_ts)
            print(hist)
        except Exception as e:
            import traceback
            traceback.print_exc()
manager.stop()
