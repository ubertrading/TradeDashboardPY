# ─── Configuration ──────────────────────────────────────────────────────────
...
SETTINGS_FILE = os.environ.get("TRADE_SETTINGS_FILE", "dashboard_settings.json")
USE_MT_BRIDGE = True  # True = C# bridge service (stable) | False = pythonnet (old, crash-prone)
Just flip it to False if you ever need to go back to the old method.
-----------------------------



That's it. The dashboard will auto-start MtBridgeService.exe on port 5090 when it initializes. You'll see logs like:

MtBridgeService already running
# or
Starting MtBridgeService from ...MtBridgeService.exe
Bridge loaded N accounts
To test the bridge independently (without the dashboard):

bash
# Terminal 1: start the bridge
dotnet run --project MtBridgeService
# Terminal 2: test it
curl http://localhost:5090/api/status
Performance Impact
Minimal in practice, with some trade-offs:

Aspect		Before (pythonnet)		After (HTTP bridge)			Impact
Quote latency	~0ms (in-process callback)	~1-3ms (HTTP round-trip)		Negligible — dashboard polls at 15s intervals anyway
Trade execution	~0ms overhead			~2-5ms HTTP overhead			Negligible — broker latency is 50-500ms
Position sync	Instant (shared memory)		15s poll interval			Same as before — dashboard already used 15-30s heartbeats
Memory		Single process, shared CLR	Two processes (~50-80MB for C# service)	Slight increase
Connect serialization	threading.Lock in Python	SemaphoreSlim in C# + HTTP wait	Faster — native C# threading, no GIL contention
Reconnect	pythonnet callbacks could corrupt CLR	Self-contained in C#		Much more stable
The real win is reliability, not speed. The whole point is that CLR crashes (0x80131506, AccessViolationException) now only kill the bridge process — the dashboard stays up and can restart it automatically. Before, these crashes took down the entire dashboard.

Rollback if needed — just change line 718 in 

trade_dashboard.py
 back to:

python
from mt_direct_connector import MTDirectManager