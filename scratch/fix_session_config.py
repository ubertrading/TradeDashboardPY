"""Fix trailing dots in pair names and clear spread_rejects for session b0739ae9."""
import json

paths = [
    r"d:\Documents\dev\temp\configs\trade_sessions.json",
]

for p in paths:
    with open(p, "r") as f:
        data = json.load(f)

    modified = False
    for sid, s in data.items():
        # Clear spread rejects for session b0739ae9
        if sid.startswith("b0739ae9"):
            s["spread_rejects"] = {acc: 0 for acc in s.get("sides", {})}
            s["errors"] = {acc: [] for acc in s.get("sides", {})}
            modified = True
            print(f"Cleared errors/spread_rejects for session {sid}")

        # Clean up pair names ending with dots
        pair = s.get("pair", "")
        if pair.endswith("."):
            clean = pair.rstrip(".")
            s["pair"] = clean
            modified = True
            print(f"Normalized pair '{pair}' -> '{clean}' for session {sid}")

        for acc, side in s.get("sides", {}).items():
            spair = side.get("pair", "")
            if spair.endswith("."):
                clean = spair.rstrip(".")
                side["pair"] = clean
                modified = True
                print(f"Normalized side pair '{spair}' -> '{clean}' for {acc} in session {sid}")

    if modified:
        with open(p, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved: {p}")
    else:
        print(f"No changes needed: {p}")
