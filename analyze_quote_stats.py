#!/usr/bin/env python3
"""
analyze_quote_stats.py — Market Quote Stats Analysis

Reads CSV data collected by trade_dashboard.py from the stats/ directory
and produces insights on optimal trading windows for hedging.

Requires: pandas (pip install pandas)
  You'll need at least a day or two of data collection before the
  analysis is meaningful.

Reports:
  1. Average spread by hour — shows which hour has the lowest spread
     per account/pair
  2. Tick rate by hour — shows the calmest (least volatile) hours
  3. Day-of-week patterns — spread and volatility by weekday
  4. Optimal windows — ranked list of best day+hour combos, scored
     70% on spread + 30% on tick rate

Usage:
  python analyze_quote_stats.py                  # analyze all data
  python analyze_quote_stats.py --days 3         # last 3 days only
  python analyze_quote_stats.py --account 12345  # specific account
  python analyze_quote_stats.py --pair USDCHF    # specific pair
  python analyze_quote_stats.py --export         # save HTML report to stats/reports/
"""

import os
import sys
import glob
import argparse
from datetime import datetime, timedelta

try:
    import pandas as pd
except ImportError:
    print("pandas is required. Install with: pip install pandas")
    sys.exit(1)

STATS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stats")
EXPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stats", "reports")

DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def load_data(days=None, account=None, pair=None):
    """Load and filter CSV data from the stats directory."""
    csv_files = sorted(glob.glob(os.path.join(STATS_DIR, "market_*.csv")))
    if not csv_files:
        print(f"No data files found in {STATS_DIR}/")
        print("Enable stats logging on desired accounts and wait for data to accumulate.")
        sys.exit(0)

    # Filter by date range
    # Filenames can be market_YYYY-MM-DD.csv (old) or market_ACCT_YYYY-MM-DD.csv (new per-account)
    if days:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        def _extract_date(fname):
            base = os.path.basename(fname).replace("market_", "").replace(".csv", "")
            # Per-account format: "649159_2026-03-09" → take last 10 chars as date
            if len(base) > 10 and "_" in base:
                return base.split("_", 1)[-1]
            return base  # Old format: just the date
        csv_files = [f for f in csv_files if _extract_date(f) >= cutoff]

    if not csv_files:
        print(f"No data files in the last {days} day(s).")
        sys.exit(0)

    frames = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            frames.append(df)
        except Exception as e:
            print(f"  Warning: skipping {os.path.basename(f)}: {e}")

    if not frames:
        print("No valid data loaded.")
        sys.exit(0)

    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.hour
    df["dow"] = df["timestamp"].dt.dayofweek  # 0=Mon
    df["dow_name"] = df["dow"].map(lambda x: DOW_NAMES[x])
    df["account"] = df["account"].astype(str)

    # Filter
    if account:
        df = df[df["account"].str.contains(str(account))]
    if pair:
        df = df[df["pair"].str.upper().str.contains(pair.upper())]

    if df.empty:
        print("No data matches the filters.")
        sys.exit(0)

    return df


def print_header(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def analyze_spread_by_hour(df):
    """Show average spread by hour for each account/pair."""
    print_header("AVERAGE SPREAD BY HOUR")

    for (account, pair), group in df.groupby(["account", "pair"]):
        print(f"\n  Account: {account}  |  Pair: {pair}")
        print(f"  {'Hour':<6} {'Avg Spd':>8} {'Min':>6} {'Max':>6} {'Samples':>8}")
        print(f"  {'-' * 40}")

        hourly = group.groupby("hour")["spread"].agg(["mean", "min", "max", "count"])
        best_hour = hourly["mean"].idxmin()

        for hour in range(24):
            if hour in hourly.index:
                row = hourly.loc[hour]
                marker = " ◀ BEST" if hour == best_hour else ""
                print(f"  {hour:02d}:00  {row['mean']:8.1f} {row['min']:6.0f} {row['max']:6.0f} {int(row['count']):8d}{marker}")

        print(f"\n  ★ Best hour: {best_hour:02d}:00 (avg spread: {hourly.loc[best_hour, 'mean']:.1f})")


def analyze_ticks_by_hour(df):
    """Show average tick rate by hour for each account/pair."""
    print_header("AVERAGE TICK RATE BY HOUR (ticks/5s)")

    for (account, pair), group in df.groupby(["account", "pair"]):
        print(f"\n  Account: {account}  |  Pair: {pair}")
        print(f"  {'Hour':<6} {'Avg Ticks':>9} {'Min':>5} {'Max':>5} {'Samples':>8}")
        print(f"  {'-' * 40}")

        hourly = group.groupby("hour")["ticks_5s"].agg(["mean", "min", "max", "count"])
        calmest_hour = hourly["mean"].idxmin()

        for hour in range(24):
            if hour in hourly.index:
                row = hourly.loc[hour]
                marker = " ◀ CALMEST" if hour == calmest_hour else ""
                print(f"  {hour:02d}:00  {row['mean']:9.1f} {row['min']:5.0f} {row['max']:5.0f} {int(row['count']):8d}{marker}")

        print(f"\n  ★ Calmest hour: {calmest_hour:02d}:00 (avg ticks/5s: {hourly.loc[calmest_hour, 'mean']:.1f})")


def analyze_by_day_of_week(df):
    """Show spread and tick patterns by day of week."""
    print_header("PATTERNS BY DAY OF WEEK")

    for (account, pair), group in df.groupby(["account", "pair"]):
        print(f"\n  Account: {account}  |  Pair: {pair}")
        print(f"  {'Day':<5} {'Avg Spread':>10} {'Avg Ticks':>10} {'Samples':>8}")
        print(f"  {'-' * 38}")

        daily = group.groupby("dow").agg({
            "spread": "mean",
            "ticks_5s": "mean",
            "timestamp": "count"
        }).rename(columns={"timestamp": "samples"})

        best_day = daily["spread"].idxmin() if not daily.empty else 0

        for dow in range(7):
            if dow in daily.index:
                row = daily.loc[dow]
                marker = " ◀ BEST" if dow == best_day else ""
                print(f"  {DOW_NAMES[dow]:<5} {row['spread']:10.1f} {row['ticks_5s']:10.1f} {int(row['samples']):8d}{marker}")


def find_optimal_windows(df, top_n=5):
    """Find the best hour+day combinations for trading (lowest spread + lowest ticks)."""
    print_header(f"TOP {top_n} OPTIMAL TRADING WINDOWS")

    for (account, pair), group in df.groupby(["account", "pair"]):
        print(f"\n  Account: {account}  |  Pair: {pair}")

        # Group by day-of-week + hour
        windows = group.groupby(["dow", "hour"]).agg({
            "spread": "mean",
            "ticks_5s": "mean",
            "timestamp": "count"
        }).rename(columns={"timestamp": "samples"})

        # Only consider windows with enough samples (at least 30)
        windows = windows[windows["samples"] >= 30]

        if windows.empty:
            print("  Not enough data yet (need ≥30 samples per hour-slot).")
            continue

        # Normalize spread and ticks to 0-1 range, then combine as a score
        # Lower is better for both
        s_min, s_max = windows["spread"].min(), windows["spread"].max()
        t_min, t_max = windows["ticks_5s"].min(), windows["ticks_5s"].max()

        if s_max > s_min:
            windows["spread_norm"] = (windows["spread"] - s_min) / (s_max - s_min)
        else:
            windows["spread_norm"] = 0

        if t_max > t_min:
            windows["ticks_norm"] = (windows["ticks_5s"] - t_min) / (t_max - t_min)
        else:
            windows["ticks_norm"] = 0

        # Score = weighted combination (spread matters more for hedging)
        windows["score"] = windows["spread_norm"] * 0.7 + windows["ticks_norm"] * 0.3
        best = windows.nsmallest(top_n, "score")

        print(f"  {'Rank':<5} {'Day':<5} {'Hour':<6} {'Avg Spd':>8} {'Avg Ticks':>10} {'Score':>7} {'Samples':>8}")
        print(f"  {'-' * 55}")

        for rank, ((dow, hour), row) in enumerate(best.iterrows(), 1):
            print(f"  {rank:<5} {DOW_NAMES[dow]:<5} {hour:02d}:00  {row['spread']:8.1f} {row['ticks_5s']:10.1f} {row['score']:7.3f} {int(row['samples']):8d}")


def summary_stats(df):
    """Print a quick data summary."""
    print_header("DATA SUMMARY")

    date_range = f"{df['timestamp'].min().strftime('%Y-%m-%d %H:%M')} → {df['timestamp'].max().strftime('%Y-%m-%d %H:%M')}"
    accounts = df["account"].nunique()
    pairs = df["pair"].nunique()
    total_rows = len(df)

    print(f"\n  Date range:  {date_range}")
    print(f"  Accounts:    {accounts}")
    print(f"  Pairs:       {pairs}")
    print(f"  Total rows:  {total_rows:,}")

    print(f"\n  {'Account':<15} {'Pair':<10} {'Rows':>8} {'Avg Spread':>10} {'Avg Ticks':>10}")
    print(f"  {'-' * 58}")
    for (acc, pair), group in df.groupby(["account", "pair"]):
        print(f"  {acc:<15} {pair:<10} {len(group):>8,} {group['spread'].mean():>10.1f} {group['ticks_5s'].mean():>10.1f}")


def export_html(df, top_n=10):
    """Export analysis results to an HTML file."""
    os.makedirs(EXPORT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filepath = os.path.join(EXPORT_DIR, f"analysis_{timestamp}.html")

    html_parts = ["""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Market Stats Analysis</title>
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #1a1a2e; color: #e0e0e0; padding: 20px; }
  h1, h2 { color: #a78bfa; }
  table { border-collapse: collapse; margin: 10px 0 20px 0; }
  th, td { padding: 6px 12px; border: 1px solid #333; text-align: right; font-size: 0.9rem; }
  th { background: #2a2a4a; color: #a78bfa; }
  tr:nth-child(even) { background: #1e1e36; }
  .best { background: #1a3a2a !important; color: #4ade80; font-weight: bold; }
  .summary { background: #2a2a4a; padding: 15px; border-radius: 8px; margin-bottom: 20px; }
</style></head><body>
<h1>📊 Market Stats Analysis</h1>
"""]

    # Summary
    html_parts.append('<div class="summary">')
    html_parts.append(f'<strong>Date range:</strong> {df["timestamp"].min()} → {df["timestamp"].max()}<br>')
    html_parts.append(f'<strong>Total rows:</strong> {len(df):,}<br>')
    html_parts.append(f'<strong>Generated:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    html_parts.append('</div>')

    for (account, pair), group in df.groupby(["account", "pair"]):
        html_parts.append(f'<h2>Account: {account} — {pair}</h2>')

        # Hourly spread table
        html_parts.append('<h3>Hourly Spread</h3><table><tr><th>Hour</th><th>Avg Spread</th><th>Min</th><th>Max</th><th>Samples</th></tr>')
        hourly = group.groupby("hour")["spread"].agg(["mean", "min", "max", "count"])
        best_h = hourly["mean"].idxmin() if not hourly.empty else -1
        for hour in range(24):
            if hour in hourly.index:
                r = hourly.loc[hour]
                cls = ' class="best"' if hour == best_h else ''
                html_parts.append(f'<tr{cls}><td>{hour:02d}:00</td><td>{r["mean"]:.1f}</td><td>{r["min"]:.0f}</td><td>{r["max"]:.0f}</td><td>{int(r["count"])}</td></tr>')
        html_parts.append('</table>')

        # Optimal windows
        windows = group.groupby(["dow", "hour"]).agg({"spread": "mean", "ticks_5s": "mean", "timestamp": "count"}).rename(columns={"timestamp": "samples"})
        windows = windows[windows["samples"] >= 10]
        if not windows.empty:
            s_min, s_max = windows["spread"].min(), windows["spread"].max()
            t_min, t_max = windows["ticks_5s"].min(), windows["ticks_5s"].max()
            windows["score"] = 0
            if s_max > s_min:
                windows["score"] += ((windows["spread"] - s_min) / (s_max - s_min)) * 0.7
            if t_max > t_min:
                windows["score"] += ((windows["ticks_5s"] - t_min) / (t_max - t_min)) * 0.3
            best = windows.nsmallest(top_n, "score")
            html_parts.append(f'<h3>Top {top_n} Optimal Windows</h3><table><tr><th>#</th><th>Day</th><th>Hour</th><th>Avg Spread</th><th>Avg Ticks</th><th>Score</th><th>Samples</th></tr>')
            for rank, ((dow, hour), r) in enumerate(best.iterrows(), 1):
                html_parts.append(f'<tr><td>{rank}</td><td>{DOW_NAMES[dow]}</td><td>{hour:02d}:00</td><td>{r["spread"]:.1f}</td><td>{r["ticks_5s"]:.1f}</td><td>{r["score"]:.3f}</td><td>{int(r["samples"])}</td></tr>')
            html_parts.append('</table>')

    html_parts.append('</body></html>')

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))
    print(f"\n  ✓ Report exported to: {filepath}")


def main():
    parser = argparse.ArgumentParser(description="Analyze market stats CSV data")
    parser.add_argument("--days", type=int, help="Only analyze last N days")
    parser.add_argument("--account", type=str, help="Filter by account number")
    parser.add_argument("--pair", type=str, help="Filter by pair (e.g. USDCHF)")
    parser.add_argument("--export", action="store_true", help="Export results to HTML")
    parser.add_argument("--top", type=int, default=10, help="Number of optimal windows to show (default: 10)")
    args = parser.parse_args()

    df = load_data(days=args.days, account=args.account, pair=args.pair)

    summary_stats(df)
    analyze_spread_by_hour(df)
    analyze_ticks_by_hour(df)
    analyze_by_day_of_week(df)
    find_optimal_windows(df, top_n=args.top)

    if args.export:
        export_html(df, top_n=args.top)

    print(f"\n{'=' * 60}")
    print(f"  Tip: Run with --export to save an HTML report")
    print(f"       Run with --days 1 to see just today's data")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
