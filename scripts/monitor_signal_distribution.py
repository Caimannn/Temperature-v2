#!/usr/bin/env python3
"""
Lightweight signal state distribution monitor.
Reads CSV audit log and periodically reports state frequency analysis (NO_SIGNAL, WATCH, PAPER, TRADE_CANDIDATE).
Runs as a background daemon and logs pattern observations.
"""

import asyncio
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter

AUDIT_CSV_PATH = Path(__file__).resolve().parents[1] / "output" / "audit" / "live_signal_runs.csv"
MONITOR_LOG_PATH = Path(__file__).resolve().parents[1] / "output" / "audit" / "signal_distribution_monitor.log"


def read_signal_states_since(cutoff_time: datetime) -> list[str]:
    """Read all signal states from CSV rows since cutoff_time."""
    states = []
    if not AUDIT_CSV_PATH.exists():
        return states
    
    try:
        with open(AUDIT_CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ts_str = row.get("timestamp", "")
                    if not ts_str:
                        continue
                    # Parse ISO timestamp like "2026-04-07T09:22:35+00:00"
                    ts = datetime.fromisoformat(ts_str)
                    if ts >= cutoff_time:
                        state = row.get("state", "UNKNOWN")
                        states.append(state)
                except (ValueError, KeyError):
                    pass
    except Exception as e:
        print(f"[monitor] read error: {e}")
    
    return states


def analyze_distribution(states: list[str]) -> dict:
    """Count state frequencies and return analysis."""
    if not states:
        return {}
    
    counter = Counter(states)
    total = len(states)
    return {
        name: {
            "count": count,
            "percentage": round(100 * count / total, 1)
        }
        for name, count in counter.most_common()
    }


def format_analysis(analysis: dict, total_rows: int) -> str:
    """Format analysis as readable log line."""
    if not analysis:
        return "no_states_recorded"
    
    lines = [f"total_rows={total_rows}"]
    for state, stats in analysis.items():
        lines.append(f"{state}={stats['count']}({stats['percentage']}%)")
    return " | ".join(lines)


def log_observation(observation: str) -> None:
    """Append observation to monitor log."""
    MONITOR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=timezone.utc).isoformat()
    with open(MONITOR_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {observation}\n")
    print(f"[monitor] {observation}")


async def monitor_loop(interval_hours: int = 6, max_days: int = 3) -> None:
    """Run monitoring loop for max_days, reporting every interval_hours."""
    end_time = datetime.now(tz=timezone.utc) + timedelta(days=max_days)
    
    log_observation(f"monitoring_started | interval={interval_hours}h | duration={max_days}d")
    
    while datetime.now(tz=timezone.utc) < end_time:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=interval_hours)
        states = read_signal_states_since(cutoff)
        analysis = analyze_distribution(states)
        
        # Count total CSV rows for context
        try:
            total_rows = sum(1 for _ in open(AUDIT_CSV_PATH)) - 1  # exclude header
        except Exception:
            total_rows = 0
        
        report = format_analysis(analysis, total_rows)
        log_observation(f"{report}")
        
        # Simple heuristic: flag if any state is >70% or <5% (unusual distribution)
        if analysis:
            for state, stats in analysis.items():
                if stats["percentage"] > 70:
                    log_observation(f"PATTERN_ALERT | {state} appears {stats['percentage']}% (dominant)")
                elif stats["count"] > 0 and stats["percentage"] < 5:
                    log_observation(f"PATTERN_ALERT | {state} appears {stats['percentage']}% (rare)")
        
        await asyncio.sleep(interval_hours * 3600)
    
    log_observation("monitoring_ended | max_days reached")


if __name__ == "__main__":
    # Run for 3 days, report every 6 hours
    asyncio.run(monitor_loop(interval_hours=6, max_days=3))
