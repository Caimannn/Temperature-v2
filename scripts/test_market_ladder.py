"""Manual runner to validate real event ladder parsing and compare output."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sys
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.collectors.weather_registry import build_weather_collector
from src.common.models import CityConfig
from src.engine.market_compare import compare_market_probabilities
from src.engine.market_ladder import parse_temperature_bin_label
from src.engine.weather_aggregate import aggregate_forecasts
from src.engine.weather_probability import build_temperature_bin_probabilities
from scripts.test_clob_evaluator import _city_slug_prefix, resolve_event_markets_for_city_day


CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


def load_config(path: Path) -> dict[str, Any]:
    """Load JSON-compatible config file."""

    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description="Validate market ladder on real event bins")
    parser.add_argument("--target-date", required=True, help="UTC target date YYYY-MM-DD")
    parser.add_argument("--gamma-limit", type=int, default=3000, help="Max records scanned in discovery fallback")
    return parser.parse_args()


def build_city_config(city_key: str, city_block: Mapping[str, Any]) -> CityConfig:
    """Build CityConfig from config city block."""

    resolver = city_block.get("resolver", {}) if isinstance(city_block, Mapping) else {}
    if not isinstance(resolver, Mapping):
        resolver = {}

    return CityConfig(
        key=city_key,
        label=str(city_block.get("label", city_key.upper())) if isinstance(city_block, Mapping) else city_key.upper(),
        slug=str(resolver.get("slug")) if resolver.get("slug") is not None else None,
        resolution_slug=str(resolver.get("resolution")) if resolver.get("resolution") is not None else None,
        resolver=dict(resolver),
    )


def run_city(city_key: str, config: Mapping[str, Any], target_date: dt.date, gamma_limit: int) -> None:
    """Run ladder parse + compare for one city."""

    cities = config.get("cities", {})
    city_block = cities.get(city_key, {}) if isinstance(cities, Mapping) else {}
    city = build_city_config(city_key, city_block)

    bundle = build_weather_collector(config)
    snapshots = [
        bundle.collector.collect(provider_name=name, city=city, target_day="today", provider_config=provider_cfg)
        for name, provider_cfg in bundle.provider_configs.items()
    ]
    snapshots = [s for s in snapshots if not (math.isnan(s.predicted_tmax_f) or "error" in s.raw_payload)]

    if not snapshots:
        print(f"\n[{city_key}] no provider snapshots")
        return

    aggregate = aggregate_forecasts(snapshots)
    distribution = build_temperature_bin_probabilities(aggregate)

    slug_prefix = _city_slug_prefix(city_key, city_block)
    metadata = resolve_event_markets_for_city_day(
        city_key=city_key,
        horizon="today",
        gamma_limit=gamma_limit,
        target_date=target_date,
        city_slug_prefix=slug_prefix,
    )

    print(f"\n=== {city_key.upper()} {target_date.isoformat()} ===")
    print(f"bins_found={len(metadata)}")
    if not metadata:
        print("no event bins resolved")
        return

    market_bins = [
        {
            "label": row["range_label"],
            "probability": float(row["market_yes_probability"]),
        }
        for row in metadata
    ]
    comparison = compare_market_probabilities(distribution, market_bins)

    print("parsed ladder")
    print("-" * 72)
    for row in metadata:
        parsed = parse_temperature_bin_label(row["range_label"])
        if parsed is None:
            print(f"label={row['range_label']} parse=FAIL")
            continue
        print(
            " | ".join(
                [
                    f"original={parsed.original_label}",
                    f"canonical={parsed.canonical_label}",
                    f"low_f={parsed.low_f}",
                    f"high_f={parsed.high_f}",
                    f"open_left={parsed.open_left}",
                    f"open_right={parsed.open_right}",
                ]
            )
        )

    print("\nmodel vs market")
    print("-" * 72)
    for row in comparison.rows:
        model_s = f"{row.model_probability:.6f}" if row.model_probability is not None else "None"
        market_s = f"{row.market_probability:.6f}" if row.market_probability is not None else "None"
        edge_s = f"{row.probability_edge:+.6f}" if row.probability_edge is not None else "None"
        print(
            f"{row.range_label:14} model={model_s:>10} market={market_s:>10} edge={edge_s:>10}"
        )


def main() -> int:
    """Entry point."""

    args = parse_args()
    try:
        target_date = dt.date.fromisoformat(str(args.target_date).strip())
    except ValueError:
        print("Invalid --target-date. Use YYYY-MM-DD.", file=sys.stderr)
        return 1

    try:
        config = load_config(CONFIG_PATH)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"Failed to load config: {exc}", file=sys.stderr)
        return 1

    run_city("nyc", config, target_date, int(args.gamma_limit))
    run_city("atlanta", config, target_date, int(args.gamma_limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
