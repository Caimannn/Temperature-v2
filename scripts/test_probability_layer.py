#!/usr/bin/env python3
"""Manual test runner for the probability layer."""

from __future__ import annotations

import argparse
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
from src.engine.weather_aggregate import aggregate_forecasts
from src.engine.weather_probability import build_temperature_bin_probabilities


CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
SUPPORTED_HORIZONS = ("today", "tomorrow")


def load_config(path: Path) -> dict[str, Any]:
    """Load the JSON-compatible config file."""
    return json.loads(path.read_text(encoding="utf-8"))


def build_city_config(city_key: str, city_block: Mapping[str, Any]) -> CityConfig:
    """Build CityConfig from a config city block."""
    resolver = city_block.get("resolver", {}) if isinstance(city_block, Mapping) else {}
    if not isinstance(resolver, Mapping):
        resolver = {}
    slug = resolver.get("slug")
    resolution_slug = resolver.get("resolution")
    return CityConfig(
        key=city_key,
        label=str(city_block.get("label", city_key.upper())) if isinstance(city_block, Mapping) else city_key.upper(),
        slug=str(slug) if slug is not None else None,
        resolution_slug=str(resolution_slug) if resolution_slug is not None else None,
        resolver=dict(resolver),
    )


def parse_args(available_cities: list[str]) -> argparse.Namespace:
    """Parse command-line arguments for city and horizon selection."""
    parser = argparse.ArgumentParser(description="Run probability layer test")
    parser.add_argument("--city", default=available_cities[0], help="City key to test")
    parser.add_argument("--horizon", default="today", help="Target horizon: today or tomorrow")
    return parser.parse_args()


def snapshot_failed(snapshot: Any) -> bool:
    """Detect whether a snapshot represents a provider failure."""
    return math.isnan(snapshot.predicted_tmax_f) or "error" in snapshot.raw_payload


def main() -> int:
    """Run probability layer test."""
    try:
        config = load_config(CONFIG_PATH)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"Failed to load config: {exc}", file=sys.stderr)
        return 1

    cities_block = config.get("cities", {})
    if not isinstance(cities_block, Mapping) or not cities_block:
        print("Config has no cities.", file=sys.stderr)
        return 1

    available_cities = list(cities_block.keys())
    args = parse_args(available_cities)

    city_key = str(args.city).strip().lower()
    if city_key not in cities_block:
        print(f"Unknown city '{city_key}'. Available: {', '.join(available_cities)}", file=sys.stderr)
        return 1

    # Build collector
    bundle = build_weather_collector(config)
    city_block = cities_block[city_key]
    city_config = build_city_config(city_key, city_block)

    # Collect snapshots using the same pattern as test_weather_aggregate.py
    snapshots = []
    for provider_name, provider_cfg in bundle.provider_configs.items():
        snapshot = bundle.collector.collect(
            provider_name=provider_name,
            city=city_config,
            target_day=args.horizon,
            provider_config=provider_cfg,
        )
        snapshots.append(snapshot)

    # Filter out failed snapshots
    good_snapshots = [s for s in snapshots if not snapshot_failed(s)]

    if not good_snapshots:
        print(f"All providers failed for {city_key}/{args.horizon}")
        return 1

    # Aggregate
    aggregate = aggregate_forecasts(good_snapshots)

    # Build probability distribution
    distribution = build_temperature_bin_probabilities(aggregate)

    # Print results
    print(f"Probability Layer Test")
    print(f"city={distribution.city_key} target_day={distribution.target_day}")
    print("=" * 60)
    print(f"center used:         {distribution.center_used_f:.2f}F")
    print(f"spread proxy used:   {distribution.spread_proxy_f:.2f}F")
    print(f"sum of probabilities: {sum(b.probability for b in distribution.bins):.4f}")
    print()
    print("Top 5 bins (by probability):")
    print("-" * 60)
    ranked = sorted(distribution.bins, key=lambda b: b.probability, reverse=True)
    for i, bin_prob in enumerate(ranked[:5], 1):
        print(f"{i}. {bin_prob.label:15s} {bin_prob.probability:.4f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
