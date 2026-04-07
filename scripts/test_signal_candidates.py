"""Manual runner for signal candidate generation."""

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
from src.engine.signal_candidates import SignalCandidateFilters, build_signal_candidates
from src.engine.weather_aggregate import aggregate_forecasts
from src.engine.weather_probability import build_temperature_bin_probabilities
from scripts.test_clob_evaluator import _city_slug_prefix, resolve_event_markets_for_city_day


CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
SUPPORTED_HORIZONS = ("today", "tomorrow")


def load_config(path: Path) -> dict[str, Any]:
    """Load the JSON-compatible config file."""

    return json.loads(path.read_text(encoding="utf-8"))


def parse_args(default_city: str) -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description="Run manual signal candidate test")
    parser.add_argument("--city", default=default_city, help="City key")
    parser.add_argument("--horizon", default="today", help="today or tomorrow")
    parser.add_argument("--target-date", default=None, help="Optional UTC target date YYYY-MM-DD")
    parser.add_argument("--gamma-limit", type=int, default=3000, help="Max records scanned in discovery fallback")
    parser.add_argument("--min-abs-edge", type=float, default=0.01, help="Minimum absolute probability edge")
    return parser.parse_args()


def build_city_config(city_key: str, city_block: Mapping[str, Any]) -> CityConfig:
    """Build CityConfig from one city block in config."""

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


def main() -> int:
    """Run manual signal candidate generation."""

    try:
        config = load_config(CONFIG_PATH)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"Failed to load config: {exc}", file=sys.stderr)
        return 1

    cities = config.get("cities", {})
    if not isinstance(cities, Mapping) or not cities:
        print("Config has no cities.", file=sys.stderr)
        return 1

    args = parse_args(default_city=list(cities.keys())[0])
    city_key = str(args.city).strip().lower()
    horizon = str(args.horizon).strip().lower()

    target_date: dt.date | None = None
    if args.target_date is not None and str(args.target_date).strip():
        try:
            target_date = dt.date.fromisoformat(str(args.target_date).strip())
        except ValueError:
            print("Invalid --target-date. Use YYYY-MM-DD.", file=sys.stderr)
            return 1

    if city_key not in cities:
        print(f"Unknown city '{city_key}'. Available: {', '.join(cities.keys())}", file=sys.stderr)
        return 1
    if horizon not in SUPPORTED_HORIZONS:
        print(f"Unsupported horizon '{horizon}'. Use today or tomorrow.", file=sys.stderr)
        return 1

    bundle = build_weather_collector(config)
    if not bundle.provider_configs:
        print("No enabled weather providers found in config.")
        return 0

    city = build_city_config(city_key, cities[city_key])
    snapshots = [
        bundle.collector.collect(provider_name=name, city=city, target_day=horizon, provider_config=provider_cfg)
        for name, provider_cfg in bundle.provider_configs.items()
    ]
    snapshots = [s for s in snapshots if not (math.isnan(s.predicted_tmax_f) or "error" in s.raw_payload)]

    if not snapshots:
        print("No successful provider snapshots.", file=sys.stderr)
        return 1

    aggregate = aggregate_forecasts(snapshots)
    distribution = build_temperature_bin_probabilities(aggregate)

    effective_target_date = target_date
    slug_prefix = _city_slug_prefix(city_key, cities[city_key])
    metadata = resolve_event_markets_for_city_day(
        city_key=city_key,
        horizon=horizon,
        gamma_limit=int(args.gamma_limit),
        target_date=effective_target_date,
        city_slug_prefix=slug_prefix,
    )
    if not metadata:
        print("No event-scoped market bins resolved.", file=sys.stderr)
        return 1

    market_bins = [
        {"label": row["range_label"], "probability": float(row["market_yes_probability"])}
        for row in metadata
    ]
    comparison = compare_market_probabilities(distribution, market_bins)

    filters = SignalCandidateFilters(minimum_absolute_edge=float(args.min_abs_edge))
    result = build_signal_candidates(comparison, filters)

    print(f"city={result.city_key} target_day={result.target_day} target_date={target_date}")
    print(f"rows_in={result.diagnostics.input_rows_count} rows_kept={result.diagnostics.kept_rows_count}")
    print(f"best_yes={result.best_yes_candidate.range_label if result.best_yes_candidate else None}")
    print(f"best_no={result.best_no_candidate.range_label if result.best_no_candidate else None}")
    print("\nRanked Candidates")
    print("=" * 48)

    for row in result.all_ranked_candidates:
        print(
            f"{row.range_label:14} {row.raw_signal_direction:7} "
            f"edge={row.probability_edge:+.4f} abs_edge={row.abs_edge:.4f} "
            f"model={row.model_probability:.4f} market={row.market_probability:.4f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
