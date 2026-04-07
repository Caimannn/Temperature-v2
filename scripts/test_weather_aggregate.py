"""Manual runner for baseline weather aggregation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.collectors.weather_registry import build_weather_collector
from src.common.models import CityConfig, ForecastSnapshot
from src.engine.weather_aggregate import WeatherAggregate, aggregate_forecasts


CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
SUPPORTED_HORIZONS = ("today", "tomorrow")


def load_config(path: Path) -> dict[str, Any]:
    """Load the JSON-compatible config file."""

    return json.loads(path.read_text(encoding="utf-8"))


def build_city_config(city_key: str, city_block: Mapping[str, Any]) -> CityConfig:
    """Build CityConfig from one config city block."""

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


def parse_args(available_cities: list[str]) -> argparse.Namespace:
    """Parse CLI args for city and horizon."""

    parser = argparse.ArgumentParser(description="Run manual weather aggregation smoke test")
    parser.add_argument("--city", default=available_cities[0], help="City key to test")
    parser.add_argument("--horizon", default="today", help="Target horizon: today or tomorrow")
    return parser.parse_args()


def collect_snapshots(
    collector_bundle: Any,
    city: CityConfig,
    horizon: str,
) -> list[ForecastSnapshot]:
    """Collect snapshots for one city/horizon from all enabled providers."""

    snapshots: list[ForecastSnapshot] = []

    for provider_name, provider_cfg in collector_bundle.provider_configs.items():
        snapshot = collector_bundle.collector.collect(
            provider_name=provider_name,
            city=city,
            target_day=horizon,
            provider_config=provider_cfg,
        )
        snapshots.append(snapshot)

    return snapshots


def print_summary(aggregate: WeatherAggregate) -> None:
    """Print aggregate summary fields in a readable format."""

    print("Aggregate Summary")
    print("=" * 48)
    print(f"city: {aggregate.city_key}")
    print(f"target_day: {aggregate.target_day}")
    print(f"provider_ok_count / provider_total_count: {aggregate.provider_ok_count} / {aggregate.provider_total_count}")
    print(f"central_predicted_tmax_f: {aggregate.central_predicted_tmax_f}")
    print(f"median_tmax_f: {aggregate.median_tmax_f}")
    print(f"min_tmax_f / max_tmax_f: {aggregate.min_tmax_f} / {aggregate.max_tmax_f}")
    print(f"spread_tmax_f: {aggregate.spread_tmax_f}")
    print(f"disagreement_summary: {aggregate.disagreement_summary}")


def print_provider_details(aggregate: WeatherAggregate) -> None:
    """Print per-provider diagnostics rows."""

    print("\nProvider Diagnostics")
    print("=" * 48)
    for detail in aggregate.provider_details:
        error_text = detail.error or ""
        print(
            f"provider={detail.provider_name} "
            f"status={detail.status} "
            f"tmax_f={detail.predicted_tmax_f} "
            f"observed_at={detail.observed_at} "
            f"error={error_text}"
        )


def main() -> int:
    """Run manual collection + aggregation for one city and horizon."""

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

    horizon = str(args.horizon).strip().lower()
    if horizon not in SUPPORTED_HORIZONS:
        print(f"Unsupported horizon '{horizon}'. Use today or tomorrow.", file=sys.stderr)
        return 1

    bundle = build_weather_collector(config)
    if not bundle.provider_configs:
        print("No enabled weather providers found in config.")
        print("Enable providers under weather_provider.providers.<name>.enabled.")
        return 0

    city = build_city_config(city_key, cities_block[city_key])
    snapshots = collect_snapshots(bundle, city, horizon)

    aggregate = aggregate_forecasts(snapshots)
    print_summary(aggregate)
    print_provider_details(aggregate)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
