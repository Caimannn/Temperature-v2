"""Manual smoke test for weather provider collection."""

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
from src.common.models import CityConfig, ForecastSnapshot


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

    parser = argparse.ArgumentParser(description="Run manual weather provider smoke test")
    parser.add_argument("--city", default=available_cities[0], help="City key to test")
    parser.add_argument("--horizon", default="today", help="Target horizon: today or tomorrow")
    return parser.parse_args()


def snapshot_failed(snapshot: ForecastSnapshot) -> bool:
    """Detect whether a snapshot represents a provider failure."""

    return math.isnan(snapshot.predicted_tmax_f) or "error" in snapshot.raw_payload


def print_snapshot(snapshot: ForecastSnapshot) -> None:
    """Print a readable snapshot line block."""

    status = "FAIL" if snapshot_failed(snapshot) else "OK"
    error_text = str(snapshot.raw_payload.get("error", "")) if snapshot_failed(snapshot) else ""

    print(f"Provider: {snapshot.provider_name}")
    print(f"Status:   {status}")
    print(f"City:     {snapshot.city.key}")
    print(f"Day:      {snapshot.target_day}")
    print(f"Observed: {snapshot.observed_at}")
    print(f"Tmax(F):  {snapshot.predicted_tmax_f}")
    if error_text:
        print(f"Error:    {error_text}")
    print("-" * 40)


def main() -> int:
    """Run a simple manual smoke test for enabled weather providers."""

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

    city = build_city_config(city_key, cities_block[city_key])
    bundle = build_weather_collector(config)

    if not bundle.provider_configs:
        print("No enabled weather providers found in config.")
        print("Enable providers under weather_provider.providers.<name>.enabled.")
        return 0

    print(f"Smoke test city={city.key} horizon={horizon}")
    print("=" * 40)

    for provider_name, provider_cfg in bundle.provider_configs.items():
        snapshot = bundle.collector.collect(
            provider_name=provider_name,
            city=city,
            target_day=horizon,
            provider_config=provider_cfg,
        )
        print_snapshot(snapshot)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
