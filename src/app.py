"""Application entrypoint for the weather bot foundation."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
REQUIRED_TOP_LEVEL_KEYS = {
    "mode",
    "automation_enabled",
    "execution_enabled",
    "kill_switch",
    "horizons",
    "cities",
    "weather_provider",
    "discord",
    "polling",
    "signal_thresholds",
    "cheap_tail_thresholds",
    "positions_advice_thresholds",
}
EXPECTED_CITIES = ["nyc", "atlanta", "dallas", "chicago"]
EXPECTED_HORIZONS = ["today", "tomorrow"]


def load_config(path: Path) -> dict[str, Any]:
    """Load the JSON-compatible config file."""

    try:
        raw_config = path.read_text(encoding="utf-8")
        loaded = json.loads(raw_config)
    except FileNotFoundError as exc:
        raise ValueError(f"Config file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Config file is not valid JSON/YAML: {exc.msg}") from exc

    if not isinstance(loaded, dict):
        raise ValueError("Config root must be an object.")

    return loaded


def validate_config(config: dict[str, Any]) -> list[str]:
    """Validate the minimal config contract."""

    errors: list[str] = []

    missing_keys = sorted(REQUIRED_TOP_LEVEL_KEYS - set(config))
    if missing_keys:
        errors.append(f"Missing top-level keys: {', '.join(missing_keys)}")

    if config.get("mode") != "manual-only":
        errors.append("mode must be manual-only")

    if config.get("automation_enabled") is not False:
        errors.append("automation_enabled must be false")

    if config.get("execution_enabled") is not False:
        errors.append("execution_enabled must be false")

    if config.get("kill_switch") is not False:
        errors.append("kill_switch must be false")

    if config.get("horizons") != EXPECTED_HORIZONS:
        errors.append("horizons must be today and tomorrow")

    cities = config.get("cities")
    if not isinstance(cities, dict):
        errors.append("cities must be an object")
    else:
        missing_cities = [city for city in EXPECTED_CITIES if city not in cities]
        extra_cities = [city for city in cities if city not in EXPECTED_CITIES]
        if missing_cities:
            errors.append(f"Missing cities: {', '.join(missing_cities)}")
        if extra_cities:
            errors.append(f"Unexpected cities: {', '.join(extra_cities)}")
        for city, city_config in cities.items():
            resolver = city_config.get("resolver") if isinstance(city_config, dict) else None
            if not isinstance(resolver, dict) or resolver.get("type") != "placeholder":
                errors.append(f"{city} must include a placeholder resolver block")

    if not isinstance(config.get("weather_provider"), dict):
        errors.append("weather_provider must be an object")

    if not isinstance(config.get("discord"), dict):
        errors.append("discord must be an object")

    if not isinstance(config.get("polling"), dict):
        errors.append("polling must be an object")

    for section in ("signal_thresholds", "cheap_tail_thresholds", "positions_advice_thresholds"):
        if not isinstance(config.get(section), dict):
            errors.append(f"{section} must be an object")

    return errors


def main() -> int:
    """Load config, validate it, and print the runtime summary."""

    try:
        config = load_config(CONFIG_PATH)
        errors = validate_config(config)
    except ValueError as exc:
        print(f"Invalid config: {exc}", file=sys.stderr)
        return 1

    if errors:
        print("Invalid config:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"mode: {config['mode']}")
    print(f"execution_enabled: {config['execution_enabled']}")
    print(f"kill_switch: {config['kill_switch']}")
    print(f"enabled_cities: {', '.join(config['cities'].keys())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
