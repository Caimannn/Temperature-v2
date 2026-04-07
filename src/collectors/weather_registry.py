"""Registry helpers for building weather collectors from config."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os
from typing import Any, Callable, Mapping

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in some environments
    load_dotenv = None

from src.collectors.weather_base import WeatherCollector, WeatherProvider
from src.collectors.weather_openweather import OpenWeatherProvider
from src.collectors.weather_tomorrow import TomorrowProvider
from src.collectors.weather_weatherapi import WeatherApiProvider
from src.common.models import CityConfig, ForecastSnapshot


ProviderFactory = Callable[[], WeatherProvider]
_ENV_LOADED = False


@dataclass(frozen=True)
class WeatherCollectorBundle:
    """Bundle of collector plus provider configs selected from settings."""

    collector: WeatherCollector
    provider_configs: dict[str, Mapping[str, Any]]


class _SafeProvider(WeatherProvider):
    """Guard wrapper that prevents provider exceptions from bubbling up."""

    def __init__(self, provider: WeatherProvider) -> None:
        self._provider = provider

    @property
    def name(self) -> str:
        """Return wrapped provider name."""

        return self._provider.name

    def fetch_forecast(
        self,
        city: CityConfig,
        target_day: str,
        provider_config: Mapping[str, Any],
    ) -> ForecastSnapshot:
        """Fetch safely and convert unexpected errors into a failure snapshot."""

        try:
            return self._provider.fetch_forecast(city=city, target_day=target_day, provider_config=provider_config)
        except Exception as exc:  # noqa: BLE001
            return ForecastSnapshot(
                city=city,
                target_day=target_day,
                provider_name=self._provider.name,
                observed_at=datetime.now(UTC).isoformat(),
                predicted_tmax_f=float("nan"),
                raw_payload={"error": f"unexpected provider failure: {exc}"},
            )


def build_weather_collector(config: Mapping[str, Any]) -> WeatherCollectorBundle:
    """Create a WeatherCollector using only providers enabled in config."""

    _load_env()
    provider_settings = _extract_provider_settings(config)
    factories: dict[str, ProviderFactory] = {
        "openweather": OpenWeatherProvider,
        "weatherapi": WeatherApiProvider,
        "tomorrow": TomorrowProvider,
    }

    enabled_providers: dict[str, WeatherProvider] = {}
    enabled_configs: dict[str, Mapping[str, Any]] = {}

    for name, factory in factories.items():
        raw_cfg = provider_settings.get(name, {})
        cfg = raw_cfg if isinstance(raw_cfg, Mapping) else {}
        if not _is_enabled(cfg):
            continue
        resolved_cfg = _resolve_provider_config(cfg)
        enabled_providers[name] = _SafeProvider(factory())
        enabled_configs[name] = resolved_cfg

    return WeatherCollectorBundle(
        collector=WeatherCollector(enabled_providers),
        provider_configs=enabled_configs,
    )


def _extract_provider_settings(config: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Extract provider config blocks from multiple supported config shapes."""

    section = config.get("weather_provider")
    if not isinstance(section, Mapping):
        return {}

    nested = section.get("providers")
    if isinstance(nested, Mapping):
        return {str(k): v for k, v in nested.items() if isinstance(v, Mapping)}

    return {
        name: cfg
        for name, cfg in section.items()
        if name in {"openweather", "weatherapi", "tomorrow"} and isinstance(cfg, Mapping)
    }


def _is_enabled(provider_cfg: Mapping[str, Any]) -> bool:
    """Check whether a provider block is marked as enabled."""

    return bool(provider_cfg.get("enabled", False))


def _resolve_provider_config(provider_cfg: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve provider config with backward-compatible api key handling."""

    resolved = dict(provider_cfg)

    explicit_api_key = str(resolved.get("api_key", "")).strip()
    if explicit_api_key:
        resolved["api_key"] = explicit_api_key
        return resolved

    api_key_env = str(resolved.get("api_key_env", "")).strip()
    if not api_key_env:
        return resolved

    env_value = os.getenv(api_key_env)
    if env_value is not None:
        env_api_key = env_value.strip()
        if env_api_key:
            resolved["api_key"] = env_api_key

    return resolved


def _load_env() -> None:
    """Load .env once when python-dotenv is available."""

    global _ENV_LOADED
    if _ENV_LOADED:
        return

    if load_dotenv is not None:
        load_dotenv()

    _ENV_LOADED = True
