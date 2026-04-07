"""Provider-agnostic base contracts for weather collection."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Mapping

from src.common.models import CityConfig, ForecastSnapshot


class WeatherProvider(ABC):
    """Base interface that concrete weather providers must implement."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return provider name used in normalized snapshots."""

    @abstractmethod
    def fetch_forecast(
        self,
        city: CityConfig,
        target_day: str,
        provider_config: Mapping[str, Any],
    ) -> ForecastSnapshot:
        """Fetch one city-day forecast and return a normalized snapshot."""


class WeatherCollector:
    """Thin router that delegates forecast collection to named providers."""

    def __init__(self, providers: Mapping[str, WeatherProvider]) -> None:
        self._providers = dict(providers)

    def collect(
        self,
        provider_name: str,
        city: CityConfig,
        target_day: str | date,
        provider_config: Mapping[str, Any],
    ) -> ForecastSnapshot:
        """Collect a normalized forecast snapshot for one city and day."""

        provider = self._providers.get(provider_name)
        if provider is None:
            raise ValueError(f"Unknown weather provider: {provider_name}")

        normalized_target_day = target_day.isoformat() if isinstance(target_day, date) else str(target_day)
        return provider.fetch_forecast(city=city, target_day=normalized_target_day, provider_config=provider_config)
