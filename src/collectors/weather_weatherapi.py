"""WeatherAPI provider implementation for normalized Tmax forecasts."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from src.collectors.weather_base import WeatherProvider
from src.collectors.weather_helpers import failed_snapshot, location_query, normalize_target_day, now_iso
from src.common.models import CityConfig, ForecastSnapshot


class WeatherApiProvider(WeatherProvider):
    """Fetch weather forecasts from WeatherAPI and normalize output."""

    @property
    def name(self) -> str:
        """Return provider identifier."""

        return "weatherapi"

    def fetch_forecast(
        self,
        city: CityConfig,
        target_day: str,
        provider_config: Mapping[str, Any],
    ) -> ForecastSnapshot:
        """Return one normalized city-day Tmax snapshot."""

        normalized_day = normalize_target_day(target_day)
        if normalized_day is None:
            return failed_snapshot(city, target_day, self.name, "unsupported target day")

        api_key = str(provider_config.get("api_key", "")).strip()
        if not api_key:
            return failed_snapshot(city, normalized_day, self.name, "missing api key")

        base_url = str(provider_config.get("base_url") or "https://api.weatherapi.com/v1/forecast.json")
        location_query_str = location_query(city)
        params = urlencode({"key": api_key, "q": location_query_str, "days": 2, "aqi": "no", "alerts": "no"})
        url = f"{base_url}?{params}"

        try:
            with urlopen(url, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
            predicted_tmax = _extract_weatherapi_tmax(payload, normalized_day)
            if predicted_tmax is None:
                return failed_snapshot(city, normalized_day, self.name, "unable to parse tmax", payload)
            return ForecastSnapshot(
                city=city,
                target_day=normalized_day,
                provider_name=self.name,
                observed_at=now_iso(),
                predicted_tmax_f=predicted_tmax,
                raw_payload=payload,
            )
        except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            return failed_snapshot(city, normalized_day, self.name, f"provider failure: {exc}")


def _extract_weatherapi_tmax(payload: Mapping[str, Any], target_day: str) -> float | None:
    """Extract daily max temperature from WeatherAPI payload."""

    forecast = payload.get("forecast")
    if not isinstance(forecast, Mapping):
        return None
    forecast_days = forecast.get("forecastday")
    if not isinstance(forecast_days, list):
        return None

    for day_item in forecast_days:
        if not isinstance(day_item, Mapping):
            continue
        if str(day_item.get("date", "")) != target_day:
            continue
        day_block = day_item.get("day")
        if not isinstance(day_block, Mapping):
            return None
        value = day_block.get("maxtemp_f")
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return None
