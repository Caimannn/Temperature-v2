"""OpenWeather provider implementation for normalized Tmax forecasts."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from src.collectors.weather_base import WeatherProvider
from src.collectors.weather_helpers import failed_snapshot, location_query, normalize_target_day, now_iso
from src.common.models import CityConfig, ForecastSnapshot


_LOGGER = logging.getLogger(__name__)
_DEFAULT_GEO_BASE_URL = "https://api.openweathermap.org/geo/1.0/direct"
_DEFAULT_FORECAST_BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"


class OpenWeatherProvider(WeatherProvider):
    """Fetch weather forecasts from OpenWeather and normalize output."""

    @property
    def name(self) -> str:
        """Return provider identifier."""

        return "openweather"

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

        geo_base_url = str(provider_config.get("geo_base_url") or _DEFAULT_GEO_BASE_URL)
        base_url = str(provider_config.get("forecast_base_url") or provider_config.get("base_url") or _DEFAULT_FORECAST_BASE_URL)
        location_query_str = location_query(city)

        try:
            geo_params = {"q": location_query_str, "limit": 1, "appid": api_key}
            _debug_endpoint(provider_config, geo_base_url, geo_params)
            geo_payload = _request_json(geo_base_url, geo_params)

            lat, lon = _extract_coordinates(geo_payload)
            if lat is None or lon is None:
                return failed_snapshot(city, normalized_day, self.name, "unable to resolve city coordinates", {"geo": geo_payload})

            forecast_params = {"lat": lat, "lon": lon, "appid": api_key, "units": "imperial"}
            _debug_endpoint(provider_config, base_url, forecast_params)
            payload = _request_json(base_url, forecast_params)

            predicted_tmax = _extract_openweather_tmax(payload, normalized_day)
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


def _request_json(base_url: str, params: Mapping[str, Any]) -> Any:
    """Fetch JSON from an endpoint with query parameters."""

    query = urlencode(params)
    url = f"{base_url}?{query}" if "?" not in base_url else f"{base_url}&{query}"
    with urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _extract_coordinates(geo_payload: Any) -> tuple[float | None, float | None]:
    """Extract latitude and longitude from OpenWeather geocoding payload."""

    if not isinstance(geo_payload, list) or not geo_payload:
        return None, None
    first = geo_payload[0]
    if not isinstance(first, Mapping):
        return None, None
    lat = _as_float(first.get("lat"))
    lon = _as_float(first.get("lon"))
    return lat, lon


def _as_float(value: Any) -> float | None:
    """Safely coerce a numeric value to float."""

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _debug_endpoint(provider_config: Mapping[str, Any], endpoint: str, params: Mapping[str, Any]) -> None:
    """Log endpoint and main params for manual smoke testing."""

    safe_params = {key: value for key, value in params.items() if key != "appid"}
    safe_params["appid"] = "***"
    _LOGGER.info("[openweather] endpoint=%s params=%s", endpoint, safe_params)
    if _debug_enabled(provider_config):
        print(f"[openweather] endpoint={endpoint} params={safe_params}")


def _debug_enabled(provider_config: Mapping[str, Any]) -> bool:
    """Enable manual debug logging from config or environment."""

    if bool(provider_config.get("debug", False)):
        return True
    return os.getenv("WEATHER_PROVIDER_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}


def _extract_openweather_tmax(payload: Mapping[str, Any], target_day: str) -> float | None:
    """Extract daily maximum from 3-hour forecast entries."""

    forecast_items = payload.get("list")
    if not isinstance(forecast_items, list):
        return None

    matches: list[float] = []
    for item in forecast_items:
        if not isinstance(item, Mapping):
            continue
        dt_txt = str(item.get("dt_txt", ""))
        if not dt_txt.startswith(target_day):
            continue
        main_block = item.get("main")
        if not isinstance(main_block, Mapping):
            continue
        value = main_block.get("temp_max")
        try:
            matches.append(float(value))
        except (TypeError, ValueError):
            continue

    return max(matches) if matches else None
