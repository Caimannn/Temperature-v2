"""Shared utility functions for weather provider implementations."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any, Mapping

from src.common.models import CityConfig, ForecastSnapshot


def normalize_target_day(value: str) -> str | None:
    """Allow only today and tomorrow target days."""

    today = date.today()
    tomorrow = today + timedelta(days=1)
    normalized = str(value).strip().lower()

    if normalized == "today":
        return today.isoformat()
    if normalized == "tomorrow":
        return tomorrow.isoformat()
    if normalized in {today.isoformat(), tomorrow.isoformat()}:
        return normalized
    return None


def failed_snapshot(
    city: CityConfig,
    target_day: str,
    provider_name: str,
    message: str,
    raw_payload: Mapping[str, Any] | None = None,
) -> ForecastSnapshot:
    """Return a non-raising failure snapshot."""

    payload = dict(raw_payload or {})
    payload.setdefault("error", message)
    return ForecastSnapshot(
        city=city,
        target_day=target_day,
        provider_name=provider_name,
        observed_at=now_iso(),
        predicted_tmax_f=float("nan"),
        raw_payload=payload,
    )


def now_iso() -> str:
    """Return current UTC timestamp in ISO format."""

    return datetime.now(UTC).isoformat()


def location_query(city: CityConfig) -> str:
    """Build provider query string from city config."""

    return city.label or city.key
