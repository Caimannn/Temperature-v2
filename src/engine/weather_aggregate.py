"""Baseline weather forecast aggregation utilities."""

from __future__ import annotations

from dataclasses import dataclass
import math
from statistics import median
from typing import Iterable, Literal, Sequence

from src.common.models import ConfidenceBand, ForecastSnapshot


@dataclass(frozen=True)
class ProviderForecastDetail:
    """Per-provider normalized diagnostic detail."""

    provider_name: str
    status: Literal["ok", "failed"]
    predicted_tmax_f: float | None
    observed_at: str
    error: str | None
    confidence: float | None
    confidence_band: ConfidenceBand | None
    prediction_interval: ConfidenceBand | None


@dataclass(frozen=True)
class WeatherAggregate:
    """Simple aggregate output for later probability and signal steps."""

    city_key: str
    target_day: str
    central_predicted_tmax_f: float | None
    provider_ok_count: int
    provider_total_count: int
    median_tmax_f: float | None
    min_tmax_f: float | None
    max_tmax_f: float | None
    spread_tmax_f: float | None
    disagreement_summary: str
    aggregate_confidence_band: ConfidenceBand | None
    confidence_note: str
    provider_details: tuple[ProviderForecastDetail, ...]


def aggregate_forecasts(snapshots: Sequence[ForecastSnapshot]) -> WeatherAggregate:
    """Aggregate multiple provider snapshots for one city and target day."""

    if not snapshots:
        raise ValueError("At least one ForecastSnapshot is required.")

    _validate_same_city_and_day(snapshots)

    details = tuple(_to_detail(snapshot) for snapshot in snapshots)
    ok_values = [
        detail.predicted_tmax_f
        for detail in details
        if detail.status == "ok" and detail.predicted_tmax_f is not None
    ]

    median_tmax = median(ok_values) if ok_values else None
    min_tmax = min(ok_values) if ok_values else None
    max_tmax = max(ok_values) if ok_values else None
    spread = (max_tmax - min_tmax) if min_tmax is not None and max_tmax is not None else None

    agg_band, band_note = _aggregate_confidence_band(
        snapshot
        for snapshot in snapshots
        if _is_ok_snapshot(snapshot)
    )

    city_key = snapshots[0].city.key
    target_day = snapshots[0].target_day
    ok_count = sum(1 for detail in details if detail.status == "ok")

    return WeatherAggregate(
        city_key=city_key,
        target_day=target_day,
        central_predicted_tmax_f=median_tmax,
        provider_ok_count=ok_count,
        provider_total_count=len(snapshots),
        median_tmax_f=median_tmax,
        min_tmax_f=min_tmax,
        max_tmax_f=max_tmax,
        spread_tmax_f=spread,
        disagreement_summary=_disagreement_summary(spread, ok_count, len(snapshots)),
        aggregate_confidence_band=agg_band,
        confidence_note=band_note,
        provider_details=details,
    )


def _validate_same_city_and_day(snapshots: Sequence[ForecastSnapshot]) -> None:
    """Ensure all snapshots belong to one city and target day."""

    city_key = snapshots[0].city.key
    target_day = snapshots[0].target_day

    for snapshot in snapshots[1:]:
        if snapshot.city.key != city_key or snapshot.target_day != target_day:
            raise ValueError("All ForecastSnapshot items must share the same city and target_day.")


def _to_detail(snapshot: ForecastSnapshot) -> ProviderForecastDetail:
    """Convert a snapshot into a compact per-provider diagnostic detail."""

    ok = _is_ok_snapshot(snapshot)
    error = None if ok else str(snapshot.raw_payload.get("error", "provider failed"))

    return ProviderForecastDetail(
        provider_name=snapshot.provider_name,
        status="ok" if ok else "failed",
        predicted_tmax_f=snapshot.predicted_tmax_f if ok else None,
        observed_at=snapshot.observed_at,
        error=error,
        confidence=snapshot.confidence,
        confidence_band=snapshot.confidence_band,
        prediction_interval=snapshot.prediction_interval,
    )


def _is_ok_snapshot(snapshot: ForecastSnapshot) -> bool:
    """Determine whether a snapshot should be counted as successful."""

    if "error" in snapshot.raw_payload:
        return False

    try:
        return math.isfinite(float(snapshot.predicted_tmax_f))
    except (TypeError, ValueError):
        return False


def _aggregate_confidence_band(snapshots: Iterable[ForecastSnapshot]) -> tuple[ConfidenceBand | None, str]:
    """Build a placeholder aggregate confidence band when bands are available."""

    lowers: list[float] = []
    uppers: list[float] = []
    levels: list[float] = []

    for snapshot in snapshots:
        if snapshot.confidence_band is None:
            continue
        if snapshot.confidence_band.lower_tmax_f is not None:
            lowers.append(snapshot.confidence_band.lower_tmax_f)
        if snapshot.confidence_band.upper_tmax_f is not None:
            uppers.append(snapshot.confidence_band.upper_tmax_f)
        if snapshot.confidence_band.confidence_level is not None:
            levels.append(snapshot.confidence_band.confidence_level)

    if not lowers and not uppers:
        return None, "confidence band unavailable"

    return (
        ConfidenceBand(
            lower_tmax_f=min(lowers) if lowers else None,
            upper_tmax_f=max(uppers) if uppers else None,
            confidence_level=median(levels) if levels else None,
        ),
        "placeholder confidence band aggregated from available providers",
    )


def _disagreement_summary(spread: float | None, ok_count: int, total_count: int) -> str:
    """Summarize provider disagreement in plain text."""

    if ok_count == 0:
        return f"no usable providers ({ok_count}/{total_count} ok)"
    if spread is None:
        return f"single usable provider ({ok_count}/{total_count} ok)"
    if spread < 2:
        return f"low disagreement (spread={spread:.2f}F, {ok_count}/{total_count} ok)"
    if spread < 5:
        return f"moderate disagreement (spread={spread:.2f}F, {ok_count}/{total_count} ok)"
    return f"high disagreement (spread={spread:.2f}F, {ok_count}/{total_count} ok)"
