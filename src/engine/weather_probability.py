"""Baseline probability layer for temperature-bin forecasts."""

from __future__ import annotations

from dataclasses import dataclass
from math import erf, isfinite, sqrt
import re
from typing import Sequence

from src.engine.weather_aggregate import ProviderForecastDetail, WeatherAggregate


@dataclass(frozen=True)
class TemperatureBin:
    """A temperature bin with optional open-ended bounds."""

    label: str
    lower_f: int | None = None
    upper_f: int | None = None


@dataclass(frozen=True)
class TemperatureBinProbability:
    """Probability assigned to one temperature bin."""

    label: str
    probability: float


@dataclass(frozen=True)
class WeatherProbabilityDistribution:
    """Probability output for one city and target day."""

    city_key: str
    target_day: str
    center_used_f: float
    raw_spread_input_f: float | None
    spread_proxy_f: float
    spread_floor_applied: bool
    probability_floor_applied: bool
    max_single_bin_cap_applied: bool
    provider_ok_count: int
    provider_total_count: int
    reasoning_note: str
    provider_details: tuple[ProviderForecastDetail, ...]
    bins: tuple[TemperatureBinProbability, ...]


_DEFAULT_BIN_LABELS: tuple[str, ...] = (
    "57F or below",
    "58-59F",
    "60-61F",
    "62-63F",
    "64-65F",
    "66-67F",
    "68-69F",
    "70-71F",
    "72F or higher",
)

# Guardrails for baseline probability model
_MINIMUM_SPREAD_FLOOR_F = 3.0
_PROBABILITY_FLOOR = 0.01
_MAX_SINGLE_BIN_PROBABILITY = 0.95

_RANGE_PATTERN = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*F?\s*$", re.IGNORECASE)
_BELOW_PATTERN = re.compile(r"^\s*(\d+)\s*F?\s*or\s*below\s*$", re.IGNORECASE)
_ABOVE_PATTERN = re.compile(r"^\s*(\d+)\s*F?\s*or\s*higher\s*$", re.IGNORECASE)


def build_temperature_bin_probabilities(
    aggregate: WeatherAggregate,
    bin_labels: Sequence[str] | None = None,
    minimum_spread_floor_f: float = _MINIMUM_SPREAD_FLOOR_F,
    probability_floor: float = _PROBABILITY_FLOOR,
    max_single_bin_probability: float | None = _MAX_SINGLE_BIN_PROBABILITY,
) -> WeatherProbabilityDistribution:
    """Convert one weather aggregate into a normalized bin distribution."""

    bins = tuple(_parse_bin_label(label) for label in (bin_labels or _DEFAULT_BIN_LABELS))
    center_used = _resolve_center(aggregate)
    spread_proxy, raw_spread_input, spread_floor_applied = _derive_spread_proxy(
        aggregate,
        minimum_spread_floor_f=minimum_spread_floor_f,
    )

    raw_probabilities = [
        _probability_for_bin(center=center_used, sigma=spread_proxy, temp_bin=temp_bin)
        for temp_bin in bins
    ]
    normalized_probabilities = _normalize_probabilities(raw_probabilities)
    guardrail_probabilities, probability_floor_applied, max_single_bin_cap_applied = (
        _apply_probability_guardrails(
            normalized_probabilities,
            probability_floor=probability_floor,
            max_single_bin_probability=max_single_bin_probability,
        )
    )

    distribution = tuple(
        TemperatureBinProbability(label=temp_bin.label, probability=probability)
        for temp_bin, probability in zip(bins, guardrail_probabilities)
    )

    reasoning_note = (
        f"Baseline normal model: center={center_used:.2f}F, "
        f"raw_spread_input={raw_spread_input}, final_spread_used={spread_proxy:.2f}F, "
        f"spread_floor_applied={spread_floor_applied}, "
        f"probability_floor={probability_floor}, "
        f"probability_floor_applied={probability_floor_applied}, "
        f"max_single_bin_probability={max_single_bin_probability}, "
        f"max_single_bin_cap_applied={max_single_bin_cap_applied}."
    )

    return WeatherProbabilityDistribution(
        city_key=aggregate.city_key,
        target_day=aggregate.target_day,
        center_used_f=center_used,
        raw_spread_input_f=raw_spread_input,
        spread_proxy_f=spread_proxy,
        spread_floor_applied=spread_floor_applied,
        probability_floor_applied=probability_floor_applied,
        max_single_bin_cap_applied=max_single_bin_cap_applied,
        provider_ok_count=aggregate.provider_ok_count,
        provider_total_count=aggregate.provider_total_count,
        reasoning_note=reasoning_note,
        provider_details=aggregate.provider_details,
        bins=distribution,
    )


def _resolve_center(aggregate: WeatherAggregate) -> float:
    """Resolve the center used by the baseline model."""

    center = aggregate.central_predicted_tmax_f
    if center is not None and isfinite(center):
        return float(center)

    if aggregate.min_tmax_f is not None and aggregate.max_tmax_f is not None:
        return (aggregate.min_tmax_f + aggregate.max_tmax_f) / 2.0

    return 65.0


def _derive_spread_proxy(
    aggregate: WeatherAggregate,
    minimum_spread_floor_f: float,
) -> tuple[float, float | None, bool]:
    """Derive spread proxy with a configurable floor and diagnostics."""

    raw_spread_input = aggregate.spread_tmax_f
    raw_spread = (
        float(raw_spread_input)
        if raw_spread_input is not None and isfinite(raw_spread_input)
        else None
    )

    if raw_spread is None or raw_spread <= 0:
        baseline = 1.75 if aggregate.provider_ok_count <= 1 else 1.25
    else:
        # For now sigma scales directly with disagreement and can be calibrated later.
        baseline = max(1.0, raw_spread * 0.40)

    spread_floor = max(0.0, float(minimum_spread_floor_f))
    final_spread = max(spread_floor, baseline)
    floor_applied = final_spread > baseline
    return final_spread, raw_spread, floor_applied


def _probability_for_bin(center: float, sigma: float, temp_bin: TemperatureBin) -> float:
    """Compute bin probability from a normal CDF approximation."""

    if temp_bin.lower_f is None and temp_bin.upper_f is None:
        return 0.0

    if temp_bin.lower_f is None:
        upper_edge = temp_bin.upper_f + 0.5  # type: ignore[operator]
        return _normal_cdf(upper_edge, center, sigma)

    if temp_bin.upper_f is None:
        lower_edge = temp_bin.lower_f - 0.5
        return 1.0 - _normal_cdf(lower_edge, center, sigma)

    lower_edge = temp_bin.lower_f - 0.5
    upper_edge = temp_bin.upper_f + 0.5
    return _normal_cdf(upper_edge, center, sigma) - _normal_cdf(lower_edge, center, sigma)


def _normal_cdf(x: float, mu: float, sigma: float) -> float:
    """Compute CDF of a normal distribution using erf."""

    scaled = (x - mu) / (sigma * sqrt(2.0))
    return 0.5 * (1.0 + erf(scaled))


def _apply_probability_guardrails(
    probabilities: list[float],
    probability_floor: float,
    max_single_bin_probability: float | None,
) -> tuple[list[float], bool, bool]:
    """Apply floor/cap guardrails and return normalized probabilities plus flags."""

    if not probabilities:
        return probabilities, False, False

    safe_floor = max(0.0, float(probability_floor))
    safe_floor = min(safe_floor, 1.0 / len(probabilities))

    floored = [max(safe_floor, p) for p in probabilities]
    probability_floor_applied = any(after > before for before, after in zip(probabilities, floored))
    normalized = _normalize_probabilities(floored)

    max_single_bin_cap_applied = False
    if max_single_bin_probability is not None:
        safe_cap = max(1.0 / len(normalized), float(max_single_bin_probability))
        safe_cap = min(1.0, safe_cap)
        normalized, max_single_bin_cap_applied = _cap_and_redistribute(normalized, safe_cap)

    return normalized, probability_floor_applied, max_single_bin_cap_applied


def _cap_and_redistribute(probabilities: list[float], cap: float) -> tuple[list[float], bool]:
    """Cap large bins and redistribute excess mass deterministically."""

    if not probabilities:
        return probabilities, False

    values = list(probabilities)
    applied = False
    epsilon = 1e-12

    for _ in range(len(values) + 1):
        over_cap = [idx for idx, value in enumerate(values) if value > cap + epsilon]
        if not over_cap:
            break

        applied = True
        excess = sum(values[idx] - cap for idx in over_cap)
        for idx in over_cap:
            values[idx] = cap

        recipients = [idx for idx, value in enumerate(values) if value < cap - epsilon]
        if not recipients:
            break

        recipient_weight = sum(values[idx] for idx in recipients)
        if recipient_weight <= epsilon:
            share = excess / len(recipients)
            for idx in recipients:
                values[idx] += share
        else:
            for idx in recipients:
                values[idx] += excess * (values[idx] / recipient_weight)

    return _normalize_probabilities(values), applied


def _normalize_probabilities(values: Sequence[float]) -> list[float]:
    """Normalize raw probabilities to sum exactly to 1.0."""

    clipped = [max(0.0, float(value)) for value in values]
    total = sum(clipped)

    if total <= 0.0:
        if not clipped:
            return []
        uniform = 1.0 / len(clipped)
        return [uniform for _ in clipped]

    normalized = [value / total for value in clipped]
    running = sum(normalized[:-1]) if len(normalized) > 1 else 0.0
    normalized[-1] = max(0.0, 1.0 - running)
    return normalized


def _parse_bin_label(label: str) -> TemperatureBin:
    """Parse a polymarket-style bin label into numeric bounds."""

    cleaned = label.replace("\u00b0", "").strip()

    range_match = _RANGE_PATTERN.match(cleaned)
    if range_match:
        lower = int(range_match.group(1))
        upper = int(range_match.group(2))
        if lower > upper:
            raise ValueError(f"Invalid bin range: {label}")
        return TemperatureBin(label=label, lower_f=lower, upper_f=upper)

    below_match = _BELOW_PATTERN.match(cleaned)
    if below_match:
        upper = int(below_match.group(1))
        return TemperatureBin(label=label, lower_f=None, upper_f=upper)

    above_match = _ABOVE_PATTERN.match(cleaned)
    if above_match:
        lower = int(above_match.group(1))
        return TemperatureBin(label=label, lower_f=lower, upper_f=None)

    raise ValueError(f"Unsupported temperature bin label: {label}")
