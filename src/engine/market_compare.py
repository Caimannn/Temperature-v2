"""Baseline comparison layer between model and market bin probabilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from src.engine.market_ladder import (
    CanonicalTemperatureBin,
    infer_missing_ladder_bins,
    model_probability_for_canonical_bin,
    parse_temperature_bin_label,
)
from src.engine.weather_probability import WeatherProbabilityDistribution


@dataclass(frozen=True)
class PolymarketTemperatureBin:
    """Normalized market bin input used by the comparison layer."""

    range_label: str
    market_probability: float | None
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketBinComparison:
    """One row of model vs market comparison."""

    range_label: str
    model_probability: float | None
    market_probability: float | None
    probability_edge: float | None
    diagnostics: str = ""


@dataclass(frozen=True)
class MarketComparisonDiagnostics:
    """Diagnostics for parse failures and matching gaps."""

    parse_failed_bins: tuple[str, ...]
    unmatched_market_bins: tuple[str, ...]
    unmatched_model_bins: tuple[str, ...]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class MarketComparisonResult:
    """Comparison output for one event/day market set."""

    city_key: str
    target_day: str
    rows: tuple[MarketBinComparison, ...]
    diagnostics: MarketComparisonDiagnostics

def compare_market_probabilities(
    distribution: WeatherProbabilityDistribution,
    market_bins: Sequence[PolymarketTemperatureBin | Mapping[str, Any]],
) -> MarketComparisonResult:
    """Compare model bin probabilities against market-implied probabilities."""

    rows: list[MarketBinComparison] = []
    normalized_bins: list[PolymarketTemperatureBin] = []
    normalized_notes: dict[str, list[str]] = {}

    for raw_bin in market_bins:
        normalized_bin, notes = _normalize_market_bin(raw_bin)
        normalized_bins.append(normalized_bin)
        normalized_notes[normalized_bin.range_label] = notes

    canonical_by_label: dict[str, CanonicalTemperatureBin] = {}
    parse_failed_bins: list[str] = []
    unmatched_market_bins: list[str] = []
    ladder_bins: list[CanonicalTemperatureBin] = []

    for normalized_bin in normalized_bins:
        range_label = normalized_bin.range_label
        parsed = parse_temperature_bin_label(range_label)
        if parsed is None:
            parse_failed_bins.append(range_label)
            continue
        canonical_by_label[range_label] = parsed
        ladder_bins.append(parsed)

    missing_ladder_bins = infer_missing_ladder_bins(ladder_bins)

    for normalized_bin in normalized_bins:
        range_label = normalized_bin.range_label
        market_probability = normalized_bin.market_probability
        notes = list(normalized_notes.get(range_label, []))

        if range_label == "<missing label>":
            unmatched_market_bins.append(range_label)

        parsed_bin = canonical_by_label.get(range_label)
        if parsed_bin is None:
            rows.append(
                MarketBinComparison(
                    range_label=range_label,
                    model_probability=None,
                    market_probability=market_probability,
                    probability_edge=None,
                    diagnostics=_join_notes(notes + ["parse failed: unsupported market label"]),
                )
            )
            continue

        model_probability = model_probability_for_canonical_bin(
            center_f=distribution.center_used_f,
            spread_f=distribution.spread_proxy_f,
            bin_def=parsed_bin,
        )
        edge = None if market_probability is None else model_probability - market_probability

        row_notes = list(notes)
        row_notes.append(f"canonical={parsed_bin.canonical_label}")
        rows.append(
            MarketBinComparison(
                range_label=range_label,
                model_probability=model_probability,
                market_probability=market_probability,
                probability_edge=edge,
                diagnostics=_join_notes(row_notes),
            )
        )

    diagnostics = MarketComparisonDiagnostics(
        parse_failed_bins=tuple(parse_failed_bins),
        unmatched_market_bins=tuple(unmatched_market_bins),
        unmatched_model_bins=tuple(missing_ladder_bins),
        notes=tuple([f"missing ladder bin: {label}" for label in missing_ladder_bins]),
    )

    return MarketComparisonResult(
        city_key=distribution.city_key,
        target_day=distribution.target_day,
        rows=tuple(rows),
        diagnostics=diagnostics,
    )


def _normalize_market_bin(
    raw_bin: PolymarketTemperatureBin | Mapping[str, Any],
) -> tuple[PolymarketTemperatureBin, list[str]]:
    """Normalize incoming market bin data into one typed structure."""

    notes: list[str] = []

    if isinstance(raw_bin, PolymarketTemperatureBin):
        return raw_bin, notes

    label = _extract_label(raw_bin)
    probability, prob_note = _extract_market_probability(raw_bin)
    if prob_note:
        notes.append(prob_note)

    return (
        PolymarketTemperatureBin(
            range_label=label,
            market_probability=probability,
            raw=dict(raw_bin),
        ),
        notes,
    )


def _extract_label(raw_bin: Mapping[str, Any]) -> str:
    """Extract market label using common field names."""

    for key in ("range_label", "label", "outcome_label", "market_label"):
        value = raw_bin.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()

    return "<missing label>"


def _extract_market_probability(raw_bin: Mapping[str, Any]) -> tuple[float | None, str | None]:
    """Extract market implied probability from common field names."""

    for key in (
        "market_probability",
        "implied_probability",
        "yes_implied_probability",
        "yes_probability",
        "probability",
        "yes_price",
        "price",
    ):
        if key not in raw_bin:
            continue
        value = _to_float(raw_bin.get(key))
        if value is None:
            return None, f"invalid market probability value in field '{key}'"
        if value > 1.0 and value <= 100.0:
            return value / 100.0, f"converted percentage-like value from field '{key}'"
        return value, None

    return None, "missing market probability"

def _to_float(value: Any) -> float | None:
    """Convert a value to float when possible."""

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _join_notes(notes: Sequence[str]) -> str:
    """Join notes into one diagnostic string."""

    clean_notes = [note for note in notes if note]
    return "; ".join(clean_notes)
