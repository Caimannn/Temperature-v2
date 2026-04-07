"""Build ranked signal candidates from market/model comparisons."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from src.engine.market_compare import MarketComparisonResult


SignalDirection = Literal["BUY_YES", "BUY_NO"]


@dataclass(frozen=True)
class SignalCandidateFilters:
    """Configurable filters for candidate generation."""

    minimum_absolute_edge: float = 0.0
    ignore_missing_probabilities: bool = True
    ignore_parse_failed_or_unmatched_rows: bool = True


@dataclass(frozen=True)
class SignalCandidateRow:
    """One ranked candidate row derived from a comparison row."""

    range_label: str
    model_probability: float
    market_probability: float
    probability_edge: float
    abs_edge: float
    raw_signal_direction: SignalDirection
    confidence_note: str
    diagnostics: str


@dataclass(frozen=True)
class SignalCandidatesDiagnostics:
    """Summary of filtering and passthrough diagnostics."""

    input_rows_count: int
    kept_rows_count: int
    excluded_for_parse_or_unmatched: int
    excluded_for_missing_probabilities: int
    excluded_for_minimum_edge: int
    parse_failed_bins: tuple[str, ...]
    unmatched_market_bins: tuple[str, ...]
    unmatched_model_bins: tuple[str, ...]
    notes: tuple[str, ...]


@dataclass(frozen=True)
class SignalCandidatesResult:
    """Output container for ranked candidates and diagnostics."""

    city_key: str
    target_day: str
    all_ranked_candidates: tuple[SignalCandidateRow, ...]
    best_yes_candidate: SignalCandidateRow | None
    best_no_candidate: SignalCandidateRow | None
    diagnostics: SignalCandidatesDiagnostics


def build_signal_candidates(
    comparison: MarketComparisonResult,
    filters: SignalCandidateFilters | None = None,
) -> SignalCandidatesResult:
    """Build deterministic, explainable signal candidates from comparisons."""

    effective_filters = filters or SignalCandidateFilters()

    parse_failed = set(comparison.diagnostics.parse_failed_bins)
    unmatched_market = set(comparison.diagnostics.unmatched_market_bins)

    kept: list[SignalCandidateRow] = []
    excluded_parse_unmatched = 0
    excluded_missing_probs = 0
    excluded_min_edge = 0

    for row in comparison.rows:
        if effective_filters.ignore_parse_failed_or_unmatched_rows:
            if row.range_label in parse_failed or row.range_label in unmatched_market:
                excluded_parse_unmatched += 1
                continue

        if row.model_probability is None or row.market_probability is None or row.probability_edge is None:
            if effective_filters.ignore_missing_probabilities:
                excluded_missing_probs += 1
                continue
            else:
                excluded_missing_probs += 1
                continue

        abs_edge = abs(row.probability_edge)
        if abs_edge < effective_filters.minimum_absolute_edge:
            excluded_min_edge += 1
            continue

        direction = _direction_from_edge(row.probability_edge)
        if direction is None:
            # Edge exactly zero has no directional signal.
            excluded_min_edge += 1
            continue

        kept.append(
            SignalCandidateRow(
                range_label=row.range_label,
                model_probability=row.model_probability,
                market_probability=row.market_probability,
                probability_edge=row.probability_edge,
                abs_edge=abs_edge,
                raw_signal_direction=direction,
                confidence_note=f"abs_edge={abs_edge:.4f}; min_abs_edge={effective_filters.minimum_absolute_edge:.4f}",
                diagnostics=row.diagnostics,
            )
        )

    ranked = tuple(
        sorted(
            kept,
            key=lambda item: (
                -item.abs_edge,
                item.range_label,
                item.raw_signal_direction,
            ),
        )
    )

    best_yes = next((item for item in ranked if item.raw_signal_direction == "BUY_YES"), None)
    best_no = next((item for item in ranked if item.raw_signal_direction == "BUY_NO"), None)

    summary = SignalCandidatesDiagnostics(
        input_rows_count=len(comparison.rows),
        kept_rows_count=len(ranked),
        excluded_for_parse_or_unmatched=excluded_parse_unmatched,
        excluded_for_missing_probabilities=excluded_missing_probs,
        excluded_for_minimum_edge=excluded_min_edge,
        parse_failed_bins=comparison.diagnostics.parse_failed_bins,
        unmatched_market_bins=comparison.diagnostics.unmatched_market_bins,
        unmatched_model_bins=comparison.diagnostics.unmatched_model_bins,
        notes=comparison.diagnostics.notes,
    )

    return SignalCandidatesResult(
        city_key=comparison.city_key,
        target_day=comparison.target_day,
        all_ranked_candidates=ranked,
        best_yes_candidate=best_yes,
        best_no_candidate=best_no,
        diagnostics=summary,
    )


def _direction_from_edge(edge: float) -> SignalDirection | None:
    """Map probability edge to raw yes/no direction."""

    if edge > 0:
        return "BUY_YES"
    if edge < 0:
        return "BUY_NO"
    return None
