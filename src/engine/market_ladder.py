"""Market ladder parsing and model-probability helpers for temperature bins."""

from __future__ import annotations

from dataclasses import dataclass
from math import erf, sqrt
import re
from typing import Sequence


_RANGE_PATTERN = re.compile(r"^\s*(\d{1,3})\s*-\s*(\d{1,3})\s*(?:°)?\s*F\s*$", re.IGNORECASE)
_BELOW_PATTERN = re.compile(r"^\s*(\d{1,3})\s*(?:°)?\s*F\s*or\s*(?:lower|below)\s*$", re.IGNORECASE)
_ABOVE_PATTERN = re.compile(r"^\s*(\d{1,3})\s*(?:°)?\s*F\+\s*$", re.IGNORECASE)
_ABOVE_ALT_PATTERN = re.compile(r"^\s*(\d{1,3})\s*(?:°)?\s*F\s*or\s*higher\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class CanonicalTemperatureBin:
    """Canonical representation of one market ladder bin."""

    original_label: str
    canonical_label: str
    low_f: int | None
    high_f: int | None
    open_left: bool
    open_right: bool


def parse_temperature_bin_label(label: str) -> CanonicalTemperatureBin | None:
    """Parse one temperature label into canonical bin fields."""

    text = str(label).strip()
    if not text:
        return None

    range_match = _RANGE_PATTERN.match(text)
    if range_match:
        low = int(range_match.group(1))
        high = int(range_match.group(2))
        if low > high:
            return None
        return CanonicalTemperatureBin(
            original_label=text,
            canonical_label=f"{low}-{high}F",
            low_f=low,
            high_f=high,
            open_left=False,
            open_right=False,
        )

    below_match = _BELOW_PATTERN.match(text)
    if below_match:
        high = int(below_match.group(1))
        return CanonicalTemperatureBin(
            original_label=text,
            canonical_label=f"{high}F or lower",
            low_f=None,
            high_f=high,
            open_left=True,
            open_right=False,
        )

    above_match = _ABOVE_PATTERN.match(text) or _ABOVE_ALT_PATTERN.match(text)
    if above_match:
        low = int(above_match.group(1))
        return CanonicalTemperatureBin(
            original_label=text,
            canonical_label=f"{low}F+",
            low_f=low,
            high_f=None,
            open_left=False,
            open_right=True,
        )

    return None


def infer_missing_ladder_bins(bins: Sequence[CanonicalTemperatureBin]) -> list[str]:
    """Infer obvious missing ladder bins from parsed bins."""

    missing: list[str] = []
    closed_bins = sorted(
        [item for item in bins if item.low_f is not None and item.high_f is not None],
        key=lambda item: item.low_f if item.low_f is not None else -10_000,
    )

    lower_tails = [item for item in bins if item.open_left and item.high_f is not None]
    upper_tails = [item for item in bins if item.open_right and item.low_f is not None]

    if lower_tails and closed_bins:
        highest_lower_tail = max(t.high_f for t in lower_tails if t.high_f is not None)
        first_closed_low = closed_bins[0].low_f
        if first_closed_low is not None and highest_lower_tail is not None and first_closed_low > highest_lower_tail + 1:
            gap_low = highest_lower_tail + 1
            gap_high = first_closed_low - 1
            missing.append(f"{gap_low}-{gap_high}F")

    for index in range(len(closed_bins) - 1):
        current_high = closed_bins[index].high_f
        next_low = closed_bins[index + 1].low_f
        if current_high is None or next_low is None:
            continue
        if next_low > current_high + 1:
            gap_low = current_high + 1
            gap_high = next_low - 1
            missing.append(f"{gap_low}-{gap_high}F")

    if upper_tails and closed_bins:
        lowest_upper_tail = min(t.low_f for t in upper_tails if t.low_f is not None)
        last_closed_high = closed_bins[-1].high_f
        if last_closed_high is not None and lowest_upper_tail is not None and lowest_upper_tail > last_closed_high + 1:
            gap_low = last_closed_high + 1
            gap_high = lowest_upper_tail - 1
            missing.append(f"{gap_low}-{gap_high}F")

    return missing


def model_probability_for_canonical_bin(center_f: float, spread_f: float, bin_def: CanonicalTemperatureBin) -> float:
    """Compute model probability over one canonical bin from center/spread."""

    if bin_def.open_left and bin_def.high_f is not None:
        upper_edge = bin_def.high_f + 0.5
        return _normal_cdf(upper_edge, center_f, spread_f)

    if bin_def.open_right and bin_def.low_f is not None:
        lower_edge = bin_def.low_f - 0.5
        return 1.0 - _normal_cdf(lower_edge, center_f, spread_f)

    if bin_def.low_f is not None and bin_def.high_f is not None:
        lower_edge = bin_def.low_f - 0.5
        upper_edge = bin_def.high_f + 0.5
        return _normal_cdf(upper_edge, center_f, spread_f) - _normal_cdf(lower_edge, center_f, spread_f)

    return 0.0


def _normal_cdf(x: float, mu: float, sigma: float) -> float:
    """Compute CDF of a normal distribution using erf."""

    scaled = (x - mu) / (sigma * sqrt(2.0))
    return 0.5 * (1.0 + erf(scaled))
