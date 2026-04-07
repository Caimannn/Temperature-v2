"""Shared domain models for the weather bot foundation."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping
from typing import Literal


class OperationMode(str, Enum):
    """Supported high-level operating modes."""

    MANUAL_ONLY = "manual-only"
    COLLECT_ONLY = "collect-only"


class ForecastHorizon(str, Enum):
    """Supported forecast horizons."""

    TODAY = "today"
    TOMORROW = "tomorrow"


class AdviceAction(str, Enum):
    """Text-only advice vocabulary."""

    HOLD = "HOLD"
    CLOSE = "CLOSE"
    ADD = "ADD"
    BUY_NEW = "BUY NEW"
    SWITCH = "SWITCH"


@dataclass(frozen=True)
class CityConfig:
    """Static city metadata loaded from configuration."""

    key: str
    label: str
    slug: str | None = None
    resolution_slug: str | None = None
    resolver: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketRange:
    """Normalized temperature range for a market."""

    label: str
    minimum_fahrenheit: float | None = None
    maximum_fahrenheit: float | None = None


@dataclass(frozen=True)
class OutcomeQuote:
    """Explicit YES/NO quote for a market outcome."""

    side: Literal["YES", "NO"]
    label: str
    normalized_label: str
    price: float | None = None


@dataclass(frozen=True)
class MarketRecord:
    """One logged market snapshot for tuning and review."""

    city_key: str
    horizon: ForecastHorizon
    market_name: str
    observed_at: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PositionAdvice:
    """A single position recommendation in text form."""

    city_key: str
    horizon: ForecastHorizon
    action: AdviceAction
    switch_to_city: str | None = None
    note: str = ""


@dataclass(frozen=True)
class DayMarketSnapshot:
    """Normalized market snapshot for one city and one horizon."""

    city: CityConfig
    horizon: ForecastHorizon
    market_name: str
    market_slug: str | None
    market_range: MarketRange
    outcome_quotes: tuple[OutcomeQuote, OutcomeQuote]
    observed_at: str
    source: str = "polymarket"
    raw_market: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ConfidenceBand:
    """Optional confidence band for Tmax forecasts."""

    lower_tmax_f: float | None = None
    upper_tmax_f: float | None = None
    confidence_level: float | None = None


@dataclass(frozen=True)
class ForecastSnapshot:
    """Provider-normalized forecast snapshot for one city and one day."""

    city: CityConfig
    target_day: str
    provider_name: str
    observed_at: str
    predicted_tmax_f: float
    confidence: float | None = None
    confidence_band: ConfidenceBand | None = None
    prediction_interval: ConfidenceBand | None = None
    raw_payload: Mapping[str, Any] = field(default_factory=dict)

    @property
    def raw_provider_payload(self) -> Mapping[str, Any]:
        """Backward-compatible alias for older field naming."""

        return self.raw_payload

