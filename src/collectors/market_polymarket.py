"""Polymarket market collector placeholder."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from src.common.models import CityConfig, DayMarketSnapshot, ForecastHorizon, MarketRange, OutcomeQuote


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"
_RANGE_NUMBER_PATTERN = re.compile(r"\d+(?:\.\d+)?")


MarketSource = Callable[[CityConfig, ForecastHorizon], Mapping[str, Any]]


@dataclass(frozen=True)
class _PlaceholderMarketSource:
    """Fallback source that reads only config placeholders."""

    def fetch(self, city: CityConfig, horizon: ForecastHorizon) -> Mapping[str, Any]:
        """Build a small raw payload from config placeholders."""

        return {
            "market_name": f"{city.label} highest temperature",
            "market_slug": city.slug,
            "resolution_slug": city.resolution_slug,
            "market_range": city.resolution_slug or city.slug or city.key,
            "outcomes": [
                {"side": "YES", "label": "YES", "price": None},
                {"side": "NO", "label": "NO", "price": None},
            ],
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "city_key": city.key,
            "horizon": horizon.value,
        }


class PolymarketMarketCollector:
    """Small Polymarket collector that normalizes highest-temperature markets."""

    def __init__(self, config_path: Path | None = None, source: MarketSource | None = None) -> None:
        self.config_path = config_path or CONFIG_PATH
        self.source = source
        self.config = _load_config(self.config_path)

    def collect(self, city_key: str, horizon: ForecastHorizon | str) -> DayMarketSnapshot:
        """Fetch and normalize one city's market snapshot for one horizon."""

        horizon_value = _normalize_horizon(horizon)
        city = _load_city_config(self.config, city_key)
        raw_market = self._fetch_market(city, horizon_value)
        return _build_snapshot(city, horizon_value, raw_market)

    def _fetch_market(self, city: CityConfig, horizon: ForecastHorizon) -> Mapping[str, Any]:
        """Fetch raw market data from the configured source or placeholder source."""

        if self.source is not None:
            return self.source(city, horizon)
        return _PlaceholderMarketSource().fetch(city, horizon)


def _load_config(path: Path) -> dict[str, Any]:
    """Load the JSON-compatible config file."""

    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_horizon(horizon: ForecastHorizon | str) -> ForecastHorizon:
    """Normalize and validate the supported horizon."""

    if isinstance(horizon, ForecastHorizon):
        return horizon
    try:
        return ForecastHorizon(horizon)
    except ValueError as exc:
        raise ValueError(f"Unsupported horizon: {horizon}") from exc


def _load_city_config(config: Mapping[str, Any], city_key: str) -> CityConfig:
    """Load one city definition from config without hardcoding resolver logic."""

    cities = config.get("cities", {})
    if city_key not in cities:
        raise ValueError(f"Unknown city: {city_key}")

    city_block = cities[city_key]
    if not isinstance(city_block, Mapping):
        raise ValueError(f"Invalid city block for {city_key}")

    resolver = city_block.get("resolver", {})
    if not isinstance(resolver, Mapping):
        resolver = {}

    label = str(city_block.get("label", city_key.replace("_", " ").title()))
    slug = resolver.get("slug")
    resolution_slug = resolver.get("resolution")

    return CityConfig(
        key=city_key,
        label=label,
        slug=str(slug) if slug is not None else None,
        resolution_slug=str(resolution_slug) if resolution_slug is not None else None,
        resolver=dict(resolver),
    )


def _build_snapshot(city: CityConfig, horizon: ForecastHorizon, raw_market: Mapping[str, Any]) -> DayMarketSnapshot:
    """Build a structured snapshot from raw market data."""

    market_name = str(raw_market.get("market_name") or f"{city.label} highest temperature")
    market_slug = raw_market.get("market_slug") or city.slug
    market_range = _normalize_market_range(raw_market.get("market_range") or city.resolution_slug or city.slug)
    outcome_quotes = _normalize_outcome_quotes(raw_market.get("outcomes"))
    observed_at = str(raw_market.get("observed_at") or datetime.now(timezone.utc).isoformat())

    return DayMarketSnapshot(
        city=city,
        horizon=horizon,
        market_name=market_name,
        market_slug=str(market_slug) if market_slug is not None else None,
        market_range=market_range,
        outcome_quotes=outcome_quotes,
        observed_at=observed_at,
        raw_market=dict(raw_market),
    )


def _normalize_market_range(value: Any) -> MarketRange:
    """Normalize a market range label into a typed temperature range."""

    text = str(value).strip() if value is not None else ""
    matches = [float(item) for item in _RANGE_NUMBER_PATTERN.findall(text)]
    if len(matches) >= 2:
        low, high = matches[0], matches[1]
        return MarketRange(label=f"{_format_number(low)}-{_format_number(high)}", minimum_fahrenheit=low, maximum_fahrenheit=high)
    if len(matches) == 1:
        temperature = matches[0]
        return MarketRange(label=_format_number(temperature), minimum_fahrenheit=temperature, maximum_fahrenheit=temperature)
    return MarketRange(label=text or "unresolved")


def _normalize_outcome_quotes(raw_outcomes: Any) -> tuple[OutcomeQuote, OutcomeQuote]:
    """Normalize raw outcomes into explicit YES and NO quotes."""

    if not isinstance(raw_outcomes, list):
        raw_outcomes = []

    quotes: dict[str, OutcomeQuote] = {}
    for raw_outcome in raw_outcomes:
        if not isinstance(raw_outcome, Mapping):
            continue
        side = _normalize_side(raw_outcome.get("side") or raw_outcome.get("label") or raw_outcome.get("name"))
        label = str(raw_outcome.get("label") or raw_outcome.get("name") or side)
        quotes[side] = OutcomeQuote(
            side=side,
            label=label,
            normalized_label=side,
            price=_coerce_float(raw_outcome.get("price")),
        )

    yes_quote = quotes.get("YES") or OutcomeQuote(side="YES", label="YES", normalized_label="YES")
    no_quote = quotes.get("NO") or OutcomeQuote(side="NO", label="NO", normalized_label="NO")
    return yes_quote, no_quote


def _normalize_side(value: Any) -> str:
    """Map a raw label to the explicit YES/NO side."""

    text = str(value).strip().lower()
    if text in {"yes", "y", "buy", "long"}:
        return "YES"
    if text in {"no", "n", "sell", "short"}:
        return "NO"
    if "yes" in text:
        return "YES"
    if "no" in text:
        return "NO"
    return "YES"


def _coerce_float(value: Any) -> float | None:
    """Convert a raw numeric field to float when possible."""

    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_number(value: float) -> str:
    """Format a temperature value without trailing decimals when possible."""

    return str(int(value)) if value.is_integer() else str(value)
