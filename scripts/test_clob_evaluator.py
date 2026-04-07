"""Manual runner for executable signal evaluation using live CLOB quotes."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import sys
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.collectors.weather_registry import build_weather_collector
from src.common.models import CityConfig
from src.engine.clob_evaluator import ClobEvaluatorFilters, evaluate_executable_signal_candidates
from src.engine.market_compare import compare_market_probabilities
from src.engine.signal_candidates import SignalCandidateFilters, build_signal_candidates
from src.engine.signal_policy import SignalPolicyInputRow, apply_signal_policy
from src.engine.weather_aggregate import aggregate_forecasts
from src.engine.weather_probability import build_temperature_bin_probabilities


CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
SUPPORTED_HORIZONS = ("today", "tomorrow")
_TEMPERATURE_LABEL_PATTERN = re.compile(
    r"(?P<low>\d{1,3})(?:\s*[-to]+\s*(?P<high>\d{1,3}))?\s*(?:°)?\s*F",
    re.IGNORECASE,
)

_CITY_EVENT_ALIASES: dict[str, tuple[str, ...]] = {
    "nyc": ("new york", "central park", "nyc"),
    "atlanta": ("atlanta",),
}

_WEATHER_EVENT_HINTS: tuple[str, ...] = (
    "high temperature",
    "temperature",
    "fahrenheit",
    "degrees",
)


def _normalize_slug_part(value: str) -> str:
    """Normalize free text into a slug-like token."""

    cleaned = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return re.sub(r"-+", "-", cleaned).strip("-")


def _date_slug_variants(target_date: dt.date) -> tuple[str, ...]:
    """Build deterministic date strings used by historical weather market slugs."""

    month = target_date.strftime("%B").lower()
    day_no_zero = str(target_date.day)
    return (
        target_date.isoformat(),
        target_date.strftime("%Y%m%d"),
        f"{month}-{day_no_zero}-{target_date.year}",
        f"{month}-{target_date.day:02d}-{target_date.year}",
    )


def _city_slug_prefix(city_key: str, city_block: Mapping[str, Any]) -> str:
    """Read city-specific slug prefix from config/resolver settings."""

    resolver = city_block.get("resolver", {}) if isinstance(city_block, Mapping) else {}
    if not isinstance(resolver, Mapping):
        resolver = {}

    raw_prefix = resolver.get("slug_prefix")
    if raw_prefix is not None and str(raw_prefix).strip():
        return _normalize_slug_part(str(raw_prefix))

    # Safe fallback for repos where slug_prefix is not configured yet.
    fallback_by_city = {
        "nyc": "highest-temperature-in-nyc-on",
        "atlanta": "highest-temperature-in-atlanta-on",
    }
    if city_key in fallback_by_city:
        return fallback_by_city[city_key]

    return _normalize_slug_part(f"highest-temperature-in-{city_key}-on")


def _build_slug_candidates(slug_prefix: str, target_date: dt.date) -> list[str]:
    """Build expected event slug candidates from slug prefix and date."""

    variants = _date_slug_variants(target_date)
    candidates: list[str] = []
    for date_part in variants:
        candidates.append(f"{slug_prefix}-{date_part}")
        candidates.append(f"{slug_prefix}-on-{date_part}")
    return candidates


def load_config(path: Path) -> dict[str, Any]:
    """Load JSON-compatible config."""

    return json.loads(path.read_text(encoding="utf-8"))


def parse_args(default_city: str) -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description="Run manual CLOB executable-signal evaluation")
    parser.add_argument("--city", default=default_city, help="City key")
    parser.add_argument("--horizon", default="today", help="today or tomorrow")
    parser.add_argument("--min-abs-edge", type=float, default=0.01, help="Signal pre-filter min abs edge")
    parser.add_argument("--min-exec-edge", type=float, default=0.0, help="Minimum executable edge")
    parser.add_argument("--max-spread", type=float, default=None, help="Maximum spread (optional)")
    parser.add_argument("--min-size", type=float, default=0.0, help="Minimum available size")
    parser.add_argument("--gamma-limit", type=int, default=80, help="How many active gamma markets to read")
    parser.add_argument(
        "--target-date",
        default=None,
        help="Optional UTC target date in YYYY-MM-DD format for historical/future event validation",
    )
    parser.add_argument(
        "--discovery-only",
        action="store_true",
        help="Run strict event discovery report only (no CLOB edge evaluation)",
    )
    parser.add_argument(
        "--discovery-debug",
        action="store_true",
        help="Print detailed event-discovery tracing and rejection reasons",
    )
    parser.add_argument(
        "--debug-side-resolution",
        action="store_true",
        help="Print full BUY_YES/BUY_NO quote resolution diagnostics for 3 bins",
    )
    parser.add_argument(
        "--with-policy",
        action="store_true",
        help="Apply final signal policy on top executable candidates and print suppressions",
    )
    return parser.parse_args()


def build_city_config(city_key: str, city_block: Mapping[str, Any]) -> CityConfig:
    """Build CityConfig from config city block."""

    resolver = city_block.get("resolver", {}) if isinstance(city_block, Mapping) else {}
    if not isinstance(resolver, Mapping):
        resolver = {}

    return CityConfig(
        key=city_key,
        label=str(city_block.get("label", city_key.upper())) if isinstance(city_block, Mapping) else city_key.upper(),
        slug=str(resolver.get("slug")) if resolver.get("slug") is not None else None,
        resolution_slug=str(resolver.get("resolution")) if resolver.get("resolution") is not None else None,
        resolver=dict(resolver),
    )


def _fetch_markets_page(*, offset: int, limit: int, include_closed: bool) -> list[dict[str, Any]]:
    """Fetch one active Gamma markets page."""

    req = urllib.request.Request(
        (
            "https://gamma-api.polymarket.com/markets"
            f"?limit={int(limit)}&offset={int(offset)}"
            + ("&closed=true" if include_closed else "&active=true&closed=false")
        ),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=25) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    return data if isinstance(data, list) else []


def _fetch_events_by_slug(slug: str) -> list[dict[str, Any]]:
    """Fetch events for one exact slug query."""

    endpoint = f"https://gamma-api.polymarket.com/events?slug={urllib.parse.quote(slug)}"
    req = urllib.request.Request(endpoint, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    return data if isinstance(data, list) else []


def _fetch_events_page(*, offset: int, limit: int) -> list[dict[str, Any]]:
    """Fetch one paginated events page."""

    endpoint = f"https://gamma-api.polymarket.com/events?limit={int(limit)}&offset={int(offset)}"
    req = urllib.request.Request(endpoint, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    return data if isinstance(data, list) else []


def _target_date_for_horizon(horizon: str) -> dt.date:
    """Resolve UTC target date for horizon."""

    today_utc = dt.datetime.now(dt.timezone.utc).date()
    if horizon == "today":
        return today_utc
    return today_utc + dt.timedelta(days=1)


def _parse_target_date(raw_target_date: str | None) -> dt.date | None:
    """Parse optional CLI target date."""

    if raw_target_date is None:
        return None
    text = raw_target_date.strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"Invalid --target-date '{raw_target_date}'. Expected YYYY-MM-DD.") from exc


def _coerce_iso_date(value: Any) -> dt.date | None:
    """Parse a date from Gamma ISO timestamp fields."""

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _extract_event_row(market: Mapping[str, Any]) -> Mapping[str, Any]:
    """Extract first event row from market payload."""

    events = market.get("events")
    if isinstance(events, list):
        for row in events:
            if isinstance(row, Mapping):
                return row

    # Historical rows may not include events[]; fallback to market-level fields.
    return {
        "id": market.get("eventId"),
        "slug": market.get("eventSlug") or market.get("slug"),
        "title": market.get("title") or market.get("question"),
        "description": market.get("description"),
        "endDate": market.get("endDate") or market.get("endDateIso"),
    }


def _normalize_temperature_label(raw_label: str) -> str | None:
    """Normalize label to the internal temperature-bin label convention."""

    text = str(raw_label).strip()
    if not text:
        return None
    lowered = text.lower().replace("°", "")

    if "or higher" in lowered:
        match = re.search(r"(\d{1,3})", lowered)
        if match:
            return f"{match.group(1)}F+"
    if "or lower" in lowered or "or below" in lowered:
        match = re.search(r"(\d{1,3})", lowered)
        if match:
            return f"{match.group(1)}F or lower"

    match = _TEMPERATURE_LABEL_PATTERN.search(text)
    if match:
        low = match.group("low")
        high = match.group("high")
        if high:
            return f"{int(low)}-{int(high)}F"
        return f"{int(low)}F"

    compact = re.sub(r"\s+", " ", text)
    return compact if compact else None


def _extract_market_label(market: Mapping[str, Any]) -> str | None:
    """Extract the bin label from market payload."""

    for key in ("groupItemTitle", "title", "question"):
        value = market.get(key)
        if value is None:
            continue
        normalized = _normalize_temperature_label(str(value))
        if normalized is not None:
            return normalized
    return None


def _extract_event_markets(event_row: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Extract embedded event markets payload."""

    markets = event_row.get("markets")
    if isinstance(markets, list):
        return [item for item in markets if isinstance(item, Mapping)]
    return []


def _rows_from_event_markets(
    event_row: Mapping[str, Any],
    target_date: dt.date,
    rejection_reasons: list[str],
) -> list[dict[str, str]]:
    """Convert one event's markets into normalized metadata rows."""

    event_id = str(event_row.get("id") or "")
    event_slug = str(event_row.get("slug") or "")

    markets = _extract_event_markets(event_row)
    if not markets:
        rejection_reasons.append(f"event '{event_slug or event_id}' has no embedded markets")
        return []

    rows: list[dict[str, str]] = []
    for market in markets:
        label = _extract_market_label(market)
        if label is None:
            rejection_reasons.append(f"market {market.get('id')} rejected: missing temperature label")
            continue

        token_pair = _extract_yes_no_token_pair(market)
        if token_pair is None:
            rejection_reasons.append(f"market {market.get('id')} rejected: missing yes/no token pair")
            continue

        market_yes_probability = _extract_yes_market_probability(market)
        if market_yes_probability is None:
            rejection_reasons.append(f"market {market.get('id')} rejected: missing implied yes probability")
            continue

        end_date = _coerce_iso_date(market.get("endDate") or market.get("endDateIso") or event_row.get("endDate"))
        if end_date is None:
            rejection_reasons.append(f"market {market.get('id')} rejected: missing endDate")
            continue

        if end_date not in {target_date, target_date + dt.timedelta(days=1)}:
            rejection_reasons.append(
                f"market {market.get('id')} rejected: endDate {end_date.isoformat()} not in target window"
            )
            continue

        yes_token_id, no_token_id = token_pair
        rows.append(
            {
                "event_slug": event_slug,
                "event_id": event_id,
                "market_id": str(market.get("id") or ""),
                "range_label": label,
                "yes_token_id": yes_token_id,
                "no_token_id": no_token_id,
                "clobTokenIds": json.dumps([yes_token_id, no_token_id]),
                "market_yes_probability": f"{market_yes_probability:.6f}",
            }
        )

    dedup: dict[str, dict[str, str]] = {}
    for row in rows:
        dedup[row["range_label"]] = row
    return list(dedup.values())


def _extract_yes_market_probability(market: Mapping[str, Any]) -> float | None:
    """Extract market implied YES probability from market payload."""

    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            parsed = json.loads(outcome_prices)
            if isinstance(parsed, list):
                outcome_prices = parsed
        except json.JSONDecodeError:
            outcome_prices = None

    outcomes = market.get("outcomes")
    if isinstance(outcomes, str):
        try:
            parsed = json.loads(outcomes)
            if isinstance(parsed, list):
                outcomes = parsed
        except json.JSONDecodeError:
            outcomes = None

    if isinstance(outcome_prices, list) and isinstance(outcomes, list):
        normalized_outcomes = [str(item).strip().lower() for item in outcomes]
        yes_index = next((i for i, item in enumerate(normalized_outcomes) if item == "yes"), None)
        if yes_index is not None and yes_index < len(outcome_prices):
            try:
                value = float(outcome_prices[yes_index])
                if 0.0 <= value <= 1.0:
                    return value
            except (TypeError, ValueError):
                pass

    for key in ("yesPrice", "bestBid", "lastTradePrice"):
        raw = market.get(key)
        try:
            value = float(raw)
            if 0.0 <= value <= 1.0:
                return value
        except (TypeError, ValueError):
            continue

    return None


def _is_weather_event_for_city(
    *,
    city_key: str,
    market: Mapping[str, Any],
    event_row: Mapping[str, Any],
) -> bool:
    """Check if one market belongs to weather event for city."""

    aliases = _CITY_EVENT_ALIASES.get(city_key, (city_key,))
    haystack = " ".join(
        [
            str(market.get("question", "")),
            str(market.get("groupItemTitle", "")),
            str(event_row.get("title", "")),
            str(event_row.get("slug", "")),
            str(event_row.get("description", "")),
        ]
    ).lower()

    city_match = any(alias in haystack for alias in aliases)
    weather_match = any(hint in haystack for hint in _WEATHER_EVENT_HINTS)
    return city_match and weather_match


def _event_matches_city_and_date(city_key: str, event_row: Mapping[str, Any], target_date: dt.date) -> bool:
    """Loose fallback event matcher by city/weather/date."""

    aliases = _CITY_EVENT_ALIASES.get(city_key, (city_key,))
    haystack = " ".join(
        [
            str(event_row.get("slug", "")),
            str(event_row.get("title", "")),
            str(event_row.get("description", "")),
        ]
    ).lower()

    city_match = any(alias in haystack for alias in aliases)
    weather_match = any(hint in haystack for hint in _WEATHER_EVENT_HINTS) or "highest-temperature" in haystack

    event_end = _coerce_iso_date(event_row.get("endDate"))
    date_match = event_end in {target_date, target_date + dt.timedelta(days=1)} if event_end is not None else False
    return city_match and weather_match and date_match


def resolve_event_markets_for_city_day(
    city_key: str,
    horizon: str,
    gamma_limit: int,
    target_date: dt.date | None = None,
    city_slug_prefix: str | None = None,
    debug_trace: list[str] | None = None,
    rejection_reasons: list[str] | None = None,
) -> list[dict[str, str]]:
    """Resolve exact event markets for one city/day and return label->token metadata."""

    effective_target_date = target_date or _target_date_for_horizon(horizon)
    slug_prefix = _normalize_slug_part(city_slug_prefix or city_key)
    slug_candidates = _build_slug_candidates(slug_prefix, effective_target_date)

    trace = debug_trace if debug_trace is not None else []
    rejects = rejection_reasons if rejection_reasons is not None else []
    trace.append(f"slug_candidates_generated={json.dumps(slug_candidates)}")

    best_exact_rows: list[dict[str, str]] = []
    for slug_candidate in slug_candidates:
        endpoint = f"https://gamma-api.polymarket.com/events?slug={urllib.parse.quote(slug_candidate)}"
        try:
            events = _fetch_events_by_slug(slug_candidate)
        except Exception as exc:  # noqa: BLE001
            trace.append(f"endpoint={endpoint} events_returned=ERROR:{type(exc).__name__}")
            rejects.append(f"slug '{slug_candidate}' rejected: events endpoint error {type(exc).__name__}")
            continue

        trace.append(f"endpoint={endpoint} events_returned={len(events)}")
        exact_matches = [
            row
            for row in events
            if _normalize_slug_part(str(row.get("slug") or "")) == _normalize_slug_part(slug_candidate)
        ]
        if not exact_matches:
            rejects.append(f"slug '{slug_candidate}' rejected: exact event slug not found")
            continue

        for event_row in exact_matches:
            event_slug = str(event_row.get("slug") or "")
            embedded_markets = _extract_event_markets(event_row)
            trace.append(f"event_slug={event_slug} embedded_markets={len(embedded_markets)}")
            rows = _rows_from_event_markets(event_row, effective_target_date, rejects)
            trace.append(f"event_slug={event_slug} usable_bins={len(rows)}")
            if rows and len(rows) > len(best_exact_rows):
                best_exact_rows = rows

    if best_exact_rows:
        return best_exact_rows

    trace.append("exact_stage_failed=true")

    by_event: dict[str, list[dict[str, str]]] = {}
    page_size = 200
    scanned_events = 0

    for offset in range(0, int(gamma_limit), page_size):
        endpoint = f"https://gamma-api.polymarket.com/events?limit={page_size}&offset={offset}"
        try:
            events_page = _fetch_events_page(offset=offset, limit=page_size)
        except Exception as exc:  # noqa: BLE001
            trace.append(f"endpoint={endpoint} events_returned=ERROR:{type(exc).__name__}")
            rejects.append(f"events page offset {offset} rejected: endpoint error {type(exc).__name__}")
            continue

        trace.append(f"endpoint={endpoint} events_returned={len(events_page)}")
        if not events_page:
            break

        for event_row in events_page:
            scanned_events += 1
            if not _event_matches_city_and_date(city_key, event_row, effective_target_date):
                continue

            event_slug = str(event_row.get("slug") or "")
            rows = _rows_from_event_markets(event_row, effective_target_date, rejects)
            trace.append(f"fallback_event_slug={event_slug} usable_bins={len(rows)}")
            if not rows:
                continue

            event_key = str(event_row.get("id") or event_slug)
            by_event[event_key] = rows

    trace.append(f"fallback_scanned_events={scanned_events}")

    if not by_event:
        rejects.append("no event produced usable bins after exact and fallback discovery")
        return []

    best_event_key = max(by_event.keys(), key=lambda key: len(by_event[key]))
    return by_event[best_event_key]


def _label_ladder_score(labels: list[str]) -> tuple[bool, int, bool, bool]:
    """Return ladder-like quality metrics for validity checks."""

    normalized = [_normalize_temperature_label(item) for item in labels]
    valid_labels = [item for item in normalized if item is not None]
    has_high_tail = any(item.endswith("F+") for item in valid_labels)
    has_low_tail = any("or lower" in item.lower() for item in valid_labels)

    middle_ranges = 0
    for item in valid_labels:
        match = re.match(r"^(\d{1,3})-(\d{1,3})F$", item)
        if not match:
            continue
        low = int(match.group(1))
        high = int(match.group(2))
        if high - low == 1:
            middle_ranges += 1

    looks_full = len(valid_labels) >= 8 and middle_ranges >= 6 and (has_high_tail or has_low_tail)
    return looks_full, middle_ranges, has_high_tail, has_low_tail


def _build_discovery_report(
    *,
    city_key: str,
    city_block: Mapping[str, Any],
    horizon: str,
    target_date: dt.date,
    gamma_limit: int,
    enable_debug: bool,
) -> dict[str, Any]:
    """Run strict discovery and return transparent diagnostics."""

    slug_prefix = _city_slug_prefix(city_key, city_block)
    slug_candidates = _build_slug_candidates(slug_prefix, target_date)
    debug_trace: list[str] = []
    rejection_reasons: list[str] = []
    metadata = resolve_event_markets_for_city_day(
        city_key=city_key,
        horizon=horizon,
        gamma_limit=gamma_limit,
        target_date=target_date,
        city_slug_prefix=slug_prefix,
        debug_trace=debug_trace,
        rejection_reasons=rejection_reasons,
    )

    labels = sorted({row["range_label"] for row in metadata})
    looks_full, middle_ranges, has_high_tail, has_low_tail = _label_ladder_score(labels)
    is_valid = len(labels) >= 8 and looks_full

    matched_event_slug = metadata[0].get("event_slug") if metadata else None
    matched_event_id = metadata[0].get("event_id") if metadata else None
    rejection_reason = None if is_valid else (rejection_reasons[0] if rejection_reasons else "insufficient bins")
    return {
        "city": city_key,
        "target_date": target_date.isoformat(),
        "slug_prefix": slug_prefix,
        "expected_slug": slug_candidates[0] if slug_candidates else None,
        "slug_candidates": slug_candidates,
        "matched_event_slug": matched_event_slug,
        "matched_event_id": matched_event_id,
        "bins_found": len(labels),
        "labels_found": labels,
        "ladder_middle_ranges": middle_ranges,
        "has_high_tail": has_high_tail,
        "has_low_tail": has_low_tail,
        "valid": is_valid,
        "rejection_reason": rejection_reason,
        "debug_trace": debug_trace if enable_debug else [],
        "metadata": metadata,
    }


def _print_discovery_report(report: Mapping[str, Any]) -> None:
    """Print compact strict-discovery report."""

    print("\n=== Discovery Report ===")
    print("=" * 72)
    print(f"city={report.get('city')}")
    print(f"target_date={report.get('target_date')}")
    print(f"expected_slug={report.get('expected_slug')}")
    print(f"slug_candidates_tried={json.dumps(report.get('slug_candidates', []))}")
    print(f"matched_event_slug={report.get('matched_event_slug')}")
    print(f"matched_event_id={report.get('matched_event_id')}")
    print(f"bins_found={report.get('bins_found')}")
    print(f"labels_found={json.dumps(report.get('labels_found', []))}")
    print(f"valid={'VALID' if report.get('valid') else 'INVALID'}")
    if not report.get("valid"):
        print(f"rejection_reason={report.get('rejection_reason')}")
    debug_lines = report.get("debug_trace") or []
    if debug_lines:
        print("discovery_trace:")
        for line in debug_lines:
            print(f"- {line}")


def _print_two_city_validation(nyc_report: Mapping[str, Any], atl_report: Mapping[str, Any]) -> bool:
    """Print two-city validity summary and return if both are VALID."""

    nyc_valid = bool(nyc_report.get("valid"))
    atl_valid = bool(atl_report.get("valid"))
    both_valid = nyc_valid and atl_valid

    print("\n=== Two-City Validation ===")
    print("=" * 72)
    print(f"nyc_valid={nyc_valid}")
    print(f"atlanta_valid={atl_valid}")
    print(f"both_valid={both_valid}")
    return both_valid


def _print_overlap_check(
    target_date: dt.date,
    nyc_metadata: list[dict[str, str]],
    atl_metadata: list[dict[str, str]],
) -> None:
    """Print overlap check between NYC and Atlanta markets for one date."""

    nyc_market_ids = {row["market_id"] for row in nyc_metadata if row.get("market_id")}
    atl_market_ids = {row["market_id"] for row in atl_metadata if row.get("market_id")}
    overlapping_market_ids = sorted(nyc_market_ids & atl_market_ids)

    nyc_token_ids = {
        token_id
        for row in nyc_metadata
        for token_id in (row.get("yes_token_id"), row.get("no_token_id"))
        if token_id
    }
    atl_token_ids = {
        token_id
        for row in atl_metadata
        for token_id in (row.get("yes_token_id"), row.get("no_token_id"))
        if token_id
    }
    overlapping_token_ids = sorted(nyc_token_ids & atl_token_ids)

    print("\n=== NYC vs ATL Overlap Check ===")
    print("=" * 72)
    print(f"target_date={target_date.isoformat()}")
    print(f"nyc_bins_found={len(nyc_metadata)} atlanta_bins_found={len(atl_metadata)}")
    print(f"market_id_overlap={bool(overlapping_market_ids)} count={len(overlapping_market_ids)}")
    if overlapping_market_ids:
        print(f"overlapping_market_ids={json.dumps(overlapping_market_ids)}")
    print(f"token_id_overlap={bool(overlapping_token_ids)} count={len(overlapping_token_ids)}")
    if overlapping_token_ids:
        print(f"overlapping_token_ids={json.dumps(overlapping_token_ids)}")


def _print_overlap_from_reports(target_date: dt.date, nyc_report: Mapping[str, Any], atl_report: Mapping[str, Any]) -> None:
    """Compute overlap check from discovery reports metadata."""

    nyc_metadata = nyc_report.get("metadata") or []
    atl_metadata = atl_report.get("metadata") or []
    _print_overlap_check(target_date, nyc_metadata, atl_metadata)


def _extract_yes_no_token_pair(market: Mapping[str, Any]) -> tuple[str, str] | None:
    """Extract (yes_token_id, no_token_id) from one gamma market row."""

    raw_token_ids = market.get("clobTokenIds")
    token_ids: list[str] | None = None
    if isinstance(raw_token_ids, str):
        try:
            parsed = json.loads(raw_token_ids)
            if isinstance(parsed, list):
                token_ids = [str(item) for item in parsed]
        except json.JSONDecodeError:
            token_ids = None
    elif isinstance(raw_token_ids, list):
        token_ids = [str(item) for item in raw_token_ids]

    if not token_ids or len(token_ids) < 2:
        return None

    raw_outcomes = market.get("outcomes")
    outcomes: list[str] | None = None
    if isinstance(raw_outcomes, str):
        try:
            parsed_outcomes = json.loads(raw_outcomes)
            if isinstance(parsed_outcomes, list):
                outcomes = [str(item).strip().lower() for item in parsed_outcomes]
        except json.JSONDecodeError:
            outcomes = None
    elif isinstance(raw_outcomes, list):
        outcomes = [str(item).strip().lower() for item in raw_outcomes]

    if outcomes is not None and len(outcomes) >= 2:
        yes_index = next((i for i, label in enumerate(outcomes) if label == "yes"), None)
        no_index = next((i for i, label in enumerate(outcomes) if label == "no"), None)
        if yes_index is not None and no_index is not None:
            if yes_index < len(token_ids) and no_index < len(token_ids):
                return token_ids[yes_index], token_ids[no_index]

    # Fallback for rows where outcomes are missing: preserve prior behavior.
    return token_ids[0], token_ids[1]


def fetch_quotes_for_metadata(metadata: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    """Fetch live CLOB book for each token id used by metadata."""

    quotes: dict[str, dict[str, Any]] = {}
    token_ids = sorted({row["yes_token_id"] for row in metadata} | {row["no_token_id"] for row in metadata})

    for token_id in token_ids:
        req = urllib.request.Request(
            f"https://clob.polymarket.com/book?token_id={token_id}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                payload = response.read().decode("utf-8")
            data = json.loads(payload)
            if isinstance(data, dict):
                quotes[token_id] = data
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
            # Keep running: evaluator will mark rows as missing quote for this token.
            continue

    return quotes


def _book_top(raw_book: Mapping[str, Any], side: str) -> tuple[float | None, float | None]:
    """Get top-of-book price/size for bids or asks."""

    levels = raw_book.get(side)
    if not isinstance(levels, list):
        return None, None

    best_price: float | None = None
    best_size: float | None = None
    for level in levels:
        if not isinstance(level, Mapping):
            continue
        price = _to_float(level.get("price", level.get("p")))
        size = _to_float(level.get("size", level.get("s", level.get("quantity"))))
        if price is None:
            continue
        if best_price is None:
            best_price = price
            best_size = size
            continue
        if side == "bids" and price > best_price:
            best_price = price
            best_size = size
        if side == "asks" and price < best_price:
            best_price = price
            best_size = size

    return best_price, best_size


def _to_float(value: Any) -> float | None:
    """Convert value to float when possible."""

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _print_side_resolution_debug(
    signal_rows: tuple[Any, ...],
    metadata: list[dict[str, str]],
    quotes: dict[str, dict[str, Any]],
) -> None:
    """Print full side/token/entry diagnostics for 3 bins."""

    mapping_by_label = {row["range_label"]: row for row in metadata}

    print("\n=== BUY_YES / BUY_NO Resolution Debug (3 bins) ===")
    print("=" * 72)

    for candidate in signal_rows[:3]:
        mapping = mapping_by_label.get(candidate.range_label)
        yes_token_id = mapping.get("yes_token_id") if mapping else None
        no_token_id = mapping.get("no_token_id") if mapping else None

        yes_quote = quotes.get(yes_token_id) if yes_token_id else None
        no_quote = quotes.get(no_token_id) if no_token_id else None

        yes_bid, yes_bid_size = _book_top(yes_quote, "bids") if isinstance(yes_quote, Mapping) else (None, None)
        yes_ask, yes_ask_size = _book_top(yes_quote, "asks") if isinstance(yes_quote, Mapping) else (None, None)
        no_bid, no_bid_size = _book_top(no_quote, "bids") if isinstance(no_quote, Mapping) else (None, None)
        no_ask, no_ask_size = _book_top(no_quote, "asks") if isinstance(no_quote, Mapping) else (None, None)

        if candidate.raw_signal_direction == "BUY_YES":
            chosen_entry = yes_ask
            formula = f"edge = model_yes({candidate.model_probability:.4f}) - yes_best_ask({yes_ask})"
            side_prob = candidate.model_probability
        else:
            side_prob = 1.0 - candidate.model_probability
            chosen_entry = no_ask
            formula = f"edge = model_no(1-model_yes={side_prob:.4f}) - no_best_ask({no_ask})"

        edge_value = None if chosen_entry is None else (side_prob - chosen_entry)

        print(f"range_label: {candidate.range_label}")
        print(f"raw_signal_direction: {candidate.raw_signal_direction}")
        print(f"token_ids_used: yes={yes_token_id} no={no_token_id}")
        print(f"interpreted YES token: {yes_token_id}")
        print(f"interpreted NO token:  {no_token_id}")
        print(f"YES book: bid={yes_bid} (size={yes_bid_size}) ask={yes_ask} (size={yes_ask_size})")
        print(f"NO  book: bid={no_bid} (size={no_bid_size}) ask={no_ask} (size={no_ask_size})")
        print(f"entry_price_chosen: {chosen_entry}")
        print(f"executable_edge_formula: {formula}")
        print(f"executable_edge_value: {edge_value}")
        if candidate.raw_signal_direction == "BUY_NO":
            print("BUY_NO validation: entry uses NO best ask, and edge uses model NO probability against NO quote.")
        print("-" * 72)


def _print_event_market_mapping(metadata: list[dict[str, str]]) -> None:
    """Print resolved event-market mapping for every bin."""

    print("\n=== Event Market Mapping (all bins) ===")
    print("=" * 72)
    for row in sorted(metadata, key=lambda item: item["range_label"]):
        print(
            " | ".join(
                [
                    f"event_slug={row.get('event_slug')}",
                    f"event_id={row.get('event_id')}",
                    f"market_id={row.get('market_id')}",
                    f"label={row.get('range_label')}",
                    f"market_yes={row.get('market_yes_probability')}",
                    f"clobTokenIds={row.get('clobTokenIds')}",
                ]
            )
        )


def _print_policy_summary(policy_result: Any) -> None:
    """Print final policy-ranked candidates and suppressed neighbors."""

    print("\n=== Final Policy Ranked Candidates ===")
    print("=" * 72)
    for i, row in enumerate(policy_result.policy_ranked_candidates, 1):
        exec_edge = f"{row.executable_edge:+.4f}" if row.executable_edge is not None else "N/A"
        spread = f"{row.spread:.4f}" if row.spread is not None else "N/A"
        size = f"{row.available_size:.2f}" if row.available_size is not None else "N/A"
        print(
            f"{i}. {row.range_label:14} {row.side:7} state={row.policy_state:15} "
            f"score={row.policy_score:.3f} exec_edge={exec_edge:>8} spread={spread:>7} size={size:>8}"
        )
        print(f"   decision_reason={row.decision_reason}")

    print("\n=== Suppressed Neighbors ===")
    print("=" * 72)
    if not policy_result.suppressed_neighbors:
        print("none")
        return

    for item in policy_result.suppressed_neighbors:
        print(
            f"cluster={item.cluster_id} suppressed={item.suppressed_range_label}/{item.suppressed_side} "
            f"primary={item.primary_range_label}/{item.primary_side} reason={item.reject_reason}"
        )


def main() -> int:
    """Run manual executable-signal evaluation with live quotes."""

    try:
        config = load_config(CONFIG_PATH)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"Failed to load config: {exc}", file=sys.stderr)
        return 1

    cities = config.get("cities", {})
    if not isinstance(cities, Mapping) or not cities:
        print("Config has no cities.", file=sys.stderr)
        return 1

    args = parse_args(default_city=list(cities.keys())[0])
    city_key = str(args.city).strip().lower()
    horizon = str(args.horizon).strip().lower()

    try:
        cli_target_date = _parse_target_date(args.target_date)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if city_key not in cities:
        print(f"Unknown city '{city_key}'. Available: {', '.join(cities.keys())}", file=sys.stderr)
        return 1
    if horizon not in SUPPORTED_HORIZONS:
        print(f"Unsupported horizon '{horizon}'. Use today or tomorrow.", file=sys.stderr)
        return 1

    effective_target_date = cli_target_date or _target_date_for_horizon(horizon)

    requested_report = _build_discovery_report(
        city_key=city_key,
        city_block=cities[city_key],
        horizon=horizon,
        target_date=effective_target_date,
        gamma_limit=int(args.gamma_limit),
        enable_debug=bool(args.discovery_debug or args.discovery_only),
    )
    _print_discovery_report(requested_report)

    nyc_report = _build_discovery_report(
        city_key="nyc",
        city_block=cities.get("nyc", {}),
        horizon=horizon,
        target_date=effective_target_date,
        gamma_limit=int(args.gamma_limit),
        enable_debug=bool(args.discovery_debug),
    )
    atl_report = _build_discovery_report(
        city_key="atlanta",
        city_block=cities.get("atlanta", {}),
        horizon=horizon,
        target_date=effective_target_date,
        gamma_limit=int(args.gamma_limit),
        enable_debug=bool(args.discovery_debug),
    )

    both_valid = _print_two_city_validation(nyc_report, atl_report)
    if both_valid:
        _print_overlap_from_reports(effective_target_date, nyc_report, atl_report)
    else:
        print("\n=== NYC vs ATL Overlap Check ===")
        print("=" * 72)
        print("skipped=true reason=both city events must be VALID")

    if args.discovery_only:
        return 0

    bundle = build_weather_collector(config)
    if not bundle.provider_configs:
        print("No enabled weather providers found in config.")
        return 0

    city = build_city_config(city_key, cities[city_key])
    snapshots = [
        bundle.collector.collect(provider_name=name, city=city, target_day=horizon, provider_config=provider_cfg)
        for name, provider_cfg in bundle.provider_configs.items()
    ]
    snapshots = [s for s in snapshots if not (math.isnan(s.predicted_tmax_f) or "error" in s.raw_payload)]

    if not snapshots:
        print("No successful provider snapshots.")
        return 1

    aggregate = aggregate_forecasts(snapshots)
    distribution = build_temperature_bin_probabilities(aggregate)

    metadata = requested_report.get("metadata") or []
    if not metadata:
        print(
            "No event-specific temperature markets resolved for the requested city/day. "
            "Executable edges are intentionally aborted.",
            file=sys.stderr,
        )
        return 1

    if not requested_report.get("valid"):
        print(
            "Resolved event is not VALID (needs >=8 bins and full ladder-like labels). "
            "Executable edges are intentionally aborted.",
            file=sys.stderr,
        )
        return 1

    _print_event_market_mapping(metadata)

    market_bins = [
        {"label": row["range_label"], "probability": float(row["market_yes_probability"])}
        for row in metadata
    ]
    comparison = compare_market_probabilities(distribution, market_bins)

    signal_filters = SignalCandidateFilters(minimum_absolute_edge=float(args.min_abs_edge))
    signal_result = build_signal_candidates(comparison, signal_filters)

    print("\n=== All Ranked Signal Candidates (real event bins) ===")
    for i, row in enumerate(signal_result.all_ranked_candidates, 1):
        print(
            f"{i}. {row.range_label:14} {row.raw_signal_direction:7} "
            f"edge={row.probability_edge:+.4f} abs_edge={row.abs_edge:.4f} "
            f"model={row.model_probability:.4f} market={row.market_probability:.4f} "
            f"diagnostics={row.diagnostics}"
        )

    event_scoped_candidates = signal_result.all_ranked_candidates
    if not event_scoped_candidates:
        print("No ranked signal candidates after filtering on real event ladder (check minimum_absolute_edge).")
        return 0

    quotes = fetch_quotes_for_metadata(metadata)

    eval_filters = ClobEvaluatorFilters(
        minimum_executable_edge=float(args.min_exec_edge),
        maximum_spread=float(args.max_spread) if args.max_spread is not None else None,
        minimum_available_size=float(args.min_size),
    )

    evaluation = evaluate_executable_signal_candidates(
        event_scoped_candidates,
        metadata,
        quotes,
        eval_filters,
    )

    print(f"city={city_key} target_day={distribution.target_day} target_date={effective_target_date.isoformat()}")
    print(
        f"rows={len(evaluation.rows)} executable={len(evaluation.top_executable_candidates)} "
        f"filtered={len(evaluation.filtered_candidates)}"
    )

    print("\n=== Top 5 Executable Candidates ===")
    for i, row in enumerate(evaluation.top_executable_candidates[:5], 1):
        entry_str = f"{row.entry_price:.4f}" if row.entry_price is not None else "N/A"
        edge_str = f"{row.executable_edge:.4f}" if row.executable_edge is not None else "N/A"
        spread_str = f"{row.spread:.4f}" if row.spread is not None else "N/A"
        size_str = f"{row.available_size:.2f}" if row.available_size is not None else "N/A"
        print(
            f"{i}. {row.range_label:14} side={str(row.side):7} "
            f"entry={entry_str:>6} edge={edge_str:>7} spread={spread_str:>6} size={size_str:>7}"
        )

    print("\n=== Top 5 Filtered Candidates ===")
    for i, row in enumerate(evaluation.filtered_candidates[:5], 1):
        side_str = str(row.side) if row.side else "N/A"
        print(f"{i}. {row.range_label:14} side={side_str:>7} reason={row.reason}")

    print("\n=== Detailed Quote Info (up to 3 bins) ===")
    sample_rows = evaluation.top_executable_candidates[:3]
    if not sample_rows:
        sample_rows = evaluation.rows[:3]
    for row in sample_rows:
        if row.side and row.entry_price is not None:
            entry_p = f"{row.entry_price:.4f}"
            spread_p = f"{row.spread:.4f}" if row.spread is not None else "N/A"
            size_p = f"{row.available_size:.2f}" if row.available_size is not None else "N/A"
            edge_p = f"{row.executable_edge:.4f}" if row.executable_edge is not None else "N/A"
            print(
                f"{row.range_label:14} side={str(row.side):7} "
                f"entry_price={entry_p} spread={spread_p:>6} "
                f"available_size={size_p:>7} executable_edge={edge_p:>7}"
            )

    if args.with_policy:
        by_key: dict[tuple[str, str], tuple[int, Any]] = {}
        for rank, candidate in enumerate(signal_result.all_ranked_candidates, 1):
            by_key[(candidate.range_label, candidate.raw_signal_direction)] = (rank, candidate)

        event_slug = str(metadata[0].get("event_slug", "")) if metadata else ""
        policy_inputs: list[SignalPolicyInputRow] = []
        for row in evaluation.top_executable_candidates:
            if row.side is None:
                continue
            lookup = by_key.get((row.range_label, row.side))
            if lookup is None:
                continue
            rank, signal_row = lookup
            policy_inputs.append(
                SignalPolicyInputRow(
                    city=city_key,
                    target_date=effective_target_date.isoformat(),
                    event_slug=event_slug,
                    range_label=row.range_label,
                    side=row.side,
                    model_probability=signal_row.model_probability,
                    market_probability=signal_row.market_probability,
                    probability_edge=signal_row.probability_edge,
                    abs_edge=signal_row.abs_edge,
                    executable_edge=row.executable_edge,
                    entry_price=row.entry_price,
                    spread=row.spread,
                    available_size=row.available_size,
                    rank=rank,
                )
            )

        policy_result = apply_signal_policy(policy_inputs)
        _print_policy_summary(policy_result)

    if args.debug_side_resolution:
        _print_side_resolution_debug(event_scoped_candidates, metadata, quotes)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
