"""Historical backtest for resolved Polymarket temperature markets.

Scope:
- Resolved markets for nyc/atlanta/chicago/dallas.
- Reconstruct ladder bins and winning bin from resolved outcome prices.
- Compare a simple climatology model vs historical market prices (when available).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.engine.market_ladder import model_probability_for_canonical_bin, parse_temperature_bin_label


CITY_ORDER = ("nyc", "atlanta", "chicago", "dallas")
CITY_SLUG_PREFIX: dict[str, str] = {
    "nyc": "highest-temperature-in-nyc-on",
    "atlanta": "highest-temperature-in-atlanta-on",
    "chicago": "highest-temperature-in-chicago-on",
    "dallas": "highest-temperature-in-dallas-on",
}
CITY_COORDS: dict[str, tuple[float, float]] = {
    "nyc": (40.7128, -74.0060),
    "atlanta": (33.7490, -84.3880),
    "chicago": (41.8781, -87.6298),
    "dallas": (32.7767, -96.7970),
}

TEMP_LABEL_PATTERN = re.compile(r"(?P<low>\d{1,3})(?:\s*[-to]+\s*(?P<high>\d{1,3}))?\s*(?:°)?\s*F", re.IGNORECASE)


@dataclass
class BinRow:
    label: str
    yes_price_resolved: float
    yes_token_id: str | None
    end_dt_utc: dt.datetime
    market_price_hist: float | None
    market_price_hist_ts: int | None


@dataclass
class MarketResult:
    city: str
    event_date: dt.date
    winner_label: str
    model_top_label: str
    model_top_prob: float
    bins_count: int
    model_brier_sum: float
    probability_sum: float
    market_top_label: str | None
    model_edge: float | None
    pnl: float | None
    ladder: list[str]
    model_probabilities: dict[str, float]
    market_prices_used: dict[str, float | None]
    model_top_entry_price: float | None
    model_top_entry_ts: int | None
    model_top_end_ts: int | None
    winner_rank: int
    top_winner_distance: int
    signed_top_winner_distance: int
    raw_center_f: float
    raw_sigma_f: float
    predicted_top_mid_f: float | None
    winner_mid_f: float | None
    signed_error_f: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest resolved Polymarket temperature markets")
    today = dt.datetime.now(dt.timezone.utc).date()
    default_end = today - dt.timedelta(days=1)
    default_start = default_end - dt.timedelta(days=89)
    parser.add_argument("--start-date", default=default_start.isoformat(), help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=default_end.isoformat(), help="YYYY-MM-DD")
    parser.add_argument("--request-timeout", type=int, default=25)
    return parser.parse_args()


def http_json(url: str, timeout: int) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def normalize_slug_part(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return re.sub(r"-+", "-", cleaned).strip("-")


def date_slug_variants(target_date: dt.date) -> tuple[str, ...]:
    month = target_date.strftime("%B").lower()
    day_no_zero = str(target_date.day)
    return (
        target_date.isoformat(),
        target_date.strftime("%Y%m%d"),
        f"{month}-{day_no_zero}-{target_date.year}",
        f"{month}-{target_date.day:02d}-{target_date.year}",
    )


def slug_candidates(prefix: str, target_date: dt.date) -> list[str]:
    out: list[str] = []
    for d in date_slug_variants(target_date):
        out.append(f"{prefix}-{d}")
        out.append(f"{prefix}-on-{d}")
    return out


def parse_temperature_label(raw_label: str) -> str | None:
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

    match = TEMP_LABEL_PATTERN.search(text)
    if match:
        low = int(match.group("low"))
        high = match.group("high")
        if high:
            return f"{low}-{int(high)}F"
        return f"{low}F"

    compact = re.sub(r"\s+", " ", text)
    return compact if compact else None


def ladder_sort_key(label: str) -> tuple[int, int, str]:
    parsed = parse_temperature_bin_label(label)
    if parsed is None:
        return (10**9, 10**9, label)
    low = parsed.low_f if parsed.low_f is not None else -10**9
    high = parsed.high_f if parsed.high_f is not None else 10**9
    return (low, high, label)


def distance_bucket(distance: int) -> str:
    if distance <= 2:
        return str(distance)
    return "3+"


def _is_open_tail_label(label: str) -> bool:
    parsed = parse_temperature_bin_label(label)
    if parsed is None:
        return False
    return parsed.open_left or parsed.open_right


def bin_midpoint_f(label: str, ladder_labels: list[str], warm_open_tails: bool = False) -> float | None:
    target = parse_temperature_bin_label(label)
    if target is None:
        return None

    parsed_ladder = [parse_temperature_bin_label(item) for item in ladder_labels]
    closed_widths = [
        float(item.high_f - item.low_f + 1)
        for item in parsed_ladder
        if item is not None and item.low_f is not None and item.high_f is not None
    ]
    typical_width = statistics.mean(closed_widths) if closed_widths else 2.0

    if target.low_f is not None and target.high_f is not None:
        return (target.low_f + target.high_f) / 2.0
    if target.open_left and target.high_f is not None:
        base_mid = float(target.high_f) - (typical_width / 2.0)
        return base_mid + 0.5 if warm_open_tails else base_mid
    if target.open_right and target.low_f is not None:
        base_mid = float(target.low_f) + (typical_width / 2.0)
        return base_mid + 0.5 if warm_open_tails else base_mid
    return None


def model_distribution_for_labels(labels: list[str], center_f: float, spread_f: float) -> dict[str, float] | None:
    probs: dict[str, float] = {}
    total = 0.0
    for label in labels:
        parsed = parse_temperature_bin_label(label)
        if parsed is None:
            continue
        p = model_probability_for_canonical_bin(center_f, spread_f, parsed)
        p = max(0.0, float(p))
        probs[label] = p
        total += p

    if total <= 0.0:
        return None

    return {key: value / total for key, value in probs.items()}


def remap_warmer_open_tail_probs(labels: list[str], probabilities: dict[str, float]) -> dict[str, float]:
    adjusted = dict(probabilities)
    ordered = sorted(labels, key=ladder_sort_key)
    if not ordered:
        return adjusted

    warm_shift_share = 0.5
    for index, label in enumerate(ordered):
        parsed = parse_temperature_bin_label(label)
        if parsed is None or not parsed.open_left:
            continue
        next_label = ordered[index + 1] if index + 1 < len(ordered) else None
        if next_label is None:
            continue
        mass = adjusted.get(label, 0.0)
        shift = mass * warm_shift_share
        adjusted[label] = mass - shift
        adjusted[next_label] = adjusted.get(next_label, 0.0) + shift

    total = sum(adjusted.values())
    if total > 0:
        adjusted = {key: value / total for key, value in adjusted.items()}
    return adjusted


def evaluate_variant(
    results: list[MarketResult],
    bias_shift_by_city: dict[str, float] | None = None,
    use_warmer_tail_mapping: bool = False,
) -> dict[str, Any]:
    signed_bias_values: list[float] = []
    absolute_error_values: list[float] = []
    top_hits = 0
    brier_sum = 0.0
    total_bins = 0
    pnl_sum = 0.0
    pnl_count = 0
    winner_distance_values: list[float] = []
    bias_by_city: dict[str, list[float]] = {}

    for row in results:
        probs = dict(row.model_probabilities)
        top_mid_warm = False
        if use_warmer_tail_mapping:
            probs = remap_warmer_open_tail_probs(row.ladder, probs)
            top_mid_warm = True
        if bias_shift_by_city is not None:
            city_shift = bias_shift_by_city.get(row.city, 0.0)
            shifted_probs = model_distribution_for_labels(row.ladder, row.raw_center_f + city_shift, row.raw_sigma_f)
            if shifted_probs is not None:
                probs = shifted_probs
                top_mid_warm = False

        if not probs:
            continue

        top_label = max(probs.keys(), key=lambda key: probs[key])
        top_prob = probs[top_label]
        winner_mid = bin_midpoint_f(row.winner_label, row.ladder, warm_open_tails=top_mid_warm)
        top_mid = bin_midpoint_f(top_label, row.ladder, warm_open_tails=top_mid_warm)

        if top_mid is not None and winner_mid is not None:
            signed = top_mid - winner_mid
            signed_bias_values.append(signed)
            absolute_error_values.append(abs(signed))
            winner_distance_values.append(abs(signed))
            bias_by_city.setdefault(row.city, []).append(signed)

        if top_label == row.winner_label:
            top_hits += 1

        for label in row.ladder:
            p = probs.get(label, 0.0)
            y = 1.0 if label == row.winner_label else 0.0
            brier_sum += (p - y) ** 2
            total_bins += 1

        entry_price = row.market_prices_used.get(top_label)
        if entry_price is not None:
            pnl_sum += (1.0 - entry_price) if top_label == row.winner_label else (-entry_price)
            pnl_count += 1

    city_bias_summary = {
        city: (sum(values) / len(values)) for city, values in bias_by_city.items() if values
    }

    return {
        "signed_temperature_bias_f": (sum(signed_bias_values) / len(signed_bias_values)) if signed_bias_values else 0.0,
        "bias_by_city": city_bias_summary,
        "top_bin_accuracy": (top_hits / len(results)) if results else 0.0,
        "brier": (brier_sum / total_bins) if total_bins > 0 else 0.0,
        "avg_winner_distance_f": (sum(absolute_error_values) / len(absolute_error_values)) if absolute_error_values else 0.0,
        "theoretical_pnl": pnl_sum,
        "pnl_tradable_count": pnl_count,
    }


def coerce_utc_dt(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def extract_yes_token_id(market: dict[str, Any]) -> str | None:
    raw_token_ids = market.get("clobTokenIds")
    token_ids: list[str] = []
    if isinstance(raw_token_ids, str):
        try:
            parsed = json.loads(raw_token_ids)
            if isinstance(parsed, list):
                token_ids = [str(x) for x in parsed]
        except json.JSONDecodeError:
            token_ids = []
    elif isinstance(raw_token_ids, list):
        token_ids = [str(x) for x in raw_token_ids]

    if len(token_ids) < 2:
        return None

    outcomes_raw = market.get("outcomes")
    outcomes: list[str] | None = None
    if isinstance(outcomes_raw, str):
        try:
            parsed = json.loads(outcomes_raw)
            if isinstance(parsed, list):
                outcomes = [str(x).strip().lower() for x in parsed]
        except json.JSONDecodeError:
            outcomes = None
    elif isinstance(outcomes_raw, list):
        outcomes = [str(x).strip().lower() for x in outcomes_raw]

    if outcomes is not None:
        yes_idx = next((i for i, o in enumerate(outcomes) if o == "yes"), None)
        if yes_idx is not None and yes_idx < len(token_ids):
            return token_ids[yes_idx]

    return token_ids[0]


def extract_yes_price(market: dict[str, Any]) -> float | None:
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
        labels = [str(o).strip().lower() for o in outcomes]
        yes_idx = next((i for i, o in enumerate(labels) if o == "yes"), None)
        if yes_idx is not None and yes_idx < len(outcome_prices):
            try:
                val = float(outcome_prices[yes_idx])
                if 0.0 <= val <= 1.0:
                    return val
            except (TypeError, ValueError):
                return None

    return None


def fetch_event_by_city_date(city: str, target_date: dt.date, timeout: int) -> dict[str, Any] | None:
    prefix = CITY_SLUG_PREFIX[city]
    candidates = slug_candidates(prefix, target_date)
    for slug in candidates:
        url = f"https://gamma-api.polymarket.com/events?slug={urllib.parse.quote(slug)}"
        try:
            events = http_json(url, timeout)
        except Exception:
            continue
        if not isinstance(events, list):
            continue
        expected = normalize_slug_part(slug)
        for event in events:
            if not isinstance(event, dict):
                continue
            if normalize_slug_part(str(event.get("slug") or "")) == expected:
                return event
    return None


def fetch_market_history_yes_price(token_id: str, end_dt_utc: dt.datetime, timeout: int) -> tuple[float, int] | None:
    # Best publicly available endpoint shape seen in production probes.
    url = f"https://clob.polymarket.com/prices-history?market={urllib.parse.quote(token_id)}&interval=max"
    try:
        payload = http_json(url, timeout)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None
    history = payload.get("history")
    if not isinstance(history, list) or not history:
        return None

    cutoff_ts = int(end_dt_utc.timestamp()) - 1800
    candidates: list[tuple[int, float]] = []
    for point in history:
        if not isinstance(point, dict):
            continue
        try:
            ts = int(point.get("t"))
            price = float(point.get("p"))
        except (TypeError, ValueError):
            continue
        if 0.0 <= price <= 1.0:
            candidates.append((ts, price))

    if not candidates:
        return None

    before_cutoff = [item for item in candidates if item[0] <= cutoff_ts]
    if before_cutoff:
        before_cutoff.sort(key=lambda x: x[0])
        ts, price = before_cutoff[-1]
        return price, ts

    # No pre-resolution data point: skip to avoid any lookahead leakage.
    return None


def fetch_city_climatology(city: str, timeout: int) -> dict[tuple[int, int], list[float]]:
    lat, lon = CITY_COORDS[city]
    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        "&start_date=2016-01-01&end_date=2025-12-31"
        "&daily=temperature_2m_max&timezone=UTC"
    )
    payload = http_json(url, timeout)
    daily = payload.get("daily") if isinstance(payload, dict) else None
    times = daily.get("time") if isinstance(daily, dict) else None
    tmax = daily.get("temperature_2m_max") if isinstance(daily, dict) else None
    if not isinstance(times, list) or not isinstance(tmax, list):
        return {}

    out: dict[tuple[int, int], list[float]] = {}
    for date_str, temp in zip(times, tmax):
        try:
            d = dt.date.fromisoformat(str(date_str))
            v = float(temp)
        except (ValueError, TypeError):
            continue
        out.setdefault((d.month, d.day), []).append((v * 9.0 / 5.0) + 32.0)
    return out


def model_distribution_for_labels(labels: list[str], center_f: float, spread_f: float) -> dict[str, float] | None:
    probs: dict[str, float] = {}
    total = 0.0
    for label in labels:
        parsed = parse_temperature_bin_label(label)
        if parsed is None:
            continue
        p = model_probability_for_canonical_bin(center_f, spread_f, parsed)
        p = max(0.0, float(p))
        probs[label] = p
        total += p

    if total <= 0.0:
        return None
    return {k: v / total for k, v in probs.items()}


def model_distribution_for_bins(
    bins: list[BinRow], climatology_by_md: dict[tuple[int, int], list[float]], event_date: dt.date
) -> tuple[dict[str, float], float, float] | None:
    md_key = (event_date.month, event_date.day)
    samples = climatology_by_md.get(md_key, [])
    if len(samples) < 5:
        return None

    mu = statistics.mean(samples)
    sigma = statistics.pstdev(samples)
    sigma = max(sigma, 3.0)

    probs = model_distribution_for_labels([row.label for row in bins], center_f=mu, spread_f=sigma)
    if probs is None:
        return None
    return probs, mu, sigma


def daterange(start: dt.date, end: dt.date) -> list[dt.date]:
    days: list[dt.date] = []
    current = start
    while current <= end:
        days.append(current)
        current += dt.timedelta(days=1)
    return days


def collect_market_rows(event: dict[str, Any], timeout: int, gaps: Counter[str]) -> list[BinRow]:
    markets = event.get("markets")
    if not isinstance(markets, list):
        gaps["no_markets"] += 1
        return []

    rows: list[BinRow] = []
    for market in markets:
        if not isinstance(market, dict):
            continue

        label = parse_temperature_label(str(market.get("groupItemTitle") or market.get("question") or ""))
        if not label:
            continue

        yes_price = extract_yes_price(market)
        if yes_price is None:
            continue

        end_dt = coerce_utc_dt(market.get("endDate") or market.get("endDateIso") or event.get("endDate"))
        if end_dt is None:
            gaps["missing_end_date"] += 1
            continue

        yes_token = extract_yes_token_id(market)
        if yes_token is None:
            gaps["missing_yes_token"] += 1

        hist_point = fetch_market_history_yes_price(yes_token, end_dt, timeout) if yes_token is not None else None
        hist_price = hist_point[0] if hist_point is not None else None
        hist_ts = hist_point[1] if hist_point is not None else None
        if hist_point is None:
            gaps["missing_price_history"] += 1

        rows.append(
            BinRow(
                label=label,
                yes_price_resolved=yes_price,
                yes_token_id=yes_token,
                end_dt_utc=end_dt,
                market_price_hist=hist_price,
                market_price_hist_ts=hist_ts,
            )
        )

    dedup: dict[str, BinRow] = {}
    for row in rows:
        dedup[row.label] = row
    return list(dedup.values())


def resolve_winner_label(rows: list[BinRow]) -> str | None:
    if not rows:
        return None
    max_price = max(row.yes_price_resolved for row in rows)
    winners = [row.label for row in rows if abs(row.yes_price_resolved - max_price) <= 1e-9]
    if max_price < 0.99 or len(winners) != 1:
        return None
    return winners[0]


def run_backtest(start_date: dt.date, end_date: dt.date, timeout: int) -> tuple[list[MarketResult], Counter[str]]:
    gaps: Counter[str] = Counter()
    results: list[MarketResult] = []

    climatology_cache: dict[str, dict[tuple[int, int], list[float]]] = {}

    for city in CITY_ORDER:
        try:
            climatology_cache[city] = fetch_city_climatology(city, timeout)
        except Exception:
            climatology_cache[city] = {}
            gaps["climatology_fetch_error"] += 1

    for day in daterange(start_date, end_date):
        for city in CITY_ORDER:
            try:
                event = fetch_event_by_city_date(city, day, timeout)
            except Exception:
                event = None
                gaps["event_fetch_error"] += 1

            if event is None:
                gaps["event_not_found"] += 1
                continue

            rows = collect_market_rows(event, timeout, gaps)
            if len(rows) < 6:
                gaps["insufficient_bins"] += 1
                continue

            winner = resolve_winner_label(rows)
            if winner is None:
                gaps["winner_unresolved"] += 1
                continue

            model_out = model_distribution_for_bins(rows, climatology_cache.get(city, {}), day)
            if model_out is None:
                gaps["model_unavailable"] += 1
                continue
            model_probs, raw_center_f, raw_sigma_f = model_out

            if len(model_probs) != len(rows):
                gaps["model_bin_coverage_mismatch"] += 1
                continue

            prob_sum = sum(model_probs.values())
            brier_sum = 0.0
            for row in rows:
                p = model_probs.get(row.label, 0.0)
                y = 1.0 if row.label == winner else 0.0
                brier_sum += (p - y) ** 2

            model_top_label = max(model_probs.keys(), key=lambda k: model_probs[k])
            model_top_prob = model_probs[model_top_label]
            ordered_by_prob = sorted(model_probs.keys(), key=lambda k: (-model_probs[k], k))
            winner_rank = ordered_by_prob.index(winner) + 1 if winner in ordered_by_prob else len(ordered_by_prob) + 1

            with_market = [r for r in rows if r.market_price_hist is not None]
            market_top_label = None
            if with_market:
                market_top_label = max(with_market, key=lambda r: (r.market_price_hist or -1.0)).label

            ladder_labels = sorted((r.label for r in rows), key=ladder_sort_key)
            ladder_pos = {label: idx for idx, label in enumerate(ladder_labels)}
            top_idx = ladder_pos.get(model_top_label, -1)
            win_idx = ladder_pos.get(winner, -1)
            top_winner_distance = abs(top_idx - win_idx) if top_idx >= 0 and win_idx >= 0 else len(ladder_labels)
            signed_top_winner_distance = (top_idx - win_idx) if top_idx >= 0 and win_idx >= 0 else 0
            predicted_top_mid = bin_midpoint_f(model_top_label, ladder_labels)
            winner_mid = bin_midpoint_f(winner, ladder_labels)
            signed_error = (predicted_top_mid - winner_mid) if predicted_top_mid is not None and winner_mid is not None else None

            model_top_row = next((r for r in rows if r.label == model_top_label), None)
            edge = None
            pnl = None
            top_entry_ts = None
            top_end_ts = None
            if model_top_row is not None:
                entry = model_top_row.market_price_hist
                top_entry_ts = model_top_row.market_price_hist_ts
                top_end_ts = int(model_top_row.end_dt_utc.timestamp())
                if entry is not None:
                    edge = model_top_prob - entry
                    if edge > 0:
                        pnl = (1.0 - entry) if model_top_label == winner else (-entry)

            market_prices_used = {r.label: r.market_price_hist for r in rows}

            results.append(
                MarketResult(
                    city=city,
                    event_date=day,
                    winner_label=winner,
                    model_top_label=model_top_label,
                    model_top_prob=model_top_prob,
                    bins_count=len(rows),
                    model_brier_sum=brier_sum,
                    probability_sum=prob_sum,
                    market_top_label=market_top_label,
                    model_edge=edge,
                    pnl=pnl,
                    ladder=ladder_labels,
                    model_probabilities={k: model_probs[k] for k in sorted(model_probs.keys())},
                    market_prices_used=market_prices_used,
                    model_top_entry_price=model_top_row.market_price_hist if model_top_row is not None else None,
                    model_top_entry_ts=top_entry_ts,
                    model_top_end_ts=top_end_ts,
                    winner_rank=winner_rank,
                    top_winner_distance=top_winner_distance,
                    signed_top_winner_distance=signed_top_winner_distance,
                    raw_center_f=raw_center_f,
                    raw_sigma_f=raw_sigma_f,
                    predicted_top_mid_f=predicted_top_mid,
                    winner_mid_f=winner_mid,
                    signed_error_f=signed_error,
                )
            )

            # Small delay to reduce API burst.
            time.sleep(0.02)

    return results, gaps


def main() -> int:
    args = parse_args()
    try:
        start_date = dt.date.fromisoformat(str(args.start_date))
        end_date = dt.date.fromisoformat(str(args.end_date))
    except ValueError:
        print("baseline_metrics: null")
        print("tail_mapping_variant_metrics: null")
        print("city_shift_variant_metrics: null")
        print("best_variant: null")
        print("likely_root_cause: invalid_date_input")
        print("recommended_live_change: null")
        return 1

    if end_date < start_date:
        print("baseline_metrics: null")
        print("tail_mapping_variant_metrics: null")
        print("city_shift_variant_metrics: null")
        print("best_variant: null")
        print("likely_root_cause: end_before_start")
        print("recommended_live_change: null")
        return 1

    results, gaps = run_backtest(start_date, end_date, int(args.request_timeout))

    markets_tested = len(results)
    if markets_tested == 0:
        print("baseline_metrics: null")
        print("tail_mapping_variant_metrics: null")
        print("city_shift_variant_metrics: null")
        print("best_variant: null")
        print("likely_root_cause: no_events")
        print("recommended_live_change: collect_more_history")
        return 0

    baseline_metrics = evaluate_variant(results)
    
    city_shift_by_city = {
        city: -baseline_metrics["bias_by_city"].get(city, 0.0)
        for city in CITY_ORDER
        if city in baseline_metrics["bias_by_city"]
    }
    city_shift_variant_metrics = evaluate_variant(results, bias_shift_by_city=city_shift_by_city)
    
    city_shift_values_f = {city: round(shift, 2) for city, shift in city_shift_by_city.items()}
    
    baseline_abs_bias = abs(baseline_metrics["signed_temperature_bias_f"])
    city_abs_bias = abs(city_shift_variant_metrics["signed_temperature_bias_f"])
    bias_improvement = baseline_abs_bias - city_abs_bias
    accuracy_improvement = city_shift_variant_metrics["top_bin_accuracy"] - baseline_metrics["top_bin_accuracy"]
    brier_improvement = baseline_metrics["brier"] - city_shift_variant_metrics["brier"]
    
    improvement_vs_baseline = {
        "abs_bias_reduction_f": round(bias_improvement, 3),
        "top_bin_accuracy_delta": round(accuracy_improvement, 4),
        "brier_score_delta": round(brier_improvement, 4),
    }
    
    safe_to_shadow = (
        bias_improvement > 0.5 and
        brier_improvement > 0.001 and
        city_shift_variant_metrics["signed_temperature_bias_f"]**2 < baseline_metrics["signed_temperature_bias_f"]**2
    )
    
    rollout_readiness = "ready_for_shadow_mode" if safe_to_shadow else "requires_further_validation"
    
    if safe_to_shadow:
        recommended_shadow_mode_plan = (
            "deploy_per_city_shift_in_shadow_mode_for_7_days_"
            "monitor_bias_and_top_bin_accuracy_"
            "compare_to_baseline_signal_on_live_ladder_if_acceptable_proceed_to_staged_rollout"
        )
    else:
        recommended_shadow_mode_plan = "investigate_city_variance_increase_sample_size_or_rebalance_provider_weights"

    print(f"baseline_metrics: {json.dumps(baseline_metrics, sort_keys=True)}")
    print(f"city_shift_values_f: {json.dumps(city_shift_values_f, sort_keys=True)}")
    print(f"90d_city_shift_variant_metrics: {json.dumps(city_shift_variant_metrics, sort_keys=True)}")
    print(f"improvement_vs_baseline: {json.dumps(improvement_vs_baseline, sort_keys=True)}")
    print(f"rollout_readiness: {rollout_readiness}")
    print(f"recommended_shadow_mode_plan: {recommended_shadow_mode_plan}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
