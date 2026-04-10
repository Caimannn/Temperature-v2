"""Discord control surface for the weather bot."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from datetime import time as dtime
from datetime import timedelta
from datetime import timezone
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from time import time as time_now
from typing import Any
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DISCORD_IMPORT_ERROR: str | None = None
try:
    import discord
    from discord import app_commands
    from discord.ext import commands, tasks
except ModuleNotFoundError as exc:  # pragma: no cover - import guard for scheduler diagnostics
    discord = None  # type: ignore[assignment]
    app_commands = None  # type: ignore[assignment]
    commands = None  # type: ignore[assignment]
    tasks = None  # type: ignore[assignment]
    DISCORD_IMPORT_ERROR = str(exc)


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"
POLICY_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "test_signal_policy.py"
AUDIT_LOG_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "live_signal_runs.csv"
SCHEDULER_RUN_LOG_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "daily_scheduler_runs.csv"
BOT_CONSOLE_LOG_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "bot_live_console.log"
SIGNAL_PLAN_LOG_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "signal_plan_log.csv"
SIGNAL_POLICY_UPDATES_LOG_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "signal_policy_updates.csv"
SIGNAL_RESOLUTION_LOG_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "signal_resolution_log.csv"
SIGNAL_SETTLEMENT_LOG_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "signal_settlement_log.csv"
SIGNAL_PERFORMANCE_SUMMARY_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "signal_policy_performance_summary.csv"
SIGNAL_PERFORMANCE_BREAKDOWN_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "signal_policy_performance_breakdown.csv"
DEFAULT_POLICY_VERSION = "signal_policy_v1"
DEFAULT_SIGNAL_CITIES = ("nyc", "atlanta", "chicago", "dallas")
PANEL_CITY_OPTIONS = ("nyc", "atlanta", "dallas", "chicago")
DAILY_TRIGGER_HOUR_UTC = 12
DAILY_TRIGGER_MINUTE_UTC = 0
PANEL_AUTO_REFRESH_INTERVAL_SECONDS = 30
REPORT_CACHE_TTL_SEC = 120  # 2-minute cache for performance report reads
POLICY_SCRIPT_TIMEOUT_SEC = 120  # 2-minute timeout for policy dry-run subprocess
API_HEALTH_TTL_SEC = 60  # Keep panel API health lightweight with short cache
PEAK_TIMING_TTL_SEC = 600

CITY_COORDS: dict[str, tuple[float, float]] = {
    "nyc": (40.7128, -74.0060),
    "atlanta": (33.7490, -84.3880),
    "chicago": (41.8781, -87.6298),
    "dallas": (32.7767, -96.7970),
}
CITY_TIMEZONES: dict[str, str] = {
    "nyc": "America/New_York",
    "atlanta": "America/New_York",
    "dallas": "America/Chicago",
    "chicago": "America/Chicago",
}
_TEMP_LABEL_PATTERN = re.compile(
    r"(?P<low>\d{1,3})(?:\s*[-to]+\s*(?P<high>\d{1,3}))?\s*(?:°)?\s*F",
    re.IGNORECASE,
)
_MONTH_NAME_TO_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

# Simple in-memory TTL cache for report reads to reduce disk I/O on repeated queries
_REPORT_CACHE: dict[str, tuple[float, str]] = {}
_API_HEALTH_CACHE: dict[str, tuple[float, str, tuple[str, ...]]] = {}
_PEAK_TIMING_CACHE: dict[str, tuple[float, tuple[str, ...]]] = {}
_RUNTIME_REFRESH_TS: dict[str, datetime | None] = {
    "data": None,
    "market": None,
    "weather": None,
    "positions": None,
    "panel": None,
}

# Tail cache for signal updates (recent signals most frequently accessed)
_UPDATE_TAIL_CACHE: dict[str, list[dict[str, Any]]] = {}
_UPDATE_CACHE_TTL_SEC = 300  # 5 minutes per-signal cache


def _get_cached_report_text() -> str | None:
    """Return cached performance report text if TTL not expired, else None."""
    if "report" not in _REPORT_CACHE:
        return None
    cached_ts, cached_text = _REPORT_CACHE["report"]
    if time_now() - cached_ts > REPORT_CACHE_TTL_SEC:
        return None
    return cached_text


def _set_cached_report_text(text: str) -> None:
    """Cache performance report text with current timestamp."""
    _REPORT_CACHE["report"] = (time_now(), text)


def _utc_now() -> datetime:
    """Return current timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


def _last_sunday_utc(year: int, month: int) -> datetime:
    """Return last Sunday of month at 01:00 UTC (EU DST transition basis)."""

    day = datetime(year, month, 31, 1, 0, 0, tzinfo=timezone.utc)
    while day.weekday() != 6:  # Sunday
        day -= timedelta(days=1)
    return day


def _is_europe_rome_dst(ts_utc: datetime) -> bool:
    """Return True if Europe/Rome should be in DST for given UTC timestamp."""

    year = ts_utc.year
    dst_start = _last_sunday_utc(year, 3)
    dst_end = _last_sunday_utc(year, 10)
    return dst_start <= ts_utc < dst_end


def _rome_tz(ts_utc: datetime | None = None) -> timezone | ZoneInfo:
    """Return Europe/Rome timezone with DST-correct fallback if zoneinfo DB is unavailable."""

    try:
        return ZoneInfo("Europe/Rome")
    except ZoneInfoNotFoundError:
        base_utc = ts_utc or _utc_now()
        if base_utc.tzinfo is None:
            base_utc = base_utc.replace(tzinfo=timezone.utc)
        offset_hours = 2 if _is_europe_rome_dst(base_utc.astimezone(timezone.utc)) else 1
        label = "CEST" if offset_hours == 2 else "CET"
        return timezone(timedelta(hours=offset_hours), name=label)


def _format_rome_clock(ts_utc: datetime) -> str:
    """Format UTC timestamp as HH:MM:SS Europe/Rome."""

    base_utc = ts_utc
    if base_utc.tzinfo is None:
        base_utc = base_utc.replace(tzinfo=timezone.utc)
    return base_utc.astimezone(_rome_tz(base_utc)).strftime("%H:%M:%S Europe/Rome")


def _mark_runtime_refresh(kind: str, ts_utc: datetime | None = None) -> None:
    """Track runtime refresh timestamps for observability fields."""

    if kind in _RUNTIME_REFRESH_TS:
        _RUNTIME_REFRESH_TS[kind] = ts_utc or _utc_now()


def _runtime_age_seconds(kind: str, now_utc: datetime | None = None) -> int | None:
    """Return refresh age in seconds for tracked source."""

    last = _RUNTIME_REFRESH_TS.get(kind)
    if last is None:
        return None
    now = now_utc or _utc_now()
    return max(0, int((now - last).total_seconds()))


def _format_age_text(age_seconds: int | None) -> str:
    """Render age text with fallback."""

    return "n/a" if age_seconds is None else f"{age_seconds}s"


def _format_eta(delta_seconds: int) -> str:
    """Format delta seconds as compact hours/minutes string."""

    abs_seconds = abs(delta_seconds)
    hours = abs_seconds // 3600
    minutes = (abs_seconds % 3600) // 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _collect_peak_timing_lines(force_refresh: bool = False) -> list[str]:
    """Collect per-city peak ETA and peak clock in Europe/Rome."""

    cached = _PEAK_TIMING_CACHE.get("cities")
    if not force_refresh and cached is not None:
        cached_ts, cached_lines = cached
        if time_now() - cached_ts <= PEAK_TIMING_TTL_SEC:
            return list(cached_lines)

    now_utc = _utc_now()
    lines: list[str] = []
    for city in PANEL_CITY_OPTIONS:
        coords = CITY_COORDS.get(city)
        city_tz_name = CITY_TIMEZONES.get(city)
        if coords is None or city_tz_name is None:
            lines.append(f"{city.upper()}: Peak ETA n/a | Peak at n/a")
            continue

        try:
            city_tz = ZoneInfo(city_tz_name)
        except ZoneInfoNotFoundError:
            lines.append(f"{city.upper()}: Peak ETA n/a | Peak at n/a")
            continue

        lat, lon = coords
        endpoint = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            "&hourly=temperature_2m"
            "&forecast_days=2"
            "&timezone=UTC"
        )
        try:
            payload = _http_json(endpoint, timeout=20)
            hourly = payload.get("hourly") if isinstance(payload, dict) else None
            times = hourly.get("time") if isinstance(hourly, dict) else None
            temps = hourly.get("temperature_2m") if isinstance(hourly, dict) else None
            if not isinstance(times, list) or not isinstance(temps, list) or not times:
                raise ValueError("missing_hourly_data")

            now_city = now_utc.astimezone(city_tz)
            city_day = now_city.date()
            peak_utc: datetime | None = None
            peak_temp = float("-inf")
            for idx, raw_time in enumerate(times):
                if idx >= len(temps):
                    break
                try:
                    ts_utc = datetime.fromisoformat(str(raw_time)).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                ts_city = ts_utc.astimezone(city_tz)
                if ts_city.date() != city_day:
                    continue
                temp_value = _safe_float(temps[idx], default=float("nan"))
                if temp_value != temp_value:
                    continue
                if temp_value > peak_temp:
                    peak_temp = temp_value
                    peak_utc = ts_utc

            if peak_utc is None:
                lines.append(f"{city.upper()}: Peak ETA n/a | Peak at n/a")
                continue

            delta_seconds = int((peak_utc - now_utc).total_seconds())
            peak_rome = peak_utc.astimezone(_rome_tz()).strftime("%H:%M")
            if delta_seconds >= 0:
                eta_text = f"Peak ETA: {_format_eta(delta_seconds)}"
            else:
                eta_text = f"Peak passed {_format_eta(delta_seconds)} ago"
            lines.append(f"{city.upper()}: {eta_text} | Peak at: {peak_rome} Europe/Rome")
        except Exception as exc:
            lines.append(f"{city.upper()}: Peak ETA n/a | Peak at n/a ({type(exc).__name__})")

    _PEAK_TIMING_CACHE["cities"] = (time_now(), tuple(lines))
    return lines


def _next_daily_trigger_utc(now_utc: datetime | None = None) -> datetime:
    """Return the next planned daily trigger timestamp in UTC."""

    now = now_utc or _utc_now()
    today_trigger = now.replace(
        hour=DAILY_TRIGGER_HOUR_UTC,
        minute=DAILY_TRIGGER_MINUTE_UTC,
        second=0,
        microsecond=0,
    )
    if now < today_trigger:
        return today_trigger
    return today_trigger + timedelta(days=1)


@dataclass
class DryRunCityBlock:
    """One parsed dry-run output block for one city."""

    city: str
    discord_preview: str
    source_candidate_count: int
    primary_candidate: str
    signal_meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalSnapshot:
    """Structured signal snapshot used for advisory ledgers and Discord output."""

    signal_id: str
    ts_signal: str
    city: str
    target_date: str
    market_slug: str
    token_id: str
    market_label: str
    side: str
    entry_price: float
    model_prob_initial: float
    executable_edge_initial: float
    spread: float
    calibration_state: str
    take_profit_initial: float
    stop_loss_initial: float
    policy_version: str
    market_price_current: float
    model_prob_current: float
    executable_edge_current: float
    take_profit_current: float
    stop_loss_current: float
    status: str
    policy_status_reason: str
    reason: str
    yes_token_id: str = ""
    no_token_id: str = ""
    resolved_token_id: str = ""
    book_best_bid: float | None = None
    book_best_ask: float | None = None


@dataclass
class SignalResolutionRow:
    """One append-only final policy-resolution row for a signal."""

    signal_id: str
    ts_signal: str
    ts_resolution: str
    city: str
    target_date: str
    market_slug: str
    token_id: str
    side: str
    entry_price: float
    exit_price_policy: float
    exit_reason: str
    final_status: str
    resolved_outcome: str
    pnl_if_followed_policy: float
    pnl_pct_if_followed_policy: float
    resolution_source: str
    policy_version: str


@dataclass
class SignalSettlementRow:
    """One append-only row that joins policy resolution with real market settlement."""

    signal_id: str
    city: str
    target_date: str
    market_slug: str
    token_id: str
    side: str
    ts_signal: str
    ts_resolution_policy: str
    ts_settlement_real: str
    entry_price: float
    exit_price_policy: float
    final_status_policy: str
    pnl_if_followed_policy: float
    actual_market_outcome: str
    winning_bin_label: str
    actual_tmax_f: float
    pnl_if_held_to_resolution: float
    pnl_pct_if_held_to_resolution: float
    settlement_source: str
    policy_version: str


@dataclass
class DryRunSummary:
    """Parsed summary footer from multi-city dry-run output."""

    total_cities: int
    cities_with_signal: int
    top_signal_city: str
    top_signal_candidate: str


@dataclass
class PanelState:
    """In-memory panel state for the Discord control surface."""

    cities: list[str]
    horizons: list[str]
    active_city_index: int = 0
    active_horizon_index: int = 0
    mode: str = "manual-only"
    kill_switch: bool = False

    @property
    def active_city(self) -> str:
        """Return the current city key."""

        return self.cities[self.active_city_index]

    @property
    def active_horizon(self) -> str:
        """Return the current horizon."""

        return self.horizons[self.active_horizon_index]


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load the JSON-compatible config file used by the bot."""

    return json.loads(path.read_text(encoding="utf-8"))


def build_panel_text(state: PanelState, config: dict[str, Any]) -> str:
    """Render the text-only control panel."""

    city_block = config.get("cities", {}).get(state.active_city, {})
    resolver_type = city_block.get("resolver", {}).get("type", "placeholder")
    mode_label = "AUTO+MANUAL" if state.mode == "auto+manual" else "MANUAL-ONLY"
    kill_label = "ON" if state.kill_switch else "OFF"
    lines = [
        "Weather Bot Control Panel",
        f"City: {state.active_city.upper()} | Horizon: {state.active_horizon.upper()}",
        f"Mode: {mode_label} | Kill Switch: {kill_label}",
        f"Resolver: {resolver_type}",
        "Actions: HOLD | CLOSE | ADD | BUY NEW | SWITCH",
    ]
    return "\n".join(lines)


def build_panel_embed(
    state: PanelState,
    config: dict[str, Any],
    status_text: str | None = None,
    api_health_text: str | None = None,
    refresh_ts: str | None = None,
    day_setups: dict[str, list[SignalSnapshot]] | None = None,
    provider_lines: list[str] | None = None,
    wallet_target: str | None = None,
    last_error: str | None = None,
    data_age_seconds: int | None = None,
    market_age_seconds: int | None = None,
    weather_age_seconds: int | None = None,
    positions_age_seconds: int | None = None,
    peak_lines: list[str] | None = None,
    panel_loop_interval_seconds: int | None = None,
) -> "discord.Embed":
    """Render single-message operational console panel using old UX layout."""

    _ = config  # Keep signature stable for existing call sites.
    scheduler = status_text or "Scheduler: unknown"
    api_health = api_health_text or "api=unknown"
    refreshed_utc = refresh_ts or _utc_now().replace(microsecond=0).isoformat()
    try:
        refreshed_dt = datetime.fromisoformat(refreshed_utc)
        if refreshed_dt.tzinfo is None:
            refreshed_dt = refreshed_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        refreshed_dt = _utc_now()
    refreshed_rome = _format_rome_clock(refreshed_dt)
    wallet_label = wallet_target or "missing"
    day_map = day_setups or {"today": [], "tomorrow": []}

    embed = discord.Embed(title=f"Weather Bot Console | {state.active_city.upper()}", color=0x2B6CB0)
    embed.add_field(
        name="Console Header",
        value=(
            "Timezone display: Europe/Rome\n"
            f"Last refresh: {refreshed_rome}\n"
            f"Data age: {_format_age_text(data_age_seconds)} | Market age: {_format_age_text(market_age_seconds)}\n"
            f"Weather age: {_format_age_text(weather_age_seconds)} | Positions age: {_format_age_text(positions_age_seconds)}\n"
            f"Panel auto-refresh interval: {panel_loop_interval_seconds or PANEL_AUTO_REFRESH_INTERVAL_SECONDS}s\n"
            f"Status: {scheduler}\n"
            f"API: {api_health}\n"
            f"Wallet target: {wallet_label}"
        )[:1024],
        inline=False,
    )
    embed.add_field(name="TODAY", value=_build_day_block_text("today", day_map.get("today", []))[:1024], inline=False)
    embed.add_field(name="TOMORROW", value=_build_day_block_text("tomorrow", day_map.get("tomorrow", []))[:1024], inline=False)
    if provider_lines:
        embed.add_field(name="Provider Status", value="\n".join(provider_lines)[:1024], inline=False)
    if peak_lines:
        embed.add_field(name="Peak Timing (Europe/Rome)", value="\n".join(peak_lines)[:1024], inline=False)
    if last_error:
        embed.add_field(name="Last Error", value=last_error[:1024], inline=False)
    embed.add_field(
        name="Ops Status",
        value=(
            "Actions: Refresh | Move Bottom | Trade Setup | Check Positions | API Health | Model Wants Open"
            f" | Loop: {panel_loop_interval_seconds or PANEL_AUTO_REFRESH_INTERVAL_SECONDS}s"
        )[:1024],
        inline=False,
    )
    embed.set_footer(text="Advisory-only: no automatic trading execution")
    return embed


def _infer_city_from_text(text: str) -> str:
    """Infer city key from free text for weather-market filtering."""

    lowered = text.lower()
    if "new york" in lowered or "nyc" in lowered or "central park" in lowered:
        return "nyc"
    for city in PANEL_CITY_OPTIONS:
        if city in lowered:
            return city
    return "unknown"


def _edge_confidence_label(edge_value: float) -> str:
    """Map absolute edge to a compact confidence label."""

    edge_abs = abs(edge_value)
    if edge_abs >= 0.08:
        return "HIGH"
    if edge_abs >= 0.04:
        return "MED"
    return "LOW"


async def _collect_trade_setup_snapshots(cities: list[str]) -> list[SignalSnapshot]:
    """Run dry-run for cities and return executable snapshots sorted by edge desc."""

    blocks, _summary = await run_policy_dry_run(cities)
    snapshots: list[SignalSnapshot] = []
    for block in blocks:
        if block.primary_candidate == "NONE":
            continue
        try:
            snapshots.append(_build_signal_snapshot(block))
        except Exception as exc:
            _log_panel_event("trade_setup_snapshot_error", f"city={block.city} error={type(exc).__name__}")
    snapshots.sort(key=lambda item: item.executable_edge_current, reverse=True)
    return snapshots


def _build_trade_setup_text(city: str, setups: list[SignalSnapshot]) -> str:
    """Render compact trade setup view for one city."""

    if not setups:
        return f"Trade Setup [{city.upper()}]\nNo executable setup now"

    lines = [
        f"Trade Setup [{city.upper()}]",
        "rank | signal side | target day | market ask live | model fair probability | edge | TP/SL/RR | reason",
    ]
    for rank, setup in enumerate(setups, start=1):
        edge_pct = setup.executable_edge_current * 100.0
        short_note = (setup.reason or "n/a")[:40]
        rr = _format_rr(setup.market_price_current, setup.take_profit_current, setup.stop_loss_current)
        lines.append(
            (
                f"{rank}. {setup.side} | {setup.target_date} | "
                f"ask={setup.market_price_current:.3f} | fair={setup.model_prob_current:.3f} | {edge_pct:+.2f}% | "
                f"{setup.take_profit_current:.3f}/{setup.stop_loss_current:.3f}/{rr} | {short_note}"
            )
        )
        if rank >= 6:
            break
    return "\n".join(lines)


def _format_rr(entry_price: float, take_profit: float, stop_loss: float) -> str:
    """Return compact risk/reward text for panel output."""

    risk = max(entry_price - stop_loss, 0.0)
    reward = max(take_profit - entry_price, 0.0)
    if risk <= 0.0:
        return "n/a"
    return f"{(reward / risk):.2f}"


def _log_panel_top_signal_mapping(horizon: str, setup: SignalSnapshot) -> None:
    """Log side/token/price diagnostics for current panel top signal only."""

    side_token = _normalize_side_token(setup.side)
    expected_token_id = setup.no_token_id if side_token == "NO" else setup.yes_token_id
    token_match = "n/a"
    if expected_token_id:
        token_match = "yes" if setup.resolved_token_id == expected_token_id else "no"

    _log_panel_event(
        "panel_side_token_diag",
        (
            f"city={setup.city.lower()} target_day={horizon} slug={setup.market_slug} "
            f"range={setup.market_label} row.side={setup.side} tokenid={setup.resolved_token_id or 'n/a'} "
            f"yes_token_id={setup.yes_token_id or 'n/a'} no_token_id={setup.no_token_id or 'n/a'} "
            f"book_bid={('n/a' if setup.book_best_bid is None else f'{setup.book_best_bid:.3f}')} "
            f"book_ask={('n/a' if setup.book_best_ask is None else f'{setup.book_best_ask:.3f}')} "
            f"panel_price={setup.market_price_current:.3f} token_matches_side={token_match}"
        ),
    )


def _build_day_block_text(horizon: str, setups: list[SignalSnapshot]) -> str:
    """Render old-panel style day block for TODAY/TOMORROW using v2 snapshots."""

    day = horizon.upper()
    if not setups:
        return (
            f"{day}\n"
            "date: n/a\n"
            "obs: n/a | fcst: n/a\n"
            "range/bin: n/a\n"
            "signal side: NO TRADE\n"
            "target day: n/a\n"
            "market ask live: n/a\n"
            "model fair probability: n/a\n"
            "edge: n/a\n"
            "TP / SL / RR: n/a\n"
            "reason: no executable setup"
        )

    primary = setups[0]
    _log_panel_top_signal_mapping(horizon, primary)
    side_token = _normalize_side_token(primary.side)
    if side_token == "YES":
        trade = "BUY YES"
    elif side_token == "NO":
        trade = "BUY NO"
    else:
        trade = "NO TRADE"

    lines = [
        f"{day}",
        f"date: {primary.target_date or 'n/a'}",
        "obs: n/a | fcst: n/a",
        f"range/bin: {primary.market_label}",
        f"signal side: {trade}",
        f"target day: {horizon}",
        f"market ask live: {primary.market_price_current:.3f}",
        f"model fair probability (policy): {primary.model_prob_current:.3f}",
        f"edge: {primary.executable_edge_current * 100.0:+.2f}%",
        (
            f"TP / SL / RR: {primary.take_profit_current:.3f} / "
            f"{primary.stop_loss_current:.3f} / "
            f"{_format_rr(primary.market_price_current, primary.take_profit_current, primary.stop_loss_current)}"
        ),
        f"reason: {(primary.reason or 'n/a')[:80]}",
    ]

    if len(setups) > 1:
        others: list[str] = []
        for item in setups[1:3]:
            others.append(
                (
                    f"{item.market_label[:24]} {item.side} "
                    f"edge={item.executable_edge_current * 100.0:+.2f}%"
                )
            )
        lines.append(f"other candidates: {' | '.join(others)}")

    return "\n".join(lines)


def _probe_http_endpoint(url: str, timeout: int = 5) -> tuple[str, str]:
    """Probe endpoint with latency and compact status string."""

    started = time_now()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status_code = getattr(resp, "status", 200)
        latency_ms = int((time_now() - started) * 1000)
        if 200 <= int(status_code) < 300:
            return "OK", f"{latency_ms}ms"
        return "DEGRADED", f"http={status_code} {latency_ms}ms"
    except Exception as exc:
        latency_ms = int((time_now() - started) * 1000)
        return "FAIL", f"{type(exc).__name__} {latency_ms}ms"


def _collect_api_health(config: dict[str, Any], force_refresh: bool = False) -> tuple[str, list[str]]:
    """Collect compact API health status for panel and detailed button output."""

    cached = _API_HEALTH_CACHE.get("health")
    if not force_refresh and cached is not None:
        ts_cached, summary_cached, details_cached = cached
        if time_now() - ts_cached <= API_HEALTH_TTL_SEC:
            return summary_cached, list(details_cached)

    details: list[str] = []
    poly_status, poly_detail = _probe_http_endpoint("https://clob.polymarket.com/")
    gamma_status, gamma_detail = _probe_http_endpoint("https://gamma-api.polymarket.com/events?limit=1")
    details.append(f"Polymarket Data: {poly_status} ({poly_detail})")
    details.append(f"Gamma Metadata: {gamma_status} ({gamma_detail})")

    providers = config.get("weather_provider", {}).get("providers", {})
    weather_lines: list[str] = []
    weather_ok = 0
    weather_total = 0

    def _provider_line(name: str, status: str, detail: str) -> None:
        nonlocal weather_ok, weather_total
        weather_total += 1
        if status == "OK":
            weather_ok += 1
        weather_lines.append(f"{name}: {status} ({detail})")

    if isinstance(providers, dict):
        if bool(providers.get("openweather", {}).get("enabled")):
            env_name = str(providers.get("openweather", {}).get("api_key_env", "OPENWEATHER_API_KEY"))
            key = os.getenv(env_name, "").strip()
            if not key:
                _provider_line("OpenWeather", "FAIL", f"missing {env_name}")
            else:
                st, dtl = _probe_http_endpoint(
                    (
                        "https://api.openweathermap.org/data/2.5/weather"
                        f"?lat=40.7128&lon=-74.0060&units=imperial&appid={urllib.parse.quote(key)}"
                    )
                )
                _provider_line("OpenWeather", st, dtl)
        if bool(providers.get("weatherapi", {}).get("enabled")):
            env_name = str(providers.get("weatherapi", {}).get("api_key_env", "WEATHERAPI_KEY"))
            key = os.getenv(env_name, "").strip()
            if not key:
                _provider_line("WeatherAPI", "FAIL", f"missing {env_name}")
            else:
                st, dtl = _probe_http_endpoint(
                    (
                        "http://api.weatherapi.com/v1/current.json"
                        f"?key={urllib.parse.quote(key)}&q=New%20York"
                    )
                )
                _provider_line("WeatherAPI", st, dtl)
        if bool(providers.get("tomorrow", {}).get("enabled")):
            env_name = str(providers.get("tomorrow", {}).get("api_key_env", "TOMORROW_API_KEY"))
            key = os.getenv(env_name, "").strip()
            if not key:
                _provider_line("Tomorrow", "FAIL", f"missing {env_name}")
            else:
                st, dtl = _probe_http_endpoint(
                    (
                        "https://api.tomorrow.io/v4/weather/realtime"
                        f"?location=40.7128,-74.0060&apikey={urllib.parse.quote(key)}"
                    )
                )
                _provider_line("Tomorrow", st, dtl)

    details.extend(weather_lines)
    summary = f"poly={poly_status} gamma={gamma_status} weather={weather_ok}/{weather_total}"
    _API_HEALTH_CACHE["health"] = (time_now(), summary, tuple(details))
    return summary, details


def _resolve_positions_wallet_target(config: dict[str, Any]) -> tuple[str | None, str]:
    """Resolve wallet target from env first, then config polymarket.user_address."""

    env_wallet = os.getenv("POLY_USER_ADDRESS", "").strip()
    if env_wallet:
        return env_wallet, "POLY_USER_ADDRESS"

    poly_cfg = config.get("polymarket") if isinstance(config.get("polymarket"), dict) else {}
    cfg_wallet = str(poly_cfg.get("user_address", "")).strip() if isinstance(poly_cfg, dict) else ""
    if cfg_wallet:
        return cfg_wallet, "config.polymarket.user_address"

    legacy_wallet = os.getenv("POLYMARKET_WALLET_ADDRESS", "").strip()
    if legacy_wallet:
        return legacy_wallet, "POLYMARKET_WALLET_ADDRESS"

    return None, "missing"


def _resolve_positions_endpoint(config: dict[str, Any]) -> tuple[str | None, str]:
    """Resolve positions endpoint with env -> config -> default fallback chain."""

    env_endpoint = os.getenv("POLYMARKET_POSITIONS_API_URL", "").strip()
    if env_endpoint:
        return env_endpoint, "POLYMARKET_POSITIONS_API_URL"

    poly_cfg = config.get("polymarket") if isinstance(config.get("polymarket"), dict) else {}
    cfg_endpoint = str(poly_cfg.get("data_api_base", "")).strip() if isinstance(poly_cfg, dict) else ""
    if cfg_endpoint:
        return cfg_endpoint, "config.polymarket.data_api_base"

    return "https://data-api.polymarket.com", "default"


def _extract_positions_rows(payload: Any) -> list[dict[str, Any]]:
    """Extract position rows from generic API payload shapes."""

    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("positions", "data", "results", "items"):
        maybe = payload.get(key)
        if isinstance(maybe, list):
            return [row for row in maybe if isinstance(row, dict)]
    return []


def _normalize_side_token(side: str) -> str:
    """Normalize side values into stable YES/NO/UNKNOWN tokens."""

    raw = str(side).strip().upper()
    if raw in {"YES", "BUY_YES"}:
        return "YES"
    if raw in {"NO", "BUY_NO"}:
        return "NO"
    return raw or "UNKNOWN"


def _fetch_open_weather_positions(config: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None, str | None]:
    """Fetch and filter open weather positions from configured Polymarket positions API."""

    _mark_runtime_refresh("positions")
    endpoint, endpoint_source = _resolve_positions_endpoint(config)
    wallet, _wallet_source = _resolve_positions_wallet_target(config)
    if not wallet:
        return [], "wallet missing (POLY_USER_ADDRESS or config polymarket.user_address)", None
    if not endpoint:
        return [], "endpoint not resolved", wallet

    # Normalize base endpoint fallback into a concrete positions resource.
    lowered = endpoint.lower()
    if "positions" not in lowered:
        endpoint = endpoint.rstrip("/") + "/positions"

    if "{wallet}" in endpoint:
        endpoint = endpoint.format(wallet=urllib.parse.quote(wallet))
    elif "user=" not in endpoint and "address=" not in endpoint and "wallet=" not in endpoint:
        sep = "&" if "?" in endpoint else "?"
        endpoint = f"{endpoint}{sep}user={urllib.parse.quote(wallet)}"

    try:
        req = urllib.request.Request(
            endpoint,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw_body = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw_body)
    except urllib.error.HTTPError as exc:
        status = int(getattr(exc, "code", 0) or 0)
        try:
            body_text = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body_text = ""
        body_snippet = " ".join(body_text.strip().split())[:180] or "(empty)"
        if status == 400:
            category = "bad request"
        elif status in {401, 403}:
            category = "unauthorized"
        elif status == 404:
            category = "not found"
        elif status == 429:
            category = "rate limit"
        elif status >= 500:
            category = "server error"
        else:
            category = "http failure"
        return [], (
            f"{category} (HTTP {status}) "
            f"url={endpoint} body={body_snippet}"
        ), wallet
    except json.JSONDecodeError as exc:
        return [], (
            "response parse failure "
            f"url={endpoint} error={type(exc).__name__}"
        ), wallet
    except Exception as exc:
        return [], (
            f"HTTP/API failure ({type(exc).__name__}) "
            f"url={endpoint} source={endpoint_source}"
        ), wallet

    rows = _extract_positions_rows(payload)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        market_text = " ".join(
            [
                str(row.get("market_slug", "")),
                str(row.get("slug", "")),
                str(row.get("market", "")),
                str(row.get("title", "")),
                str(row.get("question", "")),
                str(row.get("outcome", "")),
            ]
        ).lower()
        city = _infer_city_from_text(market_text)
        if city not in PANEL_CITY_OPTIONS:
            continue
        if "temperature" not in market_text and "weather" not in market_text:
            continue
        size = _safe_float(row.get("size") or row.get("amount") or row.get("position_size"), default=0.0)
        if size <= 0.0:
            continue
        filtered.append(row)

    return filtered, None, wallet


def _position_setup_key(slug: str, side: str) -> str:
    """Build stable key to align positions with model setup snapshots."""

    return f"{_normalize_slug_part(slug)}::{_normalize_side_token(side)}"


def _extract_position_side(row: dict[str, Any]) -> str:
    """Extract compact side/outcome from positions payload."""

    side_raw = str(row.get("side") or row.get("outcome") or row.get("position_side") or "")
    return _normalize_side_token(side_raw)


def _recommend_position_action(current_price: float, fair_value: float, take_profit: float, stop_loss: float) -> tuple[str, str]:
    """Generate compact position recommendation using already-computed model guardrails."""

    if current_price <= 0.0 or fair_value <= 0.0:
        return "WAIT", "insufficient pricing inputs"
    if current_price >= take_profit > 0.0:
        return "TAKE PROFIT", "price reached take-profit"
    if current_price <= stop_loss and stop_loss > 0.0:
        return "STOP LOSS", "price breached stop-loss"
    if current_price > fair_value + 0.03:
        return "REDUCE", "price above fair value"
    if current_price < fair_value - 0.03:
        return "HOLD", "edge still positive"
    return "WAIT", "near fair value"


def build_status_text(kind: str, state: PanelState) -> str:
    """Return placeholder text-only status responses."""

    if kind == "positions":
        return f"Positions: review {state.active_city.upper()} ({state.active_horizon})."
    if kind == "health":
        return "Health: bot connected | providers pending | execution disabled."
    return "Ready."


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    """Load a small CSV as list of dict rows."""

    if not path.exists():
        _log_panel_event("csv_missing", f"path={path.name}")
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None or len(reader.fieldnames) == 0:
                _log_panel_event("csv_header_invalid", f"path={path.name}")
                return []
            return list(reader)
    except (OSError, ValueError) as e:
        _log_panel_event("csv_read_error", f"path={path.name} error={type(e).__name__}")
        return []





def _build_performance_summary_text() -> str:
    """Render minimal trade performance review from ledgers and report CSVs."""

    # Check TTL cache first to reduce disk I/O on repeated queries
    cached = _get_cached_report_text()
    if cached is not None:
        return cached

    summary_rows = _load_csv_rows(SIGNAL_PERFORMANCE_SUMMARY_PATH)
    summary = summary_rows[0] if summary_rows else {}
    breakdown_rows = _load_csv_rows(SIGNAL_PERFORMANCE_BREAKDOWN_PATH)
    plan_rows = _load_csv_rows(SIGNAL_PLAN_LOG_PATH)
    update_rows = _load_csv_rows(SIGNAL_POLICY_UPDATES_LOG_PATH)
    resolution_rows = _load_csv_rows(SIGNAL_RESOLUTION_LOG_PATH)
    settlement_rows = _load_csv_rows(SIGNAL_SETTLEMENT_LOG_PATH)

    if not plan_rows and not summary_rows:
        return "Report non disponibile. Nessuna trade suggerita trovata nei ledger."

    update_latest: dict[str, dict[str, str]] = {}
    for row in update_rows:
        signal_id = str(row.get("signal_id") or "").strip()
        if not signal_id:
            continue
        prev = update_latest.get(signal_id)
        prev_ts = str(prev.get("ts_update") or "") if prev else ""
        curr_ts = str(row.get("ts_update") or "")
        if prev is None or curr_ts >= prev_ts:
            update_latest[signal_id] = row

    resolution_by_id = {
        str(row.get("signal_id") or "").strip(): row
        for row in resolution_rows
        if str(row.get("signal_id") or "").strip()
    }
    settlement_by_id = {
        str(row.get("signal_id") or "").strip(): row
        for row in settlement_rows
        if str(row.get("signal_id") or "").strip()
    }

    suggested = len(plan_rows)
    executed = 0
    status_counts = {"open": 0, "closed": 0, "resolved": 0}
    win_count = 0
    loss_count = 0
    total_pnl_estimated = 0.0
    city_pnl: dict[str, float] = {}
    horizon_pnl = {"today": 0.0, "tomorrow": 0.0}
    trade_lines: list[tuple[str, str]] = []

    utc_today = _utc_now().date().isoformat()
    utc_tomorrow = (_utc_now().date() + timedelta(days=1)).isoformat()

    for plan in plan_rows:
        signal_id = str(plan.get("signal_id") or "").strip()
        if not signal_id:
            continue

        city = str(plan.get("city") or "N/A").strip().upper()
        side = str(plan.get("side") or "N/A").strip().upper()
        raw_target = str(plan.get("target_date") or "").strip().lower()
        if raw_target in {"today", "tomorrow"}:
            target_bucket = raw_target
        elif raw_target == utc_today:
            target_bucket = "today"
        elif raw_target == utc_tomorrow:
            target_bucket = "tomorrow"
        else:
            target_bucket = "today"

        entry_price = _safe_float(plan.get("entry_price"), default=0.0)
        latest = update_latest.get(signal_id)
        resolution = resolution_by_id.get(signal_id)
        settlement = settlement_by_id.get(signal_id)

        status = "open"
        exit_reason = "n/a"
        final_result = "n/a"
        current_or_exit = entry_price
        pnl_value = 0.0

        if settlement is not None:
            status = "resolved"
            status_counts["resolved"] += 1
            executed += 1
            current_or_exit = _safe_float(settlement.get("exit_price_policy"), default=entry_price)
            pnl_value = _safe_float(settlement.get("pnl_if_followed_policy"), default=0.0)
            if resolution is not None:
                exit_reason = str(resolution.get("exit_reason") or "n/a").strip() or "n/a"
            else:
                exit_reason = "policy_resolution"
            final_result = str(settlement.get("actual_market_outcome") or "n/a").strip().upper()
        elif resolution is not None:
            status = "closed"
            status_counts["closed"] += 1
            executed += 1
            current_or_exit = _safe_float(resolution.get("exit_price_policy"), default=entry_price)
            pnl_value = _safe_float(resolution.get("pnl_if_followed_policy"), default=0.0)
            exit_reason = str(resolution.get("exit_reason") or "n/a").strip() or "n/a"
        else:
            status = "open"
            status_counts["open"] += 1
            if latest is not None:
                executed += 1
                current_or_exit = _safe_float(latest.get("market_price_current"), default=entry_price)
                pnl_value = current_or_exit - entry_price

        if pnl_value > 0:
            win_count += 1
        elif pnl_value < 0:
            loss_count += 1

        total_pnl_estimated += pnl_value
        city_pnl[city] = city_pnl.get(city, 0.0) + pnl_value
        horizon_pnl[target_bucket] = horizon_pnl.get(target_bucket, 0.0) + pnl_value

        trade_lines.append(
            (
                str(plan.get("ts_signal") or ""),
                (
                    f"- {city} {target_bucket.upper()} {side} entry={entry_price:.3f} current/exit={current_or_exit:.3f} "
                    f"status={status} pnl={pnl_value:+.3f} exit_reason={exit_reason} final={final_result}"
                ),
            )
        )

    # Prioritize segments with most settled+resolved info and strongest absolute delta.
    def _segment_rank_key(item: dict[str, str]) -> tuple[float, float, float]:
        settled = _safe_float(item.get("settled_signals"))
        resolved = _safe_float(item.get("resolved_signals"))
        delta = abs(_safe_float(item.get("avg_delta_policy_vs_hold")))
        return (settled, resolved, delta)

    top_segments = sorted(breakdown_rows, key=_segment_rank_key, reverse=True)[:5]
    segment_lines: list[str] = []
    for row in top_segments:
        city = str(row.get("city") or "N/A").strip() or "N/A"
        side = str(row.get("side") or "N/A").strip() or "N/A"
        calib = str(row.get("calibration_state") or "N/A").strip() or "N/A"
        edge = str(row.get("edge_bucket") or "N/A").strip() or "N/A"
        settled = str(row.get("settled_signals") or "0").strip() or "0"
        delta = _safe_float(row.get("avg_delta_policy_vs_hold"))
        segment_lines.append(f"- {city}/{side} {calib} {edge} | settled={settled} avg_delta={delta:+.4f}")

    if not segment_lines:
        segment_lines.append("- Nessun breakdown disponibile")

    city_pnl_text = " | ".join(
        f"{city}:{value:+.3f}" for city, value in sorted(city_pnl.items())
    ) or "n/a"

    recent_trade_lines = [line for _ts, line in sorted(trade_lines, key=lambda item: item[0], reverse=True)[:6]]
    if not recent_trade_lines:
        recent_trade_lines = ["- No trade rows yet"]

    text_lines = [
        "Trade Performance Review",
        f"suggested={suggested} executed/tracked={executed}",
        f"status open={status_counts['open']} closed={status_counts['closed']} resolved={status_counts['resolved']}",
        f"wins={win_count} losses={loss_count}",
        f"total_pnl_estimated={total_pnl_estimated:+.4f}",
        f"pnl_city={city_pnl_text}",
        f"pnl_today={horizon_pnl.get('today', 0.0):+.4f} pnl_tomorrow={horizon_pnl.get('tomorrow', 0.0):+.4f}",
        f"report_total_signals={summary.get('total_signals', '0')} report_resolved={summary.get('resolved_signals', '0')} report_settled={summary.get('settled_signals', '0')}",
        f"report_total_pnl_policy={_safe_float(summary.get('total_pnl_policy')):+.4f}",
        (
            f"tp={summary.get('count_tp_hit', '0')} sl={summary.get('count_sl_hit', '0')} "
            f"invalidated={summary.get('count_invalidated', '0')} expired={summary.get('count_expired', '0')}"
        ),
        "Recent trades:",
        *recent_trade_lines,
        "Top segments:",
        *segment_lines,
    ]
    result = "\n".join(text_lines)
    _set_cached_report_text(result)  # Cache for TTL_SEC
    return result


def _show_performance_report_cached() -> str:
    """Generate or retrieve cached performance report, truncate for Discord, return formatted message."""

    report_text = _build_performance_summary_text()
    payload = report_text
    if len(payload) > 1900:
        payload = payload[:1900]
    return f"```text\n{payload}\n```"


def _format_scheduler_health(bot: Any) -> str:
    """Format scheduler task health for panel display (returns readable status string)."""
    if not hasattr(bot, 'daily_signal_task'):
        return "Scheduler: not available"
    
    is_running = bot.daily_signal_task.is_running() if callable(getattr(bot.daily_signal_task, 'is_running', None)) else False
    status_icon = "✓" if is_running else "✗"
    status_text = "running" if is_running else "stopped"
    
    lines = [f"Scheduler {status_icon}: {status_text}"]
    
    # Last success
    if hasattr(bot, 'last_daily_task_success_ts') and bot.last_daily_task_success_ts:
        lines.append(f"  Last success: {bot.last_daily_task_success_ts}")
    
    # Last error info
    if hasattr(bot, 'last_daily_task_error_ts') and bot.last_daily_task_error_ts:
        lines.append(f"  Last error: {bot.last_daily_task_error_ts}")
        if hasattr(bot, 'last_daily_task_error_reason') and bot.last_daily_task_error_reason:
            reason = bot.last_daily_task_error_reason[:60]  # Truncate for readability
            lines.append(f"  Reason: {reason}")
    
    return " | ".join(lines)


def _log_panel_event(event: str, detail: str = "") -> None:
    """Minimal stdout logging for panel actions and posting flow."""

    suffix = f" | {detail}" if detail else ""
    print(f"[panel] {event}{suffix}")


def _extract_signal_state_score(preview: str) -> tuple[str, str]:
    """Extract compact state and score from signal preview text."""

    state_match = re.search(r"\b(TRADE_CANDIDATE|PAPER|WATCH|IGNORE)\b", preview)
    state = state_match.group(1) if state_match else "UNKNOWN"
    score_match = re.search(r"score\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", preview, flags=re.IGNORECASE)
    score = score_match.group(1) if score_match else "n/a"
    return state, score


def _extract_first_float(text: str) -> float | None:
    """Extract first numeric token from text, handling optional percent signs."""

    match = re.search(r"([+-]?[0-9]+(?:\.[0-9]+)?)\s*%?", text)
    if not match:
        return None
    try:
        value = float(match.group(1))
    except ValueError:
        return None
    return (value / 100.0) if "%" in text else value


def _clamp(value: float, low: float, high: float) -> float:
    """Clamp value into [low, high]."""

    return max(low, min(high, value))


def _compute_dynamic_tp_sl(
    *,
    model_prob: float,
    market_price: float,
    executable_edge: float,
    spread: float,
    calibration_state: str,
) -> tuple[float, float]:
    """Compute dynamic TP/SL from model, market and microstructure diagnostics."""

    edge_strength = abs(executable_edge)
    directional_advantage = max(model_prob - market_price, 0.0)
    confidence = _clamp((edge_strength * 2.0) + (directional_advantage * 1.5), 0.0, 1.0)

    calibration = calibration_state.upper()
    if "LOW" in calibration:
        confidence *= 0.70
    elif "UNCERTAIN" in calibration:
        confidence *= 0.85

    spread_safe = max(spread, 0.0)
    tp_move = max(0.0100, (0.35 * edge_strength) + (0.20 * spread_safe) + (0.05 * confidence))
    sl_move = max(0.0080, (0.20 * edge_strength) + (0.35 * spread_safe) + (0.02 * (1.0 - confidence)))

    take_profit = round(_clamp(market_price + tp_move, 0.01, 0.99), 4)
    stop_loss = round(_clamp(market_price - sl_move, 0.01, 0.99), 4)
    return take_profit, stop_loss


def _compute_policy_status(
    *,
    target_date: str,
    market_price_current: float,
    executable_edge_initial: float,
    executable_edge_current: float,
    take_profit_current: float,
    stop_loss_current: float,
) -> tuple[str, str]:
    """Compute advisory-only lifecycle status for one tracked signal."""

    try:
        if target_date not in {"today", "tomorrow"}:
            signal_day = datetime.fromisoformat(target_date).date()
            if _utc_now().date() > signal_day:
                return "EXPIRED", "target_date_passed"
    except ValueError:
        pass

    if executable_edge_current < -0.005:
        return "INVALIDATED", "edge_negative_below_threshold"
    if market_price_current >= take_profit_current:
        return "TP_HIT", "market_price_reached_take_profit"
    if market_price_current <= stop_loss_current:
        return "SL_HIT", "market_price_reached_stop_loss"
    if executable_edge_current < max(0.02, executable_edge_initial * 0.5):
        return "EDGE_DECAY", "edge_dropped_below_decay_floor"
    return "HOLD", "edge_and_price_within_hold_band"


def _derive_market_slug(block: DryRunCityBlock, target_date: str, market_label: str) -> str:
    """Resolve market slug/token reference from structured metadata or safe fallback."""

    meta_slug = str(block.signal_meta.get("market_slug", "") or "").strip()
    if meta_slug:
        return meta_slug
    slug_market = re.sub(r"[^a-z0-9]+", "-", market_label.lower()).strip("-")
    slug_target = target_date.lower().replace(" ", "-")
    return f"{block.city.lower()}-{slug_target}-{slug_market}"


def _safe_csv_text(value: Any) -> str:
    """Normalize values for plain append-only CSV rows."""

    text = "" if value is None else str(value)
    return " ".join(text.replace("\r", " ").replace("\n", " ").split()).replace(",", ";")


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Parse float defensively from csv tokens."""

    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def _http_json(url: str, timeout: int = 20, max_retries: int = 2) -> Any:
    """Small JSON GET helper with retry on transient errors (503, 502, 504)."""

    last_error = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_error = e
            # Retry only on transient server errors
            if e.code in {502, 503, 504}:
                if attempt < max_retries - 1:
                    _log_panel_event("http_retry_transient", f"code={e.code} url={url[:50]} attempt={attempt + 1}")
                    time.sleep(0.1 * (2 ** attempt))  # Exponential backoff
                    continue
            # Don't retry on client errors (404, 403) or other server errors
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            last_error = e
            if attempt < max_retries - 1:
                _log_panel_event("http_retry_error", f"error={type(e).__name__} url={url[:50]} attempt={attempt + 1}")
                time.sleep(0.1 * (2 ** attempt))
                continue
            raise
    # Fallback: raise last error if all retries exhausted
    if last_error:
        raise last_error


def _signal_exists_in_ledger(ledger_path: Path, signal_id: str) -> bool:
    """Generic ledger existence check: return True if signal_id found in first column."""
    if not ledger_path.exists():
        return False
    try:
        with ledger_path.open("r", encoding="utf-8") as handle:  # Remove errors="ignore"; validate UTF-8
            next(handle, None)  # Skip header
            for line in handle:
                if line.split(",", 1)[0].strip() == signal_id:
                    return True
    except UnicodeDecodeError as e:
        _log_panel_event("ledger_unicode_error", f"path={ledger_path.name} error={e}")
        return False
    return False


def _signal_plan_exists(signal_id: str) -> bool:
    """Return True if a signal plan with this id already exists."""
    return _signal_exists_in_ledger(SIGNAL_PLAN_LOG_PATH, signal_id)


def _signal_resolution_exists(signal_id: str) -> bool:
    """Return True if this signal already has a final resolution row."""
    return _signal_exists_in_ledger(SIGNAL_RESOLUTION_LOG_PATH, signal_id)


def _signal_settlement_exists(signal_id: str) -> bool:
    """Return True if this signal already has a real-settlement row."""
    return _signal_exists_in_ledger(SIGNAL_SETTLEMENT_LOG_PATH, signal_id)


def _load_plan_row(signal_id: str) -> dict[str, Any] | None:
    """Load the initial plan row for signal_id from append-only plan ledger."""

    if not SIGNAL_PLAN_LOG_PATH.exists():
        return None

    with SIGNAL_PLAN_LOG_PATH.open("r", encoding="utf-8", errors="ignore") as handle:
        next(handle, None)
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            parts = stripped.split(",")
            if not parts or parts[0].strip() != signal_id:
                continue
            if len(parts) < 13:
                continue
            return {
                "signal_id": parts[0].strip(),
                "ts_signal": parts[1].strip(),
                "city": parts[2].strip(),
                "target_date": parts[3].strip(),
                "market_slug": parts[4].strip(),
                "token_id": parts[5].strip(),
                "side": parts[6].strip(),
                "entry_price": _safe_float(parts[7]),
                "policy_version": parts[12].strip() if len(parts) > 12 else DEFAULT_POLICY_VERSION,
                "market_label": (parts[20].strip() if len(parts) > 20 else ""),
            }
    return None


def _load_policy_resolution_row(signal_id: str) -> dict[str, Any] | None:
    """Load existing policy-resolution row for a signal if present."""

    if not SIGNAL_RESOLUTION_LOG_PATH.exists():
        return None

    with SIGNAL_RESOLUTION_LOG_PATH.open("r", encoding="utf-8", errors="ignore") as handle:
        next(handle, None)
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            parts = stripped.split(",")
            if len(parts) < 17 or parts[0].strip() != signal_id:
                continue
            return {
                "signal_id": parts[0].strip(),
                "ts_signal": parts[1].strip(),
                "ts_resolution": parts[2].strip(),
                "city": parts[3].strip(),
                "target_date": parts[4].strip(),
                "market_slug": parts[5].strip(),
                "token_id": parts[6].strip(),
                "side": parts[7].strip(),
                "entry_price": _safe_float(parts[8]),
                "exit_price_policy": _safe_float(parts[9]),
                "final_status": parts[11].strip(),
                "pnl_if_followed_policy": _safe_float(parts[13]),
                "policy_version": parts[16].strip() if len(parts) > 16 else DEFAULT_POLICY_VERSION,
            }
    return None


def _normalize_slug_part(value: str) -> str:
    """Normalize text to slug-like token for stable comparisons."""

    cleaned = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return re.sub(r"-+", "-", cleaned).strip("-")


def _parse_temperature_label(raw_label: str) -> str | None:
    """Normalize polymarket weather label to canonical bin label."""

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

    match = _TEMP_LABEL_PATTERN.search(text)
    if match:
        low = int(match.group("low"))
        high = match.group("high")
        if high:
            return f"{low}-{int(high)}F"
        return f"{low}F"

    compact = re.sub(r"\s+", " ", text)
    return compact if compact else None


def _extract_yes_price_from_market(market: dict[str, Any]) -> float | None:
    """Extract YES-side outcome price from market payload."""

    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            parsed = json.loads(outcome_prices)
            outcome_prices = parsed if isinstance(parsed, list) else None
        except json.JSONDecodeError:
            outcome_prices = None

    outcomes = market.get("outcomes")
    if isinstance(outcomes, str):
        try:
            parsed = json.loads(outcomes)
            outcomes = parsed if isinstance(parsed, list) else None
        except json.JSONDecodeError:
            outcomes = None

    if isinstance(outcome_prices, list) and isinstance(outcomes, list):
        labels = [str(item).strip().lower() for item in outcomes]
        yes_idx = next((i for i, label in enumerate(labels) if label == "yes"), None)
        if yes_idx is not None and yes_idx < len(outcome_prices):
            val = _safe_float(outcome_prices[yes_idx], default=-1.0)
            if 0.0 <= val <= 1.0:
                return val
    return None


def _extract_yes_token_id_from_market(market: dict[str, Any]) -> str | None:
    """Extract YES token id from gamma market payload when available."""

    raw_token_ids = market.get("clobTokenIds")
    token_ids: list[str] = []
    if isinstance(raw_token_ids, str):
        try:
            parsed = json.loads(raw_token_ids)
            if isinstance(parsed, list):
                token_ids = [str(item) for item in parsed]
        except json.JSONDecodeError:
            token_ids = []
    elif isinstance(raw_token_ids, list):
        token_ids = [str(item) for item in raw_token_ids]

    if len(token_ids) < 2:
        return token_ids[0] if token_ids else None

    outcomes = market.get("outcomes")
    if isinstance(outcomes, str):
        try:
            parsed = json.loads(outcomes)
            outcomes = parsed if isinstance(parsed, list) else None
        except json.JSONDecodeError:
            outcomes = None

    if isinstance(outcomes, list):
        labels = [str(item).strip().lower() for item in outcomes]
        yes_idx = next((i for i, label in enumerate(labels) if label == "yes"), None)
        if yes_idx is not None and yes_idx < len(token_ids):
            return token_ids[yes_idx]
    return token_ids[0]


def _fetch_event_by_slug(slug: str) -> dict[str, Any] | None:
    """Fetch one event by exact slug from gamma API."""

    if not slug:
        return None
    url = f"https://gamma-api.polymarket.com/events?slug={urllib.parse.quote(slug)}"
    try:
        payload = _http_json(url, timeout=25)
    except Exception:
        return None
    if not isinstance(payload, list):
        return None
    expected = _normalize_slug_part(slug)
    for item in payload:
        if not isinstance(item, dict):
            continue
        if _normalize_slug_part(str(item.get("slug") or "")) == expected:
            return item
    return None


def _resolve_winning_bin_from_event(event: dict[str, Any]) -> tuple[str | None, dict[str, str]]:
    """Resolve winning bin label from resolved event markets and map token->label."""

    markets = event.get("markets") if isinstance(event, dict) else None
    if not isinstance(markets, list):
        return None, {}

    winner_label: str | None = None
    winner_price = -1.0
    token_to_label: dict[str, str] = {}
    for market in markets:
        if not isinstance(market, dict):
            continue
        raw_label = str(market.get("title") or market.get("question") or "")
        label = _parse_temperature_label(raw_label)
        if not label:
            continue
        yes_price = _extract_yes_price_from_market(market)
        if yes_price is None:
            continue
        token_id = _extract_yes_token_id_from_market(market)
        if token_id:
            token_to_label[token_id] = label
        if yes_price > winner_price:
            winner_price = yes_price
            winner_label = label

    return winner_label, token_to_label


def _parse_market_slug_date(market_slug: str) -> datetime.date | None:
    """Parse target date from market slug across common slug formats."""

    text = (market_slug or "").lower()
    iso_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if iso_match:
        try:
            return datetime.fromisoformat(iso_match.group(1)).date()
        except ValueError:
            pass

    compact_match = re.search(r"\b(\d{8})\b", text)
    if compact_match:
        raw = compact_match.group(1)
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            pass

    month_match = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)-(\d{1,2})-(\d{4})",
        text,
    )
    if month_match:
        month_name, day_raw, year_raw = month_match.groups()
        month_num = _MONTH_NAME_TO_NUM.get(month_name)
        if month_num is None:
            return None
        try:
            return datetime(int(year_raw), month_num, int(day_raw), tzinfo=timezone.utc).date()
        except ValueError:
            return None

    return None


def _resolve_signal_calendar_date(target_date: str, market_slug: str, ts_signal: str) -> datetime.date | None:
    """Resolve signal calendar date used for real settlement checks."""

    target = (target_date or "").strip().lower()
    if target not in {"today", "tomorrow"}:
        try:
            return datetime.fromisoformat(target).date()
        except ValueError:
            pass

    parsed_signal_ts: datetime | None = None
    try:
        signal_text = ts_signal.replace("Z", "+00:00")
        parsed_signal_ts = datetime.fromisoformat(signal_text)
    except ValueError:
        parsed_signal_ts = None

    if parsed_signal_ts is not None:
        base_date = parsed_signal_ts.astimezone(timezone.utc).date()
        if target == "today":
            return base_date
        if target == "tomorrow":
            return base_date + timedelta(days=1)

    return _parse_market_slug_date(market_slug)


def _fetch_actual_tmax_f(city: str, target_day: datetime.date) -> float | None:
    """Fetch official observed Tmax (F) from Open-Meteo archive for target date."""

    coords = CITY_COORDS.get(city.lower())
    if coords is None:
        return None
    lat, lon = coords
    endpoint = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={target_day.isoformat()}&end_date={target_day.isoformat()}"
        "&daily=temperature_2m_max&timezone=UTC"
    )
    try:
        payload = _http_json(endpoint, timeout=25)
    except Exception:
        return None
    daily = payload.get("daily") if isinstance(payload, dict) else None
    temps = daily.get("temperature_2m_max") if isinstance(daily, dict) else None
    if not isinstance(temps, list) or not temps:
        return None
    celsius = _safe_float(temps[0], default=float("nan"))
    if celsius != celsius:
        return None
    return round((celsius * 9.0 / 5.0) + 32.0, 2)


def _build_settlement_row_from_ledgers(signal_id: str, settlement_source: str) -> SignalSettlementRow | None:
    """Build real-settlement comparison row for one signal when final data exists."""

    if _signal_settlement_exists(signal_id):
        return None

    policy_row = _load_policy_resolution_row(signal_id)
    if policy_row is None:
        return None
    plan_row = _load_plan_row(signal_id)
    if plan_row is None:
        return None

    target_day = _resolve_signal_calendar_date(
        target_date=str(plan_row.get("target_date", "")),
        market_slug=str(plan_row.get("market_slug", "")),
        ts_signal=str(plan_row.get("ts_signal", "")),
    )
    if target_day is None:
        return None

    # Require final daily data availability: do not force settlement for ongoing day.
    if target_day >= _utc_now().date():
        return None

    market_slug = str(plan_row.get("market_slug", ""))
    event = _fetch_event_by_slug(market_slug)
    if event is None:
        return None

    winning_bin_label, token_to_label = _resolve_winning_bin_from_event(event)
    if not winning_bin_label:
        return None

    actual_tmax_f = _fetch_actual_tmax_f(str(plan_row.get("city", "")).lower(), target_day)
    if actual_tmax_f is None:
        return None

    market_label = str(plan_row.get("market_label") or "")
    token_id = str(plan_row.get("token_id") or "")
    if not market_label and token_id and token_id in token_to_label:
        market_label = token_to_label[token_id]
    if not market_label:
        # Cannot compute hold-to-resolution payout without the specific traded bin.
        return None

    side = str(plan_row.get("side", "")).upper()
    if side == "BUY_NO":
        payout_at_resolution = 0.0 if market_label == winning_bin_label else 1.0
    else:
        payout_at_resolution = 1.0 if market_label == winning_bin_label else 0.0

    entry_price = _safe_float(plan_row.get("entry_price"), default=0.0)
    pnl_hold = payout_at_resolution - entry_price
    pnl_hold_pct = (pnl_hold / entry_price * 100.0) if entry_price > 0 else 0.0
    actual_market_outcome = "WIN" if payout_at_resolution >= 0.5 else "LOSE"

    return SignalSettlementRow(
        signal_id=signal_id,
        city=str(plan_row.get("city", "")),
        target_date=str(plan_row.get("target_date", "")),
        market_slug=market_slug,
        token_id=token_id,
        side=side,
        ts_signal=str(policy_row.get("ts_signal", "")),
        ts_resolution_policy=str(policy_row.get("ts_resolution", "")),
        ts_settlement_real=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        entry_price=entry_price,
        exit_price_policy=_safe_float(policy_row.get("exit_price_policy"), default=0.0),
        final_status_policy=str(policy_row.get("final_status", "")),
        pnl_if_followed_policy=_safe_float(policy_row.get("pnl_if_followed_policy"), default=0.0),
        actual_market_outcome=actual_market_outcome,
        winning_bin_label=winning_bin_label,
        actual_tmax_f=actual_tmax_f,
        pnl_if_held_to_resolution=pnl_hold,
        pnl_pct_if_held_to_resolution=pnl_hold_pct,
        settlement_source=settlement_source,
        policy_version=str(policy_row.get("policy_version", DEFAULT_POLICY_VERSION)),
    )


def _append_csv_row_locked(path: Path, row: str, max_retries: int = 3) -> None:
    """Append CSV row with retry on transient OSError (guards concurrent read-write)."""
    for attempt in range(max_retries):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(row)
                handle.flush()  # Ensure OS flushes buffers to disk
            return
        except OSError as e:
            if attempt < max_retries - 1:
                _log_panel_event("csv_append_retry", f"path={path.name} attempt={attempt + 1} error={e}")
                time.sleep(0.01 * (2 ** attempt))  # Exponential backoff: 10ms, 20ms, 40ms
            else:
                _log_panel_event("csv_append_failed", f"path={path.name} max_retries={max_retries} error={e}")
                raise


def _append_signal_settlement_row(settlement: SignalSettlementRow) -> None:
    """Append one real-settlement row (append-only, one per signal_id)."""

    if _signal_settlement_exists(settlement.signal_id):
        return

    header = (
        "signal_id,city,target_date,market_slug,token_id,side,ts_signal,ts_resolution_policy,"
        "ts_settlement_real,entry_price,exit_price_policy,final_status_policy,pnl_if_followed_policy,"
        "actual_market_outcome,winning_bin_label,actual_tmax_f,pnl_if_held_to_resolution,"
        "pnl_pct_if_held_to_resolution,settlement_source,policy_version\n"
    )
    _ensure_settlement_log_exists(header)
    row = (
        f"{_safe_csv_text(settlement.signal_id)},{_safe_csv_text(settlement.city)},{_safe_csv_text(settlement.target_date)},"
        f"{_safe_csv_text(settlement.market_slug)},{_safe_csv_text(settlement.token_id)},{_safe_csv_text(settlement.side)},"
        f"{settlement.ts_signal},{settlement.ts_resolution_policy},{settlement.ts_settlement_real},"
        f"{settlement.entry_price:.4f},{settlement.exit_price_policy:.4f},{_safe_csv_text(settlement.final_status_policy)},"
        f"{settlement.pnl_if_followed_policy:+.4f},{_safe_csv_text(settlement.actual_market_outcome)},"
        f"{_safe_csv_text(settlement.winning_bin_label)},{settlement.actual_tmax_f:.2f},"
        f"{settlement.pnl_if_held_to_resolution:+.4f},{settlement.pnl_pct_if_held_to_resolution:+.2f},"
        f"{_safe_csv_text(settlement.settlement_source)},{_safe_csv_text(settlement.policy_version)}\n"
    )
    _append_csv_row_locked(SIGNAL_SETTLEMENT_LOG_PATH, row)


def _ensure_csv_initialized(path: Path, header: str) -> None:
    """Create CSV file with header if it does not exist yet (centralized init)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    path.write_text(header, encoding="utf-8")


def _ensure_settlement_log_exists(header: str | None = None) -> None:
    """Create settlement ledger with header if it does not exist yet."""
    default_header = (
        "signal_id,city,target_date,market_slug,token_id,side,ts_signal,ts_resolution_policy,"
        "ts_settlement_real,entry_price,exit_price_policy,final_status_policy,pnl_if_followed_policy,"
        "actual_market_outcome,winning_bin_label,actual_tmax_f,pnl_if_held_to_resolution,"
        "pnl_pct_if_held_to_resolution,settlement_source,policy_version\n"
    )
    _ensure_csv_initialized(SIGNAL_SETTLEMENT_LOG_PATH, header or default_header)


def _load_updates_for_signal(signal_id: str) -> list[dict[str, Any]]:
    """Load all update rows for signal_id from append-only updates ledger (with local cache)."""

    # Check cache first (5-minute TTL per signal)
    if signal_id in _UPDATE_TAIL_CACHE:
        return _UPDATE_TAIL_CACHE[signal_id]

    rows: list[dict[str, Any]] = []
    if not SIGNAL_POLICY_UPDATES_LOG_PATH.exists():
        return rows

    with SIGNAL_POLICY_UPDATES_LOG_PATH.open("r", encoding="utf-8", errors="ignore") as handle:
        next(handle, None)
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            parts = stripped.split(",")
            if len(parts) < 8 or parts[0].strip() != signal_id:
                continue
            rows.append(
                {
                    "signal_id": parts[0].strip(),
                    "ts_update": parts[1].strip(),
                    "market_price_current": _safe_float(parts[2]),
                    "status": parts[7].strip().upper(),
                    "refresh_source": (parts[8].strip() if len(parts) > 8 else "unknown"),
                    "update_reason": (parts[9].strip() if len(parts) > 9 else "unknown"),
                    "policy_status_reason": (parts[12].strip() if len(parts) > 12 else "unknown"),
                }
            )
    
    # Cache results (5-minute TTL handled externally via validation)
    _UPDATE_TAIL_CACHE[signal_id] = rows
    return rows


def _build_resolution_from_ledgers(signal_id: str, resolution_source: str) -> SignalResolutionRow | None:
    """Build final resolution row when policy has a clear exit or final outcome exists."""

    if _signal_resolution_exists(signal_id):
        return None

    plan = _load_plan_row(signal_id)
    if plan is None:
        return None

    updates = _load_updates_for_signal(signal_id)
    if not updates:
        return None

    # Priority: first TP/SL trigger price closes the simulated trade.
    for row in updates:
        if row["status"] in {"TP_HIT", "SL_HIT"}:
            final_status = row["status"]
            exit_reason = "policy_take_profit" if final_status == "TP_HIT" else "policy_stop_loss"
            resolved_outcome = "POLICY_EXIT"
            exit_price = row["market_price_current"]
            ts_resolution = row["ts_update"]
            break
    else:
        # No market trigger: allow coherent policy closes only for invalidated/expired.
        closure = next((r for r in updates if r["status"] in {"INVALIDATED", "EXPIRED"}), None)
        if closure is None:
            # Keep signal open when no final outcome and no clear policy exit yet.
            return None
        final_status = closure["status"]
        exit_reason = "policy_invalidated" if final_status == "INVALIDATED" else "policy_expired"
        resolved_outcome = "POLICY_CLOSED_NO_SETTLEMENT"
        exit_price = closure["market_price_current"]
        ts_resolution = closure["ts_update"]

    entry_price = float(plan["entry_price"])
    side = str(plan["side"]).upper()
    # Transparent simulated PnL for both BUY YES / BUY NO uses side-token price delta.
    pnl_if_followed_policy = exit_price - entry_price
    base = entry_price if entry_price > 0 else 1.0
    pnl_pct_if_followed_policy = (pnl_if_followed_policy / base) * 100.0

    return SignalResolutionRow(
        signal_id=signal_id,
        ts_signal=str(plan["ts_signal"]),
        ts_resolution=ts_resolution,
        city=str(plan["city"]),
        target_date=str(plan["target_date"]),
        market_slug=str(plan["market_slug"]),
        token_id=str(plan["token_id"]),
        side=side,
        entry_price=entry_price,
        exit_price_policy=exit_price,
        exit_reason=exit_reason,
        final_status=final_status,
        resolved_outcome=resolved_outcome,
        pnl_if_followed_policy=pnl_if_followed_policy,
        pnl_pct_if_followed_policy=pnl_pct_if_followed_policy,
        resolution_source=resolution_source,
        policy_version=str(plan["policy_version"] or DEFAULT_POLICY_VERSION),
    )


def _append_signal_resolution_row(resolution: SignalResolutionRow) -> None:
    """Append one final resolution row (append-only, dedup by signal_id)."""

    if _signal_resolution_exists(resolution.signal_id):
        return

    header = (
        "signal_id,ts_signal,ts_resolution,city,target_date,market_slug,token_id,side,entry_price,"
        "exit_price_policy,exit_reason,final_status,resolved_outcome,pnl_if_followed_policy,"
        "pnl_pct_if_followed_policy,resolution_source,policy_version\n"
    )
    _ensure_csv_initialized(SIGNAL_RESOLUTION_LOG_PATH, header)
    row = (
        f"{_safe_csv_text(resolution.signal_id)},{resolution.ts_signal},{resolution.ts_resolution},"
        f"{_safe_csv_text(resolution.city)},{_safe_csv_text(resolution.target_date)},"
        f"{_safe_csv_text(resolution.market_slug)},{_safe_csv_text(resolution.token_id)},"
        f"{_safe_csv_text(resolution.side)},{resolution.entry_price:.4f},{resolution.exit_price_policy:.4f},"
        f"{_safe_csv_text(resolution.exit_reason)},{_safe_csv_text(resolution.final_status)},"
        f"{_safe_csv_text(resolution.resolved_outcome)},{resolution.pnl_if_followed_policy:+.4f},"
        f"{resolution.pnl_pct_if_followed_policy:+.2f},{_safe_csv_text(resolution.resolution_source)},"
        f"{_safe_csv_text(resolution.policy_version)}\n"
    )
    _append_csv_row_locked(SIGNAL_RESOLUTION_LOG_PATH, row)


def _build_signal_snapshot(block: DryRunCityBlock) -> SignalSnapshot:
    """Build one structured signal snapshot from dry-run block + metadata."""

    preview = block.discord_preview
    target_date_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", preview)
    target_date = str(block.signal_meta.get("target_date") or (target_date_match.group(0) if target_date_match else "today"))
    market_label = block.primary_candidate.split("/", 1)[0] if "/" in block.primary_candidate else block.primary_candidate
    side = str(block.signal_meta.get("side") or (block.primary_candidate.split("/", 1)[1] if "/" in block.primary_candidate else "UNKNOWN"))

    state_match = re.search(r"\b(TRADE_CANDIDATE|PAPER|WATCH|IGNORE)\b", preview)
    policy_state = state_match.group(1) if state_match else "UNKNOWN"
    reason_match = re.search(r"reason\s*[:=]\s*([^|]+)", preview, flags=re.IGNORECASE)
    reason = reason_match.group(1).strip() if reason_match else "n/a"

    entry_price = block.signal_meta.get("entry_price")
    if entry_price is None:
        entry_match = re.search(r"entry\s*[:=]\s*([^|]+)", preview, flags=re.IGNORECASE)
        entry_price = _extract_first_float(entry_match.group(1) if entry_match else "")
    entry_value = float(entry_price) if entry_price is not None else 0.5

    model_prob = block.signal_meta.get("model_prob")
    if model_prob is None:
        model_match = re.search(r"model_prob\s*[:=]\s*([^|\s]+)", preview, flags=re.IGNORECASE)
        model_prob = _extract_first_float(model_match.group(1) if model_match else "")
    model_prob_value = float(model_prob) if model_prob is not None else entry_value

    market_prob = block.signal_meta.get("market_prob")

    book_best_ask_raw = block.signal_meta.get("book_best_ask")
    book_best_bid_raw = block.signal_meta.get("book_best_bid")
    book_best_ask = float(book_best_ask_raw) if book_best_ask_raw is not None else None
    book_best_bid = float(book_best_bid_raw) if book_best_bid_raw is not None else None

    side_token = _normalize_side_token(side)
    if book_best_ask is not None:
        market_price_current = float(book_best_ask)
    elif entry_price is not None:
        market_price_current = float(entry_value)
    elif market_prob is not None:
        # `market_prob` from dry-run can be YES-side probability; convert for BUY NO fallback.
        yes_prob = float(market_prob)
        market_price_current = 1.0 - yes_prob if side_token == "NO" else yes_prob
    else:
        market_price_current = entry_value

    executable_edge = block.signal_meta.get("executable_edge")
    if executable_edge is None:
        edge_match = re.search(r"(?:executable_edge|exec_edge|edge)\s*[:=]\s*([+-]?[0-9]+(?:\.[0-9]+)?%?)", preview, flags=re.IGNORECASE)
        executable_edge = _extract_first_float(edge_match.group(1) if edge_match else "")
    executable_edge_value = float(executable_edge) if executable_edge is not None else 0.0

    spread = block.signal_meta.get("spread")
    if spread is None:
        spread_match = re.search(r"spread\s*[:=]\s*([^|\s]+)", preview, flags=re.IGNORECASE)
        spread = _extract_first_float(spread_match.group(1) if spread_match else "")
    spread_value = float(spread) if spread is not None else 0.03

    calibration_state = str(block.signal_meta.get("calibration_state") or "UNKNOWN")
    policy_version = str(block.signal_meta.get("policy_version") or DEFAULT_POLICY_VERSION)
    market_slug = _derive_market_slug(block, target_date=target_date, market_label=market_label)
    token_id = str(block.signal_meta.get("token_id", "") or "")
    yes_token_id = str(block.signal_meta.get("yes_token_id", "") or "")
    no_token_id = str(block.signal_meta.get("no_token_id", "") or "")
    signal_id = "|".join([block.city.upper(), target_date, market_slug, side.upper()])

    take_profit, stop_loss = _compute_dynamic_tp_sl(
        model_prob=model_prob_value,
        market_price=entry_value,
        executable_edge=executable_edge_value,
        spread=spread_value,
        calibration_state=calibration_state,
    )
    status, policy_status_reason = _compute_policy_status(
        target_date=target_date,
        market_price_current=market_price_current,
        executable_edge_initial=executable_edge_value,
        executable_edge_current=executable_edge_value,
        take_profit_current=take_profit,
        stop_loss_current=stop_loss,
    )
    ts_now = _utc_now().replace(microsecond=0).isoformat()

    return SignalSnapshot(
        signal_id=signal_id,
        ts_signal=ts_now,
        city=block.city.upper(),
        target_date=target_date,
        market_slug=market_slug,
        token_id=token_id,
        market_label=market_label,
        side=side.upper(),
        entry_price=round(entry_value, 4),
        model_prob_initial=round(model_prob_value, 4),
        executable_edge_initial=round(executable_edge_value, 4),
        spread=round(spread_value, 4),
        calibration_state=calibration_state,
        take_profit_initial=take_profit,
        stop_loss_initial=stop_loss,
        policy_version=policy_version,
        market_price_current=round(market_price_current, 4),
        model_prob_current=round(model_prob_value, 4),
        executable_edge_current=round(executable_edge_value, 4),
        take_profit_current=take_profit,
        stop_loss_current=stop_loss,
        status=status,
        policy_status_reason=policy_status_reason,
        reason=reason,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        resolved_token_id=token_id,
        book_best_bid=book_best_bid,
        book_best_ask=book_best_ask,
    )


def _append_signal_plan_row(snapshot: SignalSnapshot) -> None:
    """Append one initial trade-plan row (append-only)."""
    header = (
        "signal_id,ts_signal,city,target_date,market_slug,token_id,side,entry_price,"
        "model_prob_initial,executable_edge_initial,take_profit_initial,stop_loss_initial,policy_version,"
        "refresh_source,status,update_reason,calibration_state,spread_current,policy_status_reason,pnl_mark_to_market,market_label\n"
    )
    _ensure_csv_initialized(SIGNAL_PLAN_LOG_PATH, header)
    # Keep initial plan immutable: mark-to-market is fixed at plan creation.
    pnl_mark_to_market = 0.0
    row = (
        f"{_safe_csv_text(snapshot.signal_id)},{snapshot.ts_signal},{snapshot.city},{snapshot.target_date},"
        f"{_safe_csv_text(snapshot.market_slug)},{_safe_csv_text(snapshot.token_id)},{snapshot.side},"
        f"{snapshot.entry_price:.4f},{snapshot.model_prob_initial:.4f},{snapshot.executable_edge_initial:.4f},"
        f"{snapshot.take_profit_initial:.4f},{snapshot.stop_loss_initial:.4f},{_safe_csv_text(snapshot.policy_version)},"
        f"initial_signal,INITIAL,initial_plan,{_safe_csv_text(snapshot.calibration_state)},{snapshot.spread:.4f},"
        f"initial_signal_snapshot,{pnl_mark_to_market:+.4f},{_safe_csv_text(snapshot.market_label)}\n"
    )
    _append_csv_row_locked(SIGNAL_PLAN_LOG_PATH, row)


def _has_meaningful_update_change(
    snapshot: SignalSnapshot,
    refresh_source: str,
    update_reason: str,
) -> bool:
    """Return True only if key update fields changed vs latest row for signal_id (tail-scan optimized)."""

    if not SIGNAL_POLICY_UPDATES_LOG_PATH.exists():
        return True

    new_signature = [
        f"{snapshot.market_price_current:.4f}",
        f"{snapshot.model_prob_current:.4f}",
        f"{snapshot.executable_edge_current:.4f}",
        f"{snapshot.take_profit_current:.4f}",
        f"{snapshot.stop_loss_current:.4f}",
        snapshot.status,
        _safe_csv_text(refresh_source),
        _safe_csv_text(update_reason),
        _safe_csv_text(snapshot.calibration_state),
        f"{snapshot.spread:.4f}",
        _safe_csv_text(snapshot.policy_status_reason),
    ]

    # Optimization: Check recent lines first (95% of signals are recent), then full scan fallback
    try:
        with SIGNAL_POLICY_UPDATES_LOG_PATH.open("r", encoding="utf-8", errors="ignore") as handle:
            all_lines = handle.readlines()
        
        # Skip header and scan last 10 lines (tail-scan for recent signal updates)
        tail_lines = all_lines[-10:] if len(all_lines) > 1 else []
        for line in tail_lines:
            stripped = line.strip()
            if not stripped:
                continue
            parts = stripped.split(",")
            if not parts or parts[0].strip() != snapshot.signal_id:
                continue
            if len(parts) < 15:
                continue
            old_signature = [part.strip() for part in parts[2:13]]
            if old_signature == new_signature:
                return False  # Found match in tail (95% case)
        
        # Fallback: Check remaining lines (full scan for older signals)
        for line in all_lines[1:-10] if len(all_lines) > 11 else []:
            stripped = line.strip()
            if not stripped:
                continue
            parts = stripped.split(",")
            if not parts or parts[0].strip() != snapshot.signal_id:
                continue
            if len(parts) < 15:
                continue
            old_signature = [part.strip() for part in parts[2:13]]
            if old_signature == new_signature:
                return False
    except OSError:
        pass
    
    return True


def _append_signal_policy_update_row(snapshot: SignalSnapshot, refresh_source: str, update_reason: str) -> None:
    """Append one policy refresh row for an existing signal id (append-only)."""

    if not _has_meaningful_update_change(snapshot, refresh_source=refresh_source, update_reason=update_reason):
        return

    header = (
        "signal_id,ts_update,market_price_current,model_prob_current,executable_edge_current,"
        "take_profit_current,stop_loss_current,status,refresh_source,update_reason,"
        "calibration_state,spread_current,policy_status_reason,pnl_mark_to_market\n"
    )
    _ensure_csv_initialized(SIGNAL_POLICY_UPDATES_LOG_PATH, header)
    pnl_mark_to_market = round(snapshot.market_price_current - snapshot.entry_price, 4)
    row = (
        f"{_safe_csv_text(snapshot.signal_id)},{snapshot.ts_signal},{snapshot.market_price_current:.4f},"
        f"{snapshot.model_prob_current:.4f},{snapshot.executable_edge_current:.4f},"
        f"{snapshot.take_profit_current:.4f},{snapshot.stop_loss_current:.4f},{snapshot.status},"
        f"{_safe_csv_text(refresh_source)},{_safe_csv_text(update_reason)},"
        f"{_safe_csv_text(snapshot.calibration_state)},{snapshot.spread:.4f},"
        f"{_safe_csv_text(snapshot.policy_status_reason)},{pnl_mark_to_market:+.4f}\n"
    )
    _append_csv_row_locked(SIGNAL_POLICY_UPDATES_LOG_PATH, row)


def _record_signal_plan_and_update(block: DryRunCityBlock, refresh_source: str, update_reason: str) -> SignalSnapshot:
    """Record initial plan (once) and current update (always) for a signal."""

    snapshot = _build_signal_snapshot(block)
    if not _signal_plan_exists(snapshot.signal_id):
        _append_signal_plan_row(snapshot)
    _append_signal_policy_update_row(snapshot, refresh_source=refresh_source, update_reason=update_reason)
    _ensure_settlement_log_exists()
    resolution = _build_resolution_from_ledgers(snapshot.signal_id, resolution_source=refresh_source)
    if resolution is not None:
        _append_signal_resolution_row(resolution)
    settlement = _build_settlement_row_from_ledgers(
        snapshot.signal_id,
        settlement_source="gamma_events+open_meteo_archive",
    )
    if settlement is not None:
        _append_signal_settlement_row(settlement)
    return snapshot


def _append_signal_audit_row(city: str, state: str, candidate: str, score: str, posted: bool) -> None:
    """Append one compact audit row for live signal posting runs."""

    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    header = "timestamp,city,state,candidate,score,posted\n"
    row = f"{timestamp},{city},{state},{candidate},{score},{'yes' if posted else 'no'}\n"
    if not AUDIT_LOG_PATH.exists():
        AUDIT_LOG_PATH.write_text(header + row, encoding="utf-8")
        return
    with AUDIT_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(row)


def _append_scheduler_run_row(
    run_timestamp_utc: datetime,
    city: str,
    source: str,
    outcome: str,
    reason: str,
) -> None:
    """Append one scheduler diagnostic row for each city processed by daily task."""

    SCHEDULER_RUN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    timestamp = run_timestamp_utc.replace(microsecond=0).isoformat()
    clean_reason = " ".join(str(reason).replace("\n", " ").replace("\r", " ").split())
    clean_reason = clean_reason.replace(",", ";")
    header = "run_timestamp_utc,city,source,outcome,reason\n"
    row = f"{timestamp},{city},{source},{outcome},{clean_reason}\n"
    if not SCHEDULER_RUN_LOG_PATH.exists():
        SCHEDULER_RUN_LOG_PATH.write_text(header + row, encoding="utf-8")
        return
    with SCHEDULER_RUN_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(row)


def build_signal_embed(block: DryRunCityBlock, snapshot: SignalSnapshot | None = None) -> "discord.Embed":
    """Build advisory signal embed with policy plan fields visible."""

    signal = snapshot or _build_signal_snapshot(block)
    embed = discord.Embed(
        title=f"🌡️ SIGNAL | {signal.city} | {signal.target_date.upper()}",
        color=0x1F8B4C,
    )
    embed.add_field(name="Mercato", value=signal.market_label, inline=True)
    embed.add_field(name="Lato", value=signal.side.replace("_", " "), inline=True)
    embed.add_field(name="Prezzo entry", value=f"{signal.entry_price:.4f}", inline=True)
    embed.add_field(name="Edge attuabile", value=f"{signal.executable_edge_initial:+.4f}", inline=True)
    embed.add_field(name="TP iniziale", value=f"{signal.take_profit_initial:.4f}", inline=True)
    embed.add_field(name="SL iniziale", value=f"{signal.stop_loss_initial:.4f}", inline=True)
    embed.add_field(name="Stato policy", value=signal.status, inline=True)
    embed.add_field(name="Modalita", value=signal.calibration_state, inline=True)
    embed.add_field(name="Perche lo segnaliamo", value=signal.reason, inline=False)
    return embed


def parse_policy_dry_run_output(raw_output: str, expected_cities: list[str]) -> tuple[list[DryRunCityBlock], DryRunSummary | None]:
    """Parse script dry-run stdout into city blocks and summary footer."""

    lines = [line.strip() for line in raw_output.splitlines() if line.strip()]
    blocks: list[DryRunCityBlock] = []

    index = 0
    city_idx = 0
    while index + 2 < len(lines):
        first = lines[index]
        if not first.startswith("discord_preview="):
            break
        second = lines[index + 1]
        third = lines[index + 2]
        if not second.startswith("source_candidate_count=") or not third.startswith("primary_candidate="):
            break

        city = expected_cities[city_idx] if city_idx < len(expected_cities) else f"city_{city_idx + 1}"
        preview = first.split("=", 1)[1].strip()
        source_raw = second.split("=", 1)[1].strip()
        primary = third.split("=", 1)[1].strip()
        signal_meta: dict[str, Any] = {}
        next_idx = index + 3
        if next_idx < len(lines) and lines[next_idx].startswith("signal_meta="):
            raw_meta = lines[next_idx].split("=", 1)[1].strip()
            if raw_meta:
                try:
                    parsed_meta = json.loads(raw_meta)
                    if isinstance(parsed_meta, dict):
                        signal_meta = parsed_meta
                except json.JSONDecodeError:
                    signal_meta = {}
            next_idx += 1
        try:
            source_count = int(source_raw)
        except ValueError:
            source_count = 0

        blocks.append(
            DryRunCityBlock(
                city=city,
                discord_preview=preview,
                source_candidate_count=source_count,
                primary_candidate=primary,
                signal_meta=signal_meta,
            )
        )
        index = next_idx
        city_idx += 1

    summary_map: dict[str, str] = {}
    for line in lines[index:]:
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        summary_map[key.strip()] = value.strip()

    summary: DryRunSummary | None = None
    required = {"total_cities", "cities_with_signal", "top_signal_city", "top_signal_candidate"}
    if required.issubset(summary_map.keys()):
        try:
            summary = DryRunSummary(
                total_cities=int(summary_map["total_cities"]),
                cities_with_signal=int(summary_map["cities_with_signal"]),
                top_signal_city=summary_map["top_signal_city"],
                top_signal_candidate=summary_map["top_signal_candidate"],
            )
        except ValueError:
            summary = None

    return blocks, summary


async def run_policy_dry_run(cities: list[str], horizon: str = "today") -> tuple[list[DryRunCityBlock], DryRunSummary | None]:
    """Run multi-city policy dry-run script and parse its output."""

    command = [
        sys.executable,
        str(POLICY_SCRIPT_PATH),
        "--cities",
        ",".join(cities),
        "--horizon",
        horizon,
        "--gamma-limit",
        "3000",
        "--min-abs-edge",
        "0.01",
        "--min-exec-edge",
        "0.0",
        "--discord-dry-run",
    ]

    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(POLICY_SCRIPT_PATH.parents[1]),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    
    # Add timeout to prevent indefinite hangs (default 120s)
    try:
        stdout_data, stderr_data = await asyncio.wait_for(
            process.communicate(),
            timeout=POLICY_SCRIPT_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise RuntimeError(f"policy dry-run timeout after {POLICY_SCRIPT_TIMEOUT_SEC}s (subprocess hung or too slow)")
    
    if process.returncode != 0:
        error_preview = stderr_data.decode("utf-8", errors="ignore").strip() or "policy dry-run failed"
        raise RuntimeError(error_preview)

    raw_output = stdout_data.decode("utf-8", errors="ignore")
    return parse_policy_dry_run_output(raw_output, cities)


async def _collect_city_setups_by_horizon(city: str) -> dict[str, list[SignalSnapshot]]:
    """Collect executable snapshots for one city split by today/tomorrow horizons."""

    results: dict[str, list[SignalSnapshot]] = {"today": [], "tomorrow": []}

    async def _run_one(horizon: str) -> list[SignalSnapshot]:
        blocks, _summary = await run_policy_dry_run([city], horizon=horizon)
        snapshots: list[SignalSnapshot] = []
        for block in blocks:
            if block.primary_candidate == "NONE":
                continue
            try:
                snapshots.append(_build_signal_snapshot(block))
            except Exception as exc:
                _log_panel_event(
                    "panel_day_snapshot_error",
                    f"city={city} horizon={horizon} error={type(exc).__name__}",
                )
        snapshots.sort(key=lambda item: item.executable_edge_current, reverse=True)
        return snapshots

    today_result, tomorrow_result = await asyncio.gather(
        _run_one("today"),
        _run_one("tomorrow"),
        return_exceptions=True,
    )

    if isinstance(today_result, list):
        results["today"] = today_result
        _mark_runtime_refresh("data")
        _mark_runtime_refresh("market")
        _mark_runtime_refresh("weather")
    else:
        _log_panel_event("panel_day_snapshot_error", f"city={city} horizon=today error={type(today_result).__name__}")

    if isinstance(tomorrow_result, list):
        results["tomorrow"] = tomorrow_result
        _mark_runtime_refresh("data")
        _mark_runtime_refresh("market")
        _mark_runtime_refresh("weather")
    else:
        _log_panel_event("panel_day_snapshot_error", f"city={city} horizon=tomorrow error={type(tomorrow_result).__name__}")

    return results


if discord is not None:

    class CitySelect(discord.ui.Select):
        """Dropdown selector for active city in the operator panel."""

        def __init__(self, bot: "DiscordBot") -> None:
            self.bot = bot
            city_order = [city for city in PANEL_CITY_OPTIONS if city in bot.state.cities]
            if not city_order:
                city_order = list(bot.state.cities)
            options = []
            for city in city_order:
                options.append(
                    discord.SelectOption(
                        label=("NYC" if city == "nyc" else city.title()),
                        value=city,
                        default=(city == bot.state.active_city),
                    )
                )
            super().__init__(placeholder="Select City", min_values=1, max_values=1, options=options, custom_id="panel_city_select")

        async def callback(self, interaction: discord.Interaction) -> None:
            custom_id = "panel_city_select"
            action = "city_select"
            user_id = getattr(getattr(interaction, "user", None), "id", "unknown")
            try:
                if not interaction.response.is_done():
                    await interaction.response.defer()
                selected_city = self.values[0] if self.values else self.bot.state.active_city
                if selected_city in self.bot.state.cities:
                    self.bot.state.active_city_index = self.bot.state.cities.index(selected_city)
                for option in self.options:
                    option.default = (option.value == self.bot.state.active_city)
                _log_panel_event("city_select", f"city={self.bot.state.active_city}")
                await self.bot.panel_view.update_panel(interaction)
            except Exception as exc:
                _log_panel_event(
                    "callback_error",
                    (
                        f"custom_id={custom_id} user={user_id} action={action} "
                        f"exc_type={type(exc).__name__} exc={str(exc)[:200]}"
                    ),
                )
                try:
                    if interaction.response.is_done():
                        await interaction.followup.send("City switch failed. Try again.", ephemeral=True)
                    else:
                        await interaction.response.send_message("City switch failed. Try again.", ephemeral=True)
                except Exception:
                    pass

    class PanelView(discord.ui.View):
        """Button view for the one-city-at-a-time control panel."""

        def __init__(self, bot: "DiscordBot") -> None:
            super().__init__(timeout=None)
            self.bot = bot
            self.add_item(CitySelect(bot))

        async def update_panel(self, interaction: discord.Interaction, message: str | None = None) -> None:
            """Refresh the current panel message."""

            await self.bot.update_panel_message(interaction=interaction, status_override=message)

        async def _send_ephemeral(self, interaction: discord.Interaction, content: str) -> None:
            """Send ephemeral response safely whether interaction was deferred or not."""

            if interaction.response.is_done():
                await interaction.followup.send(content, ephemeral=True)
            else:
                await interaction.response.send_message(content, ephemeral=True)

        async def _log_callback_error(
            self,
            interaction: discord.Interaction,
            custom_id: str,
            action: str,
            exc: Exception,
            user_message: str,
        ) -> None:
            """Log callback exception details and return an ephemeral fallback message."""

            user_id = getattr(getattr(interaction, "user", None), "id", "unknown")
            _log_panel_event(
                "callback_error",
                (
                    f"custom_id={custom_id} user={user_id} action={action} "
                    f"exc_type={type(exc).__name__} exc={str(exc)[:200]}"
                ),
            )
            try:
                await self._send_ephemeral(interaction, user_message)
            except Exception:
                pass

        @discord.ui.button(label="Refresh Panel", style=discord.ButtonStyle.primary, custom_id="panel_refresh")
        async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            _log_panel_event("button_click", "refresh")
            try:
                if not interaction.response.is_done():
                    await interaction.response.defer()
                await self.bot.update_panel_message(
                    interaction=interaction,
                    refresh_source="button_refresh",
                    force_data_refresh=True,
                )
            except Exception as exc:
                await self._log_callback_error(interaction, "panel_refresh", "refresh", exc, "Refresh failed. Try again.")

        @discord.ui.button(label="Move Bottom", style=discord.ButtonStyle.secondary, custom_id="panel_move_bottom", row=1)
        async def move_bottom(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            _log_panel_event("button_click", "move_bottom")
            try:
                if not interaction.response.is_done():
                    await interaction.response.defer(ephemeral=True, thinking=True)
                moved = await self.bot.repost_panel_to_bottom()
                if moved:
                    await self._send_ephemeral(interaction, "Panel moved to channel bottom.")
                else:
                    await self._send_ephemeral(interaction, "Panel move failed.")
            except Exception as exc:
                await self._log_callback_error(interaction, "panel_move_bottom", "move_bottom", exc, "Panel move failed.")

        @discord.ui.button(label="Trade Setup", style=discord.ButtonStyle.success, custom_id="panel_trade_setup")
        async def trade_setup(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            _log_panel_event("button_click", "trade_setup")
            try:
                if not interaction.response.is_done():
                    await interaction.response.defer(ephemeral=True, thinking=True)
                setups = await _collect_trade_setup_snapshots([self.bot.state.active_city])
                message = _build_trade_setup_text(self.bot.state.active_city, setups)
                await self._send_ephemeral(interaction, f"```text\n{message[:1900]}\n```")
            except Exception as exc:
                await self._log_callback_error(
                    interaction,
                    "panel_trade_setup",
                    "trade_setup",
                    exc,
                    "Trade setup unavailable right now.",
                )

        @discord.ui.button(label="Check Positions", style=discord.ButtonStyle.success, custom_id="panel_positions")
        async def positions(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            _log_panel_event("button_click", "positions")
            try:
                if not interaction.response.is_done():
                    await interaction.response.defer(ephemeral=True, thinking=True)
                setups = await _collect_trade_setup_snapshots(list(DEFAULT_SIGNAL_CITIES))
                setup_map = {
                    _position_setup_key(item.market_slug, _normalize_side_token(item.side)): item
                    for item in setups
                }
                rows, fetch_error, wallet_target = _fetch_open_weather_positions(self.bot.config)
                if fetch_error is not None:
                    if fetch_error.startswith("wallet missing"):
                        user_error = (
                            "Positions unavailable: wallet missing. "
                            "Set POLY_USER_ADDRESS or config polymarket.user_address."
                        )
                    elif fetch_error.startswith("endpoint not resolved"):
                        user_error = (
                            "Positions unavailable: endpoint not resolved. "
                            "Optional env: POLYMARKET_POSITIONS_API_URL."
                        )
                    elif fetch_error.startswith("HTTP/API failure"):
                        user_error = f"Positions unavailable: {fetch_error}"
                    else:
                        user_error = f"Positions unavailable: {fetch_error}"
                    await self._send_ephemeral(interaction, user_error)
                    return
                if not rows:
                    await self._send_ephemeral(interaction, f"No open weather positions found for wallet {wallet_target}.")
                    return

                lines = [f"Open Positions (weather only) | wallet={wallet_target}"]
                shown = 0
                for row in rows:
                    slug = str(row.get("market_slug") or row.get("slug") or "")
                    side = _extract_position_side(row)
                    setup = setup_map.get(_position_setup_key(slug, side))
                    market_text = str(row.get("market") or row.get("title") or row.get("question") or slug)
                    city = _infer_city_from_text(market_text)
                    size = _safe_float(row.get("size") or row.get("amount") or row.get("position_size"), default=0.0)
                    avg_entry = _safe_float(row.get("avg_entry") or row.get("entry_price") or row.get("average_price"), default=0.0)
                    current_px = _safe_float(row.get("current_price") or row.get("mark_price") or row.get("price"), default=0.0)
                    unreal = _safe_float(row.get("unrealized_pnl") or row.get("pnl"), default=0.0)
                    fair = setup.model_prob_current if setup is not None else 0.0
                    tp = setup.take_profit_current if setup is not None else 0.0
                    sl = setup.stop_loss_current if setup is not None else 0.0
                    rec, rec_reason = _recommend_position_action(current_px, fair, tp, sl)
                    lines.append(
                        (
                            f"- {city.upper()} | {market_text[:32]} | {side} | size={size:.2f} "
                            f"entry={avg_entry:.3f} px={current_px:.3f} fair={fair:.3f} "
                            f"upnl={unreal:+.3f} tp={tp:.3f} sl={sl:.3f} | {rec} ({rec_reason})"
                        )
                    )
                    shown += 1
                    if shown >= 8:
                        break

                await self._send_ephemeral(interaction, f"```text\n{'\n'.join(lines)[:1900]}\n```")
            except Exception as exc:
                await self._log_callback_error(
                    interaction,
                    "panel_positions",
                    "positions",
                    exc,
                    "Positions check failed.",
                )

        @discord.ui.button(label="API Health", style=discord.ButtonStyle.secondary, custom_id="panel_api_health")
        async def api_health(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            _log_panel_event("button_click", "api_health")
            try:
                if not interaction.response.is_done():
                    await interaction.response.defer(ephemeral=True, thinking=True)
                summary, details = _collect_api_health(self.bot.config, force_refresh=True)
                lines = [f"API Health Summary: {summary}", *details]
                await self._send_ephemeral(interaction, f"```text\n{'\n'.join(lines)[:1900]}\n```")
            except Exception as exc:
                await self._log_callback_error(
                    interaction,
                    "panel_api_health",
                    "api_health",
                    exc,
                    "API health check unavailable.",
                )

        @discord.ui.button(label="Model Wants Open", style=discord.ButtonStyle.secondary, custom_id="panel_model_wants_open")
        async def model_wants_open(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            _log_panel_event("button_click", "model_wants_open")
            try:
                if not interaction.response.is_done():
                    await interaction.response.defer(ephemeral=True, thinking=True)
                setups = await _collect_trade_setup_snapshots(list(DEFAULT_SIGNAL_CITIES))
                open_positions, fetch_error, _wallet_target = _fetch_open_weather_positions(self.bot.config)
                open_keys: set[str] = set()
                if fetch_error is None:
                    for row in open_positions:
                        row_slug = str(row.get("market_slug") or row.get("slug") or "")
                        row_side = _extract_position_side(row)
                        open_keys.add(_position_setup_key(row_slug, row_side))

                candidates: list[SignalSnapshot] = []
                for setup in setups:
                    if _position_setup_key(setup.market_slug, _normalize_side_token(setup.side)) in open_keys:
                        continue
                    candidates.append(setup)

                candidates.sort(key=lambda item: item.executable_edge_current, reverse=True)
                if not candidates:
                    await self._send_ephemeral(interaction, "No model-open ideas right now.")
                    return

                lines = ["Model Wants Open (excluding open portfolio positions)"]
                for rank, setup in enumerate(candidates, start=1):
                    lines.append(
                        (
                            f"{rank}. {setup.city} {setup.side} px={setup.market_price_current:.3f} "
                            f"fair={setup.model_prob_current:.3f} edge={setup.executable_edge_current * 100.0:+.2f}% "
                            f"reason={setup.reason[:40]}"
                        )
                    )
                    if rank >= 10:
                        break
                await self._send_ephemeral(interaction, f"```text\n{'\n'.join(lines)[:1900]}\n```")
            except Exception as exc:
                await self._log_callback_error(
                    interaction,
                    "panel_model_wants_open",
                    "model_wants_open",
                    exc,
                    "Model-open view unavailable right now.",
                )


    class DiscordBot(commands.Bot):
        """Discord.py bot shell for the operator panel."""

        def __init__(self, token: str | None = None, quiet_ready_log: bool = False, reset_panel: bool = False) -> None:
            intents = discord.Intents.default()
            super().__init__(command_prefix="!", intents=intents)
            self.token = token
            self.quiet_ready_log = quiet_ready_log
            self.reset_panel = reset_panel
            self.commands_synced = False
            self.panel_message: discord.Message | None = None  # Track persistent panel message
            self.last_daily_run_date: datetime.date | None = None
            # Task hardening: track success/failure for observability
            self.last_daily_task_success_ts: str | None = None  # ISO format UTC timestamp
            self.last_daily_task_error_ts: str | None = None    # ISO format UTC timestamp
            self.last_daily_task_error_reason: str | None = None  # Error message/traceback
            self.last_panel_error: str | None = None
            self.panel_refresh_lock = asyncio.Lock()
            self.last_panel_render_signature: str | None = None
            self.panel_auto_refresh_interval_seconds = PANEL_AUTO_REFRESH_INTERVAL_SECONDS
            self.last_refresh_completed_ts: datetime | None = None
            self._cached_day_setups: dict[str, list[SignalSnapshot]] | None = None
            self._cached_api_summary: str | None = None
            self._cached_api_details: list[str] = []
            self._cached_wallet_target: str | None = None
            self.config = load_config()
            cities = list(self.config.get("cities", {}).keys())
            horizons = list(self.config.get("horizons", ["today", "tomorrow"]))
            self.state = PanelState(
                cities=cities or ["nyc"],
                horizons=horizons or ["today", "tomorrow"],
                mode=self.config.get("mode", "manual-only"),
                kill_switch=bool(self.config.get("kill_switch", False)),
            )
            self.panel_view = PanelView(self)

        async def ensure_panel_message(self) -> None:
            """Ensure a single persistent panel message exists in configured channel."""

            panel_channel_id_env = os.getenv("DISCORD_PANEL_CHANNEL_ID")
            if not panel_channel_id_env:
                return

            try:
                channel_id = int(panel_channel_id_env)
            except ValueError:
                return

            channel = self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except Exception:
                    return

            if not isinstance(channel, discord.abc.Messageable):
                return

            if self.panel_message is not None and self.panel_message.channel.id == channel_id:
                return

            existing: discord.Message | None = None
            if isinstance(channel, discord.TextChannel):
                # Prefer pinned panel first (restart-safe even in busy channels).
                try:
                    pinned: list[discord.Message] = []
                    async for pin_msg in channel.pins(limit=25):
                        pinned.append(pin_msg)
                except Exception:
                    pinned = []
                for msg in pinned:
                    has_panel_title = bool(msg.embeds) and (
                        msg.embeds[0].title in {
                            "Weather Bot Control Panel",
                            "Weather Bot Operational Panel",
                        }
                        or str(msg.embeds[0].title or "").startswith("Weather Bot Console")
                    )
                    has_legacy_text = msg.content.startswith("Weather Bot Panel") or msg.content.startswith("Weather Bot Control Panel")
                    if msg.author.id == self.user.id and (has_panel_title or has_legacy_text):
                        existing = msg
                        break

                async for msg in channel.history(limit=50):
                    if existing is not None:
                        break
                    has_panel_title = bool(msg.embeds) and (
                        msg.embeds[0].title in {
                            "Weather Bot Control Panel",
                            "Weather Bot Operational Panel",
                        }
                        or str(msg.embeds[0].title or "").startswith("Weather Bot Console")
                    )
                    has_legacy_text = msg.content.startswith("Weather Bot Panel") or msg.content.startswith("Weather Bot Control Panel")
                    if msg.author.id == self.user.id and (has_panel_title or has_legacy_text):
                        existing = msg
                        break

            if existing is not None and not self.reset_panel:
                self.panel_message = existing
                return

            if existing is not None and self.reset_panel:
                try:
                    await existing.delete()
                    _log_panel_event("panel_post", "deleted_old")
                except Exception:
                    pass

            self.panel_message = await channel.send(content="Loading panel...", view=self.panel_view)
            _log_panel_event("panel_post", "created_new")
            try:
                await self.panel_message.pin()
            except Exception:
                pass

        async def refresh(self, force: bool = False) -> None:
            """Refresh panel data snapshot and mark completion timestamp."""

            api_summary, api_details = _collect_api_health(self.config, force_refresh=force)
            day_setups = await _collect_city_setups_by_horizon(self.state.active_city)
            wallet_target, _wallet_source = _resolve_positions_wallet_target(self.config)

            self._cached_api_summary = api_summary
            self._cached_api_details = api_details
            self._cached_day_setups = day_setups
            self._cached_wallet_target = wallet_target
            self.last_refresh_completed_ts = _utc_now().replace(microsecond=0)
            _log_panel_event(
                "panel_data_refresh",
                (
                    f"force={str(force).lower()} completed={self.last_refresh_completed_ts.isoformat()} "
                    f"city={self.state.active_city}"
                ),
            )

        async def update_panel_message(
            self,
            interaction: discord.Interaction | None = None,
            status_override: str | None = None,
            refresh_source: str = "event",
            allow_skip_if_busy: bool = False,
            force_data_refresh: bool = False,
        ) -> bool:
            """Update the single panel message content/embed without reposting."""

            if allow_skip_if_busy and self.panel_refresh_lock.locked():
                _log_panel_event("panel_refresh_skip", f"source={refresh_source} reason=busy")
                return False

            async with self.panel_refresh_lock:
                return await self._update_panel_message_locked(
                    interaction=interaction,
                    status_override=status_override,
                    refresh_source=refresh_source,
                    force_data_refresh=force_data_refresh,
                )

        async def _update_panel_message_locked(
            self,
            interaction: discord.Interaction | None = None,
            status_override: str | None = None,
            refresh_source: str = "event",
            force_data_refresh: bool = False,
        ) -> bool:
            """Internal panel update assuming refresh lock is already held."""

            await self.ensure_panel_message()
            if self.panel_message is None:
                return False

            if force_data_refresh or self.last_refresh_completed_ts is None or self._cached_day_setups is None:
                await self.refresh(force=True)

            scheduler_health = status_override or _format_scheduler_health(self)
            refreshed_now = _utc_now().replace(microsecond=0)
            refreshed_at = (
                self.last_refresh_completed_ts.isoformat()
                if self.last_refresh_completed_ts is not None
                else refreshed_now.isoformat()
            )
            refreshed_base = self.last_refresh_completed_ts or refreshed_now
            refreshed_base_utc = refreshed_base.astimezone(timezone.utc)
            refreshed_local = refreshed_base_utc.astimezone(_rome_tz(refreshed_base_utc))
            refreshed_rendered = _format_rome_clock(refreshed_base_utc)
            _log_panel_event(
                "panel_time_diag",
                (
                    f"raw_lastrefreshcompletedts={refreshed_at} tz_label=Europe/Rome "
                    f"utc_derived={refreshed_base_utc.isoformat()} "
                    f"local_derived={refreshed_local.isoformat()} rendered={refreshed_rendered}"
                ),
            )
            api_summary = self._cached_api_summary or "api=unknown"
            api_details = list(self._cached_api_details)
            day_setups = self._cached_day_setups or {"today": [], "tomorrow": []}
            wallet_target = self._cached_wallet_target
            provider_lines = [line for line in api_details if line.startswith(("OpenWeather", "WeatherAPI", "Tomorrow"))]
            last_error = self.last_daily_task_error_reason or self.last_panel_error
            peak_lines = _collect_peak_timing_lines()
            _mark_runtime_refresh("panel", refreshed_now)

            data_age = _runtime_age_seconds("data", refreshed_now)
            market_age = _runtime_age_seconds("market", refreshed_now)
            weather_age = _runtime_age_seconds("weather", refreshed_now)
            positions_age = _runtime_age_seconds("positions", refreshed_now)

            _log_panel_event(
                "panel_refresh_clock",
                (
                    f"last_refresh={_format_rome_clock(refreshed_now)} "
                    f"data_age={_format_age_text(data_age)} market_age={_format_age_text(market_age)} "
                    f"weather_age={_format_age_text(weather_age)} positions_age={_format_age_text(positions_age)}"
                ),
            )

            embed = build_panel_embed(
                self.state,
                self.config,
                status_text=scheduler_health,
                api_health_text=api_summary,
                refresh_ts=refreshed_at,
                day_setups=day_setups,
                provider_lines=provider_lines,
                wallet_target=wallet_target,
                last_error=last_error,
                data_age_seconds=data_age,
                market_age_seconds=market_age,
                weather_age_seconds=weather_age,
                positions_age_seconds=positions_age,
                peak_lines=peak_lines,
                panel_loop_interval_seconds=self.panel_auto_refresh_interval_seconds,
            )

            render_signature = json.dumps(embed.to_dict(), sort_keys=True, ensure_ascii=True)
            if interaction is None and render_signature == self.last_panel_render_signature:
                _log_panel_event("panel_refresh_dedupe", f"source={refresh_source} unchanged=true")
                return False

            if interaction is not None:
                if not interaction.response.is_done():
                    await interaction.response.edit_message(content=None, embed=embed, view=self.panel_view)
                else:
                    target_msg = interaction.message if interaction.message is not None else self.panel_message
                    await target_msg.edit(content=None, embed=embed, view=self.panel_view)
                self.last_panel_render_signature = render_signature
                return True
            await self.panel_message.edit(content=None, embed=embed, view=self.panel_view)
            self.last_panel_render_signature = render_signature
            return True

        @tasks.loop(seconds=PANEL_AUTO_REFRESH_INTERVAL_SECONDS)
        async def panel_auto_refresh_task(self) -> None:
            """Refresh panel on fixed cadence without posting additional messages."""

            try:
                now_utc = _utc_now()
                if self.last_refresh_completed_ts is None:
                    stale_age = None
                else:
                    stale_age = max(0, int((now_utc - self.last_refresh_completed_ts).total_seconds()))

                refresh_called = self.last_refresh_completed_ts is None or (stale_age is not None and stale_age > 70)
                mode = "data_refresh" if refresh_called else "edit_only"
                edited = await self.update_panel_message(
                    refresh_source="auto_loop",
                    allow_skip_if_busy=True,
                    force_data_refresh=refresh_called,
                )

                stale_age_text = "n/a" if stale_age is None else f"{stale_age}s"
                last_refresh_text = (
                    self.last_refresh_completed_ts.isoformat()
                    if self.last_refresh_completed_ts is not None
                    else "n/a"
                )
                _log_panel_event(
                    "panel_auto_refresh",
                    (
                        f"mode={mode} interval={self.panel_auto_refresh_interval_seconds}s "
                        f"stale_age={stale_age_text} refresh_called={'yes' if refresh_called else 'no'} "
                        f"edit_only={'no' if refresh_called else 'yes'} "
                        f"edited={'yes' if edited else 'no'} "
                        f"lastrefreshcompletedts={last_refresh_text}"
                    ),
                )
            except Exception as exc:
                self.last_panel_error = f"panel_auto_refresh_failed={type(exc).__name__}"
                _log_panel_event(
                    "panel_auto_refresh",
                    f"tick=error interval={self.panel_auto_refresh_interval_seconds}s error={type(exc).__name__}",
                )

        @panel_auto_refresh_task.before_loop
        async def before_panel_auto_refresh_task(self) -> None:
            """Wait until client is ready before panel auto-refresh starts."""

            await self.wait_until_ready()

        async def setup_hook(self) -> None:
            """Register static panel commands."""

            # Register persistent view handlers before gateway events so panel buttons
            # continue to work across process restarts.
            self.add_view(self.panel_view)
            _log_panel_event("persistent_views_registered", "panel_view")
            self.tree.add_command(self._panel_command())
            self.tree.add_command(self._panel_bottom_command())
            self.tree.add_command(self._policy_signals_command())
            self.tree.add_command(self._performance_command())

        def _panel_command(self) -> app_commands.Command:
            """Build the slash command that renders the panel."""

            async def panel(interaction: discord.Interaction) -> None:
                try:
                    if not interaction.response.is_done():
                        await interaction.response.defer(ephemeral=True, thinking=True)
                    await self.update_panel_message()
                    await interaction.followup.send("Panel aggiornato (messaggio unico).", ephemeral=True)
                except Exception as exc:
                    self.last_panel_error = f"panel_update_failed={type(exc).__name__}"
                    _log_panel_event("panel_update_error", self.last_panel_error)
                    if interaction.response.is_done():
                        await interaction.followup.send("Panel update failed.", ephemeral=True)
                    else:
                        await interaction.response.send_message("Panel update failed.", ephemeral=True)

            return app_commands.Command(name="panel", description="Show the weather bot control panel", callback=panel)

        def _panel_bottom_command(self) -> app_commands.Command:
            """Build slash command that moves panel to bottom and keeps single-panel invariant."""

            async def panel_bottom(interaction: discord.Interaction) -> None:
                try:
                    if not interaction.response.is_done():
                        await interaction.response.defer(ephemeral=True, thinking=True)
                    moved = await self.repost_panel_to_bottom()
                    if moved:
                        await interaction.followup.send("Panel moved to channel bottom.", ephemeral=True)
                    else:
                        await interaction.followup.send("Panel move failed.", ephemeral=True)
                except Exception as exc:
                    self.last_panel_error = f"panel_bottom_failed={type(exc).__name__}"
                    _log_panel_event("panel_bottom_error", self.last_panel_error)
                    if interaction.response.is_done():
                        await interaction.followup.send("Panel move failed.", ephemeral=True)
                    else:
                        await interaction.response.send_message("Panel move failed.", ephemeral=True)

            return app_commands.Command(
                name="panel_bottom",
                description="Move panel to bottom and keep one persistent panel",
                callback=panel_bottom,
            )

        def _policy_signals_command(self) -> app_commands.Command:
            """Build slash command that posts dry-run policy signals for all core cities."""

            async def policy_signals(interaction: discord.Interaction) -> None:
                await interaction.response.defer(thinking=True)

                city_list = list(DEFAULT_SIGNAL_CITIES)
                try:
                    # Must complete before Discord 3s timeout; use 2.5s limit to leave margin
                    blocks, summary = await asyncio.wait_for(
                        run_policy_dry_run(city_list),
                        timeout=2.5
                    )
                except asyncio.TimeoutError:
                    await interaction.followup.send("Policy dry-run timed out (>2.5s); retry with fewer cities or try later.")
                    _log_panel_event("policy_signals", "discord_timeout")
                    return
                except RuntimeError as exc:
                    error_preview = str(exc).strip() or "policy dry-run failed"
                    await interaction.followup.send(f"Policy dry-run failed: {error_preview}")
                    return

                posted = 0
                for block in blocks:
                    if block.primary_candidate == "NONE":
                        continue
                    snapshot = _record_signal_plan_and_update(
                        block,
                        refresh_source="slash_policy_signals",
                        update_reason="manual_refresh",
                    )
                    await interaction.followup.send(embed=build_signal_embed(block, snapshot=snapshot))
                    state, score = _extract_signal_state_score(block.discord_preview)
                    _append_signal_audit_row(
                        city=block.city.upper(),
                        state=state,
                        candidate=block.primary_candidate,
                        score=score,
                        posted=True,
                    )
                    posted += 1

                if posted == 0:
                    await interaction.followup.send("No actionable signal right now for configured cities.")
                    _append_signal_audit_row(
                        city="ALL",
                        state="NO_SIGNAL",
                        candidate="NONE",
                        score="n/a",
                        posted=False,
                    )
                    _log_panel_event("policy_signals", "no_signal")

                if summary is None:
                    summary_lines = [
                        "Daily Policy Summary",
                        f"total_cities={len(blocks)}",
                        f"cities_with_signal={posted}",
                        "top_signal_city=NONE",
                        "top_signal_candidate=NONE",
                    ]
                else:
                    summary_lines = [
                        "Daily Policy Summary",
                        f"total_cities={summary.total_cities}",
                        f"cities_with_signal={summary.cities_with_signal}",
                        f"top_signal_city={summary.top_signal_city}",
                        f"top_signal_candidate={summary.top_signal_candidate}",
                    ]
                await interaction.followup.send("\n".join(summary_lines))
                _log_panel_event("policy_signals", f"posted={posted}")

            return app_commands.Command(
                name="policy_signals",
                description="Post dry-run policy signals for nyc/atlanta/chicago/dallas",
                callback=policy_signals,
            )

        def _performance_command(self) -> app_commands.Command:
            """Build slash command that shows compact policy-performance report."""

            async def performance(interaction: discord.Interaction) -> None:
                try:
                    message = _show_performance_report_cached()
                    await interaction.response.send_message(message, ephemeral=True)
                except Exception as exc:
                    _log_panel_event("command_error", f"performance={exc}")
                    await interaction.response.send_message(
                        "❌ Report non disponibile (errore interno).",
                        ephemeral=True,
                    )

            return app_commands.Command(
                name="performance",
                description="Show policy performance summary from report CSVs",
                callback=performance,
            )

        async def on_ready(self) -> None:
            """Report readiness and post persistent panel."""

            if not self.quiet_ready_log:
                print(f"Discord bot ready: {self.user}")
            if self.commands_synced:
                if not self.daily_signal_task.is_running():
                    self.daily_signal_task.start()
                    next_iteration = self.daily_signal_task.next_iteration
                    now_utc = _utc_now()
                    _log_panel_event(
                        "daily_task_boot",
                        (
                            f"startup_utc={now_utc.isoformat()} "
                            f"next_trigger_utc={(next_iteration.isoformat() if next_iteration else 'none')}"
                        ),
                    )
                    _log_panel_event("daily_task", "started")
                if not self.panel_auto_refresh_task.is_running():
                    self.panel_auto_refresh_task.start()
                    _log_panel_event("panel_auto_refresh", f"started interval={self.panel_auto_refresh_interval_seconds}s")
                return

            guild_env = os.getenv("DISCORD_GUILD_ID")
            try:
                if guild_env:
                    guild_obj = discord.Object(id=int(guild_env))
                    await self.tree.sync(guild=guild_obj)
                else:
                    await self.tree.sync()
                self.commands_synced = True
                if not self.quiet_ready_log:
                    print("Discord commands synced.")
            except Exception as exc:
                if not self.quiet_ready_log:
                    print(f"Discord command sync failed: {exc}")

            # Post persistent panel if configured
            try:
                await self.post_persistent_panel()
            except Exception as exc:
                if not self.quiet_ready_log:
                    print(f"Failed to post persistent panel: {exc}")

            try:
                await self._self_check_signal_channel_permissions()
            except Exception as exc:
                _log_panel_event("permission_check", f"failed={exc}")

            if not self.daily_signal_task.is_running():
                self.daily_signal_task.start()
                next_iteration = self.daily_signal_task.next_iteration
                now_utc = _utc_now()
                _log_panel_event(
                    "daily_task_boot",
                    (
                        f"startup_utc={now_utc.isoformat()} "
                        f"next_trigger_utc={(next_iteration.isoformat() if next_iteration else 'none')}"
                    ),
                )
                _log_panel_event("daily_task", "started")
            if not self.panel_auto_refresh_task.is_running():
                self.panel_auto_refresh_task.start()
                _log_panel_event("panel_auto_refresh", f"started interval={self.panel_auto_refresh_interval_seconds}s")

        async def _run_daily_signal_once(self, trigger_source: str) -> None:
            """Run one daily signal posting cycle with explicit branch diagnostics."""

            now_utc = _utc_now()
            if self.last_daily_run_date == now_utc.date():
                _log_panel_event(
                    "daily_task",
                    f"wakeup_utc={now_utc.isoformat()} source={trigger_source} skipped_already_ran_today",
                )
                return

            channel_id = _resolve_scheduler_channel_id(self.config)
            if channel_id is None:
                _log_panel_event(
                    "daily_task",
                    f"wakeup_utc={now_utc.isoformat()} source={trigger_source} skipped_missing_channel",
                )
                return

            channel = self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except Exception as exc:
                    _log_panel_event(
                        "daily_task",
                        (
                            f"wakeup_utc={now_utc.isoformat()} source={trigger_source} "
                            f"channel_resolve_failed={exc}"
                        ),
                    )
                    return

            if channel is None or not isinstance(channel, discord.abc.Messageable):
                _log_panel_event(
                    "daily_task",
                    f"wakeup_utc={now_utc.isoformat()} source={trigger_source} channel_not_messageable",
                )
                return

            try:
                posted, no_signal = await self._post_policy_signals_to_channel(
                    channel,
                    trigger_source=trigger_source,
                    run_timestamp_utc=now_utc,
                )
                self.last_daily_run_date = now_utc.date()
                if no_signal:
                    _log_panel_event(
                        "daily_task",
                        f"wakeup_utc={now_utc.isoformat()} source={trigger_source} no_signal",
                    )
                else:
                    _log_panel_event(
                        "daily_task",
                        f"wakeup_utc={now_utc.isoformat()} source={trigger_source} posted={posted}",
                    )
            except Exception as exc:
                _append_scheduler_run_row(
                    run_timestamp_utc=now_utc,
                    city="ALL",
                    source=trigger_source,
                    outcome="failed",
                    reason=str(exc),
                )
                _log_panel_event(
                    "daily_task",
                    f"wakeup_utc={now_utc.isoformat()} source={trigger_source} failed={exc}",
                )

        async def _self_check_signal_channel_permissions(self) -> None:
            """Log minimal send/history permissions for the configured signal channel."""

            channel_id = _resolve_scheduler_channel_id(self.config)
            if channel_id is None:
                _log_panel_event("permission_check", "skipped_missing_channel")
                return

            channel = self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except Exception as exc:
                    _log_panel_event("permission_check", f"channel_resolve_failed={exc}")
                    return

            if channel is None or not isinstance(channel, discord.abc.GuildChannel):
                _log_panel_event(
                    "permission_check",
                    f"channel={channel_id} view_channel=True send_messages=True read_message_history=True",
                )
                return

            me = channel.guild.me
            if me is None and self.user is not None:
                me = channel.guild.get_member(self.user.id)
            if me is None and self.user is not None:
                try:
                    me = await channel.guild.fetch_member(self.user.id)
                except Exception as exc:
                    _log_panel_event("permission_check", f"member_resolve_failed={exc}")
                    return

            if me is None:
                _log_panel_event("permission_check", "member_unavailable")
                return

            perms = channel.permissions_for(me)
            permission_status = {
                "view_channel": bool(getattr(perms, "view_channel", False)),
                "send_messages": bool(getattr(perms, "send_messages", False)),
                "read_message_history": bool(getattr(perms, "read_message_history", False)),
            }
            _log_panel_event(
                "permission_check",
                (
                    f"channel={channel_id} "
                    f"view_channel={permission_status['view_channel']} "
                    f"send_messages={permission_status['send_messages']} "
                    f"read_message_history={permission_status['read_message_history']}"
                ),
            )

            missing = [name for name, allowed in permission_status.items() if not allowed]
            if missing:
                _log_panel_event("permission_check_missing", ",".join(missing))

        async def _post_policy_signals_to_channel(
            self,
            channel: discord.abc.Messageable,
            trigger_source: str,
            run_timestamp_utc: datetime,
        ) -> tuple[int, bool]:
            """Post policy dry-run blocks to a channel and return (posted_count, no_signal)."""

            blocks, summary = await run_policy_dry_run(list(DEFAULT_SIGNAL_CITIES))

            posted = 0
            for block in blocks:
                if block.primary_candidate == "NONE":
                    _append_scheduler_run_row(
                        run_timestamp_utc=run_timestamp_utc,
                        city=block.city.upper(),
                        source=trigger_source,
                        outcome="no_signal",
                        reason="primary_candidate_none",
                    )
                    continue
                try:
                    snapshot = _record_signal_plan_and_update(
                        block,
                        refresh_source=trigger_source,
                        update_reason="scheduled_refresh",
                    )
                    await channel.send(embed=build_signal_embed(block, snapshot=snapshot))
                except Exception as exc:
                    _append_scheduler_run_row(
                        run_timestamp_utc=run_timestamp_utc,
                        city=block.city.upper(),
                        source=trigger_source,
                        outcome="failed",
                        reason=f"send_embed_failed:{exc}",
                    )
                    raise
                state, score = _extract_signal_state_score(block.discord_preview)
                _append_signal_audit_row(
                    city=block.city.upper(),
                    state=state,
                    candidate=block.primary_candidate,
                    score=score,
                    posted=True,
                )
                _append_scheduler_run_row(
                    run_timestamp_utc=run_timestamp_utc,
                    city=block.city.upper(),
                    source=trigger_source,
                    outcome="posted",
                    reason=block.primary_candidate,
                )
                posted += 1

            no_signal = posted == 0
            if no_signal:
                await channel.send("No actionable signal right now for configured cities.")
                _append_signal_audit_row(
                    city="ALL",
                    state="NO_SIGNAL",
                    candidate="NONE",
                    score="n/a",
                    posted=False,
                )
                if not blocks:
                    _append_scheduler_run_row(
                        run_timestamp_utc=run_timestamp_utc,
                        city="ALL",
                        source=trigger_source,
                        outcome="no_signal",
                        reason="no_blocks_from_policy",
                    )

            if summary is None:
                summary_lines = [
                    "Daily Policy Summary",
                    f"total_cities={len(blocks)}",
                    f"cities_with_signal={posted}",
                    "top_signal_city=NONE",
                    "top_signal_candidate=NONE",
                ]
            else:
                summary_lines = [
                    "Daily Policy Summary",
                    f"total_cities={summary.total_cities}",
                    f"cities_with_signal={summary.cities_with_signal}",
                    f"top_signal_city={summary.top_signal_city}",
                    f"top_signal_candidate={summary.top_signal_candidate}",
                ]
            await channel.send("\n".join(summary_lines))
            return posted, no_signal

        async def post_persistent_panel(self) -> None:
            """Post or update the persistent control panel in the configured channel."""

            try:
                await self.ensure_panel_message()
                await self.update_panel_message()
                if self.panel_message is not None:
                    _log_panel_event("panel_post", "updated_existing")
            except Exception as exc:
                self.last_panel_error = f"post_persistent_panel_failed={type(exc).__name__}"
                if not self.quiet_ready_log:
                    print(f"Failed to post panel: {exc}")

        async def repost_panel_to_bottom(self) -> bool:
            """Recreate the panel at channel bottom while keeping exactly one panel message."""

            await self.ensure_panel_message()
            if self.panel_message is None:
                return False

            old_panel = self.panel_message
            channel = old_panel.channel
            if not isinstance(channel, discord.abc.Messageable):
                return False

            try:
                self.panel_message = await channel.send(content="Loading panel...", view=self.panel_view)
                _log_panel_event("panel_post", "created_new_bottom")
                await self.update_panel_message()
                try:
                    await self.panel_message.pin()
                except Exception:
                    pass
                try:
                    await old_panel.delete()
                    _log_panel_event("panel_post", "deleted_old_after_move")
                except Exception:
                    pass
                return True
            except Exception as exc:
                self.last_panel_error = f"repost_panel_failed={type(exc).__name__}"
                _log_panel_event("panel_post_error", self.last_panel_error)
                return False

        @tasks.loop(time=dtime(hour=DAILY_TRIGGER_HOUR_UTC, minute=DAILY_TRIGGER_MINUTE_UTC, tzinfo=timezone.utc))
        async def daily_signal_task(self) -> None:
            """Post one daily policy signal batch using the connected bot client (with error resilience)."""
            now_utc = _utc_now()
            try:
                next_iteration = self.daily_signal_task.next_iteration
                _log_panel_event(
                    "daily_task_wakeup",
                    (
                        f"current_utc={now_utc.isoformat()} "
                        f"next_trigger_utc={(next_iteration.isoformat() if next_iteration else 'none')}"
                    ),
                )
                await self._run_daily_signal_once(trigger_source="scheduled_loop")
                # Update success timestamp after successful run
                self.last_daily_task_success_ts = now_utc.isoformat()
            except Exception as exc:
                # Capture error state for observability
                exc_str = f"{type(exc).__name__}: {str(exc)[:100]}"
                self.last_daily_task_error_ts = now_utc.isoformat()
                self.last_daily_task_error_reason = exc_str
                _log_panel_event("daily_task_exception", f"error_ts={self.last_daily_task_error_ts} reason={exc_str}")
                # Re-raise so @daily_signal_task.error handler can attempt restart
                raise

        @daily_signal_task.before_loop
        async def before_daily_signal_task(self) -> None:
            """Wait for full client readiness before starting daily task loop."""
            boot_utc = _utc_now()
            _log_panel_event("daily_task_before_loop", f"wait_until_ready_start_utc={boot_utc.isoformat()}")
            await self.wait_until_ready()
            ready_utc = _utc_now()
            next_trigger_utc = _next_daily_trigger_utc(ready_utc)
            _log_panel_event(
                "daily_task_before_loop",
                (
                    f"wait_until_ready_done_utc={ready_utc.isoformat()} "
                    f"next_trigger_utc={next_trigger_utc.isoformat()}"
                ),
            )

            # Reliability catch-up: if readiness happens after today's scheduled minute,
            # execute once immediately so the daily run is not missed.
            today_trigger_utc = ready_utc.replace(
                hour=DAILY_TRIGGER_HOUR_UTC,
                minute=DAILY_TRIGGER_MINUTE_UTC,
                second=0,
                microsecond=0,
            )
            if ready_utc >= today_trigger_utc and self.last_daily_run_date != ready_utc.date():
                _log_panel_event(
                    "daily_task_before_loop",
                    (
                        f"startup_after_target_utc={ready_utc.isoformat()} "
                        f"trigger_utc={today_trigger_utc.isoformat()} catchup_run=true"
                    ),
                )
                await self._run_daily_signal_once(trigger_source="startup_catchup")

        @daily_signal_task.error
        async def daily_signal_task_error(self, exc: Exception) -> None:
            """Handle task-level errors: log, update state, attempt restart."""
            error_ts = _utc_now().isoformat()
            error_reason = f"{type(exc).__name__}: {str(exc)[:200]}"
            
            # Ensure state is updated (in case error happened before assignment in task)
            if self.last_daily_task_error_ts is None or self.last_daily_task_error_ts < error_ts:
                self.last_daily_task_error_ts = error_ts
                self.last_daily_task_error_reason = error_reason
            
            _log_panel_event(
                "daily_task_error_handler",
                (
                    f"error_ts={error_ts} "
                    f"reason={error_reason} "
                    f"loop_running={self.daily_signal_task.is_running()}"
                ),
            )
            
            # Attempt restart if loop is not running (resilience)
            if not self.daily_signal_task.is_running():
                _log_panel_event("daily_task_restart_attempt", f"restarting_loop_at={error_ts}")
                try:
                    self.daily_signal_task.restart()
                    _log_panel_event("daily_task_restart_attempt", "restart_success")
                except Exception as restart_exc:
                    _log_panel_event(
                        "daily_task_restart_failed",
                        f"reason={type(restart_exc).__name__}: {str(restart_exc)[:100]}",
                    )

        async def start_bot(self) -> None:
            """Start the bot if a token is configured."""

            if not self.token:
                raise RuntimeError("Discord token is not configured.")
            await self.start(self.token)


else:

    class DiscordBot:  # type: ignore[no-redef]
        """Fallback stub when discord.py is unavailable."""

        def __init__(self, token: str | None = None, quiet_ready_log: bool = False, reset_panel: bool = False) -> None:
            self.token = token
            self.quiet_ready_log = quiet_ready_log
            self.reset_panel = reset_panel


def _resolve_scheduler_token(config: dict[str, Any]) -> str | None:
    """Resolve Discord token from config/env for scheduler mode."""

    discord_cfg = config.get("discord", {}) if isinstance(config.get("discord"), dict) else {}
    token_env_name = str(discord_cfg.get("token_env", "DISCORD_BOT_TOKEN"))
    token = os.getenv(token_env_name)
    if token:
        return token.strip()
    direct = discord_cfg.get("token")
    if direct is None:
        return None
    direct_text = str(direct).strip()
    return direct_text or None


def _resolve_scheduler_channel_id(config: dict[str, Any], channel_override: int | None = None) -> int | None:
    """Resolve target Discord channel id from CLI/config/env."""

    if channel_override is not None:
        return channel_override

    discord_cfg = config.get("discord", {}) if isinstance(config.get("discord"), dict) else {}
    env_name = str(discord_cfg.get("signal_channel_id_env", "DISCORD_SIGNAL_CHANNEL_ID"))
    env_value = os.getenv(env_name)
    if env_value:
        try:
            return int(env_value.strip())
        except ValueError:
            return None

    direct_value = discord_cfg.get("signal_channel_id")
    if direct_value is None:
        return None
    try:
        return int(str(direct_value).strip())
    except ValueError:
        return None


async def _post_daily_signals_once(channel_id_override: int | None = None) -> tuple[str, str, str]:
    """Post one daily multi-city signal batch and return compact run summary."""

    if DISCORD_IMPORT_ERROR is not None:
        return "FAILED", "discord_import", DISCORD_IMPORT_ERROR

    failure_stage = "config_load"
    config = load_config()
    failure_stage = "token_config_loaded"
    token = _resolve_scheduler_token(config)
    channel_id = _resolve_scheduler_channel_id(config, channel_override=channel_id_override)
    city_list = list(DEFAULT_SIGNAL_CITIES)

    if not token or not channel_id:
        return "FAILED", "token_config", "missing token or signal channel id"

    try:
        failure_stage = "policy_dry_run"
        blocks, summary = await run_policy_dry_run(city_list)
    except RuntimeError as exc:
        return "FAILED", failure_stage, str(exc)

    # Use a simple Client for scheduler mode
    # Use default intents only - some intents require library-wide declaration
    intents = discord.Intents.default()
    # Only request message content if needed
    intents.message_content = False
    client = discord.Client(intents=intents)

    connect_task = None
    try:
        failure_stage = "discord_login"
        
        # Use login() separately and then connect()
        await client.login(token)
        
        try:
            # Start connect in background
            connect_task = asyncio.create_task(client.connect(reconnect=False))
            
            # Wait for client to be ready (user populated)
            for i in range(300):  # up to 30 seconds
                if client.user is not None:
                    break
                await asyncio.sleep(0.1)
            
            if client.user is None:
                if connect_task:
                    connect_task.cancel()
                raise RuntimeError("client failed to become ready (timeout 30s)")
            
        except asyncio.TimeoutError:
            raise RuntimeError("timeout during client.connect()")
        
        if client.user is None:
            raise RuntimeError("client.user is None after connect")
            
    except asyncio.CancelledError:
        return "FAILED", failure_stage, "login cancelled"
    except discord.errors.LoginFailure as exc:
        return "FAILED", failure_stage, f"login failed: {str(exc)}"
    except Exception as exc:
        try:
            await client.close()
        except Exception:
            pass
        return "FAILED", failure_stage, str(exc)

    try:
        failure_stage = "channel_resolve"
        # Fetch channel using the logged-in client
        try:
            channel = await client.fetch_channel(channel_id)
        except Exception as fetch_exc:
            return "FAILED", failure_stage, f"failed to fetch channel: {str(fetch_exc)}"
        
        if channel is None or not isinstance(channel, discord.abc.Messageable):
            return "FAILED", failure_stage, "target channel not found or not messageable"

        for block in blocks:
            if block.primary_candidate == "NONE":
                continue
            failure_stage = "send_message"
            snapshot = _record_signal_plan_and_update(
                block,
                refresh_source="one_shot_cli",
                update_reason="manual_post_daily_signals",
            )
            await channel.send(embed=build_signal_embed(block, snapshot=snapshot))
            state, score = _extract_signal_state_score(block.discord_preview)
            _append_signal_audit_row(
                city=block.city.upper(),
                state=state,
                candidate=block.primary_candidate,
                score=score,
                posted=True,
            )
            _log_panel_event("one_shot_post", f"city={block.city} candidate={block.primary_candidate}")

        if not any(block.primary_candidate != "NONE" for block in blocks):
            failure_stage = "send_message"
            await channel.send("No actionable signal right now for configured cities.")
            _append_signal_audit_row(
                city="ALL",
                state="NO_SIGNAL",
                candidate="NONE",
                score="n/a",
                posted=False,
            )
            _log_panel_event("one_shot_post", "no_signal")

        if summary is None:
            summary_text = (
                f"total_cities={len(blocks)}; cities_with_signal=0; "
                "top_signal_city=NONE; top_signal_candidate=NONE"
            )
        else:
            summary_text = (
                f"total_cities={summary.total_cities}; cities_with_signal={summary.cities_with_signal}; "
                f"top_signal_city={summary.top_signal_city}; top_signal_candidate={summary.top_signal_candidate}"
            )
        failure_stage = "send_message"
        await channel.send(
            "\n".join(
                [
                    "Daily Policy Summary",
                    summary_text.replace("; ", "\n"),
                ]
            )
        )
        _log_panel_event("one_shot_post", "summary_sent")
        return "SUCCESS", "NONE", "NONE"
    except Exception as exc:
        return "FAILED", failure_stage, str(exc)
    finally:
        try:
            await client.close()
        except Exception:
            pass
        if connect_task is not None and not connect_task.done():
            connect_task.cancel()


def _parse_cli_args() -> argparse.Namespace:
    """Parse CLI options for scheduler-ready one-shot run."""

    parser = argparse.ArgumentParser(description="Discord bot entrypoint")
    parser.add_argument(
        "--post-daily-signals",
        action="store_true",
        help="Run one-shot daily multi-city signal posting for scheduler use.",
    )
    parser.add_argument(
        "--channel-id",
        type=int,
        default=None,
        help="Optional channel id override for one-shot posting.",
    )
    parser.add_argument(
        "--run-console",
        action="store_true",
        help="Run persistent Discord operator console (slash commands + panel).",
    )
    parser.add_argument(
        "--reset-panel",
        action="store_true",
        help="Delete and recreate the control panel (for UI updates).",
    )
    return parser.parse_args()


async def _run_operator_console() -> int:
    """Run the persistent Discord operator console bot."""

    if DISCORD_IMPORT_ERROR is not None:
        print(f"run_status=FAILED")
        print(f"failure_stage=discord_import")
        print(f"failure_reason={DISCORD_IMPORT_ERROR}")
        return 1

    config = load_config()
    token = _resolve_scheduler_token(config)
    if not token:
        print("run_status=FAILED")
        print("failure_stage=token_config")
        print("failure_reason=missing token")
        return 1

    args = _parse_cli_args()
    bot = DiscordBot(token=token, quiet_ready_log=False, reset_panel=args.reset_panel)
    await bot.start_bot()
    return 0


def main() -> int:
    """CLI entrypoint for scheduler-ready one-shot signal posting."""

    args = _parse_cli_args()
    if args.run_console:
        return asyncio.run(_run_operator_console())

    if not args.post_daily_signals:
        return 0

    run_status, failure_stage, failure_reason = asyncio.run(
        _post_daily_signals_once(channel_id_override=args.channel_id)
    )

    print(f"run_status={run_status}")
    print(f"failure_stage={failure_stage}")
    print(f"failure_reason={failure_reason}")
    return 0 if run_status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
