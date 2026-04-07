"""Manual runner for final signal policy on real executable candidates."""

from __future__ import annotations

import argparse
import datetime as dt
from dataclasses import asdict
import json
import math
import sys
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
from src.engine.signal_policy import SignalPolicyConfig, SignalPolicyInputRow, apply_signal_policy, legacy_signal_policy_config
from src.engine.weather_aggregate import aggregate_forecasts
from src.engine.weather_probability import build_temperature_bin_probabilities
from scripts.test_clob_evaluator import _city_slug_prefix, fetch_quotes_for_metadata, resolve_event_markets_for_city_day


CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
SUPPORTED_HORIZONS = ("today", "tomorrow")


def load_config(path: Path) -> dict[str, Any]:
    """Load JSON-compatible config."""

    return json.loads(path.read_text(encoding="utf-8"))


def parse_args(default_city: str) -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description="Run final signal policy test on real event ladders")
    parser.add_argument("--city", default=default_city, help="City key")
    parser.add_argument(
        "--cities",
        default=None,
        help="Optional comma-separated city keys for batch dry-run mode.",
    )
    parser.add_argument("--horizon", default="today", help="today or tomorrow")
    parser.add_argument("--target-date", default=None, help="Optional UTC target date YYYY-MM-DD")
    parser.add_argument("--gamma-limit", type=int, default=3000, help="How many events to scan in fallback")
    parser.add_argument("--min-abs-edge", type=float, default=0.01, help="Signal pre-filter min abs edge")
    parser.add_argument("--min-exec-edge", type=float, default=0.0, help="Executable min edge")
    parser.add_argument("--max-spread", type=float, default=None, help="Optional max spread at executable layer")
    parser.add_argument("--min-size", type=float, default=0.0, help="Min available size at executable layer")
    parser.add_argument(
        "--snapshot-out",
        default=None,
        help="Optional output path for policy snapshot JSON. If omitted, no snapshot is written.",
    )
    parser.add_argument(
        "--snapshot-dir",
        default="output/policy_snapshots",
        help="Default snapshot directory used when --snapshot-out is omitted but --save-snapshot is enabled.",
    )
    parser.add_argument(
        "--save-snapshot",
        action="store_true",
        help="Save snapshot to snapshot-dir with deterministic filename for this run.",
    )
    parser.add_argument(
        "--replay-snapshot",
        default=None,
        help="Replay policy from saved snapshot JSON without live quote fetching.",
    )
    parser.add_argument(
        "--discord-dry-run",
        action="store_true",
        help="Print only compact Discord-safe preview fields from final policy rows.",
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


def _parse_target_date(value: str | None) -> dt.date | None:
    """Parse optional target date from CLI."""

    if value is None or not str(value).strip():
        return None
    return dt.date.fromisoformat(str(value).strip())


def _snapshot_default_path(snapshot_dir: str, city_key: str, target_date: str, event_slug: str) -> Path:
    """Build deterministic snapshot filename for one run."""

    safe_event = "-".join(event_slug.strip().split()) if event_slug.strip() else "unknown-event"
    safe_event = safe_event.replace("/", "-")
    filename = f"policy_snapshot_{city_key}_{target_date}_{safe_event}.json"
    return Path(snapshot_dir) / filename


def _policy_input_from_dict(payload: Mapping[str, Any]) -> SignalPolicyInputRow:
    """Deserialize one snapshot policy input row."""

    return SignalPolicyInputRow(
        city=str(payload.get("city", "")),
        target_date=str(payload.get("target_date", "")),
        event_slug=str(payload.get("event_slug", "")),
        range_label=str(payload.get("range_label", "")),
        side=str(payload.get("side", "BUY_YES")),
        model_probability=float(payload.get("model_probability", 0.0)),
        market_probability=float(payload.get("market_probability", 0.0)),
        probability_edge=float(payload.get("probability_edge", 0.0)),
        abs_edge=float(payload.get("abs_edge", 0.0)),
        executable_edge=(None if payload.get("executable_edge") is None else float(payload.get("executable_edge"))),
        entry_price=(None if payload.get("entry_price") is None else float(payload.get("entry_price"))),
        spread=(None if payload.get("spread") is None else float(payload.get("spread"))),
        available_size=(None if payload.get("available_size") is None else float(payload.get("available_size"))),
        rank=int(payload.get("rank", 0)),
    )


def _print_replay_diff(snapshot: Mapping[str, Any], recomputed_final: Any) -> None:
    """Print score/state/reason diff between saved snapshot and recomputed policy."""

    saved_rows = snapshot.get("policy", {}).get("final_rows", [])
    if not isinstance(saved_rows, list):
        saved_rows = []

    saved_map: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in saved_rows:
        if not isinstance(row, Mapping):
            continue
        saved_map[(str(row.get("range_label", "")), str(row.get("side", "")))] = row

    print("\nPolicy Replay Diff")
    print("=" * 72)
    changed = 0
    for row in sorted(recomputed_final.rows, key=lambda item: (item.rank, item.range_label, item.side)):
        key = (row.range_label, row.side)
        saved = saved_map.get(key, {})

        old_score = float(saved.get("policy_score", 0.0)) if saved else 0.0
        old_state = str(saved.get("policy_state", "MISSING")) if saved else "MISSING"
        old_reason = str(saved.get("decision_reason", "")) if saved else ""

        score_changed = abs(old_score - row.policy_score) > 1e-9
        state_changed = old_state != row.policy_state
        reason_changed = old_reason != row.decision_reason
        if score_changed or state_changed or reason_changed:
            changed += 1

        print(
            f"candidate={row.range_label}/{row.side} "
            f"old_score={old_score:.3f} new_score={row.policy_score:.3f} "
            f"old_state={old_state} new_state={row.policy_state}"
        )
        print(f"  old_reason={old_reason}")
        print(f"  new_reason={row.decision_reason}")

    print(f"\nreplay_rows={len(recomputed_final.rows)} changed_rows={changed}")


def _run_replay(snapshot_path: str) -> int:
    """Replay policy from a saved snapshot without live market calls."""

    path = Path(snapshot_path)
    if not path.exists():
        print(f"Snapshot not found: {path}", file=sys.stderr)
        return 1

    try:
        snapshot = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Invalid snapshot JSON: {exc}", file=sys.stderr)
        return 1

    policy_input_payload = snapshot.get("policy_inputs", [])
    if not isinstance(policy_input_payload, list) or not policy_input_payload:
        print("Snapshot missing policy_inputs; cannot replay.", file=sys.stderr)
        return 1

    policy_inputs = [_policy_input_from_dict(item) for item in policy_input_payload if isinstance(item, Mapping)]
    if not policy_inputs:
        print("Snapshot has no valid policy inputs.", file=sys.stderr)
        return 1

    raw_cfg = SignalPolicyConfig(cluster_gap_f=-1.0)
    raw_policy = apply_signal_policy(policy_inputs, raw_cfg)
    final_policy = apply_signal_policy(policy_inputs, SignalPolicyConfig())

    print(f"replay_snapshot={path}")
    print(f"city={snapshot.get('city')} target_date={snapshot.get('target_date')} event_slug={snapshot.get('event_slug')}")
    print(f"raw_rows={len(raw_policy.rows)} final_rows={len(final_policy.rows)}")

    _print_replay_diff(snapshot, final_policy)
    return 0


def _resolve_city_list(args: argparse.Namespace) -> list[str]:
    """Resolve one or more city keys from CLI args."""

    if args.cities is None or not str(args.cities).strip():
        return [str(args.city).strip().lower()]
    parts = [item.strip().lower() for item in str(args.cities).split(",")]
    return [item for item in parts if item]


def _build_discord_preview(final_rows: list[Any]) -> tuple[str, int, str, float | None]:
    """Build compact Discord-safe preview from final policy rows only."""

    source_count = len(final_rows)
    actionable = [
        row
        for row in final_rows
        if bool(row.is_primary_candidate) and row.policy_state in {"PAPER", "TRADE_CANDIDATE"}
    ]

    if not actionable:
        return (
            "NO_SIGNAL | no actionable primary candidates",
            source_count,
            "NONE",
            None,
        )

    primary = sorted(
        actionable,
        key=lambda item: (-item.policy_score, item.rank, item.range_label, item.side),
    )[0]

    entry = "N/A" if primary.entry_price is None else f"{primary.entry_price:.4f}"
    exec_edge = "N/A" if primary.executable_edge is None else f"{primary.executable_edge:+.4f}"
    preview = (
        f"{primary.city.upper()} {primary.target_date} | "
        f"{primary.range_label} {primary.side} | "
        f"{primary.policy_state} score={primary.policy_score:.3f} | "
        f"entry={entry} exec_edge={exec_edge} | "
        f"reason={primary.decision_reason}"
    )
    primary_id = f"{primary.range_label}/{primary.side}"
    return preview, source_count, primary_id, float(primary.policy_score)


def _snapshot_path_for_city(
    args: argparse.Namespace,
    city_key: str,
    target_date: str,
    event_slug: str,
    is_multi_city: bool,
) -> Path | None:
    """Resolve optional snapshot path for a city run."""

    if args.snapshot_out is not None and str(args.snapshot_out).strip():
        raw_path = str(args.snapshot_out).strip()
        if is_multi_city:
            if "{city}" in raw_path:
                return Path(raw_path.replace("{city}", city_key))
            base = Path(raw_path)
            return base.with_name(f"{base.stem}_{city_key}{base.suffix or '.json'}")
        return Path(raw_path)

    if bool(args.save_snapshot):
        return _snapshot_default_path(
            snapshot_dir=str(args.snapshot_dir),
            city_key=city_key,
            target_date=target_date,
            event_slug=event_slug,
        )
    return None


def _write_snapshot(
    snapshot_path: Path,
    *,
    city_key: str,
    target_date: str,
    event_slug: str,
    metadata: list[dict[str, Any]],
    signal_result: Any,
    evaluation: Any,
    policy_inputs: list[SignalPolicyInputRow],
    raw_policy_result: Any,
    policy_result: Any,
) -> None:
    """Persist one full policy snapshot."""

    snapshot_payload = {
        "captured_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "city": city_key,
        "target_date": target_date,
        "event_slug": event_slug,
        "event_metadata": metadata,
        "ranked_signal_candidates": [asdict(row) for row in signal_result.all_ranked_candidates],
        "all_evaluated_candidates": [asdict(row) for row in evaluation.rows],
        "executable_candidates": [asdict(row) for row in evaluation.top_executable_candidates],
        "quote_details": [asdict(row) for row in evaluation.quote_details_used],
        "policy_inputs": [asdict(row) for row in policy_inputs],
        "policy": {
            "raw_rows": [asdict(row) for row in raw_policy_result.rows],
            "final_rows": [asdict(row) for row in policy_result.rows],
            "policy_ranked_candidates": [asdict(row) for row in policy_result.policy_ranked_candidates],
            "suppression_records": [asdict(row) for row in policy_result.suppressed_neighbors],
        },
    }

    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(snapshot_payload, indent=2), encoding="utf-8")


def _run_city_dry_run(
    *,
    city_key: str,
    cities_cfg: Mapping[str, Any],
    bundle: Any,
    args: argparse.Namespace,
    target_date: dt.date | None,
    is_multi_city: bool,
) -> tuple[str, int, str, float | None]:
    """Run one city policy pipeline in quiet dry-run mode and return discord preview triple."""

    horizon = str(args.horizon).strip().lower()
    if city_key not in cities_cfg or horizon not in SUPPORTED_HORIZONS:
        return ("NO_SIGNAL | city/horizon unavailable", 0, "NONE", None)

    city = build_city_config(city_key, cities_cfg[city_key])
    snapshots = [
        bundle.collector.collect(provider_name=name, city=city, target_day=horizon, provider_config=provider_cfg)
        for name, provider_cfg in bundle.provider_configs.items()
    ]
    snapshots = [s for s in snapshots if not (math.isnan(s.predicted_tmax_f) or "error" in s.raw_payload)]
    if not snapshots:
        return ("NO_SIGNAL | no successful provider snapshots", 0, "NONE", None)

    aggregate = aggregate_forecasts(snapshots)
    distribution = build_temperature_bin_probabilities(aggregate)

    slug_prefix = _city_slug_prefix(city_key, cities_cfg[city_key])
    metadata = resolve_event_markets_for_city_day(
        city_key=city_key,
        horizon=horizon,
        gamma_limit=int(args.gamma_limit),
        target_date=target_date,
        city_slug_prefix=slug_prefix,
    )
    if not metadata:
        return ("NO_SIGNAL | no event-scoped market bins", 0, "NONE", None)

    event_slug = str(metadata[0].get("event_slug", ""))
    effective_target_date = target_date.isoformat() if target_date is not None else "today"

    market_bins = [{"label": row["range_label"], "probability": float(row["market_yes_probability"])} for row in metadata]
    comparison = compare_market_probabilities(distribution, market_bins)
    signal_result = build_signal_candidates(
        comparison,
        SignalCandidateFilters(minimum_absolute_edge=float(args.min_abs_edge)),
    )
    if not signal_result.all_ranked_candidates:
        return ("NO_SIGNAL | no ranked signal candidates", 0, "NONE", None)

    quotes = fetch_quotes_for_metadata(metadata)
    evaluation = evaluate_executable_signal_candidates(
        signal_result.all_ranked_candidates,
        metadata,
        quotes,
        ClobEvaluatorFilters(
            minimum_executable_edge=float(args.min_exec_edge),
            maximum_spread=float(args.max_spread) if args.max_spread is not None else None,
            minimum_available_size=float(args.min_size),
        ),
    )

    by_key: dict[tuple[str, str], tuple[int, Any]] = {}
    for rank, candidate in enumerate(signal_result.all_ranked_candidates, 1):
        by_key[(candidate.range_label, candidate.raw_signal_direction)] = (rank, candidate)

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
                target_date=effective_target_date,
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

    raw_policy_result = apply_signal_policy(policy_inputs, SignalPolicyConfig(cluster_gap_f=-1.0))
    policy_result = apply_signal_policy(policy_inputs, SignalPolicyConfig())

    snapshot_path = _snapshot_path_for_city(
        args,
        city_key=city_key,
        target_date=effective_target_date,
        event_slug=event_slug,
        is_multi_city=is_multi_city,
    )
    if snapshot_path is not None:
        _write_snapshot(
            snapshot_path,
            city_key=city_key,
            target_date=effective_target_date,
            event_slug=event_slug,
            metadata=metadata,
            signal_result=signal_result,
            evaluation=evaluation,
            policy_inputs=policy_inputs,
            raw_policy_result=raw_policy_result,
            policy_result=policy_result,
        )

    return _build_discord_preview(list(policy_result.rows))


def main() -> int:
    """Run final policy test for one city/day using real event-scoped executable candidates."""

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

    if args.replay_snapshot is not None and str(args.replay_snapshot).strip():
        return _run_replay(str(args.replay_snapshot).strip())

    city_keys = _resolve_city_list(args)
    city_key = city_keys[0]
    horizon = str(args.horizon).strip().lower()

    try:
        target_date = _parse_target_date(args.target_date)
    except ValueError:
        print("Invalid --target-date. Use YYYY-MM-DD.", file=sys.stderr)
        return 1

    if city_key not in cities and not args.discord_dry_run:
        print(f"Unknown city '{city_key}'. Available: {', '.join(cities.keys())}", file=sys.stderr)
        return 1
    if horizon not in SUPPORTED_HORIZONS:
        print(f"Unsupported horizon '{horizon}'. Use today or tomorrow.", file=sys.stderr)
        return 1

    bundle = build_weather_collector(config)
    if not bundle.provider_configs:
        if args.discord_dry_run:
            for _ in city_keys:
                print("discord_preview=NO_SIGNAL | no enabled weather providers")
                print("source_candidate_count=0")
                print("primary_candidate=NONE")
            return 0
        print("No enabled weather providers found in config.")
        return 0

    if args.discord_dry_run:
        is_multi_city = len(city_keys) > 1
        city_results: list[tuple[str, int, str, float | None, str]] = []
        for city_item in city_keys:
            preview, source_count, primary_id, top_score = _run_city_dry_run(
                city_key=city_item,
                cities_cfg=cities,
                bundle=bundle,
                args=args,
                target_date=target_date,
                is_multi_city=is_multi_city,
            )
            city_results.append((preview, source_count, primary_id, top_score, city_item))
            print(f"discord_preview={preview}")
            print(f"source_candidate_count={source_count}")
            print(f"primary_candidate={primary_id}")

        if is_multi_city:
            with_signal = [item for item in city_results if item[2] != "NONE"]
            if with_signal:
                top_city_item = max(with_signal, key=lambda item: (item[3] if item[3] is not None else -1.0, item[4]))
                top_signal_city = top_city_item[4]
                top_signal_candidate = top_city_item[2]
            else:
                top_signal_city = "NONE"
                top_signal_candidate = "NONE"

            print(f"total_cities={len(city_results)}")
            print(f"cities_with_signal={len(with_signal)}")
            print(f"top_signal_city={top_signal_city}")
            print(f"top_signal_candidate={top_signal_candidate}")
        return 0

    city = build_city_config(city_key, cities[city_key])
    snapshots = [
        bundle.collector.collect(provider_name=name, city=city, target_day=horizon, provider_config=provider_cfg)
        for name, provider_cfg in bundle.provider_configs.items()
    ]
    snapshots = [s for s in snapshots if not (math.isnan(s.predicted_tmax_f) or "error" in s.raw_payload)]
    if not snapshots:
        print("No successful provider snapshots.", file=sys.stderr)
        return 1

    aggregate = aggregate_forecasts(snapshots)
    distribution = build_temperature_bin_probabilities(aggregate)

    slug_prefix = _city_slug_prefix(city_key, cities[city_key])
    metadata = resolve_event_markets_for_city_day(
        city_key=city_key,
        horizon=horizon,
        gamma_limit=int(args.gamma_limit),
        target_date=target_date,
        city_slug_prefix=slug_prefix,
    )
    if not metadata:
        print("No event-scoped market bins resolved.", file=sys.stderr)
        return 1

    event_slug = str(metadata[0].get("event_slug", ""))
    effective_target_date = target_date.isoformat() if target_date is not None else "today"

    market_bins = [{"label": row["range_label"], "probability": float(row["market_yes_probability"])} for row in metadata]
    comparison = compare_market_probabilities(distribution, market_bins)

    signal_result = build_signal_candidates(
        comparison,
        SignalCandidateFilters(minimum_absolute_edge=float(args.min_abs_edge)),
    )
    if not signal_result.all_ranked_candidates:
        if args.discord_dry_run:
            print("discord_preview=NO_SIGNAL | no ranked signal candidates")
            print("source_candidate_count=0")
            print("primary_candidate=NONE")
            return 0
        print(f"city={city_key} target_date={effective_target_date} executable_rows=0 policy_rows=0")
        print("No ranked signal candidates on real ladder after minimum edge filter.")
        return 0

    quotes = fetch_quotes_for_metadata(metadata)
    evaluation = evaluate_executable_signal_candidates(
        signal_result.all_ranked_candidates,
        metadata,
        quotes,
        ClobEvaluatorFilters(
            minimum_executable_edge=float(args.min_exec_edge),
            maximum_spread=float(args.max_spread) if args.max_spread is not None else None,
            minimum_available_size=float(args.min_size),
        ),
    )

    by_key: dict[tuple[str, str], tuple[int, Any]] = {}
    for rank, candidate in enumerate(signal_result.all_ranked_candidates, 1):
        by_key[(candidate.range_label, candidate.raw_signal_direction)] = (rank, candidate)

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
                target_date=effective_target_date,
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

    old_policy_result = apply_signal_policy(policy_inputs, legacy_signal_policy_config())
    raw_policy_result = apply_signal_policy(policy_inputs, SignalPolicyConfig(cluster_gap_f=-1.0))
    policy_result = apply_signal_policy(policy_inputs, SignalPolicyConfig())

    old_state_by_key = {(row.range_label, row.side): row.policy_state for row in old_policy_result.rows}

    print(f"city={city_key} target_date={effective_target_date} event_slug={event_slug}")
    print(f"executable_rows={len(evaluation.top_executable_candidates)} policy_rows={len(policy_result.rows)}")

    print("\nAll Executable Candidates")
    print("=" * 72)
    for i, row in enumerate(evaluation.top_executable_candidates, 1):
        entry = f"{row.entry_price:.4f}" if row.entry_price is not None else "N/A"
        edge = f"{row.executable_edge:+.4f}" if row.executable_edge is not None else "N/A"
        spread = f"{row.spread:.4f}" if row.spread is not None else "N/A"
        size = f"{row.available_size:.2f}" if row.available_size is not None else "N/A"
        print(f"{i}. {row.range_label:14} {str(row.side):7} entry={entry:>7} edge={edge:>8} spread={spread:>7} size={size:>8}")

    print("\nFinal Policy Ranked Candidates")
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

    print("\nPolicy State Transition (old -> new)")
    print("=" * 72)
    for row in sorted(policy_result.rows, key=lambda item: (item.rank, item.range_label, item.side)):
        old_state = old_state_by_key.get((row.range_label, row.side), "N/A")
        candidate = f"{row.range_label}/{row.side}"
        print(
            f"candidate={candidate:24} policy_score={row.policy_score:.3f} "
            f"old_state={old_state:15} new_state={row.policy_state:15} decision_reason={row.decision_reason}"
        )

    print("\nSuppressed Neighbors")
    print("=" * 72)
    if not policy_result.suppressed_neighbors:
        print("none")
    else:
        for item in policy_result.suppressed_neighbors:
            print(
                f"cluster={item.cluster_id} suppressed={item.suppressed_range_label}/{item.suppressed_side} "
                f"primary={item.primary_range_label}/{item.primary_side} reason={item.reject_reason}"
            )

    snapshot_path: Path | None = None
    if args.snapshot_out is not None and str(args.snapshot_out).strip():
        snapshot_path = Path(str(args.snapshot_out).strip())
    elif bool(args.save_snapshot):
        snapshot_path = _snapshot_default_path(
            snapshot_dir=str(args.snapshot_dir),
            city_key=city_key,
            target_date=effective_target_date,
            event_slug=event_slug,
        )

    if snapshot_path is not None:
        _write_snapshot(
            snapshot_path,
            city_key=city_key,
            target_date=effective_target_date,
            event_slug=event_slug,
            metadata=metadata,
            signal_result=signal_result,
            evaluation=evaluation,
            policy_inputs=policy_inputs,
            raw_policy_result=raw_policy_result,
            policy_result=policy_result,
        )
        print(f"\nSnapshot saved: {snapshot_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
