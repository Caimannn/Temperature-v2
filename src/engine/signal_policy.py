"""Final signal policy layer for real executable temperature candidates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence

from src.engine.market_ladder import parse_temperature_bin_label


PolicyState = Literal["IGNORE", "WATCH", "PAPER", "TRADE_CANDIDATE"]
SignalSide = Literal["BUY_YES", "BUY_NO"]


@dataclass(frozen=True)
class SignalPolicyConfig:
    """Configurable thresholds and scoring weights for policy decisions."""

    minimum_executable_edge_watch: float = 0.0
    paper_min_score: float = 0.30
    trade_candidate_min_score: float = 0.60
    paper_min_size: float = 10.0
    trade_candidate_min_size: float = 25.0
    paper_max_spread: float = 0.08
    trade_candidate_max_spread: float = 0.05
    paper_min_executable_edge: float = 0.02
    trade_min_executable_edge: float = 0.08
    cluster_gap_f: float = 2.1

    abs_edge_norm: float = 0.20
    executable_edge_norm: float = 0.20
    size_norm: float = 500.0
    spread_norm: float = 0.10

    abs_edge_weight: float = 0.45
    executable_edge_weight: float = 0.45
    size_weight: float = 0.15
    spread_penalty_weight: float = 0.05

    # Explicit operational penalties.
    spread_unavailable_penalty: float = 0.20
    very_low_size_threshold: float = 5.0
    very_low_size_penalty: float = 0.22
    low_size_penalty: float = 0.08
    high_abs_edge_threshold: float = 0.12
    weak_exec_edge_threshold: float = 0.05
    weak_exec_vs_abs_penalty: float = 0.15


def legacy_signal_policy_config() -> SignalPolicyConfig:
    """Return pre-retune policy config to compare old/new state transitions."""

    return SignalPolicyConfig(
        minimum_executable_edge_watch=0.0,
        paper_min_score=0.25,
        trade_candidate_min_score=0.40,
        paper_min_size=10.0,
        trade_candidate_min_size=25.0,
        paper_max_spread=0.08,
        trade_candidate_max_spread=0.05,
        paper_min_executable_edge=0.0,
        trade_min_executable_edge=0.0,
        cluster_gap_f=2.1,
        abs_edge_norm=0.20,
        executable_edge_norm=0.20,
        size_norm=500.0,
        spread_norm=0.10,
        abs_edge_weight=0.45,
        executable_edge_weight=0.45,
        size_weight=0.15,
        spread_penalty_weight=0.05,
        spread_unavailable_penalty=0.0,
        very_low_size_threshold=0.0,
        very_low_size_penalty=0.0,
        low_size_penalty=0.0,
        high_abs_edge_threshold=1.0,
        weak_exec_edge_threshold=0.0,
        weak_exec_vs_abs_penalty=0.0,
    )


@dataclass(frozen=True)
class SignalPolicyInputRow:
    """Input candidate row for policy evaluation."""

    city: str
    target_date: str
    event_slug: str
    range_label: str
    side: SignalSide
    model_probability: float
    market_probability: float
    probability_edge: float
    abs_edge: float
    executable_edge: float | None
    entry_price: float | None
    spread: float | None
    available_size: float | None
    rank: int


@dataclass(frozen=True)
class SignalPolicyDecisionRow:
    """Policy decision row with explicit reasons and deterministic score."""

    city: str
    target_date: str
    event_slug: str
    range_label: str
    side: SignalSide
    model_probability: float
    market_probability: float
    probability_edge: float
    abs_edge: float
    executable_edge: float | None
    entry_price: float | None
    spread: float | None
    available_size: float | None
    rank: int
    policy_state: PolicyState
    policy_score: float
    decision_reason: str
    reject_reason: str | None
    is_primary_candidate: bool


@dataclass(frozen=True)
class SignalPolicySuppression:
    """Suppressed neighbor details for transparency."""

    cluster_id: int
    suppressed_range_label: str
    suppressed_side: SignalSide
    primary_range_label: str
    primary_side: SignalSide
    reject_reason: str


@dataclass(frozen=True)
class SignalPolicyResult:
    """Policy result container."""

    rows: tuple[SignalPolicyDecisionRow, ...]
    policy_ranked_candidates: tuple[SignalPolicyDecisionRow, ...]
    suppressed_neighbors: tuple[SignalPolicySuppression, ...]


def apply_signal_policy(
    inputs: Sequence[SignalPolicyInputRow],
    config: SignalPolicyConfig | None = None,
) -> SignalPolicyResult:
    """Apply deterministic policy scoring and neighbor suppression."""

    cfg = config or SignalPolicyConfig()

    base_rows: list[SignalPolicyDecisionRow] = []
    for row in inputs:
        policy_state, score, decision_reason, reject_reason = _base_policy_decision(row, cfg)
        base_rows.append(
            SignalPolicyDecisionRow(
                city=row.city,
                target_date=row.target_date,
                event_slug=row.event_slug,
                range_label=row.range_label,
                side=row.side,
                model_probability=row.model_probability,
                market_probability=row.market_probability,
                probability_edge=row.probability_edge,
                abs_edge=row.abs_edge,
                executable_edge=row.executable_edge,
                entry_price=row.entry_price,
                spread=row.spread,
                available_size=row.available_size,
                rank=row.rank,
                policy_state=policy_state,
                policy_score=score,
                decision_reason=decision_reason,
                reject_reason=reject_reason,
                is_primary_candidate=False,
            )
        )

    rows_with_primary, suppressions = _suppress_neighbors(base_rows, cfg)

    ranked = tuple(
        sorted(
            [item for item in rows_with_primary if item.policy_state != "IGNORE" and item.is_primary_candidate],
            key=lambda item: (
                -item.policy_score,
                -item.abs_edge,
                -(item.executable_edge if item.executable_edge is not None else -1e9),
                item.rank,
                item.range_label,
                item.side,
            ),
        )
    )

    return SignalPolicyResult(
        rows=tuple(rows_with_primary),
        policy_ranked_candidates=ranked,
        suppressed_neighbors=tuple(suppressions),
    )


def _base_policy_decision(
    row: SignalPolicyInputRow,
    cfg: SignalPolicyConfig,
) -> tuple[PolicyState, float, str, str | None]:
    """Compute base state and score before neighbor suppression."""

    if row.executable_edge is None or row.entry_price is None:
        return (
            "IGNORE",
            0.0,
            "ignored due to missing executable inputs",
            "missing executable inputs",
        )

    if row.executable_edge < cfg.minimum_executable_edge_watch:
        return (
            "IGNORE",
            0.0,
            (
                f"ignored: executable_edge {row.executable_edge:.4f} below "
                f"minimum_executable_edge_watch {cfg.minimum_executable_edge_watch:.4f}"
            ),
            "executable edge below watch threshold",
        )

    abs_component = min(max(row.abs_edge, 0.0) / cfg.abs_edge_norm, 1.0) * cfg.abs_edge_weight
    exec_component = min(max(row.executable_edge, 0.0) / cfg.executable_edge_norm, 1.0) * cfg.executable_edge_weight
    size_value = row.available_size if row.available_size is not None else 0.0
    size_component = min(max(size_value, 0.0) / cfg.size_norm, 1.0) * cfg.size_weight

    spread_penalty = 0.0
    spread_text = "spread=N/A"
    if row.spread is not None:
        spread_penalty = min(max(row.spread, 0.0) / cfg.spread_norm, 1.0) * cfg.spread_penalty_weight
        spread_text = f"spread={row.spread:.4f}"

    explicit_penalty = 0.0
    penalty_notes: list[str] = []

    if row.spread is None:
        explicit_penalty += cfg.spread_unavailable_penalty
        penalty_notes.append(f"spread_unavailable(-{cfg.spread_unavailable_penalty:.2f})")

    if size_value < cfg.very_low_size_threshold:
        explicit_penalty += cfg.very_low_size_penalty
        penalty_notes.append(f"very_low_size(-{cfg.very_low_size_penalty:.2f})")
    elif size_value < cfg.paper_min_size:
        explicit_penalty += cfg.low_size_penalty
        penalty_notes.append(f"low_size(-{cfg.low_size_penalty:.2f})")

    if row.abs_edge >= cfg.high_abs_edge_threshold and row.executable_edge < cfg.weak_exec_edge_threshold:
        explicit_penalty += cfg.weak_exec_vs_abs_penalty
        penalty_notes.append(f"weak_exec_vs_abs(-{cfg.weak_exec_vs_abs_penalty:.2f})")

    score = max(0.0, min(1.0, abs_component + exec_component + size_component - spread_penalty - explicit_penalty))

    can_be_paper = (
        row.available_size is not None
        and row.available_size >= cfg.paper_min_size
        and row.executable_edge >= cfg.paper_min_executable_edge
        and (row.spread is None or row.spread <= cfg.paper_max_spread)
    )
    can_be_trade = (
        row.available_size is not None
        and row.available_size >= cfg.trade_candidate_min_size
        and row.executable_edge >= cfg.trade_min_executable_edge
        and row.spread is not None
        and row.spread <= cfg.trade_candidate_max_spread
    )

    state: PolicyState = "WATCH"
    if score >= cfg.trade_candidate_min_score and can_be_trade:
        state = "TRADE_CANDIDATE"
    elif score >= cfg.paper_min_score and can_be_paper:
        state = "PAPER"

    penalty_text = "none" if not penalty_notes else ", ".join(penalty_notes)
    decision_reason = (
        f"{state} score={score:.3f}; abs_edge={row.abs_edge:.4f}; "
        f"executable_edge={row.executable_edge:.4f}; {spread_text}; "
        f"size={size_value:.2f}; penalties={penalty_text}"
    )
    return state, score, decision_reason, None


def _suppress_neighbors(
    rows: Sequence[SignalPolicyDecisionRow],
    cfg: SignalPolicyConfig,
) -> tuple[list[SignalPolicyDecisionRow], list[SignalPolicySuppression]]:
    """Suppress neighboring bins and keep one primary per local cluster."""

    indexed: list[tuple[int, SignalPolicyDecisionRow, float | None]] = []
    for index, row in enumerate(rows):
        center = _label_center_f(row.range_label)
        indexed.append((index, row, center))

    clusters = _build_clusters(indexed, cfg.cluster_gap_f)
    rows_out: list[SignalPolicyDecisionRow] = list(rows)
    suppressions: list[SignalPolicySuppression] = []

    for cluster_id, cluster in enumerate(clusters, 1):
        active = [item for item in cluster if item[1].policy_state != "IGNORE"]
        if not active:
            continue

        primary_index, primary_row, _ = sorted(active, key=_cluster_rank_key)[0]
        rows_out[primary_index] = _replace_primary(rows_out[primary_index], True)

        for item_index, item_row, center in active:
            if item_index == primary_index:
                continue
            center_text = "unknown"
            primary_center = _label_center_f(primary_row.range_label)
            if center is not None and primary_center is not None:
                center_text = f"{abs(center - primary_center):.1f}F"

            reject_reason = (
                f"suppressed neighbor in cluster {cluster_id}; "
                f"primary={primary_row.range_label}/{primary_row.side}; delta={center_text}"
            )
            updated = _replace_suppressed(rows_out[item_index], reject_reason)
            rows_out[item_index] = updated
            suppressions.append(
                SignalPolicySuppression(
                    cluster_id=cluster_id,
                    suppressed_range_label=item_row.range_label,
                    suppressed_side=item_row.side,
                    primary_range_label=primary_row.range_label,
                    primary_side=primary_row.side,
                    reject_reason=reject_reason,
                )
            )

    return rows_out, suppressions


def _cluster_rank_key(item: tuple[int, SignalPolicyDecisionRow, float | None]) -> tuple[float, float, float, float, int, str, str]:
    """Deterministic ranking key for selecting cluster primary."""

    _, row, _ = item
    spread_value = row.spread if row.spread is not None else 999.0
    size_value = row.available_size if row.available_size is not None else 0.0
    executable_edge = row.executable_edge if row.executable_edge is not None else -1e9
    return (
        -row.policy_score,
        -row.abs_edge,
        -executable_edge,
        spread_value,
        row.rank,
        row.range_label,
        row.side,
    )


def _replace_primary(row: SignalPolicyDecisionRow, is_primary: bool) -> SignalPolicyDecisionRow:
    """Clone row with updated primary flag."""

    return SignalPolicyDecisionRow(
        city=row.city,
        target_date=row.target_date,
        event_slug=row.event_slug,
        range_label=row.range_label,
        side=row.side,
        model_probability=row.model_probability,
        market_probability=row.market_probability,
        probability_edge=row.probability_edge,
        abs_edge=row.abs_edge,
        executable_edge=row.executable_edge,
        entry_price=row.entry_price,
        spread=row.spread,
        available_size=row.available_size,
        rank=row.rank,
        policy_state=row.policy_state,
        policy_score=row.policy_score,
        decision_reason=row.decision_reason,
        reject_reason=row.reject_reason,
        is_primary_candidate=is_primary,
    )


def _replace_suppressed(row: SignalPolicyDecisionRow, reject_reason: str) -> SignalPolicyDecisionRow:
    """Convert one row into suppressed IGNORE state."""

    return SignalPolicyDecisionRow(
        city=row.city,
        target_date=row.target_date,
        event_slug=row.event_slug,
        range_label=row.range_label,
        side=row.side,
        model_probability=row.model_probability,
        market_probability=row.market_probability,
        probability_edge=row.probability_edge,
        abs_edge=row.abs_edge,
        executable_edge=row.executable_edge,
        entry_price=row.entry_price,
        spread=row.spread,
        available_size=row.available_size,
        rank=row.rank,
        policy_state="IGNORE",
        policy_score=row.policy_score,
        decision_reason=row.decision_reason,
        reject_reason=reject_reason,
        is_primary_candidate=False,
    )


def _build_clusters(
    indexed_rows: Sequence[tuple[int, SignalPolicyDecisionRow, float | None]],
    cluster_gap_f: float,
) -> list[list[tuple[int, SignalPolicyDecisionRow, float | None]]]:
    """Build local temperature clusters from label centers."""

    known = sorted([item for item in indexed_rows if item[2] is not None], key=lambda item: item[2] or -1e9)
    unknown = [item for item in indexed_rows if item[2] is None]

    clusters: list[list[tuple[int, SignalPolicyDecisionRow, float | None]]] = []
    current: list[tuple[int, SignalPolicyDecisionRow, float | None]] = []

    for item in known:
        if not current:
            current = [item]
            continue

        prev_center = current[-1][2]
        current_center = item[2]
        if prev_center is not None and current_center is not None and abs(current_center - prev_center) <= cluster_gap_f:
            current.append(item)
            continue

        clusters.append(current)
        current = [item]

    if current:
        clusters.append(current)

    for item in unknown:
        clusters.append([item])

    return clusters


def _label_center_f(label: str) -> float | None:
    """Estimate temperature center for cluster suppression from canonical label."""

    parsed = parse_temperature_bin_label(label)
    if parsed is None:
        return None

    if parsed.low_f is not None and parsed.high_f is not None:
        return (parsed.low_f + parsed.high_f) / 2.0

    if parsed.open_left and parsed.high_f is not None:
        return parsed.high_f - 1.0

    if parsed.open_right and parsed.low_f is not None:
        return parsed.low_f + 1.0

    return None
