"""Executable-signal evaluation layer using CLOB quotes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping, Sequence

from src.engine.signal_candidates import SignalCandidateRow

SignalDirection = Literal["BUY_YES", "BUY_NO"]


@dataclass(frozen=True)
class ClobEvaluatorFilters:
    """Configurable filters for executable signal selection."""

    minimum_executable_edge: float = 0.0
    maximum_spread: float | None = None
    minimum_available_size: float = 0.0


@dataclass(frozen=True)
class ClobQuoteSnapshot:
    """Normalized top-of-book quote snapshot for one token."""

    token_id: str
    best_bid: float | None
    best_ask: float | None
    bid_size: float | None
    ask_size: float | None
    spread: float | None
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExecutableSignalCandidate:
    """One signal candidate evaluated against executable CLOB entry."""

    range_label: str
    side: SignalDirection | None
    model_probability: float | None
    entry_price: float | None
    executable_edge: float | None
    spread: float | None
    available_size: float | None
    executable: bool
    reason: str | None


@dataclass(frozen=True)
class ClobEvaluationDiagnostics:
    """Aggregate diagnostics for executable-signal evaluation."""

    input_count: int
    executable_count: int
    filtered_count: int
    missing_quote_count: int
    unresolved_side_count: int
    missing_token_mapping_count: int
    illiquid_count: int
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ClobEvaluationResult:
    """Output of executable-signal evaluation."""

    rows: tuple[ExecutableSignalCandidate, ...]
    top_executable_candidates: tuple[ExecutableSignalCandidate, ...]
    filtered_candidates: tuple[ExecutableSignalCandidate, ...]
    quote_details_used: tuple[ClobQuoteSnapshot, ...]
    diagnostics: ClobEvaluationDiagnostics


@dataclass(frozen=True)
class _TokenMapping:
    """Token ids for one temperature range label."""

    range_label: str
    yes_token_id: str | None
    no_token_id: str | None


def evaluate_executable_signal_candidates(
    candidates: Sequence[SignalCandidateRow],
    market_metadata: Sequence[Mapping[str, Any]],
    quote_data: Mapping[str, Any],
    filters: ClobEvaluatorFilters | None = None,
) -> ClobEvaluationResult:
    """Evaluate ranked candidates using real executable CLOB entry prices."""

    effective_filters = filters or ClobEvaluatorFilters()
    token_map, mapping_notes = _build_token_mapping(market_metadata)

    rows: list[ExecutableSignalCandidate] = []
    quote_details: list[ClobQuoteSnapshot] = []

    missing_quote_count = 0
    unresolved_side_count = 0
    missing_token_mapping_count = 0
    illiquid_count = 0

    for candidate in candidates:
        normalized_label = _normalize_label(candidate.range_label)
        side = _resolve_side(candidate)
        if side is None:
            unresolved_side_count += 1
            rows.append(
                ExecutableSignalCandidate(
                    range_label=candidate.range_label,
                    side=None,
                    model_probability=None,
                    entry_price=None,
                    executable_edge=None,
                    spread=None,
                    available_size=None,
                    executable=False,
                    reason="unresolved side",
                )
            )
            continue

        mapping = token_map.get(normalized_label)
        if mapping is None:
            missing_token_mapping_count += 1
            rows.append(
                ExecutableSignalCandidate(
                    range_label=candidate.range_label,
                    side=side,
                    model_probability=_model_probability_for_side(candidate.model_probability, side),
                    entry_price=None,
                    executable_edge=None,
                    spread=None,
                    available_size=None,
                    executable=False,
                    reason="missing token mapping",
                )
            )
            continue

        token_id = mapping.yes_token_id if side == "BUY_YES" else mapping.no_token_id
        if not token_id:
            missing_token_mapping_count += 1
            rows.append(
                ExecutableSignalCandidate(
                    range_label=candidate.range_label,
                    side=side,
                    model_probability=_model_probability_for_side(candidate.model_probability, side),
                    entry_price=None,
                    executable_edge=None,
                    spread=None,
                    available_size=None,
                    executable=False,
                    reason="missing token mapping",
                )
            )
            continue

        quote_snapshot = _to_quote_snapshot(token_id, quote_data.get(token_id))
        quote_details.append(quote_snapshot)

        if quote_snapshot.best_ask is None:
            missing_quote_count += 1
            illiquid_count += 1
            rows.append(
                ExecutableSignalCandidate(
                    range_label=candidate.range_label,
                    side=side,
                    model_probability=_model_probability_for_side(candidate.model_probability, side),
                    entry_price=None,
                    executable_edge=None,
                    spread=quote_snapshot.spread,
                    available_size=quote_snapshot.ask_size,
                    executable=False,
                    reason="missing quote",
                )
            )
            continue

        model_probability = _model_probability_for_side(candidate.model_probability, side)
        if model_probability is None:
            rows.append(
                ExecutableSignalCandidate(
                    range_label=candidate.range_label,
                    side=side,
                    model_probability=None,
                    entry_price=quote_snapshot.best_ask,
                    executable_edge=None,
                    spread=quote_snapshot.spread,
                    available_size=quote_snapshot.ask_size,
                    executable=False,
                    reason="missing model probability",
                )
            )
            continue

        entry_price = quote_snapshot.best_ask
        executable_edge = model_probability - entry_price
        available_size = quote_snapshot.ask_size
        spread = quote_snapshot.spread

        reason = _apply_filters(
            executable_edge=executable_edge,
            spread=spread,
            available_size=available_size,
            filters=effective_filters,
        )
        executable = reason is None

        if reason == "illiquid market":
            illiquid_count += 1

        rows.append(
            ExecutableSignalCandidate(
                range_label=candidate.range_label,
                side=side,
                model_probability=model_probability,
                entry_price=entry_price,
                executable_edge=executable_edge,
                spread=spread,
                available_size=available_size,
                executable=executable,
                reason=reason,
            )
        )

    executable_rows = [row for row in rows if row.executable]
    filtered_rows = [row for row in rows if not row.executable]

    top_executable = tuple(
        sorted(
            executable_rows,
            key=lambda item: (
                -(item.executable_edge if item.executable_edge is not None else -1e9),
                item.range_label,
                item.side or "",
            ),
        )
    )

    diagnostics = ClobEvaluationDiagnostics(
        input_count=len(candidates),
        executable_count=len(executable_rows),
        filtered_count=len(filtered_rows),
        missing_quote_count=missing_quote_count,
        unresolved_side_count=unresolved_side_count,
        missing_token_mapping_count=missing_token_mapping_count,
        illiquid_count=illiquid_count,
        notes=tuple(mapping_notes),
    )

    return ClobEvaluationResult(
        rows=tuple(rows),
        top_executable_candidates=top_executable,
        filtered_candidates=tuple(filtered_rows),
        quote_details_used=tuple(quote_details),
        diagnostics=diagnostics,
    )


def _build_token_mapping(
    market_metadata: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, _TokenMapping], list[str]]:
    """Build range-label to yes/no token mapping from market metadata."""

    mapping: dict[str, _TokenMapping] = {}
    notes: list[str] = []

    for row in market_metadata:
        label = _extract_label(row)
        if not label:
            notes.append("missing label in market metadata")
            continue

        normalized = _normalize_label(label)
        yes_token_id = _extract_token_id(row, ("yes_token_id", "yesTokenId", "token_id_yes"))
        no_token_id = _extract_token_id(row, ("no_token_id", "noTokenId", "token_id_no"))

        mapping[normalized] = _TokenMapping(
            range_label=label,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
        )

    return mapping, notes


def _extract_label(row: Mapping[str, Any]) -> str:
    """Extract temperature range label from metadata row."""

    for key in ("range_label", "label", "market_label", "outcome_label"):
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _extract_token_id(row: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    """Extract token id from first available key."""

    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _resolve_side(candidate: SignalCandidateRow) -> SignalDirection | None:
    """Resolve candidate side from signal row direction."""

    direction = candidate.raw_signal_direction
    if direction == "BUY_YES":
        return "BUY_YES"
    if direction == "BUY_NO":
        return "BUY_NO"
    return None


def _model_probability_for_side(model_yes_probability: float | None, side: SignalDirection) -> float | None:
    """Map model YES probability to side-specific probability."""

    if model_yes_probability is None:
        return None
    if side == "BUY_YES":
        return float(model_yes_probability)
    return 1.0 - float(model_yes_probability)


def _to_quote_snapshot(token_id: str, raw_quote: Any) -> ClobQuoteSnapshot:
    """Normalize a raw CLOB book payload into top-of-book quote fields."""

    if raw_quote is None:
        return ClobQuoteSnapshot(
            token_id=token_id,
            best_bid=None,
            best_ask=None,
            bid_size=None,
            ask_size=None,
            spread=None,
            notes=("missing quote",),
        )

    if isinstance(raw_quote, ClobQuoteSnapshot):
        return raw_quote

    if not isinstance(raw_quote, Mapping):
        return ClobQuoteSnapshot(
            token_id=token_id,
            best_bid=None,
            best_ask=None,
            bid_size=None,
            ask_size=None,
            spread=None,
            notes=("unsupported quote payload",),
        )

    bids = _extract_book_side(raw_quote, "bids")
    asks = _extract_book_side(raw_quote, "asks")

    best_bid, bid_size = _best_level(bids, side="bid")
    best_ask, ask_size = _best_level(asks, side="ask")

    spread = None
    if best_bid is not None and best_ask is not None:
        spread = max(0.0, best_ask - best_bid)

    notes: list[str] = []
    if best_ask is None:
        notes.append("missing best ask")
    if best_bid is None:
        notes.append("missing best bid")

    return ClobQuoteSnapshot(
        token_id=token_id,
        best_bid=best_bid,
        best_ask=best_ask,
        bid_size=bid_size,
        ask_size=ask_size,
        spread=spread,
        notes=tuple(notes),
    )


def _extract_book_side(raw_quote: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    """Extract one side of the order book as a list of levels."""

    value = raw_quote.get(key)
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _best_level(levels: Sequence[Mapping[str, Any]], side: Literal["bid", "ask"]) -> tuple[float | None, float | None]:
    """Get best price/size pair from book levels."""

    best_price: float | None = None
    best_size: float | None = None

    for level in levels:
        price = _to_float(level.get("price", level.get("p")))
        size = _to_float(level.get("size", level.get("s", level.get("quantity"))))
        if price is None:
            continue

        if best_price is None:
            best_price = price
            best_size = size
            continue

        if side == "bid" and price > best_price:
            best_price = price
            best_size = size
        if side == "ask" and price < best_price:
            best_price = price
            best_size = size

    return best_price, best_size


def _apply_filters(
    executable_edge: float,
    spread: float | None,
    available_size: float | None,
    filters: ClobEvaluatorFilters,
) -> str | None:
    """Apply deterministic filter checks and return exclusion reason if any."""

    if executable_edge < float(filters.minimum_executable_edge):
        return "below minimum executable edge"

    if filters.maximum_spread is not None and spread is not None and spread > float(filters.maximum_spread):
        return "spread too wide"

    if available_size is None or available_size <= 0:
        return "illiquid market"

    if available_size < float(filters.minimum_available_size):
        return "insufficient available size"

    return None


def _normalize_label(label: str) -> str:
    """Normalize range labels for deterministic matching."""

    return " ".join(label.strip().lower().split())


def _to_float(value: Any) -> float | None:
    """Convert value to float when possible."""

    try:
        return float(value)
    except (TypeError, ValueError):
        return None
