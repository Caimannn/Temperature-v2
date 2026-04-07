"""Disabled execution router for future trading support."""

from __future__ import annotations

from src.common.models import PositionAdvice


class ExecutionRouter:
    """Execution remains disabled in the initial foundation."""

    def __init__(self, enabled: bool = False) -> None:
        self.enabled = enabled

    def route(self, advice: PositionAdvice) -> None:
        """Reject execution until trading is explicitly enabled."""

        raise NotImplementedError("Execution is disabled in the initial foundation.")
