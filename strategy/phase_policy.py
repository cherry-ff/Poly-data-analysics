from __future__ import annotations

from decimal import Decimal

from core.enums import MarketPhase


class PhasePolicy:
    """Phase-aware gate: controls which strategy actions are permitted in each lifecycle phase.

    Mirrors the risk engine's phase-dependent limits but is consulted by strategy
    modules *before* they generate intents.  This lets strategies self-throttle
    without reaching all the way to the risk engine for obvious no-ops.
    """

    _QUOTE_PHASES = {
        MarketPhase.PREWARM,
        MarketPhase.ACTIVE,
        MarketPhase.FAST_CLOSE,
    }

    _TAKER_PHASES = {
        MarketPhase.ACTIVE,
        MarketPhase.FAST_CLOSE,
    }

    _RECOVERY_FORBIDDEN = {
        MarketPhase.RESOLVED,
        MarketPhase.ARCHIVED,
    }

    # Maximum tolerated unhedged exposure (token units) per phase.
    # Phases not listed default to Decimal("0") (no new exposure allowed).
    _UNHEDGED_LIMITS: dict[MarketPhase, Decimal] = {
        MarketPhase.PREWARM: Decimal("10000"),
        MarketPhase.ACTIVE: Decimal("5000"),
        MarketPhase.FAST_CLOSE: Decimal("1000"),
        MarketPhase.FINAL_SECONDS: Decimal("200"),
        MarketPhase.CLOSED_WAIT_RESOLUTION: Decimal("200"),
    }

    def allow_new_quotes(self, phase: MarketPhase) -> bool:
        """Return True if the strategy may post new maker quotes in *phase*."""
        return phase in self._QUOTE_PHASES

    def allow_selective_taker(self, phase: MarketPhase) -> bool:
        """Return True if selective taker orders may be sent in *phase*."""
        return phase in self._TAKER_PHASES

    def allow_recovery(self, phase: MarketPhase) -> bool:
        """Return True if recovery / unwind intents may be sent in *phase*."""
        return phase not in self._RECOVERY_FORBIDDEN

    def max_unhedged_exposure(self, phase: MarketPhase) -> Decimal:
        """Maximum tolerated unhedged exposure for *phase*.

        Returns ``Decimal("0")`` for phases where no new unhedged position is
        allowed (e.g. DISCOVERED) or after market resolution.
        """
        return self._UNHEDGED_LIMITS.get(phase, Decimal("0"))
