"""Read-only price activity classification from observed trading sessions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


ACTIVE = "ACTIVE"
DELISTED_OR_INACTIVE = "DELISTED_OR_INACTIVE"
NO_PRICE_HISTORY = "NO_PRICE_HISTORY"


@dataclass(frozen=True)
class PriceActivity:
    ticker: str
    last_price_date: str | None
    latest_market_date: str
    active_cutoff_date: str
    trading_sessions_stale: int | None
    activity_classification: str


def classify_price_activity(
    *,
    ticker: str,
    last_price_date: str | None,
    trading_sessions_desc: Iterable[str],
    active_session_window: int = 5,
) -> PriceActivity:
    """Classify a ticker using observed trading-session positions, not calendar days."""

    sessions = list(trading_sessions_desc)
    if not sessions:
        raise ValueError("trading_sessions_desc must not be empty")
    if active_session_window <= 0:
        raise ValueError("active_session_window must be positive")
    if len(sessions) < active_session_window:
        raise ValueError("not enough trading sessions for active window")

    latest_market_date = sessions[0]
    active_cutoff_date = sessions[active_session_window - 1]
    if last_price_date is None:
        return PriceActivity(
            ticker=ticker,
            last_price_date=None,
            latest_market_date=latest_market_date,
            active_cutoff_date=active_cutoff_date,
            trading_sessions_stale=None,
            activity_classification=NO_PRICE_HISTORY,
        )

    session_index = {session_date: index for index, session_date in enumerate(sessions)}
    stale = session_index.get(last_price_date)
    if stale is None:
        stale = _stale_distance_for_non_session_date(last_price_date, sessions)

    return PriceActivity(
        ticker=ticker,
        last_price_date=last_price_date,
        latest_market_date=latest_market_date,
        active_cutoff_date=active_cutoff_date,
        trading_sessions_stale=stale,
        activity_classification=ACTIVE if stale < active_session_window else DELISTED_OR_INACTIVE,
    )


def _stale_distance_for_non_session_date(last_price_date: str, sessions_desc: list[str]) -> int:
    for index, session_date in enumerate(sessions_desc):
        if last_price_date >= session_date:
            return index
    return len(sessions_desc)
