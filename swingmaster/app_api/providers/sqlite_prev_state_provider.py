"""SQLite-backed provider for previous state lookup.

Responsibilities:
  - Fetch prior state/attrs for a ticker/day from persistence.
Must not:
  - Compute signals or policy decisions.
"""

from __future__ import annotations

import json
import sqlite3

from swingmaster.app_api.ports import PrevStateProvider
from swingmaster.core.domain.enums import State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.infra.sqlite.repos.rc_state_reader import RcStateReader


class SQLitePrevStateProvider(PrevStateProvider):
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._reader = RcStateReader(conn)

    def get_prev(self, ticker: str, date: str) -> tuple[State, StateAttrs]:
        row = self._reader.get_latest_before(ticker, date)
        if row is None:
            return State.NO_TRADE, StateAttrs(confidence=None, age=0, status=None)

        state_value, confidence_value, age_value, status_value = row
        downtrend_origin = None
        decline_profile = None
        if status_value:
            try:
                parsed = json.loads(status_value)
                if isinstance(parsed, dict):
                    value = parsed.get("downtrend_origin")
                    if isinstance(value, str):
                        downtrend_origin = value
                    value = parsed.get("decline_profile")
                    if isinstance(value, str):
                        decline_profile = value
            except Exception:
                pass
        return State(state_value), StateAttrs(
            confidence=confidence_value,
            age=age_value,
            status=status_value,
            downtrend_origin=downtrend_origin,
            decline_profile=decline_profile,
        )
