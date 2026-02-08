"""SQLite-backed provider for previous state lookup.

Responsibilities:
  - Fetch prior state/attrs for a ticker/day from persistence.
Must not:
  - Compute signals or policy decisions.
"""

from __future__ import annotations

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

        state_value, confidence_value, age_value, _status_value = row
        return State(state_value), StateAttrs(
            confidence=confidence_value,
            age=age_value,
            status=None,
        )
