from __future__ import annotations

import json
import sqlite3
from typing import List, Optional

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.policy.ports.state_history_port import StateHistoryDay, StateHistoryPort
from swingmaster.core.signals.enums import SignalKey


class StateHistoryPortSqlite(StateHistoryPort):
    """SQLite-backed history port for policy windows.

    Ordering contract: returns most recent days on or before as_of_date, ordered newest -> oldest.
    Completeness contract: returns up to `limit` rows; fewer rows means an incomplete window.
    This port defines the temporal truth for policy window evaluation.
    """

    def __init__(self, conn: sqlite3.Connection, table_name: str = "rc_state_daily") -> None:
        self._conn = conn
        self._table = table_name

    def get_recent_days(self, ticker: str, as_of_date: str, limit: int) -> List[StateHistoryDay]:
        if limit <= 0:
            return []
        if not ticker or not as_of_date:
            return []
        try:
            rows = self._conn.execute(
                f"""
                SELECT date, state, reason_codes, signal_keys, churn_guard_hits
                FROM {self._table}
                WHERE ticker=? AND date <= ?
                ORDER BY date DESC
                LIMIT ?
                """,
                (ticker, as_of_date, limit),
            ).fetchall()
        except Exception:
            return []

        days: List[StateHistoryDay] = []
        for row in rows:
            date = row[0]
            try:
                state = State(row[1])
            except Exception:
                continue
            reason_codes = _parse_reason_codes(row[2])
            signal_keys = _parse_signal_keys(row[3])
            churn_guard_hits = _coerce_int_optional(row[4])
            days.append(
                StateHistoryDay(
                    date=date,
                    state=state,
                    reason_codes=reason_codes,
                    signal_keys=signal_keys,
                    churn_guard_hits=churn_guard_hits,
                )
            )
        return days


def _parse_reason_codes(raw: Optional[str]) -> List[ReasonCode]:
    values = _parse_json_list(raw)
    codes: List[ReasonCode] = []
    for value in values:
        try:
            codes.append(ReasonCode(value))
        except Exception:
            continue
    return codes


def _parse_signal_keys(raw: Optional[str]) -> List[SignalKey]:
    values = _parse_json_list(raw)
    keys: List[SignalKey] = []
    for value in values:
        try:
            keys.append(SignalKey(value))
        except Exception:
            continue
    return keys


def _parse_json_list(raw: Optional[object]) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    if isinstance(raw, str) and not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if parsed is None or not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, str)]


def _coerce_int_optional(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None
