"""SQLite repository for rc_state_daily and rc_transition persistence.

Responsibilities:
  - Insert/update state and transition rows deterministically.
Must not:
  - Modify policy or signal logic; persistence only.
"""

from __future__ import annotations

import json
import sqlite3

from swingmaster.core.domain.enums import ReasonCode, State, reason_to_persisted
from swingmaster.core.domain.models import StateAttrs, Transition
from swingmaster.core.signals.models import SignalSet


class RcStateRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._has_state_attrs_json = self._table_has_column("rc_state_daily", "state_attrs_json")

    def _table_has_column(self, table_name: str, column_name: str) -> bool:
        try:
            rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        except Exception:
            return False
        for row in rows:
            try:
                if row[1] == column_name:
                    return True
            except Exception:
                continue
        return False

    def _normalize_reasons(self, reasons: list[ReasonCode]) -> list[ReasonCode]:
        if ReasonCode.ENTRY_CONDITIONS_MET in reasons:
            return [ReasonCode.ENTRY_CONDITIONS_MET]
        return reasons

    def _state_attrs_json(self, attrs: StateAttrs) -> str:
        downtrend_origin = attrs.downtrend_origin
        downtrend_entry_type = attrs.downtrend_entry_type
        decline_profile = attrs.decline_profile
        stabilization_phase = attrs.stabilization_phase
        entry_gate = attrs.entry_gate
        entry_quality = attrs.entry_quality
        if attrs.status:
            try:
                parsed = json.loads(attrs.status)
                if isinstance(parsed, dict):
                    if downtrend_origin is None:
                        value = parsed.get("downtrend_origin")
                        if isinstance(value, str):
                            downtrend_origin = value
                    if downtrend_entry_type is None:
                        value = parsed.get("downtrend_entry_type")
                        if isinstance(value, str):
                            downtrend_entry_type = value
                    if decline_profile is None:
                        value = parsed.get("decline_profile")
                        if isinstance(value, str):
                            decline_profile = value
                    if stabilization_phase is None:
                        value = parsed.get("stabilization_phase")
                        if isinstance(value, str):
                            stabilization_phase = value
                    if entry_gate is None:
                        value = parsed.get("entry_gate")
                        if isinstance(value, str):
                            entry_gate = value
                    if entry_quality is None:
                        value = parsed.get("entry_quality")
                        if isinstance(value, str):
                            entry_quality = value
            except Exception:
                pass
        payload: dict[str, str] = {}
        if isinstance(downtrend_origin, str):
            payload["downtrend_origin"] = downtrend_origin
        if isinstance(downtrend_entry_type, str):
            payload["downtrend_entry_type"] = downtrend_entry_type
        if isinstance(decline_profile, str):
            payload["decline_profile"] = decline_profile
        if isinstance(stabilization_phase, str):
            payload["stabilization_phase"] = stabilization_phase
        if isinstance(entry_gate, str):
            payload["entry_gate"] = entry_gate
        if isinstance(entry_quality, str):
            payload["entry_quality"] = entry_quality
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    def insert_state(
        self,
        ticker: str,
        date: str,
        state: State,
        reasons: list[ReasonCode],
        attrs: StateAttrs,
        run_id: str,
    ) -> None:
        normalized_reasons = self._normalize_reasons(reasons)
        reasons_json = json.dumps(
            [reason_to_persisted(reason) for reason in normalized_reasons],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        if self._has_state_attrs_json:
            state_attrs_json = self._state_attrs_json(attrs)
            self._conn.execute(
                """
                INSERT INTO rc_state_daily (
                    ticker,
                    date,
                    state,
                    reasons_json,
                    confidence,
                    age,
                    state_attrs_json,
                    run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker, date) DO UPDATE SET
                    state=excluded.state,
                    reasons_json=excluded.reasons_json,
                    confidence=excluded.confidence,
                    age=excluded.age,
                    state_attrs_json=excluded.state_attrs_json,
                    run_id=excluded.run_id
                """,
                (
                    ticker,
                    date,
                    state.value,
                    reasons_json,
                    attrs.confidence,
                    attrs.age,
                    state_attrs_json,
                    run_id,
                ),
            )
            return
        self._conn.execute(
            """
            INSERT INTO rc_state_daily (
                ticker,
                date,
                state,
                reasons_json,
                confidence,
                age,
                run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker, date) DO UPDATE SET
                state=excluded.state,
                reasons_json=excluded.reasons_json,
                confidence=excluded.confidence,
                age=excluded.age,
                run_id=excluded.run_id
            """,
            (
                ticker,
                date,
                state.value,
                reasons_json,
                attrs.confidence,
                attrs.age,
                run_id,
            ),
        )

    def insert_signals(
        self,
        ticker: str,
        date: str,
        signals: SignalSet,
        run_id: str,
    ) -> None:
        signal_keys = sorted({key.value for key in signals.signals})
        signal_keys_json = json.dumps(
            signal_keys,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        self._conn.execute(
            """
            INSERT INTO rc_signal_daily (
                ticker,
                date,
                signal_keys_json,
                run_id
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(ticker, date) DO UPDATE SET
                signal_keys_json=excluded.signal_keys_json,
                run_id=excluded.run_id
            """,
            (
                ticker,
                date,
                signal_keys_json,
                run_id,
            ),
        )

    def insert_transition(
        self,
        ticker: str,
        date: str,
        transition: Transition | None,
        run_id: str,
    ) -> None:
        if transition is None:
            return

        reasons = transition.reason_codes
        if not reasons:
            if transition.from_state == State.PASS and transition.to_state == State.NO_TRADE:
                reasons = [ReasonCode.PASS_COMPLETED]
            elif (
                transition.from_state == State.ENTRY_WINDOW
                and transition.to_state == State.PASS
            ):
                reasons = [ReasonCode.ENTRY_WINDOW_COMPLETED]
        normalized_reasons = self._normalize_reasons(reasons)
        reasons_json = json.dumps(
            [reason_to_persisted(reason) for reason in normalized_reasons],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        self._conn.execute(
            """
            INSERT OR REPLACE INTO rc_transition (
                ticker,
                date,
                from_state,
                to_state,
                reasons_json,
                run_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                date,
                transition.from_state.value,
                transition.to_state.value,
                reasons_json,
                run_id,
            ),
        )
