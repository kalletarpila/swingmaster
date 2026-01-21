from __future__ import annotations

import datetime
import sqlite3
import uuid

from swingmaster.core.engine.evaluator import TransitionPolicy, evaluate_step
from swingmaster.infra.sqlite.repos.rc_run_repo import RcRunRepo
from swingmaster.infra.sqlite.repos.rc_state_repo import RcStateRepo
from .ports import PrevStateProvider, SignalProvider


class SwingmasterApplication:
    def __init__(
        self,
        conn: sqlite3.Connection,
        policy: TransitionPolicy,
        signal_provider: SignalProvider,
        prev_state_provider: PrevStateProvider,
        engine_version: str,
        policy_id: str,
        policy_version: str,
    ) -> None:
        self._conn = conn
        self._policy = policy
        self._signal_provider = signal_provider
        self._prev_state_provider = prev_state_provider
        self._engine_version = engine_version
        self._policy_id = policy_id
        self._policy_version = policy_version
        self._run_repo = RcRunRepo(conn)
        self._state_repo = RcStateRepo(conn)

    def run_daily(self, as_of_date: str, tickers: list[str]) -> str:
        run_id = str(uuid.uuid4())
        created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        self._conn.execute("BEGIN")
        try:
            self._run_repo.insert_run(
                run_id,
                created_at,
                self._engine_version,
                self._policy_id,
                self._policy_version,
            )

            for ticker in tickers:
                signals = self._signal_provider.get_signals(ticker, as_of_date)
                prev_state, prev_attrs = self._prev_state_provider.get_prev(ticker, as_of_date)

                evaluation = evaluate_step(
                    prev_state=prev_state,
                    prev_attrs=prev_attrs,
                    signals=signals,
                    policy=self._policy,
                )

                self._state_repo.insert_state(
                    ticker=ticker,
                    date=as_of_date,
                    state=evaluation.final_state,
                    reasons=evaluation.reasons,
                    attrs=evaluation.final_attrs,
                    run_id=run_id,
                )
                self._state_repo.insert_transition(
                    ticker=ticker,
                    date=as_of_date,
                    transition=evaluation.transition,
                    run_id=run_id,
                )

            self._conn.commit()
            return run_id
        except Exception:
            self._conn.rollback()
            raise
