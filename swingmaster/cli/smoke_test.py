from __future__ import annotations

from pathlib import Path

from swingmaster.app_api.facade import SwingmasterApplication
from swingmaster.app_api.ports import PrevStateProvider, SignalProvider
from swingmaster.core.domain.enums import State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.rule_policy_v1 import RuleBasedTransitionPolicyV1
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet
from swingmaster.infra.sqlite.db import get_connection
from swingmaster.infra.sqlite.migrator import apply_migrations


class DummySignalProvider(SignalProvider):
    def get_signals(self, ticker: str, date: str) -> SignalSet:
        if ticker == "AAA":
            signals = {
                SignalKey.TREND_STARTED: Signal(
                    key=SignalKey.TREND_STARTED,
                    value=True,
                    confidence=None,
                    source="dummy",
                )
            }
        elif ticker == "BBB":
            signals = {
                SignalKey.DATA_INSUFFICIENT: Signal(
                    key=SignalKey.DATA_INSUFFICIENT,
                    value=True,
                    confidence=None,
                    source="dummy",
                )
            }
        else:
            signals = {}
        return SignalSet(signals=signals)


class DummyPrevStateProvider(PrevStateProvider):
    def get_prev(self, ticker: str, date: str) -> tuple[State, StateAttrs]:
        return State.NO_TRADE, StateAttrs(confidence=None, age=0, status=None)


def main() -> None:
    db_path = "swingmaster_smoke.db"
    db_file = Path(db_path)
    if db_file.exists():
        db_file.unlink()
    conn = get_connection(db_path)
    try:
        apply_migrations(conn)
        conn.commit()

        policy = RuleBasedTransitionPolicyV1()
        app = SwingmasterApplication(
            conn=conn,
            policy=policy,
            signal_provider=DummySignalProvider(),
            prev_state_provider=DummyPrevStateProvider(),
            engine_version="dev",
            policy_id="rule_v1",
            policy_version="dev",
        )

        run_id = app.run_daily(as_of_date="2026-01-21", tickers=["AAA", "BBB", "CCC"])

        rc_run_count = conn.execute("SELECT COUNT(*) FROM rc_run").fetchone()[0]
        rc_state_count = conn.execute("SELECT COUNT(*) FROM rc_state_daily").fetchone()[0]
        rc_transition_count = conn.execute("SELECT COUNT(*) FROM rc_transition").fetchone()[0]

        print(f"OK run_id={run_id}")
        print(f"rc_run={rc_run_count} rc_state_daily={rc_state_count} rc_transition={rc_transition_count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
