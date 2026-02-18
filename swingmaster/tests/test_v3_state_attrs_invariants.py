"""Invariant tests for v3 state attrs persistence and compatibility guards."""

from __future__ import annotations

import argparse
import json
import sqlite3

import pytest

from swingmaster.app_api.providers.sqlite_prev_state_provider import SQLitePrevStateProvider
from swingmaster.cli import run_daily_universe, run_range_universe
from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.rule_policy_v3 import RuleBasedTransitionPolicyV3
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.infra.sqlite.repos.rc_state_repo import RcStateRepo


class _StopAfterGuard(RuntimeError):
    pass


class _FakeUniverseReader:
    def __init__(self, _conn: sqlite3.Connection) -> None:
        pass

    def resolve_tickers(self, _spec) -> list[str]:
        return ["AAA"]

    def filter_by_osakedata(
        self,
        *,
        tickers: list[str],
        as_of_date: str,
        osakedata_table: str,
        min_history_rows: int,
        require_row_on_date: bool,
    ) -> list[str]:
        del as_of_date, osakedata_table, min_history_rows, require_row_on_date
        return tickers


def _conn_memory() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _signal_set(*keys: SignalKey) -> SignalSet:
    return SignalSet(signals={k: Signal(key=k, value=True, confidence=None, source="test") for k in keys})


def _run_daily_args(signal_version: str, policy_version: str) -> argparse.Namespace:
    return argparse.Namespace(
        date="2026-01-02",
        md_db="unused",
        rc_db="unused",
        mode="market",
        tickers=None,
        market="OMXH",
        sector=None,
        industry=None,
        limit=1,
        sample="first_n",
        seed=1,
        min_history_rows=0,
        require_row_on_date=False,
        policy_id="rule_v2",
        policy_version=policy_version,
        signal_version=signal_version,
        debug=False,
        debug_limit=0,
        debug_show_tickers=False,
        debug_show_mismatches=False,
    )


def _run_range_args(signal_version: str, policy_version: str) -> argparse.Namespace:
    return argparse.Namespace(
        date_from="2026-01-01",
        date_to="2026-01-02",
        md_db="unused",
        rc_db="unused",
        mode="market",
        tickers=None,
        market="OMXH",
        sector=None,
        industry=None,
        limit=1,
        sample="first_n",
        seed=1,
        min_history_rows=0,
        require_row_on_date=False,
        max_days=1,
        dry_run=False,
        policy_id="rule_v2",
        policy_version=policy_version,
        signal_version=signal_version,
        debug=False,
        debug_limit=0,
        debug_show_tickers=False,
        debug_show_mismatches=False,
        print_signals=False,
        print_signals_limit=0,
    )


def _patch_cli_for_guard_test(monkeypatch: pytest.MonkeyPatch, module, args: argparse.Namespace) -> None:
    monkeypatch.setattr(module, "parse_args", lambda: args)
    monkeypatch.setattr(module, "get_readonly_connection", lambda _path: _conn_memory())
    monkeypatch.setattr(module, "get_connection", lambda _path: _conn_memory())
    monkeypatch.setattr(module, "apply_migrations", lambda _conn: None)
    monkeypatch.setattr(module, "TickerUniverseReader", _FakeUniverseReader)
    if module is run_range_universe:
        monkeypatch.setattr(module, "build_trading_days", lambda *a, **k: ["2026-01-02"])

    def _stop_build(*_a, **_k):
        raise _StopAfterGuard("stop after guard")

    monkeypatch.setattr(module, "build_swingmaster_app", _stop_build)


def test_state_attrs_json_carried_forward_when_state_unchanged() -> None:
    conn = _conn_memory()
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)

    repo = RcStateRepo(conn)
    prev_provider = SQLitePrevStateProvider(conn)

    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-1", "2026-01-01T00:00:00Z", "dev", "rule_v3", "v3"),
    )
    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-2", "2026-01-02T00:00:00Z", "dev", "rule_v3", "v3"),
    )

    repo.insert_state(
        ticker="AAA",
        date="2026-01-01",
        state=State.DOWNTREND_EARLY,
        reasons=[ReasonCode.TREND_STARTED],
        attrs=StateAttrs(
            confidence=None,
            age=1,
            status=None,
            downtrend_origin="SLOW",
            decline_profile="SLOW_DRIFT",
        ),
        run_id="run-1",
    )

    _state, prev_attrs = prev_provider.get_prev("AAA", "2026-01-02")
    repo.insert_state(
        ticker="AAA",
        date="2026-01-02",
        state=State.DOWNTREND_EARLY,
        reasons=[ReasonCode.NO_SIGNAL],
        attrs=StateAttrs(
            confidence=prev_attrs.confidence,
            age=prev_attrs.age + 1,
            status=prev_attrs.status,
            downtrend_origin=None,
            decline_profile=None,
        ),
        run_id="run-2",
    )

    row = conn.execute(
        "SELECT state_attrs_json FROM rc_state_daily WHERE ticker=? AND date=?",
        ("AAA", "2026-01-02"),
    ).fetchone()
    assert row is not None
    payload = json.loads(row["state_attrs_json"])
    assert payload["decline_profile"] == "SLOW_DRIFT"
    assert payload["downtrend_origin"] == "SLOW"


def test_decline_profile_upgrade_is_one_way_only() -> None:
    policy = RuleBasedTransitionPolicyV3()

    decision1 = policy.decide(
        prev_state=State.NO_TRADE,
        prev_attrs=StateAttrs(confidence=None, age=0, status=None),
        signals=_signal_set(SignalKey.TREND_STARTED),
    )
    assert decision1.next_state == State.DOWNTREND_EARLY
    assert decision1.attrs_update is not None
    assert decision1.attrs_update.decline_profile == "UNKNOWN"

    decision2 = policy.decide(
        prev_state=decision1.next_state,
        prev_attrs=decision1.attrs_update,
        signals=_signal_set(SignalKey.STRUCTURAL_DOWNTREND_DETECTED),
    )
    assert decision2.attrs_update is not None
    assert decision2.attrs_update.decline_profile == "STRUCTURAL_DOWNTREND"

    decision3 = policy.decide(
        prev_state=decision2.next_state,
        prev_attrs=decision2.attrs_update,
        signals=_signal_set(SignalKey.SHARP_SELL_OFF_DETECTED),
    )
    assert decision3.attrs_update is not None
    assert decision3.attrs_update.decline_profile == "STRUCTURAL_DOWNTREND"


def test_downtrend_entry_type_carry_forward_not_overwritten() -> None:
    conn = _conn_memory()
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)

    repo = RcStateRepo(conn)
    prev_provider = SQLitePrevStateProvider(conn)
    policy = RuleBasedTransitionPolicyV3()

    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-1", "2026-01-01T00:00:00Z", "dev", "rule_v3", "v3"),
    )
    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-2", "2026-01-02T00:00:00Z", "dev", "rule_v3", "v3"),
    )

    day1 = policy.decide(
        prev_state=State.NO_TRADE,
        prev_attrs=StateAttrs(confidence=None, age=0, status=None),
        signals=_signal_set(SignalKey.SLOW_DECLINE_STARTED),
    )
    assert day1.next_state == State.DOWNTREND_EARLY
    assert day1.attrs_update is not None
    assert day1.attrs_update.downtrend_entry_type == "SLOW_SOFT"
    repo.insert_state(
        ticker="AAA",
        date="2026-01-01",
        state=day1.next_state,
        reasons=day1.reason_codes,
        attrs=day1.attrs_update,
        run_id="run-1",
    )

    prev_state2, prev_attrs2 = prev_provider.get_prev("AAA", "2026-01-02")
    day2 = policy.decide(
        prev_state=prev_state2,
        prev_attrs=prev_attrs2,
        signals=_signal_set(SignalKey.TREND_STARTED, SignalKey.DOW_NEW_LL),
    )
    assert day2.attrs_update is not None
    assert day2.attrs_update.downtrend_entry_type == "SLOW_SOFT"
    repo.insert_state(
        ticker="AAA",
        date="2026-01-02",
        state=day2.next_state,
        reasons=day2.reason_codes,
        attrs=day2.attrs_update,
        run_id="run-2",
    )

    row = conn.execute(
        "SELECT state_attrs_json FROM rc_state_daily WHERE ticker=? AND date=?",
        ("AAA", "2026-01-02"),
    ).fetchone()
    assert row is not None
    payload = json.loads(row["state_attrs_json"])
    assert payload["downtrend_entry_type"] == "SLOW_SOFT"


def test_no_trade_entry_with_both_signals_and_trend_reason_keeps_trend_entry_type_family() -> None:
    policy = RuleBasedTransitionPolicyV3()

    decision = policy.decide(
        prev_state=State.NO_TRADE,
        prev_attrs=StateAttrs(
            confidence=None,
            age=0,
            status=json.dumps({"custom_keep": "ok"}, separators=(",", ":"), ensure_ascii=False),
        ),
        signals=_signal_set(
            SignalKey.TREND_STARTED,
            SignalKey.SLOW_DECLINE_STARTED,
        ),
    )

    assert decision.next_state == State.DOWNTREND_EARLY
    assert ReasonCode.TREND_STARTED in decision.reason_codes
    assert decision.attrs_update is not None
    assert decision.attrs_update.downtrend_origin == "TREND"
    assert decision.attrs_update.downtrend_entry_type is not None
    assert decision.attrs_update.downtrend_entry_type.startswith("TREND_")
    assert not decision.attrs_update.downtrend_entry_type.startswith("SLOW_")


def test_stabilization_phase_carry_forward_and_updates() -> None:
    conn = _conn_memory()
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)

    repo = RcStateRepo(conn)
    prev_provider = SQLitePrevStateProvider(conn)
    policy = RuleBasedTransitionPolicyV3()

    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-1", "2026-01-01T00:00:00Z", "dev", "rule_v3", "v3"),
    )
    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-2", "2026-01-02T00:00:00Z", "dev", "rule_v3", "v3"),
    )
    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-3", "2026-01-03T00:00:00Z", "dev", "rule_v3", "v3"),
    )

    decision1 = policy.decide(
        prev_state=State.DOWNTREND_LATE,
        prev_attrs=StateAttrs(confidence=None, age=0, status=None),
        signals=_signal_set(SignalKey.SELLING_PRESSURE_EASED),
    )
    assert decision1.next_state == State.STABILIZING
    assert decision1.attrs_update is not None
    assert decision1.attrs_update.stabilization_phase == "EARLY_STABILIZATION"
    repo.insert_state(
        ticker="AAA",
        date="2026-01-01",
        state=decision1.next_state,
        reasons=decision1.reason_codes,
        attrs=decision1.attrs_update,
        run_id="run-1",
    )

    prev_state2, prev_attrs2 = prev_provider.get_prev("AAA", "2026-01-02")
    decision2 = policy.decide(
        prev_state=prev_state2,
        prev_attrs=prev_attrs2,
        signals=_signal_set(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.VOLATILITY_COMPRESSION_DETECTED,
        ),
    )
    assert decision2.next_state == State.STABILIZING
    assert decision2.attrs_update is not None
    assert decision2.attrs_update.stabilization_phase == "BASE_BUILDING"
    repo.insert_state(
        ticker="AAA",
        date="2026-01-02",
        state=decision2.next_state,
        reasons=decision2.reason_codes,
        attrs=decision2.attrs_update,
        run_id="run-2",
    )

    prev_state3, prev_attrs3 = prev_provider.get_prev("AAA", "2026-01-03")
    decision3 = policy.decide(
        prev_state=prev_state3,
        prev_attrs=prev_attrs3,
        signals=_signal_set(SignalKey.ENTRY_SETUP_VALID),
    )
    assert decision3.next_state == State.ENTRY_WINDOW
    assert decision3.attrs_update is not None
    assert decision3.attrs_update.stabilization_phase == "EARLY_REVERSAL"
    repo.insert_state(
        ticker="AAA",
        date="2026-01-03",
        state=decision3.next_state,
        reasons=decision3.reason_codes,
        attrs=decision3.attrs_update,
        run_id="run-3",
    )

    row1 = conn.execute(
        "SELECT state_attrs_json FROM rc_state_daily WHERE ticker=? AND date=?",
        ("AAA", "2026-01-01"),
    ).fetchone()
    row2 = conn.execute(
        "SELECT state_attrs_json FROM rc_state_daily WHERE ticker=? AND date=?",
        ("AAA", "2026-01-02"),
    ).fetchone()
    row3 = conn.execute(
        "SELECT state_attrs_json FROM rc_state_daily WHERE ticker=? AND date=?",
        ("AAA", "2026-01-03"),
    ).fetchone()
    assert row1 is not None
    assert row2 is not None
    assert row3 is not None

    payload1 = json.loads(row1["state_attrs_json"])
    payload2 = json.loads(row2["state_attrs_json"])
    payload3 = json.loads(row3["state_attrs_json"])
    assert payload1["stabilization_phase"] == "EARLY_STABILIZATION"
    assert payload2["stabilization_phase"] == "BASE_BUILDING"
    assert payload3["stabilization_phase"] == "EARLY_REVERSAL"


def test_entry_gate_and_quality_persistence_carry_forward() -> None:
    conn = _conn_memory()
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)

    repo = RcStateRepo(conn)
    prev_provider = SQLitePrevStateProvider(conn)
    policy = RuleBasedTransitionPolicyV3()

    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-1", "2026-01-01T00:00:00Z", "dev", "rule_v3", "v3"),
    )
    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-2", "2026-01-02T00:00:00Z", "dev", "rule_v3", "v3"),
    )

    decision1 = policy.decide(
        prev_state=State.DOWNTREND_LATE,
        prev_attrs=StateAttrs(confidence=None, age=0, status=None),
        signals=_signal_set(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.MA20_RECLAIMED,
            SignalKey.HIGHER_LOW_CONFIRMED,
        ),
    )
    assert decision1.next_state == State.ENTRY_WINDOW
    assert decision1.attrs_update is not None
    assert decision1.attrs_update.entry_gate == "EARLY_STAB_MA20_HL"
    assert decision1.attrs_update.entry_quality == "A"
    repo.insert_state(
        ticker="AAA",
        date="2026-01-01",
        state=decision1.next_state,
        reasons=decision1.reason_codes,
        attrs=decision1.attrs_update,
        run_id="run-1",
    )

    prev_state2, prev_attrs2 = prev_provider.get_prev("AAA", "2026-01-02")
    assert prev_state2 == State.ENTRY_WINDOW
    assert prev_attrs2.entry_gate == "EARLY_STAB_MA20_HL"
    assert prev_attrs2.entry_quality == "A"

    repo.insert_state(
        ticker="AAA",
        date="2026-01-02",
        state=State.ENTRY_WINDOW,
        reasons=[ReasonCode.ENTRY_CONDITIONS_MET],
        attrs=StateAttrs(
            confidence=prev_attrs2.confidence,
            age=prev_attrs2.age + 1,
            status=prev_attrs2.status,
            entry_gate=None,
            entry_quality=None,
        ),
        run_id="run-2",
    )

    row = conn.execute(
        "SELECT state_attrs_json FROM rc_state_daily WHERE ticker=? AND date=?",
        ("AAA", "2026-01-02"),
    ).fetchone()
    assert row is not None
    payload = json.loads(row["state_attrs_json"])
    assert payload["entry_gate"] == "EARLY_STAB_MA20_HL"
    assert payload["entry_quality"] == "A"


def test_no_trade_transition_clears_v3_metadata_keys_from_status() -> None:
    policy = RuleBasedTransitionPolicyV3()
    prev_status = json.dumps(
        {
            "custom_keep": "ok",
            "downtrend_origin": "SLOW",
            "downtrend_entry_type": "SLOW_STRUCTURAL",
            "decline_profile": "SLOW_DRIFT",
            "stabilization_phase": "EARLY_REVERSAL",
            "entry_gate": "LEGACY_ENTRY_SETUP_VALID",
            "entry_quality": "LEGACY",
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )

    decision = policy.decide(
        prev_state=State.STABILIZING,
        prev_attrs=StateAttrs(
            confidence=None,
            age=2,
            status=prev_status,
            downtrend_origin="SLOW",
            downtrend_entry_type="SLOW_STRUCTURAL",
            decline_profile="SLOW_DRIFT",
            stabilization_phase="EARLY_REVERSAL",
            entry_gate="LEGACY_ENTRY_SETUP_VALID",
            entry_quality="LEGACY",
        ),
        signals=_signal_set(SignalKey.INVALIDATED),
    )

    assert decision.next_state == State.NO_TRADE
    assert decision.attrs_update is not None
    assert decision.attrs_update.downtrend_origin is None
    assert decision.attrs_update.downtrend_entry_type is None
    assert decision.attrs_update.decline_profile is None
    assert decision.attrs_update.stabilization_phase is None
    assert decision.attrs_update.entry_gate is None
    assert decision.attrs_update.entry_quality is None

    assert decision.attrs_update.status is not None
    payload = json.loads(decision.attrs_update.status)
    assert payload == {"custom_keep": "ok"}


@pytest.mark.parametrize("module,args_builder", [(run_daily_universe, _run_daily_args), (run_range_universe, _run_range_args)])
def test_cli_rejects_mismatched_signal_and_policy_versions(
    monkeypatch: pytest.MonkeyPatch,
    module,
    args_builder,
) -> None:
    _patch_cli_for_guard_test(monkeypatch, module, args_builder("v3", "v2"))
    with pytest.raises(RuntimeError, match="Incompatible versions"):
        module.main()

    _patch_cli_for_guard_test(monkeypatch, module, args_builder("v2", "v3"))
    with pytest.raises(RuntimeError, match="Incompatible versions"):
        module.main()

    _patch_cli_for_guard_test(monkeypatch, module, args_builder("v2", "v2"))
    with pytest.raises(_StopAfterGuard):
        module.main()

    _patch_cli_for_guard_test(monkeypatch, module, args_builder("v3", "v3"))
    with pytest.raises(_StopAfterGuard):
        module.main()


def test_prev_state_provider_reads_state_attrs_json_for_downtrend_origin_and_decline_profile() -> None:
    conn = _conn_memory()
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)

    conn.execute(
        "INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version) VALUES (?, ?, ?, ?, ?)",
        ("run-1", "2026-01-01T00:00:00Z", "dev", "rule_v3", "v3"),
    )

    attrs_json = json.dumps(
        {
            "downtrend_origin": "TREND",
            "downtrend_entry_type": "TREND_STRUCTURAL",
            "decline_profile": "SHARP_SELL_OFF",
            "stabilization_phase": "EARLY_REVERSAL",
            "entry_gate": "LEGACY_ENTRY_SETUP_VALID",
            "entry_quality": "LEGACY",
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )
    conn.execute(
        """
        INSERT INTO rc_state_daily (
            ticker, date, state, reasons_json, confidence, age, run_id, state_attrs_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "AAA",
            "2026-01-01",
            "DOWNTREND_EARLY",
            "[]",
            None,
            1,
            "run-1",
            attrs_json,
        ),
    )

    provider = SQLitePrevStateProvider(conn)
    prev_state, prev_attrs = provider.get_prev("AAA", "2026-01-02")

    assert prev_state == State.DOWNTREND_EARLY
    assert prev_attrs.downtrend_origin == "TREND"
    assert prev_attrs.downtrend_entry_type == "TREND_STRUCTURAL"
    assert prev_attrs.decline_profile == "SHARP_SELL_OFF"
    assert prev_attrs.stabilization_phase == "EARLY_REVERSAL"
    assert prev_attrs.entry_gate == "LEGACY_ENTRY_SETUP_VALID"
    assert prev_attrs.entry_quality == "LEGACY"
