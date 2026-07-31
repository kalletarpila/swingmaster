from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

from swingmaster.cli.rebuild_earnings_event_matches import run_cli
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.quarter_earnings_match_repo import (
    AVAILABILITY_POLICY,
    MATCHER_VERSION,
    PersistedQuarterEarningsMatch,
    apply_rebuild,
    build_desired_matches,
    content_hash,
    create_verified_backup,
    dry_run_rebuild,
    next_usa_trading_day,
    resolve_effective_trading_date,
    validate_desired_matches,
    validate_temp_path,
    verify_match_table,
)


def test_migration_schema_and_indexes() -> None:
    db_path = _db_path("schema")
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(rc_fundamental_quarter_earnings_match)")}
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(rc_fundamental_quarter_earnings_match)")}

    assert {
        "id",
        "market",
        "ticker",
        "period_end_date",
        "earnings_event_id",
        "announcement_at",
        "effective_trading_date",
        "availability_policy",
        "matcher_version",
    }.issubset(columns)
    assert "idx_rc_fundamental_qem_ticker_period" in indexes
    assert "idx_rc_fundamental_qem_effective_date" in indexes


def test_natural_uniqueness_by_period_and_unique_event_use() -> None:
    db_path = _db_path("unique")
    _seed_basic_db(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        summary = apply_rebuild(conn, backup_verified=True, applied_at_utc="2026-07-31T00:00:00Z")
        first = conn.execute("SELECT * FROM rc_fundamental_quarter_earnings_match LIMIT 1").fetchone()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO rc_fundamental_quarter_earnings_match (
                    market, ticker, period_end_date, earnings_event_id, announcement_at,
                    announcement_date, announcement_session, effective_trading_date,
                    effective_date_status, reporting_delay_days, matching_status,
                    matching_confidence, matching_method, candidate_count,
                    availability_policy, matcher_version, created_at_utc, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(first[1:]),
            )

    assert summary["persisted_match_count"] == 6


def test_high_medium_low_confidence_default_and_optional_exclusion() -> None:
    db_path = _db_path("confidence")
    _seed_basic_db(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        desired, outcomes = build_desired_matches(conn)
        no_low, _ = build_desired_matches(conn, include_low_confidence=False)

    assert sum(1 for row in desired if row.matching_confidence == "HIGH") == 4
    assert sum(1 for row in desired if row.matching_confidence == "MEDIUM") == 1
    assert sum(1 for row in desired if row.matching_confidence == "LOW") == 1
    assert len(no_low) == len(desired) - 1
    assert any(outcome.matching_status == "MATCHED_LOW_CONFIDENCE" for outcome in outcomes)


def test_ambiguous_and_unmatched_results_are_not_persisted() -> None:
    db_path = _db_path("ambiguous")
    _seed_basic_db(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        summary = apply_rebuild(conn, backup_verified=True, applied_at_utc="2026-07-31T00:00:00Z")
        persisted_bad = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_quarter_earnings_match
            WHERE ticker IN ('AMBIG', 'NONE')
            """
        ).fetchone()[0]

    assert summary["ambiguous_count"] == 1
    assert summary["unmatched_count"] >= 1
    assert persisted_bad == 0


def test_effective_trading_date_sessions_and_holiday_awareness() -> None:
    assert resolve_effective_trading_date("2026-01-30", "BEFORE_MARKET") == (
        "2026-01-30",
        "RESOLVED_SAME_TRADING_DAY",
    )
    assert resolve_effective_trading_date("2026-01-30", "DURING_MARKET") == (
        "2026-01-30",
        "RESOLVED_SAME_TRADING_DAY",
    )
    assert resolve_effective_trading_date("2026-01-30", "AFTER_MARKET") == (
        "2026-02-02",
        "RESOLVED_NEXT_TRADING_DAY",
    )
    assert resolve_effective_trading_date("2026-07-02", "AFTER_MARKET") == (
        "2026-07-06",
        "RESOLVED_NEXT_TRADING_DAY",
    )
    assert resolve_effective_trading_date("2026-07-03", "BEFORE_MARKET") == (None, "NO_TRADING_CALENDAR_DATE")
    assert resolve_effective_trading_date("2026-01-30", "UNKNOWN") == (None, "UNKNOWN_SESSION")
    assert next_usa_trading_day(__import__("datetime").date(2026, 7, 2)).isoformat() == "2026-07-06"


def test_full_transactional_rebuild_idempotency_obsolete_removal_and_hash() -> None:
    db_path = _db_path("idempotent")
    _seed_basic_db(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        first = apply_rebuild(conn, backup_verified=True, applied_at_utc="2026-07-31T00:00:00Z")
        first_hash = content_hash(conn)
        second = apply_rebuild(conn, backup_verified=True, applied_at_utc="2026-07-31T00:00:00Z")
        second_hash = content_hash(conn)
        conn.execute("DELETE FROM rc_fundamental_quarterly WHERE ticker='LOW' AND period_end_date='2025-12-31'")
        conn.commit()
        third = apply_rebuild(conn, backup_verified=True, applied_at_utc="2026-07-31T00:00:00Z")

    assert first["inserted_count"] == 6
    assert second["inserted_count"] == 0
    assert second["updated_count"] == 0
    assert second["deleted_obsolete_count"] == 0
    assert second["unchanged_count"] == 6
    assert first_hash == second_hash
    assert third["deleted_obsolete_count"] == 1


def test_staging_validation_failure_preserves_prior_table() -> None:
    db_path = _db_path("validation_failure")
    _seed_basic_db(db_path)
    duplicate = _record("AAPL", "2025-03-31", 1)

    with sqlite3.connect(str(db_path)) as conn:
        apply_rebuild(conn, backup_verified=True, applied_at_utc="2026-07-31T00:00:00Z")
        before_hash = content_hash(conn)
        with pytest.raises(RuntimeError, match="DUPLICATE_PERIOD_MATCH"):
            validate_desired_matches([duplicate, duplicate], max_delay_days=140)
        after_hash = content_hash(conn)

    assert before_hash == after_hash


def test_matcher_version_update_behavior() -> None:
    db_path = _db_path("version_update")
    _seed_basic_db(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        apply_rebuild(conn, backup_verified=True, applied_at_utc="2026-07-31T00:00:00Z")
        conn.execute("UPDATE rc_fundamental_quarter_earnings_match SET matcher_version='old'")
        conn.commit()
        summary = apply_rebuild(conn, backup_verified=True, applied_at_utc="2026-07-31T00:00:00Z")
        versions = [
            row[0]
            for row in conn.execute("SELECT DISTINCT matcher_version FROM rc_fundamental_quarter_earnings_match").fetchall()
        ]

    assert summary["updated_count"] == 6
    assert versions == [MATCHER_VERSION]


def test_no_source_table_writes_dry_run_and_apply_requires_backup() -> None:
    db_path = _db_path("source_counts")
    _seed_basic_db(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        before = _source_counts(conn)
        dry = dry_run_rebuild(conn)
        after_dry = _source_counts(conn)
        with pytest.raises(RuntimeError, match="REQUIRES_VERIFIED_BACKUP"):
            apply_rebuild(conn, backup_verified=False)
        apply_rebuild(conn, backup_verified=True, applied_at_utc="2026-07-31T00:00:00Z")
        after_apply = _source_counts(conn)

    assert dry["transaction_status"] == "DRY_RUN"
    assert before == after_dry == after_apply


def test_temp_only_artifact_backup_paths_and_cli_dry_run() -> None:
    db_path = _db_path("cli")
    _seed_basic_db(db_path)
    root = _runtime_root("cli_artifacts")
    checkpoint = root / "checkpoint.json"
    summary_json = root / "summary.json"
    output_csv = root / "matches.csv"

    payload = run_cli(
        argparse.Namespace(
            fundamentals_db=str(db_path),
            max_delay_days=140,
            include_low_confidence=True,
            exclude_low_confidence=False,
            dry_run=True,
            apply=False,
            backup=None,
            checkpoint_json=str(checkpoint),
            summary_json=str(summary_json),
            output_csv=str(output_csv),
            json_output=True,
        )
    )

    assert validate_temp_path(checkpoint) == checkpoint.resolve()
    assert summary_json.exists()
    assert output_csv.exists()
    assert payload["summary"]["transaction_status"] == "DRY_RUN"
    with pytest.raises(ValueError, match="RUNTIME_PATH_OUTSIDE_TEMP"):
        validate_temp_path(Path("/tmp/outside.json"))


def test_cli_apply_creates_verified_backup_and_deterministic_output() -> None:
    db_path = _db_path("cli_apply")
    _seed_basic_db(db_path)
    root = _runtime_root("cli_apply")
    backup = root / "backups" / "fundamentals.bak"

    first = run_cli(
        argparse.Namespace(
            fundamentals_db=str(db_path),
            max_delay_days=140,
            include_low_confidence=True,
            exclude_low_confidence=False,
            dry_run=False,
            apply=True,
            backup=str(backup),
            checkpoint_json=str(root / "checkpoint.json"),
            summary_json=str(root / "summary.json"),
            output_csv=str(root / "matches.csv"),
            json_output=True,
        )
    )
    with sqlite3.connect(str(db_path)) as conn:
        checks = verify_match_table(conn)

    assert first["backup"]["verified"] is True
    assert Path(first["backup"]["path"]).exists()
    assert first["summary"]["transaction_status"] == "COMMITTED"
    assert checks["row_count"] == 6
    assert checks["duplicate_period_keys"] == 0
    assert checks["duplicate_event_keys"] == 0
    assert checks["effective_date_mismatch_rows"] == 0


def _db_path(label: str) -> Path:
    root = _runtime_root("tests")
    return root / f"{label}_{uuid4().hex}.db"


def _runtime_root(label: str) -> Path:
    root = Path.cwd() / "temp" / "earnings_event_match_persistence" / "tests" / label
    root.mkdir(parents=True, exist_ok=True)
    return root


def _seed_basic_db(db_path: Path) -> None:
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarter(conn, "AAPL", "2025-03-31")
        _insert_quarter(conn, "AAPL", "2025-06-30")
        _insert_quarter(conn, "AAPL", "2025-09-30")
        _insert_quarter(conn, "MED", "2025-03-31")
        _insert_quarter(conn, "LOW", "2025-12-31")
        _insert_quarter(conn, "HOL", "2026-06-30")
        _insert_quarter(conn, "AMBIG", "2025-03-31")
        _insert_quarter(conn, "AMBIG", "2025-06-30")
        _insert_quarter(conn, "NONE", "2025-03-31")
        _insert_event(conn, "AAPL", "2025-04-25T08:00:00-04:00", "2025-04-25", "BEFORE_MARKET")
        _insert_event(conn, "AAPL", "2025-07-25T16:00:00-04:00", "2025-07-25", "DURING_MARKET")
        _insert_event(conn, "AAPL", "2025-10-25T16:01:00-04:00", "2025-10-25", "AFTER_MARKET")
        _insert_event(conn, "MED", "2025-07-05T08:00:00-04:00", "2025-07-05", "BEFORE_MARKET")
        _insert_event(conn, "LOW", "2026-05-01T08:00:00-04:00", "2026-05-01", "BEFORE_MARKET")
        _insert_event(conn, "HOL", "2026-07-02T16:01:00-04:00", "2026-07-02", "AFTER_MARKET")
        _insert_event(conn, "AMBIG", "2025-04-10T08:00:00-04:00", "2025-04-10", "BEFORE_MARKET")
        _insert_event(conn, "AMBIG", "2025-04-20T08:00:00-04:00", "2025-04-20", "BEFORE_MARKET")
        conn.commit()


def _insert_quarter(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (ticker, period_end_date, revenue, run_id)
        VALUES (?, ?, 1.0, 'RUN')
        """,
        (ticker, period_end_date),
    )


def _insert_event(
    conn: sqlite3.Connection,
    ticker: str,
    announcement_at: str,
    announcement_date: str,
    announcement_session: str,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_earnings_event (
            market, ticker, announcement_at, announcement_date, announcement_session,
            is_reported, reported_eps, estimated_eps, surprise_pct, source,
            source_observed_at_utc, source_timezone, created_at_utc, updated_at_utc
        )
        VALUES ('usa', ?, ?, ?, ?, 1, 1.0, 0.9, 1.1, 'YAHOO_FINANCE',
                '2026-07-31T00:00:00Z', 'America/New_York',
                '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z')
        """,
        (ticker, announcement_at, announcement_date, announcement_session),
    )


def _source_counts(conn: sqlite3.Connection) -> tuple[int, int]:
    return (
        conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM rc_earnings_event").fetchone()[0],
    )


def _record(ticker: str, period_end_date: str, event_id: int) -> PersistedQuarterEarningsMatch:
    return PersistedQuarterEarningsMatch(
        market="usa",
        ticker=ticker,
        period_end_date=period_end_date,
        earnings_event_id=event_id,
        announcement_at="2025-04-25T08:00:00-04:00",
        announcement_date="2025-04-25",
        announcement_session="BEFORE_MARKET",
        effective_trading_date="2025-04-25",
        effective_date_status="RESOLVED_SAME_TRADING_DAY",
        reporting_delay_days=25,
        matching_status="MATCHED_HIGH_CONFIDENCE",
        matching_confidence="HIGH",
        matching_method="SEQUENTIAL_NEXT_REPORTED_EVENT_V1",
        candidate_count=1,
        availability_policy=AVAILABILITY_POLICY,
        matcher_version=MATCHER_VERSION,
    )
