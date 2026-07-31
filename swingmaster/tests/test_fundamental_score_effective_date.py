from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from swingmaster.cli.rebuild_fundamental_score_effective_dates import main as rebuild_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_events import repository_root
from swingmaster.fundamentals.score_effective_date import (
    SCORE_EFFECTIVE_DATE_POLICY,
    STATUS_RESOLVED,
    STATUS_SOURCE_TTM_AMBIGUOUS,
    STATUS_SOURCE_TTM_EFFECTIVE_DATE_NULL,
    STATUS_SOURCE_TTM_NOT_FOUND,
    apply_score_effective_date_rows,
    compute_score_effective_dates,
    compute_score_row,
    ensure_score_effective_date_schema,
    score_effective_fields_hash,
    score_value_fields_hash,
    select_latest_score_as_of,
    select_latest_score_current,
    summarize,
)


def test_schema_migration_adds_score_effective_date_columns() -> None:
    db = _runtime_root() / f"{uuid.uuid4().hex}.db"
    run_migration(db)

    with sqlite3.connect(db) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(rc_fundamental_ttm)")}

    assert {
        "score_effective_trading_date",
        "score_effective_date_status",
        "score_effective_date_policy",
        "score_effective_date_source_ttm_as_of_date",
    }.issubset(columns)
    assert "idx_fundamental_ttm_score_effective_date" in indexes


def test_resolved_score_effective_date_copies_exact_ttm_effective_date() -> None:
    db = _build_db()
    _insert_score_ttm(db, "AAPL", "2025-12-31", "2026-02-01", 71.0)

    with sqlite3.connect(db) as conn:
        row = compute_score_effective_dates(conn)[0]

    assert row.score_effective_date_status == STATUS_RESOLVED
    assert row.score_effective_trading_date == "2026-02-01"
    assert row.source_ttm_as_of_date == "2025-12-31"
    assert row.score_effective_date_policy == SCORE_EFFECTIVE_DATE_POLICY


def test_summary_reports_score_availability_delay_metrics() -> None:
    db = _build_db()
    _insert_score_ttm(db, "AAPL", "2025-12-31", "2026-02-01", 71.0)
    _insert_score_ttm(db, "AAPL", "2026-03-31", "2026-04-30", 72.0)

    with sqlite3.connect(db) as conn:
        summary = summarize(compute_score_effective_dates(conn))

    assert summary["rows_whose_score_period_precedes_effective_date"] == 2
    assert summary["median_score_availability_delay_days"] == 31
    assert summary["p95_score_availability_delay_days"] == 32


def test_null_source_ttm_effective_date_status() -> None:
    db = _build_db()
    _insert_score_ttm(db, "MSFT", "2025-12-31", None, 72.0)

    with sqlite3.connect(db) as conn:
        row = compute_score_effective_dates(conn)[0]

    assert row.score_effective_date_status == STATUS_SOURCE_TTM_EFFECTIVE_DATE_NULL
    assert row.score_effective_trading_date is None
    assert row.source_ttm_as_of_date == "2025-12-31"


def test_missing_and_ambiguous_source_statuses_are_deterministic() -> None:
    score_row = {
        "ticker": "AAPL",
        "as_of_date": "2025-12-31",
        "fundamental_score": 71.0,
        "fundamental_score_lifecycle": 73.0,
        "score_rule": "FUND_SCORE_RULE_V1_1",
        "score_rule_lifecycle": "FUND_SCORE_RULE_V2_LIFECYCLE_SCALING_PRE",
    }

    missing = compute_score_row(score_row, {})
    ambiguous = compute_score_row(score_row, {("AAPL", "2025-12-31"): (STATUS_SOURCE_TTM_AMBIGUOUS, None)})

    assert missing.score_effective_date_status == STATUS_SOURCE_TTM_NOT_FOUND
    assert missing.score_effective_trading_date is None
    assert ambiguous.score_effective_date_status == STATUS_SOURCE_TTM_AMBIGUOUS
    assert ambiguous.source_ttm_as_of_date is None


def test_multiple_score_rule_columns_are_preserved() -> None:
    db = _build_db()
    _insert_score_ttm(db, "NVDA", "2025-10-31", "2025-12-01", 80.0)

    with sqlite3.connect(db) as conn:
        before = conn.execute(
            "SELECT score_rule, score_rule_lifecycle FROM rc_fundamental_ttm WHERE ticker = 'NVDA'"
        ).fetchone()
        rows = compute_score_effective_dates(conn)
        apply_score_effective_date_rows(conn, rows)
        after = conn.execute(
            "SELECT score_rule, score_rule_lifecycle FROM rc_fundamental_ttm WHERE ticker = 'NVDA'"
        ).fetchone()

    assert tuple(before) == tuple(after)


def test_current_selector_unchanged_and_historical_boundaries() -> None:
    db = _build_db()
    _insert_score_ttm(db, "AAPL", "2025-12-31", "2026-02-01", 71.0)
    _insert_score_ttm(db, "AAPL", "2026-03-31", "2026-04-30", 75.0)
    with sqlite3.connect(db) as conn:
        rows = compute_score_effective_dates(conn)
        apply_score_effective_date_rows(conn, rows)
        conn.commit()
        current = select_latest_score_current(conn, "AAPL")
        before = select_latest_score_as_of(conn, "AAPL", "2026-01-31")
        on_date = select_latest_score_as_of(conn, "AAPL", "2026-02-01")
        after = select_latest_score_as_of(conn, "AAPL", "2026-05-01")

    assert current.row is not None and current.row["as_of_date"] == "2026-03-31"
    assert before.reason == "NO_AVAILABLE_SCORE"
    assert on_date.row is not None and on_date.row["as_of_date"] == "2025-12-31"
    assert after.row is not None and after.row["as_of_date"] == "2026-03-31"


def test_full_dry_run_no_write_and_temp_artifacts() -> None:
    db = _resolved_fixture_db()
    root = _runtime_root() / "dry-run"
    before_effective = _safe_effective_hash(db)
    before_score = _score_hash(db)

    assert rebuild_main(["--fundamentals-db", str(db), "--dry-run", "--output-root", str(root)]) == 0

    assert before_effective == _safe_effective_hash(db)
    assert before_score == _score_hash(db)
    assert (root / "summary.json").exists()
    assert (root / "score_effective_dates.csv").exists()


def test_apply_requires_backup_and_rejects_outside_temp() -> None:
    db = _resolved_fixture_db()
    try:
        rebuild_main(["--fundamentals-db", str(db), "--apply", "--output-root", str(_runtime_root() / "apply-no-backup")])
    except RuntimeError as exc:
        assert str(exc) == "APPLY_REQUIRES_BACKUP"
    else:
        raise AssertionError("expected apply backup guard")

    try:
        rebuild_main(["--fundamentals-db", str(db), "--dry-run", "--output-root", str(repository_root() / "bad-score-effective")])
    except ValueError as exc:
        assert "RUNTIME_PATH_OUTSIDE_TEMP" in str(exc)
    else:
        raise AssertionError("expected temp-only path guard")


def test_apply_idempotent_and_score_values_unchanged() -> None:
    db = _resolved_fixture_db()
    case_root = _runtime_root() / uuid.uuid4().hex
    root1 = case_root / "apply-1"
    root2 = case_root / "apply-2"

    before_score = _score_hash(db)
    assert rebuild_main(["--fundamentals-db", str(db), "--apply", "--backup", "--output-root", str(root1)]) == 0
    after_first = _safe_effective_hash(db)
    assert before_score == _score_hash(db)

    assert rebuild_main(["--fundamentals-db", str(db), "--apply", "--output-root", str(root2)]) == 0
    second_summary = json.loads((root2 / "summary.json").read_text(encoding="utf-8"))
    after_second = _safe_effective_hash(db)

    assert after_first == after_second
    assert not (root2 / "backups").exists()
    assert second_summary["apply_counts"]["score_effective_date_updates"] == 0
    assert second_summary["apply_counts"]["score_value_updates"] == 0
    assert second_summary["apply_counts"]["unchanged"] == 1


def test_vintage_provenance_and_percentiles_are_unused() -> None:
    db = _resolved_fixture_db(include_vintage=True, include_percentile=True)
    root = _runtime_root() / "vintage-unused"
    before = {
        "vintage": _table_count(db, "rc_fundamental_quarterly_vintage"),
        "provenance": _table_count(db, "rc_fundamental_quarterly_field_provenance"),
        "percentile": _table_count(db, "rc_fundamental_score_percentile"),
    }

    assert rebuild_main(["--fundamentals-db", str(db), "--apply", "--backup", "--output-root", str(root)]) == 0

    assert before == {
        "vintage": _table_count(db, "rc_fundamental_quarterly_vintage"),
        "provenance": _table_count(db, "rc_fundamental_quarterly_field_provenance"),
        "percentile": _table_count(db, "rc_fundamental_score_percentile"),
    }


def _runtime_root() -> Path:
    return repository_root() / "temp" / "fundamental_score_effective_date" / "tests"


def _build_db() -> Path:
    path = _runtime_root() / f"{uuid.uuid4().hex}.db"
    run_migration(path)
    return path


def _resolved_fixture_db(*, include_vintage: bool = False, include_percentile: bool = False) -> Path:
    db = _build_db()
    _insert_score_ttm(db, "AAPL", "2025-12-31", "2026-02-01", 71.0)
    with sqlite3.connect(db) as conn:
        if include_vintage:
            conn.execute(
                "INSERT INTO rc_fundamental_quarterly_vintage(ticker, market, period_end_date, statement_vintage_id, source_provider, source_document_id, source_hash, revision_number, is_restated, availability_quality, available_at_utc, ingested_at_utc, run_id, created_at_utc) VALUES ('AAPL', 'usa', '2025-12-31', 'v1', 'fixture', 'doc', 'hash', 1, 0, 'fixture', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'fixture', '2026-01-01T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO rc_fundamental_quarterly_field_provenance(ticker, market, period_end_date, statement_vintage_id, field_name, field_value, source_provider, source_table, provenance_role, merge_action, created_at_utc, run_id) VALUES ('AAPL', 'usa', '2025-12-31', 'v1', 'revenue', 100, 'fixture', 'fixture', 'primary', 'insert', '2026-01-01T00:00:00Z', 'fixture')"
            )
        if include_percentile:
            conn.execute(
                """
                INSERT INTO rc_fundamental_score_percentile(
                    ticker, as_of_date, target_date, rule_id, run_id,
                    universe_size, fundamental_score_percentile_global, created_at_utc
                ) VALUES (
                    'AAPL', '2025-12-31', '2026-02-01', 'fixture', 'fixture',
                    1, 80.0, '2026-01-01T00:00:00Z'
                )
                """
            )
    return db


def _insert_score_ttm(db: Path, ticker: str, as_of_date: str, effective_date: str | None, score: float) -> None:
    with sqlite3.connect(db) as conn:
        ensure_score_effective_date_schema(conn)
        conn.execute(
            """
            INSERT INTO rc_fundamental_ttm(
                ticker, as_of_date, latest_period_end_date, revenue_ttm,
                fundamental_score, growth_component, score_rule,
                fundamental_score_lifecycle, score_rule_lifecycle,
                effective_trading_date, effective_date_status, effective_date_policy,
                run_id
            ) VALUES (?, ?, ?, 400, ?, 5, 'FUND_SCORE_RULE_V1_1',
                      ?, 'FUND_SCORE_RULE_V2_LIFECYCLE_SCALING_PRE',
                      ?, 'RESOLVED', 'MAX_COMPONENT_QUARTER_EFFECTIVE_DATE', 'fixture')
            """,
            (ticker, as_of_date, as_of_date, score, score + 1.0, effective_date),
        )


def _safe_effective_hash(db: Path) -> str:
    with sqlite3.connect(db) as conn:
        try:
            return score_effective_fields_hash(conn)
        except sqlite3.OperationalError:
            return "SCHEMA_MISSING"


def _score_hash(db: Path) -> str:
    with sqlite3.connect(db) as conn:
        return score_value_fields_hash(conn)


def _table_count(db: Path, table: str) -> int:
    with sqlite3.connect(db) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
