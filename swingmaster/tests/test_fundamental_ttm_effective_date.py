from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from swingmaster.cli.rebuild_fundamental_ttm_effective_dates import main as rebuild_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_events import repository_root
from swingmaster.fundamentals.ttm_effective_date import (
    EFFECTIVE_DATE_POLICY,
    STATUS_INSUFFICIENT_COMPONENT_QUARTERS,
    STATUS_MISSING_QUARTER_MATCH,
    STATUS_NULL_COMPONENT_EFFECTIVE_DATE,
    STATUS_RESOLVED,
    apply_effective_date_rows,
    compute_ttm_effective_dates,
    effective_fields_hash,
    ensure_ttm_effective_date_schema,
    financial_fields_hash,
    select_latest_ttm_as_of,
    select_latest_ttm_current,
)


def test_schema_migration_adds_ttm_effective_date_columns() -> None:
    db = _runtime_root() / f"{uuid.uuid4().hex}.db"
    run_migration(db)

    with sqlite3.connect(db) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(rc_fundamental_ttm)")}

    assert {"effective_trading_date", "effective_date_status", "effective_date_policy"}.issubset(columns)
    assert "idx_fundamental_ttm_ticker_effective_date" in indexes


def test_four_resolved_components_use_max_component_date() -> None:
    db = _build_db()
    _insert_series(db, "AAPL", [("2025-03-31", "2025-04-20"), ("2025-06-30", "2025-07-25"), ("2025-09-30", "2025-10-24"), ("2025-12-31", "2026-02-01")])
    _insert_ttm(db, "AAPL", "2025-12-31")

    with sqlite3.connect(db) as conn:
        row = compute_ttm_effective_dates(conn)[0]

    assert row.effective_date_status == STATUS_RESOLVED
    assert row.effective_trading_date == "2026-02-01"
    assert row.effective_date_policy == EFFECTIVE_DATE_POLICY
    assert row.effective_date_component_count == 4


def test_missing_quarter_match_status_is_deterministic() -> None:
    db = _build_db()
    _insert_series(db, "MSFT", [("2025-03-31", "2025-04-20"), ("2025-06-30", None), ("2025-09-30", "2025-10-24"), ("2025-12-31", "2026-02-01")], skip_match_periods={"2025-06-30"})
    _insert_ttm(db, "MSFT", "2025-12-31")

    with sqlite3.connect(db) as conn:
        first = compute_ttm_effective_dates(conn)[0]
        second = compute_ttm_effective_dates(conn)[0]

    assert first == second
    assert first.effective_date_status == STATUS_MISSING_QUARTER_MATCH
    assert first.effective_trading_date is None


def test_null_component_effective_date_status() -> None:
    db = _build_db()
    _insert_series(db, "JPM", [("2025-03-31", "2025-04-20"), ("2025-06-30", "2025-07-25"), ("2025-09-30", None), ("2025-12-31", "2026-02-01")])
    _insert_ttm(db, "JPM", "2025-12-31")

    with sqlite3.connect(db) as conn:
        row = compute_ttm_effective_dates(conn)[0]

    assert row.effective_date_status == STATUS_NULL_COMPONENT_EFFECTIVE_DATE
    assert row.effective_trading_date is None


def test_fewer_than_four_components_status() -> None:
    db = _build_db()
    _insert_series(db, "XOM", [("2025-06-30", "2025-07-25"), ("2025-09-30", "2025-10-24"), ("2025-12-31", "2026-02-01")])
    _insert_ttm(db, "XOM", "2025-12-31")

    with sqlite3.connect(db) as conn:
        row = compute_ttm_effective_dates(conn)[0]

    assert row.effective_date_status == STATUS_INSUFFICIENT_COMPONENT_QUARTERS
    assert row.effective_date_component_count == 3


def test_irregular_fiscal_calendar_components() -> None:
    db = _build_db()
    _insert_series(db, "NVDA", [("2025-01-31", "2025-03-05"), ("2025-04-30", "2025-06-04"), ("2025-07-31", "2025-09-03"), ("2025-10-31", "2025-12-01")])
    _insert_ttm(db, "NVDA", "2025-10-31")

    with sqlite3.connect(db) as conn:
        row = compute_ttm_effective_dates(conn)[0]

    assert row.component_period_ends == "2025-01-31,2025-04-30,2025-07-31,2025-10-31"
    assert row.effective_trading_date == "2025-12-01"


def test_current_selector_unchanged_and_historical_selector_boundaries() -> None:
    db = _build_db()
    _insert_series(db, "AAPL", [("2025-03-31", "2025-04-20"), ("2025-06-30", "2025-07-25"), ("2025-09-30", "2025-10-24"), ("2025-12-31", "2026-02-01"), ("2026-03-31", "2026-04-30")])
    _insert_ttm(db, "AAPL", "2025-12-31")
    _insert_ttm(db, "AAPL", "2026-03-31")
    with sqlite3.connect(db) as conn:
        ensure_ttm_effective_date_schema(conn)
        rows = compute_ttm_effective_dates(conn)
        apply_effective_date_rows(conn, rows)
        conn.commit()
        current = select_latest_ttm_current(conn, "AAPL")
        before = select_latest_ttm_as_of(conn, "AAPL", "2026-01-31")
        on_date = select_latest_ttm_as_of(conn, "AAPL", "2026-02-01")
        after = select_latest_ttm_as_of(conn, "AAPL", "2026-05-01")

    assert current.row is not None and current.row["as_of_date"] == "2026-03-31"
    assert before.reason == "NO_AVAILABLE_TTM"
    assert on_date.row is not None and on_date.row["as_of_date"] == "2025-12-31"
    assert after.row is not None and after.row["as_of_date"] == "2026-03-31"


def test_ticker_isolation() -> None:
    db = _build_db()
    _insert_series(db, "AAPL", [("2025-03-31", "2025-04-20"), ("2025-06-30", "2025-07-25"), ("2025-09-30", "2025-10-24"), ("2025-12-31", "2026-02-01")])
    _insert_series(db, "MSFT", [("2025-03-31", "2025-04-10"), ("2025-06-30", "2025-07-10"), ("2025-09-30", "2025-10-10"), ("2025-12-31", "2026-01-10")])
    _insert_ttm(db, "AAPL", "2025-12-31")
    _insert_ttm(db, "MSFT", "2025-12-31")

    with sqlite3.connect(db) as conn:
        rows = compute_ttm_effective_dates(conn, ["MSFT"])

    assert len(rows) == 1
    assert rows[0].ticker == "MSFT"
    assert rows[0].effective_trading_date == "2026-01-10"


def test_full_dry_run_no_write_and_temp_artifacts() -> None:
    db = _resolved_fixture_db()
    root = _runtime_root() / "dry-run"
    before = _counts(db)
    before_hash = _safe_effective_hash(db)

    assert rebuild_main(["--fundamentals-db", str(db), "--dry-run", "--output-root", str(root)]) == 0

    assert before == _counts(db)
    assert before_hash == _safe_effective_hash(db)
    assert (root / "summary.json").exists()
    assert (root / "ttm_effective_dates.csv").exists()


def test_apply_requires_backup_and_rejects_outside_temp() -> None:
    db = _resolved_fixture_db()
    try:
        rebuild_main(["--fundamentals-db", str(db), "--apply", "--output-root", str(_runtime_root() / "apply-no-backup")])
    except RuntimeError as exc:
        assert str(exc) == "APPLY_REQUIRES_BACKUP"
    else:
        raise AssertionError("expected apply backup guard")

    try:
        rebuild_main(["--fundamentals-db", str(db), "--dry-run", "--output-root", str(repository_root() / "bad-ttm-effective")])
    except ValueError as exc:
        assert "RUNTIME_PATH_OUTSIDE_TEMP" in str(exc)
    else:
        raise AssertionError("expected temp-only path guard")


def test_apply_idempotent_and_financial_values_unchanged() -> None:
    db = _resolved_fixture_db()
    case_root = _runtime_root() / uuid.uuid4().hex
    root1 = case_root / "apply-1"
    root2 = case_root / "apply-2"

    with sqlite3.connect(db) as conn:
        before_financial = financial_fields_hash(conn)
    assert rebuild_main(["--fundamentals-db", str(db), "--apply", "--backup", "--output-root", str(root1)]) == 0
    with sqlite3.connect(db) as conn:
        after_first = effective_fields_hash(conn)
        after_financial = financial_fields_hash(conn)
    assert before_financial == after_financial

    assert rebuild_main(["--fundamentals-db", str(db), "--apply", "--output-root", str(root2)]) == 0
    second_summary = json.loads((root2 / "summary.json").read_text(encoding="utf-8"))
    with sqlite3.connect(db) as conn:
        after_second = effective_fields_hash(conn)

    assert after_first == after_second
    assert not (root2 / "backups").exists()
    assert second_summary["apply_counts"]["effective_date_updates"] == 0
    assert second_summary["apply_counts"]["financial_value_updates"] == 0
    assert second_summary["apply_counts"]["unchanged"] == 1


def test_vintage_and_provenance_tables_are_unused() -> None:
    db = _resolved_fixture_db(include_vintage=True)
    root = _runtime_root() / "vintage-unused"
    before_vintage = _table_count(db, "rc_fundamental_quarterly_vintage")
    before_provenance = _table_count(db, "rc_fundamental_quarterly_field_provenance")

    assert rebuild_main(["--fundamentals-db", str(db), "--apply", "--backup", "--output-root", str(root)]) == 0

    assert before_vintage == _table_count(db, "rc_fundamental_quarterly_vintage")
    assert before_provenance == _table_count(db, "rc_fundamental_quarterly_field_provenance")


def _runtime_root() -> Path:
    return repository_root() / "temp" / "fundamental_ttm_effective_date" / "tests"


def _build_db(*, include_vintage: bool = False) -> Path:
    path = _runtime_root() / f"{uuid.uuid4().hex}.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                revenue REAL,
                ebit REAL,
                free_cashflow REAL,
                ebitda REAL,
                gross_profit REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL,
                PRIMARY KEY (ticker, period_end_date)
            );
            CREATE TABLE rc_fundamental_ttm (
                ticker TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                latest_period_end_date TEXT NOT NULL,
                revenue_ttm REAL,
                ebit_ttm REAL,
                fcf_ttm REAL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, as_of_date)
            );
            CREATE TABLE rc_fundamental_quarter_earnings_match (
                id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                effective_trading_date TEXT,
                matching_confidence TEXT NOT NULL
            );
            """
        )
        if include_vintage:
            conn.executescript(
                """
                CREATE TABLE rc_fundamental_quarterly_vintage (ticker TEXT);
                CREATE TABLE rc_fundamental_quarterly_field_provenance (ticker TEXT);
                INSERT INTO rc_fundamental_quarterly_vintage(ticker) VALUES ('AAPL');
                INSERT INTO rc_fundamental_quarterly_field_provenance(ticker) VALUES ('AAPL');
                """
            )
    return path


def _resolved_fixture_db(*, include_vintage: bool = False) -> Path:
    db = _runtime_root() / f"{uuid.uuid4().hex}.db"
    run_migration(db)
    if include_vintage:
        with sqlite3.connect(db) as conn:
            conn.execute("INSERT INTO rc_fundamental_quarterly_vintage(ticker, market, period_end_date, statement_vintage_id, source_provider, source_document_id, source_hash, revision_number, is_restated, availability_quality, available_at_utc, ingested_at_utc, run_id, created_at_utc) VALUES ('AAPL', 'usa', '2025-12-31', 'v1', 'fixture', 'doc', 'hash', 1, 0, 'fixture', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'fixture', '2026-01-01T00:00:00Z')")
            conn.execute("INSERT INTO rc_fundamental_quarterly_field_provenance(ticker, market, period_end_date, statement_vintage_id, field_name, field_value, source_provider, source_table, provenance_role, merge_action, created_at_utc, run_id) VALUES ('AAPL', 'usa', '2025-12-31', 'v1', 'revenue', 100, 'fixture', 'fixture', 'primary', 'insert', '2026-01-01T00:00:00Z', 'fixture')")
    _insert_series(db, "AAPL", [("2025-03-31", "2025-04-20"), ("2025-06-30", "2025-07-25"), ("2025-09-30", "2025-10-24"), ("2025-12-31", "2026-02-01")])
    _insert_ttm(db, "AAPL", "2025-12-31")
    return db


def _insert_series(
    db: Path,
    ticker: str,
    periods: list[tuple[str, str | None]],
    *,
    skip_match_periods: set[str] | None = None,
) -> None:
    skip = skip_match_periods or set()
    with sqlite3.connect(db) as conn:
        quarterly_columns = {row[1] for row in conn.execute("PRAGMA table_info(rc_fundamental_quarterly)")}
        match_columns = {row[1] for row in conn.execute("PRAGMA table_info(rc_fundamental_quarter_earnings_match)")}
        event_id = 1
        for period, effective in periods:
            if "run_id" in quarterly_columns:
                conn.execute(
                    """
                    INSERT INTO rc_fundamental_quarterly(
                        ticker, period_end_date, revenue, ebit, free_cashflow,
                        ebitda, gross_profit, cash, total_debt, shares_outstanding, currency, run_id
                    ) VALUES (?, ?, 100, 20, 10, 25, 50, 5, 1, 1000, 'USD', 'fixture')
                    """,
                    (ticker, period),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO rc_fundamental_quarterly(
                        ticker, period_end_date, revenue, ebit, free_cashflow,
                        ebitda, gross_profit, cash, total_debt, shares_outstanding
                    ) VALUES (?, ?, 100, 20, 10, 25, 50, 5, 1, 1000)
                    """,
                    (ticker, period),
                )
            if period in skip:
                continue
            if "earnings_event_id" in match_columns:
                conn.execute(
                    """
                    INSERT INTO rc_fundamental_quarter_earnings_match(
                        market, ticker, period_end_date, earnings_event_id, announcement_at,
                        announcement_date, announcement_session, effective_trading_date,
                        effective_date_status, reporting_delay_days, matching_status,
                        matching_confidence, matching_method, candidate_count, availability_policy,
                        matcher_version, created_at_utc, updated_at_utc
                    ) VALUES ('usa', ?, ?, ?, ?, ?, 'bmo', ?, 'RESOLVED_SAME_TRADING_DAY',
                              30, 'MATCHED_HIGH_CONFIDENCE', 'HIGH', 'fixture', 1,
                              'EARNINGS_EFFECTIVE_DATE_ASSUMED', 'fixture',
                              '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                    """,
                    (ticker, period, event_id, effective or period, effective or period, effective),
                )
                event_id += 1
            else:
                conn.execute(
                    """
                    INSERT INTO rc_fundamental_quarter_earnings_match(
                        market, ticker, period_end_date, effective_trading_date, matching_confidence
                    ) VALUES ('usa', ?, ?, ?, 'HIGH')
                    """,
                    (ticker, period, effective),
                )


def _insert_ttm(db: Path, ticker: str, period: str) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_ttm(
                ticker, as_of_date, latest_period_end_date, revenue_ttm, ebit_ttm, fcf_ttm, run_id
            ) VALUES (?, ?, ?, 400, 80, 40, 'fixture')
            """,
            (ticker, period, period),
        )


def _counts(db: Path) -> dict[str, int]:
    with sqlite3.connect(db) as conn:
        return {
            "ttm": conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0],
            "quarterly": conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0],
            "matches": conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarter_earnings_match").fetchone()[0],
        }


def _safe_effective_hash(db: Path) -> str:
    with sqlite3.connect(db) as conn:
        try:
            return effective_fields_hash(conn)
        except sqlite3.OperationalError:
            return "SCHEMA_MISSING"


def _table_count(db: Path, table: str) -> int:
    with sqlite3.connect(db) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
