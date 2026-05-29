from __future__ import annotations

import csv
import sqlite3
from argparse import Namespace
from pathlib import Path

from swingmaster.cli import run_fundamental_reporting_frequency_audit
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals import reporting_frequency


def _insert_quarterly_row(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            run_id
        ) VALUES (?, ?, ?)
        """,
        (ticker, period_end_date, "AUDIT_FIXTURE"),
    )


def test_classify_quarterly_dates() -> None:
    classification = reporting_frequency.classify_reporting_frequency(
        ["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"]
    )
    assert classification.reporting_frequency_class == "QUARTERLY"
    assert classification.inferred_reporting_frequency == "QUARTERLY"
    assert classification.has_valid_ttm_coverage == 1
    assert classification.reason == "ENOUGH_RECENT_QUARTERS"
    assert classification.missing_period_count == 0
    assert classification.classifier_version == "reporting_frequency_v1"


def test_classify_semiannual_dates() -> None:
    classification = reporting_frequency.classify_reporting_frequency(
        ["2025-01-31", "2025-07-31"]
    )
    assert classification.reporting_frequency_class == "TRUE_SEMIANNUAL"
    assert classification.inferred_reporting_frequency == "SEMIANNUAL"
    assert classification.has_valid_ttm_coverage == 1
    assert classification.reason == "CONSISTENT_HALF_YEAR_PERIODS"
    assert classification.missing_period_count == 0


def test_classify_quarterly_missing_source_period_dates() -> None:
    classification = reporting_frequency.classify_reporting_frequency(
        ["2024-12-31", "2025-03-31", "2025-06-30", "2025-12-31", "2026-03-31"]
    )
    assert classification.reporting_frequency_class == "QUARTERLY_MISSING_SOURCE_PERIOD"
    assert classification.inferred_reporting_frequency == "INSUFFICIENT"
    assert classification.has_valid_ttm_coverage == 0
    assert classification.reason == "QUARTERLY_PATTERN_WITH_MISSING_RECENT_PERIOD"
    assert classification.expected_period_end_dates == (
        "2024-12-31",
        "2025-03-31",
        "2025-06-30",
        "2025-09-30",
        "2025-12-31",
        "2026-03-31",
    )
    assert classification.missing_period_end_dates == ("2025-09-30",)
    assert classification.missing_period_count == 1


def test_classify_four_period_quarterly_missing_source_period_regression() -> None:
    classification = reporting_frequency.classify_reporting_frequency(
        ["2024-12-31", "2025-03-31", "2025-06-30", "2025-12-31"]
    )
    assert classification.reporting_frequency_class == "QUARTERLY_MISSING_SOURCE_PERIOD"
    assert classification.inferred_reporting_frequency == "INSUFFICIENT"
    assert classification.has_valid_ttm_coverage == 0
    assert classification.reason == "QUARTERLY_PATTERN_WITH_MISSING_RECENT_PERIOD"
    assert classification.expected_period_end_dates == (
        "2024-12-31",
        "2025-03-31",
        "2025-06-30",
        "2025-09-30",
        "2025-12-31",
    )
    assert classification.missing_period_end_dates == ("2025-09-30",)
    assert classification.missing_period_count == 1


def test_classify_annual_only_dates() -> None:
    classification = reporting_frequency.classify_reporting_frequency(["2025-12-31"])
    assert classification.reporting_frequency_class == "ANNUAL_ONLY"
    assert classification.inferred_reporting_frequency == "ANNUAL_ONLY"
    assert classification.has_valid_ttm_coverage == 0
    assert classification.reason == "ONLY_ONE_RECENT_PERIOD"
    assert classification.missing_period_count == 0


def test_classify_sparse_dates_as_other_insufficient() -> None:
    classification = reporting_frequency.classify_reporting_frequency(
        ["2025-03-31", "2025-12-31"]
    )
    assert classification.reporting_frequency_class == "OTHER_INSUFFICIENT"
    assert classification.inferred_reporting_frequency == "INSUFFICIENT"
    assert classification.has_valid_ttm_coverage == 0
    assert classification.reason == "OTHER_INSUFFICIENT_RECENT_PERIODS"
    assert classification.missing_period_count == 0


def test_classify_malformed_dates_as_unknown() -> None:
    classification = reporting_frequency.classify_reporting_frequency(
        ["2025-12-31", "bad-date"]
    )
    assert classification.reporting_frequency_class == "UNKNOWN"
    assert classification.inferred_reporting_frequency == "UNKNOWN"
    assert classification.has_valid_ttm_coverage == 0
    assert classification.reason == "MALFORMED_PERIOD_DATES"
    assert classification.missing_period_count == 0


def test_migration_creates_reporting_frequency_classification_table(tmp_path: Path) -> None:
    db_path = tmp_path / "reporting_frequency_migration.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='rc_fundamental_reporting_frequency_classification'
            """
        ).fetchone()
        assert row == ("rc_fundamental_reporting_frequency_classification",)


def test_cli_smoke_writes_csv_and_text_summary(
    monkeypatch, capsys, tmp_path: Path
) -> None:
    db_path = tmp_path / "reporting_frequency_audit.db"
    output_path = tmp_path / "reporting_frequency_audit.csv"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, "NOKIA.HE", "2025-06-30")
        _insert_quarterly_row(conn, "NOKIA.HE", "2025-12-31")
        _insert_quarterly_row(conn, "KNEBV.HE", "2025-03-31")
        _insert_quarterly_row(conn, "KNEBV.HE", "2025-06-30")
        _insert_quarterly_row(conn, "KNEBV.HE", "2025-09-30")
        _insert_quarterly_row(conn, "KNEBV.HE", "2025-12-31")
        _insert_quarterly_row(conn, "TIETO.HE", "2024-12-31")
        _insert_quarterly_row(conn, "TIETO.HE", "2025-03-31")
        _insert_quarterly_row(conn, "TIETO.HE", "2025-06-30")
        _insert_quarterly_row(conn, "TIETO.HE", "2025-12-31")
        _insert_quarterly_row(conn, "TIETO.HE", "2026-03-31")
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_reporting_frequency_audit,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="omxh",
            lookback_months=30,
            output=str(output_path),
            format="text",
            write_db=False,
            as_of_date=None,
            run_id=None,
            write_mode="insert",
        ),
    )

    run_fundamental_reporting_frequency_audit.main()
    out = capsys.readouterr().out

    assert "ticker=KNEBV.HE" in out
    assert "reporting_frequency_class=QUARTERLY" in out
    assert "inferred_reporting_frequency=QUARTERLY" in out
    assert "ticker=NOKIA.HE" in out
    assert "reporting_frequency_class=TRUE_SEMIANNUAL" in out
    assert "inferred_reporting_frequency=SEMIANNUAL" in out
    assert "ticker=TIETO.HE" in out
    assert "reporting_frequency_class=QUARTERLY_MISSING_SOURCE_PERIOD" in out
    assert "SUMMARY market=omxh" in out
    assert "SUMMARY tickers_total=3" in out
    assert "SUMMARY quarterly_count=1" in out
    assert "SUMMARY true_semiannual_count=1" in out
    assert "SUMMARY quarterly_missing_source_period_count=1" in out
    assert "SUMMARY semiannual_count=1" in out
    assert "SUMMARY other_insufficient_count=0" in out
    assert f"SUMMARY output_path={output_path}" in out

    with output_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {
            "ticker": "KNEBV.HE",
            "market": "omxh",
            "period_count_in_lookback": "4",
            "period_end_dates": "2025-03-31,2025-06-30,2025-09-30,2025-12-31",
            "observed_period_end_dates": "2025-03-31,2025-06-30,2025-09-30,2025-12-31",
            "expected_period_end_dates": "",
            "missing_period_end_dates": "",
            "missing_period_count": "0",
            "source_data_max_period_end_date": "2025-12-31",
            "classifier_version": "reporting_frequency_v1",
            "reporting_frequency_class": "QUARTERLY",
            "inferred_reporting_frequency": "QUARTERLY",
            "has_valid_ttm_coverage": "1",
            "reason": "ENOUGH_RECENT_QUARTERS",
        },
        {
            "ticker": "NOKIA.HE",
            "market": "omxh",
            "period_count_in_lookback": "2",
            "period_end_dates": "2025-06-30,2025-12-31",
            "observed_period_end_dates": "2025-06-30,2025-12-31",
            "expected_period_end_dates": "",
            "missing_period_end_dates": "",
            "missing_period_count": "0",
            "source_data_max_period_end_date": "2025-12-31",
            "classifier_version": "reporting_frequency_v1",
            "reporting_frequency_class": "TRUE_SEMIANNUAL",
            "inferred_reporting_frequency": "SEMIANNUAL",
            "has_valid_ttm_coverage": "1",
            "reason": "CONSISTENT_HALF_YEAR_PERIODS",
        },
        {
            "ticker": "TIETO.HE",
            "market": "omxh",
            "period_count_in_lookback": "5",
            "period_end_dates": "2024-12-31,2025-03-31,2025-06-30,2025-12-31,2026-03-31",
            "observed_period_end_dates": "2024-12-31,2025-03-31,2025-06-30,2025-12-31,2026-03-31",
            "expected_period_end_dates": "2024-12-31,2025-03-31,2025-06-30,2025-09-30,2025-12-31,2026-03-31",
            "missing_period_end_dates": "2025-09-30",
            "missing_period_count": "1",
            "source_data_max_period_end_date": "2026-03-31",
            "classifier_version": "reporting_frequency_v1",
            "reporting_frequency_class": "QUARTERLY_MISSING_SOURCE_PERIOD",
            "inferred_reporting_frequency": "INSUFFICIENT",
            "has_valid_ttm_coverage": "0",
            "reason": "QUARTERLY_PATTERN_WITH_MISSING_RECENT_PERIOD",
        },
    ]


def test_cli_write_db_insert_writes_classification_rows(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "reporting_frequency_write_insert.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, "NOKIA.HE", "2025-06-30")
        _insert_quarterly_row(conn, "NOKIA.HE", "2025-12-31")
        _insert_quarterly_row(conn, "TIETO.HE", "2024-12-31")
        _insert_quarterly_row(conn, "TIETO.HE", "2025-03-31")
        _insert_quarterly_row(conn, "TIETO.HE", "2025-06-30")
        _insert_quarterly_row(conn, "TIETO.HE", "2025-12-31")
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_reporting_frequency_audit,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="omxh",
            lookback_months=30,
            output=None,
            format="text",
            write_db=True,
            as_of_date="2026-05-29",
            run_id="RF_AUDIT_RUN_1",
            write_mode="insert",
        ),
    )

    run_fundamental_reporting_frequency_audit.main()
    out = capsys.readouterr().out

    assert "SUMMARY write_db=1" in out
    assert "SUMMARY write_mode=insert" in out
    assert "SUMMARY rows_written=2" in out
    assert "SUMMARY as_of_date=2026-05-29" in out
    assert "SUMMARY run_id=RF_AUDIT_RUN_1" in out

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT ticker, reporting_frequency_class, missing_period_end_dates, missing_period_count, classifier_version, run_id
            FROM rc_fundamental_reporting_frequency_classification
            ORDER BY ticker
            """
        ).fetchall()
    assert rows == [
        ("NOKIA.HE", "TRUE_SEMIANNUAL", "", 0, "reporting_frequency_v1", "RF_AUDIT_RUN_1"),
        ("TIETO.HE", "QUARTERLY_MISSING_SOURCE_PERIOD", "2025-09-30", 1, "reporting_frequency_v1", "RF_AUDIT_RUN_1"),
    ]


def test_cli_write_db_replace_run_replaces_only_same_run_id(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "reporting_frequency_write_replace.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, "NOKIA.HE", "2025-06-30")
        _insert_quarterly_row(conn, "NOKIA.HE", "2025-12-31")
        conn.execute(
            """
            INSERT INTO rc_fundamental_reporting_frequency_classification (
                ticker, market, as_of_date, lookback_months, reporting_frequency_class,
                inferred_reporting_frequency, has_valid_ttm_coverage, reason,
                period_count_in_lookback, observed_period_end_dates, expected_period_end_dates,
                missing_period_end_dates, missing_period_count, source_data_max_period_end_date,
                classifier_version, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "OLD.HE", "omxh", "2026-05-29", 30, "OTHER_INSUFFICIENT",
                "INSUFFICIENT", 0, "NO_PERIOD_ROWS", 0, "", "", "", 0, "",
                "reporting_frequency_v1", "RF_REPLACE", "2026-05-29T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_reporting_frequency_classification (
                ticker, market, as_of_date, lookback_months, reporting_frequency_class,
                inferred_reporting_frequency, has_valid_ttm_coverage, reason,
                period_count_in_lookback, observed_period_end_dates, expected_period_end_dates,
                missing_period_end_dates, missing_period_count, source_data_max_period_end_date,
                classifier_version, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "KEEP.HE", "omxh", "2026-05-29", 30, "OTHER_INSUFFICIENT",
                "INSUFFICIENT", 0, "NO_PERIOD_ROWS", 0, "", "", "", 0, "",
                "reporting_frequency_v1", "RF_KEEP", "2026-05-29T00:00:00+00:00",
            ),
        )
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_reporting_frequency_audit,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="omxh",
            lookback_months=30,
            output=None,
            format="text",
            write_db=True,
            as_of_date="2026-05-29",
            run_id="RF_REPLACE",
            write_mode="replace-run",
        ),
    )

    run_fundamental_reporting_frequency_audit.main()

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT ticker, run_id
            FROM rc_fundamental_reporting_frequency_classification
            ORDER BY run_id, ticker
            """
        ).fetchall()
    assert rows == [
        ("KEEP.HE", "RF_KEEP"),
        ("NOKIA.HE", "RF_REPLACE"),
    ]
