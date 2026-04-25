from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from swingmaster.cli import run_fundamental_pipeline
from swingmaster.cli.run_fundamental_migrations import run_migration


def test_full_pipeline_with_mocked_fetch(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_full.db"
    run_migration(db_path)
    _mock_fetch(monkeypatch, run_fundamental_pipeline)

    summary = run_fundamental_pipeline.run_fundamental_pipeline(
        db_path=db_path,
        ticker="AAPL",
        run_id="BASE",
        dry_run=False,
        skip_fetch=False,
    )

    assert summary["child_run_ids"] == {
        "raw": "BASE__RAW",
        "quarterly": "BASE__QUARTERLY",
        "ttm": "BASE__TTM",
        "lifecycle": "BASE__LIFECYCLE",
        "score": "BASE__SCORE",
    }

    with sqlite3.connect(str(db_path)) as conn:
        raw_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_statement_raw").fetchone()[0]
        quarterly_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        ttm_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0]
        lifecycle_and_score = conn.execute(
            """
            SELECT lifecycle_class, fundamental_score, run_id
            FROM rc_fundamental_ttm
            ORDER BY as_of_date DESC
            LIMIT 1
            """
        ).fetchone()
        raw_run_ids = {
            row[0] for row in conn.execute("SELECT DISTINCT run_id FROM rc_fundamental_statement_raw")
        }
        quarterly_run_ids = {
            row[0] for row in conn.execute("SELECT DISTINCT run_id FROM rc_fundamental_quarterly")
        }
        ttm_run_ids = {
            row[0] for row in conn.execute("SELECT DISTINCT run_id FROM rc_fundamental_ttm")
        }

    assert raw_count > 0
    assert quarterly_count > 0
    assert ttm_count > 0
    assert lifecycle_and_score[0] is not None
    assert lifecycle_and_score[1] is not None
    assert raw_run_ids == {"BASE__RAW"}
    assert quarterly_run_ids == {"BASE__QUARTERLY"}
    assert ttm_run_ids == {"BASE__TTM"}


def test_skip_fetch_pipeline(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_skip_fetch.db"
    run_migration(db_path)
    _insert_raw_fixture_rows(db_path, raw_run_id="RAW_FIXTURE")
    fetch_calls: list[str] = []
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "fetch_quarterly_statements_raw",
        lambda _ticker: fetch_calls.append("called"),
    )

    run_fundamental_pipeline.run_fundamental_pipeline(
        db_path=db_path,
        ticker="AAPL",
        run_id="BASE",
        dry_run=False,
        skip_fetch=True,
    )

    with sqlite3.connect(str(db_path)) as conn:
        quarterly_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        ttm_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0]
        lifecycle_and_score = conn.execute(
            """
            SELECT lifecycle_class, fundamental_score
            FROM rc_fundamental_ttm
            ORDER BY as_of_date DESC
            LIMIT 1
            """
        ).fetchone()

    assert fetch_calls == []
    assert quarterly_count > 0
    assert ttm_count > 0
    assert lifecycle_and_score[0] is not None
    assert lifecycle_and_score[1] is not None


def test_dry_run_with_skip_fetch(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_dry_run.db"
    run_migration(db_path)
    _insert_raw_fixture_rows(db_path, raw_run_id="RAW_FIXTURE")
    fetch_calls: list[str] = []
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "fetch_quarterly_statements_raw",
        lambda _ticker: fetch_calls.append("called"),
    )

    summary = run_fundamental_pipeline.run_fundamental_pipeline(
        db_path=db_path,
        ticker="AAPL",
        run_id="BASE",
        dry_run=True,
        skip_fetch=True,
    )

    with sqlite3.connect(str(db_path)) as conn:
        quarterly_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        ttm_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0]

    assert fetch_calls == []
    assert quarterly_count == 0
    assert ttm_count == 0
    assert summary["raw_status"] == "skipped"
    assert summary["quarterly_status"] == "dry-run"
    assert summary["ttm_status"] == "dry-run"
    assert summary["lifecycle_status"] == "dry-run"
    assert summary["score_status"] == "dry-run"


def test_failure_stops_pipeline(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_failure.db"
    run_migration(db_path)
    _insert_raw_fixture_rows(db_path, raw_run_id="RAW_FIXTURE")
    calls: list[str] = []

    def _raise_quarterly(*args, **kwargs):
        calls.append("quarterly")
        raise RuntimeError("QUARTERLY_FAILED")

    monkeypatch.setattr(run_fundamental_pipeline, "build_and_insert_quarterly_rows", _raise_quarterly)
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "build_and_insert_ttm_rows",
        lambda *args, **kwargs: calls.append("ttm"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_lifecycle_classification",
        lambda *args, **kwargs: calls.append("lifecycle"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_fundamental_scoring",
        lambda *args, **kwargs: calls.append("score"),
    )

    with pytest.raises(RuntimeError, match="^QUARTERLY_FAILED$"):
        run_fundamental_pipeline.run_fundamental_pipeline(
            db_path=db_path,
            ticker="AAPL",
            run_id="BASE",
            dry_run=False,
            skip_fetch=True,
        )

    assert calls == ["quarterly"]


def test_child_run_ids() -> None:
    assert run_fundamental_pipeline.derive_child_run_ids("BASE") == {
        "raw": "BASE__RAW",
        "quarterly": "BASE__QUARTERLY",
        "ttm": "BASE__TTM",
        "lifecycle": "BASE__LIFECYCLE",
        "score": "BASE__SCORE",
    }


def test_cli_pipeline_final_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_cli.db"
    run_migration(db_path)
    _insert_raw_fixture_rows(db_path, raw_run_id="RAW_FIXTURE")

    monkeypatch.setattr(
        run_fundamental_pipeline,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db": str(db_path),
                "ticker": "AAPL",
                "run_id": "FUND_PIPELINE_AAPL_V1",
                "dry_run": True,
                "skip_fetch": True,
            },
        )(),
    )

    run_fundamental_pipeline.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY ticker=AAPL",
        f"SUMMARY db_path={db_path.resolve()}",
        "SUMMARY run_id=FUND_PIPELINE_AAPL_V1",
        "SUMMARY skip_fetch=true",
        "SUMMARY dry_run=true",
        "SUMMARY raw_status=skipped",
        "SUMMARY quarterly_status=dry-run",
        "SUMMARY ttm_status=dry-run",
        "SUMMARY lifecycle_status=dry-run",
        "SUMMARY score_status=dry-run",
        "SUMMARY status=ok",
    ]


def _mock_fetch(monkeypatch, module) -> None:
    statement_frames = {
        "income": pd.DataFrame(
            {
                "2024-03-31": [100.0, 20.0, 15.0, 25.0, 10.0],
                "2024-06-30": [110.0, 22.0, 17.0, 26.0, 11.0],
                "2024-09-30": [120.0, 24.0, 19.0, 27.0, 12.0],
                "2024-12-31": [130.0, 26.0, 21.0, 28.0, 13.0],
            },
            index=["Total Revenue", "Gross Profit", "Operating Income", "EBITDA", "Net Income"],
        ),
        "balance": pd.DataFrame(
            {
                "2024-03-31": [50.0, 80.0, 1000.0],
                "2024-06-30": [51.0, 78.0, 1001.0],
                "2024-09-30": [52.0, 76.0, 1002.0],
                "2024-12-31": [53.0, 74.0, 1003.0],
            },
            index=["Cash And Cash Equivalents", "Total Debt", "Ordinary Shares Number"],
        ),
        "cashflow": pd.DataFrame(
            {
                "2024-03-31": [30.0, -5.0],
                "2024-06-30": [31.0, -5.0],
                "2024-09-30": [32.0, -5.0],
                "2024-12-31": [33.0, -5.0],
            },
            index=["Operating Cash Flow", "Capital Expenditure"],
        ),
    }
    monkeypatch.setattr(module, "fetch_quarterly_statements_raw", lambda _ticker: statement_frames)


def _insert_raw_fixture_rows(db_path: Path, raw_run_id: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        raw_rows = [
            ("income", "2024-03-31", "Total Revenue", 100.0),
            ("income", "2024-03-31", "Gross Profit", 20.0),
            ("income", "2024-03-31", "Operating Income", 15.0),
            ("income", "2024-03-31", "EBITDA", 25.0),
            ("income", "2024-03-31", "Net Income", 10.0),
            ("cashflow", "2024-03-31", "Operating Cash Flow", 30.0),
            ("cashflow", "2024-03-31", "Capital Expenditure", -5.0),
            ("balance", "2024-03-31", "Cash And Cash Equivalents", 50.0),
            ("balance", "2024-03-31", "Total Debt", 80.0),
            ("balance", "2024-03-31", "Ordinary Shares Number", 1000.0),
            ("income", "2024-06-30", "Total Revenue", 110.0),
            ("income", "2024-06-30", "Gross Profit", 22.0),
            ("income", "2024-06-30", "Operating Income", 17.0),
            ("income", "2024-06-30", "EBITDA", 26.0),
            ("income", "2024-06-30", "Net Income", 11.0),
            ("cashflow", "2024-06-30", "Operating Cash Flow", 31.0),
            ("cashflow", "2024-06-30", "Capital Expenditure", -5.0),
            ("balance", "2024-06-30", "Cash And Cash Equivalents", 51.0),
            ("balance", "2024-06-30", "Total Debt", 78.0),
            ("balance", "2024-06-30", "Ordinary Shares Number", 1001.0),
            ("income", "2024-09-30", "Total Revenue", 120.0),
            ("income", "2024-09-30", "Gross Profit", 24.0),
            ("income", "2024-09-30", "Operating Income", 19.0),
            ("income", "2024-09-30", "EBITDA", 27.0),
            ("income", "2024-09-30", "Net Income", 12.0),
            ("cashflow", "2024-09-30", "Operating Cash Flow", 32.0),
            ("cashflow", "2024-09-30", "Capital Expenditure", -5.0),
            ("balance", "2024-09-30", "Cash And Cash Equivalents", 52.0),
            ("balance", "2024-09-30", "Total Debt", 76.0),
            ("balance", "2024-09-30", "Ordinary Shares Number", 1002.0),
            ("income", "2024-12-31", "Total Revenue", 130.0),
            ("income", "2024-12-31", "Gross Profit", 26.0),
            ("income", "2024-12-31", "Operating Income", 21.0),
            ("income", "2024-12-31", "EBITDA", 28.0),
            ("income", "2024-12-31", "Net Income", 13.0),
            ("cashflow", "2024-12-31", "Operating Cash Flow", 33.0),
            ("cashflow", "2024-12-31", "Capital Expenditure", -5.0),
            ("balance", "2024-12-31", "Cash And Cash Equivalents", 53.0),
            ("balance", "2024-12-31", "Total Debt", 74.0),
            ("balance", "2024-12-31", "Ordinary Shares Number", 1003.0),
        ]
        conn.executemany(
            """
            INSERT INTO rc_fundamental_statement_raw (
                ticker,
                statement_type,
                period_end_date,
                period_type,
                field_name,
                field_value,
                currency,
                source,
                retrieved_at_utc,
                run_id
            ) VALUES ('AAPL', ?, ?, 'quarterly', ?, ?, NULL, 'test', '2026-01-01T00:00:00', ?)
            """,
            [(statement_type, period_end_date, field_name, field_value, raw_run_id) for statement_type, period_end_date, field_name, field_value in raw_rows],
        )
        conn.commit()
