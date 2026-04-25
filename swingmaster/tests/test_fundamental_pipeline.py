from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from swingmaster.cli import run_fundamental_pipeline
from swingmaster.cli.run_fundamental_migrations import run_migration


def test_default_source_is_sec_edgar(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_default_sec.db"
    run_migration(db_path)
    calls: list[str] = []
    _mock_sec_pipeline(monkeypatch, calls)

    summary = run_fundamental_pipeline.run_fundamental_pipeline(
        db_path=db_path,
        ticker="NVDA",
        run_id="BASE",
        dry_run=False,
        skip_fetch=False,
        retrieved_at_utc="2026-04-25T00:00:00Z",
    )

    assert calls[:2] == ["sec_raw", "sec_reconstruct"]
    assert summary["source"] == "sec_edgar"
    assert summary["sec_raw_status"] == "ok"
    assert summary["sec_reconstruct_status"] == "ok"
    assert summary["raw_status"] == "not-applicable"


def test_sec_source_requires_retrieved_at_utc(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_sec_missing_retrieved.db"
    run_migration(db_path)

    with pytest.raises(
        RuntimeError,
        match="^FUNDAMENTAL_PIPELINE_RETRIEVED_AT_UTC_REQUIRED_FOR_SEC$",
    ):
        run_fundamental_pipeline.run_fundamental_pipeline(
            db_path=db_path,
            ticker="NVDA",
            run_id="BASE",
            source="sec_edgar",
            retrieved_at_utc=None,
            dry_run=False,
            skip_fetch=False,
        )


def test_sec_skip_fetch_skips_sec_steps(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_sec_skip_fetch.db"
    run_migration(db_path)
    _insert_raw_fixture_rows(db_path, ticker="NVDA", raw_run_id="SEC_RECON_FIXTURE")
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_sec_raw_bootstrap",
        lambda **kwargs: calls.append("sec_raw"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_sec_reconstruct_quarterly",
        lambda **kwargs: calls.append("sec_reconstruct"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "build_and_insert_quarterly_rows",
        lambda **kwargs: calls.append("quarterly"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "build_and_insert_ttm_rows",
        lambda **kwargs: calls.append("ttm"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_lifecycle_classification",
        lambda **kwargs: calls.append("lifecycle"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_fundamental_scoring",
        lambda **kwargs: calls.append("score"),
    )

    summary = run_fundamental_pipeline.run_fundamental_pipeline(
        db_path=db_path,
        ticker="NVDA",
        run_id="BASE",
        source="sec_edgar",
        retrieved_at_utc=None,
        dry_run=False,
        skip_fetch=True,
    )

    assert calls == ["quarterly", "ttm", "lifecycle", "score"]
    assert summary["sec_raw_status"] == "skipped"
    assert summary["sec_reconstruct_status"] == "skipped"
    assert summary["raw_status"] == "not-applicable"


def test_yfinance_source_preserves_old_behavior(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_yfinance.db"
    run_migration(db_path)
    _mock_fetch(monkeypatch, run_fundamental_pipeline)

    summary = run_fundamental_pipeline.run_fundamental_pipeline(
        db_path=db_path,
        ticker="AAPL",
        run_id="BASE",
        source="yfinance",
        retrieved_at_utc=None,
        dry_run=False,
        skip_fetch=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        raw_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_statement_raw WHERE ticker='AAPL'").fetchone()[0]

    assert raw_count > 0
    assert summary["source"] == "yfinance"
    assert summary["raw_status"] == "ok"
    assert summary["sec_raw_status"] == "not-applicable"
    assert summary["sec_reconstruct_status"] == "not-applicable"


def test_sec_dry_run_no_writes(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_sec_dry_run.db"
    run_migration(db_path)
    calls: list[tuple[str, bool]] = []

    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_sec_raw_bootstrap",
        lambda **kwargs: calls.append(("sec_raw", kwargs["dry_run"])) or ("0001045810", []),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_sec_reconstruct_quarterly",
        lambda **kwargs: calls.append(("sec_reconstruct", kwargs["dry_run"])) or (0, []),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "build_and_insert_quarterly_rows",
        lambda **kwargs: calls.append(("quarterly", kwargs["dry_run"])),
    )

    summary = run_fundamental_pipeline.run_fundamental_pipeline(
        db_path=db_path,
        ticker="NVDA",
        run_id="BASE",
        source="sec_edgar",
        retrieved_at_utc="2026-04-25T00:00:00Z",
        dry_run=True,
        skip_fetch=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        raw_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_statement_raw WHERE ticker='NVDA'").fetchone()[0]
        quarterly_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly WHERE ticker='NVDA'").fetchone()[0]
        ttm_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm WHERE ticker='NVDA'").fetchone()[0]

    assert calls == [("sec_raw", True), ("sec_reconstruct", True)]
    assert raw_count == 0
    assert quarterly_count == 0
    assert ttm_count == 0
    assert summary["sec_raw_status"] == "dry-run"
    assert summary["sec_reconstruct_status"] == "dry-run"
    assert summary["quarterly_status"] == "skipped"
    assert summary["ttm_status"] == "skipped"
    assert summary["lifecycle_status"] == "skipped"
    assert summary["score_status"] == "skipped"


def test_explain_works_with_sec_source(capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_sec_explain.db"
    run_migration(db_path)
    _insert_raw_fixture_rows(db_path, ticker="NVDA", raw_run_id="SEC_RECON_FIXTURE")

    summary = run_fundamental_pipeline.run_fundamental_pipeline(
        db_path=db_path,
        ticker="NVDA",
        run_id="BASE",
        source="sec_edgar",
        retrieved_at_utc=None,
        dry_run=False,
        skip_fetch=True,
        explain_score=True,
        explain_limit=3,
    )
    out = capsys.readouterr().out

    assert "FUNDAMENTAL SCORE EXPLAIN" in out
    assert summary["explain_status"] == "ok"
    assert summary["source"] == "sec_edgar"


def test_child_run_ids() -> None:
    assert run_fundamental_pipeline.derive_child_run_ids("BASE", "sec_edgar") == {
        "sec_raw": "BASE__SEC_RAW",
        "sec_quarterly_recon": "BASE__SEC_QUARTERLY_RECON",
        "quarterly": "BASE__QUARTERLY",
        "ttm": "BASE__TTM",
        "lifecycle": "BASE__LIFECYCLE",
        "score": "BASE__SCORE",
    }
    assert run_fundamental_pipeline.derive_child_run_ids("BASE", "yfinance") == {
        "raw": "BASE__RAW",
        "quarterly": "BASE__QUARTERLY",
        "ttm": "BASE__TTM",
        "lifecycle": "BASE__LIFECYCLE",
        "score": "BASE__SCORE",
    }


def test_cli_pipeline_final_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_cli.db"
    run_migration(db_path)

    monkeypatch.setattr(
        run_fundamental_pipeline,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db": str(db_path),
                "ticker": "NVDA",
                "run_id": "FUND_PIPELINE_NVDA_SEC_FIRST_V1",
                "source": "sec_edgar",
                "retrieved_at_utc": "2026-04-25T00:00:00Z",
                "dry_run": True,
                "skip_fetch": False,
                "explain_score": False,
                "explain_limit": None,
            },
        )(),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_sec_raw_bootstrap",
        lambda **kwargs: ("0001045810", []),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_sec_reconstruct_quarterly",
        lambda **kwargs: (0, []),
    )

    run_fundamental_pipeline.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY ticker=NVDA",
        f"SUMMARY db_path={db_path.resolve()}",
        "SUMMARY run_id=FUND_PIPELINE_NVDA_SEC_FIRST_V1",
        "SUMMARY source=sec_edgar",
        "SUMMARY skip_fetch=false",
        "SUMMARY dry_run=true",
        "SUMMARY sec_raw_status=dry-run",
        "SUMMARY sec_reconstruct_status=dry-run",
        "SUMMARY raw_status=not-applicable",
        "SUMMARY quarterly_status=skipped",
        "SUMMARY ttm_status=skipped",
        "SUMMARY lifecycle_status=skipped",
        "SUMMARY score_status=skipped",
        "SUMMARY explain_status=skipped",
        "SUMMARY status=ok",
    ]


def test_explain_mismatch_propagates(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_pipeline_explain_mismatch.db"
    run_migration(db_path)
    _insert_raw_fixture_rows(db_path, ticker="NVDA", raw_run_id="SEC_RECON_FIXTURE")

    monkeypatch.setattr(
        run_fundamental_pipeline,
        "build_explain_rows",
        lambda _rows: (_ for _ in ()).throw(RuntimeError("FUNDAMENTAL_SCORE_MISMATCH:NVDA:2025-12-31")),
    )

    with pytest.raises(RuntimeError, match="^FUNDAMENTAL_SCORE_MISMATCH:NVDA:2025-12-31$"):
        run_fundamental_pipeline.run_fundamental_pipeline(
            db_path=db_path,
            ticker="NVDA",
            run_id="BASE",
            source="sec_edgar",
            retrieved_at_utc=None,
            dry_run=False,
            skip_fetch=True,
            explain_score=True,
            explain_limit=3,
        )
    out = capsys.readouterr().out
    assert "SUMMARY status=ok" not in out


def _mock_sec_pipeline(monkeypatch, calls: list[str]) -> None:
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_sec_raw_bootstrap",
        lambda **kwargs: calls.append("sec_raw") or ("0001045810", []),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_sec_reconstruct_quarterly",
        lambda **kwargs: calls.append("sec_reconstruct") or (0, []),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "build_and_insert_quarterly_rows",
        lambda **kwargs: calls.append("quarterly"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "build_and_insert_ttm_rows",
        lambda **kwargs: calls.append("ttm"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_lifecycle_classification",
        lambda **kwargs: calls.append("lifecycle"),
    )
    monkeypatch.setattr(
        run_fundamental_pipeline,
        "run_fundamental_scoring",
        lambda **kwargs: calls.append("score"),
    )


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


def _insert_raw_fixture_rows(db_path: Path, ticker: str, raw_run_id: str) -> None:
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
            ) VALUES (?, ?, ?, 'quarterly', ?, ?, NULL, 'test', '2026-01-01T00:00:00', ?)
            """,
            [
                (ticker.upper(), statement_type, period_end_date, field_name, field_value, raw_run_id)
                for statement_type, period_end_date, field_name, field_value in raw_rows
            ],
        )
        conn.commit()
