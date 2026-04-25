from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_bootstrap_sec_raw
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals import sec_edgar


def test_extracts_sec_facts() -> None:
    rows = sec_edgar.extract_companyfacts_raw_rows(
        ticker="NVDA",
        companyfacts=_sample_companyfacts(),
        run_id="RUN1",
        retrieved_at_utc="2026-04-25T00:00:00Z",
    )
    assert len(rows) == 5
    assert {row["statement_type"] for row in rows} == {"income", "cashflow", "balance"}


def test_filters_forms() -> None:
    companyfacts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {"end": "2025-03-31", "form": "8-K", "val": 10},
                            {"end": "2025-03-31", "form": "10-Q", "val": 20},
                        ]
                    }
                }
            }
        }
    }
    rows = sec_edgar.extract_companyfacts_raw_rows("NVDA", companyfacts, "RUN1", "2026-04-25T00:00:00Z")
    assert len(rows) == 1
    assert "form=10-Q" in rows[0]["field_name"]


def test_skips_missing_end() -> None:
    companyfacts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {"form": "10-Q", "val": 10},
                            {"end": "2025-03-31", "form": "10-Q", "val": 20},
                        ]
                    }
                }
            }
        }
    }
    rows = sec_edgar.extract_companyfacts_raw_rows("NVDA", companyfacts, "RUN1", "2026-04-25T00:00:00Z")
    assert len(rows) == 1
    assert rows[0]["period_end_date"] == "2025-03-31"


def test_encoded_field_name_preserves_duplicates(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_raw_duplicates.db"
    run_migration(db_path)
    companyfacts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {"end": "2025-03-31", "form": "10-Q", "val": 10, "frame": "CY2025Q1", "start": "2025-01-01", "filed": "2025-04-30"},
                            {"end": "2025-03-31", "form": "10-Q", "val": 11, "frame": "CY2025Q1I", "start": "2025-01-01", "filed": "2025-05-01"},
                        ]
                    }
                }
            }
        }
    }
    rows = sec_edgar.extract_companyfacts_raw_rows("NVDA", companyfacts, "RUN1", "2026-04-25T00:00:00Z")
    with sqlite3.connect(str(db_path)) as conn:
        run_fundamental_bootstrap_sec_raw.insert_sec_raw_rows(conn, rows)
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_statement_raw").fetchone()[0]
    assert count == 2


def test_dry_run_writes_nothing(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "sec_raw_dry_run.db"
    run_migration(db_path)
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "resolve_cik", lambda _ticker, _ua: "0001045810")
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "fetch_companyfacts", lambda _cik, _ua: _sample_companyfacts())
    cik, rows = run_fundamental_bootstrap_sec_raw.run_sec_raw_bootstrap(
        db_path=db_path,
        ticker="NVDA",
        run_id="RUN1",
        retrieved_at_utc="2026-04-25T00:00:00Z",
        user_agent=sec_edgar.SEC_USER_AGENT,
        dry_run=True,
    )
    assert cik == "0001045810"
    assert len(rows) == 5
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_statement_raw").fetchone()[0]
    assert count == 0


def test_idempotency(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "sec_raw_idempotent.db"
    run_migration(db_path)
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "resolve_cik", lambda _ticker, _ua: "0001045810")
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "fetch_companyfacts", lambda _cik, _ua: _sample_companyfacts())
    run_fundamental_bootstrap_sec_raw.run_sec_raw_bootstrap(
        db_path=db_path,
        ticker="NVDA",
        run_id="RUN1",
        retrieved_at_utc="2026-04-25T00:00:00Z",
        user_agent=sec_edgar.SEC_USER_AGENT,
        dry_run=False,
    )
    run_fundamental_bootstrap_sec_raw.run_sec_raw_bootstrap(
        db_path=db_path,
        ticker="NVDA",
        run_id="RUN1",
        retrieved_at_utc="2026-04-25T00:00:00Z",
        user_agent=sec_edgar.SEC_USER_AGENT,
        dry_run=False,
    )
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_statement_raw").fetchone()[0]
    assert count == 5


def test_no_facts(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "sec_raw_no_facts.db"
    run_migration(db_path)
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "resolve_cik", lambda _ticker, _ua: "0001045810")
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "fetch_companyfacts", lambda _cik, _ua: {"facts": {"us-gaap": {}}})
    with pytest.raises(RuntimeError, match="^SEC_FACTS_NOT_FOUND:NVDA$"):
        run_fundamental_bootstrap_sec_raw.run_sec_raw_bootstrap(
            db_path=db_path,
            ticker="NVDA",
            run_id="RUN1",
            retrieved_at_utc="2026-04-25T00:00:00Z",
            user_agent=sec_edgar.SEC_USER_AGENT,
            dry_run=False,
        )


def test_deterministic_retrieved_at_utc(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "sec_raw_retrieved_at.db"
    run_migration(db_path)
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "resolve_cik", lambda _ticker, _ua: "0001045810")
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "fetch_companyfacts", lambda _cik, _ua: _sample_companyfacts())
    run_fundamental_bootstrap_sec_raw.run_sec_raw_bootstrap(
        db_path=db_path,
        ticker="NVDA",
        run_id="RUN1",
        retrieved_at_utc="2026-04-25T00:00:00Z",
        user_agent=sec_edgar.SEC_USER_AGENT,
        dry_run=False,
    )
    with sqlite3.connect(str(db_path)) as conn:
        values = {
            row[0] for row in conn.execute("SELECT DISTINCT retrieved_at_utc FROM rc_fundamental_statement_raw")
        }
    assert values == {"2026-04-25T00:00:00Z"}


def test_cli_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "sec_raw_cli.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_bootstrap_sec_raw,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            ticker="NVDA",
            run_id="RUN1",
            retrieved_at_utc="2026-04-25T00:00:00Z",
            user_agent=sec_edgar.SEC_USER_AGENT,
            dry_run=True,
        ),
    )
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "resolve_cik", lambda _ticker, _ua: "0001045810")
    monkeypatch.setattr(run_fundamental_bootstrap_sec_raw, "fetch_companyfacts", lambda _cik, _ua: _sample_companyfacts())
    run_fundamental_bootstrap_sec_raw.main()
    out = capsys.readouterr().out
    assert "SUMMARY ticker=NVDA" in out
    assert "SUMMARY cik=0001045810" in out
    assert "SUMMARY facts_selected=5" in out
    assert "SUMMARY source=sec_edgar" in out
    assert "SUMMARY period_type=sec_fact" in out
    assert "SUMMARY status=dry-run" in out


def _sample_companyfacts() -> dict:
    return {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {"end": "2025-03-31", "form": "10-Q", "val": 100, "fy": 2025, "fp": "Q1", "frame": "CY2025Q1", "start": "2025-01-01", "filed": "2025-04-30"},
                        ]
                    }
                },
                "GrossProfit": {
                    "units": {
                        "USD": [
                            {"end": "2025-03-31", "form": "10-Q", "val": 60, "fy": 2025, "fp": "Q1", "frame": "CY2025Q1", "start": "2025-01-01", "filed": "2025-04-30"},
                        ]
                    }
                },
                "NetCashProvidedByUsedInOperatingActivities": {
                    "units": {
                        "USD": [
                            {"end": "2025-03-31", "form": "10-Q", "val": 40, "fy": 2025, "fp": "Q1", "frame": "CY2025Q1", "start": "2025-01-01", "filed": "2025-04-30"},
                        ]
                    }
                },
                "CashAndCashEquivalentsAtCarryingValue": {
                    "units": {
                        "USD": [
                            {"end": "2025-03-31", "form": "10-Q", "val": 70, "fy": 2025, "fp": "Q1", "frame": "CY2025Q1", "filed": "2025-04-30"},
                        ]
                    }
                },
                "WeightedAverageNumberOfDilutedSharesOutstanding": {
                    "units": {
                        "shares": [
                            {"end": "2025-03-31", "form": "10-Q", "val": 500, "fy": 2025, "fp": "Q1", "frame": "CY2025Q1", "filed": "2025-04-30"},
                        ]
                    }
                },
            }
        }
    }
