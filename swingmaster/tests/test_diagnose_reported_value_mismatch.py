from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.cli.diagnose_reported_value_mismatch import main, run_diagnostic
from swingmaster.cli.run_fundamental_migrations import run_migration


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _insert_latest(
    conn: sqlite3.Connection,
    *,
    total_debt: float = 100.0,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            cash,
            total_debt,
            currency,
            run_id
        ) VALUES ('GIS', '2025-05-25', 10.0, 5.0, ?, 'USD', 'LATEST_RUN')
        """,
        (total_debt,),
    )


def _insert_vintage(
    conn: sqlite3.Connection,
    *,
    total_debt: float = 100.0,
    statement_vintage_id: str = "vintage:GIS:2025-05-25:1",
    available_at_utc: str = "2026-01-01T00:00:00Z",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_vintage (
            ticker,
            market,
            period_end_date,
            statement_vintage_id,
            source_provider,
            source_hash,
            available_at_utc,
            ingested_at_utc,
            run_id,
            revenue,
            cash,
            total_debt,
            currency,
            created_at_utc
        ) VALUES ('GIS', 'usa', '2025-05-25', ?, 'sec_edgar', 'hash', ?, '2026-01-01T00:05:00Z', 'VINTAGE_RUN', 10.0, 5.0, ?, 'USD', '2026-01-01T00:05:00Z')
        """,
        (statement_vintage_id, available_at_utc, total_debt),
    )


def _insert_provenance(
    conn: sqlite3.Connection,
    *,
    statement_vintage_id: str = "vintage:GIS:2025-05-25:1",
    source_row_ref: str = "LongTermDebtCurrent|LongTermDebtNoncurrent",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_field_provenance (
            ticker,
            market,
            period_end_date,
            statement_vintage_id,
            field_name,
            field_value,
            source_provider,
            source_table,
            source_row_ref,
            source_hash,
            provenance_role,
            merge_action,
            available_at_utc,
            created_at_utc,
            run_id
        ) VALUES ('GIS', 'usa', '2025-05-25', ?, 'total_debt', 100.0, 'sec_edgar', 'rc_fundamental_statement_raw', ?, 'hash', 'PRIMARY_REPORTED', 'SEC_RETAINED', '2026-01-01T00:00:00Z', '2026-01-01T00:05:00Z', 'VINTAGE_RUN')
        """,
        (statement_vintage_id, source_row_ref),
    )


def _insert_sec_fact(conn: sqlite3.Connection, field_name: str, value: float) -> None:
    conn.execute(
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
        ) VALUES ('GIS', 'balance', '2025-05-25', 'quarterly', ?, ?, 'USD', 'sec_edgar', '2026-01-01T00:00:00Z', 'SEC_RAW_RUN')
        """,
        (field_name, value),
    )


def _insert_debt_components(conn: sqlite3.Connection, *, current: float, noncurrent: float, short_term: float) -> None:
    _insert_sec_fact(conn, "LongTermDebtCurrent|filed=2026-01-01", current)
    _insert_sec_fact(conn, "LongTermDebtNoncurrent|filed=2026-01-01", noncurrent)
    _insert_sec_fact(conn, "ShortTermBorrowings|filed=2026-01-01", short_term)


def _insert_yahoo_quarterly(conn: sqlite3.Connection, total_debt: float) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_yahoo_quarterly (
            market,
            symbol,
            period_end_date,
            cash,
            total_debt,
            source_run_id,
            run_id,
            created_at_utc
        ) VALUES ('usa', 'GIS', '2025-05-25', 5.0, ?, 'YAHOO_RAW', 'YAHOO_QTR', '2026-01-01T00:00:00Z')
        """,
        (total_debt,),
    )


def _insert_yahoo_audit(conn: sqlite3.Connection, field_name: str = "cash") -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_enrichment_audit (
            ticker,
            period_end_date,
            field_name,
            old_value,
            new_value,
            primary_source,
            fallback_source,
            enrichment_status,
            run_id,
            created_at_utc,
            matched_yahoo_period_end_date,
            match_method
        ) VALUES ('GIS', '2025-05-25', ?, NULL, 1.0, 'sec_edgar', 'yahoo', 'FILLED_FROM_YAHOO', 'ENRICH_RUN', '2026-01-01T00:00:00Z', '2025-05-25', 'EXACT')
        """,
        (field_name,),
    )


def _base_db(db_path: Path, *, latest_debt: float = 100.0, vintage_debt: float = 100.0) -> None:
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn, total_debt=latest_debt)
        _insert_vintage(conn, total_debt=vintage_debt)
        _insert_provenance(conn)
        conn.commit()


def _diagnose(db_path: Path) -> dict[str, object]:
    return run_diagnostic(
        fundamentals_db=db_path,
        market="usa",
        ticker="GIS",
        period_end_date="2025-05-25",
        field_name="total_debt",
        sample_limit=50,
    )


def test_same_latest_and_vintage_value_returns_no_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "same.db"
    _base_db(db_path, latest_debt=100.0, vintage_debt=100.0)

    summary = _diagnose(db_path)

    assert summary["diagnosis_status"] == "NO_MISMATCH"
    assert summary["recommended_action"] == "none"


def test_latest_supported_by_sec_and_legacy_vintage_lower_is_stale(tmp_path: Path) -> None:
    db_path = tmp_path / "stale.db"
    _base_db(db_path, latest_debt=100.0, vintage_debt=40.0)
    with _connect(db_path) as conn:
        _insert_debt_components(conn, current=10.0, noncurrent=90.0, short_term=0.0)
        conn.commit()

    summary = _diagnose(db_path)

    assert summary["sec_debt_component_sum"] == 100.0
    assert summary["diagnosis_status"] == "VINTAGE_LEGACY_BASELINE_STALE"
    assert summary["recommended_action"] == "create_provider_derived_vintage_for_exact_row_after_review"


def test_latest_unsupported_returns_latest_value_unsupported(tmp_path: Path) -> None:
    db_path = tmp_path / "unsupported.db"
    _base_db(db_path, latest_debt=100.0, vintage_debt=40.0)

    summary = _diagnose(db_path)

    assert summary["diagnosis_status"] == "LATEST_VALUE_UNSUPPORTED"


def test_debt_component_policy_difference_is_detected(tmp_path: Path) -> None:
    db_path = tmp_path / "component_policy.db"
    _base_db(db_path, latest_debt=100.0, vintage_debt=40.0)
    with _connect(db_path) as conn:
        _insert_debt_components(conn, current=10.0, noncurrent=50.0, short_term=40.0)
        conn.commit()

    summary = _diagnose(db_path)

    assert summary["sec_debt_component_sum"] == 100.0
    assert summary["diagnosis_status"] == "DEBT_COMPONENT_POLICY_DIFF"


def test_yahoo_conflict_reported_without_audit_proof(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_conflict.db"
    _base_db(db_path, latest_debt=100.0, vintage_debt=40.0)
    with _connect(db_path) as conn:
        _insert_yahoo_quarterly(conn, total_debt=80.0)
        _insert_yahoo_audit(conn, field_name="cash")
        conn.commit()

    summary = _diagnose(db_path)

    assert summary["diagnosis_status"] == "YAHOO_VALUE_CONFLICT"
    assert summary["yahoo_audit_rows"][0]["field_name"] == "cash"


def test_all_vintage_rows_are_listed(tmp_path: Path) -> None:
    db_path = tmp_path / "vintages.db"
    _base_db(db_path, latest_debt=100.0, vintage_debt=40.0)
    with _connect(db_path) as conn:
        _insert_vintage(
            conn,
            total_debt=100.0,
            statement_vintage_id="vintage:GIS:2025-05-25:2",
            available_at_utc="2026-02-01T00:00:00Z",
        )
        conn.commit()

    summary = _diagnose(db_path)

    assert len(summary["all_vintage_rows"]) == 2
    assert summary["visible_vintage_statement_vintage_id"] == "vintage:GIS:2025-05-25:2"


def test_json_output_includes_diagnosis_and_recommended_action(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "json.db"
    _base_db(db_path, latest_debt=100.0, vintage_debt=100.0)

    rc = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--ticker",
            "GIS",
            "--period-end-date",
            "2025-05-25",
            "--field",
            "total_debt",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["diagnosis_status"] == "NO_MISMATCH"
    assert payload["summary"]["recommended_action"] == "none"


def test_invalid_db_path_exits_nonzero(tmp_path: Path, capsys) -> None:
    rc = main(
        [
            "--fundamentals-db",
            str(tmp_path / "missing.db"),
            "--market",
            "usa",
            "--ticker",
            "GIS",
            "--period-end-date",
            "2025-05-25",
            "--field",
            "total_debt",
            "--format",
            "json",
        ]
    )

    assert rc == 2
    assert "ERROR:" in capsys.readouterr().err


def test_diagnostic_does_not_write_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "readonly.db"
    _base_db(db_path, latest_debt=100.0, vintage_debt=40.0)
    with _connect(db_path) as conn:
        before = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]

    _diagnose(db_path)

    with _connect(db_path) as conn:
        after = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
    assert after == before
