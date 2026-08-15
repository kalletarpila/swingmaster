from __future__ import annotations

import json
import sqlite3

from swingmaster.fundamentals_v2.legacy_total_debt import (
    PROVIDER_FIELD,
    apply_total_debt_rows,
    parse_yahoo_total_debt_observations,
)


def test_parse_yahoo_total_debt_observations_keeps_explicit_zero_and_skips_missing() -> None:
    payload = {
        "columns": ["2025-03-31", "2025-06-30"],
        "index": [PROVIDER_FIELD],
        "data": [[0, None]],
    }

    rows = parse_yahoo_total_debt_observations(
        json.dumps(payload),
        ticker="abc",
        raw_id=1,
        payload_hash="hash",
        run_id="run",
        loaded_at_utc="2026-01-01T00:00:00Z",
    )

    assert len(rows) == 1
    assert rows[0].ticker == "ABC"
    assert rows[0].value == 0.0


def test_apply_total_debt_rows_fills_null_with_accepted_risk_provenance() -> None:
    conn = _memory_v2()

    result = apply_total_debt_rows(conn, [_eligible_row(value=123.0, tier="ACCEPTED_RISK")], run_id="rev_run", dry_run=False, now="2026-01-01T00:00:00Z")

    assert result[0]["action"] == "FILLED"
    assert conn.execute("SELECT total_debt FROM rc_v2_fundamental_quarterly WHERE quarter_id=1").fetchone()[0] == 123.0
    source = conn.execute("SELECT source_value FROM rc_v2_fundamental_field_source WHERE quarter_id=1 AND field_name='total_debt'").fetchone()[0]
    payload = json.loads(source)
    assert payload["validation_tier"] == "ACCEPTED_RISK"
    assert payload["provider_field"] == PROVIDER_FIELD
    assert payload["match_mode"] == "EXACT_DATE_INFERRED_FISCAL"
    assert payload["lease_definition_class"] == "COMPANY_DEPENDENT"


def test_apply_total_debt_rows_does_not_overwrite_existing_total_debt() -> None:
    conn = _memory_v2(existing_total_debt=99.0)

    result = apply_total_debt_rows(conn, [_eligible_row(value=123.0)], run_id="rev_run", dry_run=False)

    assert result[0]["action"] == "CONFLICT_EXISTING_DIFFERENT"
    assert conn.execute("SELECT total_debt FROM rc_v2_fundamental_quarterly WHERE quarter_id=1").fetchone()[0] == 99.0
    assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source").fetchone()[0] == 0


def test_apply_total_debt_rows_dry_run_has_no_writes() -> None:
    conn = _memory_v2()

    result = apply_total_debt_rows(conn, [_eligible_row(value=123.0)], run_id="rev_run", dry_run=True)

    assert result[0]["action"] == "WOULD_FILL"
    assert conn.execute("SELECT total_debt FROM rc_v2_fundamental_quarterly WHERE quarter_id=1").fetchone()[0] is None
    assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source").fetchone()[0] == 0


def _memory_v2(*, existing_total_debt: float | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE rc_v2_fundamental_quarterly (
            quarter_id INTEGER PRIMARY KEY,
            total_debt REAL,
            available_canonical_field_count INTEGER NOT NULL DEFAULT 0,
            updated_at_utc TEXT
        );
        CREATE TABLE rc_v2_import_run (
            import_run_id TEXT PRIMARY KEY,
            market TEXT NOT NULL,
            simfin_dir TEXT NOT NULL,
            builder_version TEXT NOT NULL,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT
        );
        CREATE TABLE rc_v2_fundamental_field_source (
            quarter_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            provider TEXT NOT NULL,
            provider_field TEXT NOT NULL,
            source_dataset TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            transformation TEXT NOT NULL,
            source_value TEXT,
            import_run_id TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            PRIMARY KEY (quarter_id, field_name, provider)
        );
        """
    )
    conn.execute(
        "INSERT INTO rc_v2_fundamental_quarterly (quarter_id, total_debt, available_canonical_field_count, updated_at_utc) VALUES (1, ?, 0, '')",
        (existing_total_debt,),
    )
    return conn


def _eligible_row(*, value: float, tier: str = "SAFE_SCOPED") -> dict[str, object]:
    return {
        "ticker": "ABC",
        "quarter_id": 1,
        "report_date": "2025-03-31",
        "candidate_total_debt": value,
        "provider_date": "2025-03-31",
        "match_mode": "EXACT_DATE_INFERRED_FISCAL",
        "date_offset_days": 0,
        "fiscal_identity_verified": 0,
        "risk_tier": tier,
        "validation_rule": "company-scoped Yahoo direct Total Debt",
        "raw_id": 7,
        "payload_hash": "hash",
        "legacy_run_id": "legacy",
    }
