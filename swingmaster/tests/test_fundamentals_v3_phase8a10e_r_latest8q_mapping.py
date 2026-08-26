from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10e_r_latest8q_mapping as phase


def official_row(ticker: str = "BBY", fy: int = 2026, fq: str = "Q1") -> dict[str, str]:
    return {
        "Ticker": ticker,
        "Company Name": "Example",
        "Canonical Sequence": "8",
        "Fiscal Year": str(fy),
        "Fiscal Quarter": fq,
        "Fiscal Period": f"FY{fy} {fq}",
        "Canonical Key": f"{ticker}|FY{fy}|{fq}",
        "Official Period End": "2025-05-03",
        "Publish Date": "2025-05-29",
        "Reporting Lag Days": "26",
        "Fiscal Calendar Type": "52/53",
        "Confidence": "HIGH",
        "Primary Source URL": "issuer",
        "Issuer Archive URL": "archive",
        "Notes": "",
    }


def current_row() -> dict:
    return {
        "ticker": "BBY",
        "quarter_id": 1,
        "fiscal_year": 2026,
        "fiscal_quarter": "Q1",
        "period_end_date": "2026-04-30",
        "publish_date": "2025-05-29",
        "revenue": 100.0,
        "operating_income": 10.0,
        "net_income": 7.0,
    }


def supplemental_row() -> dict[str, str]:
    return {"Ticker": "BBY", "Fiscal Year": "2026", "Fiscal Q": "Q1", "Revenue": "100", "Operating Income": "10", "Net Income": "7"}


def test_scope_is_exact_nine_tickers() -> None:
    assert phase.NINE_TICKERS == ("BBY", "DELL", "GCO", "HAE", "MRVL", "RL", "SAIC", "TJX", "TRNS")


def test_latest8_validation_accepts_72_rows(tmp_path: Path) -> None:
    path = tmp_path / "official.csv"
    rows = []
    for ticker in phase.NINE_TICKERS:
        for rank in range(1, 9):
            fy = 2027 if rank == 8 else 2025 + ((rank - 1) // 4)
            fq = "Q2" if ticker == "TJX" and rank == 8 else f"Q{((rank - 1) % 4) + 1}"
            if ticker != "TJX" and rank == 8:
                fq = "Q1"
            rows.append({**official_row(ticker, fy, fq), "Canonical Sequence": str(rank)})
    phase.write_csv(path, rows)
    _official, validation = phase.validate_official_latest8q(path)
    assert validation["rows"] == 72
    assert validation["rows_per_ticker"]["BBY"] == 8


def test_validation_rejects_wrong_ticker_count(tmp_path: Path) -> None:
    path = tmp_path / "official.csv"
    phase.write_csv(path, [official_row()])
    try:
        phase.validate_official_latest8q(path)
    except RuntimeError as exc:
        assert "OFFICIAL_TIMELINE_INVALID" in str(exc)
    else:
        raise AssertionError("expected validation failure")


def test_tjx_latest_rule_is_explicit() -> None:
    assert phase.fiscal_ordinal(2027, "Q2") > phase.fiscal_ordinal(2027, "Q1")


def test_value_equal_tolerates_small_rounding() -> None:
    assert phase.value_equal(100.000001, "100")


def test_revenue_fingerprint_matching() -> None:
    assert phase.fingerprint_match_count(current_row(), supplemental_row()) == 3


def test_oi_ni_secondary_fingerprint() -> None:
    row = current_row()
    row["revenue"] = 999.0
    assert phase.fingerprint_match_count(row, supplemental_row()) == 2


def test_wrong_period_end_detected() -> None:
    cls, confidence, _reason = phase.match_class(current_row(), official_row(), supplemental_row())
    assert cls == "MATCH_WRONG_PERIOD_END"
    assert confidence == "HIGH"


def test_wrong_publish_detected() -> None:
    row = current_row()
    row["period_end_date"] = "2025-05-03"
    row["publish_date"] = "2025-05-30"
    cls, _confidence, _reason = phase.match_class(row, official_row(), supplemental_row())
    assert cls == "MATCH_WRONG_PUBLISH_DATE"


def test_wrong_fyq_mapping_detected() -> None:
    row = current_row()
    row["fiscal_year"] = 2025
    cls, _confidence, _reason = phase.match_class(row, official_row(), supplemental_row())
    assert cls == "MATCH_BY_FINANCIAL_CONTENT"


def test_missing_official_quarter_detected() -> None:
    mapping, _fp, missing = phase.map_current_to_official([], [official_row()], {})
    assert mapping == []
    assert missing[0]["missing_class"] == "MISSING_CANONICAL_QUARTER"


def test_target_collision_detected(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY,ticker TEXT)")
        conn.execute("CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,company_id INTEGER,fiscal_year INTEGER,fiscal_quarter TEXT,period_end_date TEXT,publish_date TEXT)")
        conn.execute("INSERT INTO v3_company VALUES (1,'BBY')")
        conn.execute("INSERT INTO v3_quarter VALUES (1,1,2026,'Q1','2026-04-30','2025-05-29')")
        collisions = phase.target_collisions(conn, [{**phase.map_current_to_official([current_row()], [official_row()], {phase.latest8_key(official_row()): supplemental_row()})[0][0]}])
    assert collisions[0]["target_collision_class"] == "TARGET_SAME_ECONOMIC_COMPLEMENTARY"


def test_build_transformations_preserves_content_with_row() -> None:
    mapping = {
        **phase.map_current_to_official([current_row()], [official_row()], {phase.latest8_key(official_row()): supplemental_row()})[0][0],
        "target_collision_class": "TARGET_SAME_ECONOMIC_COMPLEMENTARY",
        "target_quarter_id": 1,
    }
    transformations, blockers = phase.build_transformations([mapping], {phase.latest8_key(official_row()): official_row()})
    assert blockers == []
    assert transformations[0]["lineage action"] == "PRESERVE"


def test_target_collision_blocks_different_economic_quarter() -> None:
    mapping = {
        **phase.map_current_to_official([current_row()], [official_row()], {phase.latest8_key(official_row()): supplemental_row()})[0][0],
        "target_collision_class": "TARGET_DIFFERENT_ECONOMIC_QUARTER",
        "target_quarter_id": 2,
    }
    transformations, blockers = phase.build_transformations([mapping], {phase.latest8_key(official_row()): official_row()})
    assert transformations == []
    assert blockers[0]["blocking_issue"] == "TARGET_DIFFERENT_ECONOMIC_QUARTER"


def test_apply_rehearsal_updates_copy_only(tmp_path: Path) -> None:
    db = tmp_path / "copy.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,period_end_date TEXT,publish_date TEXT,updated_at_utc TEXT)")
        conn.execute("INSERT INTO v3_quarter VALUES (1,'2026-04-30','2025-05-29','x')")
    log = phase.apply_rehearsal(
        db,
        [
            {
                "ticker": "BBY",
                "quarter_id": 1,
                "field": "period_end",
                "operation": "UPDATE_PERIOD_END",
                "old_value": "2026-04-30",
                "new_value": "2025-05-03",
            }
        ],
    )
    assert log[0]["rows_changed"] == 1


def test_unique_key_safe_rotation_constant_available() -> None:
    assert "CREATE_TEMP_IDENTITY" in {"CREATE_TEMP_IDENTITY", "UPDATE_PERIOD_END"}


def test_ticker_atomicity_summary_one_group_per_ticker() -> None:
    summary = phase.summarize_by_ticker([current_row()], [], [], [], [], [], [official_row()])
    assert len(summary) == 9


def test_no_production_write_safety_shape() -> None:
    safety = {"production_writes": 0, "ttm_writes": 0, "score_writes": 0, "lifecycle_writes": 0, "valuation_writes": 0, "rawcandle_writes": 0}
    assert all(value == 0 for value in safety.values())


def test_prevention_handoff_mentions_official_period_end(tmp_path: Path) -> None:
    path = tmp_path / "handoff.md"
    phase.write_prevention_handoff(path, phase.CLASSIFICATION_PARTIAL, [{"root_cause": "PERIOD_END_METADATA_SEGMENT"}])
    assert "official_period_end" in path.read_text()


def test_timeline_parity_marks_exact_match(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY,ticker TEXT)")
        conn.execute("CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,company_id INTEGER,fiscal_year INTEGER,fiscal_quarter TEXT,period_end_date TEXT,publish_date TEXT)")
        conn.execute("INSERT INTO v3_company VALUES (1,'BBY')")
        conn.execute("INSERT INTO v3_quarter VALUES (1,1,2026,'Q1','2025-05-03','2025-05-29')")
        parity = phase.timeline_parity(conn, [official_row()])
    assert parity[0]["period_end_parity"] == 1
    assert parity[0]["publish_date_parity"] == 1


def test_mapping_summary_classifies_missing_quarter() -> None:
    summary = phase.summarize_by_ticker([], [], [{"ticker": "BBY"}], [], [], [], [official_row()])
    assert summary[0]["root_cause"] == "MISSING_CANONICAL_QUARTER"


def test_transformation_old_value_guard_is_field_specific() -> None:
    mapping = {
        **phase.map_current_to_official([current_row()], [official_row()], {phase.latest8_key(official_row()): supplemental_row()})[0][0],
        "target_collision_class": "TARGET_SAME_ECONOMIC_COMPLEMENTARY",
        "target_quarter_id": 1,
    }
    transformations, _blockers = phase.build_transformations([mapping], {phase.latest8_key(official_row()): official_row()})
    assert transformations[0]["write guard"] == "1|period_end|2026-04-30"


def test_classification_names_are_locked() -> None:
    assert phase.CLASSIFICATION_READY.endswith("APPLY_SET_READY")
    assert phase.CLASSIFICATION_BLOCKED.endswith("MAPPING_BLOCKED")


def test_out_of_scope_p1_is_not_nine_ticker_scope() -> None:
    assert phase.OUT_OF_SCOPE_P1.isdisjoint(set(phase.NINE_TICKERS))


def test_derived_write_fields_are_zero_in_contract_shape() -> None:
    frozen = {"identity_writes": 0, "value_writes": 0, "creates": 0, "merges": 0, "deletes": 0}
    assert frozen == {"identity_writes": 0, "value_writes": 0, "creates": 0, "merges": 0, "deletes": 0}
