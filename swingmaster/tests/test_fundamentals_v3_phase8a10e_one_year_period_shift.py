from __future__ import annotations

from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10e_one_year_period_shift as phase


def current_row(ticker: str = "BBY", fy: int = 2026, fq: str = "Q1", period: str = "2026-04-30", publish: str = "2025-05-29", revenue: float = 100.0) -> dict:
    return {
        "ticker": ticker,
        "quarter_id": 1,
        "fiscal_year": fy,
        "fiscal_quarter": fq,
        "period_end_date": period,
        "publish_date": publish,
        "revenue": revenue,
        "gross_profit": 50.0,
        "operating_income": 10.0,
        "ebit": 10.0,
        "ebitda": 12.0,
        "net_income": 7.0,
    }


def official_row(ticker: str = "BBY", fy: int = 2026, fq: str = "Q1", period: str = "2025-05-03", publish: str = "2025-05-29", revenue: str = "100") -> dict[str, str]:
    return {
        "Ticker": ticker,
        "Fiscal Year": str(fy),
        "Fiscal Q": fq,
        "Official Period End": period,
        "Publish Date": publish,
        "Revenue": revenue,
        "Operating Income": "10",
        "Net Income": "7",
        "Primary Source": "issuer",
        "Confidence": "HIGH",
    }


def test_exact_nine_ticker_set() -> None:
    assert phase.NINE_TICKERS == ("BBY", "DELL", "GCO", "HAE", "MRVL", "RL", "SAIC", "TJX", "TRNS")


def test_ticker_placeholders_match_scope() -> None:
    assert phase.ticker_placeholders().count("?") == 9


def test_value_equal_accepts_rounding_tolerance() -> None:
    assert phase.value_equal(100.00001, "100")


def test_value_equal_rejects_missing_values() -> None:
    assert not phase.value_equal(None, "100")


def test_matching_value_count_uses_core_comparison_fields() -> None:
    assert phase.matching_value_count(current_row(), official_row()) == 3


def test_best_official_value_match_finds_other_identity() -> None:
    row = current_row(fy=2025, fq="Q1")
    best = phase.best_official_value_match(row, [official_row(fy=2026, fq="Q1")])
    assert best and best["Fiscal Year"] == "2026"


def test_offset_exact() -> None:
    assert phase.offset_class("2025-05-03", "2025-05-03")[0] == "EXACT"


def test_plus_one_year_same_month_day_detection() -> None:
    assert phase.offset_class("2026-05-03", "2025-05-03")[0] == "PLUS_ONE_YEAR_SAME_MONTH_DAY"


def test_minus_one_year_same_month_day_detection() -> None:
    assert phase.offset_class("2024-05-03", "2025-05-03")[0] == "MINUS_ONE_YEAR_SAME_MONTH_DAY"


def test_month_end_normalized_plus_one_year_detection() -> None:
    assert phase.offset_class("2026-04-30", "2025-05-03")[0] == "PLUS_ONE_YEAR_MONTH_END_NORMALIZED"


def test_other_date_shift_detection() -> None:
    assert phase.offset_class("2026-01-15", "2025-05-03")[0] == "OTHER_DATE_SHIFT"


def test_no_official_match_offset() -> None:
    assert phase.offset_class("2026-01-15", None)[0] == "NO_OFFICIAL_MATCH"


def test_analyze_alignment_marks_period_end_metadata_only_when_all_else_correct() -> None:
    content, period, publish = phase.analyze_alignment([current_row()], [official_row()])
    assert content[0]["classification"] == "PERIOD_END_METADATA_ONLY"
    assert period[0]["calendar_normalization_component"] == "MONTH_END_NORMALIZED"
    assert publish[0]["publish_status"] == "PUBLISH_CORRECT"


def test_analyze_alignment_marks_wrong_content_same_identity() -> None:
    content, _period, _publish = phase.analyze_alignment([current_row(revenue=200)], [official_row()])
    assert content[0]["content_status"] == "FYQ_CORRECT_CONTENT_WRONG"


def test_analyze_alignment_marks_content_for_another_quarter() -> None:
    row = current_row(fy=2025, fq="Q1")
    content, _period, _publish = phase.analyze_alignment([row], [official_row(fy=2026, fq="Q1")])
    assert content[0]["content_status"] == "FYQ_WRONG_CONTENT_CORRECT_FOR_ANOTHER_QUARTER"


def test_publish_wrong_is_separate_from_content() -> None:
    _content, _period, publish = phase.analyze_alignment([current_row(publish="2025-06-01")], [official_row()])
    assert publish[0]["publish_status"] == "PUBLISH_WRONG"


def test_repair_candidates_include_only_proven_period_end_only_rows() -> None:
    content, period, publish = phase.analyze_alignment([current_row()], [official_row()])
    candidates = phase.repair_candidates(content, period, publish, [official_row()])
    assert candidates[0]["repair_type"] == "PERIOD_END_METADATA_ONLY"


def test_repair_candidates_exclude_wrong_content() -> None:
    content, period, publish = phase.analyze_alignment([current_row(revenue=200)], [official_row()])
    assert phase.repair_candidates(content, period, publish, [official_row()]) == []


def test_repair_candidates_exclude_wrong_publish() -> None:
    content, period, publish = phase.analyze_alignment([current_row(publish="2025-06-01")], [official_row()])
    assert phase.repair_candidates(content, period, publish, [official_row()]) == []


def test_segment_summary_identifies_first_and_last_bad_row() -> None:
    segments = [current_row(fy=2025, fq="Q4", period="2026-01-31", publish="2025-03-04"), current_row(fy=2026, fq="Q1")]
    official = [official_row(fy=2025, fq="Q4", period="2025-02-01", publish="2025-03-04"), official_row()]
    content, period, publish = phase.analyze_alignment(segments, official)
    divergence, summary = phase.segment_summary(segments, content, period, publish)
    assert divergence[0]["first_bad_quarter"] == "FY2025 Q4"
    assert summary[0]["bad_rows"] == 2


def test_per_ticker_summary_blocks_non_period_only_case() -> None:
    content = [{"ticker": "BBY", "quarter_id": 1, "content_status": "FYQ_WRONG_CONTENT_CORRECT_FOR_ANOTHER_QUARTER"}]
    period = [{"ticker": "BBY", "quarter_id": 1, "official_period_end": "2025-05-03", "offset_class": "PLUS_ONE_YEAR_MONTH_END_NORMALIZED"}]
    publish = [{"ticker": "BBY", "quarter_id": 1, "official_publish_date": "2025-05-29", "publish_status": "PUBLISH_CORRECT"}]
    bad = [{"ticker": "BBY", "first_bad_quarter": "FY2026 Q1", "last_bad_quarter": "FY2026 Q1", "bad_rows": 1}]
    summaries = phase.per_ticker_summary([{"ticker": "BBY"}], content, period, publish, bad, [])
    assert summaries[0]["production_ready"] == "NO"


def test_source_code_trace_names_yahoo_seed_path() -> None:
    assert "v3_yahoo_canonical_seed.py::prepare_yahoo_seed" in phase.source_code_trace()


def test_classification_constants_are_stable() -> None:
    assert phase.CLASSIFICATION_BLOCKED.endswith("BLOCKERS_REMAIN")


def test_read_csv_handles_utf8_sig(tmp_path: Path) -> None:
    path = tmp_path / "input.csv"
    path.write_text("\ufeffTicker,Fiscal Year\nBBY,2026\n", encoding="utf-8")
    assert phase.read_csv(path)[0]["Ticker"] == "BBY"


def test_apply_rehearsal_updates_period_only_on_copy(tmp_path: Path) -> None:
    import sqlite3

    db = tmp_path / "copy.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY, period_end_date TEXT, updated_at_utc TEXT)")
        conn.execute("INSERT INTO v3_quarter VALUES (1,'2026-04-30','x')")
    log = phase.apply_rehearsal(
        db,
        [{"ticker": "BBY", "quarter_id": 1, "current_period_end": "2026-04-30", "verified_period_end": "2025-05-03"}],
    )
    with sqlite3.connect(db) as conn:
        period = conn.execute("SELECT period_end_date FROM v3_quarter WHERE quarter_id=1").fetchone()[0]
    assert log[0]["rows_changed"] == 1
    assert period == "2025-05-03"
