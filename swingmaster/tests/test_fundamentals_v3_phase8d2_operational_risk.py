from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_phase8d2_operational_risk import (
    KNOWN,
    latest_quarter_rows,
    read_csv_dicts,
    risk_for_inputs,
    summarize_rows,
    ticker_risk,
    validate_source,
)


FIELDNAMES = [
    "active",
    "block_confidence",
    "block_kind",
    "company_id",
    "fiscal_quarter",
    "fiscal_year",
    "guard_decision",
    "latest4q",
    "latest8q",
    "period_end",
    "quarter_id",
    "reason_codes",
    "ticker",
    "ttm_input",
]


def write_source(root: Path, data: list[dict[str, str]]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    with (root / "full_canonical_fiscal_guard_audit.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(data)
    summary = {
        "rows_audited": len(data),
        "decision_counts": dict(__import__("collections").Counter(row["guard_decision"] for row in data)),
        "recommendation": "KEEP_PHASE8D_GUARD_UNCHANGED",
    }
    (root / "phase8d1_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (root / "full_canonical_fiscal_guard_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    for name in ("all_blocked_rows.csv", "exact_anchor_proven_blocks.csv", "backward_inference_blocks.csv", "known_P1_guard_replay.csv"):
        (root / name).write_text("ticker\n", encoding="utf-8")


def row(ticker: str, qid: int, fy: int, fq: str, decision: str = "PASS", **extra: str) -> dict[str, str]:
    out = {
        "active": "1",
        "block_confidence": "",
        "block_kind": "",
        "company_id": str(qid // 10),
        "fiscal_quarter": fq,
        "fiscal_year": str(fy),
        "guard_decision": decision,
        "latest4q": "1",
        "latest8q": "1",
        "period_end": f"{fy}-03-31",
        "quarter_id": str(qid),
        "reason_codes": "",
        "ticker": ticker,
        "ttm_input": "1",
    }
    out.update(extra)
    return out


def test_validate_source_uses_phase8d1_artifacts(tmp_path: Path) -> None:
    data = [row("AAA", 10, 2026, "Q1", "BLOCK", block_kind="EXACT_ANCHOR_PROVEN_CONFLICT"), row("BBB", 20, 2025, "Q4", "BLOCK", block_kind="BACKWARD_INFERENCE_BLOCK")]
    write_source(tmp_path, data)

    validation = validate_source(tmp_path)

    assert validation["full_audit_rows"] == 2
    assert validation["decision_counts"]["BLOCK"] == 2
    assert validation["valid"] == 0


def test_summarize_rows_splits_exact_and_backward() -> None:
    data = [
        row("AAA", 10, 2026, "Q1", "BLOCK", block_kind="EXACT_ANCHOR_PROVEN_CONFLICT", block_confidence="PROVEN_HIGH", reason_codes="FY_SHIFT_PLUS_ONE"),
        row("BBB", 20, 2025, "Q4", "BLOCK", block_kind="BACKWARD_INFERENCE_BLOCK", block_confidence="INFERENCE_RISK", reason_codes="FQ_SLOT_MISMATCH"),
        row("CCC", 30, 2025, "Q4", "PASS_WITH_WARNING"),
    ]

    summary = summarize_rows(data)

    assert summary["rows"] == 3
    assert summary["BLOCK"] == 2
    assert summary["exact_anchor_proven_BLOCK"] == 1
    assert summary["backward_inference_BLOCK"] == 1
    assert summary["PROVEN_HIGH"] == 1
    assert summary["INFERENCE_RISK"] == 1


def test_latest_quarter_cohort_uses_latest_period_per_active_ticker() -> None:
    data = [
        row("AAA", 10, 2025, "Q4", period_end="2025-12-31"),
        row("AAA", 11, 2026, "Q1", period_end="2026-03-31"),
        row("OLD", 20, 2026, "Q1", active="0", period_end="2026-03-31"),
    ]

    latest = latest_quarter_rows(data)

    assert [r["quarter_id"] for r in latest] == ["11"]


def test_ttm_risk_classes_exact_backward_multiple_and_warning() -> None:
    exact = row("AAA", 10, 2026, "Q1", "BLOCK", block_kind="EXACT_ANCHOR_PROVEN_CONFLICT")
    backward = row("AAA", 11, 2025, "Q4", "BLOCK", block_kind="BACKWARD_INFERENCE_BLOCK")
    warning = row("AAA", 12, 2025, "Q3", "PASS_WITH_WARNING")

    assert risk_for_inputs([exact])["risk_class"] == "TTM_EXACT_ANCHOR_CONFLICT"
    assert risk_for_inputs([backward])["risk_class"] == "TTM_BACKWARD_INFERENCE_RISK"
    assert risk_for_inputs([exact, backward])["risk_class"] == "TTM_MULTIPLE_STRUCTURAL_CONFLICTS"
    assert risk_for_inputs([warning])["risk_class"] == "TTM_WARNING_ONLY"


def test_ticker_risk_includes_known_13_and_downstream_impact() -> None:
    audit = [
        row("BBY", 10, 2026, "Q1", "BLOCK", block_kind="EXACT_ANCHOR_PROVEN_CONFLICT", latest8q="1", latest4q="1"),
        row("NEW", 20, 2025, "Q4", "BLOCK", block_kind="BACKWARD_INFERENCE_BLOCK", latest8q="1", latest4q="0"),
    ]
    ttm = [
        {"ticker": "BBY", "blocked_inputs": 1, "exact_anchor_blocked_inputs": 1, "backward_inference_blocked_inputs": 0, "blocked_reason_codes": "FY_SHIFT_PLUS_ONE"},
        {"ticker": "NEW", "blocked_inputs": 1, "exact_anchor_blocked_inputs": 0, "backward_inference_blocked_inputs": 1, "blocked_reason_codes": "FQ_SLOT_MISMATCH"},
    ]
    score = [{"ticker": "BBY", "affected_by_blocked_ttm_input": 1}, {"ticker": "NEW", "affected_by_blocked_ttm_input": 0}]
    lifecycle = [{"ticker": "BBY", "affected_by_blocked_ttm_input": 1}]
    valuation = [{"ticker": "BBY", "affected_by_blocked_ttm_input": 1}]

    risks = ticker_risk(audit, ttm, score, lifecycle, valuation)
    bby = next(r for r in risks if r["ticker"] == "BBY")
    new = next(r for r in risks if r["ticker"] == "NEW")

    assert "BBY" in KNOWN
    assert bby["known_13"] == 1
    assert bby["priority"] == "P1"
    assert bby["score_impact"] == 1
    assert new["known_13"] == 0
    assert new["priority"] == "P3"


def test_read_csv_dicts_round_trip(tmp_path: Path) -> None:
    write_source(tmp_path, [row("AAA", 10, 2026, "Q1")])

    data = read_csv_dicts(tmp_path / "full_canonical_fiscal_guard_audit.csv")

    assert data[0]["ticker"] == "AAA"
    assert data[0]["quarter_id"] == "10"
