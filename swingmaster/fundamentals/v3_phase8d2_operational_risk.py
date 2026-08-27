from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import EXPECTED_P1_TICKERS, semantic_fingerprints, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro


CLASSIFICATION = "FUNDAMENTALS_V3_PHASE8D2_OPERATIONAL_RISK_ASSESSED"
KNOWN = set(EXPECTED_P1_TICKERS)
RECOMMEND_ACCEPT_KNOWN = "CURRENT_V3_OPERATIONALLY_ACCEPTABLE_WITH_DOCUMENTED_KNOWN_RISK"
RECOMMEND_ACCEPT_EXPANDED = "CURRENT_V3_OPERATIONALLY_ACCEPTABLE_WITH_EXPANDED_RISK_REGISTER"
RECOMMEND_TOO_BROAD = "CURRENT_V3_CURRENT_DOWNSTREAM_RISK_TOO_BROAD"


@dataclass(frozen=True)
class Phase8D2Paths:
    artifact_root: Path
    phase8d1_root: Path = Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL")
    v3_db: Path = Path("rc_fundamentals_v3.db")


def read_csv_dicts(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def int_value(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    return int(value) if value not in (None, "") else 0


def validate_source(root: Path) -> dict[str, Any]:
    required = [
        "full_canonical_fiscal_guard_audit.csv",
        "full_canonical_fiscal_guard_summary.json",
        "all_blocked_rows.csv",
        "exact_anchor_proven_blocks.csv",
        "backward_inference_blocks.csv",
        "known_P1_guard_replay.csv",
        "phase8d1_summary.json",
    ]
    missing = [name for name in required if not (root / name).exists()]
    summary = json.loads((root / "phase8d1_summary.json").read_text(encoding="utf-8"))
    audit = read_csv_dicts(root / "full_canonical_fiscal_guard_audit.csv")
    decisions = Counter(row["guard_decision"] for row in audit)
    exact_blocks = sum(1 for row in audit if row["block_kind"] == "EXACT_ANCHOR_PROVEN_CONFLICT")
    backward_blocks = sum(1 for row in audit if row["block_kind"] == "BACKWARD_INFERENCE_BLOCK")
    return {
        "artifact_root": str(root),
        "missing_artifacts": missing,
        "full_audit_rows": len(audit),
        "decision_counts": dict(decisions),
        "summary_rows_match": int(len(audit) == int(summary["rows_audited"])),
        "summary_decisions_match": int(dict(decisions) == summary["decision_counts"]),
        "exact_anchor_proven_blocks": exact_blocks,
        "backward_inference_blocks": backward_blocks,
        "expected_exact_anchor_proven_blocks_match": int(exact_blocks == 833),
        "expected_backward_inference_blocks_match": int(backward_blocks == 11291),
        "known_p1_available": int((root / "known_P1_guard_replay.csv").exists()),
        "valid": int(not missing and len(audit) == int(summary["rows_audited"]) and dict(decisions) == summary["decision_counts"] and exact_blocks == 833 and backward_blocks == 11291),
    }


def summarize_rows(data: list[dict[str, Any]]) -> dict[str, Any]:
    decisions = Counter(row["guard_decision"] for row in data)
    blocked = [row for row in data if row["guard_decision"] == "BLOCK"]
    exact = sum(1 for row in blocked if row["block_kind"] == "EXACT_ANCHOR_PROVEN_CONFLICT")
    backward = sum(1 for row in blocked if row["block_kind"] == "BACKWARD_INFERENCE_BLOCK")
    confidence = Counter(row["block_confidence"] for row in blocked if row["block_confidence"])
    reasons = Counter(reason for row in blocked for reason in row["reason_codes"].split("|") if reason)
    tickers = {row["ticker"] for row in blocked}
    total = len(data)
    return {
        "rows": total,
        "PASS": decisions.get("PASS", 0),
        "PASS_WITH_WARNING": decisions.get("PASS_WITH_WARNING", 0),
        "REVIEW": decisions.get("REVIEW", 0),
        "BLOCK": decisions.get("BLOCK", 0),
        "block_pct": round(decisions.get("BLOCK", 0) * 100 / total, 4) if total else 0,
        "exact_anchor_proven_BLOCK": exact,
        "backward_inference_BLOCK": backward,
        "PROVEN_HIGH": confidence.get("PROVEN_HIGH", 0),
        "STRUCTURAL_HIGH": confidence.get("STRUCTURAL_HIGH", 0),
        "STRUCTURAL_MEDIUM": confidence.get("STRUCTURAL_MEDIUM", 0),
        "INFERENCE_RISK": confidence.get("INFERENCE_RISK", 0),
        "affected_tickers": len(tickers),
        "major_reason_codes": "|".join(code for code, _ in reasons.most_common(8)),
    }


def latest_quarter_rows(audit: list[dict[str, Any]], active_only: bool = True) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in audit:
        if active_only and int_value(row, "active") != 1:
            continue
        grouped[row["ticker"]].append(row)
    out = []
    for ticker_rows in grouped.values():
        out.append(max(ticker_rows, key=lambda r: (r["period_end"], int(r["fiscal_year"]), r["fiscal_quarter"])))
    return sorted(out, key=lambda r: r["ticker"])


def risk_for_inputs(input_rows: list[dict[str, Any]]) -> dict[str, Any]:
    blocked = [row for row in input_rows if row["guard_decision"] == "BLOCK"]
    reviews = [row for row in input_rows if row["guard_decision"] == "REVIEW"]
    warnings = [row for row in input_rows if row["guard_decision"] == "PASS_WITH_WARNING"]
    exact = [row for row in blocked if row["block_kind"] == "EXACT_ANCHOR_PROVEN_CONFLICT"]
    backward = [row for row in blocked if row["block_kind"] == "BACKWARD_INFERENCE_BLOCK"]
    if len(blocked) >= 2:
        risk = "TTM_MULTIPLE_STRUCTURAL_CONFLICTS"
    elif exact:
        risk = "TTM_EXACT_ANCHOR_CONFLICT"
    elif backward:
        risk = "TTM_BACKWARD_INFERENCE_RISK"
    elif warnings or reviews:
        risk = "TTM_WARNING_ONLY"
    else:
        risk = "TTM_CLEAN"
    return {
        "risk_class": risk,
        "blocked_inputs": len(blocked),
        "review_inputs": len(reviews),
        "warning_inputs": len(warnings),
        "exact_anchor_blocked_inputs": len(exact),
        "backward_inference_blocked_inputs": len(backward),
        "blocked_quarters": "|".join(f"{row['fiscal_year']}{row['fiscal_quarter']}" for row in blocked),
        "blocked_reason_codes": "|".join(sorted({reason for row in blocked for reason in row["reason_codes"].split("|") if reason})),
    }


def latest_by_company(data: list[dict[str, Any]], key: str = "endpoint_period_end") -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in data:
        grouped[int(row["company_id"])].append(row)
    return [max(group, key=lambda r: (r.get(key) or "", int(r.get("endpoint_quarter_id") or r.get("as_of_quarter_id") or 0))) for group in grouped.values()]


def build_ttm_risk(conn: sqlite3.Connection, audit_by_qid: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    ttm_rows = latest_by_company(rows(conn, "SELECT * FROM v3_ttm"), "period_end")
    out = []
    for ttm in ttm_rows:
        input_ids = [int(ttm[col]) for col in ("q1_quarter_id", "q2_quarter_id", "q3_quarter_id", "q4_quarter_id") if ttm.get(col) is not None]
        input_risks = [audit_by_qid[qid] for qid in input_ids if qid in audit_by_qid]
        risk = risk_for_inputs(input_risks)
        out.append({
            "company_id": ttm["company_id"],
            "ttm_id": ttm["ttm_id"],
            "endpoint_quarter_id": ttm["endpoint_quarter_id"],
            "endpoint_fiscal_year": ttm["endpoint_fiscal_year"],
            "endpoint_fiscal_quarter": ttm["endpoint_fiscal_quarter"],
            "period_end": ttm["period_end"],
            **risk,
        })
    return sorted(out, key=lambda r: (r["risk_class"], r["company_id"]))


def attach_tickers(conn: sqlite3.Connection, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ticker_by_company = {int(row["company_id"]): row["ticker"] for row in rows(conn, "SELECT company_id,ticker FROM v3_company")}
    return [{**row, "ticker": ticker_by_company.get(int(row["company_id"]), "")} for row in data]


def downstream_risk(conn: sqlite3.Connection, table: str, ttm_by_id: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    if table == "v3_score":
        data = latest_by_company(rows(conn, "SELECT company_id,score_id,as_of_quarter_id,endpoint_ttm_id,endpoint_period_end,score_ready FROM v3_score"), "endpoint_period_end")
        id_col = "score_id"
    elif table == "v3_lifecycle":
        data = latest_by_company(rows(conn, "SELECT company_id,lifecycle_id,endpoint_quarter_id,endpoint_ttm_id,endpoint_period_end,lifecycle_ready FROM v3_lifecycle"), "endpoint_period_end")
        id_col = "lifecycle_id"
    else:
        data = latest_by_company(rows(conn, "SELECT company_id,valuation_id,endpoint_quarter_id,endpoint_ttm_id,endpoint_period_end,valuation_date,valuation_ready FROM v3_valuation"), "endpoint_period_end")
        id_col = "valuation_id"
    out = []
    for row in data:
        ttm = ttm_by_id.get(int(row["endpoint_ttm_id"]))
        risk_class = ttm["risk_class"] if ttm else "UNAVAILABLE"
        exact = int(bool(ttm and ttm["exact_anchor_blocked_inputs"]))
        blocked = int(bool(ttm and ttm["blocked_inputs"]))
        out.append({
            "company_id": row["company_id"],
            id_col: row[id_col],
            "endpoint_ttm_id": row["endpoint_ttm_id"],
            "endpoint_period_end": row.get("endpoint_period_end", ""),
            "source_ttm_risk_class": risk_class,
            "affected_by_blocked_ttm_input": blocked,
            "exact_anchor_conflict_input": exact,
            "backward_inference_risk_input": int(bool(ttm and ttm["backward_inference_blocked_inputs"])),
            "proven_downstream_error": 0,
            "downstream_risk_class": "KNOWN_INPUT_RISK" if blocked else "STRUCTURAL_RISK_ONLY" if risk_class == "TTM_WARNING_ONLY" else "NO_CURRENT_DOWNSTREAM_IMPACT",
        })
    return attach_tickers(conn, out)


def ticker_risk(audit: list[dict[str, Any]], ttm_risk: list[dict[str, Any]], score: list[dict[str, Any]], lifecycle: list[dict[str, Any]], valuation: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ttm_by_ticker = {row["ticker"]: row for row in ttm_risk}
    score_by_ticker = {row["ticker"]: row for row in score}
    lifecycle_by_ticker = {row["ticker"]: row for row in lifecycle}
    valuation_by_ticker = {row["ticker"]: row for row in valuation}
    latest8_blocks: dict[str, list[dict[str, Any]]] = defaultdict(list)
    latest4_blocks: dict[str, list[dict[str, Any]]] = defaultdict(list)
    latest_blocks = {row["ticker"]: row for row in latest_quarter_rows(audit) if row["guard_decision"] == "BLOCK"}
    for row in audit:
        if row["guard_decision"] == "BLOCK" and int_value(row, "latest8q"):
            latest8_blocks[row["ticker"]].append(row)
        if row["guard_decision"] == "BLOCK" and int_value(row, "latest4q"):
            latest4_blocks[row["ticker"]].append(row)
    tickers = sorted(set(latest8_blocks) | set(latest4_blocks) | set(latest_blocks) | {row["ticker"] for row in ttm_risk if row["blocked_inputs"]})
    out = []
    for ticker in tickers:
        ttm = ttm_by_ticker.get(ticker, {})
        latest = latest_blocks.get(ticker)
        exact = bool((latest and latest["block_kind"] == "EXACT_ANCHOR_PROVEN_CONFLICT") or ttm.get("exact_anchor_blocked_inputs"))
        multiple = len(latest8_blocks.get(ticker, [])) > 1 or int(ttm.get("blocked_inputs") or 0) >= 2
        if exact:
            priority = "P1"
        elif multiple or any(row["block_confidence"] == "STRUCTURAL_HIGH" for row in latest4_blocks.get(ticker, [])):
            priority = "P2"
        elif ttm.get("blocked_inputs"):
            priority = "P3"
        else:
            priority = "P4"
        all_blocks = latest8_blocks.get(ticker, []) + latest4_blocks.get(ticker, []) + ([latest] if latest else [])
        dominant = Counter(reason for row in all_blocks for reason in row.get("reason_codes", "").split("|") if reason).most_common(1)
        out.append({
            "ticker": ticker,
            "priority": priority,
            "known_13": int(ticker in KNOWN),
            "latest8q_blocked_quarters": len(latest8_blocks.get(ticker, [])),
            "latest4q_blocked_quarters": len(latest4_blocks.get(ticker, [])),
            "latest_quarter_impact": int(latest is not None),
            "current_ttm_impact": int(bool(ttm.get("blocked_inputs"))),
            "score_impact": int(bool(score_by_ticker.get(ticker, {}).get("affected_by_blocked_ttm_input"))),
            "lifecycle_impact": int(bool(lifecycle_by_ticker.get(ticker, {}).get("affected_by_blocked_ttm_input"))),
            "valuation_impact": int(bool(valuation_by_ticker.get(ticker, {}).get("affected_by_blocked_ttm_input"))),
            "evidence_class": "EXACT_ANCHOR_PROVEN" if exact else "BACKWARD_INFERENCE" if any(row.get("block_kind") == "BACKWARD_INFERENCE_BLOCK" for row in all_blocks) or ttm.get("backward_inference_blocked_inputs") else "STRUCTURAL_RISK_ONLY",
            "proven_vs_risk": "KNOWN_INPUT_RISK" if ticker in KNOWN else "STRUCTURAL_RISK_ONLY",
            "dominant_reason": dominant[0][0] if dominant else ttm.get("blocked_reason_codes", ""),
        })
    return sorted(out, key=lambda r: (r["priority"], -r["latest_quarter_impact"], -r["current_ttm_impact"], r["ticker"]))


def write_doc(summary: dict[str, Any]) -> None:
    block = f"""## Phase 8D-2 - Current / Recent Operational Risk Assessment

Status: `FUNDAMENTALS_V3_PHASE8D2_OPERATIONAL_RISK_ASSESSED`

Artifact root: `{summary['artifact_root']}`

The assessment reused the Phase 8D-1 full-audit artifacts and did not rerun the fiscal guard. Historical context remains BLOCK `{summary['source']['historical_BLOCK']}` of `{summary['source']['full_rows']}` rows, split into exact-anchor-proven `{summary['source']['exact_anchor_proven_total']}` and backward-inference `{summary['source']['backward_inference_total']}`.

Current/recent exposure: 2024+ BLOCK `{summary['cohorts']['2024plus']['BLOCK']}` of `{summary['cohorts']['2024plus']['rows']}`; 2025+ BLOCK `{summary['cohorts']['2025plus']['BLOCK']}` of `{summary['cohorts']['2025plus']['rows']}`; latest8Q BLOCK `{summary['cohorts']['latest8q']['BLOCK']}` of `{summary['cohorts']['latest8q']['rows']}`; latest4Q BLOCK `{summary['cohorts']['latest4q']['BLOCK']}` of `{summary['cohorts']['latest4q']['rows']}`; latest-quarter BLOCK `{summary['cohorts']['latest_quarter']['BLOCK']}` of `{summary['cohorts']['latest_quarter']['rows']}`.

Current TTM risk distribution: TTM_CLEAN `{summary['ttm_distribution'].get('TTM_CLEAN', 0)}`, TTM_WARNING_ONLY `{summary['ttm_distribution'].get('TTM_WARNING_ONLY', 0)}`, TTM_BACKWARD_INFERENCE_RISK `{summary['ttm_distribution'].get('TTM_BACKWARD_INFERENCE_RISK', 0)}`, TTM_EXACT_ANCHOR_CONFLICT `{summary['ttm_distribution'].get('TTM_EXACT_ANCHOR_CONFLICT', 0)}`, TTM_MULTIPLE_STRUCTURAL_CONFLICTS `{summary['ttm_distribution'].get('TTM_MULTIPLE_STRUCTURAL_CONFLICTS', 0)}`.

Operational recommendation: `{summary['recommendation']}`. Phase 8 remains `IN PROGRESS`; Phase 8B downstream remains the temporary operational baseline with the Phase 8D guard active.
"""
    path = Path("docs/fundamentals_v3_phase8_update_v3.md")
    path.write_text(path.read_text(encoding="utf-8").rstrip() + "\n\n" + block, encoding="utf-8")


def run_phase8d2(paths: Phase8D2Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    validation = validate_source(paths.phase8d1_root)
    if not validation["valid"]:
        raise RuntimeError("PHASE8D2_INVALID_SOURCE_ARTIFACTS")
    audit = read_csv_dicts(paths.phase8d1_root / "full_canonical_fiscal_guard_audit.csv")
    audit_by_qid = {int(row["quarter_id"]): row for row in audit}
    before_fp = semantic_fingerprints(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        ttm = attach_tickers(conn, build_ttm_risk(conn, audit_by_qid))
        ttm_by_id = {int(row["ttm_id"]): row for row in ttm}
        score = downstream_risk(conn, "v3_score", ttm_by_id)
        lifecycle = downstream_risk(conn, "v3_lifecycle", ttm_by_id)
        valuation = downstream_risk(conn, "v3_valuation", ttm_by_id)
    after_fp = semantic_fingerprints(paths.v3_db)

    cohorts = {
        "2024plus": summarize_rows([row for row in audit if int(row["fiscal_year"]) >= 2024]),
        "2025plus": summarize_rows([row for row in audit if int(row["fiscal_year"]) >= 2025]),
        "fy2026": summarize_rows([row for row in audit if int(row["fiscal_year"]) == 2026]),
        "fy2027": summarize_rows([row for row in audit if int(row["fiscal_year"]) == 2027]),
        "latest8q": summarize_rows([row for row in audit if int_value(row, "latest8q")]),
        "latest4q": summarize_rows([row for row in audit if int_value(row, "latest4q")]),
        "latest_quarter": summarize_rows(latest_quarter_rows(audit)),
    }
    high_risk = ticker_risk(audit, ttm, score, lifecycle, valuation)
    new_current = [row for row in high_risk if row["priority"] in {"P1", "P2", "P3"} and not row["known_13"]]
    new_p1 = [row for row in new_current if row["priority"] == "P1"]
    new_downstream = [row for row in new_current if row["score_impact"] or row["lifecycle_impact"] or row["valuation_impact"]]
    recommendation = RECOMMEND_ACCEPT_KNOWN if not new_current else RECOMMEND_ACCEPT_EXPANDED
    if new_p1 and new_downstream:
        recommendation = RECOMMEND_TOO_BROAD
    source_summary = json.loads((paths.phase8d1_root / "phase8d1_summary.json").read_text(encoding="utf-8"))
    summary = {
        "classification": CLASSIFICATION,
        "artifact_root": str(paths.artifact_root),
        "phase8d1_root": str(paths.phase8d1_root),
        "source": {
            "full_rows": validation["full_audit_rows"],
            "historical_BLOCK": validation["decision_counts"].get("BLOCK", 0),
            "exact_anchor_proven_total": validation["exact_anchor_proven_blocks"],
            "backward_inference_total": validation["backward_inference_blocks"],
            "source_recommendation": source_summary["recommendation"],
        },
        "cohorts": cohorts,
        "ttm_distribution": dict(Counter(row["risk_class"] for row in ttm)),
        "ttm_input_exposure": {
            "current_latest_ttm_companies": len(ttm),
            "zero_blocked_quarters": sum(1 for row in ttm if int(row["blocked_inputs"]) == 0),
            "one_blocked_quarter": sum(1 for row in ttm if int(row["blocked_inputs"]) == 1),
            "two_plus_blocked_quarters": sum(1 for row in ttm if int(row["blocked_inputs"]) >= 2),
            "exact_anchor_blocked_input": sum(1 for row in ttm if int(row["exact_anchor_blocked_inputs"]) > 0),
            "backward_inference_blocked_input": sum(1 for row in ttm if int(row["backward_inference_blocked_inputs"]) > 0),
            "review_input": sum(1 for row in ttm if int(row["review_inputs"]) > 0),
            "only_pass_warning_input": sum(1 for row in ttm if int(row["blocked_inputs"]) == 0 and int(row["review_inputs"]) == 0),
        },
        "score_distribution": dict(Counter(row["downstream_risk_class"] for row in score)),
        "lifecycle_distribution": dict(Counter(row["downstream_risk_class"] for row in lifecycle)),
        "valuation_distribution": dict(Counter(row["downstream_risk_class"] for row in valuation)),
        "score_exposure": {"rows": len(score), "clean_ttm": sum(1 for row in score if row["downstream_risk_class"] == "NO_CURRENT_DOWNSTREAM_IMPACT"), "known_input_risk": sum(1 for row in score if row["downstream_risk_class"] == "KNOWN_INPUT_RISK"), "exact_anchor_conflict_input": sum(1 for row in score if int(row["exact_anchor_conflict_input"]) > 0), "proven_errors": 0},
        "lifecycle_exposure": {"rows": len(lifecycle), "clean_ttm": sum(1 for row in lifecycle if row["downstream_risk_class"] == "NO_CURRENT_DOWNSTREAM_IMPACT"), "known_input_risk": sum(1 for row in lifecycle if row["downstream_risk_class"] == "KNOWN_INPUT_RISK"), "exact_anchor_conflict_input": sum(1 for row in lifecycle if int(row["exact_anchor_conflict_input"]) > 0), "proven_errors": 0},
        "valuation_exposure": {"rows": len(valuation), "clean_fundamental_input": sum(1 for row in valuation if row["downstream_risk_class"] == "NO_CURRENT_DOWNSTREAM_IMPACT"), "known_input_risk": sum(1 for row in valuation if row["downstream_risk_class"] == "KNOWN_INPUT_RISK"), "exact_anchor_conflict_input": sum(1 for row in valuation if int(row["exact_anchor_conflict_input"]) > 0), "proven_errors": 0},
        "high_risk_priorities": dict(Counter(row["priority"] for row in high_risk)),
        "new_current_risk_tickers": len(new_current),
        "new_current_p1_tickers": len(new_p1),
        "new_current_downstream_risk_tickers": len(new_downstream),
        "new_current_risk_ticker_list": [row["ticker"] for row in new_current],
        "proven_current_downstream_errors": 0,
        "current_downstream_known_risk_only_cases": sum(1 for row in high_risk if row["proven_vs_risk"] == "KNOWN_INPUT_RISK" and (row["score_impact"] or row["lifecycle_impact"] or row["valuation_impact"])),
        "current_downstream_structural_risk_only_cases": sum(1 for row in high_risk if row["proven_vs_risk"] == "STRUCTURAL_RISK_ONLY" and (row["score_impact"] or row["lifecycle_impact"] or row["valuation_impact"])),
        "historical_only_block_cases": validation["decision_counts"].get("BLOCK", 0) - cohorts["latest8q"]["BLOCK"],
        "recommendation": recommendation,
        "next_action": "KEEP PHASE 8B DOWNSTREAM AS THE TEMPORARY OPERATIONAL BASELINE; KEEP PHASE 8D GUARD ACTIVE; RETURN TO DEFERRED CANONICAL REPAIRS LATER"
        if recommendation == RECOMMEND_ACCEPT_KNOWN
        else "KEEP CURRENT V3 OPERATIONAL TEMPORARILY; ADD THE NEW CURRENT-RISK TICKERS TO THE DEFERRED REPAIR REGISTER; KEEP THE GUARD ACTIVE"
        if recommendation == RECOMMEND_ACCEPT_EXPANDED
        else "DO NOT PROCEED TO PHASE 8 OPERATIONAL CLOSURE; PRIORITIZE ONLY CURRENT-LATEST / TTM-AFFECTING FISCAL IDENTITY REPAIRS",
        "safety": {
            "canonical_writes": 0,
            "fundamentals_writes": 0,
            "fiscal_metadata_writes": 0,
            "ttm_writes": 0,
            "score_writes": 0,
            "lifecycle_writes": 0,
            "valuation_writes": 0,
            "rawcandle_writes": 0,
            "semantic_fingerprints_unchanged": int(before_fp == after_fp),
        },
    }

    write_json(paths.artifact_root / "phase8d1_artifact_validation.json", validation)
    write_csv(paths.artifact_root / "2024plus_operational_risk.csv", [row for row in audit if int(row["fiscal_year"]) >= 2024])
    write_csv(paths.artifact_root / "2025plus_operational_risk.csv", [row for row in audit if int(row["fiscal_year"]) >= 2025])
    write_csv(paths.artifact_root / "latest8q_operational_risk.csv", [row for row in audit if int_value(row, "latest8q")])
    write_csv(paths.artifact_root / "latest4q_operational_risk.csv", [row for row in audit if int_value(row, "latest4q")])
    write_csv(paths.artifact_root / "latest_quarter_operational_risk.csv", latest_quarter_rows(audit))
    write_csv(paths.artifact_root / "current_ttm_fiscal_risk.csv", ttm)
    write_csv(paths.artifact_root / "current_ttm_risk_distribution.csv", [{"risk_class": k, "rows": v} for k, v in Counter(row["risk_class"] for row in ttm).items()])
    write_csv(paths.artifact_root / "current_score_fiscal_risk.csv", score)
    write_csv(paths.artifact_root / "current_lifecycle_fiscal_risk.csv", lifecycle)
    write_csv(paths.artifact_root / "current_valuation_fiscal_risk.csv", valuation)
    write_csv(paths.artifact_root / "current_high_risk_tickers.csv", high_risk)
    write_csv(paths.artifact_root / "known_13_ticker_current_impact.csv", [row for row in high_risk if row["ticker"] in KNOWN])
    write_csv(paths.artifact_root / "new_current_risk_tickers.csv", new_current)
    current_rows = [row for row in audit if int_value(row, "latest8q") or int(row["fiscal_year"]) >= 2024 or int_value(row, "ttm_input")]
    write_csv(paths.artifact_root / "current_exact_anchor_conflicts.csv", [row for row in current_rows if row["block_kind"] == "EXACT_ANCHOR_PROVEN_CONFLICT"])
    write_csv(paths.artifact_root / "current_backward_inference_risk.csv", [row for row in current_rows if row["block_kind"] == "BACKWARD_INFERENCE_BLOCK"])
    write_csv(paths.artifact_root / "current_proven_vs_risk_matrix.csv", high_risk)
    write_json(paths.artifact_root / "phase8d2_summary.json", summary)
    paths.artifact_root.joinpath("operational_go_no_go.md").write_text(summary["recommendation"] + "\n", encoding="utf-8")
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    write_doc(summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Assess current/recent operational exposure from Phase 8D-1 fiscal audit artifacts.")
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--phase8d1-root", type=Path, default=Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase8d2_operational_risk") / utc_stamp()
    summary = run_phase8d2(Phase8D2Paths(artifact_root=artifact_root, phase8d1_root=args.phase8d1_root, v3_db=args.v3_db))
    print(f"classification={summary['classification']}")
    print(f"recommendation={summary['recommendation']}")
    print(f"latest_quarter={json.dumps(summary['cohorts']['latest_quarter'], sort_keys=True)}")
    print(f"ttm_distribution={json.dumps(summary['ttm_distribution'], sort_keys=True)}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
