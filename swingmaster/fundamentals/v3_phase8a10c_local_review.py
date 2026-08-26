from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10b_full_sequence_audit import baseline, connect_ro, file_state, fiscal_ordinal
from swingmaster.fundamentals.v3_phase8a10b_p2p3_reprioritization import as_int, current_downstream_sets
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv


CLASSIFICATION_EXTERNAL_REQUIRED = "FUNDAMENTALS_V3_PHASE8A10C_LOCAL_REVIEW_COMPLETE_CURRENT_DOWNSTREAM_EXTERNAL_RESEARCH_REQUIRED"
CLASSIFICATION_NO_BLOCKERS = "FUNDAMENTALS_V3_PHASE8A10C_LOCAL_REVIEW_COMPLETE_NO_P2P3_CURRENT_DOWNSTREAM_BLOCKERS"
DERIVED_FLAGS = ("Affects Current TTM", "Affects Score", "Affects Lifecycle", "Affects Valuation")
LOCAL_ACTION = "LOCAL_EVIDENCE_REVIEW"
GLOBAL_P1_EXCLUDE_NOTE = "Excluded: global P1 workflow remains separate."


@dataclass(frozen=True)
class Phase8A10CPaths:
    artifact_root: Path
    p2p3_root: Path
    full_audit_root: Path
    v3_db: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def row_qid(row: dict[str, Any]) -> int | None:
    value = row.get("quarter_id") or row.get("Quarter ID")
    return None if value in (None, "") else int(value)


def queue_identity(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("Ticker") or row.get("ticker") or ""),
        str(row.get("Fiscal Year") or row.get("fiscal_year") or ""),
        str(row.get("Fiscal Q") or row.get("fiscal_quarter") or ""),
        str(row.get("Issue Type") or row.get("issue_type") or ""),
    )


def current_impact(row: dict[str, Any]) -> bool:
    return any(as_int(row.get(field)) for field in DERIVED_FLAGS)


def normalize_bool(value: Any) -> str:
    return "YES" if as_int(value) else "NO"


def source_rows_for_quarter(conn: Any, quarter_id: int) -> dict[str, Any]:
    audit_rows = rows(
        conn,
        """
        SELECT source,audit_type,decision,evidence_json,created_at_utc
        FROM v3_migration_audit
        WHERE quarter_id=?
        ORDER BY audit_id DESC
        """,
        (quarter_id,),
    )
    acquisitions = rows(
        conn,
        """
        SELECT provider,acquisition_result,usable_field_count,provider_cache_ref,last_success_at_utc,last_error_code
        FROM v3_provider_q_acquisition
        WHERE quarter_id=?
        ORDER BY acquisition_id DESC
        """,
        (quarter_id,),
    )
    issues = rows(
        conn,
        """
        SELECT issue_type,field_name,status,resolution,source_details_json
        FROM v3_resolution_issue
        WHERE quarter_id=?
        ORDER BY issue_id DESC
        """,
        (quarter_id,),
    )
    return {"migration_audit": audit_rows, "acquisitions": acquisitions, "issues": issues}


def canonical_context(conn: Any, company_id: int, fiscal_year: int, fiscal_quarter: str) -> tuple[dict[str, Any] | None, str]:
    target_ord = fiscal_ordinal(fiscal_year, fiscal_quarter)
    context = rows(
        conn,
        """
        SELECT q.quarter_id,c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
               q.market_availability_date,q.sec_confirmation_state,f.accepted_source_provider,f.derivation_method,
               f.resolution_issue_id
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE q.company_id=?
        ORDER BY q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
        """,
        (company_id,),
    )
    target = None
    lines = []
    for row in context:
        ord_ = fiscal_ordinal(int(row["fiscal_year"]), row["fiscal_quarter"])
        if abs(ord_ - target_ord) <= 1:
            lines.append(
                f"{row['fiscal_year']} {row['fiscal_quarter']} period={row['period_end_date']} publish={row['publish_date']} source={row.get('accepted_source_provider') or ''}"
            )
        if int(row["fiscal_year"]) == fiscal_year and row["fiscal_quarter"] == fiscal_quarter:
            target = row
    return target, " | ".join(lines)


def metadata_outcomes(evidence: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for audit in evidence["migration_audit"]:
        try:
            payload = json.loads(audit.get("evidence_json") or "{}")
        except json.JSONDecodeError:
            payload = {}
        out.extend(str(item) for item in payload.get("metadata_outcomes", []))
    return out


def local_evidence_summary(evidence: dict[str, Any]) -> str:
    providers = sorted({row["source"] for row in evidence["migration_audit"] if row.get("source")})
    outcomes = sorted(set(metadata_outcomes(evidence)))
    acquisitions = sorted({row["provider"] for row in evidence["acquisitions"] if row.get("provider")})
    issues = sorted({f"{row['issue_type']}:{row['status']}" for row in evidence["issues"] if row.get("issue_type")})
    parts = []
    if providers:
        parts.append("migration_sources=" + "|".join(providers))
    if outcomes:
        parts.append("metadata_outcomes=" + "|".join(outcomes))
    if acquisitions:
        parts.append("acquisitions=" + "|".join(acquisitions))
    if issues:
        parts.append("resolution_issues=" + "|".join(issues))
    return "; ".join(parts) or "no local source rows found"


def local_status(row: dict[str, Any], evidence: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    issue = str(row["Issue Type"])
    outcomes = set(metadata_outcomes(evidence))
    flags = str(row.get("Signals", ""))
    impact = current_impact(row)
    confirmed_both = {"PERIOD_DATE_CONFIRMED", "PUBLISH_DATE_CONFIRMED"}.issubset(outcomes)
    has_conflict = any("CONFLICT" in item for item in outcomes)

    if confirmed_both and issue in {"LONG", "EXTREME", "VERY_SHORT"}:
        return (
            "LOCAL_CONFIRMED_VALID_FALSE_POSITIVE",
            "Local migration evidence confirms both period_end and publish_date; heuristic timing anomaly downgraded.",
            "",
            "",
            "",
            "HIGH",
        )
    if confirmed_both and issue in {"SEVERE_LONG", "SEVERE_SHORT"}:
        return (
            "LOCAL_VALID_SPECIAL_CASE",
            "Local migration evidence confirms canonical dates despite unusual adjacent-period spacing.",
            "",
            "",
            "",
            "HIGH",
        )
    if not impact:
        return (
            "DOWNGRADE_RECENT_NONBLOCKING",
            "No current TTM/Score/Lifecycle/Valuation dependency after revalidation.",
            "",
            "",
            "",
            "MEDIUM",
        )
    if "MARKET_AVAILABILITY" in flags:
        return (
            "LOCAL_MARKET_AVAILABILITY_ONLY",
            "Only market-availability metadata is implicated; canonical result fields are not repaired here.",
            "",
            "",
            "",
            "MEDIUM",
        )
    if issue == "MISSING_HISTORY":
        return (
            "LOCAL_MISSING_HISTORY_NON_BLOCKING",
            "Sparse or incomplete history is documented but not a current canonical correction candidate by itself.",
            "",
            "",
            "",
            "MEDIUM",
        )
    if has_conflict:
        return (
            "EXTERNAL_RESEARCH_REQUIRED",
            "Local evidence contains metadata conflict and no safe exact correction candidate.",
            "",
            "",
            "",
            "MEDIUM",
        )
    if issue in {"SEVERE_SHORT", "POSSIBLE_ONE_YEAR_FISCAL_SHIFT"}:
        return (
            "ESCALATE_TO_P1",
            "Current-impact structural/date anomaly remains material after local review.",
            "",
            "",
            "",
            "MEDIUM",
        )
    return (
        "EXTERNAL_RESEARCH_REQUIRED",
        "Current-impact local evidence is insufficient to prove valid timing or exact repair.",
        "",
        "",
        "",
        "MEDIUM",
    )


def freeze_local_cases(queue_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    frozen = [row for row in queue_rows if row.get("Recommended Action") == LOCAL_ACTION]
    return [
        {
            "priority_rank": row.get("Priority Rank", ""),
            "ticker": row.get("Ticker", ""),
            "company_id": row.get("Company ID", ""),
            "fiscal_year": row.get("Fiscal Year", ""),
            "fiscal_quarter": row.get("Fiscal Q", ""),
            "period_end": row.get("Period End", ""),
            "publish_date": row.get("Publish Date", ""),
            "issue_type": row.get("Issue Type", ""),
            "original_severity": row.get("Original Severity", ""),
            "reclassified_severity": row.get("Reclassified Severity", ""),
            "signals": row.get("Signals", ""),
            "signal_count": row.get("Signal Count", ""),
            "latest_quarter_rank": row.get("Latest Quarter Rank", ""),
            "in_latest_4q": row.get("In Latest 4Q", ""),
            "in_latest_8q": row.get("In Latest 8Q", ""),
            "affects_current_ttm": row.get("Affects Current TTM", ""),
            "affects_score": row.get("Affects Score", ""),
            "affects_lifecycle": row.get("Affects Lifecycle", ""),
            "affects_valuation": row.get("Affects Valuation", ""),
            "current_evidence": row.get("Current Evidence", ""),
            "exact_missing_fact": row.get("Exact Missing Fact", ""),
        }
        for row in frozen
    ]


def parity_status(snapshot: dict[str, Any], current: dict[str, Any] | None) -> str:
    if current is None:
        return "ALREADY_RESOLVED"
    if str(current["period_end_date"] or "") != str(snapshot.get("period_end") or ""):
        return "CURRENT_STATE_DRIFT"
    if str(current["publish_date"] or "") != str(snapshot.get("publish_date") or ""):
        return "CURRENT_STATE_DRIFT"
    return "CURRENT_EXACT_MATCH"


def review_local_cases(conn: Any, frozen: list[dict[str, Any]], p1_keys: set[tuple[str, str, str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in frozen:
        company_id = int(row["company_id"])
        fiscal_year = int(row["fiscal_year"])
        fiscal_quarter = str(row["fiscal_quarter"])
        current, context = canonical_context(conn, company_id, fiscal_year, fiscal_quarter)
        status = parity_status(row, current)
        evidence = source_rows_for_quarter(conn, int(current["quarter_id"])) if current else {"migration_audit": [], "acquisitions": [], "issues": []}
        local_class, conclusion, repair_field, current_value, proposed_value, confidence = local_status(
            {
                "Issue Type": row["issue_type"],
                "Signals": row["signals"],
                "Affects Current TTM": row["affects_current_ttm"],
                "Affects Score": row["affects_score"],
                "Affects Lifecycle": row["affects_lifecycle"],
                "Affects Valuation": row["affects_valuation"],
            },
            evidence,
        )
        if queue_identity({"Ticker": row["ticker"], "Fiscal Year": row["fiscal_year"], "Fiscal Q": row["fiscal_quarter"], "Issue Type": row["issue_type"]}) in p1_keys:
            local_class = "DOWNGRADE_INFORMATIONAL"
            conclusion = GLOBAL_P1_EXCLUDE_NOTE
        out.append(
            {
                **row,
                "quarter_id": current["quarter_id"] if current else "",
                "parity_status": status,
                "current_period_end": current["period_end_date"] if current else "",
                "current_publish_date": current["publish_date"] if current else "",
                "current_canonical_context": context,
                "local_evidence_reviewed": local_evidence_summary(evidence),
                "local_resolution_status": local_class,
                "local_evidence_conclusion": conclusion,
                "repair_field": repair_field,
                "current_value": current_value,
                "proposed_value": proposed_value,
                "confidence": confidence,
            }
        )
    return out


def revalidate_impact_rows(conn: Any, rows_: list[dict[str, Any]]) -> list[dict[str, Any]]:
    downstream = current_downstream_sets(conn)
    out = []
    for row in rows_:
        qid = row_qid(row)
        if qid is None and row.get("Company ID") and row.get("Fiscal Year") and row.get("Fiscal Q"):
            current, _context = canonical_context(conn, int(row["Company ID"]), int(row["Fiscal Year"]), str(row["Fiscal Q"]))
            qid = int(current["quarter_id"]) if current else None
        affects_ttm = int(qid in downstream["current_ttm_inputs"] if qid is not None else False)
        affects_score = int(qid in downstream["current_score"] if qid is not None else False)
        affects_lifecycle = int(qid in downstream["current_lifecycle"] if qid is not None else False)
        affects_valuation = int(qid in downstream["current_valuation"] if qid is not None else False)
        out.append(
            {
                **row,
                "Affects Current TTM": str(affects_ttm),
                "Affects Score": str(affects_score),
                "Affects Lifecycle": str(affects_lifecycle),
                "Affects Valuation": str(affects_valuation),
            }
        )
    return out


def precise_question(row: dict[str, Any]) -> tuple[str, str]:
    issue = str(row.get("Issue Type", ""))
    ticker = row.get("Ticker", "")
    fy = row.get("Fiscal Year", "")
    fq = row.get("Fiscal Q", "")
    period = row.get("Period End", "")
    publish = row.get("Publish Date", "")
    if "PUBLISH" in str(row.get("Signals", "")) or issue in {"LONG", "EXTREME", "VERY_SHORT"}:
        return (
            f"Verify first public earnings/result publication date for {ticker} FY{fy} {fq} period_end {period}.",
            f"Official issuer earnings release date or SEC filing/publication evidence for current publish_date {publish}.",
        )
    if "DUPLICATE" in str(row.get("Signals", "")):
        return (
            f"Determine whether {ticker} FY{fy} {fq} period_end {period} duplicates another economic quarter.",
            "Official period identity and whether two canonical rows represent the same reporting period.",
        )
    return (
        f"Verify official fiscal identity and period_end for {ticker} FY{fy} {fq} currently mapped to {period}.",
        "Official FY/FQ label and quarter period_end for this canonical row.",
    )


def dedupe_external_queue(rows_: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows_:
        grouped[queue_identity(row)].append(row)
    dedupe_rows = []
    queue = []
    for idx, (identity, items) in enumerate(sorted(grouped.items()), 1):
        merged = dict(items[0])
        for flag in DERIVED_FLAGS:
            merged[flag] = str(int(any(as_int(item.get(flag)) for item in items)))
        impact_any = current_impact(merged)
        priority = "P3"
        if as_int(merged.get("Affects Current TTM")) and any(as_int(merged.get(flag)) for flag in ("Affects Score", "Affects Lifecycle", "Affects Valuation")):
            priority = "P1"
        elif as_int(merged.get("Affects Current TTM")):
            priority = "P2"
        elif impact_any:
            priority = "P3"
        question, missing = precise_question(merged)
        dedupe_rows.append(
            {
                "dedupe_key": "|".join(identity),
                "input_rows": len(items),
                "affects_current_ttm": merged.get("Affects Current TTM", ""),
                "affects_score": merged.get("Affects Score", ""),
                "affects_lifecycle": merged.get("Affects Lifecycle", ""),
                "affects_valuation": merged.get("Affects Valuation", ""),
            }
        )
        queue.append(
            {
                "Request ID": f"P8A10C-CD-{idx:03d}",
                "Priority": priority,
                "Ticker": merged.get("Ticker", ""),
                "Company ID": merged.get("Company ID", ""),
                "Fiscal Year": merged.get("Fiscal Year", ""),
                "Fiscal Q": merged.get("Fiscal Q", ""),
                "Period End": merged.get("Period End", ""),
                "Publish Date": merged.get("Publish Date", ""),
                "Issue Type": merged.get("Issue Type", ""),
                "Original Severity": merged.get("Original Severity", ""),
                "Current Classification": merged.get("Current Classification", "EXTERNAL_RESEARCH_REQUIRED"),
                "Signal Count": merged.get("Signal Count", ""),
                "Signals": merged.get("Signals", ""),
                "Latest Quarter Rank": merged.get("Latest Quarter Rank", ""),
                "In Latest 4Q": merged.get("In Latest 4Q", ""),
                "In Latest 8Q": merged.get("In Latest 8Q", ""),
                "Affects Current TTM": merged.get("Affects Current TTM", ""),
                "Affects Score": merged.get("Affects Score", ""),
                "Affects Lifecycle": merged.get("Affects Lifecycle", ""),
                "Affects Valuation": merged.get("Affects Valuation", ""),
                "Current Canonical Context": merged.get("Current Canonical Context", ""),
                "Local Evidence Reviewed": merged.get("Local Evidence Reviewed", ""),
                "Local Evidence Conclusion": merged.get("Local Evidence Conclusion", ""),
                "Exact Missing Fact": missing,
                "Exact Research Question": question,
                "Preferred Source": "Official company IR earnings release/archive; SEC filing metadata if issuer source is insufficient",
                "Candidate Current Value": merged.get("Candidate Current Value", ""),
                "Candidate Correct Value": merged.get("Candidate Correct Value", ""),
                "Notes": merged.get("Notes", ""),
            }
        )
    queue.sort(key=lambda r: (r["Priority"], as_int(r.get("Latest Quarter Rank"), 999999), -as_int(r.get("Signal Count")), r["Ticker"]))
    for idx, row in enumerate(queue, 1):
        row["Request ID"] = f"P8A10C-CD-{idx:03d}"
    return queue, dedupe_rows


def write_human_summary(path: Path, queue: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(
            [
                "# Phase 8A10C Current-Downstream External Research Queue",
                "",
                f"Rows: `{len(queue)}`",
                "",
                *(f"- {row['Request ID']} {row['Priority']}: {row['Ticker']} FY{row['Fiscal Year']} {row['Fiscal Q']} - {row['Exact Research Question']}" for row in queue),
                "",
            ]
        ),
        encoding="utf-8",
    )


def run_phase8a10c_local_review(paths: Phase8A10CPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    v3_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)

    queue_rows = read_csv(paths.p2p3_root / "current_critical_2024plus_last8q_queue.csv")
    p2a = read_csv(paths.p2p3_root / "P2A_current_critical.csv")
    p3e = read_csv(paths.p2p3_root / "P3_escalated.csv")
    global_p1 = read_csv(paths.full_audit_root / "global_P1.csv")
    p1_keys = {queue_identity({"Ticker": row.get("ticker"), "Fiscal Year": row.get("fiscal_year"), "Fiscal Q": row.get("fiscal_quarter"), "Issue Type": row.get("pattern")}) for row in global_p1}
    frozen = freeze_local_cases(queue_rows)
    if len(frozen) != 30:
        raise RuntimeError(f"expected 30 LOCAL_EVIDENCE_REVIEW rows, got {len(frozen)}")

    with connect_ro(paths.v3_db) as conn:
        base = baseline(conn, paths.v3_db)
        reviewed = review_local_cases(conn, frozen, p1_keys)
        impact_source = revalidate_impact_rows(conn, [row for row in queue_rows if row.get("Recommended Action") == "EXTERNAL_RESEARCH"])
        after_base = baseline(conn, paths.v3_db)

    review_by_key = {queue_identity({"Ticker": row["ticker"], "Fiscal Year": row["fiscal_year"], "Fiscal Q": row["fiscal_quarter"], "Issue Type": row["issue_type"]}): row for row in reviewed}
    review_by_public_key = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row["issue_type"]): row for row in reviewed}
    external_after_local = []
    backlog = []
    local_removed = 0
    for row in queue_rows:
        key = queue_identity(row)
        local = review_by_key.get(key)
        row = dict(row)
        row["Current Classification"] = row.get("Recommended Action", "")
        if row["Current Classification"] == "EXTERNAL_RESEARCH":
            row["Current Classification"] = "EXTERNAL_RESEARCH_REQUIRED"
        row["Current Canonical Context"] = ""
        row["Local Evidence Reviewed"] = ""
        row["Local Evidence Conclusion"] = ""
        row["Candidate Current Value"] = ""
        row["Candidate Correct Value"] = ""
        if local:
            row["Current Classification"] = local["local_resolution_status"]
            row["Current Canonical Context"] = local["current_canonical_context"]
            row["Local Evidence Reviewed"] = local["local_evidence_reviewed"]
            row["Local Evidence Conclusion"] = local["local_evidence_conclusion"]
            row["Candidate Current Value"] = local["current_value"]
            row["Candidate Correct Value"] = local["proposed_value"]
            if local["local_resolution_status"] not in {"EXTERNAL_RESEARCH_REQUIRED", "ESCALATE_TO_P1", "LOCAL_EVIDENCE_SUPPORTS_REPAIR"}:
                local_removed += 1
        if row["Current Classification"] in {"EXTERNAL_RESEARCH_REQUIRED", "ESCALATE_TO_P1", "LOCAL_EVIDENCE_SUPPORTS_REPAIR"} and current_impact(row):
            external_after_local.append(row)
        elif as_int(row.get("In Latest 8Q")) and not current_impact(row) and row.get("Recommended Action") != "EXTERNAL_RESEARCH":
            backlog.append({**row, "BLOCKS_PHASE8_CLOSURE": "NO", "recommended_later_action": "REVIEW_AFTER_CURRENT_DOWNSTREAM_CLOSURE"})

    nonlocal_impact = [row for row in impact_source if current_impact(row)]
    final_candidates = []
    p1_public = {(row.get("ticker"), str(row.get("fiscal_year")), row.get("fiscal_quarter")) for row in global_p1}
    for row in external_after_local:
        public = (row.get("Ticker"), str(row.get("Fiscal Year")), row.get("Fiscal Q"))
        if public in p1_public:
            continue
        final_candidates.append(row)
    external_queue, dedupe = dedupe_external_queue(final_candidates)

    write_csv(paths.artifact_root / "phase8a10c_frozen_local_evidence_cases.csv", frozen)
    input_recon = {
        "local_evidence_rows": len(frozen),
        "expected_local_evidence_rows": 30,
        "queue_rows": len(queue_rows),
        "P2A_rows": len(p2a),
        "P3_ESCALATED_rows": len(p3e),
        "global_P1_rows": len(global_p1),
    }
    write_json(paths.artifact_root / "phase8a10c_input_reconciliation.json", input_recon)
    write_csv(paths.artifact_root / "local_evidence_review.csv", reviewed)
    write_csv(paths.artifact_root / "local_evidence_repair_candidates.csv", [row for row in reviewed if row["local_resolution_status"] == "LOCAL_EVIDENCE_SUPPORTS_REPAIR"])
    write_csv(paths.artifact_root / "local_false_positive_cases.csv", [row for row in reviewed if row["local_resolution_status"] in {"LOCAL_CONFIRMED_VALID_FALSE_POSITIVE", "LOCAL_VALID_SPECIAL_CASE"}])
    write_csv(paths.artifact_root / "local_downgraded_cases.csv", [row for row in reviewed if row["local_resolution_status"] in {"DOWNGRADE_RECENT_NONBLOCKING", "DOWNGRADE_INFORMATIONAL", "LOCAL_MISSING_HISTORY_NON_BLOCKING", "LOCAL_MARKET_AVAILABILITY_ONLY"}])
    write_csv(paths.artifact_root / "local_escalated_P1_cases.csv", [row for row in reviewed if row["local_resolution_status"] == "ESCALATE_TO_P1"])
    impact_rows = [
        {"metric": "before_local_review_raw_current_ttm", "rows": sum(1 for row in queue_rows if as_int(row.get("Affects Current TTM")))},
        {"metric": "before_local_review_raw_score", "rows": sum(1 for row in queue_rows if as_int(row.get("Affects Score")))},
        {"metric": "before_local_review_raw_lifecycle", "rows": sum(1 for row in queue_rows if as_int(row.get("Affects Lifecycle")))},
        {"metric": "before_local_review_raw_valuation", "rows": sum(1 for row in queue_rows if as_int(row.get("Affects Valuation")))},
        {"metric": "after_local_review_raw_current_ttm", "rows": sum(1 for row in final_candidates if as_int(row.get("Affects Current TTM")))},
        {"metric": "after_local_review_raw_score", "rows": sum(1 for row in final_candidates if as_int(row.get("Affects Score")))},
        {"metric": "after_local_review_raw_lifecycle", "rows": sum(1 for row in final_candidates if as_int(row.get("Affects Lifecycle")))},
        {"metric": "after_local_review_raw_valuation", "rows": sum(1 for row in final_candidates if as_int(row.get("Affects Valuation")))},
    ]
    write_csv(paths.artifact_root / "current_downstream_impact_before_after.csv", impact_rows)
    write_csv(paths.artifact_root / "current_downstream_deduplication.csv", dedupe)
    write_csv(paths.artifact_root / "current_downstream_external_research_queue.csv", external_queue)
    write_human_summary(paths.artifact_root / "current_downstream_external_research_human_summary.md", external_queue)
    write_csv(paths.artifact_root / "latest8q_nonblocking_backlog.csv", backlog)
    write_json(
        paths.artifact_root / "latest8q_nonblocking_backlog_summary.json",
        {
            "rows": len(backlog),
            "unique_tickers": len({row.get("Ticker") for row in backlog}),
            "latest_quarter": sum(as_int(row.get("Latest Quarter Rank")) == 1 for row in backlog),
            "latest_4q": sum(as_int(row.get("In Latest 4Q")) for row in backlog),
            "blocks_phase8_closure": "NO",
        },
    )

    status_counts = Counter(row["local_resolution_status"] for row in reviewed)
    priority_counts = Counter(row["Priority"] for row in external_queue)
    safety = {
        "production_writes": int(file_state(paths.v3_db) != v3_before),
        "rawcandle_writes": int(file_state(paths.rawcandle_db) != raw_before),
        "ttm_writes": 0,
        "score_writes": 0,
        "lifecycle_writes": 0,
        "valuation_writes": 0,
        "baseline_unchanged": int(base == after_base),
    }
    classification = CLASSIFICATION_EXTERNAL_REQUIRED if external_queue else CLASSIFICATION_NO_BLOCKERS
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "starting_state": {
            "local_evidence_rows": len(frozen),
            "local_evidence_companies": len({row["ticker"] for row in frozen}),
            "external_research_rows_before": sum(1 for row in queue_rows if row.get("Recommended Action") == "EXTERNAL_RESEARCH"),
            "current_ttm_impact_rows_before": sum(1 for row in queue_rows if as_int(row.get("Affects Current TTM"))),
            "score_impact_rows_before": sum(1 for row in queue_rows if as_int(row.get("Affects Score"))),
            "lifecycle_impact_rows_before": sum(1 for row in queue_rows if as_int(row.get("Affects Lifecycle"))),
            "valuation_impact_rows_before": sum(1 for row in queue_rows if as_int(row.get("Affects Valuation"))),
        },
        "local_review": {
            "exact_current_matches": sum(1 for row in reviewed if row["parity_status"] == "CURRENT_EXACT_MATCH"),
            "state_drift": sum(1 for row in reviewed if row["parity_status"] == "CURRENT_STATE_DRIFT"),
            "already_resolved": sum(1 for row in reviewed if row["parity_status"] == "ALREADY_RESOLVED"),
            "locally_confirmed_valid": status_counts["LOCAL_CONFIRMED_VALID_FALSE_POSITIVE"],
            "valid_special_cases": status_counts["LOCAL_VALID_SPECIAL_CASE"],
            "missing_history_nonblocking": status_counts["LOCAL_MISSING_HISTORY_NON_BLOCKING"],
            "market_availability_only": status_counts["LOCAL_MARKET_AVAILABILITY_ONLY"],
            "local_repair_ready": status_counts["LOCAL_EVIDENCE_SUPPORTS_REPAIR"],
            "downgraded_recent_nonblocking": status_counts["DOWNGRADE_RECENT_NONBLOCKING"],
            "downgraded_informational": status_counts["DOWNGRADE_INFORMATIONAL"],
            "external_research_still_required": status_counts["EXTERNAL_RESEARCH_REQUIRED"],
            "escalated_P1": status_counts["ESCALATE_TO_P1"],
        },
        "local_repair_candidates": {
            "repair_candidate_rows": status_counts["LOCAL_EVIDENCE_SUPPORTS_REPAIR"],
            "affected_fields": [],
            "affected_tickers": [],
            "current_downstream_impact": 0,
            "production_writes": safety["production_writes"],
        },
        "current_downstream_union": {
            "raw_ttm_impact_rows_after_local_review": sum(1 for row in final_candidates if as_int(row.get("Affects Current TTM"))),
            "raw_score_impact_rows_after_local_review": sum(1 for row in final_candidates if as_int(row.get("Affects Score"))),
            "raw_lifecycle_impact_rows_after_local_review": sum(1 for row in final_candidates if as_int(row.get("Affects Lifecycle"))),
            "raw_valuation_impact_rows_after_local_review": sum(1 for row in final_candidates if as_int(row.get("Affects Valuation"))),
            "unique_current_impact_issues_before_dedupe": len(final_candidates),
            "unique_current_impact_issues_after_dedupe": len(external_queue),
            "raw_impacted_rows_before_local_review": len(nonlocal_impact) + sum(1 for row in reviewed if any(as_int(row.get(flag)) for flag in ("affects_current_ttm", "affects_score", "affects_lifecycle", "affects_valuation"))),
            "removed_or_resolved_by_local_evidence": local_removed,
        },
        "final_external_queue": {
            "queue_rows": len(external_queue),
            "unique_tickers": len({row["Ticker"] for row in external_queue}),
            "priority_1": priority_counts["P1"],
            "priority_2": priority_counts["P2"],
            "priority_3": priority_counts["P3"],
            "latest_quarter": sum(as_int(row.get("Latest Quarter Rank")) == 1 for row in external_queue),
            "latest_4q": sum(as_int(row.get("In Latest 4Q")) for row in external_queue),
            "latest_8q": sum(as_int(row.get("In Latest 8Q")) for row in external_queue),
            "multi_signal": sum(as_int(row.get("Signal Count")) >= 2 for row in external_queue),
            "single_signal": sum(as_int(row.get("Signal Count")) == 1 for row in external_queue),
        },
        "backlog": {
            "latest8q_nonblocking_rows": len(backlog),
            "unique_backlog_tickers": len({row.get("Ticker") for row in backlog}),
            "latest_quarter_backlog": sum(as_int(row.get("Latest Quarter Rank")) == 1 for row in backlog),
            "latest_4q_backlog": sum(as_int(row.get("In Latest 4Q")) for row in backlog),
            "blocks_phase8_closure": "NO",
        },
        "global_P1": {
            "global_P1_rows_excluded": len(global_p1),
            "global_P1_overlap_with_final_queue": sum(
                1 for row in external_queue if (row.get("Ticker"), str(row.get("Fiscal Year")), row.get("Fiscal Q")) in p1_public
            ),
        },
        "safety": safety,
        "artifacts": {
            "local_review": str(paths.artifact_root / "local_evidence_review.csv"),
            "local_repair_candidates": str(paths.artifact_root / "local_evidence_repair_candidates.csv"),
            "final_external_queue": str(paths.artifact_root / "current_downstream_external_research_queue.csv"),
            "external_queue_human_summary": str(paths.artifact_root / "current_downstream_external_research_human_summary.md"),
            "latest8q_backlog": str(paths.artifact_root / "latest8q_nonblocking_backlog.csv"),
        },
        "next_action": (
            "USER EXTERNAL RESEARCH - CURRENT TTM / SCORE / LIFECYCLE / VALUATION QUEUE"
            if external_queue
            else "WAIT FOR GLOBAL P1 RESEARCH, THEN FINAL CANONICAL CLOSURE"
        ),
    }
    write_json(paths.artifact_root / "phase8a10c_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if safety["production_writes"] or safety["rawcandle_writes"] or not safety["baseline_unchanged"]:
        raise RuntimeError("read-only safety guard failed")
    return summary
