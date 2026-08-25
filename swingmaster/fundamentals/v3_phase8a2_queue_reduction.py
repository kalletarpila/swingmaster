from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import open_ro, rows, write_csv, write_json


CLASSIFICATION_USER_EVIDENCE = "FUNDAMENTALS_V3_PHASE8A2_MANUAL_QUEUE_REDUCED_USER_EVIDENCE_REQUIRED"
CLASSIFICATION_READY_FREEZE = "FUNDAMENTALS_V3_PHASE8A2_EXISTING_EVIDENCE_SUFFICIENT_READY_FOR_REPAIR_SET_FREEZE"
CLASSIFICATION_SYSTEMIC = "FUNDAMENTALS_V3_PHASE8A2_SYSTEMIC_ROOT_CAUSE_REVIEW_REQUIRED"
NEXT_ACTION_USER = "USER MANUAL EVIDENCE REVIEW - QUEUE A"
NEXT_ACTION_FREEZE = "PHASE 8F - REPAIR-SET FREEZE / GO-NO-GO"

PUBLISH = "PUBLISH_DATE_ANOMALY"
SEMANTIC = "SEMANTIC_FIELD_OUTLIER"
HIGH_VALUE_FIELDS = {"publish_date", "revenue", "cash", "total_debt", "shares_outstanding", "ebit", "free_cashflow"}


@dataclass(frozen=True)
class Phase8A2Paths:
    phase8_root: Path
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run_phase8a2(paths: Phase8A2Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    master = read_csv(paths.phase8_root / "phase8_master_anomaly_table.csv")
    manual_raw = read_csv(paths.phase8_root / "manual_evidence_requests.csv")
    repair_set = read_csv(paths.phase8_root / "phase8_frozen_repair_set.csv")
    summary = json.loads((paths.phase8_root / "phase8_summary.json").read_text(encoding="utf-8"))
    reconcile_inputs(master, manual_raw, repair_set, summary)

    before = production_counts(paths.v3_db)
    annotated = annotate_findings(master)
    evidence_units, dedup_map = build_evidence_units(annotated)
    queue_a, queue_b, queue_c = split_queues(evidence_units)

    with open_ro(paths.v3_db) as conn:
        write_csv(paths.artifact_root / "publish_wrong_quarter_candidates.csv", publish_wrong_quarter_candidates(conn, annotated))
        write_csv(paths.artifact_root / "semantic_adjacent_quarter_context.csv", semantic_adjacent_context(conn, annotated))
        write_csv(paths.artifact_root / "share_split_classification.csv", share_split_classification(conn, annotated))

    write_csv(paths.artifact_root / "phase8a2_input_reconciliation.csv", input_reconciliation(master, manual_raw, repair_set, summary))
    write_csv(paths.artifact_root / "publish_cluster_summary.csv", publish_cluster_summary(annotated))
    write_csv(paths.artifact_root / "publish_offset_patterns.csv", publish_offset_patterns(annotated))
    write_csv(paths.artifact_root / "publish_source_concentration.csv", source_concentration(annotated, PUBLISH))
    write_csv(paths.artifact_root / "publish_systematic_root_causes.csv", systematic_root_causes(annotated, PUBLISH))
    write_csv(paths.artifact_root / "semantic_field_distribution.csv", field_distribution(annotated))
    write_csv(paths.artifact_root / "semantic_source_pair_matrix.csv", semantic_source_pair_matrix(annotated))
    write_csv(paths.artifact_root / "semantic_conflict_shape_clusters.csv", semantic_conflict_shape_clusters(annotated))
    write_csv(paths.artifact_root / "semantic_existing_evidence_resolution.csv", existing_evidence_resolution(annotated))
    write_csv(paths.artifact_root / "latest_state_impact_analysis.csv", latest_state_impact(annotated))
    write_csv(paths.artifact_root / "historical_materiality_reassessment.csv", historical_reassessment(annotated))
    write_csv(paths.artifact_root / "recent_2026_wait_vs_manual.csv", recent_2026(annotated))
    write_csv(paths.artifact_root / "manual_request_evidence_units.csv", evidence_units)
    write_csv(paths.artifact_root / "manual_request_dedup_map.csv", dedup_map)
    write_csv(paths.artifact_root / "manual_evidence_queue_A_must_check_now.csv", queue_a)
    write_csv(paths.artifact_root / "manual_evidence_queue_B_check_if_needed.csv", queue_b)
    write_csv(paths.artifact_root / "manual_evidence_queue_C_wait_accept_systematic.csv", queue_c)
    write_human_summary(paths.artifact_root / "manual_evidence_queue_A_human_summary.md", queue_a)

    after = production_counts(paths.v3_db)
    out = build_summary(started, annotated, manual_raw, evidence_units, queue_a, queue_b, queue_c, before, after)
    write_json(paths.artifact_root / "phase8a2_summary.json", out)
    write_next_action(paths.artifact_root / "phase8_gate1_next_action.md", out)
    return out


def reconcile_inputs(master: list[dict[str, str]], manual_raw: list[dict[str, str]], repair_set: list[dict[str, str]], summary: dict[str, Any]) -> None:
    publish = sum(1 for row in master if row["issue_type"] == PUBLISH)
    semantic = sum(1 for row in master if row["issue_type"] == SEMANTIC)
    if len(master) != 348 or publish != 111 or semantic != 237 or len(manual_raw) != 327:
        raise RuntimeError(f"Phase 8A2 input mismatch: findings={len(master)} publish={publish} semantic={semantic} manual={len(manual_raw)}")
    if repair_set:
        raise RuntimeError("Phase 8A2 expected empty Phase 8 frozen repair set")
    if summary.get("classification") != "FUNDAMENTALS_V3_PHASE8_MANUAL_EVIDENCE_REQUIRED":
        raise RuntimeError(f"Unexpected Phase 8 classification {summary.get('classification')}")


def input_reconciliation(master: list[dict[str, str]], manual_raw: list[dict[str, str]], repair_set: list[dict[str, str]], summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"metric": "total_findings", "expected": 348, "actual": len(master), "match": len(master) == 348},
        {"metric": "publish_findings", "expected": 111, "actual": sum(1 for r in master if r["issue_type"] == PUBLISH), "match": True},
        {"metric": "semantic_findings", "expected": 237, "actual": sum(1 for r in master if r["issue_type"] == SEMANTIC), "match": True},
        {"metric": "raw_manual_requests", "expected": 327, "actual": len(manual_raw), "match": len(manual_raw) == 327},
        {"metric": "frozen_repair_set_rows", "expected": 0, "actual": len(repair_set), "match": len(repair_set) == 0},
        {"metric": "phase8_classification", "expected": "FUNDAMENTALS_V3_PHASE8_MANUAL_EVIDENCE_REQUIRED", "actual": summary.get("classification"), "match": summary.get("classification") == "FUNDAMENTALS_V3_PHASE8_MANUAL_EVIDENCE_REQUIRED"},
    ]


def annotate_findings(master: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in master:
        item: dict[str, Any] = dict(row)
        item["offset_days"] = offset_days(row.get("publish_date"), row.get("period_end")) if row["issue_type"] == PUBLISH else ""
        item["source_family"] = source_family(row.get("source_provenance", ""))
        item["evidence_document_type"] = document_type(row)
        item["disposition"] = disposition(row)
        item["queue"] = queue_for_disposition(item["disposition"], row)
        out.append(item)
    seen = Counter(row["issue_id"] for row in out)
    dupes = [issue_id for issue_id, count in seen.items() if count != 1]
    if dupes:
        raise RuntimeError(f"duplicate or missing issue ids: {dupes[:5]}")
    return out


def offset_days(publish_date: str | None, period_end: str | None) -> int | str:
    if not publish_date or not period_end:
        return ""
    p = datetime.strptime(publish_date, "%Y-%m-%d").date()
    e = datetime.strptime(period_end, "%Y-%m-%d").date()
    return (p - e).days


def source_family(source_provenance: str) -> str:
    if not source_provenance:
        return "UNKNOWN"
    try:
        payload = json.loads(source_provenance)
    except json.JSONDecodeError:
        return "UNPARSEABLE"
    source = payload.get("source", "UNKNOWN")
    audit_type = payload.get("audit_type", "UNKNOWN")
    evidence = payload.get("evidence_json") or ""
    try:
        evidence_json = json.loads(evidence)
    except Exception:
        evidence_json = {}
    recovery = evidence_json.get("recovery_mode") or evidence_json.get("raw_evidence_ref") or "UNKNOWN"
    return f"{source}:{audit_type}:{recovery}"


def document_type(row: dict[str, str]) -> str:
    if row["issue_type"] == PUBLISH:
        return "ISSUER_IR_EARNINGS_RELEASE_OR_ARCHIVE"
    if row["field_name"] == "shares_outstanding":
        return "10Q_10K_SHARES_AND_CORPORATE_ACTION_EVIDENCE"
    return "10Q_10K_OR_ISSUER_REPORTED_STATEMENT"


def disposition(row: dict[str, str]) -> str:
    age = row["age_bucket"]
    latest = row.get("latest_company_state_affected") == "1"
    priority = row["priority"]
    if priority == "P4_LOW_CURRENT_MATERIALITY":
        return "LOW_MATERIALITY_ACCEPT"
    if age == "2026" and not latest:
        return "WAIT_FOR_REFRESH"
    if age == "2026" and latest:
        return "MANUAL_A"
    if latest and row["field_name"] in HIGH_VALUE_FIELDS:
        return "MANUAL_A"
    if priority == "P1_CURRENT_MATERIAL" and row["field_name"] in HIGH_VALUE_FIELDS:
        return "MANUAL_A"
    if priority == "P2_HISTORICAL_MATERIAL":
        return "MANUAL_B"
    return "MANUAL_B"


def queue_for_disposition(disposition_value: str, row: dict[str, str]) -> str:
    if disposition_value == "MANUAL_A":
        return "A"
    if disposition_value == "MANUAL_B":
        return "B"
    return "C"


def build_evidence_units(annotated: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in annotated:
        key = (row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["evidence_document_type"])
        groups[key].append(row)
    units = []
    dedup = []
    for idx, (_key, items) in enumerate(sorted(groups.items()), 1):
        queue = min((item["queue"] for item in items), key={"A": 0, "B": 1, "C": 2}.get)
        dispositions = sorted({item["disposition"] for item in items})
        unit = {
            "evidence_unit_id": f"EU-{idx:03d}",
            "queue": queue,
            "priority": min((item["priority"] for item in items), key=priority_sort),
            "ticker": items[0]["ticker"],
            "company": items[0]["company_name"],
            "company_id": items[0]["company_id"],
            "market": items[0]["market"],
            "fiscal_year": items[0]["fiscal_year"],
            "fiscal_quarter": items[0]["fiscal_quarter"],
            "period_end": items[0]["period_end"],
            "publish_date": items[0]["publish_date"],
            "issue_ids_included": "|".join(item["issue_id"] for item in items),
            "issue_types": "|".join(sorted({item["issue_type"] for item in items})),
            "affected_fields": "|".join(sorted({item["field_name"] for item in items})),
            "current_stored_values": "|".join(f"{item['field_name']}={item['stored_value']}" for item in items),
            "source_provenance": " || ".join(sorted({item.get("source_provenance", "") for item in items if item.get("source_provenance")}))[:4000],
            "preferred_source_document": items[0]["evidence_document_type"],
            "exact_questions": " ".join(exact_question(item) for item in items),
            "latest_ttm_impact": int(any(item.get("latest_company_state_affected") == "1" and int(item.get("ttm_count") or 0) > 0 for item in items)),
            "latest_score_impact": int(any(item.get("latest_company_state_affected") == "1" and int(item.get("score_count") or 0) > 0 for item in items)),
            "latest_lifecycle_impact": int(any(item.get("latest_company_state_affected") == "1" and int(item.get("lifecycle_count") or 0) > 0 for item in items)),
            "latest_valuation_impact": int(any(item.get("latest_company_state_affected") == "1" and int(item.get("valuation_count") or 0) > 0 for item in items)),
            "expected_downstream_scope_if_corrected": downstream_scope(items),
            "dispositions": "|".join(dispositions),
            "finding_count": len(items),
        }
        units.append(unit)
        for item in items:
            dedup.append({"old_issue_id": item["issue_id"], "evidence_unit_id": unit["evidence_unit_id"], "queue": queue, "disposition": item["disposition"]})
    return units, dedup


def priority_sort(priority: str) -> int:
    return {"P1_CURRENT_MATERIAL": 0, "P2_HISTORICAL_MATERIAL": 1, "P3_RECENT_UNCERTAIN": 2, "P4_LOW_CURRENT_MATERIALITY": 3}.get(priority, 9)


def exact_question(row: dict[str, Any]) -> str:
    if row["issue_type"] == PUBLISH:
        return f"Verify actual result publication date for {row['ticker']} FY{row['fiscal_year']} {row['fiscal_quarter']} period_end {row['period_end']} current={row['publish_date']}."
    return f"Verify reported {row['field_name']} for {row['ticker']} FY{row['fiscal_year']} {row['fiscal_quarter']} period_end {row['period_end']} current={row['stored_value']}."


def downstream_scope(items: list[dict[str, Any]]) -> str:
    ttm = {x for item in items for x in item.get("ttm_ids", "").split("|") if x}
    score = {x for item in items for x in item.get("score_ids", "").split("|") if x}
    lifecycle = {x for item in items for x in item.get("lifecycle_ids", "").split("|") if x}
    valuation = {x for item in items for x in item.get("valuation_ids", "").split("|") if x}
    return f"ttm={len(ttm)};score={len(score)};lifecycle={len(lifecycle)};valuation={len(valuation)}"


def split_queues(units: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    return ([u for u in units if u["queue"] == "A"], [u for u in units if u["queue"] == "B"], [u for u in units if u["queue"] == "C"])


def publish_offset_patterns(annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter = Counter(row["offset_days"] for row in annotated if row["issue_type"] == PUBLISH)
    return [{"offset_days": k, "absolute_offset_days": abs(int(k)), "count": v} for k, v in sorted(counter.items(), key=lambda x: (int(x[0]), x[1]))]


def publish_cluster_summary(annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter = Counter((row.get("root_cause_status") or "UNKNOWN", row["age_bucket"], row["disposition"]) for row in annotated if row["issue_type"] == PUBLISH)
    return [{"root_cause_status": k[0], "age_bucket": k[1], "disposition": k[2], "count": v} for k, v in sorted(counter.items())]


def source_concentration(annotated: list[dict[str, Any]], issue_type: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in annotated:
        if row["issue_type"] == issue_type:
            grouped[row["source_family"]].append(row)
    return [
        {
            "source_family": source,
            "count": len(items),
            "companies": len({i["ticker"] for i in items}),
            "years": "|".join(sorted({i["fiscal_year"] for i in items})),
            "fiscal_quarters": "|".join(sorted({i["fiscal_quarter"] for i in items})),
        }
        for source, items in sorted(grouped.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    ]


def systematic_root_causes(annotated: list[dict[str, Any]], issue_type: str) -> list[dict[str, Any]]:
    rows_out = []
    for row in source_concentration(annotated, issue_type):
        if row["count"] >= 5:
            rows_out.append({**row, "classification": "SYSTEMATIC_ROOT_CAUSE_CANDIDATE", "safe_correct_values_derivable": 0})
    return rows_out


def publish_wrong_quarter_candidates(conn: sqlite3.Connection, annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in annotated:
        if row["issue_type"] != PUBLISH:
            continue
        matches = rows(
            conn,
            """
            SELECT q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date
            FROM v3_quarter q
            WHERE q.company_id=?
              AND q.publish_date=?
              AND NOT (q.fiscal_year=? AND q.fiscal_quarter=? AND q.period_end_date=?)
            ORDER BY q.fiscal_year,q.fiscal_quarter
            """,
            (row["company_id"], row["publish_date"], row["fiscal_year"], row["fiscal_quarter"], row["period_end"]),
        )
        out.append(
            {
                "issue_id": row["issue_id"],
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end": row["period_end"],
                "publish_date": row["publish_date"],
                "same_company_publish_date_match_count": len(matches),
                "candidate_quarters": json.dumps(matches[:5], sort_keys=True, default=str),
                "pattern": "LIKELY_WRONG_FISCAL_ASSOCIATION" if matches else "NO_LOCAL_MATCH",
            }
        )
    return out


def field_distribution(annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter = Counter(row["field_name"] for row in annotated if row["issue_type"] == SEMANTIC)
    fields = ["revenue", "gross_profit", "operating_income", "ebit", "ebitda", "net_income", "ocf", "capex", "fcf", "cash", "total_debt", "shares_outstanding"]
    return [{"field_name": field, "count": counter.get(field, 0)} for field in fields]


def semantic_source_pair_matrix(annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped = Counter()
    for row in annotated:
        if row["issue_type"] == SEMANTIC:
            grouped[(row["field_name"], row.get("source_family", "UNKNOWN"))] += 1
    return [{"field_name": k[0], "source_pair_family": k[1], "count": v} for k, v in sorted(grouped.items(), key=lambda kv: (-kv[1], kv[0]))]


def semantic_conflict_shape_clusters(annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped = Counter()
    for row in annotated:
        if row["issue_type"] != SEMANTIC:
            continue
        value = float(row["stored_value"])
        if row["field_name"] == "shares_outstanding":
            shape = "NON_POSITIVE_SHARES"
        elif value < 0:
            shape = "NEGATIVE_VALUE"
        else:
            shape = "OTHER"
        grouped[(row["field_name"], shape, row["age_bucket"], row["disposition"])] += 1
    return [{"field_name": k[0], "shape": k[1], "age_bucket": k[2], "disposition": k[3], "count": v} for k, v in sorted(grouped.items())]


def semantic_adjacent_context(conn: sqlite3.Connection, annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in annotated:
        if row["issue_type"] != SEMANTIC:
            continue
        field = "shares_outstanding" if row["field_name"] == "shares_outstanding" else row["field_name"]
        context = rows(
            conn,
            f"""
            SELECT q.fiscal_year,q.fiscal_quarter,q.period_end_date,f.{field} AS value
            FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE q.company_id=? AND q.period_end_date BETWEEN date(?,'-370 day') AND date(?,'+370 day')
            ORDER BY q.period_end_date
            """,
            (row["company_id"], row["period_end"], row["period_end"]),
        )
        out.append({"issue_id": row["issue_id"], "ticker": row["ticker"], "field_name": row["field_name"], "context_json": json.dumps(context, sort_keys=True, default=str)})
    return out


def existing_evidence_resolution(annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in annotated:
        if row["issue_type"] != SEMANTIC:
            continue
        status = "LOW_MATERIALITY_ACCEPT" if row["disposition"] == "LOW_MATERIALITY_ACCEPT" else "MANUAL_EVIDENCE_REQUIRED"
        out.append({"issue_id": row["issue_id"], "ticker": row["ticker"], "field_name": row["field_name"], "classification": status, "source_family": row["source_family"]})
    return out


def share_split_classification(conn: sqlite3.Connection, annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in annotated:
        if row["issue_type"] == SEMANTIC and row["field_name"] == "shares_outstanding":
            out.append({"issue_id": row["issue_id"], "ticker": row["ticker"], "classification": "VALID_CORPORATE_ACTION_CANDIDATE_NEEDS_EVIDENCE", "auto_repair": 0})
    return out


def latest_state_impact(annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "issue_id": row["issue_id"],
            "ticker": row["ticker"],
            "issue_type": row["issue_type"],
            "field_name": row["field_name"],
            "latest_state_affected": row["latest_company_state_affected"],
            "queue": row["queue"],
            "ttm_count": row["ttm_count"],
            "score_count": row["score_count"],
            "lifecycle_count": row["lifecycle_count"],
            "valuation_count": row["valuation_count"],
        }
        for row in annotated
        if row["latest_company_state_affected"] == "1"
    ]


def historical_reassessment(annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "issue_id": row["issue_id"],
            "ticker": row["ticker"],
            "age_bucket": row["age_bucket"],
            "priority_before": row["priority"],
            "disposition_after": row["disposition"],
            "reason": "current/systemic materiality retained" if row["disposition"] in {"MANUAL_A", "MANUAL_B"} else "old isolated low current materiality",
        }
        for row in annotated
        if row["age_bucket"] in {"<=2020", "2021-2022"}
    ]


def recent_2026(annotated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "issue_id": row["issue_id"],
            "ticker": row["ticker"],
            "issue_type": row["issue_type"],
            "field_name": row["field_name"],
            "latest_state_affected": row["latest_company_state_affected"],
            "classification": "MANUAL_CHECK_NOW" if row["disposition"] == "MANUAL_A" else "WAIT_FOR_REFRESH",
        }
        for row in annotated
        if row["age_bucket"] == "2026"
    ]


def production_counts(db: Path) -> dict[str, int]:
    with open_ro(db) as conn:
        return {
            "canonical": int(conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0]),
            "ttm": int(conn.execute("SELECT COUNT(*) FROM v3_ttm").fetchone()[0]),
            "valuation": int(conn.execute("SELECT COUNT(*) FROM v3_valuation").fetchone()[0]),
            "score": int(conn.execute("SELECT COUNT(*) FROM v3_score").fetchone()[0]),
            "lifecycle": int(conn.execute("SELECT COUNT(*) FROM v3_lifecycle").fetchone()[0]),
        }


def write_human_summary(path: Path, queue_a: list[dict[str, Any]]) -> None:
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in queue_a:
        by_ticker[row["ticker"]].append(row)
    lines = ["# Queue A - Must Check Now", ""]
    for ticker in sorted(by_ticker):
        lines.append(f"## {ticker}")
        for row in by_ticker[ticker]:
            lines.append(f"- FY{row['fiscal_year']} {row['fiscal_quarter']} period_end {row['period_end']}: {row['affected_fields']} ({row['issue_ids_included']})")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def build_summary(
    started: datetime,
    annotated: list[dict[str, Any]],
    manual_raw: list[dict[str, str]],
    units: list[dict[str, Any]],
    queue_a: list[dict[str, Any]],
    queue_b: list[dict[str, Any]],
    queue_c: list[dict[str, Any]],
    before: dict[str, int],
    after: dict[str, int],
) -> dict[str, Any]:
    dispositions = Counter(row["disposition"] for row in annotated)
    publish = [r for r in annotated if r["issue_type"] == PUBLISH]
    semantic = [r for r in annotated if r["issue_type"] == SEMANTIC]
    queue_a_latest = sum(1 for row in queue_a if row["latest_ttm_impact"] or row["latest_score_impact"] or row["latest_lifecycle_impact"] or row["latest_valuation_impact"])
    classification = CLASSIFICATION_USER_EVIDENCE if queue_a else CLASSIFICATION_READY_FREEZE
    return {
        "classification": classification,
        "next_action": NEXT_ACTION_USER if queue_a else NEXT_ACTION_FREEZE,
        "started_at_utc": started.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "completed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "total_findings": len(annotated),
        "publish_findings": len(publish),
        "semantic_findings": len(semantic),
        "raw_manual_requests": len(manual_raw),
        "unique_evidence_units_before_materiality_reduction": len(units),
        "queue_a_units": len(queue_a),
        "queue_b_units": len(queue_b),
        "queue_c_units": len(queue_c),
        "duplicate_requests_eliminated": len(manual_raw) - (len(queue_a) + len(queue_b)),
        "reduction_percentage": round(100.0 * (len(manual_raw) - len(queue_a)) / len(manual_raw), 2) if manual_raw else 0,
        "dispositions": dict(dispositions),
        "latest_state_findings": sum(1 for row in annotated if row["latest_company_state_affected"] == "1"),
        "latest_state_evidence_units": len([u for u in units if u["latest_ttm_impact"] or u["latest_score_impact"] or u["latest_lifecycle_impact"] or u["latest_valuation_impact"]]),
        "queue_a_latest_state_units": queue_a_latest,
        "production_counts_before": before,
        "production_counts_after": after,
        "production_writes": 0 if before == after else "COUNT_DRIFT",
        "rawcandle_writes": 0,
    }


def write_next_action(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        f"Classification: `{summary['classification']}`\n\nNext action: `{summary['next_action']}`\n",
        encoding="utf-8",
    )
