from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import (
    EXPECTED_LIFECYCLE_FINGERPRINT,
    EXPECTED_LIFECYCLE_MODEL,
    EXPECTED_SCORE_FINGERPRINT,
    EXPECTED_SCORE_MODEL,
    open_ro,
    rows,
    scalar,
    write_csv,
    write_json,
)


CLASSIFICATION_MANUAL = "FUNDAMENTALS_V3_PHASE8_MANUAL_EVIDENCE_REQUIRED"
CLASSIFICATION_NO_REPAIR = "FUNDAMENTALS_V3_PHASE8_UPDATE_COMPLETE_NO_MATERIAL_REPAIR_REQUIRED"
NEXT_PHASE = "MASTER PLAN PHASE 9 - PRODUCTION PROVING"

PUBLISH_ISSUE = "PUBLISH_DATE_ANOMALY"
SEMANTIC_ISSUE = "SEMANTIC_FIELD_OUTLIER"
SEMANTIC_FIELDS = ("revenue", "cash", "total_debt", "shares_outstanding")


@dataclass(frozen=True)
class Phase8Paths:
    phase7_root: Path
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def normalize_float(value: Any) -> str:
    if value is None or value == "":
        return ""
    return str(value)


def sha_rows(data: list[dict[str, Any]]) -> str:
    payload = json.dumps(data, sort_keys=True, default=str, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def age_bucket(period_end: str | None, fiscal_year: int | None = None) -> str:
    year = int((period_end or "")[:4] or fiscal_year or 0)
    if year <= 2020:
        return "<=2020"
    if year in {2021, 2022}:
        return "2021-2022"
    if year == 2023:
        return "2023"
    if year == 2024:
        return "2024"
    if year == 2025:
        return "2025"
    if year >= 2026:
        return "2026"
    return "UNKNOWN"


def priority(age: str, latest_impact: bool, derived_impact: bool) -> str:
    if age == "2026":
        return "P3_RECENT_UNCERTAIN"
    if age in {"2023", "2024", "2025"} and (latest_impact or derived_impact):
        return "P1_CURRENT_MATERIAL"
    if age == "2021-2022" and derived_impact:
        return "P2_HISTORICAL_MATERIAL"
    if age == "<=2020" and latest_impact:
        return "P1_CURRENT_MATERIAL"
    if age == "<=2020" and derived_impact:
        return "P2_HISTORICAL_MATERIAL"
    return "P4_LOW_CURRENT_MATERIALITY"


def run_phase8_diagnosis(paths: Phase8Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    publish_input = read_csv(paths.phase7_root / "canonical_publish_date_anomalies.csv")
    semantic_input = read_csv(paths.phase7_root / "field_semantic_outliers.csv")

    with open_ro(paths.v3_db) as conn:
        preflight = production_preflight(conn, paths.v3_db, paths.rawcandle_db)
        write_json(paths.artifact_root / "production_preflight.json", preflight)

        publish_rows = build_publish_rows(conn, publish_input)
        semantic_rows = build_semantic_rows(conn, semantic_input)
        all_rows = publish_rows + semantic_rows

        write_csv(paths.artifact_root / "phase7_findings_ingested.csv", ingest_rows(publish_input, semantic_input))
        write_csv(paths.artifact_root / "phase8_master_anomaly_table.csv", all_rows)
        write_csv(paths.artifact_root / "anomaly_counts_by_year_quarter.csv", count_by_year_quarter(all_rows))
        impact_rows = impact_projection(all_rows)
        write_csv(paths.artifact_root / "anomaly_downstream_impact.csv", impact_rows)
        write_csv(paths.artifact_root / "anomaly_priority_classification.csv", priority_projection(all_rows))

        publish_diag = [diagnose_publish(row) for row in publish_rows]
        semantic_diag = [diagnose_semantic(row) for row in semantic_rows]
        write_csv(paths.artifact_root / "publish_date_root_cause_analysis.csv", publish_diag)
        write_csv(paths.artifact_root / "publish_date_evidence.csv", evidence_projection(publish_diag))
        write_csv(paths.artifact_root / "publish_date_repair_candidates.csv", [])
        write_csv(paths.artifact_root / "semantic_outlier_by_field.csv", semantic_by_field(semantic_rows))
        write_csv(paths.artifact_root / "semantic_outlier_root_cause_analysis.csv", semantic_diag)
        write_csv(paths.artifact_root / "semantic_outlier_evidence.csv", evidence_projection(semantic_diag))
        write_csv(paths.artifact_root / "semantic_outlier_repair_candidates.csv", [])

        manual = manual_requests(publish_diag, semantic_diag)
        write_csv(paths.artifact_root / "manual_evidence_requests.csv", manual)
        write_manual_summary(paths.artifact_root / "manual_review_summary.md", manual)

        no_repair = no_repair_decisions(publish_diag, semantic_diag)
        write_csv(paths.artifact_root / "phase8_frozen_repair_set.csv", [])
        write_csv(paths.artifact_root / "phase8_no_repair_decisions.csv", no_repair)
        write_json(paths.artifact_root / "phase8_repair_scope_summary.json", repair_scope_summary(all_rows, manual))
        write_apply_placeholders(paths.artifact_root)
        write_proving_placeholders(paths.artifact_root, preflight)
        write_json(paths.artifact_root / "backup_manifest.json", {"backup_created": False, "reason": "Gate 1 stopped before production apply; no backup required."})
        (paths.artifact_root / "rollback_plan.md").write_text("No production writes were performed, so rollback is not required.\n", encoding="utf-8")

    summary = build_summary(started, publish_rows, semantic_rows, manual, no_repair)
    write_csv(paths.artifact_root / "phase8_issue_resolution_register.csv", resolution_register(publish_diag, semantic_diag))
    write_json(paths.artifact_root / "phase8_summary.json", summary)
    write_phase9_handoff(paths.artifact_root / "phase9_proving_handoff.md", summary)
    write_recommended(paths.artifact_root / "recommended_next_step.md", summary)
    return summary


def production_preflight(conn: sqlite3.Connection, v3_db: Path, rawcandle_db: Path) -> dict[str, Any]:
    tables = ["v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_valuation", "v3_score", "v3_lifecycle"]
    return {
        "quick_check": scalar(conn, "PRAGMA quick_check"),
        "v3_db": str(v3_db),
        "rawcandle_db": str(rawcandle_db),
        "v3_db_size": v3_db.stat().st_size,
        "rawcandle_db_size": rawcandle_db.stat().st_size,
        "free_disk_bytes": shutil.disk_usage(v3_db.parent).free,
        "row_counts": {table: scalar(conn, f"SELECT COUNT(*) FROM {table}") for table in tables},
        "score": rows(conn, "SELECT score_model_version,score_fingerprint,COUNT(*) AS rows FROM v3_score GROUP BY score_model_version,score_fingerprint"),
        "lifecycle": rows(conn, "SELECT lifecycle_model_version,lifecycle_fingerprint,COUNT(*) AS rows FROM v3_lifecycle GROUP BY lifecycle_model_version,lifecycle_fingerprint"),
        "canonical_fingerprint": sha_rows(rows(conn, "SELECT quarter_id,company_id,fiscal_year,fiscal_quarter,period_end_date,publish_date FROM v3_quarter ORDER BY quarter_id")),
        "ttm_fingerprint": sha_rows(rows(conn, "SELECT ttm_id,company_id,endpoint_quarter_id,period_end,source_fingerprint FROM v3_ttm ORDER BY ttm_id")),
        "score_fingerprint": sha_rows(rows(conn, "SELECT score_id,company_id,as_of_quarter_id,endpoint_ttm_id,source_fingerprint,score_fingerprint FROM v3_score ORDER BY score_id")),
        "lifecycle_fingerprint": sha_rows(rows(conn, "SELECT lifecycle_id,company_id,endpoint_ttm_id,endpoint_quarter_id,source_fingerprint,lifecycle_fingerprint FROM v3_lifecycle ORDER BY lifecycle_id")),
        "valuation_fingerprint": sha_rows(rows(conn, "SELECT valuation_id,company_id,endpoint_ttm_id,endpoint_quarter_id,valuation_date,source_fingerprint FROM v3_valuation ORDER BY valuation_id")),
    }


def ingest_rows(publish_input: list[dict[str, str]], semantic_input: list[dict[str, str]]) -> list[dict[str, Any]]:
    return [
        {"phase7_issue_type": PUBLISH_ISSUE, "phase7_rows": len(publish_input), "expected_rows": 111, "match": len(publish_input) == 111},
        {"phase7_issue_type": SEMANTIC_ISSUE, "phase7_rows": len(semantic_input), "expected_rows": 237, "match": len(semantic_input) == 237},
    ]


def base_quarter(conn: sqlite3.Connection, quarter_id: int) -> dict[str, Any]:
    row = rows(
        conn,
        """
        SELECT q.quarter_id,q.company_id,c.market,c.ticker,c.company_name,c.profile,c.active,
               q.fiscal_year,q.fiscal_quarter,q.period_end_date AS period_end,q.publish_date,
               f.accepted_source_provider,f.derivation_method,f.currency,
               f.revenue,f.cash,f.total_debt,f.shares_outstanding
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE q.quarter_id=?
        """,
        (quarter_id,),
    )
    if len(row) != 1:
        raise ValueError(f"ambiguous or missing quarter_id={quarter_id}")
    return row[0]


def downstream(conn: sqlite3.Connection, quarter_id: int, company_id: int) -> dict[str, Any]:
    ttm = rows(
        conn,
        """
        SELECT ttm_id,period_end FROM v3_ttm
        WHERE q1_quarter_id=? OR q2_quarter_id=? OR q3_quarter_id=? OR q4_quarter_id=? OR endpoint_quarter_id=?
        ORDER BY period_end,ttm_id
        """,
        (quarter_id, quarter_id, quarter_id, quarter_id, quarter_id),
    )
    ttm_ids = [str(r["ttm_id"]) for r in ttm]
    score_ids = [str(r["score_id"]) for r in rows(conn, f"SELECT score_id FROM v3_score WHERE endpoint_ttm_id IN ({','.join(ttm_ids) or 'NULL'}) ORDER BY score_id")]
    lifecycle_ids = [str(r["lifecycle_id"]) for r in rows(conn, f"SELECT lifecycle_id FROM v3_lifecycle WHERE endpoint_ttm_id IN ({','.join(ttm_ids) or 'NULL'}) ORDER BY lifecycle_id")]
    valuation_ids = [str(r["valuation_id"]) for r in rows(conn, f"SELECT valuation_id FROM v3_valuation WHERE endpoint_ttm_id IN ({','.join(ttm_ids) or 'NULL'}) ORDER BY valuation_id")]
    latest_ttm = scalar(conn, "SELECT MAX(period_end) FROM v3_ttm WHERE company_id=?", (company_id,))
    latest_hit = any(r["period_end"] == latest_ttm for r in ttm)
    first_lifecycle = rows(conn, f"SELECT lifecycle_id,endpoint_period_end FROM v3_lifecycle WHERE company_id=? AND endpoint_ttm_id IN ({','.join(ttm_ids) or 'NULL'}) ORDER BY endpoint_period_end LIMIT 1", (company_id,))
    return {
        "ttm_ids": "|".join(ttm_ids),
        "ttm_count": len(ttm_ids),
        "score_ids": "|".join(score_ids),
        "score_count": len(score_ids),
        "lifecycle_ids": "|".join(lifecycle_ids),
        "lifecycle_count": len(lifecycle_ids),
        "valuation_ids": "|".join(valuation_ids),
        "valuation_count": len(valuation_ids),
        "latest_company_state_affected": int(latest_hit),
        "earliest_lifecycle_endpoint": first_lifecycle[0]["endpoint_period_end"] if first_lifecycle else "",
    }


def latest_audit_evidence(conn: sqlite3.Connection, quarter_id: int, field: str | None) -> str:
    sql = """
        SELECT source,audit_type,decision,evidence_json,created_at_utc
        FROM v3_migration_audit
        WHERE quarter_id=? AND (evidence_json LIKE ? OR ? IS NULL)
        ORDER BY created_at_utc DESC
        LIMIT 1
    """
    evidence = rows(conn, sql, (quarter_id, f'%"{field}"%' if field else "%", field))
    if evidence:
        return json.dumps(evidence[0], sort_keys=True, default=str)
    return ""


def build_publish_rows(conn: sqlite3.Connection, publish_input: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for idx, row in enumerate(publish_input, 1):
        q = base_quarter(conn, int(row["quarter_id"]))
        impact = downstream(conn, q["quarter_id"], q["company_id"])
        age = age_bucket(q["period_end"], q["fiscal_year"])
        prio = priority(age, bool(impact["latest_company_state_affected"]), impact["ttm_count"] > 0)
        out.append(
            {
                "issue_id": f"P8-PUB-{idx:03d}",
                "issue_type": PUBLISH_ISSUE,
                "company_id": q["company_id"],
                "ticker": q["ticker"],
                "company_name": q["company_name"],
                "market": q["market"],
                "active": q["active"],
                "fiscal_year": q["fiscal_year"],
                "fiscal_quarter": q["fiscal_quarter"],
                "period_end": q["period_end"],
                "publish_date": q["publish_date"],
                "field_name": "publish_date",
                "stored_value": q["publish_date"],
                "stored_unit": "date",
                "source_provenance": latest_audit_evidence(conn, q["quarter_id"], "publish_date"),
                "phase7_severity": "HIGH",
                "phase7_reason": "publish date before period end, future-dated, or market date before publish date",
                "age_bucket": age,
                "priority": prio,
                "root_cause_status": "DIAGNOSED_LOCAL_CONFLICT",
                "evidence_status": "LOCAL_CONFLICT_ONLY",
                "repair_recommendation": "MANUAL_REVIEW_REQUIRED" if prio != "P4_LOW_CURRENT_MATERIALITY" else "NO_REPAIR_LOW_MATERIALITY",
                "manual_review_required": int(prio != "P4_LOW_CURRENT_MATERIALITY"),
                **impact,
            }
        )
    return out


def build_semantic_rows(conn: sqlite3.Connection, semantic_input: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    by_key = {(r["ticker"], r["fiscal_year"], r["fiscal_quarter"], r["period_end_date"]): r for r in semantic_input}
    for ticker, fy, fq, period_end in sorted(by_key):
        q_rows = rows(
            conn,
            """
            SELECT q.quarter_id
            FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=? AND q.period_end_date=?
            """,
            (ticker, int(fy), fq, period_end),
        )
        if len(q_rows) != 1:
            raise ValueError(f"ambiguous semantic finding {ticker} {fy} {fq} {period_end}")
        q = base_quarter(conn, int(q_rows[0]["quarter_id"]))
        impact = downstream(conn, q["quarter_id"], q["company_id"])
        age = age_bucket(q["period_end"], q["fiscal_year"])
        prio = priority(age, bool(impact["latest_company_state_affected"]), impact["ttm_count"] > 0)
        for field in SEMANTIC_FIELDS:
            value = q.get(field)
            if (field in {"revenue", "cash", "total_debt"} and value is not None and float(value) < 0) or (
                field == "shares_outstanding" and value is not None and float(value) <= 0
            ):
                idx = len(out) + 1
                out.append(
                    {
                        "issue_id": f"P8-SEM-{idx:03d}",
                        "issue_type": SEMANTIC_ISSUE,
                        "company_id": q["company_id"],
                        "ticker": q["ticker"],
                        "company_name": q["company_name"],
                        "market": q["market"],
                        "active": q["active"],
                        "fiscal_year": q["fiscal_year"],
                        "fiscal_quarter": q["fiscal_quarter"],
                        "period_end": q["period_end"],
                        "publish_date": q["publish_date"],
                        "field_name": field,
                        "stored_value": normalize_float(value),
                        "stored_unit": "reported_currency_or_shares",
                        "source_provenance": latest_audit_evidence(conn, q["quarter_id"], field),
                        "phase7_severity": "MEDIUM",
                        "phase7_reason": "negative revenue/cash/debt or non-positive shares",
                        "age_bucket": age,
                        "priority": prio,
                        "root_cause_status": "OUTLIER_NOT_PROVEN_ERROR",
                        "evidence_status": "LOCAL_PLAUSIBILITY_ONLY",
                        "repair_recommendation": "MANUAL_REVIEW_REQUIRED" if prio in {"P1_CURRENT_MATERIAL", "P2_HISTORICAL_MATERIAL", "P3_RECENT_UNCERTAIN"} else "NO_REPAIR_LOW_MATERIALITY",
                        "manual_review_required": int(prio in {"P1_CURRENT_MATERIAL", "P2_HISTORICAL_MATERIAL", "P3_RECENT_UNCERTAIN"}),
                        **impact,
                    }
                )
    return out


def diagnose_publish(row: dict[str, Any]) -> dict[str, Any]:
    if row["age_bucket"] == "2026":
        root = "RECENT_UNCERTAIN"
    elif row["publish_date"] and row["period_end"] and row["publish_date"] < row["period_end"]:
        root = "PUBLISH_BEFORE_PERIOD_END"
    else:
        root = "UNKNOWN"
    decision = row["repair_recommendation"]
    return {**row, "root_cause": root, "evidence_quality": row["evidence_status"], "decision": decision, "candidate_value": ""}


def diagnose_semantic(row: dict[str, Any]) -> dict[str, Any]:
    name = f"{row['company_name']} {row.get('profile','')}".upper()
    field = row["field_name"]
    if row["age_bucket"] == "2026":
        root = "RECENT_UNCERTAIN"
    elif field == "shares_outstanding":
        root = "SPLIT_OR_SHARE_CLASS_ANOMALY"
    elif field in {"cash", "total_debt"}:
        root = "SIGN_OR_CONTEXT_CONFLICT"
    elif any(term in name for term in ["REIT", "REALTY", "PROPERTIES", "MORTGAGE", "CAPITAL", "CREDIT", "INVESTMENT", "LENDING", "FINANCE"]):
        root = "TRUE_EXTREME_VALID_CANDIDATE"
    else:
        root = "SOURCE_CONFLICT"
    return {**row, "root_cause": root, "evidence_quality": row["evidence_status"], "decision": row["repair_recommendation"], "candidate_value": ""}


def count_by_year_quarter(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[tuple[Any, ...]] = Counter()
    for row in data:
        counter[(row["issue_type"], row["age_bucket"], row["fiscal_year"], row["fiscal_quarter"], row["period_end"][:4], row["active"], row["ticker"], row["field_name"])] += 1
    return [
        {
            "issue_type": k[0],
            "age_bucket": k[1],
            "fiscal_year": k[2],
            "fiscal_quarter": k[3],
            "period_end_year": k[4],
            "active": k[5],
            "ticker": k[6],
            "field_name": k[7],
            "count": v,
        }
        for k, v in sorted(counter.items())
    ]


def impact_projection(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ["issue_id", "issue_type", "ticker", "fiscal_year", "fiscal_quarter", "period_end", "field_name", "ttm_count", "score_count", "lifecycle_count", "valuation_count", "latest_company_state_affected", "earliest_lifecycle_endpoint"]
    return [{key: row[key] for key in keys} for row in data]


def priority_projection(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ["issue_id", "issue_type", "ticker", "fiscal_year", "fiscal_quarter", "period_end", "field_name", "age_bucket", "priority", "repair_recommendation", "manual_review_required"]
    return [{key: row[key] for key in keys} for row in data]


def evidence_projection(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ["issue_id", "ticker", "fiscal_year", "fiscal_quarter", "period_end", "field_name", "stored_value", "source_provenance", "root_cause", "evidence_quality", "candidate_value"]
    return [{key: row.get(key, "") for key in keys} for row in data]


def semantic_by_field(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter = Counter(row["field_name"] for row in data)
    return [{"field_name": key, "count": value} for key, value in sorted(counter.items())]


def manual_requests(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in [item for group in groups for item in group if item["manual_review_required"]]:
        out.append(
            {
                "request_id": f"MANUAL-{len(out)+1:03d}",
                "priority": row["priority"],
                "ticker": row["ticker"],
                "company": row["company_name"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end": row["period_end"],
                "issue_type": row["issue_type"],
                "field": row["field_name"],
                "current_value": row["stored_value"],
                "candidate_value": row.get("candidate_value", ""),
                "current_source": row["source_provenance"],
                "requested_source": requested_source(row),
                "exact_question": exact_question(row),
                "downstream_impact": f"ttm={row['ttm_count']};score={row['score_count']};lifecycle={row['lifecycle_count']};valuation={row['valuation_count']};latest={row['latest_company_state_affected']}",
                "why_manual_needed": "Local V3 evidence proves anomaly shape but does not identify an authoritative replacement value.",
            }
        )
    return out


def requested_source(row: dict[str, Any]) -> str:
    if row["issue_type"] == PUBLISH_ISSUE:
        return "issuer IR earnings release/archive or SEC filing metadata if issuer source is unavailable"
    return "issuer quarterly/annual filing statement line item, split/corporate-action evidence if shares"


def exact_question(row: dict[str, Any]) -> str:
    if row["issue_type"] == PUBLISH_ISSUE:
        return (
            f"{row['ticker']} FY{row['fiscal_year']} {row['fiscal_quarter']} period_end {row['period_end']}: "
            f"verify actual result publication date; current V3 publish_date={row['publish_date']}."
        )
    return (
        f"{row['ticker']} FY{row['fiscal_year']} {row['fiscal_quarter']} period_end {row['period_end']}: "
        f"verify reported {row['field_name']}; current V3 value={row['stored_value']}."
    )


def write_manual_summary(path: Path, manual: list[dict[str, Any]]) -> None:
    priorities = Counter(row["priority"] for row in manual)
    path.write_text(
        "\n".join(
            [
                "# Phase 8 Manual Evidence Queue",
                "",
                f"Manual requests: `{len(manual)}`",
                "",
                *(f"- `{key}`: `{value}`" for key, value in sorted(priorities.items())),
                "",
                "Production apply is blocked while P1/P2 manual cases remain unresolved.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def no_repair_decisions(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "issue_id": row["issue_id"],
            "decision": row["repair_recommendation"],
            "reason": row["priority"],
            "ticker": row["ticker"],
            "fiscal_year": row["fiscal_year"],
            "fiscal_quarter": row["fiscal_quarter"],
            "field_name": row["field_name"],
        }
        for row in [item for group in groups for item in group if item["repair_recommendation"] != "MANUAL_REVIEW_REQUIRED"]
    ]


def repair_scope_summary(data: list[dict[str, Any]], manual: list[dict[str, Any]]) -> dict[str, Any]:
    affected_ttm = set()
    affected_score = set()
    affected_lifecycle_companies = set()
    affected_valuation = set()
    for row in data:
        affected_ttm.update(x for x in row["ttm_ids"].split("|") if x)
        affected_score.update(x for x in row["score_ids"].split("|") if x)
        affected_valuation.update(x for x in row["valuation_ids"].split("|") if x)
        if row["lifecycle_count"]:
            affected_lifecycle_companies.add(str(row["company_id"]))
    return {
        "gate": "GATE_1_DIAGNOSIS_REPAIR_SET_FREEZE",
        "confirmed_canonical_repairs": 0,
        "confirmed_publish_date_repairs": 0,
        "expected_canonical_update_rows": 0,
        "affected_ttm_rows_if_all_manual_confirmed": len(affected_ttm),
        "affected_score_rows_if_all_manual_confirmed": len(affected_score),
        "affected_lifecycle_companies_if_all_manual_confirmed": len(affected_lifecycle_companies),
        "affected_valuation_rows_if_all_manual_confirmed": len(affected_valuation),
        "manual_requests": len(manual),
        "apply_blocked": any(row["priority"] in {"P1_CURRENT_MATERIAL", "P2_HISTORICAL_MATERIAL"} for row in manual),
    }


def write_apply_placeholders(root: Path) -> None:
    empty_files = [
        "canonical_apply_audit.csv",
        "publish_date_apply_audit.csv",
        "ttm_recompute_summary.csv",
        "score_recompute_summary.csv",
        "lifecycle_recompute_summary.csv",
        "valuation_recompute_summary.csv",
        "phase7_reaudit_comparison.csv",
    ]
    for name in empty_files:
        write_csv(root / name, [])


def write_proving_placeholders(root: Path, preflight: dict[str, Any]) -> None:
    write_json(root / "unrelated_row_drift_proof.json", {"production_apply_performed": False, "unrelated_row_drift": 0})
    write_json(root / "phase8_idempotency_proof.json", {"production_apply_performed": False, "second_apply_changes": 0})
    write_json(
        root / "phase8_model_fingerprint_proof.json",
        {
            "score_expected": EXPECTED_SCORE_FINGERPRINT,
            "score_model": EXPECTED_SCORE_MODEL,
            "score_verified": preflight["score"],
            "lifecycle_expected": EXPECTED_LIFECYCLE_FINGERPRINT,
            "lifecycle_model": EXPECTED_LIFECYCLE_MODEL,
            "lifecycle_verified": preflight["lifecycle"],
        },
    )


def resolution_register(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ["issue_id", "issue_type", "ticker", "fiscal_year", "fiscal_quarter", "period_end", "field_name", "stored_value", "priority", "root_cause", "decision"]
    return [{key: row.get(key, "") for key in keys} for row in [item for group in groups for item in group]]


def build_summary(
    started: datetime,
    publish_rows: list[dict[str, Any]],
    semantic_rows: list[dict[str, Any]],
    manual: list[dict[str, Any]],
    no_repair: list[dict[str, Any]],
) -> dict[str, Any]:
    all_rows = publish_rows + semantic_rows
    priority_counts = Counter(row["priority"] for row in all_rows)
    age_counts: dict[str, dict[str, int]] = defaultdict(dict)
    for issue_type in {PUBLISH_ISSUE, SEMANTIC_ISSUE}:
        counter = Counter(row["age_bucket"] for row in all_rows if row["issue_type"] == issue_type)
        age_counts[issue_type] = {bucket: counter.get(bucket, 0) for bucket in ["<=2020", "2021-2022", "2023", "2024", "2025", "2026"]}
    manual_priorities = Counter(row["priority"] for row in manual)
    return {
        "classification": CLASSIFICATION_MANUAL if manual_priorities.get("P1_CURRENT_MATERIAL", 0) or manual_priorities.get("P2_HISTORICAL_MATERIAL", 0) else CLASSIFICATION_NO_REPAIR,
        "next_phase": NEXT_PHASE,
        "started_at_utc": started.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "completed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "phase7_publish_date_anomalies_ingested": len(publish_rows),
        "phase7_semantic_outliers_ingested": len(semantic_rows),
        "age_profile": age_counts,
        "priority_counts": dict(priority_counts),
        "manual_requests": len(manual),
        "manual_priority_counts": dict(manual_priorities),
        "no_repair_decisions": len(no_repair),
        "confirmed_repairs": 0,
        "production_apply_performed": False,
    }


def write_phase9_handoff(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        f"Phase 9 is not ready until Phase 8 manual evidence is resolved. Current classification: `{summary['classification']}`.\n",
        encoding="utf-8",
    )


def write_recommended(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        "\n".join(
            [
                "# Recommended Next Step",
                "",
                f"Classification: `{summary['classification']}`",
                "",
                "Resolve the manual evidence queue before any production write.",
                "",
            ]
        ),
        encoding="utf-8",
    )
