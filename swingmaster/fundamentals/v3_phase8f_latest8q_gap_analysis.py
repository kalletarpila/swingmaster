from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from swingmaster.fundamentals.v3_fiscal_calendar import (
    EXPECTED_P1_TICKERS,
    downstream_fingerprint,
    semantic_fingerprints,
    utc_stamp,
)
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8d7_historical_anchor_reanalysis import (
    build_exact_interval_map,
    build_ttm_risk,
    classify_row,
    load_anchors,
    load_chains,
    load_profiles,
    resolve_extra_week,
)
from swingmaster.fundamentals.v3_phase8e_apply import active_tickers, latest_table_by_ticker
from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import semantic_rows_fingerprint


CLASSIFICATION_COMPLETE = "LATEST8Q_FULL_CLOSURE_MAP_COMPLETE"
CLASSIFICATION_STRUCTURAL = "LATEST8Q_FULL_CLOSURE_MAP_COMPLETE_WITH_STRUCTURAL_DECISIONS"
CLASSIFICATION_INCOMPLETE = "LATEST8Q_FULL_CLOSURE_MAP_INCOMPLETE"
NEXT_ACTION_COMPLETE = (
    "USE THE TICKER-LEVEL CLOSURE MAP TO RESOLVE ALL LOCAL-ONLY CASES FIRST, THEN RUN THE EXTERNAL "
    "OFFICIAL-RESEARCH QUEUE, THEN REHEARSE THE RESULTING COMPLETE LATEST8Q REPAIR SET BEFORE PRODUCTION APPLY"
)
NEXT_ACTION_STRUCTURAL = (
    "RESOLVE LOCAL-ONLY CASES AND EXTERNAL EVIDENCE FIRST; KEEP ONLY TRUE TRANSITION / COLLISION / "
    "STRUCTURAL CASES FOR MANUAL DECISION"
)
NEXT_ACTION_INCOMPLETE = "DO NOT START REPAIR; EXPAND THE DIAGNOSTIC UNTIL EVERY NON-CLEAN TICKER HAS A COMPLETE CLOSURE PATH"

PRIMARY_FIELDS = {
    "Revenue": "revenue",
    "EBIT": "ebit",
    "FCF": "free_cashflow",
    "Cash": "cash",
    "Debt": "total_debt",
    "Shares": "shares_outstanding",
}
SECONDARY_FIELDS = {
    "Gross Profit": "gross_profit",
    "Operating Income": "operating_income",
    "EBITDA": "ebitda",
    "Net Income": "net_income",
    "OCF": "operating_cashflow",
    "Capex": "capex",
}
ALL_FIELDS = PRIMARY_FIELDS | SECONDARY_FIELDS
Q_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
KNOWN_13 = tuple(EXPECTED_P1_TICKERS)


@dataclass(frozen=True)
class Phase8FPaths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")
    phase8e_root: Path = Path("temp/fundamentals_v3_phase8e_apply/20260829T_PHASE8E_APPLY")
    write_documentation: bool = True


def pct(part: int, whole: int) -> float:
    return round(part * 100 / whole, 4) if whole else 0.0


def connect_ro(db: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def production_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "companies": table_count(conn, "v3_company"),
        "active_companies": int(conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=1").fetchone()[0]),
        "canonical_quarter_rows": table_count(conn, "v3_quarter"),
        "fundamentals_rows": table_count(conn, "v3_quarter_fundamentals"),
        "fiscal_profile_rows": table_count(conn, "v3_company_fiscal_calendar_profile"),
        "fiscal_year_calendar_rows": table_count(conn, "v3_company_fiscal_year_calendar"),
        "fiscal_anchor_chain_rows": table_count(conn, "v3_company_fiscal_anchor_chain"),
        "ttm_rows": table_count(conn, "v3_ttm"),
        "score_rows": table_count(conn, "v3_score"),
        "lifecycle_rows": table_count(conn, "v3_lifecycle"),
        "valuation_rows": table_count(conn, "v3_valuation"),
    }


def production_fingerprints(db: Path) -> dict[str, Any]:
    fps = semantic_fingerprints(db)
    fps["fiscal_metadata"] = {
        "profile": semantic_rows_fingerprint(db, "v3_company_fiscal_calendar_profile"),
        "year_calendar": semantic_rows_fingerprint(db, "v3_company_fiscal_year_calendar"),
        "anchor_chain": semantic_rows_fingerprint(db, "v3_company_fiscal_anchor_chain"),
    }
    fps["derived"] = {
        table: downstream_fingerprint(db, table)
        for table in ("v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")
    }
    return fps


def canonical_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.company_id,c.market,c.ticker,c.company_name,c.active,c.admission_source,
               q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date AS period_end,
               q.publish_date,q.market_availability_date,q.q_lifecycle,q.sec_confirmation_state,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.currency,f.accepted_source_provider,f.derivation_method,f.resolution_issue_id
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        ORDER BY c.ticker,q.period_end_date,q.fiscal_year,
                 CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
        """,
    )


def load_provider_summary(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = defaultdict(lambda: {"providers": [], "acquired": [], "partial": [], "usable_field_count": 0})
    for row in rows(conn, "SELECT quarter_id,provider,acquisition_result,usable_field_count,provider_cache_ref FROM v3_provider_q_acquisition"):
        qid = int(row["quarter_id"])
        out[qid]["providers"].append(row["provider"])
        if row["acquisition_result"] == "ACQUIRED":
            out[qid]["acquired"].append(row["provider"])
        if row["acquisition_result"] == "PARTIAL":
            out[qid]["partial"].append(row["provider"])
        out[qid]["usable_field_count"] = max(int(out[qid]["usable_field_count"]), int(row.get("usable_field_count") or 0))
    return out


def load_issue_summary(conn: sqlite3.Connection) -> dict[int, list[dict[str, Any]]]:
    out: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows(conn, "SELECT quarter_id,issue_type,field_name,status,resolution FROM v3_resolution_issue WHERE quarter_id IS NOT NULL"):
        out[int(row["quarter_id"])].append(row)
    return out


def load_migration_summary(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = defaultdict(lambda: {"sources": [], "decisions": [], "audit_types": []})
    for row in rows(conn, "SELECT quarter_id,source,decision,audit_type FROM v3_migration_audit WHERE quarter_id IS NOT NULL"):
        qid = int(row["quarter_id"])
        out[qid]["sources"].append(row["source"])
        out[qid]["decisions"].append(row["decision"])
        out[qid]["audit_types"].append(row["audit_type"])
    return out


def latest_position_map(canonical: list[dict[str, Any]]) -> dict[int, int]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in canonical:
        if int(row["active"]) == 1:
            grouped[int(row["company_id"])].append(row)
    out: dict[int, int] = {}
    for group in grouped.values():
        ordered = sorted(group, key=quarter_sort_key, reverse=True)
        for idx, row in enumerate(ordered[:8], start=1):
            out[int(row["quarter_id"])] = idx
    return out


def quarter_sort_key(row: dict[str, Any]) -> tuple[str, int, int, int]:
    return (
        str(row.get("period_end") or ""),
        int(row.get("fiscal_year") or 0),
        Q_ORDER.get(str(row.get("fiscal_quarter")), 0),
        int(row.get("quarter_id") or 0),
    )


def classified_rows(conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    profiles = load_profiles(conn)
    chains = load_chains(conn)
    anchors = load_anchors(conn)
    canonical = canonical_rows(conn)
    ticker_by_company = {int(row["company_id"]): row["ticker"] for row in rows(conn, "SELECT company_id,ticker FROM v3_company")}
    intervals = build_exact_interval_map(anchors, profiles, chains, ticker_by_company)
    intervals_by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for interval in intervals:
        intervals_by_company[int(interval["company_id"])].append(interval)
    placements = resolve_extra_week(canonical, profiles, anchors)
    by_company_fyq = {(int(r["company_id"]), int(r["fiscal_year"]), str(r["fiscal_quarter"])): r for r in canonical}
    reclass = [classify_row(row, intervals_by_company, profiles, chains, anchors, placements, by_company_fyq) for row in canonical]
    provider = load_provider_summary(conn)
    issues = load_issue_summary(conn)
    migration = load_migration_summary(conn)
    positions = latest_position_map(canonical)
    out = []
    for row in reclass:
        if int(row["quarter_id"]) not in positions:
            continue
        qid = int(row["quarter_id"])
        profile = profiles.get(int(row["company_id"]), {})
        chain = chains.get(int(row["company_id"]), {})
        src = provider.get(qid, {})
        mig = migration.get(qid, {})
        row.update(
            {
                "quarter_position_latest8q": positions[qid],
                "calendar_type": profile.get("calendar_type", ""),
                "fiscal_anchor_evidence": chain.get("source_type", ""),
                "source_winner": row.get("accepted_source_provider") or "",
                "provenance": "|".join(sorted(set(src.get("providers", [])))) or "|".join(sorted(set(mig.get("sources", [])))),
                "lineage": "|".join(sorted(set(mig.get("audit_types", [])))),
                "resolution_issue_count": len(issues.get(qid, [])),
                "resolution_issue_types": "|".join(sorted({str(x.get("issue_type") or "") for x in issues.get(qid, []) if x.get("issue_type")})),
                "local_source_hint": local_source_hint(row, src, mig, issues.get(qid, [])),
            }
        )
        out.append(row)
    return sorted(out, key=lambda r: (r["ticker"], int(r["quarter_position_latest8q"]))), build_ttm_risk(conn, {int(r["quarter_id"]): r for r in reclass})


def local_source_hint(row: dict[str, Any], provider: dict[str, Any], migration: dict[str, Any], issues_: list[dict[str, Any]]) -> str:
    hints = set()
    for name in provider.get("acquired", []):
        hints.add(f"v3_provider_q_acquisition:{name}")
    for name in provider.get("partial", []):
        hints.add(f"v3_provider_q_acquisition:{name}:PARTIAL")
    for name in migration.get("sources", []):
        hints.add(f"v3_migration_audit:{name}")
    if issues_:
        hints.add("v3_resolution_issue")
    if row.get("identity_basis") == "DIRECT_EXACT_INTERVAL":
        hints.add("v3_company_fiscal_year_calendar")
    return "|".join(sorted(hints)) or "NONE_OBVIOUS"


def fiscal_identity_status(row: dict[str, Any]) -> str:
    klass = row.get("identity_class")
    if klass == "PASS_DIRECT_EXACT":
        return "FISCAL_IDENTITY_CLEAN_DIRECT_EXACT"
    if klass in {"PASS_INFERRED", "WARNING"}:
        return "FISCAL_IDENTITY_CLEAN_SUPPORTED"
    if klass == "BLOCK_EXACT_FY_CONFLICT":
        return "FY_CONFLICT_DIRECT_EXACT"
    if klass == "BLOCK_EXACT_FQ_CONFLICT":
        return "FQ_CONFLICT_DIRECT_EXACT"
    if klass == "REVIEW_TRANSITION":
        return "TRANSITION_REVIEW"
    if klass == "REVIEW_UNRESOLVED_BOUNDARY":
        return "UNRESOLVED_BOUNDARY"
    return "INSUFFICIENT_FISCAL_EVIDENCE"


def period_end_status(row: dict[str, Any]) -> str:
    if not row.get("period_end"):
        return "PERIOD_END_MISSING"
    if row.get("identity_class") == "REVIEW_TRANSITION":
        return "PERIOD_END_TRANSITION_REVIEW"
    if row.get("identity_class") in {"PASS_INFERRED", "WARNING"}:
        return "PERIOD_END_CLEAN"
    fit = row.get("period_end_structural_fit")
    if fit == "STRUCTURAL_REVIEW":
        return "PERIOD_END_FISCAL_SLOT_CONFLICT"
    if fit == "TRANSITION_OR_NONSTANDARD_REVIEW":
        return "PERIOD_END_SEQUENCE_CONFLICT"
    return "PERIOD_END_CLEAN"


def publish_date_status(row: dict[str, Any], publish_sequence_issue: bool = False) -> str:
    period = parse_date(row.get("period_end"))
    publish = parse_date(row.get("publish_date"))
    if not publish:
        return "PUBLISH_DATE_MISSING"
    if period and publish <= period:
        return "PUBLISH_BEFORE_OR_ON_PERIOD_END"
    if publish_sequence_issue:
        return "PUBLISH_SEQUENCE_CONFLICT"
    if period and (publish - period).days > 240:
        return "PUBLISH_LATE_REVIEW"
    return "PUBLISH_DATE_CLEAN"


def expected_previous(fy: int, fq: str) -> tuple[int, str]:
    idx = Q_ORDER.get(fq, 0)
    if idx <= 1:
        return fy - 1, "Q4"
    return fy, f"Q{idx - 1}"


def sequence_statuses(rows_: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows_:
        by_ticker[str(row["ticker"])].append(row)
    out: dict[int, dict[str, Any]] = {}
    for ticker, group in by_ticker.items():
        desc = sorted(group, key=lambda r: int(r["quarter_position_latest8q"]))
        fiscal_pairs = Counter((int(r["fiscal_year"]), str(r["fiscal_quarter"])) for r in desc)
        periods = Counter(str(r.get("period_end") or "") for r in desc if r.get("period_end"))
        bad_publish = publish_sequence_qids(desc)
        bad_seq: set[int] = set()
        seq_notes: dict[int, list[str]] = defaultdict(list)
        for left, right in zip(desc, desc[1:]):
            if left.get("identity_class") in {"BLOCK_EXACT_FY_CONFLICT", "BLOCK_EXACT_FQ_CONFLICT"} or right.get("identity_class") in {"BLOCK_EXACT_FY_CONFLICT", "BLOCK_EXACT_FQ_CONFLICT"}:
                continue
            left_pair = effective_issuer_pair(left)
            right_pair = effective_issuer_pair(right)
            if left_pair is None or right_pair is None:
                continue
            expected = expected_previous(left_pair[0], left_pair[1])
            if expected != right_pair:
                bad_seq.update({int(left["quarter_id"]), int(right["quarter_id"])})
                seq_notes[int(left["quarter_id"])].append(f"expected previous {expected[0]}{expected[1]} got {right_pair[0]}{right_pair[1]}")
                seq_notes[int(right["quarter_id"])].append(f"follows effective {left_pair[0]}{left_pair[1]} but expected {expected[0]}{expected[1]}")
        for row in desc:
            qid = int(row["quarter_id"])
            if row.get("identity_class") == "REVIEW_TRANSITION":
                status = "TRANSITION_SEQUENCE"
            elif fiscal_pairs[(int(row["fiscal_year"]), str(row["fiscal_quarter"]))] > 1 or periods[str(row.get("period_end") or "")] > 1:
                status = "DUPLICATE_ECONOMIC_QUARTER"
            elif qid in bad_seq:
                status = "MISSING_QUARTER"
            elif row.get("identity_class") in {"REVIEW_UNRESOLVED_BOUNDARY", "INSUFFICIENT_HISTORY"}:
                status = "UNRESOLVED_SEQUENCE"
            else:
                status = "SEQUENCE_CLEAN"
            out[qid] = {
                "sequence_status": status,
                "publish_sequence_issue": int(qid in bad_publish),
                "sequence_note": "|".join(seq_notes.get(qid, [])),
            }
    return out


def effective_issuer_pair(row: dict[str, Any]) -> tuple[int, str] | None:
    if row.get("identity_class") in {"REVIEW_TRANSITION", "REVIEW_UNRESOLVED_BOUNDARY", "INSUFFICIENT_HISTORY"}:
        return None
    if row.get("exact_fy") not in ("", None) and row.get("exact_fq") not in ("", None):
        return int(row["exact_fy"]), str(row["exact_fq"])
    return int(row["fiscal_year"]), str(row["fiscal_quarter"])


def publish_sequence_qids(desc_rows: list[dict[str, Any]]) -> set[int]:
    asc = sorted(desc_rows, key=quarter_sort_key)
    out: set[int] = set()
    prev: date | None = None
    prev_qid: int | None = None
    for row in asc:
        current = parse_date(row.get("publish_date"))
        if current and prev and current < prev:
            out.add(int(row["quarter_id"]))
            if prev_qid:
                out.add(prev_qid)
        if current:
            prev = current
            prev_qid = int(row["quarter_id"])
    return out


def field_state(row: dict[str, Any], label: str, column: str) -> tuple[str, list[str], list[str]]:
    if row.get(column) is not None:
        return "PRESENT", [], []
    if label == "FCF":
        missing = [name for name, col in (("OCF", "operating_cashflow"), ("Capex", "capex")) if row.get(col) is None]
        if not missing:
            return "DERIVABLE_EXISTING_APPROVED_RULE", ["FCF_DERIVABLE_FROM_OCF_PLUS_CAPEX"], []
        return "DERIVATION_EVIDENCE_MISSING", [], [f"Need {' and '.join(missing)} to derive FCF"]
    if label == "EBIT" and row.get("operating_income") is not None:
        return "DERIVATION_EVIDENCE_MISSING", [], ["Need approved issuer/company-specific EBIT rule; do not blanket map Operating Income"]
    return "MISSING", [], []


def analyze_quarter(row: dict[str, Any], sequence: dict[str, Any], downstream: dict[str, Any]) -> dict[str, Any]:
    fiscal_status = fiscal_identity_status(row)
    period_status = period_end_status(row)
    publish_status = publish_date_status(row, bool(sequence.get("publish_sequence_issue")))
    seq_status = str(sequence["sequence_status"])
    missing_core: list[str] = []
    missing_noncore: list[str] = []
    derivable: list[str] = []
    derivation_inputs: list[str] = []
    semantic_conflicts: list[str] = []
    source_conflicts: list[str] = []
    issue_codes: list[str] = []
    evidence_codes: list[str] = []
    evidence_desc: list[str] = []

    for label, column in PRIMARY_FIELDS.items():
        state, derived, need = field_state(row, label, column)
        if state in {"MISSING", "DERIVATION_EVIDENCE_MISSING"}:
            missing_core.append(label)
            evidence_codes.append(f"NEED_{label.upper().replace(' ', '_') if label != 'Debt' else 'DEBT'}")
            evidence_desc.append(f"{fyfq(row)}: need {label}")
        derivable.extend(derived)
        derivation_inputs.extend(need)
    for label, column in SECONDARY_FIELDS.items():
        state, derived, need = field_state(row, label, column)
        if state in {"MISSING", "DERIVATION_EVIDENCE_MISSING"}:
            missing_noncore.append(label)
            evidence_codes.append(f"NEED_{label.upper().replace(' ', '_')}")
            evidence_desc.append(f"{fyfq(row)}: need {label}")
        derivable.extend(derived)
        derivation_inputs.extend(need)

    if fiscal_status not in {"FISCAL_IDENTITY_CLEAN_DIRECT_EXACT", "FISCAL_IDENTITY_CLEAN_SUPPORTED"}:
        issue_codes.append(fiscal_status)
        if fiscal_status in {"FY_CONFLICT_DIRECT_EXACT", "FQ_CONFLICT_DIRECT_EXACT", "FY_FQ_CONFLICT"}:
            evidence_codes.append("NEED_LOCAL_LINEAGE_RECONCILIATION")
            evidence_desc.append(f"{fyfq(row)}: reconcile stored FY/FQ against direct exact fiscal anchor")
        elif fiscal_status == "TRANSITION_REVIEW":
            evidence_codes.append("NEED_TRANSITION_CALENDAR_EVIDENCE")
            evidence_desc.append(f"{fyfq(row)}: need transition/stub calendar decision")
        else:
            evidence_codes.append("NEED_OFFICIAL_FISCAL_YEAR_START")
            evidence_codes.append("NEED_OFFICIAL_FY_FQ_IDENTITY")
            evidence_desc.append(f"{fyfq(row)}: need official fiscal year start and FY/FQ identity")
    if period_status != "PERIOD_END_CLEAN":
        issue_codes.append(period_status)
        evidence_codes.append("NEED_OFFICIAL_PERIOD_END")
        evidence_desc.append(f"{fyfq(row)}: need official period_end")
    if publish_status != "PUBLISH_DATE_CLEAN":
        issue_codes.append(publish_status)
        evidence_codes.append("NEED_FIRST_PUBLIC_RESULT_DATE")
        evidence_desc.append(f"{fyfq(row)}: need first-public result date")
    if seq_status != "SEQUENCE_CLEAN":
        issue_codes.append(seq_status)
        if seq_status == "DUPLICATE_ECONOMIC_QUARTER":
            evidence_codes.append("NEED_TARGET_COLLISION_RESOLUTION")
            evidence_desc.append(f"{fyfq(row)}: resolve duplicate/collision in latest8Q sequence")
        elif seq_status == "MISSING_QUARTER":
            evidence_codes.append("NEED_MISSING_QUARTER_SOURCE")
            evidence_desc.append(f"{fyfq(row)}: verify missing adjacent fiscal quarter source")
        elif seq_status == "TRANSITION_SEQUENCE":
            evidence_codes.append("NEED_TRANSITION_CALENDAR_EVIDENCE")
            evidence_desc.append(f"{fyfq(row)}: need transition/stub calendar decision")
    if row.get("target_collision") in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING"}:
        issue_codes.append("TARGET_COLLISION")
        source_conflicts.append(str(row.get("target_collision")))
        evidence_codes.append("NEED_TARGET_COLLISION_RESOLUTION")
        evidence_desc.append(f"{fyfq(row)}: target FY/FQ collision requires reconciliation")

    if missing_core:
        issue_codes.append("PRIMARY_CORE_INCOMPLETE")
    if missing_noncore:
        issue_codes.append("SECONDARY_FIELDS_INCOMPLETE")
    if derivation_inputs:
        evidence_codes.append("NEED_SOURCE_SEMANTICS_CONFIRMATION")
        evidence_desc.extend(f"{fyfq(row)}: {item}" for item in derivation_inputs)

    external = external_required(evidence_codes)
    complexity = quarter_complexity(issue_codes, evidence_codes)
    priority = quarter_priority(row, issue_codes, downstream)
    clean_primary = not missing_core and not metadata_or_sequence_issues(issue_codes)
    clean_all = clean_primary and not missing_noncore
    return {
        "ticker": row["ticker"],
        "company_id": row["company_id"],
        "quarter_id": row["quarter_id"],
        "fiscal_year": row["fiscal_year"],
        "fiscal_quarter": row["fiscal_quarter"],
        "period_end": row.get("period_end") or "",
        "publish_date": row.get("publish_date") or "",
        "quarter_position_latest8q": row["quarter_position_latest8q"],
        "fiscal_identity_status": fiscal_status,
        "fiscal_identity_evidence": row.get("identity_basis", ""),
        "period_end_status": period_status,
        "publish_date_status": publish_status,
        "sequence_status": seq_status,
        "missing_core_fields": "|".join(missing_core),
        "missing_noncore_fields": "|".join(missing_noncore),
        "derivable_fields": "|".join(sorted(set(derivable))),
        "derivation_missing_inputs": "|".join(derivation_inputs),
        "semantic_conflicts": "|".join(semantic_conflicts),
        "source_conflicts": "|".join(source_conflicts),
        "current_ttm_impact": downstream.get("ttm_impact", "NO"),
        "score_impact": downstream.get("score_impact", "NO"),
        "lifecycle_impact": downstream.get("lifecycle_impact", "NO"),
        "valuation_impact": downstream.get("valuation_impact", "NO"),
        "issue_codes": "|".join(sorted(set(issue_codes))),
        "evidence_needed_codes": "|".join(sorted(set(evidence_codes))),
        "evidence_needed_description": " ; ".join(dedupe(evidence_desc)),
        "likely_local_source": row.get("local_source_hint", ""),
        "external_research_required": "YES" if external else "NO",
        "repair_complexity": complexity,
        "priority": priority,
        "confidence": confidence(row, issue_codes),
        "notes": sequence.get("sequence_note", ""),
        "primary_core_complete": int(not missing_core),
        "all_tracked_fields_complete": int(not missing_core and not missing_noncore),
        "clean_primary": int(clean_primary),
        "clean_all": int(clean_all),
        "calendar_type": row.get("calendar_type", ""),
        "break_reason": row.get("break_reason", ""),
        "target_collision": row.get("target_collision", ""),
        "source_winner": row.get("source_winner", ""),
        "provenance": row.get("provenance", ""),
        "lineage": row.get("lineage", ""),
    }


def fyfq(row: dict[str, Any]) -> str:
    return f"FY{row.get('fiscal_year')}{row.get('fiscal_quarter')}"


def dedupe(items: Iterable[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def metadata_or_sequence_issues(issue_codes: list[str]) -> bool:
    ignored = {"PRIMARY_CORE_INCOMPLETE", "SECONDARY_FIELDS_INCOMPLETE"}
    return any(code not in ignored for code in issue_codes)


def external_required(evidence_codes: Iterable[str]) -> bool:
    external_codes = {
        "NEED_OFFICIAL_FISCAL_YEAR_START",
        "NEED_OFFICIAL_FY_FQ_IDENTITY",
        "NEED_OFFICIAL_PERIOD_END",
        "NEED_FIRST_PUBLIC_RESULT_DATE",
        "NEED_REVENUE",
        "NEED_GROSS_PROFIT",
        "NEED_OPERATING_INCOME",
        "NEED_EBIT",
        "NEED_EBITDA",
        "NEED_NET_INCOME",
        "NEED_OCF",
        "NEED_CAPEX",
        "NEED_FCF",
        "NEED_CASH",
        "NEED_DEBT",
        "NEED_SHARES",
        "NEED_TRANSITION_CALENDAR_EVIDENCE",
        "NEED_RESTATEMENT_RECONCILIATION",
    }
    return any(code in external_codes for code in evidence_codes)


def quarter_complexity(issue_codes: list[str], evidence_codes: list[str]) -> str:
    issue_set = set(issue_codes)
    evidence_set = set(evidence_codes)
    if not issue_set:
        return "NO_REPAIR_NEEDED"
    if "TARGET_COLLISION" in issue_set or "NEED_TARGET_COLLISION_RESOLUTION" in evidence_set:
        return "SOURCE_RECONCILIATION"
    if "TRANSITION_SEQUENCE" in issue_set or "NEED_TRANSITION_CALENDAR_EVIDENCE" in evidence_set:
        return "TRANSITION_RESEARCH"
    if "MISSING_QUARTER" in issue_set:
        return "MISSING_QUARTER_CREATION"
    if evidence_set & {"NEED_REVENUE", "NEED_EBIT", "NEED_FCF", "NEED_CASH", "NEED_DEBT", "NEED_SHARES", "NEED_EBITDA"}:
        return "CONTENT_RECONSTRUCTION"
    if evidence_set & {"NEED_OFFICIAL_PERIOD_END", "NEED_FIRST_PUBLIC_RESULT_DATE", "NEED_OFFICIAL_FY_FQ_IDENTITY"}:
        return "SIMPLE_METADATA_FIX"
    if "NEED_LOCAL_LINEAGE_RECONCILIATION" in evidence_set:
        return "DETERMINISTIC_RELABEL"
    return "SIMPLE_FIELD_FILL"


def confidence(row: dict[str, Any], issue_codes: list[str]) -> str:
    if not issue_codes:
        return "HIGH"
    if row.get("identity_basis") == "DIRECT_EXACT_INTERVAL" and row.get("local_source_hint") != "NONE_OBVIOUS":
        return "MEDIUM"
    if row.get("break_reason") in {"CALENDAR_TRANSITION", "UNRESOLVED_BOUNDARY", "NO_FISCAL_YEAR"}:
        return "LOW"
    return "MEDIUM"


def quarter_priority(row: dict[str, Any], issue_codes: list[str], downstream: dict[str, Any]) -> str:
    if not issue_codes:
        return "P5_NO_ACTION"
    noncore_only = set(issue_codes) == {"SECONDARY_FIELDS_INCOMPLETE"}
    if noncore_only:
        return "P4_NONCORE"
    if int(row.get("quarter_position_latest8q") or 0) == 1 or downstream.get("ttm_impact") == "YES":
        return "P1_CURRENT"
    if int(row.get("quarter_position_latest8q") or 0) <= 4:
        return "P2_LATEST4Q"
    return "P3_LATEST8Q"


def current_downstream_maps(db: Path, ttm_risk: list[dict[str, Any]]) -> dict[str, Any]:
    score = latest_table_by_ticker(db, "v3_score", "endpoint_period_end", "score_ready")
    lifecycle = latest_table_by_ticker(db, "v3_lifecycle", "endpoint_period_end", "lifecycle_ready")
    valuation = latest_table_by_ticker(db, "v3_valuation", "endpoint_period_end", "valuation_ready")
    ttm = latest_table_by_ticker(db, "v3_ttm", "period_end", "ttm_pit_ready")
    ttm_by_ticker = {row["ticker"]: row for row in ttm_risk}
    return {"score": score, "lifecycle": lifecycle, "valuation": valuation, "ttm": ttm, "ttm_risk": ttm_by_ticker}


def quarter_downstream_impact(row: dict[str, Any], downstream: dict[str, Any]) -> dict[str, str]:
    ticker = str(row["ticker"])
    ttm_risk = downstream["ttm_risk"].get(ticker, {})
    ttm_input_ids = set(str(ttm_risk.get("input_quarter_ids") or "").split("|"))
    ttm_impact = "YES" if str(row["quarter_id"]) in ttm_input_ids and ttm_risk.get("risk_class") not in {"TTM_CLEAN_DIRECT_EXACT", "TTM_CLEAN_INFERRED"} else "NO"
    return {
        "ttm_impact": ttm_impact,
        "score_impact": "YES" if ttm_impact == "YES" and int(downstream["score"].get(ticker, {}).get("ready") or 0) == 0 else "NO",
        "lifecycle_impact": "YES" if ttm_impact == "YES" and int(downstream["lifecycle"].get(ticker, {}).get("ready") or 0) == 0 else "NO",
        "valuation_impact": "YES" if ttm_impact == "YES" and int(downstream["valuation"].get(ticker, {}).get("ready") or 0) == 0 else "NO",
    }


def latest8q_population_rows(rows_: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "company_id": row["company_id"],
            "ticker": row["ticker"],
            "quarter_id": row["quarter_id"],
            "fiscal_year": row["fiscal_year"],
            "fiscal_quarter": row["fiscal_quarter"],
            "period_end": row.get("period_end") or "",
            "publish_date": row.get("publish_date") or "",
            "quarter_position_latest8q": row["quarter_position_latest8q"],
            "fiscal_anchor_evidence": row.get("fiscal_anchor_evidence", ""),
            "calendar_type": row.get("calendar_type", ""),
            "source_winner": row.get("source_winner", ""),
            "provenance": row.get("provenance", ""),
            "lineage": row.get("lineage", ""),
        }
        for row in rows_
    ]


def build_quarter_diagnostics(db: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    with connect_ro(db) as conn:
        latest_rows, ttm_risk = classified_rows(conn)
    sequence = sequence_statuses(latest_rows)
    downstream = current_downstream_maps(db, ttm_risk)
    diagnostics = [analyze_quarter(row, sequence[int(row["quarter_id"])], quarter_downstream_impact(row, downstream)) for row in latest_rows]
    problem = [row for row in diagnostics if row["issue_codes"]]
    return latest_rows, diagnostics, problem, {"ttm_risk": ttm_risk, "downstream": downstream}


def summarize_tickers(
    db: Path,
    latest_rows: list[dict[str, Any]],
    diagnostics: list[dict[str, Any]],
    ttm_risk: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    active = active_tickers(db)
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in diagnostics:
        by_ticker[str(row["ticker"])].append(row)
    for row in latest_rows:
        source_rows[str(row["ticker"])].append(row)
    downstream = current_downstream_maps(db, ttm_risk)
    out = []
    for ticker in active:
        group = sorted(by_ticker.get(ticker, []), key=lambda r: int(r["quarter_position_latest8q"]))
        source_group = sorted(source_rows.get(ticker, []), key=lambda r: int(r["quarter_position_latest8q"]))
        problem = [r for r in group if r["issue_codes"]]
        local_actions, external_actions, structural_actions = requirement_counts(problem)
        full_clean_primary = len(group) >= 8 and all(int(r["clean_primary"]) for r in group)
        full_complete_all = len(group) >= 8 and all(int(r["clean_all"]) for r in group)
        fully_clean_now = full_clean_primary
        latest4_clean = len(group) >= 4 and all(int(r["clean_primary"]) for r in group[:4])
        latest_quarter_clean = bool(group) and int(group[0]["clean_primary"]) == 1
        missing_history_reason = fewer_than_8_reason(source_group)
        has_structural = structural_actions > 0 or missing_history_reason in {"NO_FISCAL_YEAR", "VERIFIED_TRANSITION", "UNKNOWN"}
        expected_final, closure_test = theoretical_closure(problem, len(group), missing_history_reason, has_structural)
        row = {
            "ticker": ticker,
            "company_id": source_group[0]["company_id"] if source_group else "",
            "latest8q_rows": len(group),
            "latest8q_clean_rows": sum(int(r["clean_primary"]) for r in group),
            "latest8q_problem_rows": len(problem),
            "latest8q_clean_pct": pct(sum(int(r["clean_primary"]) for r in group), len(group)),
            "latest8q_fully_clean_now": yesno(fully_clean_now),
            "latest8q_fully_clean_primary_core_now": yesno(full_clean_primary),
            "latest8q_fully_complete_all_fields_now": yesno(full_complete_all),
            "latest4q_clean": yesno(latest4_clean),
            "latest_quarter_clean": yesno(latest_quarter_clean),
            "fiscal_identity_issue_count": issue_count(problem, ("FY_CONFLICT", "FQ_CONFLICT", "FISCAL", "TRANSITION_REVIEW", "UNRESOLVED_BOUNDARY")),
            "period_end_issue_count": issue_count(problem, ("PERIOD_END",)),
            "publish_date_issue_count": issue_count(problem, ("PUBLISH",)),
            "missing_quarter_count": issue_count(problem, ("MISSING_QUARTER",)) + max(0, 8 - len(group) if missing_history_reason not in {"NO_FISCAL_YEAR", "VERIFIED_TRANSITION"} else 0),
            "duplicate_quarter_count": issue_count(problem, ("DUPLICATE_ECONOMIC_QUARTER", "TARGET_COLLISION")),
            "sequence_issue_count": issue_count(problem, ("SEQUENCE", "MISSING_QUARTER", "DUPLICATE_ECONOMIC_QUARTER", "TRANSITION_SEQUENCE", "UNRESOLVED_SEQUENCE")),
            "primary_core_incomplete_quarters": sum(bool(r["missing_core_fields"]) for r in group),
            **field_missing_counts(group),
            **downstream_statuses(ticker, downstream, problem),
            "overall_latest8q_status": overall_status(fully_clean_now, full_clean_primary, full_complete_all, problem, missing_history_reason),
            "operational_priority": ticker_priority(group, problem, missing_history_reason),
            "repair_complexity": aggregate_complexity([r["repair_complexity"] for r in problem], missing_history_reason),
            "local_data_likely_sufficient": yesno(local_actions > 0 and external_actions == 0 and structural_actions == 0),
            "external_research_required": yesno(external_actions > 0),
            "affected_fy_fq": ";".join(dedupe([fyfq(r) for r in problem])),
            "quarters_requiring_local_repair": ";".join(dedupe([fyfq(r) for r in problem if r["external_research_required"] == "NO" and "STRUCTURAL" not in r["repair_complexity"] and r["repair_complexity"] != "NO_REPAIR_NEEDED"])),
            "quarters_requiring_external_research": ";".join(dedupe([fyfq(r) for r in problem if r["external_research_required"] == "YES"])),
            "quarters_requiring_structural_review": ";".join(dedupe([fyfq(r) for r in problem if is_structural(r)])),
            "blockers_to_full_latest8q_clean_count": blocker_count(problem, len(group), missing_history_reason),
            "all_blocker_codes": "|".join(sorted({code for r in problem for code in split_codes(r["issue_codes"])})),
            "all_evidence_needed_codes": "|".join(sorted({code for r in problem for code in split_codes(r["evidence_needed_codes"])})),
            "all_evidence_needed_description": " ; ".join(dedupe([r["evidence_needed_description"] for r in problem if r["evidence_needed_description"]] + missing_history_description(len(group), missing_history_reason))),
            "known_phase8_defect": yesno(ticker in KNOWN_13),
            "expected_final_status_if_all_requested_evidence_obtained": expected_final,
            "theoretical_closure_test": closure_test,
            "recommended_next_action": recommended_next_action(problem, missing_history_reason, local_actions, external_actions, structural_actions),
            "fewer_than_8_reason": missing_history_reason,
        }
        out.append(row)
    return out


def yesno(value: bool) -> str:
    return "YES" if value else "NO"


def split_codes(value: str) -> list[str]:
    return [item for item in str(value or "").split("|") if item]


def issue_count(rows_: list[dict[str, Any]], prefixes: tuple[str, ...]) -> int:
    return sum(any(code.startswith(prefix) or prefix in code for prefix in prefixes for code in split_codes(row["issue_codes"])) for row in rows_)


def field_missing_counts(group: list[dict[str, Any]]) -> dict[str, int]:
    out = {}
    for label in PRIMARY_FIELDS:
        out[f"{label.lower().replace(' ', '_')}_missing_q"] = sum(label in split_codes_pipe(row["missing_core_fields"]) for row in group)
    for label in SECONDARY_FIELDS:
        out[f"{label.lower().replace(' ', '_')}_missing_q"] = sum(label in split_codes_pipe(row["missing_noncore_fields"]) for row in group)
    return out


def split_codes_pipe(value: str) -> list[str]:
    return [item for item in str(value or "").split("|") if item]


def requirement_counts(problem: list[dict[str, Any]]) -> tuple[int, int, int]:
    local = sum(1 for r in problem if r["external_research_required"] == "NO" and r["repair_complexity"] != "NO_REPAIR_NEEDED")
    external = sum(1 for r in problem if r["external_research_required"] == "YES")
    structural = sum(1 for r in problem if is_structural(r))
    return local, external, structural


def is_structural(row: dict[str, Any]) -> bool:
    codes = set(split_codes(row["issue_codes"]))
    return bool(codes & {"TRANSITION_SEQUENCE", "UNRESOLVED_SEQUENCE", "TARGET_COLLISION", "DUPLICATE_ECONOMIC_QUARTER"}) or row["repair_complexity"] in {"TRANSITION_RESEARCH", "SOURCE_RECONCILIATION"}


def fewer_than_8_reason(group: list[dict[str, Any]]) -> str:
    if len(group) >= 8:
        return ""
    if not group:
        return "UNKNOWN"
    break_reason = str(group[0].get("break_reason") or "")
    if break_reason == "NO_FISCAL_YEAR":
        return "NO_FISCAL_YEAR"
    if break_reason == "CALENDAR_TRANSITION":
        return "VERIFIED_TRANSITION"
    if break_reason == "SOURCE_HISTORY_EXHAUSTED":
        return "SOURCE_HISTORY_LIMITED"
    oldest = min((parse_date(r.get("period_end")) for r in group), default=None)
    if oldest and oldest >= date(2024, 1, 1):
        return "RECENT_IPO"
    return "UNKNOWN"


def missing_history_description(count: int, reason: str) -> list[str]:
    if count >= 8 or not reason:
        return []
    if reason in {"RECENT_IPO", "NEW_COMPANY", "NO_FISCAL_YEAR", "VERIFIED_TRANSITION"}:
        return [f"Latest8Q has {count} rows; classified as {reason}"]
    return [f"Latest8Q has {count} rows; need source evidence for missing historical fiscal quarters"]


def theoretical_closure(problem: list[dict[str, Any]], count: int, missing_reason: str, structural: bool) -> tuple[str, str]:
    if count < 8 and missing_reason in {"RECENT_IPO", "NEW_COMPANY", "NO_FISCAL_YEAR", "VERIFIED_TRANSITION"}:
        return "LATEST8Q_CANNOT_REACH_8Q_LEGITIMATE_HISTORY_LIMIT", "NO_LEGITIMATE_HISTORY_LIMIT"
    if structural:
        return "LATEST8Q_REQUIRES_UNRESOLVED_STRUCTURAL_DECISION", "NO_UNRESOLVED_STRUCTURAL_DECISION"
    if not problem and count >= 8:
        return "LATEST8Q_FULLY_CLEAN", "YES_FULLY_CLEAN"
    if count < 8:
        return "LATEST8Q_FULLY_CLEAN", "YES_FULLY_CLEAN"
    if any(r["missing_noncore_fields"] for r in problem) and not any(r["missing_core_fields"] or metadata_or_sequence_issues(split_codes(r["issue_codes"])) for r in problem):
        return "LATEST8Q_FULLY_CLEAN_PRIMARY_CORE_ONLY", "YES_PRIMARY_CORE_ONLY"
    return "LATEST8Q_FULLY_CLEAN", "YES_FULLY_CLEAN"


def blocker_count(problem: list[dict[str, Any]], count: int, reason: str) -> int:
    extra = 0 if count >= 8 or reason in {"RECENT_IPO", "NEW_COMPANY", "NO_FISCAL_YEAR", "VERIFIED_TRANSITION"} else 8 - count
    return sum(max(1, len(split_codes(r["issue_codes"]))) for r in problem) + extra


def downstream_statuses(ticker: str, downstream: dict[str, Any], problem: list[dict[str, Any]]) -> dict[str, str]:
    ttm = downstream["ttm"].get(ticker)
    ttm_risk = downstream["ttm_risk"].get(ticker, {})
    score = downstream["score"].get(ticker)
    lifecycle = downstream["lifecycle"].get(ticker)
    valuation = downstream["valuation"].get(ticker)
    missing_core = sorted({field for row in problem for field in split_codes_pipe(row["missing_core_fields"])})
    sequence_block = any(code in {"MISSING_QUARTER", "DUPLICATE_ECONOMIC_QUARTER", "TRANSITION_SEQUENCE", "UNRESOLVED_SEQUENCE"} for row in problem for code in split_codes(row["issue_codes"]))
    ttm_status = (
        "UNAVAILABLE_HISTORY"
        if not ttm
        else "AVAILABLE_CLEAN"
        if ttm_risk.get("risk_class") in {"TTM_CLEAN_DIRECT_EXACT", "TTM_CLEAN_INFERRED"}
        else "UNAVAILABLE_SEQUENCE"
        if sequence_block
        else "UNAVAILABLE_MISSING_CORE"
        if missing_core
        else "AVAILABLE_WITH_IDENTITY_RISK"
    )
    ttm_blocker = "" if ttm_status == "AVAILABLE_CLEAN" else "|".join(filter(None, [str(ttm_risk.get("risk_class") or ""), "missing_core=" + ",".join(missing_core) if missing_core else "", "sequence" if sequence_block else ""]))
    score_ready = int(score.get("ready") or 0) if score else 0
    lifecycle_ready = int(lifecycle.get("ready") or 0) if lifecycle else 0
    valuation_ready = int(valuation.get("ready") or 0) if valuation else 0
    publish_missing = any("PUBLISH_DATE_MISSING" in split_codes(r["issue_codes"]) for r in problem)
    return {
        "current_ttm_status": ttm_status,
        "current_ttm_blocker": ttm_blocker,
        "score_status": "AVAILABLE" if score_ready else "NOT_READY_TTM" if ttm_status != "AVAILABLE_CLEAN" else "NOT_READY_COMPONENT",
        "score_blocker": "" if score_ready else ttm_blocker or "score component not ready",
        "lifecycle_status": "AVAILABLE" if lifecycle_ready else "NOT_READY_TTM" if ttm_status != "AVAILABLE_CLEAN" else "NOT_READY_COMPONENT",
        "lifecycle_blocker": "" if lifecycle_ready else ttm_blocker or "lifecycle component/history not ready",
        "valuation_status": "AVAILABLE" if valuation_ready else "NOT_READY_PUBLISH_DATE" if publish_missing else "NOT_READY_FUNDAMENTALS" if ttm_status != "AVAILABLE_CLEAN" else "NOT_READY_PRICE",
        "valuation_blocker": "" if valuation_ready else "publish_date" if publish_missing else ttm_blocker or "price/fundamental input not ready",
    }


def overall_status(fully_clean: bool, primary: bool, all_fields: bool, problem: list[dict[str, Any]], reason: str) -> str:
    if fully_clean and all_fields:
        return "LATEST8Q_FULLY_COMPLETE_ALL_TRACKED_FIELDS"
    if primary:
        return "LATEST8Q_FULLY_CLEAN_PRIMARY_CORE"
    if reason in {"RECENT_IPO", "NEW_COMPANY", "NO_FISCAL_YEAR", "VERIFIED_TRANSITION"}:
        return "LATEST8Q_LEGITIMATE_HISTORY_LIMIT"
    if not problem:
        return "LATEST8Q_INCOMPLETE_HISTORY"
    return "LATEST8Q_REPAIR_REQUIRED"


def ticker_priority(group: list[dict[str, Any]], problem: list[dict[str, Any]], reason: str) -> str:
    if not problem and not reason:
        return "P5_NO_ACTION"
    if problem and all(set(split_codes(r["issue_codes"])) <= {"SECONDARY_FIELDS_INCOMPLETE"} for r in problem):
        return "P4_NONCORE"
    priorities = [r["priority"] for r in problem]
    for priority in ("P1_CURRENT", "P2_LATEST4Q", "P3_LATEST8Q", "P4_NONCORE"):
        if priority in priorities:
            return priority
    return "P5_NO_ACTION" if reason in {"RECENT_IPO", "NEW_COMPANY", "NO_FISCAL_YEAR", "VERIFIED_TRANSITION"} else "P3_LATEST8Q"


def aggregate_complexity(complexities: list[str], reason: str) -> str:
    order = [
        "SOURCE_RECONCILIATION",
        "TRANSITION_RESEARCH",
        "MISSING_QUARTER_CREATION",
        "CONTENT_RECONSTRUCTION",
        "ATOMIC_SEGMENT_REPAIR",
        "DETERMINISTIC_RELABEL",
        "SIMPLE_METADATA_FIX",
        "SIMPLE_FIELD_FILL",
        "NO_REPAIR_NEEDED",
    ]
    if reason in {"RECENT_IPO", "NEW_COMPANY", "NO_FISCAL_YEAR", "VERIFIED_TRANSITION"} and not complexities:
        return "NO_REPAIR_NEEDED"
    for item in order:
        if item in complexities:
            return item
    return "NO_REPAIR_NEEDED"


def recommended_next_action(problem: list[dict[str, Any]], reason: str, local: int, external: int, structural: int) -> str:
    if reason in {"RECENT_IPO", "NEW_COMPANY", "NO_FISCAL_YEAR", "VERIFIED_TRANSITION"} and not problem:
        return "No repair; legitimate latest8Q history limit"
    if structural:
        return "Manual structural review before any production repair"
    if local and not external:
        return "Prepare local-only deterministic repair rehearsal"
    if external:
        return "Collect official issuer/filing evidence, then rehearse repair"
    if problem:
        return "Review generated quarter-level blockers"
    return "No repair needed"


def local_repair_queue(summary: list[dict[str, Any]], problems: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in problems:
        if row["external_research_required"] == "YES" or is_structural(row):
            continue
        out.append(
            {
                "ticker": row["ticker"],
                "FY": row["fiscal_year"],
                "FQ": row["fiscal_quarter"],
                "issue": row["issue_codes"],
                "evidence/action needed": row["evidence_needed_description"],
                "likely local source": row["likely_local_source"],
                "exact local table/artifact/path where possible": row["likely_local_source"],
                "repair type": row["repair_complexity"],
                "priority": row["priority"],
                "downstream impact": downstream_impact_text(row),
                "prerequisite": "None",
            }
        )
    return out


def external_research_queue(problems: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in problems:
        if row["external_research_required"] != "YES":
            continue
        out.append(
            {
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "current_period_end": row["period_end"],
                "current_publish_date": row["publish_date"],
                "issue": row["issue_codes"],
                "exact_information_needed": row["evidence_needed_description"],
                "preferred_source_type": preferred_source(row["evidence_needed_codes"]),
                "downstream_impact": downstream_impact_text(row),
                "priority": row["priority"],
                "structural_context": row["calendar_type"],
                "warning": "Use first-public release date; do not use amendment/restatement date",
                "closure_dependency": row["evidence_needed_codes"],
            }
        )
    return out


def structural_review_queue(problems: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in problems:
        if not is_structural(row):
            continue
        out.append(
            {
                "ticker": row["ticker"],
                "FY/FQ": fyfq(row),
                "issue": row["issue_codes"],
                "current evidence": row["fiscal_identity_evidence"] + "|" + row["target_collision"],
                "exact decision needed": row["evidence_needed_description"],
                "evidence that would resolve it": row["evidence_needed_codes"],
                "priority": row["priority"],
            }
        )
    return out


def downstream_impact_text(row: dict[str, Any]) -> str:
    return "|".join(k for k in ("current_ttm_impact", "score_impact", "lifecycle_impact", "valuation_impact") if row.get(k) == "YES") or "none"


def preferred_source(codes: str) -> str:
    code_set = set(split_codes(codes))
    if "NEED_FIRST_PUBLIC_RESULT_DATE" in code_set:
        return "official issuer IR earnings release/archive"
    if code_set & {"NEED_REVENUE", "NEED_EBIT", "NEED_FCF", "NEED_CASH", "NEED_DEBT", "NEED_SHARES"}:
        return "official quarterly/annual report or SEC filing"
    if code_set & {"NEED_OFFICIAL_FISCAL_YEAR_START", "NEED_OFFICIAL_FY_FQ_IDENTITY", "NEED_OFFICIAL_PERIOD_END"}:
        return "official issuer financial report or earnings release"
    return "official issuer evidence"


def distributions(problems: list[dict[str, Any]], summary: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    issue_counter: Counter[str] = Counter()
    evidence_counter: Counter[str] = Counter()
    for row in problems:
        issue_counter.update(split_codes(row["issue_codes"]))
        evidence_counter.update(split_codes(row["evidence_needed_codes"]))
    complexity_counter = Counter(row["repair_complexity"] for row in summary)
    repairability_counter = Counter(
        "NO_REPAIR_NEEDED"
        if row["overall_latest8q_status"].startswith("LATEST8Q_FULLY")
        else "STRUCTURAL_REVIEW"
        if row["theoretical_closure_test"] == "NO_UNRESOLVED_STRUCTURAL_DECISION"
        else "LEGITIMATE_HISTORY_LIMIT"
        if row["theoretical_closure_test"] == "NO_LEGITIMATE_HISTORY_LIMIT"
        else "EXTERNAL_REQUIRED"
        if row["external_research_required"] == "YES"
        else "LOCAL_ONLY"
        for row in summary
    )
    return {
        "issues": [{"issue_code": k, "rows": v} for k, v in sorted(issue_counter.items())],
        "evidence": [{"evidence_needed_code": k, "rows": v} for k, v in sorted(evidence_counter.items(), key=lambda kv: (-kv[1], kv[0]))],
        "complexity": [{"repair_complexity": k, "tickers": v} for k, v in sorted(complexity_counter.items())],
        "repairability": [{"repairability": k, "tickers": v} for k, v in sorted(repairability_counter.items())],
    }


def field_completeness_rows(diagnostics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    total = len(diagnostics)
    for label in PRIMARY_FIELDS:
        missing = sum(label in split_codes_pipe(row["missing_core_fields"]) for row in diagnostics)
        out.append({"field": label, "field_group": "PRIMARY_CORE", "present_or_derivable": total - missing, "missing": missing, "total": total, "pct": pct(total - missing, total)})
    for label in SECONDARY_FIELDS:
        missing = sum(label in split_codes_pipe(row["missing_noncore_fields"]) for row in diagnostics)
        out.append({"field": label, "field_group": "SECONDARY", "present_or_derivable": total - missing, "missing": missing, "total": total, "pct": pct(total - missing, total)})
    return out


def downstream_blockers(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in summary:
        for layer, status_col, blocker_col in (
            ("TTM", "current_ttm_status", "current_ttm_blocker"),
            ("Score", "score_status", "score_blocker"),
            ("Lifecycle", "lifecycle_status", "lifecycle_blocker"),
            ("Valuation", "valuation_status", "valuation_blocker"),
        ):
            if row[status_col] not in {"AVAILABLE", "AVAILABLE_CLEAN"}:
                out.append({"ticker": row["ticker"], "layer": layer, "status": row[status_col], "blocker": row[blocker_col], "priority": row["operational_priority"]})
    return out


def theoretical_closure_rows(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "ticker": row["ticker"],
            "latest8q_rows": row["latest8q_rows"],
            "current_status": row["overall_latest8q_status"],
            "expected_final_status_if_all_requested_evidence_obtained": row["expected_final_status_if_all_requested_evidence_obtained"],
            "theoretical_closure_test": row["theoretical_closure_test"],
            "missing_requirements_in_plan": int(row["theoretical_closure_test"] == "NO_MISSING_REQUIREMENT_IN_PLAN"),
        }
        for row in summary
    ]


def known_13_rows(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "ticker": row["ticker"],
            "affected_fy_fq": row["affected_fy_fq"],
            "issue": row["all_blocker_codes"],
            "evidence_needed": row["all_evidence_needed_description"],
            "local_external": "external" if row["external_research_required"] == "YES" else "local" if row["local_data_likely_sufficient"] == "YES" else "structural/manual",
            "repair_complexity": row["repair_complexity"],
            "current_ttm_impact": row["current_ttm_status"],
            "expected_final_status": row["expected_final_status_if_all_requested_evidence_obtained"],
        }
        for row in summary
        if row["ticker"] in KNOWN_13
    ]


def build_summary(
    artifact_root: Path,
    counts: dict[str, int],
    diagnostics: list[dict[str, Any]],
    problems: list[dict[str, Any]],
    ticker_summary: list[dict[str, Any]],
    local_queue: list[dict[str, Any]],
    external_queue: list[dict[str, Any]],
    structural_queue: list[dict[str, Any]],
    before_fp: dict[str, Any],
    after_fp: dict[str, Any],
) -> dict[str, Any]:
    dists = distributions(problems, ticker_summary)
    no_missing = sum(1 for row in ticker_summary if row["theoretical_closure_test"] == "NO_MISSING_REQUIREMENT_IN_PLAN")
    structural_count = sum(1 for row in ticker_summary if row["theoretical_closure_test"] == "NO_UNRESOLVED_STRUCTURAL_DECISION")
    classification = CLASSIFICATION_INCOMPLETE if no_missing else CLASSIFICATION_STRUCTURAL if structural_count else CLASSIFICATION_COMPLETE
    next_action = NEXT_ACTION_INCOMPLETE if classification == CLASSIFICATION_INCOMPLETE else NEXT_ACTION_STRUCTURAL if classification == CLASSIFICATION_STRUCTURAL else NEXT_ACTION_COMPLETE
    headline = {
        "active_tickers": counts["active_companies"],
        "latest8q_rows": len(diagnostics),
        "already_latest8q_fully_clean": sum(row["latest8q_fully_clean_now"] == "YES" for row in ticker_summary),
        "fully_clean_primary_core": sum(row["latest8q_fully_clean_primary_core_now"] == "YES" for row in ticker_summary),
        "all_tracked_fields_complete": sum(row["latest8q_fully_complete_all_fields_now"] == "YES" for row in ticker_summary),
        "need_local_repair_only": sum(row["local_data_likely_sufficient"] == "YES" for row in ticker_summary),
        "need_external_official_evidence": sum(row["external_research_required"] == "YES" for row in ticker_summary),
        "need_structural_manual_review": structural_count,
        "legitimate_less_than_8q_history": sum(row["theoretical_closure_test"] == "NO_LEGITIMATE_HISTORY_LIMIT" for row in ticker_summary),
        "current_impact_p1": sum(row["operational_priority"] == "P1_CURRENT" for row in ticker_summary),
    }
    downstream = {
        "ttm_clean": sum(row["current_ttm_status"] == "AVAILABLE_CLEAN" for row in ticker_summary),
        "ttm_affected_or_unavailable": sum(row["current_ttm_status"] != "AVAILABLE_CLEAN" for row in ticker_summary),
        "score_available": sum(row["score_status"] == "AVAILABLE" for row in ticker_summary),
        "score_blocked": sum(row["score_status"] != "AVAILABLE" for row in ticker_summary),
        "lifecycle_available": sum(row["lifecycle_status"] == "AVAILABLE" for row in ticker_summary),
        "lifecycle_blocked": sum(row["lifecycle_status"] != "AVAILABLE" for row in ticker_summary),
        "valuation_available": sum(row["valuation_status"] == "AVAILABLE" for row in ticker_summary),
        "valuation_blocked": sum(row["valuation_status"] != "AVAILABLE" for row in ticker_summary),
    }
    full_closure = {
        "already_fully_clean": headline["already_latest8q_fully_clean"],
        "repairable_local_only": headline["need_local_repair_only"],
        "repairable_requiring_external": headline["need_external_official_evidence"],
        "structural_decision_required": structural_count,
        "legitimate_less_than_8q_history": headline["legitimate_less_than_8q_history"],
        "no_missing_requirement_in_plan": no_missing,
        "theoretical_fully_clean_tickers_after_identified_repairs": counts["active_companies"] - headline["legitimate_less_than_8q_history"] - structural_count,
        "theoretical_fully_clean_pct": pct(counts["active_companies"] - headline["legitimate_less_than_8q_history"] - structural_count, counts["active_companies"]),
        "external_research_facts_required_total": len(external_queue),
        "local_repair_actions_required_total": len(local_queue),
        "structural_review_decisions_required_total": len(structural_queue),
    }
    safety = {
        "production_writes": 0,
        "rawcandle_writes": 0,
        "fingerprints_unchanged": before_fp == after_fp,
        "before": before_fp,
        "after": after_fp,
    }
    return {
        "classification": classification,
        "next_action": next_action,
        "artifact_root": str(artifact_root),
        "counts": counts,
        "headline": headline,
        "row_quality": row_quality(diagnostics),
        "issue_distribution": dists["issues"],
        "evidence_needed_distribution": dists["evidence"],
        "repairability_distribution": dists["repairability"],
        "repair_complexity_distribution": dists["complexity"],
        "priority_distribution": [{"priority": k, "tickers": v} for k, v in sorted(Counter(row["operational_priority"] for row in ticker_summary).items())],
        "downstream": downstream,
        "full_closure": full_closure,
        "safety": safety,
        "artifacts": {
            "quarter_level_csv": str(artifact_root / "latest8q_quarter_gap_detail.csv"),
            "ticker_level_csv": str(artifact_root / "latest8q_ticker_gap_summary.csv"),
            "local_repair_queue": str(artifact_root / "latest8q_local_repair_queue.csv"),
            "external_research_queue": str(artifact_root / "latest8q_external_research_queue.csv"),
            "structural_review_queue": str(artifact_root / "latest8q_structural_review_queue.csv"),
            "theoretical_closure_artifact": str(artifact_root / "latest8q_theoretical_closure_test.csv"),
            "known_13_artifact": str(artifact_root / "known_13_latest8q_gap_analysis.csv"),
            "summary_report": "docs/fundamentals_v3_latest8q_gap_analysis.md",
        },
    }


def row_quality(diagnostics: list[dict[str, Any]]) -> dict[str, Any]:
    issue_counts = [len(split_codes(row["issue_codes"])) for row in diagnostics]
    fiscal_clean = sum(row["fiscal_identity_status"] in {"FISCAL_IDENTITY_CLEAN_DIRECT_EXACT", "FISCAL_IDENTITY_CLEAN_SUPPORTED"} for row in diagnostics)
    primary = sum(int(row["clean_primary"]) for row in diagnostics)
    all_fields = sum(int(row["clean_all"]) for row in diagnostics)
    return {
        "latest8q_total_rows": len(diagnostics),
        "fiscal_identity_clean_rows": fiscal_clean,
        "primary_core_complete_rows": primary,
        "fully_complete_all_field_rows": all_fields,
        "rows_with_one_issue": sum(count == 1 for count in issue_counts),
        "rows_with_multiple_issues": sum(count > 1 for count in issue_counts),
    }


def write_report(path: Path, summary: dict[str, Any]) -> None:
    h = summary["headline"]
    rowq = summary["row_quality"]
    down = summary["downstream"]
    closure = summary["full_closure"]
    lines = [
        "# Fundamentals V3 Latest8Q Gap Analysis",
        "",
        "## Executive Summary",
        "",
        "| Metric | Count | % |",
        "| --- | ---: | ---: |",
        f"| Active tickers | {h['active_tickers']} | 100.0 |",
        f"| Already latest8Q fully clean | {h['already_latest8q_fully_clean']} | {pct(h['already_latest8q_fully_clean'], h['active_tickers'])} |",
        f"| Fully clean primary core | {h['fully_clean_primary_core']} | {pct(h['fully_clean_primary_core'], h['active_tickers'])} |",
        f"| All tracked fields complete | {h['all_tracked_fields_complete']} | {pct(h['all_tracked_fields_complete'], h['active_tickers'])} |",
        f"| Need local repair only | {h['need_local_repair_only']} | {pct(h['need_local_repair_only'], h['active_tickers'])} |",
        f"| Need external official evidence | {h['need_external_official_evidence']} | {pct(h['need_external_official_evidence'], h['active_tickers'])} |",
        f"| Need structural/manual review | {h['need_structural_manual_review']} | {pct(h['need_structural_manual_review'], h['active_tickers'])} |",
        f"| Legitimate <8Q history | {h['legitimate_less_than_8q_history']} | {pct(h['legitimate_less_than_8q_history'], h['active_tickers'])} |",
        f"| Current-impact P1 | {h['current_impact_p1']} | {pct(h['current_impact_p1'], h['active_tickers'])} |",
        "",
        "## Row Quality",
        "",
        f"- latest8Q total rows: `{rowq['latest8q_total_rows']}`",
        f"- fiscal-identity clean rows: `{rowq['fiscal_identity_clean_rows']}`",
        f"- primary-core complete rows: `{rowq['primary_core_complete_rows']}`",
        f"- fully complete all-field rows: `{rowq['fully_complete_all_field_rows']}`",
        f"- rows with one issue: `{rowq['rows_with_one_issue']}`",
        f"- rows with multiple issues: `{rowq['rows_with_multiple_issues']}`",
        "",
        "## Issue Type Summary",
        "",
        "| Issue | Rows |",
        "| --- | ---: |",
    ]
    lines.extend(f"| {row['issue_code']} | {row['rows']} |" for row in summary["issue_distribution"])
    lines += ["", "## Top Evidence Needed", "", "| Evidence needed | Rows |", "| --- | ---: |"]
    lines.extend(f"| {row['evidence_needed_code']} | {row['rows']} |" for row in summary["evidence_needed_distribution"][:10])
    lines += [
        "",
        "## Downstream",
        "",
        f"- TTM clean: `{down['ttm_clean']}`",
        f"- TTM affected/unavailable: `{down['ttm_affected_or_unavailable']}`",
        f"- Score available: `{down['score_available']}`",
        f"- Score blocked: `{down['score_blocked']}`",
        f"- Lifecycle available: `{down['lifecycle_available']}`",
        f"- Lifecycle blocked: `{down['lifecycle_blocked']}`",
        f"- Valuation available: `{down['valuation_available']}`",
        f"- Valuation blocked: `{down['valuation_blocked']}`",
        "",
        "## Full Closure Potential",
        "",
        f"- already fully clean: `{closure['already_fully_clean']}`",
        f"- repairable from local evidence only: `{closure['repairable_local_only']}`",
        f"- requiring external official evidence: `{closure['repairable_requiring_external']}`",
        f"- requiring structural/manual decision: `{closure['structural_decision_required']}`",
        f"- legitimate <8Q history: `{closure['legitimate_less_than_8q_history']}`",
        f"- NO_MISSING_REQUIREMENT_IN_PLAN: `{closure['no_missing_requirement_in_plan']}`",
        f"- theoretical fully-clean after all identified repairs: `{closure['theoretical_fully_clean_tickers_after_identified_repairs']}`",
        f"- theoretical fully-clean %: `{closure['theoretical_fully_clean_pct']}`",
        f"- external research facts required total: `{closure['external_research_facts_required_total']}`",
        f"- local repair actions required total: `{closure['local_repair_actions_required_total']}`",
        f"- structural-review decisions required total: `{closure['structural_review_decisions_required_total']}`",
        "",
        "## Artifacts",
        "",
    ]
    lines.extend(f"- {name}: `{value}`" for name, value in summary["artifacts"].items())
    lines += ["", "## Classification", "", f"`{summary['classification']}`", "", "## Next Action", "", summary["next_action"]]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def append_docs(summary: dict[str, Any]) -> None:
    marker = "## Phase 8F - Complete Latest-8Q Gap / Full-Closure Analysis"
    section = f"""

{marker}

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Latest8Q rows audited: `{summary['headline']['latest8q_rows']}` across `{summary['headline']['active_tickers']}` active tickers. Already fully clean tickers: `{summary['headline']['already_latest8q_fully_clean']}`. Primary-core clean tickers: `{summary['headline']['fully_clean_primary_core']}`. Structural/manual-review tickers: `{summary['headline']['need_structural_manual_review']}`. `NO_MISSING_REQUIREMENT_IN_PLAN`: `{summary['full_closure']['no_missing_requirement_in_plan']}`.

Safety proof: production writes `0`, RawCandle writes `0`, fingerprints unchanged `{summary['safety']['fingerprints_unchanged']}`.

Next action: {summary['next_action']}
"""
    for path in (Path("docs/fundamentals_v3_phase8_update_v3.md"), Path("docs/fundamentals_v3_deferred_repair_handoff.md")):
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
        if marker in existing:
            prefix = existing.split(marker, 1)[0].rstrip()
            path.write_text(prefix + section.rstrip() + "\n", encoding="utf-8")
        else:
            path.write_text(existing.rstrip() + section.rstrip() + "\n", encoding="utf-8")


def run_phase8f(paths: Phase8FPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    before_fp = production_fingerprints(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        counts = production_counts(conn)
    latest_rows, diagnostics, problems, ctx = build_quarter_diagnostics(paths.v3_db)
    ticker_summary = summarize_tickers(paths.v3_db, latest_rows, diagnostics, ctx["ttm_risk"])
    local_queue = local_repair_queue(ticker_summary, problems)
    external_queue = external_research_queue(problems)
    structural_queue = structural_review_queue(problems)
    after_fp = production_fingerprints(paths.v3_db)
    summary = build_summary(paths.artifact_root, counts, diagnostics, problems, ticker_summary, local_queue, external_queue, structural_queue, before_fp, after_fp)

    write_csv(paths.artifact_root / "latest8q_population.csv", latest8q_population_rows(latest_rows))
    write_json(paths.artifact_root / "latest8q_population_summary.json", {"active_tickers": counts["active_companies"], "latest8q_rows": len(latest_rows), "tickers": len({r["ticker"] for r in latest_rows})})
    write_csv(paths.artifact_root / "latest8q_quarter_gap_detail.csv", problems)
    dists = distributions(problems, ticker_summary)
    write_csv(paths.artifact_root / "latest8q_issue_distribution.csv", dists["issues"])
    write_csv(paths.artifact_root / "latest8q_field_completeness.csv", field_completeness_rows(diagnostics))
    write_csv(paths.artifact_root / "latest8q_sequence_issues.csv", [row for row in problems if row["sequence_status"] != "SEQUENCE_CLEAN"])
    write_csv(paths.artifact_root / "latest8q_ticker_gap_summary.csv", ticker_summary)
    write_csv(paths.artifact_root / "latest8q_downstream_blockers.csv", downstream_blockers(ticker_summary))
    write_csv(paths.artifact_root / "latest8q_repairability_distribution.csv", dists["repairability"])
    write_csv(paths.artifact_root / "latest8q_local_repair_queue.csv", local_queue)
    write_csv(paths.artifact_root / "latest8q_external_research_queue.csv", external_queue)
    write_csv(paths.artifact_root / "latest8q_structural_review_queue.csv", structural_queue)
    write_csv(paths.artifact_root / "latest8q_theoretical_closure_test.csv", theoretical_closure_rows(ticker_summary))
    write_json(paths.artifact_root / "latest8q_full_closure_summary.json", summary["full_closure"])
    write_csv(paths.artifact_root / "known_13_latest8q_gap_analysis.csv", known_13_rows(ticker_summary))
    write_json(paths.artifact_root / "phase8f_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if paths.write_documentation:
        write_report(Path("docs/fundamentals_v3_latest8q_gap_analysis.md"), summary)
        append_docs(summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 8F latest8Q full-closure gap analysis")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8f_latest8q_full_closure") / utc_stamp())
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--phase8e-root", type=Path, default=Path("temp/fundamentals_v3_phase8e_apply/20260829T_PHASE8E_APPLY"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_phase8f(Phase8FPaths(artifact_root=args.artifact_root, v3_db=args.v3_db, phase8e_root=args.phase8e_root))
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"active_tickers={summary['headline']['active_tickers']}")
    print(f"latest8q_rows={summary['headline']['latest8q_rows']}")
    print(f"already_fully_clean={summary['headline']['already_latest8q_fully_clean']}")
    print(f"no_missing_requirement_in_plan={summary['full_closure']['no_missing_requirement_in_plan']}")
    return 0 if summary["classification"] != CLASSIFICATION_INCOMPLETE else 2


if __name__ == "__main__":
    raise SystemExit(main())
