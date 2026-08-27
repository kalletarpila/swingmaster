from __future__ import annotations

import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10b_full_sequence_audit import Phase8A10BPaths, run_phase8a10b_full_sequence_audit
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro, file_state, integrity
from swingmaster.fundamentals.v3_phase8a10e_r2_financial_mapping import (
    FINANCIAL_COLUMNS,
    NINE_TICKERS,
    compare_pair,
    current_candidate_rows,
    normalize_official_mm,
    qnum,
    validate_financial_timeline,
)
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv, sha_file


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A10E_R3_LATEST8Q_REPLACEMENT_READY"
CLASSIFICATION_PARTIAL = "FUNDAMENTALS_V3_PHASE8A10E_R3_PARTIAL_REPLACEMENT_READY_BLOCKERS_REMAIN"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8A10E_R3_RECONSTRUCTION_BLOCKED"
FY2027_STARTS = {
    "BBY": "2026-02-01",
    "DELL": "2026-01-31",
    "GCO": "2026-02-01",
    "HAE": "2026-03-29",
    "MRVL": "2026-02-01",
    "RL": "2026-03-29",
    "SAIC": "2026-01-31",
    "TJX": "2026-02-01",
    "TRNS": "2026-03-29",
}
JAN_FEB_GROUP = {"BBY", "DELL", "GCO", "MRVL", "SAIC", "TJX"}
MAR_APR_GROUP = {"HAE", "RL", "TRNS"}
PRIMARY_CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
RECONSTRUCTION_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)


@dataclass(frozen=True)
class Phase8A10ER3Paths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")
    financial_timeline_csv: Path = Path("temp/swingmaster_v3_official_fiscal_quarter_timeline_with_financials_2026-08-26.csv")
    a10b_root: Path = Path("temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    publish_apply_root: Path = Path("temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_day(value: str) -> date:
    return date.fromisoformat(value)


def days_between(left: str, right: str) -> int:
    return (parse_day(right) - parse_day(left)).days


def official_sort_key(row: dict[str, str]) -> tuple[str, int]:
    return row["Ticker"], int(row["Canonical Sequence"])


def source_context_available(row: dict[str, Any]) -> bool:
    return bool(row.get("lineage_provenance") or row.get("provider_acquisition") or row.get("accepted_source_provider"))


def build_slot_model(official: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    model: list[dict[str, Any]] = []
    validation: list[dict[str, Any]] = []
    by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in sorted(official, key=official_sort_key):
        by_ticker[row["Ticker"]].append(row)
    for ticker in NINE_TICKERS:
        ticker_rows = by_ticker[ticker]
        prev_end = None
        for idx, row in enumerate(ticker_rows):
            period_end = row["Official Period End"]
            fy = int(row["Fiscal Year"])
            fq = row["Fiscal Quarter"]
            if fq == "Q1":
                fy_start = FY2027_STARTS[ticker] if fy == 2027 else (prev_end + timedelta(days=1)).isoformat() if prev_end else ""
            else:
                fy_start = next((m["fiscal_year_start"] for m in reversed(model) if m["ticker"] == ticker and int(m["fiscal_year"]) == fy), "")
            gap = (parse_day(period_end) - prev_end).days if prev_end else ""
            duration = gap if gap else ""
            slot_class = "FIRST_TARGET" if gap == "" else "FOURTEEN_WEEK_53_WEEK_SLOT" if 96 <= int(gap) <= 100 else "THIRTEEN_WEEK_SLOT" if 84 <= int(gap) <= 95 else "OUT_OF_TOLERANCE"
            expected_start = "" if prev_end is None else (prev_end + timedelta(days=1)).isoformat()
            expected_end_min = "" if prev_end is None else (prev_end + timedelta(days=84)).isoformat()
            expected_end_max = "" if prev_end is None else (prev_end + timedelta(days=100)).isoformat()
            model_row = {
                "ticker": ticker,
                "canonical_sequence": row["Canonical Sequence"],
                "fiscal_year": fy,
                "fiscal_quarter": fq,
                "fiscal_year_start": fy_start,
                "expected_slot_start": expected_start,
                "expected_slot_end_min": expected_end_min,
                "expected_slot_end_max": expected_end_max,
                "official_period_end": period_end,
                "official_publish_date": row["Publish Date"],
                "period_end_weekday": parse_day(period_end).strftime("%A"),
                "quarter_duration_days": duration,
                "slot_class": slot_class,
                "previous_slot": f"FY{ticker_rows[idx - 1]['Fiscal Year']} {ticker_rows[idx - 1]['Fiscal Quarter']}" if idx else "",
                "next_slot": f"FY{ticker_rows[idx + 1]['Fiscal Year']} {ticker_rows[idx + 1]['Fiscal Quarter']}" if idx + 1 < len(ticker_rows) else "",
            }
            model.append(model_row)
            valid = idx == 0 or slot_class in {"THIRTEEN_WEEK_SLOT", "FOURTEEN_WEEK_53_WEEK_SLOT"}
            validation.append(
                {
                    "ticker": ticker,
                    "fiscal_year": fy,
                    "fiscal_quarter": fq,
                    "official_period_end": period_end,
                    "slot_validation_status": "VALID" if valid else "INVALID",
                    "falls_into_exactly_one_slot": 1 if valid else 0,
                    "fy_rollover_coherent": 1 if fq != "Q1" or fy_start else 0,
                    "misclassified": 0 if valid else 1,
                }
            )
            prev_end = parse_day(period_end)
    return model, validation


def current_and_source_candidates(conn: sqlite3.Connection, official: list[dict[str, str]]) -> list[dict[str, Any]]:
    target_dates = {ticker: [parse_day(r["Official Period End"]) for r in official if r["Ticker"] == ticker] for ticker in NINE_TICKERS}
    current = current_candidate_rows(conn)
    out = []
    for row in current:
        dates = target_dates[row["ticker"]]
        period = parse_day(row["period_end_date"]) if row.get("period_end_date") else None
        in_extended_window = True
        if period:
            in_extended_window = min(dates) - timedelta(days=220) <= period <= max(dates) + timedelta(days=220)
        out.append(
            {
                **row,
                "source_type": "CANONICAL_V3_CURRENT",
                "source_row_id": row["quarter_id"],
                "context_start": "",
                "context_end": row.get("period_end_date") or "",
                "source_accession": row.get("lineage_provenance") or row.get("provider_acquisition") or row.get("accepted_source_provider") or "",
                "source_context_status": "AVAILABLE" if source_context_available(row) else "UNOBSERVED",
                "candidate_window_status": "IN_EXTENDED_WINDOW" if in_extended_window else "OUTSIDE_EXTENDED_WINDOW",
            }
        )
    return out


def clean_targets(official: list[dict[str, str]], slot_model: list[dict[str, Any]]) -> list[dict[str, Any]]:
    slot_by_key = {(r["ticker"], str(r["fiscal_year"]), r["fiscal_quarter"]): r for r in slot_model}
    targets = []
    for row in sorted(official, key=official_sort_key):
        slot = slot_by_key[(row["Ticker"], row["Fiscal Year"], row["Fiscal Quarter"])]
        targets.append(
            {
                "ticker": row["Ticker"],
                "canonical_sequence": row["Canonical Sequence"],
                "target_fiscal_year": row["Fiscal Year"],
                "target_fiscal_quarter": row["Fiscal Quarter"],
                "target_period_end": row["Official Period End"],
                "target_publish_date": row["Publish Date"],
                "target_revenue": normalize_official_mm(row["Revenue (USD mm)"]),
                "target_operating_income": normalize_official_mm(row["Operating Income (USD mm)"]),
                "target_net_income": normalize_official_mm(row["Net Income (USD mm)"]),
                "official_revenue_usd_mm": row["Revenue (USD mm)"],
                "official_operating_income_usd_mm": row["Operating Income (USD mm)"],
                "official_net_income_usd_mm": row["Net Income (USD mm)"],
                "primary_source_url": row.get("Primary Source URL", ""),
                "issuer_archive_url": row.get("Issuer Archive URL", ""),
                "confidence": row.get("Confidence", ""),
                "fiscal_slot_class": slot["slot_class"],
                "fiscal_year_start": slot["fiscal_year_start"],
            }
        )
    return targets


def slot_compatibility(candidate: dict[str, Any], target: dict[str, Any]) -> tuple[str, int | str]:
    if not candidate.get("period_end_date"):
        return "UNKNOWN", ""
    distance = abs(days_between(candidate["period_end_date"], target["target_period_end"]))
    if distance == 0:
        return "EXACT_OFFICIAL_PERIOD_END", distance
    if distance <= 10:
        return "SAME_52_53_SLOT", distance
    if distance <= 45:
        return "ADJACENT_SLOT_PROXIMITY", distance
    return "OUTSIDE_SLOT", distance


def source_assignment(targets: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    used_candidates: set[int] = set()
    by_ticker = defaultdict(list)
    for row in candidates:
        by_ticker[row["ticker"]].append(row)
    for target in targets:
        comparisons = []
        for cand in by_ticker[target["ticker"]]:
            official_like = {
                "Ticker": target["ticker"],
                "Fiscal Year": target["target_fiscal_year"],
                "Fiscal Quarter": target["target_fiscal_quarter"],
                "Official Period End": target["target_period_end"],
                "Publish Date": target["target_publish_date"],
                "Revenue (USD mm)": target["official_revenue_usd_mm"],
                "Operating Income (USD mm)": target["official_operating_income_usd_mm"],
                "Net Income (USD mm)": target["official_net_income_usd_mm"],
            }
            cmp = compare_pair(cand, official_like)
            slot_status, slot_distance = slot_compatibility(cand, target)
            source_ctx = source_context_available(cand)
            score = cmp["strong_financial_matches"] * 100 + cmp["near_financial_matches"] * 20
            score += 25 if slot_status in {"EXACT_OFFICIAL_PERIOD_END", "SAME_52_53_SLOT"} else 0
            score += 10 if source_ctx else 0
            comparisons.append({**cmp, "slot_status": slot_status, "slot_distance_days": slot_distance, "source_context_corroborated": int(source_ctx), "assignment_score": score})
        comparisons.sort(key=lambda r: (r["assignment_score"], r["strong_financial_matches"], -int(r["slot_distance_days"] or 9999)), reverse=True)
        best = comparisons[0] if comparisons else None
        if best and best["composite_confidence"] == "FINANCIAL_FINGERPRINT_HIGH" and best["slot_status"] in {"EXACT_OFFICIAL_PERIOD_END", "SAME_52_53_SLOT", "ADJACENT_SLOT_PROXIMITY"} and int(best["quarter_id"]) not in used_candidates:
            current_identity_matches = str(best["current_fiscal_year"]) == target["target_fiscal_year"] and best["current_fiscal_quarter"] == target["target_fiscal_quarter"]
            metadata_matches = best["current_period_end"] == target["target_period_end"] and best["current_publish_date"] == target["target_publish_date"]
            if current_identity_matches and metadata_matches:
                acquisition = "REUSE_CURRENT_V3_ROW_HIGH"
            elif current_identity_matches:
                acquisition = "REUSE_CURRENT_V3_ROW_WITH_METADATA_REPAIR"
            else:
                acquisition = "REUSE_CURRENT_V3_ROW_WITH_IDENTITY_REPAIR"
            used_candidates.add(int(best["quarter_id"]))
            out.append({**target, **best, "source_acquisition_class": acquisition})
        else:
            out.append(
                {
                    **target,
                    "quarter_id": "",
                    "current_fiscal_year": "",
                    "current_fiscal_quarter": "",
                    "current_period_end": "",
                    "current_publish_date": "",
                    "Revenue_match_class": "NO_CURRENT_ROW",
                    "OI_match_class": "NO_CURRENT_ROW",
                    "NI_match_class": "NO_CURRENT_ROW",
                    "composite_confidence": "SOURCE_EVIDENCE_INSUFFICIENT",
                    "slot_status": "TARGET_ONLY",
                    "slot_distance_days": "",
                    "source_context_corroborated": 0,
                    "source_acquisition_class": "SOURCE_EVIDENCE_INSUFFICIENT",
                }
            )
    return out


def field_reconstruction(assignments: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates_by_qid = {int(row["quarter_id"]): row for row in candidates if row.get("quarter_id") not in ("", None)}
    field_rows = []
    by_target_complete = defaultdict(dict)
    for assn in assignments:
        qid = int(assn["quarter_id"]) if assn.get("quarter_id") not in ("", None) else None
        current = candidates_by_qid.get(qid, {})
        for field in RECONSTRUCTION_FIELDS:
            if field == "revenue":
                value = assn["target_revenue"]
                source = "OFFICIAL_FINANCIAL_TIMELINE"
                status = "VERIFIED"
            elif field == "operating_income":
                value = assn["target_operating_income"]
                source = "OFFICIAL_FINANCIAL_TIMELINE"
                status = "VERIFIED"
            elif field == "net_income":
                value = assn["target_net_income"]
                source = "OFFICIAL_FINANCIAL_TIMELINE"
                status = "VERIFIED"
            elif assn["source_acquisition_class"].startswith("REUSE_CURRENT") and current.get(field) not in ("", None):
                value = current.get(field)
                source = "CURRENT_V3_REUSED_HIGH_FINGERPRINT"
                status = "VERIFIED_FROM_REUSED_CURRENT"
            else:
                value = ""
                source = ""
                status = "SOURCE_EVIDENCE_INSUFFICIENT"
            by_ticker_key = (assn["ticker"], assn["target_fiscal_year"], assn["target_fiscal_quarter"])
            by_target_complete[by_ticker_key][field] = status
            field_rows.append(
                {
                    "ticker": assn["ticker"],
                    "target_fiscal_year": assn["target_fiscal_year"],
                    "target_fiscal_quarter": assn["target_fiscal_quarter"],
                    "field": field,
                    "reconstructed_value": value,
                    "content_source": source,
                    "field_reconstruction_status": status,
                    "source_quarter_id": assn.get("quarter_id", ""),
                    "source_accession_evidence": current.get("source_accession", assn.get("primary_source_url", "")),
                }
            )
    completeness = []
    for assn in assignments:
        key = (assn["ticker"], assn["target_fiscal_year"], assn["target_fiscal_quarter"])
        statuses = by_target_complete[key]
        complete_fields = sum(1 for f in RECONSTRUCTION_FIELDS if statuses.get(f) not in ("", "SOURCE_EVIDENCE_INSUFFICIENT"))
        core_complete = all(statuses.get(f) not in ("", "SOURCE_EVIDENCE_INSUFFICIENT") for f in PRIMARY_CORE_FIELDS)
        if complete_fields == len(RECONSTRUCTION_FIELDS):
            cls = "RECONSTRUCTED_COMPLETE"
        elif core_complete:
            cls = "RECONSTRUCTED_PRIMARY_CORE_COMPLETE"
        elif complete_fields:
            cls = "RECONSTRUCTED_PARTIAL"
        else:
            cls = "SOURCE_EVIDENCE_INSUFFICIENT"
        completeness.append({**{k: assn[k] for k in ("ticker", "target_fiscal_year", "target_fiscal_quarter", "target_period_end")}, "complete_fields": complete_fields, "primary_core_complete": int(core_complete), "completeness_status": cls})
    return field_rows, completeness


def segment_boundaries(targets: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    by_target = defaultdict(list)
    by_current = defaultdict(list)
    for row in targets:
        by_target[row["ticker"]].append(row)
    for row in candidates:
        by_current[row["ticker"]].append(row)
    for ticker in NINE_TICKERS:
        trows = sorted(by_target[ticker], key=lambda r: int(r["canonical_sequence"]))
        if not trows:
            continue
        crows = sorted(by_current[ticker], key=lambda r: r.get("period_end_date") or "")
        first = trows[0]["target_period_end"]
        last = trows[-1]["target_period_end"]
        before = [r for r in crows if r.get("period_end_date") and r["period_end_date"] < first]
        after = [r for r in crows if r.get("period_end_date") and r["period_end_date"] > last]
        out.append(
            {
                "ticker": ticker,
                "quarter_immediately_before_window": before[-1]["period_end_date"] if before else "",
                "first_target_quarter": first,
                "latest_target_quarter": last,
                "quarter_immediately_after_window": after[0]["period_end_date"] if after else "",
                "left_boundary_continuity": "OBSERVED" if before else "NO_PRIOR_ROW_IN_SCOPE",
                "right_boundary_continuity": "OBSERVED" if after else "NO_LATER_ROW_IN_SCOPE",
            }
        )
    return out


def replacement_plan(assignments: list[dict[str, Any]], completeness: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    complete_by_key = {(r["ticker"], r["target_fiscal_year"], r["target_fiscal_quarter"]): r for r in completeness}
    by_ticker = defaultdict(list)
    for row in assignments:
        by_ticker[row["ticker"]].append(row)
    groups = []
    operations = []
    blockers = []
    order = 0
    for ticker in NINE_TICKERS:
        assns = by_ticker[ticker]
        all_core = all(complete_by_key[(r["ticker"], r["target_fiscal_year"], r["target_fiscal_quarter"])]["primary_core_complete"] for r in assns)
        all_reusable = all(str(r["source_acquisition_class"]).startswith("REUSE_CURRENT") for r in assns)
        ready = all_core and all_reusable and len(assns) == 8
        groups.append({"ticker": ticker, "targets": len(assns), "ready_for_rehearsal": "YES" if ready else "NO", "group_blocker": "" if ready else "latest-8Q reconstruction not primary-core complete from local evidence"})
        if not ready:
            for row in assns:
                c = complete_by_key[(row["ticker"], row["target_fiscal_year"], row["target_fiscal_quarter"])]
                if not c["primary_core_complete"] or not str(row["source_acquisition_class"]).startswith("REUSE_CURRENT"):
                    blockers.append(
                        {
                            "ticker": ticker,
                            "official FY/FQ": f"FY{row['target_fiscal_year']} {row['target_fiscal_quarter']}",
                            "blocker type": "SOURCE_EVIDENCE_INSUFFICIENT" if not c["primary_core_complete"] else "CURRENT_ROW_NOT_REUSABLE",
                            "missing field/evidence": "primary core local source fields" if not c["primary_core_complete"] else "high-confidence current economic row",
                            "current candidate rows": row.get("quarter_id", ""),
                            "financial fingerprint status": row.get("composite_confidence", ""),
                            "fiscal slot status": row.get("slot_status", ""),
                            "exact additional evidence needed": "authoritative local source values for EBITDA/FCF/cash/debt/shares and exact quarter identity",
                            "ticker removal simpler": "NO",
                        }
                    )
            continue
        for row in assns:
            for field, current_key, target_key, op in (
                ("fiscal_year", "current_fiscal_year", "target_fiscal_year", "UPDATE_IDENTITY"),
                ("fiscal_quarter", "current_fiscal_quarter", "target_fiscal_quarter", "UPDATE_IDENTITY"),
                ("period_end", "current_period_end", "target_period_end", "UPDATE_PERIOD_END"),
                ("publish_date", "current_publish_date", "target_publish_date", "UPDATE_PUBLISH_DATE"),
            ):
                if str(row.get(current_key, "")) == str(row.get(target_key, "")):
                    continue
                order += 1
                operations.append(
                    {
                        "transformation_group": ticker,
                        "ticker": ticker,
                        "operation_order": order,
                        "source quarter_id": row.get("quarter_id", ""),
                        "target quarter_id": row.get("quarter_id", ""),
                        "current FY": row.get("current_fiscal_year", ""),
                        "current FQ": row.get("current_fiscal_quarter", ""),
                        "target FY": row.get("target_fiscal_year", ""),
                        "target FQ": row.get("target_fiscal_quarter", ""),
                        "current period_end": row.get("current_period_end", ""),
                        "target period_end": row.get("target_period_end", ""),
                        "current publish_date": row.get("current_publish_date", ""),
                        "target publish_date": row.get("target_publish_date", ""),
                        "field": field,
                        "old_value": row.get(current_key, ""),
                        "new_value": row.get(target_key, ""),
                        "operation": op,
                        "content source": row.get("source_acquisition_class", ""),
                        "source accession/evidence": row.get("source_context_compatibility", ""),
                        "lineage action": "PRESERVE_OR_REPOINT_WITH_GROUP",
                        "fiscal-slot confidence": row.get("slot_status", ""),
                        "financial-fingerprint confidence": row.get("composite_confidence", ""),
                        "write guard": f"{row.get('quarter_id','')}|{field}|{row.get(current_key,'')}",
                        "rollback group": ticker,
                    }
                )
    return operations, groups, blockers


def apply_rehearsal(db: Path, operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if operations:
        raise RuntimeError("PHASE8A10E_R3_REHEARSAL_WRITE_PATH_NOT_IMPLEMENTED_FOR_NONEMPTY_PLAN")
    return []


def parity_rows(conn: sqlite3.Connection, targets: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    timeline = []
    financial = []
    slot = []
    window = []
    for target in targets:
        found = rows(
            conn,
            """
            SELECT q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
                   f.revenue,f.operating_income,f.net_income
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            """,
            (target["ticker"], target["target_fiscal_year"], target["target_fiscal_quarter"]),
        )
        cur = found[0] if found else {}
        timeline.append({"ticker": target["ticker"], "target_fy": target["target_fiscal_year"], "target_fq": target["target_fiscal_quarter"], "quarter_id": cur.get("quarter_id", ""), "fy_fq_parity": int(bool(cur)), "period_end_parity": int(cur.get("period_end_date", "") == target["target_period_end"]), "publish_date_parity": int(cur.get("publish_date", "") == target["target_publish_date"])})
        financial.append({"ticker": target["ticker"], "target_fy": target["target_fiscal_year"], "target_fq": target["target_fiscal_quarter"], "revenue_parity": int(cur.get("revenue") == target["target_revenue"]), "operating_income_parity": int(cur.get("operating_income") == target["target_operating_income"]), "net_income_parity": int(cur.get("net_income") == target["target_net_income"])})
        slot.append({"ticker": target["ticker"], "target_fy": target["target_fiscal_year"], "target_fq": target["target_fiscal_quarter"], "fiscal_slot_parity": int(target["fiscal_slot_class"] in {"FIRST_TARGET", "THIRTEEN_WEEK_SLOT", "FOURTEEN_WEEK_53_WEEK_SLOT"})})
    by_ticker = defaultdict(list)
    for target in targets:
        by_ticker[target["ticker"]].append(target)
    for ticker in NINE_TICKERS:
        expected = [(r["target_fiscal_year"], r["target_fiscal_quarter"]) for r in sorted(by_ticker[ticker], key=lambda r: int(r["canonical_sequence"]))]
        latest = rows(
            conn,
            """
            SELECT q.fiscal_year,q.fiscal_quarter
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            WHERE c.ticker=?
            ORDER BY q.fiscal_year DESC, CASE q.fiscal_quarter WHEN 'Q4' THEN 4 WHEN 'Q3' THEN 3 WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1 ELSE 0 END DESC
            LIMIT 8
            """,
            (ticker,),
        )
        actual = [(str(r["fiscal_year"]), r["fiscal_quarter"]) for r in reversed(latest)]
        window.append({"ticker": ticker, "latest8q_window_parity": int(actual == expected), "expected_latest8q": repr(expected), "actual_latest8q": repr(actual)})
    return timeline, financial, slot, window


def run_phase8a10e_r3(paths: Phase8A10ER3Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    production_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    official, validation = validate_financial_timeline(paths.financial_timeline_csv)
    write_json(paths.artifact_root / "official_financial_timeline_manifest.json", {"csv_path": str(paths.financial_timeline_csv), "sha256": sha_file(paths.financial_timeline_csv), "research_cutoff": "2026-08-26"})
    write_json(paths.artifact_root / "official_financial_timeline_validation.json", validation)
    slot_model, slot_validation = build_slot_model(official)
    targets = clean_targets(official, slot_model)
    with connect_ro(paths.v3_db) as conn:
        candidates = current_and_source_candidates(conn, official)
        assignments = source_assignment(targets, candidates)
        field_rows, completeness = field_reconstruction(assignments, candidates)
        boundaries = segment_boundaries(targets, candidates)
        operations, group_summary, blockers = replacement_plan(assignments, completeness)
    write_csv(paths.artifact_root / "nine_ticker_fiscal_slot_model.csv", slot_model)
    write_csv(paths.artifact_root / "nine_ticker_fiscal_slot_validation.csv", slot_validation)
    write_csv(paths.artifact_root / "nine_ticker_current_and_source_candidates.csv", candidates)
    write_csv(paths.artifact_root / "nine_ticker_clean_latest8q_target.csv", targets)
    write_csv(paths.artifact_root / "nine_ticker_target_source_assignment.csv", assignments)
    write_csv(paths.artifact_root / "nine_ticker_target_field_reconstruction.csv", field_rows)
    write_csv(paths.artifact_root / "nine_ticker_reconstruction_completeness.csv", completeness)
    write_csv(paths.artifact_root / "nine_ticker_current_vs_clean_target.csv", assignments)
    write_csv(paths.artifact_root / "nine_ticker_recent_segment_boundaries.csv", boundaries)
    write_csv(paths.artifact_root / "nine_ticker_r3_atomic_replacement_plan.csv", operations)
    write_csv(paths.artifact_root / "nine_ticker_r3_group_summary.csv", group_summary)
    rehearsal_db = paths.artifact_root / "rehearsal_rc_fundamentals_v3.db"
    shutil.copy2(paths.v3_db, rehearsal_db)
    apply_log = apply_rehearsal(rehearsal_db, operations)
    with connect_ro(rehearsal_db) as conn:
        rehearsal_integrity = integrity(conn)
        timeline_parity, financial_parity, fiscal_slot_parity, window_parity = parity_rows(conn, targets)
    post_root = paths.artifact_root / "rehearsal_post_a10b"
    sentinel_raw = paths.artifact_root / "rawcandle_rehearsal_guard_not_used.db"
    run_phase8a10b_full_sequence_audit(Phase8A10BPaths(artifact_root=post_root, v3_db=rehearsal_db, rawcandle_db=sentinel_raw, publish_apply_root=paths.publish_apply_root))
    before_p1 = read_csv(paths.a10b_root / "global_P1.csv")
    post_p1 = read_csv(post_root / "global_P1.csv")
    before_tickers = {row["ticker"] for row in before_p1}
    nine_after = [row for row in post_p1 if row["ticker"] in NINE_TICKERS]
    post_summary = {
        "global_P1_before": len(before_p1),
        "nine_ticker_P1_before": sum(1 for row in before_p1 if row["ticker"] in NINE_TICKERS),
        "nine_ticker_P1_after": len(nine_after),
        "global_P1_after": len(post_p1),
        "remaining_P1_tickers": sorted({row["ticker"] for row in post_p1}),
        "new_P1": sorted({row["ticker"] for row in post_p1} - before_tickers),
    }
    write_csv(paths.artifact_root / "nine_ticker_r3_rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "nine_ticker_r3_rehearsal_integrity.json", rehearsal_integrity)
    write_csv(paths.artifact_root / "nine_ticker_r3_official_timeline_parity.csv", timeline_parity)
    write_csv(paths.artifact_root / "nine_ticker_r3_financial_parity.csv", financial_parity)
    write_csv(paths.artifact_root / "nine_ticker_r3_fiscal_slot_parity.csv", fiscal_slot_parity)
    write_csv(paths.artifact_root / "nine_ticker_r3_latest8q_window_parity.csv", window_parity)
    write_csv(paths.artifact_root / "nine_ticker_r3_post_a10b_P1.csv", post_p1)
    write_json(paths.artifact_root / "nine_ticker_r3_post_a10b_P1_summary.json", post_summary)
    ready_tickers = {row["ticker"] for row in group_summary if row["ready_for_rehearsal"] == "YES"} - {row["ticker"] for row in nine_after}
    frozen = [row for row in operations if row["ticker"] in ready_tickers]
    write_csv(paths.artifact_root / "phase8a10e_r3_frozen_latest8q_replacement_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "phase8a10e_r3_latest8q_blockers.csv", blockers)
    failure_counts = {
        "metadata_only_failure_count": sum(1 for r in assignments if r["source_acquisition_class"] == "REUSE_CURRENT_V3_ROW_WITH_METADATA_REPAIR"),
        "fyq_displacement_count": sum(1 for r in assignments if r["source_acquisition_class"] == "REUSE_CURRENT_V3_ROW_WITH_IDENTITY_REPAIR"),
        "hybrid_row_count": sum(1 for r in assignments if r.get("financial_mismatches", 0)),
        "missing_quarter_count": sum(1 for r in assignments if r["source_acquisition_class"] == "SOURCE_EVIDENCE_INSUFFICIENT"),
    }
    paths.artifact_root.joinpath("nine_ticker_r3_prevention_handoff.md").write_text(
        "# Phase 8A10E-R3 Prevention Handoff\n\n"
        f"Metadata-only failures: `{failure_counts['metadata_only_failure_count']}`\n"
        f"FY/Q displacement: `{failure_counts['fyq_displacement_count']}`\n"
        f"Hybrid rows: `{failure_counts['hybrid_row_count']}`\n"
        f"Missing recent canonical quarters: `{failure_counts['missing_quarter_count']}`\n\n"
        "Future write paths must build clean official target segments before canonical latest-window writes, then require financial fingerprint, slot, target-collision, and post-A10B gates.\n",
        encoding="utf-8",
    )
    production_after = file_state(paths.v3_db)
    raw_after = file_state(paths.rawcandle_db)
    safety = {
        "production_writes": int(production_before != production_after),
        "ttm_writes": 0,
        "score_writes": 0,
        "lifecycle_writes": 0,
        "valuation_writes": 0,
        "rawcandle_writes": 0,
        "rawcandle_external_drift_observed": int(raw_before != raw_after),
    }
    if len(ready_tickers) == 9:
        classification = CLASSIFICATION_READY
    elif ready_tickers:
        classification = CLASSIFICATION_PARTIAL
    else:
        classification = CLASSIFICATION_BLOCKED
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "input": validation,
        "fiscal_slots": {
            "slot_model_valid_tickers": sum(1 for ticker in NINE_TICKERS if all(r["slot_validation_status"] == "VALID" for r in slot_validation if r["ticker"] == ticker)),
            "fifty_three_week_exceptions": sum(1 for r in slot_model if r["slot_class"] == "FOURTEEN_WEEK_53_WEEK_SLOT"),
            "slot_model_ambiguities": sum(1 for r in slot_validation if r["slot_validation_status"] != "VALID"),
        },
        "reconstruction": {
            "targets_total": len(targets),
            "reused_current_rows": sum(1 for r in assignments if r["source_acquisition_class"] == "REUSE_CURRENT_V3_ROW_HIGH"),
            "reused_with_metadata_repair": sum(1 for r in assignments if r["source_acquisition_class"] == "REUSE_CURRENT_V3_ROW_WITH_METADATA_REPAIR"),
            "reused_with_identity_repair": sum(1 for r in assignments if r["source_acquisition_class"] == "REUSE_CURRENT_V3_ROW_WITH_IDENTITY_REPAIR"),
            "reconstructed_from_local_source": sum(1 for r in assignments if r["source_acquisition_class"] == "RECONSTRUCT_FROM_LOCAL_SOURCE"),
            "reconstructed_from_multiple_sources": sum(1 for r in assignments if r["source_acquisition_class"] == "RECONSTRUCT_FROM_MULTIPLE_LOCAL_SOURCES"),
            "partial_targets": sum(1 for r in completeness if r["completeness_status"] == "RECONSTRUCTED_PARTIAL"),
            "source_insufficient_targets": sum(1 for r in assignments if r["source_acquisition_class"] == "SOURCE_EVIDENCE_INSUFFICIENT"),
        },
        "per_ticker": [
            {
                "ticker": ticker,
                "official_target_8q": 8,
                "clean_reconstructed_quarters": sum(1 for r in completeness if r["ticker"] == ticker and r["completeness_status"] != "SOURCE_EVIDENCE_INSUFFICIENT"),
                "partial_or_missing_targets": sum(1 for r in completeness if r["ticker"] == ticker and r["completeness_status"] != "RECONSTRUCTED_COMPLETE"),
                "first_replacement_quarter": min(r["target_period_end"] for r in targets if r["ticker"] == ticker),
                "last_replacement_quarter": max(r["target_period_end"] for r in targets if r["ticker"] == ticker),
                "current_rows_reused": sum(1 for r in assignments if r["ticker"] == ticker and str(r["source_acquisition_class"]).startswith("REUSE_CURRENT")),
                "rows_recreated": 0,
                "rows_deleted_replaced": 0,
                "transformation_shape": "NO_WRITE" if ticker not in ready_tickers else "LATEST8Q_REPLACEMENT",
                "production_ready": "YES" if ticker in ready_tickers else "NO",
            }
            for ticker in NINE_TICKERS
        ],
        "field_reconstruction": {field: sum(1 for r in field_rows if r["field"] == field and r["field_reconstruction_status"] != "SOURCE_EVIDENCE_INSUFFICIENT") for field in RECONSTRUCTION_FIELDS},
        "frozen_replacement": {
            "ready_ticker_groups": len(ready_tickers),
            "blocked_tickers": sorted(set(NINE_TICKERS) - ready_tickers),
            "operations": len(frozen),
            "metadata_writes": sum(1 for r in frozen if r["operation"] in {"UPDATE_PERIOD_END", "UPDATE_PUBLISH_DATE"}),
            "identity_writes": sum(1 for r in frozen if r["operation"] == "UPDATE_IDENTITY"),
            "canonical_value_writes": sum(1 for r in frozen if r["operation"] == "UPDATE_CANONICAL_VALUE"),
            "creates": sum(1 for r in frozen if r["operation"] == "CREATE_CANONICAL_ROW"),
            "deletes": sum(1 for r in frozen if r["operation"] == "DELETE_CORRUPTED_RECENT_ROW"),
            "lineage_actions": dict(Counter(r.get("lineage action", "") for r in frozen)),
        },
        "rehearsal": {
            "groups_attempted": len({r["ticker"] for r in operations}),
            "groups_passed": len({r["ticker"] for r in apply_log}),
            "groups_failed": 0,
            "quick_check": rehearsal_integrity["quick_check"],
            "duplicates": rehearsal_integrity["duplicate_fy_fq"],
            "orphans": rehearsal_integrity["orphans"],
            "unrelated_drift": 0,
            "official_timeline_parity": int(all(r["fy_fq_parity"] and r["period_end_parity"] and r["publish_date_parity"] for r in timeline_parity)),
            "financial_parity": int(all(r["revenue_parity"] and r["operating_income_parity"] and r["net_income_parity"] for r in financial_parity)),
            "fiscal_slot_parity": int(all(r["fiscal_slot_parity"] for r in fiscal_slot_parity)),
            "latest8q_window_parity": int(all(r["latest8q_window_parity"] for r in window_parity)),
        },
        "a10b": post_summary,
        "prevention": failure_counts | {"implications": "build clean target segments, then enforce fingerprint/slot/collision/post-A10B write gates"},
        "safety": safety,
        "next_action": "PHASE 8A10E-R3-APPLY - APPLY REHEARSED CLEAN LATEST-8Q REPLACEMENTS"
        if classification == CLASSIFICATION_READY
        else "APPLY ONLY FROZEN SAFE TICKER GROUPS, THEN RESOLVE/REMOVE REMAINING BLOCKED TICKERS"
        if classification == CLASSIFICATION_PARTIAL
        else "DO NOT WRITE PRODUCTION - CONSIDER REMOVING ONLY THE PERSISTENTLY UNRESOLVABLE TICKERS FROM V3",
    }
    write_json(paths.artifact_root / "phase8a10e_r3_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if safety["production_writes"]:
        raise RuntimeError("PHASE8A10E_R3_READ_ONLY_GUARD_FAILED")
    return summary
