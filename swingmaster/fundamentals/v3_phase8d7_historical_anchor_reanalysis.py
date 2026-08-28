from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import (
    ANCHOR_TABLE,
    CHAIN_TABLE,
    EXPECTED_P1_TICKERS,
    PROFILE_TABLE,
    semantic_fingerprints,
    utc_stamp,
)
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro
from swingmaster.fundamentals.v3_phase8d2_operational_risk import read_csv_dicts
from swingmaster.fundamentals.v3_phase8d4_slot_model_rework import Q_INDEX, add_months, parse_date, resolve_extra_week, week_slots


CLASSIFICATION_MATERIAL = "HISTORICAL_EXACT_ANCHORS_MATERIALLY_RESOLVE_FISCAL_IDENTITY_RISK"
CLASSIFICATION_PARTIAL = "HISTORICAL_EXACT_ANCHORS_PARTIALLY_RESOLVE_RISK"
CLASSIFICATION_NO_CHANGE = "HISTORICAL_EXACT_ANCHORS_DO_NOT_MATERIALLY_CHANGE_RISK"
KNOWN_GOOD_HIGH = 11107
OLD_HISTORICAL_BLOCK = 12604
OLD_BACKWARD_INFERENCE_BLOCK = 11291
OLD_EXACT_ANCHOR_BLOCK = 833
KNOWN_13 = set(EXPECTED_P1_TICKERS)


@dataclass(frozen=True)
class Phase8D7Paths:
    artifact_root: Path
    phase8d1_root: Path = Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL")
    phase8d4_root: Path = Path("temp/fundamentals_v3_phase8d4_slot_model_rework/20260828T_PHASE8D4")
    phase8d6_root: Path = Path("temp/fundamentals_v3_phase8d6_label_provenance_audit/20260828T_PHASE8D6")
    v3_db: Path = Path("rc_fundamentals_v3.db")


def pct(part: int, whole: int) -> float:
    return round(part * 100 / whole, 4) if whole else 0.0


def load_profiles(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    return {int(row["company_id"]): row for row in rows(conn, f"SELECT * FROM {PROFILE_TABLE}")}


def load_chains(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    return {int(row["company_id"]): row for row in rows(conn, f"SELECT * FROM {CHAIN_TABLE}")}


def load_anchors(conn: sqlite3.Connection) -> dict[int, dict[int, date]]:
    out: dict[int, dict[int, date]] = defaultdict(dict)
    for row in rows(conn, f"SELECT company_id,fiscal_year,fiscal_year_start_date FROM {ANCHOR_TABLE}"):
        parsed = parse_date(row["fiscal_year_start_date"])
        if parsed:
            out[int(row["company_id"])][int(row["fiscal_year"])] = parsed
    return out


def canonical_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date AS period_end,q.publish_date,
               f.accepted_source_provider,f.revenue,f.operating_income,f.net_income
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        ORDER BY c.ticker,q.period_end_date,q.fiscal_year,q.fiscal_quarter
        """,
    )


def interval_class(calendar_type: str, days: int, break_reason: str) -> str:
    if break_reason == "CALENDAR_TRANSITION" and days not in {364, 365, 366, 371}:
        return "DIRECT_EXACT_TRANSITION"
    if calendar_type == "WEEK_BASED_52_53" and days == 364:
        return "DIRECT_EXACT_52_WEEK"
    if calendar_type == "WEEK_BASED_52_53" and days == 371:
        return "DIRECT_EXACT_53_WEEK"
    if calendar_type in {"CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR"} and days in {365, 366}:
        return "DIRECT_EXACT_FIXED_CALENDAR"
    if days in {364, 365, 366, 371}:
        return "DIRECT_EXACT_NORMAL"
    return "DIRECT_EXACT_NONSTANDARD_REVIEW"


def build_exact_interval_map(
    anchors: dict[int, dict[int, date]],
    profiles: dict[int, dict[str, Any]],
    chains: dict[int, dict[str, Any]],
    ticker_by_company: dict[int, str],
) -> list[dict[str, Any]]:
    out = []
    for company_id, by_year in anchors.items():
        years = sorted(by_year)
        profile = profiles.get(company_id, {})
        chain = chains.get(company_id, {})
        calendar_type = str(profile.get("calendar_type") or "UNKNOWN")
        break_reason = str(chain.get("break_reason") or "")
        for left, right in zip(years, years[1:]):
            days = (by_year[right] - by_year[left]).days
            klass = interval_class(calendar_type, days, break_reason)
            out.append(
                {
                    "company_id": company_id,
                    "ticker": ticker_by_company.get(company_id, ""),
                    "fiscal_year": left,
                    "start_date": by_year[left].isoformat(),
                    "next_start_date": by_year[right].isoformat(),
                    "interval_days": days,
                    "calendar_type": calendar_type,
                    "chain_status": chain.get("chain_status", ""),
                    "break_reason": break_reason,
                    "transition_boundary": int(klass == "DIRECT_EXACT_TRANSITION"),
                    "direct_interval_confidence": "DIRECT_EXACT",
                    "interval_class": klass,
                }
            )
    return sorted(out, key=lambda r: (r["ticker"], int(r["fiscal_year"])))


def find_direct_interval(company_id: int, observed: date, intervals_by_company: dict[int, list[dict[str, Any]]]) -> dict[str, Any] | None:
    for row in intervals_by_company.get(company_id, []):
        start = date.fromisoformat(row["start_date"])
        end = date.fromisoformat(row["next_start_date"])
        if start <= observed < end:
            return row
    return None


def short_or_long_inference(company_id: int, observed: date, anchors: dict[int, dict[int, date]], chain: dict[str, Any]) -> str:
    if chain.get("break_reason") in {"UNRESOLVED_BOUNDARY", "NO_FISCAL_YEAR", "CALENDAR_TRANSITION"}:
        return "UNRESOLVED"
    by_year = anchors.get(company_id, {})
    if not by_year:
        return "UNRESOLVED"
    if chain.get("break_reason") == "SOURCE_HISTORY_EXHAUSTED" and observed < min(by_year.values()):
        return "UNRESOLVED"
    nearest = min(by_year.values(), key=lambda d: abs((observed - d).days))
    years = abs((observed - nearest).days) / 365.25
    return "SHORT_INFERENCE" if years <= 2 else "LONG_INFERENCE"


def month_starts(start: date, end: date) -> list[date]:
    return [start, add_months(start, 3), add_months(start, 6), add_months(start, 9), end]


def infer_fq(row: dict[str, Any], interval: dict[str, Any], profile: dict[str, Any], anchors: dict[int, date], placements: dict[tuple[int, int], str]) -> dict[str, Any]:
    observed = parse_date(row.get("period_end"))
    if not observed:
        return {"exact_fq": "", "fq_confidence": "UNRESOLVED", "period_end_structural_fit": "NO_PERIOD_END", "period_end_offset_days": ""}
    fy = int(interval["fiscal_year"])
    start = date.fromisoformat(interval["start_date"])
    end = date.fromisoformat(interval["next_start_date"])
    calendar_type = interval["calendar_type"]
    if calendar_type == "WEEK_BASED_52_53":
        placement = placements.get((int(row["company_id"]), fy), "EXTRA_WEEK_AMBIGUOUS")
        slots = week_slots(fy, profile, anchors, placement)
        starts = slots["starts"] if slots.get("available") else [start, start + timedelta(days=91), start + timedelta(days=182), start + timedelta(days=273), end]
        confidence = "DIRECT_EXACT_FQ_HIGH" if placement != "EXTRA_WEEK_AMBIGUOUS" else "DIRECT_EXACT_FQ_REVIEW"
    elif calendar_type in {"CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR"}:
        starts = month_starts(start, end)
        confidence = "DIRECT_EXACT_FQ_HIGH"
    else:
        starts = month_starts(start, end)
        confidence = "DIRECT_EXACT_FQ_REVIEW"
    idx = None
    for i in range(4):
        if starts[i] <= observed < starts[i + 1]:
            idx = i
            break
    if idx is None:
        idx = min(range(4), key=lambda i: abs((observed - (starts[i + 1] - timedelta(days=1))).days))
        confidence = "DIRECT_EXACT_FQ_REVIEW"
    exact_fq = f"Q{idx + 1}"
    expected_end = starts[idx + 1] - timedelta(days=1)
    offset = (observed - expected_end).days
    fit = "STRUCTURAL_FIT" if abs(offset) <= 7 else "STRUCTURAL_REVIEW"
    if interval["interval_class"] in {"DIRECT_EXACT_TRANSITION", "DIRECT_EXACT_NONSTANDARD_REVIEW"}:
        fit = "TRANSITION_OR_NONSTANDARD_REVIEW"
    return {
        "exact_fq": exact_fq,
        "fq_confidence": confidence,
        "expected_period_end": expected_end.isoformat(),
        "period_end_offset_days": offset,
        "period_end_structural_fit": fit,
    }


def fy_compare(stored: int, exact: int | None) -> str:
    if exact is None:
        return "NO_EXACT_FY"
    delta = stored - exact
    if delta == 0:
        return "FY_EXACT_MATCH"
    if delta == -1:
        return "FY_MINUS_ONE"
    if delta == 1:
        return "FY_PLUS_ONE"
    return "FY_OTHER_MISMATCH"


def publish_state(row: dict[str, Any]) -> str:
    period = parse_date(row.get("period_end"))
    publish = parse_date(row.get("publish_date"))
    if not period or not publish:
        return "PUBLISH_UNRESOLVED"
    if publish < period:
        return "PUBLISH_BEFORE_PERIOD_END"
    return "PUBLISH_AFTER_PERIOD_END"


def content_state(row: dict[str, Any], target_state: str) -> str:
    if target_state in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING"}:
        return "CONTENT_MAPPING_REVIEW"
    if row.get("period_end_structural_fit") == "STRUCTURAL_FIT" and row.get("publish_chronology") == "PUBLISH_AFTER_PERIOD_END":
        return "CONTENT_NOT_PROVEN_WRONG"
    return "CONTENT_UNRESOLVED"


def target_collision(row: dict[str, Any], by_company_fyq: dict[tuple[int, int, str], dict[str, Any]]) -> str:
    if row.get("exact_fy") in ("", None) or row.get("exact_fq") in ("", None):
        return "UNRESOLVED"
    key = (int(row["company_id"]), int(row["exact_fy"]), str(row["exact_fq"]))
    target = by_company_fyq.get(key)
    if target is None:
        return "TARGET_EMPTY"
    if int(target["quarter_id"]) == int(row["quarter_id"]):
        return "TARGET_SELF"
    same_period = target.get("period_end") == row.get("period_end")
    same_values = all(str(target.get(field)) == str(row.get(field)) for field in ("revenue", "operating_income", "net_income"))
    if same_period and same_values:
        return "TARGET_SAME_ECONOMIC"
    if same_period:
        return "TARGET_CONFLICTING"
    return "TARGET_DIFFERENT_ECONOMIC"


def classify_repairability(row: dict[str, Any]) -> str:
    if row.get("identity_class") in {"PASS_DIRECT_EXACT", "PASS_INFERRED"}:
        return "NO_REPAIR_NEEDED"
    if row.get("identity_class") == "REVIEW_TRANSITION":
        return "TRANSITION_REVIEW"
    if row.get("identity_class") == "REVIEW_UNRESOLVED_BOUNDARY":
        return "UNRESOLVED"
    if row.get("period_end_structural_fit") != "STRUCTURAL_FIT":
        return "METADATA_REPAIR_REQUIRED"
    if row.get("content_integrity") == "CONTENT_MAPPING_REVIEW":
        return "CONTENT_RECONSTRUCTION_REQUIRED"
    if row.get("target_collision") in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING"}:
        return "TARGET_COLLISION_REVIEW"
    if row.get("publish_chronology") != "PUBLISH_AFTER_PERIOD_END":
        return "METADATA_REPAIR_REQUIRED"
    return "AUTO_RELABEL_READY"


def classify_row(
    row: dict[str, Any],
    intervals_by_company: dict[int, list[dict[str, Any]]],
    profiles: dict[int, dict[str, Any]],
    chains: dict[int, dict[str, Any]],
    anchors: dict[int, dict[int, date]],
    placements: dict[tuple[int, int], str],
    by_company_fyq: dict[tuple[int, int, str], dict[str, Any]],
) -> dict[str, Any]:
    observed = parse_date(row.get("period_end"))
    base = dict(row)
    chain = chains.get(int(row["company_id"]), {})
    base["chain_status"] = chain.get("chain_status", "")
    base["break_reason"] = chain.get("break_reason", "")
    if not observed:
        base.update({"identity_basis": "UNRESOLVED", "identity_class": "INSUFFICIENT_HISTORY", "exact_fy": "", "exact_fq": ""})
        return base
    interval = find_direct_interval(int(row["company_id"]), observed, intervals_by_company)
    if interval:
        fq = infer_fq(row, interval, profiles.get(int(row["company_id"]), {}), anchors.get(int(row["company_id"]), {}), placements)
        exact_fy = int(interval["fiscal_year"])
        base.update(
            {
                "identity_basis": "DIRECT_EXACT_INTERVAL",
                "exact_fy": exact_fy,
                "exact_fq": fq["exact_fq"],
                "fy_compare": fy_compare(int(row["fiscal_year"]), exact_fy),
                "fq_compare": "FQ_EXACT_MATCH" if str(row["fiscal_quarter"]) == str(fq["exact_fq"]) else "FQ_MISMATCH",
                "interval_class": interval["interval_class"],
                "interval_start": interval["start_date"],
                "interval_end_exclusive": interval["next_start_date"],
                "interval_days": interval["interval_days"],
                **fq,
            }
        )
        if interval["interval_class"] in {"DIRECT_EXACT_TRANSITION", "DIRECT_EXACT_NONSTANDARD_REVIEW"}:
            identity_class = "REVIEW_TRANSITION"
        elif base["fy_compare"] != "FY_EXACT_MATCH":
            identity_class = "BLOCK_EXACT_FY_CONFLICT"
        elif base["fq_compare"] != "FQ_EXACT_MATCH" and fq["fq_confidence"] == "DIRECT_EXACT_FQ_HIGH":
            identity_class = "BLOCK_EXACT_FQ_CONFLICT"
        elif base["fq_compare"] != "FQ_EXACT_MATCH":
            identity_class = "WARNING"
        else:
            identity_class = "PASS_DIRECT_EXACT"
    else:
        basis = short_or_long_inference(int(row["company_id"]), observed, anchors, chain)
        base.update(
            {
                "identity_basis": basis,
                "exact_fy": "",
                "exact_fq": "",
                "fy_compare": "NO_EXACT_FY",
                "fq_compare": "NO_EXACT_FQ",
                "interval_class": "",
                "period_end_structural_fit": "UNRESOLVED",
                "period_end_offset_days": "",
            }
        )
        identity_class = "PASS_INFERRED" if basis == "SHORT_INFERENCE" else "INSUFFICIENT_HISTORY" if basis == "LONG_INFERENCE" else "REVIEW_UNRESOLVED_BOUNDARY"
    base["identity_class"] = identity_class
    base["publish_chronology"] = publish_state(row)
    base["target_collision"] = target_collision(base, by_company_fyq)
    base["content_integrity"] = content_state(base, base["target_collision"])
    base["repairability"] = classify_repairability(base)
    return base


def latest_flags(canonical: list[dict[str, Any]]) -> dict[int, dict[str, int]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in canonical:
        if int(row["active"]) == 1:
            grouped[int(row["company_id"])].append(row)
    flags: dict[int, dict[str, int]] = defaultdict(lambda: {"latest_quarter": 0, "latest4q": 0, "latest8q": 0})
    for group in grouped.values():
        ordered = sorted(group, key=lambda r: (r.get("period_end") or "", int(r["fiscal_year"]), str(r["fiscal_quarter"])), reverse=True)
        for idx, row in enumerate(ordered[:8]):
            flags[int(row["quarter_id"])]["latest8q"] = 1
            if idx < 4:
                flags[int(row["quarter_id"])]["latest4q"] = 1
            if idx == 0:
                flags[int(row["quarter_id"])]["latest_quarter"] = 1
    return flags


def ttm_input_ids(conn: sqlite3.Connection) -> set[int]:
    out: set[int] = set()
    for row in rows(conn, "SELECT q1_quarter_id,q2_quarter_id,q3_quarter_id,q4_quarter_id FROM v3_ttm"):
        for key in ("q1_quarter_id", "q2_quarter_id", "q3_quarter_id", "q4_quarter_id"):
            if row.get(key) is not None:
                out.add(int(row[key]))
    return out


def summarize_classes(data: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["identity_class"] for row in data)
    return {
        "rows": len(data),
        "clean": counts.get("PASS_DIRECT_EXACT", 0) + counts.get("PASS_INFERRED", 0) + counts.get("WARNING", 0),
        "direct_exact_fy_conflicts": counts.get("BLOCK_EXACT_FY_CONFLICT", 0),
        "direct_exact_fq_conflicts": counts.get("BLOCK_EXACT_FQ_CONFLICT", 0),
        "inferred_conflicts": counts.get("BLOCK_STRUCTURAL_HIGH", 0),
        "transition_review": counts.get("REVIEW_TRANSITION", 0),
        "unresolved": counts.get("REVIEW_UNRESOLVED_BOUNDARY", 0) + counts.get("INSUFFICIENT_HISTORY", 0),
        "affected_tickers": len({row["ticker"] for row in data if row["identity_class"] not in {"PASS_DIRECT_EXACT", "PASS_INFERRED", "WARNING"}}),
        "class_counts": dict(counts),
    }


def build_ttm_risk(conn: sqlite3.Connection, reclass_by_qid: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    latest = rows(
        conn,
        """
        SELECT *
        FROM v3_ttm t
        WHERE t.ttm_id IN (SELECT t2.ttm_id FROM v3_ttm t2 WHERE t2.company_id=t.company_id ORDER BY t2.period_end DESC,t2.ttm_id DESC LIMIT 1)
        ORDER BY company_id
        """,
    )
    out = []
    for ttm in latest:
        inputs = [reclass_by_qid[int(ttm[key])] for key in ("q1_quarter_id", "q2_quarter_id", "q3_quarter_id", "q4_quarter_id") if int(ttm[key]) in reclass_by_qid]
        conflicts = [r for r in inputs if r["identity_class"] in {"BLOCK_EXACT_FY_CONFLICT", "BLOCK_EXACT_FQ_CONFLICT"}]
        transition = [r for r in inputs if r["identity_class"] == "REVIEW_TRANSITION"]
        unresolved = [r for r in inputs if r["identity_class"] in {"REVIEW_UNRESOLVED_BOUNDARY", "INSUFFICIENT_HISTORY"}]
        inferred = [r for r in inputs if r["identity_basis"] in {"SHORT_INFERENCE", "LONG_INFERENCE"} and not conflicts]
        if len(conflicts) > 1:
            risk = "TTM_MULTIPLE_CONFLICTS"
        elif conflicts:
            risk = "TTM_LABEL_CONFLICT_DIRECT_EXACT"
        elif transition:
            risk = "TTM_TRANSITION_REVIEW"
        elif unresolved:
            risk = "TTM_UNRESOLVED_HISTORY"
        elif inferred:
            risk = "TTM_CLEAN_INFERRED"
        else:
            risk = "TTM_CLEAN_DIRECT_EXACT"
        out.append(
            {
                "company_id": ttm["company_id"],
                "ttm_id": ttm["ttm_id"],
                "endpoint_quarter_id": ttm["endpoint_quarter_id"],
                "endpoint_fiscal_year": ttm["endpoint_fiscal_year"],
                "endpoint_fiscal_quarter": ttm["endpoint_fiscal_quarter"],
                "period_end": ttm["period_end"],
                "risk_class": risk,
                "conflict_inputs": len(conflicts),
                "transition_inputs": len(transition),
                "unresolved_inputs": len(unresolved),
                "input_quarter_ids": "|".join(str(r["quarter_id"]) for r in inputs),
                "conflict_quarter_ids": "|".join(str(r["quarter_id"]) for r in conflicts),
            }
        )
    ticker_by_company = {int(row["company_id"]): row["ticker"] for row in rows(conn, "SELECT company_id,ticker FROM v3_company")}
    return [{**row, "ticker": ticker_by_company.get(int(row["company_id"]), "")} for row in out]


def latest_downstream(conn: sqlite3.Connection, table: str, ttm_by_id: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    if table == "v3_score":
        data = rows(conn, "SELECT * FROM v3_score s WHERE s.score_id IN (SELECT s2.score_id FROM v3_score s2 WHERE s2.company_id=s.company_id ORDER BY s2.endpoint_period_end DESC,s2.score_id DESC LIMIT 1)")
        id_col = "score_id"
    elif table == "v3_lifecycle":
        data = rows(conn, "SELECT * FROM v3_lifecycle l WHERE l.lifecycle_id IN (SELECT l2.lifecycle_id FROM v3_lifecycle l2 WHERE l2.company_id=l.company_id ORDER BY l2.endpoint_period_end DESC,l2.lifecycle_id DESC LIMIT 1)")
        id_col = "lifecycle_id"
    else:
        data = rows(conn, "SELECT * FROM v3_valuation v WHERE v.valuation_id IN (SELECT v2.valuation_id FROM v3_valuation v2 WHERE v2.company_id=v.company_id ORDER BY v2.endpoint_period_end DESC,v2.valuation_id DESC LIMIT 1)")
        id_col = "valuation_id"
    ticker_by_company = {int(row["company_id"]): row["ticker"] for row in rows(conn, "SELECT company_id,ticker FROM v3_company")}
    out = []
    for row in data:
        ttm = ttm_by_id.get(int(row["endpoint_ttm_id"]))
        input_conflict = bool(ttm and ttm["risk_class"] in {"TTM_LABEL_CONFLICT_DIRECT_EXACT", "TTM_MULTIPLE_CONFLICTS"})
        out.append(
            {
                "company_id": row["company_id"],
                "ticker": ticker_by_company.get(int(row["company_id"]), ""),
                id_col: row[id_col],
                "endpoint_ttm_id": row["endpoint_ttm_id"],
                "endpoint_period_end": row.get("endpoint_period_end", ""),
                "source_ttm_risk_class": ttm["risk_class"] if ttm else "UNRESOLVED",
                "known_input_identity_error": int(input_conflict),
                "proven_numeric_error": 0,
            }
        )
    return out


def segment_candidates(rows_: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows_:
        if row["repairability"] == "AUTO_RELABEL_READY":
            groups[(int(row["company_id"]), int(row["fiscal_year"]) - int(row["exact_fy"]))].append(row)
    out = []
    for (cid, delta), group in groups.items():
        if len(group) < 2:
            continue
        group = sorted(group, key=lambda r: r.get("period_end") or "")
        out.append(
            {
                "company_id": cid,
                "ticker": group[0]["ticker"],
                "stored_minus_exact_fy_delta": delta,
                "rows": len(group),
                "quarter_ids": "|".join(str(r["quarter_id"]) for r in group),
                "period_range": f"{group[0].get('period_end')}..{group[-1].get('period_end')}",
                "repairability": "ATOMIC_SEGMENT_RELABEL_READY",
            }
        )
    return out


def d6_405_reanalysis(rows_: list[dict[str, Any]], reclass_by_qid: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows_:
        current = reclass_by_qid.get(int(row["quarter_id"]), {})
        if current.get("fy_compare") == "FY_MINUS_ONE" and current.get("identity_basis") == "DIRECT_EXACT_INTERVAL":
            cls = "FY_MINUS_ONE_CONFIRMED_EXACT_ANCHOR"
        elif current.get("fy_compare") == "FY_MINUS_ONE":
            cls = "FY_MINUS_ONE_CONFIRMED_SHORT_INFERENCE"
        elif current.get("identity_class") == "REVIEW_TRANSITION":
            cls = "TRANSITION_REVIEW"
        else:
            cls = "PRIOR_D6_CLASSIFICATION_NOT_CONFIRMED"
        out.append({**row, **{f"d7_{k}": v for k, v in current.items() if k in {"identity_basis", "identity_class", "exact_fy", "exact_fq", "fy_compare", "fq_compare", "repairability", "target_collision", "content_integrity"}}, "d7_reanalysis_class": cls})
    return out


def d6_513_reanalysis(rows_: list[dict[str, Any]], reclass_by_qid: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows_:
        current = reclass_by_qid.get(int(row["quarter_id"]), {})
        if current.get("identity_class") == "REVIEW_TRANSITION":
            cls = "TRANSITION_REVIEW"
        elif current.get("identity_class") == "REVIEW_UNRESOLVED_BOUNDARY":
            cls = "UNRESOLVED_BOUNDARY"
        elif current.get("identity_basis") not in {"DIRECT_EXACT_INTERVAL", "SHORT_INFERENCE"}:
            cls = "NO_EXACT_HISTORICAL_SUPPORT"
        elif current.get("fy_compare") == "FY_EXACT_MATCH" and current.get("fq_compare") == "FQ_EXACT_MATCH":
            cls = "PRIOR_CLASSIFICATION_DISPROVEN"
        elif current.get("identity_basis") == "SHORT_INFERENCE":
            cls = "LABEL_ERROR_SHORT_INFERENCE"
        elif current.get("content_integrity") == "CONTENT_NOT_PROVEN_WRONG":
            cls = "LABEL_ONLY_ERROR_DIRECT_EXACT"
        else:
            cls = "LABEL_PLUS_METADATA_ERROR_DIRECT_EXACT"
        out.append({**row, **{f"d7_{k}": v for k, v in current.items() if k in {"identity_basis", "identity_class", "exact_fy", "exact_fq", "fy_compare", "fq_compare", "repairability", "target_collision", "content_integrity"}}, "d7_reanalysis_class": cls})
    return out


def known_good_analysis(rows_: list[dict[str, Any]], reclass_by_qid: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, **{f"d7_{k}": v for k, v in reclass_by_qid.get(int(row["quarter_id"]), {}).items() if k in {"identity_basis", "identity_class", "exact_fy", "exact_fq", "fy_compare", "fq_compare", "period_end_structural_fit", "content_integrity"}}} for row in rows_]


def metadata_validation(conn: sqlite3.Connection, intervals: list[dict[str, Any]]) -> dict[str, Any]:
    anchor_count = conn.execute(f"SELECT COUNT(*) FROM {ANCHOR_TABLE}").fetchone()[0]
    unique_anchor_count = conn.execute(f"SELECT COUNT(*) FROM (SELECT company_id,fiscal_year FROM {ANCHOR_TABLE} GROUP BY company_id,fiscal_year)").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=1").fetchone()[0]
    chain_count = conn.execute(f"SELECT COUNT(*) FROM {CHAIN_TABLE}").fetchone()[0]
    conflicts = conn.execute(f"SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,COUNT(*) n FROM {ANCHOR_TABLE} GROUP BY company_id,fiscal_year HAVING n>1)").fetchone()[0]
    fy2026 = conn.execute(f"SELECT COUNT(*) FROM {ANCHOR_TABLE} WHERE fiscal_year=2026").fetchone()[0]
    fy2027 = conn.execute(f"SELECT COUNT(*) FROM {ANCHOR_TABLE} WHERE fiscal_year=2027").fetchone()[0]
    return {
        "exact_anchors": int(anchor_count),
        "active_companies": int(active),
        "anchor_conflicts": int(conflicts),
        "unique_company_fiscal_year": int(anchor_count == unique_anchor_count),
        "chain_rows": int(chain_count),
        "fy2026_anchors": int(fy2026),
        "fy2027_anchors": int(fy2027),
        "direct_exact_intervals": len(intervals),
        "valid": int(anchor_count == 35399 and active == 2470 and conflicts == 0 and chain_count == 2470 and fy2026 == 2470 and fy2027 == 259),
    }


def run_phase8d7(paths: Phase8D7Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    before_fp = semantic_fingerprints(paths.v3_db)
    old_audit = read_csv_dicts(paths.phase8d1_root / "full_canonical_fiscal_guard_audit.csv")
    d6_405 = read_csv_dicts(paths.phase8d6_root / "systematic_fy_minus_one_cases.csv")
    d6_513 = read_csv_dicts(paths.phase8d6_root / "known_good_513_label_unsupported.csv")
    known_good = read_csv_dicts(paths.phase8d4_root / "known_good_new_guard_simulation.csv")
    with connect_ro(paths.v3_db) as conn:
        profiles = load_profiles(conn)
        chains = load_chains(conn)
        anchors = load_anchors(conn)
        canonical = canonical_rows(conn)
        ticker_by_company = {int(row["company_id"]): row["ticker"] for row in rows(conn, "SELECT company_id,ticker FROM v3_company")}
        intervals = build_exact_interval_map(anchors, profiles, chains, ticker_by_company)
        validation = metadata_validation(conn, intervals)
        flags = latest_flags(canonical)
        input_ids = ttm_input_ids(conn)
        for row in canonical:
            row.update(flags.get(int(row["quarter_id"]), {}))
            row["ttm_input"] = int(int(row["quarter_id"]) in input_ids)
        by_company_fyq = {(int(r["company_id"]), int(r["fiscal_year"]), str(r["fiscal_quarter"])): r for r in canonical}
        placements = resolve_extra_week(canonical, profiles, anchors)
        intervals_by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for interval in intervals:
            intervals_by_company[int(interval["company_id"])].append(interval)
        reclass = [classify_row(row, intervals_by_company, profiles, chains, anchors, placements, by_company_fyq) for row in canonical]
        reclass_by_qid = {int(row["quarter_id"]): row for row in reclass}
        ttm = build_ttm_risk(conn, reclass_by_qid)
        ttm_by_id = {int(row["ttm_id"]): row for row in ttm}
        score = latest_downstream(conn, "v3_score", ttm_by_id)
        lifecycle = latest_downstream(conn, "v3_lifecycle", ttm_by_id)
        valuation = latest_downstream(conn, "v3_valuation", ttm_by_id)
    after_fp = semantic_fingerprints(paths.v3_db)

    d6_405_out = d6_405_reanalysis(d6_405, reclass_by_qid)
    d6_513_out = d6_513_reanalysis(d6_513, reclass_by_qid)
    known_good_out = known_good_analysis(known_good, reclass_by_qid)
    known_direct = [row for row in known_good_out if row.get("d7_identity_basis") == "DIRECT_EXACT_INTERVAL"]
    current_recent = [row for row in reclass if int(row.get("latest8q") or 0) or int(row.get("latest4q") or 0) or int(row.get("latest_quarter") or 0) or int(row.get("ttm_input") or 0) or (parse_date(row.get("period_end")) and parse_date(row.get("period_end")).year >= 2024)]
    repair_rows = [row for row in current_recent if row["repairability"] != "NO_REPAIR_NEEDED"]
    auto = [row for row in repair_rows if row["repairability"] == "AUTO_RELABEL_READY"]
    segments = segment_candidates(auto)
    direct_rows = [row for row in reclass if row["identity_basis"] == "DIRECT_EXACT_INTERVAL"]
    old_backward_reduced = sum(1 for row in old_audit if row.get("block_kind") == "BACKWARD_INFERENCE_BLOCK" and reclass_by_qid.get(int(row["quarter_id"]), {}).get("identity_basis") == "DIRECT_EXACT_INTERVAL")
    full_summary = summarize_classes(reclass)
    cohorts = {
        "2024plus": [row for row in reclass if parse_date(row.get("period_end")) and parse_date(row.get("period_end")).year >= 2024],
        "2025plus": [row for row in reclass if parse_date(row.get("period_end")) and parse_date(row.get("period_end")).year >= 2025],
        "latest8q": [row for row in reclass if int(row.get("latest8q") or 0)],
        "latest4q": [row for row in reclass if int(row.get("latest4q") or 0)],
        "latest_quarter": [row for row in reclass if int(row.get("latest_quarter") or 0)],
    }
    cohort_summaries = {name: summarize_classes(data) for name, data in cohorts.items()}
    ttm_counts = Counter(row["risk_class"] for row in ttm)
    d6_405_counts = Counter(row["d7_reanalysis_class"] for row in d6_405_out)
    d6_513_counts = Counter(row["d7_reanalysis_class"] for row in d6_513_out)
    direct_confirmed = d6_405_counts.get("FY_MINUS_ONE_CONFIRMED_EXACT_ANCHOR", 0)
    material = old_backward_reduced >= OLD_BACKWARD_INFERENCE_BLOCK * 0.6 and len(auto) >= 50
    partial = old_backward_reduced > 0 or len(auto) > 0
    classification = CLASSIFICATION_MATERIAL if material else CLASSIFICATION_PARTIAL if partial else CLASSIFICATION_NO_CHANGE
    next_action = (
        "DO NOT REPAIR PRODUCTION YET; REHEARSE DETERMINISTIC CURRENT/RECENT FY/FQ RELABELING USING DIRECT HISTORICAL EXACT ANCHORS, WITH ATOMIC SEGMENT HANDLING FOR COLLISIONS"
        if classification == CLASSIFICATION_MATERIAL
        else "REHEARSE ONLY THE DIRECT-EXACT DETERMINISTIC REPAIR SUBSET; KEEP TRANSITION/UNRESOLVED CASES DEFERRED"
        if classification == CLASSIFICATION_PARTIAL
        else "DO NOT REPAIR CANONICAL DATA; INVESTIGATE THE REMAINING NON-ANCHOR ROOT CAUSE"
    )
    known_13 = [row for row in current_recent if row["ticker"] in KNOWN_13]
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "metadata_validation": validation,
        "historical_anchor_coverage": {
            "exact_anchors": validation["exact_anchors"],
            "direct_exact_fy_intervals": len(intervals),
            "canonical_rows_resolved_by_direct_interval": len(direct_rows),
            "rows_short_inference": sum(1 for row in reclass if row["identity_basis"] == "SHORT_INFERENCE"),
            "rows_long_inference": sum(1 for row in reclass if row["identity_basis"] == "LONG_INFERENCE"),
            "transition_unresolved_rows": sum(1 for row in reclass if row["identity_class"] in {"REVIEW_TRANSITION", "REVIEW_UNRESOLVED_BOUNDARY", "INSUFFICIENT_HISTORY"}),
        },
        "d6_405": {"original": len(d6_405), "counts": dict(d6_405_counts), "pct_confirmed_without_backward_inference": pct(direct_confirmed, len(d6_405))},
        "d6_513": {"original": len(d6_513), "counts": dict(d6_513_counts)},
        "known_good": {
            "rows": len(known_good_out),
            "direct_anchor_subset_rows": len(known_direct),
            "stored_fy_match_pct": pct(sum(1 for row in known_direct if row.get("d7_fy_compare") == "FY_EXACT_MATCH"), len(known_direct)),
            "fy_minus_one_pct": pct(sum(1 for row in known_direct if row.get("d7_fy_compare") == "FY_MINUS_ONE"), len(known_direct)),
            "fq_match_pct": pct(sum(1 for row in known_direct if row.get("d7_fq_compare") == "FQ_EXACT_MATCH"), len(known_direct)),
            "combined_match_pct": pct(sum(1 for row in known_direct if row.get("d7_fy_compare") == "FY_EXACT_MATCH" and row.get("d7_fq_compare") == "FQ_EXACT_MATCH"), len(known_direct)),
            "label_only_mismatches": sum(1 for row in known_direct if row.get("d7_content_integrity") == "CONTENT_NOT_PROVEN_WRONG" and (row.get("d7_fy_compare") != "FY_EXACT_MATCH" or row.get("d7_fq_compare") != "FQ_EXACT_MATCH")),
            "content_mapping_errors": sum(1 for row in known_direct if row.get("d7_content_integrity") == "CONTENT_MAPPING_REVIEW"),
        },
        "full_canonical": {
            "old_historical_BLOCK": OLD_HISTORICAL_BLOCK,
            "old_exact_anchor_blocks": OLD_EXACT_ANCHOR_BLOCK,
            "old_backward_inference_blocks": OLD_BACKWARD_INFERENCE_BLOCK,
            "backward_inference_resolved_by_direct_interval": old_backward_reduced,
            "backward_inference_reduction_pct": pct(old_backward_reduced, OLD_BACKWARD_INFERENCE_BLOCK),
            **full_summary,
        },
        "cohorts": cohort_summaries,
        "current_ttm": {"companies": len(ttm), "counts": dict(ttm_counts), "affected_tickers": len({row["ticker"] for row in ttm if row["risk_class"] not in {"TTM_CLEAN_DIRECT_EXACT", "TTM_CLEAN_INFERRED"}})},
        "downstream": {
            "score_current_input_conflicts": sum(row["known_input_identity_error"] for row in score),
            "lifecycle_current_input_conflicts": sum(row["known_input_identity_error"] for row in lifecycle),
            "valuation_current_input_conflicts": sum(row["known_input_identity_error"] for row in valuation),
            "known_input_identity_errors": sum(row["known_input_identity_error"] for row in score) + sum(row["known_input_identity_error"] for row in lifecycle) + sum(row["known_input_identity_error"] for row in valuation),
            "proven_numeric_errors": 0,
        },
        "repairability": {
            "counts": dict(Counter(row["repairability"] for row in repair_rows)),
            "auto_relabel_rows": len(auto),
            "auto_relabel_tickers": len({row["ticker"] for row in auto}),
            "atomic_segment_rows": sum(int(row["rows"]) for row in segments),
            "atomic_segment_tickers": len({row["ticker"] for row in segments}),
        },
        "known_13": {
            "direct_exact_supported": len({row["ticker"] for row in known_13 if row["identity_basis"] == "DIRECT_EXACT_INTERVAL"}),
            "transition_review": len({row["ticker"] for row in known_13 if row["identity_class"] == "REVIEW_TRANSITION"}),
            "auto_or_segment_repairable": len({row["ticker"] for row in known_13 if row["repairability"] == "AUTO_RELABEL_READY" or row["ticker"] in {seg["ticker"] for seg in segments}}),
            "content_reconstruction_required": len({row["ticker"] for row in known_13 if row["repairability"] == "CONTENT_RECONSTRUCTION_REQUIRED"}),
            "unresolved": len({row["ticker"] for row in known_13 if row["repairability"] == "UNRESOLVED"}),
        },
        "safety": {
            "canonical_writes": 0,
            "fundamentals_writes": 0,
            "fiscal_metadata_writes": 0,
            "chain_metadata_writes": 0,
            "downstream_writes": 0,
            "rawcandle_writes": 0,
            "active_guard_changes": 0,
            "semantic_fingerprints_unchanged": before_fp == after_fp,
        },
        "rationale": f"Direct exact historical intervals now classify {len(direct_rows)} canonical rows and replace direct evidence for {old_backward_reduced} of the old {OLD_BACKWARD_INFERENCE_BLOCK} backward-inference BLOCK rows; deterministic current/recent relabel candidates = {len(auto)}.",
        "next_action": next_action,
    }
    write_outputs(paths, intervals, reclass, d6_405_out, d6_513_out, known_good_out, known_direct, cohorts, ttm, score, lifecycle, valuation, repair_rows, auto, segments, known_13, summary)
    write_docs(summary)
    return summary


def write_outputs(
    paths: Phase8D7Paths,
    intervals: list[dict[str, Any]],
    reclass: list[dict[str, Any]],
    d6_405: list[dict[str, Any]],
    d6_513: list[dict[str, Any]],
    known_good: list[dict[str, Any]],
    known_direct: list[dict[str, Any]],
    cohorts: dict[str, list[dict[str, Any]]],
    ttm: list[dict[str, Any]],
    score: list[dict[str, Any]],
    lifecycle: list[dict[str, Any]],
    valuation: list[dict[str, Any]],
    repair_rows: list[dict[str, Any]],
    auto: list[dict[str, Any]],
    segments: list[dict[str, Any]],
    known_13: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    write_json(paths.artifact_root / "historical_anchor_metadata_validation.json", summary["metadata_validation"])
    write_csv(paths.artifact_root / "historical_exact_interval_map.csv", intervals)
    write_json(paths.artifact_root / "historical_interval_coverage_summary.json", summary["historical_anchor_coverage"])
    write_csv(paths.artifact_root / "d6_405_fy_minus_one_reanalysis.csv", d6_405)
    write_csv(paths.artifact_root / "d6_513_label_residual_reanalysis.csv", d6_513)
    write_json(paths.artifact_root / "d6_old_vs_new_summary.json", {"d6_405": summary["d6_405"], "d6_513": summary["d6_513"]})
    write_csv(paths.artifact_root / "known_good_historical_anchor_analysis.csv", known_good)
    write_csv(paths.artifact_root / "known_good_direct_anchor_subset.csv", known_direct)
    write_csv(paths.artifact_root / "known_good_label_vs_content.csv", [row for row in known_direct if row.get("d7_fy_compare") != "FY_EXACT_MATCH" or row.get("d7_fq_compare") != "FQ_EXACT_MATCH"])
    write_csv(paths.artifact_root / "full_canonical_historical_anchor_reclassification.csv", reclass)
    write_json(paths.artifact_root / "old_vs_new_full_risk_summary.json", summary["full_canonical"])
    write_csv(paths.artifact_root / "backward_inference_reduction.csv", [{"metric": "old_backward_inference_blocks", "rows": OLD_BACKWARD_INFERENCE_BLOCK}, {"metric": "old_backward_blocks_now_direct_interval", "rows": summary["full_canonical"]["backward_inference_resolved_by_direct_interval"], "pct": summary["full_canonical"]["backward_inference_reduction_pct"]}])
    for name, data in cohorts.items():
        write_csv(paths.artifact_root / f"{name}_historical_anchor_risk.csv", data)
    write_csv(paths.artifact_root / "current_ttm_historical_anchor_risk.csv", ttm)
    write_csv(paths.artifact_root / "current_score_historical_anchor_risk.csv", score)
    write_csv(paths.artifact_root / "current_lifecycle_historical_anchor_risk.csv", lifecycle)
    write_csv(paths.artifact_root / "current_valuation_historical_anchor_risk.csv", valuation)
    write_csv(paths.artifact_root / "current_recent_repairability.csv", repair_rows)
    write_csv(paths.artifact_root / "deterministic_auto_relabel_candidates.csv", auto)
    write_csv(paths.artifact_root / "atomic_segment_relabel_candidates.csv", segments)
    write_csv(paths.artifact_root / "target_collision_review.csv", [row for row in repair_rows if row["repairability"] == "TARGET_COLLISION_REVIEW"])
    write_csv(paths.artifact_root / "transition_review_cases.csv", [row for row in repair_rows if row["repairability"] == "TRANSITION_REVIEW"])
    write_csv(paths.artifact_root / "known_13_historical_anchor_replay.csv", known_13)
    write_json(paths.artifact_root / "phase8d7_summary.json", summary)
    (paths.artifact_root / "repair_strategy_recommendation.md").write_text(summary["classification"] + "\n\n" + summary["next_action"] + "\n", encoding="utf-8")
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")


def write_docs(summary: dict[str, Any]) -> None:
    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    block = f"""## Phase 8D-7 - Historical Exact Anchor Fiscal-Identity Reanalysis

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Historical anchors now provide `{summary['historical_anchor_coverage']['direct_exact_fy_intervals']}` adjacent exact FY intervals and directly resolve `{summary['historical_anchor_coverage']['canonical_rows_resolved_by_direct_interval']}` canonical rows. Rows still using short inference `{summary['historical_anchor_coverage']['rows_short_inference']}`, long inference `{summary['historical_anchor_coverage']['rows_long_inference']}`, transition/unresolved `{summary['historical_anchor_coverage']['transition_unresolved_rows']}`.

D6 FY-minus-one replay: original `{summary['d6_405']['original']}`, direct-exact confirmed `{summary['d6_405']['counts'].get('FY_MINUS_ONE_CONFIRMED_EXACT_ANCHOR', 0)}`, short-inference confirmed `{summary['d6_405']['counts'].get('FY_MINUS_ONE_CONFIRMED_SHORT_INFERENCE', 0)}`, transition review `{summary['d6_405']['counts'].get('TRANSITION_REVIEW', 0)}`, not confirmed `{summary['d6_405']['counts'].get('PRIOR_D6_CLASSIFICATION_NOT_CONFIRMED', 0)}`.

Full canonical reclassification: direct FY conflicts `{summary['full_canonical']['direct_exact_fy_conflicts']}`, direct FQ conflicts `{summary['full_canonical']['direct_exact_fq_conflicts']}`, transition reviews `{summary['full_canonical']['transition_review']}`, unresolved `{summary['full_canonical']['unresolved']}`, clean `{summary['full_canonical']['clean']}`. Old backward-inference BLOCK rows now covered by direct intervals `{summary['full_canonical']['backward_inference_resolved_by_direct_interval']}` / `{summary['full_canonical']['old_backward_inference_blocks']}`.

Current repairability: AUTO_RELABEL_READY `{summary['repairability']['auto_relabel_rows']}` rows / `{summary['repairability']['auto_relabel_tickers']}` tickers; ATOMIC_SEGMENT_RELABEL_READY `{summary['repairability']['atomic_segment_rows']}` rows / `{summary['repairability']['atomic_segment_tickers']}` tickers. Phase 8 remains `IN PROGRESS`; production writes `0`; guard changes `0`.
"""
    phase8.write_text(phase8.read_text(encoding="utf-8").rstrip() + "\n\n" + block, encoding="utf-8")
    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    handoff.write_text(handoff.read_text(encoding="utf-8").rstrip() + f"\n\n## Phase 8D-7 Historical Anchor Repairability\n\nClassification: `{summary['classification']}`. Deterministic direct-exact auto relabel candidates `{summary['repairability']['auto_relabel_rows']}` rows across `{summary['repairability']['auto_relabel_tickers']}` tickers; segment candidates `{summary['repairability']['atomic_segment_rows']}` rows across `{summary['repairability']['atomic_segment_tickers']}` tickers. Artifact root: `{summary['artifact_root']}`.\n", encoding="utf-8")
    master = Path("docs/fundamentals_v3_master_plan_status.md")
    master.write_text(master.read_text(encoding="utf-8").rstrip() + f"\n\n## Phase 8D-7 - Historical Exact Anchor Reanalysis\n\nStatus: `{summary['classification']}`. Phase 8 remains `IN PROGRESS`. Artifact root: `{summary['artifact_root']}`.\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 8D-7 historical exact anchor fiscal reanalysis.")
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--phase8d1-root", type=Path, default=Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL"))
    parser.add_argument("--phase8d4-root", type=Path, default=Path("temp/fundamentals_v3_phase8d4_slot_model_rework/20260828T_PHASE8D4"))
    parser.add_argument("--phase8d6-root", type=Path, default=Path("temp/fundamentals_v3_phase8d6_label_provenance_audit/20260828T_PHASE8D6"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    args = parser.parse_args()
    root = args.artifact_root or Path("temp/fundamentals_v3_phase8d7_historical_anchor_reanalysis") / utc_stamp()
    summary = run_phase8d7(Phase8D7Paths(root, args.phase8d1_root, args.phase8d4_root, args.phase8d6_root, args.v3_db))
    print(f"classification={summary['classification']}")
    print(f"direct_rows={summary['historical_anchor_coverage']['canonical_rows_resolved_by_direct_interval']}")
    print(f"auto_relabel_rows={summary['repairability']['auto_relabel_rows']}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
