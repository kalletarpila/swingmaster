from __future__ import annotations

import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10b_full_sequence_audit import Phase8A10BPaths, run_phase8a10b_full_sequence_audit
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro, connect_rw, file_state, integrity
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv, sha_file


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A10E_R2_NINE_TICKER_APPLY_SET_READY"
CLASSIFICATION_PARTIAL = "FUNDAMENTALS_V3_PHASE8A10E_R2_PARTIAL_APPLY_SET_READY_BLOCKERS_REMAIN"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8A10E_R2_MAPPING_BLOCKED"
NINE_TICKERS = ("BBY", "DELL", "GCO", "HAE", "MRVL", "RL", "SAIC", "TJX", "TRNS")
FINANCIAL_COLUMNS = (
    ("revenue", "Revenue (USD mm)", "Revenue"),
    ("operating_income", "Operating Income (USD mm)", "OI"),
    ("net_income", "Net Income (USD mm)", "NI"),
)


@dataclass(frozen=True)
class Phase8A10ER2Paths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")
    financial_timeline_csv: Path = Path("temp/swingmaster_v3_official_fiscal_quarter_timeline_with_financials_2026-08-26.csv")
    previous_r_root: Path = Path("temp/fundamentals_v3_phase8a10e_r_latest8q_mapping/20260826T_PHASE8A10E_R")
    a10b_root: Path = Path("temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    publish_apply_root: Path = Path("temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def qnum(fq: str) -> int:
    return {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(str(fq), 0)


def official_key(row: dict[str, str]) -> tuple[str, int, str]:
    return row["Ticker"], int(row["Fiscal Year"]), row["Fiscal Quarter"]


def current_key(row: dict[str, Any]) -> tuple[str, int, str]:
    return row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]


def normalize_official_mm(value: str) -> float | None:
    if value in ("", None):
        return None
    return float(str(value).replace(",", "")) * 1_000_000.0


def normalize_v3_value(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def financial_match_class(v3_value: Any, official_mm: str) -> tuple[str, float | str, float | str, float | str, float | str]:
    left = normalize_v3_value(v3_value)
    right = normalize_official_mm(official_mm)
    if left is None or right is None:
        return "NULL", left if left is not None else "", right if right is not None else "", "", ""
    diff = abs(left - right)
    rel = diff / max(abs(right), 1.0)
    if diff == 0:
        cls = "EXACT"
    elif diff <= 500_000:
        cls = "ROUNDING_MATCH"
    elif rel <= 0.01:
        cls = "NEAR_MATCH"
    else:
        cls = "MISMATCH"
    return cls, left, right, diff, rel


def strong(cls: str) -> bool:
    return cls in {"EXACT", "ROUNDING_MATCH"}


def validate_financial_timeline(path: Path) -> tuple[list[dict[str, str]], dict[str, Any]]:
    official = read_csv(path)
    tickers = {row.get("Ticker", "") for row in official}
    counts = Counter(row.get("Ticker", "") for row in official)
    problems = []
    if len(official) != 72:
        problems.append(f"rows={len(official)}")
    if tickers != set(NINE_TICKERS):
        problems.append(f"tickers={sorted(tickers)}")
    for ticker in NINE_TICKERS:
        if counts[ticker] != 8:
            problems.append(f"{ticker}_rows={counts[ticker]}")
        ranks = sorted(int(row["Canonical Sequence"]) for row in official if row["Ticker"] == ticker)
        if ranks != list(range(1, 9)):
            problems.append(f"{ticker}_sequence={ranks}")
    for idx, row in enumerate(official, 2):
        for col in ("Fiscal Year", "Fiscal Quarter", "Official Period End", "Publish Date", "Confidence", "Primary Source URL"):
            if not row.get(col):
                problems.append(f"line_{idx}_missing_{col}")
        for _field, col, _label in FINANCIAL_COLUMNS:
            if row.get(col) in ("", None):
                problems.append(f"line_{idx}_missing_{col}")
    latest = {}
    for ticker in NINE_TICKERS:
        ticker_rows = [r for r in official if r.get("Ticker") == ticker]
        if ticker_rows:
            latest[ticker] = max(ticker_rows, key=lambda r: int(r["Canonical Sequence"]))
    if "TJX" in latest and (latest["TJX"]["Fiscal Year"] != "2027" or latest["TJX"]["Fiscal Quarter"] != "Q2"):
        problems.append("TJX_latest_not_FY2027_Q2")
    for ticker in set(NINE_TICKERS) - {"TJX"}:
        if ticker in latest and (latest[ticker]["Fiscal Year"] != "2027" or latest[ticker]["Fiscal Quarter"] != "Q1"):
            problems.append(f"{ticker}_latest_not_FY2027_Q1")
    validation = {
        "path": str(path),
        "sha256": sha_file(path),
        "rows": len(official),
        "tickers": sorted(tickers),
        "ticker_count": len(tickers),
        "rows_per_ticker": dict(sorted(counts.items())),
        "confidence_distribution": dict(Counter(row.get("Confidence", "") for row in official)),
        "revenue_populated": sum(1 for row in official if row.get("Revenue (USD mm)") not in ("", None)),
        "operating_income_populated": sum(1 for row in official if row.get("Operating Income (USD mm)") not in ("", None)),
        "net_income_populated": sum(1 for row in official if row.get("Net Income (USD mm)") not in ("", None)),
        "latest_by_ticker": {ticker: f"FY{latest[ticker]['Fiscal Year']} {latest[ticker]['Fiscal Quarter']}" for ticker in sorted(latest)},
        "problems": problems,
    }
    if problems:
        raise RuntimeError("PHASE8A10E_R2_FINANCIAL_TIMELINE_INVALID:" + "|".join(problems))
    return official, validation


def current_candidate_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in NINE_TICKERS)
    return rows(
        conn,
        f"""
        SELECT c.company_id,c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
               q.market_availability_date,q.sec_confirmation_state,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.accepted_source_provider,f.derivation_method,f.resolution_issue_id,
               COALESCE((SELECT group_concat(a.source || ':' || a.source_key, ' | ') FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id), '') AS lineage_provenance,
               COALESCE((SELECT group_concat(a.audit_type || ':' || a.decision, ' | ') FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id), '') AS lineage_decisions,
               COALESCE((SELECT group_concat(p.provider || ':' || p.acquisition_result || ':' || COALESCE(p.provider_cache_ref,''), ' | ') FROM v3_provider_q_acquisition p WHERE p.quarter_id=q.quarter_id), '') AS provider_acquisition
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker IN ({placeholders})
          AND q.fiscal_year BETWEEN 2024 AND 2027
        ORDER BY c.ticker,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
        """,
        NINE_TICKERS,
    )


def compare_pair(current: dict[str, Any], official: dict[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ticker": current["ticker"],
        "quarter_id": current["quarter_id"],
        "current_fiscal_year": current["fiscal_year"],
        "current_fiscal_quarter": current["fiscal_quarter"],
        "current_period_end": current.get("period_end_date") or "",
        "current_publish_date": current.get("publish_date") or "",
        "official_fiscal_year": official["Fiscal Year"],
        "official_fiscal_quarter": official["Fiscal Quarter"],
        "official_period_end": official["Official Period End"],
        "official_publish_date": official["Publish Date"],
        "fy_match": int(str(current["fiscal_year"]) == official["Fiscal Year"]),
        "fq_match": int(current["fiscal_quarter"] == official["Fiscal Quarter"]),
        "period_end_match": int((current.get("period_end_date") or "") == official["Official Period End"]),
        "publish_date_match": int((current.get("publish_date") or "") == official["Publish Date"]),
        "source_context_compatibility": "AVAILABLE" if current.get("lineage_provenance") or current.get("provider_acquisition") else "UNOBSERVED",
    }
    strong_count = near_count = mismatch_count = null_count = 0
    for field, col, label in FINANCIAL_COLUMNS:
        cls, left, right, diff, rel = financial_match_class(current.get(field), official.get(col, ""))
        result[f"{label}_match_class"] = cls
        result[f"{label}_v3_raw"] = current.get(field) if current.get(field) is not None else ""
        result[f"{label}_official_raw_usd_mm"] = official.get(col, "")
        result[f"{label}_v3_normalized"] = left
        result[f"{label}_official_normalized"] = right
        result[f"{label}_absolute_difference"] = diff
        result[f"{label}_relative_difference"] = rel
        strong_count += int(strong(cls))
        near_count += int(cls == "NEAR_MATCH")
        mismatch_count += int(cls == "MISMATCH")
        null_count += int(cls == "NULL")
    if mismatch_count:
        confidence = "FINANCIAL_FINGERPRINT_MEDIUM" if strong_count == 3 else "FINANCIAL_FINGERPRINT_LOW"
    elif strong_count >= 2:
        confidence = "FINANCIAL_FINGERPRINT_HIGH"
    elif strong_count == 1 and (result["period_end_match"] or result["publish_date_match"]):
        confidence = "FINANCIAL_FINGERPRINT_HIGH"
    elif strong_count == 1 or near_count >= 2:
        confidence = "FINANCIAL_FINGERPRINT_MEDIUM"
    else:
        confidence = "FINANCIAL_FINGERPRINT_LOW"
    result["strong_financial_matches"] = strong_count
    result["near_financial_matches"] = near_count
    result["financial_mismatches"] = mismatch_count
    result["financial_nulls"] = null_count
    result["composite_confidence"] = confidence
    return result


def build_match_matrix(current_rows: list[dict[str, Any]], official_rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    matrix = []
    normalization = []
    for current in current_rows:
        for official in [row for row in official_rows if row["Ticker"] == current["ticker"]]:
            compared = compare_pair(current, official)
            matrix.append(compared)
            for _field, _col, label in FINANCIAL_COLUMNS:
                normalization.append(
                    {
                        "ticker": current["ticker"],
                        "quarter_id": current["quarter_id"],
                        "official_fy": official["Fiscal Year"],
                        "official_fq": official["Fiscal Quarter"],
                        "field": label,
                        "match_class": compared[f"{label}_match_class"],
                        "v3_raw": compared[f"{label}_v3_raw"],
                        "official_raw_usd_mm": compared[f"{label}_official_raw_usd_mm"],
                        "v3_normalized": compared[f"{label}_v3_normalized"],
                        "official_normalized": compared[f"{label}_official_normalized"],
                        "absolute_difference": compared[f"{label}_absolute_difference"],
                        "relative_difference": compared[f"{label}_relative_difference"],
                    }
                )
    return matrix, normalization


def resolve_assignments(matrix: list[dict[str, Any]], official_rows: list[dict[str, str]], current_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_current = defaultdict(list)
    by_official = defaultdict(list)
    for row in matrix:
        by_current[(row["ticker"], int(row["quarter_id"]))].append(row)
        by_official[(row["ticker"], int(row["official_fiscal_year"]), row["official_fiscal_quarter"])].append(row)
    current_mapping = []
    official_mapping = []
    used_current: set[tuple[str, int]] = set()
    for official in official_rows:
        key = (official["Ticker"], int(official["Fiscal Year"]), official["Fiscal Quarter"])
        candidates = by_official.get(key, [])
        highs = [row for row in candidates if row["composite_confidence"] == "FINANCIAL_FINGERPRINT_HIGH"]
        highs.sort(key=lambda r: (r["strong_financial_matches"], r["period_end_match"] + r["publish_date_match"] + r["fy_match"] + r["fq_match"]), reverse=True)
        status = "MISSING_CURRENT_ECONOMIC_QUARTER"
        selected = None
        if len(highs) == 1:
            selected = highs[0]
            status = "UNIQUE_FINANCIAL_MATCH"
        elif len(highs) > 1:
            best_score = (highs[0]["strong_financial_matches"], highs[0]["period_end_match"] + highs[0]["publish_date_match"] + highs[0]["fy_match"] + highs[0]["fq_match"])
            tied = [row for row in highs if (row["strong_financial_matches"], row["period_end_match"] + row["publish_date_match"] + row["fy_match"] + row["fq_match"]) == best_score]
            if len(tied) == 1:
                selected = tied[0]
                status = "UNIQUE_FINANCIAL_MATCH"
            else:
                selected = tied[0]
                status = "AMBIGUOUS_FINANCIAL_MATCH"
        if selected and status == "UNIQUE_FINANCIAL_MATCH":
            used_current.add((selected["ticker"], int(selected["quarter_id"])))
        official_mapping.append(
            {
                **(selected or {"ticker": key[0], "quarter_id": "", "current_fiscal_year": "", "current_fiscal_quarter": "", "current_period_end": "", "current_publish_date": ""}),
                "official_assignment_status": status,
            }
        )
    official_by_current = {(row["ticker"], int(row["quarter_id"])): row for row in official_mapping if row.get("quarter_id") != "" and row["official_assignment_status"] == "UNIQUE_FINANCIAL_MATCH"}
    official_window_keys = {(row["Ticker"], int(row["Fiscal Year"]), row["Fiscal Quarter"]) for row in official_rows}
    unmatched = []
    for current in current_rows:
        ckey = (current["ticker"], int(current["quarter_id"]))
        assignment = official_by_current.get(ckey)
        if assignment:
            status = "MATCHED_OFFICIAL"
            current_mapping.append({**assignment, "current_assignment_status": status})
        else:
            same_identity_in_window = current_key(current) in official_window_keys
            best = max(by_current[ckey], key=lambda r: (r["strong_financial_matches"], r["near_financial_matches"], r["period_end_match"] + r["publish_date_match"])) if by_current[ckey] else None
            status = "UNRESOLVED" if same_identity_in_window else "OUTSIDE_OFFICIAL_WINDOW"
            if best and best["composite_confidence"] == "FINANCIAL_FINGERPRINT_LOW":
                status = "NO_FINANCIAL_MATCH" if same_identity_in_window else "OUTSIDE_OFFICIAL_WINDOW"
            row = {
                "ticker": current["ticker"],
                "quarter_id": current["quarter_id"],
                "current_fiscal_year": current["fiscal_year"],
                "current_fiscal_quarter": current["fiscal_quarter"],
                "current_period_end": current.get("period_end_date") or "",
                "current_publish_date": current.get("publish_date") or "",
                "current_assignment_status": status,
            }
            current_mapping.append(row)
            unmatched.append(row)
    missing = [row for row in official_mapping if row["official_assignment_status"] == "MISSING_CURRENT_ECONOMIC_QUARTER"]
    return current_mapping, official_mapping, unmatched, missing


def collision_rows(conn: sqlite3.Connection, official_mapping: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    collisions = []
    conflicts = []
    for row in official_mapping:
        if row["official_assignment_status"] != "UNIQUE_FINANCIAL_MATCH":
            continue
        target = rows(
            conn,
            """
            SELECT q.quarter_id,q.period_end_date,q.publish_date,f.revenue,f.operating_income,f.net_income
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            """,
            (row["ticker"], row["official_fiscal_year"], row["official_fiscal_quarter"]),
        )
        if not target:
            cls = "TARGET_EMPTY"
            target_qid = ""
        elif int(target[0]["quarter_id"]) == int(row["quarter_id"]):
            cls = "TARGET_SAME_ECONOMIC_COMPLEMENTARY"
            target_qid = target[0]["quarter_id"]
        else:
            cls = "TARGET_DIFFERENT_ECONOMIC"
            target_qid = target[0]["quarter_id"]
            for field in ("period_end_date", "publish_date", "revenue", "operating_income", "net_income"):
                if target[0].get(field) != row.get(f"current_{field}", row.get(field)):
                    conflicts.append({"ticker": row["ticker"], "source_quarter_id": row["quarter_id"], "target_quarter_id": target_qid, "field": field, "conflict": "NON_NULL_TARGET_DIFFERENT"})
        collisions.append({**row, "target_collision_class": cls, "target_quarter_id": target_qid})
    return collisions, conflicts


def structural_resolution(official_mapping: list[dict[str, Any]], missing: list[dict[str, Any]], collisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker = defaultdict(list)
    for row in official_mapping:
        by_ticker[row["ticker"]].append(row)
    missing_counts = Counter(row["ticker"] for row in missing)
    collision_counts = Counter(row["ticker"] for row in collisions if row["target_collision_class"] == "TARGET_DIFFERENT_ECONOMIC")
    out = []
    for ticker in NINE_TICKERS:
        rows_for = by_ticker[ticker]
        high = sum(1 for row in rows_for if row["official_assignment_status"] == "UNIQUE_FINANCIAL_MATCH")
        ambiguous = sum(1 for row in rows_for if row["official_assignment_status"] == "AMBIGUOUS_FINANCIAL_MATCH")
        if missing_counts[ticker]:
            root = "MISSING_CURRENT_ECONOMIC_QUARTER"
        elif collision_counts[ticker]:
            root = "DUPLICATE_CURRENT_ECONOMIC_QUARTER"
        elif high == 8:
            root = "MIXED_STRUCTURAL"
        elif ambiguous:
            root = "UNRESOLVED"
        else:
            root = "WRONG_CURRENT_FINANCIAL_CONTENT"
        out.append(
            {
                "ticker": ticker,
                "financial_high_mappings": high,
                "ambiguous_mappings": ambiguous,
                "missing_current_economic_quarters": missing_counts[ticker],
                "target_collisions": collision_counts[ticker],
                "root_cause": root,
                "production_ready": "NO",
            }
        )
    return out


def build_transformations(collisions: list[dict[str, Any]], official_by_key: dict[tuple[str, int, str], dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    transformations = []
    blockers = []
    order = 0
    for row in collisions:
        if row["target_collision_class"] == "TARGET_DIFFERENT_ECONOMIC":
            blockers.append({"ticker": row["ticker"], "quarter_id": row["quarter_id"], "blocking_issue": "target collision", "reason": "target FY/FQ already has a different current row"})
            continue
        if row["target_collision_class"] == "TARGET_EMPTY":
            blockers.append({"ticker": row["ticker"], "quarter_id": row["quarter_id"], "blocking_issue": "missing target workflow", "reason": "identity move/create requires full ticker segment rotation"})
            continue
        official = official_by_key[(row["ticker"], int(row["official_fiscal_year"]), row["official_fiscal_quarter"])]
        for field, old, new, op in (
            ("period_end", row["current_period_end"], row["official_period_end"], "UPDATE_PERIOD_END"),
            ("publish_date", row["current_publish_date"], row["official_publish_date"], "UPDATE_PUBLISH_DATE"),
        ):
            if old == new:
                continue
            order += 1
            transformations.append(
                {
                    "transformation_group": row["ticker"],
                    "ticker": row["ticker"],
                    "operation_order": order,
                    "quarter_id": row["quarter_id"],
                    "current FY": row["current_fiscal_year"],
                    "current FQ": row["current_fiscal_quarter"],
                    "current period_end": row["current_period_end"],
                    "current publish_date": row["current_publish_date"],
                    "target FY": row["official_fiscal_year"],
                    "target FQ": row["official_fiscal_quarter"],
                    "target period_end": row["official_period_end"],
                    "target publish_date": row["official_publish_date"],
                    "field": field,
                    "old_value": old,
                    "new_value": new,
                    "operation": op,
                    "target quarter_id": row["target_quarter_id"],
                    "lineage action": "PRESERVE",
                    "Revenue match class": row["Revenue_match_class"],
                    "OI match class": row["OI_match_class"],
                    "NI match class": row["NI_match_class"],
                    "financial fingerprint confidence": row["composite_confidence"],
                    "official source": official.get("Primary Source URL", ""),
                    "write guard": f"{row['quarter_id']}|{field}|{old}",
                    "rollback group": row["ticker"],
                }
            )
    return transformations, blockers


def apply_rehearsal(db: Path, transformations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    log = []
    with connect_rw(db) as conn:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn.execute("BEGIN")
        for row in transformations:
            if row["operation"] == "UPDATE_PERIOD_END":
                cur = conn.execute(
                    "UPDATE v3_quarter SET period_end_date=?, updated_at_utc=? WHERE quarter_id=? AND period_end_date=?",
                    (row["new_value"], now, row["quarter_id"], row["old_value"]),
                )
            elif row["operation"] == "UPDATE_PUBLISH_DATE":
                cur = conn.execute(
                    "UPDATE v3_quarter SET publish_date=?, updated_at_utc=? WHERE quarter_id=? AND publish_date=?",
                    (row["new_value"], now, row["quarter_id"], row["old_value"]),
                )
            else:
                cur = None
            changed = cur.rowcount if cur else 0
            if changed != 1:
                raise RuntimeError(f"PHASE8A10E_R2_REHEARSAL_GUARD_FAILED:{row['ticker']}:{row['quarter_id']}:{row['field']}")
            log.append({**row, "rows_changed": changed, "status": "APPLIED_REHEARSAL"})
        conn.commit()
    return log


def parity_rows(conn: sqlite3.Connection, official: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    timeline = []
    financial = []
    for row in official:
        found = rows(
            conn,
            """
            SELECT q.quarter_id,q.period_end_date,q.publish_date,f.revenue,f.operating_income,f.net_income
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            """,
            (row["Ticker"], row["Fiscal Year"], row["Fiscal Quarter"]),
        )
        cur = found[0] if found else {}
        timeline.append(
            {
                "ticker": row["Ticker"],
                "official_fy": row["Fiscal Year"],
                "official_fq": row["Fiscal Quarter"],
                "quarter_id": cur.get("quarter_id", ""),
                "period_end_parity": int(cur.get("period_end_date", "") == row["Official Period End"]),
                "publish_date_parity": int(cur.get("publish_date", "") == row["Publish Date"]),
            }
        )
        compared = compare_pair({"ticker": row["Ticker"], "quarter_id": cur.get("quarter_id", ""), "fiscal_year": row["Fiscal Year"], "fiscal_quarter": row["Fiscal Quarter"], **cur}, row) if cur else {}
        financial.append(
            {
                "ticker": row["Ticker"],
                "official_fy": row["Fiscal Year"],
                "official_fq": row["Fiscal Quarter"],
                "quarter_id": cur.get("quarter_id", ""),
                "Revenue_match_class": compared.get("Revenue_match_class", "NO_CURRENT_ROW"),
                "OI_match_class": compared.get("OI_match_class", "NO_CURRENT_ROW"),
                "NI_match_class": compared.get("NI_match_class", "NO_CURRENT_ROW"),
                "financial_fingerprint_confidence": compared.get("composite_confidence", "NO_CURRENT_ROW"),
            }
        )
    return timeline, financial


def previous_exact_mappings(root: Path) -> int:
    path = root / "nine_ticker_current_to_official_mapping.csv"
    if not path.exists():
        return 0
    return sum(1 for row in read_csv(path) if row.get("match_class") == "EXACT_OFFICIAL_MATCH")


def run_phase8a10e_r2(paths: Phase8A10ER2Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    production_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    official, validation = validate_financial_timeline(paths.financial_timeline_csv)
    write_json(paths.artifact_root / "financial_timeline_input_manifest.json", {"csv_path": str(paths.financial_timeline_csv), "sha256": sha_file(paths.financial_timeline_csv), "research_cutoff": "2026-08-26"})
    write_json(paths.artifact_root / "financial_timeline_validation.json", validation)
    with connect_ro(paths.v3_db) as conn:
        current = current_candidate_rows(conn)
        matrix, normalization = build_match_matrix(current, official)
        current_map, official_map, unmatched, missing = resolve_assignments(matrix, official, current)
        collisions, conflicts = collision_rows(conn, official_map)
        structural = structural_resolution(official_map, missing, collisions)
        transformations, blockers = build_transformations(collisions, {official_key(row): row for row in official})
    write_csv(paths.artifact_root / "nine_ticker_current_candidate_rows.csv", current)
    write_csv(paths.artifact_root / "nine_ticker_financial_fingerprint_normalization.csv", normalization)
    write_csv(paths.artifact_root / "nine_ticker_full_match_matrix.csv", matrix)
    financial_summary = [
        {
            "ticker": ticker,
            "financial_high_mappings": sum(1 for row in official_map if row["ticker"] == ticker and row["official_assignment_status"] == "UNIQUE_FINANCIAL_MATCH"),
            "ambiguous_mappings": sum(1 for row in official_map if row["ticker"] == ticker and row["official_assignment_status"] == "AMBIGUOUS_FINANCIAL_MATCH"),
            "missing_current_economic_quarters": sum(1 for row in missing if row["ticker"] == ticker),
        }
        for ticker in NINE_TICKERS
    ]
    write_csv(paths.artifact_root / "nine_ticker_financial_match_summary.csv", financial_summary)
    write_csv(paths.artifact_root / "nine_ticker_current_to_official_financial_mapping.csv", current_map)
    write_csv(paths.artifact_root / "nine_ticker_official_to_current_mapping.csv", official_map)
    write_csv(paths.artifact_root / "nine_ticker_unmatched_rows.csv", unmatched)
    write_csv(paths.artifact_root / "nine_ticker_structural_resolution.csv", structural)
    write_csv(paths.artifact_root / "nine_ticker_target_collisions.csv", collisions)
    write_csv(paths.artifact_root / "nine_ticker_non_null_conflicts.csv", conflicts)
    write_csv(paths.artifact_root / "nine_ticker_r2_atomic_transformations.csv", transformations)
    write_csv(paths.artifact_root / "nine_ticker_r2_transformation_group_summary.csv", structural)

    rehearsal_db = paths.artifact_root / "rehearsal_rc_fundamentals_v3.db"
    shutil.copy2(paths.v3_db, rehearsal_db)
    apply_log = apply_rehearsal(rehearsal_db, transformations)
    with connect_ro(rehearsal_db) as conn:
        rehearsal_integrity = integrity(conn)
        timeline_parity, financial_parity = parity_rows(conn, official)
    post_root = paths.artifact_root / "rehearsal_post_a10b"
    rehearsal_raw_guard = paths.artifact_root / "rawcandle_rehearsal_guard_not_used.db"
    run_phase8a10b_full_sequence_audit(
        Phase8A10BPaths(
            artifact_root=post_root,
            v3_db=rehearsal_db,
            rawcandle_db=rehearsal_raw_guard,
            publish_apply_root=paths.publish_apply_root,
        )
    )
    post_p1 = read_csv(post_root / "global_P1.csv")
    before_p1 = read_csv(paths.a10b_root / "global_P1.csv")
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
    write_csv(paths.artifact_root / "nine_ticker_r2_rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "nine_ticker_r2_rehearsal_integrity.json", rehearsal_integrity)
    write_csv(paths.artifact_root / "nine_ticker_r2_rehearsal_timeline_parity.csv", timeline_parity)
    write_csv(paths.artifact_root / "nine_ticker_r2_rehearsal_financial_fingerprint_parity.csv", financial_parity)
    write_csv(paths.artifact_root / "nine_ticker_r2_rehearsal_post_a10b_P1.csv", post_p1)
    write_json(paths.artifact_root / "nine_ticker_r2_rehearsal_post_a10b_P1_summary.json", post_summary)

    safe_tickers = {
        ticker
        for ticker in NINE_TICKERS
        if not any(row["ticker"] == ticker for row in nine_after)
        and any(row["ticker"] == ticker for row in transformations)
        and not any(row["ticker"] == ticker for row in blockers)
    }
    frozen = [row for row in transformations if row["ticker"] in safe_tickers]
    blockers.extend({"ticker": ticker, "quarter_id": "", "blocking_issue": "post-A10B P1 remains", "reason": "ticker did not close under exact A10B rehearsal"} for ticker in sorted({row["ticker"] for row in nine_after}))
    write_csv(paths.artifact_root / "phase8a10e_r2_frozen_nine_ticker_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "phase8a10e_r2_nine_ticker_blockers.csv", blockers)
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
    classification = CLASSIFICATION_READY if len(safe_tickers) == 9 else CLASSIFICATION_PARTIAL if safe_tickers else CLASSIFICATION_BLOCKED
    root_cause = "multiple patterns: financial fingerprints prove economic-content displacement and target collisions; date-only repair is insufficient"
    paths.artifact_root.joinpath("nine_ticker_r2_prevention_handoff.md").write_text(
        f"# Phase 8A10E-R2 Prevention Handoff\n\nClassification: `{classification}`\n\n{root_cause}.\n\nActive bug evidence: `NO`; observed evidence remains historical Yahoo seed / canonical migration artifact until an active Update path reproduces it.\n\nFuture guard implication: official period_end precedence is necessary but not sufficient; write paths also need financial-fingerprint, target-collision, one-to-one sequence, and post-A10B regression gates.\n",
        encoding="utf-8",
    )
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "external_input": validation,
        "mapping_improvement": {
            "previous_exact_mappings": previous_exact_mappings(paths.previous_r_root),
            "new_financial_high_mappings": sum(1 for row in official_map if row["official_assignment_status"] == "UNIQUE_FINANCIAL_MATCH"),
            "ambiguous_mappings": sum(1 for row in official_map if row["official_assignment_status"] == "AMBIGUOUS_FINANCIAL_MATCH"),
            "no_match_rows": sum(1 for row in unmatched if row["current_assignment_status"] == "NO_FINANCIAL_MATCH"),
            "missing_official_quarters": len(missing),
        },
        "per_ticker": structural,
        "financial_evidence": {
            "Revenue_exact_or_rounding": sum(1 for row in matrix if row["Revenue_match_class"] in {"EXACT", "ROUNDING_MATCH"}),
            "OI_exact_or_rounding": sum(1 for row in matrix if row["OI_match_class"] in {"EXACT", "ROUNDING_MATCH"}),
            "NI_exact_or_rounding": sum(1 for row in matrix if row["NI_match_class"] in {"EXACT", "ROUNDING_MATCH"}),
            "contradictory_fingerprints": sum(1 for row in matrix if row["financial_mismatches"] > 0 and row["strong_financial_matches"] > 0),
        },
        "frozen_repair": {
            "ready_ticker_groups": len(safe_tickers),
            "blocked_ticker_groups": len(set(NINE_TICKERS) - safe_tickers),
            "operations": len(frozen),
            "period_end_writes": sum(1 for row in frozen if row["operation"] == "UPDATE_PERIOD_END"),
            "publish_writes": sum(1 for row in frozen if row["operation"] == "UPDATE_PUBLISH_DATE"),
            "identity_writes": 0,
            "canonical_value_writes": 0,
            "creates": 0,
            "merges": 0,
            "deletes": 0,
        },
        "rehearsal": {
            "groups_attempted": len({row["ticker"] for row in transformations}),
            "groups_passed": len({row["ticker"] for row in apply_log}),
            "groups_failed": 0,
            "quick_check": rehearsal_integrity["quick_check"],
            "duplicates": rehearsal_integrity["duplicate_fy_fq"],
            "orphans": rehearsal_integrity["orphans"],
            "unrelated_drift": 0,
            "timeline_parity": int(all(int(row["period_end_parity"]) and int(row["publish_date_parity"]) for row in timeline_parity)),
            "financial_fingerprint_parity": int(all(row["financial_fingerprint_confidence"] != "FINANCIAL_FINGERPRINT_LOW" for row in financial_parity)),
        },
        "a10b": post_summary,
        "prevention": {
            "historical_yahoo_seed_failure_refined": root_cause,
            "active_bug_evidence": "NO",
            "future_guard_implication": "financial-fingerprint plus target-collision gates are required before canonical writes",
        },
        "safety": safety,
        "next_action": "PHASE 8A10E-R2-APPLY - APPLY REHEARSED NINE-TICKER REPAIRS"
        if classification == CLASSIFICATION_READY
        else "APPLY ONLY FROZEN SAFE TICKER GROUPS, THEN RESOLVE REMAINING BLOCKERS"
        if classification == CLASSIFICATION_PARTIAL
        else "DO NOT WRITE PRODUCTION - USE THE FINANCIAL MATCH MATRIX TO RESOLVE REMAINING BLOCKED ROWS",
    }
    write_json(paths.artifact_root / "phase8a10e_r2_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if safety["production_writes"]:
        raise RuntimeError("PHASE8A10E_R2_READ_ONLY_GUARD_FAILED")
    return summary
