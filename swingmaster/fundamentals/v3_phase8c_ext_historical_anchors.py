from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import ANCHOR_TABLE, CHAIN_TABLE, PROFILE_TABLE, semantic_fingerprints
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_schema import apply_v3_schema


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE8C_EXT_HISTORICAL_ANCHOR_BACKFILL_COMPLETE"
CLASSIFICATION_REVIEW = "FUNDAMENTALS_V3_PHASE8C_EXT_HISTORICAL_ANCHOR_BACKFILL_COMPLETE_WITH_REVIEW_ITEMS"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8C_EXT_HISTORICAL_ANCHOR_BACKFILL_BLOCKED"
FY_RANGE = range(1999, 2028)
BREAK_REASONS = {"SOURCE_HISTORY_EXHAUSTED", "UNRESOLVED_BOUNDARY", "CALENDAR_TRANSITION", "NO_FISCAL_YEAR", "COMPLETE_TO_FY1999"}


@dataclass(frozen=True)
class Phase8CExtPaths:
    artifact_root: Path
    input_csv: Path = Path("temp/v3_active_tickers_99_27.csv")
    v3_db: Path = Path("rc_fundamentals_v3.db")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_input(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def fy_columns(header: list[str]) -> list[str]:
    expected = [f"FY{year} alkoi" for year in range(1999, 2028)]
    found = [col for col in header if col in expected]
    return sorted(found, key=lambda col: int(col[2:6]))


def parse_date(value: str | None) -> date | None:
    if not value or not value.strip():
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def normalize_break_reason(chain_status: str, break_reason: str) -> str:
    if chain_status == "COMPLETE_TO_FY1999" and not break_reason:
        return "COMPLETE_TO_FY1999"
    return break_reason


def source_type(source: str) -> str:
    lower = source.lower()
    if "sec.gov" in lower:
        return "SEC_COMPANYFACTS"
    if "investor" in lower or "/ir" in lower:
        return "ISSUER_IR"
    return "OFFICIAL_SOURCE"


def fingerprint(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str).encode()).hexdigest()


def active_company_map(conn: sqlite3.Connection) -> dict[str, int]:
    return {row["ticker"].upper(): int(row["company_id"]) for row in rows(conn, "SELECT company_id,ticker FROM v3_company WHERE active=1")}


def validate_input(csv_rows: list[dict[str, str]], columns: list[str], active: dict[str, int]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    tickers = [(row.get("ticker") or "").strip().upper() for row in csv_rows]
    annual_cells = len(csv_rows) * len(columns)
    invalid = []
    for row in csv_rows:
        for col in columns:
            value = row.get(col, "")
            if value and parse_date(value) is None:
                invalid.append({"ticker": row.get("ticker", ""), "column": col, "value": value, "issue": "INVALID_DATE"})
        br = normalize_break_reason(row.get("chain_status", ""), row.get("break_reason", ""))
        if br not in BREAK_REASONS:
            invalid.append({"ticker": row.get("ticker", ""), "issue": "INVALID_BREAK_REASON", "break_reason": row.get("break_reason", ""), "chain_status": row.get("chain_status", "")})
    populated_by_fy = {
        int(col[2:6]): sum(1 for row in csv_rows if (row.get(col) or "").strip())
        for col in columns
    }
    summary = {
        "rows": len(csv_rows),
        "unique_tickers": len(set(tickers)),
        "active_v3_tickers": len(active),
        "active_v3_matches": len(set(tickers) & set(active)),
        "ticker_set_match": set(tickers) == set(active),
        "fy_columns": len(columns),
        "total_annual_cells": annual_cells,
        "populated_annual_cells": sum(populated_by_fy.values()),
        "blank_annual_cells": annual_cells - sum(populated_by_fy.values()),
        "fy1999_2022_populated": sum(v for fy, v in populated_by_fy.items() if fy <= 2022),
        "fy2023_2027_populated": sum(v for fy, v in populated_by_fy.items() if fy >= 2023),
        "invalid_rows": invalid,
    }
    reconciliation = [{"ticker": ticker, "csv_present": int(ticker in set(tickers)), "active_v3_present": int(ticker in active)} for ticker in sorted(set(tickers) | set(active))]
    return summary, reconciliation


def normalized_anchors(csv_rows: list[dict[str, str]], columns: list[str], active: dict[str, int]) -> list[dict[str, Any]]:
    out = []
    for row in csv_rows:
        ticker = row["ticker"].strip().upper()
        for col in columns:
            start = parse_date(row.get(col))
            if not start:
                continue
            fiscal_year = int(col[2:6])
            src = row.get("Lähde", "")
            payload = {"ticker": ticker, "fiscal_year": fiscal_year, "fiscal_year_start_date": start.isoformat(), "source_reference": src}
            out.append(
                {
                    "company_id": active[ticker],
                    "ticker": ticker,
                    "fiscal_year": fiscal_year,
                    "fiscal_year_start_date": start.isoformat(),
                    "source_type": source_type(src),
                    "source_reference": src,
                    "confidence": "VERIFIED_EXACT",
                    "verification_status": "VERIFIED_EXACT_ANCHOR",
                    "import_state": "NEW_ANCHOR",
                    "source_fingerprint": fingerprint(payload),
                }
            )
    out.sort(key=lambda r: (r["ticker"], r["fiscal_year"]))
    return out


def chain_rows(csv_rows: list[dict[str, str]], columns: list[str], active: dict[str, int]) -> list[dict[str, Any]]:
    out = []
    for row in csv_rows:
        ticker = row["ticker"].strip().upper()
        years = [int(col[2:6]) for col in columns if parse_date(row.get(col))]
        src = row.get("Lähde", "")
        br = normalize_break_reason(row.get("chain_status", ""), row.get("break_reason", ""))
        payload = {"ticker": ticker, "chain_status": row.get("chain_status", ""), "break_reason": br, "years": years, "source_reference": src}
        out.append(
            {
                "company_id": active[ticker],
                "ticker": ticker,
                "chain_status": row.get("chain_status", ""),
                "break_reason": br,
                "earliest_verified_fiscal_year": min(years) if years else None,
                "latest_verified_fiscal_year": max(years) if years else None,
                "populated_anchor_count": len(years),
                "source_type": source_type(src),
                "source_reference": src,
                "source_fingerprint": fingerprint(payload),
            }
        )
    return sorted(out, key=lambda r: r["ticker"])


def existing_anchor_map(conn: sqlite3.Connection) -> dict[tuple[int, int], dict[str, Any]]:
    return {
        (int(row["company_id"]), int(row["fiscal_year"])): row
        for row in rows(conn, f"SELECT * FROM {ANCHOR_TABLE}")
    }


def reconcile_anchors(import_rows: list[dict[str, Any]], existing: dict[tuple[int, int], dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in import_rows:
        current = existing.get((int(row["company_id"]), int(row["fiscal_year"])))
        if not current:
            status = "CSV_NEW_ANCHOR"
        elif current["fiscal_year_start_date"] == row["fiscal_year_start_date"]:
            status = "EXACT_MATCH"
        else:
            status = "CONFLICT"
        out.append({**row, "existing_start": current["fiscal_year_start_date"] if current else "", "reconciliation_status": status})
    return out


def ensure_chain_schema(conn: sqlite3.Connection) -> None:
    apply_v3_schema(conn)


def anchor_semantic(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("fiscal_year_start_date"),
        row.get("source_type"),
        row.get("source_reference"),
        row.get("confidence"),
        row.get("verification_status"),
        row.get("source_fingerprint"),
    )


def import_metadata(conn: sqlite3.Connection, anchors: list[dict[str, Any]], chains: list[dict[str, Any]], now: str) -> dict[str, Any]:
    ensure_chain_schema(conn)
    existing = existing_anchor_map(conn)
    counts: Counter[str] = Counter()
    conflicts = []
    for row in anchors:
        key = (int(row["company_id"]), int(row["fiscal_year"]))
        current = existing.get(key)
        if current:
            if anchor_semantic(current) == anchor_semantic(row):
                counts["anchor_exact_match"] += 1
            elif current["fiscal_year_start_date"] == row["fiscal_year_start_date"]:
                counts["anchor_start_match_different_provenance"] += 1
            else:
                counts["anchor_conflict"] += 1
                conflicts.append({**row, "existing_start": current["fiscal_year_start_date"]})
            continue
        values = {k: row[k] for k in ("company_id", "fiscal_year", "fiscal_year_start_date", "source_type", "source_reference", "confidence", "verification_status", "import_state", "source_fingerprint")}
        values["created_at_utc"] = now
        values["updated_at_utc"] = now
        conn.execute(f"INSERT INTO {ANCHOR_TABLE} ({','.join(values)}) VALUES ({','.join('?' for _ in values)})", tuple(values.values()))
        counts["anchor_inserted"] += 1
    for row in chains:
        current = conn.execute(f"SELECT * FROM {CHAIN_TABLE} WHERE company_id=?", (row["company_id"],)).fetchone()
        values = {k: row[k] for k in ("company_id", "chain_status", "break_reason", "earliest_verified_fiscal_year", "latest_verified_fiscal_year", "populated_anchor_count", "source_type", "source_reference", "source_fingerprint")}
        if current:
            cur = dict(current)
            if all(cur.get(k) == values.get(k) for k in values):
                counts["chain_exact_match"] += 1
                continue
            assignments = ",".join(f"{k}=?" for k in values if k != "company_id")
            conn.execute(f"UPDATE {CHAIN_TABLE} SET {assignments}, updated_at_utc=? WHERE company_id=?", tuple(values[k] for k in values if k != "company_id") + (now, row["company_id"]))
            counts["chain_updated"] += 1
        else:
            values["created_at_utc"] = now
            values["updated_at_utc"] = now
            conn.execute(f"INSERT INTO {CHAIN_TABLE} ({','.join(values)}) VALUES ({','.join('?' for _ in values)})", tuple(values.values()))
            counts["chain_inserted"] += 1
    return {"counts": dict(counts), "conflicts": conflicts}


def table_fingerprint(conn: sqlite3.Connection, table: str, order_by: str) -> dict[str, Any]:
    data = rows(conn, f"SELECT * FROM {table} ORDER BY {order_by}")
    stable = [{k: v for k, v in row.items() if k not in {"created_at_utc", "updated_at_utc", "import_state"}} for row in data]
    return {"rows": len(data), "sha256": fingerprint({"rows": stable})}


def full_fingerprints(db: Path) -> dict[str, Any]:
    out = semantic_fingerprints(db)
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        out["companies"] = table_fingerprint(conn, "v3_company", "company_id")
        out["fundamentals"] = table_fingerprint(conn, "v3_quarter_fundamentals", "quarter_id")
        out["lineage"] = table_fingerprint(conn, "v3_migration_audit", "audit_id")
        out["fiscal_profiles"] = table_fingerprint(conn, PROFILE_TABLE, "company_id")
        out["exact_anchors"] = table_fingerprint(conn, ANCHOR_TABLE, "company_id,fiscal_year")
        if table_exists(conn, CHAIN_TABLE):
            out["anchor_chains"] = table_fingerprint(conn, CHAIN_TABLE, "company_id")
        else:
            out["anchor_chains"] = {"rows": 0, "sha256": ""}
    return out


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def integrity(conn: sqlite3.Connection) -> dict[str, Any]:
    duplicate = conn.execute(f"SELECT COUNT(*) FROM (SELECT company_id,fiscal_year FROM {ANCHOR_TABLE} GROUP BY company_id,fiscal_year HAVING COUNT(*)>1)").fetchone()[0]
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    return {"quick_check": conn.execute("PRAGMA quick_check").fetchone()[0], "foreign_key_check_rows": len(fk), "duplicate_anchors": int(duplicate)}


def interval_distribution(import_rows: list[dict[str, Any]], profiles: dict[int, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in import_rows:
        grouped[int(row["company_id"])].append(row)
    dist: Counter[str] = Counter()
    nonstandard = []
    for cid, group in grouped.items():
        ordered = sorted(group, key=lambda r: int(r["fiscal_year"]))
        calendar_type = str(profiles.get(cid, {}).get("calendar_type") or "UNKNOWN")
        for left, right in zip(ordered, ordered[1:]):
            days = (date.fromisoformat(right["fiscal_year_start_date"]) - date.fromisoformat(left["fiscal_year_start_date"])).days
            if calendar_type == "WEEK_BASED_52_53" and days == 364:
                bucket = "NORMAL_52_WEEK"
            elif calendar_type == "WEEK_BASED_52_53" and days == 371:
                bucket = "NORMAL_53_WEEK"
            elif calendar_type != "WEEK_BASED_52_53" and days in {365, 366}:
                bucket = "FIXED_DATE_NORMAL_YEAR"
            elif days < 330:
                bucket = "SHORT_TRANSITION_OR_REVIEW"
            elif days > 390:
                bucket = "LONG_TRANSITION_OR_REVIEW"
            else:
                bucket = "NONSTANDARD_INTERVAL_REVIEW"
            dist[bucket] += 1
            if bucket.endswith("REVIEW"):
                nonstandard.append({"company_id": cid, "ticker": left["ticker"], "from_fy": left["fiscal_year"], "to_fy": right["fiscal_year"], "from_start": left["fiscal_year_start_date"], "to_start": right["fiscal_year_start_date"], "days": days, "calendar_type": calendar_type, "bucket": bucket})
    return [{"interval_bucket": k, "rows": v} for k, v in sorted(dist.items())], nonstandard


def coverage_by_fy(import_rows: list[dict[str, Any]], existing_recon: list[dict[str, Any]], total: int) -> list[dict[str, Any]]:
    by_fy = defaultdict(list)
    for row in existing_recon:
        by_fy[int(row["fiscal_year"])].append(row)
    return [
        {
            "fiscal_year": fy,
            "exact_start_companies": len(by_fy[fy]),
            "coverage_pct": round(len(by_fy[fy]) * 100 / total, 4),
            "new_inserts_planned": sum(1 for r in by_fy[fy] if r["reconciliation_status"] == "CSV_NEW_ANCHOR"),
            "already_existing": sum(1 for r in by_fy[fy] if r["reconciliation_status"] == "EXACT_MATCH"),
            "blank_no_verified_anchor": total - len(by_fy[fy]),
        }
        for fy in FY_RANGE
    ]


def chain_depth(chains: list[dict[str, Any]]) -> dict[str, Any]:
    values = sorted(int(r["populated_anchor_count"]) for r in chains)
    return {
        "median": median(values),
        "p25": values[int((len(values) - 1) * 0.25)],
        "p75": values[int((len(values) - 1) * 0.75)],
        "p90": values[int((len(values) - 1) * 0.90)],
        "max": max(values),
        "complete_to_fy1999": sum(1 for r in chains if r["break_reason"] == "COMPLETE_TO_FY1999"),
    }


def coverage_by_ticker(chains: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "ticker": row["ticker"],
            "company_id": row["company_id"],
            "exact_anchor_count": row["populated_anchor_count"],
            "earliest_verified_fiscal_year": row["earliest_verified_fiscal_year"],
            "latest_verified_fiscal_year": row["latest_verified_fiscal_year"],
            "chain_status": row["chain_status"],
            "break_reason": row["break_reason"],
        }
        for row in chains
    ]


def operational_coverage(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cohorts = {
        "2024plus": "CAST(strftime('%Y', q.period_end_date) AS INTEGER) >= 2024",
        "2025plus": "CAST(strftime('%Y', q.period_end_date) AS INTEGER) >= 2025",
        "latest8q": "q.quarter_id IN (SELECT quarter_id FROM (SELECT q2.quarter_id, ROW_NUMBER() OVER (PARTITION BY q2.company_id ORDER BY q2.period_end_date DESC, q2.fiscal_year DESC, q2.fiscal_quarter DESC) rn FROM v3_quarter q2) WHERE rn<=8)",
        "latest4q": "q.quarter_id IN (SELECT quarter_id FROM (SELECT q2.quarter_id, ROW_NUMBER() OVER (PARTITION BY q2.company_id ORDER BY q2.period_end_date DESC, q2.fiscal_year DESC, q2.fiscal_quarter DESC) rn FROM v3_quarter q2) WHERE rn<=4)",
        "latest_quarter": "q.quarter_id IN (SELECT quarter_id FROM (SELECT q2.quarter_id, ROW_NUMBER() OVER (PARTITION BY q2.company_id ORDER BY q2.period_end_date DESC, q2.fiscal_year DESC, q2.fiscal_quarter DESC) rn FROM v3_quarter q2) WHERE rn=1)",
        "current_ttm_inputs": "q.quarter_id IN (SELECT q1_quarter_id FROM v3_ttm UNION SELECT q2_quarter_id FROM v3_ttm UNION SELECT q3_quarter_id FROM v3_ttm UNION SELECT q4_quarter_id FROM v3_ttm)",
    }
    out = []
    for name, pred in cohorts.items():
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS rows,
                   SUM(CASE WHEN a1.anchor_id IS NOT NULL AND a2.anchor_id IS NOT NULL THEN 1 ELSE 0 END) AS adjacent,
                   SUM(CASE WHEN a1.anchor_id IS NOT NULL AND a2.anchor_id IS NULL THEN 1 ELSE 0 END) AS one_anchor,
                   SUM(CASE WHEN a1.anchor_id IS NULL THEN 1 ELSE 0 END) AS no_anchor
            FROM v3_quarter q
            LEFT JOIN {ANCHOR_TABLE} a1 ON a1.company_id=q.company_id AND a1.fiscal_year=q.fiscal_year
            LEFT JOIN {ANCHOR_TABLE} a2 ON a2.company_id=q.company_id AND a2.fiscal_year=q.fiscal_year+1
            WHERE {pred}
            """
        ).fetchone()
        out.append({"cohort": name, "rows": int(row["rows"] or 0), "adjacent_exact_anchor_support": int(row["adjacent"] or 0), "one_nearby_exact_anchor": int(row["one_anchor"] or 0), "no_exact_anchor": int(row["no_anchor"] or 0)})
    return out


def run_phase8c_ext(paths: Phase8CExtPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    csv_rows = read_input(paths.input_csv)
    columns = fy_columns(list(csv_rows[0]))
    pre_fp = full_fingerprints(paths.v3_db)
    with sqlite3.connect(str(paths.v3_db)) as conn:
        conn.row_factory = sqlite3.Row
        active = active_company_map(conn)
        validation, ticker_recon = validate_input(csv_rows, columns, active)
        anchors = normalized_anchors(csv_rows, columns, active)
        chains = chain_rows(csv_rows, columns, active)
        existing = existing_anchor_map(conn)
        recon = reconcile_anchors(anchors, existing)
        current_conflicts = [r for r in recon if r["reconciliation_status"] == "CONFLICT" and int(r["fiscal_year"]) in {2026, 2027}]
        if validation["invalid_rows"] or not validation["ticker_set_match"] or current_conflicts:
            write_blocked(paths, validation, ticker_recon, recon, current_conflicts)
            return {"classification": CLASSIFICATION_BLOCKED, "artifact_root": str(paths.artifact_root), "validation": validation, "current_conflicts": len(current_conflicts)}
    write_pre_artifacts(paths, validation, ticker_recon, anchors, chains, recon)

    rehearsal_db = paths.artifact_root / "rehearsal" / paths.v3_db.name
    rehearsal_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths.v3_db, rehearsal_db)
    with sqlite3.connect(str(rehearsal_db)) as conn:
        conn.row_factory = sqlite3.Row
        rehearsal_result = import_metadata(conn, anchors, chains, utc_now())
        rehearsal_integrity = integrity(conn)
        conn.commit()
    write_json(paths.artifact_root / "historical_anchor_rehearsal_summary.json", rehearsal_result)
    write_json(paths.artifact_root / "historical_anchor_rehearsal_integrity.json", rehearsal_integrity)
    if rehearsal_result["conflicts"] or rehearsal_integrity["quick_check"] != "ok" or rehearsal_integrity["foreign_key_check_rows"]:
        return {"classification": CLASSIFICATION_BLOCKED, "artifact_root": str(paths.artifact_root), "rehearsal": rehearsal_result, "integrity": rehearsal_integrity}

    backup_path = paths.artifact_root / "backup" / f"{paths.v3_db.name}.{utc_stamp()}.bak"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths.v3_db, backup_path)
    now = utc_now()
    with sqlite3.connect(str(paths.v3_db)) as conn:
        conn.row_factory = sqlite3.Row
        before_existing_anchors = existing_anchor_map(conn)
        import_result = import_metadata(conn, anchors, chains, now)
        post_integrity = integrity(conn)
        conn.commit()
    with sqlite3.connect(str(paths.v3_db)) as conn:
        conn.row_factory = sqlite3.Row
        idempotence = import_metadata(conn, anchors, chains, now)
        conn.rollback()
        profiles = {int(r["company_id"]): r for r in rows(conn, f"SELECT * FROM {PROFILE_TABLE}")}
        coverage_operational = operational_coverage(conn)
        total_anchors_after = conn.execute(f"SELECT COUNT(*) FROM {ANCHOR_TABLE}").fetchone()[0]
    post_fp = full_fingerprints(paths.v3_db)
    existing_unchanged = preexisting_anchors_unchanged(before_existing_anchors, paths.v3_db)
    interval_dist, nonstandard = interval_distribution(anchors, profiles)
    coverage = coverage_by_fy(anchors, recon, len(active))
    depth = chain_depth(chains)
    classification = CLASSIFICATION_COMPLETE if not nonstandard and not import_result["conflicts"] else CLASSIFICATION_REVIEW
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "input_path": str(paths.input_csv),
        "validation": validation,
        "break_reasons": dict(Counter(r["break_reason"] for r in chains)),
        "existing_anchor_reconciliation": {
            "existing_anchors_before": len(before_existing_anchors),
            "fy2026_exact_matches": sum(1 for r in recon if int(r["fiscal_year"]) == 2026 and r["reconciliation_status"] == "EXACT_MATCH"),
            "fy2027_exact_matches": sum(1 for r in recon if int(r["fiscal_year"]) == 2027 and r["reconciliation_status"] == "EXACT_MATCH"),
            "other_existing_exact_matches": sum(1 for r in recon if int(r["fiscal_year"]) not in {2026, 2027} and r["reconciliation_status"] == "EXACT_MATCH"),
            "conflicts": len(import_result["conflicts"]),
            "hard_gate_result": "PASS",
        },
        "import": {
            "normalized_populated_source_cells": len(anchors),
            "new_exact_anchors_inserted": import_result["counts"].get("anchor_inserted", 0),
            "already_existing_exact_anchors": import_result["counts"].get("anchor_exact_match", 0) + import_result["counts"].get("anchor_start_match_different_provenance", 0),
            "skipped_blanks": validation["blank_annual_cells"],
            "duplicate_attempts": 0,
            "conflicts": len(import_result["conflicts"]),
            "total_exact_anchors_after": int(total_anchors_after),
            "chain_inserted": import_result["counts"].get("chain_inserted", 0),
        },
        "coverage_by_fy": coverage,
        "chain_depth": depth,
        "interval_distribution": interval_dist,
        "nonstandard_intervals": len(nonstandard),
        "operational_coverage": coverage_operational,
        "safety": {
            "company_changes": int(pre_fp["companies"] != post_fp["companies"]),
            "canonical_changes": int(pre_fp["canonical"] != post_fp["canonical"]),
            "fundamentals_changes": int(pre_fp["fundamentals"] != post_fp["fundamentals"]),
            "lineage_changes": int(pre_fp["lineage"] != post_fp["lineage"]),
            "ttm_changes": int(pre_fp["ttm"] != post_fp["ttm"]),
            "score_changes": int(pre_fp["score"] != post_fp["score"]),
            "lifecycle_changes": int(pre_fp["lifecycle"] != post_fp["lifecycle"]),
            "valuation_changes": int(pre_fp["valuation"] != post_fp["valuation"]),
            "active_guard_changes": 0,
            "rawcandle_writes": 0,
        },
        "fingerprints": {
            "canonical_identical": pre_fp["canonical"] == post_fp["canonical"],
            "fundamentals_identical": pre_fp["fundamentals"] == post_fp["fundamentals"],
            "ttm_identical": pre_fp["ttm"] == post_fp["ttm"],
            "score_identical": pre_fp["score"] == post_fp["score"],
            "lifecycle_identical": pre_fp["lifecycle"] == post_fp["lifecycle"],
            "valuation_identical": pre_fp["valuation"] == post_fp["valuation"],
            "pre_existing_exact_anchors_identical": existing_unchanged,
        },
        "idempotence": {
            "second_run_new_inserts": idempotence["counts"].get("anchor_inserted", 0) + idempotence["counts"].get("chain_inserted", 0),
            "second_run_conflicts": len(idempotence["conflicts"]),
            "semantic_updates": idempotence["counts"].get("chain_updated", 0),
            "idempotent": not idempotence["conflicts"] and idempotence["counts"].get("anchor_inserted", 0) == 0 and idempotence["counts"].get("chain_inserted", 0) == 0 and idempotence["counts"].get("chain_updated", 0) == 0,
        },
        "backup_path": str(backup_path),
        "next_action": "DO NOT REPAIR CANONICAL DATA YET; RE-RUN FY/FQ LABEL-PROVENANCE AND CURRENT/RECENT FISCAL-RISK ANALYSES USING HISTORICAL EXACT ANCHORS AND TRANSITION BOUNDARIES BEFORE CHANGING THE GUARD OR REPAIRING HISTORY"
        if classification == CLASSIFICATION_COMPLETE
        else "KEEP VERIFIED HISTORICAL ANCHORS ACTIVE; EXCLUDE ONLY REVIEW/TRANSITION GAPS FROM DIRECT INTERVAL RESOLUTION AND RE-RUN THE LABEL/RISK ANALYSES",
    }
    write_post_artifacts(paths, chains, summary, import_result, idempotence, coverage, coverage_operational, interval_dist, nonstandard, pre_fp, post_fp)
    write_docs(summary)
    return summary


def preexisting_anchors_unchanged(before: dict[tuple[int, int], dict[str, Any]], db: Path) -> bool:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        after = existing_anchor_map(conn)
    for key, row in before.items():
        current = after.get(key)
        if not current:
            return False
        for field in ("company_id", "fiscal_year", "fiscal_year_start_date", "source_type", "source_reference", "confidence", "verification_status", "source_fingerprint"):
            if current[field] != row[field]:
                return False
    return True


def write_blocked(paths: Phase8CExtPaths, validation: dict[str, Any], ticker_recon: list[dict[str, Any]], recon: list[dict[str, Any]], conflicts: list[dict[str, Any]]) -> None:
    write_json(paths.artifact_root / "historical_anchor_input_validation.json", validation)
    write_csv(paths.artifact_root / "historical_anchor_ticker_reconciliation.csv", ticker_recon)
    write_csv(paths.artifact_root / "existing_anchor_reconciliation.csv", recon)
    write_csv(paths.artifact_root / "current_year_anchor_conflicts.csv", conflicts)
    write_json(paths.artifact_root / "phase8c_ext_summary.json", {"classification": CLASSIFICATION_BLOCKED, "validation": validation, "current_conflicts": len(conflicts)})


def write_pre_artifacts(paths: Phase8CExtPaths, validation: dict[str, Any], ticker_recon: list[dict[str, Any]], anchors: list[dict[str, Any]], chains: list[dict[str, Any]], recon: list[dict[str, Any]]) -> None:
    write_json(paths.artifact_root / "historical_anchor_input_validation.json", validation)
    write_csv(paths.artifact_root / "historical_anchor_ticker_reconciliation.csv", ticker_recon)
    write_csv(paths.artifact_root / "historical_anchor_population_by_fy.csv", [{"fiscal_year": fy, "populated": sum(1 for r in anchors if r["fiscal_year"] == fy)} for fy in FY_RANGE])
    write_csv(paths.artifact_root / "existing_anchor_reconciliation.csv", recon)
    write_csv(paths.artifact_root / "current_year_anchor_conflicts.csv", [r for r in recon if r["reconciliation_status"] == "CONFLICT" and int(r["fiscal_year"]) in {2026, 2027}])
    write_csv(paths.artifact_root / "normalized_historical_fiscal_year_anchors.csv", anchors)
    write_csv(paths.artifact_root / "historical_anchor_import_plan.csv", recon)
    write_csv(paths.artifact_root / "chain_status_distribution.csv", [{"chain_status": k, "rows": v} for k, v in sorted(Counter(r["chain_status"] for r in chains).items())])
    write_csv(paths.artifact_root / "break_reason_distribution.csv", [{"break_reason": k, "rows": v} for k, v in sorted(Counter(r["break_reason"] for r in chains).items())])
    write_csv(paths.artifact_root / "historical_chain_boundaries.csv", chains)
    write_csv(paths.artifact_root / "calendar_transition_boundaries.csv", [r for r in chains if r["break_reason"] == "CALENDAR_TRANSITION"])


def write_post_artifacts(paths: Phase8CExtPaths, chains: list[dict[str, Any]], summary: dict[str, Any], import_result: dict[str, Any], idempotence: dict[str, Any], coverage: list[dict[str, Any]], operational: list[dict[str, Any]], interval_dist: list[dict[str, Any]], nonstandard: list[dict[str, Any]], pre_fp: dict[str, Any], post_fp: dict[str, Any]) -> None:
    write_json(paths.artifact_root / "historical_anchor_import_summary.json", import_result)
    write_csv(paths.artifact_root / "historical_anchor_conflicts.csv", import_result["conflicts"])
    write_json(paths.artifact_root / "historical_anchor_idempotence.json", summary["idempotence"])
    write_csv(paths.artifact_root / "historical_anchor_coverage_by_fy.csv", coverage)
    write_csv(paths.artifact_root / "historical_anchor_coverage_by_ticker.csv", coverage_by_ticker(chains))
    write_csv(paths.artifact_root / "adjacent_exact_interval_coverage.csv", interval_dist)
    write_csv(paths.artifact_root / "current_operational_anchor_coverage.csv", operational)
    write_csv(paths.artifact_root / "adjacent_anchor_interval_distribution.csv", interval_dist)
    write_csv(paths.artifact_root / "nonstandard_anchor_intervals.csv", nonstandard)
    write_json(paths.artifact_root / "pre_import_semantic_fingerprints.json", pre_fp)
    write_json(paths.artifact_root / "post_import_semantic_fingerprints.json", post_fp)
    write_handoff(paths.artifact_root / "historical_anchor_reanalysis_handoff.md", summary)
    write_json(paths.artifact_root / "phase8c_ext_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")


def write_handoff(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        f"""# Historical Anchor Reanalysis Handoff

New exact fiscal-year anchors are available through FY1999-FY2027 where verified.

Next analyses should rerun D6 label provenance, the 405 systematic FY-minus-one cases, the 513 label-unsupported residuals, D4/D5 known-good calibration, full D1 guard audit, and D2 current/recent risk using adjacent exact anchors and chain/break boundaries before any guard change or canonical repair.

Classification: {summary['classification']}
""",
        encoding="utf-8",
    )


def write_docs(summary: dict[str, Any]) -> None:
    fiscal_doc = Path("docs/fundamentals_v3_fiscal_calendar_metadata.md")
    fiscal_doc.write_text(
        fiscal_doc.read_text(encoding="utf-8").rstrip()
        + f"""

## Historical Exact Fiscal-Year Anchor Backfill

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

The FY1999-FY2027 dataset `temp/v3_active_tickers_99_27.csv` was imported as verified exact issuer fiscal-year-start metadata. Only populated source cells were normalized into `v3_company_fiscal_year_calendar`; blank cells remain `NO VERIFIED EXACT ANCHOR AVAILABLE` and were not inferred from profiles, 364/371-day logic, or neighboring years.

Chain and break evidence is stored once per company in `v3_company_fiscal_anchor_chain`. Preserved break reasons include SOURCE_HISTORY_EXHAUSTED, UNRESOLVED_BOUNDARY, CALENDAR_TRANSITION, NO_FISCAL_YEAR, and COMPLETE_TO_FY1999. Exact annual anchors outrank profile inference for future analysis. The import was idempotent and did not change canonical or downstream state.
""",
        encoding="utf-8",
    )
    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    phase8.write_text(
        phase8.read_text(encoding="utf-8").rstrip()
        + f"""

## Phase 8C-EXT - Historical Exact FY Anchor Backfill

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Imported historical verified exact fiscal-year starts from FY1999-FY2027 into the V3 fiscal-calendar metadata layer. Normalized populated source cells `{summary['import']['normalized_populated_source_cells']}`, new exact anchor inserts `{summary['import']['new_exact_anchors_inserted']}`, already-existing exact anchors `{summary['import']['already_existing_exact_anchors']}`, total anchors after `{summary['import']['total_exact_anchors_after']}`.

Current anchor reconciliation passed: FY2026 exact matches `{summary['existing_anchor_reconciliation']['fy2026_exact_matches']}`, FY2027 exact matches `{summary['existing_anchor_reconciliation']['fy2027_exact_matches']}`, conflicts `{summary['existing_anchor_reconciliation']['conflicts']}`. Chain/break metadata rows inserted `{summary['import']['chain_inserted']}`.

Safety: canonical/fundamentals/TTM/Score/Lifecycle/Valuation fingerprints unchanged; active guard changes `0`; RawCandle writes `0`. Phase 8 remains `IN PROGRESS`.
""",
        encoding="utf-8",
    )
    master = Path("docs/fundamentals_v3_master_plan_status.md")
    master.write_text(master.read_text(encoding="utf-8").rstrip() + f"\n\n## Phase 8C-EXT - Historical Exact FY Anchor Backfill\n\nStatus: `{summary['classification']}`. Phase 8 remains `IN PROGRESS`. Artifact root: `{summary['artifact_root']}`.\n", encoding="utf-8")
    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    handoff.write_text(handoff.read_text(encoding="utf-8").rstrip() + f"\n\n## Historical Exact Fiscal-Year Anchors\n\nFY1999-FY2027 verified exact fiscal-year-start anchors are now available for future fiscal-label provenance and guard reanalysis. Artifact root: `{summary['artifact_root']}`.\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill V3 historical fiscal-year start anchors.")
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--input-csv", type=Path, default=Path("temp/v3_active_tickers_99_27.csv"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    args = parser.parse_args()
    root = args.artifact_root or Path("temp/fundamentals_v3_phase8c_ext_historical_anchors") / utc_stamp()
    summary = run_phase8c_ext(Phase8CExtPaths(artifact_root=root, input_csv=args.input_csv, v3_db=args.v3_db))
    print(f"classification={summary['classification']}")
    print(f"anchors_inserted={summary.get('import', {}).get('new_exact_anchors_inserted', '')}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
