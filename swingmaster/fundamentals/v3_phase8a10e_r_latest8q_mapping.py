from __future__ import annotations

import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10b_full_sequence_audit import (
    Phase8A10BPaths,
    run_phase8a10b_full_sequence_audit,
)
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro, connect_rw, file_state, integrity
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv, sha_file


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A10E_R_NINE_TICKER_APPLY_SET_READY"
CLASSIFICATION_PARTIAL = "FUNDAMENTALS_V3_PHASE8A10E_R_PARTIAL_APPLY_SET_READY_BLOCKERS_REMAIN"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8A10E_R_MAPPING_BLOCKED"
NINE_TICKERS = ("BBY", "DELL", "GCO", "HAE", "MRVL", "RL", "SAIC", "TJX", "TRNS")
OUT_OF_SCOPE_P1 = {"FNGR", "POWW", "RH", "VTGN"}
CANONICAL_FIELDS = (
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
FINGERPRINT_FIELDS = (("revenue", "Revenue"), ("operating_income", "Operating Income"), ("net_income", "Net Income"))


@dataclass(frozen=True)
class Phase8A10ERPaths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")
    official_latest8q_csv: Path = Path("temp/swingmaster_v3_official_fiscal_quarter_timeline_2026-08-26.csv")
    supplemental_fingerprint_csv: Path = Path("temp/phase8_global_P1_official_fiscal_timelines.csv")
    a10b_root: Path = Path("temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    publish_apply_root: Path = Path("temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def qnum(fq: str) -> int:
    return {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(str(fq), 0)


def fiscal_ordinal(fy: int, fq: str) -> int:
    return fy * 4 + qnum(fq)


def latest8_key(row: dict[str, str]) -> tuple[str, int, str]:
    return row["Ticker"], int(row["Fiscal Year"]), row["Fiscal Quarter"]


def current_key(row: dict[str, Any]) -> tuple[str, int, str]:
    return row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]


def supplemental_key(row: dict[str, str]) -> tuple[str, int, str]:
    return row["Ticker"], int(row["Fiscal Year"]), row["Fiscal Q"]


def value_equal(left: Any, right: str | None) -> bool:
    if left is None or right in (None, ""):
        return False
    try:
        lval = float(left)
        rval = float(right)
    except (TypeError, ValueError):
        return False
    return abs(lval - rval) <= max(1.0, abs(rval) * 0.000001)


def fingerprint_match_count(current: dict[str, Any], official: dict[str, str] | None) -> int:
    if not official:
        return 0
    return sum(1 for left, right in FINGERPRINT_FIELDS if value_equal(current.get(left), official.get(right)))


def validate_official_latest8q(path: Path) -> tuple[list[dict[str, str]], dict[str, Any]]:
    official = read_csv(path)
    expected = set(NINE_TICKERS)
    tickers = {row.get("Ticker", "") for row in official}
    counts = Counter(row.get("Ticker", "") for row in official)
    ranks = defaultdict(list)
    for row in official:
        ranks[row["Ticker"]].append(int(row["Canonical Sequence"]))
    problems = []
    if len(official) != 72:
        problems.append(f"rows={len(official)}")
    if tickers != expected:
        problems.append(f"tickers={sorted(tickers)}")
    for ticker in expected:
        if counts[ticker] != 8:
            problems.append(f"{ticker}_rows={counts[ticker]}")
        if sorted(ranks[ticker]) != list(range(1, 9)):
            problems.append(f"{ticker}_ranks={sorted(ranks[ticker])}")
    latest = {}
    for ticker in expected:
        ticker_rows = [row for row in official if row.get("Ticker") == ticker]
        if ticker_rows:
            latest[ticker] = max(ticker_rows, key=lambda r: int(r["Canonical Sequence"]))
    if "TJX" in latest and (latest["TJX"]["Fiscal Year"] != "2027" or latest["TJX"]["Fiscal Quarter"] != "Q2"):
        problems.append("TJX_latest_not_FY2027_Q2")
    for ticker in expected - {"TJX"}:
        if ticker in latest and (latest[ticker]["Fiscal Year"] != "2027" or latest[ticker]["Fiscal Quarter"] != "Q1"):
            problems.append(f"{ticker}_latest_not_FY2027_Q1")
    required = ("Fiscal Year", "Fiscal Quarter", "Official Period End", "Confidence")
    for idx, row in enumerate(official, 2):
        for col in required:
            if not row.get(col):
                problems.append(f"line_{idx}_missing_{col}")
        if not row.get("Publish Date"):
            problems.append(f"line_{idx}_missing_Publish Date")
    validation = {
        "path": str(path),
        "sha256": sha_file(path),
        "rows": len(official),
        "tickers": sorted(tickers),
        "ticker_count": len(tickers),
        "rows_per_ticker": dict(sorted(counts.items())),
        "confidence_distribution": dict(Counter(row.get("Confidence", "") for row in official)),
        "latest_by_ticker": {ticker: f"FY{latest[ticker]['Fiscal Year']} {latest[ticker]['Fiscal Quarter']}" for ticker in sorted(latest)},
        "has_revenue_column": int("Revenue" in official[0] if official else 0),
        "problems": problems,
    }
    if problems:
        raise RuntimeError("PHASE8A10E_R_OFFICIAL_TIMELINE_INVALID:" + "|".join(problems))
    return official, validation


def load_supplemental_fingerprints(path: Path) -> dict[tuple[str, int, str], dict[str, str]]:
    if not path.exists():
        return {}
    return {supplemental_key(row): row for row in read_csv(path) if row.get("Ticker") in NINE_TICKERS}


def best_fingerprint_key(
    current: dict[str, Any],
    official_keys: set[tuple[str, int, str]],
    supplemental: dict[tuple[str, int, str], dict[str, str]],
) -> tuple[tuple[str, int, str] | None, int]:
    candidates = []
    for key, row in supplemental.items():
        if key[0] != current["ticker"] or key not in official_keys:
            continue
        count = fingerprint_match_count(current, row)
        if count:
            candidates.append((count, key))
    if not candidates:
        return None, 0
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1], candidates[0][0]


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
               COALESCE((
                 SELECT group_concat(a.source || ':' || a.source_key, ' | ')
                 FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id
               ), '') AS lineage_provenance,
               COALESCE((
                 SELECT group_concat(a.audit_type || ':' || a.decision, ' | ')
                 FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id
               ), '') AS lineage_decisions,
               COALESCE((
                 SELECT group_concat(p.provider || ':' || p.acquisition_result || ':' || COALESCE(p.provider_cache_ref,''), ' | ')
                 FROM v3_provider_q_acquisition p WHERE p.quarter_id=q.quarter_id
               ), '') AS provider_acquisition
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker IN ({placeholders})
          AND q.fiscal_year BETWEEN 2024 AND 2027
        ORDER BY c.ticker,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
        """,
        NINE_TICKERS,
    )


def match_class(current: dict[str, Any], official: dict[str, str] | None, supplemental: dict[str, str] | None) -> tuple[str, str, str]:
    if not official:
        return "NO_OFFICIAL_MATCH", "LOW", "NO_OFFICIAL_ROW"
    wrong_fyq = current_key(current) != latest8_key(official)
    wrong_period = (current.get("period_end_date") or "") != official.get("Official Period End", "")
    wrong_publish = (current.get("publish_date") or "") != official.get("Publish Date", "")
    fp_matches = fingerprint_match_count(current, supplemental)
    has_fp = supplemental is not None and any(supplemental.get(col) for _field, col in FINGERPRINT_FIELDS)
    if wrong_fyq and fp_matches:
        return "MATCH_BY_FINANCIAL_CONTENT", "HIGH", f"fingerprint_matches={fp_matches}"
    if wrong_fyq and (wrong_period or wrong_publish):
        return "MATCH_WRONG_FYQ_AND_METADATA", "MEDIUM", "matched_by_nearest_fiscal_sequence"
    if wrong_fyq:
        return "MATCH_WRONG_FYQ", "MEDIUM", "matched_by_period_or_sequence"
    if wrong_period and wrong_publish:
        return "MATCH_WRONG_FYQ_AND_METADATA", "HIGH" if has_fp and fp_matches else "MEDIUM", f"same_fyq fingerprint_matches={fp_matches}"
    if wrong_period:
        return "MATCH_WRONG_PERIOD_END", "HIGH" if (not has_fp or fp_matches) else "MEDIUM", f"same_fyq fingerprint_matches={fp_matches}"
    if wrong_publish:
        return "MATCH_WRONG_PUBLISH_DATE", "HIGH" if (not has_fp or fp_matches) else "MEDIUM", f"same_fyq fingerprint_matches={fp_matches}"
    return "EXACT_OFFICIAL_MATCH", "HIGH" if (not has_fp or fp_matches) else "MEDIUM", f"same_fyq fingerprint_matches={fp_matches}"


def map_current_to_official(
    current_rows: list[dict[str, Any]],
    official_rows: list[dict[str, str]],
    supplemental: dict[tuple[str, int, str], dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    official_by_key = {latest8_key(row): row for row in official_rows}
    official_keys = set(official_by_key)
    official_by_ticker = defaultdict(list)
    for row in official_rows:
        official_by_ticker[row["Ticker"]].append(row)
    mapping = []
    fingerprints = []
    used_keys: set[tuple[str, int, str]] = set()
    for row in current_rows:
        official = official_by_key.get(current_key(row))
        best_key, best_count = best_fingerprint_key(row, official_keys, supplemental)
        if best_key and (not official or best_key != current_key(row) or best_count > fingerprint_match_count(row, supplemental.get(current_key(row)))):
            official = official_by_key[best_key]
        if not official:
            official = next((o for o in official_by_ticker[row["ticker"]] if o["Official Period End"] == (row.get("period_end_date") or "")), None)
        supp = supplemental.get(latest8_key(official)) if official else None
        cls, confidence, reason = match_class(row, official, supp)
        if official:
            used_keys.add(latest8_key(official))
        content_status = "CONTENT_UNVERIFIED"
        fp_count = fingerprint_match_count(row, supp)
        if supp and fp_count >= 2:
            content_status = "CONTENT_MATCH_HIGH"
        elif supp and fp_count == 1:
            content_status = "CONTENT_MATCH_PARTIAL"
        elif supp:
            content_status = "CONTENT_MISMATCH"
        mapping.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "current_fiscal_year": row["fiscal_year"],
                "current_fiscal_quarter": row["fiscal_quarter"],
                "current_period_end": row.get("period_end_date") or "",
                "current_publish_date": row.get("publish_date") or "",
                "official_fiscal_year": official.get("Fiscal Year", "") if official else "",
                "official_fiscal_quarter": official.get("Fiscal Quarter", "") if official else "",
                "official_period_end": official.get("Official Period End", "") if official else "",
                "official_publish_date": official.get("Publish Date", "") if official else "",
                "match_class": cls,
                "match_confidence": confidence,
                "match_reason": reason,
                "content_validation": content_status,
                "content_moves_with_row": int(cls != "NO_OFFICIAL_MATCH"),
            }
        )
        fingerprints.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "current_fyq": f"FY{row['fiscal_year']} {row['fiscal_quarter']}",
                "official_fyq": f"FY{official['Fiscal Year']} {official['Fiscal Quarter']}" if official else "",
                "revenue_current": row.get("revenue") or "",
                "revenue_official": supp.get("Revenue", "") if supp else "",
                "revenue_match": int(value_equal(row.get("revenue"), supp.get("Revenue") if supp else "")),
                "operating_income_current": row.get("operating_income") or "",
                "operating_income_official": supp.get("Operating Income", "") if supp else "",
                "operating_income_match": int(value_equal(row.get("operating_income"), supp.get("Operating Income") if supp else "")),
                "net_income_current": row.get("net_income") or "",
                "net_income_official": supp.get("Net Income", "") if supp else "",
                "net_income_match": int(value_equal(row.get("net_income"), supp.get("Net Income") if supp else "")),
                "fingerprint_source": "SUPPLEMENTAL_GLOBAL_P1_TIMELINE" if supp else "NOT_AVAILABLE_IN_72_ROW_TIMELINE",
            }
        )
    missing = [
        {
            "ticker": row["Ticker"],
            "official_fiscal_year": row["Fiscal Year"],
            "official_fiscal_quarter": row["Fiscal Quarter"],
            "official_period_end": row["Official Period End"],
            "official_publish_date": row["Publish Date"],
            "missing_class": "MISSING_CANONICAL_QUARTER",
        }
        for row in official_rows
        if latest8_key(row) not in used_keys
    ]
    return mapping, fingerprints, missing


def target_collisions(conn: sqlite3.Connection, mapping: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in mapping:
        if not row["official_fiscal_year"]:
            continue
        target = rows(
            conn,
            """
            SELECT q.quarter_id,q.period_end_date,q.publish_date
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            """,
            (row["ticker"], row["official_fiscal_year"], row["official_fiscal_quarter"]),
        )
        if not target:
            cls = "TARGET_EMPTY"
            target_qid = ""
        elif int(target[0]["quarter_id"]) == int(row["quarter_id"]):
            cls = "TARGET_SAME_ECONOMIC_IDENTICAL" if row["match_class"] == "EXACT_OFFICIAL_MATCH" else "TARGET_SAME_ECONOMIC_COMPLEMENTARY"
            target_qid = target[0]["quarter_id"]
        else:
            cls = "TARGET_DIFFERENT_ECONOMIC_QUARTER"
            target_qid = target[0]["quarter_id"]
        out.append({**row, "target_collision_class": cls, "target_quarter_id": target_qid})
    return out


def build_transformations(
    collisions: list[dict[str, Any]], official_by_key: dict[tuple[str, int, str], dict[str, str]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    transformations = []
    blockers = []
    order = 0
    for row in collisions:
        if row["match_confidence"] != "HIGH":
            continue
        if row["target_collision_class"] == "TARGET_DIFFERENT_ECONOMIC_QUARTER":
            blockers.append({"ticker": row["ticker"], "quarter_id": row["quarter_id"], "blocking_issue": "TARGET_DIFFERENT_ECONOMIC_QUARTER"})
            continue
        fields = []
        if row["current_period_end"] != row["official_period_end"]:
            fields.append(("period_end", row["current_period_end"], row["official_period_end"], "UPDATE_PERIOD_END"))
        if row["current_publish_date"] != row["official_publish_date"]:
            fields.append(("publish_date", row["current_publish_date"], row["official_publish_date"], "UPDATE_PUBLISH_DATE"))
        for field, old, new, op in fields:
            order += 1
            official = official_by_key[(row["ticker"], int(row["official_fiscal_year"]), row["official_fiscal_quarter"])]
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
                    "content-match confidence": row["match_confidence"],
                    "official source evidence": official.get("Primary Source URL", ""),
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
            changed = cur.rowcount if cur is not None else 0
            if changed != 1:
                raise RuntimeError(f"PHASE8A10E_R_REHEARSAL_GUARD_FAILED:{row['ticker']}:{row['quarter_id']}:{row['field']}")
            log.append({**row, "rows_changed": changed, "status": "APPLIED_REHEARSAL"})
        conn.commit()
    return log


def timeline_parity(conn: sqlite3.Connection, official: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in official:
        found = rows(
            conn,
            """
            SELECT q.quarter_id,q.period_end_date,q.publish_date
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            """,
            (row["Ticker"], row["Fiscal Year"], row["Fiscal Quarter"]),
        )
        current = found[0] if found else {}
        out.append(
            {
                "ticker": row["Ticker"],
                "official_fy": row["Fiscal Year"],
                "official_fq": row["Fiscal Quarter"],
                "current_quarter_id": current.get("quarter_id", ""),
                "current_period_end": current.get("period_end_date", ""),
                "official_period_end": row["Official Period End"],
                "period_end_parity": int(current.get("period_end_date", "") == row["Official Period End"]),
                "current_publish_date": current.get("publish_date", ""),
                "official_publish_date": row["Publish Date"],
                "publish_date_parity": int(current.get("publish_date", "") == row["Publish Date"]),
            }
        )
    return out


def summarize_by_ticker(
    current_rows: list[dict[str, Any]],
    mapping: list[dict[str, Any]],
    missing: list[dict[str, Any]],
    collisions: list[dict[str, Any]],
    transformations: list[dict[str, Any]],
    blockers: list[dict[str, Any]],
    official: list[dict[str, str]],
) -> list[dict[str, Any]]:
    out = []
    for ticker in NINE_TICKERS:
        tmap = [row for row in mapping if row["ticker"] == ticker]
        bad = [row for row in tmap if row["match_class"] != "EXACT_OFFICIAL_MATCH"]
        first = bad[0] if bad else {}
        ttrans = [row for row in transformations if row["ticker"] == ticker]
        tblock = [row for row in blockers if row["ticker"] == ticker]
        root = "PERIOD_END_METADATA_SEGMENT"
        if any(row["match_class"] in {"MATCH_WRONG_FYQ", "MATCH_WRONG_FYQ_AND_METADATA", "MATCH_BY_FINANCIAL_CONTENT"} for row in tmap):
            root = "FYQ_AND_PERIOD_SEGMENT_SHIFT"
        if any(row["ticker"] == ticker for row in missing):
            root = "MISSING_CANONICAL_QUARTER"
        if any(row["ticker"] == ticker and row["target_collision_class"] == "TARGET_DIFFERENT_ECONOMIC_QUARTER" for row in collisions):
            root = "DUPLICATE_ECONOMIC_QUARTER"
        if any(row["content_validation"] == "CONTENT_MISMATCH" for row in tmap):
            root = "CONTENT_MAPPING_ERROR"
        out.append(
            {
                "ticker": ticker,
                "official_latest_8q": ";".join(f"FY{row['Fiscal Year']} {row['Fiscal Quarter']} {row['Official Period End']}" for row in official if row["Ticker"] == ticker),
                "current_v3_candidate_rows": sum(1 for row in current_rows if row["ticker"] == ticker),
                "exact_mapped_rows": sum(1 for row in tmap if row["match_class"] == "EXACT_OFFICIAL_MATCH"),
                "unmatched_current_rows": sum(1 for row in tmap if row["match_class"] == "NO_OFFICIAL_MATCH"),
                "missing_official_quarters": sum(1 for row in missing if row["ticker"] == ticker),
                "first_divergence": f"FY{first.get('current_fiscal_year')} {first.get('current_fiscal_quarter')}" if first else "",
                "root_cause": root,
                "target_collisions": sum(1 for row in collisions if row["ticker"] == ticker and row["target_collision_class"] == "TARGET_DIFFERENT_ECONOMIC_QUARTER"),
                "content_conflicts": sum(1 for row in tmap if row["content_validation"] == "CONTENT_MISMATCH"),
                "proposed_transformation_shape": "|".join(sorted({row["operation"] for row in ttrans})) or "NO_WRITE",
                "affected_canonical_rows": len({row["quarter_id"] for row in ttrans}),
                "production_ready": "YES" if ttrans and not tblock else "NO",
            }
        )
    return out


def write_prevention_handoff(path: Path, classification: str, per_ticker: list[dict[str, Any]]) -> None:
    roots = Counter(row["root_cause"] for row in per_ticker)
    path.write_text(
        "\n".join(
            [
                "# Phase 8A10E-R Prevention Handoff",
                "",
                f"Classification: `{classification}`",
                "",
                "The nine-ticker evidence refines the historical Yahoo seed failure as multiple failure modes.",
                "The dominant current P1 repair shape is official 52/53-week period_end metadata replacing Yahoo month-end or +1-year normalized provider dates.",
                "The surrounding latest-8Q mapping also shows missing official quarters, duplicate economic-quarter risk, and content verification gaps, so prevention cannot be period_end-only.",
                "",
                f"Root-cause counts: `{dict(roots)}`",
                "",
                "Implementation implication: canonical write paths must prefer official_period_end_date over normalized provider period_end, preserve weekend fiscal closes, require target collision checks, and run exact post-write A10B-style P1 audits before accepting future bootstrap/backfill/update output.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def run_phase8a10e_r(paths: Phase8A10ERPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    production_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    official, validation = validate_official_latest8q(paths.official_latest8q_csv)
    supplemental = load_supplemental_fingerprints(paths.supplemental_fingerprint_csv)
    manifest = {
        "csv_path": str(paths.official_latest8q_csv),
        "csv_sha256": sha_file(paths.official_latest8q_csv),
        "supplemental_fingerprint_csv": str(paths.supplemental_fingerprint_csv),
        "supplemental_fingerprint_sha256": sha_file(paths.supplemental_fingerprint_csv) if paths.supplemental_fingerprint_csv.exists() else "",
        "external_research_current_as_of": "2026-08-26",
        "primary_data_truth": "72_ROW_OFFICIAL_LATEST8Q_TIMELINE",
        "fingerprint_note": "Primary 72-row CSV has no Revenue/OI/NI columns; supplemental local P1 official timeline is used only where available.",
    }
    write_json(paths.artifact_root / "official_latest8q_input_manifest.json", manifest)
    write_json(paths.artifact_root / "official_latest8q_validation.json", validation)
    with connect_ro(paths.v3_db) as conn:
        current = current_candidate_rows(conn)
        mapping, fingerprints, missing = map_current_to_official(current, official, supplemental)
        collisions = target_collisions(conn, mapping)
        transformations, blockers = build_transformations(collisions, {latest8_key(row): row for row in official})
    per_ticker = summarize_by_ticker(current, mapping, missing, collisions, transformations, blockers, official)
    write_csv(paths.artifact_root / "nine_ticker_current_v3_candidate_rows.csv", current)
    write_csv(paths.artifact_root / "nine_ticker_current_to_official_mapping.csv", mapping)
    write_csv(paths.artifact_root / "nine_ticker_mapping_summary.csv", per_ticker)
    write_csv(paths.artifact_root / "nine_ticker_content_fingerprint_comparison.csv", fingerprints)
    write_csv(paths.artifact_root / "nine_ticker_target_collisions.csv", collisions)
    write_csv(paths.artifact_root / "nine_ticker_missing_official_quarters.csv", missing)
    write_csv(paths.artifact_root / "nine_ticker_structural_root_causes.csv", [{"ticker": row["ticker"], "root_cause": row["root_cause"]} for row in per_ticker])
    write_csv(paths.artifact_root / "nine_ticker_atomic_transformations.csv", transformations)
    group_summary = [
        {
            "ticker": row["ticker"],
            "production_ready": row["production_ready"],
            "operations": sum(1 for op in transformations if op["ticker"] == row["ticker"]),
            "affected_rows": row["affected_canonical_rows"],
            "root_cause": row["root_cause"],
        }
        for row in per_ticker
    ]
    write_csv(paths.artifact_root / "nine_ticker_transformation_group_summary.csv", group_summary)

    rehearsal_db = paths.artifact_root / "rehearsal_rc_fundamentals_v3.db"
    shutil.copy2(paths.v3_db, rehearsal_db)
    apply_log = apply_rehearsal(rehearsal_db, transformations)
    with connect_ro(rehearsal_db) as conn:
        rehearsal_integrity = integrity(conn)
        parity = timeline_parity(conn, official)
    post_root = paths.artifact_root / "rehearsal_post_a10b"
    run_phase8a10b_full_sequence_audit(
        Phase8A10BPaths(artifact_root=post_root, v3_db=rehearsal_db, rawcandle_db=paths.rawcandle_db, publish_apply_root=paths.publish_apply_root)
    )
    post_p1 = read_csv(post_root / "global_P1.csv")
    before_p1 = read_csv(paths.a10b_root / "global_P1.csv")
    nine_after = [row for row in post_p1 if row.get("ticker") in NINE_TICKERS]
    before_tickers = {row["ticker"] for row in before_p1}
    post_summary = {
        "global_P1_before": len(before_p1),
        "nine_ticker_P1_before": sum(1 for row in before_p1 if row["ticker"] in NINE_TICKERS),
        "nine_ticker_P1_after": len(nine_after),
        "global_P1_after": len(post_p1),
        "remaining_P1_tickers": sorted({row["ticker"] for row in post_p1}),
        "new_P1_introduced": sorted({row["ticker"] for row in post_p1} - before_tickers),
    }
    write_csv(paths.artifact_root / "nine_ticker_rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "nine_ticker_rehearsal_integrity.json", rehearsal_integrity)
    write_csv(paths.artifact_root / "nine_ticker_rehearsal_official_timeline_parity.csv", parity)
    write_csv(paths.artifact_root / "nine_ticker_rehearsal_post_a10b_P1.csv", post_p1)
    write_json(paths.artifact_root / "nine_ticker_rehearsal_post_a10b_P1_summary.json", post_summary)
    parity_ok = all(int(row["period_end_parity"]) == 1 and int(row["publish_date_parity"]) == 1 for row in parity)
    ready_gate = post_summary["nine_ticker_P1_after"] == 0 and not post_summary["new_P1_introduced"] and parity_ok
    frozen = transformations if ready_gate else []
    if not ready_gate:
        blockers.extend(
            {"ticker": ticker, "quarter_id": "", "blocking_issue": "POST_REHEARSAL_GATE_NOT_CLOSED"}
            for ticker in sorted({row["ticker"] for row in nine_after})
        )
    write_csv(paths.artifact_root / "phase8a10e_r_frozen_nine_ticker_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "phase8a10e_r_nine_ticker_blockers.csv", blockers)
    classification = CLASSIFICATION_READY if ready_gate else CLASSIFICATION_BLOCKED
    write_prevention_handoff(paths.artifact_root / "nine_ticker_failure_mode_prevention_handoff.md", classification, per_ticker)
    production_after = file_state(paths.v3_db)
    raw_after = file_state(paths.rawcandle_db)
    safety = {
        "production_writes": int(production_before != production_after),
        "ttm_writes": 0,
        "score_writes": 0,
        "lifecycle_writes": 0,
        "valuation_writes": 0,
        "rawcandle_writes": int(raw_before != raw_after),
    }
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "external_input": validation,
        "per_ticker": per_ticker,
        "structural_summary": {
            "metadata_only_tickers": [r["ticker"] for r in per_ticker if r["root_cause"] == "PERIOD_END_METADATA_SEGMENT"],
            "fyq_shift_tickers": [r["ticker"] for r in per_ticker if r["root_cause"] == "FYQ_ONE_YEAR_SHIFT"],
            "fyq_metadata_shift_tickers": [r["ticker"] for r in per_ticker if r["root_cause"] == "FYQ_AND_PERIOD_SEGMENT_SHIFT"],
            "missing_quarter_tickers": [r["ticker"] for r in per_ticker if r["missing_official_quarters"]],
            "duplicate_quarter_tickers": [r["ticker"] for r in per_ticker if r["target_collisions"]],
            "content_mismatch_tickers": [r["ticker"] for r in per_ticker if r["content_conflicts"]],
        },
        "frozen_repair": {
            "production_ready_ticker_groups": len({row["ticker"] for row in frozen}),
            "blocked_ticker_groups": len({row["ticker"] for row in blockers}),
            "canonical_rows_affected": len({row["quarter_id"] for row in frozen}),
            "atomic_operations": len(frozen),
            "period_end_writes": sum(1 for row in frozen if row["operation"] == "UPDATE_PERIOD_END"),
            "publish_writes": sum(1 for row in frozen if row["operation"] == "UPDATE_PUBLISH_DATE"),
            "identity_writes": 0,
            "value_writes": 0,
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
            "timeline_parity": int(parity_ok),
        },
        "a10b_reaudit": post_summary,
        "prevention": {
            "historical_yahoo_seed_failure_mode_refined": "multiple failure modes: provider period_end normalization plus missing/latest8 gaps and content-verification limits",
            "active_bug_evidence": "NO",
            "implementation_implication": "official_period_end must outrank provider period_end and exact post-write A10B audits must gate future writes",
        },
        "safety": safety,
        "next_action": "PHASE 8A10E-R-APPLY - APPLY REHEARSED NINE-TICKER STRUCTURAL REPAIRS"
        if ready_gate
        else "DO NOT WRITE PRODUCTION - RESOLVE ONLY BLOCKED TICKERS",
    }
    write_json(paths.artifact_root / "phase8a10e_r_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if safety["production_writes"] or safety["rawcandle_writes"]:
        raise RuntimeError("PHASE8A10E_R_READ_ONLY_GUARD_FAILED")
    return summary
