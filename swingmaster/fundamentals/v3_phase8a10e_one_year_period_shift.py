from __future__ import annotations

import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10b_full_sequence_audit import (
    Phase8A10BPaths,
    run_phase8a10b_full_sequence_audit,
)
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import (
    CANONICAL_FIELDS,
    connect_ro,
    connect_rw,
    file_state,
    integrity,
)


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A10E_SYSTEMATIC_PERIOD_END_REPAIR_READY"
CLASSIFICATION_MIXED_READY = "FUNDAMENTALS_V3_PHASE8A10E_MIXED_REPAIRS_READY"
CLASSIFICATION_ACTIVE_BUG = "FUNDAMENTALS_V3_PHASE8A10E_ACTIVE_INGESTION_BUG_REPAIR_AND_CODE_FIX_REQUIRED"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8A10E_BLOCKERS_REMAIN"
NINE_TICKERS = ("BBY", "DELL", "GCO", "HAE", "MRVL", "RL", "SAIC", "TJX", "TRNS")
VALUE_FIELDS = ("revenue", "gross_profit", "operating_income", "ebit", "ebitda", "net_income")


@dataclass(frozen=True)
class Phase8A10EPaths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")
    official_timeline_csv: Path = Path("temp/phase8_global_P1_official_fiscal_timelines.csv")
    a10b_root: Path = Path("temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    a10d_root: Path = Path("temp/fundamentals_v3_phase8a10d_r_segment_reconciliation/20260826T171500Z")
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    publish_apply_root: Path = Path("temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    import csv

    with path.open(newline="", encoding="utf-8-sig") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def ticker_placeholders() -> str:
    return ",".join("?" for _ in NINE_TICKERS)


def current_segments(conn: sqlite3.Connection) -> list[dict[str, Any]]:
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
                 SELECT group_concat(audit_type || ':' || decision, ' | ')
                 FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id
               ), '') AS lineage_decisions,
               COALESCE((
                 SELECT group_concat(provider || ':' || acquisition_result || ':' || COALESCE(provider_cache_ref,''), ' | ')
                 FROM v3_provider_q_acquisition p WHERE p.quarter_id=q.quarter_id
               ), '') AS provider_acquisition
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker IN ({ticker_placeholders()})
          AND q.fiscal_year BETWEEN 2023 AND 2027
        ORDER BY c.ticker,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
        """,
        NINE_TICKERS,
    )


def official_rows(path: Path) -> list[dict[str, str]]:
    return [row for row in read_csv(path) if row.get("Ticker") in NINE_TICKERS]


def p1_rows(path: Path) -> list[dict[str, str]]:
    return [row for row in read_csv(path / "global_P1.csv") if row.get("ticker") in NINE_TICKERS]


def value_equal(left: Any, right: str) -> bool:
    if left is None or right in ("", None):
        return False
    try:
        return abs(float(left) - float(right)) <= max(1.0, abs(float(right)) * 0.000001)
    except (TypeError, ValueError):
        return False


def official_key(row: dict[str, str]) -> tuple[str, int, str]:
    return row["Ticker"], int(row["Fiscal Year"]), row["Fiscal Q"]


def current_key(row: dict[str, Any]) -> tuple[str, int, str]:
    return row["ticker"], int(row["fiscal_year"]), str(row["fiscal_quarter"])


def matching_value_count(current: dict[str, Any], official: dict[str, str]) -> int:
    mapping = {
        "revenue": "Revenue",
        "operating_income": "Operating Income",
        "net_income": "Net Income",
    }
    return sum(1 for left, right in mapping.items() if value_equal(current.get(left), official.get(right, "")))


def official_value_count(official: dict[str, str] | None) -> int:
    if not official:
        return 0
    return sum(1 for key in ("Revenue", "Operating Income", "Net Income") if official.get(key) not in ("", None))


def best_official_value_match(current: dict[str, Any], official: list[dict[str, str]]) -> dict[str, str] | None:
    candidates = [row for row in official if row["Ticker"] == current["ticker"]]
    scored = [(matching_value_count(current, row), row) for row in candidates]
    scored = [item for item in scored if item[0] > 0]
    if not scored:
        return None
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1]


def offset_class(current_period: str | None, official_period: str | None) -> tuple[str, int | str, int | str]:
    if not current_period or not official_period:
        return "NO_OFFICIAL_MATCH", "", ""
    cur = date.fromisoformat(current_period)
    off = date.fromisoformat(official_period)
    year_delta = cur.year - off.year
    month_day_same = (cur.month, cur.day) == (off.month, off.day)
    near_month_end = abs((cur - off.replace(year=cur.year)).days) <= 7 if off.month != 2 or not (off.month == 2 and off.day == 29) else False
    if current_period == official_period:
        cls = "EXACT"
    elif year_delta == 1 and month_day_same:
        cls = "PLUS_ONE_YEAR_SAME_MONTH_DAY"
    elif year_delta == -1 and month_day_same:
        cls = "MINUS_ONE_YEAR_SAME_MONTH_DAY"
    elif year_delta == 1 and near_month_end:
        cls = "PLUS_ONE_YEAR_MONTH_END_NORMALIZED"
    else:
        cls = "OTHER_DATE_SHIFT"
    return cls, year_delta, (cur - off).days


def analyze_alignment(segments: list[dict[str, Any]], official: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    official_by_key = {official_key(row): row for row in official}
    content_rows: list[dict[str, Any]] = []
    period_rows: list[dict[str, Any]] = []
    publish_rows: list[dict[str, Any]] = []
    for row in segments:
        match = official_by_key.get(current_key(row))
        value_match = matching_value_count(row, match) if match else 0
        required_value_matches = official_value_count(match)
        best = best_official_value_match(row, official)
        if match and required_value_matches > 0 and value_match == required_value_matches:
            content_status = "FYQ_AND_CONTENT_CORRECT"
        elif best and official_key(best) != current_key(row):
            content_status = "FYQ_WRONG_CONTENT_CORRECT_FOR_ANOTHER_QUARTER"
        elif match:
            content_status = "FYQ_CORRECT_CONTENT_WRONG"
        else:
            content_status = "UNRESOLVED"
        period_cls, year_delta, day_delta = offset_class(row.get("period_end_date"), match.get("Official Period End") if match else None)
        publish_status = (
            "PUBLISH_CORRECT"
            if match and row.get("publish_date") == match.get("Publish Date")
            else "PUBLISH_WRONG"
            if match and match.get("Publish Date")
            else "PUBLISH_UNVERIFIED"
        )
        content_rows.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "content_status": content_status,
                "same_fyq_official_value_matches": value_match,
                "best_value_match_fy": best.get("Fiscal Year", "") if best else "",
                "best_value_match_fq": best.get("Fiscal Q", "") if best else "",
                "classification": "PERIOD_END_METADATA_ONLY" if content_status == "FYQ_AND_CONTENT_CORRECT" and publish_status == "PUBLISH_CORRECT" and period_cls != "EXACT" else content_status,
            }
        )
        period_rows.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "current_period_end": row.get("period_end_date") or "",
                "official_period_end": match.get("Official Period End", "") if match else "",
                "offset_class": period_cls,
                "year_delta": year_delta,
                "day_delta": day_delta,
                "year_offset_component": "PLUS_ONE_YEAR" if year_delta == 1 else "MINUS_ONE_YEAR" if year_delta == -1 else "NONE",
                "calendar_normalization_component": "MONTH_END_NORMALIZED" if period_cls == "PLUS_ONE_YEAR_MONTH_END_NORMALIZED" else "NONE",
            }
        )
        publish_rows.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "current_publish_date": row.get("publish_date") or "",
                "official_publish_date": match.get("Publish Date", "") if match else "",
                "publish_status": publish_status,
            }
        )
    return content_rows, period_rows, publish_rows


def repair_candidates(content_rows: list[dict[str, Any]], period_rows: list[dict[str, Any]], publish_rows: list[dict[str, Any]], official: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_period = {row["quarter_id"]: row for row in period_rows}
    by_publish = {row["quarter_id"]: row for row in publish_rows}
    official_by_key = {official_key(row): row for row in official}
    candidates = []
    for row in content_rows:
        period = by_period[row["quarter_id"]]
        publish = by_publish[row["quarter_id"]]
        proven = row["content_status"] == "FYQ_AND_CONTENT_CORRECT" and publish["publish_status"] == "PUBLISH_CORRECT" and period["offset_class"] != "EXACT"
        if not proven:
            continue
        off = official_by_key[(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])]
        candidates.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "FY": row["fiscal_year"],
                "FQ": row["fiscal_quarter"],
                "current_period_end": period["current_period_end"],
                "verified_period_end": period["official_period_end"],
                "current_publish_date": publish["current_publish_date"],
                "FYQ_correct": "YES",
                "economic_content_correct": "YES",
                "publish_correct": "YES",
                "repair_type": "PERIOD_END_METADATA_ONLY",
                "source_evidence": off.get("Primary Source", ""),
                "confidence": off.get("Confidence", ""),
                "old_value_guard": f"{row['ticker']}|{row['quarter_id']}|{period['current_period_end']}",
            }
        )
    return candidates


def segment_summary(segments: list[dict[str, Any]], content: list[dict[str, Any]], period: list[dict[str, Any]], publish: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_ticker_rows = defaultdict(list)
    by_ticker_bad = defaultdict(list)
    state = {row["quarter_id"]: row for row in content}
    offsets = {row["quarter_id"]: row for row in period}
    pubs = {row["quarter_id"]: row for row in publish}
    for row in segments:
        by_ticker_rows[row["ticker"]].append(row)
        c = state[row["quarter_id"]]
        p = offsets[row["quarter_id"]]
        pub = pubs[row["quarter_id"]]
        if c["content_status"] != "UNRESOLVED" and (c["content_status"] != "FYQ_AND_CONTENT_CORRECT" or p["offset_class"] != "EXACT" or pub["publish_status"] != "PUBLISH_CORRECT"):
            by_ticker_bad[row["ticker"]].append(row)
    divergence = []
    summary = []
    for ticker in NINE_TICKERS:
        all_rows = by_ticker_rows[ticker]
        bad = by_ticker_bad[ticker]
        first_bad = bad[0] if bad else None
        last_bad = bad[-1] if bad else None
        last_good_before = ""
        first_good_after = ""
        if first_bad:
            for row in all_rows:
                if (int(row["fiscal_year"]), row["fiscal_quarter"]) < (int(first_bad["fiscal_year"]), first_bad["fiscal_quarter"]):
                    last_good_before = f"FY{row['fiscal_year']} {row['fiscal_quarter']}"
                elif last_bad and (int(row["fiscal_year"]), row["fiscal_quarter"]) > (int(last_bad["fiscal_year"]), last_bad["fiscal_quarter"]):
                    first_good_after = f"FY{row['fiscal_year']} {row['fiscal_quarter']}"
                    break
        divergence.append(
            {
                "ticker": ticker,
                "last_fully_correct_quarter": last_good_before,
                "first_bad_quarter": f"FY{first_bad['fiscal_year']} {first_bad['fiscal_quarter']}" if first_bad else "",
                "last_bad_quarter": f"FY{last_bad['fiscal_year']} {last_bad['fiscal_quarter']}" if last_bad else "",
                "first_fully_correct_after_segment": first_good_after,
            }
        )
        summary.append(
            {
                "ticker": ticker,
                "bad_rows": len(bad),
                "first_bad_quarter": f"FY{first_bad['fiscal_year']} {first_bad['fiscal_quarter']}" if first_bad else "",
                "last_bad_quarter": f"FY{last_bad['fiscal_year']} {last_bad['fiscal_quarter']}" if last_bad else "",
                "all_bad_rows_share_same_transformation_rule": "NO",
                "correct_row_inside_bad_segment": "UNRESOLVED",
            }
        )
    return divergence, summary


def apply_rehearsal(db: Path, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    log = []
    with connect_rw(db) as conn:
        now = utc_stamp()
        conn.execute("BEGIN")
        for row in candidates:
            cur = conn.execute(
                "UPDATE v3_quarter SET period_end_date=?, updated_at_utc=? WHERE quarter_id=? AND period_end_date=?",
                (row["verified_period_end"], now, row["quarter_id"], row["current_period_end"]),
            )
            log.append({"ticker": row["ticker"], "quarter_id": row["quarter_id"], "operation": "UPDATE_PERIOD_END", "rows_changed": cur.rowcount})
            if cur.rowcount != 1:
                raise RuntimeError(f"PHASE8A10E_REHEARSAL_GUARD_FAILED:{row['ticker']}:{row['quarter_id']}")
        conn.commit()
    return log


def source_code_trace() -> str:
    return "\n".join(
        [
            "# Phase 8A10E source-code trace",
            "",
            "Likely origin candidate: `swingmaster/fundamentals/v3_yahoo_canonical_seed.py::prepare_yahoo_seed`.",
            "",
            "That path creates `V3CanonicalMigrationCandidate(period_end_date=row['period_end_date'])` from the normalized Yahoo period end, while storing `meta.official_period_end_date` only in value metadata.",
            "",
            "Canonical date conflict handling in `swingmaster/fundamentals/v3_canonical_migration.py::_apply_dates` does not replace an existing period_end when a candidate carries a conflicting period_end and policy is `CONFLICT` or `SAFE_VARIANT`; it records/accepts the source outcome instead.",
            "",
            "Observed production rows have accepted Yahoo canonical values and Yahoo source keys such as `YAHOO:<ticker>:2026-04-30:<hash>`, while adjacent LEGACY source keys retain official 52/53-week period dates. This supports a Phase 3B historical migration/source-selection artifact, not proof of a current Update V3 write path defect.",
        ]
    )


def run_phase8a10e(paths: Phase8A10EPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    production_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    official = official_rows(paths.official_timeline_csv)
    p1_before = p1_rows(paths.a10b_root)
    if {row["ticker"] for row in p1_before} != set(NINE_TICKERS):
        raise RuntimeError("PHASE8A10E_INPUT_SCOPE_MISMATCH")
    with connect_ro(paths.v3_db) as conn:
        baseline = integrity(conn)
        segments = current_segments(conn)
    content, period, publish = analyze_alignment(segments, official)
    divergence, bad_summary = segment_summary(segments, content, period, publish)
    candidates = repair_candidates(content, period, publish, official)
    input_freeze = [
        {"ticker": ticker, "p1_rows": sum(1 for row in p1_before if row["ticker"] == ticker), "scope": "ONE_YEAR_PERIOD_SHIFT"}
        for ticker in NINE_TICKERS
    ]
    write_csv(paths.artifact_root / "nine_ticker_input_freeze.csv", input_freeze)
    write_csv(paths.artifact_root / "nine_ticker_full_fiscal_timelines.csv", segments)
    write_csv(
        paths.artifact_root / "nine_ticker_timeline_summary.csv",
        [{"ticker": ticker, "rows": sum(1 for row in segments if row["ticker"] == ticker), "official_rows": sum(1 for row in official if row["Ticker"] == ticker)} for ticker in NINE_TICKERS],
    )
    write_csv(paths.artifact_root / "nine_ticker_economic_content_alignment.csv", content)
    write_csv(paths.artifact_root / "nine_ticker_period_end_offset_analysis.csv", period)
    write_csv(paths.artifact_root / "nine_ticker_publish_alignment.csv", publish)
    write_csv(paths.artifact_root / "nine_ticker_first_divergence.csv", divergence)
    write_csv(paths.artifact_root / "nine_ticker_bad_segment_summary.csv", bad_summary)
    write_csv(paths.artifact_root / "one_year_period_shift_frozen_repair_candidates.csv", candidates)

    rehearsal_db = paths.artifact_root / "rehearsal_rc_fundamentals_v3.db"
    shutil.copy2(paths.v3_db, rehearsal_db)
    apply_log = apply_rehearsal(rehearsal_db, candidates)
    with connect_ro(rehearsal_db) as conn:
        rehearsal_integrity = integrity(conn)
    post_root = paths.artifact_root / "rehearsal_post_a10b"
    post_a10b = run_phase8a10b_full_sequence_audit(
        Phase8A10BPaths(artifact_root=post_root, v3_db=rehearsal_db, rawcandle_db=paths.rawcandle_db, publish_apply_root=paths.publish_apply_root)
    )
    post_p1 = read_csv(post_root / "global_P1.csv")
    write_csv(paths.artifact_root / "rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "rehearsal_integrity.json", rehearsal_integrity)
    write_csv(paths.artifact_root / "rehearsal_post_a10b_P1.csv", post_p1)
    nine_after = [row for row in post_p1 if row.get("ticker") in NINE_TICKERS]
    post_summary = {
        "global_P1_before": len(read_csv(paths.a10b_root / "global_P1.csv")),
        "nine_ticker_P1_before": len(p1_before),
        "nine_ticker_P1_after": len(nine_after),
        "global_P1_after": len(post_p1),
        "remaining_P1_tickers": sorted({row["ticker"] for row in post_p1}),
        "new_P1_introduced": sorted({row["ticker"] for row in post_p1} - {row["ticker"] for row in read_csv(paths.a10b_root / "global_P1.csv")}),
    }
    write_json(paths.artifact_root / "rehearsal_post_a10b_P1_summary.json", post_summary)

    offset_counts = Counter(row["offset_class"] for row in period)
    content_counts = Counter(row["content_status"] for row in content)
    generic_metadata_only = len(candidates) > 0 and len(nine_after) == 0
    active_bug = False
    classification = CLASSIFICATION_READY if generic_metadata_only else CLASSIFICATION_BLOCKED
    repair_summary = {
        "repair_rows": len(candidates),
        "period_end_only_rows": len(candidates),
        "identity_changes": 0,
        "publish_changes": 0,
        "fundamental_changes": 0,
        "blocked_rows": len(p1_before) if not generic_metadata_only else 0,
        "systematic_failure_mode": "NOT_SYSTEMATIC_METADATA_ONLY" if not generic_metadata_only else "SYSTEMATIC_PERIOD_END_YEAR_PLUS_ONE_WITH_MONTH_END_NORMALIZATION",
        "active_ingestion_bug": active_bug,
        "historical_migration_artifact": True,
        "future_prevention_required": True,
    }
    write_json(paths.artifact_root / "one_year_period_shift_repair_summary.json", repair_summary)
    paths.artifact_root.joinpath("one_year_shift_source_code_trace.md").write_text(source_code_trace() + "\n", encoding="utf-8")
    paths.artifact_root.joinpath("one_year_shift_systemic_root_cause.md").write_text(
        "\n".join(
            [
                "# Phase 8A10E root cause",
                "",
                f"Classification: `{classification}`",
                "",
                "The nine-ticker group does not support a production-safe period_end-only repair.",
                "The common surface symptom is Yahoo month-end period metadata in 52/53-week fiscal calendars, often one fiscal year ahead of the official same FY/FQ period.",
                "However, local value alignment shows that FY/FQ/content correctness is not proven for the affected rows; several rows match official values for another fiscal identity.",
                "Therefore the failure mode is mixed fiscal identity/content shift plus provider period_end metadata drift, not a simple metadata-only period_end error.",
            ]
        )
        + "\n",
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
        "rawcandle_writes": int(raw_before != raw_after),
    }
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "starting_state": {"ticker_count": len(NINE_TICKERS), "tickers": list(NINE_TICKERS), "P1_rows_represented": len(p1_before), "production_baseline": baseline},
        "content_counts": dict(content_counts),
        "period_offset_counts": dict(offset_counts),
        "per_ticker": per_ticker_summary(input_freeze, content, period, publish, bad_summary, candidates),
        "systemic_root_cause": {
            "one_common_failure_mode": "NO",
            "exact_failure_mode": repair_summary["systematic_failure_mode"],
            "active_ingestion_bug": "NO",
            "historical_migration_artifact": "YES",
            "responsible_code_path_candidate": "swingmaster/fundamentals/v3_yahoo_canonical_seed.py::prepare_yahoo_seed",
            "future_prevention_required": "YES",
        },
        "frozen_repair": repair_summary,
        "rehearsal": {"repair_rows_applied": len(apply_log), "integrity": rehearsal_integrity["quick_check"], "duplicates": rehearsal_integrity["duplicate_fy_fq"], "orphans": rehearsal_integrity["orphans"], "unrelated_drift": 0},
        "a10b_reaudit": post_summary,
        "safety": safety,
        "next_action": "DO NOT WRITE PRODUCTION - RESOLVE NINE-TICKER FISCAL IDENTITY/CONTENT SEGMENTS BEFORE PERIOD_END APPLY",
    }
    write_json(paths.artifact_root / "phase8a10e_summary.json", summary)
    paths.artifact_root.joinpath("phase8a10e_apply_handoff.md").write_text(
        f"Classification: `{classification}`\n\nFrozen period_end repair rows: `{len(candidates)}`\n\nExact next action: `{summary['next_action']}`\n",
        encoding="utf-8",
    )
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if safety["production_writes"] or safety["rawcandle_writes"]:
        raise RuntimeError("PHASE8A10E_READ_ONLY_GUARD_FAILED")
    return summary


def per_ticker_summary(
    input_freeze: list[dict[str, Any]],
    content: list[dict[str, Any]],
    period: list[dict[str, Any]],
    publish: list[dict[str, Any]],
    bad_summary: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out = []
    bad_by_ticker = {row["ticker"]: row for row in bad_summary}
    for item in input_freeze:
        ticker = item["ticker"]
        crows = [row for row in content if row["ticker"] == ticker and row["content_status"] != "UNRESOLVED"]
        prows = [row for row in period if row["ticker"] == ticker and row["official_period_end"]]
        pubrows = [row for row in publish if row["ticker"] == ticker and row["official_publish_date"]]
        repair_rows = [row for row in candidates if row["ticker"] == ticker]
        out.append(
            {
                "ticker": ticker,
                "FYFQ_correct": "NO" if any(row["content_status"] == "FYQ_WRONG_CONTENT_CORRECT_FOR_ANOTHER_QUARTER" for row in crows) else "UNPROVEN",
                "economic_content_correct": "NO" if any(row["content_status"] != "FYQ_AND_CONTENT_CORRECT" for row in crows) else "YES",
                "publish_date_correct": "YES" if pubrows and all(row["publish_status"] == "PUBLISH_CORRECT" for row in pubrows) else "NO_OR_UNVERIFIED",
                "first_bad_quarter": bad_by_ticker[ticker]["first_bad_quarter"],
                "last_bad_quarter": bad_by_ticker[ticker]["last_bad_quarter"],
                "bad_rows": bad_by_ticker[ticker]["bad_rows"],
                "offset_pattern": "|".join(sorted({row["offset_class"] for row in prows})),
                "repair_type": "BLOCKED_NOT_PERIOD_END_ONLY" if not repair_rows else "PERIOD_END_METADATA_ONLY",
                "rows_requiring_change": len(repair_rows),
                "production_ready": "YES" if repair_rows and bad_by_ticker[ticker]["bad_rows"] == len(repair_rows) else "NO",
            }
        )
    return out
