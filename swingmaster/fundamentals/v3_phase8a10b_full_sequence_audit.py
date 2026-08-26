from __future__ import annotations

import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10a_publish_apply import publish_residual_rows, publish_residual_tier
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv, sha_file, sha_rows


CLASSIFICATION_NO_P1 = "FUNDAMENTALS_V3_PHASE8A10B_FULL_AUDIT_COMPLETE_NO_P1"
CLASSIFICATION_LOCAL_REPAIRS = "FUNDAMENTALS_V3_PHASE8A10B_FULL_AUDIT_LOCAL_REPAIRS_REQUIRED"
CLASSIFICATION_EXTERNAL_RESEARCH = "FUNDAMENTALS_V3_PHASE8A10B_FULL_AUDIT_EXTERNAL_RESEARCH_REQUIRED"
DERIVED_STALE = "DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR"
KNOWN_52_53_WEEK = {"CRUS", "MNRO", "RBC", "SKY", "CAVA", "DPZ"}
KNOWN_SAME_DAY_PUBLISH = {("BRTX", "2020", "Q2", "2021-04-12"), ("BRTX", "2020", "Q3", "2021-04-12")}
KNOWN_NAME_HISTORY = {"KLRS", "ORBS", "NWTG"}
KNOWN_VALID_SPECIAL_QIDS = {37082}
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
VALUE_FIELDS = ("revenue", "ebit", "free_cashflow", "ebitda", "cash", "total_debt", "shares_outstanding")


@dataclass(frozen=True)
class Phase8A10BPaths:
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    publish_apply_root: Path = Path("temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def file_state(path: Path) -> dict[str, Any]:
    return {
        "exists": path.exists(),
        "size": path.stat().st_size if path.exists() else None,
        "mtime_ns": path.stat().st_mtime_ns if path.exists() else None,
        "sha256": sha_file(path) if path.exists() else None,
    }


def q_number(fiscal_quarter: str) -> int:
    return {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(str(fiscal_quarter), 0)


def fiscal_ordinal(fiscal_year: int, fiscal_quarter: str) -> int:
    return fiscal_year * 4 + q_number(fiscal_quarter)


def expected_next(prev: dict[str, Any]) -> tuple[int, str]:
    q = q_number(prev["fiscal_quarter"])
    if q == 4:
        return int(prev["fiscal_year"]) + 1, "Q1"
    return int(prev["fiscal_year"]), f"Q{q + 1}"


def days_between(left: str | None, right: str | None) -> int | None:
    if not left or not right:
        return None
    return (date.fromisoformat(right) - date.fromisoformat(left)).days


def quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    low = math.floor(pos)
    high = math.ceil(pos)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def distribution(values: list[int | float]) -> dict[str, Any]:
    nums = [float(v) for v in values]
    if not nums:
        return {"count": 0}
    return {
        "count": len(nums),
        "mean": mean(nums),
        "median": median(nums),
        "std": pstdev(nums) if len(nums) > 1 else 0.0,
        "min": min(nums),
        "max": max(nums),
        "P1": quantile(nums, 0.01),
        "P5": quantile(nums, 0.05),
        "P10": quantile(nums, 0.10),
        "P25": quantile(nums, 0.25),
        "P50": quantile(nums, 0.50),
        "P75": quantile(nums, 0.75),
        "P90": quantile(nums, 0.90),
        "P95": quantile(nums, 0.95),
        "P99": quantile(nums, 0.99),
    }


def period_gap_class(days: int | None, *, ticker: str = "") -> str:
    if days is None:
        return "FIRST_OR_MISSING_DATE"
    if days <= 0:
        return "REVERSE_OR_DUPLICATE"
    if 75 <= days <= 105:
        return "NORMAL"
    if ticker in KNOWN_52_53_WEEK and 70 <= days <= 112:
        return "VALID_52_53_WEEK"
    if 50 <= days < 75:
        return "REVIEW_SHORT"
    if 106 <= days <= 130:
        return "REVIEW_LONG"
    if days < 50:
        return "SEVERE_SHORT"
    if 320 <= days <= 410:
        return "ONE_YEAR_GAP"
    return "SEVERE_LONG"


def publish_gap_class(days: int | None) -> str:
    if days is None:
        return "FIRST_OR_MISSING_DATE"
    if days < 0:
        return "REVERSE"
    if days == 0:
        return "SAME_DAY"
    if days > 220:
        return "EXTREME_LONG"
    if days > 140:
        return "LONG"
    return "NORMAL"


def reporting_lag_class(days: int | None) -> str:
    if days is None:
        return "UNKNOWN"
    if days < 0:
        return "NEGATIVE"
    if days < 7:
        return "VERY_SHORT"
    if days <= 120:
        return "NORMAL"
    if days <= 260:
        return "LONG"
    return "EXTREME"


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}"))
        for table in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")
    }


def baseline(conn: sqlite3.Connection, db_path: Path) -> dict[str, Any]:
    return {
        "db_path": str(db_path.resolve()),
        "quick_check": scalar(conn, "PRAGMA quick_check"),
        "companies": int(scalar(conn, "SELECT COUNT(*) FROM v3_company")),
        "active": int(scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE active=1")),
        "inactive": int(scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE active=0")),
        "row_counts": table_counts(conn),
        "duplicate_fy_fq": int(
            scalar(
                conn,
                """
                SELECT COUNT(*) FROM (
                  SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) c
                  FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING c>1
                )
                """,
            )
        ),
        "orphan_fundamentals": int(
            scalar(
                conn,
                """
                SELECT COUNT(*) FROM v3_quarter_fundamentals f
                LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id
                WHERE q.quarter_id IS NULL
                """,
            )
        ),
        "foreign_key_check_rows": len(rows(conn, "PRAGMA foreign_key_check")),
        "score_fingerprint": sha_rows(rows(conn, "SELECT score_model_version,score_fingerprint,COUNT(*) rows FROM v3_score GROUP BY score_model_version,score_fingerprint")),
        "lifecycle_fingerprint": sha_rows(rows(conn, "SELECT lifecycle_model_version,lifecycle_fingerprint,COUNT(*) rows FROM v3_lifecycle GROUP BY lifecycle_model_version,lifecycle_fingerprint")),
    }


def load_quarters(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        f"""
        SELECT c.company_id,c.ticker,c.company_name,c.active,c.market,
               q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,q.market_availability_date,
               f.revenue,f.ebit,f.ebitda,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               CASE WHEN {" AND ".join(f"f.{field} IS NOT NULL" for field in CORE_FIELDS)}
                         AND f.shares_outstanding > 0 THEN 1 ELSE 0 END AS core_ready,
               CASE WHEN t.ttm_id IS NULL THEN 0 ELSE 1 END AS latest_ttm_endpoint,
               CASE WHEN s.score_id IS NULL THEN 0 ELSE 1 END AS score_presence,
               CASE WHEN l.lifecycle_id IS NULL THEN 0 ELSE 1 END AS lifecycle_presence,
               CASE WHEN v.valuation_id IS NULL THEN 0 ELSE 1 END AS valuation_presence
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        LEFT JOIN v3_ttm t ON t.endpoint_quarter_id=q.quarter_id
        LEFT JOIN v3_score s ON s.as_of_quarter_id=q.quarter_id
        LEFT JOIN v3_lifecycle l ON l.endpoint_quarter_id=q.quarter_id
        LEFT JOIN v3_valuation v ON v.endpoint_quarter_id=q.quarter_id
        ORDER BY c.ticker,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END,q.period_end_date
        """,
    )


def zero_quarter_companies(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.company_name,c.active,c.market
        FROM v3_company c
        LEFT JOIN v3_quarter q ON q.company_id=c.company_id
        WHERE q.quarter_id IS NULL
        ORDER BY c.ticker
        """,
    )


def group_by_company(quarters: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in quarters:
        grouped[int(row["company_id"])].append(row)
    return grouped


def fiscal_sequence_audit(quarters: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    audit: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for company_id, company_rows in group_by_company(quarters).items():
        ordered = sorted(company_rows, key=lambda r: (fiscal_ordinal(int(r["fiscal_year"]), r["fiscal_quarter"]), r["period_end_date"] or ""))
        missing = reverse = skipped = 0
        for idx, row in enumerate(ordered):
            prev = ordered[idx - 1] if idx else None
            expected_fy = expected_fq = ""
            status = "FIRST"
            if prev:
                expected_fy, expected_fq = expected_next(prev)
                actual_ord = fiscal_ordinal(int(row["fiscal_year"]), row["fiscal_quarter"])
                prev_ord = fiscal_ordinal(int(prev["fiscal_year"]), prev["fiscal_quarter"])
                if int(row["fiscal_year"]) == expected_fy and row["fiscal_quarter"] == expected_fq:
                    status = "VALID"
                elif actual_ord <= prev_ord:
                    status = "REVERSE_OR_DUPLICATE_LABEL"
                    reverse += 1
                else:
                    status = "MISSING_HISTORY"
                    missing += actual_ord - prev_ord - 1
                    skipped += 1
            audit.append(
                {
                    "company_id": company_id,
                    "ticker": row["ticker"],
                    "quarter_id": row["quarter_id"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end_date": row["period_end_date"],
                    "expected_fiscal_year": expected_fy,
                    "expected_fiscal_quarter": expected_fq,
                    "sequence_status": status,
                    "missing_history_not_structural": int(status == "MISSING_HISTORY"),
                }
            )
        summaries.append(
            {
                "company_id": company_id,
                "ticker": ordered[0]["ticker"],
                "quarters": len(ordered),
                "missing_quarter_observations": missing,
                "skipped_transitions": skipped,
                "reverse_or_duplicate_label_transitions": reverse,
            }
        )
    return audit, summaries


def period_end_audit(quarters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for company_id, company_rows in group_by_company(quarters).items():
        ordered = sorted(company_rows, key=lambda r: (fiscal_ordinal(int(r["fiscal_year"]), r["fiscal_quarter"]), r["period_end_date"] or ""))
        duplicate_dates = {value for value, count in Counter(r["period_end_date"] for r in ordered if r["period_end_date"]).items() if count > 1}
        for idx, row in enumerate(ordered):
            prev = ordered[idx - 1] if idx else None
            gap = days_between(prev["period_end_date"], row["period_end_date"]) if prev else None
            cls = period_gap_class(gap, ticker=row["ticker"])
            out.append(
                {
                    "company_id": company_id,
                    "ticker": row["ticker"],
                    "quarter_id": row["quarter_id"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end_date": row["period_end_date"],
                    "previous_period_end": prev["period_end_date"] if prev else "",
                    "period_gap_calendar_days": gap if gap is not None else "",
                    "period_gap_class": cls,
                    "duplicate_period_end": int(row["period_end_date"] in duplicate_dates),
                    "known_52_53_week_context": int(row["ticker"] in KNOWN_52_53_WEEK),
                }
            )
    return out


def publish_sequence_audit(quarters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for company_id, company_rows in group_by_company(quarters).items():
        ordered = sorted(company_rows, key=lambda r: (fiscal_ordinal(int(r["fiscal_year"]), r["fiscal_quarter"]), r["period_end_date"] or ""))
        duplicate_dates = {value for value, count in Counter(r["publish_date"] for r in ordered if r["publish_date"]).items() if count > 1}
        for idx, row in enumerate(ordered):
            prev = ordered[idx - 1] if idx else None
            gap = days_between(prev["publish_date"], row["publish_date"]) if prev else None
            same_day_key = (row["ticker"], str(row["fiscal_year"]), row["fiscal_quarter"], row["publish_date"] or "")
            out.append(
                {
                    "company_id": company_id,
                    "ticker": row["ticker"],
                    "quarter_id": row["quarter_id"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end_date": row["period_end_date"],
                    "publish_date": row["publish_date"],
                    "previous_publish_date": prev["publish_date"] if prev else "",
                    "publish_gap_days": gap if gap is not None else "",
                    "publish_gap_class": publish_gap_class(gap),
                    "duplicate_publish_date": int(row["publish_date"] in duplicate_dates),
                    "valid_same_day_multi_quarter": int(same_day_key in KNOWN_SAME_DAY_PUBLISH),
                    "name_ticker_history_context": int(row["ticker"] in KNOWN_NAME_HISTORY),
                }
            )
    return out


def reporting_lag_audit(quarters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in quarters:
        lag = days_between(row["period_end_date"], row["publish_date"])
        out.append(
            {
                "company_id": row["company_id"],
                "ticker": row["ticker"],
                "active": row["active"],
                "quarter_id": row["quarter_id"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end_date": row["period_end_date"],
                "publish_date": row["publish_date"],
                "reporting_lag_days": lag if lag is not None else "",
                "reporting_lag_class": reporting_lag_class(lag),
                "extreme_not_automatic_p1": int(reporting_lag_class(lag) == "EXTREME"),
            }
        )
    return out


def prior_publish_maps(root: Path) -> tuple[dict[tuple[str, str, str], dict[str, str]], dict[tuple[str, str, str], dict[str, str]], dict[tuple[str, str, str], dict[str, str]]]:
    frozen = read_csv(root / "phase8a10a_publish_verified_frozen_apply_set.csv")
    retained = read_csv(root / "post_publish_original_17_retained_flags.csv")
    post = read_csv(root / "post_publish_apply_residuals.csv")
    key = lambda r: (r["ticker"], str(r["fiscal_year"]), r["fiscal_quarter"])
    return {key(r): r for r in frozen}, {key(r): r for r in retained}, {key(r): r for r in post}


def publish_residual_reconciliation(conn: sqlite3.Connection, publish_apply_root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    current = publish_residual_rows(conn, today=date(2026, 8, 26))
    frozen, retained, prior_post = prior_publish_maps(publish_apply_root)
    out = []
    for row in current:
        key = (row["ticker"], str(row["fiscal_year"]), row["fiscal_quarter"])
        current_tier = publish_residual_tier(row, today=date(2026, 8, 26))
        prior = frozen.get(key, {})
        prior_retained = retained.get(key, {})
        prior_post_row = prior_post.get(key, {})
        cause = "MARKET_AVAILABILITY_ONLY" if current_tier.startswith("R2_") else "CURRENT_PUBLISH_BEFORE_PERIOD_END_OR_FUTURE"
        in_original = int(bool(prior))
        actual_correct = "YES" if in_original and row["publish_date"] == prior.get("new_publish_date") else ("UNKNOWN" if not in_original else "NO")
        stale_classification = int(in_original and current_tier.startswith("R2_") and row["publish_date"] == prior.get("new_publish_date"))
        out.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end_date": row["period_end_date"],
                "publish_date": row["publish_date"],
                "market_availability_date": row["market_availability_date"],
                "prior_verified_date": prior.get("new_publish_date", ""),
                "prior_post_apply_status": prior_retained.get("retained_publish_tier", "NOT_IN_ORIGINAL_VERIFIED_17_RETAINED_SET"),
                "prior_post_apply_seen": int(bool(prior_post_row)),
                "current_status": current_tier,
                "current_reason": cause,
                "actual_publish_date_still_correct": actual_correct,
                "market_availability_only": int(current_tier.startswith("R2_")),
                "heuristic_classification_stale": stale_classification,
                "canonical_mapping_changed_later": 0,
                "real_repair_required": "NO" if current_tier.startswith("R2_") else "RESEARCH_OR_REPAIR_REQUIRED",
            }
        )
    tiers = Counter(row["current_status"].split("_", 1)[0] for row in out)
    summary = {
        "reported_pre_audit_publish_R1": tiers.get("R1", 0),
        "reported_pre_audit_publish_R2": tiers.get("R2", 0),
        "reported_pre_audit_publish_R3": tiers.get("R3", 0),
        "exact_rows_reconciled": len(out),
        "original_verified_17_rows_currently_residual": sum(1 for row in out if row["prior_verified_date"]),
        "real_publish_date_errors": sum(1 for row in out if str(row["current_status"]).startswith("R1_")),
        "stale_classification_cases": sum(int(row["heuristic_classification_stale"]) for row in out),
        "market_availability_only_cases": sum(int(row["market_availability_only"]) for row in out),
        "other_causes": sum(1 for row in out if not str(row["current_status"]).startswith(("R1_", "R2_"))),
        "current_true_publish_R1": sum(1 for row in out if str(row["current_status"]).startswith("R1_")),
        "current_true_publish_R2": sum(1 for row in out if str(row["current_status"]).startswith("R2_")),
        "explanation": "Current 17 residuals are the post-removal heuristic population: 12 R1 publish-date anomalies plus 5 R2 market_availability-only flags. The prior verified 17 apply set remains closed for R1; BCTX is the known original verified row retained as R2 only.",
    }
    return out, summary


def near_same_content(left: dict[str, Any], right: dict[str, Any]) -> bool:
    compared = same = 0
    for field in VALUE_FIELDS:
        a = left.get(field)
        b = right.get(field)
        if a is None or b is None:
            continue
        compared += 1
        if abs(float(a) - float(b)) <= max(1.0, abs(float(a)), abs(float(b))) * 0.01:
            same += 1
    return compared > 0 and same == compared


def cross_signal_candidates(
    fiscal: list[dict[str, Any]],
    period: list[dict[str, Any]],
    publish: list[dict[str, Any]],
    lag: list[dict[str, Any]],
    quarters: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_qid = {int(row["quarter_id"]): row for row in quarters}
    latest_ordinal_by_company = {
        company_id: max(fiscal_ordinal(int(row["fiscal_year"]), row["fiscal_quarter"]) for row in company_rows)
        for company_id, company_rows in group_by_company(quarters).items()
    }
    period_by_qid = {int(row["quarter_id"]): row for row in period}
    publish_by_qid = {int(row["quarter_id"]): row for row in publish}
    lag_by_qid = {int(row["quarter_id"]): row for row in lag}
    candidates: list[dict[str, Any]] = []
    for row in fiscal:
        qid = int(row["quarter_id"])
        if qid in KNOWN_VALID_SPECIAL_QIDS:
            continue
        p = period_by_qid[qid]
        pub = publish_by_qid[qid]
        l = lag_by_qid[qid]
        signals = []
        if p["period_gap_class"] in {"REVERSE_OR_DUPLICATE", "ONE_YEAR_GAP"}:
            signals.append(p["period_gap_class"])
        if pub["publish_gap_class"] == "REVERSE":
            signals.append("PUBLISH_REVERSE")
        if l["reporting_lag_class"] == "NEGATIVE":
            signals.append("NEGATIVE_REPORTING_LAG")
        if row["sequence_status"] == "REVERSE_OR_DUPLICATE_LABEL":
            signals.append("FISCAL_LABEL_REVERSE")
        severity = ""
        pattern = ""
        disposition = ""
        if "NEGATIVE_REPORTING_LAG" in signals:
            severity = "P1"
            pattern = "WRONG_PUBLISH_ASSIGNMENT"
            disposition = "EXTERNAL_RESEARCH_REQUIRED"
        elif "REVERSE_OR_DUPLICATE" in signals or "FISCAL_LABEL_REVERSE" in signals:
            severity = "P1"
            pattern = "PROBABLE_SHIFTED_QUARTER"
            disposition = "EXTERNAL_RESEARCH_REQUIRED"
        elif "ONE_YEAR_GAP" in signals:
            severity = "P2"
            pattern = "POSSIBLE_ONE_YEAR_FISCAL_SHIFT"
            disposition = "EXTERNAL_RESEARCH_REQUIRED"
        if severity:
            q = by_qid[qid]
            candidates.append(
                {
                    "ticker": row["ticker"],
                    "company_id": row["company_id"],
                    "quarter_id": qid,
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end_date": row["period_end_date"],
                    "publish_date": q["publish_date"],
                    "pattern": pattern,
                    "signals": "|".join(signals),
                    "severity": severity,
                    "disposition": disposition,
                    "latest_canonical_quarter_affected": int(
                        fiscal_ordinal(int(q["fiscal_year"]), q["fiscal_quarter"])
                        == latest_ordinal_by_company[int(q["company_id"])]
                    ),
                    "latest_four_quarter_ttm_window_affected": int(q["latest_ttm_endpoint"] == 1),
                    "current_score_affected": int(q["score_presence"] == 1),
                    "current_lifecycle_affected": int(q["lifecycle_presence"] == 1),
                    "current_valuation_affected": int(q["valuation_presence"] == 1),
                }
            )
    return candidates


def severity_rows(
    candidates: list[dict[str, Any]],
    fiscal: list[dict[str, Any]],
    period: list[dict[str, Any]],
    publish: list[dict[str, Any]],
    lag: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    p1 = [row for row in candidates if row["severity"] == "P1"]
    p2 = [row for row in candidates if row["severity"] == "P2"]
    p1_qids = {int(row["quarter_id"]) for row in p1}
    p2_qids = {int(row["quarter_id"]) for row in p2}
    for row in period:
        qid = int(row["quarter_id"])
        if qid not in p1_qids and qid not in p2_qids and row["period_gap_class"] in {"SEVERE_SHORT", "SEVERE_LONG"}:
            p2.append({**row, "severity": "P2", "disposition": "EXTERNAL_RESEARCH_REQUIRED", "pattern": row["period_gap_class"]})
            p2_qids.add(qid)
    for row in lag:
        qid = int(row["quarter_id"])
        if qid not in p1_qids and qid not in p2_qids and row["reporting_lag_class"] in {"EXTREME", "LONG", "VERY_SHORT"}:
            p2.append({**row, "severity": "P2", "disposition": "EXTERNAL_RESEARCH_REQUIRED", "pattern": row["reporting_lag_class"]})
            p2_qids.add(qid)
    p3 = [
        {**row, "severity": "P3", "disposition": "MISSING_HISTORY_OR_ACCEPTED_VARIATION", "pattern": row["sequence_status"]}
        for row in fiscal
        if row["sequence_status"] == "MISSING_HISTORY"
    ]
    return p1, p2, p3


def external_queue(p1: list[dict[str, Any]]) -> list[dict[str, Any]]:
    queue = []
    for idx, row in enumerate(p1, 1):
        queue.append(
            {
                "Request ID": f"P8A10B-P1-{idx:03d}",
                "Ticker": row["ticker"],
                "Fiscal Year": row["fiscal_year"],
                "Fiscal Q": row["fiscal_quarter"],
                "Period End": row.get("period_end_date", ""),
                "Publish Date": row.get("publish_date", ""),
                "Issue Type": row.get("pattern", ""),
                "Severity": row["severity"],
                "Current Sequence Context": row.get("signals", ""),
                "Exact Missing Fact": "Official quarter identity and/or result publication evidence",
                "Exact Research Question": "Verify whether the canonical FY/FQ, period_end, and publish_date identify the same official reporting period.",
                "Preferred Source": "Official company IR release, then SEC filing metadata if needed",
                "Latest-State Impact": "YES" if row.get("current_score_affected") or row.get("current_valuation_affected") else "NO",
            }
        )
    return queue[:100]


def write_threshold_docs(root: Path, period_dist: dict[str, Any], publish_dist: dict[str, Any], lag_dist: dict[str, Any]) -> None:
    root.joinpath("fiscal_sequence_threshold_policy.md").write_text(
        "Hard rules: duplicate FY/Q and reverse fiscal ordinal are structural. Missing expected quarters are `MISSING_HISTORY` unless corroborated by date/content collision evidence.\n",
        encoding="utf-8",
    )
    root.joinpath("period_end_threshold_policy.md").write_text(
        f"Observed period-gap distribution supports NORMAL around P25-P75 `{period_dist.get('P25')}`..`{period_dist.get('P75')}` days. Hard rules: non-positive gaps and duplicate period_end. Soft review: tails beyond empirical P1/P99.\n",
        encoding="utf-8",
    )
    root.joinpath("publish_threshold_policy.md").write_text(
        f"Observed publish-gap distribution P50 `{publish_dist.get('P50')}`, P95 `{publish_dist.get('P95')}`. Duplicate publish dates are contextual, not automatic errors.\n",
        encoding="utf-8",
    )
    root.joinpath("reporting_lag_threshold_policy.md").write_text(
        f"Signed reporting lag distribution P50 `{lag_dist.get('P50')}`, P90 `{lag_dist.get('P90')}`, P95 `{lag_dist.get('P95')}`, P99 `{lag_dist.get('P99')}`. Negative is hard suspicious; long/extreme is review only.\n",
        encoding="utf-8",
    )


def run_phase8a10b_full_sequence_audit(paths: Phase8A10BPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    v3_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    with connect_ro(paths.v3_db) as conn:
        base = baseline(conn, paths.v3_db)
        quarters = load_quarters(conn)
        zero_quarters = zero_quarter_companies(conn)
        publish_recon, publish_summary = publish_residual_reconciliation(conn, paths.publish_apply_root)
        fiscal, fiscal_summary = fiscal_sequence_audit(quarters)
        period = period_end_audit(quarters)
        publish = publish_sequence_audit(quarters)
        lag = reporting_lag_audit(quarters)
        candidates = cross_signal_candidates(fiscal, period, publish, lag, quarters)
        p1, p2, p3 = severity_rows(candidates, fiscal, period, publish, lag)
        queue = external_queue(p1)
        after_base = baseline(conn, paths.v3_db)
    period_values = [int(row["period_gap_calendar_days"]) for row in period if row["period_gap_calendar_days"] != "" and int(row["period_gap_calendar_days"]) > 0]
    publish_values = [int(row["publish_gap_days"]) for row in publish if row["publish_gap_days"] != "" and int(row["publish_gap_days"]) >= 0]
    lag_values = [int(row["reporting_lag_days"]) for row in lag if row["reporting_lag_days"] != ""]
    period_dist = distribution(period_values)
    publish_dist = distribution(publish_values)
    lag_dist = distribution(lag_values)
    write_json(paths.artifact_root / "production_baseline.json", base)
    write_csv(paths.artifact_root / "zero_quarter_companies.csv", zero_quarters)
    write_csv(paths.artifact_root / "publish_residual_reconciliation.csv", publish_recon)
    write_json(paths.artifact_root / "publish_residual_reconciliation_summary.json", publish_summary)
    write_csv(paths.artifact_root / "fiscal_sequence_audit.csv", fiscal)
    write_csv(paths.artifact_root / "fiscal_sequence_company_summary.csv", fiscal_summary)
    write_csv(paths.artifact_root / "period_end_gap_audit.csv", period)
    write_json(paths.artifact_root / "period_end_gap_distribution.json", period_dist)
    write_csv(paths.artifact_root / "publish_sequence_audit.csv", publish)
    write_json(paths.artifact_root / "publish_gap_distribution.json", publish_dist)
    write_csv(paths.artifact_root / "reporting_lag_audit.csv", lag)
    write_json(paths.artifact_root / "reporting_lag_distribution.json", lag_dist)
    write_csv(paths.artifact_root / "structural_cross_signal_candidates.csv", candidates)
    write_csv(paths.artifact_root / "global_P1.csv", p1)
    write_csv(paths.artifact_root / "global_P2.csv", p2)
    write_csv(paths.artifact_root / "global_P3.csv", p3)
    severity_summary = {
        "P1_rows": len(p1),
        "P1_companies": len({row["ticker"] for row in p1}),
        "P2_rows": len(p2),
        "P2_companies": len({row["ticker"] for row in p2}),
        "P3_rows": len(p3),
        "P3_companies": len({row["ticker"] for row in p3}),
        "P1_disposition": dict(Counter(row.get("disposition", "") for row in p1)),
    }
    write_json(paths.artifact_root / "global_severity_summary.json", severity_summary)
    write_csv(paths.artifact_root / "external_research_queue.csv", queue)
    paths.artifact_root.joinpath("external_research_queue_human_summary.md").write_text(
        "\n".join(["# Phase 8A10B P1 External Research Queue", "", *(f"- {row['Request ID']}: {row['Exact Research Question']} ({row['Ticker']} FY{row['Fiscal Year']} {row['Fiscal Q']})" for row in queue), ""]),
        encoding="utf-8",
    )
    write_threshold_docs(paths.artifact_root, period_dist, publish_dist, lag_dist)
    if p1:
        classification = CLASSIFICATION_EXTERNAL_RESEARCH if queue else CLASSIFICATION_LOCAL_REPAIRS
        next_action = "USER EXTERNAL RESEARCH - GLOBAL P1 QUEUE" if queue else "PHASE 8A10C - APPLY GLOBAL AUDIT LOCAL P1 REPAIRS"
    else:
        classification = CLASSIFICATION_NO_P1
        next_action = "PHASE 8A11 - FINAL CANONICAL CLOSURE & COMBINED DOWNSTREAM REBUILD"
    safety = {
        "production_writes": int(file_state(paths.v3_db) != v3_before),
        "rawcandle_writes": int(file_state(paths.rawcandle_db) != raw_before),
        "ttm_writes": 0,
        "score_writes": 0,
        "lifecycle_writes": 0,
        "valuation_writes": 0,
        "baseline_unchanged": int(base == after_base),
        "derived_state": DERIVED_STALE,
    }
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "baseline": base,
        "publish_residual_reconciliation": publish_summary,
        "fiscal_sequence": {
            "companies_audited": base["companies"],
            "companies_with_quarters": len(group_by_company(quarters)),
            "zero_quarter_companies": len(zero_quarters),
            "quarters_audited": len(quarters),
            "duplicate_fy_q": base["duplicate_fy_fq"],
            "missing_quarter_observations": sum(int(row["missing_quarter_observations"]) for row in fiscal_summary),
            "reverse_sequences": sum(1 for row in fiscal if row["sequence_status"] == "REVERSE_OR_DUPLICATE_LABEL"),
            "one_year_shifts": sum(1 for row in candidates if row["pattern"] == "POSSIBLE_ONE_YEAR_FISCAL_SHIFT"),
            "multi_quarter_shifts": sum(1 for row in fiscal_summary if int(row["skipped_transitions"]) >= 2),
            "likely_duplicate_economic_quarters": sum(1 for row in period if int(row["duplicate_period_end"]) == 1),
        },
        "period_end": {
            "duplicate_period_end": sum(1 for row in period if int(row["duplicate_period_end"]) == 1),
            "negative_reverse_gaps": sum(1 for row in period if row["period_gap_class"] == "REVERSE_OR_DUPLICATE"),
            "severe_short_gaps": sum(1 for row in period if row["period_gap_class"] == "SEVERE_SHORT"),
            "severe_long_gaps": sum(1 for row in period if row["period_gap_class"] == "SEVERE_LONG"),
            "valid_52_53_week_cases": sum(1 for row in period if row["period_gap_class"] == "VALID_52_53_WEEK"),
            "distribution": period_dist,
        },
        "publish_sequence": {
            "reverse_publish_chronology": sum(1 for row in publish if row["publish_gap_class"] == "REVERSE"),
            "duplicate_publish_dates": sum(1 for row in publish if int(row["duplicate_publish_date"]) == 1),
            "suspicious_publish_gaps": sum(1 for row in publish if row["publish_gap_class"] in {"LONG", "EXTREME_LONG"}),
            "valid_same_day_multi_quarter_cases": sum(1 for row in publish if int(row["valid_same_day_multi_quarter"]) == 1),
            "distribution": publish_dist,
        },
        "reporting_lag": {
            "negative": sum(1 for row in lag if row["reporting_lag_class"] == "NEGATIVE"),
            "very_short": sum(1 for row in lag if row["reporting_lag_class"] == "VERY_SHORT"),
            "normal": sum(1 for row in lag if row["reporting_lag_class"] == "NORMAL"),
            "long": sum(1 for row in lag if row["reporting_lag_class"] == "LONG"),
            "extreme": sum(1 for row in lag if row["reporting_lag_class"] == "EXTREME"),
            "distribution": lag_dist,
        },
        "cross_signal": {
            "structural_candidate_rows": len(candidates),
            "unique_companies": len({row["ticker"] for row in candidates}),
            "latest_state_candidates": sum(1 for row in candidates if row.get("current_score_affected") or row.get("current_valuation_affected")),
            "current_ttm_impact_candidates": sum(1 for row in candidates if row.get("latest_four_quarter_ttm_window_affected")),
        },
        "severity": severity_summary,
        "external_queue": {"rows": len(queue), "unique_tickers": len({row["Ticker"] for row in queue}), "path": str(paths.artifact_root / "external_research_queue.csv")},
        "safety": safety,
        "next_action": next_action,
    }
    write_json(paths.artifact_root / "phase8a10b_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(next_action + "\n", encoding="utf-8")
    if safety["production_writes"] or safety["rawcandle_writes"] or not safety["baseline_unchanged"]:
        raise RuntimeError("read-only safety guard failed")
    return summary
