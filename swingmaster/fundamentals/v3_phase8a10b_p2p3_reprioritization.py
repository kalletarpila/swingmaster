from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10b_full_sequence_audit import baseline, connect_ro, file_state, fiscal_ordinal
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv


CLASSIFICATION_CURRENT_CRITICAL = "FUNDAMENTALS_V3_PHASE8A10B_P2P3_REPRIORITIZED_CURRENT_CRITICAL_REVIEW_REQUIRED"
CLASSIFICATION_NO_CURRENT_CRITICAL = "FUNDAMENTALS_V3_PHASE8A10B_P2P3_REPRIORITIZED_NO_CURRENT_CRITICAL_BLOCKERS"
RECENT_CUTOFF = date(2024, 1, 1)
KNOWN_52_53_WEEK = {"CRUS", "MNRO", "RBC", "SKY", "CAVA", "DPZ"}

QUEUE_COLUMNS = [
    "Priority Rank",
    "Ticker",
    "Company ID",
    "Fiscal Year",
    "Fiscal Q",
    "Period End",
    "Publish Date",
    "Original Severity",
    "Reclassified Severity",
    "Issue Type",
    "Signal Count",
    "Signals",
    "Latest Quarter Rank",
    "In Latest 8Q",
    "In Latest 4Q",
    "Is 2024+",
    "Affects Current TTM",
    "Affects Score",
    "Affects Lifecycle",
    "Affects Valuation",
    "Systemic Pattern",
    "Current Evidence",
    "Exact Missing Fact",
    "Recommended Action",
    "External Research Required",
    "Notes",
]


@dataclass(frozen=True)
class Phase8A10BP2P3Paths:
    artifact_root: Path
    source_audit_root: Path
    v3_db: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    return date.fromisoformat(str(value)[:10])


def as_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    return int(value)


def row_qid(row: dict[str, Any]) -> int | None:
    if row.get("quarter_id") in (None, ""):
        return None
    return int(row["quarter_id"])


def latest_quarter_membership(quarters: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in quarters:
        grouped[int(row["company_id"])].append(row)

    membership: dict[int, dict[str, Any]] = {}
    for company_rows in grouped.values():
        ordered = sorted(
            company_rows,
            key=lambda r: (
                fiscal_ordinal(int(r["fiscal_year"]), str(r["fiscal_quarter"])),
                str(r.get("period_end_date") or ""),
                int(r["quarter_id"]),
            ),
            reverse=True,
        )
        history_count = len(ordered)
        for rank, row in enumerate(ordered, 1):
            qid = int(row["quarter_id"])
            membership[qid] = {
                **row,
                "latest_quarter_rank": rank,
                "in_latest_8q": int(rank <= 8),
                "in_latest_4q": int(rank <= 4),
                "is_latest_quarter": int(rank == 1),
                "company_quarter_count": history_count,
            }
    return membership


def load_quarter_rows(conn: Any) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.company_name,c.market,c.profile,c.active,
               q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,q.market_availability_date
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        ORDER BY c.ticker,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END,q.period_end_date
        """,
    )


def _latest_endpoint_qids(rows_: list[dict[str, Any]], qid_field: str) -> set[int]:
    by_company: dict[int, dict[str, Any]] = {}
    for row in rows_:
        company_id = int(row["company_id"])
        key = (fiscal_ordinal(int(row["fiscal_year"]), str(row["fiscal_quarter"])), str(row.get("period_end_date") or ""), int(row[qid_field]))
        if company_id not in by_company:
            by_company[company_id] = {**row, "_key": key}
        elif key > by_company[company_id]["_key"]:
            by_company[company_id] = {**row, "_key": key}
    return {int(row[qid_field]) for row in by_company.values()}


def current_downstream_sets(conn: Any) -> dict[str, set[int]]:
    ttm_rows = rows(
        conn,
        """
        SELECT t.company_id,t.endpoint_quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,
               t.q1_quarter_id,t.q2_quarter_id,t.q3_quarter_id,t.q4_quarter_id
        FROM v3_ttm t
        JOIN v3_quarter q ON q.quarter_id=t.endpoint_quarter_id
        """,
    )
    latest_ttm_endpoints = _latest_endpoint_qids(ttm_rows, "endpoint_quarter_id")
    current_ttm_inputs: set[int] = set()
    for row in ttm_rows:
        if int(row["endpoint_quarter_id"]) not in latest_ttm_endpoints:
            continue
        for field in ("q1_quarter_id", "q2_quarter_id", "q3_quarter_id", "q4_quarter_id"):
            current_ttm_inputs.add(int(row[field]))

    score_rows = rows(
        conn,
        """
        SELECT s.company_id,s.as_of_quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date
        FROM v3_score s
        JOIN v3_quarter q ON q.quarter_id=s.as_of_quarter_id
        """,
    )
    lifecycle_rows = rows(
        conn,
        """
        SELECT l.company_id,l.endpoint_quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date
        FROM v3_lifecycle l
        JOIN v3_quarter q ON q.quarter_id=l.endpoint_quarter_id
        """,
    )
    valuation_rows = rows(
        conn,
        """
        SELECT v.company_id,v.endpoint_quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date
        FROM v3_valuation v
        JOIN v3_quarter q ON q.quarter_id=v.endpoint_quarter_id
        """,
    )
    return {
        "current_ttm_inputs": current_ttm_inputs,
        "current_ttm_endpoints": latest_ttm_endpoints,
        "current_score": _latest_endpoint_qids(score_rows, "as_of_quarter_id"),
        "current_lifecycle": _latest_endpoint_qids(lifecycle_rows, "endpoint_quarter_id"),
        "current_valuation": _latest_endpoint_qids(valuation_rows, "endpoint_quarter_id"),
    }


def is_2024_plus(row: dict[str, Any]) -> int:
    period = parse_date(row.get("period_end_date"))
    publish = parse_date(row.get("publish_date"))
    return int(bool((period and period >= RECENT_CUTOFF) or (publish and publish >= RECENT_CUTOFF)))


def signal_index(
    fiscal: list[dict[str, Any]],
    period: list[dict[str, Any]],
    publish: list[dict[str, Any]],
    lag: list[dict[str, Any]],
    cross: list[dict[str, Any]],
) -> dict[int, set[str]]:
    out: dict[int, set[str]] = defaultdict(set)
    for row in fiscal:
        qid = row_qid(row)
        if qid is not None and row.get("sequence_status") not in {"", "FIRST", "VALID"}:
            out[qid].add(f"FISCAL_SEQUENCE:{row.get('sequence_status')}")
    for row in period:
        qid = row_qid(row)
        if qid is None:
            continue
        cls = str(row.get("period_gap_class", ""))
        if cls not in {"", "FIRST_OR_MISSING_DATE", "NORMAL", "REVIEW_SHORT", "REVIEW_LONG", "VALID_52_53_WEEK"}:
            out[qid].add(f"PERIOD_END:{cls}")
        if as_int(row.get("duplicate_period_end")):
            out[qid].add("DUPLICATE_ECONOMIC_QUARTER")
    for row in publish:
        qid = row_qid(row)
        if qid is None:
            continue
        cls = str(row.get("publish_gap_class", ""))
        if cls in {"REVERSE", "LONG", "EXTREME_LONG"}:
            out[qid].add(f"PUBLISH_SEQUENCE:{cls}")
        if as_int(row.get("duplicate_publish_date")) and not as_int(row.get("valid_same_day_multi_quarter")):
            out[qid].add("PUBLISH_DUPLICATE_DATE")
    for row in lag:
        qid = row_qid(row)
        if qid is not None and row.get("reporting_lag_class") in {"NEGATIVE", "VERY_SHORT", "LONG", "EXTREME"}:
            out[qid].add(f"REPORTING_LAG:{row.get('reporting_lag_class')}")
    for row in cross:
        qid = row_qid(row)
        if qid is None:
            continue
        for signal in str(row.get("signals", "")).split("|"):
            if signal:
                out[qid].add(f"CROSS_SIGNAL:{signal}")
        pattern = row.get("pattern")
        if pattern:
            out[qid].add(f"CROSS_PATTERN:{pattern}")
    return out


def pattern_index(rows_: list[dict[str, Any]]) -> dict[tuple[str, str], int]:
    counts = Counter((str(row.get("pattern") or row.get("period_gap_class") or row.get("sequence_status") or "UNKNOWN"), str(row.get("ticker"))) for row in rows_)
    return dict(counts)


def classify_recommended_action(row: dict[str, Any], reclassified: str) -> str:
    ticker = str(row.get("ticker", ""))
    issue = str(row.get("issue_type", ""))
    signals = str(row.get("signals", ""))
    if "MARKET_AVAILABILITY" in issue or "MARKET_AVAILABILITY" in signals:
        return "MARKET_AVAILABILITY_ONLY"
    if ticker in KNOWN_52_53_WEEK and "VALID_52_53_WEEK" in signals:
        return "VALID_52_53_WEEK"
    if "MISSING_HISTORY" in issue and reclassified != "P3_ESCALATED":
        return "MISSING_HISTORY_NON_BLOCKING"
    if reclassified in {"P2A_CURRENT_CRITICAL_REVIEW", "P3_ESCALATED"}:
        if any(token in signals or token in issue for token in ("NEGATIVE", "REVERSE", "ONE_YEAR", "DUPLICATE", "FISCAL")):
            return "EXTERNAL_RESEARCH"
        return "LOCAL_EVIDENCE_REVIEW"
    if reclassified in {"P2C_HISTORICAL_DEFERRED", "P3B_HISTORICAL_INFORMATIONAL"}:
        return "DEFER_HISTORICAL"
    return "LIKELY_FALSE_POSITIVE"


def has_material_structural_signal(row: dict[str, Any]) -> bool:
    signals = str(row.get("signals", ""))
    return any(
        token in signals
        for token in (
            "REVERSE",
            "NEGATIVE",
            "ONE_YEAR",
            "DUPLICATE_ECONOMIC_QUARTER",
            "PERIOD_END:SEVERE_SHORT",
            "REPORTING_LAG:VERY_SHORT",
        )
    )


def annotate_row(
    row: dict[str, Any],
    *,
    original_severity: str,
    membership: dict[int, dict[str, Any]],
    downstream: dict[str, set[int]],
    signals_by_qid: dict[int, set[str]],
    p1_qids: set[int],
    systemic_patterns: set[str],
) -> dict[str, Any]:
    qid = row_qid(row)
    member = membership.get(qid or -1, {})
    raw_signals = set(signals_by_qid.get(qid or -1, set()))
    for signal in str(row.get("signals", "")).split("|"):
        if signal:
            raw_signals.add(signal)
    issue = str(row.get("pattern") or row.get("period_gap_class") or row.get("reporting_lag_class") or row.get("sequence_status") or "UNKNOWN")
    period_recent = is_2024_plus(row)
    in_latest_8q = as_int(member.get("in_latest_8q"))
    in_latest_4q = as_int(member.get("in_latest_4q"))
    affects_ttm = int(qid in downstream["current_ttm_inputs"] if qid is not None else False)
    affects_score = int(qid in downstream["current_score"] if qid is not None else False)
    affects_lifecycle = int(qid in downstream["current_lifecycle"] if qid is not None else False)
    affects_valuation = int(qid in downstream["current_valuation"] if qid is not None else False)
    systemic = int(issue in systemic_patterns)
    current_evidence = []
    if in_latest_8q:
        current_evidence.append("latest_8q")
    if in_latest_4q:
        current_evidence.append("latest_4q")
    if period_recent:
        current_evidence.append("2024plus")
    if affects_ttm:
        current_evidence.append("current_4q_ttm")
    if affects_score or affects_lifecycle or affects_valuation:
        current_evidence.append("current_downstream_endpoint")
    if systemic:
        current_evidence.append("systemic_recent_pattern")
    return {
        **row,
        "original_severity": original_severity,
        "issue_type": issue,
        "signals": "|".join(sorted(raw_signals)),
        "signal_count": len(raw_signals),
        "latest_quarter_rank": member.get("latest_quarter_rank", ""),
        "in_latest_8q": in_latest_8q,
        "in_latest_4q": in_latest_4q,
        "is_latest_quarter": as_int(member.get("is_latest_quarter")),
        "is_2024_plus": period_recent,
        "is_recent_priority": int(period_recent or in_latest_8q),
        "affects_latest_quarter": as_int(member.get("is_latest_quarter")),
        "affects_current_4q_ttm": affects_ttm,
        "affects_current_score": affects_score,
        "affects_current_lifecycle": affects_lifecycle,
        "affects_current_valuation": affects_valuation,
        "systemic_recent_pattern": systemic,
        "excluded_p1": int(qid in p1_qids if qid is not None else False),
        "current_evidence": "|".join(current_evidence),
    }


def systemic_recent_patterns(annotated: list[dict[str, Any]]) -> set[str]:
    counts: Counter[str] = Counter()
    companies: dict[str, set[str]] = defaultdict(set)
    for row in annotated:
        if not as_int(row.get("is_recent_priority")):
            continue
        issue = str(row.get("issue_type", "UNKNOWN"))
        counts[issue] += 1
        companies[issue].add(str(row.get("ticker", "")))
    return {issue for issue, count in counts.items() if count >= 10 and len(companies[issue]) >= 5}


def reclassify_p2(row: dict[str, Any]) -> str:
    if as_int(row.get("excluded_p1")):
        return "P1_EXCLUDED"
    recent_priority = as_int(row.get("is_recent_priority")) or as_int(row.get("is_2024_plus")) or as_int(row.get("in_latest_8q"))
    current_impact = any(
        as_int(row.get(field))
        for field in (
            "affects_latest_quarter",
            "affects_current_4q_ttm",
            "affects_current_score",
            "affects_current_lifecycle",
            "affects_current_valuation",
        )
    )
    if recent_priority and (current_impact or has_material_structural_signal(row)):
        return "P2A_CURRENT_CRITICAL_REVIEW"
    if recent_priority:
        return "P2B_RECENT_NONBLOCKING"
    return "P2C_HISTORICAL_DEFERRED"


def reclassify_p3(row: dict[str, Any]) -> str:
    if as_int(row.get("excluded_p1")):
        return "P1_EXCLUDED"
    recent_priority = as_int(row.get("is_recent_priority")) or as_int(row.get("is_2024_plus")) or as_int(row.get("in_latest_8q"))
    current_impact = any(
        as_int(row.get(field))
        for field in (
            "affects_latest_quarter",
            "affects_current_4q_ttm",
            "affects_current_score",
            "affects_current_lifecycle",
            "affects_current_valuation",
        )
    )
    if recent_priority and as_int(row.get("signal_count")) >= 2 and current_impact and has_material_structural_signal(row):
        return "P3_ESCALATED"
    if recent_priority:
        return "P3A_RECENT_INFORMATIONAL"
    return "P3B_HISTORICAL_INFORMATIONAL"


def enrich_classification(row: dict[str, Any], reclassified: str) -> dict[str, Any]:
    action = classify_recommended_action(row, reclassified)
    return {
        **row,
        "reclassified_severity": reclassified,
        "recommended_action": action,
        "external_research_required": int(action == "EXTERNAL_RESEARCH"),
        "exact_missing_fact": (
            "Official issuer evidence for FY/FQ, period_end, and publish_date"
            if action == "EXTERNAL_RESEARCH"
            else "No external fact required for current closure"
        ),
        "notes": "P1 rows excluded from P2/P3 reprioritization" if reclassified == "P1_EXCLUDED" else "",
    }


def priority_score(row: dict[str, Any]) -> tuple[int, str, int, str]:
    score = 0
    if as_int(row.get("is_latest_quarter")) and as_int(row.get("signal_count")) >= 2:
        score += 100
    if as_int(row.get("affects_current_4q_ttm")) and as_int(row.get("signal_count")) >= 2:
        score += 80
    if as_int(row.get("in_latest_8q")) and ("FISCAL" in str(row.get("signals", "")) or "ONE_YEAR" in str(row.get("signals", ""))):
        score += 70
    if as_int(row.get("in_latest_8q")) and ("PUBLISH" in str(row.get("signals", "")) or "PERIOD_END" in str(row.get("signals", ""))):
        score += 60
    if as_int(row.get("is_2024_plus")) and as_int(row.get("signal_count")) >= 2:
        score += 50
    score += min(as_int(row.get("signal_count")), 5)
    rank = as_int(row.get("latest_quarter_rank"), 999999)
    return (-score, str(row.get("ticker", "")), rank, str(row.get("quarter_id", "")))


def queue_row(row: dict[str, Any], rank: int) -> dict[str, Any]:
    return {
        "Priority Rank": rank,
        "Ticker": row.get("ticker", ""),
        "Company ID": row.get("company_id", ""),
        "Fiscal Year": row.get("fiscal_year", ""),
        "Fiscal Q": row.get("fiscal_quarter", ""),
        "Period End": row.get("period_end_date", ""),
        "Publish Date": row.get("publish_date", ""),
        "Original Severity": row.get("original_severity", ""),
        "Reclassified Severity": row.get("reclassified_severity", ""),
        "Issue Type": row.get("issue_type", ""),
        "Signal Count": row.get("signal_count", ""),
        "Signals": row.get("signals", ""),
        "Latest Quarter Rank": row.get("latest_quarter_rank", ""),
        "In Latest 8Q": row.get("in_latest_8q", ""),
        "In Latest 4Q": row.get("in_latest_4q", ""),
        "Is 2024+": row.get("is_2024_plus", ""),
        "Affects Current TTM": row.get("affects_current_4q_ttm", ""),
        "Affects Score": row.get("affects_current_score", ""),
        "Affects Lifecycle": row.get("affects_current_lifecycle", ""),
        "Affects Valuation": row.get("affects_current_valuation", ""),
        "Systemic Pattern": row.get("systemic_recent_pattern", ""),
        "Current Evidence": row.get("current_evidence", ""),
        "Exact Missing Fact": row.get("exact_missing_fact", ""),
        "Recommended Action": row.get("recommended_action", ""),
        "External Research Required": row.get("external_research_required", ""),
        "Notes": row.get("notes", ""),
    }


def issue_month(row: dict[str, Any]) -> str:
    parsed = parse_date(row.get("period_end_date"))
    return f"{parsed.month:02d}" if parsed else ""


def systemic_pattern_rows(classified: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in classified:
        if not as_int(row.get("is_recent_priority")):
            continue
        grouped[(str(row.get("issue_type", "UNKNOWN")), str(row.get("fiscal_quarter", "")), issue_month(row))].append(row)
    out = []
    for (issue, fq, month), items in sorted(grouped.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        out.append(
            {
                "issue_type": issue,
                "fiscal_quarter": fq,
                "period_end_month": month,
                "rows": len(items),
                "companies": len({row.get("ticker") for row in items}),
                "sources_or_providers": "",
                "known_52_53_week_rows": sum(1 for row in items if str(row.get("ticker")) in KNOWN_52_53_WEEK),
                "systemic_recent_pattern": int(any(as_int(row.get("systemic_recent_pattern")) for row in items)),
            }
        )
    return out


def write_human_summaries(root: Path, summary: dict[str, Any], queue: list[dict[str, Any]], pattern_rows: list[dict[str, Any]]) -> None:
    root.joinpath("current_critical_human_summary.md").write_text(
        "\n".join(
            [
                "# Phase 8A10B-P2P3 Current-Critical Queue",
                "",
                f"Classification: `{summary['classification']}`",
                f"Queue rows: `{summary['current_critical']['total_queue_rows']}` / tickers `{summary['current_critical']['unique_tickers']}`",
                f"Latest-4Q rows: `{summary['current_critical']['latest_4q_rows']}`",
                "",
                *(f"- {row['Priority Rank']}: {row['Ticker']} FY{row['Fiscal Year']} {row['Fiscal Q']} {row['Issue Type']} ({row['Recommended Action']})" for row in queue[:50]),
                "",
            ]
        ),
        encoding="utf-8",
    )
    root.joinpath("recent_systemic_pattern_summary.md").write_text(
        "\n".join(
            [
                "# Phase 8A10B-P2P3 Recent Systemic Patterns",
                "",
                "Grouped by issue type, fiscal quarter, and period_end month.",
                "",
                *(f"- {row['issue_type']} / {row['fiscal_quarter']} / month {row['period_end_month']}: {row['rows']} rows, {row['companies']} companies" for row in pattern_rows[:30]),
                "",
            ]
        ),
        encoding="utf-8",
    )


def run_phase8a10b_p2p3_reprioritization(paths: Phase8A10BP2P3Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    v3_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    p1 = read_csv(paths.source_audit_root / "global_P1.csv")
    p2 = read_csv(paths.source_audit_root / "global_P2.csv")
    p3 = read_csv(paths.source_audit_root / "global_P3.csv")
    fiscal = read_csv(paths.source_audit_root / "fiscal_sequence_audit.csv")
    period = read_csv(paths.source_audit_root / "period_end_gap_audit.csv")
    publish = read_csv(paths.source_audit_root / "publish_sequence_audit.csv")
    lag = read_csv(paths.source_audit_root / "reporting_lag_audit.csv")
    cross = read_csv(paths.source_audit_root / "structural_cross_signal_candidates.csv")

    with connect_ro(paths.v3_db) as conn:
        base = baseline(conn, paths.v3_db)
        quarter_rows = load_quarter_rows(conn)
        membership = latest_quarter_membership(quarter_rows)
        downstream = current_downstream_sets(conn)
        after_base = baseline(conn, paths.v3_db)

    p1_qids = {qid for qid in (row_qid(row) for row in p1) if qid is not None}
    signals = signal_index(fiscal, period, publish, lag, cross)
    early_p2 = [
        annotate_row(row, original_severity="P2", membership=membership, downstream=downstream, signals_by_qid=signals, p1_qids=p1_qids, systemic_patterns=set())
        for row in p2
    ]
    early_p3 = [
        annotate_row(row, original_severity="P3", membership=membership, downstream=downstream, signals_by_qid=signals, p1_qids=p1_qids, systemic_patterns=set())
        for row in p3
    ]
    systemic = systemic_recent_patterns(early_p2 + early_p3)
    p2_classified = [enrich_classification({**row, "systemic_recent_pattern": int(row["issue_type"] in systemic)}, reclassify_p2({**row, "systemic_recent_pattern": int(row["issue_type"] in systemic)})) for row in early_p2]
    p3_classified = [enrich_classification({**row, "systemic_recent_pattern": int(row["issue_type"] in systemic)}, reclassify_p3({**row, "systemic_recent_pattern": int(row["issue_type"] in systemic)})) for row in early_p3]

    p2a = [row for row in p2_classified if row["reclassified_severity"] == "P2A_CURRENT_CRITICAL_REVIEW"]
    p2b = [row for row in p2_classified if row["reclassified_severity"] == "P2B_RECENT_NONBLOCKING"]
    p2c = [row for row in p2_classified if row["reclassified_severity"] == "P2C_HISTORICAL_DEFERRED"]
    p3a = [row for row in p3_classified if row["reclassified_severity"] == "P3A_RECENT_INFORMATIONAL"]
    p3b = [row for row in p3_classified if row["reclassified_severity"] == "P3B_HISTORICAL_INFORMATIONAL"]
    p3e = [row for row in p3_classified if row["reclassified_severity"] == "P3_ESCALATED"]
    critical = sorted(p2a + p3e, key=priority_score)
    queue = [queue_row(row, idx) for idx, row in enumerate(critical, 1)]
    deferred = [
        row
        for row in p2c + p3b
        if not as_int(row.get("is_2024_plus"))
        and not as_int(row.get("in_latest_8q"))
        and not any(as_int(row.get(field)) for field in ("affects_current_4q_ttm", "affects_current_score", "affects_current_lifecycle", "affects_current_valuation"))
        and not as_int(row.get("systemic_recent_pattern"))
    ]
    pattern_rows = systemic_pattern_rows(p2_classified + p3_classified)

    membership_rows = sorted(membership.values(), key=lambda r: (str(r["ticker"]), int(r["latest_quarter_rank"])))
    write_json(paths.artifact_root / "p2p3_reprioritization_baseline.json", base)
    write_csv(paths.artifact_root / "company_latest8q_membership.csv", membership_rows)
    write_csv(paths.artifact_root / "P2_reclassified.csv", p2_classified)
    write_csv(paths.artifact_root / "P2A_current_critical.csv", p2a)
    write_csv(paths.artifact_root / "P2B_recent_nonblocking.csv", p2b)
    write_csv(paths.artifact_root / "P2C_historical_deferred.csv", p2c)
    write_csv(paths.artifact_root / "P3_reclassified.csv", p3_classified)
    write_csv(paths.artifact_root / "P3A_recent_informational.csv", p3a)
    write_csv(paths.artifact_root / "P3B_historical_informational.csv", p3b)
    write_csv(paths.artifact_root / "P3_escalated.csv", p3e)
    write_csv(paths.artifact_root / "current_critical_2024plus_last8q_queue.csv", queue, fieldnames=QUEUE_COLUMNS)
    write_csv(paths.artifact_root / "recent_systemic_pattern_analysis.csv", pattern_rows)
    write_csv(paths.artifact_root / "historical_deferred_pre2024_noncritical.csv", deferred)

    companies_with_quarters = len({int(row["company_id"]) for row in quarter_rows})
    companies_with_ge8 = len({int(row["company_id"]) for row in quarter_rows if as_int(membership[int(row["quarter_id"])]["company_quarter_count"]) >= 8})
    companies_with_lt8 = companies_with_quarters - companies_with_ge8
    latest8_qids = {qid for qid, row in membership.items() if as_int(row["in_latest_8q"])}
    latest4_qids = {qid for qid, row in membership.items() if as_int(row["in_latest_4q"])}
    critical_qids = {qid for qid in (row_qid(row) for row in critical) if qid is not None}
    companies_with_clean_latest8 = companies_with_quarters - len({as_int(row.get("company_id")) for row in critical if row_qid(row) in latest8_qids})
    action_counts = Counter(row["recommended_action"] for row in critical)
    classification = CLASSIFICATION_CURRENT_CRITICAL if critical else CLASSIFICATION_NO_CURRENT_CRITICAL
    next_action = (
        "RESOLVE CURRENT-CRITICAL 2024+ / LAST-8Q P2A/P3_ESCALATED"
        if critical
        else "WAIT FOR GLOBAL P1 EXTERNAL RESEARCH, THEN FINAL CANONICAL CLOSURE"
    )
    safety = {
        "production_writes": int(file_state(paths.v3_db) != v3_before),
        "rawcandle_writes": int(file_state(paths.rawcandle_db) != raw_before),
        "ttm_writes": 0,
        "score_writes": 0,
        "lifecycle_writes": 0,
        "valuation_writes": 0,
        "baseline_unchanged": int(base == after_base),
    }
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "source_audit_root": str(paths.source_audit_root),
        "starting_population": {
            "P2_rows": len(p2),
            "P2_companies": len({row.get("ticker") for row in p2}),
            "P3_rows": len(p3),
            "P3_companies": len({row.get("ticker") for row in p3}),
            "P1_rows_excluded": len(p1),
        },
        "recent_windows": {
            "retained_companies": base["companies"],
            "companies_with_quarters": companies_with_quarters,
            "companies_with_ge8_quarters": companies_with_ge8,
            "companies_with_lt8_quarters": companies_with_lt8,
            "quarters_in_latest8q_windows": len(latest8_qids),
            "quarters_in_latest4q_windows": len(latest4_qids),
            "canonical_2024plus_quarters": sum(1 for row in membership.values() if is_2024_plus(row)),
            "companies_latest8_clean_of_current_critical": companies_with_clean_latest8,
            "companies_with_latest8_current_critical": len({as_int(row.get("company_id")) for row in critical if row_qid(row) in latest8_qids}),
            "latest4_findings": sum(1 for row in critical if row_qid(row) in latest4_qids),
            "rank5_to_8_findings": sum(1 for row in critical if 5 <= as_int(row.get("latest_quarter_rank"), 999999) <= 8),
        },
        "p2_reprioritization": {
            "P2A_rows": len(p2a),
            "P2A_companies": len({row.get("ticker") for row in p2a}),
            "P2B_rows": len(p2b),
            "P2B_companies": len({row.get("ticker") for row in p2b}),
            "P2C_rows": len(p2c),
            "P2C_companies": len({row.get("ticker") for row in p2c}),
        },
        "p3_reprioritization": {
            "P3A_rows": len(p3a),
            "P3A_companies": len({row.get("ticker") for row in p3a}),
            "P3B_rows": len(p3b),
            "P3B_companies": len({row.get("ticker") for row in p3b}),
            "P3_ESCALATED_rows": len(p3e),
            "P3_ESCALATED_companies": len({row.get("ticker") for row in p3e}),
        },
        "current_critical": {
            "total_queue_rows": len(critical),
            "unique_tickers": len({row.get("ticker") for row in critical}),
            "latest_quarter_rows": sum(as_int(row.get("is_latest_quarter")) for row in critical),
            "latest_4q_rows": sum(as_int(row.get("in_latest_4q")) for row in critical),
            "latest_8q_rows": sum(as_int(row.get("in_latest_8q")) for row in critical),
            "2024plus_rows": sum(as_int(row.get("is_2024_plus")) for row in critical),
            "affects_current_ttm": sum(as_int(row.get("affects_current_4q_ttm")) for row in critical),
            "affects_score": sum(as_int(row.get("affects_current_score")) for row in critical),
            "affects_lifecycle": sum(as_int(row.get("affects_current_lifecycle")) for row in critical),
            "affects_valuation": sum(as_int(row.get("affects_current_valuation")) for row in critical),
        },
        "signal_structure": {
            "single_signal_current_critical": sum(1 for row in critical if as_int(row.get("signal_count")) == 1),
            "multi_signal_current_critical": sum(1 for row in critical if as_int(row.get("signal_count")) >= 2),
            "one_year_shift_candidates": sum(1 for row in critical if "ONE_YEAR" in str(row.get("signals", ""))),
            "duplicate_economic_quarter_candidates": sum(1 for row in critical if "DUPLICATE_ECONOMIC_QUARTER" in str(row.get("signals", ""))),
            "publish_period_contradictions": sum(1 for row in critical if "PUBLISH" in str(row.get("signals", "")) and "PERIOD_END" in str(row.get("signals", ""))),
            "systemic_recent_patterns": len(systemic),
        },
        "action": dict(action_counts),
        "historical_deferred": {
            "deferred_pre2024_rows": len(deferred),
            "deferred_companies": len({row.get("ticker") for row in deferred}),
            "deferred_rows_affecting_current_downstream": sum(
                1
                for row in deferred
                if any(as_int(row.get(field)) for field in ("affects_current_4q_ttm", "affects_current_score", "affects_current_lifecycle", "affects_current_valuation"))
            ),
        },
        "safety": safety,
        "artifacts": {
            "current_critical_queue": str(paths.artifact_root / "current_critical_2024plus_last8q_queue.csv"),
            "P2A": str(paths.artifact_root / "P2A_current_critical.csv"),
            "P2B": str(paths.artifact_root / "P2B_recent_nonblocking.csv"),
            "P2C": str(paths.artifact_root / "P2C_historical_deferred.csv"),
            "P3_escalated": str(paths.artifact_root / "P3_escalated.csv"),
            "systemic_pattern": str(paths.artifact_root / "recent_systemic_pattern_analysis.csv"),
            "historical_deferred": str(paths.artifact_root / "historical_deferred_pre2024_noncritical.csv"),
        },
        "next_action": next_action,
    }
    write_json(paths.artifact_root / "current_critical_summary.json", summary["current_critical"])
    write_json(paths.artifact_root / "phase8a10b_p2p3_reprioritization_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(next_action + "\n", encoding="utf-8")
    write_human_summaries(paths.artifact_root, summary, queue, pattern_rows)
    if safety["production_writes"] or safety["rawcandle_writes"] or not safety["baseline_unchanged"]:
        raise RuntimeError("read-only safety guard failed")
    return summary
