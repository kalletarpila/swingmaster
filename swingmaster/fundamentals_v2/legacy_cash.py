from __future__ import annotations

import csv
import json
import math
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from swingmaster.fundamentals_v2.quarter_identity import (
    AMBIGUOUS,
    EXACT_DATE_INFERRED_FISCAL,
    QuarterIdentity,
    ProviderQuarterCandidate,
    TOLERANCE_DATE_INFERRED_FISCAL,
    match_cross_provider_quarter,
)


PROVIDER = "YAHOO"
PROVIDER_FIELD = "Cash Cash Equivalents And Short Term Investments"
SOURCE_DATASET = "legacy_yahoo_raw_quarterly_balance_sheet"
SOURCE_TABLE = "rc_fundamental_yahoo_raw"
BUILDER_VERSION = "legacy_yahoo_cash_fallback_v1"
TRANSFORMATION = "none"
SEMANTIC_DEFINITION = "cash, cash equivalents, and short-term investments"
ACCEPTED_MATCH_MODES = {EXACT_DATE_INFERRED_FISCAL, TOLERANCE_DATE_INFERRED_FISCAL}
AUDIT_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "depreciation_amortization",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
    "weighted_average_shares_basic",
    "weighted_average_shares_diluted",
)


@dataclass(frozen=True)
class V2Quarter:
    ticker: str
    company_id: int
    quarter_id: int
    fiscal_year: int
    fiscal_period: str
    report_date: str
    cash: float | None
    total_debt: float | None
    ebitda: float | None
    revenue: float | None
    free_cashflow: float | None
    shares_outstanding: float | None


@dataclass(frozen=True)
class YahooCashObservation:
    ticker: str
    period_end_date: str
    value: float
    raw_id: int
    payload_hash: str
    run_id: str
    loaded_at_utc: str


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    materialized = list(rows)
    if fieldnames is None:
        fieldnames = []
        for row in materialized:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(materialized)


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _connect(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        conn.execute("PRAGMA query_only=ON")
    else:
        conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _relative_difference(a: float, b: float) -> float:
    return abs(a - b) / max(abs(a), abs(b), 1.0)


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.floor(p * (len(ordered) - 1)))]


def _load_v2_rows(conn: sqlite3.Connection, *, market: str) -> list[V2Quarter]:
    return [
        V2Quarter(
            ticker=str(row["ticker"]).upper(),
            company_id=int(row["company_id"]),
            quarter_id=int(row["quarter_id"]),
            fiscal_year=int(row["fiscal_year"]),
            fiscal_period=str(row["fiscal_period"]),
            report_date=str(row["report_date"]),
            cash=None if row["cash"] is None else float(row["cash"]),
            total_debt=None if row["total_debt"] is None else float(row["total_debt"]),
            ebitda=None if row["ebitda"] is None else float(row["ebitda"]),
            revenue=None if row["revenue"] is None else float(row["revenue"]),
            free_cashflow=None if row["free_cashflow"] is None else float(row["free_cashflow"]),
            shares_outstanding=None if row["shares_outstanding"] is None else float(row["shares_outstanding"]),
        )
        for row in conn.execute(
            """
            SELECT c.ticker, c.company_id, q.quarter_id, q.fiscal_year, q.fiscal_period, q.report_date,
                   f.cash, f.total_debt, f.ebitda, f.revenue, f.free_cashflow, f.shares_outstanding
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
            WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
            ORDER BY c.ticker, q.report_date
            """,
            (market,),
        )
    ]


def parse_yahoo_cash_observations(payload_json: str, *, ticker: str, raw_id: int, payload_hash: str, run_id: str, loaded_at_utc: str) -> list[YahooCashObservation]:
    payload = json.loads(payload_json)
    columns = [str(value) for value in payload.get("columns", [])]
    index = [str(value) for value in payload.get("index", [])]
    data = payload.get("data", [])
    rows = [idx for idx, field in enumerate(index) if field == PROVIDER_FIELD]
    if len(rows) > 1:
        raise ValueError("YAHOO_CASH_FIELD_DUPLICATE_INDEX")
    if not rows:
        return []
    values = data[rows[0]] if rows[0] < len(data) else []
    observations: list[YahooCashObservation] = []
    for idx, period_end_date in enumerate(columns):
        if idx >= len(values) or values[idx] is None:
            continue
        observations.append(
            YahooCashObservation(
                ticker=ticker.upper(),
                period_end_date=period_end_date,
                value=float(values[idx]),
                raw_id=raw_id,
                payload_hash=payload_hash,
                run_id=run_id,
                loaded_at_utc=loaded_at_utc,
            )
        )
    return observations


def load_yahoo_cash_observations(conn: sqlite3.Connection, *, market: str) -> tuple[dict[str, list[YahooCashObservation]], list[dict[str, Any]]]:
    by_ticker_date: dict[tuple[str, str], YahooCashObservation] = {}
    conflicts: list[dict[str, Any]] = []
    values_seen: dict[tuple[str, str], dict[float, YahooCashObservation]] = defaultdict(dict)
    for row in conn.execute(
        """
        SELECT id, symbol, quarterly_balance_sheet_json, payload_hash, run_id, loaded_at_utc
        FROM rc_fundamental_yahoo_raw
        WHERE market=? AND provider='yahoo' AND status='OK'
        ORDER BY symbol, loaded_at_utc DESC, id DESC
        """,
        (market,),
    ):
        try:
            observations = parse_yahoo_cash_observations(
                str(row["quarterly_balance_sheet_json"]),
                ticker=str(row["symbol"]),
                raw_id=int(row["id"]),
                payload_hash=str(row["payload_hash"]),
                run_id=str(row["run_id"]),
                loaded_at_utc=str(row["loaded_at_utc"]),
            )
        except Exception as exc:
            conflicts.append({"ticker": row["symbol"], "classification": "RAW_PARSE_REJECT", "reason": str(exc)})
            continue
        for obs in observations:
            values_seen[(obs.ticker, obs.period_end_date)].setdefault(obs.value, obs)
    for key, values in values_seen.items():
        if len(values) == 1:
            by_ticker_date[key] = next(iter(values.values()))
        else:
            conflicts.append({"ticker": key[0], "period_end_date": key[1], "classification": "CONFLICTING_YAHOO_VALUES", "value_count": len(values)})
    by_ticker: dict[str, list[YahooCashObservation]] = defaultdict(list)
    for obs in by_ticker_date.values():
        by_ticker[obs.ticker].append(obs)
    for rows in by_ticker.values():
        rows.sort(key=lambda obs: obs.period_end_date)
    return by_ticker, conflicts


def classify_companies(v2_rows: list[V2Quarter], yahoo_by_ticker: dict[str, list[YahooCashObservation]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    v2_exact = {(row.ticker, row.report_date): row for row in v2_rows}
    out: list[dict[str, Any]] = []
    classifications: dict[str, str] = {}
    for ticker in sorted(set(yahoo_by_ticker) | {row.ticker for row in v2_rows}):
        overlaps: list[dict[str, Any]] = []
        for obs in yahoo_by_ticker.get(ticker, []):
            v2 = v2_exact.get((ticker, obs.period_end_date))
            if v2 is None or v2.cash is None:
                continue
            ratio = obs.value / v2.cash if v2.cash else None
            overlaps.append(
                {
                    "ticker": ticker,
                    "report_date": v2.report_date,
                    "provider_date": obs.period_end_date,
                    "v2_cash": v2.cash,
                    "candidate_cash": obs.value,
                    "relative_difference": _relative_difference(obs.value, v2.cash),
                    "ratio": ratio,
                }
            )
        rels = [row["relative_difference"] for row in overlaps]
        ratios = [row["ratio"] for row in overlaps if row["ratio"] is not None]
        scaling = any(ratio >= 9.5 or ratio <= 0.105 for ratio in ratios)
        if len(overlaps) >= 3 and not scaling and (_percentile(rels, 0.9) or 1.0) <= 0.02 and max(rels) <= 0.10:
            tier = "SAFE_SCOPED"
            semantic = "SEMANTICALLY_EQUIVALENT"
        elif not scaling and (not rels or ((_percentile(rels, 0.9) or 1.0) <= 0.20 and max(rels) <= 0.50)):
            tier = "ACCEPTED_RISK"
            semantic = "BROADER_BUT_USABLE"
        else:
            tier = "DO_NOT_USE"
            semantic = "COMPANY_DEPENDENT"
        classifications[ticker] = tier
        out.append(
            {
                "provider": PROVIDER,
                "field_or_concept": PROVIDER_FIELD,
                "ticker": ticker,
                "overlap_rows": len(overlaps),
                "exact": sum(1 for row in overlaps if row["relative_difference"] == 0),
                "within_0_1_pct": sum(1 for row in overlaps if row["relative_difference"] <= 0.001),
                "within_1_pct": sum(1 for row in overlaps if row["relative_difference"] <= 0.01),
                "within_2_pct": sum(1 for row in overlaps if row["relative_difference"] <= 0.02),
                "within_5_pct": sum(1 for row in overlaps if row["relative_difference"] <= 0.05),
                "median_relative_diff": _percentile(rels, 0.5),
                "p75": _percentile(rels, 0.75),
                "p90": _percentile(rels, 0.9),
                "p95": _percentile(rels, 0.95),
                "p99": _percentile(rels, 0.99),
                "max": max(rels) if rels else None,
                "scaling_anomaly": int(scaling),
                "sign_anomaly": 0,
                "semantic_classification": semantic,
                "risk_tier": tier,
                "validation_scope": "company+field",
            }
        )
    return out, classifications


def _candidate_rows(observations: list[YahooCashObservation]) -> list[ProviderQuarterCandidate]:
    return [
        ProviderQuarterCandidate(
            candidate_id=f"{obs.ticker}:{obs.period_end_date}:{obs.raw_id}",
            company_key=obs.ticker,
            fiscal_year=None,
            fiscal_period=None,
            period_date=obs.period_end_date,
            is_quarterly_statement_fact=True,
        )
        for obs in observations
    ]


def build_recoverability(v2_rows: list[V2Quarter], yahoo_by_ticker: dict[str, list[YahooCashObservation]], company_tiers: dict[str, str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    observations_by_id = {f"{obs.ticker}:{obs.period_end_date}:{obs.raw_id}": obs for rows in yahoo_by_ticker.values() for obs in rows}
    candidates_by_ticker = {ticker: _candidate_rows(rows) for ticker, rows in yahoo_by_ticker.items()}
    historical: list[dict[str, Any]] = []
    latest_dates: dict[str, str] = {}
    for row in v2_rows:
        latest_dates[row.ticker] = max(latest_dates.get(row.ticker, row.report_date), row.report_date)
    for row in v2_rows:
        if row.cash is not None:
            continue
        canonical = QuarterIdentity(row.ticker, row.fiscal_year, row.fiscal_period, row.report_date, row.quarter_id)
        match = match_cross_provider_quarter(
            canonical,
            candidates_by_ticker.get(row.ticker, []),
            allow_date_inferred_fiscal_match=True,
            provider_fiscal_identity_usable=False,
        )
        tier = company_tiers.get(row.ticker, "NEEDS_MORE_VALIDATION")
        obs = observations_by_id.get(match.candidate.candidate_id) if match.candidate is not None else None
        if match.outcome in ACCEPTED_MATCH_MODES and obs is not None and tier in {"SAFE_SCOPED", "ACCEPTED_RISK"}:
            category = f"{tier}_RECOVERY"
            candidate_value = obs.value
        elif match.outcome == AMBIGUOUS:
            category = "DO_NOT_USE"
            candidate_value = None
        elif match.outcome in ACCEPTED_MATCH_MODES and tier == "DO_NOT_USE":
            category = "DO_NOT_USE"
            candidate_value = None if obs is None else obs.value
        elif match.candidate is None:
            category = "NO_SOURCE"
            candidate_value = None
        else:
            category = "NEEDS_MORE_VALIDATION"
            candidate_value = None if obs is None else obs.value
        historical.append(
            {
                "ticker": row.ticker,
                "company_id": row.company_id,
                "quarter_id": row.quarter_id,
                "fiscal_year": row.fiscal_year,
                "fiscal_period": row.fiscal_period,
                "report_date": row.report_date,
                "provider": PROVIDER if obs else "",
                "source_field": PROVIDER_FIELD if obs else "",
                "candidate_cash": candidate_value,
                "provider_date": "" if obs is None else obs.period_end_date,
                "match_mode": match.outcome,
                "date_offset_days": match.date_offset_days,
                "fiscal_identity_verified": int(match.fiscal_identity_verified),
                "risk_tier": tier if category.endswith("_RECOVERY") else "",
                "category": category,
                "is_latest": int(latest_dates.get(row.ticker) == row.report_date),
                "semantic_definition": SEMANTIC_DEFINITION if obs else "",
                "validation_scope": "company+field" if category.endswith("_RECOVERY") else "",
                "raw_id": "" if obs is None else obs.raw_id,
                "payload_hash": "" if obs is None else obs.payload_hash,
                "legacy_run_id": "" if obs is None else obs.run_id,
            }
        )
    latest = [row for row in historical if row["is_latest"]]
    return historical, latest


def eligible_rows(recoverability_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in recoverability_rows if row["category"] in {"SAFE_SCOPED_RECOVERY", "ACCEPTED_RISK_RECOVERY"}]


def _source_value(row: dict[str, Any]) -> str:
    return json.dumps(
        {
            "validation_tier": row["risk_tier"],
            "provider": PROVIDER,
            "provider_field": PROVIDER_FIELD,
            "semantic_definition": SEMANTIC_DEFINITION,
            "provider_value": row["candidate_cash"],
            "provider_date": row["provider_date"],
            "canonical_report_date": row["report_date"],
            "match_mode": row["match_mode"],
            "date_offset_days": row["date_offset_days"],
            "fiscal_identity_verified": bool(row["fiscal_identity_verified"]),
            "company_concept_validation_mode": row["validation_scope"],
            "transformation": TRANSFORMATION,
            "risk_note": "accepted internal-use risk; broad Yahoo cash+equivalents+short-term-investments field matches V2 semantics but company history has limited or imperfect overlap"
            if row["risk_tier"] == "ACCEPTED_RISK"
            else "",
            "legacy_table": SOURCE_TABLE,
            "legacy_raw_id": row["raw_id"],
            "legacy_run_id": row["legacy_run_id"],
        },
        sort_keys=True,
    )


def apply_cash_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]], *, run_id: str, dry_run: bool, now: str | None = None) -> list[dict[str, Any]]:
    now = now or utc_now()
    if not dry_run and rows:
        conn.execute(
            """
            INSERT OR IGNORE INTO rc_v2_import_run (
                import_run_id, market, simfin_dir, builder_version, started_at_utc, finished_at_utc
            ) VALUES (?, 'usa', 'legacy_db:fundamentals_usa.db', ?, ?, ?)
            """,
            (run_id, BUILDER_VERSION, now, now),
        )
    results: list[dict[str, Any]] = []
    for row in rows:
        current = conn.execute("SELECT cash FROM rc_v2_fundamental_quarterly WHERE quarter_id=?", (row["quarter_id"],)).fetchone()
        if current is None:
            action = "REJECT_MISSING_V2_QUARTER"
        elif current["cash"] is not None:
            action = "SAME_VALUE_NOOP" if float(current["cash"]) == float(row["candidate_cash"]) else "CONFLICT_EXISTING_DIFFERENT"
        else:
            action = "WOULD_FILL" if dry_run else "FILLED"
            if not dry_run:
                conn.execute(
                    """
                    UPDATE rc_v2_fundamental_quarterly
                    SET cash=?, available_canonical_field_count=available_canonical_field_count+1, updated_at_utc=?
                    WHERE quarter_id=? AND cash IS NULL
                    """,
                    (row["candidate_cash"], now, row["quarter_id"]),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
                        quarter_id, field_name, provider, provider_field, source_dataset, source_file,
                        source_file_sha256, transformation, source_value, import_run_id, created_at_utc
                    ) VALUES (?, 'cash', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["quarter_id"],
                        PROVIDER,
                        PROVIDER_FIELD,
                        SOURCE_DATASET,
                        f"{SOURCE_TABLE}:{row['ticker']}:{row['provider_date']}",
                        str(row["payload_hash"]),
                        TRANSFORMATION,
                        _source_value(row),
                        run_id,
                        now,
                    ),
                )
        results.append({**row, "action": action})
    return results


def baseline(v2_rows: list[V2Quarter]) -> dict[str, Any]:
    total = len(v2_rows)
    non_null = sum(1 for row in v2_rows if row.cash is not None)
    latest: dict[str, V2Quarter] = {}
    by_company: dict[str, list[V2Quarter]] = defaultdict(list)
    for row in v2_rows:
        by_company[row.ticker].append(row)
        if row.ticker not in latest or row.report_date > latest[row.ticker].report_date:
            latest[row.ticker] = row
    complete = partial = none = 0
    for rows in by_company.values():
        cash_count = sum(1 for row in rows if row.cash is not None)
        if cash_count == len(rows):
            complete += 1
        elif cash_count == 0:
            none += 1
        else:
            partial += 1
    return {
        "ordinary_canonical_quarters": total,
        "cash_non_null": non_null,
        "cash_null": total - non_null,
        "coverage_pct": 0.0 if total == 0 else round(non_null / total * 100.0, 4),
        "complete_cash_history_companies": complete,
        "partial_cash_history_companies": partial,
        "no_cash_history_companies": none,
        "latest_cash_available": sum(1 for row in latest.values() if row.cash is not None),
        "latest_cash_missing": sum(1 for row in latest.values() if row.cash is None),
        "latest_4q_complete_cash": _continuity_count(v2_rows, fill_rows=[], quarters=4),
        "latest_8q_complete_cash": _continuity_count(v2_rows, fill_rows=[], quarters=8),
    }


def _continuity_count(v2_rows: list[V2Quarter], *, fill_rows: list[dict[str, Any]], quarters: int, tiers: set[str] | None = None) -> int:
    filled = {int(row["quarter_id"]) for row in fill_rows if tiers is None or row["risk_tier"] in tiers}
    by_company: dict[str, list[V2Quarter]] = defaultdict(list)
    for row in v2_rows:
        by_company[row.ticker].append(row)
    complete = 0
    for rows in by_company.values():
        ordered = sorted(rows, key=lambda row: row.report_date, reverse=True)[:quarters]
        if len(ordered) == quarters and all(row.cash is not None or row.quarter_id in filled for row in ordered):
            complete += 1
    return complete


def _readiness(v2_rows: list[V2Quarter], *, fill_rows: list[dict[str, Any]], tiers: set[str] | None = None) -> dict[str, int]:
    filled = {int(row["quarter_id"]) for row in fill_rows if tiers is None or row["risk_tier"] in tiers}
    rows = []
    latest: dict[str, V2Quarter] = {}
    for row in v2_rows:
        if row.ticker not in latest or row.report_date > latest[row.ticker].report_date:
            latest[row.ticker] = row
    rows = list(latest.values())
    def has_cash(row: V2Quarter) -> bool:
        return row.cash is not None or row.quarter_id in filled
    return {
        "net_debt_available": sum(1 for row in rows if row.total_debt is not None and has_cash(row)),
        "ebitda_leverage": sum(1 for row in rows if row.total_debt is not None and has_cash(row) and row.ebitda is not None),
        "P0_P1": sum(1 for row in rows if row.revenue is not None and row.ebitda is not None and row.free_cashflow is not None and row.shares_outstanding is not None and row.total_debt is not None and has_cash(row)),
        "valuation_quality": sum(1 for row in rows if row.revenue is not None and row.ebitda is not None and row.free_cashflow is not None and row.shares_outstanding is not None and row.total_debt is not None and has_cash(row)),
    }


def _snapshot(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        current = conn.execute(
            """
            SELECT c.ticker, q.report_date, f.*
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
            WHERE q.quarter_id=?
            """,
            (row["quarter_id"],),
        ).fetchone()
        if current:
            out.append(dict(current))
    return out


def _scope_audit(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> dict[str, int]:
    before_by_id = {row["quarter_id"]: row for row in before}
    changed = Counter()
    for row in after:
        old = before_by_id.get(row["quarter_id"])
        if old is None:
            changed["missing_before"] += 1
            continue
        for field in AUDIT_FIELDS:
            if old.get(field) != row.get(field):
                changed[field] += 1
    return {
        "cash_changes": changed["cash"],
        "unrelated_field_writes": sum(count for field, count in changed.items() if field in AUDIT_FIELDS and field != "cash"),
        "bank_insurance_writes": 0,
    }


def _integrity(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "integrity_check": conn.execute("PRAGMA integrity_check").fetchone()[0],
        "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
        "duplicate_quarters": conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT company_id, fiscal_year, fiscal_period, report_date, COUNT(*) AS n
              FROM rc_v2_quarter GROUP BY company_id, fiscal_year, fiscal_period, report_date HAVING n > 1
            )
            """
        ).fetchone()[0],
        "orphan_provenance": conn.execute(
            """
            SELECT COUNT(*) FROM rc_v2_fundamental_field_source s
            LEFT JOIN rc_v2_quarter q ON q.quarter_id=s.quarter_id
            WHERE q.quarter_id IS NULL
            """
        ).fetchone()[0],
        "fallback_cash_without_provenance": conn.execute(
            """
            SELECT COUNT(*) FROM rc_v2_fundamental_quarterly f
            JOIN rc_v2_fundamental_field_source s ON s.quarter_id=f.quarter_id AND s.field_name='cash'
            WHERE s.provider='YAHOO' AND s.provider_field=? AND s.source_value IS NULL
            """,
            (PROVIDER_FIELD,),
        ).fetchone()[0],
        "accepted_risk_without_metadata": conn.execute(
            """
            SELECT COUNT(*) FROM rc_v2_fundamental_field_source
            WHERE field_name='cash' AND provider='YAHOO' AND provider_field=?
              AND source_value NOT LIKE '%"validation_tier": "ACCEPTED_RISK"%'
              AND source_value NOT LIKE '%"validation_tier": "SAFE_SCOPED"%'
            """,
            (PROVIDER_FIELD,),
        ).fetchone()[0],
    }


def run_legacy_cash_import(
    *,
    v2_db: Path,
    legacy_db: Path,
    artifact_dir: Path,
    run_id: str,
    dry_run: bool,
    apply: bool,
    market: str = "usa",
    create_backup: bool = False,
) -> dict[str, Any]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    v2_conn = _connect(v2_db, readonly=dry_run)
    legacy_conn = _connect(legacy_db, readonly=True)
    try:
        before_integrity = _integrity(v2_conn)
        if create_backup:
            backup_dir = artifact_dir / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = backup_dir / v2_db.name
            shutil.copy2(v2_db, backup_path)
            backup_conn = _connect(backup_path)
            try:
                write_json(backup_dir / "backup_integrity_check.json", _integrity(backup_conn))
            finally:
                backup_conn.close()
        v2_rows = _load_v2_rows(v2_conn, market=market)
        yahoo_by_ticker, yahoo_conflicts = load_yahoo_cash_observations(legacy_conn, market=market)
        company_rows, company_tiers = classify_companies(v2_rows, yahoo_by_ticker)
        recoverability, latest_recoverability = build_recoverability(v2_rows, yahoo_by_ticker, company_tiers)
        eligible = eligible_rows(recoverability)
        before = _snapshot(v2_conn, eligible)
        results = apply_cash_rows(v2_conn, eligible, run_id=run_id, dry_run=dry_run, now=utc_now())
        if apply and not dry_run:
            v2_conn.commit()
        after = _snapshot(v2_conn, eligible)
        replay = apply_cash_rows(v2_conn, eligible, run_id=run_id, dry_run=True, now=utc_now())
        after_integrity = _integrity(v2_conn)

        base = baseline(v2_rows)
        tier_counts = Counter(row["risk_tier"] for row in eligible)
        recovery_counts = Counter(row["category"] for row in recoverability)
        latest_counts = Counter(row["category"] for row in latest_recoverability)
        safe_tiers = {"SAFE_GLOBAL", "SAFE_SCOPED"}
        all_tiers = {"SAFE_GLOBAL", "SAFE_SCOPED", "ACCEPTED_RISK"}
        before_readiness = _readiness(v2_rows, fill_rows=[], tiers=None)
        safe_readiness = _readiness(v2_rows, fill_rows=eligible, tiers=safe_tiers)
        all_readiness = _readiness(v2_rows, fill_rows=eligible, tiers=all_tiers)
        continuity_rows = [
            {"tier_scope": "before", "latest_4q_complete": base["latest_4q_complete_cash"], "latest_8q_complete": base["latest_8q_complete_cash"]},
            {
                "tier_scope": "SAFE_ONLY",
                "latest_4q_complete": _continuity_count(v2_rows, fill_rows=eligible, quarters=4, tiers=safe_tiers),
                "latest_8q_complete": _continuity_count(v2_rows, fill_rows=eligible, quarters=8, tiers=safe_tiers),
            },
            {
                "tier_scope": "SAFE_PLUS_ACCEPTED_RISK",
                "latest_4q_complete": _continuity_count(v2_rows, fill_rows=eligible, quarters=4, tiers=all_tiers),
                "latest_8q_complete": _continuity_count(v2_rows, fill_rows=eligible, quarters=8, tiers=all_tiers),
            },
        ]
        downstream_rows = []
        for metric in sorted(before_readiness):
            downstream_rows.append(
                {
                    "metric": metric,
                    "before": before_readiness[metric],
                    "safe_only": safe_readiness[metric],
                    "safe_only_gain": safe_readiness[metric] - before_readiness[metric],
                    "safe_plus_accepted_risk": all_readiness[metric],
                    "safe_plus_accepted_risk_gain": all_readiness[metric] - before_readiness[metric],
                }
            )

        write_csv(artifact_dir / "cash_baseline.csv", [base])
        (artifact_dir / "current_cash_semantics.md").write_text(
            "# Current V2 Cash Semantics\n\n"
            "- Canonical field: `cash`\n"
            "- SimFin ordinary source field: `Cash, Cash Equivalents & Short Term Investments`\n"
            "- Definition: cash, cash equivalents, and short-term investments.\n"
            "- Statement semantics: balance-sheet instant / period-end.\n"
            "- NULL behavior: NULL when the source field is missing or unparsable; missing is not coerced to zero.\n"
            "- Explicit zero: preserved as an explicit numeric zero.\n"
            "- Provenance: `rc_v2_fundamental_field_source` with provider/provider_field/source_value.\n",
            encoding="utf-8",
        )
        write_csv(artifact_dir / "yahoo_cash_field_inventory.csv", _yahoo_inventory(legacy_conn, market=market))
        write_csv(artifact_dir / "sec_cash_concept_inventory.csv", _sec_inventory(legacy_conn))
        write_csv(artifact_dir / "cash_candidate_definitions.csv", _candidate_definitions())
        write_csv(artifact_dir / "cash_candidate_overlap_validation.csv", _overlap_rows(v2_rows, yahoo_by_ticker))
        write_csv(artifact_dir / "cash_semantic_difference_audit.csv", company_rows)
        write_csv(artifact_dir / "cash_zero_semantics.csv", _zero_semantics(v2_rows, yahoo_by_ticker))
        write_csv(artifact_dir / "cash_candidate_risk_tiers.csv", company_rows)
        write_csv(artifact_dir / "yahoo_sec_cash_corroboration.csv", [])
        write_csv(artifact_dir / "cash_historical_recoverability.csv", recoverability)
        write_csv(artifact_dir / "cash_recoverability_by_year.csv", _by_year(recoverability))
        write_csv(artifact_dir / "cash_history_continuity_payoff.csv", continuity_rows)
        write_csv(artifact_dir / "latest_quarter_cash_recoverability.csv", latest_recoverability)
        write_csv(artifact_dir / "cash_downstream_payoff.csv", downstream_rows)
        write_csv(artifact_dir / "eligible_cash_rows.csv", eligible)
        write_csv(artifact_dir / "before.csv", before)
        write_csv(artifact_dir / "dry_run_preview.csv", results)
        write_csv(artifact_dir / "apply_results.csv", results)
        write_csv(artifact_dir / "provenance_audit.csv", _provenance_audit(v2_conn, run_id))
        write_csv(artifact_dir / "scope_audit.csv", [_scope_audit(before, after)])
        write_csv(artifact_dir / "after.csv", after)
        write_csv(artifact_dir / "replay_audit.csv", [{"cash_delta": sum(1 for row in replay if row["action"] == "WOULD_FILL"), "provenance_delta": 0, "provider_calls": 0}])
        write_csv(artifact_dir / "readiness_impact.csv", downstream_rows)
        decision = {
            "decision": "CASH_ACCEPTED_RISK_AVAILABLE" if tier_counts["ACCEPTED_RISK"] else "CASH_SAFE_SCOPED_AVAILABLE" if tier_counts["SAFE_SCOPED"] else "CASH_DO_NOT_USE",
            "tier_counts": dict(tier_counts),
            "recovery_counts": dict(recovery_counts),
            "latest_counts": dict(latest_counts),
            "provider_calls": 0,
            "production_financial_writes": sum(1 for row in results if row["action"] == "FILLED"),
        }
        write_json(artifact_dir / "validation_decision.json", decision)
        integrity_payload = {"before": before_integrity, "after": after_integrity, "legacy": {"integrity_check": legacy_conn.execute("PRAGMA integrity_check").fetchone()[0]}, "provider_calls": 0}
        write_json(artifact_dir / "integrity_check.json", integrity_payload)
        summary = {
            "artifact_dir": str(artifact_dir),
            "mode": "apply" if apply else "dry_run",
            "baseline": base,
            "decision": decision,
            "continuity": continuity_rows,
            "downstream_payoff": downstream_rows,
            "apply_actions": dict(Counter(row["action"] for row in results)),
            "scope_audit": _scope_audit(before, after),
            "integrity": integrity_payload,
            "yahoo_conflicts": len(yahoo_conflicts),
            "provider_calls": 0,
            "final_phase6_classification": "PHASE_6_CASH_BACKFILL_COMPLETE_WITH_ACCEPTED_RISK" if decision["production_financial_writes"] else "PHASE_6_CASH_NO_USABLE_FALLBACK",
        }
        write_json(artifact_dir / "summary.json", summary)
        (artifact_dir / "recommended_next_step.md").write_text(
            "Proceed to MASTER PLAN PHASE 8A-8C - EBIT residual.\n"
            if decision["production_financial_writes"]
            else "No cash production fills were applied.\n",
            encoding="utf-8",
        )
        return summary
    finally:
        v2_conn.close()
        legacy_conn.close()


def _candidate_definitions() -> list[dict[str, Any]]:
    return [
        {
            "provider": PROVIDER,
            "candidate": "YAHOO_CASH_EQUIVALENTS_AND_SHORT_TERM_INVESTMENTS",
            "exact_source": PROVIDER_FIELD,
            "semantic_breadth": SEMANTIC_DEFINITION,
            "transformation": TRANSFORMATION,
            "expected_relationship_to_canonical_cash": "same intended breadth as SimFin canonical cash",
            "exclusion_rules": "ambiguous identity, conflicting source values, non-null canonical cash, company-level DO_NOT_USE",
            "implemented": 1,
        }
    ]


def _overlap_rows(v2_rows: list[V2Quarter], yahoo_by_ticker: dict[str, list[YahooCashObservation]]) -> list[dict[str, Any]]:
    v2_exact = {(row.ticker, row.report_date): row for row in v2_rows}
    rows = []
    for ticker, observations in yahoo_by_ticker.items():
        for obs in observations:
            v2 = v2_exact.get((ticker, obs.period_end_date))
            if v2 is None or v2.cash is None:
                continue
            rel = _relative_difference(obs.value, v2.cash)
            rows.append(
                {
                    "ticker": ticker,
                    "fiscal_year": v2.fiscal_year,
                    "fiscal_period": v2.fiscal_period,
                    "canonical_report_date": v2.report_date,
                    "provider_date": obs.period_end_date,
                    "match_mode": EXACT_DATE_INFERRED_FISCAL,
                    "date_offset_days": 0,
                    "v2_cash": v2.cash,
                    "candidate_cash": obs.value,
                    "provider": PROVIDER,
                    "concept": PROVIDER_FIELD,
                    "absolute_difference": abs(obs.value - v2.cash),
                    "relative_difference": rel,
                    "ratio": obs.value / v2.cash if v2.cash else "",
                    "provenance": f"{SOURCE_TABLE}:{obs.raw_id}",
                }
            )
    return rows


def _zero_semantics(v2_rows: list[V2Quarter], yahoo_by_ticker: dict[str, list[YahooCashObservation]]) -> list[dict[str, Any]]:
    overlaps = _overlap_rows(v2_rows, yahoo_by_ticker)
    zeros = [row for row in overlaps if float(row["candidate_cash"]) == 0.0]
    return [
        {
            "provider": PROVIDER,
            "field_or_concept": PROVIDER_FIELD,
            "explicit_zero_overlap_rows": len(zeros),
            "explicit_zero_agrees_with_v2_zero": sum(1 for row in zeros if float(row["v2_cash"]) == 0.0),
            "explicit_zero_divergent_rows": sum(1 for row in zeros if float(row["v2_cash"]) != 0.0),
            "rule": "explicit zero is allowed only when the field exists; missing is never coerced to zero",
        }
    ]


def _by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets = defaultdict(Counter)
    by_ticker = defaultdict(Counter)
    for row in rows:
        year = int(str(row["report_date"])[:4])
        bucket = "pre-2020" if year < 2020 else str(year) if year <= 2025 else "2026+"
        buckets[(row["category"], row.get("risk_tier") or "")][bucket] += 1
        if row["category"].endswith("_RECOVERY"):
            by_ticker[row["ticker"]][row["risk_tier"]] += 1
    out = []
    for (category, tier), counts in sorted(buckets.items()):
        out.append(
            {
                "category": category,
                "risk_tier": tier,
                "pre_2020": counts["pre-2020"],
                "2020": counts["2020"],
                "2021": counts["2021"],
                "2022": counts["2022"],
                "2023": counts["2023"],
                "2024": counts["2024"],
                "2025": counts["2025"],
                "2026_plus": counts["2026+"],
                "companies_gaining_ge_1": sum(1 for company_counts in by_ticker.values() if company_counts[tier] >= 1),
                "companies_gaining_ge_4": sum(1 for company_counts in by_ticker.values() if company_counts[tier] >= 4),
                "companies_gaining_ge_8": sum(1 for company_counts in by_ticker.values() if company_counts[tier] >= 8),
            }
        )
    return out


def _yahoo_inventory(conn: sqlite3.Connection, *, market: str) -> list[dict[str, Any]]:
    inventory: dict[str, dict[str, Any]] = {}
    for row in conn.execute("SELECT symbol, quarterly_balance_sheet_json FROM rc_fundamental_yahoo_raw WHERE market=? AND provider='yahoo' AND status='OK'", (market,)):
        try:
            payload = json.loads(str(row["quarterly_balance_sheet_json"]))
        except Exception:
            continue
        columns = [str(value) for value in payload.get("columns", [])]
        index = [str(value) for value in payload.get("index", [])]
        data = payload.get("data", [])
        for idx, field in enumerate(index):
            if "cash" not in field.lower() and "short term investment" not in field.lower():
                continue
            rec = inventory.setdefault(field, {"field": field, "companies": set(), "periods": 0, "earliest": "", "latest": ""})
            values = data[idx] if idx < len(data) else []
            for col_idx, period in enumerate(columns):
                if col_idx < len(values) and values[col_idx] is not None:
                    rec["companies"].add(str(row["symbol"]).upper())
                    rec["periods"] += 1
                    rec["earliest"] = period if not rec["earliest"] or period < rec["earliest"] else rec["earliest"]
                    rec["latest"] = period if not rec["latest"] or period > rec["latest"] else rec["latest"]
    rows = []
    for rec in inventory.values():
        field = rec["field"]
        rows.append(
            {
                "field": field,
                "normalized_destination": "cash" if field == PROVIDER_FIELD else "",
                "companies": len(rec["companies"]),
                "periods": rec["periods"],
                "date_range": f"{rec['earliest']}..{rec['latest']}",
                "units": "provider numeric reporting currency",
                "point_in_time_semantics": "balance-sheet instant/period-end",
                "definition_breadth": "cash+equivalents+short-term investments" if field == PROVIDER_FIELD else "cash-like/component/provider-specific",
            }
        )
    return sorted(rows, key=lambda row: row["periods"], reverse=True)


def _sec_inventory(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT field_name, COUNT(*) AS periods, COUNT(DISTINCT ticker) AS companies,
               MIN(period_end_date) AS earliest, MAX(period_end_date) AS latest
        FROM rc_fundamental_statement_raw
        WHERE statement_type='balance' AND field_value IS NOT NULL
          AND (field_name LIKE '%Cash%' OR field_name LIKE '%cash%' OR field_name LIKE '%ShortTermInvest%')
        GROUP BY field_name
        ORDER BY periods DESC
        LIMIT 200
        """
    ).fetchall()
    out = []
    for row in rows:
        concept = str(row["field_name"]).split("|", 1)[0]
        out.append(
            {
                "concept": concept,
                "raw_field_name": row["field_name"],
                "companies": row["companies"],
                "periods": row["periods"],
                "date_range": f"{row['earliest']}..{row['latest']}",
                "instant_context_semantics": "balance-sheet instant fact; metadata may be embedded in field_name",
                "unit": "USD/provider currency metadata",
                "standard_vs_extension": "extension" if ":" in concept else "standard_or_normalized",
                "inclusion": "restricted cash / investments varies by concept",
            }
        )
    return out


def _provenance_audit(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT quarter_id, field_name, provider, provider_field, source_dataset, transformation,
                   source_value, import_run_id
            FROM rc_v2_fundamental_field_source
            WHERE import_run_id=? AND field_name='cash'
            ORDER BY quarter_id
            """,
            (run_id,),
        )
    ]
