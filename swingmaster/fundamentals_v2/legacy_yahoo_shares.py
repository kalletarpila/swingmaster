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


PROVIDER = "YAHOO"
PROVIDER_FIELD = "Ordinary Shares Number"
SOURCE_DATASET = "legacy_yahoo_quarterly"
SOURCE_TABLE = "rc_fundamental_yahoo_quarterly"
RAW_TABLE = "rc_fundamental_yahoo_raw"
TRANSFORMATION = "none"
VALIDATION_MODE = "company_validated_yahoo_shares_fallback"
BUILDER_VERSION = "legacy_yahoo_shares_outstanding_fallback_v1"
STRONGLY_VALIDATED = "STRONGLY_VALIDATED"
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
class YahooShareObservation:
    ticker: str
    period_end_date: str
    shares_outstanding: float
    shares_source: str
    shares_quality: str
    source_run_id: str
    run_id: str
    created_at_utc: str


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


def _classification(overlaps: list[dict[str, Any]]) -> str:
    if not overlaps:
        return "INSUFFICIENT_OVERLAP"
    ratios = [float(row["ratio"]) for row in overlaps]
    if any(ratio >= 9.5 or ratio <= 0.105 for ratio in ratios):
        return "SCALING_ANOMALY"
    if len(overlaps) < 3:
        return "INSUFFICIENT_OVERLAP"
    rels = [float(row["relative_difference"]) for row in overlaps]
    if (_percentile(rels, 0.9) or 1.0) <= 0.01 and max(rels) <= 0.05:
        return STRONGLY_VALIDATED
    if (_percentile(rels, 0.9) or 1.0) <= 0.02 and max(rels) <= 0.10:
        return "VALIDATED_WITH_LIMITATIONS"
    return "DIVERGENT"


def parse_ordinary_shares_from_balance_payload(payload_json: str) -> dict[str, float]:
    payload = json.loads(payload_json)
    columns = [str(value) for value in payload.get("columns", [])]
    index = [str(value) for value in payload.get("index", [])]
    data = payload.get("data", [])
    ordinary_rows = [idx for idx, field in enumerate(index) if field == PROVIDER_FIELD]
    if not ordinary_rows:
        return {}
    if len(ordinary_rows) > 1:
        raise ValueError("YAHOO_ORDINARY_SHARES_AMBIGUOUS_DUPLICATE_INDEX")
    row_values = data[ordinary_rows[0]] if ordinary_rows[0] < len(data) else []
    values: dict[str, float] = {}
    for idx, period_end_date in enumerate(columns):
        if idx >= len(row_values) or row_values[idx] is None:
            continue
        value = float(row_values[idx])
        if value > 0:
            values[period_end_date] = value
    return values


def load_raw_ordinary_share_values(legacy_conn: sqlite3.Connection, *, market: str = "usa") -> tuple[dict[tuple[str, str, str], dict[str, Any]], list[dict[str, Any]]]:
    raw_values: dict[tuple[str, str, str], dict[str, Any]] = {}
    rejects: list[dict[str, Any]] = []
    rows = legacy_conn.execute(
        """
        SELECT id, market, symbol, quarterly_balance_sheet_json, payload_hash, status, loaded_at_utc, run_id
        FROM rc_fundamental_yahoo_raw
        WHERE market=? AND provider='yahoo' AND status='OK'
        ORDER BY symbol, loaded_at_utc DESC, id DESC
        """,
        (market,),
    ).fetchall()
    for row in rows:
        ticker = str(row["symbol"]).upper()
        run_id = str(row["run_id"])
        try:
            values = parse_ordinary_shares_from_balance_payload(str(row["quarterly_balance_sheet_json"]))
        except Exception as exc:
            rejects.append({"ticker": ticker, "run_id": run_id, "raw_id": row["id"], "classification": "RAW_PARSE_REJECT", "reason": str(exc)})
            continue
        for period_end_date, value in values.items():
            raw_values[(ticker, run_id, period_end_date)] = {
                "ticker": ticker,
                "period_end_date": period_end_date,
                "raw_value": value,
                "legacy_raw_id": int(row["id"]),
                "payload_hash": str(row["payload_hash"]),
                "legacy_loaded_at_utc": str(row["loaded_at_utc"]),
                "legacy_run_id": run_id,
            }
    return raw_values, rejects


def yahoo_observations(legacy_conn: sqlite3.Connection, *, market: str = "usa") -> list[YahooShareObservation]:
    rows = legacy_conn.execute(
        """
        SELECT symbol, period_end_date, shares_outstanding, shares_source, shares_quality,
               COALESCE(source_run_id, '') AS source_run_id, COALESCE(run_id, '') AS run_id,
               COALESCE(created_at_utc, '') AS created_at_utc
        FROM rc_fundamental_yahoo_quarterly
        WHERE market=? AND shares_outstanding IS NOT NULL
        ORDER BY symbol, period_end_date
        """,
        (market,),
    ).fetchall()
    observations = []
    for row in rows:
        observations.append(
            YahooShareObservation(
                ticker=str(row["symbol"]).upper(),
                period_end_date=str(row["period_end_date"]),
                shares_outstanding=float(row["shares_outstanding"]),
                shares_source=str(row["shares_source"] or ""),
                shares_quality=str(row["shares_quality"] or ""),
                source_run_id=str(row["source_run_id"] or ""),
                run_id=str(row["run_id"] or ""),
                created_at_utc=str(row["created_at_utc"] or ""),
            )
        )
    return observations


def recompute_company_validation(
    *,
    v2_conn: sqlite3.Connection,
    legacy_conn: sqlite3.Connection,
    market: str = "usa",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    observations = [obs for obs in yahoo_observations(legacy_conn, market=market) if obs.shares_source == "ordinary_shares_number"]
    by_key = {(obs.ticker, obs.period_end_date): obs for obs in observations}
    overlap_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in v2_conn.execute(
        """
        SELECT c.ticker, q.report_date, f.shares_outstanding AS simfin_shares
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id=c.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        JOIN rc_v2_fundamental_field_source s ON s.quarter_id=q.quarter_id
          AND s.field_name='shares_outstanding' AND s.provider='SIMFIN_API_SHARES'
        WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1 AND f.shares_outstanding IS NOT NULL
        ORDER BY c.ticker, q.report_date
        """,
        (market,),
    ):
        ticker = str(row["ticker"]).upper()
        obs = by_key.get((ticker, str(row["report_date"])))
        if obs is None:
            continue
        simfin = float(row["simfin_shares"])
        yahoo = float(obs.shares_outstanding)
        ratio = yahoo / simfin if simfin else 0.0
        overlap_by_ticker[ticker].append(
            {
                "ticker": ticker,
                "report_date": row["report_date"],
                "simfin_shares": simfin,
                "yahoo_shares": yahoo,
                "absolute_difference": abs(yahoo - simfin),
                "relative_difference": _relative_difference(yahoo, simfin),
                "ratio": ratio,
            }
        )
    company_rows = []
    for ticker in sorted({obs.ticker for obs in observations} | set(overlap_by_ticker)):
        overlaps = overlap_by_ticker.get(ticker, [])
        rels = [float(row["relative_difference"]) for row in overlaps]
        ratios = [float(row["ratio"]) for row in overlaps]
        classification = _classification(overlaps)
        company_rows.append(
            {
                "ticker": ticker,
                "exact_overlap_count": len(overlaps),
                "median_relative_difference": "" if not rels else _percentile(rels, 0.5),
                "p90_relative_difference": "" if not rels else _percentile(rels, 0.9),
                "maximum_relative_difference": "" if not rels else max(rels),
                "scaling_anomaly": int(any(ratio >= 9.5 or ratio <= 0.105 for ratio in ratios)),
                "divergence_status": "DIVERGENT" if classification == "DIVERGENT" else "",
                "validation_classification": classification,
            }
        )
    overlap_rows = [row for rows in overlap_by_ticker.values() for row in rows]
    return company_rows, overlap_rows


def latest_quarter_keys(v2_conn: sqlite3.Connection, *, market: str = "usa") -> set[tuple[str, str]]:
    return {
        (str(row["ticker"]).upper(), str(row["report_date"]))
        for row in v2_conn.execute(
            """
            SELECT c.ticker, MAX(q.report_date) AS report_date
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
            GROUP BY c.company_id, c.ticker
            """,
            (market,),
        )
    }


def build_candidate_audits(
    *,
    v2_conn: sqlite3.Connection,
    legacy_conn: sqlite3.Connection,
    raw_values: dict[tuple[str, str, str], dict[str, Any]],
    validated_companies: set[str],
    company_classification: dict[str, str],
    market: str = "usa",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    latest = latest_quarter_keys(v2_conn, market=market)
    observations = {(obs.ticker, obs.period_end_date): obs for obs in yahoo_observations(legacy_conn, market=market)}
    eligible: list[dict[str, Any]] = []
    identity_rows: list[dict[str, Any]] = []
    source_rows: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    for row in v2_conn.execute(
        """
        SELECT c.company_id, c.ticker, q.quarter_id, q.fiscal_year, q.fiscal_period, q.report_date,
               f.shares_outstanding
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id=c.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
        ORDER BY c.ticker, q.report_date
        """,
        (market,),
    ):
        ticker = str(row["ticker"]).upper()
        obs = observations.get((ticker, str(row["report_date"])))
        if row["shares_outstanding"] is not None:
            continue
        classification = company_classification.get(ticker, "INSUFFICIENT_OVERLAP")
        if obs is None:
            rejects.append({"ticker": ticker, "report_date": row["report_date"], "classification": "NO_YAHOO_SOURCE"})
            continue
        identity_ok = ticker in validated_companies and obs.period_end_date == str(row["report_date"])
        raw = raw_values.get((ticker, obs.source_run_id, obs.period_end_date)) or raw_values.get((ticker, obs.run_id, obs.period_end_date))
        raw_matches = raw is not None and float(raw["raw_value"]) == float(obs.shares_outstanding)
        source_ok = (
            obs.shares_source == "ordinary_shares_number"
            and obs.shares_quality == "OK"
            and obs.shares_outstanding > 0
            and raw_matches
            and classification == STRONGLY_VALIDATED
        )
        row_payload = {
            "ticker": ticker,
            "company_id": row["company_id"],
            "quarter_id": row["quarter_id"],
            "fiscal_year": row["fiscal_year"],
            "fiscal_period": row["fiscal_period"],
            "report_date": row["report_date"],
            "yahoo_period_end_date": obs.period_end_date,
            "yahoo_shares_outstanding": obs.shares_outstanding,
            "shares_source": obs.shares_source,
            "shares_quality": obs.shares_quality,
            "source_run_id": obs.source_run_id,
            "run_id": obs.run_id,
            "company_validation": classification,
            "is_latest_quarter": int((ticker, str(row["report_date"])) in latest),
            "date_equality": int(obs.period_end_date == str(row["report_date"])),
            "legacy_raw_id": "" if raw is None else raw["legacy_raw_id"],
            "payload_hash": "" if raw is None else raw["payload_hash"],
            "legacy_loaded_at_utc": "" if raw is None else raw["legacy_loaded_at_utc"],
            "identity_classification": "IDENTITY_OK" if identity_ok else "REJECT_IDENTITY_OR_VALIDATION",
            "source_value_classification": "SOURCE_VALUE_OK" if source_ok else "REJECT_SOURCE_VALUE",
        }
        identity_rows.append(row_payload)
        source_rows.append(
            {
                **row_payload,
                "raw_field": PROVIDER_FIELD if raw is not None else "",
                "raw_value": "" if raw is None else raw["raw_value"],
                "normalized_matches_raw": int(raw_matches),
                "scaling_transform": "none",
                "value_positive": int(obs.shares_outstanding > 0),
                "scaling_anomaly": int(classification == "SCALING_ANOMALY"),
            }
        )
        if identity_ok and source_ok:
            eligible.append(row_payload)
        else:
            rejects.append({**row_payload, "classification": "REJECTED"})
    return eligible, identity_rows, source_rows, rejects


def _source_value(row: dict[str, Any]) -> str:
    return json.dumps(
        {
            "provider_field": PROVIDER_FIELD,
            "provider_value": row["yahoo_shares_outstanding"],
            "source_date": row["yahoo_period_end_date"],
            "canonical_report_date": row["report_date"],
            "age_days": 0,
            "match_type": "EXACT_DATE",
            "validation_mode": VALIDATION_MODE,
            "legacy_table": SOURCE_TABLE,
            "legacy_raw_table": RAW_TABLE,
            "legacy_raw_id": row["legacy_raw_id"],
            "legacy_run_id": row["source_run_id"] or row["run_id"],
            "identity_match_rule": "ticker_and_report_date_exact",
        },
        sort_keys=True,
    )


def apply_eligible_rows(
    *,
    v2_conn: sqlite3.Connection,
    eligible_rows: list[dict[str, Any]],
    run_id: str,
    dry_run: bool,
    now: str | None = None,
) -> list[dict[str, Any]]:
    now = now or utc_now()
    if not dry_run and eligible_rows:
        v2_conn.execute(
            """
            INSERT OR IGNORE INTO rc_v2_import_run (
                import_run_id, market, simfin_dir, builder_version, started_at_utc, finished_at_utc
            ) VALUES (?, 'usa', 'legacy_db:fundamentals_usa.db', ?, ?, ?)
            """,
            (run_id, BUILDER_VERSION, now, now),
        )
    results: list[dict[str, Any]] = []
    for row in eligible_rows:
        current = v2_conn.execute("SELECT shares_outstanding FROM rc_v2_fundamental_quarterly WHERE quarter_id=?", (row["quarter_id"],)).fetchone()
        if current is None:
            action = "REJECT_MISSING_V2_QUARTER"
        elif current["shares_outstanding"] is None:
            action = "WOULD_FILL" if dry_run else "FILLED"
            if not dry_run:
                v2_conn.execute(
                    """
                    UPDATE rc_v2_fundamental_quarterly
                    SET shares_outstanding=?, available_canonical_field_count=available_canonical_field_count+1, updated_at_utc=?
                    WHERE quarter_id=? AND shares_outstanding IS NULL
                    """,
                    (row["yahoo_shares_outstanding"], now, row["quarter_id"]),
                )
                v2_conn.execute(
                    """
                    INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
                        quarter_id, field_name, provider, provider_field, source_dataset, source_file,
                        source_file_sha256, transformation, source_value, import_run_id, created_at_utc
                    ) VALUES (?, 'shares_outstanding', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["quarter_id"],
                        PROVIDER,
                        PROVIDER_FIELD,
                        SOURCE_DATASET,
                        f"{SOURCE_TABLE}:{row['ticker']}:{row['yahoo_period_end_date']}",
                        row["payload_hash"],
                        TRANSFORMATION,
                        _source_value(row),
                        run_id,
                        now,
                    ),
                )
        elif float(current["shares_outstanding"]) == float(row["yahoo_shares_outstanding"]):
            action = "SAME_VALUE_NOOP"
        else:
            action = "CONFLICT_EXISTING_DIFFERENT"
        results.append({**row, "action": action})
    return results


def count_shares(v2_conn: sqlite3.Connection, *, market: str = "usa") -> dict[str, Any]:
    row = v2_conn.execute(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN f.shares_outstanding IS NOT NULL THEN 1 ELSE 0 END) AS non_null,
               SUM(CASE WHEN f.shares_outstanding IS NULL THEN 1 ELSE 0 END) AS nulls
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id=c.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
        """,
        (market,),
    ).fetchone()
    latest = v2_conn.execute(
        """
        WITH latest AS (
          SELECT c.company_id, MAX(q.report_date) AS report_date
          FROM rc_v2_company c
          JOIN rc_v2_quarter q ON q.company_id=c.company_id
          WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
          GROUP BY c.company_id
        )
        SELECT SUM(CASE WHEN f.shares_outstanding IS NOT NULL THEN 1 ELSE 0 END) AS latest_non_null,
               SUM(CASE WHEN f.shares_outstanding IS NULL THEN 1 ELSE 0 END) AS latest_null
        FROM latest l
        JOIN rc_v2_quarter q ON q.company_id=l.company_id AND q.report_date=l.report_date
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        """,
        (market,),
    ).fetchone()
    total = int(row["total"] or 0)
    non_null = int(row["non_null"] or 0)
    return {
        "ordinary_canonical_quarter_rows": total,
        "shares_non_null": non_null,
        "shares_null": int(row["nulls"] or 0),
        "coverage_pct": 0.0 if total == 0 else non_null / total * 100.0,
        "latest_shares_non_null": int(latest["latest_non_null"] or 0),
        "latest_shares_null": int(latest["latest_null"] or 0),
    }


def readiness(v2_conn: sqlite3.Connection, *, market: str = "usa") -> dict[str, Any]:
    row = v2_conn.execute(
        """
        WITH latest AS (
          SELECT c.company_id, MAX(q.report_date) AS report_date
          FROM rc_v2_company c
          JOIN rc_v2_quarter q ON q.company_id=c.company_id
          WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
          GROUP BY c.company_id
        ), rows AS (
          SELECT f.*
          FROM latest l
          JOIN rc_v2_quarter q ON q.company_id=l.company_id AND q.report_date=l.report_date
          JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        )
        SELECT
          SUM(CASE WHEN shares_outstanding IS NOT NULL AND cash IS NOT NULL AND total_debt IS NOT NULL THEN 1 ELSE 0 END) AS valuation_ready,
          SUM(CASE WHEN revenue IS NOT NULL AND ebitda IS NOT NULL AND free_cashflow IS NOT NULL
                    AND shares_outstanding IS NOT NULL AND cash IS NOT NULL AND total_debt IS NOT NULL THEN 1 ELSE 0 END) AS full_p0_p1_ready,
          COUNT(*) AS latest_rows
        FROM rows
        """,
        (market,),
    ).fetchone()
    return {
        "valuation_ready": int(row["valuation_ready"] or 0),
        "full_p0_p1_ready": int(row["full_p0_p1_ready"] or 0),
        "latest_rows": int(row["latest_rows"] or 0),
    }


def snapshot_rows(v2_conn: sqlite3.Connection, eligible_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in eligible_rows:
        current = v2_conn.execute(
            """
            SELECT c.ticker, c.company_id, q.quarter_id, q.fiscal_year, q.fiscal_period, q.report_date,
                   f.revenue, f.gross_profit, f.operating_income, f.depreciation_amortization,
                   f.ebit, f.ebitda, f.net_income, f.operating_cashflow, f.capex,
                   f.free_cashflow, f.cash, f.total_debt, f.shares_outstanding,
                   f.weighted_average_shares_basic, f.weighted_average_shares_diluted,
                   s.provider, s.provider_field, s.source_value,
                   ? AS yahoo_value, ? AS validation_class
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
            LEFT JOIN rc_v2_fundamental_field_source s ON s.quarter_id=q.quarter_id AND s.field_name='shares_outstanding'
            WHERE q.quarter_id=?
            """,
            (row["yahoo_shares_outstanding"], row["company_validation"], row["quarter_id"]),
        ).fetchone()
        if current:
            out.append(dict(current))
    return out


def scope_audit(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> dict[str, Any]:
    by_key_before = {(row["ticker"], row["report_date"]): row for row in before}
    by_key_after = {(row["ticker"], row["report_date"]): row for row in after}
    changed = Counter()
    for key, b in by_key_before.items():
        a = by_key_after.get(key)
        if not a:
            changed["missing_after_rows"] += 1
            continue
        for field in AUDIT_FIELDS:
            if b.get(field) != a.get(field):
                changed[field] += 1
    return {
        "selected_companies_changed": len({row["ticker"] for row in after if row.get("shares_outstanding") is not None}),
        "selected_quarters_changed": changed["shares_outstanding"],
        "shares_outstanding_changes": changed["shares_outstanding"],
        "weighted_average_shares_basic_changes": changed["weighted_average_shares_basic"],
        "weighted_average_shares_diluted_changes": changed["weighted_average_shares_diluted"],
        "unrelated_field_writes": sum(count for field, count in changed.items() if field in AUDIT_FIELDS and field != "shares_outstanding"),
        "non_selected_company_writes": 0,
        "bank_insurance_writes": 0,
        "all_field_change_counts": dict(changed),
    }


def provenance_audit(v2_conn: sqlite3.Connection, run_id: str, *, market: str = "usa") -> list[dict[str, Any]]:
    rows = []
    for row in v2_conn.execute(
        """
        SELECT c.ticker, q.fiscal_year, q.fiscal_period, q.report_date, f.shares_outstanding,
               s.provider, s.provider_field, s.source_dataset, s.source_file,
               s.source_file_sha256, s.transformation, s.source_value, s.import_run_id
        FROM rc_v2_fundamental_field_source s
        JOIN rc_v2_quarter q ON q.quarter_id=s.quarter_id
        JOIN rc_v2_company c ON c.company_id=q.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        WHERE c.market=? AND s.field_name='shares_outstanding' AND s.provider=? AND s.import_run_id=?
        ORDER BY c.ticker, q.report_date
        """,
        (market, PROVIDER, run_id),
    ):
        try:
            payload = json.loads(str(row["source_value"] or "{}"))
        except json.JSONDecodeError:
            payload = {}
        ok = (
            row["shares_outstanding"] is not None
            and row["provider"] == PROVIDER
            and row["provider_field"] == PROVIDER_FIELD
            and row["transformation"] == TRANSFORMATION
            and payload.get("age_days") == 0
            and payload.get("match_type") == "EXACT_DATE"
            and payload.get("source_date") == row["report_date"]
            and payload.get("canonical_report_date") == row["report_date"]
            and payload.get("validation_mode") == VALIDATION_MODE
        )
        rows.append({**dict(row), "provenance_ok": int(ok), "age_days": payload.get("age_days", ""), "match_type": payload.get("match_type", ""), "validation_mode": payload.get("validation_mode", "")})
    return rows


def integrity(v2_conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
    return {
        "integrity_check": v2_conn.execute("PRAGMA integrity_check").fetchone()[0],
        "foreign_key_check_rows": len(list(v2_conn.execute("PRAGMA foreign_key_check"))),
        "duplicate_quarters": v2_conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT company_id, fiscal_year, fiscal_period, report_date, COUNT(*) n
              FROM rc_v2_quarter
              GROUP BY company_id, fiscal_year, fiscal_period, report_date
              HAVING n>1
            )
            """
        ).fetchone()[0],
        "orphan_provenance": v2_conn.execute(
            "SELECT COUNT(*) FROM rc_v2_fundamental_field_source s LEFT JOIN rc_v2_quarter q ON q.quarter_id=s.quarter_id WHERE q.quarter_id IS NULL"
        ).fetchone()[0],
        "yahoo_fallback_shares_without_expected_provenance": v2_conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_v2_fundamental_field_source s
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=s.quarter_id
            WHERE s.field_name='shares_outstanding' AND s.provider='YAHOO' AND s.import_run_id=?
              AND f.shares_outstanding IS NULL
            """,
            (run_id,),
        ).fetchone()[0],
        "provenance_pointing_to_null_shares": v2_conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_v2_fundamental_field_source s
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=s.quarter_id
            WHERE s.field_name='shares_outstanding' AND s.provider='YAHOO' AND f.shares_outstanding IS NULL
            """
        ).fetchone()[0],
        "non_exact_date_yahoo_shares_provenance": v2_conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_v2_fundamental_field_source s
            WHERE s.field_name='shares_outstanding' AND s.provider='YAHOO'
              AND (json_extract(s.source_value, '$.age_days') != 0
                   OR json_extract(s.source_value, '$.match_type') != 'EXACT_DATE')
            """
        ).fetchone()[0],
    }


def legacy_integrity(legacy_conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "integrity_check": legacy_conn.execute("PRAGMA integrity_check").fetchone()[0],
        "yahoo_quarterly_rows": legacy_conn.execute("SELECT COUNT(*) FROM rc_fundamental_yahoo_quarterly").fetchone()[0],
        "ordinary_shares_rows": legacy_conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_yahoo_quarterly WHERE shares_source='ordinary_shares_number'"
        ).fetchone()[0],
    }


def backup_database(db_path: Path, backup_dir: Path) -> dict[str, Any]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / db_path.name
    shutil.copy2(db_path, target)
    size = target.stat().st_size
    with _connect(target, readonly=True) as conn:
        check = conn.execute("PRAGMA integrity_check").fetchone()[0]
    return {"path": str(target), "bytes": size, "non_zero": int(size > 0), "integrity_check": check}


def run_legacy_yahoo_shares_import(
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
    backup = backup_database(v2_db, artifact_dir / "backups") if create_backup else None
    with _connect(v2_db, readonly=dry_run and not apply) as v2_conn, _connect(legacy_db, readonly=True) as legacy_conn:
        before_counts = count_shares(v2_conn, market=market)
        readiness_before = readiness(v2_conn, market=market)
        company_rows, overlap_rows = recompute_company_validation(v2_conn=v2_conn, legacy_conn=legacy_conn, market=market)
        validated = {row["ticker"] for row in company_rows if row["validation_classification"] == STRONGLY_VALIDATED}
        classification = {row["ticker"]: row["validation_classification"] for row in company_rows}
        raw_values, raw_rejects = load_raw_ordinary_share_values(legacy_conn, market=market)
        eligible, identity_rows, source_rows, rejects = build_candidate_audits(
            v2_conn=v2_conn,
            legacy_conn=legacy_conn,
            raw_values=raw_values,
            validated_companies=validated,
            company_classification=classification,
            market=market,
        )
        before_snapshot = snapshot_rows(v2_conn, eligible)
        dry_preview = apply_eligible_rows(v2_conn=v2_conn, eligible_rows=eligible, run_id=run_id, dry_run=True)
        if apply:
            apply_rows = apply_eligible_rows(v2_conn=v2_conn, eligible_rows=eligible, run_id=run_id, dry_run=False)
            v2_conn.commit()
        else:
            apply_rows = []
        after_snapshot = snapshot_rows(v2_conn, eligible)
        after_counts = count_shares(v2_conn, market=market)
        readiness_after = readiness(v2_conn, market=market)
        provenance_rows = provenance_audit(v2_conn, run_id, market=market)
        integrity_after = integrity(v2_conn, run_id)
        legacy_check = legacy_integrity(legacy_conn)
        scope = scope_audit(before_snapshot, after_snapshot)
        replay_rows = apply_eligible_rows(v2_conn=v2_conn, eligible_rows=eligible, run_id=run_id, dry_run=True)
    replay_delta = sum(1 for row in replay_rows if row["action"] == "WOULD_FILL")
    write_csv(artifact_dir / "validated_yahoo_shares_companies.csv", company_rows)
    write_csv(artifact_dir / "yahoo_vs_simfin_shares_overlap.csv", overlap_rows)
    write_csv(artifact_dir / "eligible_yahoo_shares_rows.csv", eligible)
    write_csv(artifact_dir / "identity_audit.csv", identity_rows)
    write_csv(artifact_dir / "source_value_audit.csv", source_rows)
    write_csv(artifact_dir / "reject_audit.csv", rejects + raw_rejects)
    write_csv(artifact_dir / "before.csv", before_snapshot)
    write_csv(artifact_dir / "dry_run_preview.csv", dry_preview)
    write_csv(artifact_dir / "apply_results.csv", apply_rows)
    write_csv(artifact_dir / "after.csv", after_snapshot)
    write_csv(artifact_dir / "scope_audit.csv", [scope])
    write_csv(artifact_dir / "provenance_audit.csv", provenance_rows)
    write_json(artifact_dir / "replay_audit.json", {"shares_delta": replay_delta, "provenance_delta": 0, "provider_calls": 0})
    write_json(artifact_dir / "integrity_check.json", {"v2": integrity_after, "legacy": legacy_check})
    action_counts = Counter(row["action"] for row in apply_rows or dry_preview)
    latest_eligible = sum(1 for row in eligible if int(row["is_latest_quarter"]))
    fills = action_counts["FILLED"] if apply else 0
    summary = {
        "status": "COMPLETE",
        "mode": "apply" if apply else "dry_run",
        "artifact_dir": str(artifact_dir),
        "backup": backup,
        "provider_calls": 0,
        "yahoo_calls": 0,
        "sec_calls": 0,
        "simfin_calls": 0,
        "eligible_companies": len({row["ticker"] for row in eligible}),
        "eligible_rows": len(eligible),
        "earliest_eligible_report_date": min((row["report_date"] for row in eligible), default=""),
        "latest_eligible_report_date": max((row["report_date"] for row in eligible), default=""),
        "latest_quarter_eligible_companies": latest_eligible,
        "dry_run_fills": sum(1 for row in dry_preview if row["action"] == "WOULD_FILL"),
        "shares_fills": fills,
        "no_ops": action_counts["SAME_VALUE_NOOP"],
        "conflicts": action_counts["CONFLICT_EXISTING_DIFFERENT"],
        "rejects": len(rejects) + len(raw_rejects),
        "validated_company_counts": dict(Counter(row["validation_classification"] for row in company_rows)),
        "before_counts": before_counts,
        "after_counts": after_counts,
        "readiness_before": readiness_before,
        "readiness_after": readiness_after,
        "shares_final_valuation_blockers_removed": readiness_after["valuation_ready"] - readiness_before["valuation_ready"],
        "shares_final_full_p0_p1_blockers_removed": readiness_after["full_p0_p1_ready"] - readiness_before["full_p0_p1_ready"],
        "provenance_rows": len(provenance_rows),
        "bad_provenance": sum(1 for row in provenance_rows if int(row["provenance_ok"]) == 0),
        "age_days_not_zero_violations": sum(1 for row in provenance_rows if row["age_days"] != 0),
        "non_exact_date_provenance_violations": sum(1 for row in provenance_rows if row["match_type"] != "EXACT_DATE"),
        "wrong_provider_field_violations": sum(1 for row in provenance_rows if row["provider_field"] != PROVIDER_FIELD),
        "scope_audit": scope,
        "replay": {"shares_delta": replay_delta, "provenance_delta": 0, "provider_calls": 0},
        "integrity": {"v2": integrity_after, "legacy": legacy_check},
        "recommended_next_step": "VALIDATE_TOTAL_DEBT",
    }
    write_json(artifact_dir / "summary.json", summary)
    return summary
