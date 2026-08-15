from __future__ import annotations

import csv
import json
import math
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
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


TARGET_FIELDS = (
    "revenue",
    "ebitda",
    "free_cashflow",
    "operating_cashflow",
    "capex",
    "shares_outstanding",
    "cash",
    "total_debt",
    "ebit",
)
SEC_QUARTERLY_FIELDS = {
    "revenue": ("income", "Total Revenue"),
    "operating_cashflow": ("cashflow", "Operating Cash Flow"),
    "capex": ("cashflow", "Capital Expenditure"),
    "shares_outstanding": ("balance", "Ordinary Shares Number"),
    "cash": ("balance", "Cash And Cash Equivalents"),
    "total_debt": ("balance", "Total Debt"),
}
SEC_REVENUE_PROVIDER = "SEC"
SEC_REVENUE_PROVIDER_FIELD = "Total Revenue"
SEC_REVENUE_SOURCE_DATASET = "legacy_sec_edgar_reconstructed_quarterly_income"
SEC_REVENUE_SOURCE_TABLE = "rc_fundamental_statement_raw"
SEC_REVENUE_BUILDER_VERSION = "phase9_sec_revenue_residual_v1"
SEC_REVENUE_TRANSFORMATION = "none"
SEC_REVENUE_MATCH_MODES = {EXACT_DATE_INFERRED_FISCAL, TOLERANCE_DATE_INFERRED_FISCAL}
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
    values: dict[str, float | None]


@dataclass(frozen=True)
class SecObservation:
    ticker: str
    period_end_date: str
    field_name: str
    value: float
    statement_type: str
    period_type: str
    currency: str
    source: str
    retrieved_at_utc: str
    run_id: str


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
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def parse_sec_fact_field_name(field_name: str) -> dict[str, str]:
    concept, *parts = field_name.split("|")
    meta: dict[str, str] = {"concept": concept}
    for part in parts:
        if "=" in part:
            key, value = part.split("=", 1)
            meta[key] = value
    return meta


def classify_sec_context(*, statement_type: str, period_type: str, field_name: str, period_end_date: str) -> dict[str, Any]:
    meta = parse_sec_fact_field_name(field_name)
    period_start = None if meta.get("start") in {None, "NULL", ""} else meta.get("start")
    period_end = period_end_date
    duration_days = None
    if period_start:
        try:
            duration_days = (date.fromisoformat(period_end) - date.fromisoformat(period_start)).days + 1
        except ValueError:
            duration_days = None
    if period_type == "quarterly":
        context_class = "DIRECT_QUARTER" if statement_type in {"income", "cashflow"} else "INSTANT_PERIOD_END"
        duplicate_status = "RECONSTRUCTED_CACHE_UNIQUE_FIELD"
    elif period_start is None:
        context_class = "INSTANT_PERIOD_END" if statement_type == "balance" else "AMBIGUOUS_DURATION"
        duplicate_status = "RAW_CONTEXT_NEEDS_DUPLICATE_REVIEW"
    elif meta.get("fp") == "Q1":
        context_class = "DIRECT_QUARTER" if duration_days is not None and duration_days <= 120 else "AMBIGUOUS_DURATION"
        duplicate_status = "RAW_CONTEXT_NEEDS_DUPLICATE_REVIEW"
    elif meta.get("fp") == "Q2":
        context_class = "YTD_6M" if duration_days is not None and duration_days > 120 else "DIRECT_QUARTER"
        duplicate_status = "RAW_CONTEXT_NEEDS_DUPLICATE_REVIEW"
    elif meta.get("fp") == "Q3":
        context_class = "YTD_9M" if duration_days is not None and duration_days > 200 else "DIRECT_QUARTER"
        duplicate_status = "RAW_CONTEXT_NEEDS_DUPLICATE_REVIEW"
    elif meta.get("fp") == "FY":
        context_class = "FULL_YEAR"
        duplicate_status = "RAW_CONTEXT_NEEDS_DUPLICATE_REVIEW"
    else:
        context_class = "OTHER_DURATION" if duration_days is not None else "AMBIGUOUS_DURATION"
        duplicate_status = "RAW_CONTEXT_NEEDS_DUPLICATE_REVIEW"
    return {
        "concept": meta["concept"],
        "unit": meta.get("unit", ""),
        "period_start": period_start or "",
        "period_end": period_end,
        "duration_days": duration_days if duration_days is not None else "",
        "form": meta.get("form", ""),
        "fy": meta.get("fy", ""),
        "fp": meta.get("fp", ""),
        "frame": meta.get("frame", ""),
        "filed": meta.get("filed", ""),
        "standard_vs_extension": "extension" if ":" in meta["concept"] else "standard_or_normalized",
        "duration_class": context_class,
        "duplicate_context_status": duplicate_status,
    }


def run_phase9_sec_normalization(
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
        backup = None
        if create_backup:
            backup_dir = artifact_dir / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = backup_dir / v2_db.name
            shutil.copy2(v2_db, backup_path)
            with _connect(backup_path, readonly=True) as backup_conn:
                backup = {
                    "path": str(backup_path),
                    "bytes": backup_path.stat().st_size,
                    "non_zero": int(backup_path.stat().st_size > 0),
                    "integrity_check": backup_conn.execute("PRAGMA integrity_check").fetchone()[0],
                }
            write_json(backup_dir / "backup_integrity_check.json", backup)

        v2_rows = _load_v2_rows(v2_conn, market=market)
        by_key = {(row.ticker, row.report_date): row for row in v2_rows}
        latest_dates = _latest_dates(v2_rows)
        sec_observations, sec_conflicts = _load_sec_quarterly_observations(legacy_conn)
        revenue_quality, revenue_tiers = _classify_sec_revenue_companies(v2_rows, sec_observations)
        revenue_recoverability = _build_sec_revenue_recoverability(v2_rows, sec_observations, revenue_tiers, latest_dates)
        eligible_revenue = [row for row in revenue_recoverability if row["category"] == "SAFE_SCOPED_RECOVERY"]

        baseline = _baseline_by_field(v2_rows)
        residual_matrix = _residual_source_matrix(v2_rows, by_key, latest_dates, sec_observations, revenue_tiers)
        priority = _priority_rows(v2_rows, residual_matrix, revenue_recoverability)
        materiality = _materiality_decision(priority)
        asset_inventory = _legacy_asset_inventory(legacy_conn)

        before = _snapshot(v2_conn, eligible_revenue)
        results = _apply_sec_revenue_rows(v2_conn, eligible_revenue, run_id=run_id, dry_run=dry_run, now=utc_now())
        if apply and not dry_run:
            v2_conn.commit()
        after = _snapshot(v2_conn, eligible_revenue)
        replay = _apply_sec_revenue_rows(v2_conn, eligible_revenue, run_id=run_id, dry_run=True, now=utc_now())
        after_integrity = _integrity(v2_conn)

        coverage_after = _coverage_after(baseline, results)
        readiness = _readiness_impact(v2_rows, eligible_revenue)
        scope = _scope_audit(before, after)
        replay_audit = [{"field_name": "revenue", "canonical_delta": sum(1 for row in replay if row["action"] == "WOULD_FILL"), "provenance_delta": 0, "provider_calls": 0}]
        integrity_payload = {
            "before": before_integrity,
            "after": after_integrity,
            "legacy": {"readable": 1, "integrity_check": "not_run_large_readonly_cache"},
            "provider_calls": 0,
            "sec_revenue_conflicts": len(sec_conflicts),
        }

        write_csv(artifact_dir / "phase9_current_residual_baseline.csv", baseline)
        write_csv(artifact_dir / "downstream_required_legacy_asset_inventory.csv", asset_inventory)
        write_csv(artifact_dir / "target_field_source_decomposition.csv", _source_decomposition(asset_inventory))
        write_csv(artifact_dir / "phase9_residual_source_matrix.csv", residual_matrix)
        write_csv(artifact_dir / "phase9_legacy_asset_priority.csv", priority)
        write_csv(artifact_dir / "sec_context_normalization_audit.csv", _sec_context_audit(legacy_conn))
        write_csv(artifact_dir / "sec_target_concept_families.csv", _sec_target_concept_families(legacy_conn))
        write_csv(artifact_dir / "sec_normalized_target_validation.csv", _sec_validation(v2_rows, sec_observations))
        write_csv(artifact_dir / "sec_risk_tiers.csv", revenue_quality)
        write_csv(artifact_dir / "phase9_residual_recoverability_by_field.csv", _recoverability_by_field(revenue_recoverability, residual_matrix))
        write_csv(artifact_dir / "phase9_materiality_decision.csv", materiality)
        (artifact_dir / "implemented_paths.md").write_text(_implemented_paths_text(eligible_revenue), encoding="utf-8")
        write_csv(artifact_dir / "before.csv", before)
        write_csv(artifact_dir / "dry_run_preview.csv", results)
        write_csv(artifact_dir / "apply_results.csv", results)
        write_csv(artifact_dir / "provenance_audit.csv", _provenance_audit(v2_conn, run_id))
        write_csv(artifact_dir / "scope_audit.csv", [scope])
        write_csv(artifact_dir / "after.csv", after)
        write_csv(artifact_dir / "replay_audit.csv", replay_audit)
        write_csv(artifact_dir / "readiness_impact.csv", readiness)
        write_json(artifact_dir / "integrity_check.json", integrity_payload)
        final_classification = (
            "PHASE_9_SEC_NORMALIZATION_AND_BACKFILL_COMPLETE"
            if sum(1 for row in results if row["action"] == "FILLED")
            else "PHASE_9_SEC_NORMALIZATION_COMPLETE_NO_MATERIAL_BACKFILL"
        )
        if sum(1 for row in results if row["action"] == "FILLED") and any(row["risk_tier"] == "ACCEPTED_RISK" for row in eligible_revenue):
            final_classification = "PHASE_9_SEC_NORMALIZATION_AND_BACKFILL_COMPLETE_WITH_ACCEPTED_RISK"
        summary = {
            "artifact_dir": str(artifact_dir),
            "mode": "apply" if apply else "dry_run",
            "backup": backup,
            "provider_calls": 0,
            "baseline": baseline,
            "coverage_after": coverage_after,
            "eligible_revenue_rows": len(eligible_revenue),
            "apply_actions": dict(Counter(row["action"] for row in results)),
            "scope_audit": scope,
            "replay_audit": replay_audit,
            "readiness_impact": readiness,
            "integrity": integrity_payload,
            "final_phase9_classification": final_classification,
        }
        write_json(artifact_dir / "summary.json", summary)
        (artifact_dir / "recommended_next_step.md").write_text(
            "Proceed to MASTER PLAN PHASE 10A-10D - final P0/P1 completeness + readiness.\n",
            encoding="utf-8",
        )
        return summary
    finally:
        v2_conn.close()
        legacy_conn.close()


def _load_v2_rows(conn: sqlite3.Connection, *, market: str) -> list[V2Quarter]:
    rows = []
    for row in conn.execute(
        """
        SELECT c.ticker, c.company_id, q.quarter_id, q.fiscal_year, q.fiscal_period, q.report_date,
               f.revenue, f.ebitda, f.free_cashflow, f.operating_cashflow, f.capex,
               f.shares_outstanding, f.cash, f.total_debt, f.ebit
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id=c.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
        ORDER BY c.ticker, q.report_date
        """,
        (market,),
    ):
        rows.append(
            V2Quarter(
                ticker=str(row["ticker"]).upper(),
                company_id=int(row["company_id"]),
                quarter_id=int(row["quarter_id"]),
                fiscal_year=int(row["fiscal_year"]),
                fiscal_period=str(row["fiscal_period"]),
                report_date=str(row["report_date"]),
                values={field: None if row[field] is None else float(row[field]) for field in TARGET_FIELDS},
            )
        )
    return rows


def _load_sec_quarterly_observations(conn: sqlite3.Connection) -> tuple[dict[str, dict[tuple[str, str], SecObservation]], list[dict[str, Any]]]:
    target_names = {field_name for _statement, field_name in SEC_QUARTERLY_FIELDS.values()}
    placeholders = ",".join("?" for _ in target_names)
    values: dict[str, dict[tuple[str, str], dict[float, SecObservation]]] = defaultdict(lambda: defaultdict(dict))
    for row in conn.execute(
        f"""
        SELECT ticker, statement_type, period_end_date, period_type, field_name, field_value, currency, source, retrieved_at_utc, run_id
        FROM rc_fundamental_statement_raw
        WHERE source='sec_edgar' AND period_type='quarterly' AND field_value IS NOT NULL
          AND field_name IN ({placeholders})
        """,
        tuple(target_names),
    ):
        target_field = next((field for field, (_statement, source_field) in SEC_QUARTERLY_FIELDS.items() if source_field == row["field_name"]), None)
        if target_field is None:
            continue
        obs = SecObservation(
            ticker=str(row["ticker"]).upper(),
            period_end_date=str(row["period_end_date"]),
            field_name=str(row["field_name"]),
            value=float(row["field_value"]),
            statement_type=str(row["statement_type"]),
            period_type=str(row["period_type"]),
            currency=str(row["currency"] or ""),
            source=str(row["source"]),
            retrieved_at_utc=str(row["retrieved_at_utc"]),
            run_id=str(row["run_id"]),
        )
        values[target_field][(obs.ticker, obs.period_end_date)][obs.value] = obs
    observations: dict[str, dict[tuple[str, str], SecObservation]] = defaultdict(dict)
    conflicts = []
    for field, by_key in values.items():
        for key, by_value in by_key.items():
            if len(by_value) == 1:
                observations[field][key] = next(iter(by_value.values()))
            else:
                conflicts.append({"target_field": field, "ticker": key[0], "period_end_date": key[1], "classification": "CONFLICTING_SEC_QUARTERLY_VALUES", "value_count": len(by_value)})
    return observations, conflicts


def _relative_difference(a: float, b: float) -> float:
    return abs(a - b) / max(abs(a), abs(b), 1.0)


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.floor(p * (len(ordered) - 1)))]


def _classify_sec_revenue_companies(v2_rows: list[V2Quarter], observations: dict[str, dict[tuple[str, str], SecObservation]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    exact = {(row.ticker, row.report_date): row for row in v2_rows}
    by_ticker: dict[str, list[tuple[float, bool]]] = defaultdict(list)
    for key, obs in observations.get("revenue", {}).items():
        v2 = exact.get(key)
        if v2 is None or v2.values["revenue"] is None:
            continue
        by_ticker[obs.ticker].append((_relative_difference(obs.value, float(v2.values["revenue"])), (obs.value < 0) != (float(v2.values["revenue"]) < 0)))
    tickers = sorted({row.ticker for row in v2_rows} | set(by_ticker))
    rows = []
    tiers = {}
    for ticker in tickers:
        overlap = by_ticker.get(ticker, [])
        rels = [rel for rel, _sign in overlap]
        sign_anomalies = sum(1 for _rel, sign in overlap if sign)
        if len(overlap) >= 4 and sign_anomalies == 0 and rels and max(rels) <= 0.01:
            tier = "SAFE_SCOPED"
            consistency = "company_scoped_sec_quarterly_revenue_matches_v2_within_1pct"
        elif len(overlap) >= 4 and sign_anomalies == 0 and rels and max(rels) <= 0.05:
            tier = "NEEDS_MORE_VALIDATION"
            consistency = "close_but_not_phase9_write_quality"
        elif not overlap:
            tier = "NEEDS_MORE_VALIDATION"
            consistency = "no_overlap_to_validate_company_scope"
        else:
            tier = "DO_NOT_USE"
            consistency = "divergent_or_sign_anomaly"
        tiers[ticker] = tier
        rows.append(
            {
                "provider": "SEC",
                "target_field": "revenue",
                "source_field_or_concept": SEC_REVENUE_PROVIDER_FIELD,
                "ticker": ticker,
                "overlap_count": len(overlap),
                "within_0_1_pct": sum(rel <= 0.001 for rel in rels),
                "within_1_pct": sum(rel <= 0.01 for rel in rels),
                "within_2_pct": sum(rel <= 0.02 for rel in rels),
                "within_5_pct": sum(rel <= 0.05 for rel in rels),
                "median_relative_difference": _percentile(rels, 0.5),
                "p90": _percentile(rels, 0.9),
                "p95": _percentile(rels, 0.95),
                "sign_anomalies": sign_anomalies,
                "scaling_anomalies": int(any(rel >= 0.9 for rel in rels)),
                "risk_tier": tier,
                "validation_scope": "company-scoped SEC reconstructed quarterly Total Revenue",
                "semantic_consistency": consistency,
            }
        )
    return rows, tiers


def _build_sec_revenue_recoverability(
    v2_rows: list[V2Quarter],
    observations: dict[str, dict[tuple[str, str], SecObservation]],
    tiers: dict[str, str],
    latest_dates: dict[str, str],
) -> list[dict[str, Any]]:
    obs_by_id = {f"{obs.ticker}:{obs.period_end_date}:{obs.field_name}": obs for obs in observations.get("revenue", {}).values()}
    by_ticker: dict[str, list[ProviderQuarterCandidate]] = defaultdict(list)
    for obs in observations.get("revenue", {}).values():
        by_ticker[obs.ticker].append(ProviderQuarterCandidate(f"{obs.ticker}:{obs.period_end_date}:{obs.field_name}", obs.ticker, None, None, obs.period_end_date, True))
    rows = []
    for row in v2_rows:
        if row.values["revenue"] is not None:
            continue
        match = match_cross_provider_quarter(
            QuarterIdentity(row.ticker, row.fiscal_year, row.fiscal_period, row.report_date, row.quarter_id),
            by_ticker.get(row.ticker, []),
            allow_date_inferred_fiscal_match=True,
            provider_fiscal_identity_usable=False,
        )
        obs = obs_by_id.get(match.candidate.candidate_id) if match.candidate else None
        tier = tiers.get(row.ticker, "")
        if match.outcome in SEC_REVENUE_MATCH_MODES and obs is not None and tier == "SAFE_SCOPED":
            category = "SAFE_SCOPED_RECOVERY"
        elif match.outcome == AMBIGUOUS:
            category = "DO_NOT_USE"
        elif obs is None:
            category = "NO_SOURCE"
        elif tier == "NEEDS_MORE_VALIDATION":
            category = "NEEDS_MORE_VALIDATION"
        else:
            category = "DO_NOT_USE"
        rows.append(
            {
                "ticker": row.ticker,
                "company_id": row.company_id,
                "quarter_id": row.quarter_id,
                "fiscal_year": row.fiscal_year,
                "fiscal_period": row.fiscal_period,
                "report_date": row.report_date,
                "target_field": "revenue",
                "provider": "SEC" if obs else "",
                "field_concept": SEC_REVENUE_PROVIDER_FIELD if obs else "",
                "candidate_value": "" if obs is None else obs.value,
                "provider_date": "" if obs is None else obs.period_end_date,
                "match_mode": match.outcome,
                "date_offset_days": match.date_offset_days,
                "fiscal_identity_verified": int(match.fiscal_identity_verified),
                "risk_tier": tier if category.endswith("_RECOVERY") else "",
                "category": category,
                "validation_rule": "company-scoped SEC reconstructed quarterly Total Revenue",
                "sec_context": "DIRECT_QUARTER_RECONSTRUCTED",
                "is_latest": int(latest_dates.get(row.ticker) == row.report_date),
                "legacy_run_id": "" if obs is None else obs.run_id,
                "retrieved_at_utc": "" if obs is None else obs.retrieved_at_utc,
            }
        )
    return rows


def _apply_sec_revenue_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]], *, run_id: str, dry_run: bool, now: str | None = None) -> list[dict[str, Any]]:
    now = now or utc_now()
    if not dry_run and rows:
        conn.execute(
            """
            INSERT OR IGNORE INTO rc_v2_import_run (
                import_run_id, market, simfin_dir, builder_version, started_at_utc, finished_at_utc
            ) VALUES (?, 'usa', 'legacy_db:fundamentals_usa.db', ?, ?, ?)
            """,
            (run_id, SEC_REVENUE_BUILDER_VERSION, now, now),
        )
    results = []
    for row in rows:
        current = conn.execute("SELECT revenue FROM rc_v2_fundamental_quarterly WHERE quarter_id=?", (row["quarter_id"],)).fetchone()
        if current is None:
            action = "REJECT_MISSING_V2_QUARTER"
        elif current["revenue"] is not None:
            action = "SAME_VALUE_NOOP" if float(current["revenue"]) == float(row["candidate_value"]) else "CONFLICT_EXISTING_DIFFERENT"
        else:
            action = "WOULD_FILL" if dry_run else "FILLED"
            if not dry_run:
                conn.execute(
                    """
                    UPDATE rc_v2_fundamental_quarterly
                    SET revenue=?, available_canonical_field_count=available_canonical_field_count+1, updated_at_utc=?
                    WHERE quarter_id=? AND revenue IS NULL
                    """,
                    (row["candidate_value"], now, row["quarter_id"]),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
                        quarter_id, field_name, provider, provider_field, source_dataset, source_file,
                        source_file_sha256, transformation, source_value, import_run_id, created_at_utc
                    ) VALUES (?, 'revenue', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["quarter_id"],
                        SEC_REVENUE_PROVIDER,
                        SEC_REVENUE_PROVIDER_FIELD,
                        SEC_REVENUE_SOURCE_DATASET,
                        f"{SEC_REVENUE_SOURCE_TABLE}:{row['ticker']}:{row['provider_date']}:{SEC_REVENUE_PROVIDER_FIELD}",
                        "n/a",
                        SEC_REVENUE_TRANSFORMATION,
                        _sec_revenue_source_value(row),
                        run_id,
                        now,
                    ),
                )
        results.append({**row, "action": action})
    return results


def _sec_revenue_source_value(row: dict[str, Any]) -> str:
    return json.dumps(
        {
            "validation_tier": row["risk_tier"],
            "validation_scope": row["validation_rule"],
            "provider": "SEC",
            "provider_field": SEC_REVENUE_PROVIDER_FIELD,
            "source_dataset": SEC_REVENUE_SOURCE_DATASET,
            "semantic_definition": "total company revenue from SEC reconstructed quarterly cache",
            "provider_value": row["candidate_value"],
            "provider_period": row["provider_date"],
            "canonical_report_date": row["report_date"],
            "match_mode": row["match_mode"],
            "date_offset_days": row["date_offset_days"],
            "fiscal_identity_verified": bool(row["fiscal_identity_verified"]),
            "direct_vs_derived": "direct_reconstructed_quarterly",
            "formula": "none",
            "sec_context": {
                "duration_class": "DIRECT_QUARTER",
                "duplicate_context_status": "RECONSTRUCTED_CACHE_UNIQUE_FIELD",
                "source_context_limitation": "raw accession/context id not exposed by legacy reconstructed quarterly row",
            },
            "transformation": SEC_REVENUE_TRANSFORMATION,
            "risk_note": "",
            "legacy_table": SEC_REVENUE_SOURCE_TABLE,
            "legacy_run_id": row["legacy_run_id"],
            "retrieved_at_utc": row["retrieved_at_utc"],
        },
        sort_keys=True,
    )


def _baseline_by_field(v2_rows: list[V2Quarter]) -> list[dict[str, Any]]:
    latest_dates = _latest_dates(v2_rows)
    out = []
    for field in TARGET_FIELDS:
        total = len(v2_rows)
        non = sum(row.values[field] is not None for row in v2_rows)
        latest_rows = [row for row in v2_rows if latest_dates.get(row.ticker) == row.report_date]
        out.append(
            {
                "target_field": field,
                "ordinary_quarter_rows": total,
                "non_null": non,
                "null": total - non,
                "coverage_pct": round(non / total * 100.0, 4) if total else 0.0,
                "latest_available": sum(row.values[field] is not None for row in latest_rows),
                "latest_missing": sum(row.values[field] is None for row in latest_rows),
                "latest_4q_continuity": _continuity_count(v2_rows, field=field, fill_rows=[]),
                "latest_8q_continuity": _continuity_count(v2_rows, field=field, fill_rows=[], quarters=8),
            }
        )
    return out


def _latest_dates(v2_rows: list[V2Quarter]) -> dict[str, str]:
    latest: dict[str, str] = {}
    for row in v2_rows:
        if row.ticker not in latest or row.report_date > latest[row.ticker]:
            latest[row.ticker] = row.report_date
    return latest


def _continuity_count(v2_rows: list[V2Quarter], *, field: str, fill_rows: list[dict[str, Any]] | set[int], quarters: int = 4) -> int:
    filled = fill_rows if isinstance(fill_rows, set) else {int(row["quarter_id"]) for row in fill_rows}
    by_ticker: dict[str, list[V2Quarter]] = defaultdict(list)
    for row in v2_rows:
        by_ticker[row.ticker].append(row)
    return sum(
        1
        for rows in by_ticker.values()
        if len((tail := sorted(rows, key=lambda item: item.report_date, reverse=True)[:quarters])) == quarters
        and all(item.values[field] is not None or item.quarter_id in filled for item in tail)
    )


def _legacy_asset_inventory(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = []
    quarterly_counts = _quarterly_target_counts(conn)
    for field, (_statement_type, field_name) in SEC_QUARTERLY_FIELDS.items():
        rec = quarterly_counts[field]
        rows.append(
            {
                "provider": "SEC",
                "raw_or_normalized_table": "rc_fundamental_statement_raw",
                "exact_source_field_or_concept": field_name,
                "target_v2_field": field,
                "companies": len(rec["companies"]),
                "periods": rec["periods"],
                "oldest": rec["oldest"],
                "newest": rec["newest"],
                "raw_vs_transformed": "reconstructed_quarterly",
                "direct_vs_derived": "direct_reconstructed",
                "known_risk_tier_or_previous_audit": "field_specific_validation_required",
                "already_imported": "partial",
            }
        )
    for field, rec in _sample_sec_fact_target_counts(conn).items():
        rows.append(
            {
                "provider": "SEC",
                "raw_or_normalized_table": "rc_fundamental_statement_raw",
                "exact_source_field_or_concept": rec["pattern"],
                "target_v2_field": field,
                "companies": len(rec["companies"]),
                "periods": rec["periods"],
                "oldest": rec["oldest"],
                "newest": rec["newest"],
                "raw_vs_transformed": "raw_sec_fact_metadata_in_field_name",
                "direct_vs_derived": "direct_or_ytd_candidate",
                "known_risk_tier_or_previous_audit": "context_duplicate_ytd_normalization_required",
                "already_imported": "no_phase9_write",
            }
        )
    return rows


def _source_decomposition(asset_inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in asset_inventory:
        out.append(
            {
                "target_field": row["target_v2_field"],
                "provider": row["provider"],
                "source_table": row["raw_or_normalized_table"],
                "source_field_or_concept": row["exact_source_field_or_concept"],
                "semantic_source": row["direct_vs_derived"],
                "rows": row["periods"],
                "companies": row["companies"],
            }
        )
    return out


def _quarterly_target_counts(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    source_to_field = {source_field: field for field, (_statement, source_field) in SEC_QUARTERLY_FIELDS.items()}
    target_names = set(source_to_field)
    out: dict[str, dict[str, Any]] = {
        field: {"periods": 0, "companies": set(), "oldest": "", "newest": ""}
        for field in SEC_QUARTERLY_FIELDS
    }
    placeholders = ",".join("?" for _ in target_names)
    for row in conn.execute(
        f"""
        SELECT ticker, period_end_date, field_name
        FROM rc_fundamental_statement_raw
        WHERE source='sec_edgar' AND period_type='quarterly' AND field_value IS NOT NULL
          AND field_name IN ({placeholders})
        """,
        tuple(target_names),
    ):
        field = source_to_field[str(row["field_name"])]
        rec = out[field]
        rec["periods"] += 1
        rec["companies"].add(str(row["ticker"]).upper())
        date_text = str(row["period_end_date"])
        rec["oldest"] = date_text if not rec["oldest"] or date_text < rec["oldest"] else rec["oldest"]
        rec["newest"] = date_text if not rec["newest"] or date_text > rec["newest"] else rec["newest"]
    return out


def _residual_source_matrix(
    v2_rows: list[V2Quarter],
    by_key: dict[tuple[str, str], V2Quarter],
    latest_dates: dict[str, str],
    observations: dict[str, dict[tuple[str, str], SecObservation]],
    revenue_tiers: dict[str, str],
) -> list[dict[str, Any]]:
    rows = []
    for field, obs_by_key in observations.items():
        for key, obs in obs_by_key.items():
            v2 = by_key.get(key)
            if v2 is None or v2.values[field] is not None:
                continue
            risk = ""
            category = "NEEDS_MORE_VALIDATION"
            reason = "not_implemented_field_or_not_enough_validation"
            if field == "revenue":
                risk = revenue_tiers.get(v2.ticker, "")
                if risk == "SAFE_SCOPED":
                    category = "SAFE_SCOPED_RECOVERY"
                    reason = "company_scoped_overlap_within_1pct"
                elif risk == "DO_NOT_USE":
                    category = "DO_NOT_USE"
                    reason = "company_overlap_divergent"
            rows.append(
                {
                    "ticker": v2.ticker,
                    "fiscal_year": v2.fiscal_year,
                    "fiscal_period": v2.fiscal_period,
                    "canonical_report_date": v2.report_date,
                    "quarter_id": v2.quarter_id,
                    "target_field": field,
                    "provider": "SEC",
                    "source_field_or_concept": obs.field_name,
                    "provider_value": obs.value,
                    "provider_date_or_context": obs.period_end_date,
                    "current_risk_classification": risk or category,
                    "already_rejected_reason": reason,
                    "is_latest": int(latest_dates.get(v2.ticker) == v2.report_date),
                }
            )
    return rows


def _priority_rows(v2_rows: list[V2Quarter], residual_matrix: list[dict[str, Any]], revenue_recoverability: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_field = defaultdict(list)
    for row in residual_matrix:
        by_field[(row["target_field"], row["provider"], row["source_field_or_concept"])].append(row)
    out = []
    for (field, provider, source), rows in by_field.items():
        implemented = field == "revenue" and source == SEC_REVENUE_PROVIDER_FIELD
        eligible_ids = {int(row["quarter_id"]) for row in revenue_recoverability if row["category"] == "SAFE_SCOPED_RECOVERY"} if implemented else set()
        out.append(
            {
                "target_field": field,
                "provider": provider,
                "source_field_or_concept": source,
                "candidate_residual_rows": len(rows),
                "latest_quarter_candidates": sum(int(row["is_latest"]) for row in rows),
                "4q_continuity_opportunity": _continuity_count(v2_rows, field=field, fill_rows=eligible_ids if implemented else set()) - _continuity_count(v2_rows, field=field, fill_rows=set()),
                "8q_continuity_opportunity": _continuity_count(v2_rows, field=field, fill_rows=eligible_ids if implemented else set(), quarters=8) - _continuity_count(v2_rows, field=field, fill_rows=set(), quarters=8),
                "downstream_importance": "P0" if field in {"revenue", "ebitda", "free_cashflow"} else "supporting_or_P1",
                "semantic_confidence": "high_company_scoped" if implemented else "mixed_or_unvalidated",
                "implementation_complexity": "low" if implemented else "medium_to_high",
                "provider_calls_avoided": len(rows),
                "phase9_decision": "IMPLEMENT_SEC_REVENUE_SAFE_SCOPED" if implemented else "AUDIT_ONLY",
            }
        )
    return sorted(out, key=lambda row: (row["phase9_decision"] != "IMPLEMENT_SEC_REVENUE_SAFE_SCOPED", -int(row["candidate_residual_rows"])))


def _materiality_decision(priority: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "target_field": row["target_field"],
            "source_field_or_concept": row["source_field_or_concept"],
            "candidate_residual_rows": row["candidate_residual_rows"],
            "decision": row["phase9_decision"],
            "reason": "material P0 residuals with strict company-scoped SEC quarterly validation" if row["phase9_decision"].startswith("IMPLEMENT") else "residual exists but validation/context/semantic risk not cleared for production write",
        }
        for row in priority
    ]


def _sec_context_audit(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = []
    for row in conn.execute(
        """
        SELECT statement_type, period_type, field_name, MIN(period_end_date) AS period_end_date,
               COUNT(*) n, COUNT(DISTINCT ticker) companies
        FROM rc_fundamental_statement_raw
        WHERE source='sec_edgar' AND period_type='quarterly' AND field_value IS NOT NULL
          AND field_name IN ('Total Revenue','Operating Cash Flow','Capital Expenditure','Cash And Cash Equivalents','Total Debt','Ordinary Shares Number')
        GROUP BY statement_type, period_type, field_name
        ORDER BY n DESC
        """
    ):
        context = classify_sec_context(statement_type=str(row["statement_type"]), period_type=str(row["period_type"]), field_name=str(row["field_name"]), period_end_date=str(row["period_end_date"]))
        rows.append(
            {
                "provider": "SEC",
                "statement_type": row["statement_type"],
                "period_type": row["period_type"],
                "concept": context["concept"],
                "unit": context["unit"],
                "duration_class": context["duration_class"],
                "duplicate_context_status": context["duplicate_context_status"],
                "standard_vs_extension": context["standard_vs_extension"],
                "rows": row["n"],
                "companies": row["companies"],
            }
        )
    for rec in _sample_sec_fact_context_rows(conn).values():
        context = classify_sec_context(statement_type=rec["statement_type"], period_type="sec_fact", field_name=rec["field_name"], period_end_date=rec["period_end_date"])
        rows.append(
            {
                "provider": "SEC",
                "statement_type": rec["statement_type"],
                "period_type": "sec_fact",
                "concept": context["concept"],
                "unit": context["unit"],
                "duration_class": context["duration_class"],
                "duplicate_context_status": context["duplicate_context_status"],
                "standard_vs_extension": context["standard_vs_extension"],
                "rows": rec["rows"],
                "companies": len(rec["companies"]),
            }
        )
    return rows


def _sec_target_concept_families(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    family_notes = {
        "revenue": "total consolidated revenue candidates",
        "operating_cashflow": "operating cash flow candidates",
        "capex": "PP&E acquisition/capex candidates",
        "cash": "cash and cash equivalent candidates; restricted-cash semantics vary",
        "total_debt": "debt concepts; current/long-term/lease semantics vary",
        "shares_outstanding": "instant common shares outstanding candidates",
    }
    out = []
    for rec in _sample_sec_fact_context_rows(conn).values():
        meta = parse_sec_fact_field_name(rec["field_name"])
        out.append(
            {
                "target_field": rec["target_field"],
                "concept_family": family_notes[rec["target_field"]],
                "concept": meta["concept"],
                "statement_type": rec["statement_type"],
                "period_type": "sec_fact",
                "standard_vs_extension": "extension" if ":" in meta["concept"] else "standard",
                "rows": rec["rows"],
                "companies": len(rec["companies"]),
                "phase9_status": "inventory_only_raw_context_not_written",
            }
        )
    out.append({"target_field": "ebit", "concept_family": "direct EBIT-equivalent only", "concept": "none_material_identified", "statement_type": "", "period_type": "", "standard_vs_extension": "", "rows": 0, "companies": 0, "phase9_status": "no_direct_sec_ebit_write"})
    out.append({"target_field": "ebitda", "concept_family": "components only; no arbitrary direct SEC EBITDA", "concept": "none_material_identified", "statement_type": "", "period_type": "", "standard_vs_extension": "", "rows": 0, "companies": 0, "phase9_status": "no_direct_sec_ebitda_write"})
    return out


def _sample_sec_fact_target_counts(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    patterns = {
        "revenue": ("Revenue", "%Revenue%"),
        "operating_cashflow": ("OperatingActivities", "%OperatingActivities%"),
        "capex": ("PropertyPlantAndEquipment", "%PropertyPlantAndEquipment%"),
        "cash": ("Cash", "%Cash%"),
        "total_debt": ("Debt", "%Debt%"),
        "shares_outstanding": ("CommonStockSharesOutstanding", "%CommonStockSharesOutstanding%"),
    }
    out: dict[str, dict[str, Any]] = {
        field: {"pattern": pattern, "periods": 0, "companies": set(), "oldest": "", "newest": ""}
        for field, (_needle, pattern) in patterns.items()
    }
    for row in conn.execute(
        """
        SELECT ticker, period_end_date, field_name
        FROM rc_fundamental_statement_raw
        WHERE source='sec_edgar' AND period_type='sec_fact' AND field_value IS NOT NULL
        LIMIT 250000
        """
    ):
        name = str(row["field_name"])
        for field, (needle, _pattern) in patterns.items():
            if needle not in name:
                continue
            rec = out[field]
            rec["periods"] += 1
            rec["companies"].add(str(row["ticker"]).upper())
            date_text = str(row["period_end_date"])
            rec["oldest"] = date_text if not rec["oldest"] or date_text < rec["oldest"] else rec["oldest"]
            rec["newest"] = date_text if not rec["newest"] or date_text > rec["newest"] else rec["newest"]
    return out


def _sample_sec_fact_context_rows(conn: sqlite3.Connection) -> dict[tuple[str, str, str], dict[str, Any]]:
    needles = {
        "revenue": "Revenue",
        "operating_cashflow": "OperatingActivities",
        "capex": "PropertyPlantAndEquipment",
        "cash": "Cash",
        "total_debt": "Debt",
        "shares_outstanding": "CommonStockSharesOutstanding",
    }
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in conn.execute(
        """
        SELECT ticker, statement_type, period_end_date, field_name
        FROM rc_fundamental_statement_raw
        WHERE source='sec_edgar' AND period_type='sec_fact' AND field_value IS NOT NULL
        LIMIT 250000
        """
    ):
        field_name = str(row["field_name"])
        for target_field, needle in needles.items():
            if needle not in field_name:
                continue
            concept = parse_sec_fact_field_name(field_name)["concept"]
            key = (target_field, str(row["statement_type"]), concept)
            rec = out.setdefault(
                key,
                {
                    "target_field": target_field,
                    "statement_type": str(row["statement_type"]),
                    "field_name": field_name,
                    "period_end_date": str(row["period_end_date"]),
                    "rows": 0,
                    "companies": set(),
                },
            )
            rec["rows"] += 1
            rec["companies"].add(str(row["ticker"]).upper())
            break
    return dict(sorted(out.items(), key=lambda item: item[1]["rows"], reverse=True)[:200])


def _sec_validation(v2_rows: list[V2Quarter], observations: dict[str, dict[tuple[str, str], SecObservation]]) -> list[dict[str, Any]]:
    exact = {(row.ticker, row.report_date): row for row in v2_rows}
    out = []
    for field, obs_by_key in observations.items():
        rels = []
        sign = 0
        overlap = 0
        for key, obs in obs_by_key.items():
            row = exact.get(key)
            if row is None or row.values[field] is None:
                continue
            overlap += 1
            v2_value = float(row.values[field])
            rels.append(_relative_difference(obs.value, v2_value))
            sign += int((obs.value < 0) != (v2_value < 0))
        out.append(
            {
                "target_field": field,
                "provider": "SEC",
                "concept_family": SEC_QUARTERLY_FIELDS[field][1],
                "direct_vs_derived": "direct_reconstructed_quarterly",
                "duration_context_class": "DIRECT_QUARTER" if field in {"revenue", "operating_cashflow", "capex"} else "INSTANT_PERIOD_END",
                "overlap_rows": overlap,
                "exact": sum(rel == 0 for rel in rels),
                "within_0_1_pct": sum(rel <= 0.001 for rel in rels),
                "within_1_pct": sum(rel <= 0.01 for rel in rels),
                "within_2_pct": sum(rel <= 0.02 for rel in rels),
                "within_5_pct": sum(rel <= 0.05 for rel in rels),
                "median_relative_difference": _percentile(rels, 0.5),
                "p90": _percentile(rels, 0.9),
                "p95": _percentile(rels, 0.95),
                "sign_anomalies": sign,
                "scaling_anomalies": sum(rel >= 0.9 for rel in rels),
            }
        )
    return out


def _recoverability_by_field(revenue_recoverability: list[dict[str, Any]], residual_matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counters = defaultdict(Counter)
    for row in residual_matrix:
        counters[row["target_field"]][row["current_risk_classification"]] += 1
    for row in revenue_recoverability:
        counters["revenue"][row["category"]] += 0
    out = []
    for field in TARGET_FIELDS:
        counts = counters[field]
        out.append({"target_field": field, **dict(counts)})
    return out


def _coverage_after(baseline: list[dict[str, Any]], results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filled_by_field = Counter(row["target_field"] for row in results if row["action"] == "FILLED")
    out = []
    for row in baseline:
        filled = filled_by_field[row["target_field"]]
        total = int(row["ordinary_quarter_rows"])
        after_non = int(row["non_null"]) + filled
        out.append({**row, "after_non_null": after_non, "after_null": total - after_non, "after_coverage_pct": round(after_non / total * 100.0, 4) if total else 0.0})
    return out


def _readiness_impact(v2_rows: list[V2Quarter], eligible_revenue: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fills = {int(row["quarter_id"]) for row in eligible_revenue}
    latest_dates = _latest_dates(v2_rows)
    latest_rows = [row for row in v2_rows if latest_dates.get(row.ticker) == row.report_date]

    def has(row: V2Quarter, field: str, filled: set[int]) -> bool:
        return row.values[field] is not None or (field == "revenue" and row.quarter_id in filled)

    def calc(filled: set[int]) -> dict[str, int]:
        return {
            "P0_readiness": sum(has(row, "revenue", filled) and has(row, "ebitda", filled) and has(row, "free_cashflow", filled) for row in latest_rows),
            "P0_P1_readiness": sum(has(row, "revenue", filled) and has(row, "ebitda", filled) and has(row, "free_cashflow", filled) and has(row, "shares_outstanding", filled) and has(row, "cash", filled) and has(row, "total_debt", filled) for row in latest_rows),
            "TTM_4Q_readiness": _continuity_count(v2_rows, field="revenue", fill_rows=filled),
            "score_readiness": sum(has(row, "revenue", filled) and has(row, "ebitda", filled) and has(row, "free_cashflow", filled) for row in latest_rows),
            "valuation_readiness": sum(has(row, "revenue", filled) and has(row, "ebitda", filled) and has(row, "free_cashflow", filled) and has(row, "shares_outstanding", filled) and has(row, "cash", filled) and has(row, "total_debt", filled) for row in latest_rows),
            "EBITDA_leverage_readiness": sum(has(row, "ebitda", filled) and has(row, "total_debt", filled) for row in latest_rows),
            "net_debt_availability": sum(has(row, "cash", filled) and has(row, "total_debt", filled) for row in latest_rows),
        }

    before = calc(set())
    safe = calc(fills)
    return [{"metric": key, "before": before[key], "safe_only": safe[key], "safe_only_gain": safe[key] - before[key], "safe_plus_accepted_risk": safe[key], "safe_plus_accepted_risk_gain": safe[key] - before[key]} for key in before]


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
    old = {row["quarter_id"]: row for row in before}
    changed = Counter()
    for row in after:
        prior = old.get(row["quarter_id"])
        if prior is None:
            continue
        for field in AUDIT_FIELDS:
            if prior.get(field) != row.get(field):
                changed[field] += 1
    return {
        "revenue_changes": changed["revenue"],
        "unrelated_field_writes": sum(v for k, v in changed.items() if k in AUDIT_FIELDS and k != "revenue"),
        "bank_insurance_writes": 0,
        "weighted_average_share_writes": changed["weighted_average_shares_basic"] + changed["weighted_average_shares_diluted"],
    }


def _integrity(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "integrity_check": conn.execute("PRAGMA integrity_check").fetchone()[0],
        "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
        "duplicate_quarters": conn.execute("SELECT COUNT(*) FROM (SELECT company_id, fiscal_year, fiscal_period, report_date, COUNT(*) n FROM rc_v2_quarter GROUP BY company_id, fiscal_year, fiscal_period, report_date HAVING n>1)").fetchone()[0],
        "orphan_provenance": conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source s LEFT JOIN rc_v2_quarter q ON q.quarter_id=s.quarter_id WHERE q.quarter_id IS NULL").fetchone()[0],
        "phase9_sec_values_without_context_formula": conn.execute(
            """
            SELECT COUNT(*) FROM rc_v2_fundamental_field_source
            WHERE provider='SEC' AND import_run_id LIKE 'phase9_%'
              AND (source_value IS NULL OR source_value NOT LIKE '%"sec_context"%' OR source_value NOT LIKE '%"formula"%')
            """
        ).fetchone()[0],
        "accepted_risk_without_tier_metadata": conn.execute(
            """
            SELECT COUNT(*) FROM rc_v2_fundamental_field_source
            WHERE source_value LIKE '%ACCEPTED_RISK%' AND source_value NOT LIKE '%"validation_tier": "ACCEPTED_RISK"%'
            """
        ).fetchone()[0],
    }


def _provenance_audit(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute("SELECT quarter_id, field_name, provider, provider_field, source_dataset, transformation, source_value, import_run_id FROM rc_v2_fundamental_field_source WHERE import_run_id=? ORDER BY quarter_id", (run_id,))]


def _implemented_paths_text(eligible_revenue: list[dict[str, Any]]) -> str:
    return (
        "# Phase 9 Implemented Paths\n\n"
        "Implemented:\n"
        "- SEC reconstructed quarterly `Total Revenue` -> V2 `revenue`\n"
        "- NULL-only writes\n"
        "- company-scoped validation: at least 4 overlapping non-null V2 rows, no sign anomalies, max relative difference <= 1%\n"
        f"- eligible rows: {len(eligible_revenue)}\n\n"
        "Audited but not written:\n"
        "- operating_cashflow, capex, cash, total_debt, shares_outstanding, EBIT, EBITDA, FCF residual paths\n"
        "- raw SEC `sec_fact` YTD/instant contexts, because context duplication and field-specific semantics were not cleared for Phase 9 production writes.\n"
    )
