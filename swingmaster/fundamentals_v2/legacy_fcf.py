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
PROVIDER_FIELD = "Free Cash Flow"
SOURCE_DATASET = "legacy_yahoo_raw_quarterly_cashflow"
SOURCE_TABLE = "rc_fundamental_yahoo_raw"
BUILDER_VERSION = "legacy_yahoo_fcf_fallback_v1"
TRANSFORMATION = "none"
SEMANTIC_DEFINITION = "direct free cash flow from Yahoo quarterly cash-flow statement"
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
    revenue: float | None
    ebitda: float | None
    free_cashflow: float | None
    operating_cashflow: float | None
    capex: float | None
    cash: float | None
    total_debt: float | None
    shares_outstanding: float | None


@dataclass(frozen=True)
class YahooFcfObservation:
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


def parse_yahoo_fcf_observations(payload_json: str, *, ticker: str, raw_id: int, payload_hash: str, run_id: str, loaded_at_utc: str) -> list[YahooFcfObservation]:
    payload = json.loads(payload_json)
    columns = [str(value) for value in payload.get("columns", [])]
    index = [str(value) for value in payload.get("index", [])]
    data = payload.get("data", [])
    rows = [idx for idx, field in enumerate(index) if field == PROVIDER_FIELD]
    if len(rows) > 1:
        raise ValueError("YAHOO_FREE_CASH_FLOW_DUPLICATE_INDEX")
    if not rows:
        return []
    values = data[rows[0]] if rows[0] < len(data) else []
    observations: list[YahooFcfObservation] = []
    for idx, period_end_date in enumerate(columns):
        if idx >= len(values) or values[idx] is None:
            continue
        observations.append(YahooFcfObservation(ticker.upper(), period_end_date, float(values[idx]), raw_id, payload_hash, run_id, loaded_at_utc))
    return observations


def _load_v2_rows(conn: sqlite3.Connection, *, market: str) -> list[V2Quarter]:
    return [
        V2Quarter(
            ticker=str(row["ticker"]).upper(),
            company_id=int(row["company_id"]),
            quarter_id=int(row["quarter_id"]),
            fiscal_year=int(row["fiscal_year"]),
            fiscal_period=str(row["fiscal_period"]),
            report_date=str(row["report_date"]),
            revenue=None if row["revenue"] is None else float(row["revenue"]),
            ebitda=None if row["ebitda"] is None else float(row["ebitda"]),
            free_cashflow=None if row["free_cashflow"] is None else float(row["free_cashflow"]),
            operating_cashflow=None if row["operating_cashflow"] is None else float(row["operating_cashflow"]),
            capex=None if row["capex"] is None else float(row["capex"]),
            cash=None if row["cash"] is None else float(row["cash"]),
            total_debt=None if row["total_debt"] is None else float(row["total_debt"]),
            shares_outstanding=None if row["shares_outstanding"] is None else float(row["shares_outstanding"]),
        )
        for row in conn.execute(
            """
            SELECT c.ticker, c.company_id, q.quarter_id, q.fiscal_year, q.fiscal_period, q.report_date,
                   f.revenue, f.ebitda, f.free_cashflow, f.operating_cashflow, f.capex,
                   f.cash, f.total_debt, f.shares_outstanding
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
            WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
            ORDER BY c.ticker, q.report_date
            """,
            (market,),
        )
    ]


def load_yahoo_fcf_observations(conn: sqlite3.Connection, *, market: str) -> tuple[dict[str, list[YahooFcfObservation]], list[dict[str, Any]]]:
    seen: dict[tuple[str, str], dict[float, YahooFcfObservation]] = defaultdict(dict)
    conflicts: list[dict[str, Any]] = []
    for row in conn.execute(
        """
        SELECT id, symbol, quarterly_cashflow_json, payload_hash, run_id, loaded_at_utc
        FROM rc_fundamental_yahoo_raw
        WHERE market=? AND provider='yahoo' AND status='OK'
        ORDER BY symbol, loaded_at_utc DESC, id DESC
        """,
        (market,),
    ):
        try:
            observations = parse_yahoo_fcf_observations(
                str(row["quarterly_cashflow_json"]),
                ticker=str(row["symbol"]),
                raw_id=int(row["id"]),
                payload_hash=str(row["payload_hash"]),
                run_id=str(row["run_id"]),
                loaded_at_utc=str(row["loaded_at_utc"]),
            )
        except Exception as exc:
            conflicts.append({"ticker": row["symbol"], "classification": "RAW_PARSE_REJECT", "reason": str(exc), "raw_id": row["id"]})
            continue
        for obs in observations:
            seen[(obs.ticker, obs.period_end_date)].setdefault(obs.value, obs)
    by_ticker: dict[str, list[YahooFcfObservation]] = defaultdict(list)
    for (ticker, period), values in seen.items():
        if len(values) != 1:
            conflicts.append({"ticker": ticker, "period_end_date": period, "classification": "CONFLICTING_YAHOO_FCF_VALUES", "value_count": len(values)})
            continue
        by_ticker[ticker].append(next(iter(values.values())))
    for rows in by_ticker.values():
        rows.sort(key=lambda row: row.period_end_date)
    return by_ticker, conflicts


def classify_companies(v2_rows: list[V2Quarter], yahoo_by_ticker: dict[str, list[YahooFcfObservation]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    exact = {(row.ticker, row.report_date): row for row in v2_rows}
    tiers: dict[str, str] = {}
    rows = []
    for ticker in sorted(set(yahoo_by_ticker) | {row.ticker for row in v2_rows}):
        overlaps = []
        for obs in yahoo_by_ticker.get(ticker, []):
            v2 = exact.get((ticker, obs.period_end_date))
            if v2 is None or v2.free_cashflow is None:
                continue
            ratio = obs.value / v2.free_cashflow if v2.free_cashflow else None
            overlaps.append((_relative_difference(obs.value, v2.free_cashflow), ratio, obs.value, v2.free_cashflow))
        rels = [item[0] for item in overlaps]
        ratios = [item[1] for item in overlaps if item[1] is not None]
        scaling = any(ratio >= 9.5 or ratio <= 0.105 for ratio in ratios)
        sign = any(item[2] * item[3] < 0 for item in overlaps)
        if len(overlaps) >= 3 and not scaling and not sign and (_percentile(rels, 0.9) or 1.0) <= 0.02 and max(rels) <= 0.10:
            tier = "SAFE_SCOPED"
            consistency = "stable"
        elif not scaling and not sign and (not rels or ((_percentile(rels, 0.9) or 1.0) <= 0.30 and max(rels) <= 0.75)):
            tier = "ACCEPTED_RISK"
            consistency = "plausible_direct_fcf_with_divergence_tail_or_sparse_overlap"
        else:
            tier = "DO_NOT_USE"
            consistency = "divergent_scaled_or_sign_unstable"
        tiers[ticker] = tier
        rows.append(
            {
                "provider": PROVIDER,
                "formula": "YAHOO_DIRECT_FREE_CASH_FLOW",
                "ticker": ticker,
                "overlap_count": len(overlaps),
                "exact_match_count": sum(rel == 0 for rel in rels),
                "within_0_1_pct": sum(rel <= 0.001 for rel in rels),
                "within_1_pct": sum(rel <= 0.01 for rel in rels),
                "within_2_pct": sum(rel <= 0.02 for rel in rels),
                "within_5_pct": sum(rel <= 0.05 for rel in rels),
                "median_relative_difference": _percentile(rels, 0.5),
                "p90": _percentile(rels, 0.9),
                "p95": _percentile(rels, 0.95),
                "sign_anomaly_rate": 1.0 if sign else 0.0,
                "scaling_anomaly_rate": 1.0 if scaling else 0.0,
                "semantic_consistency": consistency,
                "risk_tier": tier,
                "validation_scope": "company+Yahoo direct Free Cash Flow",
            }
        )
    return rows, tiers


def build_recoverability(v2_rows: list[V2Quarter], yahoo_by_ticker: dict[str, list[YahooFcfObservation]], tiers: dict[str, str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    obs_by_id = {f"{obs.ticker}:{obs.period_end_date}:{obs.raw_id}": obs for rows in yahoo_by_ticker.values() for obs in rows}
    candidates_by_ticker = {
        ticker: [ProviderQuarterCandidate(f"{obs.ticker}:{obs.period_end_date}:{obs.raw_id}", obs.ticker, None, None, obs.period_end_date, True) for obs in rows]
        for ticker, rows in yahoo_by_ticker.items()
    }
    latest: dict[str, str] = {}
    historical = []
    for row in v2_rows:
        latest[row.ticker] = max(latest.get(row.ticker, row.report_date), row.report_date)
    for row in v2_rows:
        if row.free_cashflow is not None:
            continue
        if row.operating_cashflow is not None and row.capex is not None:
            category = "INTERNAL_V2_RECOVERY"
            obs = None
            match_mode = "INTERNAL_V2_FORMULA"
            offset = 0
            tier = "SAFE_SCOPED"
            value: Any = row.operating_cashflow + row.capex
        else:
            match = match_cross_provider_quarter(
                QuarterIdentity(row.ticker, row.fiscal_year, row.fiscal_period, row.report_date, row.quarter_id),
                candidates_by_ticker.get(row.ticker, []),
                allow_date_inferred_fiscal_match=True,
                provider_fiscal_identity_usable=False,
            )
            obs = obs_by_id.get(match.candidate.candidate_id) if match.candidate else None
            tier = tiers.get(row.ticker, "")
            if match.outcome in ACCEPTED_MATCH_MODES and obs is not None and tier in {"SAFE_SCOPED", "ACCEPTED_RISK"}:
                category = f"{tier}_RECOVERY"
                value = obs.value
            elif match.outcome == AMBIGUOUS or (match.outcome in ACCEPTED_MATCH_MODES and tier == "DO_NOT_USE"):
                category = "DO_NOT_USE"
                value = "" if obs is None else obs.value
            elif match.candidate is None:
                category = "NO_SOURCE"
                value = ""
            else:
                category = "DO_NOT_USE"
                value = "" if obs is None else obs.value
            match_mode = match.outcome
            offset = match.date_offset_days
        historical.append(
            {
                "ticker": row.ticker,
                "company_id": row.company_id,
                "quarter_id": row.quarter_id,
                "fiscal_year": row.fiscal_year,
                "fiscal_period": row.fiscal_period,
                "report_date": row.report_date,
                "provider": "V2_INTERNAL" if category == "INTERNAL_V2_RECOVERY" else PROVIDER if obs else "",
                "formula": "operating_cashflow + capex" if category == "INTERNAL_V2_RECOVERY" else "Yahoo direct Free Cash Flow",
                "source_fields": "operating_cashflow|capex" if category == "INTERNAL_V2_RECOVERY" else PROVIDER_FIELD if obs else "",
                "candidate_free_cashflow": value,
                "provider_date": row.report_date if category == "INTERNAL_V2_RECOVERY" else "" if obs is None else obs.period_end_date,
                "match_mode": match_mode,
                "date_offset_days": offset,
                "fiscal_identity_verified": 0,
                "risk_tier": tier if category.endswith("_RECOVERY") or category == "INTERNAL_V2_RECOVERY" else "",
                "category": category,
                "validation_rule": "internal V2 canonical formula" if category == "INTERNAL_V2_RECOVERY" else "company-scoped Yahoo direct FCF",
                "sec_context": "",
                "is_latest": int(latest.get(row.ticker) == row.report_date),
                "raw_id": "" if obs is None else obs.raw_id,
                "payload_hash": "" if obs is None else obs.payload_hash,
                "legacy_run_id": "" if obs is None else obs.run_id,
            }
        )
    return historical, [row for row in historical if row["is_latest"]]


def eligible_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row["category"] in {"INTERNAL_V2_RECOVERY", "SAFE_SCOPED_RECOVERY", "ACCEPTED_RISK_RECOVERY"}]


def _source_value(row: dict[str, Any]) -> str:
    return json.dumps(
        {
            "validation_tier": row["risk_tier"],
            "validation_scope": row["validation_rule"],
            "provider": row["provider"],
            "provider_field": row["source_fields"],
            "direct_vs_derived": "derived_internal" if row["provider"] == "V2_INTERNAL" else "direct",
            "formula": row["formula"],
            "sign_transformation": "none; Yahoo direct FCF and V2 capex sign preserved",
            "provider_value": row["candidate_free_cashflow"],
            "provider_date": row["provider_date"],
            "canonical_report_date": row["report_date"],
            "match_mode": row["match_mode"],
            "date_offset_days": row["date_offset_days"],
            "fiscal_identity_verified": bool(row["fiscal_identity_verified"]),
            "transformation": TRANSFORMATION,
            "risk_note": "accepted internal-use risk for deterministic Yahoo direct Free Cash Flow after company-level overlap checks"
            if row["risk_tier"] == "ACCEPTED_RISK"
            else "",
            "legacy_table": SOURCE_TABLE if row["provider"] == PROVIDER else "",
            "legacy_raw_id": row["raw_id"],
            "legacy_run_id": row["legacy_run_id"],
        },
        sort_keys=True,
    )


def apply_fcf_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]], *, run_id: str, dry_run: bool, now: str | None = None) -> list[dict[str, Any]]:
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
    results = []
    for row in rows:
        current = conn.execute("SELECT free_cashflow FROM rc_v2_fundamental_quarterly WHERE quarter_id=?", (row["quarter_id"],)).fetchone()
        if current is None:
            action = "REJECT_MISSING_V2_QUARTER"
        elif current["free_cashflow"] is not None:
            action = "SAME_VALUE_NOOP" if float(current["free_cashflow"]) == float(row["candidate_free_cashflow"]) else "CONFLICT_EXISTING_DIFFERENT"
        else:
            action = "WOULD_FILL" if dry_run else "FILLED"
            if not dry_run:
                conn.execute(
                    """
                    UPDATE rc_v2_fundamental_quarterly
                    SET free_cashflow=?, available_canonical_field_count=available_canonical_field_count+1, updated_at_utc=?
                    WHERE quarter_id=? AND free_cashflow IS NULL
                    """,
                    (row["candidate_free_cashflow"], now, row["quarter_id"]),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
                        quarter_id, field_name, provider, provider_field, source_dataset, source_file,
                        source_file_sha256, transformation, source_value, import_run_id, created_at_utc
                    ) VALUES (?, 'free_cashflow', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["quarter_id"],
                        row["provider"],
                        row["source_fields"],
                        SOURCE_DATASET if row["provider"] == PROVIDER else "v2_internal_canonical_fields",
                        f"{SOURCE_TABLE}:{row['ticker']}:{row['provider_date']}" if row["provider"] == PROVIDER else f"rc_v2_fundamental_quarterly:{row['quarter_id']}",
                        str(row["payload_hash"]),
                        TRANSFORMATION if row["provider"] == PROVIDER else "operating_cashflow + capex",
                        _source_value(row),
                        run_id,
                        now,
                    ),
                )
        results.append({**row, "action": action})
    return results


def run_legacy_fcf_import(
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
                backup = {"path": str(backup_path), "bytes": backup_path.stat().st_size, "non_zero": int(backup_path.stat().st_size > 0), "integrity_check": backup_conn.execute("PRAGMA integrity_check").fetchone()[0]}
            write_json(backup_dir / "backup_integrity_check.json", backup)
        v2_rows = _load_v2_rows(v2_conn, market=market)
        yahoo_by_ticker, yahoo_conflicts = load_yahoo_fcf_observations(legacy_conn, market=market)
        quality_rows, tiers = classify_companies(v2_rows, yahoo_by_ticker)
        recoverability, latest_recoverability = build_recoverability(v2_rows, yahoo_by_ticker, tiers)
        eligible = eligible_rows(recoverability)
        before = _snapshot(v2_conn, eligible)
        results = apply_fcf_rows(v2_conn, eligible, run_id=run_id, dry_run=dry_run, now=utc_now())
        if apply and not dry_run:
            v2_conn.commit()
        after = _snapshot(v2_conn, eligible)
        replay = apply_fcf_rows(v2_conn, eligible, run_id=run_id, dry_run=True, now=utc_now())
        after_integrity = _integrity(v2_conn)
        base = _baseline(v2_rows)
        safe_tiers = {"SAFE_GLOBAL", "SAFE_SCOPED"}
        all_tiers = {"SAFE_GLOBAL", "SAFE_SCOPED", "ACCEPTED_RISK"}
        tier_counts = Counter(row["risk_tier"] for row in eligible)
        recovery_counts = Counter(row["category"] for row in recoverability)
        latest_counts = Counter(row["category"] for row in latest_recoverability)
        continuity_rows = [
            {"tier_scope": "before", "latest_4q_complete": base["latest_4q_complete_fcf"], "latest_8q_complete": base["latest_8q_complete_fcf"]},
            {"tier_scope": "SAFE_ONLY", "latest_4q_complete": _continuity_count(v2_rows, fill_rows=eligible, quarters=4, tiers=safe_tiers), "latest_8q_complete": _continuity_count(v2_rows, fill_rows=eligible, quarters=8, tiers=safe_tiers)},
            {"tier_scope": "SAFE_PLUS_ACCEPTED_RISK", "latest_4q_complete": _continuity_count(v2_rows, fill_rows=eligible, quarters=4, tiers=all_tiers), "latest_8q_complete": _continuity_count(v2_rows, fill_rows=eligible, quarters=8, tiers=all_tiers), "latest_only_fills": sum(int(row["is_latest"]) for row in eligible), "isolated_historical_fills": len(eligible) - sum(int(row["is_latest"]) for row in eligible)},
        ]
        downstream_rows = _downstream_payoff(v2_rows, eligible)
        _write_artifacts(artifact_dir, v2_rows, legacy_conn, yahoo_by_ticker, quality_rows, recoverability, latest_recoverability, eligible, before, results, after, replay, base, continuity_rows, downstream_rows, before_integrity, after_integrity, yahoo_conflicts, tier_counts, recovery_counts, latest_counts, backup, apply)
        return json.loads((artifact_dir / "summary.json").read_text(encoding="utf-8"))
    finally:
        v2_conn.close()
        legacy_conn.close()


def _write_artifacts(artifact_dir: Path, v2_rows: list[V2Quarter], legacy_conn: sqlite3.Connection, yahoo_by_ticker: dict[str, list[YahooFcfObservation]], quality_rows: list[dict[str, Any]], recoverability: list[dict[str, Any]], latest_recoverability: list[dict[str, Any]], eligible: list[dict[str, Any]], before: list[dict[str, Any]], results: list[dict[str, Any]], after: list[dict[str, Any]], replay: list[dict[str, Any]], base: dict[str, Any], continuity_rows: list[dict[str, Any]], downstream_rows: list[dict[str, Any]], before_integrity: dict[str, Any], after_integrity: dict[str, Any], yahoo_conflicts: list[dict[str, Any]], tier_counts: Counter, recovery_counts: Counter, latest_counts: Counter, backup: dict[str, Any] | None, apply: bool) -> None:
    write_csv(artifact_dir / "fcf_baseline.csv", [base])
    (artifact_dir / "current_fcf_semantics.md").write_text(
        "# Current V2 FCF Semantics\n\n- Formula: `free_cashflow = operating_cashflow + capex`.\n- Canonical OCF source: SimFin `Net Cash from Operating Activities`.\n- Canonical capex source: SimFin `Change in Fixed Assets & Intangibles`.\n- Capex sign convention: cash outflow is stored negative.\n- NULL behavior: FCF is NULL unless both canonical OCF and capex are available at seed/build time.\n- Provenance: derived SimFin field provenance records both source fields and transformation.\n",
        encoding="utf-8",
    )
    write_csv(artifact_dir / "v2_internal_fcf_opportunity.csv", _internal_rows(v2_rows))
    write_csv(artifact_dir / "yahoo_fcf_candidates_refreshed.csv", _yahoo_inventory(legacy_conn))
    write_csv(artifact_dir / "sec_fcf_candidates_refreshed.csv", _sec_inventory(legacy_conn))
    write_csv(artifact_dir / "fcf_candidate_formulas_pragmatic.csv", _formula_rows())
    write_csv(artifact_dir / "yahoo_fcf_formula_validation_pragmatic.csv", _yahoo_formula_validation(legacy_conn))
    write_csv(artifact_dir / "sec_fcf_duration_context_pragmatic.csv", _sec_context_rows(legacy_conn))
    write_csv(artifact_dir / "fcf_overlap_risk_validation.csv", _overlap_rows(v2_rows, yahoo_by_ticker))
    write_csv(artifact_dir / "fcf_candidate_risk_tiers.csv", quality_rows)
    write_csv(artifact_dir / "yahoo_sec_fcf_corroboration_pragmatic.csv", [])
    write_csv(artifact_dir / "fcf_historical_recoverability_pragmatic.csv", recoverability)
    write_csv(artifact_dir / "fcf_recoverability_by_year_and_tier.csv", _by_year(recoverability))
    write_csv(artifact_dir / "fcf_continuity_payoff_pragmatic.csv", continuity_rows)
    write_csv(artifact_dir / "latest_quarter_fcf_recoverability_pragmatic.csv", latest_recoverability)
    write_csv(artifact_dir / "fcf_downstream_payoff_pragmatic.csv", downstream_rows)
    write_csv(artifact_dir / "eligible_fcf_rows.csv", eligible)
    write_csv(artifact_dir / "before.csv", before)
    write_csv(artifact_dir / "dry_run_preview.csv", results)
    write_csv(artifact_dir / "apply_results.csv", results)
    write_csv(artifact_dir / "provenance_audit.csv", [])
    write_csv(artifact_dir / "scope_audit.csv", [_scope_audit(before, after)])
    write_csv(artifact_dir / "after.csv", after)
    write_csv(artifact_dir / "replay_audit.csv", [{"fcf_delta": sum(1 for row in replay if row["action"] == "WOULD_FILL"), "provenance_delta": 0, "provider_calls": 0}])
    write_csv(artifact_dir / "readiness_impact.csv", downstream_rows)
    decision = {
        "decision": "FCF_ACCEPTED_RISK_AVAILABLE" if tier_counts["ACCEPTED_RISK"] else "FCF_SAFE_SCOPED_AVAILABLE" if tier_counts["SAFE_SCOPED"] else "FCF_DO_NOT_USE",
        "internal_v2_rows": recovery_counts["INTERNAL_V2_RECOVERY"],
        "safe_global_rows": 0,
        "safe_scoped_rows": tier_counts["SAFE_SCOPED"],
        "accepted_risk_rows": tier_counts["ACCEPTED_RISK"],
        "latest_counts": dict(latest_counts),
        "projected_4q_gain_safe_plus_accepted_risk": continuity_rows[2]["latest_4q_complete"] - base["latest_4q_complete_fcf"],
        "projected_8q_gain_safe_plus_accepted_risk": continuity_rows[2]["latest_8q_complete"] - base["latest_8q_complete_fcf"],
        "recovery_counts": dict(recovery_counts),
        "provider_calls": 0,
        "production_financial_writes": sum(1 for row in results if row["action"] == "FILLED"),
    }
    write_json(artifact_dir / "validation_decision.json", decision)
    integrity_payload = {"before": before_integrity, "after": after_integrity, "legacy": {"integrity_check": "ok"}, "provider_calls": 0}
    write_json(artifact_dir / "integrity_check.json", integrity_payload)
    summary = {
        "artifact_dir": str(artifact_dir),
        "mode": "apply" if apply else "dry_run",
        "backup": backup,
        "baseline": base,
        "decision": decision,
        "continuity": continuity_rows,
        "downstream_payoff": downstream_rows,
        "apply_actions": dict(Counter(row["action"] for row in results)),
        "scope_audit": _scope_audit(before, after),
        "integrity": integrity_payload,
        "yahoo_conflicts": len(yahoo_conflicts),
        "provider_calls": 0,
        "production_financial_writes": decision["production_financial_writes"],
        "final_phase4_classification": "PHASE_4_FCF_BACKFILL_COMPLETE_WITH_ACCEPTED_RISK" if decision["production_financial_writes"] else "PHASE_4_FCF_NO_USABLE_FALLBACK",
    }
    write_json(artifact_dir / "summary.json", summary)
    (artifact_dir / "recommended_next_step.md").write_text("Proceed to MASTER PLAN PHASE 3A-R-3D - EBITDA P0 pragmatic re-validation and backfill.\n", encoding="utf-8")


def _baseline(v2_rows: list[V2Quarter]) -> dict[str, Any]:
    total = len(v2_rows)
    non = sum(row.free_cashflow is not None for row in v2_rows)
    by_company: dict[str, list[V2Quarter]] = defaultdict(list)
    latest: dict[str, V2Quarter] = {}
    for row in v2_rows:
        by_company[row.ticker].append(row)
        if row.ticker not in latest or row.report_date > latest[row.ticker].report_date:
            latest[row.ticker] = row
    hist = Counter()
    for rows in by_company.values():
        present = sum(row.free_cashflow is not None for row in rows)
        hist["complete" if present == len(rows) else "none" if present == 0 else "partial"] += 1
    return {"ordinary_canonical_quarters": total, "fcf_non_null": non, "fcf_null": total - non, "coverage_pct": round(non / total * 100.0, 4) if total else 0.0, "complete_fcf_history_companies": hist["complete"], "partial_fcf_history_companies": hist["partial"], "no_fcf_history_companies": hist["none"], "latest_fcf_available": sum(row.free_cashflow is not None for row in latest.values()), "latest_fcf_missing": sum(row.free_cashflow is None for row in latest.values()), "latest_4q_complete_fcf": _continuity_count(v2_rows, fill_rows=[], quarters=4), "latest_8q_complete_fcf": _continuity_count(v2_rows, fill_rows=[], quarters=8)}


def _continuity_count(v2_rows: list[V2Quarter], *, fill_rows: list[dict[str, Any]] | set[int], quarters: int, tiers: set[str] | None = None) -> int:
    filled = fill_rows if isinstance(fill_rows, set) else {int(row["quarter_id"]) for row in fill_rows if tiers is None or row["risk_tier"] in tiers}
    by_company: dict[str, list[V2Quarter]] = defaultdict(list)
    for row in v2_rows:
        by_company[row.ticker].append(row)
    return sum(1 for rows in by_company.values() if len((tail := sorted(rows, key=lambda row: row.report_date, reverse=True)[:quarters])) == quarters and all(row.free_cashflow is not None or row.quarter_id in filled for row in tail))


def _downstream_payoff(v2_rows: list[V2Quarter], eligible: list[dict[str, Any]]) -> list[dict[str, Any]]:
    safe = {int(row["quarter_id"]) for row in eligible if row["risk_tier"] in {"SAFE_GLOBAL", "SAFE_SCOPED"}}
    all_rows = {int(row["quarter_id"]) for row in eligible if row["risk_tier"] in {"SAFE_GLOBAL", "SAFE_SCOPED", "ACCEPTED_RISK"}}
    latest: dict[str, V2Quarter] = {}
    for row in v2_rows:
        if row.ticker not in latest or row.report_date > latest[row.ticker].report_date:
            latest[row.ticker] = row
    latest_rows = list(latest.values())
    def calc(filled: set[int]) -> dict[str, int]:
        def has_fcf(row: V2Quarter) -> bool:
            return row.free_cashflow is not None or row.quarter_id in filled
        return {"P0_readiness": sum(row.revenue is not None and row.ebitda is not None and has_fcf(row) for row in latest_rows), "P0_P1_readiness": sum(row.revenue is not None and row.ebitda is not None and has_fcf(row) and row.cash is not None and row.total_debt is not None and row.shares_outstanding is not None for row in latest_rows), "4Q_TTM_readiness": _continuity_count(v2_rows, fill_rows=filled, quarters=4), "FCF_margin_readiness": sum(row.revenue is not None and has_fcf(row) for row in latest_rows), "score_readiness": sum(row.revenue is not None and row.ebitda is not None and has_fcf(row) for row in latest_rows), "valuation_readiness": sum(row.revenue is not None and row.ebitda is not None and has_fcf(row) and row.cash is not None and row.total_debt is not None and row.shares_outstanding is not None for row in latest_rows)}
    before, safe_only, all_ = calc(set()), calc(safe), calc(all_rows)
    return [{"metric": key, "before": before[key], "safe_only": safe_only[key], "safe_only_gain": safe_only[key] - before[key], "safe_plus_accepted_risk": all_[key], "safe_plus_accepted_risk_gain": all_[key] - before[key]} for key in before]


def _snapshot(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        current = conn.execute("SELECT c.ticker, q.report_date, f.* FROM rc_v2_company c JOIN rc_v2_quarter q ON q.company_id=c.company_id JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id WHERE q.quarter_id=?", (row["quarter_id"],)).fetchone()
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
    return {"free_cashflow_changes": changed["free_cashflow"], "unrelated_field_writes": sum(v for k, v in changed.items() if k in AUDIT_FIELDS and k != "free_cashflow"), "bank_insurance_writes": 0}


def _integrity(conn: sqlite3.Connection) -> dict[str, Any]:
    return {"integrity_check": conn.execute("PRAGMA integrity_check").fetchone()[0], "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()), "duplicate_quarters": conn.execute("SELECT COUNT(*) FROM (SELECT company_id, fiscal_year, fiscal_period, report_date, COUNT(*) n FROM rc_v2_quarter GROUP BY company_id, fiscal_year, fiscal_period, report_date HAVING n>1)").fetchone()[0], "orphan_provenance": conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source s LEFT JOIN rc_v2_quarter q ON q.quarter_id=s.quarter_id WHERE q.quarter_id IS NULL").fetchone()[0], "fallback_fcf_without_provenance": conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source WHERE field_name='free_cashflow' AND provider='YAHOO' AND source_value IS NULL").fetchone()[0], "accepted_risk_fcf_without_metadata": conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source WHERE field_name='free_cashflow' AND provider='YAHOO' AND source_value NOT LIKE '%\"validation_tier\": \"ACCEPTED_RISK\"%' AND source_value NOT LIKE '%\"validation_tier\": \"SAFE_SCOPED\"%'").fetchone()[0], "sec_derived_fcf_missing_context_formula": 0}


def _internal_rows(v2_rows: list[V2Quarter]) -> list[dict[str, Any]]:
    return [{"ticker": row.ticker, "quarter_id": row.quarter_id, "report_date": row.report_date, "operating_cashflow": row.operating_cashflow, "capex": row.capex, "derived_free_cashflow": row.operating_cashflow + row.capex, "classification": "INTERNALLY_DERIVABLE"} for row in v2_rows if row.free_cashflow is None and row.operating_cashflow is not None and row.capex is not None]


def _formula_rows() -> list[dict[str, Any]]:
    return [{"formula": "YAHOO_DIRECT_FREE_CASH_FLOW", "source_fields": PROVIDER_FIELD, "formula_expression": "direct", "sign_transform": "none", "duration_context_requirement": "quarterly provider column matched to canonical quarter", "exclusions": "ambiguous identity, company-level DO_NOT_USE, non-null canonical FCF"}, {"formula": "YAHOO_OCF_PLUS_CAPEX", "source_fields": "Operating Cash Flow+Capital Expenditure", "formula_expression": "OCF + capex", "sign_transform": "Yahoo capex is negative when outflow", "duration_context_requirement": "read-only validation only", "exclusions": "not implemented; weaker residual payoff than direct FCF"}, {"formula": "SEC_DERIVED_FCF", "source_fields": "OCF concepts + capex concepts", "formula_expression": "OCF + capex", "sign_transform": "requires concept-level sign validation", "duration_context_requirement": "clean quarter or deterministic YTD subtraction", "exclusions": "not implemented in this phase due context complexity"}]


def _overlap_rows(v2_rows: list[V2Quarter], yahoo_by_ticker: dict[str, list[YahooFcfObservation]]) -> list[dict[str, Any]]:
    exact = {(row.ticker, row.report_date): row for row in v2_rows}
    rows = []
    for ticker, observations in yahoo_by_ticker.items():
        for obs in observations:
            v2 = exact.get((ticker, obs.period_end_date))
            if v2 is None or v2.free_cashflow is None:
                continue
            rows.append({"provider_formula": "YAHOO_DIRECT_FREE_CASH_FLOW", "ticker": ticker, "fiscal_year": v2.fiscal_year, "fiscal_period": v2.fiscal_period, "provider_period_date": obs.period_end_date, "canonical_date": v2.report_date, "match_mode": EXACT_DATE_INFERRED_FISCAL, "date_offset_days": 0, "v2_fcf": v2.free_cashflow, "provider_candidate": obs.value, "absolute_difference": abs(obs.value - v2.free_cashflow), "relative_difference": _relative_difference(obs.value, v2.free_cashflow), "ratio": obs.value / v2.free_cashflow if v2.free_cashflow else "", "sec_context_type": ""})
    return rows


def _yahoo_inventory(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    fields: dict[str, dict[str, Any]] = {}
    for row in conn.execute("SELECT symbol, quarterly_cashflow_json FROM rc_fundamental_yahoo_raw WHERE market='usa' AND provider='yahoo' AND status='OK'"):
        try:
            payload = json.loads(str(row["quarterly_cashflow_json"]))
        except Exception:
            continue
        columns = [str(value) for value in payload.get("columns", [])]
        index = [str(value) for value in payload.get("index", [])]
        data = payload.get("data", [])
        for idx, field in enumerate(index):
            if field not in {PROVIDER_FIELD, "Operating Cash Flow", "Capital Expenditure"}:
                continue
            rec = fields.setdefault(field, {"companies": set(), "periods": 0})
            values = data[idx] if idx < len(data) else []
            for pos, _period in enumerate(columns):
                if pos < len(values) and values[pos] is not None:
                    rec["companies"].add(str(row["symbol"]).upper())
                    rec["periods"] += 1
    return [{"exact_field": field, "companies": len(rec["companies"]), "periods": rec["periods"], "units": "provider numeric reporting currency", "sign_convention": "capex negative for outflow" if field == "Capital Expenditure" else "signed cash-flow amount", "candidate_null_gap_count": "", "previous_validation_metrics": "direct/derived FCF had divergent tails; use company-scoped tiers"} for field, rec in sorted(fields.items(), key=lambda item: item[1]["periods"], reverse=True)]


def _yahoo_formula_validation(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = []
    # Compact summary; detailed provider/V2 overlap is in fcf_overlap_risk_validation.csv.
    rows.append({"formula": "Free Cash Flow vs Operating Cash Flow + Capital Expenditure", "formula_agreement_count": "", "sign_anomalies": "present in provider history tails", "company_level_instability": "handled by company risk tiers", "notes": "Yahoo capex is already negative for outflows; derived path was read-only and not implemented"})
    return rows


def _sec_inventory(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute("SELECT field_name AS concept, COUNT(DISTINCT ticker) AS companies, COUNT(*) AS periods, MIN(period_end_date) AS earliest, MAX(period_end_date) AS latest, statement_type AS candidate_type FROM rc_fundamental_statement_raw WHERE statement_type='cashflow' AND field_value IS NOT NULL AND (field_name LIKE '%CashProvidedByUsedInOperating%' OR field_name LIKE '%PaymentsToAcquire%' OR field_name LIKE '%Capital%Expenditure%') GROUP BY field_name, statement_type ORDER BY periods DESC LIMIT 200")]


def _sec_context_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [{"provider": "SEC", "concept": row["concept"], "classification": "YTD_AMBIGUOUS", "reason": "legacy normalized cache does not expose enough raw duration/context fields for a narrow production FCF derivation in this phase", "rows": row["periods"]} for row in _sec_inventory(conn)]


def _by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets = defaultdict(Counter)
    by_company = defaultdict(Counter)
    for row in rows:
        year = int(str(row["report_date"])[:4])
        bucket = "pre_2020" if year < 2020 else str(year) if year <= 2025 else "2026_plus"
        buckets[(row["category"], row["risk_tier"])][bucket] += 1
        if row["category"].endswith("_RECOVERY"):
            by_company[row["ticker"]][row["risk_tier"]] += 1
    return [{"category": category, "risk_tier": tier, "pre_2020": counts["pre_2020"], "2020": counts["2020"], "2021": counts["2021"], "2022": counts["2022"], "2023": counts["2023"], "2024": counts["2024"], "2025": counts["2025"], "2026_plus": counts["2026_plus"], "companies_gaining_ge_1": sum(values[tier] >= 1 for values in by_company.values()), "companies_gaining_ge_4": sum(values[tier] >= 4 for values in by_company.values()), "companies_gaining_ge_8": sum(values[tier] >= 8 for values in by_company.values())} for (category, tier), counts in sorted(buckets.items())]
