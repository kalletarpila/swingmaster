from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


SEC_OCF_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
SEC_OCF_PROVIDER = "SEC"
SEC_OCF_FIELD = "operating_cashflow"
SEC_OCF_SOURCE_DATASET = "legacy_sec_edgar_cashflow_raw"
SEC_OCF_SOURCE_TABLE = "rc_fundamental_statement_raw"
SEC_OCF_RULE_VERSION = "sec_ocf_q1_q3_safe_scoped_v1"
SEC_OCF_VALIDATION_TIER = "SAFE_SCOPED"

OCF_DIRECT_Q1 = "SAFE_DIRECT_Q1"
OCF_RECONSTRUCTED_Q2 = "SAFE_RECONSTRUCTED_Q2"
OCF_RECONSTRUCTED_Q3 = "SAFE_RECONSTRUCTED_Q3"
OCF_RECONSTRUCTED_Q4 = "SAFE_RECONSTRUCTED_Q4"
OCF_APPROVED_RULES = (OCF_DIRECT_Q1, OCF_RECONSTRUCTED_Q2, OCF_RECONSTRUCTED_Q3)
OCF_Q4_AUDIT_ONLY_RULES = (OCF_RECONSTRUCTED_Q4,)
OCF_MIN_COMPANY_RULE_OVERLAP = 4
OCF_MAX_SAFE_RELATIVE_DIFFERENCE = 0.01


@dataclass(frozen=True)
class SecCashflowFact:
    ticker: str
    period_end_date: str
    concept: str
    value: float
    currency: str
    form: str
    unit: str
    fiscal_year: str
    fiscal_period: str
    frame: str
    period_start: str
    filed: str
    retrieved_at_utc: str
    run_id: str
    raw_field_name: str
    dimension_policy: str = "UNDIMENSIONED_ONLY"

    @property
    def duration_days(self) -> int | None:
        try:
            return (date.fromisoformat(self.period_end_date) - date.fromisoformat(self.period_start)).days + 1
        except ValueError:
            return None

    @property
    def source_identity(self) -> str:
        return (
            f"{SEC_OCF_SOURCE_TABLE}:{self.ticker}:{self.period_start}:{self.period_end_date}:"
            f"{self.concept}:{self.form}:{self.unit}:{self.filed}:{self.run_id}"
        )


@dataclass(frozen=True)
class V2OcfQuarter:
    ticker: str
    company_id: int
    quarter_id: int
    fiscal_year: int
    fiscal_period: str
    report_date: str
    company_profile: str = "ORDINARY"
    operating_cashflow: float | None = None


@dataclass(frozen=True)
class SecOcfCandidate:
    eligible: bool
    field: str
    value: float | None
    rule_type: str
    provider: str
    provider_field: str
    source_dataset: str
    source_facts: tuple[SecCashflowFact, ...]
    transformation: str
    arithmetic: str
    validation_tier: str
    rule_version: str
    quarter_identity_evidence: Mapping[str, Any]
    rejection_reason: str | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_sec_fact_field_name(field_name: str) -> dict[str, str]:
    concept, *parts = field_name.split("|")
    meta: dict[str, str] = {"concept": concept}
    for part in parts:
        if "=" in part:
            key, value = part.split("=", 1)
            meta[key] = "" if value == "NULL" else value
    return meta


def fact_from_legacy_row(row: Mapping[str, Any]) -> SecCashflowFact:
    meta = parse_sec_fact_field_name(str(row["field_name"]))
    return SecCashflowFact(
        ticker=str(row["ticker"]).upper(),
        period_end_date=str(row["period_end_date"]),
        concept=meta.get("concept", ""),
        value=float(row["field_value"]),
        currency=str(row["currency"] or ""),
        form=meta.get("form", ""),
        unit=meta.get("unit", ""),
        fiscal_year=meta.get("fy", ""),
        fiscal_period=meta.get("fp", ""),
        frame=meta.get("frame", ""),
        period_start=meta.get("start", ""),
        filed=meta.get("filed", ""),
        retrieved_at_utc=str(row["retrieved_at_utc"]),
        run_id=str(row["run_id"]),
        raw_field_name=str(row["field_name"]),
    )


def load_sec_ocf_facts(conn: sqlite3.Connection) -> dict[tuple[str, str], list[SecCashflowFact]]:
    facts: dict[tuple[str, str], list[SecCashflowFact]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT ticker, period_end_date, field_name, field_value, currency, retrieved_at_utc, run_id
        FROM rc_fundamental_statement_raw
        WHERE source='sec_edgar'
          AND statement_type='cashflow'
          AND field_name LIKE 'NetCashProvidedByUsedInOperatingActivities%'
          AND field_value IS NOT NULL
        """
    ):
        fact = fact_from_legacy_row(row)
        if _is_supported_standard_ocf_fact(fact):
            facts[(fact.ticker, fact.period_end_date)].append(fact)
    return facts


def evaluate_sec_ocf_candidate(
    quarter: V2OcfQuarter,
    company_year_quarters: Mapping[str, V2OcfQuarter],
    facts_by_ticker_end: Mapping[tuple[str, str], Iterable[SecCashflowFact]],
    *,
    require_null_canonical: bool = True,
    allow_q4: bool = False,
) -> SecOcfCandidate:
    if quarter.company_profile != "ORDINARY":
        return _reject(quarter, "PROFILE_UNSUPPORTED")
    if require_null_canonical and quarter.operating_cashflow is not None:
        return _reject(quarter, "CANONICAL_ALREADY_NON_NULL")
    if quarter.fiscal_period == "Q1":
        fact, reason = _pick_unique_fact(facts_by_ticker_end, quarter.ticker, quarter.report_date, bucket="qtr", forms={"10-Q"})
        if fact is None:
            return _reject(quarter, reason)
        return _accepted(quarter, fact.value, OCF_DIRECT_Q1, (fact,), "DIRECT_QUARTER_OCF", f"{fact.value}")
    if quarter.fiscal_period == "Q2":
        prior = company_year_quarters.get("Q1")
        if prior is None:
            return _reject(quarter, "SUBTRACTION_COMPONENT_MISSING")
        q1, reason = _pick_unique_fact(facts_by_ticker_end, quarter.ticker, prior.report_date, bucket="qtr", forms={"10-Q"})
        if q1 is None:
            return _reject(quarter, f"SUBTRACTION_COMPONENT_MISSING:{reason}")
        h1, reason = _pick_unique_fact(
            facts_by_ticker_end,
            quarter.ticker,
            quarter.report_date,
            start=q1.period_start,
            bucket="h1",
            forms={"10-Q"},
        )
        if h1 is None:
            return _reject(quarter, f"SUBTRACTION_COMPONENT_MISSING:{reason}")
        return _accepted(
            quarter,
            h1.value - q1.value,
            OCF_RECONSTRUCTED_Q2,
            (h1, q1),
            "YTD_SUBTRACTION_Q2",
            f"{h1.value} - {q1.value}",
        )
    if quarter.fiscal_period == "Q3":
        prior = company_year_quarters.get("Q2")
        if prior is None:
            return _reject(quarter, "SUBTRACTION_COMPONENT_MISSING")
        for h1 in _facts_for(facts_by_ticker_end, quarter.ticker, prior.report_date, bucket="h1", forms={"10-Q"}):
            nine_months, reason = _pick_unique_fact(
                facts_by_ticker_end,
                quarter.ticker,
                quarter.report_date,
                start=h1.period_start,
                bucket="9m",
                forms={"10-Q"},
            )
            if nine_months is not None:
                return _accepted(
                    quarter,
                    nine_months.value - h1.value,
                    OCF_RECONSTRUCTED_Q3,
                    (nine_months, h1),
                    "YTD_SUBTRACTION_Q3",
                    f"{nine_months.value} - {h1.value}",
                )
        return _reject(quarter, "SUBTRACTION_COMPONENT_MISSING:MATCHED_H1_OR_9M")
    if quarter.fiscal_period == "Q4":
        if not allow_q4:
            return _reject(quarter, "Q4_AUDIT_ONLY_NOT_APPROVED")
        prior = company_year_quarters.get("Q3")
        if prior is None:
            return _reject(quarter, "SUBTRACTION_COMPONENT_MISSING")
        for nine_months in _facts_for(facts_by_ticker_end, quarter.ticker, prior.report_date, bucket="9m", forms={"10-Q"}):
            full_year, reason = _pick_unique_fact(
                facts_by_ticker_end,
                quarter.ticker,
                quarter.report_date,
                start=nine_months.period_start,
                bucket="fy",
                forms={"10-K"},
            )
            if full_year is not None:
                return _accepted(
                    quarter,
                    full_year.value - nine_months.value,
                    OCF_RECONSTRUCTED_Q4,
                    (full_year, nine_months),
                    "YTD_SUBTRACTION_Q4",
                    f"{full_year.value} - {nine_months.value}",
                )
        return _reject(quarter, "SUBTRACTION_COMPONENT_MISSING:MATCHED_9M_OR_FY")
    return _reject(quarter, "UNSUPPORTED_FISCAL_PERIOD")


def load_v2_ocf_quarters(conn: sqlite3.Connection, *, market: str = "usa") -> list[V2OcfQuarter]:
    rows = []
    for row in conn.execute(
        """
        SELECT c.ticker, c.company_id, c.company_profile, q.quarter_id, q.fiscal_year, q.fiscal_period,
               q.report_date, f.operating_cashflow
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id=c.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        WHERE lower(c.market)=lower(?) AND c.active=1 AND c.ticker IS NOT NULL
        ORDER BY c.ticker, q.fiscal_year, q.fiscal_period, q.report_date
        """,
        (market,),
    ):
        rows.append(
            V2OcfQuarter(
                ticker=str(row["ticker"]).upper(),
                company_id=int(row["company_id"]),
                quarter_id=int(row["quarter_id"]),
                fiscal_year=int(row["fiscal_year"]),
                fiscal_period=str(row["fiscal_period"]),
                report_date=str(row["report_date"]),
                company_profile=str(row["company_profile"]),
                operating_cashflow=None if row["operating_cashflow"] is None else float(row["operating_cashflow"]),
            )
        )
    return rows


def build_ocf_candidate_inventory(
    v2_rows: Iterable[V2OcfQuarter],
    facts_by_ticker_end: Mapping[tuple[str, str], Iterable[SecCashflowFact]],
    *,
    require_null_canonical: bool,
    allow_q4: bool = False,
) -> list[dict[str, Any]]:
    rows = list(v2_rows)
    by_company_year: dict[tuple[int, int], dict[str, V2OcfQuarter]] = defaultdict(dict)
    for row in rows:
        by_company_year[(row.company_id, row.fiscal_year)][row.fiscal_period] = row
    out = []
    for row in rows:
        candidate = evaluate_sec_ocf_candidate(
            row,
            by_company_year[(row.company_id, row.fiscal_year)],
            facts_by_ticker_end,
            require_null_canonical=require_null_canonical,
            allow_q4=allow_q4,
        )
        out.append(_candidate_row(row, candidate))
    return out


def build_ocf_company_rule_quality(rows: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    quality: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "overlap_count": 0,
            "within_1pct": 0,
            "within_5pct": 0,
            "gt5pct": 0,
            "sign_mismatch": 0,
            "large_abs_mismatch": 0,
            "max_relative_difference": 0.0,
            "safe_for_production": False,
        }
    )
    for row in rows:
        if row.get("eligible") != 1 or row.get("rule_type") not in OCF_APPROVED_RULES:
            continue
        current = row.get("current_canonical_value")
        candidate = row.get("candidate_value")
        if current in (None, "") or candidate in (None, ""):
            continue
        current_value = float(current)
        candidate_value = float(candidate)
        relative = abs(candidate_value - current_value) / max(abs(candidate_value), abs(current_value), 1.0)
        key = (str(row["ticker"]).upper(), str(row["rule_type"]))
        item = quality[key]
        item["overlap_count"] += 1
        item["within_1pct"] += int(relative <= 0.01)
        item["within_5pct"] += int(relative <= 0.05)
        item["gt5pct"] += int(relative > 0.05)
        item["sign_mismatch"] += int((candidate_value < 0) != (current_value < 0))
        item["large_abs_mismatch"] += int(abs(candidate_value - current_value) >= 1_000_000_000)
        item["max_relative_difference"] = max(float(item["max_relative_difference"]), relative)
    for item in quality.values():
        item["safe_for_production"] = (
            int(item["overlap_count"]) >= OCF_MIN_COMPANY_RULE_OVERLAP
            and int(item["gt5pct"]) == 0
            and int(item["sign_mismatch"]) == 0
            and float(item["max_relative_difference"]) <= OCF_MAX_SAFE_RELATIVE_DIFFERENCE
        )
    return dict(quality)


def build_narrowed_ocf_candidate_inventory(
    rows: Iterable[Mapping[str, Any]],
    quality: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        narrowed = dict(row)
        key = (str(row.get("ticker", "")).upper(), str(row.get("rule_type", "")))
        safe = bool(quality.get(key, {}).get("safe_for_production"))
        narrowed["company_rule_overlap_count"] = quality.get(key, {}).get("overlap_count", 0)
        narrowed["company_rule_max_relative_difference"] = quality.get(key, {}).get("max_relative_difference", "")
        narrowed["narrowed_safe_for_production"] = int(row.get("eligible") == 1 and safe)
        if row.get("eligible") == 1 and not safe:
            narrowed["rejection_reason"] = "COMPANY_RULE_OVERLAP_VALIDATION_NOT_SAFE"
        out.append(narrowed)
    return out


def validate_source_period_uniqueness(rule_type: str, source_facts: str | Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    facts = json.loads(source_facts) if isinstance(source_facts, str) else list(source_facts)
    if rule_type in {"SAFE_DIRECT_Q1_V2", OCF_DIRECT_Q1}:
        expected_periods = 1
    elif rule_type in {
        "SAFE_RECONSTRUCTED_Q2_V2",
        "SAFE_RECONSTRUCTED_Q3_V2",
        "SAFE_RECONSTRUCTED_Q4_V2",
        OCF_RECONSTRUCTED_Q2,
        OCF_RECONSTRUCTED_Q3,
        OCF_RECONSTRUCTED_Q4,
    }:
        expected_periods = 2
    else:
        return {
            "passes": False,
            "reason": "UNSUPPORTED_RULE_TYPE",
            "period_count": 0,
            "expected_period_count": 0,
            "periods": [],
        }
    grouped: dict[tuple[Any, ...], set[float]] = defaultdict(set)
    period_rows: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for fact in facts:
        key = (
            fact.get("concept"),
            fact.get("context_start"),
            fact.get("context_end"),
            fact.get("duration_days"),
            fact.get("unit"),
            fact.get("dimensions"),
        )
        grouped[key].add(float(fact["source_value"]))
        period_rows[key].append(fact)
    periods = [
        {
            "concept": key[0],
            "context_start": key[1],
            "context_end": key[2],
            "duration_days": key[3],
            "unit": key[4],
            "dimensions": key[5],
            "fact_count": len(period_rows[key]),
            "distinct_source_value_count": len(values),
            "distinct_source_values": sorted(values),
        }
        for key, values in grouped.items()
    ]
    bad_periods = [row for row in periods if row["distinct_source_value_count"] != 1]
    if len(periods) != expected_periods:
        reason = "SOURCE_PERIOD_COUNT_MISMATCH"
    elif bad_periods:
        reason = "MULTIPLE_DISTINCT_VALUES_WITHIN_SOURCE_PERIOD"
    else:
        reason = ""
    return {
        "passes": reason == "",
        "reason": reason,
        "period_count": len(periods),
        "expected_period_count": expected_periods,
        "periods": periods,
    }


def apply_sec_ocf_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    run_id: str,
    dry_run: bool,
    now: str | None = None,
) -> list[dict[str, Any]]:
    now = now or utc_now()
    if not dry_run and rows:
        conn.execute(
            """
            INSERT OR IGNORE INTO rc_v2_import_run (
                import_run_id, market, simfin_dir, builder_version, started_at_utc, finished_at_utc
            ) VALUES (?, 'usa', 'legacy_db:fundamentals_usa.db', ?, ?, ?)
            """,
            (run_id, SEC_OCF_RULE_VERSION, now, now),
        )
    results = []
    for row in rows:
        if row.get("eligible") != 1:
            results.append({**row, "action": "REJECTED"})
            continue
        current = conn.execute(
            "SELECT operating_cashflow FROM rc_v2_fundamental_quarterly WHERE quarter_id=?",
            (row["quarter_id"],),
        ).fetchone()
        if current is None:
            action = "REJECT_MISSING_V2_QUARTER"
        elif current["operating_cashflow"] is not None:
            action = (
                "SAME_VALUE_NOOP"
                if float(current["operating_cashflow"]) == float(row["candidate_value"])
                else "CONFLICT_EXISTING_DIFFERENT"
            )
        else:
            action = "WOULD_FILL" if dry_run else "FILLED"
            if not dry_run:
                conn.execute(
                    """
                    UPDATE rc_v2_fundamental_quarterly
                    SET operating_cashflow=?,
                        available_canonical_field_count=available_canonical_field_count+1,
                        updated_at_utc=?
                    WHERE quarter_id=? AND operating_cashflow IS NULL
                    """,
                    (row["candidate_value"], now, row["quarter_id"]),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
                        quarter_id, field_name, provider, provider_field, source_dataset, source_file,
                        source_file_sha256, transformation, source_value, import_run_id, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, 'n/a', ?, ?, ?, ?)
                    """,
                    (
                        row["quarter_id"],
                        SEC_OCF_FIELD,
                        SEC_OCF_PROVIDER,
                        SEC_OCF_CONCEPT,
                        SEC_OCF_SOURCE_DATASET,
                        row["source_file"],
                        row["transformation"],
                        row["source_value"],
                        run_id,
                        now,
                    ),
                )
        results.append({**row, "action": action})
    return results


def create_verified_backup(db_path: Path, artifact_dir: Path) -> dict[str, Any]:
    backup_dir = artifact_dir / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = backup_dir / f"{db_path.name}.{stamp}.bak"
    shutil.copy2(db_path, backup_path)
    with sqlite3.connect(f"file:{backup_path.as_posix()}?mode=ro", uri=True) as conn:
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
        fk_rows = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    return {
        "path": str(backup_path),
        "bytes": backup_path.stat().st_size,
        "created_at_utc": stamp,
        "quick_check": quick,
        "foreign_key_check_rows": fk_rows,
    }


def write_csv(path: Path, rows: Iterable[Mapping[str, Any]], fieldnames: list[str] | None = None) -> None:
    materialized = [dict(row) for row in rows]
    if fieldnames is None:
        fieldnames = []
        for row in materialized:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _is_supported_standard_ocf_fact(fact: SecCashflowFact) -> bool:
    return (
        fact.concept == SEC_OCF_CONCEPT
        and fact.currency == "USD"
        and fact.unit == "USD"
        and fact.form in {"10-Q", "10-K"}
        and bool(fact.period_start)
        and "[" not in fact.raw_field_name
        and "dimension" not in fact.raw_field_name.lower()
    )


def _pick_unique_fact(
    facts_by_ticker_end: Mapping[tuple[str, str], Iterable[SecCashflowFact]],
    ticker: str,
    end: str,
    *,
    start: str | None = None,
    bucket: str,
    forms: set[str],
) -> tuple[SecCashflowFact | None, str]:
    matches = []
    for fact in facts_by_ticker_end.get((ticker.upper(), end), []):
        if start is not None and fact.period_start != start:
            continue
        if fact.form not in forms:
            continue
        if not _duration_matches(fact.duration_days, bucket):
            continue
        matches.append(fact)
    if not matches:
        return None, "SOURCE_FACT_MISSING"
    values = {round(fact.value, 2) for fact in matches}
    if len(values) > 1:
        return None, "MULTIPLE_SOURCE_FACTS_AMBIGUOUS"
    ordered = sorted(matches, key=lambda item: (item.filed, item.retrieved_at_utc, item.frame == ""), reverse=True)
    return ordered[0], "OK"


def _facts_for(
    facts_by_ticker_end: Mapping[tuple[str, str], Iterable[SecCashflowFact]],
    ticker: str,
    end: str,
    *,
    bucket: str,
    forms: set[str],
) -> list[SecCashflowFact]:
    out = []
    seen = set()
    for fact in facts_by_ticker_end.get((ticker.upper(), end), []):
        if fact.form not in forms or not _duration_matches(fact.duration_days, bucket):
            continue
        selected, _reason = _pick_unique_fact(
            facts_by_ticker_end,
            ticker,
            end,
            start=fact.period_start,
            bucket=bucket,
            forms=forms,
        )
        if selected is None:
            continue
        key = selected.source_identity
        if key not in seen:
            out.append(selected)
            seen.add(key)
    return out


def _duration_matches(duration_days: int | None, bucket: str) -> bool:
    if duration_days is None:
        return False
    if bucket == "qtr":
        return 70 <= duration_days <= 115
    if bucket == "h1":
        return 150 <= duration_days <= 210
    if bucket == "9m":
        return 240 <= duration_days <= 300
    if bucket == "fy":
        return 330 <= duration_days <= 390
    return False


def _accepted(
    quarter: V2OcfQuarter,
    value: float,
    rule_type: str,
    facts: tuple[SecCashflowFact, ...],
    transformation: str,
    arithmetic: str,
) -> SecOcfCandidate:
    return SecOcfCandidate(
        eligible=True,
        field=SEC_OCF_FIELD,
        value=value,
        rule_type=rule_type,
        provider=SEC_OCF_PROVIDER,
        provider_field=SEC_OCF_CONCEPT,
        source_dataset=SEC_OCF_SOURCE_DATASET,
        source_facts=facts,
        transformation=transformation,
        arithmetic=arithmetic,
        validation_tier=SEC_OCF_VALIDATION_TIER,
        rule_version=SEC_OCF_RULE_VERSION,
        quarter_identity_evidence={
            "ticker": quarter.ticker,
            "company_id": quarter.company_id,
            "quarter_id": quarter.quarter_id,
            "fiscal_year": quarter.fiscal_year,
            "fiscal_period": quarter.fiscal_period,
            "canonical_report_date": quarter.report_date,
            "identity_basis": "existing_v2_company_quarter_report_date",
            "provider_timing_rejection_used": False,
        },
    )


def _reject(quarter: V2OcfQuarter, reason: str) -> SecOcfCandidate:
    return SecOcfCandidate(
        eligible=False,
        field=SEC_OCF_FIELD,
        value=None,
        rule_type="",
        provider=SEC_OCF_PROVIDER,
        provider_field=SEC_OCF_CONCEPT,
        source_dataset=SEC_OCF_SOURCE_DATASET,
        source_facts=(),
        transformation="",
        arithmetic="",
        validation_tier="",
        rule_version=SEC_OCF_RULE_VERSION,
        quarter_identity_evidence={
            "ticker": quarter.ticker,
            "company_id": quarter.company_id,
            "quarter_id": quarter.quarter_id,
            "fiscal_year": quarter.fiscal_year,
            "fiscal_period": quarter.fiscal_period,
            "canonical_report_date": quarter.report_date,
            "provider_timing_rejection_used": False,
        },
        rejection_reason=reason,
    )


def _candidate_row(quarter: V2OcfQuarter, candidate: SecOcfCandidate) -> dict[str, Any]:
    source_payload = _source_value(candidate, quarter)
    return {
        "ticker": quarter.ticker,
        "company_id": quarter.company_id,
        "quarter_id": quarter.quarter_id,
        "company_profile": quarter.company_profile,
        "fiscal_year": quarter.fiscal_year,
        "fiscal_period": quarter.fiscal_period,
        "report_date": quarter.report_date,
        "current_canonical_value": quarter.operating_cashflow,
        "target_field": SEC_OCF_FIELD,
        "eligible": int(candidate.eligible),
        "candidate_value": candidate.value,
        "rule_type": candidate.rule_type,
        "provider": candidate.provider,
        "provider_field": candidate.provider_field,
        "source_file": ";".join(fact.source_identity for fact in candidate.source_facts),
        "source_facts": json.dumps([_fact_payload(fact) for fact in candidate.source_facts], sort_keys=True),
        "transformation": candidate.transformation,
        "arithmetic": candidate.arithmetic,
        "validation_tier": candidate.validation_tier,
        "rule_version": candidate.rule_version,
        "rejection_reason": candidate.rejection_reason or "",
        "source_value": source_payload,
    }


def _source_value(candidate: SecOcfCandidate, quarter: V2OcfQuarter) -> str:
    return json.dumps(
        {
            "validation_tier": candidate.validation_tier,
            "rule_version": candidate.rule_version,
            "provider": SEC_OCF_PROVIDER,
            "provider_field": SEC_OCF_CONCEPT,
            "source_dataset": SEC_OCF_SOURCE_DATASET,
            "target_field": SEC_OCF_FIELD,
            "canonical_value": candidate.value,
            "canonical_report_date": quarter.report_date,
            "transformation": candidate.transformation,
            "arithmetic": candidate.arithmetic,
            "source_facts": [_fact_payload(fact) for fact in candidate.source_facts],
            "quarter_identity_evidence": dict(candidate.quarter_identity_evidence),
            "source_accession_limitation": "SEC accession is not exposed by legacy raw cache; run_id/filed/raw fact identity are recorded.",
        },
        sort_keys=True,
    )


def _fact_payload(fact: SecCashflowFact) -> dict[str, Any]:
    return {
        "source_identity": fact.source_identity,
        "source_accession": "not_exposed_by_legacy_raw_cache",
        "run_id": fact.run_id,
        "concept": fact.concept,
        "raw_field_name": fact.raw_field_name,
        "context_start": fact.period_start,
        "context_end": fact.period_end_date,
        "duration_days": fact.duration_days,
        "form": fact.form,
        "unit": fact.unit,
        "currency": fact.currency,
        "dimensions": "undimensioned",
        "source_value": fact.value,
        "filed": fact.filed,
        "retrieved_at_utc": fact.retrieved_at_utc,
    }


def action_counts(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    return dict(Counter(str(row.get("action", "")) for row in rows))
