from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_canonical_migration import (
    CANONICAL_FIELD_NAMES,
    V3CanonicalMigrationCandidate,
    V3CanonicalMigrationEngine,
)
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository, configure_connection, utc_now_text
from swingmaster.fundamentals.v3_schema import apply_v3_schema, validate_v3_schema
from swingmaster.fundamentals.v3_yahoo_bootstrap import (
    ApprovedV3Company,
    V3RawCacheRepository,
    load_yahoo_raw_cache_result,
    normalize_yahoo_raw_cache_result,
)


YAHOO_SEED_SOURCE = "YAHOO"
YAHOO_SEED_ARTIFACT_VERSION = "fundamentals_v3_phase3b_yahoo_seed_v1"
YAHOO_SEED_DERIVATION_METHOD = "YAHOO_DIRECT_OR_PROVIDER_NORMALIZED"
CORE_READY_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")


@dataclass(frozen=True)
class CompanyBaseline:
    market: str
    ticker: str
    active: bool
    activity_classification: str


@dataclass(frozen=True)
class YahooSeedMetadata:
    fiscal_year: int
    fiscal_quarter: str
    publish_date: str | None
    market_availability_date: str | None
    evidence_source: str
    disposition: str
    candidate_can_create_quarter: bool = True
    candidate_issue_type: str | None = None
    period_date_policy: str = "CONFLICT"
    official_period_end_date: str | None = None


@dataclass(frozen=True)
class YahooSeedPrepared:
    baseline: list[CompanyBaseline]
    candidates: list[V3CanonicalMigrationCandidate]
    accounting: dict[str, Any]
    normalized_rows: list[dict[str, Any]]


def prepare_yahoo_seed(
    *,
    company_baseline_csv: Path,
    raw_cache_db: Path,
    bootstrap_root: Path,
    post_a_root: Path,
    post_a2_root: Path,
    post_a3_root: Path,
    bootstrap_run_id: str,
    migration_run_id: str,
) -> YahooSeedPrepared:
    baseline = load_company_baseline(company_baseline_csv)
    normalized_rows = replay_normalized_yahoo_rows(
        baseline=baseline,
        raw_cache_db=raw_cache_db,
        bootstrap_run_id=bootstrap_run_id,
    )
    metadata = build_final_metadata_map(
        bootstrap_root=bootstrap_root,
        post_a_root=post_a_root,
        post_a2_root=post_a2_root,
        post_a3_root=post_a3_root,
    )
    candidates: list[V3CanonicalMigrationCandidate] = []
    dispositions = Counter()
    missing_metadata: list[dict[str, Any]] = []
    for row in normalized_rows:
        key = (row["ticker"], row["period_end_date"])
        meta = metadata.get(key)
        if meta is None:
            dispositions["OTHER_RESOLUTION_REQUIRED"] += 1
            missing_metadata.append({"ticker": key[0], "period_end_date": key[1]})
            continue
        dispositions[meta.disposition] += 1
        values = _canonical_values(row)
        candidates.append(
            V3CanonicalMigrationCandidate(
                source_system=YAHOO_SEED_SOURCE,
                source_record_id=f"YAHOO:{row['ticker']}:{row['period_end_date']}:{row['payload_hash']}",
                migration_run_id=migration_run_id,
                market=row["market"],
                ticker=row["ticker"],
                fiscal_year=meta.fiscal_year,
                fiscal_quarter=meta.fiscal_quarter,
                period_end_date=row["period_end_date"],
                publish_date=meta.publish_date,
                market_availability_date=meta.market_availability_date,
                values=values,
                raw_evidence_ref=row["provider_cache_ref"],
                approved_company_active=None,
                candidate_can_create_quarter=meta.candidate_can_create_quarter,
                candidate_issue_type=meta.candidate_issue_type,
                period_date_policy=meta.period_date_policy,
                value_metadata={
                    "artifact_version": YAHOO_SEED_ARTIFACT_VERSION,
                    "disposition": meta.disposition,
                    "fiscal_identity_source": meta.evidence_source,
                    "official_period_end_date": meta.official_period_end_date,
                    "provider_details": row["provider_details"],
                    "derivation_method": YAHOO_SEED_DERIVATION_METHOD,
                },
            )
        )
    candidates.sort(key=lambda item: item.source_record_id)
    accounting = {
        "normalized_rows": len(normalized_rows),
        "candidate_rows": len(candidates),
        "missing_metadata_rows": len(missing_metadata),
        "missing_metadata": missing_metadata,
        "dispositions": dict(sorted(dispositions.items())),
        "reconciles_to_normalized_rows": len(normalized_rows) == len(candidates) + len(missing_metadata),
    }
    return YahooSeedPrepared(baseline=baseline, candidates=candidates, accounting=accounting, normalized_rows=normalized_rows)


def load_company_baseline(path: Path) -> list[CompanyBaseline]:
    rows: list[CompanyBaseline] = []
    for row in _read_csv(path):
        rows.append(
            CompanyBaseline(
                market=row["market"].strip().lower(),
                ticker=row["ticker"].strip().upper(),
                active=row["recommended_v3_company_active"].strip() == "1",
                activity_classification=row["activity_classification"],
            )
        )
    rows.sort(key=lambda item: (item.market, item.ticker))
    if len({(row.market, row.ticker) for row in rows}) != len(rows):
        raise RuntimeError("V3_PHASE3B_DUPLICATE_COMPANY_BASELINE")
    return rows


def replay_normalized_yahoo_rows(*, baseline: list[CompanyBaseline], raw_cache_db: Path, bootstrap_run_id: str) -> list[dict[str, Any]]:
    raw_cache_repo = V3RawCacheRepository(raw_cache_db)
    normalized: list[dict[str, Any]] = []
    for index, company in enumerate(baseline, start=1):
        approved = ApprovedV3Company(
            company_id=index,
            market=company.market,
            ticker=company.ticker,
            provider_symbol=company.ticker,
        )
        raw_result = load_yahoo_raw_cache_result(
            raw_cache_repo=raw_cache_repo,
            company=approved,
            fetch_run_id=bootstrap_run_id,
        )
        if raw_result is None:
            raise RuntimeError(f"V3_PHASE3B_RAW_CACHE_MISSING:{company.ticker}")
        for row in normalize_yahoo_raw_cache_result(raw_result):
            provider_cache_ref = f"v3_raw_cache_entry:YAHOO:{company.ticker}:{row.fetch_run_id}:{row.payload_hash}"
            normalized.append(
                {
                    "market": row.company.market,
                    "ticker": row.company.ticker,
                    "period_end_date": row.period_end_date,
                    "payload_hash": row.payload_hash,
                    "fetch_run_id": row.fetch_run_id,
                    "provider_cache_ref": provider_cache_ref,
                    "values": dict(row.values),
                    "provider_details": dict(row.provider_details),
                }
            )
    normalized.sort(key=lambda item: (item["ticker"], item["period_end_date"], item["payload_hash"]))
    return normalized


def build_final_metadata_map(*, bootstrap_root: Path, post_a_root: Path, post_a2_root: Path, post_a3_root: Path) -> dict[tuple[str, str], YahooSeedMetadata]:
    metadata: dict[tuple[str, str], YahooSeedMetadata] = {}
    _apply_bootstrap_candidate_metadata(metadata, bootstrap_root / "candidates.jsonl")
    _apply_duplicate_candidate_metadata(metadata, bootstrap_root / "duplicate_candidate_keys.csv")
    _apply_post_a_metadata(metadata, post_a_root / "metadata_rejection_rows.csv")
    _apply_sequential_metadata(metadata, post_a_root / "sequential_recovery_candidates.csv", post_a_root / "publication_date_recovery.csv")
    _apply_a2_metadata(metadata, post_a2_root / "additional_unresolved_recovery.csv")
    _apply_a2_resolved_conflicts(metadata, post_a2_root / "resolved_anchor_conflicts.csv")
    _apply_a2_manual(metadata, post_a2_root / "manual_fiscal_calendar_resolution.csv")
    _apply_a2_company_patterns(metadata, post_a2_root / "company_fiscal_patterns.csv")
    _apply_a3_recovered(metadata, post_a3_root / "recovered_rows.csv")
    _apply_company_recent_calendar(metadata, post_a3_root / "company_recent_result_calendar.csv")
    _apply_neup_manual(metadata, post_a3_root / "manual_fiscal_calendar_resolution_neup.csv")
    _apply_reconciliation_exceptions(metadata, post_a3_root / "phase3_reconciliation_exceptions.csv")
    return metadata


def apply_company_baseline(conn: sqlite3.Connection, baseline: list[CompanyBaseline], *, now_utc: str) -> dict[str, Any]:
    repo = V3CompanyRepository(conn)
    for company in baseline:
        repo.admit_company(
            market=company.market,
            ticker=company.ticker,
            admission_source="PHASE3B_APPROVED_BASELINE",
            admission_evidence=company.activity_classification,
            active=company.active,
            now_utc=now_utc,
        )
    rows = conn.execute(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN active = 0 THEN 1 ELSE 0 END) AS inactive
        FROM v3_company
        """
    ).fetchone()
    duplicates = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT market, ticker
            FROM v3_company
            GROUP BY market, ticker
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    return {
        "approved_source_companies": len(baseline),
        "company_rows": int(rows["total"]),
        "active_rows": int(rows["active"] or 0),
        "inactive_rows": int(rows["inactive"] or 0),
        "duplicate_market_ticker": int(duplicates),
        "reconciles": int(rows["total"]) == len(baseline) and int(duplicates) == 0,
    }


def initialize_or_validate_target_db(db_path: Path) -> dict[str, Any]:
    exists_before = db_path.exists()
    if exists_before:
        with sqlite3.connect(str(db_path)) as conn:
            apply_v3_schema(conn)
            counts = _table_counts(conn)
            populated = {table: count for table, count in counts.items() if count and table != "v3_schema_version"}
            if populated:
                raise RuntimeError("V3_PHASE3B_TARGET_DB_ALREADY_POPULATED:" + json.dumps(populated, sort_keys=True))
            schema_version = validate_v3_schema(conn)
            integrity = V3CanonicalMigrationEngine(conn).validate_integrity()
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(db_path)) as conn:
            apply_v3_schema(conn)
            schema_version = validate_v3_schema(conn)
            integrity = V3CanonicalMigrationEngine(conn).validate_integrity()
    return {
        "db_path": str(db_path),
        "existed_before": exists_before,
        "schema_required_table_count": schema_version,
        "integrity": integrity,
    }


def run_yahoo_seed(
    *,
    target_db: Path,
    artifact_root: Path,
    company_baseline_csv: Path,
    raw_cache_db: Path,
    bootstrap_root: Path,
    post_a_root: Path,
    post_a2_root: Path,
    post_a3_root: Path,
    bootstrap_run_id: str,
    migration_run_id: str,
    now_utc: str | None = None,
) -> dict[str, Any]:
    now = now_utc or utc_now_text()
    artifact_root.mkdir(parents=True, exist_ok=True)
    prepared = prepare_yahoo_seed(
        company_baseline_csv=company_baseline_csv,
        raw_cache_db=raw_cache_db,
        bootstrap_root=bootstrap_root,
        post_a_root=post_a_root,
        post_a2_root=post_a2_root,
        post_a3_root=post_a3_root,
        bootstrap_run_id=bootstrap_run_id,
        migration_run_id=migration_run_id,
    )
    dry_conn = configure_connection(sqlite3.connect(":memory:"))
    apply_v3_schema(dry_conn)
    dry_company_summary = apply_company_baseline(dry_conn, prepared.baseline, now_utc=now)
    dry_summary = V3CanonicalMigrationEngine(dry_conn).apply_source_batch(
        prepared.candidates,
        source=YAHOO_SEED_SOURCE,
        migration_run_id=migration_run_id,
        dry_apply=False,
        now_utc=now,
    ).to_dict()
    dry_gate = validate_dry_gate(dry_conn, prepared.accounting, dry_summary)
    _write_json(artifact_root / "candidate_accounting.json", prepared.accounting)
    _write_json(artifact_root / "dry_apply_summary.json", {"company_summary": dry_company_summary, "apply_summary": dry_summary, "dry_gate": dry_gate})
    if not dry_gate["passed"]:
        raise RuntimeError("V3_PHASE3B_DRY_APPLY_GATE_FAILED:" + json.dumps(dry_gate, sort_keys=True))

    preflight = initialize_or_validate_target_db(target_db)
    with sqlite3.connect(str(target_db)) as conn:
        conn = configure_connection(conn)
        company_summary = apply_company_baseline(conn, prepared.baseline, now_utc=now)
        apply_summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
            prepared.candidates,
            source=YAHOO_SEED_SOURCE,
            migration_run_id=migration_run_id,
            now_utc=now,
        ).to_dict()
        production = build_production_summary(conn, prepared=prepared, apply_summary=apply_summary, company_summary=company_summary)
        idempotency = run_idempotency_validation(conn, prepared.candidates, migration_run_id=migration_run_id, before=production["row_counts"], now_utc=now)
        production["idempotency"] = idempotency
        backup_path = create_source_boundary_backup(target_db, artifact_root)
    production["preflight"] = preflight
    production["backup_path"] = str(backup_path)
    write_phase3b_artifacts(artifact_root, production)
    return production


def validate_dry_gate(conn: sqlite3.Connection, accounting: dict[str, Any], dry_summary: dict[str, Any]) -> dict[str, Any]:
    cava_q1 = conn.execute(
        """
        SELECT COUNT(*)
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id = q.company_id
        WHERE c.ticker = 'CAVA' AND q.fiscal_year = 2026 AND q.fiscal_quarter = 'Q1'
        """
    ).fetchone()[0]
    neup_qs = [
        tuple(row)
        for row in conn.execute(
            """
            SELECT q.period_end_date, q.fiscal_year, q.fiscal_quarter
            FROM v3_quarter q
            JOIN v3_company c ON c.company_id = q.company_id
            WHERE c.ticker = 'NEUP'
            ORDER BY q.period_end_date
            """
        )
    ]
    lfcr_variant = conn.execute(
        """
        SELECT COUNT(*)
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id = q.company_id
        WHERE c.ticker = 'LFCR' AND q.period_end_date = '2025-09-30'
        """
    ).fetchone()[0]
    integrity = V3CanonicalMigrationEngine(conn).validate_integrity()
    passed = (
        accounting["reconciles_to_normalized_rows"]
        and accounting["normalized_rows"] == 14373
        and accounting["missing_metadata_rows"] == 0
        and cava_q1 == 1
        and ("2025-09-30", 2026, "Q1") in neup_qs
        and ("2025-12-31", 2026, "Q2") in neup_qs
        and ("2026-03-31", 2026, "Q3") in neup_qs
        and lfcr_variant == 0
        and integrity["quick_check"] == "ok"
        and integrity["foreign_key_check_rows"] == 0
    )
    return {
        "passed": bool(passed),
        "candidate_accounting_reconciles": accounting["reconciles_to_normalized_rows"],
        "normalized_rows": accounting["normalized_rows"],
        "missing_metadata_rows": accounting["missing_metadata_rows"],
        "cava_fy2026_q1_count": int(cava_q1),
        "neup_quarters": neup_qs,
        "lfcr_2025_09_30_q_count": int(lfcr_variant),
        "integrity": integrity,
        "dry_rows": dry_summary["rows"],
    }


def build_production_summary(
    conn: sqlite3.Connection,
    *,
    prepared: YahooSeedPrepared,
    apply_summary: dict[str, Any],
    company_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "artifact_version": YAHOO_SEED_ARTIFACT_VERSION,
        "company_summary": company_summary,
        "candidate_accounting": prepared.accounting,
        "apply_summary": apply_summary,
        "row_counts": _table_counts(conn),
        "canonical_q_distribution": canonical_q_distribution(conn),
        "coverage": coverage_summary(conn),
        "field_contribution": field_contribution_summary(apply_summary),
        "issue_summary": issue_summary(conn),
        "special_cases": special_case_summary(conn),
        "integrity": production_integrity(conn),
        "v2_contribution": 0,
        "legacy_contribution": 0,
        "provider_network_calls": 0,
        "rawcandle_changes": 0,
    }


def run_idempotency_validation(
    conn: sqlite3.Connection,
    candidates: list[V3CanonicalMigrationCandidate],
    *,
    migration_run_id: str,
    before: dict[str, int],
    now_utc: str,
) -> dict[str, Any]:
    issue_count_before = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
    second = V3CanonicalMigrationEngine(conn).apply_source_batch(
        candidates,
        source=YAHOO_SEED_SOURCE,
        migration_run_id=migration_run_id,
        now_utc=now_utc,
    ).to_dict()
    after = _table_counts(conn)
    issue_count_after = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
    inappropriate_fills = 0
    for field_counts in second["field_contributions"].values():
        inappropriate_fills += int(field_counts.get("FIELD_INSERTED", 0))
        inappropriate_fills += int(field_counts.get("FIELD_FILLED_FROM_NULL", 0))
        inappropriate_fills += int(field_counts.get("FIELD_DERIVED", 0))
    return {
        "second_run_summary": second,
        "row_counts_unchanged": before == after,
        "second_run_q_creations": int(second["rows"].get("canonical_quarters_created", 0)),
        "inappropriate_field_fills": int(inappropriate_fills),
        "duplicate_issue_count": int(issue_count_after - issue_count_before),
    }


def canonical_q_distribution(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT c.company_id, c.active, COUNT(q.quarter_id) AS q_count
        FROM v3_company c
        LEFT JOIN v3_quarter q ON q.company_id = c.company_id
        GROUP BY c.company_id, c.active
        """
    ).fetchall()
    counts = [int(row["q_count"]) for row in rows]
    return {
        "companies_with_0_q": sum(1 for count in counts if count == 0),
        "companies_with_0_q_active": sum(1 for row in rows if int(row["q_count"]) == 0 and int(row["active"]) == 1),
        "companies_with_0_q_inactive": sum(1 for row in rows if int(row["q_count"]) == 0 and int(row["active"]) == 0),
        "companies_with_1_to_3_q": sum(1 for count in counts if 1 <= count <= 3),
        "companies_with_4_q": sum(1 for count in counts if count == 4),
        "companies_with_4_plus_q": sum(1 for count in counts if count >= 4),
        "companies_with_5_plus_q": sum(1 for count in counts if count >= 5),
        "max_q_count": max(counts) if counts else 0,
        "active_q_rows": conn.execute(
            """
            SELECT COUNT(*)
            FROM v3_quarter q
            JOIN v3_company c ON c.company_id = q.company_id
            WHERE c.active = 1
            """
        ).fetchone()[0],
        "inactive_q_rows": conn.execute(
            """
            SELECT COUNT(*)
            FROM v3_quarter q
            JOIN v3_company c ON c.company_id = q.company_id
            WHERE c.active = 0
            """
        ).fetchone()[0],
    }


def coverage_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    q_total = conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0]
    publish_known = conn.execute("SELECT COUNT(*) FROM v3_quarter WHERE publish_date IS NOT NULL").fetchone()[0]
    field_missing: dict[str, int] = {}
    field_present: dict[str, int] = {}
    for field_name in CANONICAL_FIELD_NAMES:
        present = conn.execute(f"SELECT COUNT(*) FROM v3_quarter_fundamentals WHERE {field_name} IS NOT NULL").fetchone()[0]
        field_present[field_name] = int(present)
        field_missing[field_name] = int(q_total - present)
    core_ready = conn.execute(
        """
        SELECT COUNT(*)
        FROM v3_quarter_fundamentals
        WHERE revenue IS NOT NULL
          AND ebitda IS NOT NULL
          AND free_cashflow IS NOT NULL
          AND cash IS NOT NULL
          AND total_debt IS NOT NULL
          AND shares_outstanding IS NOT NULL
          AND shares_outstanding > 0
        """
    ).fetchone()[0]
    return {
        "canonical_q_total": int(q_total),
        "publish_date_known": int(publish_known),
        "publish_date_null": int(q_total - publish_known),
        "publication_ready_percentage": round((publish_known / q_total * 100.0) if q_total else 0.0, 2),
        "core_ready_q": int(core_ready),
        "core_not_ready_q": int(q_total - core_ready),
        "field_present": field_present,
        "field_missing": field_missing,
        "core_missing_field_breakdown": {field_name: field_missing[field_name] for field_name in CORE_READY_FIELDS},
    }


def field_contribution_summary(apply_summary: dict[str, Any]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for field_name, counts in apply_summary["field_contributions"].items():
        out[field_name] = {
            "inserted_or_filled": int(counts.get("FIELD_INSERTED", 0)) + int(counts.get("FIELD_FILLED_FROM_NULL", 0)),
            "same_confirmed": int(counts.get("FIELD_CONFIRMED_SAME", 0)),
            "rounding_confirmed": int(counts.get("FIELD_ROUNDING_EQUIVALENT", 0)),
            "derived": int(counts.get("FIELD_DERIVED", 0)),
            "conflict": int(counts.get("FIELD_CONFLICT", 0)),
            "missing": int(counts.get("FIELD_SKIPPED_NULL", 0)),
        }
    return out


def issue_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT issue_type, COUNT(*) AS count
        FROM v3_resolution_issue
        GROUP BY issue_type
        ORDER BY issue_type
        """
    ).fetchall()
    return {
        "total": sum(int(row["count"]) for row in rows),
        "by_type": {str(row["issue_type"]): int(row["count"]) for row in rows},
    }


def special_case_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    cava = conn.execute(
        """
        SELECT q.fiscal_year, q.fiscal_quarter, q.period_end_date, q.publish_date,
               f.revenue, f.cash, f.total_debt
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id = q.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id = q.quarter_id
        WHERE c.ticker = 'CAVA' AND q.fiscal_year = 2026 AND q.fiscal_quarter = 'Q1'
        """
    ).fetchall()
    neup = conn.execute(
        """
        SELECT q.period_end_date, q.fiscal_year, q.fiscal_quarter, q.publish_date
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id = q.company_id
        WHERE c.ticker = 'NEUP'
        ORDER BY q.period_end_date
        """
    ).fetchall()
    lfcr_variant_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id = q.company_id
        WHERE c.ticker = 'LFCR' AND q.period_end_date = '2025-09-30'
        """
    ).fetchone()[0]
    return {
        "cava_fy2026_q1_rows": [dict(row) for row in cava],
        "cava_fy2026_q1_count": len(cava),
        "neup_rows": [dict(row) for row in neup],
        "lfcr_2025_09_30_canonical_q_count": int(lfcr_variant_count),
    }


def production_integrity(conn: sqlite3.Connection) -> dict[str, Any]:
    integrity = V3CanonicalMigrationEngine(conn).validate_integrity()
    duplicate_company = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT market, ticker
            FROM v3_company
            GROUP BY market, ticker
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    duplicate_work_unit = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT company_id, fiscal_year, fiscal_quarter
            FROM v3_quarter
            GROUP BY company_id, fiscal_year, fiscal_quarter
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    orphan_audit = conn.execute(
        """
        SELECT COUNT(*)
        FROM v3_migration_audit a
        LEFT JOIN v3_company c ON c.company_id = a.company_id
        WHERE a.company_id IS NOT NULL AND c.company_id IS NULL
        """
    ).fetchone()[0]
    orphan_issues = conn.execute(
        """
        SELECT COUNT(*)
        FROM v3_resolution_issue i
        LEFT JOIN v3_quarter q ON q.quarter_id = i.quarter_id
        WHERE i.quarter_id IS NOT NULL AND q.quarter_id IS NULL
        """
    ).fetchone()[0]
    return {
        **integrity,
        "duplicate_company_key": int(duplicate_company),
        "duplicate_work_unit_key": int(duplicate_work_unit),
        "orphan_migration_audit_company_refs": int(orphan_audit),
        "orphan_resolution_issue_quarter_refs": int(orphan_issues),
    }


def create_source_boundary_backup(target_db: Path, artifact_root: Path) -> Path:
    backup_dir = artifact_root / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / "rc_fundamentals_v3.post_yahoo_seed.db"
    shutil.copy2(target_db, backup_path)
    with sqlite3.connect(str(backup_path)) as conn:
        integrity = production_integrity(configure_connection(conn))
    if integrity["quick_check"] != "ok" or integrity["foreign_key_check_rows"] != 0:
        raise RuntimeError("V3_PHASE3B_BACKUP_INTEGRITY_FAILED")
    return backup_path


def write_phase3b_artifacts(artifact_root: Path, summary: dict[str, Any]) -> None:
    _write_json(artifact_root / "yahoo_source_contribution.json", summary)
    _write_json(artifact_root / "phase3c_baseline.json", summary)
    _write_field_contribution_csv(artifact_root / "yahoo_field_contribution.csv", summary["field_contribution"])
    _write_q_coverage_csv(artifact_root / "canonical_q_coverage.csv", summary)
    _write_core_readiness_csv(artifact_root / "core_readiness.csv", summary["coverage"])
    _write_extended_field_coverage_csv(artifact_root / "extended_field_coverage.csv", summary["coverage"])
    _write_publication_coverage_csv(artifact_root / "publication_coverage.csv", summary["coverage"])
    _write_active_inactive_csv(artifact_root / "active_inactive_coverage.csv", summary["canonical_q_distribution"])
    _write_resolution_issues_csv(artifact_root / "resolution_issues.csv", summary["issue_summary"])
    _write_text(artifact_root / "preflight.md", _preflight_text(summary))
    _write_text(artifact_root / "special_case_results.md", _special_case_text(summary["special_cases"]))
    _write_text(artifact_root / "idempotency_validation.md", _idempotency_text(summary["idempotency"]))
    _write_text(artifact_root / "production_v3_integrity.md", _integrity_text(summary["integrity"]))
    _write_text(artifact_root / "recommended_next_step.md", "MASTER PLAN PHASE 3C - V2 METADATA & VALUE ENRICHMENT\n")


def _apply_bootstrap_candidate_metadata(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for item in _read_jsonl(path):
        _set_meta(
            metadata,
            item["ticker"],
            item["period_end_date"],
            item["fiscal_year"],
            item["fiscal_quarter"],
            item.get("publish_date"),
            item.get("market_availability_date"),
            item.get("provider_details", {}).get("fiscal_identity_source", "PHASE2D_BOOTSTRAP_CANDIDATE"),
            "DIRECT_CANONICAL_CANDIDATE",
        )


def _apply_duplicate_candidate_metadata(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for row in _read_csv(path):
        _set_meta(
            metadata,
            row["ticker"],
            row["period_end_date"],
            row["fiscal_year"],
            row["fiscal_quarter"],
            row.get("publish_date"),
            row.get("publish_date"),
            "PHASE2D_DUPLICATE_CANDIDATE_KEY_ANALYSIS",
            "COMPLEMENTARY_SAME_FISCAL_Q",
            period_date_policy="SAFE_VARIANT",
            candidate_issue_type="DUPLICATE_FISCAL_WORK_UNIT",
        )


def _apply_post_a_metadata(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for row in _read_csv(path):
        fy = row.get("recovered_fiscal_year") or row.get("exact_fiscal_year") or row.get("sequential_fiscal_year")
        fq = row.get("recovered_fiscal_quarter") or row.get("exact_fiscal_quarter") or row.get("sequential_fiscal_quarter")
        if not fy or not fq:
            continue
        _set_meta(
            metadata,
            row["ticker"],
            row["period_end_date"],
            fy,
            fq,
            row.get("recovered_publish_date") or row.get("exact_publish_date"),
            row.get("recovered_publish_date") or row.get("exact_publish_date"),
            row.get("identity_recovery_source") or row.get("exact_identity_source") or "PHASE2D_POST_A_METADATA_RECOVERY",
            "DIRECT_CANONICAL_CANDIDATE",
        )


def _apply_sequential_metadata(metadata: dict[tuple[str, str], YahooSeedMetadata], sequence_path: Path, publish_path: Path) -> None:
    publish = {(row["ticker"], row["period_end_date"]): row.get("recovered_publish_date") for row in _read_csv(publish_path)}
    for row in _read_csv(sequence_path):
        if not row.get("fiscal_year") or not row.get("fiscal_quarter"):
            continue
        _set_meta(
            metadata,
            row["ticker"],
            row["period_end_date"],
            row["fiscal_year"],
            row["fiscal_quarter"],
            publish.get((row["ticker"], row["period_end_date"])),
            publish.get((row["ticker"], row["period_end_date"])),
            "PHASE2D_POST_A_VALIDATED_SEQUENTIAL_DERIVED",
            "DIRECT_CANONICAL_CANDIDATE",
        )


def _apply_a2_metadata(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for row in _read_csv(path):
        if not row.get("final_fiscal_year") or not row.get("final_fiscal_quarter"):
            continue
        _set_meta(
            metadata,
            row["ticker"],
            row["period_end_date"],
            row["final_fiscal_year"],
            row["final_fiscal_quarter"],
            None,
            None,
            row.get("derivation_method") or "PHASE2D_POST_A2_COMPANY_PATTERN",
            "DIRECT_CANONICAL_CANDIDATE",
        )


def _apply_a2_resolved_conflicts(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for row in _read_csv(path):
        _set_meta(
            metadata,
            row["ticker"],
            row["period_end_date"],
            row["final_fiscal_year"],
            row["final_fiscal_quarter"],
            None,
            None,
            row.get("confidence_class") or "PHASE2D_POST_A2_ANCHOR_CONFLICT_RESOLUTION",
            "FISCAL_MAPPING_CORRECTION",
        )


def _apply_a2_manual(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for row in _read_csv(path):
        _set_meta(
            metadata,
            row["ticker"],
            row["period_end_date"],
            row["final_fiscal_year"],
            row["final_fiscal_quarter"],
            None,
            None,
            row.get("evidence_source") or "USER_SUPPLIED_MANUAL_FISCAL_CALENDAR",
            "FISCAL_MAPPING_CORRECTION",
        )


def _apply_a2_company_patterns(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    by_ticker: dict[str, dict[int, tuple[str, int]]] = defaultdict(dict)
    for row in _read_csv(path):
        if not row.get("ticker") or not row.get("month") or not row.get("fiscal_quarter"):
            continue
        by_ticker[row["ticker"].strip().upper()][int(row["month"])] = (
            row["fiscal_quarter"].strip().upper(),
            int(row.get("fiscal_year_offset") or 0),
        )
    for ticker, period_end_date in list(metadata):
        month_map = by_ticker.get(ticker)
        if not month_map:
            continue
        year, month = _year_month(period_end_date)
        pattern = month_map.get(month)
        if pattern is None:
            continue
        fiscal_quarter, fiscal_year_offset = pattern
        existing = metadata[(ticker, period_end_date)]
        fiscal_year = year + fiscal_year_offset
        disposition = existing.disposition
        if existing.fiscal_year != fiscal_year or existing.fiscal_quarter != fiscal_quarter:
            disposition = "FISCAL_MAPPING_CORRECTION"
        metadata[(ticker, period_end_date)] = YahooSeedMetadata(
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            publish_date=existing.publish_date,
            market_availability_date=existing.market_availability_date,
            evidence_source="PHASE2D_POST_A2_COMPANY_FISCAL_PATTERN",
            disposition=disposition,
            candidate_can_create_quarter=existing.candidate_can_create_quarter,
            candidate_issue_type=existing.candidate_issue_type,
            period_date_policy=existing.period_date_policy,
            official_period_end_date=existing.official_period_end_date,
        )


def _apply_a3_recovered(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for row in _read_csv(path):
        if not row.get("final_fiscal_year") or not row.get("final_fiscal_quarter"):
            continue
        _set_meta(
            metadata,
            row["ticker"],
            row["period_end_date"],
            row["final_fiscal_year"],
            row["final_fiscal_quarter"],
            row.get("publication_date_recovered"),
            row.get("publication_date_recovered"),
            row.get("evidence_quality") or row.get("recovery_method") or "PHASE2D_POST_A3_OFFICIAL_FISCAL_RECOVERY",
            "DIRECT_CANONICAL_CANDIDATE",
        )


def _apply_company_recent_calendar(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for row in _read_csv(path):
        if not row.get("fiscal_year") or not row.get("fiscal_quarter") or not row.get("period_end_date"):
            continue
        _set_meta(
            metadata,
            row["ticker"],
            row["period_end_date"],
            row["fiscal_year"],
            row["fiscal_quarter"],
            row.get("official_result_publication_date"),
            row.get("official_result_publication_date"),
            row.get("evidence_quality") or "OFFICIAL_RECENT_RESULT_CALENDAR",
            "DIRECT_CANONICAL_CANDIDATE",
        )


def _apply_neup_manual(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for row in _read_csv(path):
        if row["period_end_date"] == "2026-06-30":
            continue
        _set_meta(
            metadata,
            row["ticker"],
            row["period_end_date"],
            row["fiscal_year"],
            row["fiscal_quarter"],
            row.get("publish_date"),
            row.get("publish_date"),
            row.get("evidence_quality") or "USER_SUPPLIED_OFFICIAL_EARNINGS_RELEASE",
            "FISCAL_MAPPING_CORRECTION" if row["period_end_date"] in {"2025-09-30", "2026-03-31"} else "DIRECT_CANONICAL_CANDIDATE",
        )


def _apply_reconciliation_exceptions(metadata: dict[tuple[str, str], YahooSeedMetadata], path: Path) -> None:
    for row in _read_csv(path):
        ticker = row["ticker"]
        if row["exception_type"] == "TRANSITION_PERIOD_DATE_VARIANT":
            _set_meta(
                metadata,
                ticker,
                row["yahoo_period_end_date"],
                2025,
                "Q4",
                row.get("publication_date"),
                row.get("publication_date"),
                row["exception_type"],
                "PROVIDER_PERIOD_VARIANT_EXCLUDED",
                candidate_can_create_quarter=False,
                candidate_issue_type="TRANSITION_PERIOD_VARIANT",
                official_period_end_date=row.get("official_period_end_date"),
            )
            continue
        _set_meta(
            metadata,
            ticker,
            row["yahoo_period_end_date"],
            row["resolved_fiscal_year"],
            row["resolved_fiscal_quarter"],
            row.get("publication_date"),
            row.get("publication_date"),
            row["exception_type"],
            "COMPLEMENTARY_SAME_FISCAL_Q" if ticker == "CAVA" else "FISCAL_MAPPING_CORRECTION",
            candidate_issue_type="DUPLICATE_FISCAL_WORK_UNIT" if ticker == "CAVA" else "FISCAL_MAPPING_CORRECTION",
            period_date_policy="SAFE_VARIANT" if ticker == "CAVA" else "CONFLICT",
            official_period_end_date=row.get("official_period_end_date"),
        )
        if ticker == "CAVA" and row.get("competing_period_end_date"):
            _set_meta(
                metadata,
                ticker,
                row["competing_period_end_date"],
                row["resolved_fiscal_year"],
                row["resolved_fiscal_quarter"],
                row.get("publication_date"),
                row.get("publication_date"),
                row["exception_type"],
                "COMPLEMENTARY_SAME_FISCAL_Q",
                candidate_issue_type="DUPLICATE_FISCAL_WORK_UNIT",
                period_date_policy="SAFE_VARIANT",
                official_period_end_date=row.get("official_period_end_date"),
            )


def _set_meta(
    metadata: dict[tuple[str, str], YahooSeedMetadata],
    ticker: str,
    period_end_date: str,
    fiscal_year: Any,
    fiscal_quarter: Any,
    publish_date: Any,
    market_availability_date: Any,
    evidence_source: str,
    disposition: str,
    *,
    candidate_can_create_quarter: bool = True,
    candidate_issue_type: str | None = None,
    period_date_policy: str = "CONFLICT",
    official_period_end_date: str | None = None,
) -> None:
    publish = _iso_or_none(publish_date)
    market_availability = _iso_or_none(market_availability_date) or publish
    metadata[(ticker.strip().upper(), str(period_end_date))] = YahooSeedMetadata(
        fiscal_year=int(fiscal_year),
        fiscal_quarter=str(fiscal_quarter).strip().upper(),
        publish_date=publish,
        market_availability_date=market_availability,
        evidence_source=evidence_source,
        disposition=disposition,
        candidate_can_create_quarter=candidate_can_create_quarter,
        candidate_issue_type=candidate_issue_type,
        period_date_policy=period_date_policy,
        official_period_end_date=official_period_end_date,
    )


def _canonical_values(row: dict[str, Any]) -> dict[str, float]:
    values = {field_name: row["values"][field_name] for field_name in row["values"] if field_name in CANONICAL_FIELD_NAMES}
    operating_income = row["provider_details"].get("operating_income")
    if operating_income is not None:
        values["operating_income"] = float(operating_income)
    return values


def _table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    tables = (
        "v3_company",
        "v3_quarter",
        "v3_quarter_fundamentals",
        "v3_migration_audit",
        "v3_resolution_issue",
    )
    return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    return None


def _year_month(period_end_date: str) -> tuple[int, int]:
    return int(period_end_date[:4]), int(period_end_date[5:7])


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def _write_text(path: Path, text: str) -> None:
    path.write_text(text)


def _write_field_contribution_csv(path: Path, data: dict[str, dict[str, int]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["field", "inserted_or_filled", "same_confirmed", "rounding_confirmed", "derived", "conflict", "missing"])
        writer.writeheader()
        for field_name in sorted(data):
            writer.writerow({"field": field_name, **data[field_name]})


def _write_q_coverage_csv(path: Path, summary: dict[str, Any]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        for key, value in summary["canonical_q_distribution"].items():
            writer.writerow([key, value])


def _write_core_readiness_csv(path: Path, coverage: dict[str, Any]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        writer.writerow(["core_ready_q", coverage["core_ready_q"]])
        writer.writerow(["core_not_ready_q", coverage["core_not_ready_q"]])
        for field_name, missing in coverage["core_missing_field_breakdown"].items():
            writer.writerow([f"missing_{field_name}", missing])


def _write_extended_field_coverage_csv(path: Path, coverage: dict[str, Any]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["field", "present", "missing"])
        for field_name in CANONICAL_FIELD_NAMES:
            writer.writerow([field_name, coverage["field_present"][field_name], coverage["field_missing"][field_name]])


def _write_publication_coverage_csv(path: Path, coverage: dict[str, Any]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        for key in ("canonical_q_total", "publish_date_known", "publish_date_null", "publication_ready_percentage"):
            writer.writerow([key, coverage[key]])


def _write_active_inactive_csv(path: Path, distribution: dict[str, Any]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        for key in ("active_q_rows", "inactive_q_rows", "companies_with_0_q_active", "companies_with_0_q_inactive"):
            writer.writerow([key, distribution[key]])


def _write_resolution_issues_csv(path: Path, issue_data: dict[str, Any]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["issue_type", "count"])
        for issue_type, count in issue_data["by_type"].items():
            writer.writerow([issue_type, count])


def _preflight_text(summary: dict[str, Any]) -> str:
    return (
        "# Phase 3B Preflight\n\n"
        f"Target DB: `{summary['preflight']['db_path']}`\n\n"
        f"Schema required table count: `{summary['preflight']['schema_required_table_count']}`\n\n"
        f"Company rows: `{summary['company_summary']['company_rows']}`\n\n"
        "Yahoo/network calls: `0`\n"
    )


def _special_case_text(data: dict[str, Any]) -> str:
    return (
        "# Special Case Results\n\n"
        f"CAVA FY2026 Q1 count: `{data['cava_fy2026_q1_count']}`\n\n"
        f"NEUP rows: `{data['neup_rows']}`\n\n"
        f"LFCR 2025-09-30 canonical Q count: `{data['lfcr_2025_09_30_canonical_q_count']}`\n"
    )


def _idempotency_text(data: dict[str, Any]) -> str:
    return (
        "# Idempotency Validation\n\n"
        f"Row counts unchanged: `{data['row_counts_unchanged']}`\n\n"
        f"Second-run Q creations: `{data['second_run_q_creations']}`\n\n"
        f"Inappropriate field fills: `{data['inappropriate_field_fills']}`\n\n"
        f"Duplicate issue count: `{data['duplicate_issue_count']}`\n"
    )


def _integrity_text(data: dict[str, Any]) -> str:
    return (
        "# Production V3 Integrity\n\n"
        f"quick_check: `{data['quick_check']}`\n\n"
        f"foreign_key_check_rows: `{data['foreign_key_check_rows']}`\n\n"
        f"duplicate_company_key: `{data['duplicate_company_key']}`\n\n"
        f"duplicate_work_unit_key: `{data['duplicate_work_unit_key']}`\n"
    )
