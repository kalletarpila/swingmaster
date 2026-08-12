from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.fundamentals_v2.simfin_seed import (
    LEGACY_DB_NAME,
    QUARTERLY_PERIODS,
    SourceFileManifest,
    first_non_null_date,
    load_active_tickers_readonly,
    parse_float,
    read_csv_rows,
    statement_key,
)


BANK_PROFILE = "BANK"
INSURANCE_PROFILE = "INSURANCE"
BANK_PROVIDER = "SIMFIN_BANK_BULK"
INSURANCE_PROVIDER = "SIMFIN_INSURANCE_BULK"
BUILDER_VERSION = "rc_fundamentals_v2_simfin_specialized_v1"
SOURCE_FILES = {
    BANK_PROFILE: {
        "income": "us-income-banks-quarterly.csv",
        "balance": "us-balance-banks-quarterly.csv",
        "cashflow": "us-cashflow-banks-quarterly.csv",
    },
    INSURANCE_PROFILE: {
        "income": "us-income-insurance-quarterly.csv",
        "balance": "us-balance-insurance-quarterly.csv",
        "cashflow": "us-cashflow-insurance-quarterly.csv",
    },
}


@dataclass(frozen=True)
class FieldPolicy:
    fact_column: str
    dataset: str
    source_field: str


BANK_FIELD_POLICY = (
    FieldPolicy("revenue", "income", "Revenue"),
    FieldPolicy("provision_for_loan_losses", "income", "Provision for Loan Losses"),
    FieldPolicy("net_revenue_after_provisions", "income", "Net Revenue after Provisions"),
    FieldPolicy("total_non_interest_expense", "income", "Total Non-Interest Expense"),
    FieldPolicy("operating_income", "income", "Operating Income (Loss)"),
    FieldPolicy("non_operating_income", "income", "Non-Operating Income (Loss)"),
    FieldPolicy("pretax_income", "income", "Pretax Income (Loss)"),
    FieldPolicy("income_tax", "income", "Income Tax (Expense) Benefit, Net"),
    FieldPolicy("continuing_income", "income", "Income (Loss) from Continuing Operations"),
    FieldPolicy("net_income", "income", "Net Income"),
    FieldPolicy("net_income_common", "income", "Net Income (Common)"),
    FieldPolicy("cash", "balance", "Cash, Cash Equivalents & Short Term Investments"),
    FieldPolicy("interbank_assets", "balance", "Interbank Assets"),
    FieldPolicy("investments", "balance", "Short & Long Term Investments"),
    FieldPolicy("net_loans", "balance", "Net Loans"),
    FieldPolicy("total_assets", "balance", "Total Assets"),
    FieldPolicy("total_deposits", "balance", "Total Deposits"),
    FieldPolicy("short_term_debt", "balance", "Short Term Debt"),
    FieldPolicy("long_term_debt", "balance", "Long Term Debt"),
    FieldPolicy("total_liabilities", "balance", "Total Liabilities"),
    FieldPolicy("preferred_equity", "balance", "Preferred Equity"),
    FieldPolicy("total_equity", "balance", "Total Equity"),
    FieldPolicy("weighted_average_shares_basic", "income", "Shares (Basic)"),
    FieldPolicy("weighted_average_shares_diluted", "income", "Shares (Diluted)"),
)

INSURANCE_FIELD_POLICY = (
    FieldPolicy("revenue", "income", "Revenue"),
    FieldPolicy("total_claims_losses", "income", "Total Claims & Losses"),
    FieldPolicy("operating_income", "income", "Operating Income (Loss)"),
    FieldPolicy("pretax_income", "income", "Pretax Income (Loss)"),
    FieldPolicy("income_tax", "income", "Income Tax (Expense) Benefit, Net"),
    FieldPolicy("affiliate_income", "income", "Income (Loss) from Affiliates, Net of Taxes"),
    FieldPolicy("continuing_income", "income", "Income (Loss) from Continuing Operations"),
    FieldPolicy("net_income", "income", "Net Income"),
    FieldPolicy("net_income_common", "income", "Net Income (Common)"),
    FieldPolicy("investments", "balance", "Total Investments"),
    FieldPolicy("cash", "balance", "Cash, Cash Equivalents & Short Term Investments"),
    FieldPolicy("receivables", "balance", "Accounts & Notes Receivable"),
    FieldPolicy("ppe_net", "balance", "Property, Plant & Equipment, Net"),
    FieldPolicy("total_assets", "balance", "Total Assets"),
    FieldPolicy("insurance_reserves", "balance", "Insurance Reserves"),
    FieldPolicy("short_term_debt", "balance", "Short Term Debt"),
    FieldPolicy("long_term_debt", "balance", "Long Term Debt"),
    FieldPolicy("total_liabilities", "balance", "Total Liabilities"),
    FieldPolicy("policyholders_equity", "balance", "Policyholders Equity"),
    FieldPolicy("total_equity", "balance", "Total Equity"),
    FieldPolicy("weighted_average_shares_basic", "income", "Shares (Basic)"),
    FieldPolicy("weighted_average_shares_diluted", "income", "Shares (Diluted)"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_run_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def validate_v2_db_target(db_path: Path) -> Path:
    resolved = db_path.expanduser().resolve()
    if resolved.name == LEGACY_DB_NAME:
        raise ValueError("RC_V2_SPECIALIZED_REFUSES_LEGACY_DB_TARGET")
    return resolved


def read_source_manifest(simfin_dir: Path, profiles: Iterable[str]) -> list[SourceFileManifest]:
    root = simfin_dir.expanduser().resolve()
    manifests = []
    for profile in profiles:
        for dataset, filename in SOURCE_FILES[profile].items():
            path = root / filename
            if not path.exists():
                raise FileNotFoundError(f"SIMFIN_SPECIALIZED_SOURCE_FILE_MISSING:{path}")
            data = path.read_bytes()
            text = data.decode("utf-8-sig")
            first_line = text.splitlines()[0] if text.splitlines() else ""
            columns = tuple(first_line.split(";"))
            manifests.append(
                SourceFileManifest(
                    dataset=f"{profile.lower()}_{dataset}",
                    path=path,
                    filename=filename,
                    sha256=hashlib.sha256(data).hexdigest(),
                    bytes=path.stat().st_size,
                    row_count=max(len(text.splitlines()) - 1, 0),
                    columns=columns,
                )
            )
    return manifests


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS rc_v2_fundamental_bank_quarterly (
            quarter_id INTEGER PRIMARY KEY,
            revenue REAL,
            provision_for_loan_losses REAL,
            net_revenue_after_provisions REAL,
            total_non_interest_expense REAL,
            operating_income REAL,
            non_operating_income REAL,
            pretax_income REAL,
            income_tax REAL,
            continuing_income REAL,
            net_income REAL,
            net_income_common REAL,
            cash REAL,
            interbank_assets REAL,
            investments REAL,
            net_loans REAL,
            total_assets REAL,
            total_deposits REAL,
            short_term_debt REAL,
            long_term_debt REAL,
            total_liabilities REAL,
            preferred_equity REAL,
            total_equity REAL,
            weighted_average_shares_basic REAL,
            weighted_average_shares_diluted REAL,
            available_field_count INTEGER NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            FOREIGN KEY (quarter_id) REFERENCES rc_v2_quarter(quarter_id)
        );

        CREATE TABLE IF NOT EXISTS rc_v2_fundamental_insurance_quarterly (
            quarter_id INTEGER PRIMARY KEY,
            revenue REAL,
            total_claims_losses REAL,
            operating_income REAL,
            pretax_income REAL,
            income_tax REAL,
            affiliate_income REAL,
            continuing_income REAL,
            net_income REAL,
            net_income_common REAL,
            investments REAL,
            cash REAL,
            receivables REAL,
            ppe_net REAL,
            total_assets REAL,
            insurance_reserves REAL,
            short_term_debt REAL,
            long_term_debt REAL,
            total_liabilities REAL,
            policyholders_equity REAL,
            total_equity REAL,
            weighted_average_shares_basic REAL,
            weighted_average_shares_diluted REAL,
            available_field_count INTEGER NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            FOREIGN KEY (quarter_id) REFERENCES rc_v2_quarter(quarter_id)
        );
        """
    )


def import_simfin_specialized(
    *,
    db_path: Path,
    simfin_dir: Path,
    artifact_dir: Path,
    profiles: Iterable[str] = (BANK_PROFILE, INSURANCE_PROFILE),
    legacy_db: Path | None = None,
    market: str = "usa",
    dry_run: bool = False,
) -> dict[str, Any]:
    target = validate_v2_db_target(db_path)
    selected_profiles = tuple(profile.upper() for profile in profiles)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    run_db = target
    if dry_run:
        run_db = artifact_dir / "dry_run.db"
        if run_db.exists():
            run_db.unlink()
        shutil.copy2(target, run_db)
    manifests = read_source_manifest(simfin_dir, selected_profiles)
    active_tickers = load_active_tickers_readonly(legacy_db)
    import_run_id = "SIMFIN_SPECIALIZED_" + utc_run_stamp()
    now = utc_now()
    conn = sqlite3.connect(str(run_db))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        ensure_schema(conn)
        before = global_counts(conn)
        if not dry_run:
            ensure_import_run(conn, import_run_id, market, simfin_dir, now)
            insert_source_files(conn, import_run_id, manifests, now)
        results = []
        profile_conflicts = []
        for profile in selected_profiles:
            manifests_by_dataset = {manifest.dataset.split("_", 1)[1]: manifest for manifest in manifests if manifest.dataset.startswith(profile.lower() + "_")}
            result = import_profile(
                conn,
                profile=profile,
                manifests=manifests_by_dataset,
                import_run_id=import_run_id,
                market=market,
                active_tickers=active_tickers,
                now=now,
                dry_run=dry_run,
            )
            results.append(result)
            profile_conflicts.extend(result["profile_conflicts"])
        after = global_counts(conn) if dry_run else None
        if not dry_run:
            conn.commit()
            after = global_counts(conn)
        assert after is not None
        integrity = run_integrity_checks(conn)
        write_artifacts(
            conn=conn,
            artifact_dir=artifact_dir,
            manifests=manifests,
            results=results,
            profile_conflicts=profile_conflicts,
            integrity=integrity,
            dry_run=dry_run,
        )
        return {
            "import_run_id": import_run_id,
            "dry_run": dry_run,
            "results": results,
            "profile_conflict_count": len(profile_conflicts),
            "deltas": {key: after[key] - before[key] for key in before},
            **integrity,
        }
    finally:
        conn.close()


def import_profile(
    conn: sqlite3.Connection,
    *,
    profile: str,
    manifests: Mapping[str, SourceFileManifest],
    import_run_id: str,
    market: str,
    active_tickers: set[str],
    now: str,
    dry_run: bool,
) -> dict[str, Any]:
    rows_by_dataset = {dataset: read_csv_rows(manifest.path) for dataset, manifest in manifests.items()}
    keyed_rows = {dataset: {statement_key(row): row for row in rows} for dataset, rows in rows_by_dataset.items()}
    keys = sorted(set().union(*(set(rows) for rows in keyed_rows.values())))
    simfin_ids = sorted({key[0] for key in keys})
    metadata_by_simfin = metadata_from_rows(rows_by_dataset.values())
    profile_conflicts = []
    companies_created = quarters_created = facts_created = provenance_created = active_matches = 0
    for simfin_id in simfin_ids:
        meta = metadata_by_simfin[simfin_id]
        ticker = meta["ticker"]
        active = int(ticker in active_tickers)
        active_matches += active
        existing = conn.execute("SELECT * FROM rc_v2_company WHERE simfin_id=?", (simfin_id,)).fetchone()
        if existing and existing["company_profile"] != profile:
            profile_conflicts.append(
                {
                    "profile": profile,
                    "ticker": ticker,
                    "simfin_id": simfin_id,
                    "existing_profile": existing["company_profile"],
                    "action": "SKIPPED",
                }
            )
            continue
        if existing:
            company_id = int(existing["company_id"])
            if not dry_run and active and not int(existing["active"]):
                conn.execute("UPDATE rc_v2_company SET active=1, updated_at_utc=? WHERE company_id=?", (now, company_id))
        else:
            if dry_run:
                company_id = -simfin_id
            else:
                cur = conn.execute(
                    """
                    INSERT INTO rc_v2_company (
                        market, ticker, simfin_id, company_name, company_profile, active, created_at_utc, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (market, ticker, simfin_id, meta.get("company_name"), profile, active, now, now),
                )
                company_id = int(cur.lastrowid)
            companies_created += 1
    company_id_by_simfin = {
        int(row["simfin_id"]): int(row["company_id"])
        for row in conn.execute("SELECT company_id, simfin_id FROM rc_v2_company WHERE company_profile=?", (profile,))
    }
    if dry_run:
        company_id_by_simfin.update({simfin_id: -simfin_id for simfin_id in simfin_ids if simfin_id not in company_id_by_simfin})
    for key in keys:
        simfin_id, fiscal_year, fiscal_period, report_date = key
        if fiscal_period not in QUARTERLY_PERIODS:
            continue
        if simfin_id not in company_id_by_simfin:
            continue
        group = {dataset: keyed_rows[dataset].get(key) for dataset in keyed_rows}
        company_id = company_id_by_simfin[simfin_id]
        quarter_id, quarter_created = get_or_create_quarter(conn, company_id, key, group, now=now, dry_run=dry_run)
        quarters_created += int(quarter_created)
        values = map_profile_fields(profile, group)
        fact_created = insert_profile_fact(conn, profile=profile, quarter_id=quarter_id, values=values, now=now, dry_run=dry_run)
        facts_created += int(fact_created)
        provenance_created += insert_specialized_provenance(
            conn,
            profile=profile,
            quarter_id=quarter_id,
            rows=group,
            values=values,
            manifests=manifests,
            import_run_id=import_run_id,
            now=now,
            dry_run=dry_run,
        )
    return {
        "profile": profile,
        "companies": len(simfin_ids),
        "active_matches": active_matches,
        "quarter_keys": len(keys),
        "companies_created": companies_created,
        "quarters_created": quarters_created,
        "fact_rows_created": facts_created,
        "provenance_rows_created": provenance_created,
        "profile_conflicts": profile_conflicts,
    }


def metadata_from_rows(row_groups: Iterable[list[dict[str, str]]]) -> dict[int, dict[str, str]]:
    output = {}
    for rows in row_groups:
        for row in rows:
            simfin_id = int(row["SimFinId"])
            output.setdefault(
                simfin_id,
                {
                    "ticker": str(row.get("Ticker") or "").upper(),
                    "company_name": None,
                },
            )
    return output


def ensure_import_run(conn: sqlite3.Connection, import_run_id: str, market: str, simfin_dir: Path, now: str) -> None:
    conn.execute(
        "INSERT INTO rc_v2_import_run VALUES (?, ?, ?, ?, ?, NULL)",
        (import_run_id, market, str(simfin_dir.expanduser().resolve()), BUILDER_VERSION, now),
    )


def insert_source_files(conn: sqlite3.Connection, import_run_id: str, manifests: Iterable[SourceFileManifest], now: str) -> None:
    for manifest in manifests:
        conn.execute(
            """
            INSERT OR IGNORE INTO rc_v2_source_file VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                import_run_id,
                manifest.dataset,
                manifest.filename,
                str(manifest.path),
                manifest.sha256,
                manifest.bytes,
                manifest.row_count,
                json.dumps(list(manifest.columns), sort_keys=True),
                now,
            ),
        )


def get_or_create_quarter(
    conn: sqlite3.Connection,
    company_id: int,
    key: tuple[int, int, str, str],
    rows: Mapping[str, dict[str, str] | None],
    *,
    now: str,
    dry_run: bool,
) -> tuple[int, bool]:
    _simfin_id, fiscal_year, fiscal_period, report_date = key
    existing = None if company_id < 0 else conn.execute(
        """
        SELECT quarter_id FROM rc_v2_quarter
        WHERE company_id=? AND fiscal_year=? AND fiscal_period=? AND report_date=?
        """,
        (company_id, fiscal_year, fiscal_period, report_date),
    ).fetchone()
    if existing:
        return int(existing["quarter_id"]), False
    if dry_run:
        return -abs(hash((company_id, fiscal_year, fiscal_period, report_date))), True
    cur = conn.execute(
        """
        INSERT INTO rc_v2_quarter (
            company_id, fiscal_year, fiscal_period, report_date, publish_date, restated_date,
            quarter_identity_source, has_income, has_balance, has_cashflow, created_at_utc, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, 'SIMFIN_SPECIALIZED_FISCAL_PERIOD_REPORT_DATE', ?, ?, ?, ?, ?)
        """,
        (
            company_id,
            fiscal_year,
            fiscal_period,
            report_date,
            first_non_null_date(rows.values(), "Publish Date"),
            first_non_null_date(rows.values(), "Restated Date"),
            int(rows.get("income") is not None),
            int(rows.get("balance") is not None),
            int(rows.get("cashflow") is not None),
            now,
            now,
        ),
    )
    return int(cur.lastrowid), True


def map_profile_fields(profile: str, rows: Mapping[str, dict[str, str] | None]) -> dict[str, float | None]:
    policy = BANK_FIELD_POLICY if profile == BANK_PROFILE else INSURANCE_FIELD_POLICY
    values = {}
    for item in policy:
        row = rows.get(item.dataset) or {}
        values[item.fact_column] = parse_float(row.get(item.source_field))
    return values


def insert_profile_fact(
    conn: sqlite3.Connection,
    *,
    profile: str,
    quarter_id: int,
    values: Mapping[str, float | None],
    now: str,
    dry_run: bool,
) -> bool:
    if dry_run:
        return True
    table = "rc_v2_fundamental_bank_quarterly" if profile == BANK_PROFILE else "rc_v2_fundamental_insurance_quarterly"
    if conn.execute(f"SELECT 1 FROM {table} WHERE quarter_id=?", (quarter_id,)).fetchone():
        return False
    columns = list(values)
    available = sum(1 for value in values.values() if value is not None)
    conn.execute(
        f"""
        INSERT INTO {table} (
            quarter_id, {", ".join(columns)}, available_field_count, created_at_utc, updated_at_utc
        ) VALUES ({", ".join("?" for _ in range(len(columns) + 4))})
        """,
        (quarter_id, *[values[column] for column in columns], available, now, now),
    )
    return True


def insert_specialized_provenance(
    conn: sqlite3.Connection,
    *,
    profile: str,
    quarter_id: int,
    rows: Mapping[str, dict[str, str] | None],
    values: Mapping[str, float | None],
    manifests: Mapping[str, SourceFileManifest],
    import_run_id: str,
    now: str,
    dry_run: bool,
) -> int:
    policy = BANK_FIELD_POLICY if profile == BANK_PROFILE else INSURANCE_FIELD_POLICY
    provider = BANK_PROVIDER if profile == BANK_PROFILE else INSURANCE_PROVIDER
    inserted = 0
    for item in policy:
        value = values[item.fact_column]
        if value is None:
            continue
        row = rows.get(item.dataset)
        if row is None:
            continue
        if dry_run:
            inserted += 1
            continue
        manifest = manifests[item.dataset]
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
                quarter_id, field_name, provider, provider_field, source_dataset, source_file,
                source_file_sha256, transformation, source_value, import_run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'DIRECT', ?, ?, ?)
            """,
            (
                quarter_id,
                f"{profile.lower()}_{item.fact_column}",
                provider,
                item.source_field,
                f"{profile.lower()}_{item.dataset}",
                manifest.filename,
                manifest.sha256,
                row.get(item.source_field),
                import_run_id,
                now,
            ),
        )
        inserted += int(cur.rowcount > 0)
    return inserted


def global_counts(conn: sqlite3.Connection) -> dict[str, int]:
    counts = {
        "companies": conn.execute("SELECT COUNT(*) FROM rc_v2_company").fetchone()[0],
        "active_companies": conn.execute("SELECT COUNT(*) FROM rc_v2_company WHERE active=1").fetchone()[0],
        "quarters": conn.execute("SELECT COUNT(*) FROM rc_v2_quarter").fetchone()[0],
        "ordinary_facts": conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_quarterly").fetchone()[0],
        "provenance": conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source").fetchone()[0],
    }
    for table, key in (
        ("rc_v2_fundamental_bank_quarterly", "bank_facts"),
        ("rc_v2_fundamental_insurance_quarterly", "insurance_facts"),
    ):
        counts[key] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return counts


def run_integrity_checks(conn: sqlite3.Connection) -> dict[str, Any]:
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    duplicate_companies = conn.execute(
        "SELECT COUNT(*) FROM (SELECT simfin_id, COUNT(*) c FROM rc_v2_company GROUP BY simfin_id HAVING c > 1)"
    ).fetchone()[0]
    duplicate_quarters = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT company_id, fiscal_year, fiscal_period, report_date, COUNT(*) c
            FROM rc_v2_quarter GROUP BY company_id, fiscal_year, fiscal_period, report_date HAVING c > 1
        )
        """
    ).fetchone()[0]
    orphan_bank = conn.execute(
        "SELECT COUNT(*) FROM rc_v2_fundamental_bank_quarterly f LEFT JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL"
    ).fetchone()[0]
    orphan_insurance = conn.execute(
        "SELECT COUNT(*) FROM rc_v2_fundamental_insurance_quarterly f LEFT JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL"
    ).fetchone()[0]
    orphan_provenance = conn.execute(
        "SELECT COUNT(*) FROM rc_v2_fundamental_field_source p LEFT JOIN rc_v2_quarter q ON q.quarter_id=p.quarter_id WHERE q.quarter_id IS NULL"
    ).fetchone()[0]
    return {
        "integrity_check": integrity,
        "foreign_key_errors": len(fk_rows),
        "duplicate_company_simfin_ids": duplicate_companies,
        "duplicate_canonical_quarters": duplicate_quarters,
        "duplicate_bank_fact_keys": 0,
        "duplicate_insurance_fact_keys": 0,
        "orphan_bank_facts": orphan_bank,
        "orphan_insurance_facts": orphan_insurance,
        "orphan_provenance": orphan_provenance,
    }


def write_artifacts(
    *,
    conn: sqlite3.Connection,
    artifact_dir: Path,
    manifests: list[SourceFileManifest],
    results: list[dict[str, Any]],
    profile_conflicts: list[dict[str, Any]],
    integrity: Mapping[str, Any],
    dry_run: bool,
) -> None:
    write_csv(
        artifact_dir / "source_file_manifest.csv",
        [
            {
                "dataset": manifest.dataset,
                "filename": manifest.filename,
                "path": str(manifest.path),
                "sha256": manifest.sha256,
                "bytes": manifest.bytes,
                "row_count": manifest.row_count,
                "columns": "|".join(manifest.columns),
            }
            for manifest in manifests
        ],
    )
    for result in results:
        profile = result["profile"].lower()
        write_csv(artifact_dir / f"{profile}_dry_run.csv", [strip_conflicts(result)])
        write_csv(artifact_dir / f"{profile}_import_results.csv", [strip_conflicts(result)])
    write_csv(artifact_dir / "profile_conflicts.csv", profile_conflicts)
    query_to_csv(conn, artifact_dir / "bank_field_coverage.csv", specialized_coverage_query("rc_v2_fundamental_bank_quarterly"))
    query_to_csv(conn, artifact_dir / "insurance_field_coverage.csv", specialized_coverage_query("rc_v2_fundamental_insurance_quarterly"))
    query_to_csv(
        conn,
        artifact_dir / "provenance_audit.csv",
        """
        SELECT provider, source_dataset, COUNT(*) AS rows
        FROM rc_v2_fundamental_field_source
        WHERE provider IN ('SIMFIN_BANK_BULK','SIMFIN_INSURANCE_BULK')
        GROUP BY provider, source_dataset
        ORDER BY provider, source_dataset
        """,
    )
    (artifact_dir / "integrity_check.json").write_text(json.dumps(dict(integrity), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_policy_docs(artifact_dir)
    (artifact_dir / "summary.json").write_text(
        json.dumps({"dry_run": dry_run, "results": results, "profile_conflict_count": len(profile_conflicts), **integrity}, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def strip_conflicts(result: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in result.items() if key != "profile_conflicts"}


def specialized_coverage_query(table: str) -> str:
    excluded = {"quarter_id", "available_field_count", "created_at_utc", "updated_at_utc"}
    # The table list is fixed and not user-controlled.
    return " UNION ALL ".join(
        f"SELECT '{column}' AS field_name, SUM(CASE WHEN {column} IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count, COUNT(*) AS total_count FROM {table}"
        for column in table_columns(table)
        if column not in excluded
    )


def table_columns(table: str) -> list[str]:
    if table == "rc_v2_fundamental_bank_quarterly":
        return [item.fact_column for item in BANK_FIELD_POLICY]
    return [item.fact_column for item in INSURANCE_FIELD_POLICY]


def query_to_csv(conn: sqlite3.Connection, path: Path, query: str) -> None:
    rows = conn.execute(query).fetchall()
    columns = [description[0] for description in conn.execute(query).description]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_policy_docs(artifact_dir: Path) -> None:
    (artifact_dir / "bank_field_policy.md").write_text(
        """
# Bank Field Policy

Banks use `rc_v2_fundamental_bank_quarterly`. Ordinary EBIT, EBITDA, free cash flow, operating cash flow, total debt, net debt, and net debt/EBITDA are not derived or populated for BANK profiles. Direct bank balance debt fields are stored only as profile-specific fields.
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "insurance_field_policy.md").write_text(
        """
# Insurance Field Policy

Insurance companies use `rc_v2_fundamental_insurance_quarterly`. Ordinary EBIT, EBITDA, ordinary cash-flow metrics, total debt, net debt, and ordinary leverage ratios are not derived or populated for INSURANCE profiles.
""".strip()
        + "\n",
        encoding="utf-8",
    )


def backup_v2_db(db_path: Path, backup_dir: Path) -> dict[str, Any]:
    source = validate_v2_db_target(db_path)
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / f"{source.name}.{utc_now().replace('-', '').replace(':', '')}.pre_simfin_specialized.bak"
    shutil.copy2(source, backup)
    conn = sqlite3.connect(f"file:{backup.as_posix()}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA query_only=ON")
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        conn.close()
    return {"path": str(backup), "bytes": backup.stat().st_size, "integrity_check": integrity, "read_only_open": True}
