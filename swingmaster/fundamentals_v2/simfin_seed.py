from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


SIMFIN_PROVIDER = "SIMFIN"
SIMFIN_DERIVED_PROVIDER = "SIMFIN_DERIVED"
ORDINARY_PROFILE = "ORDINARY"
QUARTERLY_PERIODS = ("Q1", "Q2", "Q3", "Q4")
SOURCE_FILES = {
    "income": "us-income-quarterly.csv",
    "balance": "us-balance-quarterly.csv",
    "cashflow": "us-cashflow-quarterly.csv",
    "companies": "us-companies.csv",
}
LEGACY_DB_NAME = "fundamentals_usa.db"


@dataclass(frozen=True)
class SourceFileManifest:
    dataset: str
    path: Path
    filename: str
    sha256: str
    bytes: int
    row_count: int
    columns: tuple[str, ...]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_content_hash(db_path: Path) -> str:
    conn = sqlite3.connect(str(db_path))
    try:
        payload: dict[str, Any] = {}
        volatile_by_table = {
            "rc_v2_company": {"created_at_utc", "updated_at_utc"},
            "rc_v2_quarter": {"created_at_utc", "updated_at_utc"},
            "rc_v2_fundamental_quarterly": {"created_at_utc", "updated_at_utc"},
            "rc_v2_fundamental_field_source": {"created_at_utc", "import_run_id"},
        }
        for table in volatile_by_table:
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
            columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
            keep = [idx for idx, column in enumerate(columns) if column not in volatile_by_table[table]]
            payload[table] = [[row[idx] for idx in keep] for row in rows]
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    finally:
        conn.close()


def validate_output_path(output_db: Path, *, rebuild: bool) -> Path:
    output = output_db.expanduser().resolve()
    if output.name == LEGACY_DB_NAME:
        raise ValueError("RC_V2_REFUSES_LEGACY_DB_TARGET")
    if output.exists() and not rebuild:
        raise FileExistsError(f"RC_V2_OUTPUT_EXISTS_REBUILD_REQUIRED:{output}")
    return output


def read_source_manifest(simfin_dir: Path) -> list[SourceFileManifest]:
    root = simfin_dir.expanduser().resolve()
    manifests = []
    for dataset, filename in SOURCE_FILES.items():
        path = root / filename
        if not path.exists():
            raise FileNotFoundError(f"SIMFIN_SOURCE_FILE_MISSING:{path}")
        data = path.read_bytes()
        text = data.decode("utf-8-sig")
        first_line = text.splitlines()[0] if text.splitlines() else ""
        columns = tuple(first_line.split(";"))
        row_count = max(len(text.splitlines()) - 1, 0)
        manifests.append(
            SourceFileManifest(
                dataset=dataset,
                path=path,
                filename=filename,
                sha256=hashlib.sha256(data).hexdigest(),
                bytes=path.stat().st_size,
                row_count=row_count,
                columns=columns,
            )
        )
    return manifests


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;

        CREATE TABLE rc_v2_import_run (
            import_run_id TEXT PRIMARY KEY,
            market TEXT NOT NULL,
            simfin_dir TEXT NOT NULL,
            builder_version TEXT NOT NULL,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT
        );

        CREATE TABLE rc_v2_source_file (
            import_run_id TEXT NOT NULL,
            dataset TEXT NOT NULL,
            filename TEXT NOT NULL,
            path TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            row_count INTEGER NOT NULL,
            columns_json TEXT NOT NULL,
            imported_at_utc TEXT NOT NULL,
            PRIMARY KEY (import_run_id, dataset),
            FOREIGN KEY (import_run_id) REFERENCES rc_v2_import_run(import_run_id)
        );

        CREATE TABLE rc_v2_company (
            company_id INTEGER PRIMARY KEY,
            market TEXT NOT NULL,
            ticker TEXT,
            simfin_id INTEGER UNIQUE,
            company_name TEXT,
            company_profile TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE rc_v2_quarter (
            quarter_id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL,
            fiscal_year INTEGER NOT NULL,
            fiscal_period TEXT NOT NULL,
            report_date TEXT NOT NULL,
            publish_date TEXT,
            restated_date TEXT,
            quarter_identity_source TEXT NOT NULL,
            has_income INTEGER NOT NULL DEFAULT 0,
            has_balance INTEGER NOT NULL DEFAULT 0,
            has_cashflow INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            UNIQUE (company_id, fiscal_year, fiscal_period, report_date),
            FOREIGN KEY (company_id) REFERENCES rc_v2_company(company_id)
        );

        CREATE TABLE rc_v2_fundamental_quarterly (
            quarter_id INTEGER PRIMARY KEY,
            revenue REAL,
            gross_profit REAL,
            operating_income REAL,
            depreciation_amortization REAL,
            ebit REAL,
            ebitda REAL,
            net_income REAL,
            operating_cashflow REAL,
            capex REAL,
            free_cashflow REAL,
            cash REAL,
            total_debt REAL,
            shares_outstanding REAL,
            weighted_average_shares_basic REAL,
            weighted_average_shares_diluted REAL,
            available_canonical_field_count INTEGER NOT NULL,
            has_income INTEGER NOT NULL,
            has_balance INTEGER NOT NULL,
            has_cashflow INTEGER NOT NULL,
            seed_status TEXT NOT NULL,
            missing_seed_fields_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            FOREIGN KEY (quarter_id) REFERENCES rc_v2_quarter(quarter_id)
        );

        CREATE TABLE rc_v2_fundamental_field_source (
            quarter_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            provider TEXT NOT NULL,
            provider_field TEXT NOT NULL,
            source_dataset TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            transformation TEXT NOT NULL,
            source_value TEXT,
            import_run_id TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            PRIMARY KEY (quarter_id, field_name, provider),
            FOREIGN KEY (quarter_id) REFERENCES rc_v2_quarter(quarter_id),
            FOREIGN KEY (import_run_id) REFERENCES rc_v2_import_run(import_run_id)
        );

        CREATE TABLE rc_v2_validation_summary (
            metric TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return float(text)


def parse_int(value: str | None) -> int | None:
    number = parse_float(value)
    return None if number is None else int(number)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def source_by_dataset(manifests: Iterable[SourceFileManifest]) -> dict[str, SourceFileManifest]:
    return {manifest.dataset: manifest for manifest in manifests}


def build_rc_fundamentals_v2_from_simfin(
    *,
    simfin_dir: Path,
    output_db: Path,
    artifact_dir: Path,
    market: str = "usa",
    rebuild: bool = False,
    legacy_db: Path | None = None,
) -> dict[str, Any]:
    final_db = validate_output_path(output_db, rebuild=rebuild)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifests = read_source_manifest(simfin_dir)
    manifest_by_dataset = source_by_dataset(manifests)
    import_run_id = "SIMFIN_V2_SEED_" + utc_now().replace("-", "").replace(":", "")
    temp_db = final_db.with_suffix(final_db.suffix + f".{os.getpid()}.tmp")
    if temp_db.exists():
        temp_db.unlink()
    if final_db.exists() and rebuild:
        final_db.unlink()
    started = utc_now()
    conn = sqlite3.connect(str(temp_db))
    conn.row_factory = sqlite3.Row
    try:
        create_schema(conn)
        conn.execute(
            "INSERT INTO rc_v2_import_run VALUES (?, ?, ?, ?, ?, NULL)",
            (import_run_id, market, str(simfin_dir.expanduser().resolve()), "rc_fundamentals_v2_simfin_seed_v1", started),
        )
        for manifest in manifests:
            conn.execute(
                """
                INSERT INTO rc_v2_source_file VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    started,
                ),
            )
        companies = read_csv_rows(manifest_by_dataset["companies"].path)
        income_rows = read_csv_rows(manifest_by_dataset["income"].path)
        balance_rows = read_csv_rows(manifest_by_dataset["balance"].path)
        cashflow_rows = read_csv_rows(manifest_by_dataset["cashflow"].path)
        active_tickers = load_active_tickers_readonly(legacy_db) if legacy_db else set()
        company_rows = build_company_rows(companies, income_rows, balance_rows, cashflow_rows, active_tickers, market)
        insert_companies(conn, company_rows, started)
        company_id_by_simfin = {row["simfin_id"]: row["company_id"] for row in conn.execute("SELECT company_id, simfin_id FROM rc_v2_company")}
        source_rows = {
            "income": {statement_key(row): row for row in income_rows},
            "balance": {statement_key(row): row for row in balance_rows},
            "cashflow": {statement_key(row): row for row in cashflow_rows},
        }
        all_keys = sorted(set().union(*(set(rows) for rows in source_rows.values())))
        for key in all_keys:
            simfin_id, fiscal_year, fiscal_period, report_date = key
            if fiscal_period not in QUARTERLY_PERIODS:
                continue
            company_id = company_id_by_simfin.get(simfin_id)
            if company_id is None:
                continue
            group_rows = {name: rows.get(key) for name, rows in source_rows.items()}
            insert_quarter_and_fundamentals(
                conn,
                company_id=company_id,
                key=key,
                rows=group_rows,
                manifests=manifest_by_dataset,
                import_run_id=import_run_id,
                now=started,
            )
        conn.execute("UPDATE rc_v2_import_run SET finished_at_utc=? WHERE import_run_id=?", (utc_now(), import_run_id))
        write_validation_summary(conn)
        conn.commit()
        integrity = run_integrity_checks(conn)
    finally:
        conn.close()
    shutil.move(str(temp_db), str(final_db))
    content_hash = canonical_content_hash(final_db)
    artifacts = write_artifacts(
        db_path=final_db,
        artifact_dir=artifact_dir,
        manifests=manifests,
        summary_extra={
            "output_db": str(final_db),
            "content_hash": content_hash,
            "import_run_id": import_run_id,
            "legacy_db_readonly": bool(legacy_db),
            "yahoo_calls": 0,
            "sec_calls": 0,
            **integrity,
        },
    )
    return {"db_path": str(final_db), "content_hash": content_hash, "artifacts": artifacts, **integrity}


def statement_key(row: Mapping[str, str]) -> tuple[int, int, str, str]:
    return (
        int(str(row["SimFinId"])),
        int(str(row["Fiscal Year"])),
        str(row["Fiscal Period"]),
        str(row["Report Date"]),
    )


def load_active_tickers_readonly(legacy_db: Path | None) -> set[str]:
    if legacy_db is None or not legacy_db.exists():
        return set()
    conn = sqlite3.connect(f"file:{legacy_db.expanduser().resolve().as_posix()}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA query_only=ON")
        return {
            str(row[0]).upper()
            for row in conn.execute("SELECT DISTINCT ticker FROM rc_fundamental_quarterly WHERE ticker NOT LIKE '%.HE'")
        }
    finally:
        conn.close()


def build_company_rows(
    companies: list[dict[str, str]],
    income_rows: list[dict[str, str]],
    balance_rows: list[dict[str, str]],
    cashflow_rows: list[dict[str, str]],
    active_tickers: set[str],
    market: str,
) -> list[dict[str, Any]]:
    metadata = {int(row["SimFinId"]): row for row in companies if row.get("SimFinId")}
    tickers: dict[int, str] = {}
    for row in income_rows + balance_rows + cashflow_rows:
        simfin_id = int(row["SimFinId"])
        ticker = str(row.get("Ticker") or "").upper()
        if ticker:
            tickers.setdefault(simfin_id, ticker)
    output = []
    for simfin_id in sorted(tickers):
        meta = metadata.get(simfin_id, {})
        ticker = tickers.get(simfin_id) or str(meta.get("Ticker") or "").upper() or None
        output.append(
            {
                "market": market,
                "ticker": ticker,
                "simfin_id": simfin_id,
                "company_name": meta.get("Company Name") or None,
                "company_profile": ORDINARY_PROFILE,
                "active": int(bool(ticker and ticker.upper() in active_tickers)),
            }
        )
    return output


def insert_companies(conn: sqlite3.Connection, company_rows: list[dict[str, Any]], now: str) -> None:
    for row in company_rows:
        conn.execute(
            """
            INSERT INTO rc_v2_company (
                market, ticker, simfin_id, company_name, company_profile, active, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["market"],
                row["ticker"],
                row["simfin_id"],
                row["company_name"],
                row["company_profile"],
                row["active"],
                now,
                now,
            ),
        )


def first_non_null_date(rows: Iterable[dict[str, str] | None], column: str) -> str | None:
    values = sorted({str(row.get(column) or "") for row in rows if row and row.get(column)})
    return values[-1] if values else None


def insert_quarter_and_fundamentals(
    conn: sqlite3.Connection,
    *,
    company_id: int,
    key: tuple[int, int, str, str],
    rows: Mapping[str, dict[str, str] | None],
    manifests: Mapping[str, SourceFileManifest],
    import_run_id: str,
    now: str,
) -> None:
    _simfin_id, fiscal_year, fiscal_period, report_date = key
    has_income = int(rows["income"] is not None)
    has_balance = int(rows["balance"] is not None)
    has_cashflow = int(rows["cashflow"] is not None)
    publish_date = first_non_null_date(rows.values(), "Publish Date")
    restated_date = first_non_null_date(rows.values(), "Restated Date")
    cur = conn.execute(
        """
        INSERT INTO rc_v2_quarter (
            company_id, fiscal_year, fiscal_period, report_date, publish_date, restated_date,
            quarter_identity_source, has_income, has_balance, has_cashflow, created_at_utc, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, 'SIMFIN_FISCAL_PERIOD_REPORT_DATE', ?, ?, ?, ?, ?)
        """,
        (company_id, fiscal_year, fiscal_period, report_date, publish_date, restated_date, has_income, has_balance, has_cashflow, now, now),
    )
    quarter_id = int(cur.lastrowid)
    values = map_ordinary_fields(rows)
    missing = [field for field, value in values.items() if field not in {"ebit", "shares_outstanding"} and value is None]
    available_count = sum(1 for value in values.values() if value is not None)
    seed_status = "SEED_STRONG" if has_income and has_balance and has_cashflow and not missing else "SEED_PARTIAL" if available_count else "SEED_MINIMAL"
    conn.execute(
        """
        INSERT INTO rc_v2_fundamental_quarterly (
            quarter_id, revenue, gross_profit, operating_income, depreciation_amortization, ebit, ebitda,
            net_income, operating_cashflow, capex, free_cashflow, cash, total_debt, shares_outstanding,
            weighted_average_shares_basic, weighted_average_shares_diluted, available_canonical_field_count,
            has_income, has_balance, has_cashflow, seed_status, missing_seed_fields_json, created_at_utc, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            quarter_id,
            values["revenue"],
            values["gross_profit"],
            values["operating_income"],
            values["depreciation_amortization"],
            values["ebit"],
            values["ebitda"],
            values["net_income"],
            values["operating_cashflow"],
            values["capex"],
            values["free_cashflow"],
            values["cash"],
            values["total_debt"],
            values["shares_outstanding"],
            values["weighted_average_shares_basic"],
            values["weighted_average_shares_diluted"],
            available_count,
            has_income,
            has_balance,
            has_cashflow,
            seed_status,
            json.dumps(missing, sort_keys=True),
            now,
            now,
        ),
    )
    insert_provenance(conn, quarter_id, rows, values, manifests, import_run_id, now)


def map_ordinary_fields(rows: Mapping[str, dict[str, str] | None]) -> dict[str, float | None]:
    income = rows["income"] or {}
    balance = rows["balance"] or {}
    cashflow = rows["cashflow"] or {}
    operating_income = parse_float(income.get("Operating Income (Loss)"))
    depreciation_amortization = parse_float(cashflow.get("Depreciation & Amortization"))
    operating_cashflow = parse_float(cashflow.get("Net Cash from Operating Activities"))
    capex = parse_float(cashflow.get("Change in Fixed Assets & Intangibles"))
    short_debt = parse_float(balance.get("Short Term Debt"))
    long_debt = parse_float(balance.get("Long Term Debt"))
    total_debt = None if short_debt is None and long_debt is None else (short_debt or 0.0) + (long_debt or 0.0)
    return {
        "revenue": parse_float(income.get("Revenue")),
        "gross_profit": parse_float(income.get("Gross Profit")),
        "operating_income": operating_income,
        "depreciation_amortization": depreciation_amortization,
        "ebit": None,
        "ebitda": None if operating_income is None or depreciation_amortization is None else operating_income + depreciation_amortization,
        "net_income": parse_float(income.get("Net Income")),
        "operating_cashflow": operating_cashflow,
        "capex": capex,
        "free_cashflow": None if operating_cashflow is None or capex is None else operating_cashflow + capex,
        "cash": parse_float(balance.get("Cash, Cash Equivalents & Short Term Investments")),
        "total_debt": total_debt,
        "shares_outstanding": None,
        "weighted_average_shares_basic": parse_float(income.get("Shares (Basic)")) or parse_float(balance.get("Shares (Basic)")),
        "weighted_average_shares_diluted": parse_float(income.get("Shares (Diluted)")) or parse_float(balance.get("Shares (Diluted)")),
    }


FIELD_SOURCE_POLICY = {
    "revenue": ("income", "Revenue", SIMFIN_PROVIDER, "DIRECT"),
    "gross_profit": ("income", "Gross Profit", SIMFIN_PROVIDER, "DIRECT"),
    "operating_income": ("income", "Operating Income (Loss)", SIMFIN_PROVIDER, "DIRECT"),
    "depreciation_amortization": ("cashflow", "Depreciation & Amortization", SIMFIN_PROVIDER, "DIRECT"),
    "net_income": ("income", "Net Income", SIMFIN_PROVIDER, "DIRECT"),
    "operating_cashflow": ("cashflow", "Net Cash from Operating Activities", SIMFIN_PROVIDER, "DIRECT"),
    "capex": ("cashflow", "Change in Fixed Assets & Intangibles", SIMFIN_PROVIDER, "DIRECT"),
    "cash": ("balance", "Cash, Cash Equivalents & Short Term Investments", SIMFIN_PROVIDER, "DIRECT"),
    "weighted_average_shares_basic": ("income", "Shares (Basic)", SIMFIN_PROVIDER, "DIRECT"),
    "weighted_average_shares_diluted": ("income", "Shares (Diluted)", SIMFIN_PROVIDER, "DIRECT"),
}


def insert_provenance(
    conn: sqlite3.Connection,
    quarter_id: int,
    rows: Mapping[str, dict[str, str] | None],
    values: Mapping[str, float | None],
    manifests: Mapping[str, SourceFileManifest],
    import_run_id: str,
    now: str,
) -> None:
    for field, value in values.items():
        if value is None:
            continue
        if field == "ebitda":
            provider = SIMFIN_DERIVED_PROVIDER
            provider_field = "Operating Income (Loss)+Depreciation & Amortization"
            dataset = "income+cashflow"
            source_file = f"{manifests['income'].filename}+{manifests['cashflow'].filename}"
            sha = f"{manifests['income'].sha256}+{manifests['cashflow'].sha256}"
            transformation = "operating_income + depreciation_amortization"
            source_value = json.dumps({"operating_income": values["operating_income"], "depreciation_amortization": values["depreciation_amortization"]}, sort_keys=True)
        elif field == "free_cashflow":
            provider = SIMFIN_DERIVED_PROVIDER
            provider_field = "Net Cash from Operating Activities+Change in Fixed Assets & Intangibles"
            dataset = "cashflow"
            source_file = manifests["cashflow"].filename
            sha = manifests["cashflow"].sha256
            transformation = "operating_cashflow + capex"
            source_value = json.dumps({"operating_cashflow": values["operating_cashflow"], "capex": values["capex"]}, sort_keys=True)
        elif field == "total_debt":
            provider = SIMFIN_DERIVED_PROVIDER
            provider_field = "Short Term Debt+Long Term Debt"
            dataset = "balance"
            source_file = manifests["balance"].filename
            sha = manifests["balance"].sha256
            transformation = "short_term_debt + long_term_debt"
            balance = rows["balance"] or {}
            source_value = json.dumps({"short_term_debt": balance.get("Short Term Debt"), "long_term_debt": balance.get("Long Term Debt")}, sort_keys=True)
        else:
            dataset, provider_field, provider, transformation = FIELD_SOURCE_POLICY[field]
            source_file = manifests[dataset].filename
            sha = manifests[dataset].sha256
            source_value = None if rows[dataset] is None else rows[dataset].get(provider_field)
        conn.execute(
            """
            INSERT INTO rc_v2_fundamental_field_source VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                quarter_id,
                field,
                provider,
                provider_field,
                dataset,
                source_file,
                sha,
                transformation,
                str(source_value) if source_value is not None else None,
                import_run_id,
                now,
            ),
        )


def write_validation_summary(conn: sqlite3.Connection) -> None:
    metrics = {
        "company_count": conn.execute("SELECT COUNT(*) FROM rc_v2_company").fetchone()[0],
        "ordinary_company_count": conn.execute("SELECT COUNT(*) FROM rc_v2_company WHERE company_profile='ORDINARY'").fetchone()[0],
        "quarter_count": conn.execute("SELECT COUNT(*) FROM rc_v2_quarter").fetchone()[0],
        "all_three_statement_quarters": conn.execute("SELECT COUNT(*) FROM rc_v2_quarter WHERE has_income=1 AND has_balance=1 AND has_cashflow=1").fetchone()[0],
        "partial_statement_quarters": conn.execute("SELECT COUNT(*) FROM rc_v2_quarter WHERE NOT (has_income=1 AND has_balance=1 AND has_cashflow=1)").fetchone()[0],
        "ebit_non_null": conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_quarterly WHERE ebit IS NOT NULL").fetchone()[0],
        "ebitda_non_null": conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_quarterly WHERE ebitda IS NOT NULL").fetchone()[0],
        "active_company_count": conn.execute("SELECT COUNT(*) FROM rc_v2_company WHERE active=1").fetchone()[0],
    }
    for metric, value in metrics.items():
        conn.execute("INSERT OR REPLACE INTO rc_v2_validation_summary VALUES (?, ?)", (metric, str(value)))


def run_integrity_checks(conn: sqlite3.Connection) -> dict[str, Any]:
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    duplicate_quarters = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT company_id, fiscal_year, fiscal_period, report_date, COUNT(*) c
            FROM rc_v2_quarter
            GROUP BY company_id, fiscal_year, fiscal_period, report_date
            HAVING c > 1
        )
        """
    ).fetchone()[0]
    orphan_fundamentals = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_v2_fundamental_quarterly f
        LEFT JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id
        WHERE q.quarter_id IS NULL
        """
    ).fetchone()[0]
    ok = integrity == "ok" and not fk_rows and duplicate_quarters == 0 and orphan_fundamentals == 0
    if not ok:
        raise RuntimeError(
            f"RC_V2_INTEGRITY_FAILED integrity={integrity} fk={len(fk_rows)} duplicate_quarters={duplicate_quarters} orphan_fundamentals={orphan_fundamentals}"
        )
    return {
        "integrity_check": integrity,
        "foreign_key_errors": len(fk_rows),
        "duplicate_canonical_quarters": duplicate_quarters,
        "orphan_fundamentals": orphan_fundamentals,
    }


def write_artifacts(
    *,
    db_path: Path,
    artifact_dir: Path,
    manifests: list[SourceFileManifest],
    summary_extra: Mapping[str, Any],
) -> dict[str, str]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
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
        query_to_csv(conn, artifact_dir / "company_mapping.csv", "SELECT * FROM rc_v2_company ORDER BY simfin_id")
        query_to_csv(conn, artifact_dir / "company_profile_summary.csv", "SELECT company_profile, COUNT(*) AS company_count, SUM(active) AS active_count FROM rc_v2_company GROUP BY company_profile ORDER BY company_profile")
        query_to_csv(conn, artifact_dir / "quarter_build_summary.csv", "SELECT has_income, has_balance, has_cashflow, COUNT(*) AS quarter_count FROM rc_v2_quarter GROUP BY has_income, has_balance, has_cashflow ORDER BY has_income DESC, has_balance DESC, has_cashflow DESC")
        query_to_csv(conn, artifact_dir / "statement_join_validation.csv", "SELECT has_income, has_balance, has_cashflow, COUNT(*) AS quarter_count FROM rc_v2_quarter GROUP BY has_income, has_balance, has_cashflow")
        query_to_csv(conn, artifact_dir / "field_coverage.csv", field_coverage_query())
        query_to_csv(conn, artifact_dir / "missing_field_patterns.csv", "SELECT missing_seed_fields_json, COUNT(*) AS quarter_count FROM rc_v2_fundamental_quarterly GROUP BY missing_seed_fields_json ORDER BY quarter_count DESC")
        query_to_csv(conn, artifact_dir / "ebitda_formula_validation.csv", ebitda_validation_query())
        query_to_csv(conn, artifact_dir / "debt_semantics_validation.csv", "SELECT COUNT(*) AS rows_with_debt, SUM(CASE WHEN total_debt=0 THEN 1 ELSE 0 END) AS zero_debt_rows, SUM(CASE WHEN total_debt<0 THEN 1 ELSE 0 END) AS negative_debt_rows FROM rc_v2_fundamental_quarterly WHERE total_debt IS NOT NULL")
        query_to_csv(conn, artifact_dir / "representative_tickers.csv", representative_query())
        write_static_docs(artifact_dir)
        summary = {**summary_extra, **{row["metric"]: row["value"] for row in conn.execute("SELECT metric, value FROM rc_v2_validation_summary")}}
        (artifact_dir / "integrity_check.json").write_text(
            json.dumps({key: summary_extra[key] for key in ("integrity_check", "foreign_key_errors", "duplicate_canonical_quarters", "orphan_fundamentals")}, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        (artifact_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        write_unmatched_active_tickers(conn, artifact_dir / "unmatched_active_tickers.csv")
        write_legacy_comparison(conn, artifact_dir / "legacy_comparison.csv")
        (artifact_dir / "bank_insurance_gap_analysis.csv").write_text(bank_insurance_gap_csv(), encoding="utf-8")
        return {path.name: str(path) for path in artifact_dir.iterdir() if path.is_file() and path.suffix != ".db"}
    finally:
        conn.close()


def query_to_csv(conn: sqlite3.Connection, path: Path, query: str) -> None:
    rows = conn.execute(query).fetchall()
    columns = [description[0] for description in conn.execute(query).description]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def field_coverage_query() -> str:
    fields = [
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
    ]
    parts = [
        f"SELECT '{field}' AS field_name, SUM(CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count, COUNT(*) AS total_count FROM rc_v2_fundamental_quarterly"
        for field in fields
    ]
    return " UNION ALL ".join(parts)


def ebitda_validation_query() -> str:
    return """
    SELECT
        c.ticker,
        q.fiscal_year,
        q.fiscal_period,
        q.report_date,
        f.operating_income,
        f.depreciation_amortization,
        f.ebitda,
        f.ebit
    FROM rc_v2_fundamental_quarterly f
    JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id
    JOIN rc_v2_company c ON c.company_id=q.company_id
    WHERE c.ticker IN ('AAPL','NVDA','WMT','COST','KMX','MSFT','XOM','CAT','ADBE','HD')
    ORDER BY c.ticker, q.fiscal_year, q.fiscal_period
    """


def representative_query() -> str:
    return """
    SELECT
        c.ticker,
        q.fiscal_year,
        q.fiscal_period,
        q.report_date,
        q.publish_date,
        f.operating_income,
        f.depreciation_amortization,
        f.ebitda,
        f.operating_cashflow,
        f.capex,
        f.free_cashflow,
        f.cash,
        f.total_debt
    FROM rc_v2_fundamental_quarterly f
    JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id
    JOIN rc_v2_company c ON c.company_id=q.company_id
    WHERE c.ticker IN ('AAPL','NVDA','WMT','COST','KMX','MSFT','XOM','CAT','ADBE','HD')
    ORDER BY c.ticker, q.fiscal_year, q.fiscal_period
    """


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


def write_static_docs(artifact_dir: Path) -> None:
    (artifact_dir / "future_yahoo_enrichment_contract.md").write_text(
        """
# Future Yahoo Enrichment Contract

One future Yahoo fetch per ticker should persist all returned quarterly periods. Yahoo direct EBIT may fill canonical `ebit`. Yahoo direct EBITDA must be stored as a comparison/enrichment value unless a later policy explicitly overrides the SimFin ordinary-company canonical EBITDA formula.
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "future_sec_enrichment_contract.md").write_text(
        """
# Future SEC Enrichment Contract

SEC may fill SimFin gaps, validate SimFin fields, and cover uncovered 2020Q4-present companies. SEC `OperatingIncomeLoss` may fill `operating_income` and must not fill `ebit`.
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "bank_insurance_next_step.md").write_text(
        """
# Bank And Insurance Next Step

Acquire dedicated SimFin quarterly bank and insurance datasets before importing financial-sector fundamentals. Expected loaders/datasets include income, balance, cashflow, and derived variants for banks and insurance. Do not apply ordinary EBITDA, OCF, or debt policy to those profiles.
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (artifact_dir / "schema.md").write_text(
        """
# rc_fundamentals_v2 Schema

Core tables: `rc_v2_company`, `rc_v2_quarter`, `rc_v2_fundamental_quarterly`, `rc_v2_fundamental_field_source`, `rc_v2_source_file`, `rc_v2_import_run`, `rc_v2_validation_summary`.

Ordinary-company EBITDA is `Operating Income (Loss) + Depreciation & Amortization`. EBIT remains NULL for SimFin seed.
""".strip()
        + "\n",
        encoding="utf-8",
    )


def bank_insurance_gap_csv() -> str:
    rows = [
        ["profile", "local_files_present", "status", "required_next_datasets"],
        ["BANK", "0", "PENDING_DEDICATED_IMPORT", "quarterly bank income|quarterly bank balance|quarterly bank cashflow|bank derived"],
        ["INSURANCE", "0", "PENDING_DEDICATED_IMPORT", "quarterly insurance income|quarterly insurance balance|quarterly insurance cashflow|insurance derived"],
    ]
    return "\n".join(",".join(row) for row in rows) + "\n"


def write_unmatched_active_tickers(conn: sqlite3.Connection, path: Path) -> None:
    legacy_path = Path("/home/kalle/projects/swingmaster/fundamentals_usa.db")
    if not legacy_path.exists():
        path.write_text("ticker,note\n", encoding="utf-8")
        return
    legacy = sqlite3.connect(f"file:{legacy_path.as_posix()}?mode=ro", uri=True)
    try:
        legacy.execute("PRAGMA query_only=ON")
        active = {
            str(row[0]).upper()
            for row in legacy.execute("SELECT DISTINCT ticker FROM rc_fundamental_quarterly WHERE ticker NOT LIKE '%.HE'")
        }
    finally:
        legacy.close()
    covered = {str(row[0]).upper() for row in conn.execute("SELECT ticker FROM rc_v2_company WHERE ticker IS NOT NULL")}
    rows = [{"ticker": ticker, "note": "not_covered_by_local_ordinary_simfin_files"} for ticker in sorted(active - covered)]
    write_csv(path, rows)


def write_legacy_comparison(conn: sqlite3.Connection, path: Path) -> None:
    legacy_path = Path("/home/kalle/projects/swingmaster/fundamentals_usa.db")
    if not legacy_path.exists():
        path.write_text("field,bucket,count\n", encoding="utf-8")
        return
    legacy = sqlite3.connect(f"file:{legacy_path.as_posix()}?mode=ro", uri=True)
    legacy.row_factory = sqlite3.Row
    try:
        legacy.execute("PRAGMA query_only=ON")
        legacy_rows = {
            (str(row["ticker"]).upper(), str(row["period_end_date"])): row
            for row in legacy.execute(
                """
                SELECT ticker, period_end_date, revenue, gross_profit, operating_income,
                       net_income, operating_cashflow, capex, free_cashflow, cash, total_debt
                FROM rc_fundamental_quarterly
                WHERE ticker NOT LIKE '%.HE'
                """
            )
        }
    finally:
        legacy.close()
    v2_rows = {
        (str(row["ticker"]).upper(), str(row["report_date"])): row
        for row in conn.execute(
            """
            SELECT c.ticker, q.report_date, f.revenue, f.gross_profit, f.operating_income,
                   f.net_income, f.operating_cashflow, f.capex, f.free_cashflow, f.cash, f.total_debt
            FROM rc_v2_fundamental_quarterly f
            JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id
            JOIN rc_v2_company c ON c.company_id=q.company_id
            WHERE c.ticker IS NOT NULL
            """
        )
    }
    fields = (
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "operating_cashflow",
        "capex",
        "free_cashflow",
        "cash",
        "total_debt",
    )
    counters: dict[str, dict[str, int]] = {field: {} for field in fields}
    for key in sorted(set(v2_rows) & set(legacy_rows)):
        v2 = v2_rows[key]
        legacy_row = legacy_rows[key]
        for field in fields:
            bucket = value_diff_bucket(v2[field], legacy_row[field])
            counters[field][bucket] = counters[field].get(bucket, 0) + 1
    rows = []
    for field, counter in counters.items():
        for bucket in ("exact", "<1%", "1-5%", "5-10%", ">10%", "missing_v2", "missing_legacy", "both_missing"):
            rows.append({"field": field, "bucket": bucket, "count": counter.get(bucket, 0)})
    write_csv(path, rows)


def value_diff_bucket(left: Any, right: Any) -> str:
    if left is None and right is None:
        return "both_missing"
    if left is None:
        return "missing_v2"
    if right is None:
        return "missing_legacy"
    left_f = float(left)
    right_f = float(right)
    if left_f == right_f:
        return "exact"
    denominator = max(abs(left_f), abs(right_f), 1.0)
    diff = abs(left_f - right_f) / denominator
    if diff < 0.01:
        return "<1%"
    if diff < 0.05:
        return "1-5%"
    if diff < 0.10:
        return "5-10%"
    return ">10%"
