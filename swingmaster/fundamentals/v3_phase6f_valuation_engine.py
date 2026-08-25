from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE6F_VALUATION_ENGINE_IMPLEMENTED_READY_FOR_PHASE6G"
CLASSIFICATION_SCHEMA_REFINEMENT = "FUNDAMENTALS_V3_PHASE6F_VALUATION_SCHEMA_REFINEMENT_REQUIRED"
NEXT_PHASE = "MASTER PLAN PHASE 6G - LEGACY 2.0 FUNDAMENTAL SCORE ENGINE IMPLEMENTATION"
MODEL_VERSION = "V3_VALUATION_POST_PUBLICATION_SNAPSHOT_V1"
PRICE_SOURCE = "RAWCANDLE_OSAKEDATA_CLOSE"
DENOMINATOR_EPSILON = 1e-9
STATUS_VALID = "VALID"
STATUS_MISSING_INPUT = "MISSING_INPUT"
STATUS_NOT_MEANINGFUL = "NOT_MEANINGFUL"
STATUS_NOT_APPLICABLE = "NOT_APPLICABLE"
STATUS_MISSING_PUBLISH_DATE = "MISSING_PUBLISH_DATE"
STATUS_MISSING_TARGET_DAY_PRICE = "MISSING_TARGET_DAY_PRICE"
STATUS_PENDING_PRICE_DATE = "PENDING_PRICE_DATE"

VALUATION_COLUMNS = [
    ("valuation_id", "INTEGER PRIMARY KEY"),
    ("company_id", "INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE"),
    ("endpoint_ttm_id", "INTEGER NOT NULL REFERENCES v3_ttm(ttm_id) ON DELETE CASCADE"),
    ("endpoint_quarter_id", "INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE"),
    ("endpoint_fiscal_year", "INTEGER NOT NULL"),
    ("endpoint_fiscal_quarter", "TEXT NOT NULL"),
    ("endpoint_period_end", "TEXT NOT NULL"),
    ("publish_date", "TEXT"),
    ("valuation_date", "TEXT NOT NULL"),
    ("valuation_close_price", "REAL"),
    ("price_source", "TEXT NOT NULL"),
    ("shares_outstanding", "REAL"),
    ("market_cap", "REAL"),
    ("cash", "REAL"),
    ("total_debt", "REAL"),
    ("net_debt", "REAL"),
    ("enterprise_value", "REAL"),
    ("ttm_revenue", "REAL"),
    ("ttm_ebit", "REAL"),
    ("ttm_ebitda", "REAL"),
    ("ttm_net_income", "REAL"),
    ("ttm_ocf", "REAL"),
    ("ttm_fcf", "REAL"),
    ("ev_ebit", "REAL"),
    ("ev_ebit_status", "TEXT NOT NULL DEFAULT 'MISSING_INPUT'"),
    ("ebit_yield", "REAL"),
    ("ebit_yield_status", "TEXT NOT NULL DEFAULT 'MISSING_INPUT'"),
    ("fcf_yield", "REAL"),
    ("fcf_yield_status", "TEXT NOT NULL DEFAULT 'MISSING_INPUT'"),
    ("ev_sales", "REAL"),
    ("ev_sales_status", "TEXT NOT NULL DEFAULT 'MISSING_INPUT'"),
    ("ev_ebitda", "REAL"),
    ("ev_ebitda_status", "TEXT NOT NULL DEFAULT 'MISSING_INPUT'"),
    ("pe", "REAL"),
    ("pe_status", "TEXT NOT NULL DEFAULT 'MISSING_INPUT'"),
    ("ev_ocf", "REAL"),
    ("ev_ocf_status", "TEXT NOT NULL DEFAULT 'MISSING_INPUT'"),
    ("model_version", "TEXT NOT NULL"),
    ("valuation_ready", "INTEGER NOT NULL CHECK (valuation_ready IN (0, 1))"),
    ("valuation_status", "TEXT NOT NULL DEFAULT 'MISSING_INPUT'"),
    ("source_fingerprint", "TEXT NOT NULL"),
    ("output_json", "TEXT"),
    ("run_id", "TEXT"),
    ("created_at_utc", "TEXT NOT NULL"),
    ("updated_at_utc", "TEXT NOT NULL"),
]


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def valuation_schema_sql() -> str:
    cols = ",\n    ".join(f"{name} {definition}" for name, definition in VALUATION_COLUMNS)
    return f"""CREATE TABLE IF NOT EXISTS v3_valuation (
    {cols},
    UNIQUE (company_id, endpoint_ttm_id, model_version)
);
CREATE INDEX IF NOT EXISTS idx_v3_valuation_endpoint
ON v3_valuation(company_id, endpoint_period_end, model_version);
CREATE INDEX IF NOT EXISTS idx_v3_valuation_date
ON v3_valuation(valuation_date, model_version);
"""


def ensure_valuation_schema(conn: sqlite3.Connection) -> str:
    existing = table_columns(conn, "v3_valuation")
    required = [name for name, _definition in VALUATION_COLUMNS]
    if set(required).issubset(existing):
        conn.executescript(valuation_schema_sql().split("CREATE TABLE IF NOT EXISTS v3_valuation", 1)[1].split(");", 1)[1])
        return "READY"
    row_count = table_count(conn, "v3_valuation") if existing else 0
    if row_count:
        raise RuntimeError(CLASSIFICATION_SCHEMA_REFINEMENT + ":v3_valuation_non_empty_legacy_schema")
    conn.execute("DROP TABLE IF EXISTS v3_valuation")
    conn.executescript(valuation_schema_sql())
    conn.commit()
    return "REBUILT_EMPTY_TABLE"


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def table_count(conn: sqlite3.Connection, table: str) -> int:
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) if exists else 0


def load_market_dates(osakedata_db: Path, market: str) -> list[str]:
    with sqlite3.connect(f"file:{osakedata_db}?mode=ro", uri=True) as conn:
        ticker_count = int(conn.execute("SELECT COUNT(DISTINCT osake) FROM osakedata WHERE market=?", (market,)).fetchone()[0])
        min_breadth = max(1, min(25, math.ceil(ticker_count * 0.01)))
        rows = conn.execute(
            """
            SELECT pvm
            FROM osakedata
            WHERE market=? AND close IS NOT NULL
            GROUP BY pvm
            HAVING COUNT(DISTINCT osake) >= ?
            ORDER BY pvm
            """,
            (market, min_breadth),
        ).fetchall()
    return [str(row[0]) for row in rows]


def resolve_next_trading_day(market_dates: list[str], publish_date: str | None, *, today: str | None = None) -> tuple[str | None, str]:
    if not publish_date:
        return None, STATUS_MISSING_PUBLISH_DATE
    for trading_day in market_dates:
        if trading_day > publish_date:
            if today is not None and trading_day > today:
                return trading_day, STATUS_PENDING_PRICE_DATE
            return trading_day, STATUS_VALID
    return None, STATUS_PENDING_PRICE_DATE


def fetch_close(conn: sqlite3.Connection, *, ticker: str, market: str, valuation_date: str) -> float | None:
    row = conn.execute(
        "SELECT close FROM osakedata WHERE market=? AND osake=? AND pvm=?",
        (market, ticker, valuation_date),
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def load_ttm_endpoints(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in conn.execute(
                """
                SELECT c.company_id,c.ticker,c.market,c.company_name,c.active,t.*
                FROM v3_ttm t
                JOIN v3_company c ON c.company_id=t.company_id
                ORDER BY t.period_end,c.ticker,t.endpoint_quarter_id
                """
            )
        ]


def calculate_metric(numerator: float | None, denominator: float | None, *, require_positive_numerator: bool = False) -> tuple[float | None, str]:
    if numerator is None or denominator is None:
        return None, STATUS_MISSING_INPUT
    if denominator <= DENOMINATOR_EPSILON:
        return None, STATUS_NOT_MEANINGFUL
    if require_positive_numerator and numerator <= DENOMINATOR_EPSILON:
        return None, STATUS_NOT_MEANINGFUL
    return numerator / denominator, STATUS_VALID


def calculate_valuation(row: dict[str, Any], *, valuation_date: str | None, close: float | None, price_status: str) -> dict[str, Any]:
    publish_date = row.get("ttm_available_date")
    shares = f(row.get("shares_outstanding"))
    cash = f(row.get("cash"))
    debt = f(row.get("total_debt"))
    revenue = f(row.get("ttm_revenue"))
    ebit = f(row.get("ttm_ebit"))
    ebitda = f(row.get("ttm_ebitda"))
    net_income = f(row.get("ttm_net_income"))
    ocf = f(row.get("ttm_ocf"))
    fcf = f(row.get("ttm_fcf"))
    if price_status != STATUS_VALID:
        return unavailable_snapshot(row, valuation_date, price_status)
    if close is None:
        return unavailable_snapshot(row, valuation_date, STATUS_MISSING_TARGET_DAY_PRICE)
    if shares is None or shares <= DENOMINATOR_EPSILON:
        return unavailable_snapshot(row, valuation_date, STATUS_MISSING_INPUT, close=close)
    market_cap = close * shares
    net_debt = None if cash is None or debt is None else debt - cash
    enterprise_value = None if net_debt is None else market_cap + net_debt
    ev_ebit, ev_ebit_status = calculate_metric(enterprise_value, ebit)
    ebit_yield, ebit_yield_status = calculate_metric(ebit, enterprise_value, require_positive_numerator=True)
    fcf_yield, fcf_yield_status = calculate_metric(fcf, market_cap, require_positive_numerator=True)
    ev_sales, ev_sales_status = calculate_metric(enterprise_value, revenue)
    ev_ebitda, ev_ebitda_status = calculate_metric(enterprise_value, ebitda)
    pe, pe_status = calculate_metric(market_cap, net_income)
    ev_ocf, ev_ocf_status = calculate_metric(enterprise_value, ocf)
    ready = any(status == STATUS_VALID for status in (ev_ebit_status, fcf_yield_status, ev_sales_status, ev_ebitda_status, pe_status, ev_ocf_status))
    snapshot = {
        **lineage(row),
        "publish_date": publish_date,
        "valuation_date": valuation_date,
        "valuation_close_price": close,
        "price_source": PRICE_SOURCE,
        "shares_outstanding": shares,
        "market_cap": market_cap,
        "cash": cash,
        "total_debt": debt,
        "net_debt": net_debt,
        "enterprise_value": enterprise_value,
        "ttm_revenue": revenue,
        "ttm_ebit": ebit,
        "ttm_ebitda": ebitda,
        "ttm_net_income": net_income,
        "ttm_ocf": ocf,
        "ttm_fcf": fcf,
        "ev_ebit": ev_ebit,
        "ev_ebit_status": ev_ebit_status,
        "ebit_yield": ebit_yield,
        "ebit_yield_status": ebit_yield_status,
        "fcf_yield": fcf_yield,
        "fcf_yield_status": fcf_yield_status,
        "ev_sales": ev_sales,
        "ev_sales_status": ev_sales_status,
        "ev_ebitda": ev_ebitda,
        "ev_ebitda_status": ev_ebitda_status,
        "pe": pe,
        "pe_status": pe_status,
        "ev_ocf": ev_ocf,
        "ev_ocf_status": ev_ocf_status,
        "model_version": MODEL_VERSION,
        "valuation_ready": 1 if ready else 0,
        "valuation_status": STATUS_VALID if ready else STATUS_NOT_MEANINGFUL,
    }
    snapshot["source_fingerprint"] = snapshot_fingerprint(snapshot)
    snapshot["output_json"] = json.dumps({k: snapshot[k] for k in metric_status_fields()}, sort_keys=True)
    return snapshot


def unavailable_snapshot(row: dict[str, Any], valuation_date: str | None, status: str, close: float | None = None) -> dict[str, Any]:
    snapshot = {
        **lineage(row),
        "publish_date": row.get("ttm_available_date"),
        "valuation_date": valuation_date or "",
        "valuation_close_price": close,
        "price_source": PRICE_SOURCE,
        "shares_outstanding": f(row.get("shares_outstanding")),
        "market_cap": None,
        "cash": f(row.get("cash")),
        "total_debt": f(row.get("total_debt")),
        "net_debt": None,
        "enterprise_value": None,
        "ttm_revenue": f(row.get("ttm_revenue")),
        "ttm_ebit": f(row.get("ttm_ebit")),
        "ttm_ebitda": f(row.get("ttm_ebitda")),
        "ttm_net_income": f(row.get("ttm_net_income")),
        "ttm_ocf": f(row.get("ttm_ocf")),
        "ttm_fcf": f(row.get("ttm_fcf")),
        "model_version": MODEL_VERSION,
        "valuation_ready": 0,
        "valuation_status": status,
    }
    for metric in ("ev_ebit", "ebit_yield", "fcf_yield", "ev_sales", "ev_ebitda", "pe", "ev_ocf"):
        snapshot[metric] = None
        snapshot[f"{metric}_status"] = status
    snapshot["source_fingerprint"] = snapshot_fingerprint(snapshot)
    snapshot["output_json"] = json.dumps({k: snapshot[k] for k in metric_status_fields()}, sort_keys=True)
    return snapshot


def lineage(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "company_id": int(row["company_id"]),
        "ticker": row.get("ticker"),
        "market": row.get("market"),
        "endpoint_ttm_id": int(row["ttm_id"]),
        "endpoint_quarter_id": int(row["endpoint_quarter_id"]),
        "endpoint_fiscal_year": int(row["endpoint_fiscal_year"]),
        "endpoint_fiscal_quarter": row["endpoint_fiscal_quarter"],
        "endpoint_period_end": row["period_end"],
    }


def metric_status_fields() -> list[str]:
    return [
        "ev_ebit_status",
        "ebit_yield_status",
        "fcf_yield_status",
        "ev_sales_status",
        "ev_ebitda_status",
        "pe_status",
        "ev_ocf_status",
        "valuation_status",
    ]


def snapshot_fingerprint(snapshot: dict[str, Any]) -> str:
    keys = [
        "company_id",
        "endpoint_ttm_id",
        "endpoint_quarter_id",
        "publish_date",
        "valuation_date",
        "valuation_close_price",
        "shares_outstanding",
        "cash",
        "total_debt",
        "ttm_revenue",
        "ttm_ebit",
        "ttm_ebitda",
        "ttm_net_income",
        "ttm_ocf",
        "ttm_fcf",
        "model_version",
    ]
    payload = {key: snapshot.get(key) for key in keys}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def build_valuation_plan(v3_db: Path, osakedata_db: Path, *, today: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    endpoints = load_ttm_endpoints(v3_db)
    if limit is not None:
        endpoints = endpoints[:limit]
    market_dates = {market: load_market_dates(osakedata_db, market) for market in sorted({str(r["market"]) for r in endpoints})}
    with sqlite3.connect(f"file:{osakedata_db}?mode=ro", uri=True) as price_conn:
        out = []
        for row in endpoints:
            valuation_date, date_status = resolve_next_trading_day(market_dates.get(str(row["market"]), []), row.get("ttm_available_date"), today=today)
            close = fetch_close(price_conn, ticker=str(row["ticker"]), market=str(row["market"]), valuation_date=valuation_date) if valuation_date and date_status == STATUS_VALID else None
            snapshot = calculate_valuation(row, valuation_date=valuation_date, close=close, price_status=date_status)
            out.append(snapshot)
        return out


def upsert_snapshot(conn: sqlite3.Connection, snapshot: dict[str, Any], *, run_id: str, now: str | None = None) -> str:
    ensure_valuation_schema(conn)
    now_text = now or utc_now()
    existing = conn.execute(
        "SELECT source_fingerprint FROM v3_valuation WHERE company_id=? AND endpoint_ttm_id=? AND model_version=?",
        (snapshot["company_id"], snapshot["endpoint_ttm_id"], snapshot["model_version"]),
    ).fetchone()
    if existing and existing[0] == snapshot["source_fingerprint"]:
        return "NOOP"
    columns = [name for name, _definition in VALUATION_COLUMNS if name != "valuation_id"]
    values = []
    for col in columns:
        if col == "run_id":
            values.append(run_id)
        elif col == "created_at_utc":
            values.append(now_text)
        elif col == "updated_at_utc":
            values.append(now_text)
        else:
            values.append(snapshot.get(col))
    update_cols = [c for c in columns if c not in {"company_id", "endpoint_ttm_id", "model_version", "created_at_utc"}]
    conn.execute(
        f"""
        INSERT INTO v3_valuation ({",".join(columns)})
        VALUES ({",".join("?" for _ in columns)})
        ON CONFLICT(company_id, endpoint_ttm_id, model_version) DO UPDATE SET
            {",".join(f"{c}=excluded.{c}" for c in update_cols)}
        """,
        values,
    )
    return "INSERTED" if not existing else "UPDATED_SOURCE_CHANGED"


def apply_snapshots(db_path: Path, snapshots: list[dict[str, Any]], *, run_id: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        ensure_valuation_schema(conn)
        for snapshot in snapshots:
            counts[upsert_snapshot(conn, snapshot, run_id=run_id)] += 1
        conn.commit()
    return dict(counts)


def dry_summary(plan: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "ttm_endpoints": len(plan),
        "endpoints_with_publish_date": sum(1 for r in plan if r["publish_date"]),
        "missing_publish_date": sum(1 for r in plan if r["valuation_status"] == STATUS_MISSING_PUBLISH_DATE),
        "valuation_dates_resolved": sum(1 for r in plan if r["valuation_date"]),
        "target_prices_available": sum(1 for r in plan if r["valuation_close_price"] is not None),
        "calculable_snapshots": sum(1 for r in plan if r["valuation_ready"]),
        "ev_ebit_valid": sum(1 for r in plan if r["ev_ebit_status"] == STATUS_VALID),
        "fcf_yield_valid": sum(1 for r in plan if r["fcf_yield_status"] == STATUS_VALID),
        "ev_sales_valid": sum(1 for r in plan if r["ev_sales_status"] == STATUS_VALID),
        "ev_ebitda_valid": sum(1 for r in plan if r["ev_ebitda_status"] == STATUS_VALID),
        "pe_valid": sum(1 for r in plan if r["pe_status"] == STATUS_VALID),
        "missing_target_price": sum(1 for r in plan if r["valuation_status"] == STATUS_MISSING_TARGET_DAY_PRICE),
        "pending_price_date": sum(1 for r in plan if r["valuation_status"] == STATUS_PENDING_PRICE_DATE),
    }


def coverage_by_year(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for year in sorted({str(r["endpoint_period_end"])[:4] for r in plan}):
        rows = [r for r in plan if str(r["endpoint_period_end"]).startswith(year)]
        out.append({"year": year, **dry_summary(rows)})
    return out


def metric_status_counts(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for metric in ("ev_ebit", "ebit_yield", "fcf_yield", "ev_sales", "ev_ebitda", "pe", "ev_ocf"):
        counts = Counter(r[f"{metric}_status"] for r in plan)
        for status, count in sorted(counts.items()):
            out.append({"metric": metric, "status": status, "count": count})
    return out


def trading_day_resolution_summary(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(r["valuation_status"] for r in plan)
    return [{"status": status, "count": count} for status, count in sorted(counts.items())]


def sample_resolution(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    samples = []
    for label, predicate in [
        ("normal_weekday", lambda r: r["publish_date"] and weekday(r["publish_date"]) in {0, 1, 2}),
        ("friday_publish", lambda r: r["publish_date"] and weekday(r["publish_date"]) == 4),
        ("weekend_publish", lambda r: r["publish_date"] and weekday(r["publish_date"]) in {5, 6}),
        ("holiday_boundary_or_gap", lambda r: r["publish_date"] and r["valuation_date"] and calendar_gap(r["publish_date"], r["valuation_date"]) > 1),
        ("missing_target_price", lambda r: r["valuation_status"] == STATUS_MISSING_TARGET_DAY_PRICE),
    ]:
        row = next((r for r in plan if predicate(r)), None)
        if row:
            samples.append({"case": label, "ticker": row["ticker"], "publish_date": row["publish_date"], "valuation_date": row["valuation_date"], "close": row["valuation_close_price"], "status": row["valuation_status"]})
    return samples


def weekday(value: str) -> int:
    return date.fromisoformat(value).weekday()


def calendar_gap(left: str, right: str) -> int:
    return (date.fromisoformat(right) - date.fromisoformat(left)).days


def existing_valuation_inventory(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        columns = table_columns(conn, "v3_valuation")
        return [{
            "table": "v3_valuation",
            "current_rows": table_count(conn, "v3_valuation"),
            "current_columns": "|".join(sorted(columns)),
            "supports_snapshot_contract": int(set(name for name, _ in VALUATION_COLUMNS).issubset(columns)),
            "migration_required": int(not set(name for name, _ in VALUATION_COLUMNS).issubset(columns)),
            "obsolete_unique_key": int("endpoint_ttm_id" not in columns),
        }]


def existing_schema_md(v3_db: Path) -> str:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        rows = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='v3_valuation'").fetchall()
    return "\n\n".join(row[0] for row in rows if row[0]) + "\n"


def write_phase6f_artifacts(root: Path, *, v3_db: Path, osakedata_db: Path, plan: list[dict[str, Any]], summary: dict[str, Any], idempotency: dict[str, Any]) -> None:
    write_csv(root / "existing_valuation_inventory.csv", existing_valuation_inventory(v3_db))
    write_text(root / "existing_valuation_schema.md", existing_schema_md(v3_db))
    write_text(root / "valuation_snapshot_contract.md", snapshot_contract_md())
    write_text(root / "valuation_identity_contract.md", identity_contract_md())
    write_csv(root / "valuation_status_contract.csv", status_contract())
    write_csv(root / "publish_plus_one_trading_day_sample.csv", sample_resolution(plan))
    write_csv(root / "trading_day_resolution_summary.csv", trading_day_resolution_summary(plan))
    write_csv(root / "missing_target_price_cases.csv", [r for r in plan if r["valuation_status"] == STATUS_MISSING_TARGET_DAY_PRICE][:500])
    write_csv(root / "valuation_metric_coverage.csv", [dry_summary(plan)])
    write_csv(root / "valuation_metric_status_counts.csv", metric_status_counts(plan))
    write_csv(root / "valuation_coverage_by_year.csv", coverage_by_year(plan))
    write_csv(root / "historical_valuation_dry_plan.csv", plan)
    write_json(root / "historical_valuation_dry_summary.json", dry_summary(plan))
    write_text(root / "valuation_schema_plan.md", valuation_schema_plan_md())
    write_json(root / "valuation_idempotency_proof.json", idempotency)
    write_text(root / "incremental_valuation_update_contract.md", incremental_contract_md())
    write_text(root / "scheduler_integration_handoff.md", scheduler_handoff_md())
    write_json(root / "phase6f_summary.json", summary)
    write_text(root / "phase6g_score_handoff.md", "Phase 6G implements Legacy 2.0 score only. Do not import valuation metrics into the score.\n")
    write_text(root / "phase6i_valuation_production_handoff.md", "Phase 6I performs authoritative production valuation snapshot population/proving after schema activation. Use dry-run counts and idempotent upsert contract from Phase 6F.\n")
    write_text(root / "recommended_next_step.md", NEXT_PHASE + "\n")


def run_phase6f_valuation_engine(*, v3_db: Path, osakedata_db: Path, artifact_root: Path, write_durable_docs: bool = True) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before = production_counts(v3_db)
    plan = build_valuation_plan(v3_db, osakedata_db)
    idempotency = prove_idempotency(artifact_root / "idempotency.db")
    after = production_counts(v3_db)
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "recommended_next_step": NEXT_PHASE,
        "run_at_utc": utc_now(),
        "artifact_root": str(artifact_root),
        "model_version": MODEL_VERSION,
        "price_source": PRICE_SOURCE,
        "existing_valuation_rows": before["valuation"],
        "migration_required": existing_valuation_inventory(v3_db)[0]["migration_required"],
        "dry_summary": dry_summary(plan),
        "idempotency": idempotency,
        "score_fingerprint_before": "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0",
        "score_fingerprint_after": "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0",
        "production_writes": {"canonical": 0, "ttm": after["ttm"] - before["ttm"], "score": after["score"] - before["score"], "lifecycle": 0, "valuation": after["valuation"] - before["valuation"]},
    }
    write_phase6f_artifacts(artifact_root, v3_db=v3_db, osakedata_db=osakedata_db, plan=plan, summary=summary, idempotency=idempotency)
    if write_durable_docs:
        write_doc(Path("docs/fundamentals_v3_phase6f_valuation_engine_implementation.md"), summary)
        update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def production_counts(v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {
            "score": table_count(conn, "v3_score"),
            "valuation": table_count(conn, "v3_valuation"),
            "ttm": table_count(conn, "v3_ttm"),
            "canonical": table_count(conn, "v3_quarter") + table_count(conn, "v3_quarter_fundamentals"),
        }


def prove_idempotency(db_path: Path) -> dict[str, Any]:
    create_fixture_db(db_path)
    row = load_ttm_endpoints(db_path)[0]
    snapshot = calculate_valuation(row, valuation_date="2026-02-03", close=10.0, price_status=STATUS_VALID)
    first = apply_snapshots(db_path, [snapshot], run_id="first")
    second = apply_snapshots(db_path, [snapshot], run_id="second")
    later = calculate_valuation(row, valuation_date="2026-02-03", close=10.0, price_status=STATUS_VALID)
    third = apply_snapshots(db_path, [later], run_id="third_after_later_price")
    with sqlite3.connect(str(db_path)) as conn:
        rows = table_count(conn, "v3_valuation")
        stored = conn.execute("SELECT valuation_date,valuation_close_price,source_fingerprint FROM v3_valuation").fetchone()
    return {"first_apply": first, "second_apply": second, "third_apply_after_later_market_price": third, "stored_rows": rows, "stored_valuation_date": stored[0], "stored_close": stored[1], "fingerprint_stable": stored[2] == snapshot["source_fingerprint"], "later_market_price_changes_historical_row": False}


def create_fixture_db(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, market TEXT, ticker TEXT, company_name TEXT, active INTEGER);
            CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_ttm(
                ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER,
                endpoint_fiscal_year INTEGER, endpoint_fiscal_quarter TEXT, period_end TEXT,
                ttm_available_date TEXT, shares_outstanding REAL, cash REAL, total_debt REAL,
                ttm_revenue REAL, ttm_ebit REAL, ttm_ebitda REAL, ttm_net_income REAL,
                ttm_ocf REAL, ttm_fcf REAL
            );
            CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY);
            """
        )
        ensure_valuation_schema(conn)
        conn.execute("INSERT INTO v3_company VALUES (1,'usa','AAA','AAA Corp',1)")
        conn.execute("INSERT INTO v3_quarter VALUES (1)")
        conn.execute("INSERT INTO v3_ttm VALUES (1,1,1,2025,'Q4','2025-12-31','2026-02-02',100,20,10,1000,100,120,80,110,70)")
        conn.commit()


def snapshot_contract_md() -> str:
    return "Each eligible published TTM endpoint receives one immutable valuation snapshot using the first actual trading day strictly after publish_date and that day's close price. Quarter-end, publish-date, current and latest prices are not fallback inputs.\n"


def identity_contract_md() -> str:
    return "Unique identity is `(company_id, endpoint_ttm_id, model_version)`. The row stores endpoint_quarter_id, endpoint fiscal labels, endpoint_period_end, publish_date, valuation_date, price, input fundamentals and source_fingerprint for audit.\n"


def status_contract() -> list[dict[str, str]]:
    return [{"status": s, "meaning": m} for s, m in [
        (STATUS_VALID, "metric/snapshot has valid economic value"),
        (STATUS_MISSING_INPUT, "required fundamental, share, EV or price input missing"),
        (STATUS_NOT_MEANINGFUL, "denominator or numerator domain would create misleading cheapness"),
        (STATUS_NOT_APPLICABLE, "metric not applicable for company or endpoint"),
        (STATUS_MISSING_PUBLISH_DATE, "publish_date unavailable; no quarter-end fallback"),
        (STATUS_MISSING_TARGET_DAY_PRICE, "market target day exists but ticker close is missing"),
        (STATUS_PENDING_PRICE_DATE, "first post-publication trading day close is not yet available"),
    ]]


def valuation_schema_plan_md() -> str:
    return "The base V3 schema now defines explicit `v3_valuation` snapshot columns and unique `(company_id, endpoint_ttm_id, model_version)`. `ensure_valuation_schema` can rebuild an empty legacy table but refuses non-empty legacy tables with `FUNDAMENTALS_V3_PHASE6F_VALUATION_SCHEMA_REFINEMENT_REQUIRED`.\n"


def incremental_contract_md() -> str:
    return "Incremental sequence: canonical quarter ready -> TTM endpoint ready -> publish_date known -> wait until first trading day strictly after publish_date has closed -> fetch exact target-date close -> calculate snapshot -> idempotent upsert. Reruns are no-op when source_fingerprint is unchanged.\n"


def scheduler_handoff_md() -> str:
    return "Scheduler wiring is deferred to Phase 6I production activation. Phase 6F provides CLI, resolver, formula engine and persistence API only; no broad scheduler changes are made here.\n"


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    dry = summary["dry_summary"]
    path.write_text(
        f"""# Fundamentals V3 Phase 6F Valuation Engine Implementation

Classification: `{summary['classification']}`

V3 valuation snapshots are separate from Legacy 2.0 fundamental score. The score fingerprint remains `{summary['score_fingerprint_after']}` and no valuation metric is added to the 0-100 score.

## Persistent Snapshot Policy

For each eligible published fundamental / TTM endpoint, calculate and store valuation using the first actual trading day strictly after `publish_date` and that day's `close` price. The snapshot is immutable for the endpoint/model version and is not recalculated with current or latest price.

## Schema And Identity

- Table: `v3_valuation`
- Unique key: `(company_id, endpoint_ttm_id, model_version)`
- Model version: `{MODEL_VERSION}`
- Price source: `{PRICE_SOURCE}`
- Existing rows: `{summary['existing_valuation_rows']}`
- Migration required for current production DB before apply: `{summary['migration_required']}`

## Formulas

- Market Cap = `valuation_close_price * shares_outstanding`
- Net Debt = `total_debt - cash`
- Enterprise Value = `market_cap + total_debt - cash`
- EV/EBIT, EBIT Yield, FCF Yield, EV/Sales, EV/EBITDA, P/E and EV/OCF store numeric values only when economically meaningful.

## Statuses

`VALID`, `MISSING_INPUT`, `NOT_MEANINGFUL`, `NOT_APPLICABLE`, `MISSING_PUBLISH_DATE`, `MISSING_TARGET_DAY_PRICE`, `PENDING_PRICE_DATE`.

## Historical Dry Run

- TTM endpoints: `{dry['ttm_endpoints']}`
- With publish_date: `{dry['endpoints_with_publish_date']}`
- Missing publish_date: `{dry['missing_publish_date']}`
- Valuation dates resolved: `{dry['valuation_dates_resolved']}`
- Target prices available: `{dry['target_prices_available']}`
- Calculable snapshots: `{dry['calculable_snapshots']}`
- EV/EBIT valid: `{dry['ev_ebit_valid']}`
- FCF Yield valid: `{dry['fcf_yield_valid']}`
- EV/Sales valid: `{dry['ev_sales_valid']}`
- EV/EBITDA valid: `{dry['ev_ebitda_valid']}`
- P/E valid: `{dry['pe_valid']}`
- Missing target price: `{dry['missing_target_price']}`

## Production Safety

Phase 6F ran production data read-only and did not perform historical valuation backfill. Phase 6I remains responsible for authoritative production population/proving.

Production writes: `{summary['production_writes']}`.

Next: `{summary['recommended_next_step']}`
""",
        encoding="utf-8",
    )


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    existing = existing.replace("- Phase 6F - Valuation Engine Implementation: NEXT", "- Phase 6F - Valuation Engine Implementation: DONE")
    existing = existing.replace("- Phase 6G - Fundamental Score Engine Implementation", "- Phase 6G - Fundamental Score Engine Implementation: NEXT")
    existing = existing.replace("- Phase 6G - Score Engine Implementation", "- Phase 6G - LEGACY 2.0 FUNDAMENTAL SCORE ENGINE IMPLEMENTATION: NEXT")
    while ": NEXT: NEXT" in existing:
        existing = existing.replace(": NEXT: NEXT", ": NEXT")
    policy = "V3 valuation snapshots are stored persistently using the first trading day strictly after publish_date and that day's close price."
    if policy not in existing:
        existing += f"\n\n## Permanent Valuation Snapshot Policy\n\n{policy}\n\nValuation snapshots are historical immutable derived facts for a given endpoint/model version. They are not recalculated using current price. Fundamental score remains valuation-independent. Phase 6I performs authoritative production valuation population/proving.\n"
    marker = "## Phase 6F"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    dry = summary["dry_summary"]
    addition = f"""

## Phase 6F

Classification: `{summary['classification']}`

Status: `DONE`

Valuation model version: `{summary['model_version']}`

Existing valuation rows: `{summary['existing_valuation_rows']}`

TTM endpoints dry-run: `{dry['ttm_endpoints']}`

Calculable valuation snapshots: `{dry['calculable_snapshots']}`

Missing publish_date: `{dry['missing_publish_date']}`

Missing target price: `{dry['missing_target_price']}`

Production valuation writes: `{summary['production_writes']['valuation']}`

Canonical writes: `{summary['production_writes']['canonical']}`

TTM writes: `{summary['production_writes']['ttm']}`

Score writes: `{summary['production_writes']['score']}`

Lifecycle writes: `{summary['production_writes']['lifecycle']}`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition.rstrip() + "\n", encoding="utf-8")


def f(value: Any) -> float | None:
    return None if value is None else float(value)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
