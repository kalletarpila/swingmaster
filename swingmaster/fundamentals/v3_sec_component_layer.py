from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from swingmaster.fundamentals.sec_edgar import SEC_COMPANYFACTS_URL_TEMPLATE, SEC_TICKER_CIK_FALLBACKS, SEC_USER_AGENT, fetch_companyfacts, load_ticker_cik_map
from swingmaster.fundamentals.v3_canonical_closure import final_canonical_baseline, field_coverage_summary


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE4C2C_SEC_COMPONENT_LAYER_COMPLETE_READY_FOR_FORMULA_RERUN"
CLASSIFICATION_INCOMPLETE = "FUNDAMENTALS_V3_PHASE4C2C_SEC_COMPONENT_LAYER_BUILT_ACQUISITION_INCOMPLETE"
NEXT_STEP = "MASTER PLAN PHASE 4C-2D - COMPANY-SPECIFIC FORMULA DISCOVERY RERUN ON SEC COMPONENT LAYER"
HISTORICAL_FLOOR = "2018-01-01"
COMPONENT_DB_DEFAULT = Path("temp/fundamentals_v3_sec_components_runtime/rc_fundamentals_v3_sec_components.db")
RAW_CACHE_DEFAULT = Path("temp/fundamentals_v3_sec_components_runtime/raw_companyfacts")
ARTIFACT_ROOT_DEFAULT = Path("temp/fundamentals_v3_phase4c_2c_sec_component_acquisition/20260823T_PHASE4C2C_SEC_COMPONENTS")
FACT_FORMS = {"10-Q", "10-K", "10-Q/A", "10-K/A"}
FLOW_ROLES = {
    "PRETAX",
    "INTEREST_EXPENSE_GROSS",
    "INTEREST_EXPENSE_NET",
    "INTEREST_INCOME",
    "DEBT_INTEREST",
    "FINANCE_LEASE_INTEREST",
    "FINANCIAL_PRODUCTS_INTEREST",
    "ISSUER_SPECIFIC_INTEREST",
    "D_AND_A_COMBINED",
    "DEPRECIATION",
    "AMORTIZATION",
    "DEPRECIATION_PPE",
    "AMORTIZATION_INTANGIBLES",
    "ISSUER_SPECIFIC_DA",
    "OPERATING_INCOME",
    "NONOPERATING_INCOME",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def connect_component_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialize_component_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sec_component_raw_cache (
            company_id INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            cik TEXT NOT NULL,
            source_url TEXT NOT NULL,
            retrieved_at_utc TEXT NOT NULL,
            http_status INTEGER,
            status TEXT NOT NULL,
            payload_sha256 TEXT,
            payload_json TEXT,
            error TEXT,
            latest_filed_date TEXT,
            fact_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (cik)
        );

        CREATE TABLE IF NOT EXISTS sec_component_acquisition_state (
            company_id INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            cik TEXT,
            requested_at_utc TEXT NOT NULL,
            status TEXT NOT NULL,
            http_status INTEGER,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            retryable INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            payload_sha256 TEXT,
            fact_count INTEGER NOT NULL DEFAULT 0,
            component_coverage_json TEXT,
            PRIMARY KEY (company_id)
        );

        CREATE TABLE IF NOT EXISTS sec_component_fact (
            fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            cik TEXT NOT NULL,
            taxonomy_namespace TEXT NOT NULL,
            concept_name TEXT NOT NULL,
            concept_label TEXT,
            semantic_role TEXT NOT NULL,
            standard_or_extension TEXT NOT NULL,
            value REAL,
            value_text TEXT,
            unit TEXT NOT NULL,
            scale TEXT,
            start_date TEXT,
            end_date TEXT NOT NULL,
            duration_days INTEGER,
            instant_or_duration TEXT NOT NULL,
            form TEXT,
            accession TEXT NOT NULL,
            filed_date TEXT,
            fiscal_year INTEGER,
            fiscal_period TEXT,
            frame TEXT,
            source_url TEXT NOT NULL,
            dimensions_json TEXT,
            fact_json TEXT NOT NULL,
            acquired_at_utc TEXT NOT NULL,
            source_payload_sha256 TEXT NOT NULL,
            natural_key TEXT NOT NULL UNIQUE
        );

        CREATE INDEX IF NOT EXISTS idx_sec_component_fact_company_role
        ON sec_component_fact(company_id, semantic_role, end_date, fiscal_year, fiscal_period);

        CREATE INDEX IF NOT EXISTS idx_sec_component_fact_ticker_role
        ON sec_component_fact(ticker, semantic_role, end_date);

        CREATE INDEX IF NOT EXISTS idx_sec_component_fact_accession
        ON sec_component_fact(accession, filed_date);

        CREATE TABLE IF NOT EXISTS sec_component_concept_registry (
            taxonomy_namespace TEXT NOT NULL,
            concept_name TEXT NOT NULL,
            concept_label TEXT,
            semantic_role_candidate TEXT NOT NULL,
            standard_or_extension TEXT NOT NULL,
            companies_observed INTEGER NOT NULL,
            fact_count INTEGER NOT NULL,
            duration_or_instant TEXT NOT NULL,
            approval_status TEXT NOT NULL,
            notes TEXT,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (taxonomy_namespace, concept_name)
        );
        """
    )


def load_v3_universe(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT company_id,market,ticker,company_name,active FROM v3_company WHERE market='usa' ORDER BY ticker")]


def map_universe_to_cik(companies: list[dict[str, Any]], ticker_map: dict[str, str]) -> list[dict[str, Any]]:
    rows = []
    ticker_map = {**ticker_map, **SEC_TICKER_CIK_FALLBACKS}
    inverse = defaultdict(list)
    for ticker, cik in ticker_map.items():
        inverse[cik].append(ticker)
    for company in companies:
        ticker = str(company["ticker"]).upper()
        cik = ticker_map.get(ticker)
        status = "MAPPED" if cik else "UNMAPPED"
        rows.append({**company, "ticker": ticker, "cik": cik or "", "mapping_status": status, "ambiguity": "AMBIGUOUS_CIK_SHARED" if cik and len(inverse[cik]) > 1 else ""})
    return rows


def calibration_tickers(universe: list[dict[str, Any]]) -> list[str]:
    preferred = ["AAPL", "MSFT", "NVDA", "AMZN", "WMT", "CAT", "XOM", "NEE", "O", "JPM", "SJM", "CAVA"]
    available = {row["ticker"] for row in universe}
    return [ticker for ticker in preferred if ticker in available]


def fetch_or_load_companyfacts(
    *,
    company: dict[str, Any],
    raw_cache_dir: Path,
    user_agent: str,
    now: str,
    fetcher: Callable[[str, str], dict[str, Any]] = fetch_companyfacts,
    use_cache: bool = True,
) -> tuple[str, dict[str, Any] | None, dict[str, Any]]:
    cik = company.get("cik")
    if not cik:
        return "CIK_MISSING", None, {"retryable": 0, "error": "CIK_MISSING", "http_status": None, "cache_hit": 0}
    raw_cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = raw_cache_dir / f"CIK{cik}.json"
    if use_cache and cache_path.exists():
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        return "FETCH_OK", payload, {"retryable": 0, "error": "", "http_status": 200, "cache_hit": 1}
    try:
        payload = fetcher(cik, user_agent)
    except Exception as exc:
        text = str(exc)
        status = "RATE_LIMITED" if "429" in text or "RATE_LIMIT" in text else "FETCH_FAILED"
        return status, None, {"retryable": 1, "error": text[:500], "http_status": 429 if status == "RATE_LIMITED" else None, "cache_hit": 0}
    if not payload.get("facts"):
        cache_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        return "FETCH_EMPTY", payload, {"retryable": 0, "error": "", "http_status": 200, "cache_hit": 0}
    cache_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return "FETCH_OK", payload, {"retryable": 0, "error": "", "http_status": 200, "cache_hit": 0}


def normalize_companyfacts(company: dict[str, Any], companyfacts: dict[str, Any], *, acquired_at_utc: str, source_payload_sha256: str, floor: str = HISTORICAL_FLOOR) -> list[dict[str, Any]]:
    cik = str(company["cik"]).zfill(10)
    rows: list[dict[str, Any]] = []
    for namespace, concepts in companyfacts.get("facts", {}).items():
        if not isinstance(concepts, dict):
            continue
        for concept, concept_payload in concepts.items():
            label = concept_payload.get("label") or concept_payload.get("description") or ""
            role = classify_concept(namespace, concept, label)
            if role == "UNKNOWN":
                keep_unknown = namespace not in {"dei", "srt"} and looks_component_relevant(concept, label)
                if not keep_unknown:
                    continue
            standard = "STANDARD" if namespace == "us-gaap" else "ISSUER_EXTENSION"
            for unit, facts in concept_payload.get("units", {}).items():
                for fact in facts:
                    end = fact.get("end")
                    if not end or end < floor:
                        continue
                    form = fact.get("form") or ""
                    if form and form not in FACT_FORMS:
                        continue
                    accession = fact.get("accn") or fact.get("accession") or ""
                    if not accession:
                        continue
                    value = parse_number(fact.get("val"))
                    start = fact.get("start")
                    duration = duration_days(start, end)
                    instant_or_duration = "DURATION" if start else "INSTANT"
                    fact_json = json.dumps(fact, sort_keys=True)
                    dimensions_json = json.dumps({"frame": fact.get("frame"), "segment": fact.get("segment"), "dim": fact.get("dim")}, sort_keys=True)
                    natural_key = sha256_text("|".join([
                        cik,
                        namespace,
                        concept,
                        str(unit),
                        str(start or ""),
                        str(end),
                        str(accession),
                        str(fact.get("filed") or ""),
                        str(fact.get("fy") or ""),
                        str(fact.get("fp") or ""),
                        dimensions_json,
                    ]))
                    rows.append(
                        {
                            "company_id": int(company["company_id"]),
                            "ticker": str(company["ticker"]).upper(),
                            "cik": cik,
                            "taxonomy_namespace": namespace,
                            "concept_name": concept,
                            "concept_label": label,
                            "semantic_role": role,
                            "standard_or_extension": standard,
                            "value": value,
                            "value_text": "" if fact.get("val") is None else str(fact.get("val")),
                            "unit": str(unit),
                            "scale": None,
                            "start_date": start,
                            "end_date": end,
                            "duration_days": duration,
                            "instant_or_duration": instant_or_duration,
                            "form": form,
                            "accession": accession,
                            "filed_date": fact.get("filed"),
                            "fiscal_year": int(fact["fy"]) if str(fact.get("fy", "")).isdigit() else None,
                            "fiscal_period": fact.get("fp"),
                            "frame": fact.get("frame"),
                            "source_url": SEC_COMPANYFACTS_URL_TEMPLATE.format(cik=cik),
                            "dimensions_json": dimensions_json,
                            "fact_json": fact_json,
                            "acquired_at_utc": acquired_at_utc,
                            "source_payload_sha256": source_payload_sha256,
                            "natural_key": natural_key,
                        }
                    )
    return rows


def parse_number(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def duration_days(start: str | None, end: str | None) -> int | None:
    if not start or not end:
        return None
    try:
        return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).days
    except ValueError:
        return None


def classify_concept(namespace: str, concept: str, label: str = "") -> str:
    text = f"{concept} {label}".lower()
    if "interestpaid" in concept.lower() or "interest paid" in text:
        return "INTEREST_PAID_CASHFLOW_EXCLUDED"
    if namespace != "us-gaap":
        if "interest" in text:
            return "ISSUER_SPECIFIC_INTEREST"
        if any(term in text for term in ("depreciation", "amortization", "depletion")):
            return "ISSUER_SPECIFIC_DA"
        return "UNKNOWN"
    if "beforeincometax" in concept.lower() or "beforetax" in concept.lower() or "pretax" in text or "before income tax" in text:
        return "PRETAX"
    if "financelease" in concept.lower() and "interest" in text:
        return "FINANCE_LEASE_INTEREST"
    if "interestincome" in concept.lower() or "interest income" in text:
        return "INTEREST_INCOME"
    if "interestexpense" in concept.lower() or "interest expense" in text:
        if "net" in text or "nonoperating" in concept.lower():
            return "INTEREST_EXPENSE_NET" if "net" in text else "INTEREST_EXPENSE_GROSS"
        return "INTEREST_EXPENSE_GROSS"
    if "interestanddebtexpense" in concept.lower() or ("debt" in text and "interest" in text):
        return "DEBT_INTEREST"
    if "financialproduct" in concept.lower() and "interest" in text:
        return "FINANCIAL_PRODUCTS_INTEREST"
    if any(token in concept.lower() for token in ("depreciationdepletionandamortization", "depreciationandamortization")):
        return "D_AND_A_COMBINED"
    if "depreciation" in text and "amortization" in text:
        return "D_AND_A_COMBINED"
    if "depreciation" in text:
        return "DEPRECIATION_PPE" if "property" in text or "ppe" in text else "DEPRECIATION"
    if "amortization" in text:
        return "AMORTIZATION_INTANGIBLES" if "intangible" in text else "AMORTIZATION"
    if concept == "OperatingIncomeLoss":
        return "OPERATING_INCOME"
    if "nonoperating" in concept.lower() and "income" in text:
        return "NONOPERATING_INCOME"
    return "UNKNOWN"


def looks_component_relevant(concept: str, label: str) -> bool:
    text = f"{concept} {label}".lower()
    return any(term in text for term in ("pretax", "before tax", "before income tax", "interest", "depreciation", "amortization", "depletion", "operating income", "nonoperating"))


def upsert_raw_cache(conn: sqlite3.Connection, company: dict[str, Any], payload: dict[str, Any] | None, status: str, meta: dict[str, Any], now: str) -> None:
    cik = company.get("cik") or ""
    payload_text = json.dumps(payload, sort_keys=True) if payload is not None else None
    payload_hash = sha256_text(payload_text) if payload_text is not None else None
    conn.execute(
        """
        INSERT INTO sec_component_raw_cache(company_id,ticker,cik,source_url,retrieved_at_utc,http_status,status,payload_sha256,payload_json,error,latest_filed_date,fact_count)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(cik) DO UPDATE SET
            company_id=excluded.company_id,ticker=excluded.ticker,source_url=excluded.source_url,retrieved_at_utc=excluded.retrieved_at_utc,
            http_status=excluded.http_status,status=excluded.status,payload_sha256=excluded.payload_sha256,payload_json=excluded.payload_json,
            error=excluded.error,latest_filed_date=excluded.latest_filed_date,fact_count=excluded.fact_count
        """,
        (
            company["company_id"],
            company["ticker"],
            cik,
            SEC_COMPANYFACTS_URL_TEMPLATE.format(cik=cik) if cik else "",
            now,
            meta.get("http_status"),
            status,
            payload_hash,
            payload_text,
            meta.get("error", ""),
            latest_filed_date(payload),
            count_payload_facts(payload),
        ),
    )


def upsert_facts(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT INTO sec_component_fact(
            company_id,ticker,cik,taxonomy_namespace,concept_name,concept_label,semantic_role,standard_or_extension,value,value_text,unit,scale,
            start_date,end_date,duration_days,instant_or_duration,form,accession,filed_date,fiscal_year,fiscal_period,frame,source_url,
            dimensions_json,fact_json,acquired_at_utc,source_payload_sha256,natural_key
        ) VALUES (
            :company_id,:ticker,:cik,:taxonomy_namespace,:concept_name,:concept_label,:semantic_role,:standard_or_extension,:value,:value_text,:unit,:scale,
            :start_date,:end_date,:duration_days,:instant_or_duration,:form,:accession,:filed_date,:fiscal_year,:fiscal_period,:frame,:source_url,
            :dimensions_json,:fact_json,:acquired_at_utc,:source_payload_sha256,:natural_key
        )
        ON CONFLICT(natural_key) DO UPDATE SET
            concept_label=excluded.concept_label, semantic_role=excluded.semantic_role, value=excluded.value, value_text=excluded.value_text,
            acquired_at_utc=excluded.acquired_at_utc, source_payload_sha256=excluded.source_payload_sha256
        """,
        rows,
    )
    return len(rows)


def update_acquisition_state(conn: sqlite3.Connection, company: dict[str, Any], status: str, meta: dict[str, Any], facts: list[dict[str, Any]], now: str) -> None:
    prior = conn.execute("SELECT attempt_count FROM sec_component_acquisition_state WHERE company_id=?", (company["company_id"],)).fetchone()
    attempts = int(prior["attempt_count"]) + 1 if prior else 1
    coverage = coverage_for_facts(facts)
    conn.execute(
        """
        INSERT INTO sec_component_acquisition_state(company_id,ticker,cik,requested_at_utc,status,http_status,attempt_count,retryable,error,payload_sha256,fact_count,component_coverage_json)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(company_id) DO UPDATE SET
            ticker=excluded.ticker,cik=excluded.cik,requested_at_utc=excluded.requested_at_utc,status=excluded.status,http_status=excluded.http_status,
            attempt_count=excluded.attempt_count,retryable=excluded.retryable,error=excluded.error,payload_sha256=excluded.payload_sha256,
            fact_count=excluded.fact_count,component_coverage_json=excluded.component_coverage_json
        """,
        (
            company["company_id"],
            company["ticker"],
            company.get("cik", ""),
            now,
            status,
            meta.get("http_status"),
            attempts,
            int(meta.get("retryable", 0)),
            meta.get("error", ""),
            facts[0]["source_payload_sha256"] if facts else None,
            len(facts),
            json.dumps(coverage, sort_keys=True),
        ),
    )


def refresh_concept_registry(conn: sqlite3.Connection, now: str) -> None:
    rows = conn.execute(
        """
        SELECT taxonomy_namespace,concept_name,MAX(concept_label) concept_label,semantic_role,standard_or_extension,
               COUNT(DISTINCT company_id) companies_observed,COUNT(*) fact_count,
               CASE WHEN SUM(instant_or_duration='DURATION') >= SUM(instant_or_duration='INSTANT') THEN 'DURATION' ELSE 'INSTANT' END duration_or_instant
        FROM sec_component_fact
        GROUP BY taxonomy_namespace,concept_name,semantic_role,standard_or_extension
        """
    ).fetchall()
    conn.executemany(
        """
        INSERT INTO sec_component_concept_registry(taxonomy_namespace,concept_name,concept_label,semantic_role_candidate,standard_or_extension,companies_observed,fact_count,duration_or_instant,approval_status,notes,updated_at_utc)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(taxonomy_namespace,concept_name) DO UPDATE SET
            concept_label=excluded.concept_label, semantic_role_candidate=excluded.semantic_role_candidate,
            standard_or_extension=excluded.standard_or_extension, companies_observed=excluded.companies_observed,
            fact_count=excluded.fact_count, duration_or_instant=excluded.duration_or_instant,
            approval_status=excluded.approval_status, notes=excluded.notes, updated_at_utc=excluded.updated_at_utc
        """,
        [
            (
                row["taxonomy_namespace"],
                row["concept_name"],
                row["concept_label"],
                row["semantic_role"],
                row["standard_or_extension"],
                row["companies_observed"],
                row["fact_count"],
                row["duration_or_instant"],
                "CANDIDATE" if row["semantic_role"] != "UNKNOWN" and not row["semantic_role"].endswith("EXCLUDED") else "NOT_APPROVED",
                "Issuer extensions retained but not auto-approved." if row["standard_or_extension"] == "ISSUER_EXTENSION" else "",
                now,
            )
            for row in rows
        ],
    )


def latest_filed_date(payload: dict[str, Any] | None) -> str | None:
    dates = []
    if payload:
        for concepts in payload.get("facts", {}).values():
            for concept_payload in concepts.values():
                for facts in concept_payload.get("units", {}).values():
                    dates.extend(str(fact["filed"]) for fact in facts if fact.get("filed"))
    return max(dates) if dates else None


def count_payload_facts(payload: dict[str, Any] | None) -> int:
    if not payload:
        return 0
    return sum(len(facts) for concepts in payload.get("facts", {}).values() for concept_payload in concepts.values() for facts in concept_payload.get("units", {}).values())


def coverage_for_facts(facts: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(row["semantic_role"] for row in facts)
    return {role: counts[role] for role in sorted(counts)}


def acquire_sec_components(
    *,
    v3_db: Path,
    component_db: Path,
    raw_cache_dir: Path,
    artifact_root: Path,
    user_agent: str = SEC_USER_AGENT,
    tickers: list[str] | None = None,
    calibration: bool = False,
    limit: int | None = None,
    plan_only: bool = False,
    rate_limit_seconds: float = 0.11,
    fetcher: Callable[[str, str], dict[str, Any]] = fetch_companyfacts,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    now = utc_now()
    disk_before = shutil.disk_usage(Path(".").resolve()).free
    baseline = final_canonical_baseline(v3_db)
    missing = {row["field"]: int(row["null_q"]) for row in field_coverage_summary(v3_db)}
    universe = load_v3_universe(v3_db)
    ticker_map = load_ticker_cik_map(user_agent) if not plan_only or tickers or calibration else {}
    mapped = map_universe_to_cik(universe, ticker_map) if ticker_map else [{**row, "cik": "", "mapping_status": "UNMAPPED", "ambiguity": ""} for row in universe]
    wanted = set(t.upper() for t in tickers) if tickers else None
    if calibration:
        wanted = set(calibration_tickers(universe))
    selected = [row for row in mapped if (wanted is None or row["ticker"] in wanted)]
    if limit is not None:
        selected = selected[:limit]
    write_design_artifacts(artifact_root)
    write_csv(artifact_root / "universe_cik_mapping.csv", mapped)
    write_csv(artifact_root / "calibration_company_list.csv", selected if calibration else [{"ticker": ticker} for ticker in calibration_tickers(universe)])
    write_csv(artifact_root / "acquisition_plan.csv", selected)
    if plan_only:
        summary = build_summary(v3_db, component_db, raw_cache_dir, artifact_root, baseline, missing, mapped, selected, disk_before, shutil.disk_usage(Path(".").resolve()).free, plan_only=True)
        write_closure_artifacts(artifact_root, summary)
        write_durable_doc(summary)
        update_master_plan(summary)
        return summary

    with connect_component_db(component_db) as conn:
        initialize_component_schema(conn)
        acquisition_rows = []
        for index, company in enumerate(selected, 1):
            status, payload, meta = fetch_or_load_companyfacts(company=company, raw_cache_dir=raw_cache_dir, user_agent=user_agent, now=now, fetcher=fetcher)
            facts: list[dict[str, Any]] = []
            if payload is not None:
                payload_text = json.dumps(payload, sort_keys=True)
                payload_hash = sha256_text(payload_text)
                upsert_raw_cache(conn, company, payload, status, meta, now)
                try:
                    facts = normalize_companyfacts(company, payload, acquired_at_utc=now, source_payload_sha256=payload_hash)
                    upsert_facts(conn, facts)
                except Exception as exc:
                    status = "PARSE_FAILED"
                    meta = {**meta, "retryable": 0, "error": str(exc)[:500]}
            update_acquisition_state(conn, company, status, meta, facts, now)
            acquisition_rows.append({**company, "status": status, "cache_hit": meta.get("cache_hit", 0), "retryable": meta.get("retryable", 0), "facts_normalized": len(facts), "error": meta.get("error", "")})
            conn.commit()
            if rate_limit_seconds > 0 and index < len(selected) and not meta.get("cache_hit"):
                time.sleep(rate_limit_seconds)
        refresh_concept_registry(conn, now)
        conn.commit()

    disk_after = shutil.disk_usage(Path(".").resolve()).free
    write_csv(artifact_root / "acquisition_summary.csv", acquisition_rows)
    write_csv(artifact_root / "acquisition_failures.csv", [row for row in acquisition_rows if row["status"] not in {"FETCH_OK", "FETCH_EMPTY"}])
    summary = build_summary(v3_db, component_db, raw_cache_dir, artifact_root, baseline, missing, mapped, selected, disk_before, disk_after, plan_only=False)
    write_analysis_artifacts(artifact_root, component_db, v3_db)
    write_closure_artifacts(artifact_root, summary)
    write_durable_doc(summary)
    update_master_plan(summary)
    return summary


def close_sec_component_phase(
    *,
    v3_db: Path,
    component_db: Path,
    raw_cache_dir: Path,
    artifact_root: Path,
    user_agent: str = SEC_USER_AGENT,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    disk_before = shutil.disk_usage(Path(".").resolve()).free
    baseline = final_canonical_baseline(v3_db)
    missing = {row["field"]: int(row["null_q"]) for row in field_coverage_summary(v3_db)}
    universe = load_v3_universe(v3_db)
    mapped = map_universe_to_cik(universe, load_ticker_cik_map(user_agent))
    write_design_artifacts(artifact_root)
    write_csv(artifact_root / "universe_cik_mapping.csv", mapped)
    write_csv(artifact_root / "acquisition_plan.csv", mapped)
    write_csv(artifact_root / "calibration_company_list.csv", [{"ticker": ticker} for ticker in calibration_tickers(universe)])
    with connect_component_db(component_db) as conn:
        initialize_component_schema(conn)
        refresh_concept_registry(conn, utc_now())
        conn.commit()
        write_csv(artifact_root / "acquisition_summary.csv", [dict(row) for row in conn.execute("SELECT s.*, CASE WHEN s.cik IS NULL OR s.cik='' THEN 'UNMAPPED' ELSE 'MAPPED' END AS mapping_status, 0 AS cache_hit, s.fact_count AS facts_normalized FROM sec_component_acquisition_state s ORDER BY s.ticker")])
        write_csv(artifact_root / "acquisition_failures.csv", [dict(row) for row in conn.execute("SELECT * FROM sec_component_acquisition_state WHERE status NOT IN ('FETCH_OK','FETCH_EMPTY') ORDER BY ticker")])
    summary = build_summary(v3_db, component_db, raw_cache_dir, artifact_root, baseline, missing, mapped, mapped, disk_before, shutil.disk_usage(Path(".").resolve()).free, plan_only=False)
    write_analysis_artifacts(artifact_root, component_db, v3_db)
    write_closure_artifacts(artifact_root, summary)
    write_durable_doc(summary)
    update_master_plan(summary)
    return summary


def component_db_counts(component_db: Path) -> dict[str, int]:
    if not component_db.exists():
        return {"facts": 0, "raw_cache": 0, "state": 0, "concepts": 0}
    with connect_component_db(component_db) as conn:
        initialize_component_schema(conn)
        return {
            "facts": scalar(conn, "SELECT COUNT(*) FROM sec_component_fact"),
            "raw_cache": scalar(conn, "SELECT COUNT(*) FROM sec_component_raw_cache"),
            "state": scalar(conn, "SELECT COUNT(*) FROM sec_component_acquisition_state"),
            "concepts": scalar(conn, "SELECT COUNT(*) FROM sec_component_concept_registry"),
        }


def build_summary(
    v3_db: Path,
    component_db: Path,
    raw_cache_dir: Path,
    artifact_root: Path,
    baseline: dict[str, Any],
    missing: dict[str, int],
    mapped: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    disk_before: int,
    disk_after: int,
    *,
    plan_only: bool,
) -> dict[str, Any]:
    counts = component_db_counts(component_db)
    analysis = analyze_component_coverage(component_db, v3_db) if component_db.exists() else empty_analysis(missing)
    acquisition = acquisition_summary(component_db, selected) if component_db.exists() else {"companies_fetched": 0, "fetch_ok": 0, "empty": 0, "failed": 0, "retryable_failures": 0, "resumed_successfully": 0}
    mapped_count = sum(1 for row in mapped if row["mapping_status"] == "MAPPED")
    complete = (
        not plan_only
        and len(selected) == len(mapped)
        and acquisition["fetch_ok"] + acquisition["empty"] + acquisition["failed"] == len(selected)
        and analysis["pretax"]["companies_with_pretax"] > 0
        and analysis["interest"]["companies_with_any_interest"] > 0
        and analysis["da"]["companies_with_any_da"] > 0
        and acquisition["hard_failed"] == 0
    )
    return {
        "classification": CLASSIFICATION_COMPLETE if complete else CLASSIFICATION_INCOMPLETE,
        "artifact_root": str(artifact_root),
        "component_db": str(component_db),
            "raw_cache_dir": str(raw_cache_dir),
        "baseline": {
            "companies": baseline["company_total"],
            "active": baseline["active"],
            "inactive": baseline["inactive"],
            "canonical_q": baseline["coverage"]["canonical_q_total"],
            "core_ready": baseline["coverage"]["core_ready_q"],
            "core_not_ready": baseline["coverage"]["core_not_ready_q"],
            "ebit_missing": missing.get("ebit", 0),
            "ebitda_missing": missing.get("ebitda", 0),
        },
        "architecture": {
            "source_selected": "SEC EDGAR companyfacts primary; filing-level XBRL retained as future extension fallback when issuer-extension coverage is insufficient.",
            "endpoints": [SEC_COMPANYFACTS_URL_TEMPLATE, "https://www.sec.gov/files/company_tickers.json"],
            "raw_cache_path": str(raw_cache_dir),
            "normalized_tables": ["sec_component_raw_cache", "sec_component_acquisition_state", "sec_component_fact", "sec_component_concept_registry"],
            "raw_vs_normalized_separation": True,
            "issuer_extensions_retained": True,
            "accession_retained": True,
            "dimensions_retained": True,
            "filing_vintage_retained": True,
            "natural_key": "sha256(cik|namespace|concept|unit|start|end|accession|filed|fy|fp|dimensions_json)",
        },
        "universe": {
            "approved_companies": len(mapped),
            "cik_mapped": mapped_count,
            "cik_unmapped": len(mapped) - mapped_count,
            "cik_ambiguous": sum(1 for row in mapped if row.get("ambiguity")),
            **acquisition,
        },
        "storage": {
            "raw_cache_size_bytes": dir_size(raw_cache_dir),
            "normalized_db_size_bytes": component_db.stat().st_size if component_db.exists() else 0,
            "normalized_fact_count": counts["facts"],
            "disk_free_before_bytes": disk_before,
            "disk_free_after_bytes": disk_after,
            "storage_guard_result": "PASS" if disk_after > 2_000_000_000 else "LOW_SPACE_STOP_REQUIRED",
        },
        **analysis,
        "safety": {
            "canonical_ebit_writes": 0,
            "canonical_ebitda_writes": 0,
            "other_canonical_financial_writes": 0,
            "v3_identity_changes": 0,
            "provider_calls_other_than_sec": 0,
            "acquisition_idempotency": "NATURAL_KEY_UPSERT",
        },
        "recommended_next_step": NEXT_STEP,
    }


def acquisition_summary(component_db: Path, selected: list[dict[str, Any]]) -> dict[str, int]:
    with connect_component_db(component_db) as conn:
        initialize_component_schema(conn)
        rows = [dict(row) for row in conn.execute("SELECT status,retryable,attempt_count FROM sec_component_acquisition_state")]
    counts = Counter(row["status"] for row in rows)
    hard_failed = sum(counts[s] for s in ("FETCH_FAILED", "PARSE_FAILED", "RATE_LIMITED", "OTHER"))
    return {
        "companies_fetched": len(rows),
        "fetch_ok": counts["FETCH_OK"],
        "empty": counts["FETCH_EMPTY"],
        "cik_missing": counts["CIK_MISSING"],
        "failed": counts["CIK_MISSING"] + hard_failed,
        "hard_failed": hard_failed,
        "retryable_failures": sum(1 for row in rows if row["retryable"]),
        "resumed_successfully": sum(1 for row in rows if row["attempt_count"] > 1 and row["status"] == "FETCH_OK"),
    }


def analyze_component_coverage(component_db: Path, v3_db: Path) -> dict[str, Any]:
    with connect_component_db(component_db) as conn:
        initialize_component_schema(conn)
        pretax = role_company_fact_counts(conn, ["PRETAX"])
        interest = {
            "companies_with_gross_interest_candidate": role_company_count(conn, ["INTEREST_EXPENSE_GROSS", "DEBT_INTEREST"]),
            "companies_with_net_interest_candidate": role_company_count(conn, ["INTEREST_EXPENSE_NET"]),
            "companies_with_interest_income": role_company_count(conn, ["INTEREST_INCOME"]),
            "companies_with_finance_lease_interest": role_company_count(conn, ["FINANCE_LEASE_INTEREST"]),
            "companies_with_issuer_specific_interest": role_company_count(conn, ["ISSUER_SPECIFIC_INTEREST"]),
            "companies_with_multiple_interest_candidates": multiple_role_companies(conn, ["INTEREST_EXPENSE_GROSS", "INTEREST_EXPENSE_NET", "DEBT_INTEREST", "FINANCE_LEASE_INTEREST", "ISSUER_SPECIFIC_INTEREST"]),
            "interest_paid_accepted": role_company_count(conn, ["INTEREST_PAID_CASHFLOW_EXCLUDED"], approved_only=True),
            "companies_with_any_interest": role_company_count(conn, ["INTEREST_EXPENSE_GROSS", "INTEREST_EXPENSE_NET", "DEBT_INTEREST", "FINANCE_LEASE_INTEREST", "ISSUER_SPECIFIC_INTEREST"]),
        }
        da = {
            "companies_with_combined_da": role_company_count(conn, ["D_AND_A_COMBINED"]),
            "companies_with_depreciation": role_company_count(conn, ["DEPRECIATION", "DEPRECIATION_PPE"]),
            "companies_with_amortization": role_company_count(conn, ["AMORTIZATION", "AMORTIZATION_INTANGIBLES"]),
            "companies_with_issuer_specific_da": role_company_count(conn, ["ISSUER_SPECIFIC_DA"]),
            "companies_with_any_da": role_company_count(conn, ["D_AND_A_COMBINED", "DEPRECIATION", "DEPRECIATION_PPE", "AMORTIZATION", "AMORTIZATION_INTANGIBLES", "ISSUER_SPECIFIC_DA"]),
        }
        issuer = issuer_extension_summary(conn)
        qready = quarterization_readiness_rows(conn)
        qcounts = Counter(row["readiness_class"] for row in qready)
        direct_ytd = direct_vs_ytd_rows(conn)
        annual = annual_reconciliation_rows(conn)
        annual_pass_rate = sum(row["within_1pct"] for row in annual) / len(annual) if annual else 0.0
        missing_ebit = missing_component_rows(conn, v3_db, "ebit")
        missing_ebitda = missing_component_rows(conn, v3_db, "ebitda")
    return {
        "pretax": pretax,
        "interest": interest,
        "da": da,
        "missing_ebit_population": summarize_missing_ebit(missing_ebit),
        "missing_ebitda_population": summarize_missing_ebitda(missing_ebitda),
        "quarterization": {
            "q1_direct_ready": qcounts["DIRECT_Q1"],
            "q2_direct_ready": qcounts["DIRECT_Q2_3M"],
            "q2_ytd_difference_ready": qcounts["Q2_H1_MINUS_Q1_READY"],
            "q3_direct_ready": qcounts["DIRECT_Q3_3M"],
            "q3_ytd_difference_ready": qcounts["Q3_9M_MINUS_H1_READY"],
            "q4_fy_minus_9m_ready": qcounts["Q4_FY_MINUS_9M_READY"],
            "direct_vs_ytd_validation_observations": len(direct_ytd),
            "direct_vs_ytd_within_1pct": sum(row["within_1pct"] for row in direct_ytd),
            "annual_reconciliation_pass_rate": annual_pass_rate,
            "vintage_conflict_cases": qcounts["VINTAGE_CONFLICT"],
            "dimension_conflict_cases": qcounts["DIMENSION_CONFLICT"],
        },
        "issuer_extensions": issuer,
    }


def empty_analysis(missing: dict[str, int]) -> dict[str, Any]:
    return {
        "pretax": {"companies_with_pretax": 0, "pretax_facts": 0, "standard_concept_companies": 0, "issuer_extension_only_companies": 0},
        "interest": {"companies_with_gross_interest_candidate": 0, "companies_with_net_interest_candidate": 0, "companies_with_interest_income": 0, "companies_with_finance_lease_interest": 0, "companies_with_issuer_specific_interest": 0, "companies_with_multiple_interest_candidates": 0, "interest_paid_accepted": 0, "companies_with_any_interest": 0},
        "da": {"companies_with_combined_da": 0, "companies_with_depreciation": 0, "companies_with_amortization": 0, "companies_with_issuer_specific_da": 0, "companies_with_any_da": 0},
        "missing_ebit_population": {"ebit_missing_baseline": missing.get("ebit", 0), "with_pretax": 0, "with_usable_interest": 0, "with_pretax_one_interest_candidate": 0, "with_pretax_multiple_interest_candidates": 0, "issuer_extension_only": 0, "quarterization_ready_ebit_component_cases": 0},
        "missing_ebitda_population": {"ebitda_missing_baseline": missing.get("ebitda", 0), "canonical_ebit_plus_da": 0, "canonical_ebit_plus_dep_amort": 0, "derivable_ebit_components_plus_da": 0, "derivable_ebit_components_plus_dep_amort": 0, "q4_ready_component_cases": 0},
        "quarterization": {"q1_direct_ready": 0, "q2_direct_ready": 0, "q2_ytd_difference_ready": 0, "q3_direct_ready": 0, "q3_ytd_difference_ready": 0, "q4_fy_minus_9m_ready": 0, "direct_vs_ytd_validation_observations": 0, "direct_vs_ytd_within_1pct": 0, "annual_reconciliation_pass_rate": 0, "vintage_conflict_cases": 0, "dimension_conflict_cases": 0},
        "issuer_extensions": {"extension_concept_count": 0, "companies_using_extensions": 0, "likely_ebit_relevant_extensions": 0, "likely_da_relevant_extensions": 0, "extension_retention_assessment": "NO_EXTENSION_FACTS_OBSERVED"},
    }


def role_company_fact_counts(conn: sqlite3.Connection, roles: list[str]) -> dict[str, int]:
    placeholders = ",".join("?" for _ in roles)
    return {
        "companies_with_pretax": scalar(conn, f"SELECT COUNT(DISTINCT company_id) FROM sec_component_fact WHERE semantic_role IN ({placeholders})", roles),
        "pretax_facts": scalar(conn, f"SELECT COUNT(*) FROM sec_component_fact WHERE semantic_role IN ({placeholders})", roles),
        "standard_concept_companies": scalar(conn, f"SELECT COUNT(DISTINCT company_id) FROM sec_component_fact WHERE semantic_role IN ({placeholders}) AND standard_or_extension='STANDARD'", roles),
        "issuer_extension_only_companies": scalar(conn, f"SELECT COUNT(*) FROM (SELECT company_id, SUM(standard_or_extension='STANDARD') standard_count FROM sec_component_fact WHERE semantic_role IN ({placeholders}) GROUP BY company_id HAVING standard_count=0)", roles),
    }


def role_company_count(conn: sqlite3.Connection, roles: list[str], *, approved_only: bool = False) -> int:
    placeholders = ",".join("?" for _ in roles)
    extra = " AND 1=0" if approved_only and roles == ["INTEREST_PAID_CASHFLOW_EXCLUDED"] else ""
    return scalar(conn, f"SELECT COUNT(DISTINCT company_id) FROM sec_component_fact WHERE semantic_role IN ({placeholders}){extra}", roles)


def multiple_role_companies(conn: sqlite3.Connection, roles: list[str]) -> int:
    placeholders = ",".join("?" for _ in roles)
    return scalar(conn, f"SELECT COUNT(*) FROM (SELECT company_id,end_date,COUNT(DISTINCT semantic_role) c FROM sec_component_fact WHERE semantic_role IN ({placeholders}) GROUP BY company_id,end_date HAVING c>1)", roles)


def issuer_extension_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "extension_concept_count": scalar(conn, "SELECT COUNT(DISTINCT taxonomy_namespace || ':' || concept_name) FROM sec_component_fact WHERE standard_or_extension='ISSUER_EXTENSION'"),
        "companies_using_extensions": scalar(conn, "SELECT COUNT(DISTINCT company_id) FROM sec_component_fact WHERE standard_or_extension='ISSUER_EXTENSION'"),
        "likely_ebit_relevant_extensions": scalar(conn, "SELECT COUNT(DISTINCT taxonomy_namespace || ':' || concept_name) FROM sec_component_fact WHERE semantic_role IN ('ISSUER_SPECIFIC_INTEREST','PRETAX','NONOPERATING_INCOME') AND standard_or_extension='ISSUER_EXTENSION'"),
        "likely_da_relevant_extensions": scalar(conn, "SELECT COUNT(DISTINCT taxonomy_namespace || ':' || concept_name) FROM sec_component_fact WHERE semantic_role='ISSUER_SPECIFIC_DA'"),
        "extension_retention_assessment": "RETAINED_NOT_AUTO_APPROVED",
    }


def quarterization_readiness_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = [dict(row) for row in conn.execute("SELECT company_id,ticker,semantic_role,concept_name,end_date,start_date,duration_days,form,accession,filed_date,fiscal_year,fiscal_period,frame,dimensions_json FROM sec_component_fact WHERE instant_or_duration='DURATION' AND semantic_role IN (%s)" % ",".join("?" for _ in FLOW_ROLES), sorted(FLOW_ROLES))]
    out = []
    for row in rows:
        fp = row.get("fiscal_period") or ""
        days = row.get("duration_days") or 0
        if fp == "Q1" and duration_compatible(days, 60, 115):
            cls = "DIRECT_Q1"
        elif fp == "Q2" and duration_compatible(days, 60, 115):
            cls = "DIRECT_Q2_3M"
        elif fp == "Q2" and duration_compatible(days, 150, 210):
            cls = "Q2_H1_MINUS_Q1_READY"
        elif fp == "Q3" and duration_compatible(days, 60, 115):
            cls = "DIRECT_Q3_3M"
        elif fp == "Q3" and duration_compatible(days, 240, 310):
            cls = "Q3_9M_MINUS_H1_READY"
        elif fp == "FY":
            cls = "Q4_FY_MINUS_9M_READY"
        elif row.get("dimensions_json") not in {"{}", '{"dim": null, "frame": null, "segment": null}'}:
            cls = "DIMENSION_CONFLICT"
        else:
            cls = "UNAVAILABLE"
        out.append({**row, "readiness_class": cls})
    return out


def duration_compatible(days: int, low: int, high: int) -> bool:
    return low <= int(days or 0) <= high


def direct_vs_ytd_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    facts = quarterization_value_index(conn)
    out = []
    for key, by_fp in facts.items():
        for target, cumulative, prior in (("Q2", "Q2", "Q1"), ("Q3", "Q3", "Q2")):
            direct = by_fp.get((target, "DIRECT"))
            ytd = by_fp.get((cumulative, "YTD"))
            prev_ytd = by_fp.get((prior, "YTD")) or by_fp.get((prior, "DIRECT"))
            if direct is None or ytd is None or prev_ytd is None:
                continue
            derived = ytd["value"] - prev_ytd["value"]
            rel = relative_error(direct["value"], derived)
            out.append({"ticker": direct["ticker"], "semantic_role": key[1], "concept_name": key[2], "fiscal_year": key[3], "fiscal_quarter": target, "direct_value": direct["value"], "derived_value": derived, "relative_error": rel, "within_1pct": int(rel <= 0.01), "same_accession": int(direct["accession"] == ytd["accession"] == prev_ytd["accession"])})
    return out


def annual_reconciliation_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    facts = quarterization_value_index(conn)
    out = []
    for key, by_fp in facts.items():
        qvals = [by_fp.get((q, "DIRECT")) for q in ("Q1", "Q2", "Q3", "Q4")]
        fy = by_fp.get(("FY", "YTD"))
        if fy is None or any(v is None for v in qvals):
            continue
        total = sum(v["value"] for v in qvals if v)
        rel = relative_error(fy["value"], total)
        out.append({"ticker": fy["ticker"], "semantic_role": key[1], "concept_name": key[2], "fiscal_year": key[3], "fy_value": fy["value"], "sum_quarters": total, "relative_error": rel, "within_1pct": int(rel <= 0.01)})
    return out


def quarterization_value_index(conn: sqlite3.Connection) -> dict[tuple[Any, ...], dict[tuple[str, str], dict[str, Any]]]:
    rows = conn.execute(
        """
        SELECT company_id,ticker,semantic_role,concept_name,fiscal_year,fiscal_period,duration_days,value,accession,filed_date
        FROM sec_component_fact
        WHERE value IS NOT NULL AND instant_or_duration='DURATION'
        """
    )
    out: dict[tuple[Any, ...], dict[tuple[str, str], dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        fp = row["fiscal_period"]
        if fp not in {"Q1", "Q2", "Q3", "Q4", "FY"}:
            continue
        duration_class = "DIRECT" if fp.startswith("Q") and duration_compatible(row["duration_days"] or 0, 60, 115) else "YTD"
        key = (row["company_id"], row["semantic_role"], row["concept_name"], row["fiscal_year"])
        out[key].setdefault((fp, duration_class), dict(row))
    return out


def missing_component_rows(conn: sqlite3.Connection, v3_db: Path, field: str) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as v3:
        v3.row_factory = sqlite3.Row
        rows = [dict(row) for row in v3.execute(
            f"""
            SELECT c.company_id,c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,f.ebit,f.ebitda
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE f.{field} IS NULL
            """
        )]
    facts = facts_by_company_fyfp(conn)
    out = []
    for row in rows:
        roles = facts.get((row["company_id"], row["fiscal_year"], row["fiscal_quarter"]), set())
        interest_roles = roles & {"INTEREST_EXPENSE_GROSS", "INTEREST_EXPENSE_NET", "DEBT_INTEREST", "FINANCE_LEASE_INTEREST", "ISSUER_SPECIFIC_INTEREST"}
        da_roles = roles & {"D_AND_A_COMBINED", "DEPRECIATION", "DEPRECIATION_PPE", "AMORTIZATION", "AMORTIZATION_INTANGIBLES", "ISSUER_SPECIFIC_DA"}
        out.append({**row, "has_pretax": int("PRETAX" in roles), "interest_candidate_count": len(interest_roles), "has_usable_interest": int(bool(interest_roles)), "issuer_extension_only": int(bool(roles & {"ISSUER_SPECIFIC_INTEREST", "ISSUER_SPECIFIC_DA"})), "has_da": int(bool(da_roles)), "has_combined_da": int("D_AND_A_COMBINED" in roles or "ISSUER_SPECIFIC_DA" in roles), "has_dep_amort": int(bool(roles & {"DEPRECIATION", "DEPRECIATION_PPE"}) and bool(roles & {"AMORTIZATION", "AMORTIZATION_INTANGIBLES"})), "q4_ready": int(row["fiscal_quarter"] == "Q4" and bool(roles))})
    return out


def facts_by_company_fyfp(conn: sqlite3.Connection) -> dict[tuple[int, int, str], set[str]]:
    out: dict[tuple[int, int, str], set[str]] = defaultdict(set)
    for row in conn.execute("SELECT company_id,fiscal_year,fiscal_period,semantic_role FROM sec_component_fact WHERE fiscal_year IS NOT NULL AND fiscal_period IN ('Q1','Q2','Q3','Q4')"):
        out[(row["company_id"], row["fiscal_year"], row["fiscal_period"])].add(row["semantic_role"])
    return out


def summarize_missing_ebit(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "ebit_missing_baseline": len(rows),
        "with_pretax": sum(row["has_pretax"] for row in rows),
        "with_usable_interest": sum(row["has_usable_interest"] for row in rows),
        "with_pretax_one_interest_candidate": sum(1 for row in rows if row["has_pretax"] and row["interest_candidate_count"] == 1),
        "with_pretax_multiple_interest_candidates": sum(1 for row in rows if row["has_pretax"] and row["interest_candidate_count"] > 1),
        "issuer_extension_only": sum(row["issuer_extension_only"] for row in rows),
        "quarterization_ready_ebit_component_cases": sum(1 for row in rows if row["has_pretax"] and row["has_usable_interest"]),
    }


def summarize_missing_ebitda(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "ebitda_missing_baseline": len(rows),
        "canonical_ebit_plus_da": sum(1 for row in rows if row["ebit"] is not None and row["has_combined_da"]),
        "canonical_ebit_plus_dep_amort": sum(1 for row in rows if row["ebit"] is not None and row["has_dep_amort"]),
        "derivable_ebit_components_plus_da": sum(1 for row in rows if row["has_pretax"] and row["has_usable_interest"] and row["has_combined_da"]),
        "derivable_ebit_components_plus_dep_amort": sum(1 for row in rows if row["has_pretax"] and row["has_usable_interest"] and row["has_dep_amort"]),
        "q4_ready_component_cases": sum(row["q4_ready"] for row in rows),
    }


def relative_error(a: float, b: float) -> float:
    return abs(float(a) - float(b)) / max(abs(float(a)), abs(float(b)), 1_000.0)


def write_analysis_artifacts(root: Path, component_db: Path, v3_db: Path) -> None:
    with connect_component_db(component_db) as conn:
        initialize_component_schema(conn)
        write_csv(root / "sec_component_semantic_registry.csv", [dict(row) for row in conn.execute("SELECT * FROM sec_component_concept_registry ORDER BY semantic_role_candidate, taxonomy_namespace, concept_name")])
        write_csv(root / "pretax_component_coverage.csv", role_coverage_rows(conn, ["PRETAX"]))
        write_csv(root / "interest_component_coverage.csv", role_coverage_rows(conn, ["INTEREST_EXPENSE_GROSS", "INTEREST_EXPENSE_NET", "INTEREST_INCOME", "DEBT_INTEREST", "FINANCE_LEASE_INTEREST", "FINANCIAL_PRODUCTS_INTEREST", "ISSUER_SPECIFIC_INTEREST", "INTEREST_PAID_CASHFLOW_EXCLUDED"]))
        write_csv(root / "da_component_coverage.csv", role_coverage_rows(conn, ["D_AND_A_COMBINED", "DEPRECIATION", "AMORTIZATION", "DEPRECIATION_PPE", "AMORTIZATION_INTANGIBLES", "ISSUER_SPECIFIC_DA"]))
        write_csv(root / "issuer_extension_coverage.csv", [dict(row) for row in conn.execute("SELECT ticker,taxonomy_namespace,concept_name,semantic_role,COUNT(*) fact_count FROM sec_component_fact WHERE standard_or_extension='ISSUER_EXTENSION' GROUP BY ticker,taxonomy_namespace,concept_name,semantic_role ORDER BY ticker,concept_name")])
        write_csv(root / "coverage_by_company.csv", coverage_by_company(conn))
        write_csv(root / "coverage_by_quarter.csv", coverage_by_quarter(conn))
        write_csv(root / "quarterization_readiness.csv", quarterization_readiness_rows(conn))
        write_csv(root / "direct_vs_ytd_component_validation.csv", direct_vs_ytd_rows(conn))
        write_csv(root / "q4_fy_minus_9m_component_validation.csv", q4_component_rows(conn))
        write_csv(root / "annual_component_reconciliation.csv", annual_reconciliation_rows(conn))
        write_csv(root / "vintage_consistency_analysis.csv", vintage_rows(conn))
        write_csv(root / "calibration_component_coverage.csv", coverage_by_company(conn))
        write_csv(root / "calibration_issuer_extensions.csv", [dict(row) for row in conn.execute("SELECT ticker,taxonomy_namespace,concept_name,semantic_role,COUNT(*) fact_count FROM sec_component_fact WHERE standard_or_extension='ISSUER_EXTENSION' GROUP BY ticker,taxonomy_namespace,concept_name,semantic_role")])
        write_csv(root / "calibration_context_validation.csv", context_validation_rows(conn))
        write_csv(root / "phase4c2d_input_population.csv", formula_input_population(conn))
        write_csv(root / "missing_ebit_component_coverage.csv", missing_component_rows(conn, v3_db, "ebit"))
        write_csv(root / "missing_ebitda_component_coverage.csv", missing_component_rows(conn, v3_db, "ebitda"))


def role_coverage_rows(conn: sqlite3.Connection, roles: list[str]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in roles)
    return [dict(row) for row in conn.execute(f"SELECT semantic_role,taxonomy_namespace,concept_name,standard_or_extension,COUNT(DISTINCT company_id) companies,COUNT(*) facts,MIN(end_date) first_end_date,MAX(end_date) last_end_date FROM sec_component_fact WHERE semantic_role IN ({placeholders}) GROUP BY semantic_role,taxonomy_namespace,concept_name,standard_or_extension ORDER BY facts DESC", roles)]


def coverage_by_company(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute("SELECT ticker,company_id,COUNT(*) facts,COUNT(DISTINCT concept_name) concepts,SUM(semantic_role='PRETAX') pretax_facts,SUM(semantic_role IN ('INTEREST_EXPENSE_GROSS','INTEREST_EXPENSE_NET','DEBT_INTEREST','FINANCE_LEASE_INTEREST','ISSUER_SPECIFIC_INTEREST')) interest_facts,SUM(semantic_role IN ('D_AND_A_COMBINED','DEPRECIATION','DEPRECIATION_PPE','AMORTIZATION','AMORTIZATION_INTANGIBLES','ISSUER_SPECIFIC_DA')) da_facts FROM sec_component_fact GROUP BY ticker,company_id ORDER BY ticker")]


def coverage_by_quarter(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute("SELECT fiscal_period,semantic_role,COUNT(DISTINCT company_id) companies,COUNT(*) facts FROM sec_component_fact WHERE fiscal_period IN ('Q1','Q2','Q3','Q4','FY') GROUP BY fiscal_period,semantic_role ORDER BY fiscal_period,semantic_role")]


def q4_component_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    facts = quarterization_value_index(conn)
    out = []
    for key, by_fp in facts.items():
        fy = by_fp.get(("FY", "YTD"))
        q3 = by_fp.get(("Q3", "YTD"))
        q4 = by_fp.get(("Q4", "DIRECT"))
        if not fy or not q3:
            continue
        derived = fy["value"] - q3["value"]
        rel = relative_error(q4["value"], derived) if q4 else ""
        out.append({"ticker": fy["ticker"], "semantic_role": key[1], "concept_name": key[2], "fiscal_year": key[3], "fy_minus_9m_value": derived, "explicit_q4_value": q4["value"] if q4 else "", "relative_error": rel, "within_1pct": int(rel != "" and rel <= 0.01), "same_accession": int(q3["accession"] == fy["accession"])})
    return out


def vintage_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute("SELECT ticker,semantic_role,concept_name,end_date,COUNT(DISTINCT accession) accessions,COUNT(DISTINCT filed_date) filed_dates,COUNT(*) facts FROM sec_component_fact GROUP BY ticker,semantic_role,concept_name,end_date HAVING accessions>1 OR filed_dates>1 ORDER BY facts DESC")]


def context_validation_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute("SELECT ticker,semantic_role,concept_name,COUNT(*) facts,SUM(dimensions_json IS NOT NULL AND dimensions_json NOT IN ('{}','{\"dim\": null, \"frame\": null, \"segment\": null}')) dimensional_facts,COUNT(DISTINCT accession) accessions FROM sec_component_fact GROUP BY ticker,semantic_role,concept_name")]


def formula_input_population(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute("SELECT ticker,company_id,fiscal_year,fiscal_period,COUNT(*) facts,SUM(semantic_role='PRETAX') pretax_facts,SUM(semantic_role IN ('INTEREST_EXPENSE_GROSS','INTEREST_EXPENSE_NET','DEBT_INTEREST','FINANCE_LEASE_INTEREST','ISSUER_SPECIFIC_INTEREST')) interest_facts,SUM(semantic_role IN ('D_AND_A_COMBINED','DEPRECIATION','DEPRECIATION_PPE','AMORTIZATION','AMORTIZATION_INTANGIBLES','ISSUER_SPECIFIC_DA')) da_facts,COUNT(DISTINCT accession) accessions FROM sec_component_fact WHERE fiscal_year IS NOT NULL AND fiscal_period IN ('Q1','Q2','Q3','Q4','FY') GROUP BY ticker,company_id,fiscal_year,fiscal_period ORDER BY ticker,fiscal_year,fiscal_period")]


def write_design_artifacts(root: Path) -> None:
    write_text(root / "preflight.md", "Phase 4C-2C SEC component acquisition. Canonical financial writes: 0. V3 identity changes: 0.\n")
    write_text(root / "sec_endpoint_analysis.md", "Selected primary source: SEC companyfacts. Endpoints: company_tickers.json for CIK mapping and data.sec.gov/api/xbrl/companyfacts/CIK##########.json for facts. Filing-level XBRL remains future fallback for issuer extensions not exposed by companyfacts. Existing urllib SEC infrastructure and User-Agent are reused.\n")
    write_text(root / "sec_component_architecture.md", "SEC companyfacts -> raw JSON cache -> normalized sec_component_fact rows -> concept registry -> quarterization/formula-discovery input. Raw cache and normalized component DB are separate runtime artifacts and are not committed.\n")
    write_text(root / "sec_component_schema.md", "Tables: sec_component_raw_cache, sec_component_acquisition_state, sec_component_fact, sec_component_concept_registry. sec_component_fact stores explicit metadata columns for namespace, concept, unit, start/end, duration, form, accession, filed date, FY/FP, frame, dimensions_json, fact_json and payload hash.\n")
    write_text(root / "future_incremental_update_design.md", "Future update flow: detect new SEC filing/accession, fetch companyfacts for the issuer, upsert raw cache and normalized facts by natural key, refresh registry/coverage, then let formula fingerprints derive candidates for canonical validation. No automatic canonical write is enabled in this phase.\n")
    write_text(root / "formula_fingerprint_input_contract.md", "Phase 4C-2D input rows are normalized component facts grouped by ticker/company_id/fiscal_year/fiscal_period/accession/semantic_role. Formula discovery may use only approved semantic roles, same-vintage compatible facts, and explicit company-specific fingerprints.\n")
    write_text(root / "canonical_derivation_provenance_design.md", "Future canonical EBIT/EBITDA derivations should reference formula profile id/version, component fact natural keys, accessions, filed dates, quarterization method, and validation status. Provenance must remain separate from raw SEC fact storage.\n")


def write_closure_artifacts(root: Path, summary: dict[str, Any]) -> None:
    write_json(root / "phase4c2c_summary.json", summary)
    write_text(root / "recommended_next_step.md", summary["recommended_next_step"] + "\n")
    write_text(root / "acquisition_storage_summary.md", json.dumps(summary["storage"], indent=2, sort_keys=True) + "\n")
    write_text(root / "disk_space_guard.md", f"free_before_bytes={summary['storage']['disk_free_before_bytes']}\nfree_after_bytes={summary['storage']['disk_free_after_bytes']}\nresult={summary['storage']['storage_guard_result']}\n")


def write_durable_doc(summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 4C-2C SEC Component Acquisition

Classification: `{summary['classification']}`

Canonical financial writes: `0`

## Source

Primary source is SEC companyfacts. Filing-level XBRL remains the designed fallback for issuer-specific extension gaps.

## Storage

- Raw cache: `{summary['architecture']['raw_cache_path']}`
- Component DB: `{summary['component_db']}`
- Tables: {', '.join(summary['architecture']['normalized_tables'])}
- Natural key: `{summary['architecture']['natural_key']}`

## Universe

- Approved companies: {summary['universe']['approved_companies']}
- CIK mapped: {summary['universe']['cik_mapped']}
- CIK unmapped: {summary['universe']['cik_unmapped']}
- Companies fetched/state rows: {summary['universe']['companies_fetched']}
- Fetch OK: {summary['universe']['fetch_ok']}
- Empty: {summary['universe']['empty']}
- CIK missing bounded residuals: {summary['universe']['cik_missing']}
- Hard fetch/parse/rate failures: {summary['universe']['hard_failed']}

## Coverage

- Pretax companies: {summary['pretax']['companies_with_pretax']}
- Interest companies: {summary['interest']['companies_with_any_interest']}
- D&A companies: {summary['da']['companies_with_any_da']}
- Missing EBIT with pretax + interest: {summary['missing_ebit_population']['quarterization_ready_ebit_component_cases']}
- Missing EBITDA derivable EBIT + D&A: {summary['missing_ebitda_population']['derivable_ebit_components_plus_da']}

## Quarterization

- Q1 direct-ready: {summary['quarterization']['q1_direct_ready']}
- Q2 direct-ready: {summary['quarterization']['q2_direct_ready']}
- Q2 YTD-difference-ready: {summary['quarterization']['q2_ytd_difference_ready']}
- Q3 direct-ready: {summary['quarterization']['q3_direct_ready']}
- Q3 YTD-difference-ready: {summary['quarterization']['q3_ytd_difference_ready']}
- Q4 FY-minus-9M-ready: {summary['quarterization']['q4_fy_minus_9m_ready']}

## Issuer Extensions

Issuer extensions retained: `{summary['architecture']['issuer_extensions_retained']}`. Semantic approval is deferred to company-specific mapping.

## Next

`{summary['recommended_next_step']}`
"""
    write_text(Path("docs/fundamentals_v3_phase4c_2c_sec_component_acquisition.md"), text)


def update_master_plan(summary: dict[str, Any]) -> None:
    path = Path("docs/fundamentals_v3_master_plan_status.md")
    text = path.read_text(encoding="utf-8")
    section = f"""

## Phase 4C-2C

Classification: `{summary['classification']}`

Status: `SEC_COMPONENT_LAYER_BUILT`

Canonical financial writes: `0`

Metadata writes: `0`

Component facts: `{summary['storage']['normalized_fact_count']}`

Next: `{summary['recommended_next_step']}`
"""
    marker = "\n## Phase 4C-2C\n"
    if marker in text:
        text = text.split(marker, 1)[0].rstrip() + section
    else:
        text = text.rstrip() + section
    write_text(path, text)


def scalar(conn: sqlite3.Connection, sql: str, params: list[Any] | tuple[Any, ...] = ()) -> int:
    return int(conn.execute(sql, params).fetchone()[0] or 0)


def dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


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


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
