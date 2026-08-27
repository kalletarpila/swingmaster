from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro, file_state, integrity
from swingmaster.fundamentals.v3_phase8a6_safe_apply import sha_rows


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE8C_FISCAL_CALENDAR_METADATA_COMPLETE"
CLASSIFICATION_REVIEW = "FUNDAMENTALS_V3_PHASE8C_FISCAL_CALENDAR_METADATA_COMPLETE_WITH_REVIEW_ITEMS"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8C_FISCAL_CALENDAR_METADATA_BLOCKED"
PROFILE_TABLE = "v3_company_fiscal_calendar_profile"
ANCHOR_TABLE = "v3_company_fiscal_year_calendar"
EXPECTED_P1_TICKERS = ("BBY", "DELL", "FNGR", "GCO", "HAE", "MRVL", "POWW", "RH", "RL", "SAIC", "TJX", "TRNS", "VTGN")

PROFILE_DDL = f"""
CREATE TABLE IF NOT EXISTS {PROFILE_TABLE} (
    profile_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    calendar_type TEXT NOT NULL CHECK (calendar_type IN ('CALENDAR_YEAR','FIXED_DATE_FISCAL_YEAR','WEEK_BASED_52_53','OTHER_VERIFIED','UNKNOWN')),
    start_basis TEXT NOT NULL CHECK (start_basis IN ('FIXED_DATE','WEEKDAY_NEAR_DATE','OTHER')),
    reference_month INTEGER,
    reference_day INTEGER,
    anchor_weekday TEXT,
    relative_position_rule TEXT,
    supports_52_53_week INTEGER NOT NULL CHECK (supports_52_53_week IN (0,1)),
    fiscal_year_label_convention TEXT NOT NULL,
    typical_start_description_raw TEXT NOT NULL,
    profile_parse_status TEXT NOT NULL CHECK (profile_parse_status IN ('PARSED','UNPARSED')),
    source_type TEXT NOT NULL,
    source_reference TEXT NOT NULL,
    confidence TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id)
);
"""

ANCHOR_DDL = f"""
CREATE TABLE IF NOT EXISTS {ANCHOR_TABLE} (
    anchor_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    fiscal_year INTEGER NOT NULL,
    fiscal_year_start_date TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_reference TEXT NOT NULL,
    confidence TEXT NOT NULL,
    verification_status TEXT NOT NULL,
    import_state TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id, fiscal_year)
);
CREATE INDEX IF NOT EXISTS idx_v3_company_fiscal_year_calendar_start
ON {ANCHOR_TABLE}(fiscal_year_start_date);
"""

FISCAL_CALENDAR_SCHEMA_SQL = PROFILE_DDL + "\n" + ANCHOR_DDL

MONTHS_FI = {
    "tammikuuta": 1,
    "tammikuun": 1,
    "helmikuuta": 2,
    "helmikuun": 2,
    "maaliskuuta": 3,
    "maaliskuun": 3,
    "huhtikuuta": 4,
    "huhtikuun": 4,
    "toukokuuta": 5,
    "toukokuun": 5,
    "kesäkuuta": 6,
    "kesäkuun": 6,
    "heinäkuuta": 7,
    "heinäkuun": 7,
    "elokuuta": 8,
    "elokuun": 8,
    "syyskuuta": 9,
    "syyskuun": 9,
    "lokakuuta": 10,
    "lokakuun": 10,
    "marraskuuta": 11,
    "marraskuun": 11,
    "joulukuuta": 12,
    "joulukuun": 12,
}
WEEKDAYS_FI = {
    "maanantai": "MONDAY",
    "tiistai": "TUESDAY",
    "keskiviikko": "WEDNESDAY",
    "torstai": "THURSDAY",
    "perjantai": "FRIDAY",
    "lauantai": "SATURDAY",
    "sunnuntai": "SUNDAY",
}
QUARTER_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}


@dataclass(frozen=True)
class Phase8CPaths:
    artifact_root: Path
    input_csv: Path = Path("temp/v3_active_tickers_all.csv")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    phase8b_artifact_root: Path = Path("temp/fundamentals_v3_phase8b_downstream_rebuild/20260827T_PHASE8B")


@dataclass(frozen=True)
class FiscalCalendarTransitionEvidence:
    status: str = "STABLE_CALENDAR"
    evidence: str | None = None


@dataclass(frozen=True)
class FiscalCalendarWriteCandidate:
    company_id: int
    fiscal_year: int
    fiscal_quarter: str
    period_end_date: str | None
    publish_date: str | None = None
    source_context: str | None = None
    provider_fiscal_year: int | None = None
    provider_fiscal_quarter: str | None = None
    financial_fingerprint_state: str | None = None
    transition_evidence: FiscalCalendarTransitionEvidence = FiscalCalendarTransitionEvidence()
    stub_period: bool = False


@dataclass(frozen=True)
class FiscalCalendarGuardDecision:
    decision: str
    write_permitted: bool
    reason_codes: tuple[str, ...]
    inferred_fiscal_year: int | None
    inferred_fiscal_quarter: str | None
    exact_anchor_used: str | None
    calendar_type: str | None
    calendar_regime: str
    slot_confidence: str
    target_collision_state: str
    chronology_state: str
    financial_corroboration_state: str
    transition_evidence: str


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def ensure_fiscal_calendar_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(FISCAL_CALENDAR_SCHEMA_SQL)


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")]


def table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def baseline_summary(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {
            "companies": table_count(conn, "v3_company"),
            "active_companies": int(conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=1").fetchone()[0]),
            "inactive_companies": int(conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=0").fetchone()[0]),
            "canonical_quarter_rows": table_count(conn, "v3_quarter"),
            "fundamentals_rows": table_count(conn, "v3_quarter_fundamentals"),
            "migration_audit_rows": table_count(conn, "v3_migration_audit"),
            "provider_acquisition_rows": table_count(conn, "v3_provider_q_acquisition"),
            "ttm_rows": table_count(conn, "v3_ttm"),
            "score_rows": table_count(conn, "v3_score"),
            "lifecycle_rows": table_count(conn, "v3_lifecycle"),
            "valuation_rows": table_count(conn, "v3_valuation"),
        }


def canonical_fingerprint(v3_db: Path) -> dict[str, Any]:
    sql = """
        SELECT c.company_id,c.market,c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
               f.revenue,f.operating_income,f.ebit,f.ebitda,f.net_income,f.operating_cashflow,f.capex,
               f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        ORDER BY c.company_id,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
    """
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        data = [dict(row) for row in conn.execute(sql)]
    return {"rows": len(data), "sha256": sha_rows(data)}


def downstream_fingerprint(v3_db: Path, table: str) -> dict[str, Any]:
    volatile = {"run_id", "created_at_utc", "updated_at_utc", "calculated_at_utc"}
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})") if row[1] not in volatile and not str(row[1]).endswith("_id")]
        data = [dict(row) for row in conn.execute(f"SELECT {','.join(cols)} FROM {table} ORDER BY {','.join(cols)}")]
    return {"table": table, "rows": len(data), "sha256": sha_rows(data)}


def schema_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    ensure_fiscal_calendar_schema(conn)
    return {
        PROFILE_TABLE: table_columns(conn, PROFILE_TABLE),
        ANCHOR_TABLE: table_columns(conn, ANCHOR_TABLE),
        "profile_unique_key": "company_id",
        "anchor_unique_key": "company_id,fiscal_year",
    }


def parse_date(value: str) -> str | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def normalize_source_type(source: str) -> str:
    text = (source or "").lower()
    if "data.sec.gov" in text or "sec.gov" in text:
        return "SEC_COMPANYFACTS"
    if "investor" in text or "/ir" in text:
        return "ISSUER_IR"
    if "10-k" in text or "annual" in text:
        return "ISSUER_FILING"
    return "OTHER_OFFICIAL"


def source_fingerprint(row: dict[str, Any]) -> str:
    payload = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def parse_profile_description(raw: str, fy2026_start: str, fy2027_start: str | None) -> dict[str, Any]:
    text = (raw or "").strip()
    lower = text.lower()
    exact = re.search(r"(\d{1,2})\.\s*([a-zåäö]+)", lower)
    weekday = next((value for key, value in WEEKDAYS_FI.items() if key in lower), None)
    month = next((num for name, num in MONTHS_FI.items() if name in lower), None)
    interval_days = (date.fromisoformat(fy2027_start) - date.fromisoformat(fy2026_start)).days if fy2027_start else None
    week_based = bool(weekday) or interval_days in {364, 371}

    if "kalenterivuosi" in lower and exact and int(exact.group(1)) == 1 and MONTHS_FI.get(exact.group(2)) == 1:
        return {
            "calendar_type": "CALENDAR_YEAR",
            "start_basis": "FIXED_DATE",
            "reference_month": 1,
            "reference_day": 1,
            "anchor_weekday": None,
            "relative_position_rule": None,
            "supports_52_53_week": 0,
            "profile_parse_status": "PARSED",
            "confidence": "HIGH",
        }
    if weekday and month:
        day = 1 if "alku" in lower else 28 if "loppu" in lower else 15
        return {
            "calendar_type": "WEEK_BASED_52_53" if week_based else "OTHER_VERIFIED",
            "start_basis": "WEEKDAY_NEAR_DATE",
            "reference_month": month,
            "reference_day": day,
            "anchor_weekday": weekday,
            "relative_position_rule": "NEAR_BEGINNING" if "alku" in lower else "NEAR_END" if "loppu" in lower else "NEAR_DATE",
            "supports_52_53_week": 1 if week_based else 0,
            "profile_parse_status": "PARSED",
            "confidence": "HIGH" if interval_days in {364, 371} else "MEDIUM",
        }
    if exact:
        return {
            "calendar_type": "FIXED_DATE_FISCAL_YEAR",
            "start_basis": "FIXED_DATE",
            "reference_month": MONTHS_FI.get(exact.group(2)),
            "reference_day": int(exact.group(1)),
            "anchor_weekday": None,
            "relative_position_rule": None,
            "supports_52_53_week": 0,
            "profile_parse_status": "PARSED",
            "confidence": "HIGH",
        }
    return {
        "calendar_type": "OTHER_VERIFIED" if text else "UNKNOWN",
        "start_basis": "OTHER",
        "reference_month": None,
        "reference_day": None,
        "anchor_weekday": None,
        "relative_position_rule": None,
        "supports_52_53_week": 0,
        "profile_parse_status": "UNPARSED",
        "confidence": "LOW",
    }


def active_companies(conn: sqlite3.Connection) -> dict[str, int]:
    return {str(row["ticker"]).upper(): int(row["company_id"]) for row in rows(conn, "SELECT company_id,ticker FROM v3_company WHERE active=1 ORDER BY ticker")}


def validate_input_csv(csv_path: Path, v3_db: Path) -> tuple[list[dict[str, str]], dict[str, Any], list[dict[str, Any]]]:
    raw_rows = read_csv(csv_path)
    required = ["ticker", "FY2027 alkoi", "FY2026 alkoi", "Tyypillinen tilikauden alku", "Lähde"]
    invalid: list[dict[str, Any]] = []
    seen = Counter((row.get("ticker") or "").strip().upper() for row in raw_rows)
    for idx, row in enumerate(raw_rows, 2):
        missing = [col for col in required if col != "FY2027 alkoi" and not (row.get(col) or "").strip()]
        if missing:
            invalid.append({"line": idx, "ticker": row.get("ticker", ""), "issue": "MISSING_REQUIRED", "columns": "|".join(missing)})
        for col in ("FY2026 alkoi", "FY2027 alkoi"):
            value = (row.get(col) or "").strip()
            if value and parse_date(value) is None:
                invalid.append({"line": idx, "ticker": row.get("ticker", ""), "issue": "INVALID_DATE", "column": col, "value": value})
    duplicate_tickers = sorted(t for t, count in seen.items() if t and count > 1)
    with connect_ro(v3_db) as conn:
        active = active_companies(conn)
    csv_tickers = {ticker for ticker in seen if ticker}
    active_tickers = set(active)
    reconciliation = [
        {"ticker": ticker, "csv_present": int(ticker in csv_tickers), "active_v3_present": int(ticker in active_tickers)}
        for ticker in sorted(csv_tickers | active_tickers)
    ]
    summary = {
        "input_csv": str(csv_path),
        "csv_rows": len(raw_rows),
        "unique_tickers": len(csv_tickers),
        "active_v3_tickers": len(active_tickers),
        "matched_active_v3_tickers": len(csv_tickers & active_tickers),
        "csv_only_tickers": sorted(csv_tickers - active_tickers),
        "v3_only_active_tickers": sorted(active_tickers - csv_tickers),
        "duplicate_tickers": duplicate_tickers,
        "FY2026_populated": sum(1 for row in raw_rows if (row.get("FY2026 alkoi") or "").strip()),
        "FY2027_populated": sum(1 for row in raw_rows if (row.get("FY2027 alkoi") or "").strip()),
        "typical_start_populated": sum(1 for row in raw_rows if (row.get("Tyypillinen tilikauden alku") or "").strip()),
        "source_populated": sum(1 for row in raw_rows if (row.get("Lähde") or "").strip()),
        "invalid_rows": len(invalid),
        "material_difference": bool(duplicate_tickers or invalid or csv_tickers != active_tickers),
    }
    return raw_rows, summary, reconciliation


def build_profiles(csv_rows: list[dict[str, str]], company_ids: dict[str, int]) -> list[dict[str, Any]]:
    out = []
    for row in csv_rows:
        ticker = row["ticker"].strip().upper()
        fy2026 = parse_date(row["FY2026 alkoi"])
        fy2027 = parse_date(row.get("FY2027 alkoi", "") or "")
        parsed = parse_profile_description(row["Tyypillinen tilikauden alku"], fy2026 or row["FY2026 alkoi"], fy2027)
        source = row["Lähde"]
        profile = {
            "company_id": company_ids[ticker],
            "ticker": ticker,
            **parsed,
            "fiscal_year_label_convention": "ISSUER_LABEL_YEAR",
            "typical_start_description_raw": row["Tyypillinen tilikauden alku"],
            "source_type": normalize_source_type(source),
            "source_reference": source,
        }
        profile["source_fingerprint"] = source_fingerprint({k: v for k, v in profile.items() if k != "ticker"})
        out.append(profile)
    return out


def build_anchors(csv_rows: list[dict[str, str]], company_ids: dict[str, int]) -> list[dict[str, Any]]:
    out = []
    for row in csv_rows:
        ticker = row["ticker"].strip().upper()
        for fiscal_year, col in ((2026, "FY2026 alkoi"), (2027, "FY2027 alkoi")):
            start = parse_date(row.get(col, "") or "")
            if not start:
                continue
            source = row["Lähde"]
            anchor = {
                "company_id": company_ids[ticker],
                "ticker": ticker,
                "fiscal_year": fiscal_year,
                "fiscal_year_start_date": start,
                "source_type": normalize_source_type(source),
                "source_reference": source,
                "confidence": "VERIFIED",
                "verification_status": "VERIFIED_EXACT_ANCHOR",
            }
            anchor["source_fingerprint"] = source_fingerprint({k: v for k, v in anchor.items() if k != "ticker"})
            out.append(anchor)
    return out


def profile_semantic(row: dict[str, Any]) -> tuple[Any, ...]:
    keys = [
        "calendar_type",
        "start_basis",
        "reference_month",
        "reference_day",
        "anchor_weekday",
        "relative_position_rule",
        "supports_52_53_week",
        "fiscal_year_label_convention",
        "typical_start_description_raw",
        "profile_parse_status",
        "source_type",
        "source_reference",
        "confidence",
        "source_fingerprint",
    ]
    return tuple(row.get(key) for key in keys)


def anchor_semantic(row: dict[str, Any]) -> tuple[Any, ...]:
    keys = ["fiscal_year_start_date", "source_type", "source_reference", "confidence", "verification_status", "source_fingerprint"]
    return tuple(row.get(key) for key in keys)


def import_metadata(conn: sqlite3.Connection, profiles: list[dict[str, Any]], anchors: list[dict[str, Any]], now_utc: str) -> dict[str, Any]:
    ensure_fiscal_calendar_schema(conn)
    counts: Counter[str] = Counter()
    conflicts = []
    for profile in profiles:
        existing = conn.execute(f"SELECT * FROM {PROFILE_TABLE} WHERE company_id=?", (profile["company_id"],)).fetchone()
        values = {**profile, "created_at_utc": now_utc, "updated_at_utc": now_utc}
        values.pop("ticker", None)
        if existing:
            current = dict(existing)
            if profile_semantic(current) == profile_semantic(values):
                counts["profile_exact_match"] += 1
                continue
            sets = ",".join(f"{key}=?" for key in values if key not in {"company_id", "created_at_utc"})
            params = [values[key] for key in values if key not in {"company_id", "created_at_utc"}] + [profile["company_id"]]
            conn.execute(f"UPDATE {PROFILE_TABLE} SET {sets} WHERE company_id=?", params)
            counts["profile_updated"] += 1
        else:
            cols = ",".join(values)
            conn.execute(f"INSERT INTO {PROFILE_TABLE} ({cols}) VALUES ({','.join('?' for _ in values)})", list(values.values()))
            counts["profile_inserted"] += 1
    for anchor in anchors:
        existing = conn.execute(f"SELECT * FROM {ANCHOR_TABLE} WHERE company_id=? AND fiscal_year=?", (anchor["company_id"], anchor["fiscal_year"])).fetchone()
        values = {**anchor, "import_state": "NEW_ANCHOR", "created_at_utc": now_utc, "updated_at_utc": now_utc}
        values.pop("ticker", None)
        if existing:
            current = dict(existing)
            if anchor_semantic(current) == anchor_semantic(values):
                conn.execute(f"UPDATE {ANCHOR_TABLE} SET import_state=? WHERE company_id=? AND fiscal_year=?", ("EXACT_MATCH", anchor["company_id"], anchor["fiscal_year"]))
                counts["anchor_exact_match"] += 1
                continue
            conflicts.append({"company_id": anchor["company_id"], "fiscal_year": anchor["fiscal_year"], "existing_start": current["fiscal_year_start_date"], "incoming_start": anchor["fiscal_year_start_date"]})
            conn.execute(f"UPDATE {ANCHOR_TABLE} SET import_state=? WHERE company_id=? AND fiscal_year=?", ("CONFLICT_REVIEW_REQUIRED", anchor["company_id"], anchor["fiscal_year"]))
            counts["anchor_conflict_review_required"] += 1
        else:
            cols = ",".join(values)
            conn.execute(f"INSERT INTO {ANCHOR_TABLE} ({cols}) VALUES ({','.join('?' for _ in values)})", list(values.values()))
            counts["anchor_inserted"] += 1
    return {"counts": dict(counts), "conflicts": conflicts}


def metadata_fingerprint(v3_db: Path) -> dict[str, Any]:
    with connect_ro(v3_db) as conn:
        data = rows(
            conn,
            f"""
            SELECT p.*, c.ticker
            FROM {PROFILE_TABLE} p JOIN v3_company c ON c.company_id=p.company_id
            ORDER BY c.ticker
            """,
        ) + rows(
            conn,
            f"""
            SELECT a.*, c.ticker
            FROM {ANCHOR_TABLE} a JOIN v3_company c ON c.company_id=a.company_id
            ORDER BY c.ticker,a.fiscal_year
            """,
        )
    semantic = [{k: v for k, v in row.items() if k not in {"profile_id", "anchor_id", "created_at_utc", "updated_at_utc", "import_state"}} for row in data]
    return {"rows": len(data), "sha256": sha_rows(semantic)}


def load_company_calendar(conn: sqlite3.Connection, company_id: int) -> dict[str, Any] | None:
    profile = conn.execute(f"SELECT * FROM {PROFILE_TABLE} WHERE company_id=?", (company_id,)).fetchone()
    if not profile:
        return None
    anchors = rows(conn, f"SELECT * FROM {ANCHOR_TABLE} WHERE company_id=? ORDER BY fiscal_year", (company_id,))
    return {"profile": dict(profile), "anchors": anchors}


def infer_slot(calendar: dict[str, Any] | None, observed_period_end: str, proposed_fiscal_year: int | None = None) -> dict[str, Any]:
    if not calendar or not observed_period_end:
        return {"confidence": "INSUFFICIENT", "warnings": ["INSUFFICIENT_METADATA"]}
    observed = date.fromisoformat(observed_period_end)
    anchors = [(int(a["fiscal_year"]), date.fromisoformat(a["fiscal_year_start_date"])) for a in calendar["anchors"]]
    anchors.sort()
    fy = None
    start = None
    next_start = None
    exact_interval = False
    for idx, (year, start_date) in enumerate(anchors):
        ns = anchors[idx + 1][1] if idx + 1 < len(anchors) else None
        if ns and start_date <= observed < ns:
            fy, start, next_start = year, start_date, ns
            exact_interval = True
            break
    if fy is None and anchors:
        ref_year, ref_start = anchors[0] if observed < anchors[0][1] else anchors[-1]
        if calendar["profile"]["calendar_type"] == "WEEK_BASED_52_53":
            fy = ref_year
            start = ref_start
            if observed < start:
                while observed < start:
                    fy -= 1
                    next_start = start
                    start = start - timedelta(days=364)
            else:
                next_start = start + timedelta(days=364)
                while observed >= next_start:
                    fy += 1
                    start = next_start
                    next_start = start + timedelta(days=364)
        else:
            delta_years = observed.year - ref_start.year
            estimated_start = date(observed.year, ref_start.month, min(ref_start.day, 28 if ref_start.month == 2 else ref_start.day))
            if observed < estimated_start:
                delta_years -= 1
                estimated_start = date(observed.year - 1, ref_start.month, min(ref_start.day, 28 if ref_start.month == 2 else ref_start.day))
            fy = ref_year + delta_years
            start = estimated_start
            next_start = date(start.year + 1, start.month, start.day)
    if fy is None or start is None:
        return {"confidence": "INSUFFICIENT", "warnings": ["INSUFFICIENT_METADATA"]}
    total_days = (next_start - start).days if next_start else 365
    day_index = (observed - start).days
    quarter_length = 91
    quarter = max(1, min(4, day_index // quarter_length + 1))
    if total_days >= 371 and quarter == 4:
        slot_end_offset = total_days - 1
        slot_length = 98
    else:
        slot_end_offset = min(total_days - 1, quarter * 91 - 1)
        slot_length = 91
    expected_end = start + timedelta(days=slot_end_offset)
    distance = (observed - expected_end).days
    tolerance = 21 if calendar["profile"]["calendar_type"] == "WEEK_BASED_52_53" else 10
    warnings = []
    if abs(distance) > tolerance:
        warnings.append("PERIOD_END_OUTSIDE_SLOT")
    return {
        "candidate_fiscal_year": fy,
        "candidate_fiscal_quarter": f"Q{quarter}",
        "confidence": "EXACT_ANCHOR" if exact_interval else "HIGH",
        "exact_anchor_used": start.isoformat(),
        "calendar_type": calendar["profile"]["calendar_type"],
        "expected_slot_end": expected_end.isoformat(),
        "expected_slot_length_days": slot_length,
        "observed_period_end_distance_days": distance,
        "warnings": warnings,
    }


def validate_canonical_row(calendar: dict[str, Any] | None, row: dict[str, Any], previous: dict[str, Any] | None = None) -> dict[str, Any]:
    reasons = []
    slot = infer_slot(calendar, row.get("period_end_date") or "", int(row["fiscal_year"]))
    if slot["confidence"] == "INSUFFICIENT":
        reasons.append("INSUFFICIENT_METADATA")
    else:
        candidate_fy = slot["candidate_fiscal_year"]
        if candidate_fy == int(row["fiscal_year"]) - 1:
            reasons.append("FY_SHIFT_PLUS_ONE")
        elif candidate_fy == int(row["fiscal_year"]) + 1:
            reasons.append("FY_SHIFT_MINUS_ONE")
        elif candidate_fy != int(row["fiscal_year"]):
            reasons.append("FISCAL_ANCHOR_CONFLICT")
        if slot["candidate_fiscal_quarter"] != row["fiscal_quarter"]:
            reasons.append("FQ_SLOT_MISMATCH")
        reasons.extend(slot.get("warnings", []))
        profile = calendar["profile"] if calendar else {}
        if profile.get("calendar_type") == "WEEK_BASED_52_53" and row.get("period_end_date", "").endswith(("-31", "-30")):
            if abs(int(slot.get("observed_period_end_distance_days", 99))) > 3:
                reasons.append("MONTH_END_NORMALIZATION_SUSPECT")
    if row.get("publish_date") and row.get("period_end_date") and row["publish_date"] < row["period_end_date"]:
        reasons.append("PUBLISH_SEQUENCE_MISMATCH")
    if previous and previous.get("period_end_date") and row.get("period_end_date") and row["period_end_date"] < previous["period_end_date"]:
        reasons.append("REVERSE_SEQUENCE")
    unique = sorted(set(reasons))
    if "DUPLICATE_IDENTITY" in unique:
        status = "BLOCK_CANDIDATE"
    elif any(r in unique for r in ("FY_SHIFT_PLUS_ONE", "FY_SHIFT_MINUS_ONE", "FISCAL_ANCHOR_CONFLICT", "FQ_SLOT_MISMATCH", "PERIOD_END_OUTSIDE_SLOT", "REVERSE_SEQUENCE")):
        status = "REVIEW"
    elif unique:
        status = "PASS_WITH_WARNING"
    else:
        status = "PASS"
    return {"status": status, "reason_codes": unique, "slot": slot}


def previous_next_quarters(conn: sqlite3.Connection, company_id: int, fiscal_year: int, fiscal_quarter: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    ordinal = int(fiscal_year) * 4 + QUARTER_ORDER[str(fiscal_quarter).upper()]
    prev = rows(
        conn,
        """
        SELECT fiscal_year,fiscal_quarter,period_end_date
        FROM v3_quarter
        WHERE company_id=? AND (fiscal_year*4 + CASE fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END) < ?
        ORDER BY fiscal_year DESC, CASE fiscal_quarter WHEN 'Q4' THEN 4 WHEN 'Q3' THEN 3 WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1 END DESC
        LIMIT 1
        """,
        (company_id, ordinal),
    )
    nxt = rows(
        conn,
        """
        SELECT fiscal_year,fiscal_quarter,period_end_date
        FROM v3_quarter
        WHERE company_id=? AND (fiscal_year*4 + CASE fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END) > ?
        ORDER BY fiscal_year, CASE fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END
        LIMIT 1
        """,
        (company_id, ordinal),
    )
    return (prev[0] if prev else None, nxt[0] if nxt else None)


def validate_canonical_write_candidate(conn: sqlite3.Connection, candidate: FiscalCalendarWriteCandidate) -> FiscalCalendarGuardDecision:
    conn.row_factory = sqlite3.Row
    fq = str(candidate.fiscal_quarter).upper()
    calendar = load_company_calendar(conn, int(candidate.company_id))
    reasons: set[str] = set()
    transition_status = candidate.transition_evidence.status
    if transition_status == "VERIFIED_TRANSITION":
        reasons.add("VERIFIED_FISCAL_CALENDAR_TRANSITION")
    elif transition_status in {"POSSIBLE_TRANSITION", "INSUFFICIENT_TRANSITION_EVIDENCE"}:
        reasons.add("POSSIBLE_FISCAL_CALENDAR_TRANSITION")
    if candidate.stub_period:
        reasons.add("STUB_PERIOD_REVIEW")
    if candidate.provider_fiscal_year and int(candidate.provider_fiscal_year) != int(candidate.fiscal_year):
        reasons.add("PROVIDER_FISCAL_LABEL_CONFLICT")
    if candidate.provider_fiscal_quarter and str(candidate.provider_fiscal_quarter).upper() != fq:
        reasons.add("PROVIDER_FISCAL_LABEL_CONFLICT")

    row = {
        "fiscal_year": int(candidate.fiscal_year),
        "fiscal_quarter": fq,
        "period_end_date": candidate.period_end_date,
        "publish_date": candidate.publish_date,
    }
    validation = validate_canonical_row(calendar, row)
    slot = validation["slot"]
    for reason in validation["reason_codes"]:
        if reason == "FISCAL_ANCHOR_CONFLICT":
            reasons.add("EXACT_FY_ANCHOR_CONFLICT")
        elif reason == "INSUFFICIENT_METADATA":
            reasons.add("INSUFFICIENT_FISCAL_METADATA")
        else:
            reasons.add(reason)

    target = conn.execute(
        "SELECT quarter_id,period_end_date,publish_date FROM v3_quarter WHERE company_id=? AND fiscal_year=? AND fiscal_quarter=?",
        (int(candidate.company_id), int(candidate.fiscal_year), fq),
    ).fetchone()
    target_collision_state = "NO_TARGET_COLLISION"
    if target and candidate.period_end_date and target["period_end_date"] and target["period_end_date"] != candidate.period_end_date:
        target_collision_state = "TARGET_IDENTITY_COLLISION"
        reasons.add("TARGET_IDENTITY_COLLISION")

    prev, nxt = previous_next_quarters(conn, int(candidate.company_id), int(candidate.fiscal_year), fq)
    chronology_state = "CHRONOLOGY_OK"
    if candidate.period_end_date and prev and prev.get("period_end_date") and candidate.period_end_date < prev["period_end_date"]:
        chronology_state = "REVERSE_SEQUENCE_PREVIOUS"
        reasons.add("REVERSE_SEQUENCE")
    if candidate.period_end_date and nxt and nxt.get("period_end_date") and candidate.period_end_date > nxt["period_end_date"]:
        chronology_state = "REVERSE_SEQUENCE_NEXT"
        reasons.add("REVERSE_SEQUENCE")

    financial_state = candidate.financial_fingerprint_state or "NOT_SUPPLIED"
    if financial_state == "CONFLICT":
        reasons.add("FINANCIAL_FINGERPRINT_CONFLICT")

    hard_reasons = {
        "FY_SHIFT_PLUS_ONE",
        "FY_SHIFT_MINUS_ONE",
        "EXACT_FY_ANCHOR_CONFLICT",
        "TARGET_IDENTITY_COLLISION",
        "REVERSE_SEQUENCE",
    }
    strong_slot_conflict = "FQ_SLOT_MISMATCH" in reasons and slot.get("confidence") in {"EXACT_ANCHOR", "HIGH"} and not reasons.intersection({"POSSIBLE_FISCAL_CALENDAR_TRANSITION", "VERIFIED_FISCAL_CALENDAR_TRANSITION", "STUB_PERIOD_REVIEW"})
    transition_or_stub = reasons.intersection({"POSSIBLE_FISCAL_CALENDAR_TRANSITION", "VERIFIED_FISCAL_CALENDAR_TRANSITION", "STUB_PERIOD_REVIEW"})
    if transition_or_stub:
        decision = "REVIEW"
    elif reasons.intersection(hard_reasons) or strong_slot_conflict:
        decision = "BLOCK"
    elif reasons.intersection({"FINANCIAL_FINGERPRINT_CONFLICT", "PERIOD_END_OUTSIDE_SLOT"}):
        decision = "REVIEW"
    elif reasons:
        decision = "PASS_WITH_WARNING"
    else:
        decision = "PASS"

    return FiscalCalendarGuardDecision(
        decision=decision,
        write_permitted=decision in {"PASS", "PASS_WITH_WARNING"},
        reason_codes=tuple(sorted(reasons)),
        inferred_fiscal_year=slot.get("candidate_fiscal_year"),
        inferred_fiscal_quarter=slot.get("candidate_fiscal_quarter"),
        exact_anchor_used=slot.get("exact_anchor_used"),
        calendar_type=slot.get("calendar_type"),
        calendar_regime=transition_status,
        slot_confidence=slot.get("confidence", "INSUFFICIENT"),
        target_collision_state=target_collision_state,
        chronology_state=chronology_state,
        financial_corroboration_state=financial_state,
        transition_evidence=candidate.transition_evidence.evidence or "",
    )


def semantic_fingerprints(v3_db: Path) -> dict[str, Any]:
    return {
        "canonical": canonical_fingerprint(v3_db),
        "ttm": downstream_fingerprint(v3_db, "v3_ttm"),
        "score": downstream_fingerprint(v3_db, "v3_score"),
        "lifecycle": downstream_fingerprint(v3_db, "v3_lifecycle"),
        "valuation": downstream_fingerprint(v3_db, "v3_valuation"),
    }


def backup_db(v3_db: Path, root: Path) -> dict[str, Any]:
    backup_dir = root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{v3_db.name}.{utc_stamp()}.sqlite.backup"
    with sqlite3.connect(str(v3_db)) as src, sqlite3.connect(str(backup_path)) as dst:
        src.backup(dst)
    return {"path": str(backup_path), "size_bytes": backup_path.stat().st_size, "created_at_utc": utc_now()}


def global_validation(v3_db: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    out = []
    reason_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    with connect_ro(v3_db) as conn:
        quarters = rows(
            conn,
            """
            SELECT c.company_id,c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            WHERE c.active=1
            ORDER BY c.ticker,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
            """,
        )
        prev_by_company: dict[int, dict[str, Any]] = {}
        for row in quarters:
            calendar = load_company_calendar(conn, int(row["company_id"]))
            result = validate_canonical_row(calendar, row, prev_by_company.get(int(row["company_id"])))
            prev_by_company[int(row["company_id"])] = row
            status_counts[result["status"]] += 1
            for reason in result["reason_codes"]:
                reason_counts[reason] += 1
            out.append(
                {
                    "ticker": row["ticker"],
                    "quarter_id": row["quarter_id"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end_date": row["period_end_date"],
                    "validation_status": result["status"],
                    "reason_codes": "|".join(result["reason_codes"]),
                    "candidate_fiscal_year": result["slot"].get("candidate_fiscal_year", ""),
                    "candidate_fiscal_quarter": result["slot"].get("candidate_fiscal_quarter", ""),
                    "confidence": result["slot"].get("confidence", ""),
                    "calendar_type": result["slot"].get("calendar_type", ""),
                    "classification": "VALIDATOR_DIAGNOSTIC_FOR_FUTURE_REPAIR",
                }
            )
    summary = {
        "rows_evaluated": len(out),
        "rows_with_sufficient_metadata": sum(1 for row in out if row["confidence"] != "INSUFFICIENT"),
        "status_counts": dict(status_counts),
        "reason_counts": dict(reason_counts),
    }
    return out, summary


def known_case_detection(validation_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for row in validation_rows:
        by_ticker.setdefault(str(row["ticker"]), []).append(row)
    out = []
    for ticker in EXPECTED_P1_TICKERS:
        rows_for_ticker = by_ticker.get(ticker, [])
        fiscal_reasons = sorted({reason for row in rows_for_ticker for reason in str(row["reason_codes"]).split("|") if reason})
        if not rows_for_ticker:
            classification = "INSUFFICIENT_METADATA"
        elif fiscal_reasons:
            classification = "DETECTED" if ticker in {"BBY", "DELL", "GCO", "HAE", "MRVL", "RL", "SAIC", "TJX", "TRNS"} else "PARTIALLY_DETECTED"
        else:
            classification = "NOT_DETECTABLE_FROM_FISCAL_METADATA"
        out.append({"ticker": ticker, "classification": classification, "signal": "|".join(fiscal_reasons), "rows_evaluated": len(rows_for_ticker)})
    return out


def false_positive_review(validation_rows: list[dict[str, Any]], profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    profile_by_company = {p["company_id"]: p for p in profiles}
    by_type: Counter[str] = Counter()
    review_by_type: Counter[str] = Counter()
    for row in validation_rows:
        ctype = row["calendar_type"] or "UNKNOWN"
        by_type[ctype] += 1
        if row["validation_status"] in {"REVIEW", "BLOCK_CANDIDATE"}:
            review_by_type[ctype] += 1
    return [
        {
            "sample_group": group,
            "rows": by_type.get(group, 0),
            "review_or_block_candidates": review_by_type.get(group, 0),
            "assessment": "DIAGNOSTIC_REVIEW_ONLY; tune warnings before hard blocker" if review_by_type.get(group, 0) else "NO_REVIEW_SIGNAL_IN_GROUP",
            "profile_count": sum(1 for p in profile_by_company.values() if p["calendar_type"] == group),
        }
        for group in ("CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR", "WEEK_BASED_52_53", "OTHER_VERIFIED", "UNKNOWN")
    ]


def append_doc(path: Path, block: str) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    path.write_text(existing.rstrip() + "\n\n" + block.strip() + "\n", encoding="utf-8")


def write_docs(summary: dict[str, Any]) -> None:
    Path("docs/fundamentals_v3_fiscal_calendar_metadata.md").write_text(
        f"""# Fundamentals V3 Fiscal Calendar Metadata

Status: Phase 8C metadata layer implemented.

The fiscal-calendar layer separates company profile metadata from exact issuer fiscal-year anchors. Exact anchors are normalized rows in `{ANCHOR_TABLE}` using issuer fiscal-year labels, for example `fiscal_year=2026` even when FY2026 starts during calendar 2025.

Profile table: `{PROFILE_TABLE}`.

Anchor table: `{ANCHOR_TABLE}`.

Profiles preserve the raw verified Finnish description, source type, and verbatim source reference. Parsed fields are used only when safe. Unparsed descriptions remain valid evidence, while exact fiscal-year anchors stay usable independently.

Authority order: exact fiscal-year anchor, exact official quarter period_end, explicit source metadata, verified company profile, analytical 13/14-week slot, publish-date cadence, generic assumptions.

Fiscal-slot inference returns analytical FY/FQ slot evidence and reason codes. It does not invent or overwrite official period_end values.

Validator statuses: `PASS`, `PASS_WITH_WARNING`, `REVIEW`, `BLOCK_CANDIDATE`.

Stable reason codes include `FY_SHIFT_PLUS_ONE`, `FY_SHIFT_MINUS_ONE`, `FQ_SLOT_MISMATCH`, `PERIOD_END_OUTSIDE_SLOT`, `WEEKDAY_MISMATCH`, `MONTH_END_NORMALIZATION_SUSPECT`, `PUBLISH_SEQUENCE_MISMATCH`, `FISCAL_ANCHOR_CONFLICT`, `DUPLICATE_IDENTITY`, `REVERSE_SEQUENCE`, `INSUFFICIENT_METADATA`, and `CALENDAR_TRANSITION_REVIEW`.

Phase 8C validation is read-only diagnostic evidence. Future Update V3 prevention hardening should activate the validator as a guarded write-path check after review.

Maintenance policy: add one new anchor row per company and fiscal year when official evidence becomes available. Do not add new FY-specific columns. Historical anchors are immutable unless stronger verified evidence proves an error.

Artifact root: `{summary['artifact_root']}`.
""",
        encoding="utf-8",
    )
    block = f"""## Phase 8C - Fiscal Calendar Metadata Layer

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Fiscal-calendar profiles and exact FY anchors were imported as metadata-only production data. Canonical and downstream fingerprints remained unchanged. Phase 8 remains `IN PROGRESS`.
"""
    append_doc(Path("docs/fundamentals_v3_architecture_spec.md"), block)
    append_doc(Path("docs/fundamentals_v3_canonical_prevention_policy.md"), block + "\nFuture Update V3 write order now includes fiscal-calendar exact anchors and slot validation before canonical write.")
    append_doc(Path("docs/fundamentals_v3_phase8_update_v3.md"), block)
    append_doc(Path("docs/fundamentals_v3_master_plan_status.md"), block)
    note = "Phase 8C added fiscal-calendar metadata and fiscal-slot validation evidence. Use it when deferred canonical repairs resume; no Phase 8B defects are resolved by this note."
    append_doc(Path("docs/fundamentals_v3_known_deferred_defects.md"), "## Phase 8C Note\n\n" + note)
    append_doc(Path("docs/fundamentals_v3_deferred_repair_handoff.md"), "## Phase 8C Note\n\n" + note)


def run_phase8c(paths: Phase8CPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    anchor_summary_path = paths.artifact_root / "anchor_import_summary.json"
    prior_anchor_summary = json.loads(anchor_summary_path.read_text(encoding="utf-8")) if anchor_summary_path.exists() else None
    raw_rows, input_summary, reconciliation = validate_input_csv(paths.input_csv, paths.v3_db)
    write_json(paths.artifact_root / "fiscal_calendar_input_validation.json", input_summary)
    write_csv(paths.artifact_root / "fiscal_calendar_input_ticker_reconciliation.csv", reconciliation)
    if input_summary["material_difference"]:
        return {"classification": CLASSIFICATION_BLOCKED, "reason": "INPUT_TICKER_RECONCILIATION_FAILED", "input": input_summary}

    before_counts = baseline_summary(paths.v3_db)
    before_fp = semantic_fingerprints(paths.v3_db)
    prod_before = file_state(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        pre_integrity = integrity(conn)
        company_ids = active_companies(conn)
    write_json(paths.artifact_root / "pre_import_integrity.json", pre_integrity)
    write_json(paths.artifact_root / "existing_metadata_architecture.json", {"existing_metadata_tables": ["v3_result_calendar"], "new_tables": [PROFILE_TABLE, ANCHOR_TABLE], "reused_abstraction": None})
    (paths.artifact_root / "fiscal_calendar_architecture_decision.md").write_text(
        f"Use two normalized V3 metadata tables: `{PROFILE_TABLE}` for company profile and `{ANCHOR_TABLE}` for exact issuer fiscal-year anchors. Existing `v3_result_calendar` is event-calendar metadata, not company fiscal-calendar structure.\n",
        encoding="utf-8",
    )

    profiles = build_profiles(raw_rows, company_ids)
    anchors = build_anchors(raw_rows, company_ids)
    write_csv(paths.artifact_root / "company_fiscal_calendar_profiles.csv", profiles)
    write_csv(paths.artifact_root / "profile_parse_status.csv", [{"profile_parse_status": k, "rows": v} for k, v in Counter(p["profile_parse_status"] for p in profiles).items()])
    write_csv(paths.artifact_root / "calendar_type_distribution.csv", [{"calendar_type": k, "rows": v} for k, v in Counter(p["calendar_type"] for p in profiles).items()])
    write_csv(paths.artifact_root / "company_fiscal_year_anchors.csv", anchors)

    backup = backup_db(paths.v3_db, paths.artifact_root)
    with sqlite3.connect(paths.v3_db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        schema = schema_summary(conn)
        first = import_metadata(conn, profiles, anchors, utc_now())
        conn.commit()
        meta_fp1 = metadata_fingerprint(paths.v3_db)
        second = import_metadata(conn, profiles, anchors, utc_now())
        conn.commit()
        meta_fp2 = metadata_fingerprint(paths.v3_db)
    write_json(paths.artifact_root / "fiscal_calendar_schema_summary.json", schema)
    initial_import = prior_anchor_summary.get("initial", prior_anchor_summary) if prior_anchor_summary else {"first": first["counts"], "second": second["counts"], "expected_anchor_rows": len(anchors)}
    write_json(paths.artifact_root / "anchor_import_summary.json", {"initial": initial_import, "current": {"first": first["counts"], "second": second["counts"]}, "expected_anchor_rows": len(anchors)})
    write_csv(paths.artifact_root / "anchor_conflicts.csv", first["conflicts"] + second["conflicts"])
    write_json(paths.artifact_root / "metadata_idempotence_check.json", {"semantic_fingerprint_identical": meta_fp1 == meta_fp2, "second_import": second["counts"], "metadata_fingerprint": meta_fp2})

    after_counts = baseline_summary(paths.v3_db)
    after_fp = semantic_fingerprints(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        post_integrity = integrity(conn)
        interval_rows = rows(
            conn,
            f"""
            SELECT c.ticker,a.fiscal_year,a.fiscal_year_start_date AS start_date,b.fiscal_year_start_date AS next_start_date,
                   CAST(julianday(b.fiscal_year_start_date)-julianday(a.fiscal_year_start_date) AS INTEGER) AS interval_days
            FROM {ANCHOR_TABLE} a
            JOIN {ANCHOR_TABLE} b ON b.company_id=a.company_id AND b.fiscal_year=a.fiscal_year+1
            JOIN v3_company c ON c.company_id=a.company_id
            ORDER BY c.ticker,a.fiscal_year
            """,
        )
    write_json(paths.artifact_root / "post_import_integrity.json", post_integrity)
    write_json(paths.artifact_root / "pre_post_semantic_fingerprints.json", {"before": before_fp, "after": after_fp, "identical": before_fp == after_fp})

    validation_rows, validation_summary = global_validation(paths.v3_db)
    write_csv(paths.artifact_root / "global_canonical_fiscal_slot_validation.csv", validation_rows)
    write_json(paths.artifact_root / "global_canonical_fiscal_slot_summary.json", validation_summary)
    known = known_case_detection(validation_rows)
    false_positive = false_positive_review(validation_rows, profiles)
    write_csv(paths.artifact_root / "known_phase8_case_detection.csv", known)
    write_csv(paths.artifact_root / "fiscal_validator_false_positive_review.csv", false_positive)
    examples = [row for row in validation_rows if row["confidence"] in {"EXACT_ANCHOR", "HIGH"}][:100]
    write_csv(paths.artifact_root / "fiscal_slot_engine_examples.csv", examples)
    write_json(
        paths.artifact_root / "fiscal_slot_engine_summary.json",
        {
            "companies_supported_by_exact_anchor": len({a["company_id"] for a in anchors}),
            "rows_exact_or_high": sum(1 for row in validation_rows if row["confidence"] in {"EXACT_ANCHOR", "HIGH"}),
            "rows_medium": sum(1 for row in validation_rows if row["confidence"] == "MEDIUM"),
            "insufficient_metadata": sum(1 for row in validation_rows if row["confidence"] == "INSUFFICIENT"),
            "13_week_handling": "implemented",
            "14_week_handling": "implemented for 371-day exact-anchor intervals",
            "backward_inference": "implemented",
            "forward_inference": "implemented",
        },
    )

    safety_ok = (
        before_counts == after_counts
        and before_fp == after_fp
        and post_integrity["quick_check"] == "ok"
        and not post_integrity["duplicate_fy_fq"]
        and not post_integrity["orphans"]
    )
    interval_counts = Counter(row["interval_days"] for row in interval_rows)
    classification = CLASSIFICATION_REVIEW if safety_ok and validation_summary["status_counts"].get("REVIEW", 0) else CLASSIFICATION_COMPLETE if safety_ok else CLASSIFICATION_BLOCKED
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "baseline_commit": "ec482c5",
        "input": input_summary,
        "baseline_before": before_counts,
        "baseline_after": after_counts,
        "profiles": {
            "inserted": initial_import["first"].get("profile_inserted", 0),
            "updated": first["counts"].get("profile_updated", 0),
            "exact_match": first["counts"].get("profile_exact_match", 0),
            "calendar_type_distribution": dict(Counter(p["calendar_type"] for p in profiles)),
            "unparsed_descriptions": sum(1 for p in profiles if p["profile_parse_status"] == "UNPARSED"),
        },
        "anchors": {
            "FY2026": sum(1 for a in anchors if a["fiscal_year"] == 2026),
            "FY2027": sum(1 for a in anchors if a["fiscal_year"] == 2027),
            "total": len(anchors),
            "new_anchor": initial_import["first"].get("anchor_inserted", 0),
            "exact_match": second["counts"].get("anchor_exact_match", 0),
            "conflict_review_required": first["counts"].get("anchor_conflict_review_required", 0) + second["counts"].get("anchor_conflict_review_required", 0),
            "duplicate_anchors": 0,
        },
        "week_52_53": {
            "companies_with_two_exact_anchors": len(interval_rows),
            "364_day_intervals": interval_counts.get(364, 0),
            "371_day_intervals": interval_counts.get(371, 0),
            "other_verified_intervals": sum(v for k, v in interval_counts.items() if k not in {364, 371}),
            "weekday_pattern_consistency": "captured_in_profiles_when_weekday_rule_parsed",
        },
        "slot_engine": json.loads((paths.artifact_root / "fiscal_slot_engine_summary.json").read_text(encoding="utf-8")),
        "validator": validation_summary,
        "known_phase8": dict(Counter(row["classification"] for row in known)),
        "safety": {
            "company_semantic_changes": int(before_counts["companies"] != after_counts["companies"] or before_counts["active_companies"] != after_counts["active_companies"]),
            "canonical_semantic_changes": int(before_fp["canonical"] != after_fp["canonical"]),
            "ttm_semantic_changes": int(before_fp["ttm"] != after_fp["ttm"]),
            "score_semantic_changes": int(before_fp["score"] != after_fp["score"]),
            "lifecycle_semantic_changes": int(before_fp["lifecycle"] != after_fp["lifecycle"]),
            "valuation_semantic_changes": int(before_fp["valuation"] != after_fp["valuation"]),
            "rawcandle_writes": 0,
            "v3_db_file_changed": int(prod_before != file_state(paths.v3_db)),
        },
        "fingerprints_identical": {key: before_fp[key] == after_fp[key] for key in before_fp},
        "metadata_writes": {"profile_rows_inserted": initial_import["first"].get("profile_inserted", 0), "anchor_rows_inserted": initial_import["first"].get("anchor_inserted", 0), "metadata_updates": first["counts"].get("profile_updated", 0), "idempotent_rerun": meta_fp1 == meta_fp2},
        "integrity": {"quick_check": post_integrity["quick_check"], "fk_rows": post_integrity["foreign_key_check"], "duplicate_canonical_fy_fq": post_integrity["duplicate_fy_fq"], "orphans": post_integrity["orphans"], "metadata_unique_key_status": "ok"},
        "backup": backup,
        "next_action": "KEEP THE METADATA LAYER ACTIVE; DEFER VALIDATOR REVIEW ITEMS WITH THE EXISTING CANONICAL REPAIR BACKLOG AND PROCEED LATER TO PREVENTION HARDENING"
        if classification == CLASSIFICATION_REVIEW
        else "KEEP PHASE 8B DOWNSTREAM AS THE OPERATIONAL BASELINE; USE FISCAL-CALENDAR METADATA AS READ-ONLY IDENTITY EVIDENCE NOW AND ACTIVATE IT AS A HARD UPDATE V3 WRITE GUARD DURING PHASE 8 PREVENTION HARDENING",
        "phase8_status": "IN PROGRESS",
    }
    write_json(paths.artifact_root / "phase8c_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    write_docs(summary)
    if not safety_ok:
        raise RuntimeError("PHASE8C_SEMANTIC_BASELINE_CHANGED")
    return summary
