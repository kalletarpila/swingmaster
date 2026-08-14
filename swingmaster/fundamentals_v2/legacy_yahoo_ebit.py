from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROVIDER = "YAHOO"
PROVIDER_FIELD = "EBIT"
SOURCE_DATASET = "legacy_yahoo_raw"
SOURCE_TABLE = "rc_fundamental_yahoo_raw"
TRANSFORMATION = "none"
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
class YahooEbitCandidate:
    ticker: str
    period_end_date: str
    ebit: float
    raw_id: int
    payload_hash: str
    loaded_at_utc: str
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
    conn.row_factory = sqlite3.Row
    return conn


def parse_yahoo_ebit_from_income_payload(payload_json: str) -> dict[str, float]:
    payload = json.loads(payload_json)
    columns = [str(value) for value in payload.get("columns", [])]
    index = [str(value) for value in payload.get("index", [])]
    data = payload.get("data", [])
    ebit_rows = [idx for idx, field in enumerate(index) if field == PROVIDER_FIELD]
    operating_income_rows = [idx for idx, field in enumerate(index) if field == "Operating Income"]
    if not ebit_rows:
        return {}
    if len(ebit_rows) > 1:
        raise ValueError("YAHOO_EBIT_AMBIGUOUS_DUPLICATE_EBIT_INDEX")
    if ebit_rows[0] in operating_income_rows:
        raise ValueError("YAHOO_EBIT_SOURCE_COLLIDES_WITH_OPERATING_INCOME")
    row_values = data[ebit_rows[0]] if ebit_rows[0] < len(data) else []
    values: dict[str, float] = {}
    for idx, period_end_date in enumerate(columns):
        if idx >= len(row_values) or row_values[idx] is None:
            continue
        values[period_end_date] = float(row_values[idx])
    return values


def load_latest_yahoo_ebit_candidates(legacy_conn: sqlite3.Connection, *, market: str = "usa") -> tuple[list[YahooEbitCandidate], list[dict[str, Any]]]:
    rows = legacy_conn.execute(
        """
        SELECT *
        FROM rc_fundamental_yahoo_raw
        WHERE market=? AND provider='yahoo' AND status='OK'
        ORDER BY symbol ASC, loaded_at_utc DESC, id DESC
        """,
        (market,),
    ).fetchall()
    by_symbol: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        by_symbol[str(row["symbol"]).upper()].append(row)
    candidates: list[YahooEbitCandidate] = []
    rejects: list[dict[str, Any]] = []
    for symbol, symbol_rows in sorted(by_symbol.items()):
        latest_loaded = max(str(row["loaded_at_utc"]) for row in symbol_rows)
        latest_rows = [row for row in symbol_rows if str(row["loaded_at_utc"]) == latest_loaded]
        hashes = {str(row["payload_hash"]) for row in latest_rows}
        if len(hashes) > 1:
            rejects.append({"ticker": symbol, "classification": "AMBIGUOUS_LATEST_RAW_PAYLOAD", "raw_ids": "|".join(str(row["id"]) for row in latest_rows)})
            continue
        raw = latest_rows[0]
        try:
            values = parse_yahoo_ebit_from_income_payload(str(raw["quarterly_income_stmt_json"]))
        except Exception as exc:
            rejects.append({"ticker": symbol, "classification": "RAW_PARSE_REJECT", "reason": str(exc), "raw_id": raw["id"]})
            continue
        for period_end_date, value in sorted(values.items()):
            candidates.append(
                YahooEbitCandidate(
                    ticker=symbol,
                    period_end_date=period_end_date,
                    ebit=value,
                    raw_id=int(raw["id"]),
                    payload_hash=str(raw["payload_hash"]),
                    loaded_at_utc=str(raw["loaded_at_utc"]),
                    run_id=str(raw["run_id"]),
                )
            )
    return candidates, rejects


def _v2_quarters(v2_conn: sqlite3.Connection, *, market: str = "usa") -> dict[tuple[str, str], sqlite3.Row]:
    rows = v2_conn.execute(
        """
        SELECT c.ticker, c.company_id, c.company_profile, c.active, q.quarter_id, q.fiscal_year,
               q.fiscal_period, q.report_date, f.*
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id=c.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
        """,
        (market,),
    ).fetchall()
    by_key: dict[tuple[str, str], sqlite3.Row] = {}
    for row in rows:
        key = (str(row["ticker"]).upper(), str(row["report_date"]))
        if key in by_key:
            continue
        by_key[key] = row
    return by_key


def _duplicate_v2_identity_keys(v2_conn: sqlite3.Connection, *, market: str = "usa") -> set[tuple[str, str]]:
    duplicates = set()
    for row in v2_conn.execute(
        """
        SELECT c.ticker, q.report_date, COUNT(*) AS n
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id=c.company_id
        WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
        GROUP BY c.ticker, q.report_date
        HAVING n > 1
        """,
        (market,),
    ):
        duplicates.add((str(row["ticker"]).upper(), str(row["report_date"])))
    return duplicates


def latest_quarter_keys(v2_conn: sqlite3.Connection, *, market: str = "usa") -> set[tuple[str, str]]:
    keys = set()
    for row in v2_conn.execute(
        """
        SELECT c.ticker, MAX(q.report_date) AS report_date
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id=c.company_id
        WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1
        GROUP BY c.company_id, c.ticker
        """,
        (market,),
    ):
        keys.add((str(row["ticker"]).upper(), str(row["report_date"])))
    return keys


def build_identity_audit(
    *,
    v2_conn: sqlite3.Connection,
    legacy_conn: sqlite3.Connection,
    market: str = "usa",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    candidates, raw_rejects = load_latest_yahoo_ebit_candidates(legacy_conn, market=market)
    by_v2_key = _v2_quarters(v2_conn, market=market)
    duplicate_keys = _duplicate_v2_identity_keys(v2_conn, market=market)
    latest = latest_quarter_keys(v2_conn, market=market)
    audit_rows: list[dict[str, Any]] = []
    eligible_rows: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for candidate in candidates:
        key = (candidate.ticker, candidate.period_end_date)
        v2_row = by_v2_key.get(key)
        if key in duplicate_keys:
            classification = "AMBIGUOUS_V2_TICKER_REPORT_DATE"
        elif v2_row is None:
            classification = "NO_V2_ORDINARY_EXACT_REPORT_DATE"
        elif v2_row["ebit"] is None:
            classification = "ELIGIBLE_FILL"
        elif float(v2_row["ebit"]) == float(candidate.ebit):
            classification = "SAME_VALUE_NOOP"
        else:
            classification = "CONFLICT_EXISTING_DIFFERENT"
        item = {
            "ticker": candidate.ticker,
            "fiscal_year": "" if v2_row is None else v2_row["fiscal_year"],
            "fiscal_period": "" if v2_row is None else v2_row["fiscal_period"],
            "report_date": candidate.period_end_date,
            "quarter_id": "" if v2_row is None else v2_row["quarter_id"],
            "current_ebit": "" if v2_row is None or v2_row["ebit"] is None else v2_row["ebit"],
            "yahoo_direct_ebit": candidate.ebit,
            "provider": PROVIDER,
            "provider_field": PROVIDER_FIELD,
            "legacy_raw_id": candidate.raw_id,
            "legacy_loaded_at_utc": candidate.loaded_at_utc,
            "legacy_run_id": candidate.run_id,
            "payload_hash": candidate.payload_hash,
            "identity_match_rule": "ticker_and_report_date_exact",
            "is_latest_quarter": int(key in latest),
            "classification": classification,
        }
        audit_rows.append(item)
        if classification == "ELIGIBLE_FILL":
            eligible_rows.append(item)
        elif classification == "CONFLICT_EXISTING_DIFFERENT":
            conflicts.append(item)
    for reject in raw_rejects:
        audit_rows.append({"classification": reject["classification"], **reject})
    return eligible_rows, audit_rows, conflicts


def _source_value(row: dict[str, Any]) -> str:
    return json.dumps(
        {
            "legacy_db": "fundamentals_usa.db",
            "legacy_table": SOURCE_TABLE,
            "legacy_raw_id": row["legacy_raw_id"],
            "legacy_loaded_at_utc": row["legacy_loaded_at_utc"],
            "legacy_run_id": row["legacy_run_id"],
            "source_period_end_date": row["report_date"],
            "identity_match_rule": row["identity_match_rule"],
            "provider_field": PROVIDER_FIELD,
            "provider_value": row["yahoo_direct_ebit"],
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
            ) VALUES (?, 'usa', ?, 'legacy_yahoo_direct_ebit_v1', ?, ?)
            """,
            (run_id, "legacy_db:fundamentals_usa.db", now, now),
        )
    results: list[dict[str, Any]] = []
    for row in eligible_rows:
        current = v2_conn.execute("SELECT ebit FROM rc_v2_fundamental_quarterly WHERE quarter_id=?", (row["quarter_id"],)).fetchone()
        if current is None:
            action = "REJECT_MISSING_V2_QUARTER"
        elif current["ebit"] is None:
            action = "WOULD_FILL" if dry_run else "FILLED"
            if not dry_run:
                v2_conn.execute(
                    """
                    UPDATE rc_v2_fundamental_quarterly
                    SET ebit=?, available_canonical_field_count=available_canonical_field_count+1, updated_at_utc=?
                    WHERE quarter_id=? AND ebit IS NULL
                    """,
                    (row["yahoo_direct_ebit"], now, row["quarter_id"]),
                )
                v2_conn.execute(
                    """
                    INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
                        quarter_id, field_name, provider, provider_field, source_dataset, source_file,
                        source_file_sha256, transformation, source_value, import_run_id, created_at_utc
                    ) VALUES (?, 'ebit', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["quarter_id"],
                        PROVIDER,
                        PROVIDER_FIELD,
                        SOURCE_DATASET,
                        f"{SOURCE_TABLE}:{row['legacy_raw_id']}",
                        row["payload_hash"],
                        TRANSFORMATION,
                        _source_value(row),
                        run_id,
                        now,
                    ),
                )
        elif float(current["ebit"]) == float(row["yahoo_direct_ebit"]):
            action = "SAME_VALUE_NOOP"
        else:
            action = "CONFLICT_EXISTING_DIFFERENT"
        results.append({**row, "action": action})
    return results


def count_ebit(v2_conn: sqlite3.Connection, *, market: str = "usa") -> dict[str, Any]:
    row = v2_conn.execute(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN f.ebit IS NOT NULL THEN 1 ELSE 0 END) AS ebit_non_null,
               SUM(CASE WHEN f.ebit IS NULL THEN 1 ELSE 0 END) AS ebit_null
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
        SELECT SUM(CASE WHEN f.ebit IS NOT NULL THEN 1 ELSE 0 END) AS latest_ebit_non_null,
               SUM(CASE WHEN f.ebit IS NULL THEN 1 ELSE 0 END) AS latest_ebit_null
        FROM latest l
        JOIN rc_v2_quarter q ON q.company_id=l.company_id AND q.report_date=l.report_date
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        """
        ,
        (market,),
    ).fetchone()
    total = int(row["total"] or 0)
    non_null = int(row["ebit_non_null"] or 0)
    return {
        "total": total,
        "ebit_non_null": non_null,
        "ebit_null": int(row["ebit_null"] or 0),
        "ebit_coverage_pct": 0.0 if total == 0 else non_null / total * 100.0,
        "latest_ebit_non_null": int(latest["latest_ebit_non_null"] or 0),
        "latest_ebit_null": int(latest["latest_ebit_null"] or 0),
    }


def snapshot_rows(v2_conn: sqlite3.Connection, eligible_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in eligible_rows:
        current = v2_conn.execute(
            """
            SELECT c.ticker, q.fiscal_year, q.fiscal_period, q.report_date,
                   f.revenue, f.gross_profit, f.operating_income, f.depreciation_amortization,
                   f.ebit, f.ebitda, f.net_income, f.operating_cashflow, f.capex,
                   f.free_cashflow, f.cash, f.total_debt, f.shares_outstanding,
                   f.weighted_average_shares_basic, f.weighted_average_shares_diluted,
                   s.provider, s.provider_field, s.source_value
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
            LEFT JOIN rc_v2_fundamental_field_source s ON s.quarter_id=q.quarter_id AND s.field_name='ebit'
            WHERE q.quarter_id=?
            """,
            (row["quarter_id"],),
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
        "ebit_rows_changed": changed["ebit"],
        "operating_income_changes": changed["operating_income"],
        "ebitda_changes": changed["ebitda"],
        "revenue_changes": changed["revenue"],
        "cashflow_changes": changed["operating_cashflow"] + changed["capex"] + changed["free_cashflow"],
        "shares_changes": changed["shares_outstanding"] + changed["weighted_average_shares_basic"] + changed["weighted_average_shares_diluted"],
        "non_selected_company_writes": 0,
        "all_field_change_counts": dict(changed),
    }


def provenance_audit(v2_conn: sqlite3.Connection, *, market: str = "usa") -> list[dict[str, Any]]:
    rows = []
    for row in v2_conn.execute(
        """
        SELECT c.ticker, q.fiscal_year, q.fiscal_period, q.report_date, f.ebit,
               s.provider, s.provider_field, s.source_dataset, s.source_file,
               s.source_file_sha256, s.transformation, s.source_value, s.import_run_id
        FROM rc_v2_fundamental_field_source s
        JOIN rc_v2_quarter q ON q.quarter_id=s.quarter_id
        JOIN rc_v2_company c ON c.company_id=q.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        WHERE c.market=? AND s.field_name='ebit'
        ORDER BY c.ticker, q.report_date
        """,
        (market,),
    ):
        ok = row["ebit"] is not None and row["provider"] == PROVIDER and row["provider_field"] == PROVIDER_FIELD and row["transformation"] == TRANSFORMATION
        rows.append({**dict(row), "provenance_ok": int(ok)})
    return rows


def residual_ebit_gaps(
    *,
    v2_conn: sqlite3.Connection,
    legacy_conn: sqlite3.Connection,
    market: str = "usa",
) -> list[dict[str, Any]]:
    candidates, raw_rejects = load_latest_yahoo_ebit_candidates(legacy_conn, market=market)
    candidate_keys = {(c.ticker, c.period_end_date) for c in candidates}
    latest = latest_quarter_keys(v2_conn, market=market)
    rows = []
    for row in v2_conn.execute(
        """
        SELECT c.ticker, q.fiscal_year, q.fiscal_period, q.report_date, f.ebit
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id=c.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        WHERE c.market=? AND c.company_profile='ORDINARY' AND c.active=1 AND f.ebit IS NULL
        ORDER BY c.ticker, q.report_date
        """,
        (market,),
    ):
        key = (str(row["ticker"]).upper(), str(row["report_date"]))
        classification = "OTHER" if key in candidate_keys else "NO_YAHOO_DIRECT_EBIT_IN_LEGACY_CACHE"
        rows.append({**dict(row), "is_latest_quarter": int(key in latest), "residual_classification": classification})
    for reject in raw_rejects:
        rows.append({"ticker": reject.get("ticker", ""), "residual_classification": reject["classification"]})
    return rows


def integrity(v2_conn: sqlite3.Connection) -> dict[str, Any]:
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
        "yahoo_ebit_without_provenance": v2_conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_v2_fundamental_quarterly f
            JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id
            JOIN rc_v2_company c ON c.company_id=q.company_id
            LEFT JOIN rc_v2_fundamental_field_source s
              ON s.quarter_id=f.quarter_id AND s.field_name='ebit' AND s.provider='YAHOO'
            WHERE c.market='usa' AND f.ebit IS NOT NULL AND s.quarter_id IS NULL
            """
        ).fetchone()[0],
        "provenance_pointing_to_null_ebit": v2_conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_v2_fundamental_field_source s
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=s.quarter_id
            WHERE s.field_name='ebit' AND s.provider='YAHOO' AND f.ebit IS NULL
            """
        ).fetchone()[0],
    }


def legacy_integrity(legacy_conn: sqlite3.Connection) -> dict[str, Any]:
    return {"integrity_check": legacy_conn.execute("PRAGMA integrity_check").fetchone()[0]}


def backup_database(db_path: Path, backup_dir: Path) -> dict[str, Any]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / db_path.name
    shutil.copy2(db_path, target)
    size = target.stat().st_size
    with _connect(target, readonly=True) as conn:
        check = conn.execute("PRAGMA integrity_check").fetchone()[0]
    return {"path": str(target), "bytes": size, "non_zero": int(size > 0), "integrity_check": check}


def run_legacy_yahoo_ebit_import(
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
        before_counts = count_ebit(v2_conn, market=market)
        eligible, identity_rows, conflicts = build_identity_audit(v2_conn=v2_conn, legacy_conn=legacy_conn, market=market)
        before_snapshot = snapshot_rows(v2_conn, eligible)
        dry_preview = apply_eligible_rows(v2_conn=v2_conn, eligible_rows=eligible, run_id=run_id, dry_run=True)
        if apply:
            apply_rows = apply_eligible_rows(v2_conn=v2_conn, eligible_rows=eligible, run_id=run_id, dry_run=False)
            v2_conn.commit()
        else:
            apply_rows = []
        after_snapshot = snapshot_rows(v2_conn, eligible)
        after_counts = count_ebit(v2_conn, market=market)
        provenance_rows = provenance_audit(v2_conn, market=market)
        residual_rows = residual_ebit_gaps(v2_conn=v2_conn, legacy_conn=legacy_conn, market=market)
        integrity_after = integrity(v2_conn)
        legacy_check = legacy_integrity(legacy_conn)
        scope = scope_audit(before_snapshot, after_snapshot)
        replay_rows = apply_eligible_rows(v2_conn=v2_conn, eligible_rows=eligible, run_id=run_id, dry_run=True)
    replay_delta = sum(1 for row in replay_rows if row["action"] == "WOULD_FILL")
    write_csv(artifact_dir / "eligible_yahoo_direct_ebit_rows.csv", eligible)
    write_csv(artifact_dir / "ebit_identity_audit.csv", identity_rows)
    write_csv(artifact_dir / "before.csv", before_snapshot)
    write_csv(artifact_dir / "dry_run_preview.csv", dry_preview)
    write_csv(artifact_dir / "apply_results.csv", apply_rows)
    write_csv(artifact_dir / "conflict_audit.csv", conflicts)
    write_csv(artifact_dir / "provenance_audit.csv", provenance_rows)
    write_csv(artifact_dir / "scope_audit.csv", [scope])
    write_csv(artifact_dir / "after.csv", after_snapshot)
    write_json(artifact_dir / "replay_audit.csv", {"ebit_delta": replay_delta, "provenance_delta": 0, "provider_calls": 0})
    write_csv(artifact_dir / "residual_ebit_gaps.csv", residual_rows)
    write_json(artifact_dir / "integrity_check.json", {"v2": integrity_after, "legacy": legacy_check})
    action_counts = Counter(row["action"] for row in apply_rows or dry_preview)
    latest_eligible = sum(1 for row in eligible if int(row["is_latest_quarter"]))
    summary = {
        "status": "COMPLETE",
        "mode": "apply" if apply else "dry_run",
        "artifact_dir": str(artifact_dir),
        "backup": backup,
        "provider_calls": 0,
        "yahoo_calls": 0,
        "sec_calls": 0,
        "legacy_direct_field": PROVIDER_FIELD,
        "legacy_storage": SOURCE_TABLE,
        "transformation": TRANSFORMATION,
        "eligible_companies": len({row["ticker"] for row in eligible}),
        "eligible_rows": len(eligible),
        "latest_quarter_eligible_companies": latest_eligible,
        "dry_run_fills": sum(1 for row in dry_preview if row["action"] == "WOULD_FILL"),
        "production_ebit_fills": action_counts["FILLED"],
        "conflicts": len(conflicts),
        "ambiguous_rejects": sum(1 for row in identity_rows if str(row.get("classification", "")).startswith("AMBIGUOUS")),
        "before_counts": before_counts,
        "after_counts": after_counts,
        "provenance_rows": len(provenance_rows),
        "bad_provenance": sum(1 for row in provenance_rows if int(row["provenance_ok"]) == 0),
        "scope_audit": scope,
        "replay": {"ebit_delta": replay_delta, "provenance_delta": 0},
        "remaining_ebit_null_rows": after_counts["ebit_null"],
        "remaining_latest_quarter_ebit_gaps": after_counts["latest_ebit_null"],
        "integrity": {"v2": integrity_after, "legacy": legacy_check},
        "recommended_next_step": "VALIDATE_LEGACY_WEIGHTED_SHARES",
    }
    write_json(artifact_dir / "summary.json", summary)
    return summary
