from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any


SCORECARD_ENGINE_VERSION = "RISK_APPETITE_SCORECARD_V1"
NORMALIZED_TABLE = "macro_source_daily"
SCORE_TABLE = "rc_risk_appetite_daily"

SOURCE_CODES = (
    "BTC_USD_CBBTCUSD",
    "HY_OAS_BAMLH0A0HYM2",
    "FED_WALCL",
    "USD_BROAD_DTWEXBGS",
    "PCR_EQUITY_CBOE",
)

REGIME_LABELS = ("RISK_OFF", "DEFENSIVE", "NEUTRAL", "RISK_ON", "EUPHORIC")
QUALITY_STATUS_OK = "OK"
QUALITY_STATUS_PARTIAL = "PARTIAL_FORWARD_FILL"
QUALITY_STATUS_MISSING = "MISSING_COMPONENT"
QUALITY_STATUS_INVALID = "INVALID_SOURCE_VALUE"


@dataclass(frozen=True)
class ScorecardRow:
    as_of_date: str
    btc_ref_5d: float | None
    btc_ma90: float | None
    btc_mom: float | None
    bitcoin_score: float | None
    hy_spread_5d: float | None
    credit_score: float | None
    pcr_10d: float | None
    pcr_score: float | None
    walcl_latest: float | None
    walcl_13w_ago: float | None
    liquidity_change_13w: float | None
    liquidity_score: float | None
    dxy_ref_5d: float | None
    dxy_ma200: float | None
    dxy_diff: float | None
    dxy_score: float | None
    risk_score_raw: float | None
    risk_score_final: float | None
    regime_label: str | None
    regime_label_confirmed: str | None
    data_quality_status: str
    component_count: int
    run_id: str
    created_at_utc: str


@dataclass(frozen=True)
class ScorecardSummary:
    date_from: str
    date_to: str
    mode: str
    normalized_rows_scanned: int
    score_rows_inserted: int
    score_rows_updated: int
    score_rows_deleted: int
    score_rows_skipped: int
    valid_rows_published: int
    missing_component_rows: int
    summary_status: str


def _validate_iso_date(value: str, arg_name: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"INVALID_{arg_name}") from exc
    return parsed.isoformat()


def _validate_mode(mode: str) -> str:
    if mode not in {"upsert", "replace-all", "insert-missing"}:
        raise ValueError("INVALID_MODE")
    return mode


def _default_created_at_utc(date_to: str) -> str:
    return f"{date_to}T00:00:00+00:00"


def _compute_run_id(date_from: str, date_to: str, mode: str) -> str:
    return f"{SCORECARD_ENGINE_VERSION}_{date_from.replace('-', '')}_{date_to.replace('-', '')}_{mode}"


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _iter_calendar_days(date_from: str, date_to: str) -> list[str]:
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    tail = values[-length:]
    return sum(tail) / float(length)


def _bucket_bitcoin(mom: float) -> float:
    if mom > 0.40:
        return 100.0
    if mom > 0.25:
        return 80.0
    if mom > 0.10:
        return 60.0
    if mom > 0.00:
        return 50.0
    if mom > -0.10:
        return 40.0
    return 20.0


def _bucket_credit(spread_5d: float) -> float:
    if spread_5d < 3.5:
        return 100.0
    if spread_5d < 4.5:
        return 80.0
    if spread_5d < 5.5:
        return 60.0
    if spread_5d < 7.0:
        return 40.0
    if spread_5d < 9.0:
        return 20.0
    return 0.0


def _bucket_pcr(pcr_10d: float) -> float:
    if pcr_10d < 0.60:
        return 90.0
    if pcr_10d < 0.80:
        return 70.0
    if pcr_10d < 1.00:
        return 50.0
    if pcr_10d < 1.20:
        return 30.0
    return 10.0


def _bucket_liquidity(change_13w: float) -> float:
    if change_13w > 0.05:
        return 100.0
    if change_13w > 0.02:
        return 80.0
    if change_13w > 0.00:
        return 60.0
    if change_13w > -0.02:
        return 40.0
    if change_13w > -0.05:
        return 20.0
    return 0.0


def _bucket_dxy(diff: float) -> float:
    if diff < -0.05:
        return 100.0
    if diff < -0.02:
        return 80.0
    if diff < 0.00:
        return 60.0
    if diff < 0.03:
        return 40.0
    return 20.0


def _map_regime(score: float) -> str:
    if score < 30.0:
        return "RISK_OFF"
    if score < 45.0:
        return "DEFENSIVE"
    if score < 60.0:
        return "NEUTRAL"
    if score < 75.0:
        return "RISK_ON"
    return "EUPHORIC"


def _confirm_regime(
    candidate_today: str,
    candidate_yday: str | None,
    confirmed_yday: str | None,
) -> str | None:
    if candidate_today == confirmed_yday:
        return confirmed_yday
    if candidate_yday is not None and candidate_today == candidate_yday:
        return candidate_today
    return confirmed_yday


def _load_normalized_rows(
    conn: Any,
    *,
    date_from: str,
    date_to: str,
) -> tuple[int, dict[str, list[tuple[str, float, int]]]]:
    need_back_days = 220
    min_needed = (date.fromisoformat(date_from) - timedelta(days=need_back_days)).isoformat()
    placeholders = ",".join("?" for _ in SOURCE_CODES)
    rows = conn.execute(
        f"""
        SELECT as_of_date, source_code, source_value
             , is_forward_filled
        FROM {NORMALIZED_TABLE}
        WHERE source_code IN ({placeholders})
          AND as_of_date >= ?
          AND as_of_date <= ?
        ORDER BY source_code, as_of_date
        """,
        (*SOURCE_CODES, min_needed, date_to),
    ).fetchall()
    out: dict[str, list[tuple[str, float, int]]] = {k: [] for k in SOURCE_CODES}
    for as_of_date, source_code, source_value, is_forward_filled in rows:
        out[str(source_code)].append((str(as_of_date), float(source_value), int(is_forward_filled)))
    return len(rows), out


def _latest_on_or_before(rows: list[tuple[str, float, int]], target_day: str) -> tuple[float, int] | None:
    target = date.fromisoformat(target_day)
    latest: tuple[float, int] | None = None
    for as_of, value, is_forward_filled in rows:
        day = date.fromisoformat(as_of)
        if day <= target:
            latest = (value, is_forward_filled)
        else:
            break
    return latest


def _values_on_or_before(rows: list[tuple[str, float, int]], target_day: str) -> list[tuple[float, int]]:
    target = date.fromisoformat(target_day)
    out: list[tuple[float, int]] = []
    for as_of, value, is_forward_filled in rows:
        if date.fromisoformat(as_of) <= target:
            out.append((value, is_forward_filled))
        else:
            break
    return out


def _safe_ratio(num: float, den: float) -> float | None:
    if den <= 0.0:
        return None
    return (num / den) - 1.0


def _build_score_rows(
    *,
    date_from: str,
    date_to: str,
    created_at_utc: str,
    run_id: str,
    normalized_by_source: dict[str, list[tuple[str, float, int]]],
    prev_regime_label: str | None,
    prev_regime_confirmed: str | None,
) -> list[ScorecardRow]:
    days = _iter_calendar_days(date_from, date_to)
    out: list[ScorecardRow] = []
    recent_raw_scores: list[float] = []
    has_valid_row = False
    candidate_prev_day = prev_regime_label
    confirmed_prev_day = prev_regime_confirmed

    for as_of in days:
        invalid = False
        missing = False

        used_forward_fill = False

        btc_rows = _values_on_or_before(normalized_by_source["BTC_USD_CBBTCUSD"], as_of)
        btc_ref_5d = _sma([v for v, _ in btc_rows], 5)
        btc_ma90 = _sma([v for v, _ in btc_rows], 90)
        btc_mom = None if btc_ref_5d is None or btc_ma90 is None else _safe_ratio(btc_ref_5d, btc_ma90)
        if btc_ref_5d is None or btc_ma90 is None or btc_mom is None:
            missing = True
            if btc_ma90 is not None and btc_ma90 <= 0.0:
                invalid = True
        else:
            used_forward_fill = used_forward_fill or any(int(flag) == 1 for _, flag in btc_rows[-5:])
            used_forward_fill = used_forward_fill or any(int(flag) == 1 for _, flag in btc_rows[-90:])
        bitcoin_score = None if btc_mom is None else _bucket_bitcoin(btc_mom)

        hy_rows = _values_on_or_before(normalized_by_source["HY_OAS_BAMLH0A0HYM2"], as_of)
        hy_spread_5d = _sma([v for v, _ in hy_rows], 5)
        if hy_spread_5d is None:
            missing = True
        else:
            used_forward_fill = used_forward_fill or any(int(flag) == 1 for _, flag in hy_rows[-5:])
        credit_score = None if hy_spread_5d is None else _bucket_credit(hy_spread_5d)

        pcr_rows = _values_on_or_before(normalized_by_source["PCR_EQUITY_CBOE"], as_of)
        pcr_10d = _sma([v for v, _ in pcr_rows], 10)
        if pcr_10d is None:
            missing = True
        else:
            used_forward_fill = used_forward_fill or any(int(flag) == 1 for _, flag in pcr_rows[-10:])
        pcr_score = None if pcr_10d is None else _bucket_pcr(pcr_10d)

        walcl_latest_row = _latest_on_or_before(normalized_by_source["FED_WALCL"], as_of)
        lag_day = (date.fromisoformat(as_of) - timedelta(days=91)).isoformat()
        walcl_13w_ago_row = _latest_on_or_before(normalized_by_source["FED_WALCL"], lag_day)
        walcl_latest = None if walcl_latest_row is None else float(walcl_latest_row[0])
        walcl_13w_ago = None if walcl_13w_ago_row is None else float(walcl_13w_ago_row[0])
        liquidity_change_13w = (
            None if walcl_latest is None or walcl_13w_ago is None else _safe_ratio(walcl_latest, walcl_13w_ago)
        )
        if walcl_latest is None or walcl_13w_ago is None or liquidity_change_13w is None:
            missing = True
            if walcl_13w_ago is not None and walcl_13w_ago <= 0.0:
                invalid = True
        else:
            used_forward_fill = used_forward_fill or int(walcl_latest_row[1]) == 1  # type: ignore[index]
            used_forward_fill = used_forward_fill or int(walcl_13w_ago_row[1]) == 1  # type: ignore[index]
        liquidity_score = None if liquidity_change_13w is None else _bucket_liquidity(liquidity_change_13w)

        dxy_rows = _values_on_or_before(normalized_by_source["USD_BROAD_DTWEXBGS"], as_of)
        dxy_ref_5d = _sma([v for v, _ in dxy_rows], 5)
        dxy_ma200 = _sma([v for v, _ in dxy_rows], 200)
        dxy_diff = None if dxy_ref_5d is None or dxy_ma200 is None else _safe_ratio(dxy_ref_5d, dxy_ma200)
        if dxy_ref_5d is None or dxy_ma200 is None or dxy_diff is None:
            missing = True
            if dxy_ma200 is not None and dxy_ma200 <= 0.0:
                invalid = True
        else:
            used_forward_fill = used_forward_fill or any(int(flag) == 1 for _, flag in dxy_rows[-5:])
            used_forward_fill = used_forward_fill or any(int(flag) == 1 for _, flag in dxy_rows[-200:])
        dxy_score = None if dxy_diff is None else _bucket_dxy(dxy_diff)

        component_scores = [bitcoin_score, credit_score, pcr_score, liquidity_score, dxy_score]
        component_count = sum(1 for v in component_scores if v is not None)

        risk_score_raw: float | None = None
        risk_score_final: float | None = None
        regime_label: str | None = None
        regime_confirmed: str | None = None
        data_quality_status = QUALITY_STATUS_OK

        if invalid:
            data_quality_status = QUALITY_STATUS_INVALID
            recent_raw_scores = []
        elif missing or component_count < 5:
            data_quality_status = QUALITY_STATUS_MISSING
            recent_raw_scores = []
        else:
            risk_score_raw = round(
                (0.30 * float(bitcoin_score))
                + (0.25 * float(credit_score))
                + (0.15 * float(pcr_score))
                + (0.15 * float(liquidity_score))
                + (0.15 * float(dxy_score)),
                2,
            )
            recent_raw_scores.append(risk_score_raw)
            if len(recent_raw_scores) > 3:
                recent_raw_scores = recent_raw_scores[-3:]
            if len(recent_raw_scores) == 3:
                risk_score_final = round(sum(recent_raw_scores) / 3.0, 2)
                regime_label = _map_regime(risk_score_final)
                if not has_valid_row:
                    regime_confirmed = regime_label
                    has_valid_row = True
                else:
                    regime_confirmed = _confirm_regime(regime_label, candidate_prev_day, confirmed_prev_day)
                data_quality_status = QUALITY_STATUS_PARTIAL if used_forward_fill else QUALITY_STATUS_OK
            else:
                data_quality_status = QUALITY_STATUS_MISSING

        candidate_prev_day = regime_label
        regime_confirmed_for_row = regime_confirmed if risk_score_final is not None else None
        confirmed_prev_day = regime_confirmed_for_row

        out.append(
            ScorecardRow(
                as_of_date=as_of,
                btc_ref_5d=btc_ref_5d,
                btc_ma90=btc_ma90,
                btc_mom=btc_mom,
                bitcoin_score=bitcoin_score,
                hy_spread_5d=hy_spread_5d,
                credit_score=credit_score,
                pcr_10d=pcr_10d,
                pcr_score=pcr_score,
                walcl_latest=walcl_latest,
                walcl_13w_ago=walcl_13w_ago,
                liquidity_change_13w=liquidity_change_13w,
                liquidity_score=liquidity_score,
                dxy_ref_5d=dxy_ref_5d,
                dxy_ma200=dxy_ma200,
                dxy_diff=dxy_diff,
                dxy_score=dxy_score,
                risk_score_raw=risk_score_raw,
                risk_score_final=risk_score_final,
                regime_label=regime_label,
                regime_label_confirmed=regime_confirmed_for_row,
                data_quality_status=data_quality_status,
                component_count=component_count,
                run_id=run_id,
                created_at_utc=created_at_utc,
            )
        )
    return out


def _load_previous_regime_state(
    conn: Any,
    *,
    date_from: str,
) -> tuple[str | None, str | None]:
    prev_day = (date.fromisoformat(date_from) - timedelta(days=1)).isoformat()
    row = conn.execute(
        f"""
        SELECT regime_label, regime_label_confirmed
        FROM {SCORE_TABLE}
        WHERE as_of_date = ?
        """,
        (prev_day,),
    ).fetchone()
    if row is None:
        return None, None
    return (None if row[0] is None else str(row[0]), None if row[1] is None else str(row[1]))


def _write_score_rows(
    conn: Any,
    rows: list[ScorecardRow],
    *,
    date_from: str,
    date_to: str,
    mode: str,
) -> tuple[int, int, int, int]:
    score_rows_deleted = 0
    if mode == "replace-all":
        before = conn.total_changes
        conn.execute(
            f"""
            DELETE FROM {SCORE_TABLE}
            WHERE as_of_date >= ?
              AND as_of_date <= ?
            """,
            (date_from, date_to),
        )
        score_rows_deleted = conn.total_changes - before

    existing = conn.execute(
        f"""
        SELECT as_of_date
        FROM {SCORE_TABLE}
        WHERE as_of_date >= ?
          AND as_of_date <= ?
        """,
        (date_from, date_to),
    ).fetchall()
    existing_days = {str(row[0]) for row in existing}

    score_rows_inserted = 0
    score_rows_updated = 0
    score_rows_skipped = 0
    for row in rows:
        payload = (
            row.as_of_date,
            row.btc_ref_5d,
            row.btc_ma90,
            row.btc_mom,
            row.bitcoin_score,
            row.hy_spread_5d,
            row.credit_score,
            row.pcr_10d,
            row.pcr_score,
            row.walcl_latest,
            row.walcl_13w_ago,
            row.liquidity_change_13w,
            row.liquidity_score,
            row.dxy_ref_5d,
            row.dxy_ma200,
            row.dxy_diff,
            row.dxy_score,
            row.risk_score_raw,
            row.risk_score_final,
            row.regime_label,
            row.regime_label_confirmed,
            row.data_quality_status,
            row.component_count,
            row.run_id,
            row.created_at_utc,
        )
        if mode == "insert-missing":
            before = conn.total_changes
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {SCORE_TABLE} (
                  as_of_date, btc_ref_5d, btc_ma90, btc_mom, bitcoin_score,
                  hy_spread_5d, credit_score, pcr_10d, pcr_score,
                  walcl_latest, walcl_13w_ago, liquidity_change_13w, liquidity_score,
                  dxy_ref_5d, dxy_ma200, dxy_diff, dxy_score,
                  risk_score_raw, risk_score_final, regime_label, regime_label_confirmed,
                  data_quality_status, component_count, run_id, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            if conn.total_changes > before:
                score_rows_inserted += 1
            else:
                score_rows_skipped += 1
            continue

        conn.execute(
            f"""
            INSERT INTO {SCORE_TABLE} (
              as_of_date, btc_ref_5d, btc_ma90, btc_mom, bitcoin_score,
              hy_spread_5d, credit_score, pcr_10d, pcr_score,
              walcl_latest, walcl_13w_ago, liquidity_change_13w, liquidity_score,
              dxy_ref_5d, dxy_ma200, dxy_diff, dxy_score,
              risk_score_raw, risk_score_final, regime_label, regime_label_confirmed,
              data_quality_status, component_count, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(as_of_date) DO UPDATE SET
              btc_ref_5d=excluded.btc_ref_5d,
              btc_ma90=excluded.btc_ma90,
              btc_mom=excluded.btc_mom,
              bitcoin_score=excluded.bitcoin_score,
              hy_spread_5d=excluded.hy_spread_5d,
              credit_score=excluded.credit_score,
              pcr_10d=excluded.pcr_10d,
              pcr_score=excluded.pcr_score,
              walcl_latest=excluded.walcl_latest,
              walcl_13w_ago=excluded.walcl_13w_ago,
              liquidity_change_13w=excluded.liquidity_change_13w,
              liquidity_score=excluded.liquidity_score,
              dxy_ref_5d=excluded.dxy_ref_5d,
              dxy_ma200=excluded.dxy_ma200,
              dxy_diff=excluded.dxy_diff,
              dxy_score=excluded.dxy_score,
              risk_score_raw=excluded.risk_score_raw,
              risk_score_final=excluded.risk_score_final,
              regime_label=excluded.regime_label,
              regime_label_confirmed=excluded.regime_label_confirmed,
              data_quality_status=excluded.data_quality_status,
              component_count=excluded.component_count,
              run_id=excluded.run_id,
              created_at_utc=excluded.created_at_utc
            """,
            payload,
        )
        if row.as_of_date in existing_days:
            score_rows_updated += 1
        else:
            score_rows_inserted += 1
            existing_days.add(row.as_of_date)

    conn.commit()
    return score_rows_inserted, score_rows_updated, score_rows_deleted, score_rows_skipped


def compute_and_store_risk_appetite_scorecard(
    conn: Any,
    *,
    date_from: str,
    date_to: str,
    mode: str,
    computed_at: str | None = None,
) -> ScorecardSummary:
    date_from = _validate_iso_date(date_from, "DATE_FROM")
    date_to = _validate_iso_date(date_to, "DATE_TO")
    if date_from > date_to:
        raise ValueError("INVALID_DATE_RANGE")
    mode = _validate_mode(mode)

    if not _table_exists(conn, NORMALIZED_TABLE):
        raise RuntimeError("MACRO_SOURCE_DAILY_TABLE_MISSING_RUN_MIGRATIONS")
    if not _table_exists(conn, SCORE_TABLE):
        raise RuntimeError("RISK_APPETITE_TABLE_MISSING_RUN_MIGRATIONS")

    created_at_utc = computed_at or _default_created_at_utc(date_to)
    run_id = _compute_run_id(date_from, date_to, mode)
    normalized_rows_scanned, by_source = _load_normalized_rows(conn, date_from=date_from, date_to=date_to)
    prev_label, prev_confirmed = _load_previous_regime_state(conn, date_from=date_from)
    rows = _build_score_rows(
        date_from=date_from,
        date_to=date_to,
        created_at_utc=created_at_utc,
        run_id=run_id,
        normalized_by_source=by_source,
        prev_regime_label=prev_label,
        prev_regime_confirmed=prev_confirmed,
    )
    score_rows_inserted, score_rows_updated, score_rows_deleted, score_rows_skipped = _write_score_rows(
        conn,
        rows,
        date_from=date_from,
        date_to=date_to,
        mode=mode,
    )
    valid_rows_published = sum(
        1
        for row in rows
        if row.risk_score_final is not None
        and row.regime_label is not None
        and row.data_quality_status in {QUALITY_STATUS_OK, QUALITY_STATUS_PARTIAL}
    )
    missing_component_rows = sum(1 for row in rows if row.data_quality_status == QUALITY_STATUS_MISSING)
    return ScorecardSummary(
        date_from=date_from,
        date_to=date_to,
        mode=mode,
        normalized_rows_scanned=normalized_rows_scanned,
        score_rows_inserted=score_rows_inserted,
        score_rows_updated=score_rows_updated,
        score_rows_deleted=score_rows_deleted,
        score_rows_skipped=score_rows_skipped,
        valid_rows_published=valid_rows_published,
        missing_component_rows=missing_component_rows,
        summary_status="OK",
    )
