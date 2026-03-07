from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_OSAKEDATA_DB = "/home/kalle/projects/rawcandle/data/osakedata.db"
DEFAULT_REGIME_VERSION = "REGIME_USA_MA50_MA200_CLOSE_CRASH2_V1"
DEFAULT_CRASH_CONFIRM_DAYS = 2

RC_MARKET_REGIME_DAILY_TABLE = "rc_market_regime_daily"
RC_EPISODE_REGIME_TABLE = "rc_episode_regime"

STATE_BULL = "BULL"
STATE_CRASH = "CRASH_ALERT"
STATE_BEAR = "BEAR"
STATE_SIDEWAYS = "SIDEWAYS"
VALID_STATES = {STATE_BULL, STATE_CRASH, STATE_BEAR, STATE_SIDEWAYS}


@dataclass(frozen=True)
class RegimeSyncSummary:
    rows_daily_source: int
    rows_daily_changed: int
    rows_episode_source: int
    rows_episode_changed: int
    market: str
    regime_version: str
    crash_confirm_days: int


@dataclass(frozen=True)
class _IndexRow:
    trade_date: str
    close: float
    ma50: float
    ma200: float
    state: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _state_from_values(close: float, ma50: float, ma200: float, crash_confirmed: bool) -> str:
    if ma50 > ma200 and close > ma200:
        return STATE_BULL
    if ma50 < ma200:
        return STATE_BEAR
    if ma50 > ma200 and close < ma200 and crash_confirmed:
        return STATE_CRASH
    return STATE_SIDEWAYS


def _load_index_rows(os_conn: sqlite3.Connection, symbol: str, crash_confirm_days: int) -> dict[str, _IndexRow]:
    rows = os_conn.execute(
        """
        WITH base AS (
          SELECT pvm AS trade_date, close
          FROM osakedata
          WHERE osake = ?
          ORDER BY pvm
        ), ma AS (
          SELECT
            trade_date,
            close,
            AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
            AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
            ROW_NUMBER() OVER (ORDER BY trade_date) AS rn
          FROM base
        )
        SELECT trade_date, close, ma50, ma200
        FROM ma
        WHERE rn >= 200
        ORDER BY trade_date
        """,
        (symbol,),
    ).fetchall()

    out: dict[str, _IndexRow] = {}
    consecutive_candidates = 0
    for row in rows:
        trade_date = str(row[0])
        close = float(row[1])
        ma50 = float(row[2])
        ma200 = float(row[3])
        is_candidate = ma50 > ma200 and close < ma200
        if is_candidate:
            consecutive_candidates += 1
        else:
            consecutive_candidates = 0
        crash_confirmed = consecutive_candidates >= crash_confirm_days
        state = _state_from_values(close, ma50, ma200, crash_confirmed)
        out[trade_date] = _IndexRow(
            trade_date=trade_date,
            close=close,
            ma50=ma50,
            ma200=ma200,
            state=state,
        )
    return out


def _combine_state(sp_state: str, ndx_state: str) -> str:
    if sp_state == ndx_state:
        return sp_state
    return STATE_SIDEWAYS


def _ensure_tables_exist(conn: sqlite3.Connection) -> None:
    daily_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (RC_MARKET_REGIME_DAILY_TABLE,),
    ).fetchone()
    ep_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (RC_EPISODE_REGIME_TABLE,),
    ).fetchone()
    if daily_exists is None or ep_exists is None:
        raise RuntimeError(
            "Regime tables missing. Run migrations first (009_rc_market_regime_tables.sql)."
        )


def _build_daily_payload(
    sp_rows: dict[str, _IndexRow],
    ndx_rows: dict[str, _IndexRow],
    *,
    market: str,
    regime_version: str,
    crash_confirm_days: int,
    computed_at: str,
) -> list[tuple[object, ...]]:
    dates = sorted(set(sp_rows).intersection(ndx_rows))
    payload: list[tuple[object, ...]] = []
    for trade_date in dates:
        sp = sp_rows[trade_date]
        ndx = ndx_rows[trade_date]
        combined = _combine_state(sp.state, ndx.state)
        payload.append(
            (
                trade_date,
                market,
                regime_version,
                sp.close,
                sp.ma50,
                sp.ma200,
                sp.state,
                ndx.close,
                ndx.ma50,
                ndx.ma200,
                ndx.state,
                combined,
                int(crash_confirm_days),
                computed_at,
            )
        )
    return payload


def _build_episode_payload(
    conn: sqlite3.Connection,
    regime_by_date: dict[str, tuple[str, str, str]],
    *,
    market: str,
    regime_version: str,
    computed_at: str,
) -> list[tuple[object, ...]]:
    rows = conn.execute(
        """
        SELECT episode_id, entry_window_date, entry_window_exit_date
        FROM rc_pipeline_episode
        ORDER BY entry_window_date ASC, ticker ASC
        """
    ).fetchall()

    payload: list[tuple[object, ...]] = []
    for row in rows:
        episode_id = str(row[0])
        ew_entry_date = row[1]
        ew_exit_date = row[2]

        entry = regime_by_date.get(str(ew_entry_date)) if ew_entry_date is not None else None
        exit_ = regime_by_date.get(str(ew_exit_date)) if ew_exit_date is not None else None

        payload.append(
            (
                episode_id,
                market,
                regime_version,
                ew_entry_date,
                entry[0] if entry is not None else None,
                entry[1] if entry is not None else None,
                entry[2] if entry is not None else None,
                ew_exit_date,
                exit_[0] if exit_ is not None else None,
                exit_[1] if exit_ is not None else None,
                exit_[2] if exit_ is not None else None,
                computed_at,
            )
        )
    return payload


def _write_daily(
    conn: sqlite3.Connection,
    payload: list[tuple[object, ...]],
    *,
    mode: str,
    market: str,
    regime_version: str,
) -> int:
    before = conn.total_changes
    if mode == "replace-all":
        conn.execute(
            f"DELETE FROM {RC_MARKET_REGIME_DAILY_TABLE} WHERE market=? AND regime_version=?",
            (market, regime_version),
        )

    if mode == "insert-missing":
        conn.executemany(
            f"""
            INSERT OR IGNORE INTO {RC_MARKET_REGIME_DAILY_TABLE} (
              trade_date, market, regime_version,
              sp500_close, sp500_ma50, sp500_ma200, sp500_state,
              ndx_close, ndx_ma50, ndx_ma200, ndx_state,
              regime_combined, crash_confirm_days, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        return conn.total_changes - before

    conn.executemany(
        f"""
        INSERT INTO {RC_MARKET_REGIME_DAILY_TABLE} (
          trade_date, market, regime_version,
          sp500_close, sp500_ma50, sp500_ma200, sp500_state,
          ndx_close, ndx_ma50, ndx_ma200, ndx_state,
          regime_combined, crash_confirm_days, computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(trade_date, market, regime_version) DO UPDATE SET
          sp500_close=excluded.sp500_close,
          sp500_ma50=excluded.sp500_ma50,
          sp500_ma200=excluded.sp500_ma200,
          sp500_state=excluded.sp500_state,
          ndx_close=excluded.ndx_close,
          ndx_ma50=excluded.ndx_ma50,
          ndx_ma200=excluded.ndx_ma200,
          ndx_state=excluded.ndx_state,
          regime_combined=excluded.regime_combined,
          crash_confirm_days=excluded.crash_confirm_days,
          computed_at=excluded.computed_at
        """,
        payload,
    )
    return conn.total_changes - before


def _write_episode(
    conn: sqlite3.Connection,
    payload: list[tuple[object, ...]],
    *,
    mode: str,
    market: str,
    regime_version: str,
) -> int:
    before = conn.total_changes
    if mode == "replace-all":
        conn.execute(
            f"DELETE FROM {RC_EPISODE_REGIME_TABLE} WHERE market=? AND regime_version=?",
            (market, regime_version),
        )

    if mode == "insert-missing":
        conn.executemany(
            f"""
            INSERT OR IGNORE INTO {RC_EPISODE_REGIME_TABLE} (
              episode_id, market, regime_version,
              ew_entry_date, ew_entry_regime_combined, ew_entry_sp500_state, ew_entry_ndx_state,
              ew_exit_date, ew_exit_regime_combined, ew_exit_sp500_state, ew_exit_ndx_state,
              computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        return conn.total_changes - before

    conn.executemany(
        f"""
        INSERT INTO {RC_EPISODE_REGIME_TABLE} (
          episode_id, market, regime_version,
          ew_entry_date, ew_entry_regime_combined, ew_entry_sp500_state, ew_entry_ndx_state,
          ew_exit_date, ew_exit_regime_combined, ew_exit_sp500_state, ew_exit_ndx_state,
          computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(episode_id, regime_version) DO UPDATE SET
          market=excluded.market,
          ew_entry_date=excluded.ew_entry_date,
          ew_entry_regime_combined=excluded.ew_entry_regime_combined,
          ew_entry_sp500_state=excluded.ew_entry_sp500_state,
          ew_entry_ndx_state=excluded.ew_entry_ndx_state,
          ew_exit_date=excluded.ew_exit_date,
          ew_exit_regime_combined=excluded.ew_exit_regime_combined,
          ew_exit_sp500_state=excluded.ew_exit_sp500_state,
          ew_exit_ndx_state=excluded.ew_exit_ndx_state,
          computed_at=excluded.computed_at
        """,
        payload,
    )
    return conn.total_changes - before


def compute_and_store_market_regimes(
    conn: sqlite3.Connection,
    *,
    osakedata_db_path: str = DEFAULT_OSAKEDATA_DB,
    market: str = "usa",
    regime_version: str = DEFAULT_REGIME_VERSION,
    crash_confirm_days: int = DEFAULT_CRASH_CONFIRM_DAYS,
    mode: str = "upsert",
    computed_at: str | None = None,
) -> RegimeSyncSummary:
    if mode not in {"upsert", "replace-all", "insert-missing"}:
        raise ValueError(f"Unsupported mode: {mode}")
    if crash_confirm_days < 2:
        raise ValueError("crash_confirm_days must be >= 2")
    if not osakedata_db_path:
        raise ValueError("osakedata_db_path is required")
    if not Path(osakedata_db_path).exists():
        raise FileNotFoundError(f"osakedata DB not found: {osakedata_db_path}")
    _ensure_tables_exist(conn)

    ts = computed_at or _utc_now_iso()
    os_conn = sqlite3.connect(str(Path(osakedata_db_path)))
    try:
        sp_rows = _load_index_rows(os_conn, "^GSPC", crash_confirm_days)
        ndx_rows = _load_index_rows(os_conn, "^NDX", crash_confirm_days)
    finally:
        os_conn.close()

    daily_payload = _build_daily_payload(
        sp_rows,
        ndx_rows,
        market=market,
        regime_version=regime_version,
        crash_confirm_days=crash_confirm_days,
        computed_at=ts,
    )
    regime_by_date: dict[str, tuple[str, str, str]] = {}
    for row in daily_payload:
        regime_by_date[str(row[0])] = (str(row[11]), str(row[6]), str(row[10]))
    episode_payload = _build_episode_payload(
        conn,
        regime_by_date,
        market=market,
        regime_version=regime_version,
        computed_at=ts,
    )

    conn.execute("BEGIN")
    try:
        rows_daily_changed = _write_daily(
            conn,
            daily_payload,
            mode=mode,
            market=market,
            regime_version=regime_version,
        )
        rows_episode_changed = _write_episode(
            conn,
            episode_payload,
            mode=mode,
            market=market,
            regime_version=regime_version,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return RegimeSyncSummary(
        rows_daily_source=len(daily_payload),
        rows_daily_changed=rows_daily_changed,
        rows_episode_source=len(episode_payload),
        rows_episode_changed=rows_episode_changed,
        market=market,
        regime_version=regime_version,
        crash_confirm_days=crash_confirm_days,
    )
