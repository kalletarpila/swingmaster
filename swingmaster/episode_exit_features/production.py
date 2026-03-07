from __future__ import annotations

import bisect
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev

DEFAULT_OSAKEDATA_DB = "/home/kalle/projects/rawcandle/data/osakedata.db"
DEFAULT_INDEX_SYMBOL = "^GSPC"
DEFAULT_INDEX_SYMBOL_2 = "^NDX"

TABLE_NAME = "rc_episode_exit_features"


@dataclass(frozen=True)
class EpisodeExitFeatureSummary:
    episodes_scanned: int
    inserted: int
    updated: int
    skipped: int
    rows_written: int


@dataclass(frozen=True)
class _Bar:
    pvm: str
    open: float
    high: float
    low: float
    close: float
    volume: float


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return numerator / denominator


def _ma(closes: list[float], window: int, offset: int = 0) -> float | None:
    end = len(closes) - offset
    start = end - window
    if start < 0 or end <= 0:
        return None
    return mean(closes[start:end])


def _ret(closes: list[float], n: int) -> float | None:
    if len(closes) < n + 1:
        return None
    past = closes[-(n + 1)]
    if past == 0.0:
        return None
    return closes[-1] / past - 1.0


def _rolling_high(highs: list[float], window: int) -> float | None:
    if len(highs) < window:
        return None
    return max(highs[-window:])


def _bars_since_window_high(highs: list[float], window: int) -> int | None:
    if len(highs) < window:
        return None
    segment = highs[-window:]
    target = max(segment)
    for idx in range(len(segment) - 1, -1, -1):
        if segment[idx] == target:
            return len(segment) - 1 - idx
    return None


def _daily_returns(closes: list[float], n: int) -> list[float] | None:
    if len(closes) < n + 1:
        return None
    segment = closes[-(n + 1) :]
    out: list[float] = []
    for i in range(1, len(segment)):
        prev = segment[i - 1]
        if prev == 0.0:
            return None
        out.append(segment[i] / prev - 1.0)
    return out


def _volatility(closes: list[float], n: int) -> float | None:
    rets = _daily_returns(closes, n)
    if rets is None:
        return None
    return pstdev(rets)


def _true_range_for_index(bars: list[_Bar], idx: int) -> float | None:
    if idx <= 0:
        return None
    today = bars[idx]
    prev_close = bars[idx - 1].close
    return max(
        today.high - today.low,
        abs(today.high - prev_close),
        abs(today.low - prev_close),
    )


def _atr_pct(bars: list[_Bar], period: int) -> float | None:
    if len(bars) < period + 1:
        return None
    trs: list[float] = []
    start_idx = len(bars) - period
    for i in range(start_idx, len(bars)):
        tr = _true_range_for_index(bars, i)
        if tr is None:
            return None
        trs.append(tr)
    close = bars[-1].close
    if close == 0.0:
        return None
    return mean(trs) / close


def _green_red_counts(closes: list[float], n: int) -> tuple[int, int] | None:
    if len(closes) < n + 1:
        return None
    green = 0
    red = 0
    for i in range(len(closes) - n, len(closes)):
        if closes[i] > closes[i - 1]:
            green += 1
        elif closes[i] < closes[i - 1]:
            red += 1
    return green, red


def _window_volume_ratio(bars: list[_Bar], n: int, direction: str) -> float | None:
    if len(bars) < n + 1:
        return None
    segment = bars[-n:]
    total = sum(float(b.volume) for b in segment)
    if total == 0.0:
        return None
    vol = 0.0
    for i in range(len(bars) - n, len(bars)):
        if direction == "down" and bars[i].close < bars[i - 1].close:
            vol += bars[i].volume
        if direction == "up" and bars[i].close > bars[i - 1].close:
            vol += bars[i].volume
    return vol / total


class _PriceCache:
    def __init__(self, os_conn: sqlite3.Connection) -> None:
        self._conn = os_conn
        self._bars_by_ticker: dict[str, list[_Bar]] = {}
        self._dates_by_ticker: dict[str, list[str]] = {}

    def _load_ticker(self, ticker: str) -> None:
        rows = self._conn.execute(
            """
            SELECT pvm, open, high, low, close, volume
            FROM osakedata
            WHERE osake=?
            ORDER BY pvm ASC
            """,
            (ticker,),
        ).fetchall()
        bars = [
            _Bar(
                pvm=str(r[0]),
                open=float(r[1]),
                high=float(r[2]),
                low=float(r[3]),
                close=float(r[4]),
                volume=float(r[5] if r[5] is not None else 0.0),
            )
            for r in rows
        ]
        self._bars_by_ticker[ticker] = bars
        self._dates_by_ticker[ticker] = [b.pvm for b in bars]

    def bars_as_of(self, ticker: str, as_of_date: str) -> list[_Bar]:
        if ticker not in self._bars_by_ticker:
            self._load_ticker(ticker)
        bars = self._bars_by_ticker[ticker]
        dates = self._dates_by_ticker[ticker]
        end = bisect.bisect_right(dates, as_of_date)
        return bars[:end]

    def has_exact_date(self, ticker: str, as_of_date: str) -> bool:
        if ticker not in self._bars_by_ticker:
            self._load_ticker(ticker)
        dates = self._dates_by_ticker[ticker]
        idx = bisect.bisect_left(dates, as_of_date)
        return idx < len(dates) and dates[idx] == as_of_date

    def ew_age_days(self, ticker: str, entry_date: str | None, exit_date: str | None) -> int | None:
        if entry_date is None or exit_date is None:
            return None
        if ticker not in self._bars_by_ticker:
            self._load_ticker(ticker)
        dates = self._dates_by_ticker[ticker]
        i0 = bisect.bisect_right(dates, entry_date)
        i1 = bisect.bisect_right(dates, exit_date)
        return max(0, i1 - i0)


FEATURE_COLUMNS = [
    "ew_window_age_days",
    "ew_window_age_pct_of_10",
    "close_vs_ma10_pct",
    "close_vs_ma20_pct",
    "close_vs_ma50_pct",
    "close_vs_ma100_pct",
    "close_vs_ma200_pct",
    "ma10_vs_ma20_pct",
    "ma20_vs_ma50_pct",
    "ma50_vs_ma200_pct",
    "ma20_slope_5d",
    "ma50_slope_10d",
    "pullback_from_20d_high_pct",
    "pullback_from_60d_high_pct",
    "pullback_from_120d_high_pct",
    "bars_since_20d_high",
    "bars_since_60d_high",
    "down_leg_depth_pct",
    "down_leg_length_bars",
    "rebound_from_low_pct",
    "bars_since_local_low",
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "ret_from_low_3d",
    "ret_from_low_5d",
    "green_days_last_5",
    "red_days_last_5",
    "green_days_last_10",
    "atr5_pct",
    "atr14_pct",
    "atr20_pct",
    "true_range_pct_today",
    "range_pct_today",
    "range_pct_avg10",
    "volatility_10d",
    "volatility_20d",
    "volume_vs_avg10",
    "volume_vs_avg20",
    "volume_vs_avg50",
    "down_volume_ratio_5d",
    "up_volume_ratio_5d",
    "highest_volume_10d_flag",
    "volume_dryup_pct",
    "body_pct_of_range",
    "close_location_in_range",
    "upper_wick_pct",
    "lower_wick_pct",
    "inside_day_flag",
    "outside_day_flag",
    "gap_up_pct",
    "gap_down_pct",
    "index_ret_1d",
    "index_ret_5d",
    "index_ret_20d",
    "index_close_vs_ma50_pct",
    "index_close_vs_ma200_pct",
    "index_volatility_10d",
    "distance_from_52w_high_pct",
]


def build_episode_exit_feature_row(
    *,
    episode_id: str,
    ticker: str,
    entry_window_date: str | None,
    entry_window_exit_date: str | None,
    as_of_date: str,
    computed_at: str,
    price_cache: _PriceCache,
    index_symbol: str = DEFAULT_INDEX_SYMBOL,
    index_symbol_2: str = DEFAULT_INDEX_SYMBOL_2,
) -> dict[str, object] | None:
    bars = price_cache.bars_as_of(ticker, as_of_date)
    if not bars:
        return None
    if not price_cache.has_exact_date(ticker, as_of_date):
        return None

    closes = [b.close for b in bars]
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    volumes = [b.volume for b in bars]
    today = bars[-1]
    close = today.close

    out: dict[str, object] = {
        "episode_id": episode_id,
        "ticker": ticker,
        "entry_window_date": entry_window_date,
        "entry_window_exit_date": entry_window_exit_date,
        "as_of_date": as_of_date,
        "computed_at": computed_at,
    }
    for key in FEATURE_COLUMNS:
        out[key] = None

    age_days = price_cache.ew_age_days(ticker, entry_window_date, entry_window_exit_date)
    out["ew_window_age_days"] = age_days
    out["ew_window_age_pct_of_10"] = (age_days / 10.0) if age_days is not None else None

    ma10 = _ma(closes, 10)
    ma20 = _ma(closes, 20)
    ma50 = _ma(closes, 50)
    ma100 = _ma(closes, 100)
    ma200 = _ma(closes, 200)

    out["close_vs_ma10_pct"] = (close / ma10 - 1.0) if ma10 not in (None, 0.0) else None
    out["close_vs_ma20_pct"] = (close / ma20 - 1.0) if ma20 not in (None, 0.0) else None
    out["close_vs_ma50_pct"] = (close / ma50 - 1.0) if ma50 not in (None, 0.0) else None
    out["close_vs_ma100_pct"] = (close / ma100 - 1.0) if ma100 not in (None, 0.0) else None
    out["close_vs_ma200_pct"] = (close / ma200 - 1.0) if ma200 not in (None, 0.0) else None

    out["ma10_vs_ma20_pct"] = (ma10 / ma20 - 1.0) if ma10 not in (None, 0.0) and ma20 not in (None, 0.0) else None
    out["ma20_vs_ma50_pct"] = (ma20 / ma50 - 1.0) if ma20 not in (None, 0.0) and ma50 not in (None, 0.0) else None
    out["ma50_vs_ma200_pct"] = (ma50 / ma200 - 1.0) if ma50 not in (None, 0.0) and ma200 not in (None, 0.0) else None

    ma20_5d = _ma(closes, 20, offset=5)
    ma50_10d = _ma(closes, 50, offset=10)
    out["ma20_slope_5d"] = (ma20 / ma20_5d - 1.0) if ma20 not in (None, 0.0) and ma20_5d not in (None, 0.0) else None
    out["ma50_slope_10d"] = (ma50 / ma50_10d - 1.0) if ma50 not in (None, 0.0) and ma50_10d not in (None, 0.0) else None

    high20 = _rolling_high(highs, 20)
    high60 = _rolling_high(highs, 60)
    high120 = _rolling_high(highs, 120)
    high252 = _rolling_high(highs, 252)
    out["pullback_from_20d_high_pct"] = (close / high20 - 1.0) if high20 not in (None, 0.0) else None
    out["pullback_from_60d_high_pct"] = (close / high60 - 1.0) if high60 not in (None, 0.0) else None
    out["pullback_from_120d_high_pct"] = (close / high120 - 1.0) if high120 not in (None, 0.0) else None
    out["distance_from_52w_high_pct"] = (close / high252 - 1.0) if high252 not in (None, 0.0) else None

    out["bars_since_20d_high"] = _bars_since_window_high(highs, 20)
    out["bars_since_60d_high"] = _bars_since_window_high(highs, 60)

    # Simple deterministic down-leg approximation over last 20 bars:
    # local low = most recent min(low); pre-low peak = highest high before that low in the same window.
    # depth = (local_low / pre_low_peak) - 1; length = bars from peak to low.
    if len(bars) >= 20:
        seg = bars[-20:]
        seg_lows = [b.low for b in seg]
        seg_highs = [b.high for b in seg]
        local_low = min(seg_lows)
        idx_low = max(i for i, v in enumerate(seg_lows) if v == local_low)
        out["bars_since_local_low"] = len(seg) - 1 - idx_low
        out["rebound_from_low_pct"] = (close / local_low - 1.0) if local_low != 0.0 else None
        if idx_low > 0:
            pre_high = max(seg_highs[:idx_low])
            idx_peak = max(i for i, v in enumerate(seg_highs[:idx_low]) if v == pre_high)
            out["down_leg_depth_pct"] = (local_low / pre_high - 1.0) if pre_high != 0.0 else None
            out["down_leg_length_bars"] = idx_low - idx_peak

    out["ret_1d"] = _ret(closes, 1)
    out["ret_3d"] = _ret(closes, 3)
    out["ret_5d"] = _ret(closes, 5)
    out["ret_10d"] = _ret(closes, 10)
    out["ret_20d"] = _ret(closes, 20)

    if len(bars) >= 3:
        low3 = min(lows[-3:])
        out["ret_from_low_3d"] = (close / low3 - 1.0) if low3 != 0.0 else None
    if len(bars) >= 5:
        low5 = min(lows[-5:])
        out["ret_from_low_5d"] = (close / low5 - 1.0) if low5 != 0.0 else None

    c5 = _green_red_counts(closes, 5)
    if c5 is not None:
        out["green_days_last_5"] = c5[0]
        out["red_days_last_5"] = c5[1]
    c10 = _green_red_counts(closes, 10)
    if c10 is not None:
        out["green_days_last_10"] = c10[0]

    out["atr5_pct"] = _atr_pct(bars, 5)
    out["atr14_pct"] = _atr_pct(bars, 14)
    out["atr20_pct"] = _atr_pct(bars, 20)
    tr_today = _true_range_for_index(bars, len(bars) - 1)
    out["true_range_pct_today"] = (tr_today / close) if tr_today is not None and close != 0.0 else None

    range_today = today.high - today.low
    out["range_pct_today"] = (range_today / close) if close != 0.0 else None
    if len(bars) >= 10 and close != 0.0:
        out["range_pct_avg10"] = mean([(b.high - b.low) / b.close for b in bars[-10:] if b.close != 0.0]) if all(
            b.close != 0.0 for b in bars[-10:]
        ) else None

    out["volatility_10d"] = _volatility(closes, 10)
    out["volatility_20d"] = _volatility(closes, 20)

    if len(volumes) >= 10:
        avg10 = mean(volumes[-10:])
        out["volume_vs_avg10"] = (today.volume / avg10) if avg10 != 0.0 else None
    if len(volumes) >= 20:
        avg20 = mean(volumes[-20:])
        out["volume_vs_avg20"] = (today.volume / avg20) if avg20 != 0.0 else None
        out["volume_dryup_pct"] = (today.volume / avg20 - 1.0) if avg20 != 0.0 else None
    if len(volumes) >= 50:
        avg50 = mean(volumes[-50:])
        out["volume_vs_avg50"] = (today.volume / avg50) if avg50 != 0.0 else None

    out["down_volume_ratio_5d"] = _window_volume_ratio(bars, 5, "down")
    out["up_volume_ratio_5d"] = _window_volume_ratio(bars, 5, "up")
    if len(volumes) >= 10:
        out["highest_volume_10d_flag"] = 1 if today.volume == max(volumes[-10:]) else 0

    if range_today != 0.0:
        out["body_pct_of_range"] = abs(today.close - today.open) / range_today
        out["close_location_in_range"] = (today.close - today.low) / range_today
        out["upper_wick_pct"] = (today.high - max(today.open, today.close)) / range_today
        out["lower_wick_pct"] = (min(today.open, today.close) - today.low) / range_today

    if len(bars) >= 2:
        prev = bars[-2]
        out["inside_day_flag"] = 1 if today.high <= prev.high and today.low >= prev.low else 0
        out["outside_day_flag"] = 1 if today.high >= prev.high and today.low <= prev.low else 0
        out["gap_up_pct"] = max(0.0, (today.open - prev.high) / prev.high) if prev.high != 0.0 else None
        out["gap_down_pct"] = max(0.0, (prev.low - today.open) / prev.low) if prev.low != 0.0 else None

    idx_bars_1 = price_cache.bars_as_of(index_symbol, as_of_date)
    idx_bars_2 = price_cache.bars_as_of(index_symbol_2, as_of_date)
    has_1 = bool(idx_bars_1) and price_cache.has_exact_date(index_symbol, as_of_date)
    has_2 = bool(idx_bars_2) and price_cache.has_exact_date(index_symbol_2, as_of_date)
    if has_1 and has_2:
        closes_1 = [b.close for b in idx_bars_1]
        closes_2 = [b.close for b in idx_bars_2]
        close_1 = closes_1[-1]
        close_2 = closes_2[-1]

        ret1_1 = _ret(closes_1, 1)
        ret1_2 = _ret(closes_2, 1)
        ret5_1 = _ret(closes_1, 5)
        ret5_2 = _ret(closes_2, 5)
        ret20_1 = _ret(closes_1, 20)
        ret20_2 = _ret(closes_2, 20)
        ma50_1 = _ma(closes_1, 50)
        ma50_2 = _ma(closes_2, 50)
        ma200_1 = _ma(closes_1, 200)
        ma200_2 = _ma(closes_2, 200)
        vol10_1 = _volatility(closes_1, 10)
        vol10_2 = _volatility(closes_2, 10)

        def _avg_pair(a: float | None, b: float | None) -> float | None:
            if a is None or b is None:
                return None
            return (a + b) / 2.0

        close_vs_ma50_1 = (close_1 / ma50_1 - 1.0) if ma50_1 not in (None, 0.0) else None
        close_vs_ma50_2 = (close_2 / ma50_2 - 1.0) if ma50_2 not in (None, 0.0) else None
        close_vs_ma200_1 = (close_1 / ma200_1 - 1.0) if ma200_1 not in (None, 0.0) else None
        close_vs_ma200_2 = (close_2 / ma200_2 - 1.0) if ma200_2 not in (None, 0.0) else None

        out["index_ret_1d"] = _avg_pair(ret1_1, ret1_2)
        out["index_ret_5d"] = _avg_pair(ret5_1, ret5_2)
        out["index_ret_20d"] = _avg_pair(ret20_1, ret20_2)
        out["index_close_vs_ma50_pct"] = _avg_pair(close_vs_ma50_1, close_vs_ma50_2)
        out["index_close_vs_ma200_pct"] = _avg_pair(close_vs_ma200_1, close_vs_ma200_2)
        out["index_volatility_10d"] = _avg_pair(vol10_1, vol10_2)

    return out


def _ensure_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, TABLE_NAME):
        raise RuntimeError(
            "Table rc_episode_exit_features missing. Run migrations first (010_rc_episode_exit_features.sql)."
        )


def _load_episodes(
    conn: sqlite3.Connection,
    *,
    date_from: str | None,
    date_to: str | None,
) -> list[tuple[str, str, str | None, str]]:
    where = ["episode_id IS NOT NULL", "entry_window_exit_date IS NOT NULL"]
    params: list[object] = []
    if date_from is not None:
        where.append("entry_window_exit_date >= ?")
        params.append(date_from)
    if date_to is not None:
        where.append("entry_window_exit_date <= ?")
        params.append(date_to)
    sql = f"""
    SELECT episode_id, ticker, entry_window_date, entry_window_exit_date
    FROM rc_pipeline_episode
    WHERE {" AND ".join(where)}
    ORDER BY entry_window_date ASC, ticker ASC
    """
    rows = conn.execute(sql, params).fetchall()
    return [(str(r[0]), str(r[1]), None if r[2] is None else str(r[2]), str(r[3])) for r in rows]


def _build_insert_sql() -> str:
    cols = ["episode_id", "ticker", "entry_window_date", "entry_window_exit_date", "as_of_date", "computed_at", *FEATURE_COLUMNS]
    placeholders = ", ".join("?" for _ in cols)
    col_sql = ", ".join(cols)
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols[1:])
    return (
        f"INSERT INTO {TABLE_NAME} ({col_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT(episode_id) DO UPDATE SET {updates}"
    )


def _build_insert_ignore_sql() -> str:
    cols = ["episode_id", "ticker", "entry_window_date", "entry_window_exit_date", "as_of_date", "computed_at", *FEATURE_COLUMNS]
    placeholders = ", ".join("?" for _ in cols)
    col_sql = ", ".join(cols)
    return f"INSERT OR IGNORE INTO {TABLE_NAME} ({col_sql}) VALUES ({placeholders})"


def _row_to_tuple(row: dict[str, object]) -> tuple[object, ...]:
    cols = ["episode_id", "ticker", "entry_window_date", "entry_window_exit_date", "as_of_date", "computed_at", *FEATURE_COLUMNS]
    return tuple(row.get(c) for c in cols)


def compute_and_store_episode_exit_features(
    conn: sqlite3.Connection,
    *,
    osakedata_db_path: str = DEFAULT_OSAKEDATA_DB,
    mode: str = "upsert",
    date_from: str | None = None,
    date_to: str | None = None,
    computed_at: str | None = None,
) -> EpisodeExitFeatureSummary:
    if mode not in {"upsert", "replace-all", "insert-missing"}:
        raise ValueError(f"Unsupported mode: {mode}")
    if not osakedata_db_path:
        raise ValueError("osakedata_db_path is required")
    if not Path(osakedata_db_path).exists():
        raise FileNotFoundError(f"osakedata DB not found: {osakedata_db_path}")
    _ensure_table(conn)

    episodes = _load_episodes(conn, date_from=date_from, date_to=date_to)
    scanned = len(episodes)
    ts = computed_at or _utc_now_iso()

    os_conn = sqlite3.connect(str(Path(osakedata_db_path)))
    os_conn.row_factory = sqlite3.Row
    cache = _PriceCache(os_conn)

    built: list[dict[str, object]] = []
    skipped = 0
    try:
        for episode_id, ticker, ew_entry_date, ew_exit_date in episodes:
            row = build_episode_exit_feature_row(
                episode_id=episode_id,
                ticker=ticker,
                entry_window_date=ew_entry_date,
                entry_window_exit_date=ew_exit_date,
                as_of_date=ew_exit_date,
                computed_at=ts,
                price_cache=cache,
                index_symbol=DEFAULT_INDEX_SYMBOL,
            )
            if row is None:
                skipped += 1
                continue
            built.append(row)
    finally:
        os_conn.close()

    if mode == "replace-all":
        conn.execute(f"DELETE FROM {TABLE_NAME}")

    existing = {
        str(r[0])
        for r in conn.execute(
            f"SELECT episode_id FROM {TABLE_NAME} WHERE episode_id IN ({','.join('?' for _ in built)})",
            [str(row["episode_id"]) for row in built],
        ).fetchall()
    } if built else set()

    inserted = 0
    updated = 0
    rows_written = 0
    conn.execute("BEGIN")
    try:
        if mode == "insert-missing":
            sql = _build_insert_ignore_sql()
            for row in built:
                episode_id = str(row["episode_id"])
                before = conn.total_changes
                conn.execute(sql, _row_to_tuple(row))
                changed = conn.total_changes - before
                if changed > 0:
                    inserted += 1
                    rows_written += 1
        else:
            sql = _build_insert_sql()
            for row in built:
                episode_id = str(row["episode_id"])
                conn.execute(sql, _row_to_tuple(row))
                rows_written += 1
                if episode_id in existing and mode == "upsert":
                    updated += 1
                else:
                    inserted += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return EpisodeExitFeatureSummary(
        episodes_scanned=scanned,
        inserted=inserted,
        updated=updated,
        skipped=skipped,
        rows_written=rows_written,
    )
