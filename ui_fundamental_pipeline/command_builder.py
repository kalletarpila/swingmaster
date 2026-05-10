"""Build deterministic commands for fundamental pipeline CLIs."""
from pathlib import Path
from typing import Iterable

try:
    from .config import (
        CLI_QUARTER_UPDATE_USA,
        CLI_SCORE_PERCENTILE,
        CLI_TICKER_SNAPSHOT,
        CLI_YAHOO_BATCH_FIN,
        FUNDAMENTALS_FIN_DB,
        FUNDAMENTALS_USA_DB,
        OSAKEDATA_DB,
        ANALYSIS_DB,
        PYTHON_EXECUTABLE,
        SNAPSHOTS_DIR,
    )
except ImportError:  # pragma: no cover
    from config import (
        CLI_QUARTER_UPDATE_USA,
        CLI_SCORE_PERCENTILE,
        CLI_TICKER_SNAPSHOT,
        CLI_YAHOO_BATCH_FIN,
        FUNDAMENTALS_FIN_DB,
        FUNDAMENTALS_USA_DB,
        OSAKEDATA_DB,
        ANALYSIS_DB,
        PYTHON_EXECUTABLE,
        SNAPSHOTS_DIR,
    )


def build_usa_update_command(run_id: str) -> list[str]:
    return [
        str(PYTHON_EXECUTABLE),
        str(CLI_QUARTER_UPDATE_USA),
        "--db",
        str(FUNDAMENTALS_USA_DB),
        "--osakedata-db",
        str(OSAKEDATA_DB),
        "--run-id",
        run_id,
        "--market",
        "usa",
    ]


def build_fin_update_command(run_id: str) -> list[str]:
    return [
        str(PYTHON_EXECUTABLE),
        str(CLI_YAHOO_BATCH_FIN),
        "--db",
        str(FUNDAMENTALS_FIN_DB),
        "--osakedata-db",
        str(OSAKEDATA_DB),
        "--run-id",
        run_id,
        "--replace-symbol",
    ]


def build_score_percentile_command(market: str, run_id: str, as_of_date: str) -> list[str]:
    db_path = FUNDAMENTALS_USA_DB if market == "usa" else FUNDAMENTALS_FIN_DB
    return [
        str(PYTHON_EXECUTABLE),
        str(CLI_SCORE_PERCENTILE),
        "--db",
        str(db_path),
        "--osakedata-db",
        str(OSAKEDATA_DB),
        "--as-of-date",
        as_of_date,
        "--run-id",
        run_id,
        "--market",
        market,
    ]


def build_snapshot_command(market: str, tickers: Iterable[str], percentile_target_date: str) -> list[str]:
    db_path = FUNDAMENTALS_USA_DB if market == "usa" else FUNDAMENTALS_FIN_DB
    command = [
        str(PYTHON_EXECUTABLE),
        str(CLI_TICKER_SNAPSHOT),
        "--db",
        str(db_path),
        "--ticker",
    ]
    command.extend(tickers)
    command.extend(
        [
            "--quarters",
            "4",
            "--percentile-target-date",
            percentile_target_date,
            "--ohlcv-db",
            str(OSAKEDATA_DB),
            "--price-behavior-snapshot",
            "--dow-structure-snapshot",
            "--dow-analysis-db",
            str(ANALYSIS_DB),
            "--candlestick-snapshot",
            "--candlestick-analysis-db",
            str(ANALYSIS_DB),
            "--divergence-snapshot",
            "--divergence-analysis-db",
            str(ANALYSIS_DB),
            "--moving-average-snapshot",
            "--output-dir",
            str(SNAPSHOTS_DIR),
        ]
    )
    return command


def build_single_ticker_snapshot_command(
    market: str, ticker: str, percentile_target_date: str
) -> list[str]:
    return build_snapshot_command(market=market, tickers=[ticker], percentile_target_date=percentile_target_date)
