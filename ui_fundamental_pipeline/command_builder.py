"""Build deterministic commands for fundamental pipeline CLIs."""
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    from .config import (
        CLI_MISSING_PERIOD_RECOVERY_CHECK,
        CLI_QUARTER_UPDATE,
        CLI_QUARTER_UPDATE_VINTAGE_PREFLIGHT,
        CLI_REPORTING_FREQUENCY_AUDIT,
        CLI_SCORE_PERCENTILE,
        CLI_TTM_BATCH,
        CLI_TICKER_SNAPSHOT,
        CLI_YAHOO_BATCH_FIN,
        FUNDAMENTALS_FIN_DB,
        FUNDAMENTALS_USA_DB,
        OSAKEDATA_DB,
        ANALYSIS_DB,
        PYTHON_EXECUTABLE,
        SEC_LATEST_WRITER_VINTAGE_APPLY_APPROVAL_TOKEN,
        SNAPSHOTS_DIR,
        TEMP_DIR,
        YAHOO_AWARE_APPLY_APPROVAL_TOKEN,
    )
except ImportError:  # pragma: no cover
    from config import (
        CLI_MISSING_PERIOD_RECOVERY_CHECK,
        CLI_QUARTER_UPDATE,
        CLI_QUARTER_UPDATE_VINTAGE_PREFLIGHT,
        CLI_REPORTING_FREQUENCY_AUDIT,
        CLI_SCORE_PERCENTILE,
        CLI_TTM_BATCH,
        CLI_TICKER_SNAPSHOT,
        CLI_YAHOO_BATCH_FIN,
        FUNDAMENTALS_FIN_DB,
        FUNDAMENTALS_USA_DB,
        OSAKEDATA_DB,
        ANALYSIS_DB,
        PYTHON_EXECUTABLE,
        SEC_LATEST_WRITER_VINTAGE_APPLY_APPROVAL_TOKEN,
        SNAPSHOTS_DIR,
        TEMP_DIR,
        YAHOO_AWARE_APPLY_APPROVAL_TOKEN,
    )


@dataclass(frozen=True)
class UsaQuarterUpdateVintageOptions:
    """Explicit PIT/vintage metadata for a USA quarter update UI launch."""

    launch_timestamp_utc: str
    vintage_run_id: str


@dataclass(frozen=True)
class UsaYahooAwareApplyOptions:
    """Explicit metadata for a USA Yahoo-aware/final-mixed vintage apply."""

    source_run_id: str
    vintage_run_id: str
    launch_timestamp_utc: str
    approved: bool = False


@dataclass(frozen=True)
class UsaSecVintageRecoveryDryRunOptions:
    """Explicit metadata for planning SEC latest-writer vintage recovery."""

    vintage_run_id: str
    launch_timestamp_utc: str
    source_run_id: str | None = None


@dataclass(frozen=True)
class UsaSecVintageRecoveryApplyOptions:
    """Explicit metadata for applying SEC latest-writer vintage recovery."""

    source_run_id: str
    vintage_run_id: str
    launch_timestamp_utc: str
    expected_count: int
    approved: bool = False


@dataclass(frozen=True)
class UsaYahooAwareRecoveryOptions:
    """Explicit metadata for Yahoo-aware/final-mixed vintage recovery."""

    source_run_id: str
    vintage_run_id: str
    launch_timestamp_utc: str
    dry_run: bool = False
    expected_final_mixed_count: int | None = None
    expected_yahoo_vintage_count: int | None = None
    approved: bool = False


def build_usa_update_command(
    run_id: str,
    vintage_options: UsaQuarterUpdateVintageOptions | None = None,
) -> list[str]:
    command = [
        str(PYTHON_EXECUTABLE),
        str(CLI_QUARTER_UPDATE),
        "--db",
        str(FUNDAMENTALS_USA_DB),
        "--osakedata-db",
        str(OSAKEDATA_DB),
        "--run-id",
        run_id,
        "--market",
        "usa",
    ]
    if vintage_options is not None:
        command.extend(
            [
                "--write-vintage",
                "--vintage-mode",
                "sec_latest_writer",
                "--vintage-market",
                "usa",
                "--vintage-available-at-utc",
                vintage_options.launch_timestamp_utc,
                "--vintage-ingested-at-utc",
                vintage_options.launch_timestamp_utc,
                "--vintage-run-id",
                vintage_options.vintage_run_id,
                "--vintage-yahoo-aware-action",
                "plan_only",
            ]
        )
    return command


def build_usa_vintage_preflight_command() -> list[str]:
    return [
        str(PYTHON_EXECUTABLE),
        "-m",
        "swingmaster.cli.preflight_quarter_update_vintage_readiness",
        "--fundamentals-db",
        str(FUNDAMENTALS_USA_DB),
        "--market",
        "usa",
        "--format",
        "json",
    ]


def build_usa_yahoo_aware_apply_command(options: UsaYahooAwareApplyOptions) -> list[str]:
    command = [
        str(PYTHON_EXECUTABLE),
        "-m",
        "swingmaster.cli.apply_quarter_update_yahoo_aware_vintage",
        "--fundamentals-db",
        str(FUNDAMENTALS_USA_DB),
        "--market",
        "usa",
        "--source-run-id",
        options.source_run_id,
        "--vintage-run-id",
        options.vintage_run_id,
        "--available-at-utc",
        options.launch_timestamp_utc,
        "--ingested-at-utc",
        options.launch_timestamp_utc,
    ]
    if options.approved:
        command.extend(["--approval-token", YAHOO_AWARE_APPLY_APPROVAL_TOKEN])
    return command


def build_usa_sec_vintage_recovery_dry_run_command(
    options: UsaSecVintageRecoveryDryRunOptions,
) -> list[str]:
    command = [
        str(PYTHON_EXECUTABLE),
        "-m",
        "swingmaster.cli.dry_run_sec_vintage_for_missing_latest",
        "--fundamentals-db",
        str(FUNDAMENTALS_USA_DB),
        "--market",
        "usa",
        "--candidate-mode",
        "latest_writer",
        "--available-at-utc",
        options.launch_timestamp_utc,
        "--ingested-at-utc",
        options.launch_timestamp_utc,
        "--vintage-run-id",
        options.vintage_run_id,
        "--format",
        "json",
        "--sample-limit",
        "20",
    ]
    if options.source_run_id:
        command.extend(["--source-run-id", options.source_run_id])
    return command


def build_usa_sec_vintage_recovery_apply_command(
    options: UsaSecVintageRecoveryApplyOptions,
) -> list[str]:
    command = [
        str(PYTHON_EXECUTABLE),
        "-m",
        "swingmaster.cli.apply_sec_vintage_for_missing_latest",
        "--fundamentals-db",
        str(FUNDAMENTALS_USA_DB),
        "--market",
        "usa",
        "--source-run-id",
        options.source_run_id,
        "--candidate-mode",
        "latest_writer",
        "--available-at-utc",
        options.launch_timestamp_utc,
        "--ingested-at-utc",
        options.launch_timestamp_utc,
        "--vintage-run-id",
        options.vintage_run_id,
        "--expected-count",
        str(options.expected_count),
    ]
    if options.approved:
        command.extend(["--approval-token", SEC_LATEST_WRITER_VINTAGE_APPLY_APPROVAL_TOKEN])
    return command


def build_usa_yahoo_aware_recovery_command(options: UsaYahooAwareRecoveryOptions) -> list[str]:
    command = [
        str(PYTHON_EXECUTABLE),
        "-m",
        "swingmaster.cli.apply_quarter_update_yahoo_aware_vintage",
        "--fundamentals-db",
        str(FUNDAMENTALS_USA_DB),
        "--market",
        "usa",
        "--source-run-id",
        options.source_run_id,
        "--vintage-run-id",
        options.vintage_run_id,
        "--available-at-utc",
        options.launch_timestamp_utc,
        "--ingested-at-utc",
        options.launch_timestamp_utc,
        "--format",
        "json",
    ]
    if options.dry_run:
        command.append("--dry-run")
    if options.expected_final_mixed_count is not None:
        command.extend(["--expected-final-mixed-count", str(options.expected_final_mixed_count)])
    if options.expected_yahoo_vintage_count is not None:
        command.extend(["--expected-yahoo-vintage-count", str(options.expected_yahoo_vintage_count)])
    if options.approved:
        command.extend(["--approval-token", YAHOO_AWARE_APPLY_APPROVAL_TOKEN])
    return command


def build_fin_update_command(run_id: str) -> list[str]:
    return [
        str(PYTHON_EXECUTABLE),
        str(CLI_QUARTER_UPDATE),
        "--db",
        str(FUNDAMENTALS_FIN_DB),
        "--osakedata-db",
        str(OSAKEDATA_DB),
        "--run-id",
        run_id,
        "--market",
        "omxh",
    ]


def build_fin_classification_ttm_commands(
    as_of_date: str,
    classification_run_id: str,
    ttm_run_id: str,
    recovery_run_id: str,
) -> list[list[str]]:
    recovery_output_path = TEMP_DIR / f"omxh_missing_period_recovery_{as_of_date.replace('-', '_')}.csv"
    classification_command = [
        str(PYTHON_EXECUTABLE),
        str(CLI_REPORTING_FREQUENCY_AUDIT),
        "--db",
        str(FUNDAMENTALS_FIN_DB),
        "--market",
        "omxh",
        "--lookback-months",
        "30",
        "--write-db",
        "--as-of-date",
        as_of_date,
        "--run-id",
        classification_run_id,
        "--write-mode",
        "replace-run",
        "--format",
        "text",
    ]
    ttm_command = [
        str(PYTHON_EXECUTABLE),
        str(CLI_TTM_BATCH),
        "--db",
        str(FUNDAMENTALS_FIN_DB),
        "--market",
        "omxh",
        "--classification-run-id",
        classification_run_id,
        "--run-id",
        ttm_run_id,
    ]
    recovery_command = [
        str(PYTHON_EXECUTABLE),
        str(CLI_MISSING_PERIOD_RECOVERY_CHECK),
        "--db",
        str(FUNDAMENTALS_FIN_DB),
        "--market",
        "omxh",
        "--classification-run-id",
        classification_run_id,
        "--format",
        "csv",
        "--output",
        str(recovery_output_path),
        "--write-db",
        "--run-id",
        recovery_run_id,
        "--write-mode",
        "replace-run",
    ]
    return [classification_command, ttm_command, recovery_command]


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
