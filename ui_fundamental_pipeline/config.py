"""
Configuration and constants for Fundamental Pipeline Manager UI.
"""
from pathlib import Path
from datetime import datetime, timezone

# Project root
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Database paths (hardcoded)
FUNDAMENTALS_USA_DB = PROJECT_ROOT / "fundamentals_usa.db"
FUNDAMENTALS_FIN_DB = PROJECT_ROOT / "fundamentals_fin.db"
OSAKEDATA_DB = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
ANALYSIS_DB = Path("/home/kalle/projects/rawcandle/data/analysis.db")

# Output and snapshots
SNAPSHOTS_DIR = PROJECT_ROOT / "snapshots"
TEMP_DIR = PROJECT_ROOT / "temp"

# Python venv executable
PYTHON_EXECUTABLE = PROJECT_ROOT / ".venv" / "bin" / "python"

# CLI scripts
CLI_QUARTER_UPDATE = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_quarter_update.py"
CLI_QUARTER_UPDATE_VINTAGE_PREFLIGHT = (
    PROJECT_ROOT / "swingmaster" / "cli" / "preflight_quarter_update_vintage_readiness.py"
)
YAHOO_AWARE_APPLY_APPROVAL_TOKEN = "USER_APPROVES_YAHOO_AWARE_VINTAGE_APPLY"
SEC_LATEST_WRITER_VINTAGE_APPLY_APPROVAL_TOKEN = "USER_APPROVES_SEC_LATEST_WRITER_VINTAGE_APPLY"
CLI_YAHOO_BATCH_FIN = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_yahoo_batch_fin.py"
CLI_REPORTING_FREQUENCY_AUDIT = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_reporting_frequency_audit.py"
CLI_TTM_BATCH = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_ttm_batch.py"
CLI_MISSING_PERIOD_RECOVERY_CHECK = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_missing_period_recovery_check.py"
CLI_SCORE_PERCENTILE = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_score_percentile.py"
CLI_TICKER_SNAPSHOT = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_ticker_snapshot.py"

# UI settings
WINDOW_TITLE = "Swing Master"
WINDOW_WIDTH = 1400
WINDOW_HEIGHT = 900
WEB_HOST = "127.0.0.1"
WEB_PORT = 8550

# Execution settings
PROCESS_TIMEOUT_SECONDS = 7200  # 2 hours
LOG_BUFFER_LINES = 50  # Keep last 50 lines visible
PROCESS_TERMINATION_WAIT_SECONDS = 5  # Wait before SIGKILL

# Snapshot file pattern
SNAPSHOT_FILE_PATTERN = "*.csv"

# Summary section marker
SUMMARY_MARKER = "SUMMARY:"

# Display formatting
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_run_id_usa() -> str:
    """Generate USA run ID based on current date."""
    return f"USA_QUARTER_UPDATE_{datetime.now().strftime(DATE_FORMAT)}__QUARTERLY"


def get_utc_launch_timestamp() -> str:
    """Generate an explicit UTC launch timestamp for PIT/vintage metadata."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_vintage_run_id_usa(source_run_id: str) -> str:
    """Derive the USA SEC latest-writer vintage run ID from the source run ID."""
    if source_run_id.endswith("__QUARTERLY"):
        return source_run_id.removesuffix("__QUARTERLY") + "__SEC_LATEST_WRITER_VINTAGE"
    return f"{source_run_id}__SEC_LATEST_WRITER_VINTAGE"


def get_yahoo_aware_vintage_run_id_usa(source_run_id: str) -> str:
    """Derive the USA Yahoo-aware/final-mixed apply vintage run ID."""
    if source_run_id.endswith("__QUARTERLY"):
        return source_run_id.removesuffix("__QUARTERLY") + "__YAHOO_AWARE_VINTAGE"
    return f"{source_run_id}__YAHOO_AWARE_VINTAGE"


def get_yahoo_aware_recovery_run_id_usa(source_run_id: str) -> str:
    """Derive the USA Yahoo-aware/final-mixed recovery vintage run ID."""
    if source_run_id.endswith("__QUARTERLY"):
        return source_run_id.removesuffix("__QUARTERLY") + "__YAHOO_AWARE_VINTAGE_RECOVERY"
    return f"{source_run_id}__YAHOO_AWARE_VINTAGE_RECOVERY"


def get_sec_vintage_recovery_run_id_usa() -> str:
    """Generate a USA SEC latest-writer vintage recovery run ID."""
    return f"USA_PIT_VINTAGE_RECOVERY_{datetime.now().strftime(DATE_FORMAT)}__SEC_LATEST_WRITER_VINTAGE_RECOVERY"


def get_run_id_fin() -> str:
    """Generate FIN run ID based on current date."""
    return f"FIN_YAHOO_BATCH_{datetime.now().strftime(DATE_FORMAT)}"


def get_fin_chain_as_of_date() -> str:
    """Generate FIN classification chain as-of date in YYYY-MM-DD format."""
    return datetime.now().strftime(DATE_FORMAT)


def get_run_id_fin_classification() -> str:
    """Generate FIN classification snapshot run ID."""
    return f"OMXH_REPORTING_FREQ_{datetime.now().strftime('%Y_%m_%d')}"


def get_run_id_fin_ttm() -> str:
    """Generate FIN TTM run ID."""
    return f"OMXH_TTM_{datetime.now().strftime('%Y_%m_%d')}"


def get_run_id_fin_recovery() -> str:
    """Generate FIN missing-period recovery run ID."""
    return f"OMXH_MISSING_PERIOD_RECOVERY_{datetime.now().strftime('%Y_%m_%d')}"


def validate_config() -> bool:
    """Validate that all required paths exist."""
    required = [
        FUNDAMENTALS_USA_DB,
        FUNDAMENTALS_FIN_DB,
        OSAKEDATA_DB,
        ANALYSIS_DB,
        PYTHON_EXECUTABLE,
        CLI_QUARTER_UPDATE,
        CLI_QUARTER_UPDATE_VINTAGE_PREFLIGHT,
        CLI_YAHOO_BATCH_FIN,
        CLI_REPORTING_FREQUENCY_AUDIT,
        CLI_TTM_BATCH,
        CLI_MISSING_PERIOD_RECOVERY_CHECK,
        CLI_SCORE_PERCENTILE,
        CLI_TICKER_SNAPSHOT,
    ]
    return all(p.exists() for p in required)


def get_missing_paths() -> list:
    """Return list of missing paths."""
    required = [
        FUNDAMENTALS_USA_DB,
        FUNDAMENTALS_FIN_DB,
        OSAKEDATA_DB,
        ANALYSIS_DB,
        PYTHON_EXECUTABLE,
        CLI_QUARTER_UPDATE,
        CLI_QUARTER_UPDATE_VINTAGE_PREFLIGHT,
        CLI_YAHOO_BATCH_FIN,
        CLI_REPORTING_FREQUENCY_AUDIT,
        CLI_TTM_BATCH,
        CLI_MISSING_PERIOD_RECOVERY_CHECK,
        CLI_SCORE_PERCENTILE,
        CLI_TICKER_SNAPSHOT,
    ]
    return [str(p) for p in required if not p.exists()]
