"""
Configuration and constants for Fundamental Pipeline Manager UI.
"""
from pathlib import Path
from datetime import datetime

# Project root
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Database paths (hardcoded)
FUNDAMENTALS_USA_DB = PROJECT_ROOT / "fundamentals_usa.db"
FUNDAMENTALS_FIN_DB = PROJECT_ROOT / "fundamentals_fin.db"
OSAKEDATA_DB = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
ANALYSIS_DB = Path("/home/kalle/projects/rawcandle/data/analysis.db")

# Output and snapshots
SNAPSHOTS_DIR = PROJECT_ROOT / "snapshots"

# Python venv executable
PYTHON_EXECUTABLE = PROJECT_ROOT / ".venv" / "bin" / "python"

# CLI scripts
CLI_QUARTER_UPDATE = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_quarter_update.py"
CLI_YAHOO_BATCH_FIN = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_yahoo_batch_fin.py"
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
    return f"USA_QUARTER_UPDATE_{datetime.now().strftime(DATE_FORMAT)}"


def get_run_id_fin() -> str:
    """Generate FIN run ID based on current date."""
    return f"FIN_YAHOO_BATCH_{datetime.now().strftime(DATE_FORMAT)}"


def validate_config() -> bool:
    """Validate that all required paths exist."""
    required = [
        FUNDAMENTALS_USA_DB,
        FUNDAMENTALS_FIN_DB,
        OSAKEDATA_DB,
        ANALYSIS_DB,
        PYTHON_EXECUTABLE,
        CLI_QUARTER_UPDATE,
        CLI_YAHOO_BATCH_FIN,
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
        CLI_YAHOO_BATCH_FIN,
        CLI_SCORE_PERCENTILE,
        CLI_TICKER_SNAPSHOT,
    ]
    return [str(p) for p in required if not p.exists()]
