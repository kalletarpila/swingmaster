"""Database access helpers for the UI."""
import sqlite3
from pathlib import Path

try:
    from .config import OSAKEDATA_DB
except ImportError:  # pragma: no cover
    from config import OSAKEDATA_DB


def resolve_latest_close_as_of_date(market: str) -> str:
    """Resolve latest close date from osakedata for the given market."""
    with sqlite3.connect(str(OSAKEDATA_DB)) as conn:
        row = conn.execute(
            """
            SELECT MAX(pvm)
            FROM osakedata
            WHERE market = ?
              AND close IS NOT NULL
            """,
            (market,),
        ).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"AS_OF_DATE_NOT_FOUND:{market}")
    return str(row[0])


def load_valid_tickers(db_path: Path) -> set[str]:
    """Load uppercase ticker set from rc_fundamental_ttm."""
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute("SELECT DISTINCT ticker FROM rc_fundamental_ttm").fetchall()
    return {str(row[0]).upper() for row in rows if row and row[0]}
