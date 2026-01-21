from __future__ import annotations

import re
import sqlite3
from typing import List

_IDENT_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_identifier(name: str) -> str:
    if not _IDENT_PATTERN.fullmatch(name):
        raise ValueError("Invalid identifier")
    return name


class OsakeDataReader:
    def __init__(self, conn: sqlite3.Connection, table_name: str = "osakedata") -> None:
        self._conn = conn
        self._table = _validate_identifier(table_name)

    def get_last_n_closes(self, ticker: str, as_of_date: str, n: int) -> List[float]:
        query = (
            f"SELECT close FROM {self._table} "
            f"WHERE osake=? AND pvm<=? "
            f"ORDER BY pvm DESC LIMIT ?"
        )
        rows = self._conn.execute(query, (ticker, as_of_date, n)).fetchall()
        return [float(row[0]) for row in rows]
