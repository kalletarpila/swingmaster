"""SQLite reader for resolving ticker universes.

Responsibilities:
  - Query available tickers by market/sector/industry filters.
Must not:
  - Implement signal/policy logic; data access only.
"""

from __future__ import annotations

import random
import re
import sqlite3
from typing import List

from swingmaster.app_api.dto import UniverseSpec

_IDENT_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_identifier(name: str) -> str:
    if not _IDENT_PATTERN.fullmatch(name):
        raise ValueError("Invalid identifier")
    return name


class TickerUniverseReader:
    def __init__(self, conn: sqlite3.Connection, table_name: str = "ticker_meta") -> None:
        self._conn = conn
        self._table = _validate_identifier(table_name)

    def list_markets(self) -> List[str]:
        rows = self._conn.execute(
            f"SELECT DISTINCT market FROM {self._table} WHERE market IS NOT NULL ORDER BY market"
        ).fetchall()
        return [row[0] for row in rows]

    def list_sectors(self, market: str) -> List[str]:
        rows = self._conn.execute(
            f"SELECT DISTINCT sector FROM {self._table} WHERE market=? AND sector IS NOT NULL ORDER BY sector",
            (market,),
        ).fetchall()
        return [row[0] for row in rows]

    def list_industries(self, market: str, sector: str) -> List[str]:
        rows = self._conn.execute(
            f"""
            SELECT DISTINCT industry FROM {self._table}
            WHERE market=? AND sector=? AND industry IS NOT NULL
            ORDER BY industry
            """,
            (market, sector),
        ).fetchall()
        return [row[0] for row in rows]

    def resolve_tickers(self, spec: UniverseSpec) -> List[str]:
        spec.validate()

        if spec.mode == "tickers":
            seen = set()
            ordered = []
            for t in spec.tickers or []:
                if t not in seen:
                    seen.add(t)
                    ordered.append(t)
            return ordered[: spec.limit]

        filters = []
        params = []
        if spec.mode in ("market", "market_sector", "market_sector_industry"):
            assert spec.market is not None
            filters.append("market=?")
            params.append(spec.market)
        if spec.mode in ("market_sector", "market_sector_industry"):
            assert spec.sector is not None
            filters.append("sector=?")
            params.append(spec.sector)
        if spec.mode == "market_sector_industry":
            assert spec.industry is not None
            filters.append("industry=?")
            params.append(spec.industry)

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        if spec.sample == "first_n":
            query = f"SELECT ticker FROM {self._table} {where_clause} ORDER BY ticker LIMIT ?"
            params_with_limit = (*params, spec.limit)
            rows = self._conn.execute(query, params_with_limit).fetchall()
            return [row[0] for row in rows]

        # random sample: deterministic via seed
        query = f"SELECT ticker FROM {self._table} {where_clause} ORDER BY ticker"
        rows = self._conn.execute(query, params).fetchall()
        tickers = [row[0] for row in rows]
        rng = random.Random(spec.seed)
        rng.shuffle(tickers)
        return tickers[: spec.limit]

    def filter_by_osakedata(
        self,
        tickers: List[str],
        as_of_date: str,
        osakedata_table: str = "osakedata",
        min_history_rows: int = 21,
        require_row_on_date: bool = False,
    ) -> List[str]:
        if min_history_rows < 1:
            raise ValueError("min_history_rows must be >= 1")

        table = _validate_identifier(osakedata_table)
        if not tickers:
            return []

        chunk_size = 500
        qualified = set()

        for i in range(0, len(tickers), chunk_size):
            chunk = tickers[i : i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            params = [*chunk, as_of_date, min_history_rows]
            rows = self._conn.execute(
                f"""
                SELECT osake, COUNT(*) as c
                FROM {table}
                WHERE osake IN ({placeholders}) AND pvm <= ?
                GROUP BY osake
                HAVING c >= ?
                """,
                params,
            ).fetchall()
            qualified.update(row[0] for row in rows)

        if require_row_on_date and qualified:
            has_date = set()
            for i in range(0, len(tickers), chunk_size):
                chunk = [t for t in tickers[i : i + chunk_size] if t in qualified]
                if not chunk:
                    continue
                placeholders = ",".join(["?"] * len(chunk))
                rows = self._conn.execute(
                    f"""
                    SELECT DISTINCT osake FROM {table}
                    WHERE osake IN ({placeholders}) AND pvm = ?
                    """,
                    [*chunk, as_of_date],
                ).fetchall()
                has_date.update(row[0] for row in rows)
            qualified = qualified.intersection(has_date)

        return [t for t in tickers if t in qualified]
