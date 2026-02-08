"""DTO definitions for app-level data exchange.

Responsibilities:
  - Define stable, typed structures for app inputs/outputs.
Must not:
  - Implement business logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

UniverseMode = Literal["tickers", "market", "market_sector", "market_sector_industry"]
UniverseSample = Literal["first_n", "random"]


@dataclass(frozen=True)
class UniverseSpec:
    mode: UniverseMode
    tickers: Optional[list[str]] = None
    market: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    limit: int = 50
    sample: UniverseSample = "first_n"
    seed: int = 1

    def validate(self) -> None:
        if self.limit < 1:
            raise ValueError("limit must be >= 1")

        if self.sample not in ("first_n", "random"):
            raise ValueError("sample must be 'first_n' or 'random'")

        if self.seed < 0:
            raise ValueError("seed must be >= 0")

        if self.mode == "tickers":
            if not self.tickers:
                raise ValueError("tickers must be provided for mode 'tickers'")
            cleaned = []
            for t in self.tickers:
                stripped = t.strip()
                if not stripped:
                    raise ValueError("tickers contain empty value")
                cleaned.append(stripped)
            object.__setattr__(self, "tickers", cleaned)
        elif self.mode == "market":
            if not self.market:
                raise ValueError("market must be provided for mode 'market'")
            if not self.market.strip():
                raise ValueError("market must be non-empty")
        elif self.mode == "market_sector":
            if not self.market or not self.sector:
                raise ValueError("market and sector must be provided for mode 'market_sector'")
            if not self.market.strip() or not self.sector.strip():
                raise ValueError("market and sector must be non-empty")
        elif self.mode == "market_sector_industry":
            if not self.market or not self.sector or not self.industry:
                raise ValueError(
                    "market, sector, and industry must be provided for mode 'market_sector_industry'"
                )
            if not self.market.strip() or not self.sector.strip() or not self.industry.strip():
                raise ValueError("market, sector, and industry must be non-empty")
        else:
            raise ValueError("unsupported mode")
