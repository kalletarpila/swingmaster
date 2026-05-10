"""Ticker parsing and validation helpers."""
from typing import Iterable


def parse_and_validate_tickers(raw_input: str, valid_tickers: Iterable[str]) -> list[str]:
    """Parse comma/space separated ticker input and return unique valid uppercase tickers."""
    if not raw_input:
        return []

    allowed = {ticker.upper() for ticker in valid_tickers}
    normalized = raw_input.replace(" ", ",")
    tokens = [token.strip().upper() for token in normalized.split(",") if token.strip()]

    seen = set()
    valid = []
    for ticker in tokens:
        if ticker in allowed and ticker not in seen:
            valid.append(ticker)
            seen.add(ticker)
    return valid
