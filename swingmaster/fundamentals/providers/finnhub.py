from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


FINNHUB_API_BASE_URL = "https://finnhub.io/api/v1"


class FinnhubClient:
    def __init__(self, api_key: str | None = None, base_url: str = FINNHUB_API_BASE_URL) -> None:
        resolved_api_key = api_key or os.environ.get("FINNHUB_API_KEY")
        if not resolved_api_key:
            raise RuntimeError("FINNHUB_API_KEY_MISSING")
        self._api_key = resolved_api_key
        self._base_url = base_url.rstrip("/")

    def get_stock_symbols(self, exchange: str) -> list[dict]:
        payload = self._fetch_json("/stock/symbol", {"exchange": exchange})
        if not isinstance(payload, list):
            raise RuntimeError("FINNHUB_RESPONSE_INVALID:stock/symbol")
        return payload

    def get_financials_reported(self, symbol: str, freq: str = "quarterly") -> dict:
        payload = self._fetch_json("/stock/financials-reported", {"symbol": symbol, "freq": freq})
        if not isinstance(payload, dict):
            raise RuntimeError("FINNHUB_RESPONSE_INVALID:stock/financials-reported")
        return payload

    def _fetch_json(self, path: str, params: dict[str, str]) -> Any:
        query = urlencode({**params, "token": self._api_key})
        url = f"{self._base_url}{path}?{query}"
        request = Request(url, headers={"Accept": "application/json"})
        try:
            with urlopen(request) as response:
                payload = response.read()
        except Exception as exc:
            raise RuntimeError(f"FINNHUB_REQUEST_FAILED:{path}:{type(exc).__name__}:{exc}") from exc
        try:
            return json.loads(payload.decode("utf-8"))
        except Exception as exc:
            raise RuntimeError(f"FINNHUB_RESPONSE_DECODE_FAILED:{path}:{type(exc).__name__}:{exc}") from exc
