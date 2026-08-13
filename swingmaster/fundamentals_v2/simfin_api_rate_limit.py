from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping


DEFAULT_RATE_LIMIT_RETRY_DELAY_SECONDS = 120.0


@dataclass(frozen=True)
class Single429RetryResult:
    result: Mapping[str, Any]
    attempts: list[Mapping[str, Any]]
    retry_performed: bool
    recovered_after_429: bool
    stopped_after_second_429: bool

    @property
    def http_requests_made(self) -> int:
        return len(self.attempts)


def request_with_single_429_retry(
    *,
    ticker: str,
    requester: Callable[[str], Mapping[str, Any]],
    retry_delay_seconds: float = DEFAULT_RATE_LIMIT_RETRY_DELAY_SECONDS,
    sleeper: Callable[[float], None] = time.sleep,
) -> Single429RetryResult:
    first = requester(ticker)
    attempts = [first]
    if first.get("provider_status") != "RATE_LIMITED":
        return Single429RetryResult(
            result=first,
            attempts=attempts,
            retry_performed=False,
            recovered_after_429=False,
            stopped_after_second_429=False,
        )
    sleeper(retry_delay_seconds)
    retry = requester(ticker)
    attempts.append(retry)
    retry_status = retry.get("provider_status")
    return Single429RetryResult(
        result=retry,
        attempts=attempts,
        retry_performed=True,
        recovered_after_429=retry_status in {"SUCCESS", "NO_DATA"},
        stopped_after_second_429=retry_status == "RATE_LIMITED",
    )


def summarize_request_accounting(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    fetched_rows = [row for row in rows if row.get("action") == "FETCHED"]
    return {
        "logical_tickers_attempted": len(fetched_rows),
        "http_requests_made": sum(int(row.get("http_requests_made") or 0) for row in fetched_rows),
        "first_429_count": sum(int(row.get("first_429_detected") or 0) for row in fetched_rows),
        "recovered_429_count": sum(int(row.get("recovered_after_429") or 0) for row in fetched_rows),
        "second_429_stop_count": sum(int(row.get("stopped_after_second_429") or 0) for row in fetched_rows),
    }
