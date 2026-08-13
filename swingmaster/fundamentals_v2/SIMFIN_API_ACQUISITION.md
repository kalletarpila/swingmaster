# SimFin API Acquisition

Both V2 SimFin API acquisition paths use the same 429 policy:

- statement acquisition: `run_rc_v2_simfin_api_statements.py acquire`
- point-in-time shares acquisition: `run_rc_v2_simfin_api_shares.py acquire`

Requests are still made one ticker at a time through the shared request-start limiter. When a ticker returns HTTP 429, acquisition persists the rate-limited fetch state, waits once, and retries the same ticker exactly once. The default wait is 120 seconds and can be configured with `--rate-limit-retry-delay-seconds`.

If the retry returns `SUCCESS` or `NO_DATA`, acquisition records the retry result and continues with the remaining tickers. If the retry also returns HTTP 429, acquisition records the second rate-limited state and stops the entire run with `SIMFIN_RATE_LIMITED_AFTER_RETRY`.

The retry policy is implemented in `simfin_api_rate_limit.py` and is intentionally shared by statement and shares acquisition to keep their provider behavior identical.

Acquire results include request accounting: logical fetched ticker count, total HTTP requests made, first-429 count, recovered-429 count, and second-429 stop count. A recovered 429 therefore counts as one logical ticker and two HTTP requests.
