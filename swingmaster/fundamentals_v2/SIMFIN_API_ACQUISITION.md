# SimFin API Acquisition

V2 SimFin API acquisition paths use source-specific request grouping with the same one-retry 429 shape:

- statement acquisition: `run_rc_v2_simfin_api_statements.py acquire`
- point-in-time shares acquisition: `run_rc_v2_simfin_api_shares.py acquire`

Statement requests are made with up to two tickers per HTTP request through the shared request-start limiter. When a statement request group returns HTTP 429, acquisition persists the rate-limited fetch state, waits once, and retries the same request group exactly once. The statement default wait is 300 seconds and can be configured with `--rate-limit-retry-delay-seconds`.

Shares requests keep their separate shares acquisition defaults and behavior.

If the retry returns `SUCCESS` or `NO_DATA`, acquisition records the retry result and continues with the remaining tickers. If the retry also returns HTTP 429, acquisition records the second rate-limited state and stops the entire run with `SIMFIN_RATE_LIMITED_AFTER_RETRY`.

Acquire results include request accounting: logical fetched ticker count, total HTTP requests made, first-429 count, recovered-429 count, and second-429 stop count. A recovered two-ticker 429 therefore counts as two logical tickers and two HTTP requests.
