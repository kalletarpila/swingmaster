# Fundamentals Ticker Cleanup Audit

This document records the read-only fundamentals ticker cleanup audit run on `fundamentals_usa.db`.

## Policy

Delisted operating companies are not removal candidates merely because they are delisted. If a delisted company has usable historical quarterly fundamentals or other meaningful historical data, classify it as `KEEP_DELISTED_HISTORICAL_COMPANY`, set `active_status` to false, keep `delisted_status` true, and retain it for historical research to avoid survivorship bias. Such tickers should be excluded from future active update universes rather than deleted.

Only unsupported instrument types or genuinely empty placeholders are cleanup candidates:

- ETFs, funds, indexes, benchmarks
- warrants, rights, units, preferred-share variants
- unsupported non-operating instruments such as SPAC-related instruments
- rows that are only empty quarterly placeholders by the usable-quarter rule

Symbol suffixes alone are not enough for deletion. Pattern-only evidence is routed to review.

## Usable-Quarter Rule

A quarterly fundamentals row is usable when:

- `period_end_date` is a valid date;
- at least two core quarterly fields are non-null;
- at least one core quarterly field is non-zero.

Core fields are revenue, net income, operating income, EBIT, EBITDA, operating cashflow, free cashflow, cash, total debt, and shares outstanding.

## Implementation

The audit is implemented as a read-only CLI:

```bash
.venv/bin/python -m swingmaster.cli.audit_fundamentals_ticker_cleanup --fundamentals-db fundamentals_usa.db
```

Runtime artifacts are written only under:

```text
temp/fundamentals_ticker_cleanup_audit/<UTC timestamp>/
```

The audit records pre/post `PRAGMA quick_check` and row counts for key production tables and reports `database_content_unchanged`.

## Full-DB Result

Latest artifact root:

```text
temp/fundamentals_ticker_cleanup_audit/20260731T155253Z/
```

Summary:

- total distinct tickers: 2,937
- tickers with quarterly rows: 2,936
- tickers with usable quarters: 2,936
- tickers without quarterly rows: 1
- safe removal candidates: 0
- manual review candidates: 1
- delisted historical companies kept: 0
- projected rows affected by safe cleanup: 0 in every table
- database content unchanged: true

Category counts:

- `KEEP_USABLE_QUARTERLY_HISTORY`: 2,936
- `KEEP_MANUAL_REVIEW`: 1

The single manual review ticker is `NOKIA.HE`. It has no usable USA quarterly rows and no active dependencies in this database, but the local evidence does not prove it is an unsupported instrument or an empty placeholder, so it is not a deletion candidate under the corrected policy.

## Recommendations

Do not delete any ticker from this audit result. Treat `NOKIA.HE` as a manual review / inactive-universe candidate unless a later audit proves it is a genuine placeholder in this database.

Next phase, if desired, should add explicit active-universe exclusion metadata for review or inactive tickers. That should be a separate application/schema change, not part of this read-only audit.
