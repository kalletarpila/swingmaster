# Fundamentals V3 Phase 2D Resumable Yahoo Bootstrap Rollout

Date: 2026-08-21

Classification: `FUNDAMENTALS_V3_PHASE2D_RESUMABLE_YAHOO_BOOTSTRAP_ROLLOUT_IMPLEMENTED`

## Scope

Phase 2D implements bounded, resumable orchestration for the approved V3 Yahoo bootstrap universe
using the Phase 2C adapter.

This phase does not execute the full 2,812-company bootstrap, write canonical V3 quarters or
fundamentals, reconcile Legacy/V2 values, implement Phase 3 canonical migration, change RawCandle,
or change production Check/Update.

## Implementation

Primary module:

```text
swingmaster/fundamentals/v3_yahoo_bootstrap_rollout.py
```

CLI wrapper:

```text
swingmaster/cli/run_fundamentals_v3_yahoo_bootstrap_rollout.py
```

The rollout flow is:

```text
approved V3 companies
        ↓
bounded deterministic selection
        ↓
Phase 2C Yahoo adapter per company
        ↓
external V3 raw cache
        ↓
candidate / rejection artifacts
        ↓
atomic checkpoint after each ticker
```

## Bounds

Fetch mode is fail-closed unless one of these is supplied:

- `--ticker`
- `--tickers`
- `--tickers-file`
- `--limit`
- `--allow-full-universe`

Ticker inputs are still checked against approved active V3 companies. V2-only, osakedata-only, or
arbitrary tickers are rejected.

`--dry-run` is plan-only. It resolves the selected approved universe, validates artifact paths, and
writes plan/checkpoint artifacts with `PLANNED` rows. It does not call Yahoo and does not write raw
cache.

## Resume Contract

The rollout writes an atomic checkpoint after each ticker. Resume requires:

- compatible artifact schema
- same run id
- same selected ticker order

Already processed tickers are skipped by default. `SOURCE_ERROR` tickers are retried only when
`--retry-failed-on-resume` is supplied.

Each checkpoint ticker row contains a deterministic work key, status, attempt count, retry
eligibility, last outcome counters, candidate keys, and candidate/rejection records. Candidate and
rejection JSONL files are deterministic rewrites from checkpoint-owned state, so repeated resume
does not append duplicate rows. A checkpoint row left as `RUNNING` is treated as interrupted and
retried safely on restart.

Broad live execution includes a configurable consecutive source-error circuit breaker through
`--max-consecutive-source-errors`, default `25`.

## Replay Contract

`--replay-raw-cache` reads existing external V3 raw-cache entries and rebuilds candidate/rejection
artifacts without Yahoo calls.

Replay uses the Phase 2C raw-cache replay path and never falls back to Yahoo.

The replay invariant is:

```text
same raw payload
→ same normalized quarterly rows
→ same metadata enrichment
→ same migration candidates
```

## Artifacts

The CLI writes only runtime artifacts under `temp/` and, in non-dry-run fetch mode, the external V3
raw-cache database.

Required artifacts:

- checkpoint JSON
- summary JSON
- migration candidates JSONL
- candidate rejections JSONL

Optional artifact:

- progress log

No artifact path is accepted outside repository `temp/`.

## Phase 2D Closure Results

Closure artifacts:

```text
temp/fundamentals_v3_phase2d_closure/20260821T135751Z/
```

Full approved-universe dry plan:

| Metric | Value |
| --- | ---: |
| Approved active companies | 2812 |
| Legacy-only approved companies | 361 |
| Profile exclusions | 124 |
| Duplicate work keys | 0 |
| Provider calls | 0 |

Plan hash:

```text
441e74cc7c1f83ddb3366299f0213a429ce3de41837e6507d566e8f3dcdc26f4
```

First 10 work keys:

```text
usa|A|YAHOO|A
usa|AA|YAHOO|AA
usa|AAL|YAHOO|AAL
usa|AAMI|YAHOO|AAMI
usa|AAOI|YAHOO|AAOI
usa|AAON|YAHOO|AAON
usa|AAP|YAHOO|AAP
usa|AAPL|YAHOO|AAPL
usa|AAT|YAHOO|AAT
usa|AB|YAHOO|AB
```

Last 10 work keys:

```text
usa|ZM|YAHOO|ZM
usa|ZNTL|YAHOO|ZNTL
usa|ZS|YAHOO|ZS
usa|ZTS|YAHOO|ZTS
usa|ZUMZ|YAHOO|ZUMZ
usa|ZURA|YAHOO|ZURA
usa|ZVIA|YAHOO|ZVIA
usa|ZVRA|YAHOO|ZVRA
usa|ZWS|YAHOO|ZWS
usa|ZYME|YAHOO|ZYME
```

Live canary tickers:

```text
AAMI, AAPL, COST, UBER, WMT
```

Segment A processed 2 of 5. Segment B resumed the same run id and completed the remaining 3.

Canary result:

| Metric | Value |
| --- | ---: |
| Provider calls | 5 |
| Configured delay seconds | 0.5 |
| Measured elapsed seconds | 12.41 |
| Raw OK | 5 |
| Raw EMPTY | 0 |
| Raw ERROR | 0 |
| Yahoo raw rows | 5 |
| Normalized rows | 25 |
| Migration-ready rows | 20 |
| Identity unresolved rows | 5 |
| Identity ambiguous rows | 0 |
| Publication unresolved rows | 5 |
| No usable values | 0 |

AAMI was the Legacy-only canary. Its Yahoo values were retained as metadata rejections because no
exact FY/FQ + publication metadata was available.

Canary field coverage had all tracked Yahoo/V3 fields present for all five canary tickers. Earliest
and latest returned periods:

| Ticker | Earliest | Latest |
| --- | --- | --- |
| AAMI | 2024-12-31 | 2026-06-30 |
| AAPL | 2024-12-31 | 2026-06-30 |
| COST | 2024-11-30 | 2026-05-31 |
| UBER | 2024-12-31 | 2026-06-30 |
| WMT | 2025-01-31 | 2026-04-30 |

Raw-cache `PRAGMA quick_check` returned `ok`.

Cache-only replay provider calls: `0`.

Replay parity:

- summary equal: yes
- candidate JSONL equal: yes
- rejection JSONL equal: yes
- candidate count: 20
- rejection count: 5

Duration estimate for 2812 companies, based on the five-ticker canary:

```text
1.9-3.1 hours
```

Disk estimate:

```text
projected raw cache: ~164 MB
projected checkpoint/candidate artifacts: ~41 MB
conservative 3x total: ~612 MB
available free space during closure: ~727 GB
preflight: ok
```

Future full bootstrap command:

```bash
PYTHONPATH=. .venv/bin/python -m swingmaster.cli.run_fundamentals_v3_yahoo_bootstrap_rollout \
  --v3-db rc_fundamentals_v3.db \
  --raw-cache-db rc_fundamentals_v3_raw.db \
  --v2-db rc_fundamentals_v2.db \
  --legacy-db fundamentals_usa.db \
  --market usa \
  --allow-full-universe \
  --run-id V3_YAHOO_FULL_BOOTSTRAP_YYYYMMDDTHHMMSSZ \
  --checkpoint-json temp/fundamentals_v3_yahoo_full_bootstrap/YYYYMMDDTHHMMSSZ/checkpoint.json \
  --summary-json temp/fundamentals_v3_yahoo_full_bootstrap/YYYYMMDDTHHMMSSZ/summary.json \
  --candidates-jsonl temp/fundamentals_v3_yahoo_full_bootstrap/YYYYMMDDTHHMMSSZ/candidates.jsonl \
  --rejections-jsonl temp/fundamentals_v3_yahoo_full_bootstrap/YYYYMMDDTHHMMSSZ/rejections.jsonl \
  --progress-log temp/fundamentals_v3_yahoo_full_bootstrap/YYYYMMDDTHHMMSSZ/progress.log \
  --delay-seconds 0.5
```

Status: `NOT EXECUTED IN PHASE 2D-CLOSURE`.

## Canonical Safety

Phase 2D never writes canonical V3 tables. Migration candidates remain file artifacts for Phase 3.
