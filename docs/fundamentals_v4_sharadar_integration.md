# Fundamentals V4 Sharadar Integration

SwingMaster V4 uses the Sharadar Direct REST API at `https://api.sharadar.com/v1.0`.
This integration intentionally does not use Nasdaq Data Link, `quandl`, or `nasdaqdatalink`.

Authentication is loaded from `SHARADAR_API_KEY` and sent with the `x-api-key` HTTP header.
The key must never be committed, logged, embedded in URLs, or written to artifacts. If the
variable is missing, the client returns `SHARADAR_API_KEY_NOT_CONFIGURED` guidance using only:

```bash
export SHARADAR_API_KEY="YOUR_KEY_HERE"
```

The reusable client lives in `swingmaster/providers/sharadar.py`. It keeps provider-native raw
records separate from any future V4 canonical quarterly model. The expected flow remains:

```text
SharadarClient -> raw provider records -> future V4 normalization layer -> future V4 storage
```

No V4 production database or canonical schema is created by this phase.

The smoke CLI is:

```bash
/home/kalle/projects/swingmaster/.venv/bin/python \
  -m swingmaster.cli.run_sharadar_v4_smoke \
  --ticker AAPL
```

For the limited free-tier boundary check:

```bash
/home/kalle/projects/swingmaster/.venv/bin/python \
  -m swingmaster.cli.run_sharadar_v4_smoke \
  --ticker AAPL \
  --test-free-tier-boundary
```

Generated reports are written under `temp/fundamentals_v4_sharadar_free_api_smoke/<timestamp>/`
and are not intended for git.
