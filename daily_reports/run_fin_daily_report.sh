#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/kalle/projects/swingmaster"

exec "$ROOT/daily_reports/run_daily_report.sh" "$@" fin
