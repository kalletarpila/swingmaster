#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/kalle/projects/swingmaster"
cd "$ROOT"
exec PYTHONPATH=. python3 swingmaster/cli/run_weekly_report.py "$@"
