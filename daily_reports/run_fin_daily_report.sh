#!/usr/bin/env bash
set -euo pipefail

AS_OF_DATE="${1:-2025-12-12}"
ROOT="/home/kalle/projects/swingmaster"
DB="$ROOT/swingmaster_rc.db"
SQL_TEMPLATE="$ROOT/daily_reports/fin_daily_report.sql"
TXT_OUT="$ROOT/daily_reports/fin_daily_report_${AS_OF_DATE}.txt"
CSV_OUT="$ROOT/daily_reports/fin_daily_report_${AS_OF_DATE}.csv"
TMP_SQL="$(mktemp)"

cleanup() {
  rm -f "$TMP_SQL"
}
trap cleanup EXIT

sed "s/date('2025-12-12')/date('$AS_OF_DATE')/g; s/date('2025-12-12', '-1 day')/date('$AS_OF_DATE', '-1 day')/g" "$SQL_TEMPLATE" > "$TMP_SQL"

sqlite3 -header -column "$DB" < "$TMP_SQL" > "$TXT_OUT"

sqlite3 -header -csv "$DB" < "$TMP_SQL" \
  | python3 -c "import csv, re, sys; out_path = sys.argv[1]; reader = csv.reader(sys.stdin); num_re = re.compile(r'^-?\\d+(?:\\.\\d+)?$'); f = open(out_path, 'w', newline='', encoding='utf-8'); writer = csv.writer(f, delimiter=';', lineterminator='\\n'); [writer.writerow([(v.replace('.', ',') if num_re.match(v) else v) for v in row]) for row in reader]; f.close()" "$CSV_OUT"

printf 'TXT: %s\n' "$TXT_OUT"
printf 'CSV: %s\n' "$CSV_OUT"
