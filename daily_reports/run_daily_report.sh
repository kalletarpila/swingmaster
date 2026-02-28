#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/kalle/projects/swingmaster"
SQL_TEMPLATE="$ROOT/daily_reports/fin_daily_report.sql"
DEFAULT_DATE="$(date +%F)"

usage() {
  cat <<'EOF'
Usage:
  run_daily_report.sh [AS_OF_DATE] MARKET [MARKET...]
  run_daily_report.sh [AS_OF_DATE] MARKET1,MARKET2,...

Markets:
  fin | omxh
  se  | omxs
  usa
  usa500
EOF
}

is_date() {
  [[ "${1:-}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]
}

normalize_market_tokens() {
  local raw
  for raw in "$@"; do
    raw="${raw//,/ }"
    for token in $raw; do
      printf '%s\n' "$token"
    done
  done
}

market_config() {
  case "$1" in
    fin|omxh)
      MARKET_CODE="fin"
      MARKET_LABEL="FIN"
      DB_PATH="$ROOT/swingmaster_rc.db"
      FP_THRESHOLD="0.60"
      BUY_SECTION="BUYS_FIN"
      BUY_RULE="FIN_PASS_FP60"
      ;;
    se|omxs)
      MARKET_CODE="se"
      MARKET_LABEL="SE"
      DB_PATH="$ROOT/swingmaster_rc_se.db"
      FP_THRESHOLD="0.80"
      BUY_SECTION="BUYS_1_SE"
      BUY_RULE="SE_BUY_1_FP80"
      ;;
    usa)
      MARKET_CODE="usa"
      MARKET_LABEL="USA"
      DB_PATH="$ROOT/swingmaster_rc_usa_2024_2025.db"
      FP_THRESHOLD="0.80"
      BUY_SECTION="BUYS_USA"
      BUY_RULE="USA_PASS_FP80"
      ;;
    usa500)
      MARKET_CODE="usa500"
      MARKET_LABEL="USA500"
      DB_PATH="$ROOT/swingmaster_rc_usa_500.db"
      FP_THRESHOLD="0.80"
      BUY_SECTION="BUYS_USA500"
      BUY_RULE="USA500_PASS_FP80"
      ;;
    *)
      printf 'Unknown market: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
}

AS_OF_DATE="$DEFAULT_DATE"
if is_date "${1:-}"; then
  AS_OF_DATE="$1"
  shift
fi

if [ "$#" -eq 0 ]; then
  usage >&2
  exit 1
fi

mapfile -t MARKETS < <(normalize_market_tokens "$@")

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

for market in "${MARKETS[@]}"; do
  market_config "$market"

  TMP_SQL="$TMP_DIR/${MARKET_CODE}_${AS_OF_DATE}.sql"
  TXT_OUT="$ROOT/daily_reports/${MARKET_CODE}_daily_report_${AS_OF_DATE}.txt"
  CSV_OUT="$ROOT/daily_reports/${MARKET_CODE}_daily_report_${AS_OF_DATE}.csv"

  sed \
    -e "s/__AS_OF_DATE__/${AS_OF_DATE}/g" \
    -e "s/__MARKET__/${MARKET_LABEL}/g" \
    -e "s/__BUY_SECTION__/${BUY_SECTION}/g" \
    -e "s/__BUY_RULE__/${BUY_RULE}/g" \
    -e "s/__FP_THRESHOLD__/${FP_THRESHOLD}/g" \
    "$SQL_TEMPLATE" > "$TMP_SQL"

  sqlite3 -header -column "$DB_PATH" < "$TMP_SQL" > "$TXT_OUT"

  sqlite3 -header -csv "$DB_PATH" < "$TMP_SQL" \
    | python3 -c "import csv, re, sys; out_path = sys.argv[1]; reader = csv.reader(sys.stdin); num_re = re.compile(r'^-?\\d+(?:\\.\\d+)?$'); f = open(out_path, 'w', newline='', encoding='utf-8'); writer = csv.writer(f, delimiter=';', lineterminator='\\n'); [writer.writerow([(v.replace('.', ',') if num_re.match(v) else v) for v in row]) for row in reader]; f.close()" "$CSV_OUT"

  printf '%s TXT: %s\n' "$MARKET_LABEL" "$TXT_OUT"
  printf '%s CSV: %s\n' "$MARKET_LABEL" "$CSV_OUT"
done
