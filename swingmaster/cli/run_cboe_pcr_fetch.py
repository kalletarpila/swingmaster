from __future__ import annotations

import argparse
import csv
import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = "https://cdn.cboe.com/data/us/options/market_statistics/daily"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "cboe"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
TARGETS = {
    "TOTAL PUT/CALL RATIO": "total_put_call_ratio",
    "INDEX PUT/CALL RATIO": "index_put_call_ratio",
    "EQUITY PUT/CALL RATIO": "equity_put_call_ratio",
}


@dataclass(frozen=True)
class FetchRow:
    date: str
    total_put_call_ratio: str
    index_put_call_ratio: str
    equity_put_call_ratio: str
    status: str
    fetched_at_utc: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch CBOE Daily Market Statistics put/call ratios into a timestamped CSV file."
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Directory for the CSV output (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.5,
        help="Delay between requests in seconds (default: 0.5)",
    )
    return parser.parse_args()


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO date: {value}") from exc


def iter_dates(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    current = start_date
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)
    return days


def build_output_path(output_dir: Path, start_date: date, end_date: date, fetched_at: datetime) -> Path:
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    file_name = f"cboe_pcr_{start_date.isoformat()}_{end_date.isoformat()}_{stamp}.csv"
    return output_dir / file_name


def fetch_daily_payload(day: date) -> dict[str, object]:
    url = f"{BASE_URL}/{day.isoformat()}_daily_options"
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def fetch_row(day: date, fetched_at: datetime) -> FetchRow:
    ratio_values = {
        "total_put_call_ratio": "",
        "index_put_call_ratio": "",
        "equity_put_call_ratio": "",
    }
    status = "ok"
    fetched_at_utc = fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        payload = fetch_daily_payload(day)
        ratios = {item["name"]: item["value"] for item in payload.get("ratios", [])}
        for source_name, target_name in TARGETS.items():
            ratio_values[target_name] = str(ratios.get(source_name, ""))
        if not any(ratio_values.values()):
            status = "missing_ratios"
    except HTTPError as exc:
        status = f"http_{exc.code}"
    except URLError as exc:
        status = f"url_error:{exc.reason}"
    except Exception as exc:  # pragma: no cover - defensive CLI path
        status = f"error:{type(exc).__name__}"

    return FetchRow(
        date=day.isoformat(),
        total_put_call_ratio=ratio_values["total_put_call_ratio"],
        index_put_call_ratio=ratio_values["index_put_call_ratio"],
        equity_put_call_ratio=ratio_values["equity_put_call_ratio"],
        status=status,
        fetched_at_utc=fetched_at_utc,
    )


def write_csv(rows: list[FetchRow], output_path: Path) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "date",
                "total_put_call_ratio",
                "index_put_call_ratio",
                "equity_put_call_ratio",
                "status",
                "fetched_at_utc",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def collect_rows(start_date: date, end_date: date, fetched_at: datetime, sleep_seconds: float) -> list[FetchRow]:
    rows: list[FetchRow] = []
    days = iter_dates(start_date, end_date)
    for index, day in enumerate(days):
        row = fetch_row(day, fetched_at)
        if row.status != "http_403":
            rows.append(row)
        if sleep_seconds > 0 and index < len(days) - 1:
            time.sleep(sleep_seconds)
    return rows


def run(
    start_date: date,
    end_date: date,
    output_dir: Path,
    fetched_at: datetime,
    sleep_seconds: float = 0.5,
) -> Path:
    if end_date < start_date:
        raise ValueError("end-date must be on or after start-date")
    if sleep_seconds < 0:
        raise ValueError("sleep-seconds must be non-negative")
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = collect_rows(start_date, end_date, fetched_at, sleep_seconds)
    output_path = build_output_path(output_dir, start_date, end_date, fetched_at)
    write_csv(rows, output_path)
    return output_path


def main() -> None:
    args = parse_args()
    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)
    fetched_at = datetime.now(timezone.utc)
    output_path = run(
        start_date,
        end_date,
        Path(args.output_dir),
        fetched_at,
        sleep_seconds=args.sleep_seconds,
    )
    print(f"WROTE {output_path}")


if __name__ == "__main__":
    main()
