from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.error import HTTPError

from swingmaster.cli import run_cboe_pcr_fetch


class _Response(io.BytesIO):
    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def test_run_writes_timestamped_csv(tmp_path: Path, monkeypatch) -> None:
    payloads = {
        "2026-03-10": {
            "ratios": [
                {"name": "TOTAL PUT/CALL RATIO", "value": "0.94"},
                {"name": "INDEX PUT/CALL RATIO", "value": "1.07"},
                {"name": "EQUITY PUT/CALL RATIO", "value": "0.57"},
            ]
        },
        "2026-03-11": {
            "ratios": [
                {"name": "TOTAL PUT/CALL RATIO", "value": "1.05"},
                {"name": "INDEX PUT/CALL RATIO", "value": "1.01"},
                {"name": "EQUITY PUT/CALL RATIO", "value": "0.80"},
            ]
        },
    }

    def fake_urlopen(request, timeout=30):
        url = request.full_url
        day = url.split("/")[-1].replace("_daily_options", "")
        if day not in payloads:
            raise HTTPError(url, 403, "Forbidden", hdrs=None, fp=None)
        return _Response(str(payloads[day]).replace("'", '"').encode("utf-8"))

    sleep_calls: list[float] = []

    monkeypatch.setattr(run_cboe_pcr_fetch, "urlopen", fake_urlopen)
    monkeypatch.setattr(run_cboe_pcr_fetch.time, "sleep", lambda seconds: sleep_calls.append(seconds))
    fetched_at = datetime(2026, 3, 13, 9, 15, 0, tzinfo=timezone.utc)

    output_path = run_cboe_pcr_fetch.run(
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 12),
        output_dir=tmp_path,
        fetched_at=fetched_at,
        sleep_seconds=0.5,
    )

    assert output_path.name == "cboe_pcr_2026-03-10_2026-03-12_20260313T091500Z.csv"
    assert sleep_calls == [0.5, 0.5]
    rows = list(csv.DictReader(output_path.open(newline="", encoding="utf-8")))
    assert rows == [
        {
            "date": "2026-03-10",
            "total_put_call_ratio": "0.94",
            "index_put_call_ratio": "1.07",
            "equity_put_call_ratio": "0.57",
            "status": "ok",
            "fetched_at_utc": "2026-03-13T09:15:00Z",
        },
        {
            "date": "2026-03-11",
            "total_put_call_ratio": "1.05",
            "index_put_call_ratio": "1.01",
            "equity_put_call_ratio": "0.80",
            "status": "ok",
            "fetched_at_utc": "2026-03-13T09:15:00Z",
        },
    ]


def test_run_rejects_inverted_range(tmp_path: Path) -> None:
    fetched_at = datetime(2026, 3, 13, 9, 15, 0, tzinfo=timezone.utc)
    try:
        run_cboe_pcr_fetch.run(
            start_date=date(2026, 3, 12),
            end_date=date(2026, 3, 10),
            output_dir=tmp_path,
            fetched_at=fetched_at,
        )
    except ValueError as exc:
        assert str(exc) == "end-date must be on or after start-date"
    else:
        raise AssertionError("Expected ValueError for inverted date range")


def test_run_rejects_negative_sleep(tmp_path: Path) -> None:
    fetched_at = datetime(2026, 3, 13, 9, 15, 0, tzinfo=timezone.utc)
    try:
        run_cboe_pcr_fetch.run(
            start_date=date(2026, 3, 10),
            end_date=date(2026, 3, 12),
            output_dir=tmp_path,
            fetched_at=fetched_at,
            sleep_seconds=-0.1,
        )
    except ValueError as exc:
        assert str(exc) == "sleep-seconds must be non-negative"
    else:
        raise AssertionError("Expected ValueError for negative sleep")
