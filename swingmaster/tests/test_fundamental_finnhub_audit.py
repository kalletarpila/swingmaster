from __future__ import annotations

import io
import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_finnhub_audit
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.providers import finnhub


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_missing_finnhub_api_key_raises_clear_error(monkeypatch) -> None:
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="^FINNHUB_API_KEY_MISSING$"):
        finnhub.FinnhubClient()


def test_finnhub_client_financials_reported_uses_symbol_and_freq(monkeypatch) -> None:
    monkeypatch.setenv("FINNHUB_API_KEY", "token-1")
    captured_url: list[str] = []

    def _fake_urlopen(request):
        captured_url.append(request.full_url)
        return _FakeResponse({"data": []})

    monkeypatch.setattr(finnhub, "urlopen", _fake_urlopen)

    client = finnhub.FinnhubClient()
    payload = client.get_financials_reported("NOKIA.HE", "quarterly")

    assert payload == {"data": []}
    assert captured_url == [
        "https://finnhub.io/api/v1/stock/financials-reported?symbol=NOKIA.HE&freq=quarterly&token=token-1"
    ]


def test_deterministic_symbol_sorting() -> None:
    assert run_fundamental_finnhub_audit.normalize_symbols("KONE.HE,NOKIA.HE,AAK.HE") == [
        "AAK.HE",
        "KONE.HE",
        "NOKIA.HE",
    ]


def test_payload_hash_is_stable_for_different_key_order() -> None:
    payload_left = {"data": [{"report": {"bs": [{"value": 1, "concept": "Cash"}]}, "symbol": "NOKIA.HE"}]}
    payload_right = {"data": [{"symbol": "NOKIA.HE", "report": {"bs": [{"concept": "Cash", "value": 1}]}}]}

    assert run_fundamental_finnhub_audit.compute_payload_hash(payload_left) == (
        run_fundamental_finnhub_audit.compute_payload_hash(payload_right)
    )


def test_status_classification_ok_empty_error_and_dry_run(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "finnhub_audit.db"
    run_migration(db_path)
    monkeypatch.setenv("FINNHUB_API_KEY", "token-1")

    class _FakeFinnhubClient:
        def get_financials_reported(self, symbol: str, freq: str = "quarterly") -> dict:
            assert freq == "quarterly"
            if symbol == "AAA.HE":
                return {"data": [{"report": {"bs": [{"concept": "Cash", "value": 10}]}}]}
            if symbol == "BBB.HE":
                return {"data": []}
            raise RuntimeError(f"FETCH_FAILED:{symbol}")

    monkeypatch.setattr(run_fundamental_finnhub_audit, "FinnhubClient", lambda: _FakeFinnhubClient())

    summary = run_fundamental_finnhub_audit.run_finnhub_audit(
        db_path=db_path,
        market="omxh",
        exchange="HE",
        symbols_arg="BBB.HE,CCC.HE,AAA.HE",
        limit=None,
        run_id="RUN1",
        dry_run=True,
    )

    assert summary == {
        "market": "omxh",
        "exchange": "HE",
        "symbols_total": 3,
        "symbols_processed": 3,
        "ok_count": 1,
        "empty_count": 1,
        "error_count": 1,
        "rows_written": 0,
        "dry_run": "true",
        "run_id": "RUN1",
    }

    with sqlite3.connect(str(db_path)) as conn:
        row_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_finnhub_raw").fetchone()[0]
    assert row_count == 0


def test_status_classification_persists_rows(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "finnhub_audit_persist.db"
    run_migration(db_path)
    monkeypatch.setenv("FINNHUB_API_KEY", "token-1")

    class _FakeFinnhubClient:
        def get_financials_reported(self, symbol: str, freq: str = "quarterly") -> dict:
            if symbol == "NOKIA.HE":
                return {"data": [{"report": {"ic": [{"concept": "Revenue", "value": 100}]}}]}
            return {"data": []}

    monkeypatch.setattr(run_fundamental_finnhub_audit, "FinnhubClient", lambda: _FakeFinnhubClient())

    summary = run_fundamental_finnhub_audit.run_finnhub_audit(
        db_path=db_path,
        market="omxh",
        exchange="HE",
        symbols_arg=None,
        limit=None,
        run_id="RUN2",
        dry_run=False,
    )

    assert summary["symbols_total"] == 1
    assert summary["symbols_processed"] == 1
    assert summary["ok_count"] == 1
    assert summary["rows_written"] == 1

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT symbol, status, run_id, payload_json, error_message
            FROM rc_fundamental_finnhub_raw
            ORDER BY id
            """
        ).fetchall()
    assert rows == [
        (
            "NOKIA.HE",
            "OK",
            "RUN2",
            run_fundamental_finnhub_audit.canonical_json_dumps(
                {"data": [{"report": {"ic": [{"concept": "Revenue", "value": 100}]}}]}
            ),
            None,
        )
    ]


def test_cli_summary_output(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "finnhub_audit_cli.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_finnhub_audit,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="omxh",
            exchange="HE",
            symbols=None,
            limit=None,
            run_id="RUN3",
            dry_run=True,
        ),
    )
    monkeypatch.setattr(
        run_fundamental_finnhub_audit,
        "run_finnhub_audit",
        lambda **kwargs: {
            "market": "omxh",
            "exchange": "HE",
            "symbols_total": 1,
            "symbols_processed": 1,
            "ok_count": 0,
            "empty_count": 1,
            "error_count": 0,
            "rows_written": 0,
            "dry_run": "true",
            "run_id": "RUN3",
        },
    )

    run_fundamental_finnhub_audit.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY market=omxh",
        "SUMMARY exchange=HE",
        "SUMMARY symbols_total=1",
        "SUMMARY symbols_processed=1",
        "SUMMARY ok_count=0",
        "SUMMARY empty_count=1",
        "SUMMARY error_count=0",
        "SUMMARY rows_written=0",
        "SUMMARY dry_run=true",
        "SUMMARY run_id=RUN3",
    ]
