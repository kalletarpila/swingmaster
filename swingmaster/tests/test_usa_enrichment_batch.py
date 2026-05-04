from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from swingmaster.cli import run_usa_enrichment_batch


def test_run_id_derivation_is_correct() -> None:
    assert run_usa_enrichment_batch.derive_child_run_ids("USA_ENRICH_20260504") == {
        "raw": "USA_ENRICH_20260504__RAW",
        "quarterly": "USA_ENRICH_20260504__QTR",
        "enrichment": "USA_ENRICH_20260504__ENRICH",
    }


def test_cli_argument_parsing_works(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_usa_enrichment_batch.py",
            "--db",
            "fundamentals_usa.db",
            "--run-id",
            "BASE",
            "--dry-run",
            "--limit-tickers",
            "5",
            "--tickers",
            "LRCX,AAPL",
            "--skip-raw",
        ],
    )
    args = run_usa_enrichment_batch.parse_args()
    assert args.db == "fundamentals_usa.db"
    assert args.run_id == "BASE"
    assert args.dry_run is True
    assert args.limit_tickers == 5
    assert args.tickers == "LRCX,AAPL"
    assert args.skip_raw is True
    assert args.skip_quarterly is False
    assert args.skip_enrichment is False


def test_dry_run_prints_commands_without_execution(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], cwd: str | None = None) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(run_usa_enrichment_batch.subprocess, "run", _fake_run)

    summary = run_usa_enrichment_batch.run_usa_enrichment_batch(
        db_path=tmp_path / "fundamentals_usa.db",
        run_id="BASE",
        dry_run=True,
        limit_tickers=5,
        tickers_arg="LRCX,AAPL",
        skip_raw=False,
        skip_quarterly=False,
        skip_enrichment=False,
    )
    out = capsys.readouterr().out

    assert calls == []
    assert "run_fundamental_yahoo_audit.py" in out
    assert "run_fundamental_yahoo_quarterly_write.py" in out
    assert "run_fundamental_yahoo_fallback_enrich.py" in out
    assert "SUMMARY dry_run=1" in out
    assert summary["dry_run"] == 1
    assert summary["raw_step_executed"] == 0
    assert summary["quarterly_step_executed"] == 0
    assert summary["enrichment_step_executed"] == 0


def test_skip_flags_work(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], cwd: str | None = None) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(run_usa_enrichment_batch.subprocess, "run", _fake_run)

    summary = run_usa_enrichment_batch.run_usa_enrichment_batch(
        db_path=tmp_path / "fundamentals_usa.db",
        run_id="BASE",
        dry_run=False,
        limit_tickers=None,
        tickers_arg="LRCX",
        skip_raw=True,
        skip_quarterly=True,
        skip_enrichment=False,
    )
    out = capsys.readouterr().out

    assert len(calls) == 1
    assert "run_fundamental_yahoo_fallback_enrich.py" in " ".join(calls[0])
    assert "STEP raw=SKIPPED" in out
    assert "STEP quarterly=SKIPPED" in out
    assert "STEP enrichment=OK" in out
    assert summary["raw_step_executed"] == 0
    assert summary["quarterly_step_executed"] == 0
    assert summary["enrichment_step_executed"] == 1


def test_subprocess_failure_propagates_correctly(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], cwd: str | None = None) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        if any("run_fundamental_yahoo_quarterly_write.py" in part for part in cmd):
            return subprocess.CompletedProcess(cmd, 9)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(run_usa_enrichment_batch.subprocess, "run", _fake_run)
    monkeypatch.setattr(run_usa_enrichment_batch, "load_raw_symbols", lambda *_args, **_kwargs: ["LRCX"])
    monkeypatch.setattr(
        run_usa_enrichment_batch,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db": str(tmp_path / "fundamentals_usa.db"),
                "run_id": "BASE",
                "dry_run": False,
                "limit_tickers": None,
                "tickers": None,
                "skip_raw": False,
                "skip_quarterly": False,
                "skip_enrichment": False,
            },
        )(),
    )

    with pytest.raises(SystemExit) as exc:
        run_usa_enrichment_batch.main()
    out = capsys.readouterr().out

    assert exc.value.code == 9
    assert "STEP quarterly=FAILED" in out
    assert "ERROR step=quarterly" in out
    assert len(calls) == 2


def test_quarterly_step_uses_loaded_symbols_when_no_tickers(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], cwd: str | None = None) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(run_usa_enrichment_batch.subprocess, "run", _fake_run)
    monkeypatch.setattr(run_usa_enrichment_batch, "load_raw_symbols", lambda *_args, **_kwargs: ["AAPL", "LRCX"])

    run_usa_enrichment_batch.run_usa_enrichment_batch(
        db_path=tmp_path / "fundamentals_usa.db",
        run_id="BASE",
        dry_run=False,
        limit_tickers=2,
        tickers_arg=None,
        skip_raw=True,
        skip_quarterly=False,
        skip_enrichment=True,
    )
    out = capsys.readouterr().out

    assert len(calls) == 2
    assert "--symbol AAPL" in out
    assert "--symbol LRCX" in out
