from __future__ import annotations

from pathlib import Path

from swingmaster.cli import run_weekly_report


def test_weekly_report_empty_output_includes_buy_badges_column(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.setattr(
        run_weekly_report,
        "parse_args",
        lambda: type("Args", (), {"date": "2026-01-09", "out_dir": str(tmp_path)})(),
    )
    monkeypatch.setattr(run_weekly_report, "fetch_recent_trading_dates", lambda *_args, **_kwargs: [])
    run_weekly_report.main()

    csv_path = tmp_path / "weekly_report_2026-01-09.csv"
    txt_path = tmp_path / "weekly_report_2026-01-09.txt"

    assert csv_path.exists()
    assert txt_path.exists()

    csv_text = csv_path.read_text(encoding="utf-8")
    txt_text = txt_path.read_text(encoding="utf-8")
    captured = capsys.readouterr()

    assert "buy_badges" in csv_text.splitlines()[0]
    assert "buy_badges" in txt_text.splitlines()[0]
    assert "WEEKLY CSV:" in captured.out
