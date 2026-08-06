from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

from swingmaster.cli.rebuild_net_debt_to_ebit import rebuild_net_debt_to_ebit


def test_rebuild_net_debt_to_ebit_dry_run_apply_and_idempotency(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_legacy.db"
    _create_legacy_db(db_path)
    output_root = Path("temp") / "net_debt_to_ebit_migration" / f"pytest_{tmp_path.name}"
    shutil.rmtree(output_root, ignore_errors=True)

    try:
        dry_run = rebuild_net_debt_to_ebit(
            db_path,
            output_root=output_root / "dry_run",
            backup_path=output_root / "dry_run" / "backups" / "fundamentals.bak",
            apply_mode=False,
            representative_tickers=["AAPL"],
        )
        assert dry_run["summary"]["schema_has_net_debt_to_ebit"] is False
        assert dry_run["summary"]["metric_updates"] == 0
        assert _has_column(db_path, "rc_fundamental_ttm", "net_debt_to_ebit") is False
        assert dry_run["representative_rows"][0]["new_leverage_component"] == 4.0
        assert dry_run["representative_rows"][0]["new_total_score"] is not None

        backup_path = output_root / "apply" / "backups" / "fundamentals.bak"
        applied = rebuild_net_debt_to_ebit(
            db_path,
            output_root=output_root / "apply",
            backup_path=backup_path,
            apply_mode=True,
            representative_tickers=["AAPL"],
        )
        assert applied["summary"]["schema_has_net_debt_to_ebit"] is True
        assert applied["summary"]["metric_updates"] == 1
        assert applied["summary"]["score_updates"] == 0
        assert applied["summary"]["deprecated_metric_unchanged"] is True
        assert applied["summary"]["quick_check"] == "ok"
        assert backup_path.exists()
        first_backup_size = backup_path.stat().st_size

        with sqlite3.connect(str(db_path)) as conn:
            ratio, deprecated = conn.execute(
                "SELECT net_debt_to_ebit, net_debt_to_ebitda FROM rc_fundamental_ttm WHERE ticker='AAPL'"
            ).fetchone()
        assert ratio == 3.0
        assert deprecated == 3.0

        second_apply = rebuild_net_debt_to_ebit(
            db_path,
            output_root=output_root / "apply_again",
            backup_path=backup_path,
            apply_mode=True,
            representative_tickers=["AAPL"],
        )
        assert second_apply["summary"]["metric_updates"] == 0
        assert second_apply["summary"]["score_updates"] == 0
        assert backup_path.stat().st_size == first_backup_size
    finally:
        shutil.rmtree(output_root, ignore_errors=True)


def _create_legacy_db(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_fundamental_schema_version (
                version INTEGER PRIMARY KEY,
                applied_at_utc TEXT NOT NULL
            );
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                ebitda REAL,
                cash REAL,
                total_debt REAL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, period_end_date)
            );
            CREATE TABLE rc_fundamental_ttm (
                ticker TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                latest_period_end_date TEXT NOT NULL,
                revenue_growth_ttm_yoy REAL,
                ebit_ttm REAL,
                ebit_margin_ttm REAL,
                ebit_margin_trend_4q REAL,
                fcf_margin_ttm REAL,
                net_debt REAL,
                net_debt_to_ebitda REAL,
                share_dilution_yoy REAL,
                lifecycle_class TEXT,
                fundamental_score REAL,
                fundamental_score_lifecycle REAL,
                leverage_component REAL,
                leverage_component_lifecycle REAL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, as_of_date)
            );
            INSERT INTO rc_fundamental_quarterly (
                ticker, period_end_date, ebitda, cash, total_debt, run_id
            ) VALUES ('AAPL', '2024-12-31', 999.0, 30.0, 90.0, 'Q_RUN');
            INSERT INTO rc_fundamental_ttm (
                ticker, as_of_date, latest_period_end_date,
                revenue_growth_ttm_yoy, ebit_ttm, ebit_margin_ttm, ebit_margin_trend_4q,
                fcf_margin_ttm, net_debt, net_debt_to_ebitda, share_dilution_yoy,
                lifecycle_class, fundamental_score, fundamental_score_lifecycle,
                leverage_component, leverage_component_lifecycle, run_id
            ) VALUES (
                'AAPL', '2024-12-31', '2024-12-31',
                0.10, 20.0, 0.20, 0.01,
                0.12, 60.0, 3.0, 0.0,
                'MATURE', 50.0, 50.0,
                4.0, 4.0, 'TTM_RUN'
            );
            """
        )


def _has_column(db_path: Path, table: str, column: str) -> bool:
    with sqlite3.connect(str(db_path)) as conn:
        return any(str(row[1]) == column for row in conn.execute(f"PRAGMA table_info({table})"))
