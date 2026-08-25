from __future__ import annotations

import importlib
import sqlite3


def test_run_range_universe_imports_without_optional_ml_dependencies() -> None:
    module = importlib.import_module("swingmaster.cli.run_range_universe")

    assert callable(module.maybe_run_dual_score_production)


def test_disabled_dual_score_path_does_not_require_optional_ml_dependencies() -> None:
    module = importlib.import_module("swingmaster.cli.run_range_universe")
    conn = sqlite3.connect(":memory:")
    try:
        result = module.maybe_run_dual_score_production(
            conn,
            enabled=False,
            osakedata_db_path="/nonexistent/osakedata.db",
            mode="upsert",
        )
    finally:
        conn.close()

    assert result == (0, 0, 0)
