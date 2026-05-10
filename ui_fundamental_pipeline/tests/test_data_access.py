import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ui_fundamental_pipeline.data_access as data_access


class TestDataAccess(unittest.TestCase):
    def test_resolve_latest_close_as_of_date(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "osakedata.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE osakedata (market TEXT, pvm TEXT, close REAL)")
                conn.execute("INSERT INTO osakedata VALUES ('usa', '2026-05-07', 1.0)")
                conn.execute("INSERT INTO osakedata VALUES ('usa', '2026-05-08', 1.1)")
                conn.execute("INSERT INTO osakedata VALUES ('usa', '2026-05-09', NULL)")
                conn.commit()

            with patch.object(data_access, "OSAKEDATA_DB", db_path):
                result = data_access.resolve_latest_close_as_of_date("usa")
            self.assertEqual(result, "2026-05-08")

    def test_resolve_latest_close_as_of_date_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "osakedata.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE osakedata (market TEXT, pvm TEXT, close REAL)")
                conn.commit()

            with patch.object(data_access, "OSAKEDATA_DB", db_path):
                with self.assertRaises(RuntimeError):
                    data_access.resolve_latest_close_as_of_date("omxh")

    def test_load_valid_tickers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "fundamentals.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE rc_fundamental_ttm (ticker TEXT)")
                conn.execute("INSERT INTO rc_fundamental_ttm VALUES ('aapl')")
                conn.execute("INSERT INTO rc_fundamental_ttm VALUES ('AAPL')")
                conn.execute("INSERT INTO rc_fundamental_ttm VALUES ('MSFT')")
                conn.commit()

            tickers = data_access.load_valid_tickers(db_path)
            self.assertEqual(tickers, {"AAPL", "MSFT"})


if __name__ == "__main__":
    unittest.main()
