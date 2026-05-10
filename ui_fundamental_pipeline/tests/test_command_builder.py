import unittest

from ui_fundamental_pipeline.command_builder import (
    build_fin_update_command,
    build_score_percentile_command,
    build_snapshot_command,
    build_usa_update_command,
)


class TestCommandBuilder(unittest.TestCase):
    def test_build_usa_update_command(self):
        command = build_usa_update_command("USA_QUARTER_UPDATE_2026-05-10")
        self.assertIn("--market", command)
        self.assertIn("usa", command)
        self.assertIn("--run-id", command)

    def test_build_fin_update_command(self):
        command = build_fin_update_command("FIN_YAHOO_BATCH_2026-05-10")
        self.assertIn("--replace-symbol", command)
        self.assertIn("--osakedata-db", command)

    def test_build_percentile_command_usa(self):
        command = build_score_percentile_command("usa", "USA_PERCENTILE_2026-05-08", "2026-05-08")
        self.assertIn("--as-of-date", command)
        self.assertIn("2026-05-08", command)
        self.assertIn("--market", command)
        self.assertIn("usa", command)

    def test_build_snapshot_command_all_flags(self):
        command = build_snapshot_command("fin", ["TYRES.HE"], "2026-05-08")
        required_flags = [
            "--price-behavior-snapshot",
            "--dow-structure-snapshot",
            "--candlestick-snapshot",
            "--divergence-snapshot",
            "--moving-average-snapshot",
            "--output-dir",
            "--quarters",
            "4",
        ]
        for flag in required_flags:
            self.assertIn(flag, command)


if __name__ == "__main__":
    unittest.main()
