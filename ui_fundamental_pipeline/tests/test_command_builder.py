import unittest

from ui_fundamental_pipeline.command_builder import (
    build_fin_classification_ttm_commands,
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
        self.assertIn("--osakedata-db", command)
        self.assertIn("--market", command)
        self.assertIn("omxh", command)
        self.assertIn("fundamentals_fin.db", " ".join(command))
        self.assertIn("osakedata.db", " ".join(command))
        self.assertIn("run_fundamental_quarter_update.py", " ".join(command))
        self.assertNotIn("--replace-symbol", command)
        self.assertNotIn("run_fundamental_yahoo_batch_fin.py", " ".join(command))

    def test_build_percentile_command_usa(self):
        command = build_score_percentile_command("usa", "USA_PERCENTILE_2026-05-08", "2026-05-08")
        self.assertIn("--as-of-date", command)
        self.assertIn("2026-05-08", command)
        self.assertIn("--market", command)
        self.assertIn("usa", command)

    def test_build_fin_classification_ttm_commands(self):
        commands = build_fin_classification_ttm_commands(
            as_of_date="2026-05-29",
            classification_run_id="OMXH_REPORTING_FREQ_2026_05_29",
            ttm_run_id="OMXH_TTM_2026_05_29",
            recovery_run_id="OMXH_MISSING_PERIOD_RECOVERY_2026_05_29",
        )
        self.assertEqual(len(commands), 3)

        classification_command, ttm_command, recovery_command = commands

        self.assertIn("run_fundamental_reporting_frequency_audit.py", " ".join(classification_command))
        self.assertIn("--market", classification_command)
        self.assertIn("omxh", classification_command)
        self.assertIn("--write-db", classification_command)
        self.assertIn("--as-of-date", classification_command)
        self.assertIn("2026-05-29", classification_command)
        self.assertIn("--run-id", classification_command)
        self.assertIn("OMXH_REPORTING_FREQ_2026_05_29", classification_command)
        self.assertIn("--write-mode", classification_command)
        self.assertIn("replace-run", classification_command)

        self.assertIn("run_fundamental_ttm_batch.py", " ".join(ttm_command))
        self.assertIn("--market", ttm_command)
        self.assertIn("omxh", ttm_command)
        self.assertIn("--classification-run-id", ttm_command)
        self.assertIn("OMXH_REPORTING_FREQ_2026_05_29", ttm_command)
        self.assertIn("--run-id", ttm_command)
        self.assertIn("OMXH_TTM_2026_05_29", ttm_command)

        self.assertIn("run_fundamental_missing_period_recovery_check.py", " ".join(recovery_command))
        self.assertIn("--market", recovery_command)
        self.assertIn("omxh", recovery_command)
        self.assertIn("--classification-run-id", recovery_command)
        self.assertIn("OMXH_REPORTING_FREQ_2026_05_29", recovery_command)
        self.assertIn("--write-db", recovery_command)
        self.assertIn("--run-id", recovery_command)
        self.assertIn("OMXH_MISSING_PERIOD_RECOVERY_2026_05_29", recovery_command)
        self.assertIn("--write-mode", recovery_command)
        self.assertIn("replace-run", recovery_command)
        self.assertIn("--output", recovery_command)
        self.assertIn("temp/omxh_missing_period_recovery_2026_05_29.csv", " ".join(recovery_command))

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
