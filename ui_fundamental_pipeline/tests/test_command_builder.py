import unittest
from pathlib import Path

from ui_fundamental_pipeline.command_builder import (
    UsaQuarterUpdateVintageOptions,
    UsaSecVintageRecoveryApplyOptions,
    UsaSecVintageRecoveryDryRunOptions,
    UsaYahooAwareRecoveryOptions,
    UsaYahooAwareApplyOptions,
    build_fin_classification_ttm_commands,
    build_fin_update_command,
    build_usa_result_check_command,
    build_score_percentile_command,
    build_snapshot_command,
    build_usa_update_command,
    build_usa_sec_vintage_recovery_apply_command,
    build_usa_sec_vintage_recovery_dry_run_command,
    build_usa_yahoo_aware_recovery_command,
    build_usa_yahoo_aware_apply_command,
    build_usa_vintage_preflight_command,
)
from ui_fundamental_pipeline.config import (
    OSAKEDATA_DB,
    SEC_LATEST_WRITER_VINTAGE_APPLY_APPROVAL_TOKEN,
    YAHOO_AWARE_APPLY_APPROVAL_TOKEN,
    get_vintage_run_id_usa,
)


class TestCommandBuilder(unittest.TestCase):
    def test_build_usa_update_command(self):
        command = build_usa_update_command("USA_QUARTER_UPDATE_2026-05-10__QUARTERLY")
        self.assertIn("--market", command)
        self.assertIn("usa", command)
        self.assertIn("--run-id", command)
        self.assertIn("--osakedata-db", command)
        self.assertIn(str(OSAKEDATA_DB), command)
        self.assertNotIn("--write-vintage", command)
        self.assertNotIn("--vintage-mode", command)
        self.assertNotIn("--vintage-yahoo-aware-action", command)
        self.assertNotIn("--quarter-refresh-plan-json", command)

    def test_build_usa_update_command_with_quarter_refresh_plan(self):
        command = build_usa_update_command(
            "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
            quarter_refresh_plan_json=Path("temp/fundamental_result_check/plan.json"),
        )

        self.assertIn("--quarter-refresh-plan-json", command)
        self.assertEqual(command[command.index("--quarter-refresh-plan-json") + 1], "temp/fundamental_result_check/plan.json")

    def test_build_usa_result_check_command(self):
        command = build_usa_result_check_command(decision_date="2026-08-07")

        self.assertIn("check_fundamental_new_results.py", " ".join(command))
        self.assertIn("--fundamentals-db", command)
        self.assertIn("fundamentals_usa.db", " ".join(command))
        self.assertIn("--ohlcv-db", command)
        self.assertEqual(command[command.index("--ohlcv-db") + 1], str(OSAKEDATA_DB))
        self.assertEqual(command[command.index("--decision-date") + 1], "2026-08-07")
        self.assertEqual(command[command.index("--ohlcv-stale-days") + 1], "14")
        self.assertEqual(command[command.index("--event-watch-days-after") + 1], "5")
        self.assertEqual(command[command.index("--calendar-confirmation-days-before") + 1], "7")
        self.assertEqual(command[command.index("--calendar-maintenance-limit") + 1], "50")
        self.assertEqual(command[command.index("--calendar-stale-days") + 1], "45")
        self.assertEqual(command[command.index("--calendar-failure-retry-days") + 1], "3")
        self.assertIn("--json", command)

    def test_build_usa_update_command_with_retired_vintage_options_remains_latest_only(self):
        source_run_id = "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY"
        launch_timestamp = "2026-05-10T12:00:00Z"
        vintage_run_id = get_vintage_run_id_usa(source_run_id)
        command = build_usa_update_command(
            source_run_id,
            vintage_options=UsaQuarterUpdateVintageOptions(
                launch_timestamp_utc=launch_timestamp,
                vintage_run_id=vintage_run_id,
            ),
        )

        self.assertNotIn("--write-vintage", command)
        self.assertNotIn("--vintage-mode", command)
        self.assertNotIn("--vintage-market", command)
        self.assertNotIn("--vintage-available-at-utc", command)
        self.assertNotIn("--vintage-ingested-at-utc", command)
        self.assertNotIn("--vintage-run-id", command)
        self.assertNotIn("--vintage-yahoo-aware-action", command)
        self.assertEqual(command[command.index("--osakedata-db") + 1], str(OSAKEDATA_DB))
        self.assertIn("2026-05-10", command[command.index("--run-id") + 1])

    def test_build_usa_vintage_preflight_command(self):
        command = build_usa_vintage_preflight_command()

        self.assertIn("-m", command)
        self.assertEqual(
            command[command.index("-m") + 1],
            "swingmaster.cli.preflight_quarter_update_vintage_readiness",
        )
        self.assertIn("--fundamentals-db", command)
        self.assertIn("fundamentals_usa.db", " ".join(command))
        self.assertEqual(command[command.index("--market") + 1], "usa")
        self.assertEqual(command[command.index("--format") + 1], "json")

    def test_build_usa_yahoo_aware_apply_command_without_approval_token(self):
        command = build_usa_yahoo_aware_apply_command(
            UsaYahooAwareApplyOptions(
                source_run_id="USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                vintage_run_id="USA_QUARTER_UPDATE_2026-05-10__YAHOO_AWARE_VINTAGE",
                launch_timestamp_utc="2026-05-10T12:00:00Z",
                approved=False,
            )
        )

        self.assertIn("-m", command)
        self.assertEqual(command[command.index("-m") + 1], "swingmaster.cli.apply_quarter_update_yahoo_aware_vintage")
        self.assertNotIn("run_fundamental_quarter_update", " ".join(command))
        self.assertNotIn("--vintage-yahoo-aware-action", command)
        self.assertNotIn("--approval-token", command)

    def test_build_usa_yahoo_aware_apply_command_with_approval_token(self):
        command = build_usa_yahoo_aware_apply_command(
            UsaYahooAwareApplyOptions(
                source_run_id="USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                vintage_run_id="USA_QUARTER_UPDATE_2026-05-10__YAHOO_AWARE_VINTAGE",
                launch_timestamp_utc="2026-05-10T12:00:00Z",
                approved=True,
            )
        )

        self.assertEqual(command[command.index("--market") + 1], "usa")
        self.assertEqual(command[command.index("--source-run-id") + 1], "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY")
        self.assertEqual(command[command.index("--vintage-run-id") + 1], "USA_QUARTER_UPDATE_2026-05-10__YAHOO_AWARE_VINTAGE")
        self.assertEqual(command[command.index("--available-at-utc") + 1], "2026-05-10T12:00:00Z")
        self.assertEqual(command[command.index("--ingested-at-utc") + 1], "2026-05-10T12:00:00Z")
        self.assertEqual(command[command.index("--approval-token") + 1], YAHOO_AWARE_APPLY_APPROVAL_TOKEN)
        self.assertNotIn("--provider", command)

    def test_build_usa_sec_vintage_recovery_dry_run_command(self):
        command = build_usa_sec_vintage_recovery_dry_run_command(
            UsaSecVintageRecoveryDryRunOptions(
                source_run_id="USA_QUARTER_UPDATE_2026-07-07__QUARTERLY",
                vintage_run_id="USA_PIT_VINTAGE_RECOVERY_2026-07-07__SEC_LATEST_WRITER_VINTAGE_RECOVERY",
                launch_timestamp_utc="2026-07-07T12:00:00Z",
            )
        )

        self.assertEqual(command[command.index("-m") + 1], "swingmaster.cli.dry_run_sec_vintage_for_missing_latest")
        self.assertEqual(command[command.index("--candidate-mode") + 1], "latest_writer")
        self.assertEqual(command[command.index("--source-run-id") + 1], "USA_QUARTER_UPDATE_2026-07-07__QUARTERLY")
        self.assertEqual(command[command.index("--format") + 1], "json")
        self.assertNotIn("run_fundamental_quarter_update", " ".join(command))
        self.assertNotIn("--approval-token", command)
        self.assertNotIn("--provider", command)

    def test_build_usa_sec_vintage_recovery_apply_command_with_approval_token(self):
        command = build_usa_sec_vintage_recovery_apply_command(
            UsaSecVintageRecoveryApplyOptions(
                source_run_id="USA_QUARTER_UPDATE_2026-07-07__QUARTERLY",
                vintage_run_id="USA_PIT_VINTAGE_RECOVERY_2026-07-07__SEC_LATEST_WRITER_VINTAGE_RECOVERY",
                launch_timestamp_utc="2026-07-07T12:00:00Z",
                expected_count=2,
                approved=True,
            )
        )

        self.assertEqual(command[command.index("-m") + 1], "swingmaster.cli.apply_sec_vintage_for_missing_latest")
        self.assertEqual(command[command.index("--source-run-id") + 1], "USA_QUARTER_UPDATE_2026-07-07__QUARTERLY")
        self.assertEqual(command[command.index("--candidate-mode") + 1], "latest_writer")
        self.assertEqual(command[command.index("--expected-count") + 1], "2")
        self.assertEqual(command[command.index("--approval-token") + 1], SEC_LATEST_WRITER_VINTAGE_APPLY_APPROVAL_TOKEN)
        self.assertNotIn("run_fundamental_quarter_update", " ".join(command))
        self.assertNotIn("--provider", command)

    def test_build_usa_yahoo_aware_recovery_dry_run_command(self):
        command = build_usa_yahoo_aware_recovery_command(
            UsaYahooAwareRecoveryOptions(
                source_run_id="USA_QUARTER_UPDATE_2026-07-07__QUARTERLY",
                vintage_run_id="USA_QUARTER_UPDATE_2026-07-07__YAHOO_AWARE_VINTAGE_RECOVERY",
                launch_timestamp_utc="2026-07-07T12:00:00Z",
                dry_run=True,
            )
        )

        self.assertEqual(command[command.index("-m") + 1], "swingmaster.cli.apply_quarter_update_yahoo_aware_vintage")
        self.assertIn("--dry-run", command)
        self.assertEqual(command[command.index("--format") + 1], "json")
        self.assertNotIn("--approval-token", command)
        self.assertNotIn("run_fundamental_quarter_update", " ".join(command))
        self.assertNotIn("--provider", command)

    def test_build_usa_yahoo_aware_recovery_apply_command_with_expected_counts(self):
        command = build_usa_yahoo_aware_recovery_command(
            UsaYahooAwareRecoveryOptions(
                source_run_id="USA_QUARTER_UPDATE_2026-07-07__QUARTERLY",
                vintage_run_id="USA_QUARTER_UPDATE_2026-07-07__YAHOO_AWARE_VINTAGE_RECOVERY",
                launch_timestamp_utc="2026-07-07T12:00:00Z",
                expected_final_mixed_count=2,
                expected_yahoo_vintage_count=0,
                approved=True,
            )
        )

        self.assertNotIn("--dry-run", command)
        self.assertEqual(command[command.index("--expected-final-mixed-count") + 1], "2")
        self.assertEqual(command[command.index("--expected-yahoo-vintage-count") + 1], "0")
        self.assertIn("--approval-token", command)
        self.assertNotIn("run_fundamental_quarter_update", " ".join(command))
        self.assertNotIn("--provider", command)

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
