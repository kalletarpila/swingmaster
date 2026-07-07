import unittest
from unittest.mock import MagicMock, Mock, patch

import flet as ft

from ui_fundamental_pipeline.config import FUNDAMENTALS_USA_DB, OSAKEDATA_DB
from ui_fundamental_pipeline.executor import ProcessExecutor
from ui_fundamental_pipeline.main import SwingMasterApp
from ui_fundamental_pipeline.vintage_status import map_vintage_completion_status_to_ui_severity


class FakeExecutor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.commands = []

    def execute(self, command, on_output, on_summary, cwd=None):
        self.commands.append(command)
        exit_code, summary = self.responses.pop(0)
        if summary:
            on_summary(summary)
        return exit_code, []


def _mock_page():
    page = MagicMock(spec=ft.Page)
    page.title = ""
    page.window_width = 1200
    page.window_height = 800
    page.padding = 10
    page.add = Mock()
    page.update = Mock()
    page.run_thread = Mock()
    return page


class TestUiQuarterUpdateVintageSmoke(unittest.TestCase):
    def setUp(self):
        self.snapshot_refresh_patcher = patch(
            "ui_fundamental_pipeline.components.snapshot_browser.SnapshotBrowser.refresh_file_list"
        )
        self.valid_tickers_patcher = patch(
            "ui_fundamental_pipeline.components.market_panel.load_valid_tickers",
            return_value=set(),
        )
        self.popen_patcher = patch(
            "ui_fundamental_pipeline.executor.subprocess.Popen",
            side_effect=AssertionError("subprocess.Popen must not run in no-provider smoke tests"),
        )
        self.snapshot_refresh_patcher.start()
        self.valid_tickers_patcher.start()
        self.popen_patcher.start()

    def tearDown(self):
        self.popen_patcher.stop()
        self.valid_tickers_patcher.stop()
        self.snapshot_refresh_patcher.stop()

    def _app(self):
        return SwingMasterApp(_mock_page())

    def test_checkbox_off_uses_default_single_command_without_vintage(self):
        app = self._app()
        app.usa_panel.vintage_write_checkbox.value = False
        captured = {}

        app._run_in_background = lambda target: captured.setdefault("target", target)
        with patch.object(app, "_execute_single_command") as execute_single:
            app._run_usa_update()
            captured["target"]()

        execute_single.assert_called_once()
        command, status_prefix, market = execute_single.call_args[0]
        self.assertEqual(status_prefix, "USA Quarter Update")
        self.assertEqual(market, "usa")
        self.assertNotIn("preflight_quarter_update_vintage_readiness", " ".join(command))
        self.assertNotIn("--write-vintage", command)
        self.assertNotIn("--vintage-mode", command)
        self.assertNotIn("--vintage-yahoo-aware-action", command)

    def test_checkbox_on_builds_preflight_first_and_exact_vintage_flags(self):
        app = self._app()
        app.usa_panel.vintage_write_checkbox.value = True
        captured = {}

        app._run_in_background = lambda target: captured.setdefault("target", target)
        with patch("ui_fundamental_pipeline.main.get_run_id_usa", return_value="USA_QUARTER_UPDATE_2026-05-10__QUARTERLY"):
            with patch("ui_fundamental_pipeline.main.get_utc_launch_timestamp", return_value="2026-05-10T12:00:00Z"):
                with patch.object(app, "_execute_command_chain") as execute_chain:
                    app._run_usa_update()
                    captured["target"]()

        execute_chain.assert_called_once()
        commands, status_prefix, market = execute_chain.call_args[0]
        self.assertEqual(status_prefix, "USA Quarter Update")
        self.assertEqual(market, "usa")
        self.assertEqual(len(commands), 2)

        preflight_command, quarter_update_command = commands
        self.assertEqual(preflight_command[1], "-m")
        self.assertEqual(
            preflight_command[2],
            "swingmaster.cli.preflight_quarter_update_vintage_readiness",
        )
        self.assertEqual(preflight_command[preflight_command.index("--fundamentals-db") + 1], str(FUNDAMENTALS_USA_DB))
        self.assertEqual(preflight_command[preflight_command.index("--market") + 1], "usa")
        self.assertEqual(preflight_command[preflight_command.index("--format") + 1], "json")

        self.assertEqual(quarter_update_command[quarter_update_command.index("--db") + 1], str(FUNDAMENTALS_USA_DB))
        self.assertEqual(quarter_update_command[quarter_update_command.index("--osakedata-db") + 1], str(OSAKEDATA_DB))
        self.assertEqual(
            quarter_update_command[quarter_update_command.index("--run-id") + 1],
            "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
        )
        self.assertEqual(quarter_update_command[quarter_update_command.index("--market") + 1], "usa")
        self.assertIn("--write-vintage", quarter_update_command)
        self.assertEqual(quarter_update_command[quarter_update_command.index("--vintage-mode") + 1], "sec_latest_writer")
        self.assertEqual(quarter_update_command[quarter_update_command.index("--vintage-market") + 1], "usa")
        self.assertEqual(
            quarter_update_command[quarter_update_command.index("--vintage-yahoo-aware-action") + 1],
            "plan_only",
        )
        self.assertNotEqual(
            quarter_update_command[quarter_update_command.index("--vintage-yahoo-aware-action") + 1],
            "write",
        )
        self.assertEqual(
            quarter_update_command[quarter_update_command.index("--vintage-available-at-utc") + 1],
            quarter_update_command[quarter_update_command.index("--vintage-ingested-at-utc") + 1],
        )
        self.assertIn(
            "2026-05-10",
            quarter_update_command[quarter_update_command.index("--vintage-run-id") + 1],
        )

    def test_preflight_failure_stops_before_quarter_update(self):
        app = self._app()
        fake_executor = FakeExecutor([(1, {})])
        app.executor = fake_executor

        app._execute_command_chain(
            [
                ["python", "-m", "swingmaster.cli.preflight_quarter_update_vintage_readiness"],
                ["python", "run_fundamental_quarter_update.py", "--write-vintage"],
            ],
            "USA Quarter Update",
            "usa",
        )

        self.assertEqual(len(fake_executor.commands), 1)
        self.assertIn("preflight_quarter_update_vintage_readiness", " ".join(fake_executor.commands[0]))
        self.assertEqual(app.usa_panel.status_badge.value, "USA Quarter Update: exit=1")
        self.assertEqual(app.usa_panel.status_badge.color, "red")

    def test_summary_key_value_parser_reads_vintage_statuses(self):
        executor = ProcessExecutor()
        parsed = executor._parse_summary_block(
            [
                "2026-05-10 10:00:01 | SUMMARY vintage_completion_status=SEC_VINTAGE_SUFFICIENT",
                "2026-05-10 10:00:02 | SUMMARY vintage_next_required_action=NONE",
                "2026-05-10 10:00:03 | SUMMARY vintage_post_run_latest_without_vintage_count=0",
                "2026-05-10 10:00:04 | SUMMARY vintage_yahoo_aware_planning_status=NO_ACTION_REQUIRED",
            ],
            {},
        )

        self.assertEqual(parsed["vintage_completion_status"], "SEC_VINTAGE_SUFFICIENT")
        self.assertEqual(parsed["vintage_next_required_action"], "NONE")
        self.assertEqual(parsed["vintage_post_run_latest_without_vintage_count"], "0")
        self.assertEqual(parsed["vintage_yahoo_aware_planning_status"], "NO_ACTION_REQUIRED")

    def test_summary_key_value_parser_reads_review_statuses(self):
        executor = ProcessExecutor()
        parsed = executor._parse_summary_block(
            [
                "2026-05-10 10:00:01 | SUMMARY vintage_completion_status=FINAL_MIXED_REQUIRED",
                "2026-05-10 10:00:02 | SUMMARY vintage_next_required_action=CREATE_FINAL_MIXED_VINTAGE",
            ],
            {},
        )

        self.assertEqual(parsed["vintage_completion_status"], "FINAL_MIXED_REQUIRED")
        self.assertEqual(parsed["vintage_next_required_action"], "CREATE_FINAL_MIXED_VINTAGE")

    def test_severity_mapping_and_status_badge_for_success_review_stop(self):
        cases = [
            (
                {
                    "vintage_completion_status": "SEC_VINTAGE_SUFFICIENT",
                    "vintage_post_run_parity_status": "OK",
                    "vintage_post_run_latest_without_vintage_count": "0",
                },
                "success",
                "green",
            ),
            ({"vintage_completion_status": "FINAL_MIXED_REQUIRED"}, "review", "orange"),
            ({"vintage_completion_status": "YAHOO_VINTAGE_REQUIRED"}, "review", "orange"),
            ({"vintage_completion_status": "BLOCKED_POST_RUN_DRIFT"}, "stop", "red"),
            ({"vintage_completion_status": "UNKNOWN"}, "stop", "red"),
            ({}, "unknown", "green"),
        ]

        for summary, expected_severity, expected_color in cases:
            with self.subTest(summary=summary):
                app = self._app()
                app.output_panel.set_summary(summary)
                self.assertEqual(map_vintage_completion_status_to_ui_severity(summary), expected_severity)
                self.assertEqual(app._status_color(0), expected_color)
                suffix = app._vintage_status_suffix()
                if expected_severity == "unknown":
                    self.assertEqual(suffix, "")
                else:
                    self.assertIn(f"severity={expected_severity}", suffix)

    def test_yahoo_aware_apply_button_disabled_before_relevant_summary(self):
        app = self._app()

        self.assertIsNotNone(app.usa_panel.yahoo_aware_apply_btn)
        self.assertTrue(app.usa_panel.yahoo_aware_apply_btn.disabled)

    def test_yahoo_aware_apply_button_enabled_after_plan_ready_summary(self):
        app = self._app()
        summary = {
            "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
            "vintage_completion_status": "FINAL_MIXED_REQUIRED",
            "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
            "vintage_planned_final_mixed_rows": "1",
            "vintage_yahoo_aware_blocked_rows": "0",
            "vintage_yahoo_aware_unknown_provenance_fields": "",
        }

        app._handle_summary("usa", summary)

        self.assertFalse(app.usa_panel.yahoo_aware_apply_btn.disabled)

    def test_yahoo_aware_apply_button_disabled_after_blocked_status(self):
        app = self._app()
        summary = {
            "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
            "vintage_completion_status": "BLOCKED_POST_RUN_DRIFT",
        }

        app._handle_summary("usa", summary)

        self.assertTrue(app.usa_panel.yahoo_aware_apply_btn.disabled)

    def test_yahoo_aware_apply_builds_separate_apply_command(self):
        app = self._app()
        app.last_usa_quarter_update_summary = {
            "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
            "vintage_completion_status": "YAHOO_VINTAGE_REQUIRED",
            "vintage_yahoo_aware_planning_status": "YAHOO_VINTAGE_PLAN_READY",
            "vintage_planned_yahoo_vintage_rows": "1",
        }
        captured = {}

        app._run_in_background = lambda target: captured.setdefault("target", target)
        with patch("ui_fundamental_pipeline.main.get_utc_launch_timestamp", return_value="2026-05-10T13:00:00Z"):
            with patch.object(app, "_execute_single_command") as execute_single:
                app._run_usa_yahoo_aware_apply()
                captured["target"]()

        execute_single.assert_called_once()
        command, status_prefix, market = execute_single.call_args[0]
        self.assertEqual(status_prefix, "USA Yahoo-Aware Vintage Apply")
        self.assertEqual(market, "usa")
        self.assertIn("swingmaster.cli.apply_quarter_update_yahoo_aware_vintage", command)
        self.assertNotIn("run_fundamental_quarter_update", " ".join(command))
        self.assertEqual(command[command.index("--source-run-id") + 1], "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY")
        self.assertEqual(command[command.index("--vintage-run-id") + 1], "USA_QUARTER_UPDATE_2026-05-10__YAHOO_AWARE_VINTAGE")
        self.assertIn("--approval-token", command)
        self.assertNotIn("--vintage-yahoo-aware-action", command)


if __name__ == "__main__":
    unittest.main()
