import unittest
from unittest.mock import MagicMock, Mock, patch

import flet as ft

from ui_fundamental_pipeline.executor import ProcessExecutor
from ui_fundamental_pipeline.main import SwingMasterApp


RUN_ID = "USA_QUARTER_UPDATE_2026-07-07__QUARTERLY"


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


def _summary_from_lines(lines):
    timestamped = [f"2026-07-07 12:00:00 | {line}" for line in lines]
    return ProcessExecutor()._parse_summary_block(timestamped, {})


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


class TestUiQuarterUpdateVintageFullWorkflowSmoke(unittest.TestCase):
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
            side_effect=AssertionError("subprocess.Popen must not run in full workflow smoke tests"),
        )
        self.snapshot_refresh_patcher.start()
        self.valid_tickers_patcher.start()
        self.popen_patcher.start()

    def tearDown(self):
        self.popen_patcher.stop()
        self.valid_tickers_patcher.stop()
        self.snapshot_refresh_patcher.stop()

    def _app(self):
        app = SwingMasterApp(_mock_page())
        app._run_in_background = lambda target: target()
        return app

    def _run_usa_vintage_workflow(self, app):
        app.usa_panel.vintage_write_checkbox.value = True
        with patch("ui_fundamental_pipeline.main.get_run_id_usa", return_value=RUN_ID):
            with patch("ui_fundamental_pipeline.main.get_utc_launch_timestamp", return_value="2026-07-07T12:00:00Z"):
                app._run_usa_update()

    def _assert_preflight_then_primary(self, commands):
        self.assertGreaterEqual(len(commands), 2)
        self.assertIn("preflight_quarter_update_vintage_readiness", " ".join(commands[0]))
        primary_command = commands[1]
        self.assertIn("run_fundamental_quarter_update", " ".join(primary_command))
        self.assertIn("--vintage-yahoo-aware-action", primary_command)
        self.assertEqual(primary_command[primary_command.index("--vintage-yahoo-aware-action") + 1], "plan_only")
        self.assertNotEqual(primary_command[primary_command.index("--vintage-yahoo-aware-action") + 1], "write")

    def _assert_provider_free_apply(self, command):
        joined = " ".join(command)
        self.assertIn("swingmaster.cli.apply_quarter_update_yahoo_aware_vintage", joined)
        self.assertNotIn("run_fundamental_quarter_update", joined)
        self.assertNotIn("--vintage-yahoo-aware-action", command)
        self.assertNotIn("--provider", command)
        self.assertIn("--approval-token", command)

    def test_sec_sufficient_path_runs_preflight_and_primary_only(self):
        app = self._app()
        app.executor = FakeExecutor(
            [
                (0, {"overall_status": "READY_NOOP"}),
                (
                    0,
                    _summary_from_lines(
                        [
                            f"SUMMARY run_id={RUN_ID}",
                            "SUMMARY vintage_completion_status=SEC_VINTAGE_SUFFICIENT",
                            "SUMMARY vintage_next_required_action=NONE",
                            "SUMMARY vintage_post_run_latest_without_vintage_count=0",
                            "SUMMARY vintage_post_run_parity_status=OK",
                            "SUMMARY vintage_yahoo_aware_planning_status=NO_ACTION_REQUIRED",
                            "SUMMARY vintage_yahoo_aware_auto_apply_attempted=false",
                        ]
                    ),
                ),
            ]
        )

        self._run_usa_vintage_workflow(app)

        self.assertEqual(len(app.executor.commands), 2)
        self._assert_preflight_then_primary(app.executor.commands)
        self.assertIn("SEC_VINTAGE_SUFFICIENT", app.usa_panel.status_badge.value)
        self.assertIn("severity=success", app.usa_panel.status_badge.value)
        self.assertEqual(app.usa_panel.status_badge.color, "green")
        self.assertTrue(app.usa_panel.yahoo_aware_apply_btn.disabled)
        self.assertEqual(app.last_usa_quarter_update_summary["vintage_yahoo_aware_auto_apply_attempted"], "0")

    def test_final_mixed_required_auto_apply_success(self):
        app = self._app()
        app.executor = FakeExecutor(
            [
                (0, {"overall_status": "READY_NOOP"}),
                (
                    0,
                    _summary_from_lines(
                        [
                            f"SUMMARY source_run_id={RUN_ID}",
                            "SUMMARY vintage_completion_status=FINAL_MIXED_REQUIRED",
                            "SUMMARY vintage_next_required_action=CREATE_FINAL_MIXED_VINTAGE",
                            "SUMMARY vintage_yahoo_aware_planning_status=FINAL_MIXED_PLAN_READY",
                            "SUMMARY vintage_planned_final_mixed_rows=2",
                            "SUMMARY vintage_planned_yahoo_vintage_rows=0",
                            "SUMMARY vintage_yahoo_aware_blocked_rows=0",
                            "SUMMARY vintage_yahoo_aware_unknown_provenance_fields=0",
                            "SUMMARY vintage_post_run_latest_without_vintage_count=0",
                            "SUMMARY vintage_post_run_duplicate_statement_vintage_id_count=0",
                        ]
                    ),
                ),
                (
                    0,
                    _summary_from_lines(
                        [
                            "SUMMARY vintage_yahoo_aware_execution_status=EXECUTION_COMPLETED",
                            "SUMMARY vintage_yahoo_aware_final_mixed_rows_written=2",
                            "SUMMARY vintage_yahoo_aware_yahoo_vintage_rows_written=0",
                            "SUMMARY vintage_yahoo_aware_provenance_rows_written=10",
                            "SUMMARY vintage_yahoo_aware_rows_blocked=0",
                            "SUMMARY vintage_yahoo_aware_error=",
                        ]
                    ),
                ),
            ]
        )

        self._run_usa_vintage_workflow(app)

        self.assertEqual(len(app.executor.commands), 3)
        self._assert_preflight_then_primary(app.executor.commands)
        self._assert_provider_free_apply(app.executor.commands[2])
        self.assertEqual(app.executor.commands[2][app.executor.commands[2].index("--source-run-id") + 1], RUN_ID)
        self.assertIn("Yahoo-Aware Apply", app.usa_panel.status_badge.value)
        self.assertIn("severity=success", app.usa_panel.status_badge.value)
        self.assertEqual(app.usa_panel.status_badge.color, "green")
        self.assertEqual(app.last_usa_quarter_update_summary["vintage_yahoo_aware_auto_apply_attempted"], "1")
        self.assertIn("FINAL_MIXED_PLAN_READY", app.last_usa_quarter_update_summary["vintage_yahoo_aware_auto_apply_reason"])
        self.assertEqual(app.last_usa_quarter_update_summary["vintage_yahoo_aware_final_mixed_rows_written"], "2")
        self.assertTrue(app.usa_panel.yahoo_aware_apply_btn.disabled)

    def test_yahoo_vintage_required_auto_apply_success(self):
        app = self._app()
        app.executor = FakeExecutor(
            [
                (0, {"overall_status": "READY_NOOP"}),
                (
                    0,
                    _summary_from_lines(
                        [
                            f"SUMMARY run_id={RUN_ID}",
                            "SUMMARY vintage_completion_status=YAHOO_VINTAGE_REQUIRED",
                            "SUMMARY vintage_yahoo_aware_planning_status=YAHOO_VINTAGE_PLAN_READY",
                            "SUMMARY vintage_planned_yahoo_vintage_rows=1",
                            "SUMMARY vintage_yahoo_aware_blocked_rows=0",
                            "SUMMARY vintage_yahoo_aware_unknown_provenance_fields=0",
                        ]
                    ),
                ),
                (
                    0,
                    _summary_from_lines(
                        [
                            "SUMMARY vintage_yahoo_aware_execution_status=EXECUTION_COMPLETED",
                            "SUMMARY vintage_yahoo_aware_yahoo_vintage_rows_written=1",
                            "SUMMARY vintage_yahoo_aware_rows_blocked=0",
                            "SUMMARY vintage_yahoo_aware_error=",
                        ]
                    ),
                ),
            ]
        )

        self._run_usa_vintage_workflow(app)

        self.assertEqual(len(app.executor.commands), 3)
        self._assert_provider_free_apply(app.executor.commands[2])
        self.assertEqual(app.usa_panel.status_badge.color, "green")
        self.assertIn("severity=success", app.usa_panel.status_badge.value)
        self.assertEqual(app.last_usa_quarter_update_summary["vintage_yahoo_aware_yahoo_vintage_rows_written"], "1")

    def test_blocked_summary_prevents_auto_apply_and_disables_manual_apply(self):
        app = self._app()
        app.executor = FakeExecutor(
            [
                (0, {"overall_status": "READY_NOOP"}),
                (
                    0,
                    _summary_from_lines(
                        [
                            f"SUMMARY run_id={RUN_ID}",
                            "SUMMARY vintage_completion_status=FINAL_MIXED_REQUIRED",
                            "SUMMARY vintage_yahoo_aware_planning_status=FINAL_MIXED_PLAN_READY",
                            "SUMMARY vintage_planned_final_mixed_rows=2",
                            "SUMMARY vintage_yahoo_aware_blocked_rows=1",
                        ]
                    ),
                ),
            ]
        )

        self._run_usa_vintage_workflow(app)

        self.assertEqual(len(app.executor.commands), 2)
        self.assertEqual(app.usa_panel.status_badge.color, "red")
        self.assertIn("severity=stop", app.usa_panel.status_badge.value)
        self.assertTrue(app.usa_panel.yahoo_aware_apply_btn.disabled)
        self.assertEqual(app.last_usa_quarter_update_summary["vintage_yahoo_aware_auto_apply_attempted"], "0")
        self.assertIn("blocked", app.last_usa_quarter_update_summary["vintage_yahoo_aware_auto_apply_reason"].lower())

    def test_unknown_provenance_and_missing_source_run_id_prevent_auto_apply(self):
        cases = [
            _summary_from_lines(
                [
                    f"SUMMARY run_id={RUN_ID}",
                    "SUMMARY vintage_completion_status=FINAL_MIXED_REQUIRED",
                    "SUMMARY vintage_yahoo_aware_planning_status=FINAL_MIXED_PLAN_READY",
                    "SUMMARY vintage_planned_final_mixed_rows=2",
                    "SUMMARY vintage_yahoo_aware_unknown_provenance_fields=AAPL:2026-03-31:cash",
                ]
            ),
            _summary_from_lines(
                [
                    "SUMMARY vintage_completion_status=FINAL_MIXED_REQUIRED",
                    "SUMMARY vintage_yahoo_aware_planning_status=FINAL_MIXED_PLAN_READY",
                    "SUMMARY vintage_planned_final_mixed_rows=2",
                ]
            ),
            _summary_from_lines(["SUMMARY vintage_completion_status=BLOCKED_POST_RUN_DRIFT"]),
            _summary_from_lines(["SUMMARY vintage_completion_status=UNKNOWN"]),
        ]

        for summary in cases:
            with self.subTest(summary=summary):
                app = self._app()
                app.executor = FakeExecutor([(0, {"overall_status": "READY_NOOP"}), (0, summary)])
                self._run_usa_vintage_workflow(app)

                self.assertEqual(len(app.executor.commands), 2)
                self.assertTrue(app.usa_panel.yahoo_aware_apply_btn.disabled)
                self.assertEqual(app.last_usa_quarter_update_summary["vintage_yahoo_aware_auto_apply_attempted"], "0")

    def test_preflight_failure_stops_primary_and_auto_apply(self):
        app = self._app()
        app.executor = FakeExecutor([(1, {"overall_status": "BLOCKED"})])

        self._run_usa_vintage_workflow(app)

        self.assertEqual(len(app.executor.commands), 1)
        self.assertIn("preflight_quarter_update_vintage_readiness", " ".join(app.executor.commands[0]))
        self.assertEqual(app.usa_panel.status_badge.value, "USA Quarter Update: exit=1")
        self.assertEqual(app.usa_panel.status_badge.color, "red")

    def test_checkbox_disabled_uses_default_primary_without_preflight_or_auto_apply(self):
        app = self._app()
        app.usa_panel.vintage_write_checkbox.value = False
        app.executor = FakeExecutor(
            [
                (
                    0,
                    _summary_from_lines(
                        [
                            f"SUMMARY run_id={RUN_ID}",
                            "SUMMARY vintage_completion_status=FINAL_MIXED_REQUIRED",
                            "SUMMARY vintage_yahoo_aware_planning_status=FINAL_MIXED_PLAN_READY",
                            "SUMMARY vintage_planned_final_mixed_rows=2",
                        ]
                    ),
                )
            ]
        )

        with patch("ui_fundamental_pipeline.main.get_run_id_usa", return_value=RUN_ID):
            app._run_usa_update()

        self.assertEqual(len(app.executor.commands), 1)
        command = app.executor.commands[0]
        self.assertNotIn("preflight_quarter_update_vintage_readiness", " ".join(command))
        self.assertNotIn("--write-vintage", command)
        self.assertNotIn("--vintage-yahoo-aware-action", command)
        self.assertNotIn("apply_quarter_update_yahoo_aware_vintage", " ".join(command))

    def test_user_stop_prevents_primary_and_auto_apply(self):
        app = self._app()
        app.stop_requested = True
        app.executor = FakeExecutor([])

        app._execute_usa_quarter_update_workflow(
            [["python", "-m", "swingmaster.cli.preflight_quarter_update_vintage_readiness"], ["python", "quarter"]],
            "USA Quarter Update",
            auto_apply_enabled=True,
        )

        self.assertEqual(app.executor.commands, [])
        self.assertTrue(app.usa_panel.yahoo_aware_apply_btn.disabled)


if __name__ == "__main__":
    unittest.main()
