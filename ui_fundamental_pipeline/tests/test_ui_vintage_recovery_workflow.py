import unittest
from unittest.mock import MagicMock, Mock, patch

import flet as ft

from ui_fundamental_pipeline.executor import ProcessExecutor
from ui_fundamental_pipeline.main import SwingMasterApp


RUN_ID = "USA_QUARTER_UPDATE_2026-07-07__QUARTERLY"
RECOVERY_RUN_ID = "USA_PIT_VINTAGE_RECOVERY_2026-07-07__SEC_LATEST_WRITER_VINTAGE_RECOVERY"


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


class TestUiVintageRecoveryWorkflow(unittest.TestCase):
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
            side_effect=AssertionError("subprocess.Popen must not run in vintage recovery smoke tests"),
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

    def _run_recovery(self, app):
        with patch("ui_fundamental_pipeline.main.get_utc_launch_timestamp", return_value="2026-07-07T12:00:00Z"):
            with patch("ui_fundamental_pipeline.main.get_sec_vintage_recovery_run_id_usa", return_value=RECOVERY_RUN_ID):
                app._run_usa_vintage_recovery()

    def _ready_noop_preflight(self):
        return {
            "overall_status": "READY_NOOP",
            "latest_without_vintage_count": "0",
            "vintage_without_latest_count": "0",
            "duplicate_statement_vintage_id_count": "0",
        }

    def _parity_drift_preflight(self):
        return {
            "overall_status": "PARITY_DRIFT",
            "latest_without_vintage_count": "2",
            "vintage_without_latest_count": "0",
            "duplicate_statement_vintage_id_count": "0",
        }

    def _ready_dry_run(self):
        return {
            "overall_status": "DRY_RUN_READY",
            "source_run_id": RUN_ID,
            "latest_missing_vintage_rows": "2",
            "candidates_checked": "2",
            "planned_vintage_rows": "2",
            "planned_provenance_rows": "10",
            "blocked_rows": "0",
            "unknown_provenance_rows": "0",
            "unknown_provenance_field_counts": {},
        }

    def _apply_summary(self):
        return {
            "apply_applied": True,
            "apply_vintage_rows_inserted": "2",
            "apply_provenance_rows_inserted": "10",
        }

    def test_recovery_button_exists_and_locks_with_other_buttons(self):
        app = self._app()

        self.assertIsNotNone(app.usa_panel.vintage_recovery_btn)
        self.assertFalse(app.usa_panel.vintage_recovery_btn.disabled)

        app.usa_panel.disable_buttons(True)

        self.assertTrue(app.usa_panel.vintage_recovery_btn.disabled)

    def test_noop_recovery_runs_preflight_only(self):
        app = self._app()
        app.executor = FakeExecutor([(0, self._ready_noop_preflight())])

        self._run_recovery(app)

        self.assertEqual(len(app.executor.commands), 1)
        self.assertIn("preflight_quarter_update_vintage_readiness", " ".join(app.executor.commands[0]))
        self.assertEqual(app.usa_panel.status_badge.color, "green")
        self.assertIn("RECOVERY_NOOP", app.usa_panel.status_badge.value)
        self.assertEqual(app.last_usa_quarter_update_summary["vintage_recovery_status"], "RECOVERY_NOOP")

    def test_sec_recovery_success_runs_dry_run_apply_and_post_preflight(self):
        app = self._app()
        app.executor = FakeExecutor(
            [
                (1, self._parity_drift_preflight()),
                (0, self._ready_dry_run()),
                (0, self._apply_summary()),
                (0, self._ready_noop_preflight()),
            ]
        )

        self._run_recovery(app)

        self.assertEqual(len(app.executor.commands), 4)
        self.assertIn("preflight_quarter_update_vintage_readiness", " ".join(app.executor.commands[0]))
        self.assertIn("dry_run_sec_vintage_for_missing_latest", " ".join(app.executor.commands[1]))
        apply_command = app.executor.commands[2]
        joined_apply = " ".join(apply_command)
        self.assertIn("apply_sec_vintage_for_missing_latest", joined_apply)
        self.assertNotIn("run_fundamental_quarter_update", joined_apply)
        self.assertNotIn("--provider", apply_command)
        self.assertIn("--approval-token", apply_command)
        self.assertEqual(apply_command[apply_command.index("--source-run-id") + 1], RUN_ID)
        self.assertEqual(apply_command[apply_command.index("--expected-count") + 1], "2")
        self.assertIn("preflight_quarter_update_vintage_readiness", " ".join(app.executor.commands[3]))
        self.assertEqual(app.usa_panel.status_badge.color, "green")
        self.assertIn("RECOVERY_APPLIED", app.usa_panel.status_badge.value)
        self.assertEqual(app.last_usa_quarter_update_summary["vintage_recovery_status"], "RECOVERY_APPLIED")
        self.assertEqual(app.last_usa_quarter_update_summary["apply_vintage_rows_inserted"], "2")

    def test_duplicate_statement_ids_stop_before_dry_run(self):
        app = self._app()
        app.executor = FakeExecutor(
            [
                (
                    1,
                    {
                        "overall_status": "DUPLICATE_VINTAGE",
                        "latest_without_vintage_count": "2",
                        "duplicate_statement_vintage_id_count": "1",
                    },
                )
            ]
        )

        self._run_recovery(app)

        self.assertEqual(len(app.executor.commands), 1)
        self.assertEqual(app.usa_panel.status_badge.color, "red")
        self.assertIn("RECOVERY_BLOCKED", app.usa_panel.status_badge.value)

    def test_dry_run_blocked_rows_stop_before_apply(self):
        app = self._app()
        dry_run = self._ready_dry_run()
        dry_run["overall_status"] = "DRY_RUN_BLOCKED"
        dry_run["blocked_rows"] = "1"
        app.executor = FakeExecutor([(1, self._parity_drift_preflight()), (0, dry_run)])

        self._run_recovery(app)

        self.assertEqual(len(app.executor.commands), 2)
        self.assertEqual(app.usa_panel.status_badge.color, "red")
        self.assertIn("RECOVERY_BLOCKED", app.usa_panel.status_badge.value)

    def test_unknown_provenance_stops_before_apply(self):
        app = self._app()
        dry_run = self._ready_dry_run()
        dry_run["unknown_provenance_rows"] = "1"
        dry_run["unknown_provenance_field_counts"] = {"cash": 1}
        app.executor = FakeExecutor([(1, self._parity_drift_preflight()), (0, dry_run)])

        self._run_recovery(app)

        self.assertEqual(len(app.executor.commands), 2)
        self.assertEqual(app.usa_panel.status_badge.color, "red")
        self.assertIn("unknown provenance", app.last_usa_quarter_update_summary["vintage_recovery_reason"].lower())

    def test_missing_source_run_id_stops_before_apply(self):
        app = self._app()
        dry_run = self._ready_dry_run()
        dry_run["source_run_id"] = ""
        app.executor = FakeExecutor([(1, self._parity_drift_preflight()), (0, dry_run)])

        self._run_recovery(app)

        self.assertEqual(len(app.executor.commands), 2)
        self.assertIn("SOURCE_RUN_ID_REQUIRED", app.last_usa_quarter_update_summary["vintage_recovery_reason"])

    def test_yahoo_final_mixed_pending_action_stops_before_sec_apply(self):
        app = self._app()
        app.executor = FakeExecutor(
            [
                (
                    1,
                    {
                        "overall_status": "PENDING_YAHOO_AWARE_ACTION",
                        "latest_without_vintage_count": "0",
                        "yahoo_aware_pending_action_count": "2",
                    },
                )
            ]
        )

        self._run_recovery(app)

        self.assertEqual(len(app.executor.commands), 1)
        self.assertEqual(app.usa_panel.status_badge.color, "red")
        self.assertIn("Yahoo/final mixed", app.last_usa_quarter_update_summary["vintage_recovery_reason"])

    def test_recovery_does_not_change_quarter_update_checkbox_default(self):
        app = self._app()

        self.assertFalse(app.usa_panel.vintage_write_checkbox.value)

    def test_executor_parses_json_summary_output(self):
        parsed = ProcessExecutor()._parse_summary_block(
            [
                '2026-07-07 12:00:00 | {"summary": {',
                '2026-07-07 12:00:00 | "overall_status": "DRY_RUN_READY",',
                '2026-07-07 12:00:00 | "planned_vintage_rows": 2',
                "2026-07-07 12:00:00 | }}",
            ],
            {},
        )

        self.assertEqual(parsed["overall_status"], "DRY_RUN_READY")
        self.assertEqual(parsed["planned_vintage_rows"], 2)


if __name__ == "__main__":
    unittest.main()
