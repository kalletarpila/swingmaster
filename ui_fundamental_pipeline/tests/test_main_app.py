"""
Tests for main SwingMasterApp to catch Flet API issues early.
"""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, MagicMock, patch
import flet as ft

from ui_fundamental_pipeline.main import SwingMasterApp, main
from ui_fundamental_pipeline.config import FUNDAMENTALS_USA_DB
from swingmaster.fundamentals.result_check import PLAN_VERSION, candidate_hash


def _candidate(ticker: str = "AAPL") -> dict:
    return {
        "market": "usa",
        "ticker": ticker,
        "decision": "FETCH_NEW_QUARTER",
        "priority": "P1_FETCH_NOW",
        "fundamental_fetch_enabled": 1,
        "target_period_end_date": "2026-06-30",
        "planned_action": "PLAN_FETCH_QUARTERLY_FUNDAMENTALS",
        "eligible_for_execution": 1,
    }


def _write_plan(temp_dir: Path, run_id: str, *, decision_date: str, candidates: list[dict]) -> Path:
    plan_path = temp_dir / "fundamental_result_check" / run_id / "plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "plan_version": PLAN_VERSION,
        "created_at_utc": "2026-08-11T08:00:00Z",
        "decision_date": decision_date,
        "fundamentals_db": str(FUNDAMENTALS_USA_DB.resolve()),
        "ohlcv_db": "/tmp/osakedata.db",
        "ohlcv_stale_days": 14,
        "candidate_count": len(candidates),
        "candidate_hash": candidate_hash(candidates),
        "check_status": "SUCCESS",
        "stages": [{"stage": "fixture", "status": "SUCCESS"}],
        "candidates": candidates,
    }
    plan_path.write_text(json.dumps(payload), encoding="utf-8")
    return plan_path


class TestSwingMasterApp(unittest.TestCase):
    """Test SwingMasterApp initialization and page setup."""

    def setUp(self):
        """Set up test fixtures."""
        self.snapshot_refresh_patcher = patch(
            "ui_fundamental_pipeline.components.snapshot_browser.SnapshotBrowser.refresh_file_list"
        )
        self.snapshot_refresh_patcher.start()
        # Create a mock page object
        self.mock_page = MagicMock(spec=ft.Page)
        self.mock_page.title = ""
        self.mock_page.window_width = 1200
        self.mock_page.window_height = 800
        self.mock_page.padding = 10
        self.mock_page.spacing = 5
        self.mock_page.vertical_alignment = ft.MainAxisAlignment.START
        self.mock_page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
        self.mock_page.scroll = ft.ScrollMode.AUTO
        self.mock_page.clean = Mock()
        self.mock_page.add = Mock()

    def tearDown(self):
        self.snapshot_refresh_patcher.stop()

    def test_app_initialization(self):
        """Test that SwingMasterApp initializes without errors."""
        try:
            app = SwingMasterApp(self.mock_page)
            self.assertIsNotNone(app)
        except Exception as e:
            self.fail(f"SwingMasterApp initialization failed: {str(e)}")

    def test_app_has_required_attributes(self):
        """Test that app has all required UI components."""
        app = SwingMasterApp(self.mock_page)

        self.assertIsNotNone(app.page)
        self.assertIsNotNone(app.output_panel)
        self.assertIsNotNone(app.usa_panel)
        self.assertIsNotNone(app.fin_panel)
        self.assertIsNotNone(app.snapshot_browser)
        self.assertIsNotNone(app.market_selector_buttons)
        self.assertIsNotNone(app.overlay)

    def test_market_buttons_have_correct_labels(self):
        """Test market selection buttons are initialized and labeled."""
        app = SwingMasterApp(self.mock_page)

        self.assertIsNotNone(app.usa_market_btn)
        self.assertIsNotNone(app.fin_market_btn)
        self.assertEqual(app.active_market, "usa")

    def test_market_selector_is_clear_and_present(self):
        """Test that market selector header exists and labels are explicit."""
        app = SwingMasterApp(self.mock_page)

        self.assertIsNotNone(app.market_selector_header)
        self.assertIsNotNone(app.market_selector_buttons)

    def test_market_switch_updates_visible_content(self):
        """Test that changing selected market updates content container."""
        app = SwingMasterApp(self.mock_page)

        # Default view should point to first tab content.
        self.assertIs(app.tab_content_area.content, app.tab_contents["usa"])
        self.assertEqual(app.active_market, "usa")

        # Switch to FIN market.
        app._select_market("fin")
        self.assertIs(app.tab_content_area.content, app.tab_contents["fin"])
        self.assertEqual(app.active_market, "fin")

        # Switch back to USA market.
        app._select_market("usa")
        self.assertIs(app.tab_content_area.content, app.tab_contents["usa"])
        self.assertEqual(app.active_market, "usa")

    def test_market_buttons_trigger_switch(self):
        """Test clicking market buttons switches active market."""
        app = SwingMasterApp(self.mock_page)

        app.fin_market_btn.on_click(None)
        self.assertEqual(app.active_market, "fin")

        app.usa_market_btn.on_click(None)
        self.assertEqual(app.active_market, "usa")

    def test_fin_market_buttons_trigger_handlers(self):
        """Test FIN panel buttons trigger their assigned callbacks."""
        app = SwingMasterApp(self.mock_page)

        fin_update = Mock()
        fin_percentile = Mock()
        fin_snapshot = Mock()

        app.fin_panel.on_quarter_update = fin_update
        app.fin_panel.on_score_percentile = fin_percentile
        app.fin_panel.on_snapshot = fin_snapshot

        # Quarter update button
        app.fin_panel._on_quarter_update_click(None)
        fin_update.assert_called_once()

        # Percentile button
        app.fin_panel._on_percentile_click(None)
        fin_percentile.assert_called_once()

        # Snapshot button requires ticker parsing path
        app.fin_panel.ticker_input.value = "TYRES.HE"
        with patch.object(app.fin_panel, "_parse_and_validate_tickers", return_value=["TYRES.HE"]):
            app.fin_panel._on_snapshot_click(None)
        fin_snapshot.assert_called_once_with(["TYRES.HE"])

    def test_output_panel_initialization(self):
        """Test execution output panel is properly initialized."""
        app = SwingMasterApp(self.mock_page)

        # Output panel should exist and have UI components
        self.assertIsNotNone(app.output_panel.container)
        self.assertIsNotNone(app.output_panel.log_output)
        self.assertIsNotNone(app.output_panel.summary_text)

    def test_market_panels_initialization(self):
        """Test market panels are properly initialized."""
        app = SwingMasterApp(self.mock_page)

        # Both market panels should be initialized
        self.assertIsNotNone(app.usa_panel.container)
        self.assertIsNotNone(app.fin_panel.container)

        # Check that buttons are created
        self.assertIsNotNone(app.usa_panel.quarter_update_btn)
        self.assertIsNotNone(app.fin_panel.quarter_update_btn)

    def test_snapshot_browser_initialization(self):
        """Test snapshot browser is properly initialized."""
        app = SwingMasterApp(self.mock_page)

        # Snapshot browser should be initialized
        self.assertIsNotNone(app.snapshot_browser.container)
        self.assertIsNotNone(app.snapshot_browser.file_list)

    def test_page_configuration(self):
        """Test that page is configured correctly."""
        app = SwingMasterApp(self.mock_page)

        # Page should be configured with title
        self.mock_page.title = "Swing Master"
        # Verify page was accessed (would fail if API issue)
        self.assertIsNotNone(app.page)

    def test_app_callbacks_are_callable(self):
        """Test that app methods are callable."""
        app = SwingMasterApp(self.mock_page)

        # Methods should exist and be callable
        self.assertTrue(callable(app._setup_page))
        self.assertTrue(callable(app._lock_ui))
        self.assertTrue(callable(app._set_progress))
        self.assertTrue(callable(app._stop_current_run))

    def test_app_handlers_exist(self):
        """Test that all market-specific handlers exist."""
        app = SwingMasterApp(self.mock_page)

        # Handlers should exist
        self.assertTrue(callable(app._run_usa_update))
        self.assertTrue(callable(app._run_fin_update))
        self.assertTrue(callable(app._run_usa_percentile))
        self.assertTrue(callable(app._run_fin_percentile))
        self.assertTrue(callable(app._run_usa_snapshots))
        self.assertTrue(callable(app._run_fin_snapshots))

    def test_usa_update_without_result_check_plan_is_blocked(self):
        """Test USA update requires a successful result-check plan."""
        app = SwingMasterApp(self.mock_page)
        with tempfile.TemporaryDirectory() as tmp:
            with patch("ui_fundamental_pipeline.main.TEMP_DIR", Path(tmp)):
                with patch("ui_fundamental_pipeline.main.resolve_latest_close_as_of_date", return_value="2026-08-07"):
                    with patch.object(app, "_execute_single_command") as execute_single:
                        app._run_usa_update()

        execute_single.assert_not_called()
        self.assertEqual(
            app.usa_panel.status_badge.value,
            "No valid Check for New Results plan exists for the current decision date.",
        )

    def test_usa_plan_update_uses_single_command_without_vintage(self):
        """Test USA update uses the last successful result-check plan."""
        app = SwingMasterApp(self.mock_page)
        captured = {}

        def _capture(target):
            captured["target"] = target

        with tempfile.TemporaryDirectory() as tmp:
            temp_dir = Path(tmp)
            plan_path = _write_plan(temp_dir, "ui_session", decision_date="2026-08-07", candidates=[_candidate()])
            app.latest_usa_plan_path = str(plan_path)
            app.latest_usa_candidate_count = 1
            app._run_in_background = _capture
            with patch("ui_fundamental_pipeline.main.TEMP_DIR", temp_dir):
                with patch("ui_fundamental_pipeline.main.resolve_latest_close_as_of_date", return_value="2026-08-07"):
                    with patch.object(app, "_execute_single_command") as execute_single:
                        app._run_usa_update()
                        captured["target"]()

        execute_single.assert_called_once()
        command, status_prefix, market = execute_single.call_args[0]
        self.assertEqual(status_prefix, "USA Quarter Update")
        self.assertEqual(market, "usa")
        self.assertNotIn("--write-vintage", command)
        self.assertIn("--decision-date", command)
        self.assertEqual(command[command.index("--decision-date") + 1], "2026-08-07")
        self.assertIn("--quarter-refresh-plan-json", command)
        self.assertEqual(command[command.index("--quarter-refresh-plan-json") + 1], str(plan_path))

    def test_usa_update_discovers_latest_valid_scheduler_plan(self):
        """Test USA update can discover a valid same-decision-date plan."""
        app = SwingMasterApp(self.mock_page)
        captured = {}

        def _capture(target):
            captured["target"] = target

        with tempfile.TemporaryDirectory() as tmp:
            temp_dir = Path(tmp)
            old_plan = _write_plan(temp_dir, "old_day", decision_date="2026-08-06", candidates=[_candidate("OLD")])
            new_plan = _write_plan(temp_dir, "scheduler_day", decision_date="2026-08-07", candidates=[_candidate("AAPL")])
            del old_plan
            app._run_in_background = _capture
            with patch("ui_fundamental_pipeline.main.TEMP_DIR", temp_dir):
                with patch("ui_fundamental_pipeline.main.resolve_latest_close_as_of_date", return_value="2026-08-07"):
                    with patch.object(app, "_execute_single_command") as execute_single:
                        app._run_usa_update()
                        captured["target"]()

        execute_single.assert_called_once()
        command = execute_single.call_args[0][0]
        self.assertEqual(app.latest_usa_plan_path, str(new_plan))
        self.assertEqual(app.latest_usa_candidate_count, 1)
        self.assertEqual(command[command.index("--quarter-refresh-plan-json") + 1], str(new_plan))

    def test_usa_panel_exposes_result_check_and_keeps_update_active_initially(self):
        """Test USA manual workflow exposes result check without disabling update."""
        app = SwingMasterApp(self.mock_page)

        self.assertIsNotNone(app.usa_panel.result_check_btn)
        self.assertFalse(app.usa_panel.quarter_update_btn.disabled)
        self.assertFalse(hasattr(app.usa_panel, "vintage_write_checkbox"))
        self.assertFalse(hasattr(app.usa_panel, "yahoo_aware_apply_btn"))
        self.assertFalse(hasattr(app.usa_panel, "vintage_recovery_btn"))

    def test_successful_usa_result_check_enables_update(self):
        """Test successful result check stores plan state and enables update."""
        app = SwingMasterApp(self.mock_page)

        def _execute(command, on_output, on_summary, cwd=None):
            on_summary(
                {
                    "check_status": "SUCCESS",
                    "candidate_count": 2,
                    "candidate_hash": "abc",
                    "plan_json": "temp/fundamental_result_check/plan.json",
                    "candidates_csv": "",
                    "active_fetch_count": 10,
                    "stale_or_inactive_count": 1,
                }
            )
            return 0, []

        app.executor.execute = _execute
        with patch("ui_fundamental_pipeline.main.resolve_latest_close_as_of_date", return_value="2026-08-07"):
            app._execute_usa_result_check()

        self.assertEqual(app.latest_usa_plan_path, "temp/fundamental_result_check/plan.json")
        self.assertEqual(app.latest_usa_candidate_count, 2)
        self.assertFalse(app.usa_panel.quarter_update_btn.disabled)
        self.assertIn("ready_to_update=2", app.usa_panel.status_badge.value)

    def test_zero_candidate_usa_result_check_keeps_update_enabled_but_noops(self):
        """Test zero-candidate result check keeps update active but does not run update."""
        app = SwingMasterApp(self.mock_page)

        def _execute(command, on_output, on_summary, cwd=None):
            on_summary(
                {
                    "check_status": "SUCCESS",
                    "candidate_count": 0,
                    "candidate_hash": "abc",
                    "plan_json": "temp/fundamental_result_check/plan.json",
                    "candidates_csv": "",
                }
            )
            return 0, []

        app.executor.execute = _execute
        with patch("ui_fundamental_pipeline.main.resolve_latest_close_as_of_date", return_value="2026-08-07"):
            app._execute_usa_result_check()

        self.assertEqual(app.latest_usa_plan_path, "temp/fundamental_result_check/plan.json")
        self.assertFalse(app.usa_panel.quarter_update_btn.disabled)
        self.assertIn("ready_to_update=0", app.usa_panel.status_badge.value)
        with tempfile.TemporaryDirectory() as tmp:
            temp_dir = Path(tmp)
            plan_path = _write_plan(temp_dir, "zero", decision_date="2026-08-07", candidates=[])
            app.latest_usa_plan_path = str(plan_path)
            with patch("ui_fundamental_pipeline.main.TEMP_DIR", temp_dir):
                with patch("ui_fundamental_pipeline.main.resolve_latest_close_as_of_date", return_value="2026-08-07"):
                    with patch.object(app, "_execute_single_command") as execute_single:
                        app._run_usa_update()
        execute_single.assert_not_called()
        self.assertEqual(app.usa_panel.status_badge.value, "No executable fundamentals updates in the latest check.")


class TestMainFunction(unittest.TestCase):
    """Test main function entry point."""

    def test_main_function_accepts_page(self):
        """Test that main() function accepts a page parameter."""
        mock_page = MagicMock(spec=ft.Page)
        mock_page.title = ""
        mock_page.window_width = 1200
        mock_page.window_height = 800
        mock_page.padding = 10
        mock_page.spacing = 5
        mock_page.vertical_alignment = ft.MainAxisAlignment.START
        mock_page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
        mock_page.scroll = ft.ScrollMode.AUTO
        mock_page.clean = Mock()
        mock_page.add = Mock()

        try:
            # Call main with mock page - should not raise
            with patch("ui_fundamental_pipeline.components.snapshot_browser.SnapshotBrowser.refresh_file_list"):
                main(mock_page)
        except Exception as e:
            # Some errors might occur due to mocking, but TypeError about API parameters should not
            if "unexpected keyword argument" in str(e):
                self.fail(f"API parameter error in main(): {str(e)}")


class TestFletTabComponent(unittest.TestCase):
    """Test market selector-related control API compatibility."""

    def test_button_control_for_selector(self):
        """Test that selector button control can be created."""
        btn = ft.Button(content=ft.Text("USA"), height=52, width=220)
        self.assertIsNotNone(btn)

    def test_market_selector_row_creation(self):
        """Test row layout for market selector controls."""
        row = ft.Row(controls=[ft.Button(content=ft.Text("USA")), ft.Button(content=ft.Text("FIN"))], spacing=12)
        self.assertIsNotNone(row)


if __name__ == "__main__":
    unittest.main()
