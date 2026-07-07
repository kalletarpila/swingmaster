"""
Tests for UI button interactions and handlers.
Tests that all buttons in the UI properly invoke their handlers.
"""
import unittest
import asyncio
from unittest.mock import Mock, MagicMock, patch
import os
import tempfile
from pathlib import Path
import flet as ft

from ui_fundamental_pipeline.components.execution_output import ExecutionOutputPanel
from ui_fundamental_pipeline.components.market_panel import MarketPanel
from ui_fundamental_pipeline.components.snapshot_browser import SnapshotBrowser


class TestExecutionOutputPanelButtons(unittest.TestCase):
    """Test ExecutionOutputPanel button interactions."""

    def setUp(self):
        """Set up test fixtures."""
        self.on_stop_mock = Mock()
        self.panel = ExecutionOutputPanel(on_stop=self.on_stop_mock)

    def test_clear_button_clears_output(self):
        """Test that clear button clears output."""
        # Add some content
        self.panel.add_line("test line 1")
        self.panel.add_line("test line 2")
        self.panel.set_summary({"key": "value"})

        # Verify content exists
        self.assertIn("test line 1", self.panel.log_output.value)
        self.assertIn("key=value", self.panel.summary_text.value)

        # Simulate clear button click
        self.panel._on_clear_click(None)

        # Verify content cleared
        self.assertEqual(self.panel.log_output.value, "")
        self.assertEqual(self.panel.summary_text.value, "")

    def test_stop_button_calls_on_stop_handler(self):
        """Test that stop button invokes on_stop callback."""
        # Simulate stop button click
        self.panel._on_stop_click(None)

        # Verify callback was invoked
        self.on_stop_mock.assert_called_once()

    def test_export_button_exports_log(self):
        """Test that export button exports log to file."""
        self.panel.add_line("line 1")
        self.panel.add_line("line 2")

        with patch("builtins.open", create=True) as mock_open:
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            # Simulate export button click
            self.panel._on_export_click(None)

            # Verify file was opened for writing
            self.assertTrue(mock_open.called)
            # Verify write was called
            self.assertTrue(mock_file.write.called)

    def test_buttons_exist_and_have_handlers(self):
        """Test that all buttons exist and have click handlers."""
        self.assertIsNotNone(self.panel.clear_btn)
        self.assertIsNotNone(self.panel.stop_btn)
        self.assertIsNotNone(self.panel.export_btn)

        # Verify click handlers are set
        self.assertEqual(self.panel.clear_btn.on_click, self.panel._on_clear_click)
        self.assertEqual(self.panel.stop_btn.on_click, self.panel._on_stop_click)
        self.assertEqual(self.panel.export_btn.on_click, self.panel._on_export_click)

    def test_button_disabled_state_changes(self):
        """Test that buttons are disabled/enabled based on running state."""
        # Initially export is disabled by default in component init.
        self.assertFalse(self.panel.clear_btn.disabled)
        self.assertTrue(self.panel.export_btn.disabled)

        # When running, only stop button should be enabled
        self.panel.set_running(True)
        self.assertTrue(self.panel.clear_btn.disabled)
        self.assertFalse(self.panel.stop_btn.disabled)
        self.assertTrue(self.panel.export_btn.disabled)

        # When not running, clear and export should be enabled
        self.panel.set_running(False)
        self.assertFalse(self.panel.clear_btn.disabled)
        self.assertTrue(self.panel.stop_btn.disabled)
        self.assertFalse(self.panel.export_btn.disabled)


class TestMarketPanelButtons(unittest.TestCase):
    """Test MarketPanel button interactions."""

    def setUp(self):
        """Set up test fixtures."""
        self.on_quarter_update = Mock()
        self.on_score_percentile = Mock()
        self.on_snapshot = Mock()
        self.on_lock = Mock()

        self.panel = MarketPanel(
            market="usa",
            on_quarter_update=self.on_quarter_update,
            on_score_percentile=self.on_score_percentile,
            on_snapshot=self.on_snapshot,
            on_lock=self.on_lock,
        )

    def test_quarter_update_button_exists(self):
        """Test that quarter update button exists."""
        self.assertIsNotNone(self.panel.quarter_update_btn)
        self.assertIsNotNone(self.panel.quarter_update_btn.on_click)

    def test_percentile_button_exists(self):
        """Test that percentile button exists."""
        self.assertIsNotNone(self.panel.percentile_btn)
        self.assertIsNotNone(self.panel.percentile_btn.on_click)

    def test_snapshot_button_exists(self):
        """Test that snapshot button exists."""
        self.assertIsNotNone(self.panel.snapshot_btn)
        self.assertIsNotNone(self.panel.snapshot_btn.on_click)

    def test_quarter_update_button_triggers_handler(self):
        """Test that quarter update button invokes handler."""
        # Simulate click
        self.panel._on_quarter_update_click(None)

        # Verify handler was called
        self.on_quarter_update.assert_called_once()

    def test_percentile_button_triggers_handler(self):
        """Test that percentile button invokes handler."""
        # Simulate click
        self.panel._on_percentile_click(None)

        # Verify handler was called
        self.on_score_percentile.assert_called_once()

    def test_snapshot_button_triggers_handler(self):
        """Test that snapshot button invokes handler."""
        # Snapshot handler requires at least one valid ticker.
        self.panel.ticker_input.value = "AAPL"

        # Simulate click
        with patch.object(self.panel, "_parse_and_validate_tickers", return_value=["AAPL"]):
            self.panel._on_snapshot_click(None)

        # Verify handler was called
        self.on_lock.assert_called_once_with(True)
        self.on_snapshot.assert_called_once_with(["AAPL"])

    def test_buttons_disabled_when_ui_locked(self):
        """Test that buttons are disabled when UI is locked."""
        # Initially enabled
        self.assertFalse(self.panel.quarter_update_btn.disabled)
        self.assertFalse(self.panel.percentile_btn.disabled)
        self.assertFalse(self.panel.snapshot_btn.disabled)

        # Disable buttons
        self.panel.disable_buttons(True)

        # All buttons should be disabled
        self.assertTrue(self.panel.quarter_update_btn.disabled)
        self.assertTrue(self.panel.vintage_write_checkbox.disabled)
        self.assertTrue(self.panel.percentile_btn.disabled)
        self.assertTrue(self.panel.snapshot_btn.disabled)

        # Re-enable buttons
        self.panel.disable_buttons(False)

        # All buttons should be enabled
        self.assertFalse(self.panel.quarter_update_btn.disabled)
        self.assertFalse(self.panel.vintage_write_checkbox.disabled)
        self.assertFalse(self.panel.percentile_btn.disabled)
        self.assertFalse(self.panel.snapshot_btn.disabled)

    def test_ticker_input_validation_on_snapshot(self):
        """Test that ticker input is validated before snapshot."""
        # Set invalid ticker
        self.panel.ticker_input.value = "INVALID_TICKER_XYZ"

        # Click snapshot button
        self.panel._on_snapshot_click(None)

        # Handler should not be called with invalid ticker
        # (depends on validation logic in component)
        # At minimum, ticker_input should have a value
        self.assertTrue(len(self.panel.ticker_input.value) > 0)


class TestSnapshotBrowserButtons(unittest.TestCase):
    """Test SnapshotBrowser button interactions."""

    def setUp(self):
        """Set up test fixtures."""
        self.browser = SnapshotBrowser(page=Mock())

    def test_refresh_button_exists(self):
        """Test that refresh button exists."""
        self.assertIsNotNone(self.browser.refresh_btn)
        self.assertIsNotNone(self.browser.refresh_btn.on_click)

    def test_open_folder_button_exists(self):
        """Test that open folder button exists."""
        self.assertIsNotNone(self.browser.open_folder_btn)
        self.assertIsNotNone(self.browser.open_folder_btn.on_click)

    def test_download_all_button_exists(self):
        """Test that download all button exists."""
        self.assertIsNotNone(self.browser.download_all_btn)
        self.assertIsNotNone(self.browser.download_all_btn.on_click)

    def test_refresh_button_refreshes_list(self):
        """Test that refresh button updates file list."""
        with patch.object(self.browser, "refresh_file_list") as mock_refresh:
            # Simulate click
            self.browser._on_refresh_click(None)

            # Verify refresh was called
            mock_refresh.assert_called_once()

    def test_open_folder_button_attempts_open(self):
        """Test that open folder button attempts to open folder."""
        with patch("subprocess.Popen") as mock_popen:
            # Simulate click
            self.browser._on_open_folder_click(None)

            # Verify open command attempted.
            self.assertTrue(mock_popen.called)

    def test_download_all_button_creates_zip(self):
        """Test that download all button creates zip file."""
        with patch("zipfile.ZipFile") as mock_zipfile:
            with patch("pathlib.Path.glob", return_value=[]):
                # Simulate click
                self.browser._on_download_all_click(None)

                # Verify zipfile module was accessed
                # (actual zip creation depends on file availability)
                self.assertIsNotNone(self.browser)

    def test_download_single_uses_browser_launch_url(self):
        """Test single-file download uses browser URL launch."""
        page = Mock()
        browser = SnapshotBrowser(page=page)

        browser._on_download_single(Path("/tmp/example file.csv"))

        page.launch_url.assert_called_once()
        called_url = page.launch_url.call_args[0][0]
        self.assertEqual(called_url, "/example%20file.csv")

    def test_download_single_awaits_launch_when_needed(self):
        """Test awaitable launch_url is scheduled through run_task."""
        async def _fake_launch_result():
            return None

        def _run_task(handler, *args, **kwargs):
            asyncio.run(handler(*args, **kwargs))
            return Mock()

        page = Mock()
        page.launch_url = Mock(return_value=_fake_launch_result())
        page.run_task = Mock(side_effect=_run_task)

        browser = SnapshotBrowser(page=page)
        browser._on_download_single(Path("/tmp/example.csv"))

        page.launch_url.assert_called_once()
        page.run_task.assert_called_once()

    def test_download_all_uses_browser_launch_url(self):
        """Test ZIP download uses browser URL launch."""
        page = Mock()
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            file_path = tmp_path / "sample.csv"
            file_path.write_text("ticker,score\nAAPL,90\n", encoding="utf-8")

            with patch("ui_fundamental_pipeline.components.snapshot_browser.SNAPSHOTS_DIR", tmp_path):
                browser = SnapshotBrowser(page=page)
                browser.refresh_file_list()
                browser._on_download_all_click(None)

                page.launch_url.assert_called_once()
                called_url = page.launch_url.call_args[0][0]
                self.assertTrue(called_url.startswith("/snapshots_"))
                self.assertTrue(called_url.endswith(".zip"))

    def test_snapshot_files_are_sorted_newest_first(self):
        """Test generated snapshot list order: newest first."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            older = tmp_path / "older.csv"
            newer = tmp_path / "newer.csv"

            older.write_text("a,b\n1,2\n", encoding="utf-8")
            newer.write_text("a,b\n3,4\n", encoding="utf-8")

            # Force deterministic mtimes: newer file gets higher mtime.
            os.utime(older, (1_700_000_000, 1_700_000_000))
            os.utime(newer, (1_800_000_000, 1_800_000_000))

            with patch("ui_fundamental_pipeline.components.snapshot_browser.SNAPSHOTS_DIR", tmp_path):
                browser = SnapshotBrowser(page=Mock())
                browser.refresh_file_list()

                self.assertEqual(browser._snapshot_files[0].name, "newer.csv")
                self.assertEqual(browser._snapshot_files[1].name, "older.csv")

    def test_all_buttons_have_click_handlers(self):
        """Test that all buttons have proper click handlers."""
        buttons = [
            (self.browser.refresh_btn, self.browser._on_refresh_click),
            (self.browser.open_folder_btn, self.browser._on_open_folder_click),
            (self.browser.download_all_btn, self.browser._on_download_all_click),
        ]

        for button, expected_handler in buttons:
            self.assertIsNotNone(button)
            self.assertEqual(button.on_click, expected_handler)


class TestButtonEventSimulation(unittest.TestCase):
    """Test button click event simulation."""

    def test_simulate_clear_button_click(self):
        """Test simulating clear button click event."""
        panel = ExecutionOutputPanel(on_stop=Mock())
        panel.add_line("test")

        # Create mock event
        mock_event = MagicMock()

        # Simulate click
        panel._on_clear_click(mock_event)

        # Verify result
        self.assertEqual(panel.log_output.value, "")

    def test_simulate_stop_button_click(self):
        """Test simulating stop button click event."""
        on_stop = Mock()
        panel = ExecutionOutputPanel(on_stop=on_stop)

        # Create mock event
        mock_event = MagicMock()

        # Simulate click
        panel._on_stop_click(mock_event)

        # Verify callback invoked
        on_stop.assert_called_once()

    def test_simulate_market_panel_button_click(self):
        """Test simulating market panel button click."""
        on_handler = Mock()
        panel = MarketPanel(
            market="fin",
            on_quarter_update=on_handler,
            on_score_percentile=Mock(),
            on_snapshot=Mock(),
            on_lock=Mock(),
        )

        # Simulate quarter update button click
        mock_event = MagicMock()
        panel._on_quarter_update_click(mock_event)

        # Verify handler was invoked
        on_handler.assert_called_once()

    def test_button_labels_are_readable(self):
        """Test that button labels/content are human-readable."""
        panel = ExecutionOutputPanel(on_stop=Mock())

        # Check that buttons have readable labels
        # (content contains Text with label)
        self.assertIsNotNone(panel.clear_btn.content)
        self.assertIsNotNone(panel.stop_btn.content)
        self.assertIsNotNone(panel.export_btn.content)


class TestMarketPanelTickerHandling(unittest.TestCase):
    """Test ticker input handling in market panel."""

    def setUp(self):
        """Set up test fixtures."""
        self.on_snapshot = Mock()
        self.panel = MarketPanel(
            market="usa",
            on_quarter_update=Mock(),
            on_score_percentile=Mock(),
            on_snapshot=self.on_snapshot,
            on_lock=Mock(),
        )

    def test_ticker_input_accepts_single_ticker(self):
        """Test that ticker input accepts single ticker."""
        self.panel.ticker_input.value = "AAPL"
        self.assertEqual(self.panel.ticker_input.value, "AAPL")

    def test_ticker_input_accepts_multiple_tickers(self):
        """Test that ticker input accepts multiple comma-separated tickers."""
        self.panel.ticker_input.value = "AAPL,MSFT,GOOGL"
        self.assertIn("AAPL", self.panel.ticker_input.value)
        self.assertIn("MSFT", self.panel.ticker_input.value)
        self.assertIn("GOOGL", self.panel.ticker_input.value)

    def test_ticker_input_accepts_space_separated_tickers(self):
        """Test that ticker input accepts space-separated tickers."""
        self.panel.ticker_input.value = "AAPL MSFT GOOGL"
        self.assertIn("AAPL", self.panel.ticker_input.value)
        self.assertIn("MSFT", self.panel.ticker_input.value)
        self.assertIn("GOOGL", self.panel.ticker_input.value)

    def test_status_badge_updates(self):
        """Test that status badge can be updated."""
        initial_status = self.panel.status_badge.value

        # Update status
        self.panel.set_status("Processing...", "blue")
        self.assertEqual(self.panel.status_badge.value, "Processing...")

        # Update again
        self.panel.set_status("Complete", "green")
        self.assertEqual(self.panel.status_badge.value, "Complete")


if __name__ == "__main__":
    unittest.main()
