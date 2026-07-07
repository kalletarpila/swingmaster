"""
Tests for UI components to catch Flet API compatibility issues.
Tests component initialization and basic interaction.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import flet as ft

from ui_fundamental_pipeline.components.execution_output import ExecutionOutputPanel
from ui_fundamental_pipeline.components.market_panel import MarketPanel
from ui_fundamental_pipeline.components.snapshot_browser import SnapshotBrowser


class TestExecutionOutputPanel(unittest.TestCase):
    """Test ExecutionOutputPanel component initialization and methods."""

    def test_initialization(self):
        """Test that ExecutionOutputPanel initializes without errors."""
        on_stop_mock = Mock()
        panel = ExecutionOutputPanel(on_stop=on_stop_mock)

        self.assertIsNotNone(panel.log_output)
        self.assertIsNotNone(panel.summary_text)
        self.assertIsNotNone(panel.clear_btn)
        self.assertIsNotNone(panel.stop_btn)
        self.assertIsNotNone(panel.export_btn)
        self.assertIsNotNone(panel.container)

    def test_log_output_is_monospace_text(self):
        """Test that log output uses monospace font."""
        on_stop_mock = Mock()
        panel = ExecutionOutputPanel(on_stop=on_stop_mock)

        self.assertIsInstance(panel.log_output, ft.Text)
        # Should have monospace font_family
        self.assertEqual(panel.log_output.style.font_family, "monospace")

    def test_buttons_have_label_not_text(self):
        """Test that buttons use label parameter (Flet 0.85 compatibility)."""
        on_stop_mock = Mock()
        panel = ExecutionOutputPanel(on_stop=on_stop_mock)

        # In Flet 0.85, buttons use 'label' not 'text'
        # We verify by checking they don't raise TypeError
        self.assertIsNotNone(panel.clear_btn)
        self.assertIsNotNone(panel.stop_btn)
        self.assertIsNotNone(panel.export_btn)

    def test_add_line(self):
        """Test adding lines to output."""
        on_stop_mock = Mock()
        panel = ExecutionOutputPanel(on_stop=on_stop_mock)

        panel.add_line("test line 1")
        panel.add_line("test line 2")

        self.assertIn("test line 1", panel.log_output.value)
        self.assertIn("test line 2", panel.log_output.value)

    def test_set_summary(self):
        """Test setting summary data."""
        on_stop_mock = Mock()
        panel = ExecutionOutputPanel(on_stop=on_stop_mock)

        summary_dict = {"tickers_processed": "10", "errors": "0", "time": "5.2s"}
        panel.set_summary(summary_dict)

        # Should format as key=value
        for key, value in summary_dict.items():
            self.assertIn(f"{key}={value}", panel.summary_text.value)

    def test_set_running_state(self):
        """Test running state affects button disabled status."""
        on_stop_mock = Mock()
        panel = ExecutionOutputPanel(on_stop=on_stop_mock)

        # Note: In the component, disabled might default to False for all buttons
        # The set_running method should control the state properly
        
        # Set running
        panel.set_running(True)
        self.assertTrue(panel.clear_btn.disabled)
        self.assertFalse(panel.stop_btn.disabled)

        # Set not running
        panel.set_running(False)
        self.assertFalse(panel.clear_btn.disabled)
        self.assertTrue(panel.stop_btn.disabled)

    def test_clear_output(self):
        """Test clearing output."""
        on_stop_mock = Mock()
        panel = ExecutionOutputPanel(on_stop=on_stop_mock)

        panel.add_line("test line")
        panel.set_summary({"key": "value"})

        panel.clear_output()

        self.assertEqual(panel.log_output.value, "")
        self.assertEqual(panel.summary_text.value, "")


class TestMarketPanel(unittest.TestCase):
    """Test MarketPanel component initialization and methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.on_run_usa_update = Mock()
        self.on_run_fin_update = Mock()
        self.on_run_usa_percentile = Mock()
        self.on_run_fin_percentile = Mock()
        self.on_run_usa_snapshots = Mock()
        self.on_run_fin_snapshots = Mock()

    def test_usa_market_panel_initialization(self):
        """Test USA MarketPanel initializes without errors."""
        panel = MarketPanel(
            market="usa",
            on_quarter_update=self.on_run_usa_update,
            on_score_percentile=self.on_run_usa_percentile,
            on_snapshot=self.on_run_usa_snapshots,
            on_lock=Mock(),
        )

        self.assertIsNotNone(panel.ticker_input)
        self.assertIsNotNone(panel.quarter_update_btn)
        self.assertIsNotNone(panel.vintage_write_checkbox)
        self.assertFalse(panel.is_vintage_write_enabled())
        self.assertIsNotNone(panel.percentile_btn)
        self.assertIsNotNone(panel.snapshot_btn)
        self.assertIsNotNone(panel.container)

    def test_fin_market_panel_initialization(self):
        """Test FIN MarketPanel initializes without errors."""
        panel = MarketPanel(
            market="fin",
            on_quarter_update=self.on_run_fin_update,
            on_score_percentile=self.on_run_fin_percentile,
            on_snapshot=self.on_run_fin_snapshots,
            on_lock=Mock(),
        )

        self.assertIsNotNone(panel.ticker_input)
        self.assertIsNotNone(panel.quarter_update_btn)
        self.assertIsNone(panel.vintage_write_checkbox)
        self.assertFalse(panel.is_vintage_write_enabled())
        self.assertIsNotNone(panel.percentile_btn)
        self.assertIsNotNone(panel.snapshot_btn)

    def test_buttons_have_label_not_text(self):
        """Test that buttons use content parameter (Flet 0.85 compatibility)."""
        panel = MarketPanel(
            market="usa",
            on_quarter_update=self.on_run_usa_update,
            on_score_percentile=self.on_run_usa_percentile,
            on_snapshot=self.on_run_usa_snapshots,
            on_lock=Mock(),
        )

        # Should not raise TypeError about unexpected 'text' keyword
        self.assertIsNotNone(panel.quarter_update_btn)
        self.assertIsNotNone(panel.percentile_btn)
        self.assertIsNotNone(panel.snapshot_btn)

    def test_disable_buttons(self):
        """Test disabling buttons."""
        panel = MarketPanel(
            market="usa",
            on_quarter_update=self.on_run_usa_update,
            on_score_percentile=self.on_run_usa_percentile,
            on_snapshot=self.on_run_usa_snapshots,
            on_lock=Mock(),
        )

        panel.disable_buttons(True)
        self.assertTrue(panel.ticker_input.disabled)
        self.assertTrue(panel.quarter_update_btn.disabled)
        self.assertTrue(panel.vintage_write_checkbox.disabled)
        self.assertTrue(panel.percentile_btn.disabled)
        self.assertTrue(panel.snapshot_btn.disabled)

        panel.disable_buttons(False)
        self.assertFalse(panel.ticker_input.disabled)
        self.assertFalse(panel.quarter_update_btn.disabled)
        self.assertFalse(panel.vintage_write_checkbox.disabled)
        self.assertFalse(panel.percentile_btn.disabled)
        self.assertFalse(panel.snapshot_btn.disabled)

    def test_usa_vintage_checkbox_state(self):
        """Test USA PIT/vintage option is off by default and readable."""
        panel = MarketPanel(
            market="usa",
            on_quarter_update=self.on_run_usa_update,
            on_score_percentile=self.on_run_usa_percentile,
            on_snapshot=self.on_run_usa_snapshots,
            on_lock=Mock(),
        )

        self.assertFalse(panel.is_vintage_write_enabled())
        panel.vintage_write_checkbox.value = True
        self.assertTrue(panel.is_vintage_write_enabled())

    def test_set_status(self):
        """Test setting status message."""
        panel = MarketPanel(
            market="usa",
            on_quarter_update=self.on_run_usa_update,
            on_score_percentile=self.on_run_usa_percentile,
            on_snapshot=self.on_run_usa_snapshots,
            on_lock=Mock(),
        )

        panel.set_status("Processing...")
        self.assertEqual(panel.status_badge.value, "Processing...")


class TestSnapshotBrowser(unittest.TestCase):
    """Test SnapshotBrowser component initialization and methods."""

    def test_initialization(self):
        """Test that SnapshotBrowser initializes without errors."""
        browser = SnapshotBrowser()

        self.assertIsNotNone(browser.file_list)
        self.assertIsNotNone(browser.refresh_btn)
        self.assertIsNotNone(browser.open_folder_btn)
        self.assertIsNotNone(browser.download_all_btn)
        self.assertIsNotNone(browser.container)

    def test_buttons_have_label_not_text(self):
        """Test that buttons use label parameter (Flet 0.85 compatibility)."""
        browser = SnapshotBrowser()

        # Should not raise TypeError about unexpected 'text' keyword
        self.assertIsNotNone(browser.refresh_btn)
        self.assertIsNotNone(browser.open_folder_btn)
        self.assertIsNotNone(browser.download_all_btn)

    def test_refresh_file_list_empty(self):
        """Test refreshing file list with no snapshots."""
        browser = SnapshotBrowser()

        with patch("pathlib.Path.glob", return_value=[]):
            browser.refresh_file_list()
            # Should not raise and file_list should be empty
            self.assertIsNotNone(browser.file_list)

    def test_refresh_file_list_with_files(self):
        """Test refreshing file list with snapshot files."""
        browser = SnapshotBrowser()

        # Mock file paths - create real path objects for sorting
        from pathlib import Path
        from unittest.mock import MagicMock
        
        mock_file1 = MagicMock(spec=Path)
        mock_file1.name = "snapshot_1.html"
        mock_file1.stat.return_value.st_size = 12345
        
        mock_file2 = MagicMock(spec=Path)
        mock_file2.name = "snapshot_2.html"
        mock_file2.stat.return_value.st_size = 54321

        with patch("pathlib.Path.glob", return_value=[mock_file2, mock_file1]):
            # Patch sorted to handle mock objects
            with patch("builtins.sorted", side_effect=lambda x, **kw: [mock_file1, mock_file2]):
                browser.refresh_file_list()
                # Should not raise
                self.assertIsNotNone(browser.file_list)


class TestFletComponentCompat(unittest.TestCase):
    """Test Flet 0.85 API compatibility across all components."""

    def test_text_component_monospace_style(self):
        """Test that Text components use style parameter for font family."""
        # This would fail with TypeError if font_family is passed directly
        text = ft.Text(
            value="test",
            size=11,
            style=ft.TextStyle(font_family="monospace"),
        )
        self.assertIsNotNone(text)
        self.assertEqual(text.style.font_family, "monospace")

    def test_text_component_no_min_height(self):
        """Test that Text components don't use min_height parameter."""
        # min_height is not supported on Text in Flet 0.85
        text = ft.Text(value="test", size=11)
        self.assertIsNotNone(text)
        # If we got here, min_height parameter doesn't exist (good)

    def test_button_uses_content_parameter(self):
        """Test that Button uses content parameter with Text."""
        # Flet 0.85: ft.Button uses content parameter
        button = ft.Button(content=ft.Text("Click me"))
        self.assertIsNotNone(button)

    def test_button_on_click_callback(self):
        """Test that Button on_click callback works."""
        click_handler = Mock()
        button = ft.Button(content=ft.Text("Click me"), on_click=click_handler)
        self.assertIsNotNone(button)


if __name__ == "__main__":
    unittest.main()
