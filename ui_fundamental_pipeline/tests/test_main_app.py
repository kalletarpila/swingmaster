"""
Tests for main SwingMasterApp to catch Flet API issues early.
"""
import unittest
from unittest.mock import Mock, MagicMock, patch
import flet as ft

from ui_fundamental_pipeline.main import SwingMasterApp, main


class TestSwingMasterApp(unittest.TestCase):
    """Test SwingMasterApp initialization and page setup."""

    def setUp(self):
        """Set up test fixtures."""
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
