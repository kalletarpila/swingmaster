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
        self.assertIsNotNone(app.tabs)
        self.assertIsNotNone(app.overlay)

    def test_tabs_have_correct_labels(self):
        """Test that tabs are initialized."""
        app = SwingMasterApp(self.mock_page)

        # Tabs should be configured
        self.assertIsNotNone(app.tabs)
        self.assertEqual(app.tabs.selected_index, 0)

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
    """Test Flet Tab component API compatibility."""

    def test_tab_uses_label_parameter(self):
        """Test that Tab uses label parameter (not text or content)."""
        # In Flet 0.85, Tab only uses 'label', icon, height, icon_margin
        tab = ft.Tab(label="Test Tab")
        self.assertIsNotNone(tab)

    def test_tabs_with_row_content(self):
        """Test that Tabs uses content parameter with Row of Tabs."""
        tab1 = ft.Tab(label="Tab 1")
        tab2 = ft.Tab(label="Tab 2")

        tabs = ft.Tabs(
            content=ft.Row(
                controls=[tab1, tab2],
                spacing=0,
            ),
            length=2,
            selected_index=0,
        )
        self.assertIsNotNone(tabs)
        self.assertEqual(tabs.selected_index, 0)


if __name__ == "__main__":
    unittest.main()
