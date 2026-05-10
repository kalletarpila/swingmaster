"""
Execution output panel with log viewer and summary parser.
"""
from typing import Callable
import flet as ft

try:
    from ..config import LOG_BUFFER_LINES
except ImportError:  # pragma: no cover
    from config import LOG_BUFFER_LINES


class ExecutionOutputPanel:
    """Display execution output and summary."""

    def __init__(self, on_stop: Callable[[], None]):
        """
        Initialize execution output panel.

        Args:
            on_stop: Callback when stop button clicked
        """
        self.on_stop = on_stop

        self.log_output = ft.Text(
            value="",
            size=11,
            family="monospace",
            selectable=True,
            min_height=300,
        )

        self.log_container = ft.Column(
            controls=[self.log_output],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
            height=300,
        )

        self.clear_btn = ft.ElevatedButton(
            text="Clear",
            on_click=self._on_clear_click,
        )

        self.stop_btn = ft.ElevatedButton(
            text="Stop",
            on_click=self._on_stop_click,
        )

        self.export_btn = ft.ElevatedButton(
            text="Export Log",
            on_click=self._on_export_click,
            disabled=True,
        )

        # Summary display
        self.summary_text = ft.Text(
            value="",
            size=10,
            family="monospace",
            min_height=100,
        )

        self.container = ft.Column(
            controls=[
                ft.Row(
                    controls=[self.clear_btn, self.stop_btn, self.export_btn],
                    spacing=10,
                ),
                ft.Divider(),
                ft.Text("EXECUTION LOG (last 50 lines)", weight="bold", size=12),
                self.log_container,
                ft.Divider(),
                ft.Text("SUMMARY", weight="bold", size=12),
                self.summary_text,
            ],
            spacing=5,
            expand=True,
        )

        self._all_output = []
        self._current_summary = {}

    def add_line(self, line: str):
        """Add line to output log."""
        self._all_output.append(line)

        # Keep only last N lines for display
        display_lines = self._all_output[-LOG_BUFFER_LINES:]
        self.log_output.value = "\n".join(display_lines)

        # Auto-scroll to bottom
        # (Note: Flet doesn't have built-in auto-scroll, but Column with scroll=AUTO helps)

    def set_summary(self, summary_dict: dict):
        """Display summary data."""
        self._current_summary = summary_dict

        # Format summary as key=value lines
        summary_lines = [f"{k}={v}" for k, v in sorted(summary_dict.items())]
        self.summary_text.value = "\n".join(summary_lines)

    def clear_output(self):
        """Clear all output."""
        self._all_output.clear()
        self.log_output.value = ""
        self._current_summary.clear()
        self.summary_text.value = ""

    def set_running(self, running: bool):
        """Set UI state based on execution status."""
        self.clear_btn.disabled = running
        self.stop_btn.disabled = not running
        self.export_btn.disabled = running

    def _on_clear_click(self, e):
        """Clear button handler."""
        self.clear_output()

    def _on_stop_click(self, e):
        """Stop button handler."""
        self.on_stop()

    def _on_export_click(self, e):
        """Export log button handler."""
        # Note: Flet doesn't have built-in file download;
        # this would need file_picker.FilePicker() integration
        # For now, just export to a file in the project root
        try:
            from pathlib import Path
            try:
                from ..config import PROJECT_ROOT
            except ImportError:  # pragma: no cover
                from config import PROJECT_ROOT

            export_path = PROJECT_ROOT / "pipeline_execution.log"
            with open(export_path, "w") as f:
                f.write("\n".join(self._all_output))
            self.add_line(f"Log exported to {export_path}")
        except Exception as e:
            self.add_line(f"ERROR: Failed to export log: {str(e)}")
