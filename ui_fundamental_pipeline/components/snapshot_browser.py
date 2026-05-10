"""
Snapshot browser component for displaying and downloading CSV files.
"""
from typing import Callable
from pathlib import Path
import zipfile
from datetime import datetime
import flet as ft

try:
    from ..config import SNAPSHOTS_DIR, PROJECT_ROOT
except ImportError:  # pragma: no cover
    from config import SNAPSHOTS_DIR, PROJECT_ROOT


class SnapshotBrowser:
    """Browse and download generated snapshot CSV files."""

    def __init__(self):
        """Initialize snapshot browser."""

        self.file_list = ft.ListView(
            expand=True,
            spacing=5,
            height=200,
        )

        self.refresh_btn = ft.ElevatedButton(
            text="Refresh List",
            on_click=self._on_refresh_click,
        )

        self.open_folder_btn = ft.ElevatedButton(
            text="Open Snapshots Folder",
            on_click=self._on_open_folder_click,
        )

        self.download_all_btn = ft.ElevatedButton(
            text="Download All as ZIP",
            on_click=self._on_download_all_click,
        )

        self.container = ft.Column(
            controls=[
                ft.Text("GENERATED SNAPSHOTS", weight="bold", size=12),
                ft.Row(
                    controls=[self.refresh_btn],
                    spacing=10,
                ),
                self.file_list,
                ft.Row(
                    controls=[self.open_folder_btn, self.download_all_btn],
                    spacing=10,
                ),
            ],
            spacing=5,
            expand=True,
        )

        self._snapshot_files = []
        self.refresh_file_list()

    def refresh_file_list(self):
        """Scan and display snapshot CSV files."""
        if not SNAPSHOTS_DIR.exists():
            SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

        # Find all CSV files
        csv_files = sorted(SNAPSHOTS_DIR.glob("*.csv"))

        self._snapshot_files = csv_files
        self.file_list.controls.clear()

        if not csv_files:
            self.file_list.controls.append(
                ft.Text("No snapshot files found", size=11, color="gray")
            )
        else:
            for csv_file in csv_files:
                size_kb = csv_file.stat().st_size / 1024
                row = ft.Row(
                    controls=[
                        ft.Text(
                            f"🔗 {csv_file.name}",
                            size=11,
                            expand=True,
                        ),
                        ft.Text(
                            f"({size_kb:.1f} KB)",
                            size=10,
                            color="gray",
                        ),
                        ft.IconButton(
                            icon=ft.icons.DOWNLOAD,
                            icon_size=16,
                            on_click=lambda e, f=csv_file: self._on_download_single(f),
                            tooltip="Download",
                        ),
                    ],
                    spacing=5,
                )
                self.file_list.controls.append(row)

    def _on_refresh_click(self, e):
        """Refresh file list."""
        self.refresh_file_list()

    def _on_open_folder_click(self, e):
        """Open snapshots folder in file manager."""
        import subprocess
        import platform

        try:
            SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
            if platform.system() == "Linux":
                subprocess.Popen(["xdg-open", str(SNAPSHOTS_DIR)])
            elif platform.system() == "Darwin":
                subprocess.Popen(["open", str(SNAPSHOTS_DIR)])
            elif platform.system() == "Windows":
                subprocess.Popen(["explorer", str(SNAPSHOTS_DIR)])
        except Exception as e:
            pass

    def _on_download_single(self, file_path: Path):
        """Download single snapshot file."""
        import subprocess
        import platform

        try:
            if platform.system() == "Linux":
                subprocess.Popen(["xdg-open", str(file_path)])
            elif platform.system() == "Darwin":
                subprocess.Popen(["open", str(file_path)])
            elif platform.system() == "Windows":
                subprocess.Popen(["start", str(file_path)])
        except Exception:
            pass

    def _on_download_all_click(self, e):
        """Download all snapshots as ZIP."""
        if not self._snapshot_files:
            return

        try:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            zip_path = PROJECT_ROOT / f"snapshots_{timestamp}.zip"

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for csv_file in self._snapshot_files:
                    zf.write(csv_file, arcname=csv_file.name)

            # Open the ZIP file
            import subprocess
            import platform

            if platform.system() == "Linux":
                subprocess.Popen(["xdg-open", str(zip_path.parent)])
            elif platform.system() == "Darwin":
                subprocess.Popen(["open", str(zip_path.parent)])
            elif platform.system() == "Windows":
                subprocess.Popen(["explorer", str(zip_path.parent)])

        except Exception:
            pass
