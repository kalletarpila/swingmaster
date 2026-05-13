"""
Snapshot browser component for displaying and downloading CSV files.
"""
import inspect
from pathlib import Path
import zipfile
from datetime import datetime
from urllib.parse import quote
import flet as ft

try:
    from ..config import SNAPSHOTS_DIR, PROJECT_ROOT
except ImportError:  # pragma: no cover
    from config import SNAPSHOTS_DIR, PROJECT_ROOT


class SnapshotBrowser:
    """Browse and download generated snapshot CSV files."""

    def __init__(self, page: ft.Page | None = None):
        """Initialize snapshot browser."""
        self.page = page

        self.file_list = ft.ListView(
            expand=True,
            spacing=5,
            height=200,
        )

        self.refresh_btn = ft.Button(
            content=ft.Text("Refresh List"),
            on_click=self._on_refresh_click,
        )

        self.open_folder_btn = ft.Button(
            content=ft.Text("Open Snapshots Folder"),
            on_click=self._on_open_folder_click,
        )

        self.download_all_btn = ft.Button(
            content=ft.Text("Download All as ZIP"),
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

        # Find all CSV files ordered by mtime (newest first)
        csv_files = sorted(
            SNAPSHOTS_DIR.glob("*.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

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
                            icon=ft.icons.Icons.CLOUD_DOWNLOAD,
                            icon_size=16,
                            on_click=lambda e, f=csv_file: self._on_download_single(f),
                            tooltip="Download",
                        ),
                    ],
                    spacing=5,
                )
                self.file_list.controls.append(row)

    def _file_download_url(self, file_path: Path) -> str:
        """Return browser URL for downloading an asset file."""
        return f"/{quote(file_path.name)}"

    def _launch_download_url(self, url: str):
        """Launch a browser download URL and await when API returns awaitable."""
        if not self.page:
            return

        result = self.page.launch_url(url)

        # In some Flet runtimes this may return an awaitable even though
        # launch_url is not marked as coroutine function.
        if inspect.isawaitable(result):
            async def _await_launch():
                await result

            self.page.run_task(_await_launch)

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
        try:
            self._launch_download_url(self._file_download_url(file_path))
        except Exception:
            pass

    def _on_download_all_click(self, e):
        """Download all snapshots as ZIP."""
        if not self._snapshot_files:
            return

        try:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            zip_path = SNAPSHOTS_DIR / f"snapshots_{timestamp}.zip"

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for csv_file in self._snapshot_files:
                    zf.write(csv_file, arcname=csv_file.name)

            self._launch_download_url(self._file_download_url(zip_path))

        except Exception:
            pass
