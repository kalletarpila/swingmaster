"""Swing Master UI for running fundamental CLI workflows."""
from __future__ import annotations

from datetime import datetime
import threading
from typing import Optional

import flet as ft

try:
    from .command_builder import (
        build_fin_update_command,
        build_score_percentile_command,
        build_single_ticker_snapshot_command,
        build_usa_update_command,
    )
    from .components.execution_output import ExecutionOutputPanel
    from .components.market_panel import MarketPanel
    from .components.snapshot_browser import SnapshotBrowser
    from .config import (
        DATETIME_FORMAT,
        WINDOW_HEIGHT,
        WINDOW_TITLE,
        WINDOW_WIDTH,
        get_missing_paths,
        get_run_id_fin,
        get_run_id_usa,
        validate_config,
    )
    from .data_access import resolve_latest_close_as_of_date
    from .executor import ProcessExecutor
except ImportError:  # pragma: no cover
    from command_builder import (
        build_fin_update_command,
        build_score_percentile_command,
        build_single_ticker_snapshot_command,
        build_usa_update_command,
    )
    from components.execution_output import ExecutionOutputPanel
    from components.market_panel import MarketPanel
    from components.snapshot_browser import SnapshotBrowser
    from config import (
        DATETIME_FORMAT,
        WINDOW_HEIGHT,
        WINDOW_TITLE,
        WINDOW_WIDTH,
        get_missing_paths,
        get_run_id_fin,
        get_run_id_usa,
        validate_config,
    )
    from data_access import resolve_latest_close_as_of_date
    from executor import ProcessExecutor


class SwingMasterApp:
    """Application controller for Swing Master."""

    def __init__(self, page: ft.Page):
        self.page = page
        self.executor = ProcessExecutor()
        self.current_worker: Optional[threading.Thread] = None
        self.stop_requested = False

        self._setup_page()

        self.output_panel = ExecutionOutputPanel(on_stop=self._stop_current_run)
        self.snapshot_browser = SnapshotBrowser()
        self.progress_text = ft.Text("Idle", size=12, color="gray")
        self.progress_bar = ft.ProgressBar(width=600, value=0)

        self.usa_panel = MarketPanel(
            market="usa",
            on_quarter_update=self._run_usa_update,
            on_score_percentile=self._run_usa_percentile,
            on_snapshot=self._run_usa_snapshots,
            on_lock=self._lock_ui,
        )
        self.fin_panel = MarketPanel(
            market="fin",
            on_quarter_update=self._run_fin_update,
            on_score_percentile=self._run_fin_percentile,
            on_snapshot=self._run_fin_snapshots,
            on_lock=self._lock_ui,
        )

        self.tabs = ft.Tabs(
            selected_index=0,
            tabs=[
                ft.Tab(
                    text="USA MARKET",
                    content=ft.Container(content=self.usa_panel.container, padding=10),
                ),
                ft.Tab(
                    text="FIN MARKET",
                    content=ft.Container(content=self.fin_panel.container, padding=10),
                ),
            ],
            expand=False,
        )

        self.overlay = ft.Container(
            content=ft.Column(
                [
                    ft.ProgressRing(),
                    ft.Text("Processing...", size=16, weight="bold"),
                    ft.Text("UI is locked until run completes or is stopped.", size=12),
                ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                alignment=ft.MainAxisAlignment.CENTER,
            ),
            bgcolor="#AA000000",
            alignment=ft.alignment.center,
            visible=False,
            expand=True,
        )

        self.main_content = ft.Column(
            controls=[
                ft.Text(WINDOW_TITLE, size=24, weight="bold"),
                self.tabs,
                ft.Row([self.progress_text], alignment=ft.MainAxisAlignment.START),
                self.progress_bar,
                ft.Divider(),
                self.output_panel.container,
                ft.Divider(),
                self.snapshot_browser.container,
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        )

        self.stack = ft.Stack([self.main_content, self.overlay], expand=True)
        self.page.add(self.stack)

        if not validate_config():
            missing = "\n".join(get_missing_paths())
            self._log(f"ERROR: Missing required paths:\n{missing}")

    def _setup_page(self) -> None:
        self.page.title = WINDOW_TITLE
        self.page.window_width = WINDOW_WIDTH
        self.page.window_height = WINDOW_HEIGHT
        self.page.padding = 12

    def _log(self, message: str) -> None:
        timestamp = datetime.now().strftime(DATETIME_FORMAT)
        self.output_panel.add_line(f"{timestamp} | {message}")
        self.page.update()

    def _lock_ui(self, locked: bool) -> None:
        self.usa_panel.disable_buttons(locked)
        self.fin_panel.disable_buttons(locked)
        self.tabs.disabled = locked
        self.overlay.visible = locked
        self.output_panel.set_running(locked)
        self.page.update()

    def _set_progress(self, current: int, total: int, label: str) -> None:
        if total <= 0:
            self.progress_bar.value = 0
            self.progress_text.value = label
        else:
            self.progress_bar.value = current / total
            self.progress_text.value = f"{label}: step {current}/{total}"
        self.page.update()

    def _run_in_background(self, target) -> None:
        self.stop_requested = False
        self.current_worker = threading.Thread(target=target, daemon=True)
        self.current_worker.start()

    def _stop_current_run(self) -> None:
        self.stop_requested = True
        self._log("Stopping current process...")
        self.executor.terminate()

    def _execute_single_command(self, command: list[str], status_prefix: str, market: str) -> None:
        self.output_panel.clear_output()
        self.page.call_from_thread(self._set_progress, 0, 1, status_prefix)

        exit_code, _ = self.executor.execute(
            command=command,
            on_output=lambda line: self.page.call_from_thread(self.output_panel.add_line, line),
            on_summary=lambda summary: self.page.call_from_thread(self.output_panel.set_summary, summary),
        )

        color = "green" if exit_code == 0 else "red"
        target_panel = self.usa_panel if market == "usa" else self.fin_panel
        self.page.call_from_thread(target_panel.set_status, f"{status_prefix}: exit={exit_code}", color)
        self.page.call_from_thread(self._set_progress, 1, 1, status_prefix)
        self.page.call_from_thread(self._lock_ui, False)

    def _execute_snapshot_batch(self, market: str, tickers: list[str]) -> None:
        self.output_panel.clear_output()
        total = len(tickers)
        close_market = "usa" if market == "usa" else "omxh"
        as_of_date = resolve_latest_close_as_of_date(close_market)

        for idx, ticker in enumerate(tickers, start=1):
            if self.stop_requested:
                self.page.call_from_thread(self._log, "Snapshot run stopped by user.")
                break

            self.page.call_from_thread(
                self._set_progress,
                idx - 1,
                total,
                f"Generating {market.upper()} snapshots",
            )
            self.page.call_from_thread(self._log, f"Running snapshot for {ticker}")

            command = build_single_ticker_snapshot_command(
                market=market,
                ticker=ticker,
                percentile_target_date=as_of_date,
            )
            exit_code, _ = self.executor.execute(
                command=command,
                on_output=lambda line: self.page.call_from_thread(self.output_panel.add_line, line),
                on_summary=lambda summary: self.page.call_from_thread(self.output_panel.set_summary, summary),
            )

            if exit_code != 0:
                self.page.call_from_thread(self._log, f"ERROR: Snapshot failed for {ticker} (exit={exit_code})")
            else:
                self.page.call_from_thread(self._log, f"OK: Snapshot generated for {ticker}")

            self.page.call_from_thread(
                self._set_progress,
                idx,
                total,
                f"Generating {market.upper()} snapshots",
            )

        self.page.call_from_thread(self.snapshot_browser.refresh_file_list)
        self.page.call_from_thread(self._lock_ui, False)

    def _run_usa_update(self) -> None:
        run_id = get_run_id_usa()
        command = build_usa_update_command(run_id=run_id)
        self._run_in_background(lambda: self._execute_single_command(command, "USA Quarter Update", "usa"))

    def _run_fin_update(self) -> None:
        run_id = get_run_id_fin()
        command = build_fin_update_command(run_id=run_id)
        self._run_in_background(lambda: self._execute_single_command(command, "FIN Batch Update", "fin"))

    def _run_usa_percentile(self) -> None:
        as_of_date = resolve_latest_close_as_of_date("usa")
        run_id = f"USA_PERCENTILE_{as_of_date}"
        command = build_score_percentile_command(market="usa", run_id=run_id, as_of_date=as_of_date)
        self._run_in_background(lambda: self._execute_single_command(command, "USA Percentile", "usa"))

    def _run_fin_percentile(self) -> None:
        as_of_date = resolve_latest_close_as_of_date("omxh")
        run_id = f"FIN_PERCENTILE_{as_of_date}"
        command = build_score_percentile_command(market="omxh", run_id=run_id, as_of_date=as_of_date)
        self._run_in_background(lambda: self._execute_single_command(command, "FIN Percentile", "fin"))

    def _run_usa_snapshots(self, tickers: list[str]) -> None:
        self._run_in_background(lambda: self._execute_snapshot_batch("usa", tickers))

    def _run_fin_snapshots(self, tickers: list[str]) -> None:
        self._run_in_background(lambda: self._execute_snapshot_batch("fin", tickers))


def main(page: ft.Page):
    SwingMasterApp(page)


if __name__ == "__main__":
    ft.app(target=main)
