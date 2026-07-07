"""Swing Master UI for running fundamental CLI workflows."""
from __future__ import annotations

from datetime import datetime
import threading
from typing import Optional

import flet as ft

try:
    from .command_builder import (
        UsaQuarterUpdateVintageOptions,
        UsaYahooAwareApplyOptions,
        build_fin_classification_ttm_commands,
        build_fin_update_command,
        build_score_percentile_command,
        build_single_ticker_snapshot_command,
        build_usa_update_command,
        build_usa_yahoo_aware_apply_command,
        build_usa_vintage_preflight_command,
    )
    from .components.execution_output import ExecutionOutputPanel
    from .components.market_panel import MarketPanel
    from .components.snapshot_browser import SnapshotBrowser
    from .config import (
        DATETIME_FORMAT,
        SNAPSHOTS_DIR,
        WINDOW_HEIGHT,
        WEB_HOST,
        WEB_PORT,
        WINDOW_TITLE,
        WINDOW_WIDTH,
        get_fin_chain_as_of_date,
        get_missing_paths,
        get_run_id_fin_classification,
        get_run_id_fin_recovery,
        get_run_id_fin_ttm,
        get_run_id_fin,
        get_run_id_usa,
        get_utc_launch_timestamp,
        get_yahoo_aware_vintage_run_id_usa,
        get_vintage_run_id_usa,
        validate_config,
    )
    from .data_access import resolve_latest_close_as_of_date
    from .executor import ProcessExecutor
    from .vintage_status import (
        map_vintage_completion_status_to_ui_severity,
        should_enable_yahoo_aware_apply,
    )
except ImportError:  # pragma: no cover
    from command_builder import (
        UsaQuarterUpdateVintageOptions,
        UsaYahooAwareApplyOptions,
        build_fin_classification_ttm_commands,
        build_fin_update_command,
        build_score_percentile_command,
        build_single_ticker_snapshot_command,
        build_usa_update_command,
        build_usa_yahoo_aware_apply_command,
        build_usa_vintage_preflight_command,
    )
    from components.execution_output import ExecutionOutputPanel
    from components.market_panel import MarketPanel
    from components.snapshot_browser import SnapshotBrowser
    from config import (
        DATETIME_FORMAT,
        SNAPSHOTS_DIR,
        WINDOW_HEIGHT,
        WEB_HOST,
        WEB_PORT,
        WINDOW_TITLE,
        WINDOW_WIDTH,
        get_fin_chain_as_of_date,
        get_missing_paths,
        get_run_id_fin_classification,
        get_run_id_fin_recovery,
        get_run_id_fin_ttm,
        get_run_id_fin,
        get_run_id_usa,
        get_utc_launch_timestamp,
        get_yahoo_aware_vintage_run_id_usa,
        get_vintage_run_id_usa,
        validate_config,
    )
    from data_access import resolve_latest_close_as_of_date
    from executor import ProcessExecutor
    from vintage_status import (
        map_vintage_completion_status_to_ui_severity,
        should_enable_yahoo_aware_apply,
    )


class SwingMasterApp:
    """Application controller for Swing Master."""

    def __init__(self, page: ft.Page):
        self.page = page
        self.executor = ProcessExecutor()
        self.current_worker: Optional[threading.Thread] = None
        self.stop_requested = False
        self.last_usa_quarter_update_summary: dict = {}

        self._setup_page()

        self.output_panel = ExecutionOutputPanel(on_stop=self._stop_current_run)
        self.snapshot_browser = SnapshotBrowser(page=self.page)
        self.progress_text = ft.Text("Idle", size=12, color="gray")
        self.progress_bar = ft.ProgressBar(width=600, value=0)

        self.usa_panel = MarketPanel(
            market="usa",
            on_quarter_update=self._run_usa_update,
            on_score_percentile=self._run_usa_percentile,
            on_snapshot=self._run_usa_snapshots,
            on_lock=self._lock_ui,
            on_yahoo_aware_apply=self._run_usa_yahoo_aware_apply,
        )
        self.fin_panel = MarketPanel(
            market="fin",
            on_quarter_update=self._run_fin_update,
            on_score_percentile=self._run_fin_percentile,
            on_snapshot=self._run_fin_snapshots,
            on_lock=self._lock_ui,
            on_secondary_action=self._run_fin_classification_ttm,
            secondary_action_label="Run FIN Classification + TTM",
        )

        self.active_market = "usa"

        self.usa_market_btn = ft.Button(
            content=ft.Text("USA (NYSE/NASDAQ)", weight="bold"),
            on_click=lambda e: self._select_market("usa"),
            height=52,
            width=260,
        )
        self.fin_market_btn = ft.Button(
            content=ft.Text("FIN (OMXH)", weight="bold"),
            on_click=lambda e: self._select_market("fin"),
            height=52,
            width=220,
        )

        self.market_selector_buttons = ft.Row(
            controls=[self.usa_market_btn, self.fin_market_btn],
            spacing=12,
            wrap=True,
        )

        self.market_selector_header = ft.Container(
            content=ft.Column(
                controls=[
                    ft.Text("MARKET SELECTION", weight="bold", size=13),
                    ft.Text("Choose the market workflow to run", size=11, color="gray"),
                    self.market_selector_buttons,
                ],
                spacing=8,
            ),
            bgcolor="#F4F6F8",
            border_radius=8,
            padding=10,
        )
        
        # Create tab content panels
        usa_content = ft.Container(content=self.usa_panel.container, padding=10)
        fin_content = ft.Container(content=self.fin_panel.container, padding=10)
        
        # Store content references for dynamic switching
        self.tab_contents = {
            "usa": usa_content,
            "fin": fin_content,
        }

        # Initialize tab content area with first market
        self.tab_content_area = ft.Container(
            content=self.tab_contents[self.active_market],
            expand=True,
        )

        self._update_market_selector_visuals()

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
            alignment=ft.Alignment(0, 0),
            visible=False,
            expand=True,
        )

        self.main_content = ft.Column(
            controls=[
                ft.Text(WINDOW_TITLE, size=24, weight="bold"),
                self.market_selector_header,
                self.tab_content_area,
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
        self.usa_market_btn.disabled = locked
        self.fin_market_btn.disabled = locked
        self.overlay.visible = locked
        self.output_panel.set_running(locked)
        self.page.update()

    def _update_market_selector_visuals(self) -> None:
        """Highlight active market button and keep inactive one neutral."""
        if self.active_market == "usa":
            self.usa_market_btn.bgcolor = "#0B5FFF"
            self.usa_market_btn.color = "white"
            self.fin_market_btn.bgcolor = "#E5E7EB"
            self.fin_market_btn.color = "black"
        else:
            self.fin_market_btn.bgcolor = "#0B5FFF"
            self.fin_market_btn.color = "white"
            self.usa_market_btn.bgcolor = "#E5E7EB"
            self.usa_market_btn.color = "black"

    def _select_market(self, market: str) -> None:
        """Switch visible market panel."""
        if market not in self.tab_contents:
            return
        self.active_market = market
        self.tab_content_area.content = self.tab_contents[market]
        self._update_market_selector_visuals()
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
        """Run target function in background thread using Flet's thread pool."""
        self.stop_requested = False
        self.page.run_thread(target)

    def _ui_callback(self, func):
        """Create callback that executes func and updates page."""
        def callback(*args, **kwargs):
            result = func(*args, **kwargs)
            self.page.update()
            return result
        return callback

    def _stop_current_run(self) -> None:
        self.stop_requested = True
        self._log("Stopping current process...")
        self.executor.terminate()

    def _handle_summary(self, market: str, summary: dict) -> None:
        self.output_panel.set_summary(summary)
        if market == "usa":
            self.last_usa_quarter_update_summary = summary.copy()
            enabled, reason = should_enable_yahoo_aware_apply(summary)
            self.usa_panel.set_yahoo_aware_apply_available(enabled, reason)

    def _execute_single_command(self, command: list[str], status_prefix: str, market: str) -> None:
        self.output_panel.clear_output()
        self._set_progress(0, 1, status_prefix)

        exit_code, _ = self.executor.execute(
            command=command,
            on_output=self._ui_callback(self.output_panel.add_line),
            on_summary=self._ui_callback(lambda summary: self._handle_summary(market, summary)),
        )

        target_panel = self.usa_panel if market == "usa" else self.fin_panel
        color = self._status_color(exit_code)
        suffix = self._vintage_status_suffix()
        target_panel.set_status(f"{status_prefix}: exit={exit_code}{suffix}", color)
        self._set_progress(1, 1, status_prefix)
        self._lock_ui(False)

    def _execute_command_chain(self, commands: list[list[str]], status_prefix: str, market: str) -> None:
        self.output_panel.clear_output()
        total = len(commands)
        self._set_progress(0, total, status_prefix)

        for idx, command in enumerate(commands, start=1):
            if self.stop_requested:
                self._log("Run stopped by user.")
                break
            self._set_progress(idx - 1, total, status_prefix)
            exit_code, _ = self.executor.execute(
                command=command,
                on_output=self._ui_callback(self.output_panel.add_line),
                on_summary=self._ui_callback(lambda summary: self._handle_summary(market, summary)),
            )
            if exit_code != 0:
                target_panel = self.usa_panel if market == "usa" else self.fin_panel
                target_panel.set_status(f"{status_prefix}: exit={exit_code}", "red")
                self._set_progress(idx, total, status_prefix)
                self._lock_ui(False)
                return
            self._set_progress(idx, total, status_prefix)

        target_panel = self.usa_panel if market == "usa" else self.fin_panel
        color = self._status_color(0)
        suffix = self._vintage_status_suffix()
        target_panel.set_status(f"{status_prefix}: exit=0{suffix}", color)
        self._lock_ui(False)

    def _status_color(self, exit_code: int) -> str:
        if exit_code != 0:
            return "red"
        severity = map_vintage_completion_status_to_ui_severity(self.output_panel._current_summary)
        if severity == "success":
            return "green"
        if severity == "review":
            return "orange"
        if severity == "stop":
            return "red"
        return "green"

    def _vintage_status_suffix(self) -> str:
        summary = self.output_panel._current_summary
        completion_status = summary.get("vintage_completion_status")
        if completion_status:
            severity = map_vintage_completion_status_to_ui_severity(summary)
            return f" vintage={completion_status} severity={severity}"
        return ""

    def _execute_snapshot_batch(self, market: str, tickers: list[str]) -> None:
        self.output_panel.clear_output()
        total = len(tickers)
        close_market = "usa" if market == "usa" else "omxh"
        as_of_date = resolve_latest_close_as_of_date(close_market)

        for idx, ticker in enumerate(tickers, start=1):
            if self.stop_requested:
                self._log("Snapshot run stopped by user.")
                break

            self._set_progress(
                idx - 1,
                total,
                f"Generating {market.upper()} snapshots",
            )
            self._log(f"Running snapshot for {ticker}")

            command = build_single_ticker_snapshot_command(
                market=market,
                ticker=ticker,
                percentile_target_date=as_of_date,
            )
            exit_code, _ = self.executor.execute(
                command=command,
                on_output=self._ui_callback(self.output_panel.add_line),
                on_summary=self._ui_callback(lambda summary: self._handle_summary(market, summary)),
            )

            if exit_code != 0:
                self._log(f"ERROR: Snapshot failed for {ticker} (exit={exit_code})")
            else:
                self._log(f"OK: Snapshot generated for {ticker}")

            self._set_progress(
                idx,
                total,
                f"Generating {market.upper()} snapshots",
            )

        self.snapshot_browser.refresh_file_list()
        self._lock_ui(False)

    def _run_usa_update(self) -> None:
        run_id = get_run_id_usa()
        if self.usa_panel.is_vintage_write_enabled():
            launch_timestamp_utc = get_utc_launch_timestamp()
            vintage_options = UsaQuarterUpdateVintageOptions(
                launch_timestamp_utc=launch_timestamp_utc,
                vintage_run_id=get_vintage_run_id_usa(run_id),
            )
            commands = [
                build_usa_vintage_preflight_command(),
                build_usa_update_command(run_id=run_id, vintage_options=vintage_options),
            ]
            self._run_in_background(lambda: self._execute_command_chain(commands, "USA Quarter Update", "usa"))
            return
        command = build_usa_update_command(run_id=run_id)
        self._run_in_background(lambda: self._execute_single_command(command, "USA Quarter Update", "usa"))

    def _run_usa_yahoo_aware_apply(self) -> None:
        summary = self.last_usa_quarter_update_summary.copy()
        enabled, reason = should_enable_yahoo_aware_apply(summary)
        if not enabled:
            self.usa_panel.set_status(f"Yahoo-aware apply unavailable: {reason}", "red")
            self._lock_ui(False)
            return
        source_run_id = str(summary["run_id"])
        launch_timestamp_utc = get_utc_launch_timestamp()
        command = build_usa_yahoo_aware_apply_command(
            UsaYahooAwareApplyOptions(
                source_run_id=source_run_id,
                vintage_run_id=get_yahoo_aware_vintage_run_id_usa(source_run_id),
                launch_timestamp_utc=launch_timestamp_utc,
                approved=True,
            )
        )
        self._run_in_background(lambda: self._execute_single_command(command, "USA Yahoo-Aware Vintage Apply", "usa"))

    def _run_fin_update(self) -> None:
        run_id = get_run_id_fin()
        command = build_fin_update_command(run_id=run_id)
        self._run_in_background(lambda: self._execute_single_command(command, "FIN Quarter Update", "fin"))

    def _run_fin_classification_ttm(self) -> None:
        as_of_date = get_fin_chain_as_of_date()
        classification_run_id = get_run_id_fin_classification()
        ttm_run_id = get_run_id_fin_ttm()
        recovery_run_id = get_run_id_fin_recovery()
        commands = build_fin_classification_ttm_commands(
            as_of_date=as_of_date,
            classification_run_id=classification_run_id,
            ttm_run_id=ttm_run_id,
            recovery_run_id=recovery_run_id,
        )
        self._run_in_background(
            lambda: self._execute_command_chain(commands, "FIN Classification + TTM", "fin")
        )

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
    ft.run(
        main,
        host=WEB_HOST,
        port=WEB_PORT,
        view=ft.AppView.WEB_BROWSER,
        assets_dir=str(SNAPSHOTS_DIR),
    )
