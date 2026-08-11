"""Swing Master UI for running fundamental CLI workflows."""
from __future__ import annotations

import csv
from datetime import datetime
import json
from pathlib import Path
import threading
from typing import Any
from typing import Optional

import flet as ft

try:
    from .command_builder import (
        build_fin_classification_ttm_commands,
        build_fin_update_command,
        build_score_percentile_command,
        build_single_ticker_snapshot_command,
        build_usa_result_check_command,
        build_usa_update_command,
    )
    from .components.execution_output import ExecutionOutputPanel
    from .components.market_panel import MarketPanel
    from .components.snapshot_browser import SnapshotBrowser
    from .config import (
        DATETIME_FORMAT,
        FUNDAMENTALS_USA_DB,
        SNAPSHOTS_DIR,
        TEMP_DIR,
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
        validate_config,
    )
    from .data_access import resolve_latest_close_as_of_date
    from .executor import ProcessExecutor
except ImportError:  # pragma: no cover
    from command_builder import (
        build_fin_classification_ttm_commands,
        build_fin_update_command,
        build_score_percentile_command,
        build_single_ticker_snapshot_command,
        build_usa_result_check_command,
        build_usa_update_command,
    )
    from components.execution_output import ExecutionOutputPanel
    from components.market_panel import MarketPanel
    from components.snapshot_browser import SnapshotBrowser
    from config import (
        DATETIME_FORMAT,
        FUNDAMENTALS_USA_DB,
        SNAPSHOTS_DIR,
        TEMP_DIR,
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
        validate_config,
    )
    from data_access import resolve_latest_close_as_of_date
    from executor import ProcessExecutor

try:
    from swingmaster.fundamentals.result_check import (
        CHECK_STATUS_SUCCESS,
        EXECUTABLE_DECISIONS,
        PLAN_VERSION,
        validate_candidate_hash,
    )
except ImportError:  # pragma: no cover
    from fundamentals.result_check import (
        CHECK_STATUS_SUCCESS,
        EXECUTABLE_DECISIONS,
        PLAN_VERSION,
        validate_candidate_hash,
    )


class SwingMasterApp:
    """Application controller for Swing Master."""

    def __init__(self, page: ft.Page):
        self.page = page
        self.executor = ProcessExecutor()
        self.current_worker: Optional[threading.Thread] = None
        self.stop_requested = False
        self.last_usa_quarter_update_summary: dict = {}
        self.latest_usa_plan_path: str | None = None
        self.latest_usa_plan_created_at: str | None = None
        self.latest_usa_candidate_count: int = 0
        self.latest_usa_candidate_hash: str | None = None

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
            on_result_check=self._run_usa_result_check,
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
        target_panel.set_status(f"{status_prefix}: exit={exit_code}", color)
        self._set_progress(1, 1, status_prefix)
        self._lock_ui(False)

    def _execute_command_chain(self, commands: list[list[str]], status_prefix: str, market: str) -> None:
        self.output_panel.clear_output()
        total = len(commands)
        self._set_progress(0, total, status_prefix)

        for idx, command in enumerate(commands, start=1):
            if self.stop_requested:
                self._log("Run stopped by user.")
                self._lock_ui(False)
                return
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
        target_panel.set_status(f"{status_prefix}: exit=0", color)
        self._lock_ui(False)

    def _status_color(self, exit_code: int) -> str:
        if exit_code != 0:
            return "red"
        return "green"

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

    def _execute_usa_result_check(self) -> None:
        self.output_panel.clear_output()
        self.latest_usa_plan_path = None
        self.latest_usa_plan_created_at = None
        self.latest_usa_candidate_count = 0
        self.latest_usa_candidate_hash = None
        self.usa_panel.set_quarter_update_available(True, "Run Check for New Results first.")
        self._set_progress(0, 1, "USA Result Check")
        decision_date = resolve_latest_close_as_of_date("usa")
        command = build_usa_result_check_command(decision_date=decision_date)
        exit_code, _ = self.executor.execute(
            command=command,
            on_output=self._ui_callback(self.output_panel.add_line),
            on_summary=self._ui_callback(lambda summary: self._handle_summary("usa", summary)),
        )
        summary = self.output_panel._current_summary.copy()
        check_status = str(summary.get("check_status") or "")
        candidate_count = int(summary.get("candidate_count") or 0)
        plan_path = str(summary.get("plan_json") or "")
        candidate_rows = self._read_candidate_preview(str(summary.get("candidates_csv") or ""))
        self.usa_panel.set_result_check_details(summary, candidate_rows)
        candidate_hash = str(summary.get("candidate_hash") or "")
        if exit_code == 0 and check_status == "SUCCESS" and plan_path:
            self.latest_usa_plan_path = plan_path
            self.latest_usa_plan_created_at = str(summary.get("created_at_utc") or "")
            self.latest_usa_candidate_count = candidate_count
            self.latest_usa_candidate_hash = candidate_hash
            if candidate_count > 0:
                self.usa_panel.set_quarter_update_available(True, f"{candidate_count} candidate(s) ready.")
                self.usa_panel.set_status(f"USA Result Check: SUCCESS ready_to_update={candidate_count}", "green")
            else:
                self.usa_panel.set_quarter_update_available(True, "No executable candidates in latest check.")
                self.usa_panel.set_status("USA Result Check: SUCCESS ready_to_update=0", "green")
        elif check_status == "SUCCESS":
            self.usa_panel.set_quarter_update_available(True, "No executable candidates.")
            self.usa_panel.set_status("USA Result Check: SUCCESS ready_to_update=0", "green")
        elif check_status == "PARTIAL":
            self.usa_panel.set_quarter_update_available(True, "Partial check; run check again before updating.")
            self.usa_panel.set_status("USA Result Check: PARTIAL run fresh check before update", "orange")
        else:
            self.usa_panel.set_quarter_update_available(True, "Result check failed.")
            self.usa_panel.set_status(f"USA Result Check: exit={exit_code} run fresh check before update", "red")
        self._set_progress(1, 1, "USA Result Check")
        self._lock_ui(False)

    def _run_usa_result_check(self) -> None:
        self._run_in_background(self._execute_usa_result_check)

    def _read_candidate_preview(self, candidates_csv: str, limit: int = 20) -> list[dict]:
        if not candidates_csv:
            return []
        path = Path(candidates_csv)
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                return [row for idx, row in enumerate(csv.DictReader(handle)) if idx < limit]
        except Exception:
            return []

    def _run_usa_update(self) -> None:
        decision_date = resolve_latest_close_as_of_date("usa")
        if not self._ensure_usa_plan_for_update(decision_date):
            self.usa_panel.set_status("No valid Check for New Results plan exists for the current decision date.", "red")
            self._lock_ui(False)
            return
        if self.latest_usa_candidate_count <= 0:
            self.usa_panel.set_status("No executable fundamentals updates in the latest check.", "orange")
            self._lock_ui(False)
            return
        run_id = get_run_id_usa()
        command = build_usa_update_command(
            run_id=run_id,
            quarter_refresh_plan_json=Path(self.latest_usa_plan_path),
            decision_date=decision_date,
        )
        self._run_in_background(lambda: self._execute_single_command(command, "USA Quarter Update", "usa"))

    def _ensure_usa_plan_for_update(self, decision_date: str) -> bool:
        if self.latest_usa_plan_path and self._is_usable_usa_plan_path(Path(self.latest_usa_plan_path), decision_date):
            return True
        discovered = self._discover_latest_usa_plan(decision_date)
        if discovered is None:
            return False
        plan_path, plan = discovered
        self._store_usa_plan(plan_path, plan)
        return True

    def _discover_latest_usa_plan(self, decision_date: str) -> tuple[Path, dict[str, Any]] | None:
        result_check_root = TEMP_DIR / "fundamental_result_check"
        if not result_check_root.exists():
            return None
        candidates: list[tuple[datetime, Path, dict[str, Any]]] = []
        for plan_path in result_check_root.glob("*/plan.json"):
            plan = self._read_plan_json(plan_path)
            if plan is None or not self._is_usable_usa_plan(plan, decision_date):
                continue
            candidates.append((_plan_created_at(plan), plan_path, plan))
        if not candidates:
            return None
        _created_at, plan_path, plan = max(candidates, key=lambda item: (item[0], str(item[1])))
        return plan_path, plan

    def _is_usable_usa_plan_path(self, plan_path: Path, decision_date: str) -> bool:
        plan = self._read_plan_json(plan_path)
        if plan is None or not self._is_usable_usa_plan(plan, decision_date):
            return False
        self._store_usa_plan(plan_path, plan)
        return True

    def _read_plan_json(self, plan_path: Path) -> dict[str, Any] | None:
        try:
            resolved = plan_path.resolve()
            expected_root = (TEMP_DIR / "fundamental_result_check").resolve()
            if resolved.name != "plan.json" or resolved.parent.parent != expected_root:
                return None
            payload = json.loads(resolved.read_text(encoding="utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def _is_usable_usa_plan(self, plan: dict[str, Any], decision_date: str) -> bool:
        try:
            if plan.get("plan_version") != PLAN_VERSION:
                return False
            if plan.get("check_status") != CHECK_STATUS_SUCCESS:
                return False
            if str(plan.get("fundamentals_db")) != str(FUNDAMENTALS_USA_DB.resolve()):
                return False
            if str(plan.get("decision_date")) != decision_date:
                return False
            rows = plan.get("candidates")
            if not isinstance(rows, list):
                return False
            if int(plan.get("candidate_count") or 0) != len(rows):
                return False
            seen: set[tuple[str, str]] = set()
            for row in rows:
                if not isinstance(row, dict):
                    return False
                ticker = str(row.get("ticker") or "").upper()
                period = str(row.get("target_period_end_date") or "")
                if not ticker or not period:
                    return False
                key = (ticker, period)
                if key in seen:
                    return False
                seen.add(key)
                if str(row.get("market") or "").lower() != "usa":
                    return False
                if str(row.get("decision")) not in EXECUTABLE_DECISIONS:
                    return False
                if int(row.get("fundamental_fetch_enabled") or 0) != 1:
                    return False
                if int(row.get("eligible_for_execution") or 0) != 1:
                    return False
            if not validate_candidate_hash(plan):
                return False
            return True
        except Exception:
            return False

    def _store_usa_plan(self, plan_path: Path, plan: dict[str, Any]) -> None:
        self.latest_usa_plan_path = str(plan_path)
        self.latest_usa_plan_created_at = str(plan.get("created_at_utc") or "")
        self.latest_usa_candidate_count = int(plan.get("candidate_count") or 0)
        self.latest_usa_candidate_hash = str(plan.get("candidate_hash") or "")

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


def _plan_created_at(plan: dict[str, Any]) -> datetime:
    value = str(plan.get("created_at_utc") or "")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return datetime.min


if __name__ == "__main__":
    ft.run(
        main,
        host=WEB_HOST,
        port=WEB_PORT,
        view=ft.AppView.WEB_BROWSER,
        assets_dir=str(SNAPSHOTS_DIR),
    )
