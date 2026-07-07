"""Market panel component for USA/FIN market operations."""
from typing import Callable, Set
import flet as ft

try:
    from ..config import FUNDAMENTALS_USA_DB, FUNDAMENTALS_FIN_DB
    from ..data_access import load_valid_tickers
    from ..ticker_utils import parse_and_validate_tickers
except ImportError:  # pragma: no cover
    from config import FUNDAMENTALS_USA_DB, FUNDAMENTALS_FIN_DB
    from data_access import load_valid_tickers
    from ticker_utils import parse_and_validate_tickers


class MarketPanel:
    """Panel for market-specific operations."""

    def __init__(
        self,
        market: str,
        on_quarter_update: Callable[[], None],
        on_score_percentile: Callable[[], None],
        on_snapshot: Callable[[list], None],
        on_lock: Callable[[bool], None],
        on_secondary_action: Callable[[], None] | None = None,
        secondary_action_label: str | None = None,
        on_yahoo_aware_apply: Callable[[], None] | None = None,
    ):
        """
        Initialize market panel.

        Args:
            market: 'usa' or 'fin'
            on_quarter_update: Callback when quarter update button clicked
            on_score_percentile: Callback when percentile button clicked
            on_snapshot: Callback when snapshot button clicked (receives ticker list)
            on_lock: Callback to lock/unlock UI
        """
        self.market = market.lower()
        self.on_quarter_update = on_quarter_update
        self.on_score_percentile = on_score_percentile
        self.on_snapshot = on_snapshot
        self.on_lock = on_lock
        self.on_secondary_action = on_secondary_action
        self.on_yahoo_aware_apply = on_yahoo_aware_apply
        self._yahoo_aware_apply_available = False

        # Load valid tickers from database
        self.valid_tickers = self._load_valid_tickers()

        # UI elements
        self.ticker_input = ft.TextField(
            label="Tickers (comma/space separated)",
            width=300,
            multiline=False,
        )

        self.quarter_update_btn = ft.Button(
            content=ft.Text(f"▶ Run {self.market.upper()} Quarter Update"),
            on_click=self._on_quarter_update_click,
        )
        self.vintage_write_checkbox = None
        if self.market == "usa":
            self.vintage_write_checkbox = ft.Checkbox(
                label="Enable PIT/vintage write for USA quarterly update",
                value=False,
            )
        self.yahoo_aware_apply_btn = None
        if self.market == "usa" and self.on_yahoo_aware_apply is not None:
            self.yahoo_aware_apply_btn = ft.Button(
                content=ft.Text("Apply planned Yahoo/final mixed vintage corrections"),
                on_click=self._on_yahoo_aware_apply_click,
                disabled=True,
            )

        self.percentile_btn = ft.Button(
            content=ft.Text(f"▶ Run {self.market.upper()} Score Percentile"),
            on_click=self._on_percentile_click,
        )

        self.secondary_action_btn = None
        if self.on_secondary_action is not None and secondary_action_label is not None:
            self.secondary_action_btn = ft.Button(
                content=ft.Text(f"▶ {secondary_action_label}"),
                on_click=self._on_secondary_action_click,
            )

        self.snapshot_btn = ft.Button(
            content=ft.Text(f"▶ Generate {self.market.upper()} Snapshots"),
            on_click=self._on_snapshot_click,
        )

        self.status_badge = ft.Text("Ready", size=12, color="gray")

        # Container
        controls = [
            ft.Text(f"{self.market.upper()} MARKET", weight="bold", size=16),
            ft.Divider(),
            ft.Text("1. DATABASE UPDATE", weight="bold", size=13),
            self.quarter_update_btn,
        ]
        if self.vintage_write_checkbox is not None:
            controls.append(self.vintage_write_checkbox)
        if self.yahoo_aware_apply_btn is not None:
            controls.append(self.yahoo_aware_apply_btn)
        if self.secondary_action_btn is not None:
            controls.extend(
                [
                    ft.Container(height=8),
                    self.secondary_action_btn,
                ]
            )
        controls.extend(
            [
                ft.Container(height=10),
                ft.Text("2. PERCENTILE CALCULATION", weight="bold", size=13),
                self.percentile_btn,
                ft.Container(height=10),
                ft.Text("3. SNAPSHOT GENERATION", weight="bold", size=13),
                self.ticker_input,
                self.snapshot_btn,
                ft.Container(height=10),
                self.status_badge,
            ]
        )
        self.container = ft.Column(controls=controls, spacing=5)

    def _load_valid_tickers(self) -> Set[str]:
        """Load valid tickers from database."""
        db_path = (
            FUNDAMENTALS_USA_DB if self.market == "usa" else FUNDAMENTALS_FIN_DB
        )

        if not db_path.exists():
            return set()

        try:
            return load_valid_tickers(db_path)
        except Exception:
            return set()

    def _on_quarter_update_click(self, e):
        """Handle quarter update button click."""
        self.on_lock(True)
        self.on_quarter_update()

    def _on_percentile_click(self, e):
        """Handle percentile button click."""
        self.on_lock(True)
        self.on_score_percentile()

    def _on_secondary_action_click(self, e):
        """Handle secondary action button click."""
        if self.on_secondary_action is None:
            return
        self.on_lock(True)
        self.on_secondary_action()

    def _on_snapshot_click(self, e):
        """Handle snapshot button click."""
        tickers_raw = self.ticker_input.value.strip()
        if not tickers_raw:
            self.status_badge.value = "ERROR: No tickers provided"
            self.status_badge.color = "red"
            return

        # Parse and validate tickers
        tickers = self._parse_and_validate_tickers(tickers_raw)

        if not tickers:
            self.status_badge.value = "ERROR: No valid tickers found"
            self.status_badge.color = "red"
            return

        self.on_lock(True)
        self.on_snapshot(tickers)

    def _parse_and_validate_tickers(self, raw_input: str) -> list:
        """
        Parse and validate tickers.

        Args:
            raw_input: Comma or space-separated ticker string

        Returns:
            List of valid uppercase ticker symbols
        """
        return parse_and_validate_tickers(raw_input=raw_input, valid_tickers=self.valid_tickers)

    def set_status(self, message: str, color: str = "gray"):
        """Update status badge."""
        self.status_badge.value = message
        self.status_badge.color = color

    def disable_buttons(self, disable: bool = True):
        """Disable/enable all action buttons."""
        self.quarter_update_btn.disabled = disable
        if self.vintage_write_checkbox is not None:
            self.vintage_write_checkbox.disabled = disable
        if self.yahoo_aware_apply_btn is not None:
            self.yahoo_aware_apply_btn.disabled = disable or not bool(getattr(self, "_yahoo_aware_apply_available", False))
        self.percentile_btn.disabled = disable
        if self.secondary_action_btn is not None:
            self.secondary_action_btn.disabled = disable
        self.snapshot_btn.disabled = disable
        self.ticker_input.disabled = disable

    def clear_ticker_input(self):
        """Clear ticker input field."""
        self.ticker_input.value = ""

    def is_vintage_write_enabled(self) -> bool:
        """Return whether the USA PIT/vintage write option is enabled."""
        if self.vintage_write_checkbox is None:
            return False
        return bool(self.vintage_write_checkbox.value)

    def set_yahoo_aware_apply_available(self, available: bool, reason: str = "") -> None:
        """Enable or disable the explicit Yahoo-aware apply action."""
        self._yahoo_aware_apply_available = bool(available)
        if self.yahoo_aware_apply_btn is not None:
            self.yahoo_aware_apply_btn.disabled = not available
            self.yahoo_aware_apply_btn.tooltip = reason

    def _on_yahoo_aware_apply_click(self, e):
        """Handle explicit Yahoo-aware apply click."""
        if self.on_yahoo_aware_apply is None:
            return
        self.on_lock(True)
        self.on_yahoo_aware_apply()
