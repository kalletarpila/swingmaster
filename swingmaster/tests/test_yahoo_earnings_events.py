from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from swingmaster.cli import inspect_yahoo_earnings_dates
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_events import (
    EarningsEventRecord,
    EarningsRangePlan,
    LimitPlan,
    YahooEarningsDateClient,
    YahooEarningsParseError,
    YahooEarningsResult,
    YahooParseDiagnostics,
    assess_earnings_coverage,
    classify_announcement_session,
    fetch_yahoo_earnings_events,
    open_readonly_db,
    parse_yahoo_earnings_dates,
    plan_earnings_history_range,
    plan_yahoo_earnings_limit,
)


OBSERVED_AT = "2026-07-30T14:00:00Z"


def _create_quarterly_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                revenue REAL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, period_end_date)
            )
            """
        )
        conn.commit()


def _insert_quarters(path: Path, rows: list[tuple[str, str]]) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO rc_fundamental_quarterly (ticker, period_end_date, revenue, run_id)
            VALUES (?, ?, 1.0, 'RUN')
            """,
            rows,
        )
        conn.commit()


def _earnings_frame() -> pd.DataFrame:
    index = pd.DatetimeIndex(
        [
            pd.Timestamp("2026-07-30 16:00:00", tz="America/New_York"),
            pd.Timestamp("2026-04-30 16:01:00", tz="America/New_York"),
            pd.Timestamp("2020-01-28 08:00:00", tz="America/New_York"),
            pd.Timestamp("2019-10-30 16:00:00", tz="America/New_York"),
            pd.Timestamp("2019-10-30 16:00:00", tz="America/New_York"),
            pd.Timestamp("2018-10-30 16:00:00", tz="America/New_York"),
        ],
        name="Earnings Date",
    )
    return pd.DataFrame(
        {
            "EPS Estimate": [1.89, 1.94, 1.13, 0.71, 0.71, 0.60],
            "Reported EPS": [None, 2.01, 1.25, 0.76, 0.76, 0.61],
            "Surprise(%)": [None, 3.46, 9.97, 6.54, 6.54, None],
        },
        index=index,
    )


def test_range_planner_uses_current_quarterly_oldest_and_safety_margin(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals.db"
    _create_quarterly_db(db_path)
    _insert_quarters(
        db_path,
        [
            ("aapl", "2020-06-30"),
            ("AAPL", "2020-03-31"),
            ("AAPL", "bad-date"),
            ("MSFT", "2019-12-31"),
        ],
    )

    with open_readonly_db(db_path) as conn:
        plan = plan_earnings_history_range(conn, " aapl ")

    assert plan.ticker == "AAPL"
    assert plan.source_table == "rc_fundamental_quarterly"
    assert plan.source_period_end_column == "period_end_date"
    assert plan.qualifying_fundamentals_row_count == 2
    assert plan.oldest_required_period_end_date == "2020-03-31"
    assert plan.safety_margin_days == 120
    assert plan.fetch_lower_bound == "2019-12-02"
    assert plan.status == "OK"


def test_range_planner_configurable_margin_no_history_override_and_readonly(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals.db"
    _create_quarterly_db(db_path)
    _insert_quarters(db_path, [("AAPL", "2020-03-31")])

    with open_readonly_db(db_path) as conn:
        custom = plan_earnings_history_range(conn, "AAPL", safety_margin_days=30)
        missing = plan_earnings_history_range(conn, "MSFT")
        override = plan_earnings_history_range(conn, "MSFT", manual_start_date="2021-01-01")
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO rc_fundamental_quarterly (ticker, period_end_date, run_id) VALUES ('X', '2020-01-01', 'R')")

    assert custom.fetch_lower_bound == "2020-03-01"
    assert missing.status == "NO_FUNDAMENTALS_HISTORY"
    assert missing.fetch_lower_bound is None
    assert override.status == "OK"
    assert override.fetch_lower_bound == "2021-01-01"
    assert override.range_overridden is True
    assert override.qualifying_fundamentals_row_count == 0


def test_limit_planner_dynamic_buffer_cap_and_manual_override() -> None:
    short = plan_yahoo_earnings_limit("2026-01-01", as_of_date="2026-07-30")
    from_2020 = plan_yahoo_earnings_limit("2020-01-01", as_of_date="2026-07-30")
    long = plan_yahoo_earnings_limit("1995-01-01", as_of_date="2026-07-30")
    manual = plan_yahoo_earnings_limit("2020-01-01", as_of_date="2026-07-30", manual_limit=150)

    assert short.estimated_quarters == 3
    assert short.requested_limit == 11
    assert from_2020.requested_limit == from_2020.estimated_quarters + 8
    assert long.uncapped_requested_limit > 100
    assert long.requested_limit == 100
    assert long.capped is True
    assert manual.requested_limit == 100
    assert manual.limit_overridden is True
    assert manual.capped is True


def test_parse_yahoo_dataframe_filters_normalizes_sorts_and_counts() -> None:
    records, diagnostics = parse_yahoo_earnings_dates(
        _earnings_frame(),
        ticker="aapl",
        fetch_lower_bound="2019-01-01",
        source_observed_at_utc=OBSERVED_AT,
        include_future=False,
    )

    assert diagnostics.raw_row_count == 6
    assert diagnostics.unreported_count == 1
    assert diagnostics.duplicate_count == 1
    assert diagnostics.rows_before_lower_bound == 1
    assert diagnostics.completed_qualifying_count == 3
    assert [record.announcement_date for record in records] == ["2019-10-30", "2020-01-28", "2026-04-30"]
    assert records[0].ticker == "AAPL"
    assert records[0].market == "usa"
    assert records[0].source == "YAHOO_FINANCE"
    assert records[0].source_timezone == "America/New_York"
    assert records[0].reported_eps == 0.76
    assert records[0].estimated_eps == 0.71
    assert records[0].surprise_pct == 6.54
    assert records[1].announcement_session == "BEFORE_MARKET"
    assert records[2].announcement_session == "AFTER_MARKET"


def test_parse_yahoo_dataframe_can_include_future_unreported_rows() -> None:
    records, diagnostics = parse_yahoo_earnings_dates(
        _earnings_frame(),
        ticker="AAPL",
        fetch_lower_bound="2026-01-01",
        source_observed_at_utc=OBSERVED_AT,
        include_future=True,
    )

    assert diagnostics.unreported_count == 1
    assert [record.is_reported for record in records] == [True, False]
    assert records[-1].reported_eps is None
    assert records[-1].announcement_session == "DURING_MARKET"


def test_parse_yahoo_missing_required_optional_none_empty_and_invalid_index() -> None:
    base = _earnings_frame()
    with pytest.raises(YahooEarningsParseError):
        parse_yahoo_earnings_dates(
            base.drop(columns=["Reported EPS"]),
            ticker="AAPL",
            fetch_lower_bound="2020-01-01",
            source_observed_at_utc=OBSERVED_AT,
        )

    no_surprise = base.drop(columns=["Surprise(%)"])
    records, diagnostics = parse_yahoo_earnings_dates(
        no_surprise,
        ticker="AAPL",
        fetch_lower_bound="2026-01-01",
        source_observed_at_utc=OBSERVED_AT,
        include_future=True,
    )
    assert records[0].surprise_pct is None
    assert "Surprise(%)" not in diagnostics.actual_columns

    none_records, none_diagnostics = parse_yahoo_earnings_dates(
        None,
        ticker="AAPL",
        fetch_lower_bound="2020-01-01",
        source_observed_at_utc=OBSERVED_AT,
    )
    assert none_records == ()
    assert none_diagnostics.raw_row_count == 0

    empty = base.iloc[0:0]
    empty_records, empty_diagnostics = parse_yahoo_earnings_dates(
        empty,
        ticker="AAPL",
        fetch_lower_bound="2020-01-01",
        source_observed_at_utc=OBSERVED_AT,
    )
    assert empty_records == ()
    assert empty_diagnostics.raw_row_count == 0

    naive = pd.DataFrame(
        {"EPS Estimate": [1.0], "Reported EPS": [1.1]},
        index=pd.DatetimeIndex([pd.Timestamp("2020-01-01 16:00:00")]),
    )
    naive_records, naive_diagnostics = parse_yahoo_earnings_dates(
        naive,
        ticker="AAPL",
        fetch_lower_bound="2020-01-01",
        source_observed_at_utc=OBSERVED_AT,
    )
    assert naive_records == ()
    assert naive_diagnostics.invalid_count == 1


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        (None, "UNKNOWN"),
        (pd.Timestamp("2026-01-30 08:00:00", tz="America/New_York"), "BEFORE_MARKET"),
        (pd.Timestamp("2026-01-30 09:29:00", tz="America/New_York"), "BEFORE_MARKET"),
        (pd.Timestamp("2026-01-30 09:30:00", tz="America/New_York"), "DURING_MARKET"),
        (pd.Timestamp("2026-01-30 16:00:00", tz="America/New_York"), "DURING_MARKET"),
        (pd.Timestamp("2026-01-30 16:01:00", tz="America/New_York"), "AFTER_MARKET"),
        (pd.Timestamp("2026-01-30 16:00:00-05:00"), "DURING_MARKET"),
        (pd.Timestamp("2026-07-30 16:00:00-04:00"), "DURING_MARKET"),
        ("not-a-date", "UNKNOWN"),
    ],
)
def test_session_classification(timestamp: object, expected: str) -> None:
    assert classify_announcement_session(timestamp) == expected


def _range_plan(oldest: str | None = "2020-03-31", lower: str | None = "2019-12-02", status: str = "OK") -> EarningsRangePlan:
    return EarningsRangePlan(
        ticker="AAPL",
        status=status,
        oldest_required_period_end_date=oldest,
        safety_margin_days=120,
        fetch_lower_bound=lower,
        source_table="rc_fundamental_quarterly",
        source_period_end_column="period_end_date",
        qualifying_fundamentals_row_count=1 if oldest else 0,
    )


def _record(date_text: str) -> EarningsEventRecord:
    return EarningsEventRecord(
        market="usa",
        ticker="AAPL",
        announcement_at=f"{date_text}T16:00:00-04:00",
        announcement_date=date_text,
        announcement_session="DURING_MARKET",
        is_reported=True,
        reported_eps=1.0,
        estimated_eps=1.0,
        surprise_pct=None,
        source="YAHOO_FINANCE",
        source_observed_at_utc=OBSERVED_AT,
        source_timezone="America/New_York",
    )


def test_coverage_statuses() -> None:
    ok = assess_earnings_coverage([_record("2019-10-30")], range_plan=_range_plan())
    partial_margin = assess_earnings_coverage([_record("2020-01-30")], range_plan=_range_plan())
    partial_short = assess_earnings_coverage([_record("2020-04-30")], range_plan=_range_plan())
    no_rows = assess_earnings_coverage([], range_plan=_range_plan())
    no_history = assess_earnings_coverage([], range_plan=_range_plan(None, None, "NO_FUNDAMENTALS_HISTORY"))
    source_failed = assess_earnings_coverage([], range_plan=_range_plan(), source_failed=True)
    parse_failed = assess_earnings_coverage([], range_plan=_range_plan(), parse_failed=True)

    assert ok.coverage_status == "COVERAGE_OK"
    assert ok.covers_fetch_lower_bound is True
    assert partial_margin.coverage_status == "COVERAGE_PARTIAL"
    assert partial_margin.covers_oldest_fundamentals_period is True
    assert partial_margin.covers_fetch_lower_bound is False
    assert partial_short.coverage_status == "COVERAGE_PARTIAL"
    assert partial_short.covers_oldest_fundamentals_period is False
    assert no_rows.coverage_status == "NO_YAHOO_ROWS"
    assert no_history.coverage_status == "NO_FUNDAMENTALS_HISTORY"
    assert source_failed.coverage_status == "SOURCE_FAILED"
    assert parse_failed.coverage_status == "PARSE_FAILED"


class _FakeYF:
    def __init__(self, dataframe: pd.DataFrame | None = None, error: Exception | None = None) -> None:
        self.dataframe = dataframe
        self.error = error
        self.requested_limits: list[int] = []

    def Ticker(self, ticker: str) -> "_FakeYF":
        self.ticker = ticker
        return self

    def get_earnings_dates(self, limit: int) -> pd.DataFrame | None:
        self.requested_limits.append(limit)
        if self.error is not None:
            raise self.error
        return self.dataframe


def test_fetch_source_result_success_source_failure_parse_failure_and_none() -> None:
    range_plan = _range_plan(oldest="2020-03-31", lower="2020-01-28")
    limit_plan = plan_yahoo_earnings_limit("2019-12-02", as_of_date="2020-12-31")
    fake_yf = _FakeYF(_earnings_frame())
    result = fetch_yahoo_earnings_events(
        ticker="aapl",
        range_plan=range_plan,
        limit_plan=limit_plan,
        client=YahooEarningsDateClient(fake_yf),
        observed_at_utc=OBSERVED_AT,
    )
    assert fake_yf.requested_limits == [limit_plan.requested_limit]
    assert result.status == "COVERAGE_OK"
    assert result.records

    failed = fetch_yahoo_earnings_events(
        ticker="AAPL",
        range_plan=range_plan,
        limit_plan=limit_plan,
        client=YahooEarningsDateClient(_FakeYF(error=RuntimeError("blocked"))),
        observed_at_utc=OBSERVED_AT,
    )
    assert failed.status == "SOURCE_FAILED"
    assert failed.error_message == "blocked"

    parse_failed = fetch_yahoo_earnings_events(
        ticker="AAPL",
        range_plan=range_plan,
        limit_plan=limit_plan,
        client=YahooEarningsDateClient(_FakeYF(pd.DataFrame({"Other": []}))),
        observed_at_utc=OBSERVED_AT,
    )
    assert parse_failed.status == "PARSE_FAILED"
    assert "Reported EPS" in str(parse_failed.error_message)

    none_result = fetch_yahoo_earnings_events(
        ticker="AAPL",
        range_plan=range_plan,
        limit_plan=limit_plan,
        client=YahooEarningsDateClient(_FakeYF(None)),
        observed_at_utc=OBSERVED_AT,
    )
    assert none_result.status == "NO_YAHOO_ROWS"


def test_migration_creates_table_uniqueness_and_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_migration.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_earnings_event)")}
        for column_name in (
            "id",
            "market",
            "ticker",
            "announcement_at",
            "announcement_date",
            "announcement_session",
            "is_reported",
            "reported_eps",
            "estimated_eps",
            "surprise_pct",
            "source_observed_at_utc",
            "source_timezone",
        ):
            assert column_name in columns
        indexes = {str(row[1]) for row in conn.execute("PRAGMA index_list(rc_earnings_event)")}
        assert "idx_rc_earnings_event_ticker_date" in indexes
        assert "idx_rc_earnings_event_announcement_date" in indexes
        assert "idx_rc_earnings_event_reported" in indexes
        assert "idx_rc_earnings_event_source" in indexes
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                market, ticker, announcement_at, announcement_date, announcement_session,
                is_reported, source, source_observed_at_utc, source_timezone, created_at_utc, updated_at_utc
            ) VALUES ('usa', 'AAPL', '2020-01-28T16:00:00-05:00', '2020-01-28', 'DURING_MARKET',
                1, 'YAHOO_FINANCE', '2026-07-30T14:00:00Z', 'America/New_York',
                '2026-07-30T14:00:00Z', '2026-07-30T14:00:00Z')
            """
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO rc_earnings_event (
                    market, ticker, announcement_at, announcement_date, announcement_session,
                    is_reported, source, source_observed_at_utc, source_timezone, created_at_utc, updated_at_utc
                ) VALUES ('usa', 'AAPL', '2020-01-28T16:00:00-05:00', '2020-01-28', 'DURING_MARKET',
                    1, 'YAHOO_FINANCE', '2026-07-30T14:00:00Z', 'America/New_York',
                    '2026-07-30T14:00:00Z', '2026-07-30T14:00:00Z')
                """
            )


def test_cli_database_derived_manual_override_json_exit_codes_and_no_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "fundamentals.db"
    _create_quarterly_db(db_path)
    _insert_quarters(db_path, [("AAPL", "2020-03-31")])

    def fake_fetch(**kwargs: object) -> YahooEarningsResult:
        range_plan = kwargs["range_plan"]
        assert isinstance(range_plan, EarningsRangePlan)
        limit_plan = kwargs["limit_plan"]
        assert isinstance(limit_plan, LimitPlan)
        coverage = assess_earnings_coverage([_record("2019-10-30")], range_plan=range_plan)
        return YahooEarningsResult(
            ticker="AAPL",
            normalized_ticker="AAPL",
            requested_limit=limit_plan.requested_limit,
            source_observed_at_utc=OBSERVED_AT,
            records=(_record("2019-10-30"),),
            diagnostics=YahooParseDiagnostics(("EPS Estimate", "Reported EPS"), 1, 0, 1, 0, 0, 0),
            coverage=coverage,
            status=coverage.coverage_status,
        )

    monkeypatch.setattr(inspect_yahoo_earnings_dates, "fetch_yahoo_earnings_events", fake_fetch)

    exit_code = inspect_yahoo_earnings_dates.main(
        ["--ticker", "AAPL", "--fundamentals-db", str(db_path), "--json"]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["range_plan"]["fetch_lower_bound"] == "2019-12-02"
    assert payload["range_plan"]["range_overridden"] is False
    assert payload["source"]["raw_yahoo_row_count"] == 1
    assert payload["coverage"]["coverage_status"] == "COVERAGE_OK"

    exit_code = inspect_yahoo_earnings_dates.main(
        [
            "--ticker",
            "AAPL",
            "--fundamentals-db",
            str(db_path),
            "--start-date",
            "2021-01-01",
            "--limit",
            "10",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["range_plan"]["fetch_lower_bound"] == "2021-01-01"
    assert payload["range_plan"]["range_overridden"] is True
    assert payload["limit_plan"]["requested_limit"] == 10
    assert payload["limit_plan"]["limit_overridden"] is True

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
    assert count == 1

    def fake_failed_fetch(**kwargs: object) -> YahooEarningsResult:
        range_plan = kwargs["range_plan"]
        assert isinstance(range_plan, EarningsRangePlan)
        limit_plan = kwargs["limit_plan"]
        assert isinstance(limit_plan, LimitPlan)
        coverage = assess_earnings_coverage([], range_plan=range_plan, source_failed=True)
        return YahooEarningsResult(
            ticker="AAPL",
            normalized_ticker="AAPL",
            requested_limit=limit_plan.requested_limit,
            source_observed_at_utc=OBSERVED_AT,
            records=(),
            diagnostics=YahooParseDiagnostics((), 0, 0, 0, 0, 0, 0),
            coverage=coverage,
            status="SOURCE_FAILED",
            error_message="boom",
        )

    monkeypatch.setattr(inspect_yahoo_earnings_dates, "fetch_yahoo_earnings_events", fake_failed_fetch)
    assert inspect_yahoo_earnings_dates.main(["--ticker", "AAPL", "--fundamentals-db", str(db_path)]) == 1
    capsys.readouterr()

    empty_db = tmp_path / "empty.db"
    _create_quarterly_db(empty_db)
    assert inspect_yahoo_earnings_dates.main(["--ticker", "MSFT", "--fundamentals-db", str(empty_db), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["coverage"]["coverage_status"] == "NO_FUNDAMENTALS_HISTORY"
    assert payload["source"] is None
