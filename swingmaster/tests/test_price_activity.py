from swingmaster.fundamentals.price_activity import (
    ACTIVE,
    DELISTED_OR_INACTIVE,
    NO_PRICE_HISTORY,
    classify_price_activity,
)


def test_price_activity_uses_five_observed_sessions_boundary() -> None:
    sessions = [
        "2026-08-21",
        "2026-08-20",
        "2026-08-19",
        "2026-08-18",
        "2026-08-17",
        "2026-08-14",
    ]

    t0 = classify_price_activity(ticker="T0", last_price_date="2026-08-21", trading_sessions_desc=sessions)
    t1 = classify_price_activity(ticker="T1", last_price_date="2026-08-20", trading_sessions_desc=sessions)
    t4 = classify_price_activity(ticker="T4", last_price_date="2026-08-17", trading_sessions_desc=sessions)
    t5 = classify_price_activity(ticker="T5", last_price_date="2026-08-14", trading_sessions_desc=sessions)

    assert t0.activity_classification == ACTIVE
    assert t1.activity_classification == ACTIVE
    assert t4.activity_classification == ACTIVE
    assert t5.trading_sessions_stale == 5
    assert t5.activity_classification == DELISTED_OR_INACTIVE


def test_price_activity_no_history_is_distinct() -> None:
    result = classify_price_activity(
        ticker="MISS",
        last_price_date=None,
        trading_sessions_desc=["2026-08-21", "2026-08-20", "2026-08-19", "2026-08-18", "2026-08-17"],
    )

    assert result.trading_sessions_stale is None
    assert result.activity_classification == NO_PRICE_HISTORY


def test_price_activity_uses_observed_sessions_not_calendar_days() -> None:
    sessions = [
        "2026-07-09",
        "2026-07-08",
        "2026-07-07",
        "2026-07-06",
        "2026-07-02",
        "2026-07-01",
    ]

    active = classify_price_activity(ticker="GAP", last_price_date="2026-07-02", trading_sessions_desc=sessions)
    inactive = classify_price_activity(ticker="OLD", last_price_date="2026-07-01", trading_sessions_desc=sessions)

    assert active.trading_sessions_stale == 4
    assert active.activity_classification == ACTIVE
    assert inactive.trading_sessions_stale == 5
    assert inactive.activity_classification == DELISTED_OR_INACTIVE
