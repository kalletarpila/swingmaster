from __future__ import annotations

import sqlite3

import pytest

from swingmaster.cli.daily_report import _attach_buy_badges, apply_buy_rules, validate_buy_rules_config


@pytest.mark.parametrize(
    (
        "market",
        "rule_hit",
        "trigger",
        "threshold",
        "match_section",
        "match_ticker_a",
        "match_ticker_b",
        "nonmatch_ticker",
    ),
    [
        ("FIN", "FIN_PASS_FP60", "NEW_PASS", 0.60, "NEW_PASS", "AAA", "ZZZ", "BBB"),
        ("SE", "SE_ENTRY_FP80", "NEW_EW", 0.80, "NEW_EW", "CCC", "YYY", "DDD"),
        ("USA", "USA_PASS_FP80", "NEW_PASS", 0.80, "NEW_PASS", "EEE", "XXX", "FFF"),
    ],
)
def test_apply_buy_rules_fastpass_threshold_and_ordering(
    market: str,
    rule_hit: str,
    trigger: str,
    threshold: float,
    match_section: str,
    match_ticker_a: str,
    match_ticker_b: str,
    nonmatch_ticker: str,
) -> None:
    config = {
        "market": market,
        "version": 1,
        "rules": [
            {
                "rule_hit": rule_hit,
                "trigger": trigger,
                "conditions": {
                    "fastpass_score_gte": threshold,
                },
            }
        ],
    }
    rows = [
        {
            "section": match_section,
            "ticker": match_ticker_b,
            "ew_score_fastpass": threshold + 0.10,
            "ew_level_fastpass": 1,
        },
        {
            "section": match_section,
            "ticker": match_ticker_a,
            "ew_score_fastpass": threshold + 0.10,
            "ew_level_fastpass": 1,
        },
        {
            "section": match_section,
            "ticker": nonmatch_ticker,
            "ew_score_fastpass": threshold - 0.20,
            "ew_level_fastpass": 0,
        },
    ]

    out, missing_field_count = apply_buy_rules(rows, config, buy_section_name="BUYS")

    assert missing_field_count == 0
    assert [row["ticker"] for row in out] == sorted([match_ticker_a, match_ticker_b])
    assert all(row["section"] == "BUYS" for row in out)
    assert all(row["rule_hit"] == rule_hit for row in out)


def test_validate_buy_rules_config_rejects_unknown_condition_key() -> None:
    config = {
        "market": "FIN",
        "version": 1,
        "rules": [
            {
                "rule_hit": "FIN_PASS_FP60",
                "trigger": "NEW_PASS",
                "conditions": {
                    "unknown_key": 1,
                },
            }
        ],
    }

    with pytest.raises(ValueError, match="unknown condition key"):
        validate_buy_rules_config(config, "FIN")


def test_attach_buy_badges_enriches_matching_buy_rows() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE rc_transactions_simu (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ticker TEXT NOT NULL,
          buy_date TEXT NOT NULL,
          buy_badges TEXT NOT NULL DEFAULT '[]',
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO rc_transactions_simu (ticker, buy_date, buy_badges, created_at)
        VALUES (?, ?, ?, ?)
        """,
        ("AAA", "2026-01-02", '["LOW_VOLUME","PENNY_STOCK"]', "2026-01-03T00:00:00Z"),
    )
    buy_rows = [
        {
            "section": "BUYS",
            "ticker": "AAA",
            "event_date": "2026-01-02",
            "rule_hit": "FIN_PASS_FP60",
        },
        {
            "section": "BUYS",
            "ticker": "BBB",
            "event_date": "2026-01-02",
            "rule_hit": "FIN_PASS_FP60",
        },
    ]

    out = _attach_buy_badges(conn, buy_rows)

    assert out[0]["buy_badges"] == '["LOW_VOLUME","PENNY_STOCK"]'
    assert out[1]["buy_badges"] is None
