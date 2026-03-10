from __future__ import annotations

import sqlite3

import pytest

from swingmaster.cli.daily_report import (
    _attach_buy_badges,
    _format_cell,
    _format_csv_value,
    apply_buy_rules,
    load_buy_rules_config,
    validate_buy_rules_config,
)


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
        ("USA", "USA_NOTRADE_FP80", "NEW_NOTRADE", 0.80, "NEW_NOTRADE", "GGG", "WWW", "HHH"),
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


def test_validate_buy_rules_config_accepts_new_notrade_trigger() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "rules": [
            {
                "rule_hit": "USA_NOTRADE_FP80",
                "trigger": "NEW_NOTRADE",
                "conditions": {
                    "fastpass_score_gte": 0.80,
                },
            }
        ],
    }

    validated = validate_buy_rules_config(config, "USA")
    assert validated["rules"][0]["trigger"] == "NEW_NOTRADE"


def test_validate_buy_rules_config_accepts_probabilistic_keys() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "rules": [
            {
                "rule_hit": "BUY_BULL_PASS_FAIL10_UP20_V2",
                "trigger": "NEW_PASS",
                "conditions": {
                    "regime_eq": "BULL",
                    "entry_window_exit_state_eq": "PASS",
                    "fail10_prob_gte": 0.10,
                    "fail10_prob_lte": 0.35,
                    "up20_prob_gte": 0.25,
                },
            }
        ],
    }

    validated = validate_buy_rules_config(config, "USA")
    assert validated["rules"][0]["rule_hit"] == "BUY_BULL_PASS_FAIL10_UP20_V2"


def test_apply_buy_rules_probabilistic_conditions_support_bull_and_bear_rules() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "rules": [
            {
                "rule_hit": "BUY_BULL_PASS_FAIL10_UP20_V2",
                "trigger": "NEW_PASS",
                "conditions": {
                    "regime_eq": "BULL",
                    "entry_window_exit_state_eq": "PASS",
                    "fail10_prob_gte": 0.10,
                    "fail10_prob_lte": 0.35,
                    "up20_prob_gte": 0.25,
                },
            },
            {
                "rule_hit": "BUY_BEAR_PASS_UP20_V1",
                "trigger": "NEW_PASS",
                "conditions": {
                    "regime_eq": "BEAR",
                    "entry_window_exit_state_eq": "PASS",
                    "up20_prob_gte": 0.10,
                },
            },
        ],
    }
    rows = [
        {
            "section": "NEW_PASS",
            "ticker": "AAA",
            "regime": "BULL",
            "entry_window_exit_state": "PASS",
            "fail10_prob": 0.20,
            "up20_prob": 0.30,
        },
        {
            "section": "NEW_PASS",
            "ticker": "BBB",
            "regime": "BEAR",
            "entry_window_exit_state": "PASS",
            "fail10_prob": None,
            "up20_prob": 0.10,
        },
        {
            "section": "NEW_PASS",
            "ticker": "CCC",
            "regime": "BULL",
            "entry_window_exit_state": "PASS",
            "fail10_prob": 0.40,
            "up20_prob": 0.60,
        },
    ]

    out, missing_field_count = apply_buy_rules(rows, config, buy_section_name="BUYS")

    assert missing_field_count == 0
    assert sorted((str(row["ticker"]), str(row["rule_hit"])) for row in out) == [
        ("AAA", "BUY_BULL_PASS_FAIL10_UP20_V2"),
        ("BBB", "BUY_BEAR_PASS_UP20_V1"),
    ]


def test_apply_buy_rules_probabilistic_missing_field_is_fail_closed() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "rules": [
            {
                "rule_hit": "BUY_BULL_PASS_FAIL10_UP20_V2",
                "trigger": "NEW_PASS",
                "conditions": {
                    "regime_eq": "BULL",
                    "entry_window_exit_state_eq": "PASS",
                    "fail10_prob_gte": 0.10,
                    "fail10_prob_lte": 0.35,
                    "up20_prob_gte": 0.25,
                },
            }
        ],
    }
    rows = [
        {
            "section": "NEW_PASS",
            "ticker": "AAA",
            "regime": "BULL",
            "entry_window_exit_state": "PASS",
            "up20_prob": 0.30,
        }
    ]

    out, missing_field_count = apply_buy_rules(rows, config, buy_section_name="BUYS")

    assert out == []
    assert missing_field_count == 1


def test_load_buy_rules_config_usa_accepts_probabilistic_rules() -> None:
    config = load_buy_rules_config("USA")
    rule_ids = {str(rule["rule_hit"]) for rule in config["rules"]}

    assert "BUY_BULL_PASS_FAIL10_UP20_V2" in rule_ids
    assert "BUY_BEAR_PASS_UP20_V1" in rule_ids


def test_apply_buy_rules_dual_rules_match_all_eight_rule_names() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "rules": [
            {"rule_hit": "USA_PASS_DUAL_PREMIUM_U80F20", "trigger": "NEW_PASS", "conditions": {"dual_buy_badge_eq": "BUY_PREMIUM"}},
            {"rule_hit": "USA_PASS_DUAL_ELITE_U72F27", "trigger": "NEW_PASS", "conditions": {"dual_buy_badge_eq": "BUY_ELITE"}},
            {"rule_hit": "USA_PASS_DUAL_STRONG_U66F32", "trigger": "NEW_PASS", "conditions": {"dual_buy_badge_eq": "BUY_STRONG"}},
            {
                "rule_hit": "USA_PASS_DUAL_QUALIFIED_U60F35",
                "trigger": "NEW_PASS",
                "conditions": {"dual_buy_badge_eq": "BUY_QUALIFIED"},
            },
            {
                "rule_hit": "USA_NOTRADE_DUAL_PREMIUM_U80F20",
                "trigger": "NEW_NOTRADE",
                "conditions": {"dual_buy_badge_eq": "BUY_PREMIUM"},
            },
            {
                "rule_hit": "USA_NOTRADE_DUAL_ELITE_U72F27",
                "trigger": "NEW_NOTRADE",
                "conditions": {"dual_buy_badge_eq": "BUY_ELITE"},
            },
            {
                "rule_hit": "USA_NOTRADE_DUAL_STRONG_U66F32",
                "trigger": "NEW_NOTRADE",
                "conditions": {"dual_buy_badge_eq": "BUY_STRONG"},
            },
            {
                "rule_hit": "USA_NOTRADE_DUAL_QUALIFIED_U60F35",
                "trigger": "NEW_NOTRADE",
                "conditions": {"dual_buy_badge_eq": "BUY_QUALIFIED"},
            },
        ],
    }
    rows = [
        {"section": "NEW_PASS", "ticker": "P1", "dual_buy_badge": "BUY_PREMIUM"},
        {"section": "NEW_PASS", "ticker": "P2", "dual_buy_badge": "BUY_ELITE"},
        {"section": "NEW_PASS", "ticker": "P3", "dual_buy_badge": "BUY_STRONG"},
        {"section": "NEW_PASS", "ticker": "P4", "dual_buy_badge": "BUY_QUALIFIED"},
        {"section": "NEW_NOTRADE", "ticker": "N1", "dual_buy_badge": "BUY_PREMIUM"},
        {"section": "NEW_NOTRADE", "ticker": "N2", "dual_buy_badge": "BUY_ELITE"},
        {"section": "NEW_NOTRADE", "ticker": "N3", "dual_buy_badge": "BUY_STRONG"},
        {"section": "NEW_NOTRADE", "ticker": "N4", "dual_buy_badge": "BUY_QUALIFIED"},
    ]

    out, missing_field_count = apply_buy_rules(rows, config, buy_section_name="BUYS")

    assert missing_field_count == 0
    assert sorted(row["rule_hit"] for row in out) == [
        "USA_NOTRADE_DUAL_ELITE_U72F27",
        "USA_NOTRADE_DUAL_PREMIUM_U80F20",
        "USA_NOTRADE_DUAL_QUALIFIED_U60F35",
        "USA_NOTRADE_DUAL_STRONG_U66F32",
        "USA_PASS_DUAL_ELITE_U72F27",
        "USA_PASS_DUAL_PREMIUM_U80F20",
        "USA_PASS_DUAL_QUALIFIED_U60F35",
        "USA_PASS_DUAL_STRONG_U66F32",
    ]


def test_apply_buy_rules_dual_rules_only_highest_category_matches() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "rules": [
            {"rule_hit": "USA_PASS_DUAL_PREMIUM_U80F20", "trigger": "NEW_PASS", "conditions": {"dual_buy_badge_eq": "BUY_PREMIUM"}},
            {"rule_hit": "USA_PASS_DUAL_ELITE_U72F27", "trigger": "NEW_PASS", "conditions": {"dual_buy_badge_eq": "BUY_ELITE"}},
            {"rule_hit": "USA_PASS_DUAL_STRONG_U66F32", "trigger": "NEW_PASS", "conditions": {"dual_buy_badge_eq": "BUY_STRONG"}},
            {
                "rule_hit": "USA_PASS_DUAL_QUALIFIED_U60F35",
                "trigger": "NEW_PASS",
                "conditions": {"dual_buy_badge_eq": "BUY_QUALIFIED"},
            },
        ],
    }
    rows = [{"section": "NEW_PASS", "ticker": "AAA", "dual_buy_badge": "BUY_PREMIUM"}]

    out, missing_field_count = apply_buy_rules(rows, config, buy_section_name="BUYS")

    assert missing_field_count == 0
    assert len(out) == 1
    assert out[0]["rule_hit"] == "USA_PASS_DUAL_PREMIUM_U80F20"


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


def test_format_buy_badges_strips_key_prefix_from_display_values() -> None:
    value = '["downtrend_entry_type=SLOW_SOFT","LOW_VOLUME"]'

    assert _format_cell(value) == '["SLOW_SOFT","LOW_VOLUME"]'
    assert _format_csv_value(value) == '["SLOW_SOFT","LOW_VOLUME"]'
