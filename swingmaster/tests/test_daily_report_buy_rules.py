from __future__ import annotations

import sqlite3
from pathlib import Path

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


def test_validate_buy_rules_config_accepts_enabled_boolean() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "rules": [
            {
                "rule_hit": "USA_PASS_FP80",
                "trigger": "NEW_PASS",
                "enabled": False,
                "conditions": {"fastpass_score_gte": 0.80},
            }
        ],
    }

    validated = validate_buy_rules_config(config, "USA")
    assert validated["rules"][0]["enabled"] is False


def test_validate_buy_rules_config_rejects_non_boolean_enabled() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "rules": [
            {
                "rule_hit": "USA_PASS_FP80",
                "trigger": "NEW_PASS",
                "enabled": "yes",
                "conditions": {"fastpass_score_gte": 0.80},
            }
        ],
    }

    with pytest.raises(ValueError, match="enabled must be a boolean"):
        validate_buy_rules_config(config, "USA")


def test_validate_buy_rules_config_accepts_dual_badge_bands() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "dual_badge_bands": [
            {"badge": "DUAL_PREMIUM", "up20_prob_gte": 0.80, "fail10_prob_lte": 0.20},
            {"badge": "DUAL_ELITE", "up20_prob_gte": 0.72, "fail10_prob_lte": 0.27},
        ],
        "rules": [
            {"rule_hit": "USA_PASS_DUAL", "trigger": "NEW_PASS", "conditions": {"dual_badge_present_eq": True}}
        ],
    }

    validated = validate_buy_rules_config(config, "USA")
    assert len(validated["dual_badge_bands"]) == 2


def test_validate_buy_rules_config_rejects_invalid_dual_badge_bands() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "dual_badge_bands": [
            {"badge": "UNKNOWN", "up20_prob_gte": 0.80, "fail10_prob_lte": 0.20},
        ],
        "rules": [
            {"rule_hit": "USA_PASS_DUAL", "trigger": "NEW_PASS", "conditions": {"dual_badge_present_eq": True}}
        ],
    }

    with pytest.raises(ValueError, match="unknown dual badge"):
        validate_buy_rules_config(config, "USA")


def test_load_buy_rules_config_filters_disabled_rules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "usa.json"
    config_path.write_text(
        """
{
  "market": "USA",
  "version": 1,
  "rules": [
    {
      "rule_hit": "RULE_ENABLED_EXPLICIT",
      "trigger": "NEW_PASS",
      "enabled": true,
      "conditions": {"fastpass_score_gte": 0.8}
    },
    {
      "rule_hit": "RULE_DISABLED",
      "trigger": "NEW_PASS",
      "enabled": false,
      "conditions": {"fastpass_score_gte": 0.8}
    },
    {
      "rule_hit": "RULE_ENABLED_DEFAULT",
      "trigger": "NEW_PASS",
      "conditions": {"fastpass_score_gte": 0.8}
    }
  ]
}
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr("swingmaster.cli.daily_report.BUY_RULES_DIR", tmp_path)

    loaded = load_buy_rules_config("USA")
    rule_ids = [str(rule["rule_hit"]) for rule in loaded["rules"]]

    assert "RULE_ENABLED_EXPLICIT" in rule_ids
    assert "RULE_ENABLED_DEFAULT" in rule_ids
    assert "RULE_DISABLED" not in rule_ids


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
    assert "USA_PASS_DUAL" in rule_ids
    assert "USA_NOTRADE_DUAL" not in rule_ids


def test_apply_buy_rules_dual_rules_use_single_rule_hit_and_badge_presence() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "dual_badge_bands": [
            {"badge": "DUAL_PREMIUM", "up20_prob_gte": 0.80, "fail10_prob_lte": 0.20},
            {"badge": "DUAL_ELITE", "up20_prob_gte": 0.72, "fail10_prob_lte": 0.27},
            {"badge": "DUAL_STRONG", "up20_prob_gte": 0.66, "fail10_prob_lte": 0.32},
            {"badge": "DUAL_QUALIFIED", "up20_prob_gte": 0.60, "fail10_prob_lte": 0.35},
        ],
        "rules": [
            {"rule_hit": "USA_PASS_DUAL", "trigger": "NEW_PASS", "conditions": {"dual_badge_present_eq": True}},
            {"rule_hit": "USA_NOTRADE_DUAL", "trigger": "NEW_NOTRADE", "conditions": {"dual_badge_present_eq": True}},
        ],
    }
    rows = [
        {"section": "NEW_PASS", "ticker": "P1", "up20_prob": 0.90, "fail10_prob": 0.15},
        {"section": "NEW_PASS", "ticker": "P2", "up20_prob": 0.75, "fail10_prob": 0.25},
        {"section": "NEW_PASS", "ticker": "P3", "up20_prob": 0.67, "fail10_prob": 0.30},
        {"section": "NEW_PASS", "ticker": "P4", "up20_prob": 0.61, "fail10_prob": 0.34},
        {"section": "NEW_NOTRADE", "ticker": "N1", "up20_prob": 0.90, "fail10_prob": 0.15},
        {"section": "NEW_NOTRADE", "ticker": "N2", "up20_prob": 0.75, "fail10_prob": 0.25},
        {"section": "NEW_NOTRADE", "ticker": "N3", "up20_prob": 0.67, "fail10_prob": 0.30},
        {"section": "NEW_NOTRADE", "ticker": "N4", "up20_prob": 0.61, "fail10_prob": 0.34},
    ]

    out, missing_field_count = apply_buy_rules(rows, config, buy_section_name="BUYS")

    assert missing_field_count == 0
    assert sorted(row["rule_hit"] for row in out) == [
        "USA_NOTRADE_DUAL",
        "USA_NOTRADE_DUAL",
        "USA_NOTRADE_DUAL",
        "USA_NOTRADE_DUAL",
        "USA_PASS_DUAL",
        "USA_PASS_DUAL",
        "USA_PASS_DUAL",
        "USA_PASS_DUAL",
    ]


def test_apply_buy_rules_dual_badge_assignment_priority_first_match() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "dual_badge_bands": [
            {"badge": "DUAL_PREMIUM", "up20_prob_gte": 0.80, "fail10_prob_lte": 0.20},
            {"badge": "DUAL_ELITE", "up20_prob_gte": 0.72, "fail10_prob_lte": 0.27},
        ],
        "rules": [
            {"rule_hit": "USA_PASS_DUAL", "trigger": "NEW_PASS", "conditions": {"dual_buy_badge_eq": "DUAL_PREMIUM"}},
        ],
    }
    rows = [{"section": "NEW_PASS", "ticker": "AAA", "up20_prob": 0.90, "fail10_prob": 0.10}]

    out, missing_field_count = apply_buy_rules(rows, config, buy_section_name="BUYS")

    assert missing_field_count == 0
    assert len(out) == 1
    assert out[0]["rule_hit"] == "USA_PASS_DUAL"
    assert out[0]["dual_buy_badge"] == "DUAL_PREMIUM"


def test_apply_buy_rules_dual_badge_assignment_fail_closed_missing_prob() -> None:
    config = {
        "market": "USA",
        "version": 1,
        "dual_badge_bands": [
            {"badge": "DUAL_PREMIUM", "up20_prob_gte": 0.80, "fail10_prob_lte": 0.20},
        ],
        "rules": [
            {"rule_hit": "USA_PASS_DUAL", "trigger": "NEW_PASS", "conditions": {"dual_badge_present_eq": True}},
        ],
    }
    rows = [{"section": "NEW_PASS", "ticker": "AAA", "up20_prob": 0.90}]

    out, missing_field_count = apply_buy_rules(rows, config, buy_section_name="BUYS")

    assert missing_field_count == 0
    assert out == []


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
