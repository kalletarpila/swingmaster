from __future__ import annotations

import pytest

from swingmaster.cli.daily_report import apply_buy_rules, validate_buy_rules_config


def test_apply_buy_rules_fastpass_threshold_and_ordering() -> None:
    config = {
        "market": "FIN",
        "version": 1,
        "rules": [
            {
                "rule_hit": "FIN_PASS_FP60",
                "trigger": "NEW_PASS",
                "conditions": {
                    "fastpass_score_gte": 0.60,
                },
            }
        ],
    }
    rows = [
        {
            "section": "NEW_PASS",
            "ticker": "ZZZ",
            "ew_score_fastpass": 0.70,
            "ew_level_fastpass": 1,
        },
        {
            "section": "NEW_PASS",
            "ticker": "AAA",
            "ew_score_fastpass": 0.70,
            "ew_level_fastpass": 1,
        },
        {
            "section": "NEW_PASS",
            "ticker": "BBB",
            "ew_score_fastpass": 0.40,
            "ew_level_fastpass": 0,
        },
    ]

    out, missing_field_count = apply_buy_rules(rows, config, buy_section_name="BUYS")

    assert missing_field_count == 0
    assert [row["ticker"] for row in out] == ["AAA", "ZZZ"]
    assert all(row["section"] == "BUYS" for row in out)
    assert all(row["rule_hit"] == "FIN_PASS_FP60" for row in out)


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
