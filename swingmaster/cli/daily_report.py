from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


ROOT = Path("/home/kalle/projects/swingmaster")
SQL_TEMPLATE_PATH = ROOT / "daily_reports" / "fin_daily_report.sql"
BUY_RULES_DIR = ROOT / "daily_reports" / "buy_rules"
WEEKLY_REPORTS_DIR = ROOT / "weekly_reports"
OUTPUT_COLUMNS = [
    "section",
    "as_of_date",
    "market",
    "ticker",
    "state_prev",
    "state_today",
    "from_state",
    "to_state",
    "event_date",
    "entry_window_date",
    "first_time_in_ew_ever",
    "days_in_stabilizing_before_ew",
    "days_in_current_episode",
    "days_in_ew_trading",
    "ew_score_fastpass",
    "ew_level_fastpass",
    "ew_score_rolling",
    "ew_level_rolling",
    "regime",
    "entry_window_exit_state",
    "fail10_prob",
    "up20_prob",
    "rule_hit",
    "buy_badges",
]
REPORT_RAW_COLUMNS = [column for column in OUTPUT_COLUMNS if column != "buy_badges"]
ALLOWED_TRIGGERS = {"NEW_EW", "NEW_PASS", "NEW_NOTRADE", "EW_SNAPSHOT"}
ALLOWED_CONDITION_KEYS = {
    "fastpass_score_gte",
    "fastpass_level_eq",
    "rolling_end_level_eq",
    "dual_buy_badge_eq",
    "regime_eq",
    "entry_window_exit_state_eq",
    "fail10_prob_gte",
    "fail10_prob_lte",
    "up20_prob_gte",
    "days_in_current_episode_gte",
    "days_in_current_episode_lte",
    "days_in_stabilizing_before_ew_gte",
    "days_in_stabilizing_before_ew_eq",
}
CONDITION_FIELD_MAP = {
    "fastpass_score_gte": "ew_score_fastpass",
    "fastpass_level_eq": "ew_level_fastpass",
    "rolling_end_level_eq": "ew_level_rolling",
    "dual_buy_badge_eq": "dual_buy_badge",
    "regime_eq": "regime",
    "entry_window_exit_state_eq": "entry_window_exit_state",
    "fail10_prob_gte": "fail10_prob",
    "fail10_prob_lte": "fail10_prob",
    "up20_prob_gte": "up20_prob",
    "days_in_current_episode_gte": "days_in_current_episode",
    "days_in_current_episode_lte": "days_in_current_episode",
    "days_in_stabilizing_before_ew_gte": "days_in_stabilizing_before_ew",
    "days_in_stabilizing_before_ew_eq": "days_in_stabilizing_before_ew",
}


@dataclass(frozen=True)
class MarketConfig:
    market_arg: str
    display_market: str
    rules_market: str
    db_path: Path
    hardcoded_buy_section: str
    hardcoded_buy_rule: str
    hardcoded_fp_threshold: float
    output_prefix: str


MARKET_CONFIGS: Dict[str, MarketConfig] = {
    "fin": MarketConfig(
        market_arg="fin",
        display_market="FIN",
        rules_market="FIN",
        db_path=ROOT / "swingmaster_rc.db",
        hardcoded_buy_section="BUYS_FIN",
        hardcoded_buy_rule="FIN_PASS_FP60",
        hardcoded_fp_threshold=0.60,
        output_prefix="fin",
    ),
    "omxh": MarketConfig(
        market_arg="omxh",
        display_market="FIN",
        rules_market="FIN",
        db_path=ROOT / "swingmaster_rc.db",
        hardcoded_buy_section="BUYS_FIN",
        hardcoded_buy_rule="FIN_PASS_FP60",
        hardcoded_fp_threshold=0.60,
        output_prefix="fin",
    ),
    "se": MarketConfig(
        market_arg="se",
        display_market="SE",
        rules_market="SE",
        db_path=ROOT / "swingmaster_rc_se.db",
        hardcoded_buy_section="BUYS_1_SE",
        hardcoded_buy_rule="SE_BUY_1_FP80",
        hardcoded_fp_threshold=0.80,
        output_prefix="se",
    ),
    "omxs": MarketConfig(
        market_arg="omxs",
        display_market="SE",
        rules_market="SE",
        db_path=ROOT / "swingmaster_rc_se.db",
        hardcoded_buy_section="BUYS_1_SE",
        hardcoded_buy_rule="SE_BUY_1_FP80",
        hardcoded_fp_threshold=0.80,
        output_prefix="se",
    ),
    "usa": MarketConfig(
        market_arg="usa",
        display_market="USA",
        rules_market="USA",
        db_path=ROOT / "swingmaster_rc_usa_2024_2025.db",
        hardcoded_buy_section="BUYS_USA",
        hardcoded_buy_rule="USA_PASS_FP80",
        hardcoded_fp_threshold=0.80,
        output_prefix="usa",
    ),
    "usa500": MarketConfig(
        market_arg="usa500",
        display_market="USA500",
        rules_market="USA",
        db_path=ROOT / "swingmaster_rc_usa_500.db",
        hardcoded_buy_section="BUYS_USA500",
        hardcoded_buy_rule="USA500_PASS_FP80",
        hardcoded_fp_threshold=0.80,
        output_prefix="usa500",
    ),
}


def infer_market_from_db_path(db_path: str) -> str:
    lower = db_path.lower()
    if "swingmaster_rc_se" in lower:
        return "se"
    if "swingmaster_rc_usa" in lower:
        return "usa"
    if "swingmaster_rc" in lower:
        return "fin"
    raise ValueError(f"Cannot infer market from rc-db path: {db_path}")


def validate_buy_rules_config(config: Dict[str, Any], requested_market: str) -> Dict[str, Any]:
    top_level_keys = set(config.keys())
    if top_level_keys != {"market", "version", "rules"}:
        raise ValueError("Invalid buy-rules config: top-level keys must be exactly market, version, rules")
    if config["version"] != 1 or not isinstance(config["version"], int):
        raise ValueError("Invalid buy-rules config: version must be integer 1")
    if config["market"] != requested_market:
        raise ValueError(
            f"Invalid buy-rules config: market {config['market']} does not match requested market {requested_market}"
        )
    if not isinstance(config["rules"], list):
        raise ValueError("Invalid buy-rules config: rules must be a list")

    for rule in config["rules"]:
        rule_keys = set(rule.keys())
        required_rule_keys = {"rule_hit", "trigger", "conditions"}
        optional_rule_keys = {"enabled"}
        if not required_rule_keys.issubset(rule_keys) or not rule_keys.issubset(required_rule_keys | optional_rule_keys):
            raise ValueError(
                "Invalid buy-rules config: each rule must have rule_hit, trigger, conditions and optional enabled"
            )
        if "enabled" in rule and not isinstance(rule["enabled"], bool):
            raise ValueError("Invalid buy-rules config: enabled must be a boolean when present")
        if rule["trigger"] not in ALLOWED_TRIGGERS:
            raise ValueError(f"Invalid buy-rules config: unknown trigger {rule['trigger']}")
        if not isinstance(rule["conditions"], dict):
            raise ValueError("Invalid buy-rules config: conditions must be an object")
        for condition_key in rule["conditions"]:
            if condition_key not in ALLOWED_CONDITION_KEYS:
                raise ValueError(f"Invalid buy-rules config: unknown condition key {condition_key}")

    return config


def load_buy_rules_config(market: str) -> Dict[str, Any]:
    path = BUY_RULES_DIR / f"{market.lower()}.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing buy-rules file: {path}")
    config = json.loads(path.read_text(encoding="utf-8"))
    validated = validate_buy_rules_config(config, market)
    active_rules = [rule for rule in validated["rules"] if bool(rule.get("enabled", True))]
    return {
        "market": validated["market"],
        "version": validated["version"],
        "rules": active_rules,
    }


def _compare_condition(row_value: Any, op: str, threshold: Any) -> bool:
    if row_value is None:
        return False
    if op == "gte":
        return row_value >= threshold
    if op == "lte":
        return row_value <= threshold
    if op == "eq":
        return row_value == threshold
    raise ValueError(f"Unsupported condition operator: {op}")


def apply_buy_rules(
    base_rows: Sequence[Dict[str, Any]],
    config: Dict[str, Any],
    buy_section_name: str = "BUYS",
) -> Tuple[List[Dict[str, Any]], int]:
    out: List[Dict[str, Any]] = []
    missing_field_count = 0

    for rule in config["rules"]:
        trigger = rule["trigger"]
        matched_rows: List[Dict[str, Any]] = []
        for row in base_rows:
            if row.get("section") != trigger:
                continue
            if row.get("ticker") in {None, "", "(none)"}:
                continue

            ok = True
            for condition_key, condition_value in rule["conditions"].items():
                field_name = CONDITION_FIELD_MAP[condition_key]
                if field_name not in row:
                    missing_field_count += 1
                    ok = False
                    break

                if condition_key.endswith("_gte"):
                    if not _compare_condition(row.get(field_name), "gte", condition_value):
                        ok = False
                        break
                elif condition_key.endswith("_lte"):
                    if not _compare_condition(row.get(field_name), "lte", condition_value):
                        ok = False
                        break
                elif condition_key.endswith("_eq"):
                    if not _compare_condition(row.get(field_name), "eq", condition_value):
                        ok = False
                        break
                else:
                    raise ValueError(f"Unsupported condition key: {condition_key}")

            if ok:
                buy_row = dict(row)
                buy_row["section"] = buy_section_name
                buy_row["rule_hit"] = rule["rule_hit"]
                matched_rows.append(buy_row)

        matched_rows.sort(key=lambda r: str(r.get("ticker") or ""))
        out.extend(matched_rows)

    return out, missing_field_count


def group_buy_rows(buy_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    rule_hits_by_key: Dict[Tuple[str, str], List[str]] = {}

    for row in buy_rows:
        key = (str(row.get("ticker") or ""), str(row.get("event_date") or ""))
        if key not in grouped:
            grouped[key] = dict(row)
            rule_hits_by_key[key] = []

        rule_hit = row.get("rule_hit")
        if rule_hit is not None and str(rule_hit) not in rule_hits_by_key[key]:
            rule_hits_by_key[key].append(str(rule_hit))

    out: List[Dict[str, Any]] = []
    for key in sorted(grouped.keys(), key=lambda item: (item[1], item[0])):
        grouped_row = grouped[key]
        grouped_row["rule_hit"] = ";".join(rule_hits_by_key[key]) if rule_hits_by_key[key] else None
        out.append(grouped_row)
    return out


def _format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list) and all(isinstance(item, str) for item in parsed):
            cleaned = [item.split("=", 1)[1] if "=" in item else item for item in parsed]
            return json.dumps(cleaned, separators=(",", ":"))
    return str(value)


def build_report_rows_json_mode(
    all_rows: Sequence[Dict[str, Any]],
    buy_rows: Sequence[Dict[str, Any]],
    market_label: str,
    as_of_date: str,
) -> List[Dict[str, Any]]:
    section_order = ["NEW_EW", "NEW_PASS", "NEW_NOTRADE", "BUYS", "EW_SNAPSHOT", "ALERTS"]
    base_by_section: Dict[str, List[Dict[str, Any]]] = {name: [] for name in section_order if name != "BUYS"}
    for row in all_rows:
        section = row.get("section")
        if section in base_by_section and not str(section).startswith("BUYS"):
            base_by_section[section].append(dict(row))

    buy_rows_sorted = list(buy_rows)
    if not buy_rows_sorted:
        buy_rows_sorted = [
            {
                "section": "BUYS",
                "as_of_date": as_of_date,
                "market": market_label,
                "ticker": "(none)",
                "state_prev": None,
                "state_today": None,
                "from_state": None,
                "to_state": None,
                "event_date": None,
                "entry_window_date": None,
                "first_time_in_ew_ever": None,
                "days_in_stabilizing_before_ew": None,
                "days_in_current_episode": None,
                "days_in_ew_trading": None,
                "ew_score_fastpass": None,
                "ew_level_fastpass": None,
                "ew_score_rolling": None,
                "ew_level_rolling": None,
                "regime": None,
                "entry_window_exit_state": None,
                "fail10_prob": None,
                "up20_prob": None,
                "rule_hit": "EMPTY_SECTION",
            }
        ]

    out: List[Dict[str, Any]] = []
    for section in section_order:
        if section == "BUYS":
            out.extend(buy_rows_sorted)
        else:
            out.extend(base_by_section.get(section, []))
    return out


def _render_text(rows: Sequence[Dict[str, Any]]) -> str:
    str_rows = [[_format_cell(row.get(col)) for col in OUTPUT_COLUMNS] for row in rows]
    widths = [len(col) for col in OUTPUT_COLUMNS]
    for row in str_rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    def fmt_row(values: Sequence[str]) -> str:
        return "  ".join(value.ljust(widths[idx]) for idx, value in enumerate(values))

    out = io.StringIO()
    out.write(fmt_row(OUTPUT_COLUMNS) + "\n")
    out.write(fmt_row(["-" * width for width in widths]) + "\n")
    for row in str_rows:
        out.write(fmt_row(row) + "\n")
    return out.getvalue()


def _format_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value}".replace(".", ",")
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list) and all(isinstance(item, str) for item in parsed):
            cleaned = [item.split("=", 1)[1] if "=" in item else item for item in parsed]
            return json.dumps(cleaned, separators=(",", ":"))
    return str(value)


def _render_csv(rows: Sequence[Dict[str, Any]]) -> str:
    out = io.StringIO()
    writer = csv.writer(out, delimiter=";", lineterminator="\n")
    writer.writerow(OUTPUT_COLUMNS)
    for row in rows:
        writer.writerow([_format_csv_value(row.get(col)) for col in OUTPUT_COLUMNS])
    return out.getvalue()


def _substitute_sql_template(template: str, config: MarketConfig, as_of_date: str) -> str:
    raw = (
        template.replace("__AS_OF_DATE__", as_of_date)
        .replace("__MARKET__", config.display_market)
        .replace("__BUY_SECTION__", config.hardcoded_buy_section)
        .replace("__BUY_RULE__", config.hardcoded_buy_rule)
        .replace("__FP_THRESHOLD__", f"{config.hardcoded_fp_threshold:.2f}")
    )
    filtered_lines = [line for line in raw.splitlines() if not line.lstrip().startswith(".")]
    return "\n".join(filtered_lines)


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    if not _has_table(conn, table_name):
        return False
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row[1]) == column_name for row in rows)


def _load_buy_badges_by_key(conn: sqlite3.Connection, buy_rows: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, str], str]:
    if not buy_rows or not _has_column(conn, "rc_transactions_simu", "buy_badges"):
        return {}

    keys = sorted(
        {
            (str(row.get("ticker") or ""), str(row.get("event_date") or ""))
            for row in buy_rows
            if row.get("ticker") not in {None, "", "(none)"} and row.get("event_date") not in {None, ""}
        }
    )
    if not keys:
        return {}

    out: Dict[Tuple[str, str], str] = {}
    for ticker, buy_date in keys:
        row = conn.execute(
            """
            SELECT buy_badges
            FROM rc_transactions_simu
            WHERE ticker = ?
              AND buy_date = ?
              AND buy_badges IS NOT NULL
              AND buy_badges <> '[]'
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (ticker, buy_date),
        ).fetchone()
        if row is not None and row[0] not in {None, "", "[]"}:
            out[(ticker, buy_date)] = str(row[0])
    return out


def _attach_buy_badges(
    conn: sqlite3.Connection,
    buy_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    badges_by_key = _load_buy_badges_by_key(conn, buy_rows)
    out: List[Dict[str, Any]] = []
    for row in buy_rows:
        enriched = dict(row)
        key = (str(row.get("ticker") or ""), str(row.get("event_date") or ""))
        enriched["buy_badges"] = badges_by_key.get(key)
        out.append(enriched)
    return out


def _ensure_temp_ew_score_table(conn: sqlite3.Connection) -> bool:
    if _has_table(conn, "rc_ew_score_daily"):
        return False
    conn.executescript(
        """
        CREATE TEMP TABLE rc_ew_score_daily (
          ticker TEXT,
          date TEXT,
          ew_score_fastpass REAL,
          ew_level_fastpass INTEGER,
          ew_score_rolling REAL,
          ew_level_rolling INTEGER
        );
        """
    )
    return True


def _enrich_probabilistic_rule_fields(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> None:
    for row in rows:
        row.setdefault("regime", None)
        row.setdefault("entry_window_exit_state", None)
        row.setdefault("fail10_prob", None)
        row.setdefault("up20_prob", None)

    if not rows:
        return
    if not _has_table(conn, "rc_pipeline_episode"):
        return
    if not _has_table(conn, "rc_episode_regime"):
        return
    if not _has_table(conn, "rc_episode_model_score"):
        return
    if not _has_column(conn, "rc_pipeline_episode", "episode_id"):
        return
    if not _has_column(conn, "rc_pipeline_episode", "entry_window_exit_date"):
        return
    if not _has_column(conn, "rc_pipeline_episode", "entry_window_exit_state"):
        return

    keys = sorted(
        {
            (str(row.get("ticker")), str(row.get("event_date")), str(row.get("to_state") or ""))
            for row in rows
            if row.get("ticker") not in {None, "", "(none)"}
            and row.get("event_date") not in {None, ""}
            and row.get("to_state") in {"PASS", "NO_TRADE"}
        }
    )
    if not keys:
        return

    values_sql = ",".join("(?, ?, ?)" for _ in keys)
    params: List[Any] = []
    for ticker, event_date, expected_exit_state in keys:
        params.extend([ticker, event_date, expected_exit_state])

    sql = f"""
    WITH keys(ticker, event_date, expected_exit_state) AS (
      VALUES {values_sql}
    )
    SELECT
      k.ticker,
      k.event_date,
      k.expected_exit_state,
      reg.ew_exit_regime_combined AS regime,
      ep.entry_window_exit_state AS entry_window_exit_state,
      CASE
        WHEN reg.ew_exit_regime_combined = 'BULL' THEN fail_bull.predicted_probability
        ELSE NULL
      END AS fail10_prob,
      CASE
        WHEN reg.ew_exit_regime_combined = 'BULL' THEN up_bull.predicted_probability
        WHEN reg.ew_exit_regime_combined = 'BEAR' THEN up_bear.predicted_probability
        ELSE NULL
      END AS up20_prob
    FROM keys k
    LEFT JOIN rc_pipeline_episode ep
      ON ep.rowid = (
        SELECT MAX(ep2.rowid)
        FROM rc_pipeline_episode ep2
        WHERE ep2.ticker = k.ticker
          AND ep2.entry_window_exit_date = k.event_date
          AND ep2.entry_window_exit_state = k.expected_exit_state
      )
    LEFT JOIN rc_episode_regime reg
      ON reg.episode_id = ep.episode_id
    LEFT JOIN rc_episode_model_score fail_bull
      ON fail_bull.episode_id = ep.episode_id
     AND fail_bull.model_id = 'FAIL10_BULL_HGB_V1'
    LEFT JOIN rc_episode_model_score up_bull
      ON up_bull.episode_id = ep.episode_id
     AND up_bull.model_id = 'UP20_BULL_HGB_V1'
    LEFT JOIN rc_episode_model_score up_bear
      ON up_bear.episode_id = ep.episode_id
     AND up_bear.model_id = 'UP20_BEAR_HGB_V1'
    """
    resolved_rows = conn.execute(sql, params).fetchall()
    resolved_by_key = {(str(row[0]), str(row[1]), str(row[2])): row for row in resolved_rows}

    for row in rows:
        key = (str(row.get("ticker")), str(row.get("event_date")), str(row.get("to_state") or ""))
        resolved = resolved_by_key.get(key)
        if resolved is None:
            continue
        row["regime"] = resolved[3]
        row["entry_window_exit_state"] = resolved[4]
        row["fail10_prob"] = resolved[5]
        row["up20_prob"] = resolved[6]


def fetch_report_raw_rows(db_path: Path, config: MarketConfig, as_of_date: str) -> Tuple[List[Dict[str, Any]], bool]:
    os.environ.setdefault("SQLITE_TMPDIR", "/tmp")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        missing_fastpass_table = _ensure_temp_ew_score_table(conn)
        conn.execute("PRAGMA temp_store = MEMORY")
        sql = _substitute_sql_template(SQL_TEMPLATE_PATH.read_text(encoding="utf-8"), config, as_of_date)
        conn.executescript(sql)
        rows = conn.execute(
            "SELECT section_sort, " + ", ".join(REPORT_RAW_COLUMNS) + " FROM report_raw ORDER BY section_sort, market, ticker"
        ).fetchall()
        out = []
        for row in rows:
            payload = dict(row)
            payload["buy_badges"] = None
            out.append(payload)
        _enrich_probabilistic_rule_fields(conn, out)
        return out, missing_fastpass_table
    finally:
        conn.close()


def fetch_recent_trading_dates(db_path: Path, as_of_date: str, limit: int = 7) -> List[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT date
            FROM (
              SELECT DISTINCT date
              FROM rc_state_daily
              WHERE date <= ?
              ORDER BY date DESC
              LIMIT ?
            )
            ORDER BY date ASC
            """,
            (as_of_date, limit),
        ).fetchall()
        return [str(row[0]) for row in rows]
    finally:
        conn.close()


def build_daily_report_rows(
    db_path: Path,
    config: MarketConfig,
    as_of_date: str,
) -> Tuple[List[Dict[str, Any]], bool]:
    conn = sqlite3.connect(str(db_path))
    try:
        all_rows, missing_fastpass_table = fetch_report_raw_rows(db_path, config, as_of_date)
        rules_config = load_buy_rules_config(config.rules_market)
        base_rows = [row for row in all_rows if not str(row.get("section", "")).startswith("BUYS")]
        raw_buy_rows, _ = apply_buy_rules(base_rows, rules_config, buy_section_name="BUYS")
        json_buy_rows = group_buy_rows(raw_buy_rows)
        enriched_buy_rows = _attach_buy_badges(conn, json_buy_rows)
        final_rows = build_report_rows_json_mode(
            all_rows=all_rows,
            buy_rows=enriched_buy_rows,
            market_label=config.display_market,
            as_of_date=as_of_date,
        )
        return final_rows, missing_fastpass_table
    finally:
        conn.close()


def build_buy_rows_for_date(
    db_path: Path,
    config: MarketConfig,
    as_of_date: str,
) -> Tuple[List[Dict[str, Any]], bool]:
    conn = sqlite3.connect(str(db_path))
    try:
        all_rows, missing_fastpass_table = fetch_report_raw_rows(db_path, config, as_of_date)
        rules_config = load_buy_rules_config(config.rules_market)
        base_rows = [row for row in all_rows if not str(row.get("section", "")).startswith("BUYS")]
        raw_buy_rows, _ = apply_buy_rules(base_rows, rules_config, buy_section_name="BUYS")
        json_buy_rows = group_buy_rows(raw_buy_rows)
        return _attach_buy_badges(conn, json_buy_rows), missing_fastpass_table
    finally:
        conn.close()


def write_outputs(rows: Sequence[Dict[str, Any]], txt_out: Path, csv_out: Path) -> None:
    txt_out.write_text(_render_text(rows), encoding="utf-8")
    csv_out.write_text(_render_csv(rows), encoding="utf-8")
