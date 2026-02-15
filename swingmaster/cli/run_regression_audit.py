import argparse
import json
import sqlite3
import sys
from datetime import datetime

DEFAULT_DB = "/home/kalle/projects/swingmaster/swingmaster_rc.db"

CHECKS = [
    {
        "id": "A",
        "name": "lock_with_invalidated",
        "description": "MIN_STATE_AGE_LOCK + POLICY:INVALIDATED must be 0",
        "sql": """
SELECT COUNT(*) FROM rc_state_daily d
WHERE d.reasons_json IS NOT NULL
  AND EXISTS (SELECT 1 FROM json_each(d.reasons_json) WHERE value='MIN_STATE_AGE_LOCK')
  AND EXISTS (SELECT 1 FROM json_each(d.reasons_json) WHERE value='POLICY:INVALIDATED');
""",
    },
    {
        "id": "B",
        "name": "blocked_without_lock",
        "description": "POLICY:INVALIDATION_BLOCKED_BY_LOCK without lock must be 0",
        "sql": """
SELECT COUNT(*) FROM rc_state_daily d
WHERE d.reasons_json IS NOT NULL
  AND EXISTS (SELECT 1 FROM json_each(d.reasons_json) WHERE value='POLICY:INVALIDATION_BLOCKED_BY_LOCK')
  AND NOT EXISTS (SELECT 1 FROM json_each(d.reasons_json) WHERE value='MIN_STATE_AGE_LOCK');
""",
    },
    {
        "id": "C",
        "name": "invalidated_and_blocked_same_day",
        "description": "POLICY:INVALIDATED and POLICY:INVALIDATION_BLOCKED_BY_LOCK same day must be 0",
        "sql": """
SELECT COUNT(*) FROM rc_state_daily d
WHERE d.reasons_json IS NOT NULL
  AND EXISTS (SELECT 1 FROM json_each(d.reasons_json) WHERE value='POLICY:INVALIDATED')
  AND EXISTS (SELECT 1 FROM json_each(d.reasons_json) WHERE value='POLICY:INVALIDATION_BLOCKED_BY_LOCK');
""",
    },
    {
        "id": "D",
        "name": "empty_transition_reasons",
        "description": "transition rows with empty reasons_json must be 0",
        "sql": """
SELECT COUNT(*) FROM rc_transition
WHERE reasons_json IS NULL OR reasons_json='[]';
""",
    },
    {
        "id": "E",
        "name": "entry_window_illegal_exit",
        "description": "ENTRY_WINDOW exits only to PASS/NO_TRADE",
        "sql": """
SELECT COUNT(*) FROM rc_transition
WHERE from_state='ENTRY_WINDOW'
  AND to_state NOT IN ('PASS','NO_TRADE');
""",
    },
    {
        "id": "F",
        "name": "stabilizing_to_entry_without_gate",
        "description": "STABILIZING -> ENTRY_WINDOW must have ENTRY_CONDITIONS_MET",
        "sql": """
SELECT COUNT(*) FROM rc_transition t
WHERE t.from_state='STABILIZING'
  AND t.to_state='ENTRY_WINDOW'
  AND NOT EXISTS (SELECT 1 FROM json_each(t.reasons_json) WHERE value='ENTRY_CONDITIONS_MET');
""",
    },
    {
        "id": "G",
        "name": "nt_to_early_without_policy_reason",
        "description": "NO_TRADE -> DOWNTREND_EARLY must have POLICY:TREND_STARTED",
        "sql": """
SELECT COUNT(*) FROM rc_transition t
WHERE t.from_state='NO_TRADE'
  AND t.to_state='DOWNTREND_EARLY'
  AND NOT EXISTS (SELECT 1 FROM json_each(t.reasons_json) WHERE value='POLICY:TREND_STARTED');
""",
    },
    {
        "id": "H",
        "name": "nt_to_early_without_provider_trend_started",
        "description": "NO_TRADE -> DOWNTREND_EARLY must have provider TREND_STARTED",
        "sql": """
SELECT COUNT(*) FROM rc_transition t
JOIN rc_signal_daily s
  ON s.ticker=t.ticker AND s.date=t.date AND s.run_id=t.run_id
WHERE t.from_state='NO_TRADE'
  AND t.to_state='DOWNTREND_EARLY'
  AND NOT EXISTS (SELECT 1 FROM json_each(s.signal_keys_json) WHERE value='TREND_STARTED');
""",
    },
    {
        "id": "I",
        "name": "early_illegal_exit",
        "description": "DOWNTREND_EARLY exits only to NO_TRADE/STABILIZING/DOWNTREND_LATE",
        "sql": """
SELECT COUNT(*) FROM rc_transition
WHERE from_state='DOWNTREND_EARLY'
  AND to_state NOT IN ('NO_TRADE','STABILIZING','DOWNTREND_LATE');
""",
    },
    {
        "id": "J",
        "name": "late_illegal_exit",
        "description": "DOWNTREND_LATE exits only to NO_TRADE/STABILIZING",
        "sql": """
SELECT COUNT(*) FROM rc_transition
WHERE from_state='DOWNTREND_LATE'
  AND to_state NOT IN ('NO_TRADE','STABILIZING');
""",
    },
    {
        "id": "K",
        "name": "transitions_missing_signal_row",
        "description": "transition days must have rc_signal_daily row",
        "sql": """
SELECT COUNT(*) FROM rc_transition t
LEFT JOIN rc_signal_daily s
  ON s.ticker=t.ticker AND s.date=t.date AND s.run_id=t.run_id
WHERE s.ticker IS NULL;
""",
    },
]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run RC regression audit checks")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to rc sqlite db")
    parser.add_argument("--fail-fast", action="store_true", help="Stop after first failure")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    return parser.parse_args(argv)


def _fetch_count(conn: sqlite3.Connection, sql: str) -> int:
    cur = conn.execute(sql)
    row = cur.fetchone()
    return int(row[0]) if row is not None else 0


def _get_info(conn: sqlite3.Connection) -> dict:
    max_date = conn.execute("SELECT MAX(date) FROM rc_state_daily;").fetchone()[0]
    state_rows = conn.execute("SELECT COUNT(*) FROM rc_state_daily;").fetchone()[0]
    transition_rows = conn.execute("SELECT COUNT(*) FROM rc_transition;").fetchone()[0]
    signal_rows = conn.execute("SELECT COUNT(*) FROM rc_signal_daily;").fetchone()[0]
    return {
        "max_date": max_date,
        "state_rows": state_rows,
        "transition_rows": transition_rows,
        "signal_rows": signal_rows,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    conn = None
    results = []
    try:
        conn = sqlite3.connect(args.db)
        conn.execute("PRAGMA temp_store=MEMORY;")

        info = _get_info(conn)
        failed_checks = 0
        fail_fast_triggered = False

        for chk in CHECKS:
            if args.fail_fast and fail_fast_triggered:
                results.append(
                    {
                        "id": chk["id"],
                        "name": chk["name"],
                        "expected": 0,
                        "observed": None,
                        "status": "SKIPPED",
                    }
                )
                continue

            observed = _fetch_count(conn, chk["sql"])
            status = "PASS" if observed == 0 else "FAIL"
            if status == "FAIL":
                failed_checks += 1
                if args.fail_fast:
                    fail_fast_triggered = True

            results.append(
                {
                    "id": chk["id"],
                    "name": chk["name"],
                    "expected": 0,
                    "observed": observed,
                    "status": status,
                }
            )

        overall = "PASS" if failed_checks == 0 else "FAIL"
        exit_code = 0 if failed_checks == 0 else 2

        if args.json:
            payload = {
                "db_path": args.db,
                "timestamp": timestamp,
                "info": info,
                "checks": results,
                "overall": overall,
                "failed_checks": failed_checks,
                "exit_code": exit_code,
            }
            print(json.dumps(payload, ensure_ascii=False))
        else:
            print(f"DB: {args.db}")
            print(f"Timestamp: {timestamp}")
            print("INFO:")
            print(f"  max_date: {info['max_date']}")
            print(f"  state_rows: {info['state_rows']}")
            print(f"  transition_rows: {info['transition_rows']}")
            print(f"  signal_rows: {info['signal_rows']}")
            print("CHECKS:")
            for chk, res in zip(CHECKS, results):
                if res["status"] == "SKIPPED":
                    print(
                        f"  [{res['id']}] {chk['name']}: SKIPPED (fail-fast)"
                    )
                    continue
                print(
                    f"  [{res['id']}] {chk['name']}: {res['status']} (observed={res['observed']}, expected=0)"
                )
                print(f"       {chk['description']}")
            print(f"OVERALL: {overall}")
            print(f"failed_checks: {failed_checks}")
            print(f"exit_code={exit_code}")

        return exit_code
    except Exception as exc:
        if args.json:
            payload = {
                "db_path": args.db,
                "timestamp": timestamp,
                "info": None,
                "checks": [],
                "overall": "ERROR",
                "failed_checks": None,
                "exit_code": 1,
                "error": str(exc),
            }
            print(json.dumps(payload, ensure_ascii=False))
        else:
            print(f"DB: {args.db}")
            print(f"Timestamp: {timestamp}")
            print(f"ERROR: {exc}")
            print("exit_code=1")
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
