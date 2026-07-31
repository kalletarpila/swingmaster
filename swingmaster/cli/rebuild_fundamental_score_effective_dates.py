from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker, repository_root
from swingmaster.fundamentals.score_effective_date import (
    apply_score_effective_date_rows,
    compute_score_effective_dates,
    create_sqlite_backup,
    ensure_score_effective_date_schema,
    invariant_counts,
    rows_to_dicts,
    score_effective_fields_hash,
    score_value_fields_hash,
    summarize,
    utc_timestamp,
    verification_summary,
    write_csv_atomic,
    write_json_atomic,
)
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rebuild score effective-date metadata from exact TTM effective dates")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--tickers-file")
    parser.add_argument("--first-n", type=int)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--backup", action="store_true")
    parser.add_argument("--output-root")
    parser.add_argument("--checkpoint-json")
    parser.add_argument("--summary-json")
    parser.add_argument("--output-csv")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    dry_run = not args.apply
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    if args.apply and not args.backup and not _existing_score_effective_metadata_populated(db_path):
        raise RuntimeError("APPLY_REQUIRES_BACKUP")
    output_root = _output_root(args.output_root, dry_run=dry_run)
    tickers = _selected_tickers(args)
    if args.first_n is not None:
        if args.first_n < 0:
            raise ValueError("FIRST_N_MUST_BE_NON_NEGATIVE")
        tickers = tickers[: args.first_n] if tickers is not None else _first_n_tickers(db_path, args.first_n)

    backup_info: dict[str, Any] | None = None
    apply_counts = {"inserted": 0, "score_value_updates": 0, "score_effective_date_updates": 0, "unchanged": 0}
    if args.apply and args.backup:
        backup_path = output_root / "backups" / f"fundamentals_usa.pre_score_effective_date.{utc_timestamp()}.db"
        backup_info = create_sqlite_backup(db_path, backup_path)
    if args.apply:
        run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        if not dry_run:
            ensure_score_effective_date_schema(conn)
            conn.commit()
        pre_counts = invariant_counts(conn)
        before_effective_hash = score_effective_fields_hash(conn)
        before_score_hash = score_value_fields_hash(conn)
        rows = compute_score_effective_dates(conn, tickers)
        summary = summarize(rows)
        if args.apply:
            with conn:
                apply_counts = apply_score_effective_date_rows(conn, rows)
        after_effective_hash = score_effective_fields_hash(conn)
        after_score_hash = score_value_fields_hash(conn)
        post_counts = invariant_counts(conn)
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        verification = verification_summary(conn)

    payload = {
        "mode": "dry_run" if dry_run else "apply",
        "database_path": str(db_path),
        "runtime_artifact_root": str(output_root),
        "tickers": tickers,
        "pre_counts": pre_counts,
        "post_counts": post_counts,
        "source_table_counts_unchanged": pre_counts == post_counts,
        "quick_check": quick_check,
        "summary": summary,
        "apply_counts": apply_counts,
        "backup": backup_info,
        "score_effective_hash_before": before_effective_hash,
        "score_effective_hash_after": after_effective_hash,
        "score_value_hash_before": before_score_hash,
        "score_value_hash_after": after_score_hash,
        "score_values_unchanged": before_score_hash == after_score_hash,
        "verification": verification,
    }
    artifact_paths = _write_artifacts(args, output_root, payload, rows)
    if args.json:
        print(json.dumps({"summary": summary, "apply_counts": apply_counts, "artifact_paths": artifact_paths}, indent=2, sort_keys=True))
    else:
        _print_summary(payload, artifact_paths)
    if quick_check != "ok" or pre_counts != post_counts or before_score_hash != after_score_hash:
        return 2
    return 0


def _output_root(value: str | None, *, dry_run: bool) -> Path:
    if value:
        root = validate_temp_path(Path(value))
    else:
        root = repository_root() / "temp" / "fundamental_score_effective_date" / utc_timestamp() / ("dry_run" if dry_run else "apply")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _selected_tickers(args: argparse.Namespace) -> list[str] | None:
    tickers = [normalize_ticker(ticker) for ticker in args.ticker if ticker.strip()]
    if args.tickers_file:
        path = validate_temp_path(Path(args.tickers_file), must_exist=True)
        tickers.extend(normalize_ticker(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return sorted(dict.fromkeys(tickers)) if tickers else None


def _first_n_tickers(db_path: Path, first_n: int) -> list[str]:
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ticker
            FROM rc_fundamental_ttm
            ORDER BY ticker ASC
            LIMIT ?
            """,
            (first_n,),
        ).fetchall()
    return [str(row[0]) for row in rows]


def _existing_score_effective_metadata_populated(db_path: Path) -> bool:
    with sqlite3.connect(str(db_path)) as conn:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
        if not {"score_effective_date_status", "score_effective_date_policy"}.issubset(columns):
            return False
        row = conn.execute(
            """
            SELECT COUNT(*), SUM(CASE WHEN score_effective_date_status IS NOT NULL THEN 1 ELSE 0 END)
            FROM rc_fundamental_ttm
            WHERE fundamental_score IS NOT NULL
               OR fundamental_score_lifecycle IS NOT NULL
               OR score_rule IS NOT NULL
               OR score_rule_lifecycle IS NOT NULL
            """
        ).fetchone()
    return row is not None and int(row[0] or 0) > 0 and int(row[0] or 0) == int(row[1] or 0)


def _write_artifacts(
    args: argparse.Namespace,
    output_root: Path,
    payload: dict[str, Any],
    rows: list[Any],
) -> dict[str, str]:
    checkpoint = validate_temp_path(Path(args.checkpoint_json)) if args.checkpoint_json else output_root / "checkpoint.json"
    summary_json = validate_temp_path(Path(args.summary_json)) if args.summary_json else output_root / "summary.json"
    output_csv = validate_temp_path(Path(args.output_csv)) if args.output_csv else output_root / "score_effective_dates.csv"
    write_json_atomic(checkpoint, payload)
    write_json_atomic(
        summary_json,
        {
            key: payload[key]
            for key in (
                "mode",
                "database_path",
                "pre_counts",
                "post_counts",
                "source_table_counts_unchanged",
                "quick_check",
                "summary",
                "apply_counts",
                "backup",
                "verification",
                "score_values_unchanged",
                "score_effective_hash_before",
                "score_effective_hash_after",
            )
        },
    )
    write_csv_atomic(output_csv, rows_to_dicts(rows))
    return {"checkpoint_json": str(checkpoint), "summary_json": str(summary_json), "output_csv": str(output_csv)}


def _print_summary(payload: dict[str, Any], artifact_paths: dict[str, str]) -> None:
    print(f"mode: {payload['mode']}")
    print(f"database_path: {payload['database_path']}")
    print(f"quick_check: {payload['quick_check']}")
    print(f"source_table_counts_unchanged: {payload['source_table_counts_unchanged']}")
    print(f"score_values_unchanged: {payload['score_values_unchanged']}")
    for key, value in payload["summary"].items():
        if key != "status_counts":
            print(f"{key}: {value}")
    print("status_counts:")
    for key, value in payload["summary"]["status_counts"].items():
        print(f"  {key}: {value}")
    print("apply_counts:")
    for key, value in payload["apply_counts"].items():
        print(f"  {key}: {value}")
    if payload["backup"] is not None:
        print(f"backup_path: {payload['backup']['backup_path']}")
        print(f"backup_quick_check: {payload['backup']['quick_check']}")
        print(f"backup_size_bytes: {payload['backup']['backup_size_bytes']}")
    print("artifact_paths:")
    for key, value in artifact_paths.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    raise SystemExit(main())
