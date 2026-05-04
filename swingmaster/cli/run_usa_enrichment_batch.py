from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Sequence


DEFAULT_MARKET = "usa"
DEFAULT_EXCHANGE = "USA"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic USA Yahoo enrichment batch")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--run-id", required=True, help="Deterministic base run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Print commands only without executing subprocesses")
    parser.add_argument("--limit-tickers", type=int, default=None, help="Optional ticker limit for supported steps")
    parser.add_argument("--tickers", default=None, help="Optional comma-separated ticker list")
    parser.add_argument("--skip-raw", action="store_true", help="Skip Yahoo raw load step")
    parser.add_argument("--skip-quarterly", action="store_true", help="Skip Yahoo quarterly normalization step")
    parser.add_argument("--skip-enrichment", action="store_true", help="Skip Yahoo fallback enrichment step")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def derive_child_run_ids(base_run_id: str) -> dict[str, str]:
    return {
        "raw": f"{base_run_id}__RAW",
        "quarterly": f"{base_run_id}__QTR",
        "enrichment": f"{base_run_id}__ENRICH",
    }


def normalize_tickers(tickers_arg: str | None) -> list[str]:
    if tickers_arg is None or not tickers_arg.strip():
        return []
    return sorted({ticker.strip().upper() for ticker in tickers_arg.split(",") if ticker.strip()})


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_raw_symbols(db_path: Path, market: str, limit: int | None) -> list[str]:
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM rc_fundamental_yahoo_raw
            WHERE market = ?
            ORDER BY symbol
            """,
            (market,),
        ).fetchall()
    symbols = [str(row[0]).upper() for row in rows]
    if limit is not None:
        symbols = symbols[:limit]
    return symbols


def format_command(cmd: Sequence[str]) -> str:
    return " ".join(cmd)


def run_command(cmd: Sequence[str], dry_run: bool) -> None:
    print(f"COMMAND {format_command(cmd)}")
    if dry_run:
        return
    completed = subprocess.run(list(cmd), cwd=str(repo_root()))
    if completed.returncode != 0:
        raise subprocess.CalledProcessError(completed.returncode, list(cmd))


def build_raw_command(
    db_path: Path,
    run_id: str,
    tickers: list[str],
    limit_tickers: int | None,
    dry_run: bool,
) -> list[str]:
    cmd = [
        sys.executable,
        "swingmaster/cli/run_fundamental_yahoo_audit.py",
        "--db",
        str(db_path),
        "--market",
        DEFAULT_MARKET,
        "--exchange",
        DEFAULT_EXCHANGE,
        "--run-id",
        run_id,
    ]
    if tickers:
        cmd.extend(["--symbols", ",".join(tickers)])
    elif limit_tickers is not None:
        cmd.extend(["--limit", str(limit_tickers)])
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def build_quarterly_command(db_path: Path, run_id: str, symbol: str, dry_run: bool) -> list[str]:
    cmd = [
        sys.executable,
        "swingmaster/cli/run_fundamental_yahoo_quarterly_write.py",
        "--db",
        str(db_path),
        "--market",
        DEFAULT_MARKET,
        "--symbol",
        symbol,
        "--run-id",
        run_id,
        "--replace-symbol",
    ]
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def build_enrichment_command(db_path: Path, run_id: str, dry_run: bool) -> list[str]:
    cmd = [
        sys.executable,
        "swingmaster/cli/run_fundamental_yahoo_fallback_enrich.py",
        "--db",
        str(db_path),
        "--market",
        DEFAULT_MARKET,
        "--run-id",
        run_id,
        "--replace-audit-for-run",
    ]
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def run_usa_enrichment_batch(
    db_path: Path,
    run_id: str,
    dry_run: bool,
    limit_tickers: int | None,
    tickers_arg: str | None,
    skip_raw: bool,
    skip_quarterly: bool,
    skip_enrichment: bool,
) -> dict[str, object]:
    tickers = normalize_tickers(tickers_arg)
    child_run_ids = derive_child_run_ids(run_id)
    raw_step_executed = 0
    quarterly_step_executed = 0
    enrichment_step_executed = 0

    if not skip_raw:
        run_command(
            build_raw_command(
                db_path=db_path,
                run_id=child_run_ids["raw"],
                tickers=tickers,
                limit_tickers=limit_tickers,
                dry_run=dry_run,
            ),
            dry_run=dry_run,
        )
        raw_step_executed = 0 if dry_run else 1
        print("STEP raw=OK")
    else:
        print("STEP raw=SKIPPED")

    if not skip_quarterly:
        if tickers:
            quarterly_symbols = tickers
        else:
            quarterly_symbols = load_raw_symbols(db_path, DEFAULT_MARKET, limit_tickers)
            if limit_tickers is not None and not quarterly_symbols:
                print("WARNING quarterly_limit_tickers_ignored_no_raw_symbols=1")
        for symbol in quarterly_symbols:
            run_command(
                build_quarterly_command(
                    db_path=db_path,
                    run_id=child_run_ids["quarterly"],
                    symbol=symbol,
                    dry_run=dry_run,
                ),
                dry_run=dry_run,
            )
        quarterly_step_executed = 0 if dry_run else 1
        print("STEP quarterly=OK")
    else:
        print("STEP quarterly=SKIPPED")

    if not skip_enrichment:
        if tickers:
            print("WARNING enrichment_ticker_scope_not_supported_running_full_universe=1")
        if limit_tickers is not None:
            print("WARNING enrichment_limit_tickers_not_supported_ignored=1")
        run_command(
            build_enrichment_command(
                db_path=db_path,
                run_id=child_run_ids["enrichment"],
                dry_run=dry_run,
            ),
            dry_run=dry_run,
        )
        enrichment_step_executed = 0 if dry_run else 1
        print("STEP enrichment=OK")
    else:
        print("STEP enrichment=SKIPPED")

    summary = {
        "market": DEFAULT_MARKET,
        "raw_step_executed": raw_step_executed,
        "quarterly_step_executed": quarterly_step_executed,
        "enrichment_step_executed": enrichment_step_executed,
        "dry_run": 1 if dry_run else 0,
        "run_id": run_id,
    }
    _summary(**summary)
    return summary


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    try:
        run_usa_enrichment_batch(
            db_path=db_path,
            run_id=args.run_id,
            dry_run=args.dry_run,
            limit_tickers=args.limit_tickers,
            tickers_arg=args.tickers,
            skip_raw=args.skip_raw,
            skip_quarterly=args.skip_quarterly,
            skip_enrichment=args.skip_enrichment,
        )
    except subprocess.CalledProcessError as exc:
        failed_step = "unknown"
        command_text = " ".join(exc.cmd) if isinstance(exc.cmd, list) else str(exc.cmd)
        if "run_fundamental_yahoo_audit.py" in command_text:
            failed_step = "raw"
        elif "run_fundamental_yahoo_quarterly_write.py" in command_text:
            failed_step = "quarterly"
        elif "run_fundamental_yahoo_fallback_enrich.py" in command_text:
            failed_step = "enrichment"
        print(f"STEP {failed_step}=FAILED")
        print(f"ERROR step={failed_step}")
        raise SystemExit(exc.returncode)


if __name__ == "__main__":
    main()
