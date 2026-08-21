from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_schema import run_v3_schema_migration


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create or inspect a caller-supplied local Fundamentals V3 SQLite DB.")
    parser.add_argument("--db-path", required=True, help="Target local V3 SQLite database path.")
    parser.add_argument("--include-raw-cache", action="store_true", help="Also create raw-cache table for external raw DB smoke tests.")
    args = parser.parse_args(argv)

    migration_file, table_count = run_v3_schema_migration(Path(args.db_path), include_raw_cache=bool(args.include_raw_cache))
    print(f"migration_file={migration_file}")
    print(f"validated_table_count={table_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
