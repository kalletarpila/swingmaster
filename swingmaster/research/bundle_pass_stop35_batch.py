from __future__ import annotations

import argparse
import csv
import datetime as dt
import shlex
import shutil
import subprocess
import zipfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bundle PASS stop/35d batch artifacts")
    parser.add_argument("--base-dir", default="/tmp/pass_stop35_batch")
    parser.add_argument("--summary-csv", default=None)
    parser.add_argument("--logs-dir", default=None)
    parser.add_argument("--bundle-dir", default=None)
    parser.add_argument("--zip-path", default=None)
    parser.add_argument("--python", default="python3")
    parser.add_argument("--min-ok-only", action="store_true")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _count_summary_rows(path: Path) -> int:
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return sum(1 for _ in reader)
    except Exception:
        return 0


def _count_log_files(logs_dir: Path) -> int:
    return sum(1 for p in logs_dir.rglob("*") if p.is_file())


def _report_command(python_bin: str, summary_csv: Path, top: int, min_ok_only: bool) -> list[str]:
    cmd = [
        python_bin,
        "-m",
        "swingmaster.research.report_pass_stop35_summary",
        "--summary-csv",
        str(summary_csv),
        "--top",
        str(top),
    ]
    if min_ok_only:
        cmd.append("--min-ok-only")
    return cmd


def _zip_bundle(bundle_dir: Path, zip_path: Path) -> None:
    files = [p for p in bundle_dir.rglob("*") if p.is_file()]
    files_sorted = sorted(files, key=lambda p: str(p.relative_to(bundle_dir)))
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in files_sorted:
            rel = p.relative_to(bundle_dir)
            zf.write(p, arcname=str(rel))


def main() -> int:
    args = parse_args()

    base_dir = Path(args.base_dir)
    summary_csv = Path(args.summary_csv) if args.summary_csv else (base_dir / "summary.csv")
    logs_dir = Path(args.logs_dir) if args.logs_dir else (base_dir / "logs")
    bundle_dir = Path(args.bundle_dir) if args.bundle_dir else (base_dir / "bundle")
    zip_path = Path(args.zip_path) if args.zip_path else (base_dir / "pass_stop35_batch_bundle.zip")

    if not summary_csv.exists():
        print("BUNDLE status=ERROR message=SUMMARY_CSV_NOT_FOUND")
        return 1
    if not logs_dir.exists():
        print("BUNDLE status=ERROR message=LOGS_DIR_NOT_FOUND")
        return 1

    report_cmd = _report_command(
        python_bin=args.python,
        summary_csv=summary_csv,
        top=args.top,
        min_ok_only=args.min_ok_only,
    )
    report_cmd_str = " ".join(shlex.quote(x) for x in report_cmd)

    if args.dry_run:
        print("BUNDLE dry_run=1")
        print(f"BUNDLE base_dir={base_dir}")
        print(f"BUNDLE summary_csv={summary_csv}")
        print(f"BUNDLE logs_dir={logs_dir}")
        print(f"BUNDLE bundle_dir={bundle_dir}")
        print(f"BUNDLE zip_path={zip_path}")
        print(f"BUNDLE report_cmd={report_cmd_str}")
        return 0

    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    bundle_summary = bundle_dir / "summary.csv"
    bundle_logs = bundle_dir / "logs"
    bundle_report = bundle_dir / "report.txt"
    bundle_report_stderr = bundle_dir / "report.stderr.txt"
    bundle_manifest = bundle_dir / "manifest.txt"

    shutil.copy2(summary_csv, bundle_summary)
    shutil.copytree(logs_dir, bundle_logs, dirs_exist_ok=True)

    proc = subprocess.run(report_cmd, check=False, capture_output=True, text=True)
    report_stdout = proc.stdout or ""
    report_stderr = proc.stderr or ""
    bundle_report.write_text(report_stdout, encoding="utf-8")
    bundle_report_stderr.write_text(report_stderr, encoding="utf-8")

    created_at_utc = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    logs_files_count = _count_log_files(logs_dir)
    summary_rows_count = _count_summary_rows(summary_csv)

    manifest_lines = [
        f"MANIFEST created_at_utc={created_at_utc}",
        f"MANIFEST base_dir={base_dir}",
        f"MANIFEST summary_csv={summary_csv}",
        f"MANIFEST logs_dir={logs_dir}",
        f"MANIFEST logs_files_count={logs_files_count}",
        f"MANIFEST summary_rows_count={summary_rows_count}",
        f"MANIFEST report_cmd={report_cmd_str}",
        f"MANIFEST bundle_dir={bundle_dir}",
        f"MANIFEST zip_path={zip_path}",
        f"MANIFEST report_exit_code={proc.returncode}",
    ]
    bundle_manifest.write_text("\n".join(manifest_lines) + "\n", encoding="utf-8")

    if zip_path.exists():
        zip_path.unlink()
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    _zip_bundle(bundle_dir, zip_path)

    print("BUNDLE status=OK")
    print(f"BUNDLE bundle_dir={bundle_dir}")
    print(f"BUNDLE zip_path={zip_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
