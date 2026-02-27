from __future__ import annotations

import argparse
import csv
import shlex
import subprocess
from pathlib import Path


WINDOWS = [
    ("FULL", None, None),
    ("W2020_2021", "2020-03-01", "2021-12-31"),
    ("W2022_2023", "2022-01-01", "2023-12-31"),
    ("W2024_2026", "2024-01-01", "2026-01-15"),
]

COST_BPS_VALUES = [0, 5, 10, 20]

MANDATORY_COLUMNS = [
    "scenario_id",
    "window_id",
    "cost_bps",
    "market",
    "benchmark",
    "run.exit_code",
    "run.ok",
    "run.stderr_nonempty",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PASS stop/35d batch scenarios")
    parser.add_argument("--python", default="python3")
    parser.add_argument("--base-dir", default="/tmp/pass_stop35_batch")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _build_command(
    python_bin: str,
    market: str,
    benchmark: str,
    cost_bps: int,
    window_from: str | None,
    window_to: str | None,
) -> list[str]:
    cmd = [
        python_bin,
        "-m",
        "swingmaster.research.backtest_pass_stop35",
        "--market",
        market,
    ]
    if window_from is not None and window_to is not None:
        cmd.extend(["--from", window_from, "--to", window_to])
    cmd.extend(
        [
            "--metrics",
            "--trade-metrics",
            "--exposure-turnover",
            "--net-metrics",
            "--cost-bps",
            str(cost_bps),
            "--benchmark",
            benchmark,
            "--rolling-window",
            "252",
        ]
    )
    return cmd


def _parse_stdout_metrics(stdout_text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    allowed_blocks = {
        "REPORT_WINDOW",
        "METRICS",
        "NET_METRICS",
        "BENCHMARK",
        "RELATIVE",
        "RELATIVE_NET",
        "COST_IMPACT",
        "PORTFOLIO",
        "TURNOVER",
    }
    for line in stdout_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        tokens = stripped.split()
        if not tokens:
            continue
        block = tokens[0]
        if block not in allowed_blocks:
            continue
        for token in tokens[1:]:
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            parsed[f"{block}.{key}"] = value
    return parsed


def _scenario_iter():
    for window_id, window_from, window_to in WINDOWS:
        for cost_bps in COST_BPS_VALUES:
            scenario_id = f"{window_id}__cost{cost_bps}"
            yield scenario_id, window_id, window_from, window_to, cost_bps


def main() -> None:
    args = parse_args()

    base_dir = Path(args.base_dir)
    logs_dir = base_dir / "logs"
    summary_path = base_dir / "summary.csv"

    scenarios = list(_scenario_iter())
    print(f"planned_scenarios={len(scenarios)}")

    if args.dry_run:
        for scenario_id, _, window_from, window_to, cost_bps in scenarios:
            cmd = _build_command(
                python_bin=args.python,
                market="usa",
                benchmark="SPY",
                cost_bps=cost_bps,
                window_from=window_from,
                window_to=window_to,
            )
            cmd_line = " ".join(shlex.quote(part) for part in cmd)
            print(f"{scenario_id}: {cmd_line}")
        return

    logs_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, str]] = []
    parsed_key_union: set[str] = set()

    for scenario_id, window_id, window_from, window_to, cost_bps in scenarios:
        cmd = _build_command(
            python_bin=args.python,
            market="usa",
            benchmark="SPY",
            cost_bps=cost_bps,
            window_from=window_from,
            window_to=window_to,
        )
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        stdout_text = proc.stdout or ""
        stderr_text = proc.stderr or ""
        exit_code = int(proc.returncode)
        stderr_nonempty = 1 if stderr_text.strip() else 0

        cmd_line = " ".join(shlex.quote(part) for part in cmd)
        log_path = logs_dir / f"{scenario_id}.txt"
        log_path.write_text(
            "\n".join(
                [
                    f"command={cmd_line}",
                    f"exit_code={exit_code}",
                    "",
                    "stdout:",
                    stdout_text,
                    "",
                    "stderr:",
                    stderr_text,
                ]
            ),
            encoding="utf-8",
        )

        parsed = _parse_stdout_metrics(stdout_text)
        parsed_key_union.update(parsed.keys())

        row: dict[str, str] = {
            "scenario_id": scenario_id,
            "window_id": window_id,
            "cost_bps": str(cost_bps),
            "market": "usa",
            "benchmark": "SPY",
            "run.exit_code": str(exit_code),
            "run.ok": "1" if exit_code == 0 else "0",
            "run.stderr_nonempty": str(stderr_nonempty),
        }
        row.update(parsed)
        rows.append(row)

    dynamic_cols = sorted(col for col in parsed_key_union if col not in MANDATORY_COLUMNS)
    header = MANDATORY_COLUMNS + dynamic_cols

    base_dir.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in header})

    print(f"summary_csv={summary_path}")
    print(f"logs_dir={logs_dir}")
    print(f"scenarios_run={len(rows)}")


if __name__ == "__main__":
    main()
