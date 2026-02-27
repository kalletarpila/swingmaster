from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path


DEFAULT_SUMMARY_CSV = "/tmp/pass_stop35_batch/summary.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize PASS stop/35d batch summary CSV")
    parser.add_argument("--summary-csv", default=DEFAULT_SUMMARY_CSV)
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--min-ok-only", action="store_true")
    return parser.parse_args()


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    v = value.strip()
    if v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def _fmt(value: float | str | None) -> str:
    if value is None:
        return "NA"
    return str(value)


def _sort_key_top(row: dict[str, str | float | None]) -> tuple[float, float, float, str]:
    sharpe = row.get("NET_METRICS.sharpe")
    cagr = row.get("NET_METRICS.cagr")
    mdd = row.get("NET_METRICS.max_drawdown")
    scenario_id = str(row.get("scenario_id") or "")
    sharpe_v = -math.inf if sharpe is None else float(sharpe)
    cagr_v = -math.inf if cagr is None else float(cagr)
    abs_mdd = -math.inf if mdd is None else -abs(float(mdd))
    return (sharpe_v, cagr_v, abs_mdd, scenario_id)


def _get_total_cost(row: dict[str, str | float | None]) -> float | None:
    v1 = row.get("NET_METRICS.total_cost")
    if v1 is not None:
        return float(v1)
    v2 = row.get("COST_IMPACT.total_cost")
    if v2 is not None:
        return float(v2)
    return None


def main() -> int:
    args = parse_args()
    summary_path = Path(args.summary_csv)
    try:
        if not summary_path.exists():
            raise FileNotFoundError(f"file not found: {summary_path}")
        with summary_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            raw_rows = list(reader)
    except Exception as exc:
        print(f"REPORT status=ERROR message={exc}")
        return 1

    rows_total = len(raw_rows)
    parsed_rows: list[dict[str, str | float | None]] = []
    for r in raw_rows:
        parsed: dict[str, str | float | None] = dict(r)
        for key, value in r.items():
            if key in {"scenario_id", "window_id", "market", "benchmark", "run.ok", "run.stderr_nonempty"}:
                parsed[key] = value
            else:
                parsed[key] = _to_float(value)
        parsed_rows.append(parsed)

    if args.min_ok_only:
        rows_used = [r for r in parsed_rows if str(r.get("run.ok") or "") == "1"]
    else:
        rows_used = list(parsed_rows)

    m = len(rows_used)
    m_ok = sum(1 for r in rows_used if str(r.get("run.ok") or "") == "1")
    ok_rate = (m_ok / m) if m > 0 else 0

    print(f"REPORT summary_csv={summary_path}")
    print(f"REPORT rows_total={rows_total}")
    print(f"REPORT rows_used={m}")
    print(f"REPORT ok_rate={ok_rate}")

    print("SECTION TOP_SCENARIOS_NET")
    top_candidates = [r for r in rows_used if r.get("NET_METRICS.sharpe") is not None]
    top_sorted = sorted(top_candidates, key=_sort_key_top, reverse=True)
    n_top = max(int(args.top), 0)
    for i, r in enumerate(top_sorted[:n_top], start=1):
        print(
            "TOP_NET "
            f"rank={i} "
            f"scenario_id={_fmt(r.get('scenario_id'))} "
            f"window={_fmt(r.get('window_id'))} "
            f"cost_bps={_fmt(r.get('cost_bps'))} "
            f"sharpe={_fmt(r.get('NET_METRICS.sharpe'))} "
            f"cagr={_fmt(r.get('NET_METRICS.cagr'))} "
            f"mdd={_fmt(r.get('NET_METRICS.max_drawdown'))} "
            f"rel_net_ir={_fmt(r.get('RELATIVE_NET.information_ratio'))} "
            f"rel_net_excess_cagr={_fmt(r.get('RELATIVE_NET.excess_cagr'))}"
        )

    print("SECTION WINDOW_SUMMARY_NET_COST0")
    windows = sorted({str(r.get("window_id") or "") for r in rows_used})
    for window in windows:
        window_rows = [r for r in rows_used if str(r.get("window_id") or "") == window]
        cost0_rows = [r for r in window_rows if r.get("cost_bps") is not None and float(r["cost_bps"]) == 0.0]
        if not cost0_rows:
            continue
        chosen = sorted(cost0_rows, key=lambda x: str(x.get("scenario_id") or ""))[0]
        print(
            "WINDOW "
            f"window={window} "
            f"sharpe={_fmt(chosen.get('NET_METRICS.sharpe'))} "
            f"cagr={_fmt(chosen.get('NET_METRICS.cagr'))} "
            f"mdd={_fmt(chosen.get('NET_METRICS.max_drawdown'))} "
            f"exposure={_fmt(chosen.get('PORTFOLIO.exposure'))} "
            f"rel_net_ir={_fmt(chosen.get('RELATIVE_NET.information_ratio'))}"
        )

    print("SECTION COST_SENSITIVITY")
    for window in windows:
        window_rows = [r for r in rows_used if str(r.get("window_id") or "") == window]
        by_cost: dict[float, dict[str, str | float | None]] = {}
        for row in sorted(window_rows, key=lambda x: str(x.get("scenario_id") or "")):
            cost = row.get("cost_bps")
            if cost is None:
                continue
            cost_f = float(cost)
            if cost_f in {0.0, 5.0, 10.0, 20.0} and cost_f not in by_cost:
                by_cost[cost_f] = row
        if 0.0 not in by_cost:
            continue
        base = by_cost[0.0]
        base_sharpe = base.get("NET_METRICS.sharpe")
        base_cagr = base.get("NET_METRICS.cagr")
        print(f"COST_SENS window={window}")
        for cost in [5.0, 10.0, 20.0]:
            if cost not in by_cost:
                continue
            row = by_cost[cost]
            sharpe = row.get("NET_METRICS.sharpe")
            cagr = row.get("NET_METRICS.cagr")
            if base_sharpe is None or base_cagr is None or sharpe is None or cagr is None:
                continue
            sharpe_delta = float(sharpe) - float(base_sharpe)
            cagr_delta = float(cagr) - float(base_cagr)
            total_cost = _get_total_cost(row)
            print(
                "COST_SENS "
                f"cost={int(cost) if cost.is_integer() else cost} "
                f"sharpe_delta={_fmt(sharpe_delta)} "
                f"cagr_delta={_fmt(cagr_delta)} "
                f"total_cost={_fmt(total_cost)}"
            )

    print("SECTION SANITY_CHECKS")
    missing_net_sharpe = sum(1 for r in rows_used if r.get("NET_METRICS.sharpe") is None)
    missing_rel_net_ir = sum(1 for r in rows_used if r.get("RELATIVE_NET.information_ratio") is None)
    any_stderr_nonempty = sum(
        1 for r in rows_used if str(r.get("run.stderr_nonempty") or "") == "1"
    )
    print(f"SANITY missing_net_sharpe={missing_net_sharpe}")
    print(f"SANITY missing_rel_net_ir={missing_rel_net_ir}")
    print(f"SANITY any_stderr_nonempty={any_stderr_nonempty}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
