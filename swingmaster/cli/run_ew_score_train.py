from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from swingmaster.ew_score.training.dry_run import (
    EwScoreDatasetRow,
    PriceDbSchemaMissingError,
    PriceDbUnavailableError,
    RcDbUnavailableError,
    RcPipelineEpisodeSchemaMissingError,
    SplitRequiresEntryWindowDateError,
    run_training_dry_run,
)
from swingmaster.ew_score.training.compare_prev_version import (
    PrevRuleInvalidError,
    compute_compare_metrics,
    load_prev_rule,
)
from swingmaster.ew_score.training.fit_logistic_1d import fit_logistic_1d
from swingmaster.ew_score.training.fit_logistic_1d import selection_rate_at_threshold
from swingmaster.ew_score.training.validate_template import (
    TemplateUnavailableError,
    TemplateValidationError,
    load_and_validate_template,
)
from swingmaster.ew_score.training.versioning import resolve_versioned_rule_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EW score generic training CLI (v1 dry-run)")
    parser.add_argument("--rule-template", required=True, help="Path to EW score rule template JSON")
    parser.add_argument("--rc-db", required=True, help="Path to RC SQLite DB")
    parser.add_argument("--price-db", required=True, help="Path to price SQLite DB")
    parser.add_argument("--dry-run", action="store_true", help="Required in v1")
    parser.add_argument("--fit", action="store_true", help="Run fit and write frozen outputs")
    parser.add_argument("--out-dir", default=None, help="Output directory for --fit")
    parser.add_argument("--export-dataset", default=None, help="Optional CSV path for dataset export")
    return parser.parse_args()


def _print_error_and_exit(message: str) -> None:
    print(f"SUMMARY status=ERROR message={message}")
    raise SystemExit(2)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _mean_binary(values: np.ndarray) -> float:
    if values.size == 0:
        return 0.0
    return float(np.mean(values))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_dataset_csv(path: Path, rows: tuple[EwScoreDatasetRow, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker", "entry_window_date", "x", "y", "split"])
        for row in rows:
            writer.writerow([row.ticker, row.entry_window_date, row.x, row.y, row.split])


def main() -> None:
    args = parse_args()
    if args.dry_run and args.fit:
        _print_error_and_exit("MODE_CONFLICT")
    if not args.dry_run and not args.fit:
        _print_error_and_exit("DRY_RUN_ONLY_V1")
    if args.fit and not args.out_dir:
        _print_error_and_exit("OUT_DIR_REQUIRED_FOR_FIT")

    try:
        template = load_and_validate_template(args.rule_template)
    except TemplateUnavailableError:
        _print_error_and_exit("TEMPLATE_UNAVAILABLE")
    except TemplateValidationError:
        _print_error_and_exit("TEMPLATE_INVALID")

    try:
        report = run_training_dry_run(
            template=template,
            rc_db_path=args.rc_db,
            price_db_path=args.price_db,
        )
    except RcDbUnavailableError:
        _print_error_and_exit("RC_DB_UNAVAILABLE")
    except RcPipelineEpisodeSchemaMissingError:
        _print_error_and_exit("RC_PIPELINE_EPISODE_SCHEMA_MISSING")
    except PriceDbUnavailableError:
        _print_error_and_exit("PRICE_DB_UNAVAILABLE")
    except PriceDbSchemaMissingError:
        _print_error_and_exit("PRICE_DB_SCHEMA_MISSING")
    except SplitRequiresEntryWindowDateError:
        _print_error_and_exit("SPLIT_REQUIRES_ENTRY_WINDOW_DATE")

    if not args.fit:
        print("SUMMARY status=OK")
        print(f"SUMMARY rule_id={report.rule_id}")
        print(f"SUMMARY trained_on_market={report.trained_on_market}")
        print(f"SUMMARY cohort_name={report.cohort_name}")
        print(f"SUMMARY n_total_episodes={report.n_total_episodes}")
        print(f"SUMMARY n_with_entry_window_date={report.n_with_entry_window_date}")
        print(f"SUMMARY n_with_label_available={report.n_with_label_available}")
        print(f"SUMMARY skipped_price_check={report.skipped_price_check}")
        if report.split_type is not None:
            print(f"SUMMARY split_type={report.split_type}")
            print(f"SUMMARY train_frac={report.train_frac}")
            print(f"SUMMARY n_train={report.n_train}")
            print(f"SUMMARY n_test={report.n_test}")
            print(f"SUMMARY base_rate_train={report.base_rate_train}")
            print(f"SUMMARY base_rate_test={report.base_rate_test}")
        if report.feature_type is not None:
            print(f"SUMMARY feature_type={report.feature_type}")
            print(f"SUMMARY maturity_mode={report.maturity_mode}")
            print(f"SUMMARY maturity_n={report.maturity_n}")
            print(f"SUMMARY dataset_rows_total={report.dataset_rows_total}")
            print(f"SUMMARY dataset_rows_train={report.dataset_rows_train}")
            print(f"SUMMARY dataset_rows_test={report.dataset_rows_test}")
            print(f"SUMMARY dropped_price_missing_day0={report.dropped_price_missing_day0}")
            print(f"SUMMARY dropped_price_missing_dayN={report.dropped_price_missing_dayN}")
            print(f"SUMMARY x_mean={report.x_mean}")
            print(f"SUMMARY x_median={report.x_median}")
            print(f"SUMMARY x_min={report.x_min}")
            print(f"SUMMARY x_max={report.x_max}")
        return

    if not template.trained_on_market.strip():
        _print_error_and_exit("MARKET_REQUIRED")

    models_dir = Path(__file__).resolve().parents[1] / "ew_score" / "models"
    try:
        models_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        _print_error_and_exit("MODELS_DIR_NOT_WRITABLE")
    if not models_dir.is_dir():
        _print_error_and_exit("MODELS_DIR_NOT_WRITABLE")
    if not args.out_dir:
        _print_error_and_exit("OUT_DIR_REQUIRED_FOR_FIT")
    test_path = models_dir / ".write_test.tmp"
    try:
        test_path.write_text("", encoding="utf-8")
        test_path.unlink()
    except OSError:
        _print_error_and_exit("MODELS_DIR_NOT_WRITABLE")

    resolved_rule_id, _ = resolve_versioned_rule_id(
        models_dir=models_dir,
        market=template.trained_on_market,
        base_id="EW_SCORE_ROLLING",
    )
    frozen_rule_path = models_dir / f"{resolved_rule_id}.json"
    manifest_path = models_dir / f"{resolved_rule_id}.manifest.json"

    try:
        dataset_rows = report.dataset_rows or tuple()
        if not dataset_rows:
            raise ValueError("Dataset is empty")

        x_values = [row.x for row in dataset_rows]
        y_values = [row.y for row in dataset_rows]
        split_flags = [row.split for row in dataset_rows]
        x_all = np.array([row.x for row in dataset_rows], dtype=float)
        y_all = np.array([row.y for row in dataset_rows], dtype=int)
        x_train = np.array([row.x for row in dataset_rows if row.split == "train"], dtype=float)
        y_train = np.array([row.y for row in dataset_rows if row.split == "train"], dtype=int)
        x_test = np.array([row.x for row in dataset_rows if row.split == "test"], dtype=float)
        y_test = np.array([row.y for row in dataset_rows if row.split == "test"], dtype=int)

        fit_result = fit_logistic_1d(
            x_train=x_train,
            y_train=y_train,
            x_test=x_test,
            y_test=y_test,
            threshold_method=template.threshold.level3.method,
            threshold_percentile=template.threshold.level3.percentile,
            threshold_target_rate=template.threshold.level3.target_rate,
        )
        selection_rate_train_at_threshold = selection_rate_at_threshold(
            fit_result.scores_train,
            fit_result.level3_score_threshold,
        )
        selection_rate_test_at_threshold = selection_rate_at_threshold(
            fit_result.scores_test,
            fit_result.level3_score_threshold,
        )

        n_used_total = int(x_all.size)
        n_train_used = int(x_train.size)
        n_test_used = int(x_test.size)
        base_rate_train = _mean_binary(y_train)
        base_rate_test = _mean_binary(y_test)
        base_rate_total = _mean_binary(y_all)
        trained_at_utc = _utc_now_iso()

        frozen_rule_payload = {
            "rule_id": resolved_rule_id,
            "legacy_rule_id": template.rule_id,
            "description": template.description,
            "beta0": fit_result.beta0,
            "beta1": fit_result.beta1,
            "trained_on_market": template.trained_on_market,
            "cohort": template.cohort.name,
            "n_used": n_used_total,
            "base_rate": base_rate_total,
            "base_rate_train": base_rate_train,
            "base_rate_test": base_rate_test,
            "auc": fit_result.auc_test,
            "auc_train": fit_result.auc_train,
            "auc_test": fit_result.auc_test,
            "label_definition": template.label.definition,
            "progressive_prefix_scoring": True,
            "level3_score_threshold": fit_result.level3_score_threshold,
            "trained_at_utc": trained_at_utc,
            "split_type": template.split.type,
            "train_frac": template.split.train_frac,
            "feature_type": template.feature.type,
            "maturity_mode": template.feature.maturity.mode,
            "maturity_n": template.feature.maturity.n,
            "level3_threshold_method": template.threshold.level3.method,
            "level3_threshold_percentile": template.threshold.level3.percentile,
        }
        if template.threshold.level3.target_rate is not None:
            frozen_rule_payload["level3_target_selection_rate"] = template.threshold.level3.target_rate
        _write_json(frozen_rule_path, frozen_rule_payload)

        manifest_payload = {
            "rc_db_path": str(Path(args.rc_db).resolve()),
            "price_db_path": str(Path(args.price_db).resolve()),
            "rule_template_path": str(Path(args.rule_template).resolve()),
            "frozen_rule_path": str(frozen_rule_path.resolve()),
            "dataset_counts": {
                "n_total_episodes": report.n_total_episodes,
                "n_with_entry_window_date": report.n_with_entry_window_date,
                "n_with_label_available": report.n_with_label_available,
                "dataset_rows_total": report.dataset_rows_total,
                "dataset_rows_train": report.dataset_rows_train,
                "dataset_rows_test": report.dataset_rows_test,
                "n_used_total": n_used_total,
                "n_train_used": n_train_used,
                "n_test_used": n_test_used,
            },
            "dropped_counts": {
                "dropped_price_missing_day0": report.dropped_price_missing_day0,
                "dropped_price_missing_dayN": report.dropped_price_missing_dayN,
            },
            "fit": {
                "backend": fit_result.backend,
                "regularization": fit_result.regularization,
                "beta0": fit_result.beta0,
                "beta1": fit_result.beta1,
                "auc_train": fit_result.auc_train,
                "auc_test": fit_result.auc_test,
                "level3_threshold_method": template.threshold.level3.method,
                "level3_threshold_percentile": template.threshold.level3.percentile,
                "level3_score_threshold": fit_result.level3_score_threshold,
            },
            "timestamps": {
                "trained_at_utc": trained_at_utc,
                "manifest_written_at_utc": _utc_now_iso(),
            },
        }
        if template.threshold.level3.target_rate is not None:
            manifest_payload["fit"]["level3_target_selection_rate"] = template.threshold.level3.target_rate
        _write_json(manifest_path, manifest_payload)

        if args.export_dataset:
            _write_dataset_csv(Path(args.export_dataset), dataset_rows)
    except Exception:
        _print_error_and_exit("FIT_FAILED")

    try:
        prev_rule = load_prev_rule(models_dir=models_dir, new_rule_id=resolved_rule_id)
    except PrevRuleInvalidError:
        _print_error_and_exit("PREV_RULE_INVALID")
    except Exception:
        _print_error_and_exit("COMPARE_FAILED")

    try:
        compare_metrics = compute_compare_metrics(
            x_values=x_values,
            y_values=y_values,
            split_flags=split_flags,
            new_rule={
                "beta0": fit_result.beta0,
                "beta1": fit_result.beta1,
                "level3_score_threshold": fit_result.level3_score_threshold,
            },
            prev_rule=prev_rule,
            base_rate_test=base_rate_test,
        )
    except Exception:
        _print_error_and_exit("COMPARE_FAILED")

    print("SUMMARY status=OK")
    print(f"SUMMARY rule_id={template.rule_id}")
    print(f"SUMMARY models_dir={models_dir.resolve()}")
    print(f"SUMMARY resolved_rule_id={resolved_rule_id}")
    print(f"SUMMARY resolved_rule_path={frozen_rule_path.resolve()}")
    print(f"SUMMARY resolved_manifest_path={manifest_path.resolve()}")
    print(f"SUMMARY n_used_total={n_used_total}")
    print(f"SUMMARY n_train_used={n_train_used}")
    print(f"SUMMARY n_test_used={n_test_used}")
    print(f"SUMMARY base_rate_train={base_rate_train}")
    print(f"SUMMARY base_rate_test={base_rate_test}")
    print(f"SUMMARY beta0={fit_result.beta0}")
    print(f"SUMMARY beta1={fit_result.beta1}")
    print(f"SUMMARY auc_train={fit_result.auc_train}")
    print(f"SUMMARY auc_test={fit_result.auc_test}")
    print(f"SUMMARY level3_threshold_method={template.threshold.level3.method}")
    print(f"SUMMARY level3_threshold_percentile={template.threshold.level3.percentile}")
    if template.threshold.level3.target_rate is not None:
        print(f"SUMMARY level3_target_selection_rate={template.threshold.level3.target_rate}")
    print(f"SUMMARY level3_score_threshold={fit_result.level3_score_threshold}")
    print(f"SUMMARY selection_rate_train_at_threshold={selection_rate_train_at_threshold}")
    print(f"SUMMARY selection_rate_test_at_threshold={selection_rate_test_at_threshold}")
    print(f"SUMMARY frozen_rule_path={frozen_rule_path.resolve()}")
    print(f"SUMMARY manifest_path={manifest_path.resolve()}")
    print(f"SUMMARY prev_available={compare_metrics['prev_available']}")
    if int(compare_metrics["prev_available"]) == 1:
        print(f"SUMMARY prev_rule_id={compare_metrics['prev_rule_id']}")
        print(f"SUMMARY prev_rule_path={compare_metrics['prev_rule_path']}")
        print(f"SUMMARY auc_test_prev={compare_metrics['auc_test_prev']}")
        print(f"SUMMARY auc_test_new={compare_metrics['auc_test_new']}")
        print(f"SUMMARY delta_auc_test={compare_metrics['delta_auc_test']}")
        print(f"SUMMARY threshold_prev={compare_metrics['threshold_prev']}")
        print(f"SUMMARY threshold_new={compare_metrics['threshold_new']}")
        print(f"SUMMARY delta_threshold={compare_metrics['delta_threshold']}")
        print(f"SUMMARY selection_rate_test_prev={compare_metrics['selection_rate_test_prev']}")
        print(f"SUMMARY selection_rate_test_new={compare_metrics['selection_rate_test_new']}")
        print(f"SUMMARY delta_selection_rate_test={compare_metrics['delta_selection_rate_test']}")
        print(f"SUMMARY win_rate_selected_test_prev={compare_metrics['win_rate_selected_test_prev']}")
        print(f"SUMMARY win_rate_selected_test_new={compare_metrics['win_rate_selected_test_new']}")
        print(f"SUMMARY delta_win_rate_selected_test={compare_metrics['delta_win_rate_selected_test']}")
        print(f"SUMMARY uplift_selected_test_prev={compare_metrics['uplift_selected_test_prev']}")
        print(f"SUMMARY uplift_selected_test_new={compare_metrics['uplift_selected_test_new']}")
        print(f"SUMMARY delta_uplift_selected_test={compare_metrics['delta_uplift_selected_test']}")


if __name__ == "__main__":
    main()
