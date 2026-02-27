from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from math import floor
from pathlib import Path
from statistics import median
from urllib.parse import quote

from swingmaster.ew_score.training.template_schema_v1 import (
    FEATURE_TYPE_PREFIX_RETURN_PCT,
    MATURITY_MODE_DAY_N_READY,
    SPLIT_TYPE_TIME_BY_ENTRY_DATE,
    EwScoreTrainingTemplateV1,
)
from swingmaster.ew_score.training.validate_template import (
    extract_sql_identifiers,
    required_columns_for_template,
)


class RcDbUnavailableError(RuntimeError):
    pass


class RcPipelineEpisodeSchemaMissingError(RuntimeError):
    pass


class PriceDbUnavailableError(RuntimeError):
    pass


class PriceDbSchemaMissingError(RuntimeError):
    pass


class SplitRequiresEntryWindowDateError(RuntimeError):
    pass


@dataclass(frozen=True)
class EwScoreDatasetRow:
    ticker: str
    entry_window_date: str
    x: float
    y: int
    split: str


@dataclass(frozen=True)
class EwScoreTrainingDryRunReport:
    rule_id: str
    trained_on_market: str
    cohort_name: str
    n_total_episodes: int
    n_with_entry_window_date: int
    n_with_label_available: int
    skipped_price_check: int
    split_type: str | None = None
    train_frac: float | None = None
    n_train: int | None = None
    n_test: int | None = None
    base_rate_train: float | None = None
    base_rate_test: float | None = None
    feature_type: str | None = None
    maturity_mode: str | None = None
    maturity_n: int | None = None
    dataset_rows_total: int | None = None
    dataset_rows_train: int | None = None
    dataset_rows_test: int | None = None
    dropped_price_missing_day0: int | None = None
    dropped_price_missing_dayN: int | None = None
    x_mean: float | None = None
    x_median: float | None = None
    x_min: float | None = None
    x_max: float | None = None
    dataset_rows: tuple[EwScoreDatasetRow, ...] | None = None


def _ensure_readable_file(path: str | Path) -> None:
    file_path = Path(path)
    if not file_path.is_file() or not os.access(file_path, os.R_OK):
        raise RcDbUnavailableError(f"Unreadable file: {file_path}")


def _ensure_table_exists(conn: sqlite3.Connection, table_name: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if row is None:
        raise RcPipelineEpisodeSchemaMissingError(
            f"Required table does not exist: {table_name}"
        )


def _count(conn: sqlite3.Connection, where_sql: str) -> int:
    query = f"SELECT COUNT(*) FROM rc_pipeline_episode WHERE {where_sql}"
    row = conn.execute(query).fetchone()
    return int(row[0]) if row is not None else 0


def _avg_binary(values: list[int]) -> float:
    if not values:
        return 0.0
    return float(sum(values)) / float(len(values))


def _compute_time_split_stats(labels: list[int], train_frac: float) -> tuple[int, int, float, float]:
    n_total = len(labels)
    n_train = floor(n_total * train_frac)
    n_test = n_total - n_train
    y_train = labels[:n_train]
    y_test = labels[n_train:]
    return n_train, n_test, _avg_binary(y_train), _avg_binary(y_test)


def _ensure_osakedata_exists(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='osakedata'"
    ).fetchone()
    if row is None:
        raise PriceDbSchemaMissingError("Required table does not exist: osakedata")


def run_training_dry_run(
    template: EwScoreTrainingTemplateV1,
    rc_db_path: str,
    price_db_path: str,
) -> EwScoreTrainingDryRunReport:
    price_path = Path(price_db_path)
    if not price_path.is_file() or not os.access(price_path, os.R_OK):
        raise PriceDbUnavailableError(f"Unreadable price DB: {price_db_path}")

    _ensure_readable_file(rc_db_path)

    try:
        rc_uri = f"file:{quote(str(Path(rc_db_path).resolve()), safe='/')}?mode=ro"
        rc_conn = sqlite3.connect(rc_uri, uri=True)
    except sqlite3.Error as exc:
        raise RcDbUnavailableError(str(exc)) from exc

    try:
        price_uri = f"file:{quote(str(price_path.resolve()), safe='/')}?mode=ro"
        price_conn = sqlite3.connect(price_uri, uri=True)
    except sqlite3.Error as exc:
        rc_conn.close()
        raise PriceDbUnavailableError(str(exc)) from exc

    try:
        _ensure_table_exists(rc_conn, "rc_pipeline_episode")
        _ensure_osakedata_exists(price_conn)
        table_columns = {
            row[1]
            for row in rc_conn.execute("PRAGMA table_info('rc_pipeline_episode')").fetchall()
        }
        required_columns = required_columns_for_template(template)
        missing_columns = sorted(required_columns - table_columns)
        if missing_columns:
            raise RcPipelineEpisodeSchemaMissingError(
                ",".join(missing_columns)
            )

        where_base = template.cohort.where_sql
        n_total = _count(rc_conn, f"({where_base})")
        n_with_entry = _count(rc_conn, f"({where_base}) AND entry_window_date IS NOT NULL")

        label_identifiers = sorted(extract_sql_identifiers(template.label.sql_expr))
        if label_identifiers:
            label_presence_sql = " AND ".join(f"{col} IS NOT NULL" for col in label_identifiers)
        else:
            label_presence_sql = "1=1"
        n_with_label_available = _count(rc_conn, f"({where_base}) AND ({label_presence_sql})")

        split_type: str | None = None
        train_frac: float | None = None
        n_train: int | None = None
        n_test: int | None = None
        base_rate_train: float | None = None
        base_rate_test: float | None = None
        feature_type: str | None = None
        maturity_mode: str | None = None
        maturity_n: int | None = None
        dataset_rows_total: int | None = None
        dataset_rows_train: int | None = None
        dataset_rows_test: int | None = None
        dropped_price_missing_day0: int | None = None
        dropped_price_missing_dayN: int | None = None
        x_mean: float | None = None
        x_median: float | None = None
        x_min: float | None = None
        x_max: float | None = None
        dataset_rows: tuple[EwScoreDatasetRow, ...] | None = None
        skipped_price_check = 1

        if template.split.type == SPLIT_TYPE_TIME_BY_ENTRY_DATE:
            rows = rc_conn.execute(
                f"""
                SELECT ticker, entry_window_date, ({template.label.sql_expr}) AS y
                FROM rc_pipeline_episode
                WHERE ({where_base})
                ORDER BY entry_window_date ASC, ticker ASC
                """
            ).fetchall()

            episodes: list[tuple[str, str, int]] = []
            labels: list[int] = []
            for ticker, entry_window_date, y_raw in rows:
                if entry_window_date is None:
                    raise SplitRequiresEntryWindowDateError("entry_window_date is NULL")
                y = int(y_raw)
                if y not in (0, 1):
                    y = 1 if y else 0
                episodes.append((str(ticker), str(entry_window_date), y))
                labels.append(y)

            split_type = template.split.type
            train_frac = template.split.train_frac
            n_train, n_test, base_rate_train, base_rate_test = _compute_time_split_stats(
                labels=labels,
                train_frac=train_frac,
            )

            if (
                template.feature.type == FEATURE_TYPE_PREFIX_RETURN_PCT
                and template.feature.maturity.mode == MATURITY_MODE_DAY_N_READY
            ):
                feature_type = template.feature.type
                maturity_mode = template.feature.maturity.mode
                maturity_n = template.feature.maturity.n
                dropped_day0 = 0
                dropped_dayN = 0
                kept_train = 0
                kept_test = 0
                x_values: list[float] = []
                dataset_rows_list: list[EwScoreDatasetRow] = []

                for idx, (ticker, entry_window_date, y) in enumerate(episodes):
                    price_rows = price_conn.execute(
                        """
                        SELECT close
                        FROM osakedata
                        WHERE osake=? AND pvm>=?
                        ORDER BY pvm ASC
                        LIMIT 4
                        """,
                        (ticker, entry_window_date),
                    ).fetchall()
                    if not price_rows:
                        dropped_day0 += 1
                        continue
                    if len(price_rows) < 4:
                        dropped_dayN += 1
                        continue
                    close_day0 = float(price_rows[0][0])
                    close_dayN = float(price_rows[3][0])
                    x = 100.0 * (close_dayN / close_day0 - 1.0)
                    x_values.append(x)
                    split = "train" if n_train is not None and idx < n_train else "test"
                    dataset_rows_list.append(
                        EwScoreDatasetRow(
                            ticker=ticker,
                            entry_window_date=entry_window_date,
                            x=x,
                            y=y,
                            split=split,
                        )
                    )
                    if n_train is not None and idx < n_train:
                        kept_train += 1
                    else:
                        kept_test += 1

                dataset_rows_train = kept_train
                dataset_rows_test = kept_test
                dataset_rows_total = kept_train + kept_test
                dropped_price_missing_day0 = dropped_day0
                dropped_price_missing_dayN = dropped_dayN
                if x_values:
                    x_mean = float(sum(x_values) / len(x_values))
                    x_median = float(median(x_values))
                    x_min = float(min(x_values))
                    x_max = float(max(x_values))
                else:
                    x_mean = 0.0
                    x_median = 0.0
                    x_min = 0.0
                    x_max = 0.0
                dataset_rows = tuple(dataset_rows_list)
                skipped_price_check = 0

        return EwScoreTrainingDryRunReport(
            rule_id=template.rule_id,
            trained_on_market=template.trained_on_market,
            cohort_name=template.cohort.name,
            n_total_episodes=n_total,
            n_with_entry_window_date=n_with_entry,
            n_with_label_available=n_with_label_available,
            skipped_price_check=skipped_price_check,
            split_type=split_type,
            train_frac=train_frac,
            n_train=n_train,
            n_test=n_test,
            base_rate_train=base_rate_train,
            base_rate_test=base_rate_test,
            feature_type=feature_type,
            maturity_mode=maturity_mode,
            maturity_n=maturity_n,
            dataset_rows_total=dataset_rows_total,
            dataset_rows_train=dataset_rows_train,
            dataset_rows_test=dataset_rows_test,
            dropped_price_missing_day0=dropped_price_missing_day0,
            dropped_price_missing_dayN=dropped_price_missing_dayN,
            x_mean=x_mean,
            x_median=x_median,
            x_min=x_min,
            x_max=x_max,
            dataset_rows=dataset_rows,
        )
    finally:
        price_conn.close()
        rc_conn.close()
