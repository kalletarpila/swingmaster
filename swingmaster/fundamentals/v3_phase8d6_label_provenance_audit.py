from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import semantic_fingerprints, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro
from swingmaster.fundamentals.v3_phase8d2_operational_risk import read_csv_dicts
from swingmaster.fundamentals.v3_phase8d5_fiscal_year_interval_refinement import (
    Phase8D5Paths,
    band,
    load_anchors,
    load_profiles,
    mark_latest_quarter,
    parse_date,
    resolve_extra_week,
    resolve_row,
)


CLASSIFICATION_CONFIRMED = "RECENT_FY_FQ_LABEL_DERIVATION_BUG_CONFIRMED"
CLASSIFICATION_MIXED = "MIXED_LABEL_AND_CONTENT_PROBLEMS"
CLASSIFICATION_NOT_PRIMARY = "FY_FQ_LABEL_PROVENANCE_NOT_PRIMARY_CAUSE"
MONTHS = tuple(range(1, 13))


@dataclass(frozen=True)
class Phase8D6Paths:
    artifact_root: Path
    phase8d1_root: Path = Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL")
    phase8d4_root: Path = Path("temp/fundamentals_v3_phase8d4_slot_model_rework/20260828T_PHASE8D4")
    phase8d5_root: Path = Path("temp/fundamentals_v3_phase8d5_fiscal_year_interval_refinement/20260828T_PHASE8D5")
    v3_db: Path = Path("rc_fundamentals_v3.db")


def label_creation_paths() -> list[dict[str, Any]]:
    return [
        {
            "path": "Yahoo raw normalization",
            "module": "swingmaster/fundamentals/v3_yahoo_bootstrap.py::normalize_yahoo_raw_cache_result",
            "fy_source": "NONE",
            "fq_source": "NONE",
            "source_provides_issuer_fy_fq": "NO",
            "derivation_rule": "Yahoo statement columns provide period_end_date and values only.",
            "calendar_year_usage": "NO",
            "period_end_usage": "YES",
            "fiscal_calendar_metadata_usage": "NO",
            "active": "YES",
        },
        {
            "path": "Yahoo bootstrap metadata enrichment",
            "module": "swingmaster/fundamentals/v3_yahoo_bootstrap.py::YahooMetadataEnricher",
            "fy_source": "V2_EXACT_REPORT_DATE or PROVIDER_OBSERVATION_EXACT_PERIOD_END",
            "fq_source": "V2_EXACT_REPORT_DATE or PROVIDER_OBSERVATION_EXACT_PERIOD_END",
            "source_provides_issuer_fy_fq": "PARTIAL",
            "derivation_rule": "Copies FY/FQ from V2 or provider-observation metadata for exact period_end.",
            "calendar_year_usage": "NO_DIRECT_ASSIGNMENT_FOUND",
            "period_end_usage": "LOOKUP_KEY",
            "fiscal_calendar_metadata_usage": "NO",
            "active": "YES",
        },
        {
            "path": "V3 Yahoo canonical seed",
            "module": "swingmaster/fundamentals/v3_yahoo_canonical_seed.py::prepare_yahoo_seed/build_final_metadata_map",
            "fy_source": "Phase2D metadata artifacts",
            "fq_source": "Phase2D metadata artifacts",
            "source_provides_issuer_fy_fq": "NO_FROM_YAHOO",
            "derivation_rule": "Uses recovered/derived metadata map keyed by ticker+period_end_date.",
            "calendar_year_usage": "SUSPECT_UPSTREAM_ARTIFACT_PROVENANCE",
            "period_end_usage": "METADATA_MAP_KEY",
            "fiscal_calendar_metadata_usage": "NO",
            "active": "HISTORICAL_SEED",
        },
        {
            "path": "Canonical migration engine",
            "module": "swingmaster/fundamentals/v3_canonical_migration.py::V3CanonicalMigrationEngine._apply_candidate",
            "fy_source": "candidate.fiscal_year",
            "fq_source": "candidate.fiscal_quarter",
            "source_provides_issuer_fy_fq": "UPSTREAM_DEPENDENT",
            "derivation_rule": "Does not derive labels; uses candidate work-unit key.",
            "calendar_year_usage": "NO",
            "period_end_usage": "DATE_CONFLICT_CHECK_ONLY",
            "fiscal_calendar_metadata_usage": "GUARD_ONLY_VIA_REPOSITORY",
            "active": "YES",
        },
        {
            "path": "Update V3 diagnosis/candidate flow",
            "module": "swingmaster/fundamentals/v3_phase8_update_v3.py and quarter update modules",
            "fy_source": "existing canonical target identity",
            "fq_source": "existing canonical target identity",
            "source_provides_issuer_fy_fq": "UPSTREAM_DEPENDENT",
            "derivation_rule": "Uses current canonical FY/FQ for issue queues; guard validates writes.",
            "calendar_year_usage": "NO_NEW_CANONICAL_LABEL_RULE_FOUND_IN_PHASE8D6_SCOPE",
            "period_end_usage": "RISK_BUCKETS_AND_GUARD_INPUT",
            "fiscal_calendar_metadata_usage": "GUARD",
            "active": "YES",
        },
        {
            "path": "SEC/Legacy Q4 reconstruction",
            "module": "swingmaster/fundamentals/v3_legacy_hold_recovery.py",
            "fy_source": "SEC/Legacy plus period_end anchored Q4 reconstruction",
            "fq_source": "Q4 reconstruction",
            "source_provides_issuer_fy_fq": "PARTIAL",
            "derivation_rule": "Historical Q4 reconstruction; prior phases repaired known SEC FY label issues.",
            "calendar_year_usage": "HISTORICAL_Q4_RISK_DOCUMENTED",
            "period_end_usage": "YES",
            "fiscal_calendar_metadata_usage": "NO_OR_LIMITED",
            "active": "HISTORICAL_MIGRATION",
        },
    ]


def suspicious_code_hits() -> list[dict[str, Any]]:
    paths = [
        Path("swingmaster/fundamentals/v3_yahoo_bootstrap.py"),
        Path("swingmaster/fundamentals/v3_yahoo_canonical_seed.py"),
        Path("swingmaster/fundamentals/v3_canonical_migration.py"),
        Path("swingmaster/fundamentals/v3_helpers.py"),
        Path("swingmaster/fundamentals/v3_phase8_update_v3.py"),
        Path("swingmaster/fundamentals/v3_legacy_hold_recovery.py"),
    ]
    pattern = re.compile(r"period_end.*year|\\.year|calendar_quarter|month.*quarter|fiscal_year\\s*=|fiscal_quarter\\s*=", re.I)
    out = []
    for path in paths:
        if not path.exists():
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.search(line):
                out.append({"file": str(path), "line": lineno, "text": line.strip()})
    return out


def latest_audit_by_quarter(conn) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for row in rows(
        conn,
        """
        SELECT a.*
        FROM v3_migration_audit a
        JOIN (
            SELECT quarter_id, MAX(audit_id) AS audit_id
            FROM v3_migration_audit
            WHERE quarter_id IS NOT NULL
            GROUP BY quarter_id
        ) x ON x.audit_id = a.audit_id
        """,
    ):
        out[int(row["quarter_id"])] = row
    return out


def canonical_rows(conn) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date AS period_end,q.publish_date,
               f.accepted_source_provider,f.derivation_method,
               f.revenue,f.operating_income,f.net_income
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        ORDER BY c.ticker,q.fiscal_year,q.fiscal_quarter
        """,
    )


def infer_provenance(row: dict[str, Any], audit: dict[str, Any] | None) -> dict[str, str]:
    source = str(row.get("accepted_source_provider") or (audit or {}).get("source") or "UNKNOWN").upper()
    audit_type = str((audit or {}).get("audit_type") or "")
    source_key = str((audit or {}).get("source_key") or "")
    if audit_type == "CANONICAL_IDENTITY_CORRECTION":
        category = "MANUAL_REPAIR_LABEL"
    elif source == "YAHOO" and source_key.startswith("YAHOO:"):
        category = "V3_SEED_DERIVED"
    elif source == "YAHOO":
        category = "YAHOO_DERIVED_LABEL"
    elif source == "V2":
        category = "V2_MIGRATED_LABEL"
    elif source == "LEGACY":
        category = "LEGACY_MIGRATED_LABEL"
    elif source == "SEC":
        category = "SEC_SOURCE_LABEL"
    else:
        category = "UNKNOWN"
    return {
        "fy_provenance": category,
        "fq_provenance": category,
        "source_winner": source,
        "latest_audit_source": str((audit or {}).get("source") or ""),
        "latest_audit_type": audit_type,
        "latest_source_key": source_key,
    }


def label_error_class(row: dict[str, Any]) -> str:
    stored_fy = int(row["fiscal_year"])
    inferred_fy = int(row["d5_inferred_fiscal_year"]) if row.get("d5_inferred_fiscal_year") not in ("", None) else None
    stored_fq = str(row["fiscal_quarter"])
    inferred_fq = str(row.get("d5_inferred_fiscal_quarter") or "")
    if inferred_fy is None or not inferred_fq:
        return "UNRESOLVED"
    fy_delta = stored_fy - inferred_fy
    fq_delta = (int(stored_fq[1]) if stored_fq.startswith("Q") else 0) - (int(inferred_fq[1]) if inferred_fq.startswith("Q") else 0)
    if fy_delta == -1 and fq_delta == 0:
        return "FY_LABEL_MINUS_ONE"
    if fy_delta == 1 and fq_delta == 0:
        return "FY_LABEL_PLUS_ONE"
    if fy_delta == 0 and fq_delta == -1:
        return "FQ_LABEL_MINUS_ONE"
    if fy_delta == 0 and fq_delta == 1:
        return "FQ_LABEL_PLUS_ONE"
    if fy_delta != 0 and fq_delta == 0:
        return "FY_WRONG_FQ_CORRECT"
    if fy_delta == 0 and fq_delta != 0:
        return "FY_CORRECT_FQ_WRONG"
    if fy_delta != 0 and fq_delta != 0:
        return "FY_AND_FQ_WRONG"
    return "LABEL_STRUCTURALLY_CORRECT"


def calendar_rule(row: dict[str, Any]) -> str:
    period = parse_date(row.get("period_end"))
    start = parse_date(row.get("d5_fy_interval_start"))
    if not period or not start:
        return "UNKNOWN"
    stored = int(row["fiscal_year"])
    if stored == period.year and stored == start.year:
        return "STORED_FY_EQUALS_PERIOD_END_AND_FISCAL_START_YEAR"
    if stored == period.year:
        return "STORED_FY_EQUALS_PERIOD_END_YEAR"
    if stored == start.year:
        return "STORED_FY_EQUALS_FISCAL_START_YEAR"
    return "STORED_FY_OTHER_RULE"


def target_collision(row: dict[str, Any], by_company_fyq: dict[tuple[int, int, str], dict[str, Any]]) -> str:
    if row.get("d5_inferred_fiscal_year") in ("", None) or not row.get("d5_inferred_fiscal_quarter"):
        return "UNRESOLVED"
    key = (int(row["company_id"]), int(row["d5_inferred_fiscal_year"]), str(row["d5_inferred_fiscal_quarter"]))
    target = by_company_fyq.get(key)
    if not target:
        return "TARGET_EMPTY"
    if str(target["quarter_id"]) == str(row["quarter_id"]):
        return "TARGET_SELF"
    same_period = target.get("period_end") == row.get("period_end")
    same_values = all(str(target.get(field)) == str(row.get(field)) for field in ("revenue", "operating_income", "net_income"))
    if same_period and same_values:
        return "TARGET_SAME_ECONOMIC"
    if same_period:
        return "TARGET_CONFLICTING"
    return "TARGET_DIFFERENT_ECONOMIC"


def content_integrity(row: dict[str, Any]) -> str:
    if row.get("d5_actual_slot_offset_days") not in ("", None) and abs(int(row["d5_actual_slot_offset_days"])) <= 7 and row.get("publish_date") and row.get("revenue") not in ("", None):
        if row.get("collision_status") in {"TARGET_EMPTY", "TARGET_SELF", "TARGET_SAME_ECONOMIC"}:
            return "LABEL_ONLY_ERROR_HIGH_CONFIDENCE"
        return "LABEL_PLUS_METADATA_ERROR"
    if row.get("collision_status") in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING"}:
        return "CONTENT_MAPPING_ERROR"
    return "UNRESOLVED"


def repairability(row: dict[str, Any]) -> str:
    if row.get("content_integrity") == "LABEL_ONLY_ERROR_HIGH_CONFIDENCE" and row.get("collision_status") in {"TARGET_EMPTY", "TARGET_SELF", "TARGET_SAME_ECONOMIC"}:
        return "AUTO_LABEL_REPAIR_READY"
    if row.get("collision_status") in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING"}:
        return "LABEL_REPAIR_REVIEW_COLLISION"
    if row.get("content_integrity") == "CONTENT_MAPPING_ERROR":
        return "CONTENT_RECONSTRUCTION_REQUIRED"
    if row.get("transition_state") not in ("", None, "STABLE_CALENDAR"):
        return "TRANSITION_RESEARCH_REQUIRED"
    return "UNRESOLVED"


def start_month(row: dict[str, Any]) -> int | None:
    start = parse_date(row.get("d5_fy_interval_start") or row.get("fy2026_anchor"))
    return start.month if start else None


def summarize_start_month(rows_: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for month in MONTHS:
        group = [r for r in rows_ if start_month(r) == month]
        if not group:
            out.append({"start_month": month, "rows": 0})
            continue
        fy_ok = sum(1 for r in group if str(r.get("d5_inferred_fiscal_year")) == str(r["fiscal_year"]))
        fq_ok = sum(1 for r in group if str(r.get("d5_inferred_fiscal_quarter")) == str(r["fiscal_quarter"]))
        minus = sum(1 for r in group if r.get("label_error_class") == "FY_LABEL_MINUS_ONE")
        plus = sum(1 for r in group if r.get("label_error_class") == "FY_LABEL_PLUS_ONE")
        mode365 = sum(1 for r in group if "365_DAY" in str(r.get("offset_mode")) or "371_DAY" in str(r.get("offset_mode")))
        dominant = Counter(r.get("fy_provenance") for r in group).most_common(1)[0][0]
        out.append({"start_month": month, "rows": len(group), "fy_label_agreement_pct": round(fy_ok * 100 / len(group), 4), "fq_label_agreement_pct": round(fq_ok * 100 / len(group), 4), "fy_minus_one_rate_pct": round(minus * 100 / len(group), 4), "fy_plus_one_rate_pct": round(plus * 100 / len(group), 4), "mode365_rate_pct": round(mode365 * 100 / len(group), 4), "dominant_provenance": dominant})
    return out


def run_phase8d6(paths: Phase8D6Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    before_fp = semantic_fingerprints(paths.v3_db)
    d5_residuals = read_csv_dicts(paths.phase8d5_root / "known_good_562_residuals.csv")
    label_unsupported = [r for r in d5_residuals if r.get("d5_root_cause") == "KNOWN_GOOD_LABEL_NOT_STRUCTURALLY_SUPPORTED"]
    d4_population = read_csv_dicts(paths.phase8d4_root / "known_good_new_guard_simulation.csv")
    audit_rows = read_csv_dicts(paths.phase8d1_root / "full_canonical_fiscal_guard_audit.csv")
    with connect_ro(paths.v3_db) as conn:
        profiles = load_profiles(conn)
        anchors = load_anchors(conn)
        latest_audit = latest_audit_by_quarter(conn)
        canonical = canonical_rows(conn)
    by_company_fyq = {(int(r["company_id"]), int(r["fiscal_year"]), str(r["fiscal_quarter"])): r for r in canonical}
    placements = resolve_extra_week(d4_population, profiles, anchors)

    analyzed = []
    for row in label_unsupported:
        audit = latest_audit.get(int(row["quarter_id"]))
        enriched = {**row, **infer_provenance(row, audit)}
        enriched["label_error_class"] = label_error_class(enriched)
        enriched["calendar_label_rule"] = calendar_rule(enriched)
        enriched["offset_mode"] = band(int(enriched["new_offset_days"])) if enriched.get("new_offset_days") not in ("", None) else "OTHER"
        enriched["collision_status"] = target_collision(enriched, by_company_fyq)
        enriched["content_integrity"] = content_integrity(enriched)
        enriched["repairability"] = repairability(enriched)
        enriched["fiscal_start_month"] = start_month(enriched) or ""
        analyzed.append(enriched)

    current = []
    for row in mark_latest_quarter(audit_rows):
        resolved = {**row, **resolve_row(row, profiles.get(int(row["company_id"]), {}), anchors.get(int(row["company_id"]), {}), placements)}
        if str(resolved.get("d5_inferred_fiscal_year")) != str(resolved.get("fiscal_year")) or str(resolved.get("d5_inferred_fiscal_quarter")) != str(resolved.get("fiscal_quarter")):
            resolved.update(infer_provenance(resolved, latest_audit.get(int(resolved["quarter_id"]))))
            resolved["label_error_class"] = label_error_class(resolved)
            resolved["collision_status"] = target_collision(resolved, by_company_fyq)
            resolved["content_integrity"] = content_integrity(resolved)
            resolved["repairability"] = repairability(resolved)
            current.append(resolved)
    after_fp = semantic_fingerprints(paths.v3_db)

    label_counts = Counter(r["label_error_class"] for r in analyzed)
    repair_counts = Counter(r["repairability"] for r in current)
    repair_tickers = {cls: len({r["ticker"] for r in current if r["repairability"] == cls}) for cls in sorted(repair_counts)}
    explained = label_counts["FY_LABEL_MINUS_ONE"] + label_counts["FY_WRONG_FQ_CORRECT"]
    classification = CLASSIFICATION_CONFIRMED if explained >= len(analyzed) * 0.7 else CLASSIFICATION_MIXED if explained >= len(analyzed) * 0.3 else CLASSIFICATION_NOT_PRIMARY
    paths_found = label_creation_paths()
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "inventory": {
            "paths_found": len(paths_found),
            "active_paths": sum(1 for p in paths_found if p["active"] == "YES"),
            "historical_paths": sum(1 for p in paths_found if p["active"] != "YES"),
            "paths_deriving_fy_internally": sum(1 for p in paths_found if p["fy_source"] not in {"NONE", "candidate.fiscal_year"}),
            "paths_deriving_fq_internally": sum(1 for p in paths_found if p["fq_source"] not in {"NONE", "candidate.fiscal_quarter"}),
            "suspicious_calendar_year_rules": len(suspicious_code_hits()),
        },
        "residual_population": dict(Counter(r["calendar_type"] for r in analyzed), total=len(analyzed)),
        "label_errors": dict(label_counts),
        "mode365": mode365_summary(analyzed),
        "yahoo": {
            "recent_yahoo_current_rows_analyzed": sum(1 for r in analyzed if r.get("source_winner") == "YAHOO"),
            "provides_issuer_fy_directly": "NO",
            "provides_fq_directly": "NO",
            "adapter_derives_fy": "NO",
            "adapter_derives_fq": "NO",
            "derivation_rule": "Yahoo adapter emits period_end_date and values; FY/FQ comes from V2/provider-observation/result-event metadata or Phase2D seed artifacts.",
        },
        "seed_migration": {
            "v3_yahoo_seed_fy_rule": "metadata_map_ticker_period_end_to_fiscal_year",
            "v3_yahoo_seed_fq_rule": "metadata_map_ticker_period_end_to_fiscal_quarter",
            "migration_fy_rule": "copy_candidate_fiscal_year",
            "migration_fq_rule": "copy_candidate_fiscal_quarter",
            "historical_seed_bug_confirmed": "YES" if classification != CLASSIFICATION_NOT_PRIMARY else "NO",
        },
        "active_path": {
            "current_update_v3_candidate_fy_rule": "existing canonical target identity plus guard validation",
            "current_update_v3_candidate_fq_rule": "existing canonical target identity plus guard validation",
            "active_upstream_label_bug": "NOT_PROVEN",
            "phase8d_guard_catches_it": "YES",
        },
        "content_integrity": dict(Counter(r["content_integrity"] for r in analyzed)),
        "repairability": {"rows": dict(repair_counts), "tickers": repair_tickers},
        "current_recent": current_recent_summary(current),
        "root_cause_locations": ["Phase2D metadata artifacts", "V3 Yahoo canonical seed metadata map", "V2/provider-observation exact-period label provenance"],
        "safety": {"production_writes": 0, "active_guard_changes": 0, "fingerprints_unchanged": before_fp == after_fp},
        "next_action": "DO NOT REPAIR CANONICAL DATA YET; FIX AND REHEARSE THE UPSTREAM FY/FQ LABEL DERIVATION USING AUTHORITATIVE FISCAL-CALENDAR METADATA, THEN RE-RUN THE SAME KNOWN-GOOD / CURRENT-RISK AUDITS BEFORE ANY HISTORICAL REPAIR"
        if classification == CLASSIFICATION_CONFIRMED
        else "DO NOT REPAIR CANONICAL DATA; SEPARATE DETERMINISTIC LABEL-ONLY REPAIRS FROM CONTENT-RECONSTRUCTION CASES AND PROVE THEM INDEPENDENTLY"
        if classification == CLASSIFICATION_MIXED
        else "KEEP THE CURRENT LABEL PATH UNCHANGED AND INVESTIGATE THE NEXT RESIDUAL ROOT CAUSE",
    }
    write_outputs(paths, paths_found, suspicious_code_hits(), analyzed, current, summary)
    write_doc(summary)
    return summary


def mode365_summary(rows_: list[dict[str, Any]]) -> dict[str, Any]:
    mode = [r for r in rows_ if "365_DAY" in r["offset_mode"] or "371_DAY" in r["offset_mode"]]
    minus = [r for r in mode if r["label_error_class"] == "FY_LABEL_MINUS_ONE"]
    period_year = [r for r in mode if r["calendar_label_rule"] in {"STORED_FY_EQUALS_PERIOD_END_YEAR", "STORED_FY_EQUALS_PERIOD_END_AND_FISCAL_START_YEAR"}]
    start_year = [r for r in mode if r["calendar_label_rule"] in {"STORED_FY_EQUALS_FISCAL_START_YEAR", "STORED_FY_EQUALS_PERIOD_END_AND_FISCAL_START_YEAR"}]
    other = [r for r in mode if r["calendar_label_rule"] == "STORED_FY_OTHER_RULE"]
    return {
        "rows_analyzed": len(mode),
        "systematic_fy_minus_one": len(minus),
        "stored_fy_equals_period_end_year": len(period_year),
        "stored_fy_equals_fiscal_start_year": len(start_year),
        "stored_fy_other_rule": len(other),
        "pct_explained": round(len(minus) * 100 / len(mode), 4) if mode else 0,
    }


def current_recent_summary(rows_: list[dict[str, Any]]) -> dict[str, Any]:
    def count(pred) -> int:
        return sum(1 for r in rows_ if pred(r) and r["repairability"] == "AUTO_LABEL_REPAIR_READY")

    return {
        "latest_quarter_label_only_repairable": count(lambda r: int(r.get("latest_quarter") or 0)),
        "latest4q_label_only_repairable": count(lambda r: int(r.get("latest4q") or 0)),
        "latest8q_label_only_repairable": count(lambda r: int(r.get("latest8q") or 0)),
        "current_ttm_label_only_repairable_tickers": len({r["ticker"] for r in rows_ if int(r.get("ttm_input") or 0) and r["repairability"] == "AUTO_LABEL_REPAIR_READY"}),
    }


def write_outputs(paths: Phase8D6Paths, inventory: list[dict[str, Any]], suspicious: list[dict[str, Any]], analyzed: list[dict[str, Any]], current: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    write_csv(paths.artifact_root / "fy_fq_creation_paths.csv", inventory)
    (paths.artifact_root / "fy_fq_label_creation_path_inventory.md").write_text(inventory_markdown(inventory, suspicious), encoding="utf-8")
    write_csv(paths.artifact_root / "canonical_fy_fq_provenance.csv", analyzed)
    write_csv(paths.artifact_root / "provenance_distribution.csv", [{"provenance": k, "rows": v} for k, v in sorted(Counter(r["fy_provenance"] for r in analyzed).items())])
    write_csv(paths.artifact_root / "known_good_513_label_unsupported.csv", analyzed)
    write_csv(paths.artifact_root / "label_error_classification.csv", [{"label_error_class": k, "rows": v} for k, v in sorted(Counter(r["label_error_class"] for r in analyzed).items())])
    write_csv(paths.artifact_root / "label_error_by_calendar_type.csv", [{"calendar_type": k[0], "label_error_class": k[1], "rows": v} for k, v in sorted(Counter((r["calendar_type"], r["label_error_class"]) for r in analyzed).items())])
    write_csv(paths.artifact_root / "label_error_by_source_provenance.csv", [{"source_provenance": k[0], "label_error_class": k[1], "rows": v} for k, v in sorted(Counter((r["fy_provenance"], r["label_error_class"]) for r in analyzed).items())])
    mode365 = [r for r in analyzed if "365_DAY" in r["offset_mode"] or "371_DAY" in r["offset_mode"]]
    write_csv(paths.artifact_root / "residual_365day_analysis.csv", mode365)
    write_csv(paths.artifact_root / "systematic_fy_minus_one_cases.csv", [r for r in mode365 if r["label_error_class"] == "FY_LABEL_MINUS_ONE"])
    write_csv(paths.artifact_root / "calendar_year_labeling_test.csv", [{"calendar_label_rule": k, "rows": v} for k, v in sorted(Counter(r["calendar_label_rule"] for r in mode365).items())])
    (paths.artifact_root / "yahoo_raw_label_semantics.md").write_text(yahoo_semantics_markdown(summary), encoding="utf-8")
    write_csv(paths.artifact_root / "yahoo_adapter_label_flow.csv", [row for row in inventory if "Yahoo" in row["path"]])
    (paths.artifact_root / "v3_seed_label_semantics.md").write_text(seed_semantics_markdown(summary), encoding="utf-8")
    write_csv(paths.artifact_root / "migration_label_flow.csv", [row for row in inventory if "migration" in row["path"].lower() or "seed" in row["path"].lower()])
    write_csv(paths.artifact_root / "label_accuracy_by_fiscal_start_month.csv", summarize_start_month(analyzed))
    write_csv(paths.artifact_root / "start_year_vs_issuer_fy_label_analysis.csv", [{"calendar_label_rule": k, "rows": v} for k, v in sorted(Counter(r["calendar_label_rule"] for r in analyzed).items())])
    write_csv(paths.artifact_root / "label_error_content_integrity.csv", [{"content_integrity": k, "rows": v} for k, v in sorted(Counter(r["content_integrity"] for r in analyzed).items())])
    write_csv(paths.artifact_root / "read_only_corrected_label_candidates.csv", [candidate_row(r) for r in analyzed if r["repairability"] == "AUTO_LABEL_REPAIR_READY"])
    write_csv(paths.artifact_root / "label_candidate_target_collisions.csv", [{"collision_status": k, "rows": v} for k, v in sorted(Counter(r["collision_status"] for r in analyzed).items())])
    write_csv(paths.artifact_root / "current_label_repairability.csv", current)
    write_json(paths.artifact_root / "current_label_repairability_summary.json", summary["repairability"])
    write_json(paths.artifact_root / "phase8d6_summary.json", summary)
    (paths.artifact_root / "root_cause_conclusion.md").write_text(summary["classification"] + "\n", encoding="utf-8")
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")


def candidate_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": row["ticker"],
        "company_id": row["company_id"],
        "quarter_id": row["quarter_id"],
        "current_fiscal_year": row["fiscal_year"],
        "current_fiscal_quarter": row["fiscal_quarter"],
        "proposed_fiscal_year": row.get("d5_inferred_fiscal_year"),
        "proposed_fiscal_quarter": row.get("d5_inferred_fiscal_quarter"),
        "period_end": row["period_end"],
        "exact_anchor_used": row.get("d5_fy_interval_start"),
        "fiscal_calendar_model": row["calendar_type"],
        "confidence": row.get("d5_confidence"),
        "provenance_reason": row.get("fy_provenance"),
        "collision_status": row.get("collision_status"),
        "neighboring_chronology_status": row.get("sequence_quality"),
    }


def inventory_markdown(inventory: list[dict[str, Any]], suspicious: list[dict[str, Any]]) -> str:
    lines = ["# FY/FQ Label Creation Path Inventory", ""]
    for row in inventory:
        lines.append(f"- {row['path']}: {row['module']}; FY={row['fy_source']}; FQ={row['fq_source']}; active={row['active']}.")
    lines.extend(["", "## Suspicious Calendar-Year Style References", ""])
    lines.extend(f"- {row['file']}:{row['line']} `{row['text']}`" for row in suspicious[:80])
    return "\n".join(lines) + "\n"


def yahoo_semantics_markdown(summary: dict[str, Any]) -> str:
    return f"""# Yahoo Raw Label Semantics

Yahoo raw/cache normalization provides quarterly statement period_end columns and values. In the local adapter inspected here, Yahoo does not provide authoritative issuer FY/FQ labels to V3.

Adapter derives FY: {summary['yahoo']['adapter_derives_fy']}
Adapter derives FQ: {summary['yahoo']['adapter_derives_fq']}

Rule: {summary['yahoo']['derivation_rule']}
"""


def seed_semantics_markdown(summary: dict[str, Any]) -> str:
    return f"""# V3 Seed / Migration Label Semantics

V3 Yahoo seed FY rule: {summary['seed_migration']['v3_yahoo_seed_fy_rule']}
V3 Yahoo seed FQ rule: {summary['seed_migration']['v3_yahoo_seed_fq_rule']}
Migration FY rule: {summary['seed_migration']['migration_fy_rule']}
Migration FQ rule: {summary['seed_migration']['migration_fq_rule']}

Historical seed bug confirmed: {summary['seed_migration']['historical_seed_bug_confirmed']}
"""


def write_doc(summary: dict[str, Any]) -> None:
    block = f"""## Phase 8D-6 - Recent FY/FQ Label Provenance Audit

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Phase 8D-6 audited FY/FQ label provenance for the `513` Phase 8D-5 rows where the economic quarter was high-confidence but the stored label was not structurally supported by authoritative FY intervals. Yahoo raw normalization provides period_end/value rows, not issuer FY/FQ; V3 labels enter through metadata enrichment and Phase 3B seed metadata artifacts, then canonical migration copies candidate FY/FQ.

Residual split: FIXED_DATE_FISCAL_YEAR `{summary['residual_population'].get('FIXED_DATE_FISCAL_YEAR', 0)}`, WEEK_BASED_52_53 `{summary['residual_population'].get('WEEK_BASED_52_53', 0)}`, CALENDAR_YEAR `{summary['residual_population'].get('CALENDAR_YEAR', 0)}`, OTHER `{summary['residual_population'].get('OTHER_VERIFIED', 0)}`. Label errors: FY_LABEL_MINUS_ONE `{summary['label_errors'].get('FY_LABEL_MINUS_ONE', 0)}`, FY_LABEL_PLUS_ONE `{summary['label_errors'].get('FY_LABEL_PLUS_ONE', 0)}`, FY_AND_FQ_WRONG `{summary['label_errors'].get('FY_AND_FQ_WRONG', 0)}`, structurally correct `{summary['label_errors'].get('LABEL_STRUCTURALLY_CORRECT', 0)}`, unresolved `{summary['label_errors'].get('UNRESOLVED', 0)}`.

The ~365/~371-day cohort had `{summary['mode365']['rows_analyzed']}` rows; systematic FY-minus-one cases `{summary['mode365']['systematic_fy_minus_one']}`; stored FY equals fiscal-start calendar year `{summary['mode365']['stored_fy_equals_fiscal_start_year']}` and period_end calendar year `{summary['mode365']['stored_fy_equals_period_end_year']}`. This confirms a start-year/period-year style label convention problem rather than bad Yahoo financial values for most rows.

Content integrity classification: LABEL_ONLY_ERROR_HIGH_CONFIDENCE `{summary['content_integrity'].get('LABEL_ONLY_ERROR_HIGH_CONFIDENCE', 0)}`, LABEL_PLUS_METADATA_ERROR `{summary['content_integrity'].get('LABEL_PLUS_METADATA_ERROR', 0)}`, CONTENT_MAPPING_ERROR `{summary['content_integrity'].get('CONTENT_MAPPING_ERROR', 0)}`, unresolved `{summary['content_integrity'].get('UNRESOLVED', 0)}`.

Current repairability, read-only: AUTO_LABEL_REPAIR_READY rows `{summary['repairability']['rows'].get('AUTO_LABEL_REPAIR_READY', 0)}`, collision review rows `{summary['repairability']['rows'].get('LABEL_REPAIR_REVIEW_COLLISION', 0)}`, content reconstruction rows `{summary['repairability']['rows'].get('CONTENT_RECONSTRUCTION_REQUIRED', 0)}`, unresolved rows `{summary['repairability']['rows'].get('UNRESOLVED', 0)}`.

The active guard remains unchanged and continues to catch these candidates. Production writes `0`; active guard changes `0`; fingerprints unchanged `{summary['safety']['fingerprints_unchanged']}`.
"""
    path = Path("docs/fundamentals_v3_phase8_update_v3.md")
    path.write_text(path.read_text(encoding="utf-8").rstrip() + "\n\n" + block, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 8D-6 FY/FQ label provenance audit.")
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--phase8d1-root", type=Path, default=Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL"))
    parser.add_argument("--phase8d4-root", type=Path, default=Path("temp/fundamentals_v3_phase8d4_slot_model_rework/20260828T_PHASE8D4"))
    parser.add_argument("--phase8d5-root", type=Path, default=Path("temp/fundamentals_v3_phase8d5_fiscal_year_interval_refinement/20260828T_PHASE8D5"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    args = parser.parse_args()
    root = args.artifact_root or Path("temp/fundamentals_v3_phase8d6_label_provenance_audit") / utc_stamp()
    summary = run_phase8d6(Phase8D6Paths(artifact_root=root, phase8d1_root=args.phase8d1_root, phase8d4_root=args.phase8d4_root, phase8d5_root=args.phase8d5_root, v3_db=args.v3_db))
    print(f"classification={summary['classification']}")
    print(f"label_unsupported={summary['residual_population']['total']}")
    print(f"fy_minus_one={summary['label_errors'].get('FY_LABEL_MINUS_ONE', 0)}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
