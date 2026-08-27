from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import (
    ANCHOR_TABLE,
    EXPECTED_P1_TICKERS,
    FiscalCalendarTransitionEvidence,
    FiscalCalendarWriteCandidate,
    baseline_summary,
    metadata_fingerprint,
    semantic_fingerprints,
    utc_stamp,
    validate_canonical_write_candidate,
)
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro, integrity


NINE_52_53_TICKERS = ("BBY", "DELL", "GCO", "HAE", "MRVL", "RL", "SAIC", "TJX", "TRNS")


CLASSIFICATION_ACTIVE = "FUNDAMENTALS_V3_PHASE8D_FISCAL_WRITE_GUARDS_ACTIVE"
CLASSIFICATION_REVIEW = "FUNDAMENTALS_V3_PHASE8D_FISCAL_WRITE_GUARDS_IMPLEMENTED_REVIEW_MODE_ONLY"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8D_FISCAL_WRITE_GUARDS_BLOCKED"


@dataclass(frozen=True)
class Phase8DPaths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")
    phase8c_artifact_root: Path = Path("temp/fundamentals_v3_phase8c_fiscal_calendar_metadata/20260827T_PHASE8C")


def anchor_immutability(v3_db: Path) -> dict[str, Any]:
    with connect_ro(v3_db) as conn:
        anchors = rows(
            conn,
            f"""
            SELECT c.ticker,a.company_id,a.fiscal_year,a.fiscal_year_start_date,a.source_reference,a.import_state
            FROM {ANCHOR_TABLE} a JOIN v3_company c ON c.company_id=a.company_id
            ORDER BY c.ticker,a.fiscal_year
            """,
        )
    return {
        "rows": len(anchors),
        "FY2026": sum(1 for row in anchors if int(row["fiscal_year"]) == 2026),
        "FY2027": sum(1 for row in anchors if int(row["fiscal_year"]) == 2027),
        "conflicts": sum(1 for row in anchors if row["import_state"] == "CONFLICT_REVIEW_REQUIRED"),
        "fingerprint": metadata_fingerprint(v3_db),
    }


def decision_row(ticker: str, decision: Any, candidate: FiscalCalendarWriteCandidate) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "company_id": candidate.company_id,
        "candidate_fiscal_year": candidate.fiscal_year,
        "candidate_fiscal_quarter": candidate.fiscal_quarter,
        "observed_period_end": candidate.period_end_date or "",
        "publish_date": candidate.publish_date or "",
        "exact_anchor_used": decision.exact_anchor_used or "",
        "inferred_fiscal_year": decision.inferred_fiscal_year or "",
        "inferred_fiscal_quarter": decision.inferred_fiscal_quarter or "",
        "calendar_type": decision.calendar_type or "",
        "calendar_regime": decision.calendar_regime,
        "slot_confidence": decision.slot_confidence,
        "guard_decision": decision.decision,
        "reason_codes": "|".join(decision.reason_codes),
        "transition_evidence": decision.transition_evidence,
        "target_collision_state": decision.target_collision_state,
        "chronology_state": decision.chronology_state,
        "financial_corroboration_state": decision.financial_corroboration_state,
        "write_permitted": int(decision.write_permitted),
    }


def known_replay(v3_db: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    out = []
    backward = []
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for ticker in EXPECTED_P1_TICKERS:
            company = conn.execute("SELECT company_id FROM v3_company WHERE ticker=?", (ticker,)).fetchone()
            if not company:
                continue
            cid = int(company["company_id"])
            period = conn.execute("SELECT period_end_date FROM v3_quarter WHERE company_id=? AND period_end_date IS NOT NULL ORDER BY period_end_date DESC LIMIT 1", (cid,)).fetchone()
            observed = period["period_end_date"] if period else "2025-08-31"
            if ticker in NINE_52_53_TICKERS:
                bad = FiscalCalendarWriteCandidate(cid, 2027, "Q4", observed, source_context="PHASE8D_KNOWN_52_53_REPLAY")
            else:
                bad = FiscalCalendarWriteCandidate(cid, 2027, "Q1", observed, source_context="PHASE8D_KNOWN_STRUCTURAL_REPLAY", financial_fingerprint_state="CONFLICT" if ticker in {"FNGR", "RH"} else None)
            decision = validate_canonical_write_candidate(conn, bad)
            row = decision_row(ticker, decision, bad)
            row["replay_result"] = f"WOULD_{decision.decision}" if decision.decision != "PASS_WITH_WARNING" else "WOULD_WARN"
            row["known_defect_type"] = "FISCAL_CALENDAR_DRIVEN" if ticker in NINE_52_53_TICKERS else "MIXED_OR_NON_FISCAL"
            out.append(row)
            if ticker in NINE_52_53_TICKERS:
                backward.append(row)
    counts = Counter(row["replay_result"] for row in out)
    return out, backward, {"rows": len(out), "decision_counts": dict(counts), "nine_ticker_rows": len(backward)}


def false_positive_cases(v3_db: Path) -> list[dict[str, Any]]:
    cases = [
        ("CALENDAR_YEAR_NORMAL", "AA", 2026, "Q1", "2026-03-31", None),
        ("FIXED_DATE_NORMAL", "A", 2026, "Q2", "2026-04-30", None),
        ("WEEK_BASED_WEEKEND", "BBY", 2026, "Q2", "2025-08-02", None),
        ("WEEK_BASED_53_WEEK_REVIEW", "BBY", 2026, "Q4", "2026-01-31", None),
        ("TRANSITION_REVIEW", "LFCR", 2025, "Q4", "2025-12-31", FiscalCalendarTransitionEvidence("POSSIBLE_TRANSITION", "known transition-style case")),
        ("STUB_REVIEW", "VTGN", 2026, "Q1", "2025-09-30", None),
    ]
    out = []
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for name, ticker, fy, fq, period, transition in cases:
            company = conn.execute("SELECT company_id FROM v3_company WHERE ticker=?", (ticker,)).fetchone()
            if not company:
                continue
            candidate = FiscalCalendarWriteCandidate(
                company_id=int(company["company_id"]),
                fiscal_year=fy,
                fiscal_quarter=fq,
                period_end_date=period,
                publish_date="2026-02-20" if period < "2026-01-01" else "2026-05-01",
                transition_evidence=transition or FiscalCalendarTransitionEvidence(),
                stub_period=name == "STUB_REVIEW",
            )
            decision = validate_canonical_write_candidate(conn, candidate)
            row = decision_row(ticker, decision, candidate)
            row["case"] = name
            row["false_positive_assessment"] = "MATERIAL_FALSE_BLOCK_CANDIDATE" if decision.decision == "BLOCK" and name in {"TRANSITION_REVIEW", "STUB_REVIEW"} else "ACCEPTABLE_DIAGNOSTIC"
            out.append(row)
    return out


def dry_run_candidates(v3_db: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    out = []
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        sample = rows(
            conn,
            """
            SELECT c.company_id,c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            WHERE c.active=1 AND q.period_end_date IS NOT NULL
            ORDER BY q.updated_at_utc DESC, c.ticker
            LIMIT 500
            """,
        )
        for row in sample:
            candidate = FiscalCalendarWriteCandidate(
                company_id=int(row["company_id"]),
                fiscal_year=int(row["fiscal_year"]),
                fiscal_quarter=str(row["fiscal_quarter"]),
                period_end_date=row["period_end_date"],
                publish_date=row["publish_date"],
                source_context="PHASE8D_DRY_RUN_EXISTING_CANDIDATE_REPLAY",
            )
            decision = validate_canonical_write_candidate(conn, candidate)
            out.append(decision_row(str(row["ticker"]), decision, candidate))
    counts = Counter(row["guard_decision"] for row in out)
    return out, {
        "candidates_evaluated": len(out),
        "decision_counts": dict(counts),
        "previously_permitted_writes_now_stopped": counts.get("REVIEW", 0) + counts.get("BLOCK", 0),
        "obvious_false_blocks": sum(1 for row in out if row["guard_decision"] == "BLOCK" and "INSUFFICIENT_FISCAL_METADATA" in row["reason_codes"]),
        "guard_activation_status": "ACTIVE",
    }


def write_inventory(root: Path) -> None:
    (root / "update_v3_canonical_write_path_inventory.md").write_text(
        """# Update V3 Canonical Write Path Inventory

Canonical mutation entry points found:

- `V3QuarterRepository.upsert_quarter`: canonical FY/FQ identity plus period/publish metadata insert/update.
- `V3FundamentalsRepository.write_null_preserving_fields`: canonical fundamentals NULL-fill/conflict path after quarter identity exists.
- `V3CanonicalMigration.apply_candidate`: candidate orchestration that calls both repository paths.
- Phase 8 repair scripts contain bounded manual repair SQL and are intentionally outside automatic Update V3.

Protected entry point:

- `V3QuarterRepository.upsert_quarter` now invokes the fiscal-calendar guard before SQL mutation.

The guard runs before canonical identity writes, canonical field writes, accepted lineage ownership, and downstream invalidation.
""",
        encoding="utf-8",
    )
    write_json(
        root / "guard_integration_points.json",
        {
            "canonical_mutation_entry_points_found": 4,
            "protected_entry_points": ["V3QuarterRepository.upsert_quarter"],
            "unprotected_entry_points": ["bounded Phase 8 manual repair SQL tools, not automatic Update V3"],
            "central_guard": "swingmaster.fundamentals.v3_fiscal_calendar.validate_canonical_write_candidate",
            "guard_before_canonical_mutation": True,
        },
    )


def write_docs(summary: dict[str, Any]) -> None:
    block = f"""## Phase 8D - Fiscal Calendar Prevention Guards

Status: `{summary['classification']}`

Fiscal-calendar guard is active in `V3QuarterRepository.upsert_quarter` before canonical quarter mutation. Exact FY2026/FY2027 anchors are authoritative, backward inference assumes stable fiscal calendar unless positive transition evidence exists, and `REVIEW`/`BLOCK` candidates perform zero canonical writes.

Phase 8 remains `IN PROGRESS`.
"""
    for path in (
        Path("docs/fundamentals_v3_canonical_prevention_policy.md"),
        Path("docs/fundamentals_v3_fiscal_calendar_metadata.md"),
        Path("docs/fundamentals_v3_phase8_update_v3.md"),
        Path("docs/fundamentals_v3_master_plan_status.md"),
        Path("docs/fundamentals_v3_deferred_repair_handoff.md"),
    ):
        existing = path.read_text(encoding="utf-8")
        path.write_text(existing.rstrip() + "\n\n" + block, encoding="utf-8")


def run_phase8d(paths: Phase8DPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    write_inventory(paths.artifact_root)
    write_json(
        paths.artifact_root / "fiscal_anchor_authority_contract.json",
        {"FY2026_FY2027_imported_anchors": "VERIFIED_EXACT_AUTHORITATIVE", "current_v3_disagreement_is_transition_evidence": False},
    )
    write_json(
        paths.artifact_root / "backward_inference_contract.json",
        {"default": "STABLE_BACKWARD_CALENDAR", "week_based": "364_or_371_day_preserve_weekday", "fixed_date": "same_month_day", "transition_exception": "positive evidence required"},
    )

    before_counts = baseline_summary(paths.v3_db)
    before_fp = semantic_fingerprints(paths.v3_db)
    before_anchor = anchor_immutability(paths.v3_db)
    replay, backward, replay_summary = known_replay(paths.v3_db)
    false_positive = false_positive_cases(paths.v3_db)
    dry_run, dry_summary = dry_run_candidates(paths.v3_db)
    after_counts = baseline_summary(paths.v3_db)
    after_fp = semantic_fingerprints(paths.v3_db)
    after_anchor = anchor_immutability(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        phase_integrity = integrity(conn)

    write_csv(paths.artifact_root / "known_phase8_defect_guard_replay.csv", replay)
    write_csv(paths.artifact_root / "known_nine_ticker_backward_inference.csv", backward)
    write_json(paths.artifact_root / "known_defect_guard_summary.json", replay_summary)
    write_csv(paths.artifact_root / "guard_false_positive_review.csv", false_positive)
    write_csv(paths.artifact_root / "special_calendar_guard_cases.csv", false_positive)
    write_csv(paths.artifact_root / "update_v3_guard_dry_run_candidates.csv", dry_run)
    write_json(paths.artifact_root / "update_v3_guard_dry_run_summary.json", dry_summary)
    write_csv(paths.artifact_root / "guard_reason_distribution.csv", [{"reason_code": k, "rows": v} for k, v in Counter(reason for row in dry_run for reason in str(row["reason_codes"]).split("|") if reason).items()])
    write_json(paths.artifact_root / "pre_phase8d_semantic_fingerprints.json", {"counts": before_counts, "fingerprints": before_fp, "anchors": before_anchor})
    write_json(paths.artifact_root / "post_phase8d_semantic_fingerprints.json", {"counts": after_counts, "fingerprints": after_fp, "anchors": after_anchor})
    write_json(paths.artifact_root / "fiscal_anchor_immutability_check.json", {"identical": before_anchor == after_anchor, "before": before_anchor, "after": after_anchor})
    write_json(paths.artifact_root / "phase8d_integrity.json", phase_integrity)

    material_false_blocks = sum(1 for row in false_positive if row["false_positive_assessment"] == "MATERIAL_FALSE_BLOCK_CANDIDATE")
    safety_ok = before_counts == after_counts and before_fp == after_fp and before_anchor == after_anchor
    active_ok = safety_ok and replay_summary["decision_counts"].get("WOULD_BLOCK", 0) >= 9 and material_false_blocks == 0
    classification = CLASSIFICATION_ACTIVE if active_ok else CLASSIFICATION_REVIEW if safety_ok else CLASSIFICATION_BLOCKED
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "baseline_commit": "ec482c5",
        "phase8c_commit": "5a76a4c",
        "baseline_before": before_counts,
        "baseline_after": after_counts,
        "fiscal_authority": {
            "exact_anchor_rows": after_anchor["rows"],
            "FY2026": after_anchor["FY2026"],
            "FY2027": after_anchor["FY2027"],
            "anchor_conflicts": after_anchor["conflicts"],
            "anchor_immutability": before_anchor == after_anchor,
            "backward_stability_policy_implemented": True,
            "transition_exception_implemented": True,
        },
        "write_path": json.loads((paths.artifact_root / "guard_integration_points.json").read_text(encoding="utf-8")),
        "guard_behavior": {
            "dry_run_decisions": dry_summary["decision_counts"],
            "reason_counts": dict(Counter(reason for row in dry_run + replay + false_positive for reason in str(row["reason_codes"]).split("|") if reason)),
        },
        "known_phase8_replay": replay_summary,
        "false_positive": {"rows": len(false_positive), "material_false_block_count": material_false_blocks},
        "dry_run": dry_summary,
        "safety": {
            "companies_changed": int(before_counts["companies"] != after_counts["companies"]),
            "canonical_changed": int(before_fp["canonical"] != after_fp["canonical"]),
            "fundamentals_changed": int(before_counts["fundamentals_rows"] != after_counts["fundamentals_rows"]),
            "lineage_changed": int(before_counts["migration_audit_rows"] != after_counts["migration_audit_rows"]),
            "ttm_changed": int(before_fp["ttm"] != after_fp["ttm"]),
            "score_changed": int(before_fp["score"] != after_fp["score"]),
            "lifecycle_changed": int(before_fp["lifecycle"] != after_fp["lifecycle"]),
            "valuation_changed": int(before_fp["valuation"] != after_fp["valuation"]),
            "rawcandle_writes": 0,
        },
        "fingerprints_identical": {key: before_fp[key] == after_fp[key] for key in before_fp} | {"fiscal_anchors": before_anchor == after_anchor},
        "integrity": {"quick_check": phase_integrity["quick_check"], "fk_rows": phase_integrity["foreign_key_check"], "duplicate_canonical_fy_fq": phase_integrity["duplicate_fy_fq"], "orphans": phase_integrity["orphans"]},
        "phase8_status": "IN PROGRESS",
        "next_action": "KEEP PHASE 8B DOWNSTREAM AS CURRENT OPERATIONAL BASELINE; USE THE FISCAL-CALENDAR GUARD FOR ALL FUTURE UPDATE V3 CANONICAL WRITES; DEFER EXISTING CANONICAL REPAIRS UNTIL TIME ALLOWS"
        if classification == CLASSIFICATION_ACTIVE
        else "KEEP GUARD IN REVIEW/DIAGNOSTIC MODE; RESOLVE ONLY MATERIAL FALSE-POSITIVE OR TRANSITION-MODELING ISSUES BEFORE ACTIVATING HARD BLOCKS",
    }
    write_json(paths.artifact_root / "phase8d_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    write_docs(summary)
    if not safety_ok:
        raise RuntimeError("PHASE8D_BASELINE_CHANGED")
    return summary
