"""UI severity mapping for quarter update vintage summaries."""


def _as_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _has_text(value: object) -> bool:
    return bool(str(value or "").strip())


def _has_unknown_provenance_fields(value: object) -> bool:
    if isinstance(value, dict):
        return any(_as_int(item) > 0 for item in value.values())
    text = str(value or "").strip()
    return bool(text) and text.lower() not in {"0", "none", "[]", "{}"}


def _source_run_id(summary: dict) -> str:
    return str(summary.get("run_id") or summary.get("source_run_id") or "").strip()


def map_vintage_completion_status_to_ui_severity(summary: dict) -> str:
    """Map parsed vintage summary fields to a UI severity bucket."""
    completion_status = str(summary.get("vintage_completion_status") or "").strip()
    if not completion_status:
        return "unknown"

    stop_counts = [
        "vintage_post_run_latest_without_vintage_count",
        "vintage_post_run_duplicate_statement_vintage_id_count",
        "vintage_yahoo_aware_blocked_rows",
        "vintage_blocked_rows",
        "blocked_rows",
    ]
    if any(_as_int(summary.get(key)) > 0 for key in stop_counts):
        return "stop"
    if _has_unknown_provenance_fields(summary.get("vintage_yahoo_aware_unknown_provenance_fields")):
        return "stop"

    if completion_status in {"BLOCKED_POST_RUN_DRIFT", "UNKNOWN"}:
        return "stop"

    if completion_status in {"FINAL_MIXED_REQUIRED", "YAHOO_VINTAGE_REQUIRED"}:
        return "review"

    planned_counts = [
        "vintage_planned_final_mixed_rows",
        "vintage_planned_yahoo_vintage_rows",
    ]
    if any(_as_int(summary.get(key)) > 0 for key in planned_counts):
        return "review"

    if completion_status == "SEC_VINTAGE_SUFFICIENT":
        parity_status = str(summary.get("vintage_post_run_parity_status") or "OK").strip()
        if parity_status == "OK":
            return "success"
        return "stop"

    return "unknown"


def map_yahoo_aware_execution_status_to_ui_severity(summary: dict) -> str:
    """Map Yahoo-aware apply execution summary fields to a UI severity bucket."""
    status = str(summary.get("vintage_yahoo_aware_execution_status") or "").strip()
    if not status:
        return "unknown"
    if _as_int(summary.get("vintage_yahoo_aware_rows_blocked")) > 0:
        return "stop"
    if _has_text(summary.get("vintage_yahoo_aware_error")):
        return "stop"
    if status == "EXECUTION_COMPLETED":
        return "success"
    if status == "NO_ACTION_REQUIRED":
        return "review"
    if status == "EXECUTION_BLOCKED":
        return "stop"
    return "unknown"


def should_enable_yahoo_aware_apply(summary: dict) -> tuple[bool, str]:
    """Decide whether the UI should enable the explicit Yahoo-aware apply action."""
    completion_status = str(summary.get("vintage_completion_status") or "").strip()
    if completion_status in {"SEC_VINTAGE_SUFFICIENT", ""}:
        return False, "No Yahoo-aware apply is required."
    if completion_status in {"BLOCKED_POST_RUN_DRIFT", "UNKNOWN"}:
        return False, f"Apply blocked by completion status {completion_status}."
    if completion_status not in {"FINAL_MIXED_REQUIRED", "YAHOO_VINTAGE_REQUIRED"}:
        return False, "Apply requires FINAL_MIXED_REQUIRED or YAHOO_VINTAGE_REQUIRED."

    source_run_id = _source_run_id(summary)
    if not source_run_id:
        return False, "Apply requires source run id from the last USA quarter update summary."

    planning_status = str(summary.get("vintage_yahoo_aware_planning_status") or "").strip()
    if completion_status == "FINAL_MIXED_REQUIRED":
        if planning_status != "FINAL_MIXED_PLAN_READY":
            return False, "Final mixed apply requires FINAL_MIXED_PLAN_READY."
        if _as_int(summary.get("vintage_planned_final_mixed_rows")) <= 0:
            return False, "Final mixed apply requires planned final mixed rows."
    if completion_status == "YAHOO_VINTAGE_REQUIRED":
        if planning_status != "YAHOO_VINTAGE_PLAN_READY":
            return False, "Yahoo vintage apply requires YAHOO_VINTAGE_PLAN_READY."
        if _as_int(summary.get("vintage_planned_yahoo_vintage_rows")) <= 0:
            return False, "Yahoo vintage apply requires planned Yahoo vintage rows."

    stop_counts = [
        "vintage_yahoo_aware_blocked_rows",
        "vintage_post_run_duplicate_statement_vintage_id_count",
        "vintage_post_run_vintage_without_latest_count",
    ]
    if any(_as_int(summary.get(key)) > 0 for key in stop_counts):
        return False, "Apply blocked by drift, duplicate, or blocked-row counts."
    if _has_unknown_provenance_fields(summary.get("vintage_yahoo_aware_unknown_provenance_fields")):
        return False, "Apply blocked by unknown provenance fields."
    if str(summary.get("vintage_post_run_parity_status") or "").strip() in {"DRIFT", "UNKNOWN_RUN_LINKAGE"}:
        return False, "Apply blocked by post-run parity status."

    return True, "Yahoo-aware apply is available after explicit operator confirmation."


def should_auto_apply_yahoo_aware_vintage(
    summary: dict,
    *,
    user_enabled_vintage: bool,
) -> tuple[bool, str]:
    """Decide whether the UI may auto-run the Yahoo-aware apply follow-up."""
    if not user_enabled_vintage:
        return False, "Auto apply requires the PIT/vintage checkbox to be enabled for the primary run."

    enabled, reason = should_enable_yahoo_aware_apply(summary)
    if not enabled:
        return False, reason

    completion_status = str(summary.get("vintage_completion_status") or "").strip()
    if completion_status == "FINAL_MIXED_REQUIRED":
        return True, "Auto apply allowed for FINAL_MIXED_PLAN_READY summary."
    if completion_status == "YAHOO_VINTAGE_REQUIRED":
        return True, "Auto apply allowed for YAHOO_VINTAGE_PLAN_READY summary."
    return False, f"Auto apply is not required for completion status {completion_status or 'UNKNOWN'}."


def map_vintage_recovery_status_to_ui_severity(summary: dict) -> str:
    """Map PIT/vintage recovery status fields to a UI severity bucket."""
    status = str(summary.get("vintage_recovery_status") or "").strip()
    if not status:
        return "unknown"
    if status in {"RECOVERY_NOOP", "RECOVERY_APPLIED", "SEC_RECOVERY_APPLIED", "YAHOO_AWARE_RECOVERY_APPLIED"}:
        return "success"
    if status == "RECOVERY_READY":
        return "review"
    if status in {"RECOVERY_BLOCKED", "RECOVERY_UNKNOWN"}:
        return "stop"
    return "unknown"


def should_plan_sec_vintage_recovery(preflight_summary: dict) -> tuple[bool, str]:
    """Decide whether readiness preflight should continue to SEC recovery dry-run."""
    overall_status = str(preflight_summary.get("overall_status") or "").strip()
    latest_without_vintage = _as_int(preflight_summary.get("latest_without_vintage_count"))
    duplicate_count = _as_int(preflight_summary.get("duplicate_statement_vintage_id_count"))
    vintage_without_latest = _as_int(preflight_summary.get("vintage_without_latest_count"))

    if overall_status == "READY_NOOP" and latest_without_vintage <= 0:
        return False, "RECOVERY_NOOP"
    if duplicate_count > 0 or vintage_without_latest > 0:
        return False, "RECOVERY_BLOCKED"
    if overall_status == "PARITY_DRIFT" or latest_without_vintage > 0:
        return True, "RECOVERY_READY"
    if overall_status in {"PENDING_YAHOO_AWARE_ACTION"}:
        return False, "RECOVERY_BLOCKED"
    return False, "RECOVERY_UNKNOWN"


def should_apply_sec_vintage_recovery(
    *,
    preflight_summary: dict,
    dry_run_summary: dict,
) -> tuple[bool, str]:
    """Decide whether the SEC latest-writer recovery dry-run is safe to apply."""
    overall_status = str(dry_run_summary.get("overall_status") or "").strip()
    if overall_status != "DRY_RUN_READY":
        return False, f"SEC recovery dry-run is not ready: {overall_status or 'UNKNOWN'}."

    source_run_id = str(dry_run_summary.get("source_run_id") or "").strip()
    if not source_run_id:
        return False, "SOURCE_RUN_ID_REQUIRED"

    latest_without_vintage = _as_int(preflight_summary.get("latest_without_vintage_count"))
    latest_missing = _as_int(dry_run_summary.get("latest_missing_vintage_rows"))
    candidates_checked = _as_int(dry_run_summary.get("candidates_checked"))
    planned_vintage = _as_int(dry_run_summary.get("planned_vintage_rows"))
    planned_provenance = _as_int(dry_run_summary.get("planned_provenance_rows"))
    blocked_rows = _as_int(dry_run_summary.get("blocked_rows"))
    unknown_rows = _as_int(dry_run_summary.get("unknown_provenance_rows"))

    if blocked_rows > 0:
        return False, "SEC recovery dry-run has blocked rows."
    if unknown_rows > 0 or _has_unknown_provenance_fields(dry_run_summary.get("unknown_provenance_field_counts")):
        return False, "SEC recovery dry-run has unknown provenance."
    if planned_vintage <= 0 or planned_provenance <= 0:
        return False, "SEC recovery dry-run has no planned vintage/provenance rows."
    if latest_without_vintage > 0 and planned_vintage != latest_without_vintage:
        return False, "SEC recovery planned rows do not match readiness missing count."
    if latest_missing > 0 and planned_vintage != latest_missing:
        return False, "SEC recovery planned rows do not match dry-run missing count."
    if candidates_checked != planned_vintage:
        return False, "SEC recovery candidate count does not match planned rows."

    return True, "SEC latest-writer recovery plan is safe to apply."


def should_apply_yahoo_aware_recovery(
    *,
    preflight_summary: dict,
    plan_summary: dict,
) -> tuple[bool, str]:
    """Decide whether Yahoo-aware/final-mixed recovery is safe to apply."""
    source_run_id = str(plan_summary.get("source_run_id") or "").strip()
    if not source_run_id:
        return False, "SOURCE_RUN_ID_REQUIRED"

    planning_status = str(plan_summary.get("vintage_yahoo_aware_planning_status") or "").strip()
    if planning_status not in {"FINAL_MIXED_PLAN_READY", "YAHOO_VINTAGE_PLAN_READY"}:
        return False, f"Yahoo-aware recovery planning is not ready: {planning_status or 'UNKNOWN'}."

    planned_final = _as_int(plan_summary.get("vintage_planned_final_mixed_rows"))
    planned_yahoo = _as_int(plan_summary.get("vintage_planned_yahoo_vintage_rows"))
    planned_provenance = _as_int(plan_summary.get("vintage_planned_yahoo_aware_provenance_rows"))
    if planned_final + planned_yahoo <= 0:
        return False, "Yahoo-aware recovery has no planned vintage rows."
    if planned_provenance <= 0:
        return False, "Yahoo-aware recovery has no planned provenance rows."

    if _as_int(plan_summary.get("vintage_yahoo_aware_blocked_rows")) > 0:
        return False, "Yahoo-aware recovery has blocked rows."
    if _has_unknown_provenance_fields(plan_summary.get("vintage_yahoo_aware_unknown_provenance_fields")):
        return False, "Yahoo-aware recovery has unknown provenance."
    if _as_int(preflight_summary.get("duplicate_statement_vintage_id_count")) > 0:
        return False, "Yahoo-aware recovery blocked by duplicate statement vintage ids."
    if _as_int(preflight_summary.get("vintage_without_latest_count")) > 0:
        return False, "Yahoo-aware recovery blocked by vintage rows without latest rows."

    latest_without_vintage = _as_int(preflight_summary.get("latest_without_vintage_count"))
    if latest_without_vintage > 0 and planned_final + planned_yahoo != latest_without_vintage:
        return False, "Yahoo-aware recovery planned rows do not match readiness missing count."

    overall_status = str(plan_summary.get("overall_status") or "").strip()
    if overall_status and overall_status not in {"YAHOO_AWARE_RECOVERY_READY"}:
        return False, f"Yahoo-aware recovery status is not ready: {overall_status}."

    return True, "Yahoo-aware/final-mixed recovery plan is safe to apply."
