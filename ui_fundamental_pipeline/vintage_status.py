"""UI severity mapping for quarter update vintage summaries."""


def _as_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _has_text(value: object) -> bool:
    return bool(str(value or "").strip())


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
    if _has_text(summary.get("vintage_yahoo_aware_unknown_provenance_fields")):
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

    source_run_id = str(summary.get("run_id") or "").strip()
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
    if _has_text(summary.get("vintage_yahoo_aware_unknown_provenance_fields")):
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
