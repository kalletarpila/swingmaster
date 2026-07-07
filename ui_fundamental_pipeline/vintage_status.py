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
