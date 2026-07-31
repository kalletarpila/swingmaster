from __future__ import annotations


VINTAGE_PROVENANCE_WRITES_ENABLED = False
VINTAGE_DISABLED_STATUS = "VINTAGE_DISABLED"
VINTAGE_DISABLED_REASON = "vintage and field-provenance writes are disabled by product policy"
VINTAGE_DISABLED_ERROR = "VINTAGE_PROVENANCE_WRITES_DISABLED"


def disabled_write_summary(*, latest_rows_written: int = 0) -> dict[str, int | str | bool]:
    return {
        "latest_rows_written": latest_rows_written,
        "vintage_rows_written": 0,
        "field_provenance_rows_written": 0,
        "vintage_disabled": True,
        "vintage_status": VINTAGE_DISABLED_STATUS,
    }


def reject_vintage_write() -> None:
    raise RuntimeError(f"{VINTAGE_DISABLED_ERROR}:{VINTAGE_DISABLED_REASON}")
