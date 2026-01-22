from __future__ import annotations

import argparse
from typing import Iterable, List, Sequence


def _debug_enabled(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "debug", False))


def _debug_limit(args: argparse.Namespace) -> int | None:
    limit = getattr(args, "debug_limit", 0)
    return None if limit == 0 else limit


def _effective_limit(args: argparse.Namespace, items: Sequence[object]) -> int:
    if not items:
        return 0
    raw = getattr(args, "debug_limit", 0)
    if raw == 0:
        return len(items)
    return min(raw, len(items))


def _dbg(args: argparse.Namespace, msg: str) -> None:
    if _debug_enabled(args):
        print(f"[debug] {msg}")


def _take_head_tail(items: List[str], limit: int | None) -> tuple[List[str], List[str]]:
    if limit is None or limit <= 0:
        return items, []
    head = items[:limit]
    tail = items[-limit:] if len(items) > limit else []
    return head, tail


def infer_entry_blocker(rc_state: str, reasons: Iterable[str]) -> str:
    reasons_set = set(reasons)
    if "DATA_INSUFFICIENT" in reasons_set:
        return "BLOCKER_DATA_INSUFFICIENT"
    if "INVALIDATED" in reasons_set:
        return "BLOCKER_INVALIDATED"
    if "CHURN_GUARD" in reasons_set:
        return "BLOCKER_CHURN_GUARD"
    if "TREND_MATURED" in reasons_set:
        return "BLOCKER_TREND_MATURED"
    if "NO_SIGNAL" in reasons_set:
        return "BLOCKER_NO_SIGNAL"
    if rc_state in {"NO_TRADE"}:
        return "BLOCKER_STATE_NO_TRADE"
    if rc_state in {"DOWNTREND_LATE"}:
        return "BLOCKER_STATE_DOWNTREND_LATE"
    if rc_state in {"DOWNTREND_EARLY"}:
        return "BLOCKER_STATE_DOWNTREND_EARLY"
    if rc_state in {"STABILIZING"}:
        return "BLOCKER_STATE_STABILIZING"
    return "BLOCKER_UNKNOWN"

