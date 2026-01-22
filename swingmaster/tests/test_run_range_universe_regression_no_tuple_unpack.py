"""Regression test to prevent tuple-unpack crashes in run_range_universe debug paths."""

from __future__ import annotations

from pathlib import Path

import swingmaster.cli.run_range_universe as run_range_universe


def test_no_tuple_unpack_in_rc_conn_execute_fetchall() -> None:
    source = Path(run_range_universe.__file__).read_text()
    forbidden = [
        "for t, v in rc_conn.execute",
        "for t,v in rc_conn.execute",
        "for (t, v) in rc_conn.execute",
        "for (t,v) in rc_conn.execute",
    ]
    assert all(snippet not in source for snippet in forbidden)
