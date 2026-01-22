from swingmaster.cli.run_daily_universe import _effective_limit
from swingmaster.cli._debug_utils import _dbg


def test_effective_limit_zero_means_all():
    items = ["a", "b", "c"]

    class Args:
        debug_limit = 0

    assert _effective_limit(Args(), items) == len(items)


def test_effective_limit_nonzero_caps():
    items = ["a", "b", "c", "d"]

    class Args:
        debug_limit = 2

    assert _effective_limit(Args(), items) == 2


def test_removed_tickers_order_stable():
    before = ["A", "B", "A", "C", "D"]
    after = ["A", "D"]
    removed = [t for t in before if t not in set(after)]
    assert removed == ["B", "C"]


def test_dbg_prefix_and_suppression(capsys):
    class Args:
        debug = True
        debug_limit = 5

    _dbg(Args(), "hello")
    out = capsys.readouterr().out.strip()
    assert out.startswith("[debug] ")
    assert out.endswith("hello")

    class ArgsOff:
        debug = False
        debug_limit = 5

    _dbg(ArgsOff(), "silent")
    out_off = capsys.readouterr().out
    assert out_off == ""
