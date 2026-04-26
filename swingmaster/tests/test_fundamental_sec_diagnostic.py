from __future__ import annotations

import sqlite3

import pytest

from swingmaster.cli import run_fundamental_sec_diagnostic
from swingmaster.fundamentals import sec_edgar


def test_resolve_cik(monkeypatch) -> None:
    monkeypatch.setattr(
        sec_edgar,
        "fetch_json",
        lambda _url, _ua: {
            "0": {"ticker": "AAPL", "cik_str": 320193},
            "1": {"ticker": "NVDA", "cik_str": 1045810},
        },
    )
    assert sec_edgar.resolve_cik("aapl", sec_edgar.SEC_USER_AGENT) == "0000320193"
    assert sec_edgar.resolve_cik("NVDA", sec_edgar.SEC_USER_AGENT) == "0001045810"


def test_ticker_not_found(monkeypatch) -> None:
    monkeypatch.setattr(
        sec_edgar,
        "fetch_json",
        lambda _url, _ua: {"0": {"ticker": "AAPL", "cik_str": 320193}},
    )
    with pytest.raises(RuntimeError, match="^SEC_TICKER_NOT_FOUND:TSLA$"):
        sec_edgar.resolve_cik("TSLA", sec_edgar.SEC_USER_AGENT)


def test_inspect_found_tag() -> None:
    companyfacts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {"end": "2024-03-31", "form": "10-Q"},
                            {"end": "2024-12-31", "form": "10-K"},
                        ],
                        "USDm": [
                            {"end": "2023-12-31", "form": "10-K"},
                        ],
                    }
                }
            }
        }
    }
    result = sec_edgar.inspect_companyfacts_tags(companyfacts, ["Revenues"])[0]
    assert result["found"] is True
    assert result["namespace"] == "us-gaap"
    assert result["unit_count"] == 2
    assert result["fact_count"] == 3
    assert result["form_count_10q"] == 1
    assert result["form_count_10k"] == 2
    assert result["first_end_date"] == "2023-12-31"
    assert result["last_end_date"] == "2024-12-31"


def test_inspect_missing_tag() -> None:
    companyfacts = {"facts": {"us-gaap": {}}}
    result = sec_edgar.inspect_companyfacts_tags(companyfacts, ["Revenues"])[0]
    assert result == {
        "tag": "Revenues",
        "namespace": "us-gaap",
        "found": False,
        "unit_count": 0,
        "fact_count": 0,
        "form_count_10q": 0,
        "form_count_10k": 0,
        "first_end_date": None,
        "last_end_date": None,
    }


def test_cli_output(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        run_fundamental_sec_diagnostic,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "ticker": "AAPL",
                "user_agent": sec_edgar.SEC_USER_AGENT,
            },
        )(),
    )
    monkeypatch.setattr(run_fundamental_sec_diagnostic, "resolve_cik", lambda _ticker, _ua: "0000320193")
    monkeypatch.setattr(
        run_fundamental_sec_diagnostic,
        "fetch_companyfacts",
        lambda _cik, _ua: {
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "units": {
                            "USD": [
                                {"end": "2024-03-31", "form": "10-Q"},
                            ]
                        }
                    }
                }
            }
        },
    )
    run_fundamental_sec_diagnostic.main()
    out = capsys.readouterr().out
    assert "SEC EDGAR FUNDAMENTAL DIAGNOSTIC" in out
    assert "ticker=AAPL" in out
    assert "cik=0000320193" in out
    assert "tag=Revenues" in out
    assert "SUMMARY ticker=AAPL" in out
    assert "SUMMARY status=ok" in out
