import unittest

from ui_fundamental_pipeline.ticker_utils import parse_and_validate_tickers


class TestTickerUtils(unittest.TestCase):
    def test_parse_case_insensitive_and_trim(self):
        valid = {"AAPL", "MSFT", "TYRES.HE"}
        result = parse_and_validate_tickers(" aapl, msft  TYRES.he ", valid)
        self.assertEqual(result, ["AAPL", "MSFT", "TYRES.HE"])

    def test_parse_deduplicates(self):
        valid = {"AAPL", "MSFT"}
        result = parse_and_validate_tickers("aapl, AAPL, msft, aapl", valid)
        self.assertEqual(result, ["AAPL", "MSFT"])

    def test_parse_ignores_invalid(self):
        valid = {"AAPL"}
        result = parse_and_validate_tickers("AAPL,INVALID,", valid)
        self.assertEqual(result, ["AAPL"])

    def test_parse_empty(self):
        self.assertEqual(parse_and_validate_tickers("", {"AAPL"}), [])


if __name__ == "__main__":
    unittest.main()
