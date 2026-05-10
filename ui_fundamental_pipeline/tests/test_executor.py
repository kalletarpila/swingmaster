import unittest

from ui_fundamental_pipeline.executor import ProcessExecutor


class TestProcessExecutor(unittest.TestCase):
    def test_parse_summary_block(self):
        executor = ProcessExecutor()
        output = [
            "2026-05-10 10:00:00 | info line",
            "2026-05-10 10:00:01 | SUMMARY:",
            "2026-05-10 10:00:02 | market=usa",
            "2026-05-10 10:00:03 | symbols_processed=10",
        ]
        parsed = executor._parse_summary_block(output, {})
        self.assertEqual(parsed.get("market"), "usa")
        self.assertEqual(parsed.get("symbols_processed"), "10")


if __name__ == "__main__":
    unittest.main()
