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

    def test_parse_repeated_summary_key_value_lines(self):
        executor = ProcessExecutor()
        output = [
            "2026-05-10 10:00:00 | info line",
            "2026-05-10 10:00:01 | SUMMARY vintage_completion_status=SEC_VINTAGE_SUFFICIENT",
            "2026-05-10 10:00:02 | SUMMARY vintage_next_required_action=NONE",
            "2026-05-10 10:00:03 | SUMMARY vintage_post_run_latest_without_vintage_count=0",
            "2026-05-10 10:00:04 | SUMMARY vintage_yahoo_aware_planning_status=NO_ACTION_REQUIRED",
        ]
        parsed = executor._parse_summary_block(output, {})

        self.assertEqual(parsed.get("vintage_completion_status"), "SEC_VINTAGE_SUFFICIENT")
        self.assertEqual(parsed.get("vintage_next_required_action"), "NONE")
        self.assertEqual(parsed.get("vintage_post_run_latest_without_vintage_count"), "0")
        self.assertEqual(parsed.get("vintage_yahoo_aware_planning_status"), "NO_ACTION_REQUIRED")

    def test_parse_summary_formats_without_dropping_old_fields(self):
        executor = ProcessExecutor()
        output = [
            "2026-05-10 10:00:00 | SUMMARY:",
            "2026-05-10 10:00:01 | market=usa",
            "2026-05-10 10:00:02 | tickers_processed=10",
            "2026-05-10 10:00:03 | SUMMARY vintage_completion_status=FINAL_MIXED_REQUIRED",
        ]
        parsed = executor._parse_summary_block(output, {})

        self.assertEqual(parsed.get("market"), "usa")
        self.assertEqual(parsed.get("tickers_processed"), "10")
        self.assertEqual(parsed.get("vintage_completion_status"), "FINAL_MIXED_REQUIRED")


if __name__ == "__main__":
    unittest.main()
