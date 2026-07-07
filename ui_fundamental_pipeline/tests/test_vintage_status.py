import unittest

from ui_fundamental_pipeline.vintage_status import map_vintage_completion_status_to_ui_severity


class TestVintageStatusSeverity(unittest.TestCase):
    def test_missing_vintage_fields_returns_unknown(self):
        self.assertEqual(map_vintage_completion_status_to_ui_severity({}), "unknown")

    def test_sec_vintage_sufficient_returns_success(self):
        summary = {
            "vintage_completion_status": "SEC_VINTAGE_SUFFICIENT",
            "vintage_post_run_parity_status": "OK",
            "vintage_post_run_latest_without_vintage_count": "0",
            "vintage_yahoo_aware_blocked_rows": "0",
        }
        self.assertEqual(map_vintage_completion_status_to_ui_severity(summary), "success")

    def test_final_mixed_required_returns_review(self):
        summary = {"vintage_completion_status": "FINAL_MIXED_REQUIRED"}
        self.assertEqual(map_vintage_completion_status_to_ui_severity(summary), "review")

    def test_yahoo_vintage_required_returns_review(self):
        summary = {"vintage_completion_status": "YAHOO_VINTAGE_REQUIRED"}
        self.assertEqual(map_vintage_completion_status_to_ui_severity(summary), "review")

    def test_blocked_post_run_drift_returns_stop(self):
        summary = {"vintage_completion_status": "BLOCKED_POST_RUN_DRIFT"}
        self.assertEqual(map_vintage_completion_status_to_ui_severity(summary), "stop")

    def test_unknown_completion_returns_stop(self):
        summary = {"vintage_completion_status": "UNKNOWN"}
        self.assertEqual(map_vintage_completion_status_to_ui_severity(summary), "stop")

    def test_drift_counts_return_stop(self):
        summary = {
            "vintage_completion_status": "SEC_VINTAGE_SUFFICIENT",
            "vintage_post_run_parity_status": "OK",
            "vintage_post_run_latest_without_vintage_count": "1",
        }
        self.assertEqual(map_vintage_completion_status_to_ui_severity(summary), "stop")

    def test_unknown_provenance_returns_stop(self):
        summary = {
            "vintage_completion_status": "FINAL_MIXED_REQUIRED",
            "vintage_yahoo_aware_unknown_provenance_fields": "AAPL:2026-03-31:cash",
        }
        self.assertEqual(map_vintage_completion_status_to_ui_severity(summary), "stop")

    def test_planned_rows_return_review(self):
        summary = {
            "vintage_completion_status": "SEC_VINTAGE_SUFFICIENT",
            "vintage_post_run_parity_status": "OK",
            "vintage_planned_final_mixed_rows": "2",
        }
        self.assertEqual(map_vintage_completion_status_to_ui_severity(summary), "review")


if __name__ == "__main__":
    unittest.main()
