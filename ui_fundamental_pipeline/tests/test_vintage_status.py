import unittest

from ui_fundamental_pipeline.vintage_status import (
    map_vintage_completion_status_to_ui_severity,
    map_vintage_recovery_status_to_ui_severity,
    map_yahoo_aware_execution_status_to_ui_severity,
    should_apply_yahoo_aware_recovery,
    should_auto_apply_yahoo_aware_vintage,
    should_enable_yahoo_aware_apply,
)


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

    def test_yahoo_aware_execution_completed_returns_success(self):
        summary = {"vintage_yahoo_aware_execution_status": "EXECUTION_COMPLETED"}
        self.assertEqual(map_yahoo_aware_execution_status_to_ui_severity(summary), "success")

    def test_yahoo_aware_execution_blocked_returns_stop(self):
        summary = {"vintage_yahoo_aware_execution_status": "EXECUTION_BLOCKED"}
        self.assertEqual(map_yahoo_aware_execution_status_to_ui_severity(summary), "stop")


class TestYahooAwareApplyGate(unittest.TestCase):
    def test_sec_sufficient_disables_apply(self):
        enabled, _ = should_enable_yahoo_aware_apply({"vintage_completion_status": "SEC_VINTAGE_SUFFICIENT"})
        self.assertFalse(enabled)

    def test_final_mixed_plan_ready_enables_apply(self):
        enabled, _ = should_enable_yahoo_aware_apply(
            {
                "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
                "vintage_yahoo_aware_blocked_rows": "0",
                "vintage_yahoo_aware_unknown_provenance_fields": "",
            }
        )
        self.assertTrue(enabled)

    def test_source_run_id_and_zero_unknown_provenance_enable_apply(self):
        enabled, _ = should_enable_yahoo_aware_apply(
            {
                "source_run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
                "vintage_yahoo_aware_unknown_provenance_fields": "0",
            }
        )
        self.assertTrue(enabled)

    def test_yahoo_plan_ready_enables_apply(self):
        enabled, _ = should_enable_yahoo_aware_apply(
            {
                "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "YAHOO_VINTAGE_REQUIRED",
                "vintage_yahoo_aware_planning_status": "YAHOO_VINTAGE_PLAN_READY",
                "vintage_planned_yahoo_vintage_rows": "1",
            }
        )
        self.assertTrue(enabled)

    def test_blocked_rows_disable_apply(self):
        enabled, reason = should_enable_yahoo_aware_apply(
            {
                "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
                "vintage_yahoo_aware_blocked_rows": "1",
            }
        )
        self.assertFalse(enabled)
        self.assertIn("blocked", reason.lower())

    def test_unknown_provenance_disables_apply(self):
        enabled, reason = should_enable_yahoo_aware_apply(
            {
                "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
                "vintage_yahoo_aware_unknown_provenance_fields": "AAPL:2026-03-31:cash",
            }
        )
        self.assertFalse(enabled)
        self.assertIn("unknown provenance", reason.lower())

    def test_missing_source_run_id_disables_apply(self):
        enabled, reason = should_enable_yahoo_aware_apply(
            {
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
            }
        )
        self.assertFalse(enabled)
        self.assertIn("source run id", reason.lower())

    def test_blocked_or_unknown_completion_disables_apply(self):
        for status in ("BLOCKED_POST_RUN_DRIFT", "UNKNOWN"):
            enabled, _ = should_enable_yahoo_aware_apply({"vintage_completion_status": status})
            self.assertFalse(enabled)


class TestYahooAwareAutoApplyGate(unittest.TestCase):
    def test_safe_final_mixed_summary_enables_auto_apply(self):
        enabled, reason = should_auto_apply_yahoo_aware_vintage(
            {
                "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
                "vintage_yahoo_aware_blocked_rows": "0",
                "vintage_yahoo_aware_unknown_provenance_fields": "",
            },
            user_enabled_vintage=True,
        )
        self.assertTrue(enabled)
        self.assertIn("FINAL_MIXED_PLAN_READY", reason)

    def test_safe_yahoo_summary_enables_auto_apply(self):
        enabled, reason = should_auto_apply_yahoo_aware_vintage(
            {
                "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "YAHOO_VINTAGE_REQUIRED",
                "vintage_yahoo_aware_planning_status": "YAHOO_VINTAGE_PLAN_READY",
                "vintage_planned_yahoo_vintage_rows": "1",
            },
            user_enabled_vintage=True,
        )
        self.assertTrue(enabled)
        self.assertIn("YAHOO_VINTAGE_PLAN_READY", reason)

    def test_checkbox_off_disables_auto_apply(self):
        enabled, reason = should_auto_apply_yahoo_aware_vintage(
            {
                "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
            },
            user_enabled_vintage=False,
        )
        self.assertFalse(enabled)
        self.assertIn("checkbox", reason.lower())

    def test_sec_sufficient_disables_auto_apply(self):
        enabled, _ = should_auto_apply_yahoo_aware_vintage(
            {"vintage_completion_status": "SEC_VINTAGE_SUFFICIENT"},
            user_enabled_vintage=True,
        )
        self.assertFalse(enabled)

    def test_blocked_unknown_and_unsafe_summaries_disable_auto_apply(self):
        cases = [
            {"vintage_completion_status": "BLOCKED_POST_RUN_DRIFT"},
            {"vintage_completion_status": "UNKNOWN"},
            {
                "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
                "vintage_yahoo_aware_blocked_rows": "1",
            },
            {
                "run_id": "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
                "vintage_yahoo_aware_unknown_provenance_fields": "AAPL:field",
            },
            {
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "1",
            },
        ]
        for summary in cases:
            with self.subTest(summary=summary):
                enabled, _ = should_auto_apply_yahoo_aware_vintage(summary, user_enabled_vintage=True)
                self.assertFalse(enabled)


class TestYahooAwareRecoveryGate(unittest.TestCase):
    def test_yahoo_aware_recovery_ready_enables_apply(self):
        enabled, reason = should_apply_yahoo_aware_recovery(
            preflight_summary={
                "latest_without_vintage_count": "2",
                "duplicate_statement_vintage_id_count": "0",
                "vintage_without_latest_count": "0",
            },
            plan_summary={
                "overall_status": "YAHOO_AWARE_RECOVERY_READY",
                "source_run_id": "USA_QUARTER_UPDATE_2026-07-07__QUARTERLY",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_planned_final_mixed_rows": "2",
                "vintage_planned_yahoo_vintage_rows": "0",
                "vintage_planned_yahoo_aware_provenance_rows": "10",
                "vintage_yahoo_aware_blocked_rows": "0",
                "vintage_yahoo_aware_unknown_provenance_fields": "",
            },
        )

        self.assertTrue(enabled)
        self.assertIn("safe", reason)

    def test_yahoo_aware_recovery_blocks_unsafe_plan(self):
        cases = [
            {"source_run_id": ""},
            {"vintage_yahoo_aware_blocked_rows": "1"},
            {"vintage_yahoo_aware_unknown_provenance_fields": "AAPL:cash"},
            {"vintage_planned_final_mixed_rows": "0", "vintage_planned_yahoo_vintage_rows": "0"},
            {"overall_status": "YAHOO_AWARE_RECOVERY_BLOCKED"},
        ]
        base = {
            "overall_status": "YAHOO_AWARE_RECOVERY_READY",
            "source_run_id": "USA_QUARTER_UPDATE_2026-07-07__QUARTERLY",
            "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
            "vintage_planned_final_mixed_rows": "2",
            "vintage_planned_yahoo_vintage_rows": "0",
            "vintage_planned_yahoo_aware_provenance_rows": "10",
            "vintage_yahoo_aware_blocked_rows": "0",
            "vintage_yahoo_aware_unknown_provenance_fields": "",
        }
        preflight = {
            "latest_without_vintage_count": "2",
            "duplicate_statement_vintage_id_count": "0",
            "vintage_without_latest_count": "0",
        }
        for override in cases:
            with self.subTest(override=override):
                enabled, _ = should_apply_yahoo_aware_recovery(
                    preflight_summary=preflight,
                    plan_summary={**base, **override},
                )
                self.assertFalse(enabled)

    def test_recovery_status_success_mapping_includes_explicit_modes(self):
        self.assertEqual(
            map_vintage_recovery_status_to_ui_severity({"vintage_recovery_status": "SEC_RECOVERY_APPLIED"}),
            "success",
        )
        self.assertEqual(
            map_vintage_recovery_status_to_ui_severity({"vintage_recovery_status": "YAHOO_AWARE_RECOVERY_APPLIED"}),
            "success",
        )


if __name__ == "__main__":
    unittest.main()
