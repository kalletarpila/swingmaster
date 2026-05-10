import unittest
from pathlib import Path

from ui_fundamental_pipeline.executor import ProcessExecutor


class TestExecutorIntegration(unittest.TestCase):
    def test_execute_python_snippet_with_output_and_summary(self):
        executor = ProcessExecutor()
        lines = []
        summaries = []

        command = [
            "python3",
            "-c",
            "print('hello'); print('SUMMARY:'); print('key=value')",
        ]

        exit_code, all_output = executor.execute(
            command=command,
            on_output=lambda line: lines.append(line),
            on_summary=lambda summary: summaries.append(summary),
            cwd=Path.cwd(),
        )

        self.assertEqual(exit_code, 0)
        self.assertTrue(any("hello" in line for line in all_output))
        self.assertTrue(any(summary.get("key") == "value" for summary in summaries))


if __name__ == "__main__":
    unittest.main()
