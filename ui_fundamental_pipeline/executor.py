"""
Process executor with real-time output capture via threading.
"""
import subprocess
import threading
import queue
import signal
import os
from typing import Callable, Optional, Tuple
from datetime import datetime
from pathlib import Path

try:
    from .config import (
        PROCESS_TIMEOUT_SECONDS,
        PROCESS_TERMINATION_WAIT_SECONDS,
        DATETIME_FORMAT,
        PROJECT_ROOT,
    )
except ImportError:  # pragma: no cover
    from config import (
        PROCESS_TIMEOUT_SECONDS,
        PROCESS_TERMINATION_WAIT_SECONDS,
        DATETIME_FORMAT,
        PROJECT_ROOT,
    )


class ProcessExecutor:
    """Execute CLI scripts with real-time output streaming."""

    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self.output_queue: Optional[queue.Queue] = None
        self.stdout_thread: Optional[threading.Thread] = None
        self.stderr_thread: Optional[threading.Thread] = None
        self.is_running = False

    def execute(
        self,
        command: list,
        on_output: Callable[[str], None],
        on_summary: Callable[[dict], None],
        cwd: Optional[Path] = None,
    ) -> Tuple[int, list]:
        """
        Execute command with real-time output streaming.

        Args:
            command: List of command arguments [python_exe, script, --arg1, val1, ...]
            on_output: Callback for each output line (called from main thread)
            on_summary: Callback when SUMMARY: block is found (called from main thread)
            cwd: Working directory

        Returns:
            Tuple of (exit_code, all_output_lines)
        """
        self.is_running = True
        self.output_queue = queue.Queue()
        all_output = []

        try:
            # Set PYTHONPATH environment
            env = os.environ.copy()
            env["PYTHONPATH"] = str(PROJECT_ROOT)

            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                cwd=cwd or PROJECT_ROOT,
                env=env,
            )

            # Start threads to capture stdout and stderr
            self.stdout_thread = threading.Thread(
                target=self._read_stream,
                args=(self.process.stdout, "stdout"),
                daemon=True,
            )
            self.stderr_thread = threading.Thread(
                target=self._read_stream,
                args=(self.process.stderr, "stderr"),
                daemon=True,
            )
            self.stdout_thread.start()
            self.stderr_thread.start()

            # Process queue items
            summary_data = {}
            last_output_time = datetime.now()

            while self.is_running:
                try:
                    line = self.output_queue.get(timeout=0.1)
                    last_output_time = datetime.now()

                    # Add timestamp to line
                    timestamp = datetime.now().strftime(DATETIME_FORMAT)
                    timestamped_line = f"{timestamp} | {line}"
                    all_output.append(timestamped_line)

                    # Call output callback
                    on_output(timestamped_line)

                    # Parse SUMMARY block continuously so key=value lines are captured
                    parsed_summary = self._parse_summary_block(all_output, summary_data)
                    if parsed_summary != summary_data and parsed_summary:
                        summary_data = parsed_summary
                        on_summary(summary_data)

                except queue.Empty:
                    # Check if process is still alive
                    if self.process.poll() is not None:
                        # Process finished
                        break
                    continue

                # Timeout check (2 hours)
                elapsed = (datetime.now() - last_output_time).total_seconds()
                if elapsed > PROCESS_TIMEOUT_SECONDS:
                    self.terminate()
                    all_output.append(
                        f"{datetime.now().strftime(DATETIME_FORMAT)} | "
                        "ERROR: Process timeout (2 hours)"
                    )
                    break

            # Wait for process to finish
            exit_code = self.process.wait(timeout=5)

        except subprocess.TimeoutExpired:
            self.process.kill()
            exit_code = -1
            all_output.append(
                f"{datetime.now().strftime(DATETIME_FORMAT)} | "
                "ERROR: Process termination timeout"
            )
        except Exception as e:
            all_output.append(
                f"{datetime.now().strftime(DATETIME_FORMAT)} | "
                f"ERROR: {str(e)}"
            )
            exit_code = -1
        finally:
            if self.process is not None:
                if self.process.stdout:
                    self.process.stdout.close()
                if self.process.stderr:
                    self.process.stderr.close()
            self.is_running = False
            self.process = None

        return exit_code, all_output

    def _read_stream(self, stream, stream_type: str):
        """Read stream line by line and put into queue."""
        try:
            for line in iter(stream.readline, ""):
                if line:
                    self.output_queue.put(line.rstrip())
        except Exception:
            pass

    def _parse_summary_block(
        self, all_output: list, previous_summary: dict
    ) -> dict:
        """
        Parse SUMMARY: block from output.
        Format:
            SUMMARY:
            key1=value1
            key2=value2
        """
        summary = previous_summary.copy()

        latest_summary_index = None
        for idx, line in enumerate(all_output):
            if "SUMMARY:" in line:
                latest_summary_index = idx

        if latest_summary_index is None:
            return summary

        for line in all_output[latest_summary_index + 1 :]:
            parts = line.split("|", 1)
            if len(parts) != 2:
                continue
            payload = parts[1].strip()
            if "=" not in payload:
                break
            key, value = payload.split("=", 1)
            summary[key.strip()] = value.strip()

        return summary

    def terminate(self):
        """Terminate the process."""
        if self.process and self.process.poll() is None:
            try:
                os.kill(self.process.pid, signal.SIGTERM)
                # Wait for graceful shutdown
                try:
                    self.process.wait(timeout=PROCESS_TERMINATION_WAIT_SECONDS)
                except subprocess.TimeoutExpired:
                    # Force kill if graceful shutdown fails
                    self.process.kill()
                    self.process.wait()
            except Exception:
                pass
        self.is_running = False
