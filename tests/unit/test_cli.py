"""Tests for CLI."""

from unittest.mock import patch

from click.testing import CliRunner

from taskflow.cli import cli
from taskflow.core import task as task_module


def test_worker_imports_modules_before_starting() -> None:
    """--import flag should import modules (registering their tasks) before worker starts."""
    task_module._task_registry.clear()

    with patch("taskflow.cli.run_worker") as mock_run_worker:
        mock_run_worker.return_value = None

        with patch("asyncio.run"):
            runner = CliRunner()
            result = runner.invoke(
                cli,
                ["worker", "--import", "examples.simple_example"],
            )

    assert result.exit_code == 0, result.output
    assert "add" in task_module._task_registry
    assert "multiply" in task_module._task_registry
    assert "process_data" in task_module._task_registry
