"""Tests for task decorator and registration."""

from __future__ import annotations

import pytest

from taskflow.core.task import Task, TaskConfig, get_task, register_task, task


def test_task_decorator_basic() -> None:
    """Test basic task decoration."""

    @task(name="test_add")
    def add(x: int, y: int) -> int:
        """Add two numbers."""
        return x + y

    # Check task is registered
    registered_task = get_task("test_add")
    assert registered_task is not None
    assert registered_task.name == "test_add"
    # The decorator returns a Task object, not the original function
    assert registered_task == add

    # Check task can be called
    result = add(2, 3)
    assert result == 5


def test_task_decorator_with_config() -> None:
    """Test task decorator with custom configuration."""

    @task(
        name="test_priority",
        queue="high",
        max_retries=5,
        timeout=120,
        priority=8,
    )
    def priority_task(value: str) -> str:
        """Task with custom config."""
        return f"processed: {value}"

    registered_task = get_task("test_priority")
    assert registered_task is not None
    assert registered_task.config.queue == "high"
    assert registered_task.config.max_retries == 5
    assert registered_task.config.timeout == 120
    assert registered_task.config.priority == 8

    result = priority_task("test")
    assert result == "processed: test"


def test_task_registration_direct() -> None:
    """Test direct task registration."""

    def multiply(x: int, y: int) -> int:
        """Multiply two numbers."""
        return x * y

    config = TaskConfig(queue="math", priority=7)
    task_obj = Task(
        name="test_multiply",
        func=multiply,
        config=config,
        module=multiply.__module__,
    )

    register_task(task_obj)

    registered = get_task("test_multiply")
    assert registered is not None
    assert registered.name == "test_multiply"
    assert registered.config.queue == "math"
    assert registered.config.priority == 7


def test_async_task_decorator() -> None:
    """Test decorator with async function."""

    @task(name="test_async")
    async def async_task(value: str) -> str:
        """Async task."""
        return f"async: {value}"

    registered_task = get_task("test_async")
    assert registered_task is not None
    assert registered_task.name == "test_async"

    # Check that async function is preserved
    import asyncio

    assert asyncio.iscoroutinefunction(registered_task.func)


def test_task_duplicate_registration() -> None:
    """Test that duplicate task names raise an error."""

    @task(name="duplicate")
    def task1() -> str:
        return "task1"

    with pytest.raises(ValueError, match="Task 'duplicate' is already registered"):

        @task(name="duplicate")
        def task2() -> str:
            return "task2"


def test_get_nonexistent_task() -> None:
    """Test getting a task that doesn't exist."""
    result = get_task("nonexistent_task_xyz")
    assert result is None


def test_task_with_default_config() -> None:
    """Test task with default configuration values."""

    @task(name="test_defaults")
    def default_task() -> str:
        return "default"

    registered_task = get_task("test_defaults")
    assert registered_task is not None
    assert registered_task.config.queue == "default"
    assert registered_task.config.max_retries == 3
    assert registered_task.config.timeout is None  # Default is None, not 300
    assert registered_task.config.priority == 5
