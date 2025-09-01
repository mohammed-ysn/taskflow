"""Task definition and registration system."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

_task_registry: dict[str, Task] = {}


@dataclass
class TaskConfig:
    """Configuration for a task."""

    max_retries: int = 3
    timeout: int | None = None
    priority: int = 5
    queue: str = "default"
    retry_backoff: float = 1.0


@dataclass
class Task:
    """Represents a registered task."""

    name: str
    func: Callable[..., Any]
    config: TaskConfig
    module: str
    signature: inspect.Signature = field(init=False)

    def __post_init__(self) -> None:
        """Initialise task signature."""
        self.signature = inspect.signature(self.func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the task function directly."""
        return self.func(*args, **kwargs)

    def delay(self, *args: Any, **kwargs: Any) -> str:
        """Submit task for async execution."""
        # TODO: Implement actual task submission to broker
        task_id = f"{self.name}:{id(args)}:{id(kwargs)}"
        return task_id


def task(
    name: str | None = None,
    max_retries: int = 3,
    timeout: int | None = None,
    priority: int = 5,
    queue: str = "default",
    retry_backoff: float = 1.0,
) -> Callable[[F], Task]:
    """Decorator to register a function as a task.

    Args:
        name: Optional task name (defaults to function name)
        max_retries: Maximum number of retry attempts
        timeout: Task execution timeout in seconds
        priority: Task priority (0-10, higher is more important)
        queue: Named queue for routing
        retry_backoff: Exponential backoff factor for retries

    Returns:
        Decorated task function
    """

    def decorator(func: F) -> Task:
        task_name = name or f"{func.__module__}.{func.__name__}"

        config = TaskConfig(
            max_retries=max_retries,
            timeout=timeout,
            priority=priority,
            queue=queue,
            retry_backoff=retry_backoff,
        )

        task_obj = Task(
            name=task_name,
            func=func,
            config=config,
            module=func.__module__,
        )

        # Register task globally
        register_task(task_obj)

        # Preserve function attributes
        functools.update_wrapper(task_obj, func)

        return task_obj

    return decorator


def register_task(task_obj: Task) -> None:
    """Register a task object."""
    if task_obj.name in _task_registry:
        msg = f"Task '{task_obj.name}' is already registered"
        raise ValueError(msg)
    _task_registry[task_obj.name] = task_obj


def get_task(name: str) -> Task | None:
    """Get a registered task by name."""
    return _task_registry.get(name)


def list_tasks() -> list[str]:
    """List all registered task names."""
    return list(_task_registry.keys())
