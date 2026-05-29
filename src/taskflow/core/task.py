"""Task definition and registration system."""

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

_task_registry: dict[str, Task] = {}


@dataclass
class TaskConfig:
    max_retries: int = 3
    timeout: int | None = None
    priority: int = 5
    queue: str = "default"
    retry_backoff: float = 1.0


@dataclass
class Task:
    name: str
    func: Callable[..., Any]
    config: TaskConfig
    module: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)


def task(
    name: str | None = None,
    max_retries: int = 3,
    timeout: int | None = None,
    priority: int = 5,
    queue: str = "default",
    retry_backoff: float = 1.0,
) -> Callable[[F], Task]:
    """Decorator to register a function as a task."""

    def decorator(func: F) -> Task:
        fn_module = getattr(func, "__module__", "")
        fn_name = getattr(func, "__name__", "")
        task_name = name or f"{fn_module}.{fn_name}"

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
            module=fn_module,
        )
        register_task(task_obj)
        functools.update_wrapper(task_obj, func)
        return task_obj

    return decorator


def register_task(task_obj: Task) -> None:
    if task_obj.name in _task_registry:
        raise ValueError(f"Task '{task_obj.name}' is already registered")
    _task_registry[task_obj.name] = task_obj


def get_task(name: str) -> Task | None:
    return _task_registry.get(name)


def list_tasks() -> list[str]:
    return list(_task_registry.keys())
