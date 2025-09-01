"""Abstract broker interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseBroker(ABC):
    """Abstract base class for message brokers."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to broker."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to broker."""
        ...

    @abstractmethod
    async def send_task(
        self,
        task_name: str,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        queue: str = "default",
        priority: int = 5,
    ) -> None:
        """Send task to queue.

        Args:
            task_name: Name of the task to execute
            task_id: Unique task identifier
            args: Task positional arguments
            kwargs: Task keyword arguments
            queue: Target queue name
            priority: Task priority
        """
        ...

    @abstractmethod
    async def receive_task(self, queue: str = "default") -> dict[str, Any] | None:
        """Receive task from queue.

        Args:
            queue: Queue name to receive from

        Returns:
            Task data or None if no task available
        """
        ...

    @abstractmethod
    async def ack_task(self, task_id: str) -> None:
        """Acknowledge task completion.

        Args:
            task_id: Task identifier to acknowledge
        """
        ...

    @abstractmethod
    async def nack_task(self, task_id: str, requeue: bool = True) -> None:
        """Negative acknowledge task (failed).

        Args:
            task_id: Task identifier to nack
            requeue: Whether to requeue the task
        """
        ...
