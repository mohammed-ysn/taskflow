"""Abstract broker interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseBroker(ABC):
    """Abstract base class for message brokers."""

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def send_task(
        self,
        task_name: str,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        queue: str = "default",
        priority: int = 5,
        retries: int = 0,
    ) -> None: ...

    @abstractmethod
    async def receive_task(self, queue: str = "default") -> dict[str, Any] | None: ...

    @abstractmethod
    async def ack_task(self, task_id: str) -> None: ...

    @abstractmethod
    async def nack_task(self, task_id: str, *, requeue: bool = True) -> None: ...

    @abstractmethod
    async def dead_letter(self, task_id: str, task_data: dict[str, Any]) -> None: ...

    @abstractmethod
    async def get_dlq(self) -> dict[str, dict[str, Any]]: ...

    @abstractmethod
    async def store_result(
        self,
        task_id: str,
        result: dict[str, Any],
        ttl: int | None = None,
    ) -> None: ...

    @abstractmethod
    async def get_result(self, task_id: str) -> dict[str, Any] | None: ...
