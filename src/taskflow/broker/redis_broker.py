"""Redis broker implementation."""

from __future__ import annotations

import json
import uuid
from typing import Any

import redis.asyncio as redis

from taskflow.broker.base import BaseBroker


class RedisBroker(BaseBroker):
    """Redis-backed message broker using sorted sets for priority queuing."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        decode_responses: bool = False,
    ) -> None:
        self.host = host
        self.port = port
        self.db = db
        self.decode_responses = decode_responses
        self._client: redis.Redis[bytes] | None = None

    def _ensure_connected(self) -> redis.Redis[bytes]:
        if not self._client:
            raise RuntimeError("Broker not connected")
        return self._client

    async def connect(self) -> None:
        self._client = redis.Redis(  # type: ignore[call-overload]
            host=self.host,
            port=self.port,
            db=self.db,
            decode_responses=self.decode_responses,
        )
        await self._client.ping()

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def send_task(
        self,
        task_name: str,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        queue: str = "default",
        priority: int = 5,
        retries: int = 0,
    ) -> None:
        client = self._ensure_connected()
        task_data = {
            "id": task_id or str(uuid.uuid4()),
            "name": task_name,
            "args": args,
            "kwargs": kwargs,
            "queue": queue,
            "priority": priority,
            "retries": retries,
        }
        score = 10 - priority  # lower score = higher priority in BZPOPMIN
        await client.zadd(f"taskflow:queue:{queue}", {json.dumps(task_data): score})

    async def receive_task(self, queue: str = "default") -> dict[str, Any] | None:
        client = self._ensure_connected()
        result = await client.bzpopmin(f"taskflow:queue:{queue}", timeout=1)
        if result is None:
            return None

        _, task_json, _ = result
        if isinstance(task_json, bytes):
            task_json_str = task_json.decode()
        elif isinstance(task_json, str):
            task_json_str = task_json
        else:
            raise TypeError(f"Unexpected type from Redis: {type(task_json)}")
        task_data: dict[str, Any] = json.loads(task_json_str)
        await client.hset(
            f"taskflow:processing:{queue}",
            task_data["id"],
            task_json_str,
        )
        return task_data

    async def ack_task(self, task_id: str) -> None:
        client = self._ensure_connected()
        for key in await client.keys("taskflow:processing:*"):
            await client.hdel(key, task_id)

    async def nack_task(self, task_id: str, *, requeue: bool = True) -> None:
        client = self._ensure_connected()
        for key in await client.keys("taskflow:processing:*"):
            task_json = await client.hget(key, task_id)
            if not task_json:
                continue

            key_str = key.decode() if isinstance(key, bytes) else key
            queue = key_str.split(":")[-1]
            task_data = json.loads(task_json)
            await client.hdel(key, task_id)

            if requeue:
                task_data["priority"] = max(0, task_data.get("priority", 5) - 1)
                task_data["retries"] = task_data.get("retries", 0) + 1
                await self.send_task(
                    task_name=task_data["name"],
                    task_id=task_data["id"],
                    args=task_data["args"],
                    kwargs=task_data["kwargs"],
                    queue=queue,
                    priority=task_data["priority"],
                    retries=task_data["retries"],
                )
            break
