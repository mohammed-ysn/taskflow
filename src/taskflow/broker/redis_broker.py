"""Redis broker implementation for task queue."""

from __future__ import annotations

import json
import uuid
from typing import Any

import redis.asyncio as redis

from taskflow.broker.base import BaseBroker


class RedisBroker(BaseBroker):
    """Redis implementation of the message broker."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        decode_responses: bool = False,
    ) -> None:
        """Initialise Redis broker.

        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            decode_responses: Whether to decode responses to strings
        """
        self.host = host
        self.port = port
        self.db = db
        self.decode_responses = decode_responses
        self._client: redis.Redis | None = None

    async def connect(self) -> None:
        """Establish connection to Redis."""
        self._client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            decode_responses=self.decode_responses,
        )
        # Test connection
        await self._client.ping()

    async def disconnect(self) -> None:
        """Close connection to Redis."""
        if self._client:
            await self._client.close()
            self._client = None

    async def send_task(
        self,
        task_name: str,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        queue: str = "default",
        priority: int = 5,
    ) -> None:
        """Send task to Redis queue.

        Uses Redis sorted sets for priority queue implementation.
        Lower scores (priority) are processed first.
        """
        if not self._client:
            raise RuntimeError("Broker not connected")

        task_data = {
            "id": task_id or str(uuid.uuid4()),
            "name": task_name,
            "args": args,
            "kwargs": kwargs,
            "queue": queue,
            "priority": priority,
        }

        # Serialise task data
        task_json = json.dumps(task_data)

        # Use sorted set for priority queue (lower score = higher priority)
        queue_key = f"taskflow:queue:{queue}"
        score = 10 - priority  # Invert priority for Redis sorted set

        await self._client.zadd(queue_key, {task_json: score})

    async def receive_task(self, queue: str = "default") -> dict[str, Any] | None:
        """Receive task from Redis queue.

        Uses BZPOPMIN for blocking pop from sorted set.
        """
        if not self._client:
            raise RuntimeError("Broker not connected")

        queue_key = f"taskflow:queue:{queue}"

        # Blocking pop with 1 second timeout
        result = await self._client.bzpopmin(queue_key, timeout=1)

        if result is None:
            return None

        # result is (queue_key, task_json, score)
        _, task_json, _ = result

        # Decode if needed
        if isinstance(task_json, bytes):
            task_json = task_json.decode("utf-8")

        task_data: dict[str, Any] = json.loads(task_json)

        # Store task in processing set
        processing_key = f"taskflow:processing:{queue}"
        await self._client.hset(processing_key, task_data["id"], task_json)

        return task_data

    async def ack_task(self, task_id: str) -> None:
        """Acknowledge task completion."""
        if not self._client:
            raise RuntimeError("Broker not connected")

        # Remove from all processing sets
        keys = await self._client.keys("taskflow:processing:*")
        for key in keys:
            await self._client.hdel(key, task_id)

    async def nack_task(self, task_id: str, *, requeue: bool = True) -> None:
        """Negative acknowledge task (failed).

        Args:
            task_id: Task identifier to nack
            requeue: Whether to requeue the task
        """
        if not self._client:
            raise RuntimeError("Broker not connected")

        # Find task in processing sets
        keys = await self._client.keys("taskflow:processing:*")
        task_data = None
        queue = None

        for key in keys:
            task_json = await self._client.hget(key, task_id)
            if task_json:
                # Extract queue name from key
                queue = key.split(":")[-1]
                task_data = json.loads(task_json)
                await self._client.hdel(key, task_id)
                break

        if task_data and requeue and queue:
            # Requeue with lower priority
            task_data["priority"] = max(0, task_data.get("priority", 5) - 1)
            await self.send_task(
                task_name=task_data["name"],
                task_id=task_data["id"],
                args=task_data["args"],
                kwargs=task_data["kwargs"],
                queue=queue,
                priority=task_data["priority"],
            )
