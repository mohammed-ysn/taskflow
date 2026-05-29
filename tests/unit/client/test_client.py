"""Tests for TaskflowClient."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from taskflow.broker.base import BaseBroker
from taskflow.client import TaskflowClient


def _make_broker(**kwargs: Any) -> MagicMock:
    broker = MagicMock(spec=BaseBroker)
    broker.connect = AsyncMock()
    broker.disconnect = AsyncMock()
    broker.send_task = AsyncMock()
    broker.get_result = AsyncMock(return_value=None)
    for k, v in kwargs.items():
        setattr(broker, k, v)
    return broker


@pytest.mark.asyncio
async def test_context_manager_connects_and_disconnects() -> None:
    broker = _make_broker()
    async with TaskflowClient(broker=broker):
        broker.connect.assert_awaited_once()
    broker.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_submit_returns_task_id() -> None:
    broker = _make_broker()
    async with TaskflowClient(broker=broker) as client:
        task_id = await client.submit("add", args=(1, 2))

    assert isinstance(task_id, str)
    assert len(task_id) == 36  # UUID
    broker.send_task.assert_awaited_once()
    call_kwargs = broker.send_task.call_args.kwargs
    assert call_kwargs["task_name"] == "add"
    assert call_kwargs["args"] == (1, 2)


@pytest.mark.asyncio
async def test_submit_passes_queue_and_priority() -> None:
    broker = _make_broker()
    async with TaskflowClient(broker=broker) as client:
        await client.submit("job", queue="high", priority=9)

    call_kwargs = broker.send_task.call_args.kwargs
    assert call_kwargs["queue"] == "high"
    assert call_kwargs["priority"] == 9


@pytest.mark.asyncio
async def test_result_returns_none_when_not_ready() -> None:
    broker = _make_broker(get_result=AsyncMock(return_value=None))
    async with TaskflowClient(broker=broker) as client:
        result = await client.result("some-id")
    assert result is None


@pytest.mark.asyncio
async def test_result_returns_payload_when_ready() -> None:
    payload = {"status": "success", "result": 42, "completed_at": 1.0}
    broker = _make_broker(get_result=AsyncMock(return_value=payload))
    async with TaskflowClient(broker=broker) as client:
        result = await client.result("some-id")
    assert result == payload


@pytest.mark.asyncio
async def test_wait_polls_until_result_ready() -> None:
    payload = {"status": "success", "result": 99, "completed_at": 1.0}
    # First two calls return None, third returns result
    broker = _make_broker(
        get_result=AsyncMock(side_effect=[None, None, payload]),
    )
    async with TaskflowClient(broker=broker) as client:
        result = await client.wait("some-id", poll_interval=0.01)
    assert result == payload
    assert broker.get_result.await_count == 3


@pytest.mark.asyncio
async def test_wait_raises_on_timeout() -> None:
    broker = _make_broker(get_result=AsyncMock(return_value=None))
    async with TaskflowClient(broker=broker) as client:
        with pytest.raises(asyncio.TimeoutError):
            await client.wait("some-id", deadline=0.05, poll_interval=0.01)
