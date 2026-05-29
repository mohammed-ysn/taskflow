"""Tests for worker task execution and retry logic."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from taskflow.broker.base import BaseBroker
from taskflow.core.task import Task, TaskConfig
from taskflow.worker.executor import Worker


def _make_worker() -> tuple[Worker, AsyncMock, AsyncMock, AsyncMock, AsyncMock]:
    ack_task = AsyncMock()
    nack_task = AsyncMock()
    dead_letter = AsyncMock()
    store_result = AsyncMock()
    broker = MagicMock(spec=BaseBroker)
    broker.ack_task = ack_task
    broker.nack_task = nack_task
    broker.dead_letter = dead_letter
    broker.store_result = store_result
    return Worker(broker=broker), ack_task, nack_task, dead_letter, store_result


def _make_task(max_retries: int = 3) -> MagicMock:
    config = TaskConfig(max_retries=max_retries)
    task = MagicMock(spec=Task)
    task.config = config
    task.func = AsyncMock(return_value="ok")
    return task


def _task_data(name: str = "my_task", retries: int = 0) -> dict[str, Any]:
    return {
        "id": "test-id",
        "name": name,
        "args": (),
        "kwargs": {},
        "retries": retries,
    }


@pytest.mark.asyncio
async def test_successful_task_stores_result_then_acks() -> None:
    worker, ack_task, nack_task, _dl, store_result = _make_worker()

    with patch("taskflow.worker.executor.get_task", return_value=_make_task()):
        await worker._process_task(_task_data())

    store_result.assert_awaited_once()
    call_args = store_result.call_args
    assert call_args.args[0] == "test-id"
    assert call_args.args[1]["status"] == "success"
    assert call_args.args[1]["result"] == "ok"
    ack_task.assert_awaited_once_with("test-id")
    nack_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_task_requeued_when_retries_remaining() -> None:
    worker, ack_task, nack_task, _dl, store_result = _make_worker()
    task = _make_task(max_retries=3)
    task.func = AsyncMock(side_effect=ValueError("boom"))

    with patch("taskflow.worker.executor.get_task", return_value=task):
        await worker._process_task(_task_data(retries=1))

    nack_task.assert_awaited_once_with("test-id", requeue=True)
    ack_task.assert_not_awaited()
    store_result.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_task_stores_failure_result_when_exhausted() -> None:
    worker, ack_task, nack_task, dead_letter, store_result = _make_worker()
    task = _make_task(max_retries=3)
    task.func = AsyncMock(side_effect=ValueError("boom"))

    with patch("taskflow.worker.executor.get_task", return_value=task):
        await worker._process_task(_task_data(retries=3))

    store_result.assert_awaited_once()
    assert store_result.call_args.args[1]["status"] == "failure"
    assert "boom" in store_result.call_args.args[1]["error"]
    dead_letter.assert_awaited_once()
    ack_task.assert_awaited_once_with("test-id")
    nack_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_task_runs_in_executor() -> None:
    worker, ack_task, _, _dl, _sr = _make_worker()
    task = _make_task()
    task.func = MagicMock(return_value="sync_result")

    with patch("taskflow.worker.executor.get_task", return_value=task):
        await worker._process_task(_task_data())

    task.func.assert_called_once_with()
    ack_task.assert_awaited_once_with("test-id")


@pytest.mark.asyncio
async def test_task_timeout_counts_as_failure() -> None:
    worker, ack_task, nack_task, _dl, _sr = _make_worker()
    task = _make_task(max_retries=3)
    task.config.timeout = 0.01
    task.func = AsyncMock(side_effect=asyncio.TimeoutError)

    with patch("taskflow.worker.executor.get_task", return_value=task):
        await worker._process_task(_task_data(retries=0))

    nack_task.assert_awaited_once_with("test-id", requeue=True)
    ack_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_task_is_dropped_not_requeued() -> None:
    worker, ack_task, nack_task, _dl, _sr = _make_worker()

    with patch("taskflow.worker.executor.get_task", return_value=None):
        await worker._process_task(_task_data(name="ghost_task"))

    ack_task.assert_awaited_once_with("test-id")
    nack_task.assert_not_awaited()
