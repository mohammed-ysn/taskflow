"""Tests for Redis broker implementation."""

from __future__ import annotations

import uuid

import pytest

from taskflow.broker.redis_broker import RedisBroker


@pytest.mark.asyncio
async def test_broker_connect_disconnect(redis_broker: RedisBroker) -> None:
    """Test broker connection and disconnection."""
    # Broker is already connected via fixture
    assert redis_broker._client is not None

    # Test disconnection
    await redis_broker.disconnect()
    assert redis_broker._client is None


@pytest.mark.asyncio
async def test_send_and_receive_task(redis_broker: RedisBroker) -> None:
    """Test sending and receiving a task."""
    task_id = str(uuid.uuid4())

    # Send a task
    await redis_broker.send_task(
        task_name="test_task",
        task_id=task_id,
        args=(1, 2),
        kwargs={"key": "value"},
        queue="test_queue",
        priority=7,
    )

    # Receive the task
    task_data = await redis_broker.receive_task(queue="test_queue")

    assert task_data is not None
    assert task_data["id"] == task_id
    assert task_data["name"] == "test_task"
    assert task_data["args"] == [1, 2]
    assert task_data["kwargs"] == {"key": "value"}
    assert task_data["queue"] == "test_queue"
    assert task_data["priority"] == 7


@pytest.mark.asyncio
async def test_task_priority_ordering(redis_broker: RedisBroker) -> None:
    """Test that tasks are received in priority order."""
    # Send tasks with different priorities
    await redis_broker.send_task(
        task_name="low_priority",
        task_id="task_low",
        args=(),
        kwargs={},
        queue="priority_test",
        priority=2,
    )

    await redis_broker.send_task(
        task_name="high_priority",
        task_id="task_high",
        args=(),
        kwargs={},
        queue="priority_test",
        priority=9,
    )

    await redis_broker.send_task(
        task_name="medium_priority",
        task_id="task_medium",
        args=(),
        kwargs={},
        queue="priority_test",
        priority=5,
    )

    # Receive tasks - should come in priority order (high to low)
    task1 = await redis_broker.receive_task(queue="priority_test")
    assert task1 is not None
    assert task1["name"] == "high_priority"

    task2 = await redis_broker.receive_task(queue="priority_test")
    assert task2 is not None
    assert task2["name"] == "medium_priority"

    task3 = await redis_broker.receive_task(queue="priority_test")
    assert task3 is not None
    assert task3["name"] == "low_priority"


@pytest.mark.asyncio
async def test_ack_task(redis_broker: RedisBroker) -> None:
    """Test acknowledging a task removes it from processing."""
    task_id = str(uuid.uuid4())

    # Send and receive a task
    await redis_broker.send_task(
        task_name="ack_test",
        task_id=task_id,
        args=(),
        kwargs={},
        queue="ack_queue",
    )

    task_data = await redis_broker.receive_task(queue="ack_queue")
    assert task_data is not None

    # Task should be in processing set
    if redis_broker._client:
        processing_key = "taskflow:processing:ack_queue"
        exists = await redis_broker._client.hexists(processing_key, task_id)
        assert exists

        # Acknowledge the task
        await redis_broker.ack_task(task_id)

        # Task should be removed from processing
        exists = await redis_broker._client.hexists(processing_key, task_id)
        assert not exists


@pytest.mark.asyncio
async def test_nack_task_with_requeue(redis_broker: RedisBroker) -> None:
    """Test nacking a task with requeue."""
    task_id = str(uuid.uuid4())

    # Send and receive a task
    await redis_broker.send_task(
        task_name="nack_test",
        task_id=task_id,
        args=(),
        kwargs={},
        queue="nack_queue",
        priority=5,
    )

    task_data = await redis_broker.receive_task(queue="nack_queue")
    assert task_data is not None

    # Nack with requeue
    await redis_broker.nack_task(task_id, requeue=True)

    # Task should be back in queue with lower priority
    requeued_task = await redis_broker.receive_task(queue="nack_queue")
    assert requeued_task is not None
    assert requeued_task["id"] == task_id
    assert requeued_task["priority"] == 4  # Priority decreased


@pytest.mark.asyncio
async def test_nack_task_without_requeue(redis_broker: RedisBroker) -> None:
    """Test nacking a task without requeue."""
    task_id = str(uuid.uuid4())

    # Send and receive a task
    await redis_broker.send_task(
        task_name="nack_no_requeue",
        task_id=task_id,
        args=(),
        kwargs={},
        queue="nack_no_requeue",
    )

    task_data = await redis_broker.receive_task(queue="nack_no_requeue")
    assert task_data is not None

    # Nack without requeue
    await redis_broker.nack_task(task_id, requeue=False)

    # Task should not be in queue
    no_task = await redis_broker.receive_task(queue="nack_no_requeue")
    assert no_task is None


@pytest.mark.asyncio
async def test_receive_from_empty_queue(redis_broker: RedisBroker) -> None:
    """Test receiving from an empty queue returns None."""
    result = await redis_broker.receive_task(queue="empty_queue")
    assert result is None


@pytest.mark.asyncio
async def test_multiple_queues(redis_broker: RedisBroker) -> None:
    """Test tasks are isolated to their respective queues."""
    # Send tasks to different queues
    await redis_broker.send_task(
        task_name="queue1_task",
        task_id="q1_task",
        args=(),
        kwargs={},
        queue="queue1",
    )

    await redis_broker.send_task(
        task_name="queue2_task",
        task_id="q2_task",
        args=(),
        kwargs={},
        queue="queue2",
    )

    # Receive from queue1
    task1 = await redis_broker.receive_task(queue="queue1")
    assert task1 is not None
    assert task1["name"] == "queue1_task"

    # Receive from queue2
    task2 = await redis_broker.receive_task(queue="queue2")
    assert task2 is not None
    assert task2["name"] == "queue2_task"

    # Both queues should now be empty
    assert await redis_broker.receive_task(queue="queue1") is None
    assert await redis_broker.receive_task(queue="queue2") is None
