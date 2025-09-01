"""Tests for task scheduler."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from taskflow.core.exceptions import SchedulingError
from taskflow.scheduler.scheduler import (
    ScheduledTask,
    ScheduleType,
    TaskScheduler,
)


class TestScheduledTask:
    """Test ScheduledTask model."""

    def test_delayed_task_validation_success(self) -> None:
        """Test delayed task with valid configuration."""
        ScheduledTask(
            task_id="test-1",
            task_name="test_task",
            schedule_type=ScheduleType.DELAYED,
            trigger_time=datetime.now(UTC) + timedelta(seconds=10),
            args=("arg1",),
            kwargs={"key": "value"},
        )
        # Validation happens in __post_init__, no explicit validate() call needed

    def test_delayed_task_missing_trigger_time(self) -> None:
        """Test delayed task without trigger time raises error."""
        with pytest.raises(SchedulingError, match="Delayed tasks require trigger_time"):
            ScheduledTask(
                task_id="test-1",
                task_name="test_task",
                schedule_type=ScheduleType.DELAYED,
            )

    def test_cron_task_validation_success(self) -> None:
        """Test cron task with valid configuration."""
        ScheduledTask(
            task_id="test-2",
            task_name="test_task",
            schedule_type=ScheduleType.CRON,
            cron_expression="*/5 * * * *",
        )
        # Validation happens in __post_init__, no explicit validate() call needed

    def test_cron_task_missing_expression(self) -> None:
        """Test cron task without expression raises error."""
        with pytest.raises(SchedulingError, match="Cron tasks require cron_expression"):
            ScheduledTask(
                task_id="test-2",
                task_name="test_task",
                schedule_type=ScheduleType.CRON,
            )

    def test_periodic_task_validation_success(self) -> None:
        """Test periodic task with valid configuration."""
        ScheduledTask(
            task_id="test-3",
            task_name="test_task",
            schedule_type=ScheduleType.PERIODIC,
            interval_seconds=60,
        )
        # Validation happens in __post_init__, no explicit validate() call needed

    def test_periodic_task_missing_interval(self) -> None:
        """Test periodic task without interval raises error."""
        with pytest.raises(
            SchedulingError,
            match="Periodic tasks require interval_seconds",
        ):
            ScheduledTask(
                task_id="test-3",
                task_name="test_task",
                schedule_type=ScheduleType.PERIODIC,
            )


class TestTaskScheduler:
    """Test TaskScheduler functionality."""

    @pytest.fixture
    def scheduler(self) -> TaskScheduler:
        """Create scheduler instance."""
        return TaskScheduler()

    @pytest.fixture
    def mock_task_executor(self) -> MagicMock:
        """Create mock task executor."""
        return MagicMock()

    @pytest.mark.asyncio
    async def test_scheduler_start_shutdown(self, scheduler: TaskScheduler) -> None:
        """Test scheduler lifecycle."""
        # Initially not started
        assert scheduler._started is False

        # Start scheduler
        await scheduler.start()
        assert scheduler._started is True
        assert scheduler.scheduler.running

        # Shutdown scheduler
        await scheduler.shutdown()
        assert scheduler._started is False
        # Note: APScheduler may not immediately set running to False after shutdown

    def test_schedule_delayed_task(
        self,
        scheduler: TaskScheduler,
        mock_task_executor: MagicMock,
    ) -> None:
        """Test scheduling a delayed task."""
        with patch.object(scheduler.scheduler, "add_job") as mock_add_job:
            result = scheduler.schedule_delayed_task(
                task_id="delayed-1",
                task_name="test_task",
                delay_seconds=10,
                task_executor=mock_task_executor,
                args=("arg1",),
                kwargs={"key": "value"},
            )

            assert result.task_id == "delayed-1"
            assert result.task_name == "test_task"
            assert result.schedule_type == ScheduleType.DELAYED
            assert "delayed-1" in scheduler._scheduled_tasks
            assert scheduler._task_executors["delayed-1"] == mock_task_executor
            mock_add_job.assert_called_once()

    def test_schedule_periodic_task(
        self,
        scheduler: TaskScheduler,
        mock_task_executor: MagicMock,
    ) -> None:
        """Test scheduling a periodic task."""
        with patch.object(scheduler.scheduler, "add_job") as mock_add_job:
            result = scheduler.schedule_periodic_task(
                task_id="periodic-1",
                task_name="test_task",
                interval_seconds=30,
                task_executor=mock_task_executor,
                start_immediately=True,
            )

            assert result.task_id == "periodic-1"
            assert result.task_name == "test_task"
            assert result.schedule_type == ScheduleType.PERIODIC
            assert result.interval_seconds == 30
            assert "periodic-1" in scheduler._scheduled_tasks
            assert scheduler._task_executors["periodic-1"] == mock_task_executor
            mock_add_job.assert_called_once()

    def test_schedule_cron_task(
        self,
        scheduler: TaskScheduler,
        mock_task_executor: MagicMock,
    ) -> None:
        """Test scheduling a cron task."""
        with patch.object(scheduler.scheduler, "add_job") as mock_add_job:
            result = scheduler.schedule_cron_task(
                task_id="cron-1",
                task_name="test_task",
                cron_expression="*/5 * * * *",
                task_executor=mock_task_executor,
            )

            assert result.task_id == "cron-1"
            assert result.task_name == "test_task"
            assert result.schedule_type == ScheduleType.CRON
            assert result.cron_expression == "*/5 * * * *"
            assert "cron-1" in scheduler._scheduled_tasks
            assert scheduler._task_executors["cron-1"] == mock_task_executor
            mock_add_job.assert_called_once()

    def test_invalid_cron_expression(
        self,
        scheduler: TaskScheduler,
        mock_task_executor: MagicMock,
    ) -> None:
        """Test invalid cron expression raises error."""
        with pytest.raises(SchedulingError, match="Invalid cron expression"):
            scheduler.schedule_cron_task(
                task_id="invalid-cron",
                task_name="test_task",
                cron_expression="invalid",
                task_executor=mock_task_executor,
            )

    def test_cancel_scheduled_task(self, scheduler: TaskScheduler) -> None:
        """Test cancelling a scheduled task."""
        # Schedule a task first
        mock_executor = MagicMock()
        with patch.object(scheduler.scheduler, "add_job"):
            scheduler.schedule_delayed_task(
                task_id="cancel-1",
                task_name="test_task",
                delay_seconds=10,
                task_executor=mock_executor,
            )

        # Mock remove_job method
        with patch.object(scheduler.scheduler, "remove_job") as mock_remove:
            result = scheduler.cancel_task("cancel-1")

            assert result is True
            assert "cancel-1" not in scheduler._scheduled_tasks
            assert "cancel-1" not in scheduler._task_executors
            mock_remove.assert_called_once_with("cancel-1")

    def test_cancel_nonexistent_task(self, scheduler: TaskScheduler) -> None:
        """Test cancelling a task that doesn't exist."""
        with patch.object(scheduler.scheduler, "get_job", return_value=None):
            result = scheduler.cancel_task("nonexistent")
            assert result is False

    def test_list_scheduled_tasks(self, scheduler: TaskScheduler) -> None:
        """Test retrieving list of scheduled tasks."""
        # Schedule multiple tasks
        with patch.object(scheduler.scheduler, "add_job"):
            for i in range(3):
                scheduler.schedule_delayed_task(
                    task_id=f"task-{i}",
                    task_name="test_task",
                    delay_seconds=i + 1,
                    task_executor=MagicMock(),
                )

        # Get scheduled tasks
        scheduled = scheduler.list_scheduled_tasks()
        assert len(scheduled) == 3
        task_ids = [task.task_id for task in scheduled]
        assert all(f"task-{i}" in task_ids for i in range(3))

    @pytest.mark.asyncio
    async def test_task_execution(self, scheduler: TaskScheduler) -> None:
        """Test that scheduled task executes correctly."""
        mock_executor = MagicMock(return_value="result")

        # Schedule a task
        with patch.object(scheduler.scheduler, "add_job"):
            scheduler.schedule_delayed_task(
                task_id="exec-1",
                task_name="test_task",
                delay_seconds=1,
                task_executor=mock_executor,
                args=("arg1", "arg2"),
                kwargs={"key": "value"},
            )

        # Mock remove_job for the cleanup that happens after delayed task execution
        with patch.object(scheduler.scheduler, "remove_job"):
            # Execute the task directly
            result = await scheduler._execute_task("exec-1")

            # Verify executor was called with correct parameters
            mock_executor.assert_called_once_with(
                "arg1",
                "arg2",
                key="value",
            )
            assert result == "result"

    @pytest.mark.asyncio
    async def test_execute_nonexistent_task(self, scheduler: TaskScheduler) -> None:
        """Test executing a task that doesn't exist raises error."""
        with pytest.raises(SchedulingError, match="Task exec-404 not found"):
            await scheduler._execute_task("exec-404")

    def test_pause_resume_task(self, scheduler: TaskScheduler) -> None:
        """Test pausing and resuming a task."""
        mock_executor = MagicMock()

        # Schedule a task
        with patch.object(scheduler.scheduler, "add_job"):
            scheduler.schedule_periodic_task(
                task_id="pause-1",
                task_name="test_task",
                interval_seconds=30,
                task_executor=mock_executor,
            )

        # Test pause
        with patch.object(scheduler.scheduler, "pause_job") as mock_pause:
            result = scheduler.pause_task("pause-1")
            assert result is True
            mock_pause.assert_called_once_with("pause-1")

        # Test resume
        with patch.object(scheduler.scheduler, "resume_job") as mock_resume:
            result = scheduler.resume_task("pause-1")
            assert result is True
            mock_resume.assert_called_once_with("pause-1")

    def test_get_scheduled_task(self, scheduler: TaskScheduler) -> None:
        """Test retrieving a specific scheduled task."""
        # Schedule a task
        with patch.object(scheduler.scheduler, "add_job"):
            scheduled = scheduler.schedule_delayed_task(
                task_id="get-1",
                task_name="test_task",
                delay_seconds=10,
                task_executor=MagicMock(),
            )

        # Get the task
        task = scheduler.get_scheduled_task("get-1")
        assert task is not None
        assert task.task_id == "get-1"
        assert task == scheduled

        # Try to get non-existent task
        assert scheduler.get_scheduled_task("nonexistent") is None
