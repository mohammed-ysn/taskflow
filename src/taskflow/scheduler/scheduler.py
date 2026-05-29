"""Task scheduler for delayed and periodic execution."""

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from taskflow.core.exceptions import SchedulingError

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    DELAYED = "delayed"
    PERIODIC = "periodic"
    CRON = "cron"


@dataclass
class ScheduledTask:
    task_id: str
    task_name: str
    schedule_type: ScheduleType
    trigger_time: datetime | None = None
    cron_expression: str | None = None
    interval_seconds: float | None = None
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    max_instances: int = 1
    misfire_grace_time: int = 30

    def __post_init__(self) -> None:
        if self.schedule_type == ScheduleType.DELAYED and not self.trigger_time:
            raise SchedulingError("Delayed tasks require trigger_time")
        if self.schedule_type == ScheduleType.CRON and not self.cron_expression:
            raise SchedulingError("Cron tasks require cron_expression")
        if self.schedule_type == ScheduleType.PERIODIC and not self.interval_seconds:
            raise SchedulingError("Periodic tasks require interval_seconds")


class TaskScheduler:
    def __init__(self) -> None:
        self.scheduler = AsyncIOScheduler()
        self._scheduled_tasks: dict[str, ScheduledTask] = {}
        self._task_executors: dict[str, Any] = {}
        self._started = False

    async def start(self) -> None:
        if not self._started:
            self.scheduler.start()
            self._started = True

    async def shutdown(self) -> None:
        if self._started:
            self.scheduler.shutdown(wait=True)
            self._started = False

    def schedule_delayed_task(
        self,
        task_id: str,
        task_name: str,
        delay_seconds: float,
        task_executor: Any,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> ScheduledTask:
        trigger_time = datetime.now(UTC) + timedelta(seconds=delay_seconds)
        scheduled_task = ScheduledTask(
            task_id=task_id,
            task_name=task_name,
            schedule_type=ScheduleType.DELAYED,
            trigger_time=trigger_time,
            args=args,
            kwargs=kwargs or {},
        )
        self._task_executors[task_id] = task_executor
        self.scheduler.add_job(
            func=self._execute_task,
            trigger=DateTrigger(run_date=trigger_time),
            args=[task_id],
            id=task_id,
            name=task_name,
            misfire_grace_time=scheduled_task.misfire_grace_time,
        )
        self._scheduled_tasks[task_id] = scheduled_task
        return scheduled_task

    def schedule_periodic_task(
        self,
        task_id: str,
        task_name: str,
        interval_seconds: float,
        task_executor: Any,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        start_immediately: bool = False,
    ) -> ScheduledTask:
        scheduled_task = ScheduledTask(
            task_id=task_id,
            task_name=task_name,
            schedule_type=ScheduleType.PERIODIC,
            interval_seconds=interval_seconds,
            args=args,
            kwargs=kwargs or {},
        )
        self._task_executors[task_id] = task_executor
        start_date = None if start_immediately else datetime.now(UTC)
        self.scheduler.add_job(
            func=self._execute_task,
            trigger=IntervalTrigger(seconds=interval_seconds, start_date=start_date),
            args=[task_id],
            id=task_id,
            name=task_name,
            max_instances=scheduled_task.max_instances,
            misfire_grace_time=scheduled_task.misfire_grace_time,
        )
        self._scheduled_tasks[task_id] = scheduled_task
        return scheduled_task

    def schedule_cron_task(
        self,
        task_id: str,
        task_name: str,
        cron_expression: str,
        task_executor: Any,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> ScheduledTask:
        _CRON_PARTS = 5  # noqa: N806
        if len(cron_expression.split()) != _CRON_PARTS:
            raise SchedulingError(
                f"Invalid cron expression: {cron_expression}. "
                "Expected format: 'minute hour day month day_of_week'",
            )
        scheduled_task = ScheduledTask(
            task_id=task_id,
            task_name=task_name,
            schedule_type=ScheduleType.CRON,
            cron_expression=cron_expression,
            args=args,
            kwargs=kwargs or {},
        )
        self._task_executors[task_id] = task_executor
        self.scheduler.add_job(
            func=self._execute_task,
            trigger=CronTrigger.from_crontab(cron_expression),
            args=[task_id],
            id=task_id,
            name=task_name,
            max_instances=scheduled_task.max_instances,
            misfire_grace_time=scheduled_task.misfire_grace_time,
        )
        self._scheduled_tasks[task_id] = scheduled_task
        return scheduled_task

    async def _execute_task(self, task_id: str) -> Any:
        if task_id not in self._scheduled_tasks:
            # Task was cancelled between scheduling and execution — ignore
            logger.debug("Skipping execution of cancelled task %s", task_id)
            return None

        task = self._scheduled_tasks[task_id]
        executor = self._task_executors.get(task_id)
        if not executor:
            raise SchedulingError(f"No executor found for task {task_id}")

        if asyncio.iscoroutinefunction(executor):
            result = await executor(*task.args, **task.kwargs)
        else:
            result = executor(*task.args, **task.kwargs)

        if task.schedule_type == ScheduleType.DELAYED:
            self.cancel_task(task_id)

        return result

    def cancel_task(self, task_id: str) -> bool:
        if task_id in self._scheduled_tasks:
            with contextlib.suppress(JobLookupError):
                self.scheduler.remove_job(task_id)
            del self._scheduled_tasks[task_id]
            self._task_executors.pop(task_id, None)
            return True
        return False

    def get_scheduled_task(self, task_id: str) -> ScheduledTask | None:
        return self._scheduled_tasks.get(task_id)

    def list_scheduled_tasks(self) -> list[ScheduledTask]:
        return list(self._scheduled_tasks.values())

    def pause_task(self, task_id: str) -> bool:
        if task_id in self._scheduled_tasks:
            self.scheduler.pause_job(task_id)
            return True
        return False

    def resume_task(self, task_id: str) -> bool:
        if task_id in self._scheduled_tasks:
            self.scheduler.resume_job(task_id)
            return True
        return False

    def get_next_run_time(self, task_id: str) -> datetime | None:
        job = self.scheduler.get_job(task_id)
        return job.next_run_time if job else None
