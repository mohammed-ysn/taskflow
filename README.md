# Taskflow

Async distributed task queue for Python, backed by Redis.

## Features

- Priority queues via Redis sorted sets
- Automatic retry with configurable max attempts
- Per-task timeout enforcement
- Dead letter queue for exhausted tasks
- Concurrent async + sync task execution
- Rate limiting (sliding window, token bucket, leaky bucket)
- Circuit breaker middleware
- Delayed, periodic, and cron scheduling
- TLS and password auth for Redis

## Requirements

- Python 3.12+
- Redis

## Quick Start

```bash
git clone https://github.com/mohammed-ysn/taskflow.git
cd taskflow
uv sync
task redis:up
```

### Define tasks

```python
from taskflow.core.task import task

@task(name="add", max_retries=3, timeout=30)
def add(x: int, y: int) -> int:
    return x + y

@task(name="fetch", queue="high")
async def fetch(url: str) -> str:
    ...
```

### Submit tasks

```python
from taskflow.broker.redis_broker import RedisBroker

broker = RedisBroker()
await broker.connect()
await broker.send_task(
    task_name="add",
    task_id="task-1",
    args=(5, 3),
    kwargs={},
    queue="default",
    priority=5,
)
```

### Run a worker

```bash
# -I imports your module, registering its @task functions
uv run taskflow worker -q default -c 10 -I myapp.tasks
```

### Inspect the dead letter queue

```python
entries = await broker.get_dlq()
for task_id, data in entries.items():
    print(task_id, data["name"], data["retries"])
```

## Examples

Self-contained examples in `examples/` cover priorities, retries, timeouts, middleware, and scheduling. Run `task --list` to see all available commands.

## Development

```bash
uv sync --all-groups  # install all deps (editable)
task lint             # format + lint + type-check
task test             # run all tests
task ci               # lint + test without auto-fix (mirrors CI)
task redis:flush      # clear queues and DLQ
```
