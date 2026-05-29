# Taskflow

A modern distributed task queue for Python, built with asyncio and Redis.

## Features

- Async-first design with asyncio
- Priority queues via Redis sorted sets
- Automatic retry on failure with configurable backoff
- Concurrent task processing
- Decorator-based task registration
- Rate limiting and circuit breaker middleware
- Delayed and periodic task scheduling

## Requirements

- Python 3.12+
- Redis

## Quick Start

```bash
git clone https://github.com/mohammed-ysn/taskflow.git
cd taskflow
uv sync
```

Start Redis:

```bash
task redis:up
```

### Define tasks

```python
from taskflow.core.task import task

@task(name="add")
def add(x: int, y: int) -> int:
    return x + y
```

### Submit tasks

```python
from taskflow.broker.redis_broker import RedisBroker

broker = RedisBroker()
await broker.connect()
await broker.send_task("add", task_id="task-1", args=(5, 3), kwargs={})
```

### Run a worker

```bash
uv run taskflow worker -q default -c 5 -I myapp.tasks
```

The `-I` flag imports modules before starting, registering their tasks.

### Example

```bash
# Terminal 1
task example:worker

# Terminal 2
task example:submit
```

## Development

```bash
uv sync --all-groups  # install all deps (editable)
task lint             # format + lint + type-check
task test             # run all tests
task ci               # lint + test (no auto-fix, mirrors CI)
```
