# Taskflow

A modern distributed task queue for Python, built with asyncio and Redis.

## Features

- Async-first design with asyncio
- Priority queues with Redis sorted sets
- Automatic task retry on failure
- Concurrent task processing
- Simple decorator-based API

## Quick Start

### Prerequisites

- Python 3.12+
- Redis (via Docker or local installation)

### Installation

```bash
# Clone the repository
git clone https://github.com/mohammed-ysn/taskflow.git
cd taskflow

# Install with UV
uv pip install -e .
```

### Running Redis

```bash
docker-compose up -d
```

### Basic Usage

1. Define tasks:

```python
from taskflow.core.task import task

@task(name="add")
def add(x: int, y: int) -> int:
    return x + y
```

2. Submit tasks:

```python
from taskflow.broker.redis_broker import RedisBroker

broker = RedisBroker()
await broker.connect()
await broker.send_task("add", task_id="task-1", args=(5, 3), kwargs={})
```

3. Start a worker:

```bash
uv run taskflow worker --queues default --concurrency 5
```

### Example

Run the included example:

```bash
# Start Redis
docker-compose up -d

# Submit example tasks
uv run python -m examples.submit_tasks

# In another terminal, start worker
uv run python -m examples.worker_with_tasks
```

## Development

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run linting
make lint

# Install pre-commit hooks
pre-commit install
```
