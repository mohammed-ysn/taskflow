"""Tests for DAG execution engine."""

from __future__ import annotations

import asyncio

import pytest

from taskflow.dag.dag import DAG, CyclicDependencyError


def test_linear_dag_executes_in_order() -> None:
    order: list[str] = []

    dag = DAG("linear")
    a = dag.task("a", lambda: order.append("a") or "a")
    b = dag.task("b", lambda: order.append("b") or "b").after(a)
    dag.task("c", lambda: order.append("c") or "c").after(b)

    results = asyncio.run(dag.run())

    assert order == ["a", "b", "c"]
    assert results["a"] == "a"
    assert results["b"] == "b"
    assert results["c"] == "c"


def test_fan_out_runs_branches_independently() -> None:
    dag = DAG("fan_out")
    root = dag.task("root", lambda: "root")
    dag.task("branch_a", lambda: "a").after(root)
    dag.task("branch_b", lambda: "b").after(root)

    results = asyncio.run(dag.run())

    assert results["root"] == "root"
    assert results["branch_a"] == "a"
    assert results["branch_b"] == "b"


def test_fan_in_waits_for_all_deps() -> None:
    dag = DAG("fan_in")
    a = dag.task("a", lambda: "a")
    b = dag.task("b", lambda: "b")
    dag.task("merge", lambda: "merged").after(a, b)

    results = asyncio.run(dag.run())

    assert results["merge"] == "merged"


def test_async_tasks_execute() -> None:
    async def async_task() -> str:
        await asyncio.sleep(0.01)
        return "async_result"

    dag = DAG("async")
    dag.task("t", async_task)

    results = asyncio.run(dag.run())
    assert results["t"] == "async_result"


def test_cycle_raises() -> None:
    dag = DAG("cycle")
    a = dag.task("a", lambda: None)
    b = dag.task("b", lambda: None).after(a)
    a.after(b)  # creates cycle

    with pytest.raises(CyclicDependencyError):
        asyncio.run(dag.run())


def test_failed_task_raises() -> None:
    def boom() -> None:
        raise ValueError("exploded")

    dag = DAG("fail")
    dag.task("t", boom)

    with pytest.raises(ValueError, match="exploded"):
        asyncio.run(dag.run())


def test_independent_tasks_run_concurrently() -> None:
    import time

    async def slow() -> str:
        await asyncio.sleep(0.1)
        return "done"

    dag = DAG("concurrent")
    dag.task("a", slow)
    dag.task("b", slow)
    dag.task("c", slow)

    start = time.perf_counter()
    asyncio.run(dag.run())
    elapsed = time.perf_counter() - start

    # 3 x 0.1s tasks in parallel should take ~0.1s, not ~0.3s
    assert elapsed < 0.25
