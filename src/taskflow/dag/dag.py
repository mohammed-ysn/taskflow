"""DAG execution engine."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import networkx as nx

if TYPE_CHECKING:
    from collections.abc import Callable


class CyclicDependencyError(Exception):
    pass


@dataclass
class DAGNode:
    node_id: str
    func: Callable[..., Any]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    upstream: list[str] = field(default_factory=list, init=False, repr=False)

    def after(self, *nodes: DAGNode) -> DAGNode:
        self.upstream.extend(n.node_id for n in nodes)
        return self


class DAG:
    def __init__(self, name: str) -> None:
        self.name = name
        self._graph: nx.DiGraph = nx.DiGraph()
        self._nodes: dict[str, DAGNode] = {}

    def task(
        self,
        node_id: str,
        func: Callable[..., Any],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> DAGNode:
        node = DAGNode(node_id=node_id, func=func, args=args, kwargs=kwargs or {})
        self._graph.add_node(node_id)
        self._nodes[node_id] = node
        return node

    async def run(self) -> dict[str, Any]:
        for node in self._nodes.values():
            for uid in node.upstream:
                self._graph.add_edge(uid, node.node_id)

        if not nx.is_directed_acyclic_graph(self._graph):
            raise CyclicDependencyError(f"DAG '{self.name}' contains a cycle")

        results: dict[str, Any] = {}
        pending = set(self._nodes)
        running: dict[str, asyncio.Task[Any]] = {}

        while pending or running:
            ready = {
                nid
                for nid in pending
                if all(p in results for p in self._graph.predecessors(nid))
            }
            for nid in ready:
                pending.discard(nid)
                running[nid] = asyncio.create_task(self._run_node(self._nodes[nid]))

            if not running:
                break

            done, _ = await asyncio.wait(
                running.values(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            task_to_nid = {t: nid for nid, t in running.items()}
            for done_task in done:
                nid = task_to_nid[done_task]
                del running[nid]
                results[nid] = done_task.result()  # re-raises on failure

        return results

    async def _run_node(self, node: DAGNode) -> Any:
        if inspect.iscoroutinefunction(node.func):
            return await node.func(*node.args, **node.kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: node.func(*node.args, **node.kwargs),
        )
