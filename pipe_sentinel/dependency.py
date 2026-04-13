"""Pipeline dependency graph — detect ordering and circular dependencies."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipe_sentinel.config import PipelineConfig


@dataclass
class DependencyGraph:
    """Directed graph of pipeline dependencies."""
    edges: Dict[str, List[str]] = field(default_factory=dict)  # name -> depends_on

    def add(self, name: str, depends_on: List[str]) -> None:
        self.edges[name] = list(depends_on)

    def predecessors(self, name: str) -> List[str]:
        """Return the direct dependencies of *name*."""
        return self.edges.get(name, [])

    def all_names(self) -> Set[str]:
        return set(self.edges.keys())


@dataclass
class CycleError:
    cycle: List[str]

    def __str__(self) -> str:
        path = " -> ".join(self.cycle)
        return f"Circular dependency detected: {path}"


def build_graph(pipelines: List[PipelineConfig]) -> DependencyGraph:
    """Build a dependency graph from a list of pipeline configs."""
    graph = DependencyGraph()
    for p in pipelines:
        depends_on: List[str] = getattr(p, "depends_on", []) or []
        graph.add(p.name, depends_on)
    return graph


def _dfs(
    node: str,
    graph: DependencyGraph,
    visited: Set[str],
    stack: Set[str],
    path: List[str],
) -> Optional[List[str]]:
    visited.add(node)
    stack.add(node)
    path.append(node)

    for neighbour in graph.predecessors(node):
        if neighbour not in visited:
            result = _dfs(neighbour, graph, visited, stack, path)
            if result is not None:
                return result
        elif neighbour in stack:
            cycle_start = path.index(neighbour)
            return path[cycle_start:] + [neighbour]

    stack.discard(node)
    path.pop()
    return None


def find_cycle(graph: DependencyGraph) -> Optional[CycleError]:
    """Return a *CycleError* if the graph contains a cycle, else *None*."""
    visited: Set[str] = set()
    stack: Set[str] = set()

    for node in graph.all_names():
        if node not in visited:
            cycle = _dfs(node, graph, visited, stack, [])
            if cycle:
                return CycleError(cycle)
    return None


def topological_order(graph: DependencyGraph) -> Optional[List[str]]:
    """Return pipelines in dependency-safe execution order, or *None* on cycle."""
    if find_cycle(graph) is not None:
        return None

    in_degree: Dict[str, int] = {n: 0 for n in graph.all_names()}
    for node in graph.all_names():
        for dep in graph.predecessors(node):
            if dep in in_degree:
                in_degree[node] = in_degree.get(node, 0) + 1

    # Kahn's algorithm
    queue = [n for n, d in in_degree.items() if d == 0]
    order: List[str] = []
    while queue:
        queue.sort()  # deterministic output
        node = queue.pop(0)
        order.append(node)
        for candidate in graph.all_names():
            if node in graph.predecessors(candidate):
                in_degree[candidate] -= 1
                if in_degree[candidate] == 0:
                    queue.append(candidate)
    return order
