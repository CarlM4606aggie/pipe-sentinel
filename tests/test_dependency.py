"""Tests for pipe_sentinel.dependency."""
import pytest

from pipe_sentinel.dependency import (
    DependencyGraph,
    CycleError,
    build_graph,
    find_cycle,
    topological_order,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pipeline(name: str, depends_on=None):
    """Minimal stand-in for PipelineConfig."""
    class _P:
        pass
    p = _P()
    p.name = name
    p.depends_on = depends_on or []
    return p


# ---------------------------------------------------------------------------
# DependencyGraph
# ---------------------------------------------------------------------------

def test_add_and_predecessors():
    g = DependencyGraph()
    g.add("b", ["a"])
    assert g.predecessors("b") == ["a"]


def test_predecessors_missing_returns_empty():
    g = DependencyGraph()
    assert g.predecessors("ghost") == []


def test_all_names():
    g = DependencyGraph()
    g.add("a", [])
    g.add("b", ["a"])
    assert g.all_names() == {"a", "b"}


# ---------------------------------------------------------------------------
# build_graph
# ---------------------------------------------------------------------------

def test_build_graph_creates_edges():
    pipelines = [
        _make_pipeline("extract"),
        _make_pipeline("transform", ["extract"]),
        _make_pipeline("load", ["transform"]),
    ]
    g = build_graph(pipelines)
    assert g.predecessors("transform") == ["extract"]
    assert g.predecessors("load") == ["transform"]


def test_build_graph_no_depends_on_attr():
    """Pipelines without depends_on should default to empty list."""
    p = _make_pipeline("solo")
    del p.depends_on  # simulate missing attribute
    g = build_graph([p])
    assert g.predecessors("solo") == []


# ---------------------------------------------------------------------------
# find_cycle
# ---------------------------------------------------------------------------

def test_find_cycle_no_cycle():
    g = DependencyGraph()
    g.add("a", [])
    g.add("b", ["a"])
    assert find_cycle(g) is None


def test_find_cycle_direct_cycle():
    g = DependencyGraph()
    g.add("a", ["b"])
    g.add("b", ["a"])
    result = find_cycle(g)
    assert isinstance(result, CycleError)
    assert len(result.cycle) >= 2


def test_find_cycle_self_loop():
    g = DependencyGraph()
    g.add("a", ["a"])
    result = find_cycle(g)
    assert isinstance(result, CycleError)


def test_cycle_error_str():
    err = CycleError(["a", "b", "a"])
    assert "a" in str(err)
    assert "Circular" in str(err)


# ---------------------------------------------------------------------------
# topological_order
# ---------------------------------------------------------------------------

def test_topological_order_simple():
    g = DependencyGraph()
    g.add("a", [])
    g.add("b", ["a"])
    g.add("c", ["b"])
    order = topological_order(g)
    assert order is not None
    assert order.index("a") < order.index("b") < order.index("c")


def test_topological_order_returns_none_on_cycle():
    g = DependencyGraph()
    g.add("x", ["y"])
    g.add("y", ["x"])
    assert topological_order(g) is None


def test_topological_order_independent_pipelines():
    g = DependencyGraph()
    g.add("p1", [])
    g.add("p2", [])
    order = topological_order(g)
    assert order is not None
    assert set(order) == {"p1", "p2"}
