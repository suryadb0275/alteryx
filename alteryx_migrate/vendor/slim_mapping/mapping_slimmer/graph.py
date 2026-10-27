"""The workflow graph: nodes, edges and a deterministic dependency order."""
from __future__ import annotations

import heapq
from collections import defaultdict
from typing import NamedTuple

from .config import NO_DATA_NODES


class Edge(NamedTuple):
    source: int
    target: int
    origin: str | None         # anchor on the source tool (e.g. "Output2")
    destination: str | None    # anchor on the target tool
    index: int                 # position in the file


class Workflow:
    def __init__(self, document: dict):
        ir = document.get("ir", {})
        self.nodes: dict[int, dict] = {int(n["node_id"]): n for n in ir.get("nodes", [])}
        self.edges: list[Edge] = []
        self.preds: dict[int, list[Edge]] = defaultdict(list)
        self.succs: dict[int, list[Edge]] = defaultdict(list)
        for position, raw in enumerate(ir.get("edges", [])):
            source, target = int(raw["source"]), int(raw["target"])
            if source in self.nodes and target in self.nodes:
                edge = Edge(source, target, raw.get("origin_anchor"), raw.get("destination_anchor"), position)
                self.edges.append(edge)
                self.preds[target].append(edge)
                self.succs[source].append(edge)
        self.order = self._dependency_order()

    def _dependency_order(self) -> list[int]:
        """Kahn's algorithm, lowest tool id first, so the order is stable. Cycles are appended, never dropped."""
        waiting = {n: len({e.source for e in self.preds[n]}) for n in self.nodes}
        ready = [n for n, count in waiting.items() if count == 0]
        heapq.heapify(ready)
        order = []
        while ready:
            node = heapq.heappop(ready)
            order.append(node)
            for successor in {e.target for e in self.succs[node]}:
                waiting[successor] -= 1
                if waiting[successor] == 0:
                    heapq.heappush(ready, successor)
        return order + sorted(set(self.nodes) - set(order))

    def sinks(self) -> list[int]:
        """Tools nothing reads from (the outputs), excluding Browse/Comment."""
        return [n for n in self.order if not self.succs[n] and self.nodes[n].get("node_type") not in NO_DATA_NODES]

    def sources(self) -> list[int]:
        return [n for n in self.order if not self.preds[n] and self.nodes[n].get("node_type") not in NO_DATA_NODES]
