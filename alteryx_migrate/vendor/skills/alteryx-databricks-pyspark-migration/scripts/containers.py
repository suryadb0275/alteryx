"""Alteryx tool containers from the original .yxmd, and the order in which their tools are written.

The parsed JSON drops containers, so they are read from the XML: every `ToolContainer` node has a caption and child nodes (containers
can be nested). The notebook is then written one section per container, in the order the data flows.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import defaultdict

BIG = 40        # a container with more tools than this is shown by its own sub-containers (when it has any)


def read(path) -> dict:
    """{"path": {tool id: [container id, ...] outer -> inner}, "caption": {container id: text}, "disabled": {container id}, "size": {container id: tools}}"""
    root = ET.parse(path).getroot()
    out = {"path": {}, "caption": {}, "disabled": set(), "size": defaultdict(int)}

    def walk(node, trail):
        for child in node.findall("ChildNodes/Node") if node is not root else root.findall("Nodes/Node"):
            tid = int(child.get("ToolID"))
            gui = child.find("GuiSettings")
            plugin = (gui.get("Plugin") if gui is not None else "") or ""
            if "ToolContainer" in plugin:
                cfg = child.find("Properties/Configuration")
                caption = (cfg.findtext("Caption") if cfg is not None else "") or ""
                out["caption"][tid] = caption.strip() or f"Container {tid}"
                if cfg is not None and cfg.find("Disabled") is not None and cfg.find("Disabled").get("value") == "True":
                    out["disabled"].add(tid)
                walk(child, trail + [tid])
            else:
                out["path"][tid] = list(trail)
                for c in trail:
                    out["size"][c] += 1
    walk(root, [])
    out["size"] = dict(out["size"])
    return out


def container_of(info: dict, tool: int):
    """The container a tool is shown under: the outermost one, or a nested one when the outer one is large."""
    trail = info["path"].get(tool) or []
    for i, c in enumerate(trail):
        if info["size"].get(c, 0) <= BIG or i == len(trail) - 1:
            return c
    return None


def sections(info: dict, wf, nodes: list) -> list:
    """The tools to write (ids, in valid order) grouped as [{"container": id | None, "caption": text, "part": n, "nodes": [ids]}], in flow order.

    A container is written when every tool it needs from outside has been written; when containers are interleaved (each needs the
    other) the container is split into parts at the point where it must wait."""
    order = {n: i for i, n in enumerate(wf.topo)}
    nodes = sorted(nodes, key=order.get)
    group = {n: container_of(info, n) for n in nodes}
    preds = defaultdict(set)
    for e in wf.edges:
        if e.src in group and e.dst in group:
            preds[e.dst].add(e.src)
    remaining, done, result, parts = list(nodes), set(), [], defaultdict(int)

    def take(tools, cid):
        parts[cid] += 1
        result.append({"container": cid, "caption": info["caption"].get(cid, "Outside containers") if cid else "Outside containers",
                       "part": parts[cid], "nodes": tools})
        done.update(tools)
        for t in tools:
            remaining.remove(t)

    while remaining:
        by = defaultdict(list)
        for n in remaining:
            by[group[n]].append(n)
        ready = [c for c, tools in by.items()
                 if all(p in done or p in tools for t in tools for p in preds[t])]
        if ready:
            c = min(ready, key=lambda x: order[by[x][0]])
            take(by[c], c)
            continue
        first = remaining[0]                                   # interleaved containers: write what can go now
        cid, chunk = group[first], []
        for n in by[cid]:
            if all(p in done or p in chunk for p in preds[n]):
                chunk.append(n)
        take(chunk, cid)
    return _split_loose(result, wf, order)


def _split_loose(result: list, wf, order: dict) -> list:
    """Tools outside any container are shown as one section per connected chain, not as one pile."""
    out, count = [], 0
    for sec in result:
        if sec["container"] is not None:
            out.append(sec)
            continue
        members, parent = set(sec["nodes"]), {n: n for n in sec["nodes"]}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x
        for e in wf.edges:
            if e.src in members and e.dst in members:
                parent[find(e.src)] = find(e.dst)
        comps = {}
        for n in sec["nodes"]:
            comps.setdefault(find(n), []).append(n)
        for nodes in sorted(comps.values(), key=lambda c: order[c[0]]):
            count += 1
            out.append({**sec, "part": count, "nodes": nodes})
    return out
