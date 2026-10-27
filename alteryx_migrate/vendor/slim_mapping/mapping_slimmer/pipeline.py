"""Orchestration: read -> analyse the Python tool(s) -> slim -> write the files -> run the checks."""
from __future__ import annotations

import json
import os
from collections import defaultdict

from .common import config_value, snake, strip_empty, tool_label
from .config import EXIT_CHECK_FAILED, EXIT_OK, EXIT_SECRET_LEAK
from .graph import Workflow
from .python_tool import PythonTool, analyse_python_tool, cells_summary, python_reference
from .render_chunk import render_chunk
from .render_map import render_workflow_map
from .report import RunData, evaluate
from .script_cells import extract_script
from .slimming import slim_node
from .yxmd import YxmdInfo, read_yxmd


def _write(path: str, text: str) -> int:
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    return len(text.encode("utf-8"))


def _analyse_python_tools(workflow: Workflow, out_dir: str, yxmd: YxmdInfo) -> dict[int, PythonTool]:
    scripts = {n: s for n, node in workflow.nodes.items() if (s := extract_script(node.get("original_configuration")))}
    tools = {}
    for node_id, script in scripts.items():
        name = f"jupyter/full_script{'_' + str(node_id) if len(scripts) > 1 else ''}.py"
        _write(os.path.join(out_dir, name), script)
        tools[node_id] = analyse_python_tool(workflow, node_id, script, name, yxmd.connection_names)
    return tools


def _expressions_by_tool(document: dict) -> dict[int, list]:
    grouped = defaultdict(list)
    for item in document.get("expressions", []):
        grouped[int(item["node_id"])].append(
            strip_empty({k: item.get(k) for k in ("label", "alteryx_expression", "spark_sql", "error")}))
    return grouped


def _sink_name(workflow: Workflow, sink: int, containers: dict) -> str:
    """BigQuery target table if the output names one, else the tool container, else a generic tool name."""
    selected = config_value(workflow.nodes[sink].get("original_configuration") or {}, "selectedId")
    if selected:
        return str(selected).split(":")[-1].split(".")[-1]
    return containers.get(sink) or f"tool{sink}_{snake(tool_label(workflow.nodes[sink]), 20)}"


def _slim_document(document: dict, workflow: Workflow, slim_nodes: dict, containers: dict) -> dict:
    without_node = [strip_empty({k: t.get(k) for k in ("tool_id", "tool_type", "status", "reason")})
                    for t in document.get("tool_mapping", []) if int(t["tool_id"]) not in workflow.nodes]
    edges = [strip_empty({"source": e.source, "target": e.target, "origin_anchor": e.origin, "destination_anchor": e.destination})
             for e in workflow.edges]
    return strip_empty({"workflow": document.get("workflow"), "container_of_tool": {str(k): v for k, v in containers.items()},
                        "nodes": [slim_nodes[n] for n in workflow.order], "edges": edges, "tools_without_node": without_node})


def run(mapping_path: str, out_dir: str, yxmd_path: str | None = None, echo=print) -> int:
    """Run everything; returns the process exit code (0 ok, 1 secret leak, 2 failed check)."""
    os.makedirs(os.path.join(out_dir, "chunks"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "jupyter"), exist_ok=True)
    input_bytes = os.path.getsize(mapping_path)
    with open(mapping_path, encoding="utf-8") as handle:
        document = json.load(handle)
    workflow = Workflow(document)

    warnings = []
    yxmd = read_yxmd(yxmd_path) if yxmd_path else YxmdInfo()
    if not yxmd_path:
        warnings.append("UNVERIFIED: no --yxmd given - Python input numbers (#n) fall back to file order and may be WRONG")
    python_tools = _analyse_python_tools(workflow, out_dir, yxmd)
    if len(python_tools) > 1:
        warnings.append("several embedded Python tools found: scripts are jupyter/full_script_<tool id>.py")

    expressions = _expressions_by_tool(document)
    slim_nodes = {}
    for node_id in workflow.order:
        reference = python_reference(python_tools[node_id]) if node_id in python_tools else None
        node = slim_node(workflow.nodes[node_id], reference)
        if expressions.get(node_id):
            node["expressions"] = expressions[node_id]
        slim_nodes[node_id] = node

    _write(os.path.join(out_dir, "slim.json"),
           json.dumps(_slim_document(document, workflow, slim_nodes, yxmd.container_of), ensure_ascii=False, indent=1, default=str))
    _write(os.path.join(out_dir, "jupyter", "cells.json"),
           json.dumps({str(n): cells_summary(t) for n, t in python_tools.items()}, indent=1, ensure_ascii=False))

    sinks = workflow.sinks()
    sink_names = {s: _sink_name(workflow, s, yxmd.container_of) for s in sinks}
    workflow_map = render_workflow_map(workflow, document, sink_names, yxmd.container_of, python_tools, warnings, bool(yxmd_path))
    _write(os.path.join(out_dir, "workflow_map.md"), workflow_map)

    chunk_files, chunk_bytes, in_chunks = [], {}, set()
    for number, sink in enumerate(sinks, 1):
        text, tools = render_chunk(number, sink, sink_names[sink], workflow, workflow_map, slim_nodes, expressions,
                                   python_tools, yxmd.container_of)
        name = f"chunks/{number:02d}_{snake(sink_names[sink])}.md"
        chunk_bytes[name] = _write(os.path.join(out_dir, name), text)
        chunk_files.append(name)
        in_chunks |= tools

    outcome = evaluate(RunData(mapping_path, yxmd_path, out_dir, input_bytes, document, workflow, python_tools, slim_nodes,
                               chunk_files, chunk_bytes, in_chunks, warnings))
    _write(os.path.join(out_dir, "coverage_report.txt"), outcome.text)
    echo(outcome.text)
    if outcome.leaks:
        echo("EXIT 1: secret value(s) found in output files")
        return EXIT_SECRET_LEAK
    if outcome.problems:
        echo("EXIT 2: checks failed: " + "; ".join(outcome.problems))
        return EXIT_CHECK_FAILED
    return EXIT_OK
