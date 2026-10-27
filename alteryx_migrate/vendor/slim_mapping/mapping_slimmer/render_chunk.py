"""One chunk per output: the tools feeding it, their specs, formula expressions and that output's Python slice."""
from __future__ import annotations

import json
from collections import defaultdict

from .common import tool_label
from .describe import describe
from .graph import Workflow
from .python_tool import PythonTool, output_index


def upstream_of(workflow: Workflow, sink: int, python_tools: dict[int, PythonTool]):
    """-> (tools feeding the sink, {tool: anchors of it that are used}).

    Walks in reverse dependency order, so by the time an embedded Python tool is reached it is known which of its
    outputs are used, and only the inputs that those outputs' slices read are followed."""
    live = {sink}
    used_anchors: dict[int, set] = defaultdict(set)
    for node in reversed(workflow.order):
        if node not in live:
            continue
        allowed = None
        if node in python_tools:
            allowed = python_tools[node].input_tools_for({output_index(a) for a in used_anchors[node]})
        for edge in workflow.preds[node]:
            if allowed is not None and edge.source not in allowed:
                continue
            live.add(edge.source)
            used_anchors[edge.source].add(edge.origin)
    return live, used_anchors


def _python_section(tool: PythonTool, anchors: set) -> list[str]:
    lines = ["", f"## Python slice (tool #{tool.node_id}, `{tool.file}`)", ""]
    for anchor in sorted(anchors, key=str):
        output = output_index(anchor)
        cell_ids = tool.slices.get(output)
        if cell_ids is None:
            lines += [f"No Alteryx.write(df, {output}) found for anchor `{anchor}`: all inputs are included above.", ""]
            continue
        reads = tool.reads_for_output(output)
        inputs = ", ".join(f"#{r} ({tool.inputs[r].variable if r in tool.inputs else '?'})" for r in reads)
        lines += [f"### Output{output} (`Alteryx.write(df, {output})`) - needs cells {cell_ids} of {len(tool.cells)}; reads inputs {inputs}", ""]
        index = tool.by_index
        for i in cell_ids:
            cell = index[i]
            lines += [f"#### Cell {cell.index} (lines {cell.start}-{cell.end} of {tool.file})", "", "```python", cell.text, "```", ""]
    return lines


def render_chunk(number: int, sink: int, sink_name: str, workflow: Workflow, workflow_map: str, slim_nodes: dict,
                 expressions: dict, python_tools: dict[int, PythonTool], containers: dict):
    """-> (markdown text, set of tools included)."""
    live, used_anchors = upstream_of(workflow, sink, python_tools)
    nodes = workflow.nodes
    container = f" - container `{containers[sink]}`" if containers.get(sink) else ""
    lines = [f"# Output {number:02d}: {sink_name}", "", f"Sink tool #{sink}: {describe(nodes[sink])}{container}", "",
             "## Workflow map (shared context)", "", workflow_map.replace("# Workflow map", "### Workflow map", 1), "",
             f"## Tools feeding this output ({len(live)} of {len(nodes)}, dependency order)", ""]
    ordered = [n for n in workflow.order if n in live]
    for n in ordered:
        lines += [f"### #{n} {tool_label(nodes[n])} - {describe(nodes[n], python_tools.get(n))}", "", "```json",
                  json.dumps(slim_nodes[n], ensure_ascii=False, indent=1, default=str), "```", ""]
    formulas = [(n, e) for n in ordered for e in expressions.get(n, [])]
    lines += [f"## Formula expressions ({len(formulas)})", ""]
    for n, expr in formulas:
        error = f" - ERROR: {expr['error']}" if expr.get("error") else ""
        lines += [f"- #{n} {expr.get('label', '')}", f"  - Alteryx: `{(expr.get('alteryx_expression') or '').strip()}`",
                  f"  - Spark SQL (parser's translation, unverified): `{expr.get('spark_sql') or '-'}`{error}"]
    for n in ordered:
        if n in python_tools:
            lines += _python_section(python_tools[n], used_anchors[n])
    return "\n".join(lines) + "\n", live
