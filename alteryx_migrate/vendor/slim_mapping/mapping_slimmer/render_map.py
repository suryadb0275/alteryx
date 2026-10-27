"""workflow_map.md: the small shared context every chunk starts with."""
from __future__ import annotations

from .common import tool_label
from .describe import describe
from .graph import Workflow
from .python_tool import PythonTool


def _tool_ids(edges, attr: str) -> str:
    return ", ".join(dict.fromkeys(f"#{getattr(e, attr)}" for e in edges))


def render_workflow_map(workflow: Workflow, document: dict, sink_names: dict, containers: dict,
                        python_tools: dict[int, PythonTool], warnings: list, has_yxmd: bool) -> str:
    nodes = workflow.nodes
    source_file = (document.get("workflow") or {}).get("file_path")
    lines = ["# Workflow map", "", f"Source: `{source_file}` - {len(nodes)} tools, {len(workflow.edges)} connections.", ""]
    lines += [f"> {w}" for w in warnings] + ([""] if warnings else [])

    lines += ["## Outputs", ""]
    for sink in workflow.sinks():
        container = containers.get(sink)
        suffix = f" (container `{container}`)" if container and container != sink_names[sink] else ""
        lines.append(f"- #{sink} **{sink_names[sink]}** - {describe(nodes[sink])}{suffix}")

    lines += ["", "## Sources", ""] + [f"- #{n} {describe(nodes[n])}" for n in workflow.sources()]

    lines += ["", "## Tools (dependency order)", "", "| id | type | what it does | inputs from | feeds |", "|---|---|---|---|---|"]
    for n in workflow.order:
        what = describe(nodes[n], python_tools.get(n)).replace("|", "/")
        lines.append(f"| {n} | {tool_label(nodes[n])} | {what} | {_tool_ids(workflow.preds[n], 'source')} "
                     f"| {_tool_ids(workflow.succs[n], 'target')} |")

    for node_id, tool in python_tools.items():
        lines += ["", f"## Embedded Python tool #{node_id} - inputs (`{tool.file}`, {tool.line_count} lines, {len(tool.cells)} cells)", ""]
        if not has_yxmd:
            lines += ["> **UNVERIFIED numbering** - no .yxmd was given; #n below is file order and may be wrong.", ""]
        lines += ["| #n | DataFrame | upstream tool | source table | numbering | name check |", "|---|---|---|---|---|---|"]
        for number, item in sorted(tool.inputs.items()):
            lines.append(f"| #{number} | {item.variable or '-'} | #{item.tool} {tool_label(nodes[item.tool])} | "
                         f"{', '.join(item.sources) or '-'} | {'XML' if item.verified else 'UNVERIFIED (file order)'} | {item.name_check} |")
        lines += ["", "Outputs: " + ", ".join(f"Output{k} <- cells {v}" for k, v in sorted(tool.slices.items()))]
    return "\n".join(lines) + "\n"
