"""One-line, human-readable descriptions of tools, and the source tables behind them."""
from __future__ import annotations

from .common import config_value, plugin_of, sql_tables, tool_label
from .graph import Workflow


def source_name(node: dict) -> str | None:
    """Table or sheet a source tool reads, or None for a tool that is not a source."""
    config = node.get("original_configuration") or {}
    sql = config_value(config, "inputCustomFilter") or node.get("query")
    if sql:
        tables = sql_tables(str(sql))
        return tables[0].split(".")[-1] if tables else None
    workbook = config_value(config, "workbookName")
    if workbook:
        return str(workbook)
    return node.get("table_name") or None


def upstream_sources(workflow: Workflow, node_id: int, limit: int = 4) -> list[str]:
    """Names of the nearest source tables feeding a tool (breadth-first, the tool itself included)."""
    seen, queue, found = {node_id}, [node_id], []
    while queue and len(found) < limit:
        current = queue.pop(0)
        name = source_name(workflow.nodes[current])
        if name:
            found.append(name)
            continue
        for edge in workflow.preds[current]:
            if edge.source not in seen:
                seen.add(edge.source)
                queue.append(edge.source)
    return found


def _short(text, limit: int = 110) -> str:
    flat = " ".join(str(text).split())
    return flat[:limit] + ("..." if len(flat) > limit else "")


def describe(node: dict, python_tool=None) -> str:
    """What the tool does, in one line. `python_tool` is the PythonTool analysis for an embedded-script tool."""
    kind, config, plugin = node.get("node_type"), node.get("original_configuration") or {}, plugin_of(node)
    if python_tool is not None:
        outputs = "/".join(str(k) for k in sorted(python_tool.slices))
        return f"embedded Python: {len(python_tool.cells)} cells, reads {len(python_tool.reads)} inputs, writes Output{outputs}"
    if "big_query_input" in plugin:
        return f"BigQuery input: {_short(config_value(config, 'inputCustomFilter') or '')}"
    if "big_query_output" in plugin:
        return f"BigQuery output -> {config_value(config, 'selectedId')} ({config_value(config, 'tableWriteMode')})"
    if "google-sheets" in plugin:
        return f"Google Sheet input: {config_value(config, 'workbookName')} / {config_value(config, 'worksheetName')}"
    if kind == "FilterNode":
        return f"filter: {_short(node.get('expression', ''))}"
    if kind == "FormulaNode":
        fields = [str(f.get("output_field")) for f in node.get("formulas") or []]
        return f"formula: sets {', '.join(fields[:6])}{' ...' if len(fields) > 6 else ''}"
    if kind == "SelectNode":
        ops = node.get("field_operations") or []
        dropped = sum(1 for o in ops if o.get("selected") is False)
        renamed = sum(1 for o in ops if o.get("rename_to"))
        return f"select: {len(ops)} field rules ({dropped} dropped, {renamed} renamed)"
    if kind == "JoinNode":
        keys = ", ".join(f"{k.get('left_field')}={k.get('right_field')}" for k in node.get("join_keys") or [])
        return "join on " + keys[:110]
    if kind == "UnionNode":
        return f"union ({node.get('mode', 'auto')})"
    if kind == "SummarizeNode":
        aggregations = node.get("aggregations") or []
        groups = [str(a.get("field_name")) for a in aggregations if str(a.get("action")).lower() == "groupby"]
        return f"summarize by {', '.join(groups) or '(all)'}; {len(aggregations) - len(groups)} aggregates"
    if kind == "SortNode":
        return "sort by " + ", ".join(str(s.get("field_name")) for s in node.get("sort_fields") or [])
    if kind == "UniqueNode":
        return "unique on " + ", ".join(map(str, node.get("key_fields") or []))
    if kind == "DateTimeNode":
        return f"date/time: {node.get('input_field')} -> {node.get('output_field')} ({node.get('format_string')})"
    if kind == "ReadNode":
        return f"reads {source_name(node) or node.get('file_path') or '?'}"
    return tool_label(node)
