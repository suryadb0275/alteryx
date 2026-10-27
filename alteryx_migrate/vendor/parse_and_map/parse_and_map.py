#!/usr/bin/env python
"""Standalone parser + tool-mapping report for an Alteryx .yxmd workflow.

Does NOT generate any Databricks code (no PySpark/DLT/SQL/Lakeflow/Designer
output). It only runs the first two pipeline phases:

    Parse (.yxmd -> ParsedWorkflow) -> Convert (ParsedNode -> IRNode)

and prints, for every tool in the workflow, which IR node / converter it
mapped to (or why it didn't), plus the resulting IR graph (nodes + edges).

Usage:
    python scripts/parse_and_map/parse_and_map.py path/to/workflow.yxmd
    python scripts/parse_and_map/parse_and_map.py path/to/workflow.yxmd --json
    python scripts/parse_and_map/parse_and_map.py path/to/workflow.yxmd --json -o mapping.json

This folder is fully self-contained and movable: the a2d/ subdirectory next
to this file is a vendored copy of exactly the a2d modules this script
needs (parser, converter registry, IR, expression engine - no code
generators). See requirements.txt for the (small) pip dependency set.

Usage:
    python parse_and_map.py path/to/workflow.yxmd
    python parse_and_map.py path/to/workflow.yxmd --json
    python parse_and_map.py path/to/workflow.yxmd --json -o mapping.json
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from enum import Enum
from pathlib import Path

from a2d.config import ConversionConfig
from a2d.converters.registry import ConverterRegistry
from a2d.expressions.base_translator import BaseTranslationError
from a2d.expressions.parser import ExpressionParser
from a2d.expressions.sql_translator import SparkSQLTranslator
from a2d.frontends.alteryx import AlteryxFrontend
from a2d.ir.graph import WorkflowDAG
from a2d.ir.nodes import (
    FilterNode,
    FormulaNode,
    GenerateRowsNode,
    IRNode,
    MultiFieldFormulaNode,
    MultiRowFormulaNode,
    UnsupportedNode,
)
from a2d.parser.schema import ParsedWorkflow

# Visual-only node types with no data transformation — excluded from the IR,
# mirroring a2d.pipeline.ConversionPipeline._build_dag.
_SKIP_TYPES = frozenset({"ToolContainer", "Tab"})


@dataclasses.dataclass
class ToolMappingEntry:
    """One row of the tool -> IR-node mapping report."""

    tool_id: int
    tool_type: str
    plugin_name: str
    category: str
    status: str  # "mapped" | "unsupported" | "skipped" | "disabled"
    ir_node_type: str | None
    converter_class: str | None
    reason: str | None = None


def build_dag_and_mapping(parsed: ParsedWorkflow, config: ConversionConfig) -> tuple[WorkflowDAG, list[ToolMappingEntry]]:
    """Convert every parsed node, recording an IR node (or skip/disable reason).

    Mirrors a2d.pipeline.ConversionPipeline._build_dag, but also returns a
    per-tool mapping report instead of only the DAG.
    """
    dag = WorkflowDAG()
    mapping: list[ToolMappingEntry] = []

    for node in parsed.nodes:
        if node.disabled:
            mapping.append(
                ToolMappingEntry(
                    tool_id=node.tool_id,
                    tool_type=node.tool_type,
                    plugin_name=node.plugin_name,
                    category=node.category,
                    status="disabled",
                    ir_node_type=None,
                    converter_class=None,
                    reason="Node is disabled in the workflow",
                )
            )
            continue

        if node.tool_type in _SKIP_TYPES:
            mapping.append(
                ToolMappingEntry(
                    tool_id=node.tool_id,
                    tool_type=node.tool_type,
                    plugin_name=node.plugin_name,
                    category=node.category,
                    status="skipped",
                    ir_node_type=None,
                    converter_class=None,
                    reason="Visual-only node (no data transformation)",
                )
            )
            continue

        ir_node = ConverterRegistry.convert_node(node, config)
        dag.add_node(ir_node)

        converter = ConverterRegistry.get_converter(node.tool_type)
        if isinstance(ir_node, UnsupportedNode):
            mapping.append(
                ToolMappingEntry(
                    tool_id=node.tool_id,
                    tool_type=node.tool_type,
                    plugin_name=node.plugin_name,
                    category=node.category,
                    status="unsupported",
                    ir_node_type=type(ir_node).__name__,
                    converter_class=type(converter).__name__ if converter else None,
                    reason=ir_node.unsupported_reason,
                )
            )
        else:
            mapping.append(
                ToolMappingEntry(
                    tool_id=node.tool_id,
                    tool_type=node.tool_type,
                    plugin_name=node.plugin_name,
                    category=node.category,
                    status="mapped",
                    ir_node_type=type(ir_node).__name__,
                    converter_class=type(converter).__name__ if converter else None,
                )
            )

    node_ids = set(dag.all_node_ids())
    for conn in parsed.connections:
        src, dst = conn.origin.tool_id, conn.destination.tool_id
        if src not in node_ids or dst not in node_ids:
            continue
        dag.add_edge(src, dst, conn.origin.anchor_name, conn.destination.anchor_name, conn.is_wireless)

    return dag, mapping


@dataclasses.dataclass
class ExpressionEntry:
    """One formula/condition string found on an IR node, plus its parse result."""

    node_id: int
    tool_type: str
    label: str  # e.g. "condition", "formula:FullName", "loop"
    alteryx_expression: str
    status: str  # "parsed" | "failed" | "empty"
    ast: dict | None = None
    spark_sql: str | None = None
    error: str | None = None


# Node types whose fields hold Alteryx formula-language expressions (as
# opposed to e.g. regex patterns or find/replace literals, which aren't
# parseable by ExpressionParser).
def _extract_expressions(node: IRNode) -> list[tuple[str, str]]:
    """Return (label, raw_expression) pairs for every formula/condition on *node*."""
    if isinstance(node, FilterNode):
        return [("condition", node.expression)]
    if isinstance(node, FormulaNode):
        return [(f"formula:{f.output_field}", f.expression) for f in node.formulas]
    if isinstance(node, MultiRowFormulaNode):
        return [("multi_row_formula", node.expression)]
    if isinstance(node, MultiFieldFormulaNode):
        return [("multi_field_formula", node.expression)]
    if isinstance(node, GenerateRowsNode):
        return [
            ("init", node.init_expression),
            ("condition", node.condition_expression),
            ("loop", node.loop_expression),
        ]
    return []


def parse_expressions(dag: WorkflowDAG) -> list[ExpressionEntry]:
    """Parse every formula/condition expression found in the DAG.

    Runs each raw Alteryx expression string through the same tokenizer ->
    parser -> AST pipeline the real generators use, plus a translation to
    Spark SQL, so you can see exactly how each formula/condition resolves
    (or why it doesn't) without generating any code files.
    """
    entries: list[ExpressionEntry] = []
    translator = SparkSQLTranslator()
    for node in dag.all_nodes():
        for label, raw_expr in _extract_expressions(node):
            if not raw_expr or not raw_expr.strip():
                entries.append(
                    ExpressionEntry(
                        node_id=node.node_id,
                        tool_type=node.original_tool_type,
                        label=label,
                        alteryx_expression=raw_expr or "",
                        status="empty",
                    )
                )
                continue
            try:
                ast = ExpressionParser().parse(raw_expr)
                sql = translator.translate(ast)
                entries.append(
                    ExpressionEntry(
                        node_id=node.node_id,
                        tool_type=node.original_tool_type,
                        label=label,
                        alteryx_expression=raw_expr,
                        status="parsed",
                        ast=dataclasses.asdict(ast),
                        spark_sql=sql,
                    )
                )
            except BaseTranslationError as e:
                entries.append(
                    ExpressionEntry(
                        node_id=node.node_id,
                        tool_type=node.original_tool_type,
                        label=label,
                        alteryx_expression=raw_expr,
                        status="failed",
                        error=str(e),
                    )
                )
    return entries


def _json_default(obj: object) -> object:
    if isinstance(obj, Enum):
        return obj.value
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, Path):
        return str(obj)
    return str(obj)


def _ir_node_to_dict(node: IRNode) -> dict:
    # "node_type" (the IRNode subclass name) is a discriminator, not a
    # dataclass field — dataclasses.asdict() alone can't recover it, and a
    # consumer reconstructing real IRNode objects from this JSON (see
    # scripts/json_to_sdp.py) needs it to know which class to build.
    return {"node_type": type(node).__name__, **dataclasses.asdict(node)}


def print_report(
    parsed: ParsedWorkflow,
    dag: WorkflowDAG,
    mapping: list[ToolMappingEntry],
    expressions: list[ExpressionEntry],
) -> None:
    print(f"Workflow: {parsed.file_path}")
    print(f"Alteryx version: {parsed.alteryx_version}")
    print(f"Tools: {len(parsed.nodes)}  Connections: {len(parsed.connections)}")
    print()

    print("Tool Mapping")
    print("-" * 100)
    header = f"{'ID':>5}  {'Tool Type':<28} {'Status':<12} {'-> IR Node':<22} Converter"
    print(header)
    print("-" * 100)
    for m in mapping:
        ir = m.ir_node_type or "-"
        conv = m.converter_class or "-"
        print(f"{m.tool_id:>5}  {m.tool_type:<28} {m.status:<12} {ir:<22} {conv}")
        if m.reason:
            print(f"       └─ {m.reason}")
    print()

    mapped = sum(1 for m in mapping if m.status == "mapped")
    unsupported = sum(1 for m in mapping if m.status == "unsupported")
    skipped = sum(1 for m in mapping if m.status == "skipped")
    disabled = sum(1 for m in mapping if m.status == "disabled")
    considered = mapped + unsupported
    coverage = f"{mapped}/{considered} ({mapped / considered * 100:.0f}%)" if considered else "N/A"
    print(f"Summary: mapped={mapped}  unsupported={unsupported}  skipped={skipped}  disabled={disabled}  coverage={coverage}")
    print()

    print("IR Graph")
    print("-" * 100)
    print(f"Nodes: {dag.node_count}  Edges: {dag.edge_count}")
    for ir_node in dag.topological_order():
        print(f"  [{ir_node.node_id}] {type(ir_node).__name__} (from {ir_node.original_tool_type})")
    print()
    for src, dst, info in dag.all_edges():
        print(f"  {src} --[{info.origin_anchor} -> {info.destination_anchor}]--> {dst}")
    print()

    print("Formulas & Conditions")
    print("-" * 100)
    if not expressions:
        print("  (none found)")
    for e in expressions:
        print(f"  [{e.node_id}] {e.tool_type} / {e.label}")
        print(f"      Alteryx : {e.alteryx_expression!r}")
        if e.status == "parsed":
            print(f"      Spark   : {e.spark_sql}")
        elif e.status == "failed":
            print(f"      FAILED  : {e.error}")
        else:
            print("      (empty expression)")
    print()
    parsed_count = sum(1 for e in expressions if e.status == "parsed")
    failed_count = sum(1 for e in expressions if e.status == "failed")
    empty_count = sum(1 for e in expressions if e.status == "empty")
    print(f"Summary: parsed={parsed_count}  failed={failed_count}  empty={empty_count}")


def build_json_report(
    parsed: ParsedWorkflow,
    dag: WorkflowDAG,
    mapping: list[ToolMappingEntry],
    expressions: list[ExpressionEntry],
) -> dict:
    return {
        "workflow": {
            "file_path": parsed.file_path,
            "alteryx_version": parsed.alteryx_version,
            "tool_count": len(parsed.nodes),
            "connection_count": len(parsed.connections),
        },
        "tool_mapping": [dataclasses.asdict(m) for m in mapping],
        "ir": {
            "nodes": [_ir_node_to_dict(n) for n in dag.all_nodes()],
            "edges": [
                {
                    "source": src,
                    "target": dst,
                    "origin_anchor": info.origin_anchor,
                    "destination_anchor": info.destination_anchor,
                    "is_wireless": info.is_wireless,
                }
                for src, dst, info in dag.all_edges()
            ],
        },
        "expressions": [dataclasses.asdict(e) for e in expressions],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("workflow", type=Path, help="Path to a .yxmd Alteryx workflow file")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON instead of a text report")
    parser.add_argument("-o", "--output", type=Path, default=None, help="Write output to this file instead of stdout")
    args = parser.parse_args()

    if not args.workflow.exists():
        print(f"Error: {args.workflow} not found", file=sys.stderr)
        return 1

    config = ConversionConfig(input_path=args.workflow)
    parsed = AlteryxFrontend().parse(args.workflow)
    dag, mapping = build_dag_and_mapping(parsed, config)
    expressions = parse_expressions(dag)

    if args.json:
        report = build_json_report(parsed, dag, mapping, expressions)
        text = json.dumps(report, indent=2, default=_json_default)
        if args.output:
            args.output.write_text(text, encoding="utf-8")
        else:
            print(text)
    else:
        if args.output:
            import contextlib
            import io

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                print_report(parsed, dag, mapping, expressions)
            args.output.write_text(buf.getvalue(), encoding="utf-8")
        else:
            print_report(parsed, dag, mapping, expressions)

    return 0


if __name__ == "__main__":
    sys.exit(main())
