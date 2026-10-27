"""Analysis of one embedded Python/Jupyter tool: cells, per-output slices and the #n -> upstream-table numbering."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .describe import upstream_sources
from .graph import Workflow
from .name_scan import analyse_cell
from .script_cells import Cell, split_cells
from .slicing import SliceCheck, build_slices, check_slice


@dataclass
class PythonInput:
    number: int                 # n in Alteryx.read("#n")
    tool: int                   # upstream tool feeding it
    anchor: str | None
    verified: bool              # True when the number came from the .yxmd connection name
    variable: str = ""          # DataFrame variable the script assigns it to
    sources: list = field(default_factory=list)
    name_check: str = "?"       # "ok" when the variable name matches an upstream source table


@dataclass
class PythonTool:
    node_id: int
    file: str
    line_count: int
    cells: list[Cell]
    slices: dict[int, list[int]]
    checks: dict[int, SliceCheck]
    inputs: dict[int, PythonInput]

    @property
    def by_index(self) -> dict[int, Cell]:
        return {c.index: c for c in self.cells}

    @property
    def reads(self) -> list[int]:
        return sorted({n for c in self.cells for n in c.reads})

    @property
    def unused_cells(self) -> list[Cell]:
        used = set().union(*self.slices.values()) if self.slices else set()
        return [c for c in self.cells if c.index not in used]

    def reads_for_output(self, output: int) -> list[int]:
        index = self.by_index
        return sorted({n for i in self.slices.get(output, []) for n in index[i].reads})

    def input_tools_for(self, outputs: set) -> set | None:
        """Upstream tools whose input the slices of these outputs actually read; None means 'cannot narrow, keep all'."""
        if not outputs or any(o not in self.slices for o in outputs):
            return None
        wanted = {n for o in outputs for n in self.reads_for_output(o)}
        if not wanted:
            return None
        return {inp.tool for n, inp in self.inputs.items() if n in wanted}


def output_index(anchor) -> int:
    """'Output3' -> 3 (a missing number means 1)."""
    digits = re.sub(r"\D", "", str(anchor or ""))
    return int(digits) if digits else 1


def _number_inputs(workflow: Workflow, node_id: int, connection_names: dict) -> dict[int, PythonInput]:
    """XML connection names (#n) are authoritative; without them the file order is used and marked unverified."""
    numbered: dict[int, PythonInput] = {}
    for position, edge in enumerate(sorted(workflow.preds[node_id], key=lambda e: e.index), 1):
        name = connection_names.get((node_id, edge.source, edge.origin))
        match = re.search(r"(\d+)", name) if name else None
        number = int(match.group(1)) if match else position
        numbered[number] = PythonInput(number, edge.source, edge.origin, verified=bool(match))
    return numbered


def _check_names(inputs: dict[int, PythonInput], cells: list[Cell], workflow: Workflow) -> None:
    variables = {}
    for cell in cells:
        for number, variable in cell.read_vars.items():
            variables.setdefault(int(number), variable)
    for number, item in inputs.items():
        item.sources = upstream_sources(workflow, item.tool)
        item.variable = variables.get(number, "")
        core = re.sub(r"[^a-z0-9]", "", re.sub(r"^df_", "", item.variable.lower()))
        tables = [re.sub(r"[^a-z0-9]", "", s.lower()) for s in item.sources]
        item.name_check = "ok" if core and any(core in t or t in core for t in tables if t) else "?"


def analyse_python_tool(workflow: Workflow, node_id: int, script: str, file_name: str, connection_names: dict) -> PythonTool:
    cells = [analyse_cell(c) for c in split_cells(script)]
    slices = build_slices(cells)
    index = {c.index: c for c in cells}
    inputs = _number_inputs(workflow, node_id, connection_names)
    _check_names(inputs, cells, workflow)
    return PythonTool(node_id, file_name, len(script.splitlines()), cells, slices,
                      {k: check_slice(index, idx) for k, idx in slices.items()}, inputs)


def python_reference(tool: PythonTool) -> dict:
    """What the slimmed node keeps instead of the script itself."""
    return {"file": tool.file, "cells_json": "jupyter/cells.json", "lines": tool.line_count, "cells": len(tool.cells),
            "outputs": {f"Output{k}": {"cells": v} for k, v in sorted(tool.slices.items())},
            "inputs": {f"#{n}": {"from_tool": i.tool, "verified_in_xml": i.verified} for n, i in sorted(tool.inputs.items())}}


def cells_summary(tool: PythonTool) -> dict:
    """Content of jupyter/cells.json (no cell text - that is in full_script.py)."""
    return {"file": tool.file,
            "cells": [{"index": c.index, "start": c.start, "end": c.end, "first_line": c.first_line[:100], "parsed": c.parsed,
                       "loads_external": c.loads_external, "stores": c.stores, "reads": c.reads,
                       "writes": [{"output": w.output, "frames": w.frames} for w in c.writes]} for c in tool.cells],
            "slices": {str(k): v for k, v in tool.slices.items()},
            "unused_cells": [c.index for c in tool.unused_cells]}
