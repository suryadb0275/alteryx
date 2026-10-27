"""Which cells does each Alteryx.write(df, k) need? Walk backwards: an earlier cell is needed when it STORES a name that is needed."""
from __future__ import annotations

import builtins
from collections import defaultdict
from typing import NamedTuple

from .script_cells import Cell, neutralise_magics


class SliceCheck(NamedTuple):
    compiles: bool
    error: str
    undefined: list          # [(cell index, name)] used before any cell defines it


def build_slices(cells: list[Cell]) -> dict[int, list[int]]:
    """{output k: sorted cell indexes}. Reading a name alone never makes a cell necessary; an unparsable cell is
    conservatively included."""
    slices: dict[int, set] = defaultdict(set)
    for cell in cells:
        for write in cell.writes:
            chosen, needed = {cell.index}, set(cell.loads_external)
            for earlier in reversed([c for c in cells if c.index < cell.index]):
                if not earlier.parsed or set(earlier.stores) & needed:
                    chosen.add(earlier.index)
                    needed |= set(earlier.loads_external)
            slices[write.output] |= chosen
    return {k: sorted(v) for k, v in slices.items()}


def check_slice(cells_by_index: dict[int, Cell], indexes: list[int]) -> SliceCheck:
    """The slice must compile as one program and must not use a name before a chosen cell defines it."""
    chosen = [cells_by_index[i] for i in indexes]
    compiles, error = True, ""
    try:
        compile("\n\n".join(neutralise_magics(c.text) for c in chosen), "<slice>", "exec")
    except SyntaxError as exc:
        compiles, error = False, f"line {exc.lineno}: {exc.msg}"
    defined = set(dir(builtins)) | {"__file__", "__name__"}
    undefined = []
    for cell in chosen:
        undefined += [(cell.index, name) for name in cell.loads_external if name not in defined]
        defined |= set(cell.stores)
    return SliceCheck(compiles, error, undefined)
