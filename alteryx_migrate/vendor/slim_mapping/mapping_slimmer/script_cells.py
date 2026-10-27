"""Get the embedded Python/Jupyter script out of a tool configuration and split it into cells."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from .config import CELL_SEPARATOR, SYNTHETIC_SEPARATOR


@dataclass
class Cell:
    index: int                    # 1-based position among the non-empty cells
    start: int                    # first / last line in full_script.py (1-based)
    end: int
    text: str
    parsed: bool = True
    parse_error: str = ""
    loads: list = field(default_factory=list)            # every name read
    loads_external: list = field(default_factory=list)   # names read before this cell defines them -> need an earlier cell
    stores: list = field(default_factory=list)           # names assigned / mutated / imported
    reads: list = field(default_factory=list)            # Alteryx.read("#n") numbers
    read_vars: dict = field(default_factory=dict)        # "n" -> variable name
    writes: list = field(default_factory=list)           # [AlteryxWrite]

    @property
    def first_line(self) -> str:
        return self.text.splitlines()[0] if self.text else ""


def extract_script(config) -> str | None:
    """productionModeScript if present, otherwise the code cells of the (possibly nested) Notebook JSON."""
    if not isinstance(config, dict):
        return None
    production = config.get("productionModeScript")
    if isinstance(production, str) and production.strip():
        return production
    notebook = config.get("Notebook")
    for _ in range(6):                                    # {'#cdata-section': '<json string>'} nesting
        if isinstance(notebook, dict) and "#cdata-section" in notebook:
            notebook = notebook["#cdata-section"]
        elif isinstance(notebook, str):
            try:
                notebook = json.loads(notebook)
            except ValueError:
                return notebook
        else:
            break
    if isinstance(notebook, dict) and isinstance(notebook.get("cells"), list):
        parts = ["".join(c.get("source", [])) if isinstance(c.get("source"), list) else str(c.get("source", ""))
                 for c in notebook["cells"] if c.get("cell_type") == "code"]
        return f"\n{SYNTHETIC_SEPARATOR}\n".join(parts)
    return None


def split_cells(script: str) -> list[Cell]:
    """Cells are separated by lines of 20+ '#'; empty cells are skipped; line numbers refer to the script as saved."""
    cells: list[Cell] = []
    current: list[str] = []
    start = 0

    def flush():
        nonlocal current, start
        first = start
        body = list(current)
        while body and not body[0].strip():
            body.pop(0)
            first += 1
        while body and not body[-1].strip():
            body.pop()
        if body:
            cells.append(Cell(index=len(cells) + 1, start=first, end=first + len(body) - 1, text="\n".join(body)))
        current = []

    for number, line in enumerate(script.splitlines(), 1):
        if CELL_SEPARATOR.match(line):
            if current:
                flush()
            continue
        if not current:
            start = number
        current.append(line)
    if current:
        flush()
    return cells


def neutralise_magics(text: str) -> str:
    """Jupyter '%magic' / '!shell' lines are not Python: replace them with `pass` so the cell can be parsed."""
    return "\n".join(re.sub(r"^(\s*)[%!].*$", r"\1pass", line) for line in text.splitlines())
