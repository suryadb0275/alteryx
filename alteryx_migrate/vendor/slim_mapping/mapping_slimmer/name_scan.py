"""Static name analysis of one script cell: which names it loads and stores, and its Alteryx.read / Alteryx.write calls."""
from __future__ import annotations

import ast
import re
from typing import NamedTuple

from .config import MUTATING_METHODS
from .script_cells import Cell, neutralise_magics


class AlteryxWrite(NamedTuple):
    output: int            # k in Alteryx.write(df, k)
    frames: list           # names appearing in the written expression


def _root_name(node) -> str | None:
    while isinstance(node, (ast.Attribute, ast.Subscript)):
        node = node.value
    return node.id if isinstance(node, ast.Name) else None


def _is_alteryx_call(call, method: str) -> bool:
    return (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == method
            and isinstance(call.func.value, ast.Name) and call.func.value.id == "Alteryx")


def _digits(value) -> int | None:
    match = re.search(r"(\d+)", str(value))
    return int(match.group(1)) if match else None


class NameScanner(ast.NodeVisitor):
    """Collects ('L'|'S', name) events in evaluation order. 'Store' covers assignment, df['x']=..., augmented assignment,
    imports, loop/with targets, and in-place mutation (.drop(inplace=True), list.append(), ...). Names local to a function,
    lambda or comprehension are ignored."""

    def __init__(self):
        self.events: list[tuple[str, str]] = []
        self.reads: list[int] = []
        self.read_vars: dict[int, str] = {}
        self.writes: list[AlteryxWrite] = []
        self._scopes: list[set] = []

    # -- bookkeeping
    def _is_local(self, name):
        return any(name in scope for scope in self._scopes)

    def _load(self, name):
        if not self._is_local(name):
            self.events.append(("L", name))

    def _store(self, name):
        if not self._is_local(name):
            self.events.append(("S", name))

    def _target(self, target):
        if isinstance(target, ast.Name):
            self._store(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                self._target(element)
        elif isinstance(target, ast.Starred):
            self._target(target.value)
        else:                                              # df['x'] = ..., obj.attr = ... -> loads the base and mutates it
            self.visit(target)
            root = _root_name(target)
            if root:
                self._store(root)

    # -- statements
    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load):
            self._load(node.id)
        elif isinstance(node.ctx, ast.Del):
            self._load(node.id)
            self._store(node.id)
        else:
            self._store(node.id)

    def visit_Assign(self, node):
        self.visit(node.value)
        for target in node.targets:
            self._target(target)
        if _is_alteryx_call(node.value, "read") and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            number = self._read_number(node.value)
            if number is not None:
                self.read_vars[number] = node.targets[0].id

    def visit_AugAssign(self, node):
        self.visit(node.value)
        if isinstance(node.target, ast.Name):
            self._load(node.target.id)
            self._store(node.target.id)
        else:
            self._target(node.target)

    def visit_AnnAssign(self, node):
        if node.value is not None:
            self.visit(node.value)
        self._target(node.target)

    def visit_For(self, node):
        self.visit(node.iter)
        self._target(node.target)
        for statement in node.body + node.orelse:
            self.visit(statement)

    visit_AsyncFor = visit_For

    def visit_With(self, node):
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self._target(item.optional_vars)
        for statement in node.body:
            self.visit(statement)

    visit_AsyncWith = visit_With

    def visit_Import(self, node):
        for alias in node.names:
            self._store(alias.asname or alias.name.split(".")[0])

    def visit_ImportFrom(self, node):
        for alias in node.names:
            if alias.name != "*":
                self._store(alias.asname or alias.name)

    def visit_ExceptHandler(self, node):
        if node.type is not None:
            self.visit(node.type)
        if node.name:
            self._store(node.name)
        for statement in node.body:
            self.visit(statement)

    def visit_NamedExpr(self, node):
        self.visit(node.value)
        self._target(node.target)

    # -- scopes
    def _function(self, node, name=None):
        if name:
            self._store(name)
        args = node.args
        for default in list(args.defaults) + [d for d in args.kw_defaults if d is not None]:
            self.visit(default)
        local = {a.arg for a in args.posonlyargs + args.args + args.kwonlyargs}
        local |= {a.arg for a in (args.vararg, args.kwarg) if a}
        body = node.body if isinstance(node.body, list) else [node.body]
        for statement in body:
            for inner in ast.walk(statement):
                if isinstance(inner, ast.Name) and isinstance(inner.ctx, ast.Store):
                    local.add(inner.id)
        self._scopes.append(local)
        for statement in body:
            self.visit(statement)
        self._scopes.pop()

    def visit_FunctionDef(self, node):
        for decorator in node.decorator_list:
            self.visit(decorator)
        self._function(node, node.name)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Lambda(self, node):
        self._function(node)

    def visit_ClassDef(self, node):
        for expression in node.decorator_list + node.bases:
            self.visit(expression)
        self._store(node.name)
        for statement in node.body:
            self.visit(statement)

    def _comprehension(self, node):
        local = {n.id for g in node.generators for n in ast.walk(g.target) if isinstance(n, ast.Name)}
        for generator in node.generators:
            self.visit(generator.iter)
        self._scopes.append(local)
        for generator in node.generators:
            for condition in generator.ifs:
                self.visit(condition)
        for part in ([node.key, node.value] if isinstance(node, ast.DictComp) else [node.elt]):
            self.visit(part)
        self._scopes.pop()

    visit_ListComp = visit_SetComp = visit_GeneratorExp = visit_DictComp = _comprehension

    # -- calls
    @staticmethod
    def _read_number(call) -> int | None:
        if call.args and isinstance(call.args[0], ast.Constant):
            return _digits(call.args[0].value)
        return None

    def visit_Call(self, node):
        self.generic_visit(node)
        if _is_alteryx_call(node, "read"):
            number = self._read_number(node)
            if number is not None:
                self.reads.append(number)
        elif _is_alteryx_call(node, "write"):
            output = None
            if len(node.args) > 1 and isinstance(node.args[1], ast.Constant):
                output = node.args[1].value
            for keyword in node.keywords:
                if keyword.arg in ("anchor", "outgoing_connection_name", "k") and isinstance(keyword.value, ast.Constant):
                    output = keyword.value.value
            frames = sorted({n.id for n in ast.walk(node.args[0]) if isinstance(n, ast.Name)}) if node.args else []
            number = output if isinstance(output, int) else (_digits(output) if output is not None else None)
            self.writes.append(AlteryxWrite(number if number is not None else 1, frames))
        elif isinstance(node.func, ast.Attribute):
            inplace = any(k.arg == "inplace" and isinstance(k.value, ast.Constant) and k.value.value is True
                          for k in node.keywords)
            root = _root_name(node.func.value)
            if root and (inplace or node.func.attr in MUTATING_METHODS):
                self._store(root)                          # in-place mutation of an object the cell did not rebind


def analyse_cell(cell: Cell) -> Cell:
    """Fill the cell's name sets and Alteryx calls. A cell that cannot be parsed is flagged (parsed=False)."""
    scanner = NameScanner()
    try:
        scanner.visit(ast.parse(neutralise_magics(cell.text)))
    except SyntaxError as exc:
        cell.parsed = False
        cell.parse_error = f"line {exc.lineno}: {exc.msg}"
    first_event: dict[str, str] = {}
    for kind, name in scanner.events:
        first_event.setdefault(name, kind)
    cell.loads_external = sorted(n for n, kind in first_event.items() if kind == "L")
    cell.loads = sorted({n for kind, n in scanner.events if kind == "L"})
    cell.stores = sorted({n for kind, n in scanner.events if kind == "S"})
    cell.reads = sorted(set(scanner.reads))
    cell.read_vars = {str(k): v for k, v in scanner.read_vars.items()}
    cell.writes = scanner.writes
    return cell
