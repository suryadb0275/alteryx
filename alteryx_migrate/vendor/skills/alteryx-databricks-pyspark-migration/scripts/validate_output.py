#!/usr/bin/env python3
"""Static validation of a generated notebook folder (.ipynb or Databricks source .py).  Proves the code is well-formed and
obeys the skill's hard rules; it does NOT prove business equivalence (that needs data - see
references/07-validation-and-reconciliation.md).

    python validate_output.py OUTDIR [--strict] [--json report.json]

Exit code 1 if a hard rule is violated (always) or if blocked/unsupported stubs remain and --strict is given."""
import argparse
import ast
import json
import re
import sys
from collections import Counter
from pathlib import Path

BANNED = {
    r"\bimport dlt\b|\bfrom dlt\b|@dlt\.|\bdp\.(table|materialized_view|temporary_view)|pyspark\.pipelines": "SDP/DLT API",
    r"\breadStream\b|\bwriteStream\b|\bforeachBatch\b|\bAuto Loader\b|cloudFiles": "streaming API",
    r"\.toPandas\(": "driver-side toPandas()",
    r"\bpandas_udf\b|(?<![\w.])udf\(|F\.udf\(|@udf": "Python UDF",
    r"\.rdd\b": "RDD API",
    r"(?i)password\s*=\s*['\"][^'\"<]+['\"]": "hard-coded credential",
}
NOTEBOOK_RE = re.compile(r"^\d\d_.*\.(ipynb|py)$")


def code_cells(path: Path):
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".ipynb":
        nb = json.loads(text)
        return ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"], nb.get("nbformat") == 4
    return [c for c in text.split("\n# COMMAND ----------\n") if not c.lstrip().startswith("# MAGIC %md")], \
        text.startswith("# Databricks notebook source")


def undefined_names(tree, known):
    """Names read inside a build_* function that are never assigned there (typos, mismatched variable names)."""
    import builtins
    bad = []
    for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
        assigned = {a.arg for a in fn.args.args}
        for n in ast.walk(fn):
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store):
                assigned.add(n.id)
            elif isinstance(n, (ast.FunctionDef, ast.Lambda)):
                assigned |= {a.arg for a in n.args.args}
        for n in ast.walk(fn):
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id not in assigned                     and n.id not in known and not hasattr(builtins, n.id):
                bad.append((fn.name, n.id))
    return sorted(set(bad))


def undefined_in_flow(tree, known):
    """A notebook written as plain top-level code (one section per container): a name must be assigned before the statement that reads it."""
    import builtins
    defined, bad = set(), []
    for stmt in tree.body:
        local = {n.id for n in ast.walk(stmt) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)}
        local |= {a.arg for n in ast.walk(stmt) if isinstance(n, (ast.FunctionDef, ast.Lambda)) for a in n.args.args}
        local |= {a.asname or a.name.split(".")[0] for n in ast.walk(stmt) if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names}
        for n in ast.walk(stmt):
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id not in defined | local | known and not hasattr(builtins, n.id):
                bad.append(n.id)
        defined |= local
    return sorted(set(bad))


def table_flow_problems(out: Path, files):
    """Every table a unit reads must be a declared Bronze input or must have been written by an earlier unit."""
    cfg = next((p for p in files if p.name.startswith("00_config")), None)
    problems = []
    registry = set(re.findall(r"^\s+'((?:bronze|silver|gold)_[^']+)',$", "\n".join(code_cells(cfg)[0]), re.M)) if cfg else set()
    bronze = {t for t in registry if t.startswith("bronze_")}
    written, order = set(), [p for p in files if p.name[:2] in ("02", "03")]
    for p in order:
        for cell in code_cells(p)[0]:
            for m in re.finditer(r"spark\.table\(T\['([^']+)'\]\)", cell):
                t = m.group(1)
                if t not in registry:
                    problems.append(f"{p.name}: reads table {t!r} that is not in the 00_config registry")
                elif t not in bronze and t not in written:
                    problems.append(f"{p.name}: reads {t!r} before any earlier unit writes it")
            for m in re.finditer(r"write_table\(\w+, T\['([^']+)'\]\)", cell):
                key = m.group(1)
                if key not in registry:
                    problems.append(f"{p.name}: writes table {key!r} that is not in the 00_config registry")
                written.add(key)
            ret = re.search(r"^    return \{(.*)\}$", cell, re.M)
            if ret:
                for key in re.findall(r"'([a-z0-9_]+)':", ret.group(1)):
                    if key not in registry:
                        problems.append(f"{p.name}: writes table {key!r} that is not in the 00_config registry")
                    written.add(key)
    unused = sorted(t for t in registry if t not in bronze and t not in written)
    problems += [f"table {t!r} is registered but never written" for t in unused]
    return problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("outdir")
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--json")
    a = ap.parse_args()
    out = Path(a.outdir)
    problems, info, stubs, placeholders = [], {}, Counter(), 0
    files = sorted(p for p in out.iterdir() if NOTEBOOK_RE.match(p.name))
    if not files:
        print("No notebooks found")
        return 1
    if (out / "alteryx_helpers.py").exists() or any("alteryx_helpers" in chr(10).join(code_cells(p)[0]) for p in files):
        problems.append("the notebooks must be plain PySpark: no alteryx_helpers module")
    for p in files:
        cells, header_ok = code_cells(p)
        if not header_ok:
            problems.append(f"{p.name}: not a valid notebook (nbformat 4 / Databricks source header)")
        text = "\n".join(cells)
        pure = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith(("%", "# MAGIC %")))
        try:
            tree = ast.parse(pure)
            module_names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)} |                 {a.asname or a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))
                 for a in n.names}
            known = module_names | {"spark", "T", "F", "Window", "WRITE_MODE", "AUDIT_COLS", "write_table", "dbutils", "display"}
            for fn, name in undefined_names(tree, known):
                problems.append(f"{p.name}: {fn} uses undefined name {name!r}")
            if p.name.startswith("02_silver_gold"):
                for name in undefined_in_flow(tree, known - module_names):
                    problems.append(f"{p.name}: uses name {name!r} before it is assigned")
        except SyntaxError as exc:
            problems.append(f"{p.name}: syntax error line {exc.lineno}: {exc.msg}")
        for pat, label in BANNED.items():
            for m in re.finditer(pat, text):
                problems.append(f"{p.name}:{text.count(chr(10), 0, m.start()) + 1}: forbidden {label}")
        if re.search(r"\.collect\(\)", text):
            problems.append(f"{p.name}: .collect() on data")
        if not p.name.startswith("00_config"):
            for m in re.finditer(r"\.mode\(['\"]overwrite['\"]\)|overwriteSchema|DROP TABLE|saveAsTable", text):
                problems.append(f"{p.name}: direct write/overwrite API '{m.group()}' - use write_table (guarded, from 00_config)")
        stubs.update(int(m.group(1)) for m in re.finditer(r"raise NotImplementedError\(['\"]\[tool (\d+):", text))
        if re.search(r"\bunsupported\(|\bTODO\b|NotImplementedError|<SET:", text):
            problems.append(f"{p.name}: placeholder code (unsupported(...), TODO, NotImplementedError) - blocked tools must be left out, not stubbed")
        placeholders += len(re.findall(r"<SET:", text))
        info[p.name] = {"code_lines": len(pure.splitlines()), "code_cells": len(cells)}
    problems += table_flow_problems(out, files)
    report = {"notebooks": info, "hard_rule_violations": problems,
              "raising_stubs": {"tools": sorted(stubs), "count": len(stubs)},
              "config_placeholders_to_fill": placeholders}
    print(f"Notebooks checked: {len(files)}; code lines: {sum(v['code_lines'] for v in info.values())}")
    print(f"Hard-rule violations: {len(problems)}")
    for pr in problems[:30]:
        print("  -", pr)
    print(f"Tools that raise at run time (blocked/unsupported): {len(stubs)}")
    print(f"Config placeholders '<SET: ...>' to fill before running: {placeholders}")
    if a.json:
        Path(a.json).write_text(json.dumps(report, indent=1), encoding="utf-8")
    return 1 if problems or (a.strict and stubs) else 0


if __name__ == "__main__":
    sys.exit(main())
