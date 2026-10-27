"""Assemble Databricks notebooks (.ipynb by default), the migration report and the coverage numbers from a plan.

Layout: 00_config, 01_bronze (input tables, checked - not ingested), 02_silver, 03_gold, 04_quality_reconciliation,
All code is plain PySpark: no helper module is generated or imported.

Conversion is all-or-nothing per tool: a blocked tool (and everything downstream of it) is NOT converted and no placeholder
code is written for it; the tools upstream of the blockers are converted and materialised so work can resume when the missing
input arrives."""
from __future__ import annotations

import json
import os
import types
import pprint
import re
from collections import Counter

import codegen as C
import containers as CT
import preflight as PF
import unsupported_xml as UX
import wfmodel as M

HERE = os.path.dirname(os.path.abspath(__file__))
IMPORTS = "from pyspark.sql import functions as F, Window"


# ----------------------------------------------------------------------------- cells and rendering
def md(text):
    return ("markdown", text.strip("\n"))


def code(text):
    return ("code", text.strip("\n"))


def render(cells, fmt="ipynb"):
    """Cells -> notebook text. ipynb = Jupyter/Databricks notebook JSON; py = Databricks source format."""
    if fmt == "ipynb":
        def src(text):
            lines = text.split("\n")
            return [ln + "\n" for ln in lines[:-1]] + [lines[-1]]
        nb = {"cells": [({"cell_type": "markdown", "metadata": {}, "source": src(t)} if k == "markdown" else
                         {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": src(t)})
                        for k, t in cells],
              "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                           "language_info": {"name": "python"}},
              "nbformat": 4, "nbformat_minor": 4}
        return json.dumps(nb, indent=1, ensure_ascii=False) + "\n"
    out = []
    for k, t in cells:
        if k == "markdown":
            out.append("\n".join("# MAGIC " + ln if ln else "# MAGIC" for ln in ("%md\n" + t).splitlines()))
        else:
            out.append("\n".join("# MAGIC " + ln if ln.startswith("%") else ln for ln in t.splitlines()))
    return "# Databricks notebook source\n" + "\n# COMMAND ----------\n\n".join(out) + "\n"


def indent(lines, n=4):
    pad = " " * n
    return [pad + part if part else part for ln in lines for part in ln.split("\n")]


def one(t, n=200):
    return C.one_line(t, n)


# ----------------------------------------------------------------------------- config and bronze
def table_layers(units, bronze_used):
    layers = {"bronze": sorted(bronze_used), "silver": [], "gold": []}
    for u in units:
        for t in u.table.values():
            layers["gold" if t.startswith("gold_") else "silver"].append(t)
    return layers


def banner(summary):
    if summary["tools_waiting"] or summary["tools_blocked"]:
        return (f"**PARTIAL CONVERSION - NOT PRODUCTION-READY.** {summary['tools_converted']} of {summary['tools_live_data']} tools "
                f"({summary['converted_pct']}%) are converted; {summary['tools_blocked']} are blocked and {summary['tools_waiting']} "
                "wait for them. Nothing here is validated against the Alteryx output. See MIGRATION_REPORT.md and PREFLIGHT.md.")
    return ("**Converted, not yet validated.** All tools on a path to an output are converted; the result has not been "
            "compared with the Alteryx output (see MIGRATION_REPORT.md).")


def config_notebook(wf, units, bronze_used, comments, project, summary):
    layers = table_layers(units, bronze_used)
    context = "\n".join("> " + ln for c in comments for ln in str(c).splitlines() if ln.strip())
    name = M.snake(os.path.basename(str(wf.meta.get("file_path") or "workflow")))
    registry = "T = {\n    name: fq(layer, name)\n    for layer, names in {\n" + "".join(
        f"        {layer!r}: [\n" + "".join(f"            {t!r},\n" for t in names) + "        ],\n"
        for layer, names in layers.items() if names) + "    }.items()\n    for name in names\n}"
    ansi = project.get("ansi")
    return [
        md(f"# Config - {name}\n{banner(summary)}\n\nTable names and run settings for the Bronze -> Silver -> Gold notebooks "
           f"(generated from `{wf.meta.get('file_path')}`)." + (f"\n\nContext from the workflow:\n{context}" if context else "")),
        code(f'''import os


def cfg(name, default):
    """Notebook widget on Databricks, MIG_<NAME> environment variable elsewhere ('-' = empty)."""
    try:
        dbutils.widgets.text(name, default)
        val = dbutils.widgets.get(name)
    except NameError:
        val = os.environ.get("MIG_" + name.upper(), default)
    return "" if val == "-" else val


CATALOG = cfg("catalog", {project["catalog"]!r})
SCHEMAS = {{"bronze": cfg("bronze_schema", {project["bronze_schema"]!r}), "silver": cfg("silver_schema", {project["silver_schema"]!r}),
           "gold": cfg("gold_schema", {project["gold_schema"]!r})}}
WRITE_MODE = {project["write_mode"]!r}
ALLOW_OVERWRITE = cfg("allow_overwrite", {str(project["allow_overwrite"]).lower()!r}).lower() == "true"   # nothing is overwritten unless enabled
TABLE_FORMAT = cfg("table_format", "delta")
AUDIT_COLS = ("_ingested_at", "_batch_id", "_source_system")     # Bronze audit columns never reach business outputs

TIMEZONE = cfg("timezone", {project["timezone"]!r})            # date functions (today / now / first of month) use this zone
spark.conf.set("spark.sql.session.timeZone", TIMEZONE)
ANSI = cfg("ansi", {("true" if ansi else "false") if ansi is not None else ""!r})                    # "" = keep the cluster setting
if ANSI:
    spark.conf.set("spark.sql.ansi.enabled", ANSI)


def fq(layer, table):
    return ".".join(p for p in (CATALOG, SCHEMAS[layer], table) if p)


def write_table(df, table, mode=None):
    """Guarded batch write: overwriting needs allow_overwrite=true; Delta history keeps the previous version."""
    mode = mode or WRITE_MODE
    if mode == "overwrite" and not ALLOW_OVERWRITE:
        raise PermissionError(f"Refusing to overwrite {{table}}: set allow_overwrite=true for this run")
    if TABLE_FORMAT == "tempview":      # local tests only: no metastore needed
        df.createOrReplaceGlobalTempView(table.split(".")[-1])
    else:
        df.write.format(TABLE_FORMAT).mode(mode).saveAsTable(table)'''),
        code(registry),
    ]


def bronze_notebook(plan, bronze_used, waiting_bronze):
    def rows(keys):
        out = ["| Bronze table | Source | Original query |", "|---|---|---|"]
        for k in sorted(keys):
            v = plan.bronze[k]
            q = re.sub(r"\s+", " ", (v["query"] or "").strip().replace("|", "/"))
            out.append(f"| `{k}` | {', '.join(v['source_tables']) or v['file_path'] or v['connection']} | "
                       f"{('`' + q[:160] + ('...' if len(q) > 160 else '') + '`') if q else ''} |")
        return "\n".join(out)
    text = ("# Bronze - input tables\nThe workflow's sources are expected as existing tables (nothing is ingested here). "
            "Each table should hold what the Alteryx input tool read:\n\n" + rows(bronze_used))
    if waiting_bronze:
        text += "\n\nInputs of tools that are still waiting for input (not used yet):\n\n" + rows(waiting_bronze)
    return [md(text), code("%run ./00_config"),
            code(f"missing = [T[k] for k in {sorted(bronze_used)!r} if not spark.catalog.tableExists(T[k])]\n"
                 "if missing:\n    raise RuntimeError('Input tables not found: ' + ', '.join(missing))")]


# ----------------------------------------------------------------------------- units
def grain_of(em, u):
    """Describe grain/keys only where the workflow states them (group-by / unique keys)."""
    wf = em.wf
    n = u.node
    for _ in range(50):
        k = wf.kind(n)
        node = wf.nodes[n]
        if k == "summarize":
            keys = [a["field_name"] for a in node.get("aggregations", []) if a["action"].lower() == "groupby"]
            return f"one row per ({', '.join(keys)})" if keys else "single aggregate row"
        if k == "unique":
            return f"one row per ({', '.join(node.get('key_fields', []))}) (keep-first)"
        if k == "crosstab":
            return f"one row per ({', '.join(node.get('group_fields', []))})"
        if k in ("select", "sort", "formula", "filter", "multifield", "multirow", "text2cols", "recordid", "runningtotal"):
            ups = wf.producers(n)
            if not ups:
                break
            n = ups[0].src
            continue
        break
    return "not stated in the workflow"


def key_cols_of(em, u, stream):
    wf, n = em.wf, stream[0]
    for _ in range(50):
        k, node = wf.kind(n), wf.nodes[n]
        if k == "summarize":
            return [a["field_name"] for a in node.get("aggregations", []) if a["action"].lower() == "groupby"]
        if k == "unique":
            return list(node.get("key_fields", []))
        if k in ("select", "sort", "formula", "filter", "multifield", "multirow"):
            ups = wf.producers(n)
            if not ups:
                break
            n = ups[0].src
            continue
        break
    return []


def unit_cell(em, plan, u):
    """One code cell per unit: the build function and the call that runs and writes it."""
    wf = em.wf
    name = f"build_{u.key}"
    body = []
    for nid in u.nodes:
        if wf.kind(nid) == "read":
            continue
        body += indent(em.results[nid].lines)
    outs = ", ".join(f"{u.table[st]!r}: {em.var(st)}" for st in u.streams)
    text = "\n".join(body) + "\n" + outs
    reads = []
    for key in sorted({plan.cut_streams.get(st) or plan.bronze_of.get(st[0]) for st in u.inputs} - {None}):
        if re.search(rf"\b{re.escape(key)}\b", text):                       # only tables the unit actually uses
            reads.append(f"    {key} = spark.table(T[{key!r}]).drop(*AUDIT_COLS)")
    for st in u.streams:
        if wf.kind(st[0]) == "read":              # a Read wired straight to an output: copy of the Bronze table
            reads.append(f"    {em.var(st)} = spark.table(T[{plan.bronze_of[st[0]]!r}]).drop(*AUDIT_COLS)")
    fn = "\n".join([f"def {name}(spark):"] + reads + body + [f"    return {{{outs}}}"])
    return fn + f"\n\n\nfor table, df in {name}(spark).items():\n    write_table(df, T[table])"


def unit_heading(em, plan, u):
    ins = sorted({plan.cut_streams.get(s) or plan.bronze_of.get(s[0]) for s in u.inputs} - {None})
    grain = grain_of(em, u)
    line = f"from {', '.join('`' + i + '`' for i in ins)}" if ins else "from the workflow's own data"
    return md(f"### `{u.key}`\n{line}" + (f" - {grain}" if grain != "not stated in the workflow" else ""))


def tool_label(wf, nid):
    return f"{wf.nodes[nid].get('original_tool_type') or wf.nodes[nid]['node_type']}"


def flow_notebook(em, plan, wf, cinfo, kept, bronze_used):
    """One notebook for the whole workflow: a section (markdown + one code cell) per Alteryx container, in the order the data flows.
    Tool ids stay in comments so a step can be found on the Alteryx canvas. Every tool is written once; tables are written where a
    container produces them. -> (cells, sections)"""
    nodes, seen = [], set()
    for u in kept:
        for n in u.nodes:
            if n not in seen and wf.kind(n) != "read":
                seen.add(n)
                nodes.append(n)
    inputs = sorted({st for u in kept for st in u.inputs if st[0] in plan.bronze_of})
    em.set_unit(types.SimpleNamespace(inputs=inputs, nodes=nodes), flow=True)
    secs = CT.sections(cinfo, wf, nodes)
    produced = {}                                           # node id -> [(stream, table)] persisted right after it
    for u in kept:
        for st, tbl in u.table.items():
            if st not in [x for v in produced.values() for x, _ in v]:
                produced.setdefault(st[0], []).append((st, tbl))
    cells, rows = [], []
    src_lines = [f"{k} = spark.table(T[{k!r}]).drop(*AUDIT_COLS)" for k in bronze_used]
    for u in kept:                                          # an input wired straight to an output is a copy of the Bronze table
        for st, tbl in u.table.items():
            if st[0] in plan.bronze_of:
                src_lines.append(f"write_table({plan.bronze_of[st[0]]}, T[{tbl!r}])")
    for sec in secs:
        body, kinds, written = [], [], []
        for n in sec["nodes"]:
            r = em.emit(n)
            lines = [ln for ln in r.lines if ln.strip()]
            if lines:
                body.append(f"# [{n}] {tool_label(wf, n)}")
                body += lines
                kinds.append(f"{tool_label(wf, n)} [{n}]")
            for st, tbl in produced.get(n, []):
                body.append(f"write_table({em.var(st)}, T[{tbl!r}])")
                written.append(tbl)
        if not body:
            continue
        layer = "Gold" if any(t.startswith("gold_") for t in written) else "Silver"      # a section that writes a gold_ table is Gold; the rest cleanses / integrates
        if sec["container"]:
            label = f"Container {sec['container']} · {sec['caption']}" + (f" (part {sec['part']})" if sec["part"] > 1 else "")
        else:
            label = f"Other tools {sec['part']} · " + (", ".join(written) if written else f"{len(kinds)} tools")
        chain = " → ".join(kinds[:10]) + (f" → ... ({len(kinds)} tools)" if len(kinds) > 10 else "")
        note = f"\n\nWrites: {', '.join('`' + t + '`' for t in written)}" if written else ""
        what = "business-ready output" if layer == "Gold" else "cleansed / integrated data"
        cells += [md(f"### {label}\n**Layer: {layer}** ({what})\n\n{chain}{note}"),
                  code(f"# {label}\n# Layer: {layer}\n" + "\n".join(body))]      # the first line stays the container heading (the verifier finds cells by it)
        rows.append((label, len(kinds), written, layer))
    head = ["# Silver and Gold - one section per Alteryx container, in the order the data flows",
            "Tool ids such as `[38]` are kept in the comments so you can find the same tool in Alteryx. Variables are shared between "
            "the cells, so run them top to bottom.", "",
            "| # | Section | Layer | Tools | Tables written |", "|---|---|---|---|---|"]
    head += [f"| {i} | {label} | {layer} | {n} | {', '.join('`' + t + '`' for t in w) or '-'} |" for i, (label, n, w, layer) in enumerate(rows, 1)]
    top = [md("\n".join(head)), code("%run ./00_config"), code(IMPORTS + ("\nfrom pyspark.sql.types import StringType" if any(
        "StringType" in c for k, c in cells if k == "code") else ""))]
    top += [md("### Sources\nThe input tables (read as they are; nothing is ingested here)."), code("\n".join(src_lines) or "pass")]
    return top + cells, rows


def layer_notebook(em, plan, units, title, cell_of):
    imports = IMPORTS + ("\nfrom pyspark.sql.types import StringType" if any("StringType" in cell_of[u.key] for u in units) else "")
    cells = [md(f"# {title}"), code("%run ./00_config"), code(imports)]
    for u in units:
        cells += [unit_heading(em, plan, u), code(cell_of[u.key])]
    return cells


def quality_notebook(em, plan, units):
    specs = {}
    for u in units:
        for st, t in u.table.items():
            specs[t] = dict(keys=key_cols_of(em, u, st))
    return [
        md("# Quality checks and reconciliation\nStructural checks for every table written (rows, duplicate keys where the workflow "
           "defines a grain), and row-level comparison with outputs exported from the original Alteryx run "
           "(`EXPECTED_BASELINES`). With no baseline, reconciliation is reported as NOT RUN - equivalence is then unproven."),
        code("%run ./00_config"),
        code(IMPORTS + "\n\nDQ_SPECS = " + pprint.pformat(specs, width=110, sort_dicts=False)),
        code('''results = []
for table, spec in DQ_SPECS.items():
    df = spark.table(T[table])
    keys = [k for k in spec["keys"] if k.lower() in {c.lower() for c in df.columns}]
    rows = df.count()
    duplicate_keys = df.groupBy(*[F.col(f"`{k}`") for k in keys]).count().filter("count > 1").count() if keys else 0
    results.append({"table": table, "rows": rows, "duplicate_keys": duplicate_keys, "passed": rows > 0 and duplicate_keys == 0})
failed = [r for r in results if not r["passed"]]
print(f"{len(results) - len(failed)}/{len(results)} tables passed")
for r in failed:
    print("FAILED", r)'''),
        code('''# {gold table: "catalog.schema.table holding the Alteryx output"}
EXPECTED_BASELINES = {}
recon = []
for table, baseline in EXPECTED_BASELINES.items():
    actual, expected = spark.table(T[table]), spark.table(baseline)
    common = [c for c in expected.columns if c.lower() in {a.lower() for a in actual.columns}]
    only_actual = actual.select(*common).exceptAll(expected.select(*common)).count()
    only_expected = expected.select(*common).exceptAll(actual.select(*common)).count()
    missing = [c for c in expected.columns if c not in common]
    recon.append({"check": table, "only_in_actual": only_actual, "only_in_expected": only_expected, "missing_columns": missing,
                  "passed": only_actual == 0 and only_expected == 0 and not missing})
if not EXPECTED_BASELINES:
    print("NOT RUN: no Alteryx baseline configured - converted, not yet validated")
for r in recon:
    print("PASS" if r["passed"] else "FAIL", r["check"], {k: v for k, v in r.items() if k.startswith("only_")})
assert not failed and all(r["passed"] for r in recon), "quality or reconciliation failures - see output above"'''),
    ]


def job_definition(name, nbs, partial, base="/Workspace/Repos/<repo>/notebooks"):
    tasks, prev = [], None
    for nb in nbs:
        key = re.sub(r"\.(py|ipynb)$", "", nb)
        t = {"task_key": key, "notebook_task": {"notebook_path": f"{base}/{key}", "base_parameters": {}}}
        if prev:
            t["depends_on"] = [{"task_key": prev}]
        tasks.append(t)
        prev = key
    note = "Compute (serverless or job cluster), schedule, notifications and retries are deployment decisions"
    if partial:
        note = "PARTIAL CONVERSION - not production-ready: some tools are waiting for input (see MIGRATION_REPORT.md). " + note
    return {"name": name, "max_concurrent_runs": 1, "tasks": tasks, "_note": note}


def placement(units):
    """Notebook placement per unit: its own layer, pulled earlier when a Silver unit consumes it (run-order safety)."""
    place = {u.key: u.layer for u in units}
    producer = {st: u.key for u in units for st in u.streams}
    changed = True
    while changed:
        changed = False
        for u in units:
            if place[u.key] == "silver":
                for st in u.inputs:
                    d = producer.get(st)
                    if d and place[d] == "gold":
                        place[d] = "silver"
                        changed = True
    return place


# ----------------------------------------------------------------------------- classification and pre-flight
def classify(wf, plan, options=None):
    """Emit every tool once (no unit context) to learn its status; return the classification used by pre-flight and generation."""
    em = C.Emitter(wf, plan, options or {})
    for n in wf.topo:
        em.emit(n)
    info = M.analyse(wf)
    live_data = {n for n in info["live"] if wf.kind(n) not in M.NO_DATA_KINDS}
    blocked, waiting = PF.classify(wf, em, live_data)
    return em, info, live_data, blocked, waiting


def run_preflight(wf, plan, options=None, answers=None):
    """The pre-flight scan: returns (pre, message_markdown)."""
    em, info, live_data, blocked, waiting = classify(wf, plan, options)
    pre = PF.build(wf, plan, em, blocked, waiting, info, answers)
    return pre, PF.render_message(pre, wf, len(live_data)), PF.answers_template(pre, wf)


# ----------------------------------------------------------------------------- orchestration
def plan_with_ready_cuts(wf, plan, options=None):
    """The plan generate() really uses: materialise the last convertible step before every blocked or waiting tool, so everything
    upstream of it can be converted now. Returns (plan, (em, info, live_data, blocked, waiting))."""
    classified = classify(wf, plan, options)
    _, _, live_data, blocked, waiting = classified
    bad = set(blocked) | set(waiting)
    if bad:
        extra = {}
        for e in wf.edges:
            if e.dst in bad and e.src in live_data and e.src not in bad and wf.kind(e.src) != "read":
                extra[(e.src, e.src_anchor)] = "ready: feeds a tool that is waiting for input"
        plan = M.plan_medallion(wf, extra_cuts=extra, **plan.opts)
    return plan, classified


def generate(wf, plan, outdir, opts=None, test_results=None):
    opts = opts or {}
    fmt = opts.get("format", "ipynb")
    ext = "." + fmt
    answers = opts.get("answers") or {}
    project = PF.project_settings(answers)
    options = {"text_match_case_insensitive": project["text_match_case_insensitive"], "ansi": project.get("ansi"), "null_safe_join": project.get("null_safe_join"),
               "field_types": {str(k).lower(): str(v).lower() for k, v in (answers.get("field_types") or {}).items()},
               "join_inputs": dict(answers.get("join_inputs") or {})}
    os.makedirs(outdir, exist_ok=True)
    plan, (em0, info, live_data, blocked, waiting) = plan_with_ready_cuts(wf, plan, options)
    bad = set(blocked) | set(waiting)
    em = C.Emitter(wf, plan, options)
    kept = [u for u in plan.units if not (set(u.nodes) & bad)]
    dropped = [u for u in plan.units if set(u.nodes) & bad]
    for n in wf.topo:
        if wf.kind(n) == "read":
            em.emit(n)
    cinfo = CT.read(wf.yxmd) if (wf.yxmd and opts.get("layout", "auto") != "units") else None
    flow = bool(cinfo and cinfo["caption"] and kept)          # containers found: write the notebook container by container
    bronze_flow = sorted({plan.bronze_of[st[0]] for u in kept for st in list(u.inputs) + list(u.streams) if st[0] in plan.bronze_of})
    cell_of, flow_cells = {}, None    # a cheap step can sit in several units, so each unit's code is captured as it is emitted
    if flow:
        flow_cells, _ = flow_notebook(em, plan, wf, cinfo, kept, bronze_flow)
    else:
        for u in kept:
            em.set_unit(u)
            for n in u.nodes:
                if wf.kind(n) != "read":
                    em.emit(n)
            cell_of[u.key] = unit_cell(em, plan, u)
    em.set_unit(None)
    for n in wf.topo:
        if n not in em.results:
            em.emit(n)
    cov = coverage(wf, plan, em, info, live_data, blocked, waiting, kept, dropped, test_results)
    pre = PF.build(wf, plan, em0, blocked, waiting, info, answers)
    comments = [wf.nodes[n].get("comment_text") for n in wf.topo if wf.kind(n) == "comment" and wf.nodes[n].get("comment_text")]
    bronze_used = sorted({plan.bronze_of[st[0]] for u in kept for st in list(u.inputs) + list(u.streams)
                          if st[0] in plan.bronze_of})
    waiting_bronze = sorted(set(plan.bronze) - set(bronze_used))
    place = placement(kept)
    silver = [u for u in kept if place[u.key] == "silver"]
    gold = [u for u in kept if place[u.key] == "gold"]
    files = {}
    nbs = {"00_config": config_notebook(wf, kept, bronze_used, comments, project, cov["summary"]),
           "01_bronze": bronze_notebook(plan, bronze_used, waiting_bronze),
           "02_silver_gold": flow_cells,
           "02_silver": layer_notebook(em, plan, silver, "Silver - cleansed and integrated data", cell_of) if not flow else None,
           "03_gold": layer_notebook(em, plan, gold, "Gold - business-ready outputs", cell_of) if not flow else None,
           "04_quality_reconciliation": quality_notebook(em, plan, kept)}
    for name, cells in nbs.items():
        if cells is None:
            continue
        if name in ("02_silver", "03_gold") and not (silver if name == "02_silver" else gold):
            continue
        if name == "04_quality_reconciliation" and not kept:
            continue
        files[name + ext] = render(cells, fmt)
    order = [f for f in sorted(files) if f[0].isdigit() and not f.startswith("00_")]
    partial = bool(bad)
    files["databricks_job.json"] = json.dumps(
        job_definition(M.snake(os.path.basename(wf.meta.get("file_path") or "workflow")), order, partial), indent=2)
    unsupported = [(wf.nodes[n], (em0.results[n].reviews or [""])[0]) for n in wf.topo
                   if em0.results[n].status == "unsupported" or wf.nodes[n].get("converted_from")]
    if unsupported:       # keep every unsupported (or re-interpreted) tool, unchanged, as XML
        files["unsupported_tools.xml"] = UX.tools_xml(unsupported)
    files["coverage.json"] = json.dumps(cov, indent=2, ensure_ascii=False, default=str)
    files["tables.json"] = json.dumps(table_plan(plan, kept, dropped, place, project), indent=1, ensure_ascii=False, default=str)
    files["PREFLIGHT.md"] = PF.render_message(pre, wf, len(live_data))
    files["preflight.json"] = json.dumps(pre, indent=1, ensure_ascii=False, default=str)
    from report import build_report
    files["MIGRATION_REPORT.md"] = build_report(wf, plan, em, info, cov, comments, pre, project, kept, dropped, answers)
    for fn, txt in files.items():
        with open(os.path.join(outdir, fn), "w", encoding="utf-8", newline="\n") as fh:
            fh.write(txt)
    return em, cov, sorted(files)


def table_plan(plan, kept, dropped, place, project):
    """Machine-readable table plan (read by tools that continue the migration, e.g. for code the generator cannot convert)."""
    unit = lambda u, layer, status: {"key": u.key, "layer": layer, "status": status, "node": u.node, "nodes": list(u.nodes),
                                     "streams": [list(st) for st in u.streams],
                                     "tables": {f"{n}:{a}": t for (n, a), t in u.table.items()}}
    return {"project": {k: project[k] for k in ("catalog", "bronze_schema", "silver_schema", "gold_schema", "write_mode", "allow_overwrite")},
            "bronze": {k: {"nodes": v["nodes"], "source_tables": v["source_tables"]} for k, v in plan.bronze.items()},
            "bronze_of": {str(n): k for n, k in plan.bronze_of.items()},
            "units": [unit(u, place[u.key], "converted") for u in kept] + [unit(u, u.layer, "waiting") for u in dropped],
            "cut_streams": {f"{n}:{a}": t for (n, a), t in plan.cut_streams.items()}}


KIND_TESTS = {   # tool kind -> tests (in tests/) that must have passed for the kind to count as test-verified
    "filter": ["test_filter_null_goes_to_false_output"], "select": ["test_select_rename_retype_order"],
    "sort": ["test_sort_null_ordering"], "join": ["test_join_outputs_and_null_keys"],
    "union": ["test_union_by_name_allow_missing"], "summarize": ["test_summarize_actions"],
    "unique": ["test_unique_split_keeps_first_in_order"], "append": ["test_append_fields_crossjoin"],
    "text2cols": ["test_text_to_columns"], "transpose": ["test_transpose"], "recordid": ["test_record_id_ordered"],
    "runningtotal": ["test_running_total"], "crosstab": ["test_crosstab_pivot"], "literal": ["test_literal_data_types"],
    "formula": ["test_expression_semantics"], "multifield": ["test_expression_semantics"],
    "multirow": ["test_multirow_lag_over_explicit_sort"], "countrecords": ["test_count_records"], "autofield": ["test_auto_field_passes_rows_through"],
}


def verified_kinds(test_results):
    ok = set()
    if not test_results:
        return ok
    for kind, names in KIND_TESTS.items():
        rel = {k: v for k, v in test_results.items() if any(n in k for n in names)}
        if rel and all(v == "passed" for v in rel.values()):
            ok.add(kind)
    return ok


def coverage(wf, plan, em, info, live_data, blocked, waiting, kept, dropped, test_results=None):
    vk = verified_kinds(test_results)
    live = info["live"]
    in_kept = {n for u in kept for n in u.nodes}
    reads_used = {n for n in live_data if wf.kind(n) == "read" and any(e.dst in in_kept for e in wf.consumers(n))}
    kept_streams = {st for u in kept for st in u.streams}
    writers = {n for n in live_data if wf.kind(n) == "write" and wf.producers(n)
               and all((e.src, e.src_anchor) in kept_streams for e in wf.producers(n))}     # the Delta write replaces the Write tool
    converted = (in_kept | reads_used | writers) & live_data
    unit_of = {}
    for u in kept:
        for n in u.nodes:
            unit_of.setdefault(n, u.key)
    rows = []
    for n in wf.topo:
        r = em.results[n]
        kind = wf.kind(n)
        status = "waiting" if n in waiting else r.status
        rows.append(dict(tool_id=n, tool_type=wf.nodes[n].get("original_tool_type"), node_type=wf.nodes[n]["node_type"],
                         status=status, live=n in live, converted=n in converted, unit=unit_of.get(n),
                         variable=em.names.get((n, "Output")), reviews=r.reviews if n not in waiting else
                         [f"waiting for tool(s) {', '.join('#' + str(b) for b in sorted(waiting[n])[:4])}"],
                         waiting_on=sorted(waiting.get(n, [])), notes=r.notes, original=[one(t, 300) for t in r.orig[:3]],
                         test_verified_kind=kind in vk and r.status == "high_confidence" and n in converted))
    def is_output_tool(n):          # e.g. BigQuery Output: loading, not transformation
        return wf.kind(n) == "unsupported" and "output" in str(wf.nodes[n].get("original_plugin_name") or "").lower() and not wf.consumers(n)
    transform = {n for n in live_data if wf.kind(n) not in ("read", "write", "literal", "browse", "comment") and not is_output_tool(n)}
    transform_done = transform & converted
    total_tools = len(wf.tools) or len(wf.nodes)
    stub_kinds = {"unsupported", "spatial", "predictive"}
    mapped = sum(1 for x in rows if wf.kind(x["tool_id"]) not in stub_kinds and hasattr(C.Emitter, "e_" + wf.kind(x["tool_id"])))
    n_live = len(live_data)
    done = sum(1 for n in converted)
    summary = {
        "tools_identified": total_tools, "ir_nodes": len(wf.nodes), "visual_containers_not_migrated": len(wf.containers),
        "tools_live_data": n_live, "tools_converted": done, "converted_pct": round(100.0 * done / n_live, 1) if n_live else 100.0,
        "tools_transformation": len(transform), "tools_transformation_converted": len(transform_done),
        "transformation_converted_pct": round(100.0 * len(transform_done) / len(transform), 1) if transform else 100.0,
        "tools_blocked": len(blocked), "tools_waiting": len(waiting),
        "tools_mapped": mapped,
        "tools_not_applicable": sum(1 for x in rows if x["status"] == "not_applicable"),
        "tools_high_confidence": sum(1 for x in rows if x["converted"] and x["status"] == "high_confidence"),
        "tools_requires_review": sum(1 for x in rows if x["converted"] and x["status"] == "requires_review"),
        "tools_unsupported": sum(1 for x in rows if x["status"] == "unsupported"),
        "tools_blocked_missing_config": sum(1 for x in rows if x["status"] == "blocked"),
        "tools_verified_by_skill_tests": sum(1 for x in rows if x["test_verified_kind"]),
        "tools_verified_on_workflow_data": 0,
        "tools_not_generated_dead_end": sum(1 for x in rows if not x["live"] and x["status"] != "not_applicable"),
        "tables_converted": sum(len(u.table) for u in kept), "tables_waiting": sum(len(u.table) for u in dropped),
        "tools_recovered_from_xml": sum(wf.recovered.values()) if wf.recovered else 0,
        "unresolved_dependencies": len(info["unresolved"]) + len(info["missing_inputs"]),
        "validation_status": "not run",
        "validation_failures": (sum(1 for v in (test_results or {}).values() if v == "failed") if test_results else None),
        "tests_executed": bool(test_results),
    }
    return {"summary": summary, "tools": rows}
