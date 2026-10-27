"""Workflow in, notebooks out: parse -> slim -> the migration skill -> 00_config / 01_bronze / 02_silver / 03_gold (.ipynb) and conversion_report.md.

No model, no Databricks API: the input and the output are plain folders (a Unity Catalog Volume is one on Databricks).
A tool the skill cannot convert is never stubbed with code: its cell carries an UPDATE REQUIRED block with a hand-off for a person or another AI.
"""
from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from . import ai as ai_mod, engine, pytool, sqlsrc, templates, versions

WORKFLOW_SUFFIXES = (".yxmd", ".yxzp")


@dataclass
class Result:
    workflow: str
    out_dir: Path
    chunks: int = 0
    converted: int = 0
    unsupported: list = field(default_factory=list)
    files: list = field(default_factory=list)
    sql_tables: int = 0                      # tables that also have Spark SQL (the others say why in their SQL cell)
    ai_tables: list = field(default_factory=list)      # tables written by a model (marked AI-WRITTEN)
    ai_failed: dict = field(default_factory=dict)      # table -> why the model could not write it

    @property
    def open_items(self) -> int:
        return (self.chunks - self.converted) + len(self.unsupported)


def mapping_defaults(inputs: dict, outputs: list, catalog: str, schema: str) -> dict:
    """Every Bronze input and every output table lives in catalog.schema under the skill's table name."""
    base = ".".join(x for x in (catalog, schema) if x)           # no catalog and no schema: bare names, resolved by the current catalog/schema when the notebook runs
    return {"inputs": {k: (f"{base}.{k}" if base else k) for k in sorted(inputs)}, "outputs": {t: (f"{base}.{t}" if base else t) for t in outputs}}


def decision_lines(choices: dict) -> list:
    lines = []
    for tool, slots in ((choices or {}).get("_join_inputs") or {}).items():
        lines.append(f"Tool [{tool}] (Join Multiple): " + ", ".join(f"input {k} comes from tool [{t}]" for k, t in slots.items()) + ".")
    types = (choices or {}).get("_field_types") or {}
    if types:
        lines.append("Field types given by the user (the workflow does not state them): " + ", ".join(f"{f} = {t}" for f, t in types.items()) + ".")
    for tool, c in sorted(((k, v) for k, v in (choices or {}).items() if str(k).isdigit()), key=lambda kv: int(kv[0])):
        if c.get("mode") == "any":
            lines.append(f"Tool [{tool}]: any row may be kept (the order of the incoming rows does not matter).")
        elif c.get("mode") == "order_by" and c.get("fields"):
            lines.append(f"Tool [{tool}]: rows are ordered by {', '.join(c['fields'])} ({'ascending' if c.get('ascending', True) else 'descending'}) before the first one is kept.")
    return lines


def convert_workflow(workflow: Path, out_dir: Path, catalog: str = "", schema: str = "", decisions: dict | None = None, schemas: dict | None = None,
                     schema_lookup=None, model=None, python_port=None, describe=None, ask=None, progress=None, formats=("pyspark", "sql"), prepared=None, tool_notes=None,
                     sql_dialect="", source_catalog="", source_schema="", components=True, python_mode=None) -> Result:
    """Convert one .yxmd / .yxzp into notebooks in out_dir. `decisions` = {tool id: {mode: any | order_by, fields: [...], ascending: bool}}.

    `schemas` = {input table key: [columns] | {column: Spark type}} and/or `schema_lookup(full table name) -> [columns] | {column: type} | None`:
    the columns of the input tables, which database inputs do not carry. Spark SQL needs them; with them the types are known to the PySpark too.
    `python_mode(tool, info) -> "keep" | "port" | None` chooses what to do with an embedded Python tool: keep its ORIGINAL pandas code in a component file (no model), port it to PySpark with the model,
    or leave it as a hand-off. `python_port(tool, info) -> bool` is the older form of "port": it approves porting an embedded Python tool with the model (`describe` = {tool id: what the user says it does}, `ask(question)` puts a question to the user).
    `formats` = which notebooks to write ("pyspark", "sql"); `prepared` = (analysis, handles) from engine.analyze when the caller already looked at the workflow; `tool_notes` = {tool id: what the user says
    that tool must do} (shown in the hand-off and sent to the model).
    `sql_dialect` / `source_catalog` / `source_schema`: the input queries are translated from their SQL dialect to Spark SQL and their tables point to `source_catalog.<dataset or source_schema>.<table>`.
    `model` = an ai.Client: tables that contain a tool the skill cannot convert are then written by that Databricks-served model (marked AI-WRITTEN, to be reviewed).
    """
    workflow, out_dir = Path(workflow), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="alteryx_migrate_"))
    answers = engine.decisions_from_choices(decisions) if decisions else {}
    if prepared is not None:
        analysis, handles = prepared
    else:
        analysis, handles = engine.analyze(workflow, work, decisions=answers or None)
    found = dict(schemas or {})
    if schema_lookup is not None and catalog and schema:           # a lookup needs the full name
        for key in analysis.inputs:
            if key not in found:
                columns = schema_lookup(f"{catalog}.{schema}.{key}")
                if columns:
                    found[key] = columns
    mapper = sqlsrc.table_mapper(source_catalog, source_schema, catalog, schema)
    bronze_sql = {}
    for key, info in analysis.inputs.items():
        if info.get("query"):
            dialect = sqlsrc.dialect_of(info.get("plugin", ""), info.get("connection", ""), sql_dialect)
            bronze_sql[key] = sqlsrc.translate(info["query"], dialect, mapper)
            if key not in found and bronze_sql[key].ok and bronze_sql[key].columns:
                found[key] = bronze_sql[key].columns                       # the columns the query lists are the columns of the Bronze view
    plan_answers = dict(answers)
    if found:
        plan_answers["schemas"] = found
    port_report: dict = {}
    ported_by: dict = {}
    modules: dict = {}
    if python_mode or (model is not None and python_port):
        tools = pytool.load(handles["slim_dir"], handles["wf"])
        for tool, info in tools.items():
            mode = python_mode(tool, info) if python_mode else ("port" if python_port(tool, info) else None)
            if mode == "keep":
                outcome = pytool.keep(info)
                port_report[tool] = {"mode": "keep", "ok": outcome.ok, "failed": outcome.failed, "cells_kept": sorted(outcome.cells)}
                flag, label, who = "kept_python", f"Python tool [{tool}] ({len(outcome.cells)} cells, the original pandas code kept in {outcome.module_name})", "no model: the original code"
            elif mode == "port" and model is not None:
                outcome = pytool.port(model, info, (describe or {}).get(tool, ""), {"in": {n: f"output of tool [{i['stream'][0]}]" for n, i in info.inputs.items() if i["stream"]}},
                                      ask=ask, progress=progress, components=components)
                port_report[tool] = {"mode": "port", "ok": outcome.ok, "calls": outcome.calls, "failed": outcome.failed, "questions_without_answer": [q.get("text") for q in outcome.unanswered],
                                     "notes": outcome.notes, "cells_converted": sorted(outcome.cells)}
                flag, label, who = "ported_python", f"Python tool [{tool}] ({len(outcome.cells)} cells, ported to PySpark)", model.model
            else:
                continue
            if outcome.ok:
                overrides = dict(plan_answers.get("overrides") or {})
                overrides[str(tool)] = {"custom_python": outcome.code, flag: True}
                plan_answers["overrides"] = overrides
                if outcome.module:
                    modules[outcome.module_name] = outcome.module
                ported_by[label] = who
    if plan_answers != answers or (prepared is not None and answers):
        analysis, handles = engine.reanalyze(Path(analysis.workdir), analysis.file_name, plan_answers)
    chunks = [c.to_dict() for c in analysis.chunks]
    maps = mapping_defaults(analysis.inputs, analysis.outputs, catalog, schema)
    codes = {c["id"]: c["code"] for c in chunks if c["strategy"] == "template"}
    statuses = {c["id"]: ("ok" if c["strategy"] == "template" else "unsupported") for c in chunks}
    summary = analysis.summary()
    lines = decision_lines(decisions or {})
    waiting = [u["id"] for c in chunks for u in c["unconverted"]] + [u["id"] for u in summary["unsupported"]]
    details = {n["node_id"]: n for n in engine.slim_nodes(handles["slim_dir"], sorted(set(waiting)))}       # the slimmed settings of every tool that needs an update
    for tid, xml in engine.raw_configs(analysis.workdir, set(waiting) | {t["id"] for c in chunks if c["strategy"] == "llm" for t in c["tools"]}).items():
        details.setdefault(tid, {})["_xml"] = xml                                       # the original settings travel with the hand-off and to a model
    asked = {str(k): v for k, v in (tool_notes or {}).items() if v}
    for tid, text in asked.items():
        details.setdefault(int(tid), {})["_asked"] = text
    ai_info = {"by": dict(ported_by), "errors": {}}
    todo = [c for c in chunks if c["strategy"] == "llm" and ai_mod.eligible(c)]      # a decision (record order ...) is the user's, not the model's
    if model is not None and todo:
        slim = {c["id"]: engine.slim_nodes(handles["slim_dir"], [t["id"] for t in c["tools"]]) for c in todo}
        outcome = ai_mod.write_chunks(model, versions.skill_text(), {**maps["inputs"], **maps["outputs"]}, todo, slim, details)
        for cid, w in outcome.items():
            if w.code:
                codes[cid], statuses[cid] = w.code, "needs review (AI-written)"
                ai_info["by"][cid] = model.model
            else:
                ai_info["errors"][cid] = w.error
    files = templates.build_notebooks(analysis.file_name, chunks, codes, statuses, maps["inputs"], maps["outputs"], summary["inputs"], summary["unsupported"], lines, asked=asked,
                                      details=details, ai=ai_info,
                                      bronze_sql=bronze_sql)
    if "sql" in formats:
        files.update(templates.build_sql_notebooks(analysis.file_name, chunks, statuses, maps["inputs"], maps["outputs"], asked, details, ai_info))
    if "pyspark" not in formats:                                                  # the config and bronze notebooks stay: the SQL needs the table names they document
        for name in templates.LAYER_FILE.values():
            files.pop(name, None)
    meta = {"run_id": "-", "user": "-", "file_name": analysis.file_name, "skill_version": versions.skill_version(), "app_version": "-", "endpoint": getattr(model, "model", "")}
    report = templates.report(meta, chunks, statuses, {}, summary["unsupported"], {c["id"]: 1 for c in chunks if c["id"] in codes}, lines, ai_info)
    written = []
    for name, text in files.items():
        (out_dir / name).write_text(text, encoding="utf-8")
        written.append(name)
    (out_dir / "conversion_report.md").write_text(report, encoding="utf-8")
    if modules:                                                                 # util files: a `utils` package next to the notebooks, used as `from utils import python_tool_17`
        (out_dir / "utils").mkdir(exist_ok=True)
        (out_dir / "utils" / "__init__.py").write_text('"""Util modules of the converted workflow (the Python tools, as files)."""' + chr(10), encoding="utf-8")
        written.append("utils/__init__.py")
    for name, module in modules.items():
        (out_dir / name).write_text(module, encoding="utf-8")
        written.append(name)
    if port_report:
        (out_dir / "python_port.json").write_text(json.dumps(port_report, indent=1, default=str), encoding="utf-8")
        written.append("python_port.json")
    (out_dir / "unconverted_tools.json").write_text(json.dumps(summary["unsupported"], indent=1), encoding="utf-8")
    written += ["conversion_report.md", "unconverted_tools.json"]
    return Result(analysis.file_name, out_dir, len(chunks), sum(1 for s in statuses.values() if s == "ok"), summary["unsupported"], sorted(written),
                  sum(1 for c in chunks if c.get("sql")), sorted(ai_info["by"]), dict(ai_info["errors"]))


def find_workflows(path: Path) -> list:
    """A single file, or every .yxmd / .yxzp in a folder (a Volume folder, for example)."""
    path = Path(path)
    if path.is_file():
        return [path]
    return sorted(p for p in path.iterdir() if p.is_file() and p.suffix.lower() in WORKFLOW_SUFFIXES)


def load_decisions(path: str | Path | None) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8")) if path else {}
