"""Plain-PySpark functions, one per output table, for tools that package the conversion themselves (for example the datalab bridge).

The conversion is the skill's own: the same pre-flight, planner and emitters as the notebooks. What this module returns is the code of
each unit WITHOUT any notebook wiring: the lines that build the table from its input tables, and the variable holding the result.
No table is written here and nothing is a placeholder: a unit with a blocked or waiting tool is not returned (it is listed in ``waiting``).

    from unit_functions import build
    result = build(wf, plan, answers)           # wf, plan from wfmodel; answers as for generate_notebooks.py
    result["tables"]   -> [{"table", "layer", "unit", "inputs", "body", "result", "tools", "steps", "reviews", "notes", "sql"}]
                          sql = {"statement": "CREATE TABLE @@out:<table>@@ AS WITH ... SELECT ...", "columns": [...]} or {"statement": None, "error": why}  (references/13-spark-sql-output.md)
                          steps = [{"tool", "label", "lines"}] per Alteryx tool in flow order (body is the same lines, joined)
    result["waiting"]  -> [{"unit", "tables", "tools", "why"}]   (tools = every tool of the unit that is blocked or waits on a blocked one)
    result["blocked"]  -> {tool id: reason}  the root causes only; output tools without a converter are listed apart (outputs_without_converter)
    result["coverage"] -> the skill's coverage dict (same as coverage.json)
    result["bronze"]   -> {bronze key: {"source_tables", "query", "nodes"}}
"""
import re

import codegen as C
import notebooks as NB
import preflight as PF
import sqlframe as SQ
import wfmodel as M


def _layer(table):
    return "gold" if table.startswith("gold_") else "silver"


ALTERYX_TYPE = {"string": "V_WString", "varchar": "V_WString", "char": "V_WString", "int": "Int32", "integer": "Int32", "bigint": "Int64", "long": "Int64",
                "smallint": "Int16", "short": "Int16", "tinyint": "Byte", "byte": "Byte", "double": "Double", "float": "Float", "real": "Float",
                "boolean": "Bool", "date": "Date", "timestamp": "DateTime", "timestamp_ntz": "DateTime"}


def alteryx_type(spark_type):
    """The Alteryx type name for a Spark column type ('' when there is none: the column is then of unknown type, as for any database input)."""
    t = str(spark_type or "").strip().lower()
    if t.startswith("decimal") or t.startswith("numeric"):
        return "FixedDecimal"
    return ALTERYX_TYPE.get(t.split("(")[0], "")


def apply_schemas(wf, plan, schemas):
    """answers["schemas"] = {bronze key: [column names] | {column name: Spark type}}: the columns of inputs whose workflow stores none (database inputs).
    They are stored as the input tool's field list, so the PySpark emitters (type-dependent choices such as '+' on text or numbers) and the SQL
    both see them. Types are optional; without them the columns are of unknown type, exactly as before."""
    for key, spec in (schemas or {}).items():
        if key not in plan.bronze or not spec:
            continue
        items = list(spec.items()) if isinstance(spec, dict) else [(n, "") for n in spec]
        for nid in plan.bronze[key]["nodes"]:
            known = {str(f.get("name", "")).lower(): f for f in wf.nodes[nid].get("field_info") or [] if f.get("type")}     # types the workflow states (MetaInfo)
            wf.nodes[nid]["field_info"] = [{"name": n, **({"type": alteryx_type(t)} if alteryx_type(t) else
                                                           {k: v for k, v in known.get(n.lower(), {}).items() if k != "name"})} for n, t in items]


def bronze_columns(wf, plan, key):
    """Column names (original case, in order) of a Bronze input, from the field list the workflow (or answers.schemas) gives its input tool."""
    for nid in plan.bronze[key]["nodes"]:
        names = [f["name"] for f in wf.nodes[nid].get("field_info") or [] if f.get("name")]
        if names:
            return names
    return None


def attach_sql(wf, plan, tables):
    """Add ``t["sql"]`` to every table: the Spark SQL of the same conversion, made by running the table's PySpark steps on a symbolic DataFrame.

    ``t["sql"] = {"statement": "CREATE TABLE @@out:<table>@@ AS WITH ... SELECT ...", "columns": [...]}`` or ``{"statement": None, "error": why}``.
    Tables are written as ``@@in:<bronze key>@@`` / ``@@out:<table>@@`` so the caller can put the real catalog.schema names in.
    A table whose SQL cannot be made keeps its PySpark; nothing is guessed.
    """
    known, failed, by_unit = {}, set(), {}
    produced = {t["table"] for t in tables}
    for t in tables:
        by_unit.setdefault(t["unit"], []).append(t)
    pending = list(by_unit.items())
    while pending:
        progressed = False
        for unit, group in list(pending):
            keys = sorted({k for t in group for k in t["inputs"]})
            if any(k in produced and k not in known and k not in failed for k in keys):
                continue                                           # an earlier table it reads is not done yet
            pending.remove((unit, group))
            progressed = True
            try:
                inputs = {}
                for k in keys:
                    if k in plan.bronze:
                        cols = bronze_columns(wf, plan, k)
                        if cols is None:
                            raise SQ.SqlError(f"the columns of the input table {k!r} are not in the workflow, and SQL needs them (give answers.schemas." + k + ")")
                        inputs[k] = (f"@@in:{k}@@", cols)
                    elif k in known:
                        inputs[k] = (f"@@out:{k}@@", known[k])
                    else:
                        raise SQ.SqlError(f"it reads {k!r}, which has no SQL")
                b = SQ.Builder()
                b.start(inputs)
                steps = group[0]["steps"]
                if steps:
                    for st in steps:
                        b.step(st["tool"], st["label"], st["lines"])
                else:
                    b.step(0, "Input", group[0]["body"])
                for t in group:
                    cols = b.result(t["result"]).columns
                    t["sql"] = {"statement": b.statement(t["result"], f"@@out:{t['table']}@@"), "columns": cols}
                    known[t["table"]] = cols
            except Exception as exc:                               # noqa: BLE001 - the SQL is an extra: whatever goes wrong here must never cost the PySpark conversion
                why = str(exc) if isinstance(exc, SQ.SqlError) else f"the SQL could not be generated ({type(exc).__name__}: {str(exc)[:160]})"
                for t in group:
                    t["sql"] = {"statement": None, "error": why}
                    failed.add(t["table"])
        if not progressed:
            for unit, group in pending:
                for t in group:
                    t["sql"] = {"statement": None, "error": "it reads a table whose SQL could not be made"}
            break
    for t in tables:
        t.setdefault("sql", {"statement": None, "error": "not generated"})


def build(wf, plan, answers=None, test_results=None):
    answers = answers or {}
    project = PF.project_settings(answers)
    apply_schemas(wf, plan, answers.get("schemas"))
    options = {"text_match_case_insensitive": project["text_match_case_insensitive"], "ansi": project.get("ansi"),
               "null_safe_join": project.get("null_safe_join"), "null_string_as_empty": project.get("null_string_as_empty"),
               "field_types": {str(k).lower(): str(v).lower() for k, v in (answers.get("field_types") or {}).items()},
               "join_inputs": dict(answers.get("join_inputs") or {})}
    plan, (em0, info, live_data, blocked, waiting) = NB.plan_with_ready_cuts(wf, plan, options)
    bad = set(blocked) | set(waiting)
    em = C.Emitter(wf, plan, options)
    kept = [u for u in plan.units if not (set(u.nodes) & bad)]
    dropped = [u for u in plan.units if set(u.nodes) & bad]
    for n in wf.topo:
        if wf.kind(n) == "read":
            em.emit(n)
    tables = []
    for u in kept:
        em.set_unit(u)
        body, steps = [], []
        for nid in u.nodes:
            if wf.kind(nid) == "read":
                continue
            r = em.emit(nid)
            lines = [ln for ln in r.lines if ln.strip()]
            body += lines
            if lines:
                steps.append({"tool": nid, "label": NB.tool_label(wf, nid), "lines": lines})
        text = "\n".join(body)
        reads = sorted({plan.cut_streams.get(st) or plan.bronze_of.get(st[0]) for st in u.inputs} - {None})
        used = [k for k in reads if re.search(rf"\b{re.escape(k)}\b", text)]
        for st in u.streams:
            inputs, lines = list(used), list(body)
            if wf.kind(st[0]) == "read":                 # a Read wired straight to an output: the output is a copy of the Bronze table
                key = plan.bronze_of[st[0]]
                inputs = [key]
                lines = [f"{em.var(st)} = {key}"]
            tools = [n for n in u.nodes if wf.kind(n) != "read"]
            tables.append({"table": u.table[st], "layer": _layer(u.table[st]), "unit": u.key, "inputs": inputs, "body": lines,
                           "result": em.var(st), "tools": tools, "steps": steps,
                           "reviews": [x for n in tools for x in em.results[n].reviews],
                           "notes": [x for n in tools for x in em.results[n].notes]})
    attach_sql(wf, plan, tables)
    em.set_unit(None)
    for n in wf.topo:
        if n not in em.results:
            em.emit(n)
    cov = NB.coverage(wf, plan, em, info, live_data, blocked, waiting, kept, dropped, test_results)
    why = lambda u: sorted({(em0.results[n].reviews or ["waiting for input"])[0] for n in u.nodes if n in bad})[:3]   # noqa: E731
    is_output = lambda n: wf.kind(n) == "unsupported" and "output" in str(wf.nodes[n].get("original_plugin_name") or "").lower() and not wf.consumers(n)  # noqa: E731
    return {"tables": tables,
            "blocked": {n: why_ for n, why_ in blocked.items() if not is_output(n)},        # root causes only (their descendants are in "waiting")
            "outputs_without_converter": sorted(n for n in blocked if is_output(n)),
            "waiting": [{"unit": u.key, "tables": sorted(u.table.values()), "tools": sorted(set(u.nodes) & bad), "why": why(u)} for u in dropped],
            "coverage": cov,
            "bronze": {k: {"source_tables": v["source_tables"], "query": v["query"], "nodes": v["nodes"]} for k, v in plan.bronze.items()},
            "project": project, "plan": plan, "em": em, "kept": kept}
