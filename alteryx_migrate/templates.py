"""The notebook files: one .ipynb per stage, one section per Alteryx container, one commented block per Alteryx tool, and the report.

Files:  00_config  - table mappings and the guarded write function
        01_bronze  - the input tables the workflow starts from (checked, never ingested)
        02_silver  - cleansed / integrated tables, one section per container
        03_gold    - business-ready tables, one section per container
The per-tool PySpark is written by the migration skill; this module only lays it out. A tool that was not converted appears as an
`UPDATE REQUIRED` comment block that names the tool, where it sits in the workflow and what to write there; it is never stubbed with code.
"""
from __future__ import annotations

import json
import re

LAYER_FILE = {"silver": "02_silver.ipynb", "gold": "03_gold.ipynb"}
SQL_FILE = {"silver": "02_silver_sql.ipynb", "gold": "03_gold_sql.ipynb"}
IMPORTS = "from pyspark.sql import functions as F, Window"
BANNER = ("**Converted, not yet validated**: no output was compared with the Alteryx workflow. "
          "Tool numbers such as `[55]` are the Alteryx tool ids, so every step can be found on the canvas.")

CONFIG_CODE = '''import os
from pyspark.sql import functions as F, Window

# Unity Catalog tables the notebooks read (Alteryx input -> table) and write (output key -> table). Edit here only.
INPUTS = %(inputs)s

OUTPUTS = %(outputs)s

ALLOW_OVERWRITE = %(overwrite)r      # nothing is overwritten unless this is True


def write_output(df, key):
    """Guarded batch write of one transformed table: refuses to replace an existing table unless ALLOW_OVERWRITE is True."""
    mode = "overwrite" if ALLOW_OVERWRITE else "errorifexists"
    df.write.format("delta").mode(mode).saveAsTable(OUTPUTS[key])
'''


def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_") or "workflow"


# ---------------------------------------------------------------- ipynb
def md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(True)}


def code(text: str) -> dict:
    return {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": text.rstrip("\n").splitlines(True)}


def ipynb(cells: list) -> str:
    return json.dumps({"cells": cells, "metadata": {"language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}, indent=1) + "\n"


SEP = "# COMMAND ----------"


def ipynb_to_text(text: str) -> str:
    """A notebook as one editable Python file: cells split by `# COMMAND ----------`, markdown cells as `# MAGIC %md` lines (Databricks source format)."""
    cells = []
    for cell in json.loads(text)["cells"]:
        body = "".join(cell["source"]).rstrip("\n")
        if cell["cell_type"] == "markdown":
            body = "\n".join(["# MAGIC %md"] + [("# MAGIC " + line).rstrip() for line in body.splitlines()])
        cells.append(body)
    return ("\n\n" + SEP + "\n\n").join(cells) + "\n"


def _parts(text: str) -> list:
    return [p.strip("\n") for p in re.split(r"^" + re.escape(SEP) + r"[ \t]*$", text, flags=re.M) if p.strip()]


def text_to_ipynb(text: str) -> str:
    cells = []
    for part in _parts(text):
        lines = part.splitlines()
        if lines[0].startswith("# MAGIC %md"):
            cells.append(md("\n".join(l[len("# MAGIC "):] if l.startswith("# MAGIC ") else "" for l in lines[1:])))
        else:
            cells.append(code(part))
    return ipynb(cells)


def code_of(text: str) -> str:
    """Only the Python of an editable notebook text (markdown cells and `%run` lines removed): what the checks parse."""
    out = []
    for part in _parts(text):
        lines = part.splitlines()
        if not lines[0].startswith("# MAGIC"):
            out += [l for l in lines if not l.startswith("%")]
    return "\n".join(out) + "\n"


def _block(mapping: dict) -> str:
    return "{\n" + "".join(f"    {k!r}: {v!r},\n" for k, v in sorted(mapping.items())) + "}"


def config_code(inputs: dict, outputs: dict, allow_overwrite: bool = False) -> str:
    return CONFIG_CODE % {"inputs": _block(inputs), "outputs": _block(outputs), "overwrite": allow_overwrite}


# ---------------------------------------------------------------- not converted
SKIP_KEYS = {"node_type", "node_id", "original_tool_type", "original_plugin_name", "conversion_confidence", "conversion_method", "_xml", "_asked"}
HOW = {"order": ("Choose the field(s) that order the rows (or confirm that any row is acceptable), then keep the first row per key, for example "
                 "Window.partitionBy(*key_fields).orderBy(*order_fields) with F.row_number() == 1; or dropDuplicates(key_fields) if any row is fine.")}


def recipe_for(u: dict) -> list:
    """The replication recipe of the tool from the skill's guide (what it does, PySpark, Spark SQL, what Alteryx does not state)."""
    try:
        import tool_coverage as TC
        return TC.recipe_lines(u.get("plugin"), u.get("name"))
    except Exception:                                                                # noqa: BLE001 - the recipe is help text; never let it stop a conversion
        return []


def setting_lines(node: dict | None, tool: dict | None) -> list:
    """Every Alteryx setting the converter could read for the tool, one per line (secrets are already removed from the slimmed mapping)."""
    import json as _json
    lines = []
    for k, v in (node or {}).items():
        if k not in SKIP_KEYS and v not in (None, "", [], {}):
            lines.append(f"#     {k}: {_json.dumps(v, default=str)}"[:400])
    lines += [f"#     {e.get('label', '')}: {e.get('alteryx_expression', '')}"[:400] for e in (tool or {}).get("expressions") or []]
    return lines[:30]


def handoff(u: dict, tool: dict | None, asked: str, node: dict | None = None, chunk: dict | None = None) -> list:
    """The part of an UPDATE REQUIRED block that lets a person, or another AI, finish the tool without opening the workflow."""
    chunk = chunk or {}
    settings = setting_lines(node, tool)
    lines = ["# Hand-off (give this block to a person or another AI):"]
    lines.append(f"#   Task: write PySpark (DataFrame API, F, Window; no UDF, no collect) for Alteryx tool [{u['id']}] {u['name']} ({u.get('plugin') or 'plugin unknown'}) in container '{u['container']}'.")
    lines.append(f"#   Why the converter stopped: {u['reason']}")
    lines.append(f"#   Place in the workflow: after {u['after']}; before {u['before']}.")
    lines.append(f"#   This cell builds the table `{chunk.get('id', '?')}` ({chunk.get('layer', '?')}); the tools of this table are: {short_chain(chunk.get('chain') or '-')}.")
    reads = list(chunk.get("sources") or []) + list(chunk.get("tables_read") or [])
    lines.append("#   Tables read by this table: " + (", ".join(reads) if reads else "see the cells above"))
    lines.append(f"#   Input: the DataFrame produced by {u['after']}.")
    lines.append(f"#   Output: the DataFrame that {u['before']} expect(s). Keep the column names; name the result so the cells below can use it.")
    lines.append("#   Alteryx settings the converter could read:" + ("" if settings else " none"))
    lines += settings
    xml = (node or {}).get("_xml")
    if xml:
        lines.append("#   Original Alteryx settings (XML from the workflow file - the source of truth):")
        lines += ["#     " + l for l in xml.splitlines()[:40]]
    if not u["status"].startswith("waits"):
        recipe = recipe_for(u)
        if recipe:
            lines.append("#   How to replicate it (from the Alteryx help; the tool_coverage reference has the same text):")
            lines += ["#     " + l for l in recipe]
    if u.get("decision") in HOW:
        lines.append("#   How to finish: " + HOW[u["decision"]])
    else:
        lines.append("#   How to finish: translate each setting above one to one; if a setting is missing, ask the workflow owner instead of guessing.")
    if asked:
        lines.append("#   What the user asked for:")
        lines += ["#     " + l for l in asked.splitlines()]
    return lines


def update_block(u: dict, table: str, shown: dict, tool: dict | None = None, asked: str = "", node: dict | None = None, chunk: dict | None = None) -> str:
    """The comment that marks a tool the converter could not write: what it is, where it sits, why, and what to write.
    A tool already described in an earlier cell is only pointed to; a tool that merely waits names the tool it waits for."""
    bar = "# " + "=" * 70
    where = f"# Position in the workflow: after {u['after']}; feeds {u['before']}"
    if u["id"] in shown:
        return "\n".join([bar, f"# UPDATE REQUIRED - tool [{u['id']}] {u['name']}: same tool as in the cell for `{shown[u['id']]}`.",
                           f"# Update it there; this table (`{table}`) is built from its result.", bar])
    shown[u["id"]] = table
    if u["status"].startswith("waits for"):
        return "\n".join([bar, f"# UPDATE REQUIRED - tool [{u['id']}] {u['name']}   (container: {u['container']})", where,
                           f"# Status: {u['status']} (it cannot be written until that tool exists).",
                           f"# Where to update: this cell (`{table}`, {u['notebook']}), after the tool above is fixed.",
                           f"# What to write: PySpark for tool [{u['id']}] using the DataFrame the tool above produces.",
                           *handoff({**u, "reason": u["status"]}, tool, asked, node, chunk), bar])
    return "\n".join([bar, f"# UPDATE REQUIRED - tool [{u['id']}] {u['name']}   (container: {u['container']})", where, f"# Status: {u['status']}",
                       f"# Why: {u['reason']}",
                       f"# Where to update: this cell, which should build `{table}` ({u['notebook']}).",
                       f"# What to write: PySpark for tool [{u['id']}] that takes the DataFrame produced by the tool(s) above and returns the DataFrame the tool(s) below expect.",
                       *handoff(u, tool, asked, node, chunk), bar])


def decisions_md(decisions: list | None) -> str:
    return ("\n\n## Decisions applied in this conversion\n\n" + "\n".join(f"- {d}" for d in decisions)) if decisions else ""


def attention_table(items: list) -> str:
    """Markdown table of the tools that need the user (shown in the notebooks and the report)."""
    if not items:
        return "All tools in this workflow were converted."
    rows = ["| Tool | Name | Container | After | Feeds | Update in | Why |", "|---|---|---|---|---|---|---|"]
    for u in items:
        rows.append(f"| [{u['id']}] | {u['name']} | {u['container']} | {u['after']} | {u['before']} | {u['notebook']} | {u['reason'].replace('|', '/')} |")
    return "\n".join(rows)


def short_chain(chain: str, keep: int = 6) -> str:
    parts = chain.split(" -> ")
    return chain if len(parts) <= keep + 2 else " -> ".join(parts[:keep]) + f" -> ... -> {parts[-1]}  ({len(parts)} tools)"


# ---------------------------------------------------------------- ordering
def order_chunks(chunks: list) -> list:
    """Dependencies first (a chunk that reads another chunk's table comes after it); chunks of one container stay together when possible."""
    by_id = {c["id"]: c for c in chunks}
    deps = {c["id"]: {t for t in c.get("tables_read", []) if t in by_id} for c in chunks}
    done, out, last = set(), [], None
    pending = [c["id"] for c in chunks]
    while pending:
        ready = [i for i in pending if deps[i] <= done]
        pick = next((i for i in ready if by_id[i].get("container") == last), None) or (ready[0] if ready else pending[0])
        pending.remove(pick)
        done.add(pick)
        out.append(by_id[pick])
        last = by_id[pick].get("container")
    return out


def chunk_cell(chunk: dict, code_text: str, shown: dict, asked: dict | None = None, details: dict | None = None, ai: dict | None = None, seen: dict | None = None) -> str:
    """The code cell of one chunk. Template chunks get their reads and write around the skill's commented body."""
    head = [f"# Container: {chunk['container']}", f"# Layer: {chunk['layer'].capitalize()}  |  writes `{chunk['id']}`"]
    if chunk["strategy"] == "template":
        if seen is not None and code_text.strip() and code_text in seen:       # one tool with several outputs: the first cell already ran this body and defined every result
            return "\n".join(head + [f"# The steps are the same as in the cell for `{seen[code_text]}` above (they ran there and defined this result); only its write is repeated.",
                                     f"write_output({chunk['result']}, {chunk['id']!r})"]) + "\n"
        if seen is not None:
            seen.setdefault(code_text, chunk["id"])
        reads = [f"{k} = spark.table(OUTPUTS[{k!r}])" for k in chunk.get("tables_read", [])]
        return "\n".join(head + reads + [code_text.rstrip("\n"), f"write_output({chunk['result']}, {chunk['id']!r})"]) + "\n"
    model = ((ai or {}).get("by") or {}).get(chunk["id"])
    if code_text.strip():
        who = f"the model `{model}`" if model else "a model"
        return "\n".join(head + [f"# AI-WRITTEN by {who} from the Alteryx settings: the skill's rules did not convert this table. REVIEW every step against tools " +
                                 ", ".join(f"[{t['id']}]" for t in chunk["tools"] if t.get("plugin") != "read")[:200] + " before you rely on it.",
                                 code_text.rstrip("\n")]) + "\n"
    failed = ((ai or {}).get("errors") or {}).get(chunk["id"])
    if failed:
        head = head + [f"# A model was asked to write this table and could not: {failed}", "# The hand-off below is what to give a person or another AI."]
    tools = {t["id"]: t for t in chunk["tools"]}
    blocks = [update_block(u, chunk["id"], shown, tools.get(u["id"]), (asked or {}).get(str(u["id"]), ""), (details or {}).get(u["id"]), chunk) for u in sorted(chunk.get("unconverted", []), key=lambda u: u["status"].startswith("waits"))] or ["# UPDATE REQUIRED: no code was generated for this table."]
    return "\n".join(head + blocks) + "\n"


# ---------------------------------------------------------------- Bronze: the input queries as Spark SQL views
def py_literal(text: str) -> str:
    """A Python string literal for SQL text: a raw triple-quoted block when that is safe, else repr()."""
    if '"""' in text or text.endswith('"') or "\\" in text:
        return repr(text)
    return 'r"""' + text + '"""'


def dialect_label(dialect: str) -> str:
    from .sqlsrc import DIALECTS
    return DIALECTS.get(dialect, dialect or "unknown dialect")


def bronze_notebook(inputs: dict, bronze_info: dict, bronze_sql: dict) -> str:
    """01_bronze: an input with a query becomes a VIEW over the source tables (the query translated to Spark SQL); an input without a query is an existing table the user lands."""
    rows = ["| Table | Unity Catalog name | Alteryx source | How it is made |", "|---|---|---|---|"]
    cells, must_exist = [], []
    for key in sorted(inputs):
        info = bronze_info.get(key, {})
        t = bronze_sql.get(key)
        source = ", ".join(info.get("source_tables") or []) or "-"
        if t is not None and t.ok:
            how = f"view over {', '.join('`%s`' % x for x in t.tables)} (the input query, {dialect_label(t.dialect)} to Spark SQL)"
            comment = "\n".join([f"# {key}: the input query of the workflow ({source}), translated from {dialect_label(t.dialect)} to Spark SQL"] + [f"# note: {n}" for n in sorted(set(t.notes), key=t.notes.index)])
            cells.append(code(f"{comment}\nspark.sql(\"CREATE OR REPLACE VIEW \" + INPUTS[{key!r}] + \" AS\\n\" + {py_literal(t.sql)})"))
            must_exist += [x for x in t.tables if x not in must_exist]
        elif info.get("query"):
            how = f"**query not translated**: {t.error if t is not None else 'no translation was made'}"
            original = "\n".join("#   " + ln for ln in str(info["query"]).splitlines()[:60])
            cells.append(code(f"# {key}: UPDATE REQUIRED - the input query could not be translated: {t.error if t is not None else 'no translation was made'}\n"
                              f"# Original query (create the view {inputs[key]} from it by hand, or land its result as that table):\n{original}"))
            must_exist.append(inputs[key])
        else:
            how = "existing table (a file or other input: land it with your own ingestion)"
            must_exist.append(inputs[key])
        rows.append(f"| `{key}` | `{inputs[key]}` | {source} | {how} |")
    check = ("SOURCES = " + repr(sorted(must_exist)) + "\nmissing = [t for t in SOURCES if not spark.catalog.tableExists(t)]\n"
             "if missing:\n    raise RuntimeError('Tables not found (land them first): ' + ', '.join(missing))")
    intro = ("# Bronze - input tables\n\nThe workflow's sources are existing tables: **nothing is ingested here**. An input that carries a query in the workflow (renames, filters, joins, aggregates) "
             "becomes a Spark SQL view with the name the later notebooks read; the first cell checks the tables it needs exist.\n\n" + "\n".join(rows))
    return ipynb([md(intro), code("%run ./00_config"), code(check)] + cells)


# ---------------------------------------------------------------- notebooks
def build_notebooks(title: str, chunks: list, codes: dict, statuses: dict, inputs: dict, outputs: dict, bronze_info: dict, attention: list,
                    decisions: list | None = None, allow_overwrite: bool = False, asked: dict | None = None, details: dict | None = None, ai: dict | None = None,
                    bronze_sql: dict | None = None) -> dict:
    """-> {file name: ipynb text}. Only the stages that have content are written."""
    files, shown, seen = {}, {}, {}
    files["00_config.ipynb"] = ipynb([
        md(f"# Config - {title}\n\n{BANNER}\n\nTable names for all notebooks. Run the notebooks in order: `01_bronze`, `02_silver`, `03_gold`.\n\n"
           f"## Tools that need your update\n\n{attention_table(attention)}" + decisions_md(decisions)),
        code(config_code(inputs, outputs, allow_overwrite))])
    files["01_bronze.ipynb"] = bronze_notebook(inputs, bronze_info, bronze_sql or {})
    for layer, fname in LAYER_FILE.items():
        mine = order_chunks([c for c in chunks if c["layer"] == layer])
        if not mine:
            continue
        need = [u for u in attention if u["notebook"] == fname]
        cells = [md(f"# {layer.capitalize()} - {'business-ready outputs' if layer == 'gold' else 'cleansed and integrated tables'}\n\n"
                    f"One section per Alteryx container; one commented block per tool (`# [id] Kind`). Run top to bottom.\n\n"
                    f"## Tools that need your update in this notebook\n\n{attention_table(need)}"),
                 code("%run ./00_config"), code(IMPORTS)]
        sources = sorted({k for c in mine if c["strategy"] == "template" for k in c.get("sources", [])})
        if sources:
            cells += [md("### Sources\nInput tables, read as they are."), code("\n".join(f"{k} = spark.table(INPUTS[{k!r}])" for k in sources))]
        last = object()
        for c in mine:
            if c["container"] != last:
                cells.append(md(f"## {c['container']}"))
                last = c["container"]
            note = f"**Status: {statuses.get(c['id'], 'unknown')}**"
            cells.append(md(f"### `{c['id']}`  {note}\n\n{c.get('chain') or ''}"))
            cells.append(code(chunk_cell(c, codes.get(c["id"], ""), shown, asked, details, ai, seen)))
        files[fname] = ipynb(cells)
    return files


def report(meta: dict, chunks: list, statuses: dict, stats: dict, attention: list, versions: dict, decisions: list | None = None, ai: dict | None = None) -> str:
    done = sum(1 for c in chunks if statuses.get(c["id"]) in ("ok", "accepted"))
    lines = [f"# Conversion report - {meta.get('file_name')}", "",
             "**Status: converted, not yet validated.** Nothing was compared with real Alteryx output.", "",
             f"- Run: `{meta.get('run_id')}`   user: {meta.get('user')}",
             f"- Skill version: `{meta.get('skill_version')}`   app version: `{meta.get('app_version')}`   model: `{meta.get('endpoint') or '-'}`",
             f"- Chunks ready: {done} of {len(chunks)}   AI calls: {stats.get('ai_calls', 0)}   tokens: {stats.get('tokens', 0):,}   "
             f"estimated cost: ${stats.get('cost_est', 0):.4f}   manual edits: {stats.get('human_edits', 0)}", "",
             "## Notes from the user", meta.get("user_notes") or "(none)", "", "## Business context", meta.get("business_context") or "(none)", "",
             "## Chunks", "", "| Chunk | Container | Layer | Status | Version |", "|---|---|---|---|---|"]
    lines += [f"| `{c['id']}` | {c.get('container')} | {c.get('layer')} | {statuses.get(c['id'], 'unknown')} | v{versions.get(c['id'], '-')} |" for c in chunks]
    if ai and (ai.get("by") or ai.get("errors")):
        lines += ["", "## Written by a model (review before use)", "",
                  "These tables contain a tool the skill converts by no rule. A Databricks-served model wrote them from the Alteryx settings; they are marked AI-WRITTEN in the notebook.", ""]
        lines += [f"- `{t}` by `{m}`" for t, m in sorted((ai.get("by") or {}).items())]
        lines += [f"- `{t}`: the model could not write it ({e})" for t, e in sorted((ai.get("errors") or {}).items())]
    lines += ["", "## Tools that need your update", "", attention_table(attention)]
    if decisions:
        lines += ["", "## Decisions applied in this conversion", ""] + [f"- {d}" for d in decisions]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- Spark SQL notebooks
def sql_comment(text: str) -> str:
    """A comment block written for Python (`# ...`) as a SQL comment (`-- ...`)."""
    return "\n".join(("--" + l[1:]) if l.startswith("#") else l for l in text.splitlines())


def fill_tables(sql: str, inputs: dict, outputs: dict) -> str:
    """Put the real table names (catalog.schema.table) where the generator left @@in:<key>@@ / @@out:<table>@@."""
    def name(m):
        kind, key = m.group(1), m.group(2)
        table = (inputs if kind == "in" else outputs).get(key)
        if not table:
            raise KeyError(f"no table is configured for {kind}:{key}")
        return table
    return re.sub(r"@@(in|out):([^@]+)@@", name, sql)


def sql_cell(chunk: dict, inputs: dict, outputs: dict, shown: dict, asked: dict | None, details: dict | None, ai: dict | None = None) -> str:
    """The `%sql` cell of one table: its statement, or the reason there is none and where the complete PySpark is."""
    head = [f"-- Container: {chunk['container']}", f"-- Layer: {chunk['layer'].capitalize()}  |  writes {outputs.get(chunk['id'], chunk['id'])}"]
    if chunk.get("sql"):
        return "%sql\n" + "\n".join(head) + "\n" + fill_tables(chunk["sql"], inputs, outputs) + "\n"
    if chunk["strategy"] == "template":
        why = chunk.get("sql_error") or "no SQL was generated"
        return "%sql\n" + "\n".join(head + ["-- NO SQL FOR THIS TABLE: " + why,
                                             f"-- The complete conversion of `{chunk['id']}` is in {LAYER_FILE[chunk['layer']]} (PySpark). Write this table in SQL by hand from that cell,",
                                             "-- or run the PySpark notebook for it; do not leave this table out."]) + "\n"
    model = ((ai or {}).get("by") or {}).get(chunk["id"])
    if model:
        return "%sql\n" + "\n".join(head + [f"-- NO SQL FOR THIS TABLE: it was written by the model `{model}` in PySpark only ({LAYER_FILE[chunk['layer']]}).",
                                             "-- Review that cell first; write the SQL from it afterwards."]) + "\n"
    tools = {t["id"]: t for t in chunk["tools"]}
    blocks = [sql_comment(update_block(u, chunk["id"], shown, tools.get(u["id"]), (asked or {}).get(str(u["id"]), ""), (details or {}).get(u["id"]), chunk))
              for u in sorted(chunk.get("unconverted", []), key=lambda u: u["status"].startswith("waits"))]
    return "%sql\n" + "\n".join(head + blocks) + "\n"


def build_sql_notebooks(title: str, chunks: list, statuses: dict, inputs: dict, outputs: dict, asked: dict | None = None, details: dict | None = None,
                        ai: dict | None = None) -> dict:
    """-> {file name: ipynb text}: the Silver and Gold tables as Spark SQL, next to the PySpark notebooks. Only layers that have tables are written."""
    files, shown = {}, {}
    for layer, fname in SQL_FILE.items():
        mine = order_chunks([c for c in chunks if c["layer"] == layer])
        if not mine:
            continue
        done = sum(1 for c in mine if c.get("sql"))
        cells = [md(f"# {layer.capitalize()} (Spark SQL) - {title}\n\n{BANNER}\n\n"
                    f"The same {layer} tables as `{LAYER_FILE[layer]}`, as Spark SQL: one `CREATE TABLE ... AS WITH ...` per table, one common table expression per Alteryx tool "
                    f"(`-- [id] Tool`), named after the PySpark variable it mirrors. **{done} of {len(mine)} tables have SQL.** A table without SQL says why in its cell; its PySpark is complete.\n\n"
                    "`CREATE TABLE` fails when the table exists (like the guarded write of the PySpark notebooks); change it to `CREATE OR REPLACE TABLE` to overwrite on purpose. "
                    "Run the cells top to bottom.")]
        last = object()
        for c in mine:
            if c["container"] != last:
                cells.append(md(f"## {c['container']}"))
                last = c["container"]
            cells.append(md(f"### `{c['id']}`  **Status: {statuses.get(c['id'], 'unknown')}**\n\n{c.get('chain') or ''}"))
            cells.append(code(sql_cell(c, inputs, outputs, shown, asked, details, ai)))
        files[fname] = ipynb(cells)
    return files
