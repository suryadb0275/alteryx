"""Embedded Python (Jupyter) tools: port the script to PySpark, cell by cell, with a Databricks model.

The slimmer has already split the script into cells and worked out which cells each `Alteryx.write(df, k)` needs (its *slice*). This module sends the cells to the model in
batches under the rules of the skill's references/10-python-code-tools.md, checks every cell by code, and puts the questions the model cannot answer without guessing (interpolation,
scipy, sklearn, ordering ...) to the user in plain English. The result is ONE block of PySpark that the skill then uses in place of the Python tool, so every tool AFTER it converts by
rule as usual (see ``overrides.<tool>.custom_python`` + ``ported_python`` in the skill).

Placeholders the model writes (so it never has to escape braces): ``<<IN:n>>`` = the DataFrame that Alteryx.read("#n") gave, ``<<OUT:k>>`` = the DataFrame that Alteryx.write(df, k) wrote.
"""
from __future__ import annotations

import ast
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

from . import ai as ai_mod
from . import paths

BATCH_CHARS = 6000
BATCH_CELLS = 25
FORBIDDEN = [
    (r"\bimport\s+pandas\b|\bimport\s+numpy\b|\bfrom\s+pandas\b|\bpd\.", "pandas/numpy are not allowed in converted code"),
    (r"\.toPandas\(|\.collect\(\)|\.rdd\b", "collect()/toPandas()/.rdd pull rows to the driver"),
    (r"\budf\(|\bpandas_udf\(", "UDFs are not allowed"),
    (r"\bAlteryx\.(read|write)\b", "Alteryx.read/Alteryx.write must be replaced by the placeholders"),
    (r"\.write\b|saveAsTable|\bspark\.table\(|\bspark\.read\b|insertInto", "reading and writing tables is done only through <<IN:n>> and <<OUT:k>>: no spark.table / .write / saveAsTable"),
    (r"\bTODO\b|\bFIXME\b|NotImplementedError", "placeholder code is not allowed"),
    (r"(?m)^\s*(?:pass|\.\.\.)\s*$", "an empty `pass` / `...` is a placeholder"),
    (r"(?i)\b(password|secret|token|api[_-]?key)\s*=\s*['\"][^'\"]{6,}", "a secret value is written in the code"),
]


@dataclass
class Info:
    tool: int
    name: str
    cells: list                      # mapping_slimmer Cell objects, in script order
    slices: dict                     # output k -> [cell indexes]
    inputs: dict                     # n -> {"variable", "stream", "index" (position in the tool's flat input list), "verified"}
    outputs: list                    # [k]
    anchors: dict                    # k -> anchor name used downstream (Output<k>)
    consumed: list = field(default_factory=list)    # outputs some tool after the script reads

    def cell(self, index: int):
        return next(c for c in self.cells if c.index == index)

    @property
    def needed(self) -> list:
        return sorted(set().union(*self.slices.values())) if self.slices else []


@dataclass
class Result:
    code: str = ""                   # the ported program, with {in<i>} / {out_Output<k>} placeholders and braces escaped, ready for overrides.<tool>.custom_python
    cells: dict = field(default_factory=dict)          # cell index -> converted code
    failed: dict = field(default_factory=dict)         # cell index -> why
    unanswered: list = field(default_factory=list)     # blocking questions nobody answered
    notes: dict = field(default_factory=dict)          # cell index -> the model's note
    module: str = ""                                    # the ported script as a component file (utils/python_tool_<id>.py); `code` then only calls it
    module_name: str = ""
    calls: int = 0

    @property
    def ok(self) -> bool:
        return bool(self.code) and not self.failed and not self.unanswered


def _slimmer():
    root = str(paths.tools_root() / "slim_mapping")
    if root not in sys.path:
        sys.path.insert(0, root)
    from mapping_slimmer.name_scan import analyse_cell
    from mapping_slimmer.script_cells import split_cells
    from mapping_slimmer.slicing import build_slices
    return analyse_cell, split_cells, build_slices


def _schemas(wf) -> dict:
    """{tool id: [{name, type, size}]} from the MetaInfo of each tool's output in the workflow XML ({} without it)."""
    if not getattr(wf, "yxmd", None):
        return {}
    if not hasattr(wf, "_xml_schemas"):
        try:
            import xml_recover
            wf._xml_schemas = {tid: t.schema for tid, t in xml_recover.load(wf.yxmd).items()}
        except (ImportError, OSError, ValueError):
            wf._xml_schemas = {}
    return wf._xml_schemas


def load(slim_dir: Path, wf) -> dict:
    """{tool id: Info} for every embedded Python tool of the slimmed workflow."""
    analyse_cell, split_cells, build_slices = _slimmer()
    doc = json.loads((Path(slim_dir) / "slim.json").read_text(encoding="utf-8"))
    out: dict = {}
    for node in doc.get("nodes", []):
        ref = node.get("python_script")
        if not ref:
            continue
        tool = int(node["node_id"])
        script = (Path(slim_dir) / ref["file"]).read_text(encoding="utf-8")
        cells = [analyse_cell(c) for c in split_cells(script)]
        slices = build_slices(cells)
        variables = {}
        for c in cells:
            for n, v in c.read_vars.items():
                variables.setdefault(int(n), v)
        flat = []                                                         # the tool's inputs in the order the skill hands them out as {in0}, {in1} ...
        grouped: dict = {}
        for e in wf.producers(tool):
            grouped.setdefault(e.dst_anchor, []).append((e.src, e.src_anchor))
        flat = [s for v in grouped.values() for s in v]
        inputs = {}
        for label, item in (ref.get("inputs") or {}).items():
            n, src = int(label.lstrip("#")), int(item["from_tool"])
            matches = [i for i, (s, _a) in enumerate(flat) if s == src]
            inputs[n] = {"variable": variables.get(n, ""), "stream": flat[matches[0]] if matches else None, "index": matches[0] if len(matches) == 1 else None,
                         "verified": (bool(item.get("verified_in_xml")) or len(ref.get("inputs") or {}) == 1) and len(matches) == 1}
        for item in inputs.values():                                       # Date/DateTime fields of each input, from the workflow's MetaInfo of the tool feeding it
            schema = _schemas(wf).get(item["stream"][0]) if item["stream"] else None
            item["dates"] = sorted({f["name"] for f in schema or [] if str(f.get("type") or "").lower() in ("date", "datetime")})
        anchors = {a: f"Output{a}" for a in sorted(slices)}
        consumed = sorted({int(re.sub(r"\D", "", a)) for (n, a) in wf.outputs if n == tool and re.search(r"\d", a)})
        out[tool] = Info(tool, str(node.get("original_tool_type") or "JupyterCode"), cells, slices, inputs, sorted(slices), anchors, consumed)
    return out


# ---------------------------------------------------------------------------------------------------------------- prompts
def rules_text() -> str:
    try:
        return (paths.skill_dir() / "references" / "10-python-code-tools.md").read_text(encoding="utf-8")
    except OSError:
        return ""


def system_prompt() -> str:
    return ("You convert the cells of an embedded Alteryx Python (Jupyter) tool into Databricks PySpark, exactly as the rules below say.\n"
            "Placeholders: write `<<IN:n>>` where the cell reads Alteryx.read(\"#n\") (the DataFrame itself, e.g. `df_mseg = <<IN:1>>`) and `<<OUT:k>>` where it calls "
            "Alteryx.write(df, k) (e.g. `<<OUT:2>> = df_master_po`). F (pyspark.sql.functions), Window, T (pyspark.sql.types) and spark are already available: do not import them. "
            "No pandas, no numpy, no collect/toPandas/UDF, no placeholders such as TODO or pass.\n"
            "Pandas row order is not Spark's: add an explicit orderBy wherever the result depends on order and say so in `notes`. Keep the original variable names. "
            "Reply with ONE JSON object and nothing else.\n\n" + rules_text())


def cell_json(c, answers: dict) -> dict:
    do = [f'Alteryx.read("#{n}") must become <<IN:{n}>> (the placeholder itself, nothing else)' for n in c.reads]
    do += [f"Alteryx.write(df, {w.output}) must become `<<OUT:{w.output}>> = df` (an assignment; never write a table yourself)" for w in c.writes]
    return {"cell": c.index, "lines": f"{c.start}-{c.end}", "text": c.text, "reads_inputs": c.reads, "defines": c.stores, "needs_earlier": c.loads_external,
            "writes_outputs": [w.output for w in c.writes], "required": do}


def batches(cells: list) -> list:
    out, cur, size = [], [], 0
    for c in cells:
        if cur and (size + len(c.text) > BATCH_CHARS or len(cur) >= BATCH_CELLS):
            out.append(cur)
            cur, size = [], 0
        cur.append(c)
        size += len(c.text)
    if cur:
        out.append(cur)
    return out


def user_prompt(info: Info, batch: list, registry: dict, answers: dict, description: str, tables: dict, feedback: list | None) -> str:
    body = {"python_tool": info.tool, "what_the_user_says_this_script_does": description or "(not described)",
            "inputs": {f"#{n}": {"variable": i["variable"], "from_table": (tables.get("in") or {}).get(n), "verified": i["verified"]} for n, i in info.inputs.items()},
            "outputs": {f"{k}": (tables.get("out") or {}).get(k) for k in info.outputs},
            "dataframes_defined_by_earlier_cells": registry, "answers_given_by_the_user": answers, "cells": [cell_json(c, answers) for c in batch]}
    parts = ["Convert these cells. Reply with: {\"cells\": [{\"cell\": <index>, \"code\": \"...\", \"defines\": [{\"name\": \"...\", \"columns\": [...] | null}], \"notes\": \"...\"}], "
             "\"questions\": [{\"id\": \"q1\", \"text\": \"<plain English question>\", \"options\": [\"a\", \"b\"], \"default\": \"a\", \"blocking\": true, \"cells\": [<index>]}]}. "
             "One entry in `cells` for EVERY input cell, in order (a cell with nothing to convert has \"code\": \"\" and a note). A cell you cannot convert without guessing goes in "
             "`questions` and keeps \"code\": \"\"."]
    if feedback:
        parts += ["Problems found in your previous reply - fix exactly these:"] + [f"- {f}" for f in feedback]
    parts += ["```json", json.dumps(body, ensure_ascii=False, indent=1), "```"]
    return "\n".join(parts)


def parse_reply(text: str) -> dict:
    text = text.strip()
    m = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.S)
    if m:
        text = m.group(1)
    else:
        a, b = text.find("{"), text.rfind("}")
        text = text[a:b + 1] if a >= 0 and b > a else text
    try:
        return json.loads(text)
    except ValueError as exc:
        raise ai_mod.AIError(f"the model's reply is not valid JSON ({exc})") from exc


def check_cell(code: str, cell=None) -> list:
    """Problems of one converted cell, found by code (never by the model). `cell` = the original: every input it reads and every output it writes must go through the placeholders."""
    if not code.strip():
        return []
    bad = [why for pattern, why in FORBIDDEN if re.search(pattern, code)]
    if cell is not None:
        for n in cell.reads:
            if f"<<IN:{int(n)}>>" not in code:
                bad.append(f"the cell reads input #{n}: write <<IN:{int(n)}>> for it (not a table name)")
        for w in cell.writes:
            if not re.search(rf"<<OUT:{int(w.output)}>>\s*=", code):
                bad.append(f"the cell writes output {w.output}: write `<<OUT:{int(w.output)}>> = <the DataFrame>` (do not save a table yourself)")
    probe = re.sub(r"<<(IN|OUT):\d+>>", "_placeholder_", code)
    try:
        ast.parse(probe)
    except SyntaxError as exc:
        bad.append(f"syntax error at line {exc.lineno}: {exc.msg}")
    return bad


# ---------------------------------------------------------------------------------------------------------------- the port
def port(client, info: Info, description: str = "", tables: dict | None = None, ask=None, progress=None, answers: dict | None = None, components: bool = True) -> Result:
    """Convert every cell some output needs. `ask(question) -> str` is called for each question the model raises (None = take the default, or leave a blocking one unanswered)."""
    tables, answers, result = tables or {}, dict(answers or {}), Result()
    registry: dict = {}
    system = system_prompt()
    if hasattr(client, "max_output_tokens") and client.max_output_tokens < 12000:
        client.max_output_tokens = 12000                                 # a reply carries the code of up to 25 cells as JSON
    unverified = [n for n, i in info.inputs.items() if not i["verified"]] if len(info.inputs) > 1 else []
    if unverified:                                           # Alteryx.read("#n"): with several inputs, n came from the order of the connections in the file, not from the XML
        q = {"id": "inputs-order", "blocking": True, "options": ["yes", "no"], "default": "", "cells": [],
             "text": f"The script reads {len(info.inputs)} inputs as #1..#{max(info.inputs)}. Which connection is #n is not recorded in the workflow, so the order of the connections in the file was used "
                     f"(#{unverified[0]} is the output of tool [{(info.inputs[unverified[0]]['stream'] or ('?',))[0]}], and so on). Is that the right order?"}
        answer = ask(q) if ask else None
        if str(answer or "").strip().lower() not in ("yes", "y"):
            result.unanswered.append(q)
            return result
    groups = batches([info.cell(i) for i in info.needed])
    for number, batch in enumerate(groups, 1):
        feedback, final = None, None
        for attempt in range(3):
            try:
                reply = parse_reply(client.complete(system, user_prompt(info, batch, registry, answers, description, tables, feedback)))
                result.calls += 1
            except ai_mod.AIError as exc:
                feedback = [str(exc)]
                final = {"error": str(exc)}
                continue
            got = {int(x.get("cell", -1)): x for x in reply.get("cells") or [] if isinstance(x, dict)}
            problems = [f"cell {c.index} is missing from the reply" for c in batch if c.index not in got]
            for c in batch:
                if c.index in got:
                    problems += [f"cell {c.index}: {p}" for p in check_cell(str(got[c.index].get("code") or ""), c)]
            new_answers = False
            held = set()                                         # cells that wait for a question
            for q in reply.get("questions") or []:
                held |= {int(i) for i in q.get("cells") or []}
                qid = str(q.get("id") or f"q{len(answers) + 1}")
                if qid in answers:
                    continue
                answer = ask(q) if ask else None
                if answer in (None, "") and q.get("default") not in (None, ""):
                    answer = q["default"]
                if answer not in (None, ""):
                    answers[qid] = answer
                    new_answers = True
                    result.unanswered = [u for u in result.unanswered if u.get("id") != qid]
                elif q.get("blocking", True) and not any(u.get("id") == qid for u in result.unanswered):
                    result.unanswered.append(q)
            if attempt < 2 and (new_answers or problems):
                feedback = (["The user answered your questions (see answers_given_by_the_user): convert the cells that waited for them."] if new_answers else []) + problems
                final = {"reply": got, "held": held}
                continue
            final = {"reply": got, "held": held}
            break
        if final is None or "error" in final:
            for c in batch:
                result.failed[c.index] = (final or {}).get("error", "the model did not answer")
        else:
            got, held = final["reply"], final["held"]
            for c in batch:
                x = got.get(c.index)
                if x is None:
                    result.failed[c.index] = "the model returned no entry for this cell"
                    continue
                code = str(x.get("code") or "")
                issues = check_cell(code, c)
                if issues:
                    result.failed[c.index] = "; ".join(issues)
                elif c.index in held and not code.strip():
                    result.failed[c.index] = "waits for an answer: " + "; ".join(str(q.get("text")) for q in result.unanswered if c.index in [int(i) for i in q.get("cells") or []])
                else:
                    result.cells[c.index] = code
                    result.notes[c.index] = str(x.get("notes") or "")
                    for d in x.get("defines") or []:
                        if isinstance(d, dict) and d.get("name"):
                            registry[d["name"]] = d.get("columns")
        if progress:
            progress(number, len(groups))
    program = "\n".join(result.cells.values())
    for k in info.outputs:                                               # every Alteryx output must be assigned somewhere, or the tools after it would have nothing to read
        if not re.search(rf"<<OUT:{k}>>\s*=", program) and not result.failed and not result.unanswered:
            result.failed[0] = f"the ported script never assigns output {k}"
    if not result.failed and not result.unanswered:
        if components:
            result.module, result.code = component(info, result.cells, result.notes, getattr(client, "model", ""))
            result.module_name = f"utils/python_tool_{info.tool}.py"
        else:
            result.code = assemble(info, result.cells, result.notes, getattr(client, "model", ""))
    return result


def assemble(info: Info, cells: dict, notes: dict, model: str = "") -> str:
    """The ported program as the skill's custom_python text: placeholders resolved, braces escaped."""
    parts = []
    for index in info.needed:
        code = cells.get(index, "")
        if not code.strip():
            continue
        head = f"# --- cell {index} (script lines {info.cell(index).start}-{info.cell(index).end})" + (f": {notes[index]}" if notes.get(index) else "")
        parts.append(head.replace("\n", " ") + "\n" + code.rstrip("\n"))
    banner = (f"# AI-WRITTEN: this is the embedded Python tool [{info.tool}] ported to PySpark cell by cell" + (f" by the model `{model}`" if model else "")
              + ". The Alteryx tools after it are converted by rule. REVIEW every cell against the original script and compare the result with Alteryx.")
    text = banner + "\n" + "\n\n".join(parts)
    text = text.replace("{", "{{").replace("}", "}}")

    def read(m):
        n = int(m.group(1))
        i = info.inputs.get(n)
        if not i or i["index"] is None:
            raise ai_mod.AIError(f"Alteryx.read(\"#{n}\") cannot be tied to one input connection of the tool")
        return "{in%d}" % i["index"]
    text = re.sub(r"<<IN:(\d+)>>", read, text)
    text = re.sub(r"<<OUT:(\d+)>>", lambda m: ("{out_%s}" % info.anchors.get(int(m.group(1)), f"Output{m.group(1)}")) if int(m.group(1)) in info.consumed else f"_unused_output_{m.group(1)}", text)
    return text


def _offsets(text: str):
    """Map an (ast line, utf-8 column) position to an index into `text`."""
    starts, total = [], 0
    lines = text.splitlines(keepends=True)
    for ln in lines:
        starts.append(total)
        total += len(ln)

    def at(lineno: int, col: int) -> int:
        line = lines[lineno - 1]
        return starts[lineno - 1] + len(line.encode("utf-8")[:col].decode("utf-8", errors="ignore"))
    return at


def _is_alteryx(node, attr: str | None = None) -> bool:
    return (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "Alteryx" and (attr is None or node.attr == attr))


def rewrite_cell(text: str, packages: list) -> tuple:
    """One ORIGINAL cell with the Alteryx plumbing replaced: Alteryx.read("#n") -> input_n.copy(), Alteryx.write(df, k) -> output_k = df, `from ayx import ...` -> pass.
    -> (new text, [problems]). Everything else is left exactly as the author wrote it."""
    from mapping_slimmer.script_cells import neutralise_magics
    text = neutralise_magics(text)
    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        return text, [f"the cell does not parse (line {exc.lineno}: {exc.msg})"]
    at, edits, problems = _offsets(text), [], []

    def span(node):
        return at(node.lineno, node.col_offset), at(node.end_lineno, node.end_col_offset)
    handled = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and _is_alteryx(node.value.func, "write"):
            call = node.value
            k = call.args[1].value if len(call.args) == 2 and isinstance(call.args[1], ast.Constant) and isinstance(call.args[1].value, int) else None
            if k is None or len(call.args) != 2 or call.keywords:
                problems.append("Alteryx.write(...) is not of the form Alteryx.write(dataframe, output_number)")
                continue
            a, b = span(node)
            edits.append((a, b, f"output_{k} = {ast.get_source_segment(text, call.args[0])}"))
            handled |= {id(n) for n in ast.walk(node)}
        elif isinstance(node, (ast.ImportFrom, ast.Import)) and ((isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] == "ayx")
                                                                 or (isinstance(node, ast.Import) and any(n.name.split(".")[0] == "ayx" for n in node.names))):
            a, b = span(node)
            edits.append((a, b, "pass  # the ayx import is not needed here"))
            handled |= {id(n) for n in ast.walk(node)}
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "installPackages":
            for arg in node.value.args:
                for const in ast.walk(arg):
                    if isinstance(const, ast.Constant) and isinstance(const.value, str) and const.value not in packages:
                        packages.append(const.value)
            a, b = span(node)
            edits.append((a, b, "pass  # Package.installPackages: install the package on the cluster instead (see the module docstring)"))
            handled |= {id(n) for n in ast.walk(node)}
    for node in ast.walk(tree):
        if id(node) in handled:
            continue
        if isinstance(node, ast.Call) and _is_alteryx(node.func, "read"):
            arg = node.args[0] if node.args else None
            if len(node.args) == 1 and isinstance(arg, ast.Constant) and isinstance(arg.value, str) and re.fullmatch(r"#\d+", arg.value):
                a, b = span(node)
                edits.append((a, b, f"input_{arg.value[1:]}.copy()"))
                handled |= {id(n) for n in ast.walk(node)}
            else:
                problems.append("Alteryx.read(...) with something other than a literal \"#n\"")
        elif _is_alteryx(node):
            problems.append(f"the cell uses Alteryx.{node.attr}, which has no counterpart outside Alteryx")
    edits.sort(reverse=True)
    for (a1, b1, _), (a2, b2, _) in zip(edits, edits[1:]):
        if b2 > a1:
            problems.append("two Alteryx calls overlap and cannot be rewritten safely")
    for a, b, new in edits:
        text = text[:a] + new + text[b:]
    return text, problems


def keep(info: Info) -> Result:
    """Keep the ORIGINAL pandas script as a component file (utils/python_tool_<id>.py) and call it from the notebook - no model, no translation.

    The component converts the Spark inputs with toPandas(), runs the author's cells unchanged, and returns the outputs as Spark DataFrames. The logic is therefore exactly the script's;
    the price is that the data must fit in the driver's memory (it did in Alteryx, which ran the same code on one machine)."""
    result, packages, body = Result(), [], []
    for n in sorted(info.inputs):
        if info.inputs[n]["index"] is None:
            result.failed[0] = f"Alteryx.read(\"#{n}\") cannot be tied to one input connection of the tool"
    for index in info.needed:
        cell = info.cell(index)
        new, problems = rewrite_cell(cell.text, packages)
        if problems:
            result.failed[index] = "; ".join(sorted(set(problems)))
            continue
        result.cells[index] = new
        body.append(f"# --- cell {index} (script lines {cell.start}-{cell.end})\n" + new.rstrip("\n"))
    program = "\n\n".join(body)
    for k in info.consumed:
        if not re.search(rf"(?m)^\s*output_{k}\s*=", program) and not result.failed:
            result.failed[0] = f"the script never writes output {k}"
    if result.failed:
        return result
    name = f"python_tool_{info.tool}"
    params = ", ".join(["spark"] + [f"input_{n}" for n in sorted(info.inputs)])
    convert = "".join(f"    input_{n} = _to_pandas(input_{n}{', ' + repr(info.inputs[n].get('dates')) if info.inputs[n].get('dates') else ''})\n" for n in sorted(info.inputs))
    returned = [f"_to_spark(spark, output_{k})" for k in info.consumed]
    install = ("\nPackages the script installed with Package.installPackages: " + ", ".join(packages) + " - install them on the cluster (%pip install ...).\n") if packages else ""
    banner = (f'"""Embedded Python tool [{info.tool}] of the Alteryx workflow: the ORIGINAL pandas code, unchanged.\n\n'
              "Only the Alteryx plumbing is replaced: Alteryx.read(\"#n\") gives input_n (a pandas copy of the Spark input), Alteryx.write(df, k) sets output_k. The code runs on the DRIVER: the data must fit\n"
              "in its memory (it did in Alteryx, which ran the same code on one machine). Missing values come back to Spark as NULL (pandas NaN in a double column becomes NULL). Compare the result with the\n"
              f'Alteryx output before relying on it. The notebooks call `{name}.run(...)`; edit this file if a cell must change.{install}"""\n')
    helper = ('def _to_pandas(frame, dates=()):\n    """A Spark input as Alteryx.read("#n") hands it to the script: Date and DateTime fields are pandas datetime64 (toPandas() alone gives Python\n'
              '    date objects for a Spark date, and text for a date the workflow carries as text - comparisons and merges on them would then differ)."""\n'
              "    pdf = frame.toPandas()\n    wanted = {d.lower() for d in dates}\n    for field in frame.schema.fields:\n"
              "        if isinstance(field.dataType, (T.DateType, T.TimestampType)) or field.name.lower() in wanted:\n"
              '            pdf[field.name] = pd.to_datetime(pdf[field.name], errors="coerce")\n    return pdf\n\n\n'
              'def _to_spark(spark, frame):\n    """A pandas result as a Spark DataFrame; NaN in a floating-point column is a missing value, so it becomes NULL."""\n'
              "    sdf = spark.createDataFrame(frame)\n    for field in sdf.schema.fields:\n        if isinstance(field.dataType, (T.DoubleType, T.FloatType)):\n"
              '            sdf = sdf.withColumn(field.name, F.nanvl(F.col("`" + field.name + "`"), F.lit(None).cast(field.dataType)))\n    return sdf\n')
    indented = "\n".join(("    " + ln) if ln.strip() else "" for ln in program.splitlines())
    tail = f"    return {', '.join(returned)}\n" if returned else "    return None\n"
    result.module = (banner + "import pandas as pd\nfrom pyspark.sql import functions as F, types as T  # noqa: F401\n\n\n" + helper + "\n\n" + f"def run({params}):\n" + convert + indented + "\n" + tail)
    result.module_name = f"utils/{name}.py"
    arguments = ", ".join(["spark"] + ["{in%d}" % info.inputs[n]["index"] for n in sorted(info.inputs)])
    targets = ", ".join("{out_%s}" % info.anchors.get(k, f"Output{k}") for k in info.consumed)
    result.code = "\n".join([f"from utils import {name}  # the ORIGINAL pandas code of the Python tool: utils/{name}.py (runs on the driver)", (f"{targets} = " if returned else "") + f"{name}.run({arguments})"])
    return result


def component(info: Info, cells: dict, notes: dict, model: str = "") -> tuple:
    """The ported script as a component file plus the few lines that call it.

    -> (module text for utils/python_tool_<id>.py, custom_python text for the skill). The component is an ordinary module: `run(spark, input_1, ...)` takes the DataFrames the script read
    with Alteryx.read("#n") and returns the DataFrames it wrote with Alteryx.write(df, k) that the next tools read."""
    for n in sorted(info.inputs):
        if info.inputs[n]["index"] is None:
            raise ai_mod.AIError(f"Alteryx.read(\"#{n}\") cannot be tied to one input connection of the tool")
    name = f"python_tool_{info.tool}"
    body = []
    for index in info.needed:
        code = cells.get(index, "")
        if not code.strip():
            continue
        cell = info.cell(index)
        note = f": {notes[index]}".replace("\n", " ") if notes.get(index) else ""
        body.append(f"# --- cell {index} (script lines {cell.start}-{cell.end}){note}\n" + code.rstrip("\n"))
    program = "\n\n".join(body)
    for n in sorted(info.inputs):
        program = program.replace(f"<<IN:{n}>>", f"input_{n}")
    program = re.sub(r"<<OUT:(\d+)>>", lambda m: f"output_{m.group(1)}", program)
    params = ", ".join(["spark"] + [f"input_{n}" for n in sorted(info.inputs)])
    returned = [f"output_{k}" for k in info.consumed]
    by = f" by the model `{model}`" if model else ""
    banner = (f'"""Embedded Python tool [{info.tool}] of the Alteryx workflow, ported to PySpark cell by cell{by}.\n\n'
              "AI-WRITTEN: review every cell against the original script and compare the result with the Alteryx output. The Alteryx tools after this script are converted by rule\n"
              f'in the notebooks, which call `{name}.run(...)`. Edit this file if a cell needs to change."""\n')
    indented = "\n".join(("    " + ln) if ln.strip() else "" for ln in program.splitlines())
    tail = f"    return {', '.join(returned)}\n" if returned else "    return None\n"
    module = banner + "from pyspark.sql import functions as F, Window, types as T  # noqa: F401\n\n\n" + f"def run({params}):\n" + indented + "\n" + tail
    arguments = ", ".join(["spark"] + ["{in%d}" % info.inputs[n]["index"] for n in sorted(info.inputs)])
    targets = ", ".join("{out_%s}" % info.anchors.get(k, f"Output{k}") for k in info.consumed)
    call = [f"from utils import {name}  # the ported Python tool: utils/{name}.py (AI-WRITTEN, review it)", (f"{targets} = " if returned else "") + f"{name}.run({arguments})"]
    return module, "\n".join(call)
