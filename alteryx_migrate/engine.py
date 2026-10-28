"""Adapter over the existing tools: parse_and_map -> slim_mapping -> the migration skill (pre-flight, planner, emitters).

Nothing here converts by itself: every template chunk is the skill's own output (`unit_functions.build`), and the chunks the skill
cannot convert are reported with a hand-off comment. No model and no Databricks import in this module.

Assumptions (also in the README):
  * a chunk is one output table of the skill's plan (a Bronze/Silver/Gold table and the tools that build it);
  * the skill's answers file is empty ({}) at Analyze time except `accept.source_schema_at_runtime`; blocked tools become LLM or unsupported chunks.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

from . import paths

CHARS_PER_TOKEN = 4
EXPECTED_REPLY_TOKENS = 1500


class EngineError(Exception):
    """A step failed in a way the user must read (one message, no traceback)."""


@dataclass
class Chunk:
    id: str                                   # the output table name
    title: str
    container: str
    layer: str
    strategy: str                             # template | llm
    status: str                               # ok | review | unsupported
    inputs: list = field(default_factory=list)
    tools: list = field(default_factory=list)      # [{"id", "plugin", "expressions": [..]}]
    code: str = ""                            # the cell body (template output); empty for llm chunks until generated
    reasons: list = field(default_factory=list)
    est_tokens: int = 0
    result: str = ""
    sources: list = field(default_factory=list)       # Bronze input keys (read in the notebook's Sources cell)
    tables_read: list = field(default_factory=list)   # tables written by other chunks that this one reads (read right before it)
    chain: str = ""                                   # "Formula [55] -> Summarize [54] -> ..." shown above the cell
    unconverted: list = field(default_factory=list)   # tools that need the user: see describe_tool
    sql: str = ""                                      # the same table as one Spark SQL statement (tables written as @@in:key@@ / @@out:table@@)
    sql_error: str = ""                                # why there is no SQL for it (the PySpark is complete)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Analysis:
    file_name: str
    sha256: str
    tool_count: int
    container_count: int
    unsupported: list                          # [{"id", "plugin", "reason"}]
    chunks: list
    inputs: dict                               # bronze key -> {"source_tables": [...], "query": str}
    outputs: list                              # table names the plan produces
    warnings: list = field(default_factory=list)
    macros_missing: list = field(default_factory=list)
    coverage: dict = field(default_factory=dict)
    workdir: str = ""
    inventory: list = field(default_factory=list)       # every tool: {"id","name","container","status"}
    join_questions: list = field(default_factory=list)  # Join Multiple tools whose #k inputs are unknown: {"id","name","container","slots","tools":[{"id","name"}]}
    type_questions: list = field(default_factory=list)  # tools that depend on the type of a field the workflow does not state: {"id","name","container","fields"}

    def summary(self) -> dict:
        d = asdict(self)
        d.pop("chunks")
        return d


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def unpack(source: Path, dest: Path) -> Path:
    """Return the .yxmd inside `dest`. A .yxzp is a zip: the workflow and its macros are extracted together."""
    dest.mkdir(parents=True, exist_ok=True)
    source = Path(source)
    if source.suffix.lower() == ".yxzp":
        with zipfile.ZipFile(source) as z:
            for member in z.namelist():
                target = (dest / member).resolve()
                if dest.resolve() not in target.parents and target != dest.resolve():
                    raise EngineError(f"unsafe path inside the archive: {member}")
            z.extractall(dest)
        found = sorted(dest.rglob("*.yxmd"), key=lambda p: (len(p.parts), -p.stat().st_size))
        if not found:
            raise EngineError("the .yxzp archive contains no .yxmd workflow")
        for macro in dest.rglob("*.yxmc"):
            if macro.parent != found[0].parent:
                shutil.copyfile(macro, found[0].parent / macro.name)
        return found[0]
    if source.suffix.lower() != ".yxmd":
        raise EngineError("upload a .yxmd or .yxzp file")
    target = dest / source.name
    if source.resolve() != target.resolve():
        shutil.copyfile(source, target)
    return target


def macro_names(yxmd: Path) -> list:
    names = set()
    for _, el in ET.iterparse(str(yxmd), events=("end",)):
        for value in el.attrib.values():
            if value.lower().endswith(".yxmc"):
                names.add(Path(value.replace("\\", "/")).name)
        if el.tag == "Node" and not list(el):
            el.clear()
    return sorted(names)


def _run(args: list, what: str) -> subprocess.CompletedProcess:
    done = subprocess.run([str(a) for a in args], capture_output=True, text=True, encoding="utf-8", errors="replace")
    if done.returncode != 0:
        tail = "\n".join((done.stdout + done.stderr).strip().splitlines()[-6:])
        raise EngineError(f"{what} failed (exit {done.returncode}):\n{tail}")
    return done


def parse_and_slim(yxmd: Path, work: Path) -> tuple[Path, Path, dict]:
    """parse_and_map then slim_mapping, as separate processes. Returns (mapping.json, slim dir, the parsed document)."""
    work.mkdir(parents=True, exist_ok=True)
    mapping, slim = work / "mapping.json", work / "slim"
    _run([sys.executable, paths.parse_script(), yxmd, "--json", "-o", mapping], "parse_and_map")
    if slim.is_dir():
        shutil.rmtree(slim)
    slim.mkdir(parents=True)
    done = subprocess.run([sys.executable, str(paths.slim_script()), str(mapping), "--out", str(slim), "--yxmd", str(yxmd)],
                          capture_output=True, text=True, encoding="utf-8", errors="replace")
    if done.returncode == 1:
        raise EngineError("slim_mapping found a secret value in its own output; nothing was kept")
    if done.returncode != 0:
        raise EngineError("slim_mapping failed:\n" + "\n".join((done.stdout + done.stderr).strip().splitlines()[-6:]))
    return mapping, slim, json.loads(mapping.read_text(encoding="utf-8"))


def _plugin_of(wf, nid: int) -> str:
    node = wf.nodes.get(nid, {})
    return str(node.get("original_plugin_name") or node.get("tool_type") or wf.kind(nid))


def _expressions(doc: dict) -> dict:
    grouped: dict = {}
    for item in doc.get("expressions", []):
        grouped.setdefault(int(item["node_id"]), []).append(
            {k: item.get(k) for k in ("label", "alteryx_expression") if item.get(k)})
    return grouped


def tool_comment(step: dict) -> list:
    """The skill's own style: `# [id] Kind` above the lines of each tool."""
    return [f"# [{step['tool']}] {step['label']}"] + list(step["lines"])


def commented_body(table: dict) -> str:
    """The transformation steps of a table, one commented block per Alteryx tool. No reads and no write: the notebook adds those around it."""
    steps = table.get("steps") or []
    lines = [ln for st in steps for ln in tool_comment(st)] if steps else list(table["body"])
    return "\n".join(lines) + "\n"


def raw_configs(workdir: str, ids, limit: int = 5000) -> dict:
    """{tool id: the tool's original settings as XML} read straight from the .yxmd (the source of truth: the parser's model of a tool can be approximate).
    Includes the plugin name and, for a tool that is a macro, the macro file."""
    out: dict = {}
    wanted = {int(i) for i in ids}
    for path in sorted((Path(workdir) / "input").rglob("*.yxmd"))[:1]:
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            return out
        for node in root.iter("Node"):
            try:
                tid = int(node.get("ToolID"))
            except (TypeError, ValueError):
                continue
            if tid not in wanted:
                continue
            gui, engine_settings = node.find("GuiSettings"), node.find("EngineSettings")
            cfg = node.find("./Properties/Configuration")
            parts = [f"<!-- plugin: {(gui.get('Plugin') if gui is not None else '') or '?'}" + (f"; macro: {engine_settings.get('Macro')}" if engine_settings is not None and engine_settings.get("Macro") else "") + " -->"]
            if cfg is not None:
                parts.append(re.sub(r">\s+<", "><", ET.tostring(cfg, encoding="unicode").strip()))
            text = "\n".join(parts)
            out[tid] = text if len(text) <= limit else text[:limit] + " ... (truncated)"
    return out


def tool_name(wf, nid: int) -> str:
    import notebooks as NB
    label = NB.tool_label(wf, nid)
    if label in ("Unknown", "", "None") or str(label).strip().isdigit():                       # a tool the parser did not name: show the plugin the workflow file says
        plugin = str(wf.nodes[nid].get("original_plugin_name") or wf.nodes[nid].get("node_type") or "tool")
        plugin = re.sub(r"_v\d+(\.\d+)*$", "", plugin)               # big_query_output_v3.0.0 -> big_query_output
        return plugin.rsplit(".", 1)[-1]
    return label


def neighbours(wf, nid: int) -> tuple:
    before = sorted({e.src for e in wf.producers(nid)})
    after = sorted({e.dst for e in wf.consumers(nid)})
    return before, after


def where_text(wf, ids: list, empty: str) -> str:
    return ", ".join(f"[{n}] {tool_name(wf, n)}" for n in ids) or empty


def blocked_roots(wf, nid: int, roots: set) -> list:
    """The blocked tools upstream of (or equal to) a tool that only waits."""
    seen, stack, found = set(), [nid], []
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        if n in roots:
            found.append(n)
            continue
        stack += [e.src for e in wf.producers(n)]
    return sorted(found)


def describe_tool(wf, info: dict, nid: int, reason: str, roots: set, container: str, layer: str) -> dict:
    """Everything the user needs to find and fix a tool that was not converted: number, name, container, position and where to update the code."""
    before, after = neighbours(wf, nid)
    waits = [] if nid in roots else blocked_roots(wf, nid, roots)
    notebook = "03_gold.ipynb" if layer == "gold" else "02_silver.ipynb"
    return {"id": int(nid), "name": tool_name(wf, nid), "plugin": str(wf.nodes[nid].get("original_plugin_name") or ""), "container": container,
            "status": "cannot be converted" if nid in roots else "waits for " + ", ".join(f"[{r}] {tool_name(wf, r)}" for r in waits),
            "decision": "order" if "record order" in reason or "order key" in reason else "", "reason": reason,
            "data_input": any(not str(e.dst_anchor).lower().startswith("control") for e in wf.producers(nid)), "after": where_text(wf, before, "nothing (this tool starts the flow)"), "before": where_text(wf, after, "nothing (this tool ends the flow)"), "layer": layer, "notebook": notebook}


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // CHARS_PER_TOKEN)


def slim_nodes(slim_dir: Path, ids: list) -> list:
    """The slimmed description of the given tools (what the model is allowed to see; secrets are already removed)."""
    path = Path(slim_dir) / "slim.json"
    if not path.exists():
        return []
    wanted = {int(i) for i in ids}
    doc = json.loads(path.read_text(encoding="utf-8"))
    return [n for n in doc.get("nodes", []) if int(n.get("node_id", -1)) in wanted]


BASE_ANSWERS = {"accept": {"source_schema_at_runtime": True}}


def merge_answers(decisions: dict | None) -> dict:
    """The skill's answers file: the app's base answers plus what the user decided (accept.*, overrides.<tool id>.*, macros.*, project.*)."""
    out = {"accept": dict(BASE_ANSWERS["accept"])}
    for key, value in (decisions or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key].update(value)
        else:
            out[key] = value
    return out


def decisions_from_choices(choices: dict) -> dict:
    """UI choices -> skill answers. choices = {tool id: {"mode": "any" | "order_by", "fields": [..], "ascending": bool}};
    choices["_join_inputs"] = {tool id: {"#k": id of the tool that feeds input #k}} (Join Multiple); choices["_field_types"] = {field: "string" | "number" | "date" | "datetime" | "bool"}: types of fields the workflow does not state, for every tool."""
    any_row, overrides = [], {}
    out: dict = {}
    for tool, c in choices.items():
        if tool == "_field_types":
            if c:
                out["field_types"] = dict(c)
            continue
        if tool == "_macros":                               # {macro file: {"as_source": true, "table": "catalog.schema.table"}}
            if c:
                out["macros"] = dict(c)
            continue
        if tool == "_join_inputs":                          # {join multiple tool id: {"#1": tool id feeding it, ...}}
            if c:
                out["join_inputs"] = {str(k): dict(v) for k, v in c.items()}
            continue
        if str(tool).startswith("_"):                       # the report's records (_auto: who decided what, _left_to_model): not answers for the skill
            continue
        if c.get("mode") == "any":
            any_row.append(int(tool))
        elif c.get("mode") == "order_by" and c.get("fields"):
            desc = set(c.get("desc") or [])
            overrides[str(int(tool))] = {"order_by": [{"field_name": f, "ascending": bool(c.get("ascending", True)) and f not in desc} for f in c["fields"]]}
    if any_row:
        out["accept"] = {"order_any": sorted(any_row)}
    if overrides:
        out["overrides"] = overrides
    return out


def analyze(source: Path, work: Path, file_name: str | None = None, decisions: dict | None = None) -> tuple[Analysis, dict]:
    """Parse, slim and plan. No model is called. Returns (Analysis, internal handles for the prompts)."""
    work = Path(work)
    yxmd = unpack(source, work / "input")
    mapping, slim, doc = parse_and_slim(yxmd, work)
    return plan_workflow(yxmd, mapping, slim, doc, work, file_name or Path(source).name, sha256_file(Path(source)), decisions)


def reanalyze(work: Path, file_name: str, decisions: dict | None = None) -> tuple[Analysis, dict]:
    """Plan again from the files an earlier analyze left in `work` (no parsing): used after the user answered a question."""
    work = Path(work)
    files = sorted((work / "input").glob("*.yxmd"))
    mapping, slim = work / "mapping.json", work / "slim"
    if not files or not mapping.exists():
        raise EngineError("The analysed files of this run are no longer on the server. Upload the workflow again to apply the decisions.")
    return plan_workflow(files[0], mapping, slim, json.loads(mapping.read_text(encoding="utf-8")), work, file_name, sha256_file(files[0]), decisions)


def plan_workflow(yxmd: Path, mapping: Path, slim: Path, doc: dict, work: Path, file_name: str, sha: str, decisions: dict | None) -> tuple[Analysis, dict]:
    paths.use_skill()
    import containers as CT
    import preflight as PF
    import unit_functions as UF
    import wfmodel as M
    answers = merge_answers(decisions)
    wf = M.load_workflow(str(mapping), dict(answers.get("overrides") or {}), str(yxmd))
    PF.apply_answers(wf, answers)
    plan = M.plan_medallion(wf, naming={})
    built = UF.build(wf, plan, answers)
    info = CT.read(str(yxmd))
    expr = _expressions(doc)
    units = {u.key: u for u in plan.units}

    def tool_rows(ids):
        return [{"id": int(n), "plugin": _plugin_of(wf, n), "expressions": expr.get(int(n), [])} for n in ids]

    def container(ids):
        for n in ids:
            trail = info["path"].get(int(n)) or []
            if trail:
                return info["caption"].get(trail[-1], f"Container {trail[-1]}")
        return "(no container)"

    roots = set(built["blocked"]) | set(built["outputs_without_converter"])
    chunks: list = []
    for t in built["tables"]:
        steps = t.get("steps") or []
        chain = " -> ".join(f"{st['label']} [{st['tool']}]" for st in steps)
        chunks.append(Chunk(id=t["table"], title=t["table"], container=container(t["tools"]), layer=t["layer"], strategy="template",
                            status="review" if t["reviews"] else "ok", inputs=list(t["inputs"]), tools=tool_rows(t["tools"]), code=commented_body(t),
                            reasons=list(t["reviews"]) + list(t["notes"]), result=t["result"], chain=chain,
                            sources=[k for k in t["inputs"] if k in built["bronze"]], tables_read=[k for k in t["inputs"] if k not in built["bronze"]],
                            sql=(t.get("sql") or {}).get("statement") or "", sql_error=(t.get("sql") or {}).get("error") or ""))
    unconverted_by_tool: dict = {}
    for w in built["waiting"]:
        unit = units.get(w["unit"])
        ids = [n for n in (unit.nodes if unit else []) if wf.kind(n) != "read"]
        bad = [n for n in w["tools"] if n in ids] or ids
        for table in w["tables"]:
            layer = "gold" if table.startswith("gold_") else "silver"
            box = container(ids)
            rows = [describe_tool(wf, info, n, built["blocked"].get(n) or "; ".join(w["why"]) or "waiting for input", roots, box, layer) for n in bad]
            for r in rows:
                unconverted_by_tool.setdefault(r["id"], r)
            chunks.append(Chunk(id=table, title=table, container=box, layer=layer, strategy="llm", status="review", inputs=[], tools=tool_rows(ids),
                                reasons=list(w["why"]) or ["waiting for input"], chain=" -> ".join(f"{tool_name(wf, n)} [{n}]" for n in ids),
                                unconverted=rows))
    unsupported = []
    for n in sorted(roots):
        base = unconverted_by_tool.get(n)
        if base is None:
            output = n in built["outputs_without_converter"]
            reason = "output tool: the converter does not write to the destination; add the export after the tool it follows" if output else built["blocked"].get(n, "")
            base = describe_tool(wf, info, n, reason, roots, container([n]), "gold")
            base["notebook"] = "(none: add it where you need the export)" if output else base["notebook"]
        unsupported.append({**base, "reason": built["blocked"].get(n) or base["reason"]})
    converted_ids = {t["id"] for c in chunks if c.strategy == "template" for t in c.tools}
    waiting_ids = {u["id"] for c in chunks for u in c.unconverted}
    outputs_ids = set(built["outputs_without_converter"])
    inventory = []
    for nid in sorted(wf.nodes):
        if nid in outputs_ids:
            status = "output"
        elif nid in built["blocked"]:
            status = "blocked"
        elif nid in waiting_ids:
            status = "waiting"
        elif nid in converted_ids or (wf.kind(nid) == "read" and wf.consumers(nid)):
            status = "converted"
        else:
            status = "not_needed"                       # not needed for any output (a dead end) or a reference-only tool
        inventory.append({"id": int(nid), "name": tool_name(wf, nid), "container": container([nid]), "status": status})
    present = {p.name.lower() for p in Path(yxmd).parent.glob("*.yxmc")}
    missing = [m for m in macro_names(yxmd) if m.lower() not in present]
    cov = built["coverage"].get("summary", {}) if isinstance(built["coverage"], dict) else {}
    analysis = Analysis(file_name=file_name, sha256=sha, tool_count=int(doc["workflow"].get("tool_count") or 0),
                        container_count=len(info["caption"]), unsupported=unsupported, chunks=chunks,
                        inputs={k: {"source_tables": list(v["source_tables"]), "query": v["query"], "tools": [int(n) for n in v.get("nodes") or []],
                                "plugin": _plugin_of(wf, v["nodes"][0]) if v.get("nodes") else "",
                                "connection": str(wf.nodes[v["nodes"][0]].get("connection_string") or "") if v.get("nodes") else ""} for k, v in built["bronze"].items()},
                        outputs=[c.id for c in chunks], macros_missing=missing, coverage=cov, workdir=str(work),
                        warnings=[f"macro not uploaded: {m}" for m in missing], inventory=inventory,
                        join_questions=[{"id": int(n), "name": tool_name(wf, n), "container": container([n]), "slots": wf.nodes[n]["_join_needed"]["slots"],
                                         "tools": [{"id": int(t), "name": tool_name(wf, t)} for t in wf.nodes[n]["_join_needed"]["tools"]]}
                                        for n in sorted(wf.nodes) if wf.nodes[n].get("_join_needed")],
                        type_questions=[{"id": int(n), "name": tool_name(wf, n), "container": container([n]), "fields": list(wf.nodes[n]["_types_needed"])}
                                        for n in sorted(wf.nodes) if wf.nodes[n].get("_types_needed")])
    return analysis, {"slim_dir": slim, "wf": wf}

