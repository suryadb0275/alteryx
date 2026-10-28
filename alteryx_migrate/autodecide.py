"""Answers to the routine questions (the order of the records) taken from the workflow itself, or proposed by the model and checked in code.

Alteryx keeps the order in which rows arrive; Spark has none. A Unique, Sample, Multi-Row Formula, Running Total or Record ID with nothing upstream that sets an
order is a question for the user. Many of those questions have their answer in the workflow:

A. rules (applied, confidence high)
   - the input query ends with ORDER BY, and every tool between it and the question keeps the order and the fields: that is the order the rows arrive in;
   - a Unique whose every consumer uses only its key fields: which record of a group is kept cannot change the result, so "any" is exact.
B. the model (only for what A could not settle, and only when a model is configured): it sees the tool's settings, its neighbours, the source query and the skill's
   recipe, and must answer JSON. The answer is checked in code - every field must be in the evidence it was given (the tool, the tools before it, the source),
   an order needs a field that is not only a group / key field, "any" is accepted only for a Unique or Sample at high confidence. Anything else is "unresolved".
C. high -> applied (listed in the report for review); medium -> asked, with the proposal as the default; low, invalid, an error or no model -> asked as before.

Answers (the user's and the automatic ones) are kept per workflow in .alteryx_migrate/answers/<sha12>.json, each with a hash of the tool's settings: a re-run asks
nothing that was answered, and a changed tool is asked again.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

from . import engine, paths, sqlsrc

ORDER_KINDS = {"unique": "Unique", "sample": "Sample", "multirow": "Multi-Row Formula", "runningtotal": "Running Total", "recordid": "Record ID"}
# NULL placement of a plain ORDER BY in each engine (ascending, descending); the skill sorts like Alteryx: NULLs first ascending, last descending
NULLS_FIRST = {"bigquery": (True, False), "tsql": (True, False), "mysql": (True, False), "spark": (True, False),
               "postgres": (False, True), "oracle": (False, True), "snowflake": (False, True)}


@dataclass
class AutoResult:
    decisions: dict = field(default_factory=dict)      # str(tool id) -> {"mode": "any" | "order_by", "fields": [...], "desc": [...]}
    evidence: dict = field(default_factory=dict)       # str(tool id) -> {"source", "confidence", "why", "name", "container", "decision"}
    suggestions: dict = field(default_factory=dict)    # str(tool id) -> a medium-confidence proposal: asked, shown as the default
    unresolved: list = field(default_factory=list)     # inventory items still to ask


# ---------------------------------------------------------------------------------------------------------------- answers cache
def tool_hashes(workdir: str) -> dict:
    """{tool id: sha1 of the tool's plugin, macro and settings} from the workflow XML."""
    out = {}
    for path in sorted((Path(workdir) / "input").rglob("*.yxmd"))[:1]:
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            return out
        for node in root.iter("Node"):
            tid = node.get("ToolID")
            if not str(tid).isdigit():
                continue
            gui, eng, cfg = node.find("GuiSettings"), node.find("EngineSettings"), node.find("./Properties/Configuration")
            text = "|".join([gui.get("Plugin", "") if gui is not None else "", eng.get("Macro", "") if eng is not None else "",
                             re.sub(r">\s+<", "><", ET.tostring(cfg, encoding="unicode")) if cfg is not None else ""])
            out[int(tid)] = hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]
    return out


class AnswerCache:
    """Answers given for one workflow, per tool and kind ("order", "note", "macro", "join", "python", "type", and "run" for the run's settings). A tool's answer
    is used only while that tool's settings are unchanged, so editing one tool asks again about that tool only. The file is named after the workflow, which is
    why it survives an edit (the file hash changes); the hash of the last run is kept inside. Kept in <answers dir>/<workflow name>.json; the answers dir is
    .alteryx_migrate/answers of the folder the command runs from (or $ALTERYX_ANSWERS_DIR)."""

    def __init__(self, name: str, sha256: str, hashes: dict, root: str | os.PathLike | None = None):
        base = Path(root or os.environ.get("ALTERYX_ANSWERS_DIR") or Path.cwd() / ".alteryx_migrate" / "answers")
        self.path = base / (re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(name).stem).strip("_") + ".json")
        self.sha256 = sha256
        self.hashes = {str(k): v for k, v in hashes.items()}
        try:
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            self.data = {}

    def forget(self) -> None:
        self.data = {}
        try:
            self.path.unlink()
        except OSError:
            pass

    def get(self, kind: str, key, tool=None):
        """The stored answer, or None when there is none or the tool's settings changed since."""
        item = self.data.get(kind, {}).get(str(key))
        if not item:
            return None
        tool = key if tool is None else tool
        if str(tool).isdigit() and item.get("hash") != self.hashes.get(str(tool)):
            return None
        return item

    def put(self, kind: str, key, value, source: str = "you", tool=None, **extra) -> None:
        tool = key if tool is None else tool
        self.data.setdefault(kind, {})[str(key)] = {"value": value, "source": source, "hash": self.hashes.get(str(tool)), **extra}

    def save(self) -> None:
        try:
            self.data["sha256"] = self.sha256
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(self.data, indent=1, sort_keys=True), encoding="utf-8")
        except OSError:
            pass                                                   # a read-only folder: the answers are simply not remembered


# ---------------------------------------------------------------------------------------------------------------- A. rules
def _main_producer(wf, nid):
    edges = wf.producers(nid)
    if wf.kind(nid) == "append":
        edges = [e for e in edges if e.dst_anchor == "Targets"]
    if len(edges) != 1:
        return None
    return edges[0]


def _carry_fields(wf, nid, names):
    """The order fields after tool `nid` (renames followed), or None when the tool drops, overwrites or may change one of them."""
    node, kind = wf.nodes[nid], wf.kind(nid)
    low = [n.lower() for n in names]
    if kind == "select":
        ops = node.get("field_operations") or []
        by = {str(o.get("field_name", "")).lower(): o for o in ops}
        unknown = by.get("*unknown")
        keep_unknown = unknown is None or (unknown.get("selected") is not False and unknown.get("action") != "deselect")
        out = []
        for n in names:
            op = by.get(n.lower())
            if op is None:
                if not keep_unknown:
                    return None
                out.append(n)
            elif op.get("selected") is False or op.get("action") == "deselect":
                return None
            else:
                out.append(op.get("rename_to") or n)
        return out
    written = set()
    if kind == "formula":
        written = {str(f.get("output_field") or "").lower() for f in node.get("formulas") or []}
    elif kind in ("multirow", "datetime", "recordid"):
        written = {str(node.get("output_field") or "").lower()}
    elif kind == "text2cols":
        written = {str(node.get("field_name") or "").lower()} if node.get("split_to") == "rows" else set()
    elif kind == "findreplace":
        written = {str(node.get("fr_within") or "").lower()}
    elif kind == "cleanse":
        written = {str(f).lower() for f in (node.get("cleanse") or {}).get("fields") or []}
    elif kind == "impute":
        written = {str(f).lower() for f in (node.get("impute") or {}).get("fields") or []}
    elif kind in ("filter", "unique", "sample", "runningtotal", "passthrough", "autofield", "append", "browse", "write"):
        written = set()
    else:
        return None                                                # a tool whose effect on the fields is not certain here
    return None if written & set(low) else list(names)


def _query_order(query: str, dialect: str):
    """[(output column, descending)] of the ORDER BY that ends the query, or None (no ORDER BY, an expression, or a NULL order other than Alteryx's)."""
    import sqlglot
    from sqlglot import exp
    if not query or not dialect:
        return None
    try:
        tree = sqlglot.parse_one(query, read=dialect)
    except sqlglot.errors.SqlglotError:
        return None
    order = tree.args.get("order") if tree is not None else None
    if order is None:
        return None
    select = sqlsrc._final_select(tree)
    projections = list(select.expressions) if select is not None else []
    has_star = any(isinstance(p, exp.Star) or (isinstance(p, exp.Column) and isinstance(p.this, exp.Star)) for p in projections)
    default = NULLS_FIRST.get(dialect)
    out = []
    for o in order.expressions:
        if not isinstance(o, exp.Ordered):
            return None
        desc = bool(o.args.get("desc"))
        nulls_first = o.args.get("nulls_first")
        if nulls_first is None:
            if default is None:
                return None
            nulls_first = default[1] if desc else default[0]
        if bool(nulls_first) != (not desc):                        # Alteryx / the skill: NULLs first ascending, last descending
            return None
        e = o.this
        name = None
        if isinstance(e, exp.Literal) and e.is_int and 0 < int(e.this) <= len(projections):
            p = projections[int(e.this) - 1]
            name = p.alias if isinstance(p, exp.Alias) else (p.name if isinstance(p, exp.Column) else None)
        elif isinstance(e, exp.Column):
            for p in projections:
                if isinstance(p, exp.Alias) and isinstance(p.this, exp.Column) and p.this.name.lower() == e.name.lower() and (not e.table or p.this.table == e.table):
                    name = p.alias
                    break
                if isinstance(p, exp.Alias) and p.alias.lower() == e.name.lower() and not e.table:
                    name = p.alias
                    break
                if isinstance(p, exp.Column) and p.name.lower() == e.name.lower():
                    name = p.name
                    break
            if name is None and has_star:
                name = e.name                                      # a column of the source table that SELECT * passes on
        if not name:
            return None
        out.append((name, desc))
    return out or None


def source_order(wf, nid, sql_dialect: str = ""):
    """(decision, why) when the rows reach tool `nid` in the order of an ORDER BY at the end of the input query, through tools that keep the order and the fields."""
    paths.use_skill()
    import wfmodel as M
    path, cur = [], nid
    for _ in range(200):
        edge = _main_producer(wf, cur)
        if edge is None:
            return None
        src = edge.src
        if wf.kind(src) == "read":
            break
        if M.NODE_SPECS.get(wf.nodes[src]["node_type"], {}).get("order") != "preserve":
            return None
        path.append(src)
        cur = src
    else:
        return None
    node = wf.nodes[src]
    dialect = sqlsrc.dialect_of(engine._plugin_of(wf, src), str(node.get("connection_string") or ""), sql_dialect)
    order = _query_order(str(node.get("query") or ""), dialect)
    if not order:
        return None
    names = [n for n, _ in order]
    desc = {n for n, d in order if d}
    for step in reversed(path):
        new = _carry_fields(wf, step, names)
        if new is None:
            return None
        desc = {new[names.index(d)] for d in desc}
        names = new
    text = ", ".join(f"{n}{' desc' if n in desc else ''}" for n in names)
    return ({"mode": "order_by", "fields": names, "desc": sorted(desc)},
            f"the input query of tool [{src}] ends with ORDER BY {text}, and the tools between keep that order and those fields")


def _columns_after(wf, nid, cols):
    """The field list after tool `nid` given the list before it, or None when the tool's effect on the field list is not certain here."""
    node, kind = wf.nodes[nid], wf.kind(nid)
    low = {c.lower() for c in cols}

    def plus(*names):
        return cols + [n for n in names if n and n.lower() not in low]
    if kind == "select":
        ops = node.get("field_operations") or []
        by = {str(o.get("field_name", "")).lower(): o for o in ops}
        unknown = by.get("*unknown")
        keep_unknown = unknown is None or (unknown.get("selected") is not False and unknown.get("action") != "deselect")
        out = []
        for o in ops:
            name = str(o.get("field_name", ""))
            if name == "*Unknown" or o.get("selected") is False or o.get("action") == "deselect":
                continue
            if name.lower() not in low:
                continue                                           # a listed field the data does not have is left out (as Alteryx does)
            out.append(o.get("rename_to") or name)
        if keep_unknown:
            out += [c for c in cols if c.lower() not in by]
        return out
    if kind == "formula":
        return plus(*[str(f.get("output_field") or "") for f in node.get("formulas") or []])
    if kind in ("datetime", "recordid") or (kind == "multirow" and not node.get("update_field")):
        return plus(str(node.get("output_field") or ""))
    if kind == "runningtotal":
        return plus(*[f"RunTot_{f}" for f in node.get("running_fields") or []])
    if kind == "text2cols":
        if node.get("split_to") == "rows":
            return cols
        root = node.get("output_root_name") or node.get("field_name") or ""
        return plus(*[f"{root}{i + 1}" for i in range(int(node.get("num_columns") or 0))]) if node.get("num_columns") else None
    if kind == "cleanse":
        return None if (node.get("cleanse") or {}).get("remove_null_columns") else cols
    if kind == "findreplace":
        return plus(*(node.get("fr_append_fields") or [])) if node.get("fr_replace_mode") == "Append" else cols
    if kind in ("filter", "sort", "unique", "sample", "passthrough", "autofield", "multirow"):
        return cols
    return None


def known_columns(wf, nid, sql_dialect: str = ""):
    """The field list arriving at tool `nid` - exact from the input query's listed columns (or the input's MetaInfo), or an upper bound from a Select with
    '*Unknown' off - followed through tools with a certain effect on the field list; None when it is not known. Only used to test 'every field is a key'."""
    import sqlglot
    path, cur, cols = [], nid, None
    for _ in range(200):
        edge = _main_producer(wf, cur)
        if edge is None:
            return None
        src = wf.nodes[edge.src]
        if wf.kind(edge.src) == "select":
            ops = src.get("field_operations") or []
            if any(o.get("field_name") == "*Unknown" and (o.get("selected") is False or o.get("action") == "deselect") for o in ops):
                # '*Unknown' off: at most the listed fields go on, whatever came before (a listed field the data lacks is left out) - an upper bound
                cols = [o.get("rename_to") or o["field_name"] for o in ops if o["field_name"] != "*Unknown" and o.get("selected") is not False and o.get("action") != "deselect"]
                break
        if wf.kind(edge.src) == "read":
            break
        path.append(edge.src)
        cur = edge.src
    else:
        return None
    node = wf.nodes[edge.src]
    if cols is not None:
        pass
    elif node.get("query"):
        dialect = sqlsrc.dialect_of(engine._plugin_of(wf, edge.src), str(node.get("connection_string") or ""), sql_dialect)
        try:
            cols = sqlsrc.projection_columns(sqlglot.parse_one(str(node["query"]), read=dialect)) if dialect else None
        except sqlglot.errors.SqlglotError:
            cols = None
    elif node.get("field_info"):
        cols = [f["name"] for f in node["field_info"] if f.get("name")]
    for step in reversed(path):
        if cols is None:
            return None
        cols = _columns_after(wf, step, list(cols))
    return cols


def unique_keys_are_every_field(wf, nid, sql_dialect: str = ""):
    """Why 'any record' is exact for a Unique whose key fields are every field it receives: the records of a group are then identical."""
    keys = {str(k).lower() for k in wf.nodes[nid].get("key_fields") or []}
    cols = known_columns(wf, nid, sql_dialect)
    if not keys or not cols or not {c.lower() for c in cols} <= keys:
        return None
    return f"its key fields ({', '.join(cols)}) are every field it receives, so the records of a group are identical"


def unique_any_is_exact(wf, nid):
    """Why 'any record' is exact for a Unique: every consumer of every output uses only the key fields (a Select keeping only them, a Summarize on them)."""
    node = wf.nodes[nid]
    keys = {str(k).lower() for k in node.get("key_fields") or []}
    users = wf.consumers(nid)
    if not keys or not users:
        return None
    users = [e for e in users if wf.kind(e.dst) not in ("browse", "comment")]        # a Browse shows the data; it changes no output
    if not users:
        return None
    for e in users:
        k, dst = wf.kind(e.dst), wf.nodes[e.dst]
        if k == "select":
            ops = dst.get("field_operations") or []
            kept = [str(o["field_name"]) for o in ops if o.get("selected") is not False and o.get("action") != "deselect"]
            if "*Unknown" in kept or not {x.lower() for x in kept if x != "*Unknown"} <= keys or not any(o["field_name"] == "*Unknown" for o in ops):
                return None
        elif k == "summarize":
            if not {str(a.get("field_name", "")).lower() for a in dst.get("aggregations") or []} <= keys:
                return None
        else:
            return None
    return f"every tool after it ({', '.join('[%s]' % e.dst for e in users)}) uses only its key fields, so which record of a group is kept does not change the result"


# ---------------------------------------------------------------------------------------------------------------- B. the model
SYSTEM = """You help convert an Alteryx workflow to Databricks. Alteryx processes records in the order they arrive; Spark has no order. The tool below depends on
that order and nothing in the workflow sorts the records before it. From the evidence (the tool's XML settings, the tools before and after it, the source query and
fields, the skill's notes) decide which field(s) give the order the records arrive in for this tool's job - typically a date, sequence or id column - or, for a
Unique or Sample only, whether any record of a group is equally right.

Rules: use only field names that appear in the evidence before or at this tool. Do not invent fields. Do not order by the group / key fields alone. If the evidence
does not show such an order, answer confidence "low".
Answer with ONE JSON object and nothing else:
{"mode": "any" | "order_by", "fields": ["..."], "desc": ["..."], "confidence": "high" | "medium" | "low", "evidence": "<one sentence quoting the XML or query that shows it>"}"""


def _tool_fields(node, kind):
    """(group / key fields, the tool's own output fields): an order made of only the first is no order; ordering by the second is impossible."""
    group = node.get("key_fields") if kind == "unique" else node.get("group_fields")
    own = [node.get("output_field")] if kind in ("multirow", "recordid") and not node.get("update_field") else []
    return {str(g).lower() for g in group or []}, {str(o).lower() for o in own if o}


def _walk_up(wf, nid, depth):
    out, frontier = [], [nid]
    for _ in range(depth):
        nxt = []
        for n in frontier:
            for e in wf.producers(n):
                if e.src not in out:
                    out.append(e.src)
                    nxt.append(e.src)
        frontier = nxt
    return out


def _source_text(wf, nid):
    """The query and the field names of the inputs the tool's data comes from."""
    seen, stack, texts = set(), [nid], []
    while stack:
        n = stack.pop()
        for e in wf.producers(n):
            if e.src in seen:
                continue
            seen.add(e.src)
            node = wf.nodes[e.src]
            if wf.kind(e.src) == "read":
                names = [f.get("name") for f in node.get("field_info") or [] if f.get("name")]
                texts.append(f"input tool [{e.src}] query: {node.get('query') or '(none)'}\ninput tool [{e.src}] fields: {', '.join(names) or '(not listed)'}")
            else:
                stack.append(e.src)
    return "\n".join(texts)


def _evidence(analysis, handles, nid):
    """(prompt text, the text a field must appear in): the downstream tools are shown to the model but cannot vouch for a field."""
    import tool_coverage as TC
    wf = handles["wf"]
    up = _walk_up(wf, nid, 2)
    down = [e.dst for e in wf.consumers(nid)]
    xml = engine.raw_configs(analysis.workdir, [nid] + up + down, limit=3000)
    slim = engine.slim_nodes(handles["slim_dir"], [nid]) if handles.get("slim_dir") else []
    node = wf.nodes[nid]
    recipe = TC.explain(node.get("original_tool_type"), node.get("original_plugin_name")) or ""
    source = _source_text(wf, nid)
    vouch = "\n".join([xml.get(nid, ""), *[xml.get(n, "") for n in up], json.dumps(slim, default=str), source])
    prompt = json.dumps({"tool": {"id": nid, "type": ORDER_KINDS.get(wf.kind(nid), wf.kind(nid)), "xml": xml.get(nid, ""), "slim": slim},
                         "tools_before": {str(n): xml.get(n, "") for n in up}, "tools_after": {str(n): xml.get(n, "") for n in down},
                         "source": source, "skill_notes": recipe}, indent=1, default=str)
    return prompt, vouch


def _has_token(text: str, name: str) -> bool:
    return bool(re.search(r"(?<![\w])" + re.escape(name) + r"(?![\w])", text, re.I))


def check_proposal(reply: str, kind: str, node: dict, vouch: str):
    """The model's JSON, validated: (decision, confidence, evidence sentence) or (None, reason)."""
    m = re.search(r"\{.*\}", reply or "", re.S)
    if not m:
        return None, "no JSON in the reply"
    try:
        data = json.loads(m.group(0))
    except ValueError:
        return None, "the reply is not valid JSON"
    if not isinstance(data, dict):
        return None, "the reply is not a JSON object"
    mode, confidence = data.get("mode"), str(data.get("confidence") or "").lower()
    if confidence not in ("high", "medium", "low"):
        return None, "no confidence given"
    if mode == "any":
        if kind not in ("unique", "sample") or confidence != "high":
            return None, "'any record' is accepted only for a Unique or Sample at high confidence"
        return ({"mode": "any"}, confidence, str(data.get("evidence") or "")), ""
    if mode != "order_by":
        return None, f"unknown mode {mode!r}"
    fields = data.get("fields")
    if not isinstance(fields, list) or not fields or not all(isinstance(f, str) and f.strip() for f in fields):
        return None, "no fields"
    fields = [f.strip() for f in fields]
    unknown = [f for f in fields if not _has_token(vouch, f)]
    if unknown:
        return None, f"{unknown} do not appear in the settings of the tool, the tools before it or the source"
    group, own = _tool_fields(node, kind)
    if {f.lower() for f in fields} & own:
        return None, "it orders by the field the tool itself creates"
    if all(f.lower() in group for f in fields):
        return None, "it orders only by the group / key fields, which is no order within a group"
    desc = [d for d in data.get("desc") or [] if isinstance(d, str) and d.strip() in fields]
    return ({"mode": "order_by", "fields": fields, "desc": desc}, confidence, str(data.get("evidence") or "")), ""


def propose(client, analysis, handles, nid):
    """Ask the model; never raises. -> ((decision, confidence, evidence), "") or (None, why)."""
    wf = handles["wf"]
    try:
        prompt, vouch = _evidence(analysis, handles, nid)
        reply = client.complete(SYSTEM, "Evidence:\n```json\n" + prompt + "\n```")
    except Exception as exc:                                       # noqa: BLE001 - a model failure only means the question is asked
        return None, f"the model call failed ({type(exc).__name__})"
    return check_proposal(reply, wf.kind(nid), wf.nodes[nid], vouch)


# ---------------------------------------------------------------------------------------------------------------- the decision
def describe(decision: dict) -> str:
    if decision.get("mode") == "any":
        return "any record of a group (the order does not matter)"
    desc = set(decision.get("desc") or [])
    return "rows ordered by " + ", ".join(f + (" (descending)" if f in desc else "") for f in decision.get("fields") or [])


def decide(analysis, handles, client=None, cache=None, items=None, sql_dialect: str = "") -> AutoResult:
    """Settle the order questions of `items` (default: every order item of analysis.unsupported) from the cache, the rules, then the model."""
    wf, out = handles["wf"], AutoResult()
    for u in items if items is not None else [u for u in analysis.unsupported if u.get("decision") == "order"]:
        nid, key = int(u["id"]), str(u["id"])
        label = {"name": u.get("name", ""), "container": u.get("container", "")}
        stored = cache.get("order", nid) if cache is not None else None
        if stored:
            out.decisions[key] = stored["value"]
            out.evidence[key] = {**label, "decision": stored["value"], "source": stored.get("source", "you") + " (earlier run)",
                                 "confidence": stored.get("confidence", "-"), "why": stored.get("why", "your answer in an earlier run")}
            continue
        found = source_order(wf, nid, sql_dialect)
        if found:
            out.decisions[key], why = found
            out.evidence[key] = {**label, "decision": found[0], "source": "source query ORDER BY", "confidence": "high", "why": why}
            continue
        if wf.kind(nid) == "unique":
            for rule, source in ((unique_any_is_exact, "downstream uses only key fields"), (unique_keys_are_every_field, "the keys are every field")):
                why = rule(wf, nid) if rule is unique_any_is_exact else rule(wf, nid, sql_dialect)
                if why:
                    break
            if why:
                out.decisions[key] = {"mode": "any"}
                out.evidence[key] = {**label, "decision": {"mode": "any"}, "source": source, "confidence": "high", "why": why}
                continue
        if client is not None:
            proposal, reason = propose(client, analysis, handles, nid)
            if proposal:
                decision, confidence, sentence = proposal
                if confidence == "high":
                    out.decisions[key] = decision
                    out.evidence[key] = {**label, "decision": decision, "source": "model", "confidence": "high", "why": sentence or "the model's reading of the settings"}
                    continue
                if confidence == "medium":
                    out.suggestions[key] = decision
        out.unresolved.append(u)
    return out


def settle(analysis, handles, client=None, cache=None, decisions=None, sql_dialect: str = "", ask=None, rounds: int = 4):
    """Decide (and, with `ask`, ask) the order questions; an answer can unblock a tool after it that needs its own order, so this runs again for those.
    ask(items, auto) -> {str(tool id): decision} for the unresolved items (auto.suggestions are their defaults).
    Returns (new decisions, evidence {id: ...}, number decided automatically, number asked)."""
    decisions = dict(decisions or {})
    new, evidence, done_auto, done_asked, seen = {}, {}, 0, 0, set()
    current = (analysis, handles)
    for _ in range(rounds):
        a, h = current
        items = [u for u in a.unsupported if u.get("decision") == "order" and str(u["id"]) not in decisions and str(u["id"]) not in new and str(u["id"]) not in seen]
        seen |= {str(u["id"]) for u in items}                      # a question left unanswered is not asked again in the next round
        if not items:
            break
        auto = decide(a, h, client, cache, items, sql_dialect)
        new.update(auto.decisions)
        evidence.update(auto.evidence)
        done_auto += len(auto.decisions)
        given = ask(auto.unresolved, auto) if (ask and auto.unresolved) else {}
        for key, d in given.items():
            new[key] = d
            u = next(x for x in auto.unresolved if str(x["id"]) == key)
            evidence[key] = {"name": u.get("name", ""), "container": u.get("container", ""), "decision": d, "source": "you", "confidence": "-",
                             "why": "your answer" + (" (the model's proposal)" if auto.suggestions.get(key) == d else "")}
        done_asked += len(given)
        if cache is not None:                                      # only the user's answers: an automatic one rests on other tools too (the query, the
            for key, d in given.items():                           # tools after it), so it is worked out again on every run - which asks nothing either
                cache.put("order", int(key), d, source="you", confidence="-", why=evidence[key]["why"])
        if not auto.decisions and not given:
            break
        try:
            current = engine.reanalyze(Path(a.workdir), a.file_name, engine.decisions_from_choices({**decisions, **new}))
        except Exception:                                          # noqa: BLE001 - without a new plan the later rounds are simply not run
            break
    return new, evidence, done_auto, done_asked
