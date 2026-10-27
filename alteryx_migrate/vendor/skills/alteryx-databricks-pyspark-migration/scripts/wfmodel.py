"""Workflow model: load parsed-Alteryx JSON, build the graph, analyse it, and plan the Medallion layout.

Input format (derived from real files, see references/01-input-format.md):
    {"workflow": {...}, "tool_mapping": [...], "ir": {"nodes": [...], "edges": [...]}, "expressions": [...]}
The loader is tolerant: only ``ir.nodes`` / ``ir.edges`` are required; every other section enriches the model.
Nothing here is specific to one workflow.
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Optional

# ----------------------------------------------------------------------------- tool knowledge
# kind: logical operation.  order: how record order flows through the tool.
#   preserve = output keeps primary-input order, sets = tool defines order, breaks = order undefined afterwards
NODE_SPECS = {
    "ReadNode": dict(kind="read", order="breaks"),
    "LiteralDataNode": dict(kind="literal", order="sets"),
    "FilterNode": dict(kind="filter", order="preserve"),
    "FormulaNode": dict(kind="formula", order="preserve"),
    "MultiFieldFormulaNode": dict(kind="multifield", order="preserve"),
    "MultiRowFormulaNode": dict(kind="multirow", order="preserve", order_sensitive=True),
    "SelectNode": dict(kind="select", order="preserve"),
    "SortNode": dict(kind="sort", order="sets"),
    "UniqueNode": dict(kind="unique", order="preserve", order_sensitive=True),
    "JoinNode": dict(kind="join", order="breaks"),
    "JoinMultipleNode": dict(kind="joinmulti", order="breaks"),
    "UnionNode": dict(kind="union", order="breaks"),
    "AppendFieldsNode": dict(kind="append", order="preserve"),
    "SummarizeNode": dict(kind="summarize", order="breaks"),
    "CrossTabNode": dict(kind="crosstab", order="breaks"),
    "TransposeNode": dict(kind="transpose", order="breaks"),
    "RecordIDNode": dict(kind="recordid", order="preserve", order_sensitive=True),
    "RunningTotalNode": dict(kind="runningtotal", order="preserve", order_sensitive=True),
    "TextToColumnsNode": dict(kind="text2cols", order="preserve"),
    "DynamicRenameNode": dict(kind="dynrename", order="preserve"),
    "DateTimeNode": dict(kind="datetime", order="preserve"),
    "WriteNode": dict(kind="write", order="preserve"),
    "BrowseNode": dict(kind="browse", order="preserve"),
    "CommentNode": dict(kind="comment", order="preserve"),
    "GenerateRowsNode": dict(kind="generaterows", order="breaks"),
    "WorkflowControlNode": dict(kind="passthrough", order="preserve"),
    "CountRecordsNode": dict(kind="countrecords", order="breaks"),
    "AutoFieldNode": dict(kind="autofield", order="preserve"),
    "CreatePointsNode": dict(kind="spatial", order="preserve"),
    "DistanceNode": dict(kind="spatial", order="preserve"),
    "SpatialMatchNode": dict(kind="spatial", order="breaks"),
    "PredictiveModelNode": dict(kind="predictive", order="breaks"),
    "DataCleansingMacroNode": dict(kind="cleanse", order="preserve"),
    "ImputationMacroNode": dict(kind="impute", order="preserve"),
    "UnsupportedNode": dict(kind="unsupported", order="breaks"),
}
NO_DATA_KINDS = {"browse", "comment"}
GRAIN_CHANGING = {"summarize", "crosstab", "countrecords"}
# steps worth materialising when several tools read their result; cheap row-level steps (filter, formula, select, sort, ...)
# are recomputed where needed so the layers stay small
SUBSTANTIVE = {"join", "joinmulti", "union", "unique", "append", "summarize", "crosstab", "transpose", "multirow",
               "recordid", "runningtotal", "countrecords", "unsupported", "predictive", "spatial"}

# config keys that must be non-empty for a tool to be generated (empty = the converter lost the configuration)
REQUIRED_CONFIG = {
    "filter": ["expression"], "formula": ["formulas"], "multifield": ["expression", "fields"],
    "multirow": ["expression", "output_field"], "select": ["field_operations"], "sort": ["sort_fields"],
    "unique": ["key_fields"], "join": ["join_keys"], "joinmulti": ["join_inputs"], "summarize": ["aggregations"],
    "crosstab": ["group_fields", "header_field", "value_field"], "transpose": ["data_fields"],
    "runningtotal": ["running_fields"], "text2cols": ["field_name", "delimiter"], "dynrename": ["expression"],
    "datetime": ["input_field", "output_field", "format_string"], "recordid": ["output_field"],
    "generaterows": ["output_field", "expr_init", "expr_cond", "expr_loop"],
}
BAD_FIELD_NAMES = {"", "false", "true", "none", "null"}


# ----------------------------------------------------------------------------- model classes
@dataclass
class Edge:
    src: int
    dst: int
    src_anchor: str
    dst_anchor: str
    wireless: bool = False
    seq: int = 0   # position in the file: the only available evidence of input order for multi-input anchors


@dataclass
class Workflow:
    path: str
    meta: dict
    nodes: dict            # id -> raw IR node dict
    tools: dict            # id -> tool_mapping row
    edges: list
    expressions: dict      # node id -> [expression records]
    warnings: list = field(default_factory=list)
    unresolved: list = field(default_factory=list)   # structural problems (dangling edges, cycles, ...)
    containers: list = field(default_factory=list)   # tool ids that carry no data (visual containers)
    recovered: dict = field(default_factory=dict)    # node type -> number of tools whose settings were read from the XML
    recovered_edges: int = 0                         # connections the parser merged that were restored from the XML
    yxmd: Optional[str] = None
    inputs: dict = field(default_factory=dict)       # (node, dst_anchor) -> [Edge]
    outputs: dict = field(default_factory=dict)      # (node, src_anchor) -> [Edge]
    topo: list = field(default_factory=list)

    def kind(self, nid):
        return NODE_SPECS.get(self.nodes[nid]["node_type"], dict(kind="unknown"))["kind"]

    def label(self, nid):
        n = self.nodes[nid]
        return f"{n.get('original_tool_type', n['node_type'])}#{nid}"

    def producers(self, nid, anchor=None):
        """Upstream edges feeding a node (optionally one input anchor), in file order."""
        out = []
        for (n, a), es in self.inputs.items():
            if n == nid and (anchor is None or a == anchor):
                out += es
        return sorted(out, key=lambda e: e.seq)

    def consumers(self, nid, anchor=None):
        out = []
        for (n, a), es in self.outputs.items():
            if n == nid and (anchor is None or a == anchor):
                out += es
        return sorted(out, key=lambda e: e.seq)


# ----------------------------------------------------------------------------- loading
def load_workflow(path: str, overrides: Optional[dict] = None, yxmd: Optional[str] = None) -> Workflow:
    """Load the parsed workflow JSON. With ``yxmd`` (the original workflow XML) settings the converter lost are recovered
    from it; ``overrides`` (human decisions) are applied last and win."""
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    ir = doc.get("ir") if isinstance(doc, dict) else None
    if ir is None and isinstance(doc, dict) and "nodes" in doc and "edges" in doc:
        ir = {"nodes": doc["nodes"], "edges": doc["edges"]}
    if not ir or "nodes" not in ir or "edges" not in ir:
        keys = list(doc)[:10] if isinstance(doc, dict) else type(doc).__name__
        raise ValueError(
            f"Unrecognised workflow JSON (top-level: {keys}). Expected an 'ir' object with 'nodes' and 'edges'. "
            "See references/01-input-format.md for the supported shape and what to do with other shapes.")
    nodes = {int(n["node_id"]): dict(n) for n in ir["nodes"]}
    tools = {int(t["tool_id"]): t for t in doc.get("tool_mapping", [])}
    exprs = defaultdict(list)
    for e in doc.get("expressions", []):
        exprs[int(e["node_id"])].append(e)
    wf = Workflow(path=path, meta=doc.get("workflow", {}), nodes=nodes, tools=tools, edges=[], expressions=exprs)
    for i, e in enumerate(ir["edges"]):
        edge = Edge(int(e["source"]), int(e["target"]), e.get("origin_anchor") or "Output",
                    e.get("destination_anchor") or "Input", bool(e.get("is_wireless")), i)
        if edge.src not in nodes or edge.dst not in nodes:
            wf.unresolved.append(f"Edge {edge.src}->{edge.dst} references a tool that is not in the IR")
            continue
        wf.edges.append(edge)
        wf.outputs.setdefault((edge.src, edge.src_anchor), []).append(edge)
        wf.inputs.setdefault((edge.dst, edge.dst_anchor), []).append(edge)
    for tid, t in tools.items():
        if tid not in nodes:
            if t.get("status") == "skipped":
                wf.containers.append(tid)
            else:
                wf.unresolved.append(f"Tool {tid} ({t.get('tool_type')}) has status {t.get('status')!r} but no IR node")
    if yxmd:
        import xml_recover
        wf.recovered = xml_recover.apply(wf, yxmd)
        wf.yxmd = yxmd
        wf.recovered_edges = _recover_edges(wf, yxmd)
    for nid, ov in (overrides or {}).items():
        if not str(nid).isdigit():       # "_comment" and other annotations
            continue
        if int(nid) in nodes:
            nodes[int(nid)].update({k: v for k, v in ov.items()})
            nodes[int(nid)]["_override_keys"] = sorted(ov)
    _promote_external_sources(wf)
    _promote_macros(wf)
    wf.topo = _topo(wf)
    return wf


def _recover_edges(wf, yxmd) -> int:
    """The parser keeps one connection per pair of tools. When one tool feeds another through several outputs (a Join's Join, Left and Right
    all into one Union) the workflow XML has them all: add the missing ones and put every input in file order."""
    import xml.etree.ElementTree as ET
    root = ET.parse(yxmd).getroot()
    xml = []
    for i, c in enumerate(root.iter("Connection")):
        o, d = c.find("Origin"), c.find("Destination")
        if o is not None and d is not None:
            xml.append((int(o.get("ToolID")), o.get("Connection"), int(d.get("ToolID")), d.get("Connection"), i))
    ir_count, xml_count = {}, {}
    for e in wf.edges:
        ir_count[(e.src, e.dst)] = ir_count.get((e.src, e.dst), 0) + 1
    for s_, _, t, _, _ in xml:
        if s_ in wf.nodes and t in wf.nodes:
            xml_count[(s_, t)] = xml_count.get((s_, t), 0) + 1
    have = {(e.src, e.src_anchor, e.dst, e.dst_anchor) for e in wf.edges}
    added = 0
    for s_, sa, t, da, i in xml:
        pair = (s_, t)
        if pair in ir_count and xml_count.get(pair, 0) > ir_count[pair] and (s_, sa, t, da) not in have:
            edge = Edge(s_, t, sa or "Output", da or "Input", False, i)
            wf.edges.append(edge)
            wf.outputs.setdefault((s_, edge.src_anchor), []).append(edge)
            wf.inputs.setdefault((t, edge.dst_anchor), []).append(edge)
            have.add((s_, sa, t, da))
            ir_count[pair] += 1
            added += 1
    if added:
        position = {(s_, sa, t, da): i for s_, sa, t, da, i in xml}
        for e in wf.edges:
            e.seq = position.get((e.src, e.src_anchor, e.dst, e.dst_anchor), e.seq)
        for lst in list(wf.inputs.values()) + list(wf.outputs.values()):
            lst.sort(key=lambda e: e.seq)
    return added


_QUERY_KEYS = ("inputCustomFilter", "query", "sql", "customQuery", "Query", "SQL")


def _promote_external_sources(wf):
    """An unsupported tool that has no inputs, feeds other tools and is an input/reader by name (BigQuery Input, ...) is
    treated as a source: its result is expected in an existing Bronze table. The original tool definition is kept in
    ``unsupported_tools.xml`` and the tool stays 'requires review'."""
    for nid, n in wf.nodes.items():
        if n["node_type"] != "UnsupportedNode" or wf.producers(nid) or not wf.consumers(nid):
            continue
        label = f"{n.get('original_tool_type', '')} {n.get('original_plugin_name', '')}".lower()
        if "macro:" in label or not re.search(r"input|reader|read_|source", label):
            continue
        cfg = n.get("original_configuration") or {}
        query = next((str(cfg[k]) for k in _QUERY_KEYS if isinstance(cfg.get(k), str) and cfg[k].strip()), "")
        flat = {v.get("@name"): v.get("#text") for v in (cfg.get("Value") or []) if isinstance(v, dict)} \
            if isinstance(cfg.get("Value"), list) else {}
        table = ""
        if flat.get("workbookName"):             # Google Sheets: <workbook>[_<sheet>] names the Bronze table
            sheet = str(flat.get("worksheetName") or "")
            table = str(flat["workbookName"]) + ("" if sheet.lower() in ("", "sheet1") else f" {sheet}")
        n.update(node_type="ReadNode", source_type="external", query=query, table_name=table, file_path="", file_format="",
                 connection_string=str(n.get("original_plugin_name") or ""), converted_from="UnsupportedNode")


# The Data Cleansing tool is Alteryx's own macro Cleanse.yxmc (bin/RuntimeData/Macros). Its settings are interface questions with fixed names; the
# labels below are the <Description> of each question in that macro, so a check box is never interpreted by position or guesswork.
CLEANSE_QUESTIONS = {
    "Check Box (135)": "remove_null_rows",            # Remove null rows
    "Check Box (136)": "remove_null_columns",         # Remove null columns
    "List Box (11)": "fields",                        # Select Fields to Cleanse
    "Check Box (84)": "blanks_for_null_strings",      # Replace with Blanks (String Fields)
    "Check Box (117)": "zero_for_null_numbers",       # Replace with 0 (Numeric Fields)
    "Check Box (15)": "trim",                         # Leading and Trailing Whitespace
    "Check Box (109)": "collapse_whitespace",         # Tabs, Line Breaks, and Duplicate Whitespace
    "Check Box (122)": "remove_all_whitespace",       # All Whitespace
    "Check Box (53)": "remove_letters",               # Letters
    "Check Box (58)": "remove_numbers",               # Numbers
    "Check Box (70)": "remove_punctuation",           # Punctuation
    "Check Box (77)": "modify_case",                  # Modify Case
    "Drop Down (81)": "case",                         # upper | lower | title
}


def cleanse_options(cfg: dict) -> Optional[dict]:
    """The Data Cleansing options from the macro call's <Configuration>, or None when a setting is not one the macro defines."""
    values = cfg.get("Value") if isinstance(cfg, dict) else None
    values = [values] if isinstance(values, dict) else values
    if not isinstance(values, list):
        return None
    raw = {v.get("@name"): (v.get("#text") or "") for v in values if isinstance(v, dict)}
    if not raw or any(k not in CLEANSE_QUESTIONS for k in raw):
        return None
    opts = {key: str(raw.get(name, "")).strip().lower() == "true" for name, key in CLEANSE_QUESTIONS.items() if name.startswith("Check Box")}
    opts["fields"] = re.findall(r'"((?:[^"]|"")*)"', raw.get("List Box (11)", ""))
    opts["fields"] = [f.replace('""', '"') for f in opts["fields"]]
    opts["case"] = str(raw.get("Drop Down (81)") or "").strip().lower()
    if opts["modify_case"] and opts["case"] not in ("upper", "lower", "title"):
        return None
    return opts


# The Imputation tool is Alteryx's macro Imputation_v3.yxmc: radio buttons / check boxes / a field list, named as below in the macro (<Name> of each question).
IMPUTATION_QUESTIONS = {
    "listbox Select Incoming Fields": "fields", "radio Null Value": "from_null", "radio User Specified Replace From Value": "from_user",
    "updown User Replace Value": "from_value", "radio Mean": "to_mean", "radio Median": "to_median", "radio Mode": "to_mode",
    "radio User Specified Replace With Value": "to_user", "updown User Replace With Value": "to_value",
    "checkbox Impute Indicator": "indicator", "checkbox Imputed Values Separate Field": "separate",
}


def imputation_options(cfg: dict) -> Optional[dict]:
    """The Imputation options from the macro call's <Configuration>, or None when a setting is not one the macro defines or the choice is not exactly one."""
    values = cfg.get("Value") if isinstance(cfg, dict) else None
    values = [values] if isinstance(values, dict) else values
    if not isinstance(values, list):
        return None
    raw = {v.get("@name"): (v.get("#text") or "") for v in values if isinstance(v, dict)}
    if not raw or any(k not in IMPUTATION_QUESTIONS for k in raw):
        return None
    flag = lambda name: str(raw.get(name, "")).strip().lower() == "true"          # noqa: E731
    opts = {key: flag(name) for name, key in IMPUTATION_QUESTIONS.items() if name.startswith(("radio", "checkbox"))}
    opts["fields"] = [f.replace('""', '"') for f in re.findall(r'"((?:[^"]|"")*)"', raw.get("listbox Select Incoming Fields", ""))]
    try:
        opts["from_value"], opts["to_value"] = float(raw.get("updown User Replace Value") or 0), float(raw.get("updown User Replace With Value") or 0)
    except ValueError:
        return None
    if opts["from_null"] == opts["from_user"] or sum(opts[k] for k in ("to_mean", "to_median", "to_mode", "to_user")) != 1:
        return None
    return opts


def _promote_macros(wf):
    """Calls of Alteryx's own macros become nodes with named options: Data Cleansing (Cleanse.yxmc) and Imputation (Imputation_v3.yxmc)."""
    _promote_data_cleansing(wf)
    for nid, n in wf.nodes.items():
        if n["node_type"] != "UnsupportedNode":
            continue
        macro = str(n.get("original_plugin_name") or "").lower().replace("\\", "/").rsplit("/", 1)[-1]
        if macro in ("macro:imputation_v3.yxmc", "imputation_v3.yxmc"):
            opts = imputation_options(n.get("original_configuration") or {})
            if opts is not None:
                n.update(node_type="ImputationMacroNode", original_tool_type="Imputation", impute=opts, converted_from="UnsupportedNode")


def _promote_data_cleansing(wf):
    """A call of the Data Cleansing macro (Cleanse.yxmc) becomes a DataCleansingMacroNode with named options (see CLEANSE_QUESTIONS)."""
    for nid, n in wf.nodes.items():
        if n["node_type"] != "UnsupportedNode":
            continue
        macro = str(n.get("original_plugin_name") or "").lower().replace("\\", "/").rsplit("/", 1)[-1]
        if macro not in ("macro:cleanse.yxmc", "cleanse.yxmc"):
            continue
        opts = cleanse_options(n.get("original_configuration") or {})
        if opts is not None:
            n.update(node_type="DataCleansingMacroNode", original_tool_type="Data Cleansing", cleanse=opts, converted_from="UnsupportedNode")


def _topo(wf: Workflow):
    indeg = {n: 0 for n in wf.nodes}
    succ = defaultdict(set)
    for e in wf.edges:
        if e.dst not in succ[e.src]:
            succ[e.src].add(e.dst)
            indeg[e.dst] += 1
    ready = sorted(n for n, d in indeg.items() if d == 0)
    out = []
    import heapq
    heapq.heapify(ready)
    while ready:
        n = heapq.heappop(ready)
        out.append(n)
        for m in succ[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                heapq.heappush(ready, m)
    if len(out) != len(wf.nodes):
        cyc = sorted(set(wf.nodes) - set(out))
        wf.unresolved.append(f"Cycle detected among {len(cyc)} tools (e.g. {cyc[:5]}); iterative macros/loops are unsupported")
        out += cyc
    return out


# ----------------------------------------------------------------------------- analysis
def ancestors(wf: Workflow, targets) -> set:
    seen, stack = set(), list(targets)
    pred = defaultdict(set)
    for e in wf.edges:
        pred[e.dst].add(e.src)
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        stack.extend(pred[n])
    return seen


def config_issues(wf: Workflow, nid: int) -> list:
    """Generic detector for configuration that the upstream converter lost or corrupted."""
    n = wf.nodes[nid]
    kind = wf.kind(nid)
    issues = []
    for key in REQUIRED_CONFIG.get(kind, []):
        v = n.get(key)
        if kind == "formula" and key == "formulas" and n.get("passthrough"):
            continue                                   # the workflow XML confirms this Formula tool has no formulas
        if kind == "join" and key == "join_keys" and n.get("join_by_position"):
            continue
        if kind == "join" and key == "join_keys" and v in (None, "", [], {}):
            issues.append("join keys are empty" + ("" if wf.yxmd else " - pass the workflow .yxmd so they can be read from the XML"))
            continue
        if kind == "transpose" and key == "data_fields" and n.get("data_unknown"):
            continue                                   # remaining fields are data fields (*Unknown)
        if v in (None, "", [], {}):
            issues.append(f"config '{key}' is empty" + ("" if wf.yxmd else " - pass the workflow .yxmd so it can be read from the XML"))
    if kind == "multirow" and str(n.get("output_field", "")).strip().lower() in BAD_FIELD_NAMES - {""}:
        issues.append(f"output_field={n.get('output_field')!r} is not a field name (converter defect)")
    if kind == "transpose" and not n.get("_recovered"):
        ov = {x.lower() for x in n.get("key_fields", [])} & {x.lower() for x in n.get("data_fields", [])}
        if ov:
            issues.append(f"key_fields and data_fields overlap on {len(ov)} field(s) (converter defect)")
    return issues


def analyse(wf: Workflow) -> dict:
    """Structural analysis -> plain dict (also printed by analyze_workflow.py)."""
    kinds = Counter(wf.nodes[n]["node_type"] for n in wf.nodes)
    live_sinks = [n for n in wf.nodes if wf.kind(n) == "write"]
    sinks_source = "Write/Output tools"
    if not live_sinks:   # no explicit outputs: fall back to dangling data tools
        live_sinks = [n for n in wf.nodes if wf.kind(n) not in NO_DATA_KINDS and not wf.consumers(n)
                      and wf.kind(n) != "read"]
        sinks_source = "dangling tools (no Write tool present - assumption, confirm outputs)"
    live = ancestors(wf, live_sinks)
    sources = [n for n in wf.nodes if wf.kind(n) in ("read", "literal")]
    dead = [n for n in wf.nodes if n not in live and wf.kind(n) not in NO_DATA_KINDS]
    issues = {n: config_issues(wf, n) for n in wf.nodes}
    issues = {n: v for n, v in issues.items() if v}
    missing_inputs = []
    for n in wf.nodes:
        k = wf.kind(n)
        need = {"join": ["Left", "Right"], "append": ["Targets", "Source"], "filter": ["Input"],
                "formula": ["Input"], "select": ["Input"], "summarize": ["Input"], "sort": ["Input"],
                "unique": ["Input"], "union": ["Input"], "write": ["Input"]}.get(k, [])
        for a in need:
            if (n, a) not in wf.inputs:
                missing_inputs.append((n, a))
    return {
        "kinds": dict(kinds), "tools": len(wf.tools) or len(wf.nodes), "ir_nodes": len(wf.nodes), "edges": len(wf.edges),
        "containers": len(wf.containers), "sources": sources, "sinks": live_sinks, "sinks_source": sinks_source,
        "live": live, "dead_end_tools": dead, "config_issues": issues, "missing_inputs": missing_inputs,
        "unresolved": list(wf.unresolved), "order_sensitive": [n for n in wf.nodes if NODE_SPECS.get(
            wf.nodes[n]["node_type"], {}).get("order_sensitive")],
        "converter_confidence_lt_1": [n for n in wf.nodes if (wf.nodes[n].get("conversion_confidence") or 1) < 1],
    }


# ----------------------------------------------------------------------------- naming
_ASCII = str.maketrans({"å": "a", "Å": "A", "ø": "o", "Ø": "O", "æ": "ae", "Æ": "AE", "ß": "ss", "ð": "d", "þ": "th"})


def snake(text: str, maxlen: int = 60) -> str:
    t = unicodedata.normalize("NFKD", str(text).translate(_ASCII)).encode("ascii", "ignore").decode()
    t = re.sub(r"[^A-Za-z0-9]+", "_", t).strip("_").lower()
    return (t[:maxlen].rstrip("_")) or "unnamed"


def source_tables(query: str) -> list:
    return [t.strip("`") for t in re.findall(r"(?is)\b(?:from|join)\s+(`?[\w\-\.\[\]#]+`?)", query or "")]


def norm_query(q: str) -> str:
    return re.sub(r"\s+", " ", (q or "").strip().lower())


# ----------------------------------------------------------------------------- readable names
def fit(name: str, limit: int = 44) -> str:
    """Shorten a snake_case name at word boundaries (drops words next to the final step word first)."""
    parts = name.split("_")
    while len("_".join(parts)) > limit and len(parts) > 2:
        del parts[-2]
    return "_".join(parts)[:limit].rstrip("_")


def source_base(wf: Workflow, bronze_of: dict, nid: int) -> str:
    """Short name of the source on a node's main data path (Left of a join, Targets of an append, first input otherwise)."""
    seen, stack = {nid}, [nid]
    while stack:
        x = stack.pop()
        node = wf.nodes[x]
        if wf.kind(x) == "read" and x in bronze_of:
            return re.sub(r"_q\d+$", "", bronze_of[x].removeprefix("bronze_"))
        if wf.kind(x) == "literal":
            return snake(" ".join((node.get("field_names") or [f"table{x}"])[:2]), 20) + "_lookup"
        prods = sorted(wf.producers(x), key=lambda e: (e.dst_anchor not in ("Left", "Targets", "Input"), e.seq))
        for e in reversed(prods):                       # depth-first, preferred input popped first
            if e.src not in seen:
                seen.add(e.src)
                stack.append(e.src)
    return "data"


def step_detail(wf: Workflow, bronze_of: dict, nid: int, anchor: str = "Output") -> str:
    """What the tool does to its data, in a word or two, taken from its own configuration."""
    k, n = wf.kind(nid), wf.nodes[nid]
    if k == "filter":
        if anchor == "False":
            return "rejected"
        import alteryx_expr as X
        refs = X.referenced_fields(n.get("expression") or "")
        return "filtered_by_" + snake(refs[0], 20) if refs else "filtered"
    if k == "formula":
        names = [f.get("output_field") for f in n.get("formulas") or [] if f.get("output_field")]
        return "with_" + snake(names[0], 22) if names else "enriched"
    if k == "multirow":
        of = str(n.get("output_field") or "")
        return "with_" + snake(of, 22) if of.strip().lower() not in BAD_FIELD_NAMES else "windowed"
    if k == "join":
        base = {"Join": "joined", "Left": "unmatched_left", "Right": "unmatched_right"}.get(anchor, "joined")
        right = [e.src for e in wf.producers(nid, "Right")]
        if base == "joined" and right:
            rb, lb = source_base(wf, bronze_of, right[0]), source_base(wf, bronze_of, nid)
            return f"joined_{rb}" if rb != lb else "joined"
        return base
    if k == "unique":
        return "unique" if anchor == "Unique" else "duplicates"
    if k == "summarize":
        groups = [a["field_name"] for a in n.get("aggregations", []) if a["action"].lower() == "groupby"]
        return "by_" + snake("_".join(groups[:2]), 28) if groups else "totals"
    if k == "unsupported":
        label = str(n.get("original_plugin_name") or n.get("original_tool_type") or "tool")
        return snake(re.sub(r"(?i)^macro:|\.yxmc$|_v\d+(\.\d+)*$", "", label), 20)
    return {"multifield": "adjusted", "select": "selected", "sort": "sorted", "union": "combined", "append": "appended",
            "crosstab": "pivot", "transpose": "unpivoted", "recordid": "indexed", "runningtotal": "running_total",
            "text2cols": "split", "dynrename": "renamed", "cleanse": "cleansed", "impute": "imputed", "datetime": "dated", "countrecords": "record_count", "autofield": "fitted", "spatial": "geo", "predictive": "forecast",
            "literal": ""}.get(k, k)


def step_name(wf: Workflow, bronze_of: dict, nid: int, anchor: str = "Output") -> str:
    """<source>_<what happened>, e.g. orders_filtered_by_status, orders_joined_customers, sales_by_plant_month."""
    base = source_base(wf, bronze_of, nid)
    detail = step_detail(wf, bronze_of, nid, anchor)
    if wf.kind(nid) == "summarize" and detail.startswith("by_"):
        return fit(snake(f"{base}_{detail}"))
    return fit(snake(f"{base}_{detail}" if detail else base))


# ----------------------------------------------------------------------------- Medallion plan
@dataclass
class Unit:
    key: str                 # stable id, e.g. "silver_n88"
    layer: str               # silver | gold
    node: int                # cut node whose output(s) the unit persists
    streams: list            # [(node, anchor)] persisted by this unit
    nodes: list = field(default_factory=list)       # member node ids in topological order
    inputs: list = field(default_factory=list)      # boundary streams read from other layers/units
    table: dict = field(default_factory=dict)       # stream -> table key
    reason: str = ""


@dataclass
class Plan:
    zone: dict               # node -> bronze|silver|gold
    bronze: dict             # table key -> dict(nodes, query, connection, source_tables)
    bronze_of: dict          # read node id -> bronze key
    units: list
    cut_streams: dict        # (node, anchor) -> table key
    inspection_only: list    # live-set complement: not needed for any output
    notes: list = field(default_factory=list)
    opts: dict = field(default_factory=dict)


def primary_stream(wf: Workflow, nid: int):
    """The (node, anchor) a single-output tool exposes; for multi-output tools the first consumed anchor."""
    anchors = sorted({a for (n, a) in wf.outputs if n == nid})
    if not anchors:
        return (nid, main_anchor(wf, nid))
    return (nid, "Output" if "Output" in anchors else anchors[0])


MAIN_ANCHOR = {"filter": "True", "join": "Join", "unique": "Unique"}


def main_anchor(wf: Workflow, nid: int) -> str:
    """The output a tool exposes when nothing is connected to it (Filter: True, Join: Join, Unique: Unique)."""
    return MAIN_ANCHOR.get(wf.kind(nid), "Output")


def plan_medallion(wf: Workflow, silver_handoff: str = "auto", max_unit_nodes: int = 0,
                   naming: Optional[dict] = None, extra_cuts: Optional[dict] = None) -> Plan:
    """extra_cuts: {(node, anchor): reason} streams that must be materialised (used to separate convertible tools from
    tools that are still waiting for input)."""
    naming = {int(k): v for k, v in (naming or {}).items() if str(k).isdigit()}
    info = analyse(wf)
    live = info["live"]
    kind = wf.kind
    zone = {}
    for n in wf.topo:
        k = kind(n)
        if k == "read":
            zone[n] = "bronze"
        elif k in GRAIN_CHANGING:
            zone[n] = "gold"
        else:
            ups = [e.src for e in wf.producers(n)]
            zone[n] = "gold" if any(zone.get(u) == "gold" for u in ups) else "silver"
    # --- Bronze: one table per distinct (connection, query) / file source
    bronze, bronze_of, used = {}, {}, Counter()
    for n in sorted(x for x in wf.nodes if kind(x) == "read" and x in live):
        nd = wf.nodes[n]
        sig = (nd.get("connection_string") or nd.get("file_path") or "", norm_query(nd.get("query")),
               nd.get("table_name") or "")
        existing = next((k for k, v in bronze.items() if v["sig"] == sig), None)
        if existing:
            bronze[existing]["nodes"].append(n)
            bronze_of[n] = existing
            continue
        tabs = source_tables(nd.get("query")) or [nd.get("table_name") or (nd.get("file_path") or "").replace("\\", "/").split("/")[-1] or f"src{n}"]
        base = snake(tabs[0].split(".")[-1]) if tabs else f"src{n}"
        used[base] += 1
        key = naming.get(n) or f"bronze_{base}" + (f"_q{used[base]}" if used[base] > 1 else "")
        bronze[key] = dict(sig=sig, nodes=[n], query=nd.get("query") or "", connection=nd.get("connection_string") or "",
                           source_tables=tabs, source_type=nd.get("source_type"), file_path=nd.get("file_path"),
                           file_format=nd.get("file_format"), delimiter=nd.get("delimiter"),
                           has_header=nd.get("has_header"), encoding=nd.get("encoding"))
        bronze_of[n] = key
    # --- cut streams
    cut = {}   # (node, anchor) -> (layer, reason)

    def add_cut(stream, layer, reason):
        cut.setdefault(stream, (layer, reason))

    def pred_streams(n):
        return [(e.src, e.src_anchor) for e in wf.producers(n)]

    def live_consumers(x):
        return [e for e in wf.consumers(x) if e.dst in live and kind(e.dst) not in NO_DATA_KINDS]

    def lineage(t, cut):
        """Tools needed to compute t inside its unit (stops at cut streams and sources)."""
        seen, stack = set(), [t]
        while stack:
            x = stack.pop()
            if x in seen:
                continue
            seen.add(x)
            for st in pred_streams(x):
                if st not in cut and kind(st[0]) != "read":
                    stack.append(st[0])
        return seen

    for n in wf.topo:                  # topological, so lineage() sees the cuts already placed upstream
        if n not in live:
            continue
        k = kind(n)
        is_output = n in info["sinks"]
        if k == "write" or (is_output and k in ("unsupported", "predictive") and wf.producers(n)):
            # a writer (Write, or an unsupported output tool such as a BigQuery writer): its inputs are the outputs
            for e in wf.producers(n):
                add_cut((e.src, e.src_anchor), "gold", f"feeds output tool #{n}")
            continue
        if is_output and k not in ("read", "write") and k not in NO_DATA_KINDS:
            add_cut(primary_stream(wf, n), "gold", f"final output of tool #{n}")     # no writer in the workflow
            continue
        if k == "read" or k in NO_DATA_KINDS:
            continue
        for anchor in sorted({a for (m, a) in wf.outputs if m == n}):
            cons = {e.dst for e in wf.outputs[(n, anchor)] if e.dst in live and kind(e.dst) not in NO_DATA_KINDS}
            if not cons:
                continue
            if zone[n] == "silver":
                gold_cons = [c for c in cons if zone[c] == "gold"]
                if len(cons) >= 2:
                    if any(kind(x) in SUBSTANTIVE for x in lineage(n, cut)):
                        add_cut((n, anchor), "silver", f"shared by {len(cons)} consumers")
                elif gold_cons and silver_handoff in ("persist", "auto"):
                    add_cut((n, anchor), "silver", "Silver->Gold hand-off (record-level grain)")
    for stream, reason in (extra_cuts or {}).items():
        add_cut(stream, zone[stream[0]] if zone[stream[0]] != "bronze" else "silver", reason)
    # --- units, with promotion of nodes that would be computed twice
    def build_units(cut):
        units = {}
        for stream in cut:
            units.setdefault(stream[0], []).append(stream)
        membership = defaultdict(set)
        members = {}
        for cnode, streams in units.items():
            seen, stack, boundary = set(), [cnode], []
            while stack:
                x = stack.pop()
                if x in seen:
                    continue
                seen.add(x)
                for s in pred_streams(x):
                    if s in cut and s[0] != cnode:
                        boundary.append(s)
                    elif kind(s[0]) == "read":
                        boundary.append(s)
                    else:
                        stack.append(s[0])
            members[cnode] = (seen, boundary)
            for x in seen:
                membership[x].add(cnode)
        return units, members, membership

    if silver_handoff == "auto":
        # keep a hand-off table only if the Silver chain behind it does real cleansing/integration work
        for stream, (lay, reason) in list(cut.items()):
            if reason.startswith("Silver->Gold hand-off"):
                chain = lineage(stream[0], cut)
                if not any(kind(x) in SUBSTANTIVE | {"filter"} for x in chain):
                    del cut[stream]

    for _ in range(10_000):
        units, members, membership = build_units(cut)
        shared = {x for x, owners in membership.items() if len(owners) > 1}
        changed = False
        for x in sorted(shared):     # promote only the frontier of the shared region
            if not any(kind(y) in SUBSTANTIVE for y in lineage(x, cut)):
                continue                 # cheap row-level steps are recomputed in each unit instead of getting a table
            for e in live_consumers(x):
                if e.dst not in shared and (x, e.src_anchor) not in cut:
                    add_cut((x, e.src_anchor), zone[x], "shared across outputs: computed once")
                    changed = True
        if changed:
            continue
        if max_unit_nodes:        # split oversized units only at clean (closed) cut points
            for cnode, (seen, _) in sorted(members.items()):
                if len(seen) <= max_unit_nodes:
                    continue
                best = None
                for t in [n for n in wf.topo if n in seen and n != cnode and kind(n) != "read"]:
                    anc = lineage(t, cut)
                    if len(anc) > max_unit_nodes or (best and len(anc) <= best[0]):
                        continue
                    if all(e.dst in anc for x in anc if x != t for e in live_consumers(x)):
                        best = (len(anc), t)
                if best and best[0] >= max(10, max_unit_nodes // 5):
                    t = best[1]
                    for e in live_consumers(t):
                        add_cut((t, e.src_anchor), zone[t], f"plan-size split (unit > {max_unit_nodes} tools)")
                    changed = True
                    break
        if not changed:
            break
    oversized = [(cnode, len(seen)) for cnode, (seen, _) in members.items() if max_unit_nodes and len(seen) > max_unit_nodes]
    # --- name tables / assemble units in execution order
    cut_streams, out_units = {}, []
    gold_names, taken = Counter(), set(bronze)

    def friendly_name(lay, st):
        return f"{lay}_{step_name(wf, bronze_of, st[0], st[1])}"

    def unique(name, taken_names):
        cand, i = name, 2
        while cand in taken_names:
            cand = f"{name}_{i}"
            i += 1
        taken_names.add(cand)
        return cand

    for cnode in [n for n in wf.topo if n in units]:
        streams = sorted(units[cnode])
        tables = {}
        for st in streams:
            lay, reason = cut[st]
            consumers_write = [e.dst for e in wf.outputs.get(st, []) if kind(e.dst) == "write"]
            if consumers_write:
                w = wf.nodes[consumers_write[0]]
                base = snake(re.split(r"[|]{3}", (w.get("file_path") or w.get("table_name") or f"out{consumers_write[0]}"))[0]
                             .replace("\\", "/").split("/")[-1].rsplit(".", 1)[0])
                gold_names[base] += 1
                tkey = naming.get(st[0]) or unique("gold_" + base, taken)
                lay = "gold"
            else:
                tkey = naming.get(st[0]) or unique(friendly_name(lay, st), taken)
            tables[st] = tkey
            cut_streams[st] = tkey
        seen, boundary = members[cnode]
        layer = "silver" if any(t.startswith("silver_") for t in tables.values()) else "gold"
        out_units.append(Unit(key=tables[streams[0]], layer=layer, node=cnode, streams=streams,
                              nodes=[n for n in wf.topo if n in seen], inputs=sorted(set(boundary)), table=tables,
                              reason="; ".join(sorted({cut[s][1] for s in streams}))))
    plan = Plan(zone=zone, bronze=bronze, bronze_of=bronze_of, units=out_units, cut_streams=cut_streams,
                inspection_only=info["dead_end_tools"])
    plan.opts = dict(silver_handoff=silver_handoff, max_unit_nodes=max_unit_nodes, naming=naming)
    if oversized:
        plan.notes.append(f"{len(oversized)} unit(s) still exceed --max-unit-nodes: no clean (closed) split point exists; "
                          "refactor manually or accept the larger plan.")
    elif not max_unit_nodes and any(len(u.nodes) > 200 for u in plan.units):
        plan.notes.append("Some units exceed 200 tools: Spark plan analysis can become slow. "
                          "Re-run with --max-unit-nodes (e.g. 150) to insert checkpoint tables at clean split points.")
    return plan
