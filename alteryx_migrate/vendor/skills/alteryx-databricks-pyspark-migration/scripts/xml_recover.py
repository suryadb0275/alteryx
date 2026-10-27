"""Recover tool settings from the original .yxmd XML.

The parsed JSON (converter output) loses or corrupts several settings - join keys, Cross Tab fields, running-total fields,
transpose key/data fields, Multi-Row output names, Dynamic Rename formulas, DateTime settings, Generate Rows expressions,
input schemas.  When the workflow XML is available these are read from it (the source of truth) instead of being asked of
the user.  Values already present in the JSON are kept unless they are known to be corrupt.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET

BAD = {"", "false", "true", "none", "null"}


def _fields(el):
    return [f.get("field") for f in el.findall("Field")] if el is not None else []


def _text(cfg, tag):
    el = cfg.find(tag)
    return (el.text or "").strip() if el is not None and el.text else ""


def _value(cfg, tag):
    el = cfg.find(tag)
    return el.get("value") if el is not None else None


class XmlTool:
    def __init__(self, tool_id, plugin, cfg, macro, schema):
        self.id, self.plugin, self.cfg, self.macro, self.schema = tool_id, plugin, cfg, macro, schema


def load(path):
    """ToolID -> XmlTool for every tool in the workflow (containers' children included)."""
    root = ET.parse(path).getroot()
    tools = {}
    for nd in root.iter("Node"):
        tid = nd.get("ToolID")
        if tid is None:
            continue
        gs = nd.find("GuiSettings")
        es = nd.find("EngineSettings")
        schema = [{"name": f.get("name"), "type": f.get("type"), "size": f.get("size")}
                  for mi in nd.findall("Properties/MetaInfo") if mi.get("connection", "Output") == "Output"
                  for f in mi.iter("Field")]
        tools[int(tid)] = XmlTool(int(tid), (gs.get("Plugin") if gs is not None else "") or "",
                                  nd.find("Properties/Configuration"), es.get("Macro") if es is not None else None, schema)
    return tools


# ----------------------------------------------------------------------------- per-kind extractors -> IR fields
def _join(cfg):
    infos = {ji.get("connection"): _fields(ji) for ji in cfg.findall("JoinInfo")}
    left, right = infos.get("Left", []), infos.get("Right", [])
    return {"join_keys": [{"left_field": a, "right_field": b} for a, b in zip(left, right)],
            "join_by_position": cfg.get("joinByRecordPos") == "True"}


def _joinmulti(cfg):
    jf = cfg.find("JoinFields")
    return {"join_inputs": {ji.get("connection"): _fields(ji) for ji in jf.findall("JoinInfo")} if jf is not None else {},
            "join_by_position": _value(cfg, "JoinByRecPos") == "True"}


def _crosstab(cfg):
    h, d = cfg.find("HeaderField"), cfg.find("DataField")
    methods = [m.get("method") for m in cfg.findall("Methods/Method")]
    return {"group_fields": _fields(cfg.find("GroupFields")), "header_field": h.get("field") if h is not None else "",
            "value_field": d.get("field") if d is not None else "", "methods": methods,
            "aggregation": methods[0] if methods else ""}


def _runningtotal(cfg):
    return {"group_fields": _fields(cfg.find("GroupByFields")), "running_fields": _fields(cfg.find("RunningTotalFields"))}


def _transpose(cfg):
    data = cfg.findall("DataFields/Field")
    unknown = next((f for f in data if f.get("field") == "*Unknown"), None)
    return {"key_fields": _fields(cfg.find("KeyFields")),
            "data_fields": [f.get("field") for f in data if f.get("field") != "*Unknown" and f.get("selected") != "False"],
            "data_skip": [f.get("field") for f in data if f.get("field") != "*Unknown" and f.get("selected") == "False"],
            "data_unknown": unknown is not None and unknown.get("selected") != "False"}


def _multirow(cfg):
    update = _value(cfg, "UpdateField") == "True"
    return {"update_field": update, "output_field": _text(cfg, "UpdateField_Name" if update else "CreateField_Name"),
            "output_type": _text(cfg, "CreateField_Type") if not update else "", "output_size": _text(cfg, "CreateField_Size") or None,
            "group_fields": _fields(cfg.find("GroupByFields")), "other_rows": _text(cfg, "OtherRows") or "Empty",
            "expression": _text(cfg, "Expression")}


def _dynrename(cfg):
    fl = cfg.findall("Fields/Field")
    unknown = next((f for f in fl if f.get("name") == "*Unknown"), None)
    return {"rename_mode": _text(cfg, "RenameMode"), "expression": _text(cfg, "Expression"),
            "fields_to_rename": [f.get("name") for f in fl if f.get("name") != "*Unknown" and f.get("selected") != "False"],
            "fields_kept": [f.get("name") for f in fl if f.get("name") != "*Unknown" and f.get("selected") == "False"],
            "unknown_renamed": unknown is not None and unknown.get("selected") != "False"}


def _datetime(cfg):
    return {"is_from": _value(cfg, "IsFrom") == "True", "input_field": _text(cfg, "InputFieldName"),
            "output_field": _text(cfg, "OutputFieldName") or "DateTime_Out", "format_string": _text(cfg, "Format"), "language": _text(cfg, "Language") or "English"}


def _generaterows(cfg):
    return {"update_field": _value(cfg, "UpdateField") == "True",
            "output_field": _text(cfg, "UpdateField_Name") if _value(cfg, "UpdateField") == "True" else _text(cfg, "CreateField_Name"),
            "output_type": _text(cfg, "CreateField_Type"), "expr_init": _text(cfg, "Expression_Init"),
            "expr_cond": _text(cfg, "Expression_Cond"), "expr_loop": _text(cfg, "Expression_Loop")}


def _formula(cfg):
    ff = cfg.findall("FormulaFields/FormulaField")
    return {"formulas": [{"output_field": f.get("field"), "expression": f.get("expression"), "data_type": f.get("type"),
                          "size": f.get("size")} for f in ff], "passthrough": not ff}


EXTRACT = {"JoinNode": _join, "JoinMultipleNode": _joinmulti, "CrossTabNode": _crosstab, "RunningTotalNode": _runningtotal,
           "TransposeNode": _transpose, "MultiRowFormulaNode": _multirow, "DynamicRenameNode": _dynrename,
           "DateTimeNode": _datetime, "GenerateRowsNode": _generaterows, "FormulaNode": _formula}
# IR values that are known to be unreliable even when present: always replaced by the XML value
ALWAYS = {"TransposeNode": ("key_fields", "data_fields"), "MultiRowFormulaNode": ("output_field",),
          "CrossTabNode": ("aggregation",)}


def apply(wf, path):
    """Fill/repair node settings from the XML. Returns {node_type: count} of recovered tools."""
    tools = load(path)
    done = {}
    for nid, node in wf.nodes.items():
        xt = tools.get(nid)
        if xt is None:
            continue
        if xt.macro:
            node["macro_file"] = xt.macro
        if node["node_type"] == "ReadNode" and xt.schema and not node.get("field_info"):
            node["field_info"] = xt.schema
            node.setdefault("_recovered", []).append("field_info")
        fn = EXTRACT.get(node["node_type"])
        if fn is None or xt.cfg is None:
            continue
        got = fn(xt.cfg)
        changed = []
        for key, val in got.items():
            cur = node.get(key)
            empty = cur in (None, "", [], {}) or (isinstance(cur, str) and cur.strip().lower() in BAD)
            if (empty and val not in (None, "", [], {})) or (key in ALWAYS.get(node["node_type"], ()) and val != cur) \
                    or key in ("update_field", "join_by_position", "data_unknown", "data_skip", "passthrough", "methods",
                               "is_from", "other_rows", "unknown_renamed", "fields_kept", "join_inputs", "expr_init",
                               "expr_cond", "expr_loop"):
                if cur != val:
                    node[key] = val
                    changed.append(key)
        if changed:
            node.setdefault("_recovered", []).extend(changed)
            done[node["node_type"]] = done.get(node["node_type"], 0) + 1
    return done
