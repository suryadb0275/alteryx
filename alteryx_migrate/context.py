"""The full conversion context of one workflow, for whoever completes or re-targets the conversion (a person, or another AI that writes SDP / other code).

Written at the end of conversion_report.md and as conversion_context.json. It lists, from what the conversion itself established (nothing is guessed here):
- every source (the Bronze table, the Alteryx input tool, its query and how it was translated);
- every output table in build order: its tools in order, what it reads, its status, the generated code of each tool, and the review notes;
- every MISSING transformation - a tool whose logic is not in the notebooks - with its full Alteryx settings (XML), the skill's recipe, where it sits, which table
  and notebook it belongs to, why it is missing and what the user said about it;
- the tools that are deliberately not converted (dead branches, Browse, comments, export tools), so that they are not mistaken for gaps;
- the decisions applied (record orders, macros, field types).
"""
from __future__ import annotations

import json
import re

from . import engine, templates

XML_LIMIT = 6000


HIDDEN = templates.SKIP_KEYS | {"original_configuration", "conversion_notes", "unsupported_reason", "position", "annotation", "field_info", "converted_from"}


def _settings(node: dict) -> dict:
    """The settings the skill converts from: the parser's model with the values recovered from the workflow XML (xml_recover) - not the raw parser output."""
    hidden = HIDDEN | ({"join_type", "input_count"} if node.get("node_type") == "JoinMultipleNode" else set())      # the parser's guess; the skill uses the XML values
    return {k: v for k, v in node.items() if k not in hidden and not k.startswith("_") and v not in (None, "", [], {})}


def _blocks(code: str) -> dict:
    """{tool id: the code of that tool} from a cell body whose tools start with '# [id] Kind'."""
    out = {}
    for m in re.finditer(r"^# \[(\d+)\] .*?(?=^# \[\d+\] |^write_output\(|\Z)", code or "", re.M | re.S):
        out.setdefault(int(m.group(1)), m.group(0).rstrip())
    return out


def _link(wf, edges, side):
    rows = []
    for e in edges:
        other = e.src if side == "up" else e.dst
        rows.append({"id": int(other), "name": engine.tool_name(wf, other), "from_anchor": e.src_anchor, "to_anchor": e.dst_anchor})
    return rows


def _link_text(rows):
    return ", ".join(f"[{r['id']}] {r['name']} ({r['from_anchor']} -> {r['to_anchor']})" for r in rows) or "-"


def build(analysis, handles, chunks: list, codes: dict, statuses: dict, ai_info: dict, bronze_sql: dict, maps: dict, decisions: dict, asked: dict,
          modules: dict | None = None) -> dict:
    """The context as a plain dict (also written as conversion_context.json)."""
    wf = handles["wf"]
    inventory = {r["id"]: r for r in analysis.inventory}
    unsupported = {u["id"]: u for u in analysis.unsupported}
    rows_by_tool = {u["id"]: u for c in chunks for u in c.get("unconverted") or []}
    rows_by_tool.update(unsupported)
    ai_by = (ai_info or {}).get("by") or {}
    chunk_of = {}
    for c in chunks:
        for t in c["tools"]:
            chunk_of.setdefault(t["id"], []).append(c["id"])
    code_of = {}
    for c in chunks:
        if c["id"] in codes and codes[c["id"]]:
            for tid, block in _blocks(codes[c["id"]]).items():
                code_of.setdefault(tid, (c["id"], block))
    xml = engine.raw_configs(analysis.workdir, list(wf.nodes), limit=XML_LIMIT)
    by_chunk = {c["id"]: c for c in chunks}

    tools = []
    for nid in sorted(wf.nodes):
        kind = wf.kind(nid)
        mine = chunk_of.get(nid, [])
        template = [cid for cid in mine if by_chunk[cid]["strategy"] == "template"]
        written = [cid for cid in mine if cid in ai_by]
        row = rows_by_tool.get(nid)
        if kind == "read":
            status, missing = "source", False
        elif nid in unsupported and "output tool" in unsupported[nid].get("reason", ""):
            status, missing = "output tool (the export is not converted; the notebooks write a Delta table instead)", False
        elif template:
            status, missing = "converted by rule", False
        elif written:
            status, missing = "written by the model (AI-WRITTEN: review before use)", True
        elif row is not None and not mine and not wf.consumers(nid):
            status, missing = f"its result goes nowhere (no tool reads it and it writes nothing): not converted on purpose - {row.get('reason', '')}", False
        elif row is not None:
            status, missing = ("NOT CONVERTED - " + (row["status"] if row["status"].startswith("waits") else "cannot be converted")), True
        elif mine:
            status, missing = "NOT CONVERTED - its table is a hand-off because of another tool in it", True
        elif kind in ("browse", "comment"):
            status, missing = "no effect on the data (not converted on purpose)", False
        else:
            status, missing = "not needed for any output (a dead branch, or only feeds a Browse): not converted on purpose", False
        inv = inventory.get(nid, {})
        entry = {"id": int(nid), "name": engine.tool_name(wf, nid), "plugin": str(wf.nodes[nid].get("original_plugin_name") or ""), "container": inv.get("container", ""),
                 "status": status, "missing": missing, "tables": mine, "upstream": _link(wf, wf.producers(nid), "up"), "downstream": _link(wf, wf.consumers(nid), "down")}
        component = f"utils/python_tool_{nid}.py"
        if component in (modules or {}):                      # an embedded Python tool: its logic is in a util file, not in the notebook cell
            entry["component_file"], entry["component_code"] = component, modules[component]
            entry["status"] += f" (its logic is in {component}, called from the notebook)"
        if nid in code_of:
            entry["code_table"], entry["code"] = code_of[nid]
        elif written:                                          # the model's code for the table, when it does not mark each tool
            entry["code_table"], entry["code"] = written[0], codes.get(written[0], "")
        if missing or nid in unsupported:
            u = row or {"id": nid, "name": entry["name"], "plugin": entry["plugin"], "container": entry["container"], "status": status, "reason": "", "after": "", "before": ""}
            waits = str(u.get("status", "")).startswith("waits")
            entry.update(reason=(f"it {u['status']}; open points of its table: {u.get('reason', '')}" if waits else u.get("reason") or u.get("status", "")),
                         after=u.get("after", ""), before=u.get("before", ""), notebook=u.get("notebook", ""), decision="" if waits else u.get("decision", ""),
                         settings_xml=xml.get(nid, ""), settings_parsed=_settings(wf.nodes[nid]),
                         recipe=templates.recipe_for(u), user_said=asked.get(str(nid), ""))
        tools.append(entry)

    sources = []
    for key, info in sorted(analysis.inputs.items()):
        t = bronze_sql.get(key)
        sources.append({"table_key": key, "unity_catalog_name": maps["inputs"].get(key, key), "alteryx_tools": info.get("tools", []), "source_tables": info.get("source_tables", []),
                        "query": info.get("query") or "", "query_translated": bool(t is not None and t.ok), "translation_error": (t.error if t is not None else ""),
                        "translation_notes": list(dict.fromkeys(t.notes)) if t is not None else [], "columns": (t.columns if t is not None and t.columns else None)})

    tables = []
    for c in templates.order_chunks(chunks):
        tables.append({"table": c["id"], "unity_catalog_name": maps["outputs"].get(c["id"], c["id"]), "layer": c["layer"], "container": c["container"],
                       "notebook": templates.LAYER_FILE.get(c["layer"], ""), "status": statuses.get(c["id"], "unknown"),
                       "reads_sources": list(c.get("sources") or []), "reads_tables": list(c.get("tables_read") or []), "chain": c.get("chain") or "",
                       "tools": [t["id"] for t in c["tools"]], "spark_sql": bool(c.get("sql")), "no_sql_reason": c.get("sql_error") or "",
                       "review_notes": list(dict.fromkeys(c.get("reasons") or []))})
    return {"workflow": analysis.file_name, "sha256": analysis.sha256, "tool_count": len(tools), "sources": sources, "tables": tables, "tools": tools,
            "missing": [t["id"] for t in tools if t["missing"]], "macros_missing": list(analysis.macros_missing),
            "decisions": {k: v for k, v in (decisions or {}).items() if k != "_auto"}, "decided_for_you": (decisions or {}).get("_auto") or {}}


def _fence(text: str, lang: str = "") -> list:
    return [f"```{lang}", text.replace("```", "'''"), "```"]


def markdown(ctx: dict) -> str:
    """The context as the last part of conversion_report.md."""
    tools = {t["id"]: t for t in ctx["tools"]}
    missing = [tools[i] for i in ctx["missing"]]
    counts = {}
    for t in ctx["tools"]:
        counts[t["status"].split(" (")[0].split(" - ")[0]] = counts.get(t["status"].split(" (")[0].split(" - ")[0], 0) + 1
    out = ["", "---", "", "# Conversion context (complete - for a person or another AI that finishes or re-targets this conversion)", "",
           "Everything below comes from the conversion itself: the workflow XML, the converter's analysis and the generated notebooks. Nothing is guessed. "
           "Each output table is one table the notebooks write (one dataset in a pipeline); its tools are listed in order with the code generated for each. "
           "A tool marked **NOT CONVERTED** or **AI-WRITTEN** is a gap: its logic must be written (or reviewed) from the settings shown. "
           "Tools marked *not converted on purpose* change no output and must not be added. The same content is in `conversion_context.json`.", "",
           f"- Workflow: `{ctx['workflow']}` (sha256 `{ctx['sha256'][:12]}`), {ctx['tool_count']} tools: "
           + ", ".join(f"{n} {k}" for k, n in sorted(counts.items(), key=lambda kv: -kv[1])) + ".",
           f"- Missing transformations: **{len(missing)}**" + (f" (tools {', '.join('[%d]' % t['id'] for t in missing)})" if missing else " - every transformation that feeds an output is in the notebooks."),
           f"- Macros used but not available: {', '.join(ctx['macros_missing']) or 'none'}.", ""]

    out += ["## 1. Missing transformations (must be completed)", ""]
    if not missing:
        out += ["None.", ""]
    for t in missing:
        out += [f"### [{t['id']}] {t['name']} - {t['status']}", "",
                f"- Plugin: `{t['plugin'] or '-'}`   Container: {t['container'] or '-'}",
                f"- Belongs to table(s): {', '.join('`%s`' % x for x in t['tables']) or '-'}   Notebook: {t.get('notebook') or '-'}",
                f"- Comes after: {_link_text(t['upstream'])}",
                f"- Feeds: {_link_text(t['downstream'])}",
                f"- Why it is missing: {t.get('reason') or '-'}"]
        if t.get("decision") == "order":
            out.append("- What is needed: the order of the records (the workflow sets none before this tool). Choose the sort field(s), or confirm that any record is "
                       "acceptable (Unique / Sample), then keep the first record per key / compute over that order.")
        if t.get("user_said"):
            out += ["- What the user said this tool must do:", *[f"  > {line}" for line in t["user_said"].splitlines()]]
        if t.get("code"):
            out += ["- Code written for it (AI-WRITTEN - review against the settings):", *_fence(t["code"], "python")]
        if t.get("settings_parsed"):
            out += ["- Settings the converter read (the workflow XML recovered over the parser's model), one per line:",
                    *_fence("\n".join(f"{k}: {json.dumps(v, default=str)}"[:2000] for k, v in t["settings_parsed"].items()), "text")]
        if t.get("settings_xml"):
            out += ["- Original Alteryx settings (XML from the workflow - the source of truth):", *_fence(t["settings_xml"], "xml")]
        if t.get("recipe"):
            out += ["- How to replicate it (the skill's recipe, from the Alteryx help):", *[f"  {line}" for line in t["recipe"]]]
        out.append("")

    out += ["## 2. Sources", "", "| Table | Unity Catalog name | Alteryx input tool(s) | Reads | Query | Columns |", "|---|---|---|---|---|---|"]
    for s in ctx["sources"]:
        q = "translated to Spark SQL (a view)" if s["query_translated"] else (f"NOT translated: {s['translation_error']}" if s["query"] else "no query (an existing table)")
        out.append(f"| `{s['table_key']}` | `{s['unity_catalog_name']}` | {', '.join('[%d]' % i for i in s['alteryx_tools']) or '-'} | {', '.join(s['source_tables']) or '-'} | {q} | "
                   f"{', '.join(s['columns']) if s['columns'] else 'not listed in the workflow'} |")
    for s in ctx["sources"]:
        if s["query"]:
            out += ["", f"Query of `{s['table_key']}` (original):", *_fence(s["query"], "sql")]
            if s["translation_notes"]:
                out += ["Translation notes: " + "; ".join(s["translation_notes"])]
    out.append("")

    out += ["## 3. Output tables, in build order", ""]
    for tb in ctx["tables"]:
        reads = tb["reads_sources"] + tb["reads_tables"]
        out += [f"### `{tb['table']}` ({tb['layer']}, {tb['notebook']}) - {tb['status']}", "",
                f"- Unity Catalog name: `{tb['unity_catalog_name']}`   Container: {tb['container']}",
                f"- Reads: {', '.join('`%s`' % r for r in reads) or 'nothing (embedded data)'}",
                f"- Tools in order: {tb['chain'] or '-'}",
                f"- Spark SQL: {'yes' if tb['spark_sql'] else 'no - ' + (tb['no_sql_reason'] or 'PySpark only')}"]
        if tb["review_notes"]:
            out += ["- Review notes from the converter:", *[f"  - {n}" for n in tb["review_notes"][:40]]]
        for tid in tb["tools"]:
            t = tools.get(tid)
            if t is None or t["status"] == "source":
                continue
            flag = "" if not t["missing"] else "  **<- see section 1**"
            out.append(f"- [{tid}] {t['name']}: {t['status']}{flag}")
            if t.get("code") and not t["missing"] and t.get("code_table") == tb["table"]:
                out += _fence(t["code"], "python")
        out.append("")

    outside = [t for t in ctx["tools"] if t.get("component_file")]
    if outside:
        out += ["## 3b. Code outside the notebooks", "",
                "These tools are embedded Python (pandas) scripts. The notebook only calls the util file below; the logic is in it. It runs on the driver, so a "
                "re-targeted pipeline must port it (or keep it as a component).", ""]
        for t in outside:
            out += [f"### [{t['id']}] {t['name']} - `{t['component_file']}`", "", *_fence(t["component_code"], "python"), ""]

    on_purpose = [t for t in ctx["tools"] if not t["missing"] and not t["status"].startswith(("source", "converted by rule"))]
    out += ["## 4. Not converted on purpose (do not add these)", ""]
    out += [f"- [{t['id']}] {t['name']} ({t['container'] or '-'}): {t['status']}" for t in on_purpose] or ["None."]
    out.append("")

    out += ["## 5. Every tool", "", "| Tool | Name | Container | Status | Table(s) | After | Feeds |", "|---|---|---|---|---|---|---|"]
    for t in ctx["tools"]:
        cell = lambda s: str(s).replace("|", "\\|")                                     # noqa: E731
        out.append(f"| [{t['id']}] | {cell(t['name'])} | {cell(t['container'])} | {cell(t['status'])} | {cell(', '.join(t['tables']) or '-')} | "
                   f"{cell(_link_text(t['upstream']))} | {cell(_link_text(t['downstream']))} |")
    out.append("")
    if ctx["decisions"]:
        out += ["## 6. Answers applied (the user's and the converter's)", "", *_fence(json.dumps(ctx["decisions"], indent=1, default=str), "json")]
        if ctx["decided_for_you"]:
            out += ["Decided for the user (see 'Decisions made for you'):", *_fence(json.dumps(ctx["decided_for_you"], indent=1, default=str), "json")]
    return "\n".join(out) + "\n"
