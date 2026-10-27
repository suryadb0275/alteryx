"""Pre-flight check: runs BEFORE any code is generated.

Scans every tool, classifies it BLOCKED / WAITING / ready, and builds ONE grouped message of questions
(1. BLOCKING, 2. RECOMMENDED, 3. CONFIRM).  Nothing is guessed: a blocked tool and everything downstream of it is not converted
until the missing input arrives (see ``answers.template.json`` for how to supply it).  Settings that exist in the workflow XML
are read from it (xml_recover.py) and never asked of the user.
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict

import wfmodel as M

TIME_FUNCS = re.compile(r"\b(DateTimeToday|DateTimeNow|DateTimeFirstOfMonth|DateTimeLastOfMonth|DateTimeStart)\s*\(", re.I)
TEXT_FUNCS = re.compile(r"\b(Contains|StartsWith|EndsWith|FindString|Replace|REGEX_Match|REGEX_Replace)\s*\(", re.I)
DECIMAL_TYPES = ("fixeddecimal",)
CONFIG_PREFIX = ("config '", "join keys are empty", "tool has no input")


def expressions_of(node):
    out = []
    for f in node.get("formulas") or []:
        out.append((f.get("expression") or "", f.get("data_type")))
    for key in ("expression", "expr_init", "expr_cond", "expr_loop"):
        if node.get(key):
            out.append((node[key], None))
    return out


def ids(lst, limit=None):
    lst = sorted(lst)
    shown = lst if limit is None else lst[:limit]
    return ", ".join(f"#{i}" for i in shown) + (f" (+{len(lst) - len(shown)} more)" if limit and len(lst) > limit else "")


def descendants(wf, start, live):
    seen, stack = set(), list(start)
    while stack:
        x = stack.pop()
        for e in wf.consumers(x):
            if e.dst not in seen and e.dst in live:
                seen.add(e.dst)
                stack.append(e.dst)
    return seen


def classify(wf, em, live):
    """-> (blocked: {id: reason}, waiting: {id: [blocking ids]}) among live data tools."""
    blocked = {n: (em.results[n].reviews or ["blocked"])[0] for n in live
               if n in em.results and em.results[n].status in ("blocked", "unsupported")}
    waiting = {}
    for b in blocked:
        for d in descendants(wf, [b], live):
            if d not in blocked:
                waiting.setdefault(d, []).append(b)
    return blocked, waiting


def code_tool_summary(node):
    """For Python/Jupyter/R code tools: (code lines, imported libraries, number of inputs read) from the stored script, or None."""
    cfg = node.get("original_configuration") or {}
    nb = cfg.get("Notebook") if isinstance(cfg, dict) else None
    script = None
    for _ in range(6):
        if isinstance(nb, dict) and "#cdata-section" in nb:
            nb = nb["#cdata-section"]
        elif isinstance(nb, str):
            try:
                nb = json.loads(nb)
            except ValueError:
                script = nb
                break
        else:
            break
    if isinstance(nb, dict) and isinstance(nb.get("cells"), list):
        script = "\n".join("".join(c.get("source", [])) if isinstance(c.get("source"), list) else str(c.get("source", ""))
                           for c in nb["cells"] if c.get("cell_type") == "code")
    if script is None and isinstance(cfg, dict) and isinstance(cfg.get("productionModeScript"), str):
        script = cfg["productionModeScript"]
    if not script:
        return None
    code_lines = [ln for ln in script.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    libs = sorted({m.group(1) for m in re.finditer(r"^\s*(?:import|from)\s+([A-Za-z_][\w]*)", script, re.M)} - {"ayx"})
    reads = len(set(re.findall(r"Alteryx\.read\(\s*[\"']#(\d+)", script)))
    return len(code_lines), libs, reads


def _category(node, reason):
    if code_tool_summary(node):
        return "codetool"
    r = reason.lower()
    if node.get("macro_file") or "macro:" in str(node.get("original_plugin_name", "")).lower():
        return "macro"
    if r.startswith(CONFIG_PREFIX):
        return "config"
    if "unsupported function" in r:
        return "function"
    if "cannot be translated" in r:
        return "expression"
    if "depends on record order" in r:
        return "order"
    return "behaviour"


def build(wf, plan, em, blocked, waiting, info, answers=None):
    answers = answers or {}
    accept = answers.get("accept", {})
    items = []

    def add(kind, tool_ids, label, missing, why, send, default):
        items.append(dict(kind=kind, tools=sorted(tool_ids), label=label, missing=missing, why=why, send=send, default=default))

    # ---- BLOCKING: group by cause
    groups = defaultdict(list)
    for n, reason in blocked.items():
        node = wf.nodes[n]
        if n in info["sinks"] and wf.kind(n) in ("unsupported", "predictive") and wf.producers(n):
            continue            # an output tool without a converter: handled as a CONFIRM item with a default below
        cat = _category(node, reason)
        if cat == "macro":
            key = (cat, node.get("macro_file") or str(node.get("original_plugin_name")).replace("macro:", ""))
        elif cat == "function":
            m = re.search(r"Unsupported function (\w+)", reason)
            key = (cat, m.group(1) if m else reason[:60])
        elif cat == "order":
            key = (cat, wf.nodes[n]["node_type"])
        else:
            key = (cat, re.sub(r"\s+", " ", reason.split("::")[0])[:110])
        groups[key].append(n)
    for (cat, what), tools in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        types = sorted({(wf.nodes[t].get("original_plugin_name") if wf.nodes[t].get("original_tool_type") in (None, "Unknown")
                         else wf.nodes[t].get("original_tool_type")) or wf.nodes[t]["node_type"] for t in tools})
        label = f"{', '.join(types[:3])}"
        if cat == "macro":
            add("BLOCKING", tools, f"Macro {what} ({label})", f"the macro file `{what}` (it is called but not part of the workflow)",
                "its logic is not in the workflow XML, so the tool cannot be converted faithfully",
                f"the `{what}` file, the name of a PySpark function that replaces it (answers.json -> macros), or - if the macro only reads a source "
                f"(for example a Google Sheet) - one line saying so: answers.macros.<name>.as_source = true (optional .table); it then becomes an input table",
                "these tools and everything downstream are not converted (listed as waiting for input)")
        elif cat == "codetool":
            n_lines, libs, reads = code_tool_summary(wf.nodes[tools[0]])
            add("BLOCKING", tools, f"{label} - embedded Python code",
                f"a decision on the embedded script ({n_lines} code lines, uses {', '.join(libs) or 'no libraries'}, reads {reads} inputs)",
                "it holds hand-written business logic that no converter can translate faithfully; its full code is kept in unsupported_tools.xml",
                "approve translating the script to PySpark as a separate reviewed step, and send an Alteryx sample output for it "
                "(or the intended rules, or confirm a pandas-on-Spark/pandas step may stay)",
                "this tool and everything downstream are not converted")
        elif cat == "config":
            add("BLOCKING", tools, label, what, "a required setting is empty or unreadable in the workflow",
                "the setting value or the original .yxmd file", "these tools and everything downstream are not converted")
        elif cat == "function":
            add("BLOCKING", tools, f"{label} - function {what}()", f"a certain translation of `{what}()`",
                "the function cannot be translated with certainty",
                "what it must return (a sample input/output from Alteryx is best)", "these tools and everything downstream are not converted")
        elif cat == "expression":
            add("BLOCKING", tools, f"{label} - expression", what, "the expression cannot be translated reliably",
                "the intended logic or an Alteryx sample result", "these tools and everything downstream are not converted")
        elif cat == "order":
            unique = [t for t in tools if wf.kind(t) == "unique"]
            add("BLOCKING", tools, f"{label} - record order", "an order key (sort field(s), ascending/descending)",
                "the result depends on record order (first/previous record) and no sort key is defined upstream",
                "the order field(s) per tool in answers.json -> overrides.<id>.order_by"
                + ("; for Unique tools reply 'any row' (accept.order_any) if which duplicate is kept does not matter" if unique else ""),
                "these tools and everything downstream are not converted")
        else:
            add("BLOCKING", tools, label, what, "no verified PySpark equivalent exists for this behaviour",
                "the expected rule (or a sample Alteryx output) so it can be implemented, or confirm it can be left out",
                "these tools and everything downstream are not converted")
    # external sources without schema
    no_schema = [n for n in info["sources"] if wf.kind(n) == "read" and not wf.nodes[n].get("field_info")
                 and n in info["live"]]
    if no_schema and not accept.get("source_schema_at_runtime", True) and not answers.get("schemas"):          # default: read the columns from the Bronze table at run time
        kinds = sorted({str(wf.nodes[n].get("connection_string") or wf.nodes[n].get("source_type")) for n in no_schema})
        add("BLOCKING", no_schema, f"External source(s) {', '.join(kinds)}",
            "a schema (column names + types) or a sample of the data",
            "the workflow does not store a schema for these inputs, so column names/types cannot be checked",
            "a column list with types or a sample file per tool - or reply 'resolve at runtime' (accept.source_schema_at_runtime) to read "
            "the columns from the Bronze table when the notebook runs",
            "these tools and everything downstream are not converted")
    # custom helper code referenced by overrides
    for n, node in wf.nodes.items():
        cp = node.get("custom_python")
        if cp and not (node.get("ported_python") or node.get("kept_python")):             # code ported from an embedded Python tool is checked by tests, not by a helper list
            called = set(re.findall(r"\b([A-Za-z_]\w*)\(", cp)) - {"H", "F", "spark"}
            helpers = set(answers.get("helpers", []))
            missing = sorted(called - helpers - {"len", "list", "dict", "str", "int", "float", "set", "sorted"})
            if missing:
                add("BLOCKING", [n], f"Helper code {', '.join(missing)}", f"the function(s) {', '.join(missing)} used by custom_python",
                    "the replacement code calls functions that are not provided", "the module/file that defines them (answers.json -> helpers)",
                    "this tool and everything downstream are not converted")
    # ---- RECOMMENDED
    unions = []
    for n in wf.nodes:
        if wf.kind(n) == "union":
            srcs = [(e.src, e.src_anchor) for e in wf.producers(n)]
            for src, c in Counter(s for s, _ in srcs).items():
                if c > 1 and n in info["live"]:
                    unions.append((n, src, sorted(a for s, a in srcs if s == src)))
    if unions:
        add("RECOMMENDED", [u[0] for u in unions], "Union fed twice by the same tool",
            "confirmation that both outputs are meant to be stacked",
            "; ".join(f"Union #{u} receives {a} from #{s}" for u, s, a in unions) + " - the second input is easy to lose",
            "ok, or which output to keep",
            "I stack both inputs by column name (unionByName) exactly as connected")
    reviews = defaultdict(list)
    for n, r in em.results.items():
        if n in info["live"] and n not in blocked and n not in waiting and r.status == "requires_review":
            for msg in r.reviews:
                if msg.startswith("input tool '") and no_schema:
                    continue                                   # covered by the external-source question above
                reviews[msg.split("::")[0][:140]].append(n)
    for msg, tools in sorted(reviews.items(), key=lambda kv: -len(kv[1]))[:8]:
        add("RECOMMENDED", tools, "Behaviour to confirm", msg, "accuracy is at risk until compared with Alteryx output",
            "an Alteryx sample result, or ok", f"I convert as described and mark these tools 'requires review' ({len(tools)} tools)")
    # ---- CONFIRM (project level)
    timef = [n for n in wf.nodes if any(TIME_FUNCS.search(e) for e, _ in expressions_of(wf.nodes[n]))]
    textf = [n for n in wf.nodes if any(TEXT_FUNCS.search(e) for e, _ in expressions_of(wf.nodes[n]))]
    decf = [n for n in wf.nodes if any((t or "").lower() in DECIMAL_TYPES for _, t in expressions_of(wf.nodes[n]))]
    gold = sorted(t for u in plan.units for t in u.table.values() if t.startswith("gold_"))
    outputs = [n for n in info["sinks"] if wf.kind(n) not in ("write",)]
    outputs = [n for n in outputs if wf.kind(n) in ("unsupported", "predictive")] or []
    add("CONFIRM", timef, "Run date and time zone", "which time zone and run date 'today', 'now' and 'first of month' use",
        "date functions change the result with the run date and session time zone", "ok, or a time zone (e.g. Europe/Oslo) and the reporting month rule",
        "UTC, the Databricks run date, and the report always covers the current month")
    add("CONFIRM", [], "Target names", "catalog, schemas and table names",
        f"{len(plan.bronze)} Bronze inputs, {sum(1 for u in plan.units if u.layer == 'silver')} Silver and {len(gold)} Gold tables are proposed",
        "ok, or a catalog/schema list and --naming file", "catalog `main`, schemas `bronze`/`silver`/`gold`, generated table names"
        + (f" (Gold: {', '.join(gold[:6])}{' ...' if len(gold) > 6 else ''})" if gold else ""))
    add("CONFIRM", [], "Write mode", "overwrite or append, and whether overwriting existing tables is allowed",
        "Alteryx recomputes everything each run", "ok, or the mode", "full refresh (overwrite); overwriting existing tables is NOT allowed until you set allow_overwrite=true")
    add("CONFIRM", [], "Spark / Databricks runtime and ANSI mode", "runtime version and spark.sql.ansi.enabled",
        "ANSI mode changes cast and divide-by-zero behaviour", "ok, or the runtime/ANSI setting",
        "Databricks Runtime 16.x (Spark 3.5), ANSI off; the code is also safe with ANSI on (Spark 4)")
    add("CONFIRM", decf, "Numeric rules", "rounding/truncation of FixedDecimal, divide-by-zero and NULL handling",
        "results can differ in the last digit or become NULL instead of an error",
        "ok, or the rule", "Spark decimal cast (round half up); divide by zero gives NULL; NULL in arithmetic gives NULL")
    add("CONFIRM", textf, "Text matching", "is Contains/StartsWith/EndsWith case-sensitive in the original?",
        "the same data can match differently", "ok, or 'case-sensitive' (answers.project.text_match_case_sensitive)",
        "case-insensitive for Contains/StartsWith/EndsWith (Alteryx function reference default); '=' on text is case-sensitive")
    joinf = [n for n in wf.nodes if wf.kind(n) == "join"]
    if joinf:
        add("CONFIRM", joinf, "Join keys that are NULL", "does a NULL key match a NULL key in the original?",
            "the Alteryx help does not say; rows with NULL keys are matched or dropped accordingly",
            "ok, or 'null-safe' (answers.project.null_safe_join = true) if NULL must match NULL",
            "NULL keys never match (SQL equality); set null_safe_join to use a null-safe comparison")
    if outputs:
        add("CONFIRM", outputs, "Output tools without a converter", "what to do with outputs written by a tool we cannot convert (e.g. BigQuery writer)",
            "the data feeding them is still produced", "ok, or the target",
            "the final dataset is written to a Delta Gold table; no write-back to the external system is generated")
    add("CONFIRM", [], "Validation data", "one frozen input snapshot AND the original Alteryx output for the same snapshot",
        "without them output equality cannot be shown", "the snapshot tables and the Alteryx output files",
        "none: results are reported as 'converted, not yet validated'")
    pre = dict(items=items, blocked=blocked, waiting={k: sorted(v) for k, v in waiting.items()})
    pre["blocking"] = sum(1 for i in items if i["kind"] == "BLOCKING")
    return pre


def render_message(pre, wf, total):
    """One grouped message in the required format."""
    out = [f"# Pre-flight check - {wf.meta.get('file_path')}", ""]
    nb, nw = len(pre["blocked"]), len(pre["waiting"])
    out.append(f"{total} data tools scanned: **{nb} blocked**, {nw} waiting on blocked tools, "
               f"{total - nb - nw} ready to convert. Settings found in the workflow XML were read from it and are not asked below.")
    if not wf.yxmd:
        out.append("\n> No `.yxmd` was supplied: settings the parser dropped could not be recovered. Pass `--yxmd <file>` first - "
                   "most BLOCKING items below disappear when the XML is read.")
    sections = (("BLOCKING", "1. BLOCKING - needed before I can start"), ("RECOMMENDED", "2. RECOMMENDED - I can start without these, but accuracy is at risk"),
                ("CONFIRM", "3. CONFIRM - project settings with my proposed default"))
    for kind, title in sections:
        out += ["", f"## {title}", ""]
        its = [i for i in pre["items"] if i["kind"] == kind]
        if not its:
            out.append("- none")
        for it in its:
            where = f"[{ids(it['tools'])} - {it['label']}]" if it["tools"] else f"[{it['label']}]"
            if kind == "CONFIRM":
                out.append(f"- {where} {it['missing']}. Proposed default: {it['default']}. Reply \"ok\" to accept.")
            elif kind == "RECOMMENDED":
                out.append(f"- {where} {it['missing']} ({it['why']}). Send: {it['send']}. If no answer: {it['default']}.")
            else:
                out.append(f"- {where} Missing: {it['missing']}. Why it matters: {it['why']}. Send: {it['send']}. "
                           f"If no answer: {it['default']}.")
    return "\n".join(out) + "\n"


def answers_template(pre, wf):
    blocking = [i for i in pre["items"] if i["kind"] == "BLOCKING"]
    order_ids = sorted({t for i in blocking if "record order" in i["label"] for t in i["tools"]})
    return {
        "_how_to_use": "Fill what you can, delete what you cannot, then run again with --answers <this file>. Tools still blocked are "
                       "listed as 'waiting for input'; everything else is converted.",
        "overrides": {str(t): {"order_by": [{"field_name": "<field>", "ascending": True}]} for t in order_ids[:3]},
        "macros": {m: {"custom_python": "{out} = <your_function>({in0})"} for m in sorted(
            {i["label"].split()[1] for i in blocking if i["label"].startswith("Macro")})},
        "accept": {"order_any": [], "source_schema_at_runtime": True, "defaults": False},
        "project": {"timezone": "UTC", "catalog": "main", "bronze_schema": "bronze", "silver_schema": "silver",
                    "gold_schema": "gold", "write_mode": "overwrite", "allow_overwrite": False, "spark_runtime": "DBR 16.x",
                    "ansi": False, "text_match_case_sensitive": False, "null_safe_join": False},
        "validation": {"input_snapshot": "", "alteryx_output": ""},
    }


def apply_answers(wf, answers):
    """Merge the user's answers into the workflow model (call after load_workflow)."""
    if not answers:
        return
    accept = answers.get("accept", {})
    order_any = accept.get("order_any", [])
    for n, node in wf.nodes.items():
        if wf.kind(n) == "unique" and (order_any == "all" or n in [int(x) for x in order_any if str(x).isdigit()]):
            node["order_any"] = True
        macro = node.get("macro_file") or (str(node.get("original_plugin_name", "")).split("macro:")[-1]
                                           if "macro:" in str(node.get("original_plugin_name", "")) else None)
        if macro:
            wanted = {k.lower(): v for k, v in answers.get("macros", {}).items()}
            m = wanted.get(str(macro).lower()) or wanted.get(str(macro).replace("/", "\\").split("\\")[-1].lower())
            if m and m.get("as_source") and not wf.producers(n) and wf.consumers(n):
                # the user says the macro only reads a source (a Google Sheet, a table): its result is expected in an existing Bronze table
                table = str(m.get("table") or re.sub(r"(?i)\.yxmc$", "", str(macro).replace("/", "\\").split("\\")[-1]))
                node.update(node_type="ReadNode", source_type="external", query="", table_name=table, file_path="", file_format="",
                            connection_string=f"macro:{macro}", converted_from="macro", macro_file=None)
                node.setdefault("_override_keys", []).append("as_source (macro answer)")
            if m and m.get("custom_python") and "<" not in m["custom_python"]:
                node["custom_python"] = m["custom_python"]
                node["node_type"] = "UnsupportedNode"
                node.setdefault("_override_keys", []).append("custom_python (macro answer)")
        if accept.get("source_schema_at_runtime", True) and wf.kind(n) == "read":
            node["schema_at_runtime"] = True


def project_settings(answers):
    p = dict((answers or {}).get("project", {}))
    return {"timezone": p.get("timezone", "UTC"), "catalog": p.get("catalog", "main"),
            "bronze_schema": p.get("bronze_schema", "bronze"), "silver_schema": p.get("silver_schema", "silver"),
            "gold_schema": p.get("gold_schema", "gold"), "write_mode": p.get("write_mode", "overwrite"),
            "allow_overwrite": bool(p.get("allow_overwrite", False)), "ansi": p.get("ansi"),
            "text_match_case_insensitive": not p.get("text_match_case_sensitive", False),
            "null_safe_join": bool(p.get("null_safe_join", False)),
            "spark_runtime": p.get("spark_runtime", "DBR 16.x"),
            "defaults_confirmed": bool((answers or {}).get("accept", {}).get("defaults"))}
