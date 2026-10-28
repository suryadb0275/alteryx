"""The questions the converter asks in the terminal, in plain English, before it converts.

Nothing is guessed: whatever the workflow does not say (which row a Unique tool keeps, what a missing macro does, what a tool without a converter must do, which code you want)
is asked here, once, and the answers go into the conversion - and, written in your own words, to the Databricks model that writes the code the rules cannot.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from . import autodecide, pytool, sqlsrc


class Console:
    """Terminal input/output that tests can script."""

    def __init__(self, input_fn=input, print_fn=print):
        self._input, self._print = input_fn, print_fn

    def say(self, text: str = "") -> None:
        self._print(text, flush=True) if self._print is print else self._print(text)

    def ask(self, text: str, default: str = "") -> str:
        suffix = f" [{default}]" if default else ""
        try:
            answer = self._input(f"{text}{suffix}\n  > ")
        except EOFError:
            answer = ""
        return (answer or "").strip() or default

    def choose(self, text: str, options: list, default: int = 1) -> int:
        """1-based number of the chosen option."""
        self.say(text)
        for i, option in enumerate(options, 1):
            self.say(f"   [{i}] {option}")
        while True:
            answer = self.ask("Your choice", str(default))
            if answer.isdigit() and 1 <= int(answer) <= len(options):
                return int(answer)
            self.say(f"   Please type a number from 1 to {len(options)}.")

    def yes_no(self, text: str, default: bool = True) -> bool:
        answer = self.ask(f"{text} (y/n)", "y" if default else "n").lower()
        return answer.startswith("y")

    def answer_question(self, q: dict) -> str:
        """A question the model raised while porting a script: shown with its options, the answer is the option's text or free text."""
        self.say("")
        self.say("The model needs to know something before it can continue:")
        self.say(f"   {q.get('text')}")
        options = [str(o) for o in q.get("options") or []]
        for i, o in enumerate(options, 1):
            self.say(f"   [{i}] {o}")
        default = str(q.get("default") or "")
        answer = self.ask("Your answer (a number, or your own words)", default)
        if answer.isdigit() and options and 1 <= int(answer) <= len(options):
            return options[int(answer) - 1]
        return answer


@dataclass
class Plan:
    formats: tuple = ("pyspark", "sql")
    catalog: str = ""
    schema: str = ""
    decisions: dict = field(default_factory=dict)
    tool_notes: dict = field(default_factory=dict)
    port: dict = field(default_factory=dict)             # python tool id -> True
    describe: dict = field(default_factory=dict)         # python tool id -> what the user says it does
    model: str = ""
    source_catalog: str = ""
    source_schema: str = ""
    sql_dialect: str = ""


def _tool_label(u: dict) -> str:
    return f"[{u['id']}] {u['name']} (container: {u['container']})"


ORDER_WHAT = {"unique": "keeps the first record of each group", "sample": "keeps the first or last records (of each group)",
              "multirowformula": "reads the previous / next row", "runningtotal": "adds up the rows in order", "recordid": "numbers the rows in order"}


def _order_text(d: dict) -> str:
    return ", ".join(f + (" desc" if f in set(d.get("desc") or []) else "") for f in d.get("fields") or [])


def ask_order(console: Console, u: dict, last: str, suggestion: dict | None = None) -> tuple:
    """The order question for one tool. -> (decision or None, the text to offer as the next default). A suggestion (the model's medium-confidence proposal) is the default."""
    name = str(u.get("name", ""))
    pick_one = name.lower() in ("unique", "sample")
    console.say(f"\nTool {_tool_label(u)} {ORDER_WHAT.get(name.lower(), 'depends on the order of the rows')}, but the workflow does not say in which order the rows come.")
    console.say(f"   It comes after {u['after']} and feeds {u['before']}.")
    if suggestion:
        console.say(f"   The model suggests: {autodecide.describe(suggestion)} (not sure enough to apply it by itself; press Enter to accept it).")
    if pick_one and console.choose("   Which record should it keep?", ["Any record (the order does not matter)", "The first one after sorting by fields I will name"],
                                   1 if not suggestion or suggestion.get("mode") == "any" else 2) == 1:
        return {"mode": "any"}, last
    default = _order_text(suggestion) if suggestion and suggestion.get("mode") == "order_by" else last
    hint = f" [{default}]" if default else ""
    fields = console.ask(f"   Sort by which fields? (comma separated, exact column names; add 'desc' after a name for descending, e.g. EmployeeID, EffectiveDate desc){hint}", default)
    names, desc = [], []
    for part in [p.strip() for p in fields.split(",") if p.strip()]:
        bits = part.split()
        names.append(bits[0])
        if len(bits) > 1 and bits[1].lower().startswith("desc"):
            desc.append(bits[0])
    if names:
        return {"mode": "order_by", "fields": names, "desc": desc}, fields
    return ({"mode": "any"} if pick_one else None), last


def gather(console: Console, analysis, handles: dict, args, make_client=None, cache=None) -> Plan:
    """Ask everything the conversion needs. Returns the Plan; asks nothing that was already given on the command line, decided from the workflow (see
    autodecide.py) or answered in an earlier run (the answers cache). --ask-all asks every question as before, with no automatic decision and no cache."""
    import tool_coverage as TC
    plan = Plan()
    ask_all = bool(getattr(args, "ask_all", False))
    if ask_all:
        cache = None
    inventory = analysis.unsupported
    py = pytool.load(handles["slim_dir"], handles["wf"]) if handles.get("slim_dir") else {}
    convertible = sum(1 for c in analysis.chunks if c.strategy == "template")
    console.say("")
    console.say(f"I read {analysis.file_name}: {analysis.tool_count} tools, {len(analysis.chunks)} output tables.")
    console.say(f"The rules convert {convertible} of the {len(analysis.chunks)} tables as they are. {len(inventory)} tool(s) need something from you or a model.")
    console.say("I will ask a few questions. Press Enter to accept the answer in [brackets]. Nothing is converted until you have answered.")

    def remembered(kind, key, tool=None):
        item = cache.get(kind, key, tool) if cache is not None else None
        return item["value"] if item else None

    run = dict(remembered("run", "settings") or {})            # the settings of the last run of this workflow (code, tables, model)
    if run:
        console.say("Using the settings of your earlier run of this workflow (run with --forget-answers to answer again).")

    # 0. the model first, so that every later step can use it (it proposes record orders and writes the tools the rules do not convert)
    client = None
    if not ask_all:
        joins_asked = {q["id"] for q in getattr(analysis, "join_questions", []) or []}            # the user maps Join Multiple inputs; a model does not help there
        useful = py or any(not u["status"].startswith("waits") and "output tool" not in u.get("reason", "") and u["id"] not in joins_asked for u in inventory)
        if getattr(args, "model", None) or os.environ.get("MODEL"):
            plan.model = getattr(args, "model", None) or os.environ["MODEL"]
        elif useful and "model" in run:
            plan.model = run["model"]
            console.say(f"   model: {plan.model or 'none'}")
        elif useful:
            console.say("\nA Databricks model can answer the routine questions from the workflow (the converter checks its answers) and write the tools the rules do not convert.")
            plan.model = console.ask("Which model? (name of a Databricks serving endpoint, e.g. alteryx.default.test; Enter = none: I ask you instead, and those tools stay as hand-offs)", "")
        if plan.model and make_client is not None:
            try:
                client = make_client(plan.model)
            except Exception as exc:                            # noqa: BLE001 - without a client the questions are asked as before
                console.say(f"   (the model {plan.model} cannot be used: {exc}; I will ask instead)")

    # 1. what code
    pick = getattr(args, "format", None)
    if pick:
        plan.formats = {"pyspark": ("pyspark",), "sql": ("sql",), "both": ("pyspark", "sql")}[pick]
    elif run.get("formats"):
        plan.formats = tuple(run["formats"])
        console.say(f"   code: {' and '.join(plan.formats)}")
    else:
        n = console.choose("\nWhich code do you need?", ["PySpark notebooks", "Spark SQL notebooks", "Both (PySpark and Spark SQL)"], 3)
        plan.formats = [("pyspark",), ("sql",), ("pyspark", "sql")][n - 1]
        if "sql" in plan.formats:
            console.say("   (Spark SQL needs the columns of the input tables. They are read from Unity Catalog when you write to the workspace, or you give them with --schemas.)")

    # 2. where the tables are
    if not (args.catalog or args.schema) and "catalog" in run:
        plan.catalog, plan.schema = run["catalog"], run.get("schema", "")
        console.say(f"   tables in: {'.'.join(x for x in (plan.catalog, plan.schema) if x) or 'the current catalog and schema'}")
    elif not (args.catalog or args.schema):
        plan.catalog = console.ask("\nWhich catalog holds the tables the notebooks read and write? (Enter = none: the notebooks use the catalog and schema that are current when they run)", "")
        if plan.catalog:
            plan.schema = console.ask("Which schema?", "")
    else:
        plan.catalog, plan.schema = args.catalog or "", args.schema or ""

    # 2b. the SQL inside the input tools
    queries = {k: v for k, v in analysis.inputs.items() if v.get("query")}
    if queries and "source_catalog" in run:
        plan.sql_dialect = getattr(args, "sql_dialect", "") or run.get("sql_dialect", "")
        plan.source_catalog = getattr(args, "source_catalog", "") or run["source_catalog"]
        plan.source_schema = getattr(args, "source_schema", "") or run.get("source_schema", "")
    elif queries:
        sample = next((t for v in queries.values() for t in v.get("source_tables") or []), "")
        console.say("")
        console.say(f"{len(queries)} input(s) carry a SQL query that renames, filters, joins or aggregates the data. It is converted to Spark SQL and becomes a view that the rest of the notebooks read.")
        if sample:
            console.say(f"   The queries read tables named like `{sample}`.")
        unknown = [k for k, v in queries.items() if not sqlsrc.dialect_of(v.get("plugin", ""), v.get("connection", ""), getattr(args, "sql_dialect", "") or "")]
        if unknown:
            keys = list(sqlsrc.DIALECTS)
            n = console.choose("   Which database do those queries come from?", [sqlsrc.DIALECTS[k] for k in keys], 2)
            plan.sql_dialect = keys[n - 1]
        else:
            plan.sql_dialect = getattr(args, "sql_dialect", "") or ""
        plan.source_catalog = getattr(args, "source_catalog", "") or console.ask("   In which Databricks catalog are those source tables? (Enter = the catalog above, or the current one)", plan.catalog)
        plan.source_schema = getattr(args, "source_schema", "") or console.ask("   In which schema? (Enter = use the dataset name written in the query, e.g. sap_data)", "")

    # 3. macros the converter has no definition for (only those that are a gap: an input macro such as BigQuery's is a plain source and needs nothing)
    seen_macros = {}
    for u in inventory:
        plugin = str(u.get("plugin", ""))
        if plugin.lower().startswith("macro") and not u["status"].startswith("waits"):
            seen_macros.setdefault(plugin.split(":", 1)[-1], []).append(u)
    sources = {}
    saved = (getattr(args, "saved_decisions", None) or {}).get("_macros") or {}
    for macro, tools in seen_macros.items():
        ids = sorted(t["id"] for t in tools)
        console.say("")
        console.say(f"The workflow uses the macro '{macro}' (tool {', '.join('[%s]' % i for i in ids[:5])}), and I do not have its definition.")
        earlier = remembered("macro", macro, ids[0])
        if earlier is not None:
            console.say("   Using your answer from an earlier run: " + (f"it only reads data, which is in the table {earlier['table']}." if earlier.get("as_source")
                                                                       else "your description of it." if earlier.get("note") else "it stays a hand-off."))
            if earlier.get("as_source"):
                sources[macro] = {**earlier, "tools": ids}
            elif earlier.get("note"):
                for t in tools:
                    plan.tool_notes[t["id"]] = earlier["note"]
            continue
        before = saved.get(macro)
        if before and before.get("as_source"):          # an answer given earlier (--decisions): shown, and used only when confirmed
            console.say(f"   A saved decision says: it only reads data, which is in the table {before.get('table')}" +
                        (f" (for tool {', '.join('[%s]' % i for i in before['tools'])})." if before.get("tools") else "."))
            if console.yes_no("   Use that decision?", True):
                sources[macro] = {**before, "tools": ids}
                continue
        # 'only reads data' is offered only when every call of the macro has no data input (nothing flows into it but a Control parameter)
        reads_only = not any(t.get("data_input") for t in tools)
        options = (["It only reads data (a Google Sheet, a database, an API ...) - the data is already in a Databricks table"] if reads_only else []) + [
            "It changes the data that flows into it - I will describe it", "Skip (the macro stays a hand-off)"]
        kind = console.choose("   What does it do?", options, 1) + (0 if reads_only else 1)
        if kind == 1:
            table = console.ask("   Which table holds its data? (catalog.schema.table, e.g. google.sheets.raw)", "")
            if table:
                sources[macro] = {"as_source": True, "table": table, "tools": ids}
                if cache is not None:
                    cache.put("macro", macro, sources[macro], tool=ids[0])
                continue
        if kind == 3:
            if cache is not None:
                cache.put("macro", macro, {"skip": True}, tool=ids[0])
            continue
        console.say("   Tell me what it does, in plain English, and I will pass that to the model that writes the code.")
        what = console.ask("What does this macro do? (Enter = skip: the macro stays a hand-off)", "")
        path = console.ask("Or the path to the .yxmc file if you have it (Enter = none)", "") if not what else ""
        text = what
        if path:
            try:
                from pathlib import Path
                text = "Macro definition (XML of the .yxmc file):" + chr(10) + Path(path.strip('"')).read_text(encoding="utf-8", errors="replace")[:20000]
            except OSError as exc:
                console.say(f"   I could not read that file ({exc}); skipping.")
        if text:
            for t in tools:
                plan.tool_notes[t["id"]] = text
        if cache is not None:
            cache.put("macro", macro, {"note": text} if text else {"skip": True}, tool=ids[0])
    if seen_macros:                                     # the answers given here replace any saved ones for this workflow
        plan.decisions["_macros"] = sources

    # 4. decisions the workflow does not state: the order of the records (Alteryx keeps the order rows arrive in; Spark has none)
    if ask_all:
        last = ""
        for u in inventory:
            if u.get("decision") == "order":
                decision, last = ask_order(console, u, last)
                if decision:
                    plan.decisions[str(u["id"])] = decision
    else:
        state = {"last": "", "told": False}

        def ask(items, auto):
            if not state["told"]:
                console.say(f"\nDecided {len(auto.decisions)} of {len(auto.decisions) + len(items)} ordering question(s) from the workflow (see the report). {len(items)} need you.")
                state["told"] = True
            given = {}
            for u in items:
                decision, state["last"] = ask_order(console, u, state["last"], auto.suggestions.get(str(u["id"])))
                if decision:
                    given[str(u["id"])] = decision
            return given
        new, evidence, decided, _asked = autodecide.settle(analysis, handles, client, cache, plan.decisions, getattr(args, "sql_dialect", "") or plan.sql_dialect, ask)
        if decided and not state["told"]:
            console.say(f"\nDecided {decided} ordering question(s) from the workflow (see the report); none need you.")
        plan.decisions.update(new)
        if evidence:
            plan.decisions["_auto"] = evidence

    # 4b. types of fields the workflow does not state (its inputs come from a database query). PySpark decides them at run time; Spark SQL needs them now.
    questions = getattr(analysis, "type_questions", []) or []
    if "sql" in plan.formats and questions:
        users: dict = {}
        for q in questions:
            for f in q["fields"]:
                users.setdefault(f, []).append(f"[{q['id']}] {q['name']}")
        console.say(f"\nThe workflow does not say the type of {len(users)} field(s) that some tools depend on (in Alteryx '+' joins text but adds numbers;")
        console.say("   Data Cleansing cleans text fields and puts 0 in empty numeric ones). The PySpark code checks the type at run time; Spark SQL needs it now.")
        kinds = ["Text (string)", "Number", "Date", "Date and time", "Boolean", "I don't know (that table gets no SQL; PySpark still works)"]
        types = {}
        for f, used in users.items():
            earlier = remembered("type", f)
            if earlier is not None:
                console.say(f"   {f}: {earlier or 'not known'} (your answer from an earlier run)")
                if earlier:
                    types[f] = earlier
                continue
            n = console.choose(f"   What type is {f}? (used by {', '.join(used[:3])})", kinds, 1)
            if n < len(kinds):
                types[f] = ("string", "number", "date", "datetime", "bool")[n - 1]
            if cache is not None:
                cache.put("type", f, types.get(f, ""))
        if types:
            plan.decisions["_field_types"] = types

    # 4c. which tool feeds each input of a Join Multiple (its output fields are named Input_#k_..., the workflow only lists the inputs in file order)
    joins = {}
    for q in getattr(analysis, "join_questions", []) or []:
        earlier = remembered("join", q["id"])
        if earlier is not None:
            console.say(f"\nJoin Multiple tool [{q['id']}]: using your answer from an earlier run (" + ", ".join(f"input {k} from tool [{v}]" for k, v in earlier.items()) + ").")
            joins[str(q["id"])] = earlier
            continue
        console.say(f"\nJoin Multiple tool [{q['id']}] (container: {q['container']}) names its fields Input_{q['slots'][0]}_... to Input_{q['slots'][-1]}_..., but the workflow does not say")
        console.say("   which tool feeds which input, and the field names do not settle it.")
        left = [f"[{t['id']}] {t['name']}" for t in q["tools"]]
        picked = {}
        for slot in q["slots"]:
            n = console.choose(f"   Which tool feeds input {slot}?", left, 1)
            picked[slot] = left.pop(n - 1)
        joins[str(q["id"])] = {slot: int(label[1:label.index("]")]) for slot, label in picked.items()}
        if cache is not None:
            cache.put("join", q["id"], joins[str(q["id"])])
    if joins:
        plan.decisions["_join_inputs"] = joins

    # 5. embedded Python tools
    for tool, info in py.items():
        earlier = remembered("python", tool)
        if earlier is not None:
            console.say(f"\nEmbedded Python tool [{tool}]: {earlier.get('mode') or 'hand-off'} (your answer from an earlier run).")
            plan.port[tool] = earlier.get("mode")
            if earlier.get("describe"):
                plan.describe[tool] = earlier["describe"]
            continue
        console.say("")
        console.say(f"The workflow has an embedded Python (pandas) script - tool [{tool}], {len(info.cells)} cells, {len(info.needed)} of them used, {len(info.inputs)} input(s), {len(info.outputs)} output(s).")
        n = console.choose("   What should I do with it?", ["Keep the pandas code as it is in a util file (utils/python_tool_%s.py) and call it from the notebook - the same logic, it runs on the driver so the data must fit in memory" % tool,
                                                            "Port it to PySpark with a Databricks model - scales, but the model's code must be reviewed; it asks you whenever it would have to guess",
                                                            "Leave it as a hand-off"], 1)
        plan.port[tool] = ("keep", "port", None)[n - 1]
        if n == 2:
            plan.describe[tool] = console.ask("   In a sentence or two: what does this script do? (Enter = skip)", "")
        if cache is not None:
            cache.put("python", tool, {"mode": plan.port[tool], "describe": plan.describe.get(tool, "")})

    # 6. other tools without a converter
    left_to_model = []
    for u in inventory:
        if u.get("decision") == "order" or str(u.get("plugin", "")).lower().startswith("macro") or "output tool" in u.get("reason", "") or u["status"].startswith("waits"):
            continue
        if int(u["id"]) in py or int(u["id"]) in {q["id"] for q in getattr(analysis, "join_questions", []) or []}:
            continue                                            # a Python tool has its own question; a Join Multiple was asked above
        recipe = TC.explain(u.get("plugin"), u.get("name")) or ""
        if recipe and not ask_all:                              # the skills cover the tool: the model gets its settings, slim JSON and this recipe - nothing to ask
            left_to_model.append({"id": u["id"], "name": u.get("name", ""), "container": u.get("container", "")})
            continue
        earlier = remembered("note", u["id"])
        if earlier is not None:
            if earlier:
                plan.tool_notes[u["id"]] = earlier
            continue
        console.say(f"\nTool {_tool_label(u)} has no converter: {u['reason']}")
        if recipe:
            console.say(f"   What I know: {recipe[:400]}")
        what = console.ask("   What should this tool do in this workflow? (plain English; Enter = the model works from the tool's settings and the Alteryx help)", "")
        if what:
            plan.tool_notes[u["id"]] = what
        if cache is not None:
            cache.put("note", u["id"], what)
    if left_to_model:
        plan.decisions["_left_to_model"] = left_to_model
        console.say(f"\n{len(left_to_model)} tool(s) the skills describe but do not convert by rule ({', '.join('[%s] %s' % (x['id'], x['name']) for x in left_to_model[:4])}) "
                    "are left to the model, from their settings - no question needed.")
    outputs = [u for u in inventory if "output tool" in u.get("reason", "")]
    if outputs:
        console.say(f"\n{len(outputs)} output tool(s) export to a database or file ({', '.join('[%s] %s' % (o['id'], o['name']) for o in outputs[:4])}). "
                    "The notebooks write Delta tables instead; add the export where you need it.")

    # 7. the model
    wants_model = any(v == "port" for v in plan.port.values()) or any(not u["status"].startswith("waits") and u.get("decision") != "order" and "output tool" not in u.get("reason", "") and int(u["id"]) not in py and str(u["id"]) not in joins and str(u.get("plugin", "")).split(":", 1)[-1] not in sources for u in inventory)
    if wants_model and ask_all:
        if getattr(args, "model", None) or os.environ.get("MODEL"):
            plan.model = getattr(args, "model", None) or os.environ["MODEL"]
        else:
            console.say("\nA Databricks model can write the code for the tools above (the rules did not convert them).")
            plan.model = console.ask("Which model? (name of a Databricks serving endpoint, e.g. alteryx.default.test; Enter = none: those tools stay as hand-offs)", "")
    if cache is not None:
        settings = {"formats": list(plan.formats), "catalog": plan.catalog, "schema": plan.schema}
        if not (getattr(args, "model", None) or os.environ.get("MODEL")) and not ask_all:
            settings["model"] = plan.model
        if queries:
            settings.update(sql_dialect=plan.sql_dialect, source_catalog=plan.source_catalog, source_schema=plan.source_schema)
        cache.put("run", "settings", settings)
        cache.save()
    console.say("\nThanks. Converting now ...")
    return plan
