"""The questions the converter asks in the terminal, in plain English, before it converts.

Nothing is guessed: whatever the workflow does not say (which row a Unique tool keeps, what a missing macro does, what a tool without a converter must do, which code you want)
is asked here, once, and the answers go into the conversion - and, written in your own words, to the Databricks model that writes the code the rules cannot.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from . import pytool, sqlsrc


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


def gather(console: Console, analysis, handles: dict, args) -> Plan:
    """Ask everything the conversion needs. Returns the Plan; asks nothing that was already given on the command line."""
    import tool_coverage as TC
    plan = Plan()
    inventory = analysis.unsupported
    py = pytool.load(handles["slim_dir"], handles["wf"]) if handles.get("slim_dir") else {}
    convertible = sum(1 for c in analysis.chunks if c.strategy == "template")
    console.say("")
    console.say(f"I read {analysis.file_name}: {analysis.tool_count} tools, {len(analysis.chunks)} output tables.")
    console.say(f"The rules convert {convertible} of the {len(analysis.chunks)} tables as they are. {len(inventory)} tool(s) need something from you or a model.")
    console.say("I will ask a few questions. Press Enter to accept the answer in [brackets]. Nothing is converted until you have answered.")

    # 1. what code
    pick = getattr(args, "format", None)
    if pick:
        plan.formats = {"pyspark": ("pyspark",), "sql": ("sql",), "both": ("pyspark", "sql")}[pick]
    else:
        n = console.choose("\nWhich code do you need?", ["PySpark notebooks", "Spark SQL notebooks", "Both (PySpark and Spark SQL)"], 3)
        plan.formats = [("pyspark",), ("sql",), ("pyspark", "sql")][n - 1]
        if "sql" in plan.formats:
            console.say("   (Spark SQL needs the columns of the input tables. They are read from Unity Catalog when you write to the workspace, or you give them with --schemas.)")

    # 2. where the tables are
    if not (args.catalog or args.schema):
        plan.catalog = console.ask("\nWhich catalog holds the tables the notebooks read and write? (Enter = none: the notebooks use the catalog and schema that are current when they run)", "")
        if plan.catalog:
            plan.schema = console.ask("Which schema?", "")
    else:
        plan.catalog, plan.schema = args.catalog or "", args.schema or ""

    # 2b. the SQL inside the input tools
    queries = {k: v for k, v in analysis.inputs.items() if v.get("query")}
    if queries:
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
    for macro, tools in seen_macros.items():
        console.say("")
        console.say(f"The workflow uses the macro '{macro}' (tool {', '.join('[%s]' % t['id'] for t in tools[:5])}), and I do not have its definition.")
        console.say("   A macro hides part of the logic. Tell me what it does, in plain English, and I will pass that to the model that writes the code.")
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

    # 4. decisions the workflow does not state
    for u in inventory:
        if u.get("decision") == "order":
            console.say(f"\nTool {_tool_label(u)} keeps the first record of each group, but the workflow does not say which one is first.")
            console.say(f"   It comes after {u['after']} and feeds {u['before']}.")
            n = console.choose("   Which record should it keep?", ["Any record (the order does not matter)", "The first one after sorting by fields I will name"], 1)
            if n == 1:
                plan.decisions[str(u["id"])] = {"mode": "any"}
            else:
                fields = console.ask("   Sort by which fields? (comma separated, exact column names; add 'desc' after a name for descending, e.g. Date desc)", "")
                names, ascending = [], True
                for part in [p.strip() for p in fields.split(",") if p.strip()]:
                    bits = part.split()
                    names.append(bits[0])
                    ascending = ascending and not (len(bits) > 1 and bits[1].lower().startswith("desc"))
                plan.decisions[str(u["id"])] = {"mode": "order_by", "fields": names, "ascending": ascending} if names else {"mode": "any"}

    # 5. embedded Python tools
    for tool, info in py.items():
        console.say("")
        console.say(f"The workflow has an embedded Python (pandas) script - tool [{tool}], {len(info.cells)} cells, {len(info.needed)} of them used, {len(info.inputs)} input(s), {len(info.outputs)} output(s).")
        n = console.choose("   What should I do with it?", ["Keep the pandas code as it is in a util file (utils/python_tool_%s.py) and call it from the notebook - the same logic, it runs on the driver so the data must fit in memory" % tool,
                                                            "Port it to PySpark with a Databricks model - scales, but the model's code must be reviewed; it asks you whenever it would have to guess",
                                                            "Leave it as a hand-off"], 1)
        plan.port[tool] = ("keep", "port", None)[n - 1]
        if n == 2:
            plan.describe[tool] = console.ask("   In a sentence or two: what does this script do? (Enter = skip)", "")

    # 6. other tools without a converter
    for u in inventory:
        if u.get("decision") == "order" or str(u.get("plugin", "")).lower().startswith("macro") or "output tool" in u.get("reason", "") or u["status"].startswith("waits"):
            continue
        if int(u["id"]) in py:
            continue
        console.say(f"\nTool {_tool_label(u)} has no converter: {u['reason']}")
        recipe = TC.explain(u.get("plugin"), u.get("name")) or ""
        if recipe:
            console.say(f"   What I know: {recipe[:400]}")
        what = console.ask("   What should this tool do in this workflow? (plain English; Enter = the model works from the tool's settings and the Alteryx help)", "")
        if what:
            plan.tool_notes[u["id"]] = what
    outputs = [u for u in inventory if "output tool" in u.get("reason", "")]
    if outputs:
        console.say(f"\n{len(outputs)} output tool(s) export to a database or file ({', '.join('[%s] %s' % (o['id'], o['name']) for o in outputs[:4])}). "
                    "The notebooks write Delta tables instead; add the export where you need it.")

    # 7. the model
    wants_model = any(v == "port" for v in plan.port.values()) or any(not u["status"].startswith("waits") and u.get("decision") != "order" and "output tool" not in u.get("reason", "") and int(u["id"]) not in py for u in inventory)
    if wants_model:
        if getattr(args, "model", None):
            plan.model = args.model
        else:
            console.say("\nA Databricks model can write the code for the tools above (the rules did not convert them).")
            plan.model = console.ask("Which model? (name of a Databricks serving endpoint, e.g. alteryx.default.test; Enter = none: those tools stay as hand-offs)", "")
    console.say("\nThanks. Converting now ...")
    return plan
