"""Per-tool PySpark code emitters.  One emitter per logical tool kind; every emitter returns a ``NodeCode``
with the generated lines, a confidence status and the semantic notes behind it.

Status vocabulary (see references/07-validation-and-reconciliation.md):
  high_confidence   semantics understood and implemented; equivalence not yet demonstrated on workflow data
  requires_review   code is emitted but an ambiguity / semantic difference must be confirmed by a human
  blocked           configuration needed to generate correct code is missing -> the cell raises when run
  unsupported       no reliable PySpark equivalent is implemented -> the cell raises when run
  not_applicable    no data effect (Browse, comments); documented, not generated
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

import alteryx_datetime as DT
import alteryx_expr as X
import tool_coverage as TC
import pretty
import wfmodel as M

JOIN_TYPES = {"string", "number", "bool", "date", "datetime"}
NUMERIC_DTYPES = ("tinyint", "smallint", "int", "bigint", "float", "double")         # Spark dtypes of the Alteryx numeric types (Byte .. Double, plus decimal(p,s))
SEVERITY = {"not_applicable": 0, "high_confidence": 1, "requires_review": 2, "blocked": 3, "unsupported": 4}

CATEGORY = {"v_wstring": "string", "v_string": "string", "string": "string", "wstring": "string", "time": "string",
            "bool": "bool", "byte": "number", "int16": "number", "int32": "number", "int64": "number",
            "float": "number", "double": "number", "fixeddecimal": "number", "date": "date", "datetime": "datetime"}

SUMMARIZE_REVIEW = {
    "median": "Median: Spark interpolates linearly like Alteryx for even counts - confirm on data",
    "stddev": "StdDev: sample (n-1) implemented; confirm Alteryx uses sample not population deviation",
    "var": "Var: sample (n-1) implemented; confirm against Alteryx",
    "first": "First: depends on incoming record order (not guaranteed after shuffles)",
    "last": "Last: depends on incoming record order (not guaranteed after shuffles)",
    "concat": "Concat: element order is non-deterministic in Spark; Alteryx concatenates in record order",
    "percentile": "Percentile: interpolation method unverified",
}
SUMMARIZE_NOTES = {
    "count": "Count implemented as count of all records in the group (NULLs included)",
    "countdistinct": "CountDistinct ignores NULLs in Spark; confirm Alteryx treatment of NULL",
    "sum": "Sum of an all-NULL group is NULL in Spark; confirm Alteryx (0 vs NULL)",
}


@dataclass
class NodeCode:
    nid: int
    status: str = "high_confidence"
    lines: list = field(default_factory=list)
    notes: list = field(default_factory=list)
    reviews: list = field(default_factory=list)
    outputs: dict = field(default_factory=dict)     # anchor -> variable name
    orig: list = field(default_factory=list)        # original Alteryx expressions, shown only for tools needing review

    def need(self, status, msg):
        if SEVERITY[status] > SEVERITY[self.status]:
            self.status = status
        target = self.reviews if status != "high_confidence" else self.notes
        if msg not in target:
            target.append(msg)


def pylit(v):
    return repr(v)


def one_line(text, n=220):
    t = re.sub(r"\s+", " ", str(text or "")).strip()
    return t if len(t) <= n else t[:n] + " ..."


# ----------------------------------------------------------------------------- plain-PySpark snippets (no helper module)
col = X.col_code


def qcol(alias, name):
    """A column of an aliased DataFrame: F.col('l.Name'), with backticks around names that contain dots."""
    n = name.replace("`", "``")
    return f"F.col({alias + '.' + (f'`{n}`' if '.' in n else n)!r})"


DYNAMIC_COL = "F.col(f'`{c}`')"            # a column taken from df.columns at run time (backticks protect dots in names)
SPARK_AGG = {"sum": "F.sum({c})", "avg": "F.avg({c})", "min": "F.min({c})", "max": "F.max({c})", "count": "F.count(F.lit(1))",
             "countnonnull": "F.count({c})", "countdistinct": "F.countDistinct({c})", "median": "F.median({c})",
             "stddev": "F.stddev_samp({c})", "var": "F.var_samp({c})", "first": "F.first({c})", "last": "F.last({c})"}
SPARK_PIVOT = {"sum": "F.sum", "avg": "F.avg", "min": "F.min", "max": "F.max", "count": "F.count", "countdistinct": "F.countDistinct",
               "first": "F.first", "last": "F.last"}


def sort_args(fields):
    """Sort-field dicts -> 'F.col(..).asc_nulls_first(), ...' (Alteryx: NULLs first when ascending, last when descending)."""
    out = []
    for f in fields:
        c = col(f["field_name"])
        nf = f.get("nulls_first")
        if f.get("ascending", True):
            out.append(f"{c}.asc_nulls_first()" if nf is None or nf else f"{c}.asc_nulls_last()")
        else:
            out.append(f"{c}.desc_nulls_last()" if nf is None or not nf else f"{c}.desc_nulls_first()")
    return ", ".join(out)


def window_code(groups, order, frame=False):
    """Window.partitionBy(groups).orderBy(order)[.rowsBetween(..)] ; order is the argument text of orderBy."""
    base = f"Window.partitionBy({', '.join(col(g) for g in groups)})" if groups else "Window"
    code = f"{base}.orderBy({order})"
    return code + ".rowsBetween(Window.unboundedPreceding, Window.currentRow)" if frame else code


def lower_set(names):
    items = sorted({str(n).lower() for n in names})
    return "{" + ", ".join(repr(n) for n in items) + "}" if items else "set()"


def set_stmt(name, names, width=96):
    """`name = {...}` holding the lower-cased names, wrapped over several lines when long."""
    items = [repr(n) for n in sorted({str(n).lower() for n in names})]
    one = f"{name} = " + ("{" + ", ".join(items) + "}" if items else "set()")
    if len(one) <= width:
        return one
    lines, cur = [], ""
    for it in items:
        piece = it + ","
        if cur and len(cur) + 1 + len(piece) > width - 4:
            lines.append(cur)
            cur = piece
        else:
            cur = f"{cur} {piece}" if cur else piece
    lines.append(cur)
    return f"{name} = {{\n" + "".join(f"    {ln}\n" for ln in lines) + "}"


def skip_expr(r, name, names):
    """The set of names to leave out, as an expression: written inline when short, else as a named set before the statement."""
    items = {str(n).lower() for n in names}
    literal = lower_set(items)
    if len(literal) <= 50:
        return literal
    r.lines.append(set_stmt(name, items))
    return name



def wrapped(items, width=96, indent=4):
    """Comma-separated items packed onto lines of at most `width` characters (each line indented)."""
    lines, cur = [], ""
    for it in items:
        piece = it + ","
        if cur and indent + len(cur) + 1 + len(piece) > width:
            lines.append(cur)
            cur = piece
        else:
            cur = f"{cur} {piece}" if cur else piece
    lines.append(cur)
    return "".join(" " * indent + ln + "\n" for ln in lines)


def pair_list(pairs):
    """[(name, output), ...] as a list literal, packed onto lines."""
    return "[\n" + wrapped([f"({n!r}, {o!r})" for n, o in pairs]) + "]"


class Emitter:
    def __init__(self, wf, plan, naming=None):
        self.wf, self.plan = wf, plan
        self.types = {}       # stream -> {lower field: category}
        self.order = {}       # stream -> list of sort-field dicts (explicit order) or None
        self.results = {}     # nid -> NodeCode
        self.boundary = {}    # streams read from tables in the unit being emitted -> table key (also the variable name)
        self.names = {}       # stream -> readable variable name, assigned per unit
        self.options = dict(naming or {})   # project settings from the user's answers (text matching, ...)
        self.alias = {}       # stream -> variable it simply forwards (omitted no-op tools)
        self._extra_skip = {}  # fields a Join side leaves out on top of its configuration (the right input's join keys)

    # ------------------------------------------------------------------ naming helpers
    PREFIX = {"read": "src", "literal": "ref", "filter": "flt", "formula": "calc", "multifield": "mcalc",
              "multirow": "win", "select": "sel", "sort": "srt", "unique": "uniq", "join": "jn", "joinmulti": "jnm",
              "union": "un", "append": "app", "summarize": "agg", "crosstab": "xtab", "transpose": "trn",
              "recordid": "rid", "runningtotal": "rt", "text2cols": "split", "dynrename": "ren", "datetime": "dt",
              "spatial": "geo", "predictive": "ml", "unsupported": "mac", "cleanse": "cln"}

    def var(self, stream):
        """Variable name for a stream: the table name when read from a table in this unit, else a readable step name."""
        if stream in self.boundary:
            return self.boundary[stream]
        if stream in self.alias:
            return self.alias[stream]
        if stream in self.names:
            return self.names[stream]
        n, a = stream
        base = f"{self.PREFIX.get(self.wf.kind(n), 'df')}_{n}"
        return base if a == "Output" else f"{base}_{M.snake(a)}"

    RESERVED = {"spark", "F", "T", "Window", "StringType", "None", "True", "False", "import", "from", "class", "def", "return", "in",
                "is", "not", "and", "or", "if", "else", "for", "while", "with", "as", "lambda", "global", "pass"}

    def set_unit(self, unit, flow=False):
        """Declare which streams the next emitted nodes read from tables and give each output a readable, unique name.
        flow=True: one notebook for the whole workflow - only the Bronze tables are read, everything else is a variable."""
        if flow and unit:
            self.boundary = {st: self.plan.bronze_of[st[0]] for st in unit.inputs if st[0] in self.plan.bronze_of}
        else:
            self.boundary = ({st: self.plan.cut_streams.get(st) or self.plan.bronze_of.get(st[0]) for st in unit.inputs}
                             if unit else {})
        if not unit:
            return
        import builtins
        used = set(self.boundary.values()) | self.RESERVED | set(dir(builtins))     # never shadow a Python builtin
        for nid in unit.nodes:
            if self.wf.kind(nid) == "read":
                continue
            for a in self.consumed_anchors(nid):
                base = M.step_name(self.wf, self.plan.bronze_of, nid, a)
                name, i = base, 2
                while name in used:
                    name, i = f"{base}_{i}", i + 1
                used.add(name)
                self.names[(nid, a)] = name

    def in_streams(self, nid):
        out = {}
        for e in self.wf.producers(nid):
            out.setdefault(e.dst_anchor, []).append((e.src, e.src_anchor))
        return out

    def first_in(self, ins, *anchors):
        for a in anchors or ("Input",):
            if ins.get(a):
                return ins[a][0]
        for v in ins.values():
            return v[0]
        return None

    def out_var(self, nid, anchor="Output"):
        return self.var((nid, anchor))

    def consumed_anchors(self, nid):
        return sorted({a for (n, a) in self.wf.outputs if n == nid}) or [M.main_anchor(self.wf, nid)]

    # ------------------------------------------------------------------ type / order propagation
    def t_in(self, stream):
        types = dict(self.types.get(stream, {})) if stream else {}
        hints = self.options.get("field_types") or {}
        for name, kind in types.items():
            if kind == "unknown" and hints.get(name):
                types[name] = hints[name]
        return types

    def need_types(self, node, names):
        """Fields whose type the workflow does not state and that a choice depends on: the app asks the user for them (answers.field_types)."""
        have = node.setdefault("_types_needed", [])
        have += [n for n in names if n not in have]

    def set_out(self, nid, anchors, types=None, order=None):
        for a in anchors:
            self.types[(nid, a)] = dict(types or {})
            self.order[(nid, a)] = order

    # ------------------------------------------------------------------ dispatch
    def emit(self, nid):
        wf = self.wf
        node = wf.nodes[nid]
        kind = wf.kind(nid)
        r = NodeCode(nid)
        conf = node.get("conversion_confidence")
        for ck in M.config_issues(wf, nid):
            r.need("blocked", ck)
        if conf is not None and conf < 1 and kind not in ("spatial", "predictive", "unsupported"):
            r.notes.append(f"upstream converter confidence {conf}")
        for cn in node.get("conversion_notes") or []:
            r.notes.append(f"converter note: {cn}")
        if node.get("_override_keys"):
            r.need("requires_review", f"configuration supplied/overridden by a human ({', '.join(node['_override_keys'])}); not verified")
        fn = getattr(self, "e_" + kind, None)
        ins = self.in_streams(nid)
        anchors = self.consumed_anchors(nid)
        if kind not in ("read", "literal", "comment", "browse") and not ins:
            r.need("blocked", "tool has no input connection in the workflow")
        pre_blocked = r.status == "blocked"
        if fn is None:
            r.need("unsupported", TC.explain(node.get("original_tool_type"), node.get("original_plugin_name")) or f"no emitter for tool kind {kind!r} ({node['node_type']})")
            self._stub(r, node, anchors, "No PySpark equivalent implemented for this tool type")
        elif pre_blocked and kind not in ("unsupported", "spatial", "predictive", "dynrename", "datetime", "joinmulti"):
            self._blocked_guard(r, node, anchors, ins)     # lost configuration: never guess, raise at run time
        else:
            fn(nid, node, ins, anchors, r)
        if not (node.get("ported_python") or node.get("kept_python")):      # ported or kept code keeps the layout it was written with
            r.lines = [self._wrap(ln) for ln in r.lines]
        for a in anchors:
            r.outputs.setdefault(a, self.var((nid, a)))
        self.results[nid] = r
        return r

    @staticmethod
    def _wrap(line):
        """Break an over-long single-line assignment at its call arguments (always inside brackets)."""
        if (len(line) <= 100 or line.startswith("#") or "\n" in line or " = " not in line or line.startswith("raise ")
                or re.search(r"\bfor \w+(, \w+)* in\b", line)):
            return line
        lhs, rhs = line.split(" = ", 1)
        return f"{lhs} = {pretty.fmt(rhs, 0)}"

    def _stub(self, r, node, anchors, why):
        nid = r.nid
        tool = node.get("original_tool_type") or node["node_type"]
        reason = one_line((r.reviews[0] if r.reviews else why), 90)
        r.lines.append(f"raise NotImplementedError({f'[tool {nid}: {tool}] {reason}'!r})")

    def _blocked_guard(self, r, node, anchors, ins, types_from=None):
        """When config is missing, emit a raising stub and keep downstream code well-formed."""
        self._stub(r, node, anchors, "Configuration required to generate correct code is missing")
        st = self.first_in(ins)
        self.set_out(r.nid, anchors, self.t_in(st), None)

    # ------------------------------------------------------------------ expression helper
    def tr(self, r, expr, types, df_var, current=None, row_ref=None, what="expression"):
        try:
            res = X.translate(expr, field_types=types, current_field=current, row_ref=row_ref, df_var=df_var,
                              ci_default=self.options.get("text_match_case_insensitive", True),
                              ansi=bool(self.options.get("ansi")))
        except X.TranslationError as exc:
            r.need("blocked", f"{what} cannot be translated: {exc} :: {one_line(expr, 160)}")
            return None
        for m in res.notes:
            if m not in r.notes:
                r.notes.append(m)
        for m in res.reviews:
            r.need("requires_review", m)
        if res.untyped:
            self.need_types(self.wf.nodes[r.nid], res.untyped)
        return res

    # ------------------------------------------------------------------ sources
    def e_read(self, nid, node, ins, anchors, r):
        if node.get("converted_from"):
            r.need("requires_review", f"input tool '{node.get('original_tool_type')}' has no converter: its result "
                                      "is expected in an existing Bronze table - confirm it holds this tool's query result")
        elif node.get("source_type") == "database":
            r.need("requires_review", f"database alias {node.get('connection_string')!r} must be mapped to a JDBC "
                                      "connection or an existing Bronze table; T-SQL query is passed through unchanged")
        elif node.get("file_format") in (None, ""):
            r.need("requires_review", "file format not captured")
        types = {f["name"].lower(): CATEGORY.get((f.get("type") or "").lower(), "unknown")
                 for f in node.get("field_info") or [] if f.get("name")}
        self.set_out(nid, anchors, types, None)

    def e_literal(self, nid, node, ins, anchors, r):
        names, types, rows = node.get("field_names") or [], node.get("field_types") or [], node.get("data_rows") or []
        v = self.var((nid, "Output"))
        if not names:
            r.need("blocked", "TextInput has no field names")
            return self._blocked_guard(r, node, anchors, ins)
        ddl = ", ".join(f"`{n}` string" for n in names)
        r.lines.append(f"{v} = spark.createDataFrame([")
        for row in rows:
            r.lines.append(f"    {tuple(row)!r},")
        r.lines.append(f"], {ddl!r})")
        casts = []
        for n, t in zip(names, types or ["V_WString"] * len(names)):
            if CATEGORY.get((t or "").lower(), "x") == "string":
                casts.append(col(n))
            else:
                casts.append(f"{self.cast_to(r, col(n), t)}.alias({n!r})")
        r.lines.append(f"{v} = {v}.select({', '.join(casts)})")
        self.set_out(nid, anchors, {n.lower(): CATEGORY.get((t or "").lower(), "unknown") for n, t in zip(names, types)},
                     None)
        r.notes.append("Static reference data embedded in the notebook (small); consider a Silver reference table")

    # ------------------------------------------------------------------ row-level tools
    def e_filter(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        src, types = self.var(st), self.t_in(st)
        res = self.tr(r, node.get("expression"), types, src, what="filter expression")
        if res is None:
            return self._blocked_guard(r, node, anchors, ins)
        r.orig.append(node.get("expression"))
        t, f = self.var((nid, "True")), self.var((nid, "False"))
        cond = pretty.fmt(res.code, 0)
        if "False" in anchors:                              # NULL is not lost: it goes to the False output
            r.lines.append(f"{t}_condition = F.coalesce({cond}, F.lit(False))")
            r.lines.append(f"{t} = {src}.filter({t}_condition)")
            r.lines.append(f"{f} = {src}.filter(~{t}_condition)")
            r.notes.append("NULL condition routes the record to the False output (as Alteryx)")
        else:
            if "\n" in cond:                                # long condition: name it so the call stays one short line
                r.lines.append(f"{t}_condition = {cond}")
                cond = f"{t}_condition"
            r.lines.append(f"{t} = {src}.filter({cond})")
            r.notes.append("A NULL condition is not kept (as Alteryx)")
        self.set_out(nid, ["True", "False"], types, self.order.get(st))
        r.outputs = {a: self.var((nid, a)) for a in anchors} or {"True": t}

    def cast_to(self, r, code, alteryx_type, size=None):
        """code converted to the Alteryx type; a failed conversion gives NULL. An unmapped type blocks the tool."""
        try:
            return X.cast_code(code, X.spark_type(alteryx_type, size), bool(self.options.get("ansi")))
        except X.TranslationError as exc:
            r.need("blocked", str(exc))
            return code

    def _cast_for(self, r, code, data_type, size=None, expr_type=None):
        """Cast to the declared Alteryx type; skipped when the expression already has it, plain cast when it cannot fail."""
        if not data_type:
            return code
        cat = CATEGORY.get(data_type.lower())
        if cat is None:
            r.need("requires_review", f"unmapped output type {data_type!r}: no cast applied")
            return code
        if cat == "string":
            return code if expr_type == "string" else f"{code}.cast('string')"
        spark = {"double": "double", "float": "float", "int32": "int", "int64": "bigint", "int16": "smallint",
                 "byte": "smallint", "bool": "boolean"}.get(data_type.lower())
        if spark and expr_type in ("number", "bool"):          # numeric -> numeric/boolean casts cannot fail on text
            return f"{code}.cast('{spark}')"
        return self.cast_to(r, code, data_type, size if data_type.lower() == "fixeddecimal" else None)

    def e_formula(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        types = self.t_in(st)
        src, v = self.var(st), self.var((nid, "Output"))
        if node.get("passthrough") and not node.get("formulas"):
            self.alias[(nid, "Output")] = src                      # the workflow XML confirms: no formulas configured
            r.notes.append("Formula tool has no formulas in the workflow: passes data through unchanged")
            self.set_out(nid, ["Output"], types, self.order.get(st))
            return
        batches, cur, cur_new, frame = [], [], set(), src   # independent formulas share one withColumns()
        for fm in node.get("formulas") or []:
            what = f"formula for [{fm.get('output_field')}]"
            res = self.tr(r, fm.get("expression"), types, frame, what=what)
            if res is None:
                return self._blocked_guard(r, node, anchors, ins)
            r.orig.append(f"[{fm.get('output_field')}] = {fm.get('expression')}")
            if {f.lower() for f in res.fields} & cur_new:      # reads an earlier output: evaluate on the updated frame
                batches.append(cur)
                cur, cur_new, frame = [], set(), v
                res = self.tr(r, fm.get("expression"), types, frame, what=what)
            code = self._cast_for(r, res.code, fm.get("data_type"), fm.get("size"), res.type)
            cur.append((fm["output_field"], code))
            cur_new.add(fm["output_field"].lower())
            types[fm["output_field"].lower()] = CATEGORY.get((fm.get("data_type") or "").lower(), res.type)
        if cur:
            batches.append(cur)
        prev = src
        for i, batch in enumerate(batches):
            if len(batch) == 1:
                name, code = batch[0]
                body = f"{prev}.withColumn({name!r}, {pretty.fmt(code, 4)})"
            else:
                items = ",\n".join(f"    {n!r}: {pretty.fmt(c, 4)}" for n, c in batch)
                body = f"{prev}.withColumns({{\n{items},\n}})"
            r.lines.append(f"{v} = {body}")
            prev = v
        self.set_out(nid, ["Output"], types, self.order.get(st))

    def e_multifield(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        types = self.t_in(st)
        src, v = self.var(st), self.var((nid, "Output"))
        if node.get("copy_output"):
            r.need("blocked", "Copy-output mode (new columns) is not implemented for Multi-Field Formula")
            return self._blocked_guard(r, node, anchors, ins)
        res = self.tr(r, node.get("expression"), types, src, current="__CURRENT__", what="multi-field expression")
        if res is None:
            return self._blocked_guard(r, node, anchors, ins)
        body = res.code.replace("F.col('__CURRENT__')", "F.col(_c)")
        out_t = node.get("output_type")
        if out_t and out_t.lower() == "numeric":
            r.need("requires_review", "output_type 'Numeric' mapped to Double (Alteryx numeric family not specified further)")
            out_t = "Double"
        if out_t:
            body = self._cast_for(r, body, out_t, expr_type=res.type)
        r.orig.append(node.get("expression"))
        fields = list(node.get("fields") or [])
        r.lines.append(f"{v}_columns = {fields!r}")
        r.lines.append(f"{v} = {src}.withColumns({{_c: {pretty.fmt(body, 8)} for _c in {v}_columns}})")
        r.notes.append("Each field is evaluated from the incoming record values (not sequentially updated)")
        self.set_out(nid, ["Output"], types, self.order.get(st))

    def order_code(self, st, node, r, what, any_ok=False):
        """Python source for the record-order expression list. Without an order key the tool is BLOCKED (never guessed),
        except Unique when the user accepted 'any row'."""
        explicit = node.get("order_by") or self.order.get(st)
        if explicit:
            return sort_args(explicit)
        if any_ok and node.get("order_any"):
            r.need("requires_review", f"{what}: user accepted that any matching row may be kept (no order key)")
            return None
        r.need("blocked", f"{what} depends on record order but no sort/order key is defined upstream - "
                          "provide the order field(s)" + (" or confirm that any row is acceptable" if any_ok else ""))
        return None

    def e_multirow(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        types = self.t_in(st)
        src, v = self.var(st), self.var((nid, "Output"))
        out_field = node.get("output_field")
        refs_own = re.search(r"\[Row\s*[+-]\s*\d+\s*:\s*" + re.escape(str(out_field)) + r"\s*\]", node.get("expression") or "")
        if refs_own:
            r.need("unsupported", "Expression references its own previous output (recursive Multi-Row Formula); "
                                  "no window-function equivalent")
            return self._blocked_guard(r, node, anchors, ins)
        if node.get("other_rows", "Empty") not in ("Empty", ""):
            r.need("requires_review", f"Multi-Row Formula 'other rows' = {node.get('other_rows')!r}: missing neighbours are NULL here")
        r.orig.append(node.get("expression"))
        groups = node.get("group_fields") or []
        oc = self.order_code(st, node, r, "Multi-Row Formula")
        if oc is None:
            return self._blocked_guard(r, node, anchors, ins)
        r.lines.append(f"{v}_window = {window_code(groups, oc)}")
        if not groups:
            r.notes.append("No group fields: window spans the whole dataset (single partition); size accordingly")

        def row_ref(name, off):
            fn = "F.lag" if off < 0 else "F.lead"
            return f"{fn}({col(name)}, {abs(off)}).over({v}_window)"
        res = self.tr(r, node.get("expression"), types, src, row_ref=row_ref, what="multi-row expression")
        if res is None:
            return self._blocked_guard(r, node, anchors, ins)
        code = self._cast_for(r, res.code, node.get("output_type"), node.get("output_size"), res.type) \
            if node.get("output_type") and not node.get("update_field") else res.code
        r.lines.append(f"{v} = {src}.withColumn({out_field!r}, {pretty.fmt(code, 4)})")
        types[str(out_field).lower()] = res.type
        self.set_out(nid, ["Output"], types, self.order.get(st))

    @staticmethod
    def select_config(ops):
        """Select configuration -> (listed field names in output order, removed names, {name: new name}, {name: (type, size)}, unknown kept)."""
        unknown, order, drop, rename, retype = True, [], [], {}, {}
        for op in ops or []:
            name = op["field_name"]
            selected = op.get("selected")
            if selected is None:
                selected = op.get("action") != "deselect"
            if name == "*Unknown":
                unknown = bool(selected)
            elif not selected:
                drop.append(name)
            else:
                order.append(name)
                if op.get("rename_to") and op["rename_to"] != name:
                    rename[name] = op["rename_to"]
                if op.get("new_type"):
                    size = op.get("new_size")
                    keep_size = size and op["new_type"].lower() in ("v_wstring", "v_string", "string", "wstring", "fixeddecimal")
                    retype[name] = (op["new_type"], size if keep_size else None)
        return order, drop, rename, retype, unknown

    def retyped(self, r, code, type_size):
        """A Select/Join retype: converted to the Alteryx type; sized text is cut to its size."""
        alteryx_type, size = type_size
        try:
            spark = X.spark_type(alteryx_type, size)
        except X.TranslationError as exc:
            r.need("blocked", str(exc))
            return code
        if spark == "string":
            out = f"{code}.cast('string')"
            return f"{out}.substr(1, {int(size)})" if size and 0 < int(size) < 1_000_000 else out
        return self.cast_to(r, code, alteryx_type, size)

    def e_select(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        types = self.t_in(st)
        ops = node.get("field_operations") or []
        v = self.var((nid, "Output"))
        order, drop, rename, retype, unknown = self.select_config(ops)
        if node.get("select_all_unknown") is None and not any(o["field_name"] == "*Unknown" for o in ops):
            r.notes.append("select_all_unknown not captured: unknown fields kept")
        for op in ops:
            nm = op["field_name"].lower()
            if nm == "*unknown":
                continue
            if op.get("selected") is False or op.get("action") == "deselect":
                types.pop(nm, None)
            else:
                t = types.pop(nm, "unknown")
                if op.get("new_type"):
                    t = CATEGORY.get(op["new_type"].lower(), t)
                types[(op.get("rename_to") or op["field_name"]).lower()] = t
        self.set_out(nid, ["Output"], types, self.order.get(st))
        if not (drop or rename or retype) and unknown and (nid, "Output") not in self.plan.cut_streams:
            self.alias[(nid, "Output")] = self.var(st)         # selects every field unchanged: nothing to run
            r.notes.append("Select keeps every field unchanged: omitted")
            return
        src = self.var(st)
        if unknown and not order:                              # only removals: no need to list the other fields
            r.lines.append(f"{v} = {src}.drop({', '.join(repr(d) for d in drop)})" if drop else f"{v} = {src}")
            return
        items = []
        for name in order:
            c = col(name)
            if name in retype:
                c = self.retyped(r, c, retype[name])
            target = rename.get(name) or (name if name in retype else None)      # a retyped column keeps its name explicitly (substr() would rename it)
            items.append(f"{c}.alias({target!r})" if target else c)
        if unknown:                                            # fields the configuration does not name follow the listed ones
            items.append(f"*[{DYNAMIC_COL} for c in {src}.columns if c.lower() not in {skip_expr(r, v + '_named', order + drop)}]")
        body = ",\n".join(f"    {i}" for i in items)
        r.lines.append(f"{v} = {src}.select(\n{body},\n)")

    def join_side(self, r, ops, prefix, frame, tag, side):
        """Emit `<tag> = [(source column, output name), ...]` for one side of a Join: the configured fields in order, then (unless
        '*Unknown' is off) every other field of the input. Returns {output name: (type, size)} for the fields the configuration retypes."""
        order, drop, rename, retype, unknown = self.select_config(ops)
        types_by_out = {rename.get(n, n): retype[n] for n in order if n in retype}
        cols_var = f"{side}_cols"
        parts = []
        if order:
            names = [n for n in order]
            fallback = any(n.startswith(prefix) for n in names)       # configs name a collided field 'Left_X' / 'Right_X' for input field X
            keys = f"(n, n.removeprefix({prefix!r}))" if fallback else "(n,)"
            r.lines.append(f"{cols_var} = {frame}.columns")
            r.lines.append(f"{tag}_fields = " + pair_list([(n, rename.get(n, n)) for n in names]))
            parts.append(f"[\n    (next(c for k in {keys} for c in {cols_var} if c.lower() == k.lower()), o)\n    for n, o in {tag}_fields\n]")
        if unknown:
            seen = set(order) | set(drop)
            seen |= {n[len(prefix):] for n in seen if n.startswith(prefix)}
            seen |= set(self._extra_skip.get(side, ()))
            if not order:
                r.lines.append(f"{cols_var} = {frame}.columns")
            cond = f" if c.lower() not in {skip_expr(r, tag + '_skip', seen)}" if seen else ""
            parts.append(f"[(c, c) for c in {cols_var}{cond}]")
        r.lines.append(f"{tag} = " + (" + ".join(parts) if parts else "[]"))
        return types_by_out

    def e_sort(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        fields = [{k: v for k, v in {"field_name": f["field_name"], "ascending": f.get("ascending", True),
                                      "nulls_first": f.get("nulls_first")}.items() if v is not None}
                  for f in node.get("sort_fields") or []]
        r.need("requires_review", "String sort order: Spark sorts by UTF-8 bytes (upper-case before lower-case, "
                                  "locale-unaware); Alteryx uses a locale-aware order - verify if string order matters")
        self.set_out(nid, ["Output"], self.t_in(st), fields)
        if (nid, "Output") in self.plan.cut_streams:          # final table: keep the sort
            r.lines.append(f"{self.var((nid, 'Output'))} = {self.var(st)}.orderBy({sort_args(fields)})")
        else:                                                 # order is applied explicitly by whoever needs it
            self.alias[(nid, "Output")] = self.var(st)
            r.notes.append("physical sort omitted: consumers that depend on order sort explicitly")

    def e_unique(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        keys = node.get("key_fields") or []
        dup_anchors = [a for a in anchors if a != "Unique"]
        oc = self.order_code(st, node, r, "Unique (keep-first)", any_ok=True)
        u, src = self.var((nid, "Unique")), self.var(st)
        if not oc and not dup_anchors:
            r.lines.append(f"{u} = {src}.dropDuplicates({keys!r})")
        else:                                                 # first record per key = Unique, the rest = Duplicates
            r.lines.append(f"{u}_window = {window_code(keys, oc or 'F.monotonically_increasing_id()')}")
            r.lines.append(f"{u}_ranked = {src}.withColumn('__rn', F.row_number().over({u}_window))")
            r.lines.append(f"{u} = {u}_ranked.filter(F.col('__rn') == 1).drop('__rn')")
            for a in dup_anchors:
                r.lines.append(f"{self.var((nid, a))} = {u}_ranked.filter(F.col('__rn') > 1).drop('__rn')")
        self.set_out(nid, ["Unique"] + dup_anchors, self.t_in(st), self.order.get(st))
        r.outputs = {a: self.var((nid, a)) for a in anchors}

    # ------------------------------------------------------------------ multi-input tools
    def e_join(self, nid, node, ins, anchors, r):
        L, R = self.first_in(ins, "Left"), self.first_in(ins, "Right")
        if not ins.get("Left") or not ins.get("Right"):
            r.need("blocked", "Join is missing a Left or Right input connection")
            return self._blocked_guard(r, node, anchors, ins)
        if node.get("join_by_position"):
            r.need("unsupported", "Join by record position is not implemented (needs an explicit row order on both inputs)")
            return self._blocked_guard(r, node, anchors, ins)
        keys = [(k["left_field"], k["right_field"]) for k in node.get("join_keys") or []]
        if not keys:
            return self._blocked_guard(r, node, anchors, ins)
        want = [a for a in ("Join", "Left", "Right") if a in anchors] or ["Join"]
        lv, rv, base = self.var(L), self.var(R), self.var((nid, "Join"))
        null_safe = bool(self.options.get("null_safe_join"))
        on = " & ".join((f"({qcol('l', a)}.eqNullSafe({qcol('r', b)}))" if null_safe else f"({qcol('l', a)} == {qcol('r', b)})") for a, b in keys)
        r.lines.append(f"{base}_on = {pretty.fmt(on, 0)}")
        lt_, rt_ = self.t_in(L), self.t_in(R)               # the Alteryx help: string joins string, number joins number, date-time joins its exact type
        for a, b in keys:
            ta, tb = lt_.get(a.lower()), rt_.get(b.lower())
            if ta in JOIN_TYPES and tb in JOIN_TYPES and ta != tb:
                r.need("requires_review", f"Join keys {a} ({ta}) and {b} ({tb}) have different types: Alteryx only joins fields of the same type, "
                                          "Spark converts one side implicitly - confirm the intended types")
            if ta == "number" or tb == "number":
                r.notes.append("Join on a numeric key: the Alteryx help advises against Double/Float keys (rounding error)")
                break
        if "Join" in want:
            # the right input's join keys are not repeated unless its configuration keeps them (Alteryx default)
            ops_r = node.get("select_right") or []
            listed_right = {o["field_name"].lower() for o in ops_r if o.get("selected", True) is not False and o["field_name"] != "*Unknown"}
            key_drop = [b for _, b in keys if b.lower() not in listed_right]
            self._extra_skip = {"right": key_drop}
            left_types = self.join_side(r, node.get("select_left"), "Left_", lv, f"{base}_left", "left")
            right_types = self.join_side(r, ops_r, "Right_", rv, f"{base}_right", "right")
            r.lines.append(f"{base}_taken = {{o.lower() for _, o in {base}_left}}")
            r.lines.append(f"{base}_right = [\n    (s, 'Right_' + o if o.lower() in {base}_taken else o)\n    for s, o in {base}_right\n]")
            j = self.var((nid, "Join"))
            sel = (f"{j} = (\n    {lv}.alias('l')\n    .join({rv}.alias('r'), {base}_on, 'inner')\n    .select(\n"
                   f"        *[F.col(f'l.`{{s}}`').alias(o) for s, o in {base}_left],\n"
                   f"        *[F.col(f'r.`{{s}}`').alias(o) for s, o in {base}_right],\n    )\n)")
            r.lines.append(sel)
            casts = [f"{o!r}: {self.retyped(r, col(o), t)}" for o, t in left_types.items()]
            for o, t in right_types.items():                  # a right field that collided with a left one is named Right_<name>
                name = f"('Right_{o}' if {o.lower()!r} in {base}_taken else {o!r})"
                casts.append(f"{name}: {self.retyped(r, f'F.col({name})', t)}")
            if casts:
                r.need("requires_review", "Join Select retype: the conversion is applied to the joined output")
                r.lines.append(f"{j} = {j}.withColumns({{{', '.join(casts)}}})")
        if "Left" in want:
            r.lines.append(f"{self.var((nid, 'Left'))} = (\n    {lv}.alias('l')\n    .join({rv}.alias('r'), {base}_on, 'left_anti')\n)")
        if "Right" in want:
            r.lines.append(f"{self.var((nid, 'Right'))} = (\n    {rv}.alias('r')\n    .join({lv}.alias('l'), {base}_on, 'left_anti')\n)")
        if len(want) > 1 or want != ["Join"]:
            r.notes.append("Left/Right unmatched outputs implemented as anti-joins on the same keys")
        r.notes.append(("NULL keys match each other (null_safe_join is on)" if null_safe else "NULL keys never match (SQL equality; the Alteryx help does not say - "
                        "confirm, or set null_safe_join)") + "; keys compared case-sensitively; right-side name collisions prefixed 'Right_'")
        lt, rt = self.t_in(L), self.t_in(R)
        self.set_out(nid, ["Join"], {**lt, **rt}, None)
        self.set_out(nid, ["Left"], lt, None)
        self.set_out(nid, ["Right"], rt, None)

    def e_joinmulti(self, nid, node, ins, anchors, r):
        n_in = len(node.get("join_inputs") or {})
        r.need("unsupported", f"Join Multiple ({n_in} inputs): output field naming for colliding fields (Input_#k_) and the join "
                              "type are not verified - send a sample of the Alteryx output or confirm the rule")
        self._blocked_guard(r, node, anchors, ins)

    def e_union(self, nid, node, ins, anchors, r):
        srcs = [self.var(s) for s in ins.get("Input", [])]
        if len(srcs) < 1:
            r.need("blocked", "Union has no inputs")
            return self._blocked_guard(r, node, anchors, ins)
        mode = node.get("mode") or "auto"
        v = self.var((nid, "Output"))
        if mode in ("auto", "name", "by_name"):
            allow = "allowMissingColumns=True" if node.get("allow_missing", True) else ""
            link = [f".unionByName({s}{', ' + allow if allow else ''})" for s in srcs[1:]]
        elif mode in ("position", "by_position"):
            link = [f".union({s}.toDF(*{srcs[0]}.columns))" for s in srcs[1:]]
        else:
            link = []
            r.need("blocked", f"Union mode {mode!r} (manual field mapping) not captured")
        if not link:
            r.lines.append(f"{v} = {srcs[0]}")
        elif len(link) == 1:
            r.lines.append(f"{v} = {srcs[0]}{link[0]}")
        else:
            r.lines.append(f"{v} = (\n    {srcs[0]}\n" + "".join(f"    {x}\n" for x in link) + ")")
        r.notes.append("Input order = connection order in the file (the only evidence available)")
        r.notes.append("Differing column types across inputs are resolved by Spark's type coercion; verify")
        merged = {}
        for s in ins.get("Input", []):
            merged.update(self.t_in(s))
        self.set_out(nid, ["Output"], merged, None)

    def e_append(self, nid, node, ins, anchors, r):
        T, S = self.first_in(ins, "Targets"), self.first_in(ins, "Source")
        if not T or not S:
            r.need("blocked", "Append Fields is missing Targets or Source input")
            return self._blocked_guard(r, node, anchors, ins)
        r.lines.append(f"{self.var((nid, 'Output'))} = {self.var(T)}.crossJoin({self.var(S)})")
        r.notes.append("Cartesian product: every Target record gets the Source record(s); Spark broadcasts a small Source by itself. "
                       "Field names must not collide - rename in the Source Select first")
        self.set_out(nid, ["Output"], {**self.t_in(T), **self.t_in(S)}, self.order.get(T))

    # ------------------------------------------------------------------ grain-changing tools
    def e_summarize(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        types = self.t_in(st)
        group, aggs, out_t = [], [], {}
        for a in node.get("aggregations") or []:
            act = a["action"].lower()
            if act not in SUPPORTED_AGG:
                r.need("unsupported", f"Summarize action {a['action']!r} has no verified equivalent")
            if act in SUMMARIZE_REVIEW:
                r.need("requires_review", SUMMARIZE_REVIEW[act])
            if act in SUMMARIZE_NOTES:
                r.notes.append(SUMMARIZE_NOTES[act])
            field_, out = a["field_name"], a.get("output_field_name") or a["field_name"]
            if act == "groupby":
                group.append(col(field_) if out == field_ else f"{col(field_)}.alias({out!r})")
            elif act in SPARK_AGG or act in ("concat", "percentile"):
                if not a.get("output_field_name"):
                    out = "Count" if act == "count" else f"{a['action']}_{field_}"
                c = col(field_)
                if act == "concat":
                    code = f"F.concat_ws({(a.get('separator') or ',')!r}, F.collect_list({c}.cast('string')))"
                elif act == "percentile":
                    code = f"F.percentile({c}, {float(a['percentile_value']) / 100.0})"
                else:
                    code = SPARK_AGG[act].format(c=c)
                aggs.append(f"{code}.alias({out!r})")
            out_t[out.lower()] = (types.get(field_.lower(), "unknown") if act in ("groupby", "min", "max", "first", "last")
                                  else "string" if act == "concat" else "number")
        if r.status == "unsupported":
            return self._blocked_guard(r, node, anchors, ins)
        src, v = self.var(st), self.var((nid, "Output"))
        if not aggs:                                           # pure GroupBy = the distinct group values
            r.lines.append(f"{v} = {src}.select({', '.join(group)}).distinct()")
        else:
            body = ",\n".join(f"    {x}" for x in aggs)
            head = f"{src}.groupBy({', '.join(group)}).agg(" if group else f"{src}.agg("
            r.lines.append(f"{v} = {head}\n{body},\n)")
        r.notes.append("Alteryx returns groups sorted by the group fields; Spark output is unordered")
        self.set_out(nid, ["Output"], out_t, None)

    def e_crosstab(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        if len(node.get("methods") or []) > 1:
            r.need("unsupported", f"Cross Tab with several methods {node['methods']}: output naming not verified")
        agg = str(node.get("aggregation") or "Sum").lower()
        if r.status not in ("blocked", "unsupported") and agg not in SPARK_PIVOT:
            r.need("unsupported", f"Cross Tab aggregation {node.get('aggregation')!r} has no verified equivalent")
        if r.status in ("blocked", "unsupported"):
            return self._blocked_guard(r, node, anchors, ins)
        r.need("requires_review", "Cross Tab output column naming and pivot value list must be confirmed against Alteryx output")
        groups = ", ".join(col(g) for g in node["group_fields"])
        r.lines.append(f"{self.var((nid, 'Output'))} = {self.var(st)}.groupBy({groups}).pivot({node['header_field']!r})"
                       f".agg({SPARK_PIVOT[agg]}({col(node['value_field'])}))")
        r.notes.append("Pass an explicit list of the distinct header values to pivot() to skip the extra distinct job and fix the schema")
        self.set_out(nid, ["Output"], {}, None)

    def e_transpose(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        if r.status == "blocked":
            return self._blocked_guard(r, node, anchors, ins)
        keys = node.get("key_fields") or []
        data = list(node.get("data_fields") or [])
        v, src = self.var((nid, "Output")), self.var(st)
        names = repr(data)
        if node.get("data_unknown"):                           # every other field is transposed as well
            taken = set(keys) | set(data) | set(node.get("data_skip") or [])
            names += f" + [c for c in {src}.columns if c.lower() not in {skip_expr(r, v + '_skip', taken)}]"
        r.lines.append(f"{v}_fields = {names}")
        key_cols = ", ".join(col(k) for k in keys)
        pairs = f"F.struct(F.lit(c).alias('Name'), {DYNAMIC_COL}.cast('string').alias('Value'))"
        r.lines.append(f"{v} = {src}.select(\n    {key_cols + ', ' if key_cols else ''}"
                       f"F.explode(F.array(*[{pairs} for c in {v}_fields])).alias('kv'),\n"
                       f").select({key_cols + ', ' if key_cols else ''}F.col('kv.Name'), F.col('kv.Value'))")
        self.set_out(nid, ["Output"], {}, None)

    # ------------------------------------------------------------------ order-dependent / structural tools
    def e_recordid(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        oc = self.order_code(st, node, r, "Record ID")
        dtype = {"int32": "int", "int64": "bigint", "int16": "smallint"}.get((node.get("output_type") or "int64").lower(), "bigint")
        start = int(node.get("starting_value", 1))
        window = f"Window.orderBy({oc or 'F.monotonically_increasing_id()'})"
        field_ = node.get("output_field")
        r.lines.append(f"{self.var((nid, 'Output'))} = {self.var(st)}.select(\n"
                       f"    (F.row_number().over({window}) + F.lit({start - 1})).cast({dtype!r}).alias({field_!r}),\n    '*',\n)")
        r.notes.append("Global row_number() window = single-partition sort; expensive on large inputs")
        t = self.t_in(st)
        t[str(node.get("output_field")).lower()] = "number"
        self.set_out(nid, ["Output"], t, self.order.get(st))

    def e_runningtotal(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        if r.status == "blocked":
            return self._blocked_guard(r, node, anchors, ins)
        oc = self.order_code(st, node, r, "Running Total")
        v = self.var((nid, "Output"))
        r.lines.append(f"{v}_window = {window_code(node.get('group_fields') or [], oc or 'F.monotonically_increasing_id()', frame=True)}")
        items = ",\n".join(f"    {('RunTot_' + f)!r}: F.sum({col(f)}).over({v}_window)" for f in node["running_fields"])
        r.lines.append(f"{v} = {self.var(st)}.withColumns({{\n{items},\n}})")
        self.set_out(nid, ["Output"], self.t_in(st), self.order.get(st))

    def e_text2cols(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        if r.status == "blocked":
            return self._blocked_guard(r, node, anchors, ins)
        delim = node.get("delimiter") or ","
        if len(delim.encode().decode("unicode_escape")) > 1:
            r.need("requires_review", "Multi-character delimiter: each character treated as a separate delimiter (Alteryx rule assumed)")
        if node.get("split_to", "columns") == "columns" and not node.get("num_columns"):
            r.need("blocked", "Split-to-columns requires the number of columns")
            return self._blocked_guard(r, node, anchors, ins)
        one = delim.encode().decode("unicode_escape") if delim else ","
        pattern = re.escape(one) if len(one) == 1 else "[" + re.escape(one) + "]"
        field_, v, src = node["field_name"], self.var((nid, "Output")), self.var(st)
        parts = f"F.split({col(field_)}.cast('string'), {pattern!r})"
        if node.get("split_to", "columns") == "rows":
            r.lines.append(f"{v} = {src}.withColumn({field_!r}, F.explode({parts}))")
            if node.get("skip_empty"):
                r.lines.append(f"{v} = {v}.filter({col(field_)} != '')")
        else:
            root = node.get("output_root_name") or field_
            r.lines.append(f"{v}_parts = {parts}")
            items = ", ".join(f"F.get({v}_parts, {i}).alias({root + str(i + 1)!r})" for i in range(int(node["num_columns"])))
            r.lines.append(f"{v} = {src}.select('*', {items})")
        r.notes.append("New columns are named <root>1..<root>N (no separator); the source field is retained")
        self.set_out(nid, ["Output"], self.t_in(st), self.order.get(st))

    def e_dynrename(self, nid, node, ins, anchors, r):
        st = self.first_in(ins)
        mode = node.get("rename_mode") or "Formula"
        if mode != "Formula":
            r.need("unsupported", f"Dynamic Rename mode {mode!r} is not implemented (only Formula mode)")
        if r.status in ("blocked", "unsupported"):
            return self._blocked_guard(r, node, anchors, ins)
        res = self.tr(r, node.get("expression"), {}, "df", current="__CURRENT__", what="dynamic rename formula")
        if res is None:
            return self._blocked_guard(r, node, anchors, ins)
        fn = res.code.replace("F.col('__CURRENT__')", "_c")
        r.orig.append(node.get("expression"))
        fields, skip = list(node.get("fields_to_rename") or []), list(node.get("fields_kept") or [])
        v, src = self.var((nid, "Output")), self.var(st)
        test = f"c.lower() in {skip_expr(r, v + '_fields', fields)}"
        if node.get("unknown_renamed"):
            test += f" or c.lower() not in {skip_expr(r, v + '_known', fields + skip)}"
        r.lines.append(f"{v}_rename = lambda _c: {pretty.fmt(fn, 8)}")
        r.lines.append(f"{v}_targets = [c for c in {src}.columns if {test}]")
        r.lines.append(f"{v}_names = dict(zip({v}_targets, spark.range(1).select(\n"
                       f"    *[{v}_rename(F.lit(c)).cast('string').alias(f'c{{i}}') for i, c in enumerate({v}_targets)]).first()))")
        r.lines.append(f"{v} = {src}.toDF(*[{v}_names.get(c, c) or c for c in {src}.columns])")
        r.notes.append("New names are computed once from the formula applied to each field name")
        self.set_out(nid, ["Output"], {}, self.order.get(st))

    def e_datetime(self, nid, node, ins, anchors, r):
        """DateTime tool, from the Alteryx help: Date/Time -> text in a format, or text in a format -> Date/Time. See alteryx_datetime.py for the tokens and the leniency of reading text."""
        st = self.first_in(ins)
        if r.status == "blocked":
            return self._blocked_guard(r, node, anchors, ins)
        src, v = self.var(st), self.var((nid, "Output"))
        fmt, inp, out = node["format_string"], node["input_field"], node.get("output_field") or "DateTime_Out"
        language = node.get("language") or "English"
        types = self.t_in(st)
        seen = types.get(str(inp).lower(), "unknown")
        # The tool only offers date-time fields for "Date/Time -> string" and string fields for "string -> Date/Time", so a known input type decides the direction;
        # otherwise IsFrom does (False = string -> Date/Time in every real workflow checked: SAP text dates such as BUDAT_MKPF with yyyyMMdd)
        by_flag = "format" if node.get("is_from") else "parse"
        by_type = {"string": "parse", "date": "format", "datetime": "format"}.get(seen)
        direction = by_type or by_flag
        if by_type and by_type != by_flag:
            r.need("requires_review", f"IsFrom says '{by_flag}' but the input field {inp!r} is of type {seen}: the type was followed (the tool only offers date-time fields for Date/Time -> string)")
        elif not by_type:
            r.need("requires_review", f"the type of {inp!r} is not known, so the direction was taken from the tool's IsFrom setting ({'Date/Time -> string' if direction == 'format' else 'string -> Date/Time'}): confirm it")
        try:
            code = DT.format_code(col(inp), fmt, language) if direction == "format" else DT.parse_code(col(inp), fmt, language)
            notes = [] if direction == "format" else DT.parse_patterns(fmt, language)[2]
        except DT.FormatError as exc:
            r.need("blocked", f"DateTime format {fmt!r}: {exc}")
            return self._blocked_guard(r, node, anchors, ins)
        r.lines.append(f"{v} = {src}.withColumn({out!r}, {code})")
        r.notes.append(f"DateTime ({'Date/Time -> string' if direction == 'format' else 'string -> Date/Time'}, format {fmt!r}): Alteryx tokens translated (hh is the 24-hour clock, Month/Mon/day/dy are names)"
                       + ("; text is read leniently as Alteryx does ('-' = '/', spaces ignored, optional leading zeros, Mon/Month take either name width)" if direction == "parse" else ""))
        r.notes += notes
        r.need("requires_review", "the Alteryx help does not state: " + "; ".join(DT.NOT_STATED))
        types[out.lower()] = "string" if direction == "format" else "datetime"
        self.set_out(nid, ["Output"], types, self.order.get(st))

    # ------------------------------------------------------------------ sinks / non-data
    def e_write(self, nid, node, ins, anchors, r):
        if node.get("destination_type") == "file" and (node.get("file_format") or "").lower() in ("xlsx", "xls", "csv"):
            r.need("requires_review", f"Original output is a {node.get('file_format')} file on a local/shared drive; "
                                      "the migration writes a Delta table - add an export job if file delivery is still required")
        else:
            r.notes.append("Written as a Delta table")
        if node.get("write_mode") == "overwrite":
            r.notes.append("Alteryx overwrite semantics -> guarded Delta overwrite (ALLOW_OVERWRITE)")
        r.lines.clear()

    def e_passthrough(self, nid, node, ins, anchors, r):
        """Block Until Done and similar control tools: the data is unchanged, only execution order is affected."""
        st = self.first_in(ins)
        self.alias[(nid, "Output")] = self.var(st)
        r.notes.append(f"{node.get('original_tool_type')}: a control tool - data passes through unchanged; its "
                       "execution-ordering effect is not needed in dependency-ordered notebooks")
        self.set_out(nid, ["Output"], self.t_in(st), self.order.get(st))

    def e_countrecords(self, nid, node, ins, anchors, r):
        """Count Records: one row, one field, the number of incoming records (the help: the count of records, named Count)."""
        st = self.first_in(ins)
        out = node.get("output_field") or "Count"
        r.lines.append(f"{self.var((nid, 'Output'))} = {self.var(st)}.agg(F.count(F.lit(1)).alias({out!r}))")
        r.notes.append("Count Records: one row holding the number of incoming records (every record counts, NULLs included)")
        self.set_out(nid, ["Output"], {out.lower(): "number"}, None)

    def e_autofield(self, nid, node, ins, anchors, r):
        """Auto Field only changes the declared field types to the smallest that fit; the values do not change, so the rows pass through."""
        st = self.first_in(ins)
        self.alias[(nid, "Output")] = self.var(st)
        r.notes.append("Auto Field: Alteryx only shrinks the field types to the smallest that fit the data; values are unchanged, so the rows pass through "
                       "(Spark types are not changed - add a select with casts if a narrower type is required)")
        self.set_out(nid, ["Output"], self.t_in(st), self.order.get(st))

    def e_browse(self, nid, node, ins, anchors, r):
        r.status = "not_applicable"
        r.lines.clear()

    def e_comment(self, nid, node, ins, anchors, r):
        r.status = "not_applicable"
        r.lines.clear()

    def e_spatial(self, nid, node, ins, anchors, r):
        r.need("unsupported", f"{node['original_tool_type']}: spatial tool - needs Databricks ST_* / H3 / Mosaic and a check against Alteryx")
        self._blocked_guard(r, node, anchors, ins)

    def e_generaterows(self, nid, node, ins, anchors, r):
        """Supported pattern: init [f]/expression, condition `[f] <= bound`, loop DateTimeAdd([f], n, unit) or [f] + n."""
        st = self.first_in(ins)
        out = node.get("output_field")
        loop, cond, init = (node.get(k) or "" for k in ("expr_loop", "expr_cond", "expr_init"))
        m_loop = re.fullmatch(r"\s*DateTimeAdd\(\s*\[(.+?)\]\s*,\s*(-?\d+)\s*,\s*['\"](\w+?)s?['\"]\s*\)\s*", loop, re.I)
        m_num = re.fullmatch(r"\s*\[(.+?)\]\s*\+\s*(-?\d+)\s*", loop)
        m_cond = re.fullmatch(r"\s*\[(.+?)\]\s*<=\s*(.+?)\s*", cond, re.S)
        if node.get("update_field") or not (m_cond and (m_loop or m_num)) or \
                (m_loop or m_num).group(1) != out or m_cond.group(1) != out:
            r.need("unsupported", "Generate Rows: only 'init; [f] <= bound; DateTimeAdd([f],n,unit) | [f]+n' creating a new field is implemented")
            return self._blocked_guard(r, node, anchors, ins)
        types = self.t_in(st)
        src, v = self.var(st), self.var((nid, "Output"))
        a = self.tr(r, init, types, src, what="Generate Rows init")
        b = self.tr(r, m_cond.group(2), types, src, what="Generate Rows condition bound")
        if a is None or b is None:
            return self._blocked_guard(r, node, anchors, ins)
        if m_loop:
            step, unit = int(m_loop.group(2)), m_loop.group(3).lower().rstrip("s")
            if unit not in ("day", "week", "month", "year"):
                r.need("unsupported", f"Generate Rows: step unit {m_loop.group(3)!r} is not implemented")
                return self._blocked_guard(r, node, anchors, ins)
            r.lines.append(f"{v} = {src}.withColumn({out!r}, F.explode(F.sequence(F.to_date({a.code}), F.to_date({b.code}), F.expr('interval {step} {unit}'))))")
            r.lines.append(f"{v} = {v}.withColumn({out!r}, F.date_format({col(out)}, 'yyyy-MM-dd'))")
        else:
            step = int(m_num.group(2))
            r.lines.append(f"{v} = {src}.withColumn({out!r}, F.explode(F.sequence(({a.code}).cast('long'), ({b.code}).cast('long'), F.lit({step}))))")
        r.need("requires_review", "Generate Rows implemented with sequence(); confirm boundary and empty-range behaviour")
        types[out.lower()] = "string" if m_loop else "number"
        self.set_out(nid, ["Output"], types, None)

    # Data Cleansing (Cleanse.yxmc): the steps, their order and their expressions are those of the macro itself. Character classes use Java's
    # Unicode mode (?U) because the macro's Boost [[:alpha:]] / [[:punct:]] / \s match Unicode characters too.
    CLEANSE_STEPS = [
        ("remove_letters", "letters", r"(?U)\p{Alpha}", ""),
        ("remove_numbers", "numbers", r"[0-9\x{FF10}-\x{FF19}]", ""),
        ("remove_punctuation", "punctuation", r"(?U)\p{Punct}|[$+<=>^`|~\x{FF01}-\x{FF0F}\x{FF1A}-\x{FF20}\x{FF3B}-\x{FF40}\x{FF5B}-\x{FF5E}\x{2200}-\x{22FF}]", ""),
        ("collapse_whitespace", "tabs, line breaks and duplicate whitespace", r"(?U)\s+", " "),
        ("trim", "leading and trailing whitespace", r"^[ \t\n\r\x{00A0}]+|[ \t\n\r\x{00A0}]+$", ""),
        ("remove_all_whitespace", "all whitespace", r"(?U)\s+", ""),
    ]

    def e_cleanse(self, nid, node, ins, anchors, r):
        """Data Cleansing: null rows, null columns, case, blanks for NULL strings, unwanted characters, 0 for NULL numbers - in the macro's order."""
        st = self.first_in(ins)
        types = self.t_in(st)
        o = node.get("cleanse") or {}
        out = anchors[0]
        src, v = self.var(st), self.var((nid, out))
        cur = src
        r.orig.append("Data Cleansing: " + ", ".join(k for k, on in o.items() if on is True) + (f"; fields {o.get('fields')}" if o.get("fields") else ""))

        def step(code):
            nonlocal cur
            r.lines.append(f"{v} = {code}")
            cur = v

        if o.get("remove_null_rows"):
            r.lines.append(f"{v}_keep = F.lit(False)")
            r.lines.append(f"for _c in {cur}.columns:")
            r.lines.append(f"    {v}_keep = {v}_keep | F.col(_c).isNotNull()")
            step(f"{cur}.filter({v}_keep)")
            r.notes.append("Remove null rows: a row is dropped only when every field is NULL (an empty string is a value)")
        if o.get("remove_null_columns"):
            r.lines.append(f"{v}_counts = {cur}.agg(*[F.count(F.col(_c)).alias(_c) for _c in {cur}.columns]).first().asDict()")
            step(f"{cur}.select(*[_c for _c in {cur}.columns if {v}_counts[_c] > 0])")
            r.notes.append("Remove null columns: a field is dropped only when all its values are NULL; this depends on the data, so Spark SQL cannot express it")
        fields = list(o.get("fields") or [])
        kind_of = {f: types.get(f.lower(), "unknown") for f in fields}             # t_in already holds the user's answers (answers.field_types)
        unknown = [f for f, k in kind_of.items() if k in ("unknown", "")]
        strings = [f for f in fields if kind_of[f] == "string"]
        numbers = [f for f in fields if kind_of[f] == "number"]
        others = [f for f in fields if f not in strings and f not in numbers and f not in unknown]
        if others:
            r.notes.append(f"Data Cleansing leaves {others} unchanged: the macro only cleans string fields and replaces NULL with 0 in numeric fields")
        self.need_types(node, unknown)                       # the app asks the user, so that Spark SQL can be written too
        if unknown:                                          # the macro itself picks string / numeric fields by their type at run time: so does this code
            r.lines.append(f"{v}_types = dict({cur}.dtypes)")
            r.lines.append(f"{v}_strings = {strings!r} + [_c for _c in {unknown!r} if {v}_types[_c] == 'string']")
            r.lines.append(f"{v}_numbers = {numbers!r} + [_c for _c in {unknown!r} if {v}_types[_c] in {NUMERIC_DTYPES!r} or {v}_types[_c].startswith('decimal')]")
            strings, numbers = f"{v}_strings", f"{v}_numbers"
            r.notes.append(f"Data Cleansing: the type of {unknown} is not in the workflow, so the code picks string / numeric fields at run time like the macro does "
                           "(Spark SQL needs the types up front: answer the type question in the terminal to get SQL for this table)")

        def per_field(cols, expr, what):
            if cols:
                listed = cols if isinstance(cols, str) else repr(cols)
                step(f"{cur}.withColumns({{_c: {expr} for _c in {listed}}})")
                r.notes.append(f"Data Cleansing: {what} in {cols if not isinstance(cols, str) else 'the ' + cols.rsplit('_', 1)[-1] + ' fields'}")

        if o.get("modify_case"):
            fn = {"upper": "F.upper", "lower": "F.lower", "title": "F.initcap"}[o["case"]]
            per_field(strings, f"{fn}(F.col(_c))", f"{o['case']} case")
            if o["case"] == "title":
                r.need("requires_review", "Title case: Spark initcap splits words on whitespace only; Alteryx TitleCase also capitalises after punctuation - compare names such as O'NEIL")
        if o.get("blanks_for_null_strings"):
            per_field(strings, "F.coalesce(F.col(_c), F.lit(''))", "NULL replaced with an empty string")
        for key, what, pattern, repl in self.CLEANSE_STEPS:
            if o.get(key):
                per_field(strings, f"F.regexp_replace(F.col(_c), {pattern!r}, {repl!r})", f"remove {what}")
        if o.get("zero_for_null_numbers"):
            per_field(numbers, "F.coalesce(F.col(_c), F.lit(0))", "NULL replaced with 0")
        if cur == src:
            self.alias[(nid, out)] = src
            r.notes.append("Data Cleansing: no option changes the selected fields, the rows pass through")
        out_types = types
        if o.get("remove_null_columns"):
            out_types = {}                                       # which fields remain is known only at run time
        self.set_out(nid, anchors, out_types, self.order.get(st))

    def e_predictive(self, nid, node, ins, anchors, r):
        r.need("unsupported", f"{node.get('model_type')}: predictive tool - re-implement the model and compare; no equivalence assumed")
        self._blocked_guard(r, node, anchors, ins)

    def e_unsupported(self, nid, node, ins, anchors, r):
        reason = node.get("unsupported_reason") or "tool has no converter"
        code = node.get("custom_python")
        if code:     # human-supplied logic (override): inputs by anchor order as {in0}, {in1}...; the first output as {out}, every output as {out_<anchor>} (e.g. {out_Output2})
            flat = [s for v in ins.values() for s in v]
            names = {f"in{i}": self.var(s) for i, s in enumerate(flat)}
            names.update({f"out_{a}": self.var((nid, a)) for a in anchors})
            body = code.format(**names, out=self.var((nid, anchors[0])))      # braces of the code itself are written {{ }}
            r.lines += body.splitlines()
            if node.get("kept_python"):
                r.need("requires_review", "the embedded Python tool's original pandas code runs unchanged in a component file on the driver (the data must fit in its memory); "
                                          "NaN becomes NULL when the result returns to Spark - compare with the Alteryx output")
                self.set_out(nid, anchors, {}, None)
            elif node.get("ported_python"):
                r.need("requires_review", "ported from the embedded Python tool by a model, cell by cell: AI-written - review every step and compare with the Alteryx output")
                self.set_out(nid, anchors, {}, None)                           # the columns the script produces are not known
            else:
                r.need("requires_review", "implemented by human-supplied custom_python override; unverified")
                self.set_out(nid, anchors, self.t_in(flat[0]) if flat else {}, None)
            return
        r.need("unsupported", f"{node.get('original_plugin_name')}: {one_line(reason, 200)}")
        self._blocked_guard(r, node, anchors, ins)


SUPPORTED_AGG = {"groupby", "sum", "avg", "min", "max", "count", "countnonnull", "countdistinct", "median", "stddev",
                 "var", "first", "last", "concat", "percentile"}
