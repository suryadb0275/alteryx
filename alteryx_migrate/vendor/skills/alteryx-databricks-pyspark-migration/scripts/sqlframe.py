"""PySpark DataFrame code -> Spark SQL, by running the generated code against a symbolic DataFrame.

The emitters in codegen.py already decided what every Alteryx tool means and wrote it as PySpark. This module does not decide anything again: it
executes that same PySpark text with ``F``, ``Window`` and the DataFrame replaced by small look-alikes that *record* what is asked of them and print it as
Spark SQL. Each DataFrame operation becomes one common table expression (CTE); the CTEs are named after the variables of the PySpark code, so the SQL
reads like the notebook it accompanies.

Rules (same as the rest of the skill): nothing is guessed. A DataFrame method, Column method or function without a verified SQL form raises
``SqlError``; the caller reports the tool and the table gets a hand-off instead of SQL. Column names must be known (they come from the workflow's
field list and from the tables built earlier), because SQL needs them where PySpark can look them up at run time.

Public API: ``Builder`` (execute the steps of one table), ``to_sql(code)`` (one Column expression), ``SqlError``, ``ident``, ``literal``.
"""
from __future__ import annotations

import ast
import re

TOKEN = "\x00"


class SqlError(Exception):
    """The PySpark code has no verified Spark SQL form."""


def ident(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _string(value: str) -> str:
    out = []
    for ch in value:
        if ch == "\\":
            out.append("\\\\")
        elif ch == "'":
            out.append("\\'")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif ord(ch) < 32:
            out.append("\\u%04x" % ord(ch))
        else:
            out.append(ch)
    return "'" + "".join(out) + "'"


def literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise SqlError(f"the float literal {value!r} has no SQL form")
        return f"{value!r}D"                      # F.lit(1.5) is a double in PySpark; a bare 1.5 would be a decimal in SQL
    if isinstance(value, str):
        return _string(value)
    raise SqlError(f"the literal {value!r} has no SQL form")


def _as_col(x) -> "Col":
    if isinstance(x, Col):
        return x
    if x is None or isinstance(x, (bool, int, float, str)):
        return Col(literal(x))
    raise SqlError(f"{type(x).__name__} cannot be used as a column")


# ---------------------------------------------------------------------------------------------------------------- columns
class Col:
    """A Column expression as Spark SQL text. `name` is the output name the column has in a select (None for an unnamed expression)."""

    def __init__(self, sql: str, name: str | None = None, plain: bool = False, aliased: bool = False, order: bool = False):
        self._sql, self.name, self.plain, self.aliased, self.order = sql, name, plain, aliased, order

    @property
    def sql(self) -> str:
        return self._sql

    __hash__ = object.__hash__

    def _op(self, op: str, other, rev: bool = False) -> "Col":
        a, b = (_as_col(other), self) if rev else (self, _as_col(other))
        return Col(f"({a.sql} {op} {b.sql})")

    def __eq__(self, o): return self._op("=", o)                # noqa: E704
    def __ne__(self, o): return self._op("<>", o)               # noqa: E704
    def __lt__(self, o): return self._op("<", o)                # noqa: E704
    def __le__(self, o): return self._op("<=", o)               # noqa: E704
    def __gt__(self, o): return self._op(">", o)                # noqa: E704
    def __ge__(self, o): return self._op(">=", o)               # noqa: E704
    def __and__(self, o): return self._op("AND", o)             # noqa: E704
    def __rand__(self, o): return self._op("AND", o, True)      # noqa: E704
    def __or__(self, o): return self._op("OR", o)               # noqa: E704
    def __ror__(self, o): return self._op("OR", o, True)        # noqa: E704
    def __add__(self, o): return self._op("+", o)               # noqa: E704
    def __radd__(self, o): return self._op("+", o, True)        # noqa: E704
    def __sub__(self, o): return self._op("-", o)               # noqa: E704
    def __rsub__(self, o): return self._op("-", o, True)        # noqa: E704
    def __mul__(self, o): return self._op("*", o)               # noqa: E704
    def __rmul__(self, o): return self._op("*", o, True)        # noqa: E704
    def __truediv__(self, o): return self._op("/", o)           # noqa: E704
    def __rtruediv__(self, o): return self._op("/", o, True)    # noqa: E704
    def __mod__(self, o): return self._op("%", o)               # noqa: E704
    def __rmod__(self, o): return self._op("%", o, True)        # noqa: E704
    def __invert__(self): return Col(f"(NOT {self.sql})")       # noqa: E704
    def __neg__(self): return Col(f"(-{self.sql})")             # noqa: E704

    def eqNullSafe(self, o): return self._op("<=>", o)          # noqa: E704,N802

    def alias(self, name: str) -> "Col":
        return Col(self.sql, name=name, aliased=True)

    def cast(self, dtype: str) -> "Col":
        return Col(f"CAST({self.sql} AS {dtype})", name=self.name)

    def try_cast(self, dtype: str) -> "Col":
        return Col(f"TRY_CAST({self.sql} AS {dtype})", name=self.name)

    def isNull(self): return Col(f"({self.sql} IS NULL)")            # noqa: E704,N802
    def isNotNull(self): return Col(f"({self.sql} IS NOT NULL)")     # noqa: E704,N802

    def isin(self, *values):
        if len(values) == 1 and isinstance(values[0], (list, tuple, set)):
            values = tuple(values[0])
        return Col(f"({self.sql} IN ({', '.join(_as_col(v).sql for v in values)}))")

    def contains(self, o): return Col(f"contains({self.sql}, {_as_col(o).sql})")        # noqa: E704
    def startswith(self, o): return Col(f"startswith({self.sql}, {_as_col(o).sql})")    # noqa: E704
    def endswith(self, o): return Col(f"endswith({self.sql}, {_as_col(o).sql})")        # noqa: E704
    def rlike(self, o): return Col(f"({self.sql} RLIKE {_as_col(o).sql})")              # noqa: E704

    def substr(self, start, length):
        return Col(f"substr({self.sql}, {_as_col(start).sql}, {_as_col(length).sql})")

    def _sorted(self, how: str) -> "Col":
        return Col(f"{self.sql} {how}", name=self.name, order=True)

    def asc(self): return self._sorted("ASC")                                   # noqa: E704
    def desc(self): return self._sorted("DESC")                                 # noqa: E704
    def asc_nulls_first(self): return self._sorted("ASC NULLS FIRST")           # noqa: E704
    def asc_nulls_last(self): return self._sorted("ASC NULLS LAST")             # noqa: E704
    def desc_nulls_first(self): return self._sorted("DESC NULLS FIRST")         # noqa: E704
    def desc_nulls_last(self): return self._sorted("DESC NULLS LAST")           # noqa: E704

    def over(self, window: "WindowSpec") -> "Col":
        return Col(f"{self.sql} OVER ({window.sql})")

    def when(self, cond, value):
        raise SqlError("when() after a non-when expression")

    def select_sql(self) -> str:
        """The text of this column in a SELECT list."""
        if self.plain and not self.aliased:
            return self.sql
        if not self.name:
            raise SqlError("an unnamed expression in a select list")
        return f"{self.sql} AS {ident(self.name)}"


class When(Col):
    def __init__(self, branches: list, otherwise=None):
        super().__init__("")
        self.branches, self.else_ = branches, otherwise

    @property
    def sql(self) -> str:
        text = "CASE " + " ".join(f"WHEN {c.sql} THEN {v.sql}" for c, v in self.branches)
        return text + (f" ELSE {self.else_.sql}" if self.else_ is not None else "") + " END"

    def when(self, cond, value):
        if self.else_ is not None:
            raise SqlError("when() after otherwise()")
        return When(self.branches + [(_as_col(cond), _as_col(value))])

    def otherwise(self, value):
        return When(self.branches, _as_col(value))

    def alias(self, name):
        return Col(self.sql, name=name, aliased=True)


def column(name: str) -> Col:
    """F.col(name): `a`, `a.b` (alias-qualified: alias a, column b) or an already quoted `a.b`."""
    if name == "*":
        return Col("*", plain=True)
    if name.startswith("`") and name.endswith("`") and len(name) > 2 and name.count("`") == 2:
        return Col(name, name=name[1:-1], plain=True)
    m = re.match(r"^([A-Za-z_][A-Za-z_0-9]*)\.(`.*`|[^.`]+)$", name)
    if m:                                                    # alias.column (the Join emitter) or struct.field (Transpose); names with dots are backticked by the emitters
        rest = m.group(2)
        bare = rest[1:-1].replace("``", "`") if rest.startswith("`") else rest
        return Col(f"{m.group(1)}.{ident(bare)}", name=bare, plain=True)
    return Col(ident(name), name=name, plain=True)


FUNCTIONS = {
    "concat", "coalesce", "length", "upper", "lower", "initcap", "left", "right", "replace", "position", "lpad", "rpad", "reverse", "get", "split",
    "size", "regexp_extract_all", "regexp_replace", "try_divide", "format_string", "typeof", "floor", "ceil", "abs", "sqrt", "log", "log10", "exp",
    "pow", "round", "least", "greatest", "date_format", "try_to_timestamp", "current_date", "trunc", "last_day", "current_timestamp", "year",
    "month", "dayofmonth", "hour", "minute", "second", "add_months", "date_add", "timestamp_seconds", "unix_timestamp", "datediff", "rank",
    "dense_rank", "row_number", "count", "sum", "avg", "min", "max", "median", "first", "last", "concat_ws", "collect_list", "percentile",
    "stddev_samp", "var_samp", "lag", "lead", "monotonically_increasing_id", "explode", "sequence", "to_date",
}


class _Functions:
    def col(self, name): return column(name)                                    # noqa: E704
    def lit(self, value): return _as_col(value)                                 # noqa: E704
    def when(self, cond, value): return When([(_as_col(cond), _as_col(value))])  # noqa: E704

    def struct(self, *cols):
        return Col("struct(" + ", ".join(_as_col(c).select_sql() for c in cols) + ")")

    def array(self, *cols):
        return Col("array(" + ", ".join(_as_col(c).sql for c in cols) + ")")

    def countDistinct(self, *cols):                                             # noqa: N802
        return Col(f"count(DISTINCT {', '.join(_as_col(c).sql for c in cols)})")

    def __getattr__(self, name):
        if name not in FUNCTIONS:
            raise SqlError(f"F.{name}() has no verified SQL form")

        def call(*args):
            return Col(f"{name}({', '.join(_as_col(a).sql for a in args)})")
        return call


F = _Functions()


class WindowSpec:
    def __init__(self, parts=(), orders=(), frame=""):
        self.parts, self.orders, self.frame = tuple(parts), tuple(orders), frame

    def partitionBy(self, *cols):                                               # noqa: N802
        return WindowSpec([_as_col(c) for c in cols], self.orders, self.frame)

    def orderBy(self, *cols):                                                   # noqa: N802
        return WindowSpec(self.parts, [_as_col(c) for c in cols], self.frame)

    def rowsBetween(self, start, end):                                          # noqa: N802
        return WindowSpec(self.parts, self.orders, f"ROWS BETWEEN {start} AND {end}")

    @property
    def sql(self) -> str:
        bits = []
        if self.parts:
            bits.append("PARTITION BY " + ", ".join(c.sql for c in self.parts))
        if self.orders:
            bits.append("ORDER BY " + ", ".join(c.sql for c in self.orders))
        if self.frame:
            bits.append(self.frame)
        return " ".join(bits)


class _Window(WindowSpec):
    unboundedPreceding = "UNBOUNDED PRECEDING"                                 # noqa: N815
    unboundedFollowing = "UNBOUNDED FOLLOWING"                                 # noqa: N815
    currentRow = "CURRENT ROW"                                                 # noqa: N815


Window = _Window()


# ---------------------------------------------------------------------------------------------------------------- frames
def _items(cols, available) -> tuple:
    """SELECT-list text and output names for select(*cols); '*' and plain names expand against `available` (the frame's columns)."""
    texts, names = [], []
    for c in cols:
        if isinstance(c, str):
            if c == "*":
                texts += [ident(n) for n in available]
                names += list(available)
            else:
                texts.append(ident(c))
                names.append(c)
            continue
        c = _as_col(c)
        if c.sql == "*":
            texts += [ident(n) for n in available]
            names += list(available)
            continue
        texts.append(c.select_sql())
        names.append(c.name)
    return texts, names


class Frame:
    """A symbolic DataFrame: either a table reference or one CTE built from earlier frames."""

    def __init__(self, ctx: "Builder", columns: list, parts: dict | None = None, refs: list | None = None, table: str | None = None):
        self.ctx, self.columns, self.parts, self.refs, self.table = ctx, list(columns), parts, list(refs or []), table
        self.name: str | None = None
        self.var: str | None = None
        self.tool = ctx.tool
        if parts is not None:
            ctx.frames.append(self)

    # --- how other frames refer to this one
    def ref(self) -> str:
        return self.table if self.table is not None else f"{TOKEN}{id(self)}{TOKEN}"

    def _derive(self, columns, **parts) -> "Frame":
        return Frame(self.ctx, columns, parts, [self])

    def _index(self, name: str) -> int | None:
        for i, c in enumerate(self.columns):
            if c.lower() == name.lower():
                return i
        return None

    # --- operations
    def filter(self, cond):
        return self._derive(self.columns, select=["*"], from_=self.ref(), where=_as_col(cond).sql)

    where = filter

    def withColumn(self, name, col):                                            # noqa: N802
        return self.withColumns({name: col})

    def withColumns(self, mapping):                                             # noqa: N802
        columns, texts, new = list(self.columns), [ident(c) for c in self.columns], []
        for name, col in mapping.items():
            col = _as_col(col)
            text = f"{col.sql} AS {ident(name)}"
            i = self._index(name)
            if i is None:
                new.append((name, text))
            else:
                texts[i], columns[i] = text, name
        columns += [n for n, _ in new]
        added = [t for _, t in new]
        if len(new) == len(mapping):                       # only new columns: keep everything and append them
            return self._derive(columns, select=["*"] + added, from_=self.ref())
        return self._derive(columns, select=texts + added, from_=self.ref())

    def select(self, *cols):
        texts, names = _items(cols, self.columns)
        return self._derive(names, select=texts, from_=self.ref())

    def drop(self, *names):
        gone = {n.lower() for n in names}
        keep = [c for c in self.columns if c.lower() not in gone]
        return self._derive(keep, select=[ident(c) for c in keep], from_=self.ref())

    def toDF(self, *names):                                                     # noqa: N802
        if len(names) != len(self.columns):
            raise SqlError("toDF() with a different number of names")
        return self._derive(list(names), select=[f"{ident(c)} AS {ident(n)}" for c, n in zip(self.columns, names)], from_=self.ref())

    def alias(self, name):
        return AliasedFrame(self, name)

    def join(self, other, on, how="inner"):
        raise SqlError("join() needs both inputs aliased")

    def crossJoin(self, other):                                                 # noqa: N802
        other = other.frame if isinstance(other, AliasedFrame) else other
        return self._derive(self.columns + other.columns, select=["*"], from_=f"{self.ref()} CROSS JOIN {other.ref()}").add_ref(other)

    def add_ref(self, other):
        self.refs.append(other)
        return self

    def unionByName(self, other, allowMissingColumns=False):                    # noqa: N802,N803
        extra = [c for c in other.columns if self._index(c) is None]
        if extra and not allowMissingColumns:
            raise SqlError("unionByName() of frames with different columns")
        columns = self.columns + extra

        def arm(frame):
            have = {c.lower(): c for c in frame.columns}
            items = [ident(have[c.lower()]) if c.lower() in have else f"NULL AS {ident(c)}" for c in columns]
            return {"select": items, "from_": frame.ref()}
        return Frame(self.ctx, columns, {"union": [arm(self), arm(other)]}, [self, other])

    def union(self, other):
        if len(other.columns) != len(self.columns):
            raise SqlError("union() of frames with a different number of columns")
        return Frame(self.ctx, self.columns, {"union": [{"select": ["*"], "from_": self.ref()}, {"select": ["*"], "from_": other.ref()}], "positional": True},
                     [self, other])

    def groupBy(self, *cols):                                                   # noqa: N802
        return GroupBy(self, cols)

    def agg(self, *aggs):
        return GroupBy(self, ()).agg(*aggs)

    def orderBy(self, *cols):                                                   # noqa: N802
        return self._derive(self.columns, select=["*"], from_=self.ref(), order=", ".join(_as_col(c).sql for c in cols))

    sort = orderBy

    def distinct(self):
        return self._derive(self.columns, select=["*"], from_=self.ref(), distinct=True)

    def dropDuplicates(self, subset=None):                                      # noqa: N802
        keys = [ident(k) for k in (subset or self.columns)]
        inner = Frame(self.ctx, self.columns + ["__rn"], {"select": ["*", "row_number() OVER (PARTITION BY " + ", ".join(keys) + " ORDER BY monotonically_increasing_id()) AS `__rn`"],
                                                         "from_": self.ref()}, [self])
        return Frame(self.ctx, self.columns, {"select": [ident(c) for c in self.columns], "from_": inner.ref(), "where": "`__rn` = 1"}, [inner])

    def __getattr__(self, name):
        raise SqlError(f"DataFrame.{name} has no verified SQL form")


class AliasedFrame:
    def __init__(self, frame: Frame, alias: str):
        self.frame, self.alias_name = frame, alias

    @property
    def columns(self):
        return self.frame.columns

    def join(self, other, on, how="inner"):
        if not isinstance(other, AliasedFrame):
            raise SqlError("join() needs both inputs aliased")
        on = _as_col(on)
        if how == "inner":
            return JoinFrame(self, other, on)
        if how == "left_anti":
            f = self.frame
            return Frame(f.ctx, f.columns, {"select": [f"{self.alias_name}.{ident(c)}" for c in f.columns],
                                            "from_": f"{f.ref()} AS {self.alias_name} LEFT ANTI JOIN {other.frame.ref()} AS {other.alias_name} ON {on.sql}"},
                         [f, other.frame])
        raise SqlError(f"join type {how!r} has no verified SQL form")

    def __getattr__(self, name):
        return getattr(self.frame, name)


class JoinFrame:
    def __init__(self, left: AliasedFrame, right: AliasedFrame, on: Col):
        self.left, self.right, self.on = left, right, on

    def select(self, *cols):
        f = self.left.frame
        texts, names = _items(cols, [])
        if any(t == "*" for t in texts):
            raise SqlError("select('*') after a join")
        return Frame(f.ctx, names, {"select": texts, "from_": f"{f.ref()} AS {self.left.alias_name} INNER JOIN {self.right.frame.ref()} AS {self.right.alias_name} ON {self.on.sql}"},
                     [f, self.right.frame])

    def __getattr__(self, name):
        raise SqlError(f"a joined DataFrame cannot be used with .{name}() before select()")


class GroupBy:
    def __init__(self, frame: Frame, cols):
        self.frame, self.cols = frame, [c if not isinstance(c, str) else column(c) for c in cols]

    def agg(self, *aggs):
        f = self.frame
        names = []
        texts = []
        for c in list(self.cols) + list(aggs):
            texts.append(c.select_sql())
            names.append(c.name)
        group = ", ".join(c.sql for c in self.cols)
        return Frame(f.ctx, names, {"select": texts, "from_": f.ref(), **({"group": group} if group else {})}, [f])

    def pivot(self, *a, **k):
        raise SqlError("pivot() has no verified SQL form")


class _SparkShim:
    """`spark` inside the generated code: only createDataFrame (the Text Input tool) has a SQL form."""

    def __init__(self, ctx):
        self.ctx = ctx

    def createDataFrame(self, rows, ddl):                                       # noqa: N802
        names = [m.strip("`") for m in re.findall(r"`[^`]+`|\b\w+\b(?=\s+\w+)", ddl)]
        names = re.findall(r"`([^`]+)`\s+\w+", ddl) or names
        if not names:
            raise SqlError("createDataFrame() with an unreadable schema")
        values = ", ".join("(" + ", ".join(literal(v) for v in row) + ")" for row in rows)
        if not values:
            raise SqlError("createDataFrame() without rows")
        return Frame(self.ctx, names, {"values": values, "names": names}, [])

    def __getattr__(self, name):
        raise SqlError(f"spark.{name} has no verified SQL form")


# ---------------------------------------------------------------------------------------------------------------- rendering
def _select_text(parts: dict, indent: str = "") -> str:
    items = parts["select"]
    head = "SELECT DISTINCT" if parts.get("distinct") else "SELECT"
    one = f"{head} {', '.join(items)}"
    if len(items) == 1 or len(one) <= 90:
        text = one
    else:
        text = head + "\n" + ",\n".join(f"{indent}    {i}" for i in items)
    text += f"\n{indent}FROM {parts['from_']}"
    if parts.get("where"):
        text += f"\n{indent}WHERE {parts['where']}"
    if parts.get("group"):
        text += f"\n{indent}GROUP BY {parts['group']}"
    if parts.get("order"):
        text += f"\n{indent}ORDER BY {parts['order']}"
    return text


class Builder:
    """Runs the PySpark of one output table step by step and collects the CTEs."""

    def __init__(self):
        self.frames: list = []
        self.tool: tuple | None = None
        self.ns: dict = {}
        self.used: set = set()

    def table(self, ref: str, columns: list) -> Frame:
        return Frame(self, columns, None, [], table=ref)

    def start(self, inputs: dict) -> None:
        """inputs = {variable name: (table reference text, [column names])}."""
        self.ns = {"F": F, "Window": Window, "spark": _SparkShim(self), "StringType": object}
        for var, (ref, columns) in inputs.items():
            self.ns[var] = self.table(ref, columns)

    def step(self, tool: int, label: str, lines: list) -> None:
        """Execute the PySpark of one Alteryx tool (the lines the emitter wrote, joined); every DataFrame it creates becomes a CTE.
        The text is run one top-level statement at a time so a variable that is assigned again keeps a CTE for each of its values."""
        self.tool = (tool, label)
        text = "\n".join(lines)
        try:
            tree = ast.parse(text)
        except SyntaxError as exc:
            raise SqlError(f"tool [{tool}] {label}: the generated PySpark does not parse ({exc.msg})") from exc
        for node in tree.body:
            before = len(self.frames)
            try:
                exec(compile(ast.Module([node], []), f"<tool {tool}>", "exec"), self.ns)           # noqa: S102 - the skill's own generated code
            except SqlError as exc:
                raise SqlError(f"tool [{tool}] {label}: {exc}") from exc
            except (AttributeError, TypeError, KeyError, NameError, ValueError, IndexError, StopIteration) as exc:
                raise SqlError(f"tool [{tool}] {label}: no verified SQL form ({type(exc).__name__}: {str(exc)[:120]})") from exc
            self._name(self.frames[before:])

    def _name(self, fresh: list) -> None:
        bound = {}
        for var, value in self.ns.items():
            if isinstance(value, Frame) and value.name is None and value in fresh:
                bound[id(value)] = var
        last = next((bound[id(f)] for f in reversed(fresh) if id(f) in bound), None)
        k = 0
        for f in fresh:
            var = bound.get(id(f))
            if var is None:
                k += 1
                var = f"{last or 'step'}_part{k}" if last else f"step_{f.tool[0] if f.tool else 0}_{k}"
            f.var = var
            name, i = var, 2
            while name in self.used:
                name, i = f"{var}_{i}", i + 1
            self.used.add(name)
            f.name = name

    def result(self, var: str) -> Frame:
        frame = self.ns.get(var)
        if not isinstance(frame, Frame):
            raise SqlError(f"the result {var!r} is not a DataFrame")
        return frame

    def query(self, result_var: str) -> str:
        """WITH <the CTEs that the result needs> SELECT * FROM <result>: the result of one DataFrame as a single query."""
        final = self.result(result_var)
        need, stack = set(), [final]
        while stack:
            f = stack.pop()
            if id(f) in need or f.parts is None:
                continue
            need.add(id(f))
            stack += f.refs
        ordered = [f for f in self.frames if id(f) in need]
        names = {str(id(f)): ident(f.name) for f in ordered}

        def resolve(text: str) -> str:
            return re.sub(TOKEN + r"(\d+)" + TOKEN, lambda m: names[m.group(1)], text)

        ctes, last_tool = [], None
        for f in ordered:
            p = f.parts
            if "values" in p:
                body = f"SELECT * FROM VALUES {p['values']} AS t({', '.join(ident(n) for n in p['names'])})"
            elif "union" in p:
                body = "\n  UNION ALL\n  ".join(_select_text(arm, "  ") for arm in p["union"])
            else:
                body = _select_text(p, "  ")
            tag = ""
            if f.tool and f.tool != last_tool:
                tag = f"  -- [{f.tool[0]}] {f.tool[1]}\n"
                last_tool = f.tool
            ctes.append(f"{tag}  {ident(f.name)} AS (\n  {resolve(body)}\n  )")
        head = ("WITH\n" + ",\n".join(ctes) + "\n") if ctes else ""
        return head + f"SELECT * FROM {ident(final.name) if final.table is None else final.table}"

    def statement(self, result_var: str, target: str, comment: str = "") -> str:
        """CREATE TABLE <target> AS <query>."""
        return (comment + "\n" if comment else "") + f"CREATE TABLE {target} AS\n" + self.query(result_var)


def to_sql(code: str) -> str:
    """Spark SQL text of one PySpark Column expression (as written by alteryx_expr.py)."""
    try:
        value = eval(code.strip(), {"F": F, "StringType": object, "Window": Window}, {})          # noqa: S307 - the skill's own generated text
    except SqlError:
        raise
    except Exception as exc:                                                                      # noqa: BLE001
        raise SqlError(f"no verified SQL form ({type(exc).__name__}: {str(exc)[:120]})") from exc
    if not isinstance(value, Col):
        raise SqlError("the expression is not a Column")
    return value.sql
