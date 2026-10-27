"""The SQL inside the workflow's input tools, parsed and translated to Spark SQL.

An Alteryx Input (or BigQuery Input) tool often carries a query that already does real work: it renames columns (`AEDAT AS PO_Date`), filters (`WHERE KSCHL IN (...)`), joins,
sub-selects and aggregates. The tools after it expect those columns. So the query is a transformation of the workflow and is converted, not skipped: it is parsed with sqlglot in its
own dialect, its table names are pointed at the tables in Databricks, and the result is written as Spark SQL (a view with the name the rest of the notebooks read).

Nothing is guessed: a query that does not parse, or a table whose Databricks name is unknown, stays a hand-off that quotes the original SQL and says why.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import logging

import sqlglot
from sqlglot import exp


class _Collect(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.messages: list = []

    def emit(self, record):
        self.messages.append(record.getMessage())

DIALECTS = {"bigquery": "BigQuery", "tsql": "SQL Server (T-SQL)", "oracle": "Oracle", "postgres": "PostgreSQL", "mysql": "MySQL", "snowflake": "Snowflake", "spark": "Spark / Databricks SQL"}
BY_PLUGIN = (("big_query", "bigquery"), ("bigquery", "bigquery"), ("snowflake", "snowflake"), ("postgres", "postgres"), ("oracle", "oracle"), ("mysql", "mysql"))


def dialect_of(plugin: str = "", connection: str = "", default: str = "") -> str:
    """The SQL dialect an input tool speaks: from its plugin or connection when they say so, else the caller's default (T-SQL is what an ODBC/SQL Server alias means in Alteryx)."""
    text = f"{plugin} {connection}".lower()
    for fragment, dialect in BY_PLUGIN:
        if fragment in text:
            return dialect
    if "sqlserver" in text or "sql server" in text or "mssql" in text:
        return "tsql"
    return default or ""


@dataclass
class Translation:
    sql: str = ""                                   # Spark SQL, table names already pointing at Databricks
    columns: list | None = None                     # output column names (original case) when the query lists them; None for SELECT *
    tables: list = field(default_factory=list)      # the Databricks tables it reads (they must exist)
    notes: list = field(default_factory=list)
    error: str = ""
    dialect: str = ""

    @property
    def ok(self) -> bool:
        return bool(self.sql) and not self.error


def projection_columns(select: exp.Expression) -> list | None:
    """Output names of a SELECT in the case the query wrote them; looks through `SELECT a.* FROM (SELECT ...) a`. None when a star cannot be resolved without the table's schema."""
    if isinstance(select, exp.Subquery):
        return projection_columns(select.this)
    if isinstance(select, (exp.Union, exp.Intersect, exp.Except)):
        return projection_columns(select.this)
    if not isinstance(select, exp.Select):
        return None
    names: list = []
    for item in select.expressions:
        is_star = isinstance(item, exp.Star) or (isinstance(item, exp.Column) and isinstance(item.this, exp.Star))
        if not is_star:
            name = item.alias_or_name
            if not name:
                return None
            names.append(name)
            continue
        sources = [select.args["from"].this] if select.args.get("from") else []
        sources += [j.this for j in select.args.get("joins") or []]
        qualifier = item.table if isinstance(item, exp.Column) else ""
        inner = [s for s in sources if isinstance(s, exp.Subquery) and (not qualifier or s.alias == qualifier)]
        if len(sources) != 1 and not qualifier or not inner:
            return None
        inner_cols = projection_columns(inner[0])
        if inner_cols is None:
            return None
        names += inner_cols
    return names


def _fix_types(tree: exp.Expression, dialect: str, notes: list) -> exp.Expression:
    """A DECIMAL without precision means something different in each engine; Spark's default (10,0) would silently cut the fraction."""
    default = {"bigquery": (38, 9), "tsql": (18, 0), "oracle": (38, 0), "postgres": None, "mysql": (10, 0), "snowflake": (38, 0)}.get(dialect)

    def fix(node):
        if isinstance(node, exp.DataType) and node.this in (exp.DataType.Type.DECIMAL, exp.DataType.Type.BIGDECIMAL) and not node.expressions:
            if node.this == exp.DataType.Type.BIGDECIMAL:
                notes.append("BIGNUMERIC has up to 76 digits; Spark's DECIMAL stops at 38 (written as DECIMAL(38,18)) - check the values")
                return exp.DataType.build("DECIMAL(38,18)")
            if default:
                notes.append(f"DECIMAL/NUMERIC without precision means DECIMAL{default} in {DIALECTS.get(dialect, dialect)}; Spark's default would be (10,0), so it is written DECIMAL{default}")
                return exp.DataType.build(f"DECIMAL({default[0]},{default[1]})")
        return node
    return tree.transform(fix)


def translate(query: str, dialect: str, mapper) -> Translation:
    """Parse `query` in `dialect`, point its tables at Databricks with `mapper(catalog, db, name) -> (catalog, db, name) | None`, return Spark SQL.
    `mapper` returning None for a table makes the whole translation an error (the table's place is unknown)."""
    out = Translation(dialect=dialect)
    if not dialect:
        out.error = "the SQL dialect of this input is not known"
        return out
    try:
        tree = sqlglot.parse_one(query, read=dialect)
    except sqlglot.errors.SqlglotError as exc:
        out.error = f"the {DIALECTS.get(dialect, dialect)} query does not parse: {str(exc).splitlines()[0][:200]}"
        return out
    if tree is None or not isinstance(tree, (exp.Select, exp.Union, exp.Subquery, exp.With)) and not tree.find(exp.Select):
        out.error = "the input query is not a SELECT"
        return out
    out.columns = projection_columns(tree)
    cte_names = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    seen: list = []
    missing: list = []

    def remap(node):
        if isinstance(node, exp.Table) and isinstance(node.this, exp.Identifier):
            if not node.args.get("db") and node.name in cte_names:
                return node
            target = mapper(node.catalog, node.db, node.name)
            if target is None:
                missing.append(".".join(x for x in (node.catalog, node.db, node.name) if x))
                return node
            catalog, db, name = target
            full = ".".join(x for x in (catalog, db, name) if x)
            if full not in seen:
                seen.append(full)
            alias = node.args.get("alias")
            new = exp.table_(name, db=db or None, catalog=catalog or None)
            if alias is not None:
                new.set("alias", alias)
            return new
        return node
    tree = tree.transform(remap)
    if missing:
        out.error = "no Databricks table is known for: " + ", ".join(sorted(set(missing)))
        return out
    tree = _fix_types(tree, dialect, out.notes)
    logger = logging.getLogger("sqlglot")                                       # sqlglot warns on stderr for what Spark SQL cannot express (e.g. recursive CTEs): keep it with the query
    collected = _Collect()
    logger.addHandler(collected)
    try:
        out.sql = tree.sql(dialect="spark", pretty=True)
    finally:
        logger.removeHandler(collected)
    out.notes += [f"sqlglot: {m} - review this query by hand" for m in dict.fromkeys(collected.messages)]
    out.tables = seen
    if dialect != "spark":
        out.notes.append(f"translated from {DIALECTS.get(dialect, dialect)} to Spark SQL by sqlglot: check dialect differences (date functions and time zone, NULL ordering, string/number casts)")
    return out


def table_mapper(source_catalog: str = "", source_schema: str = "", catalog: str = "", schema: str = ""):
    """Where the tables a query reads live in Databricks. `project.dataset.table` (BigQuery) keeps the dataset as the schema unless a source schema is given."""
    def mapper(cat: str, db: str, name: str):
        c = source_catalog or catalog
        s = source_schema or db or schema
        return (c, s, name)
    return mapper
