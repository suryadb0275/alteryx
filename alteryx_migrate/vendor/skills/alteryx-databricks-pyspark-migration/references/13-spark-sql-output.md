# Spark SQL output (next to the PySpark)

`scripts/unit_functions.py` gives every output table a second form, `table["sql"]`: the same conversion as one Spark SQL statement.

```
{"statement": "CREATE TABLE @@out:<table>@@ AS WITH <one CTE per DataFrame step> SELECT * FROM <result>", "columns": [...]}      or
{"statement": None, "error": "tool [id] Name: why there is no SQL"}
```

Tables are written as `@@in:<bronze key>@@` (an input table) and `@@out:<table>@@` (a table another unit or this one writes); the caller replaces them with `catalog.schema.table`.

## How it is made (and why it cannot disagree with the PySpark)

The emitters in `codegen.py` stay the only place where an Alteryx tool is given a meaning (NULL rules, case-insensitive text matching, ANSI-safe division and casts, 0-based Substring,
Join prefixes, Unique/order rules ...). `scripts/sqlframe.py` does not decide any of that again: it **executes the PySpark text those emitters wrote** against a symbolic DataFrame
(`Frame`, `Col`, `F`, `Window` look-alikes) that records each call and prints it as Spark SQL. Every DataFrame operation becomes one CTE, named after the PySpark variable
(`flt_3`, `calc_5`, `jn_9`), tagged with the tool (`-- [5] Formula`). CTEs the result does not need are left out.

* **Nothing is guessed.** A DataFrame method, Column method or function with no verified SQL form raises `SqlError` (for example Cross Tab / pivot, `F.expr(...)`, a runtime `isinstance` choice
  between text and number for `+`). The table then has `sql = None` with the reason, and its PySpark stays complete; the SQL notebook says so in the table's cell.
* **Columns must be known.** SQL needs column lists where PySpark can look them up at run time (`df.columns`). They come from the workflow's field lists, from the tables built earlier, and for
  database inputs (which carry none) from `answers["schemas"]` = `{bronze key: [column names] | {column name: Spark type}}`. Given types are also stored as the input tool's `field_info`, so the
  PySpark emitters know them too (fewer "type unknown at generation time" fallbacks). Without them such an input's tables have no SQL; the PySpark is unchanged.
* **Same data, same result.** `tests/test_sql_tools.py` runs every supported tool as PySpark and as the SQL made from it on local Spark and compares columns and rows;
  `tests/test_sql_expr.py` does the same for every expression case of `test_expr_translation.py`. Tools and functions those tests do not cover (for example Dynamic Rename, Generate Rows, several
  date functions with unusual arguments) get SQL only if the PySpark runs through the symbolic DataFrame unchanged - the result is still made from the same text, but it is not individually proven:
  the SQL notebook header says "converted, not yet validated", and reconciliation against the Alteryx output is what proves it.
* **Dialect.** Databricks SQL / Spark 3.5+ (`median`, `try_divide`, `try_to_timestamp`, `contains`, `startswith`, `<=>`, `LEFT ANTI JOIN`). Float literals are written `1.5D` because `F.lit(1.5)` is a double
  while a bare `1.5` is a decimal in SQL. Backslashes and quotes in string literals are escaped for Spark's default parser.
* **Known differences from the PySpark (by design):** a column replaced by `withColumn` is rewritten in place in the CTE column list; a physical sort is kept only where PySpark keeps it; `dropDuplicates`
  keeps an arbitrary row per key (as Spark does) using `row_number() ... ORDER BY monotonically_increasing_id()`.

`CREATE TABLE` fails when the table exists, like the guarded write (`ALLOW_OVERWRITE=False`) of the PySpark notebooks; the caller states how to overwrite on purpose (`CREATE OR REPLACE TABLE`).
