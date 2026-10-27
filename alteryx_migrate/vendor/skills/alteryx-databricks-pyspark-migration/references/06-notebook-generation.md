# Notebook generation conventions

## Files produced by `generate_notebooks.py` (default `--format ipynb`; `--format py` = Databricks source format)
| File | Purpose |
|---|---|
| `00_config.ipynb` | banner (partial / not yet validated), catalog / schema / time zone / ANSI widgets with the confirmed project defaults, write settings, the guarded `write_table(df, table)` function, and the table registry `T` (`bronze`, `silver`, `gold` lists); business context copied from the workflow's comment tools |
| `01_bronze.ipynb` | the **input tables** with the query each Alteryx input tool ran, plus a check that every table exists. Nothing is ingested: land these tables with your ingestion tooling |
| `02_silver_gold.ipynb` | **one section per Alteryx container, in the order the data flows** (needs the `.yxmd`): a markdown heading with the **layer (Silver = cleansed / integrated, Gold = a section that writes a `gold_` table)** and the tool chain, then one code cell starting `# Container 40 · Name` followed by `# Layer: Silver|Gold`, with `# [38] Filter` comments carrying the tool ids. Plain top-level code; variables are shared between the cells (run top to bottom); a table is written right after the tool that produces it (`write_table(df, T['x'])`). Tools outside any container are grouped by connected chain (`# Other tools 2 · ...`); a very large container is shown by its sub-containers; containers that wait on each other are split into parts. `--layout units` turns this off |
| `02_silver.ipynb`, `03_gold.ipynb` | (no containers in the workflow, or `--layout units`) | one heading + one code cell per materialised table: `build_<table>(spark)` returns `{table: DataFrame}` and a loop below it writes every returned table with `write_table` |
| `04_quality_reconciliation.ipynb` | structural checks for every table; baseline reconciliation (`EXPECTED_BASELINES`, reports NOT RUN when empty) |
| `databricks_job.json` | tasks chained in order (compute/schedule left to the deployer) |
| `PREFLIGHT.md`, `preflight.json`, `answers.template.json` | the grouped questions, the same as data, and a fill-in answers file |
| `unsupported_tools.xml` | one `<Tool>` per unsupported tool (and per input tool reinterpreted as a Bronze source) with its configuration unchanged |
| `MIGRATION_REPORT.md`, `coverage.json` | report sections A-I; per-tool status, unit, variable name, original expression, reviews |

## Naming (derived from the workflow, never invented)
- **Bronze** `bronze_<source table>` (last part of the table in the query; a different query on the same table gets `_q2`).
- **Silver / Gold** `<layer>_<source>_<what happened>`: `silver_orders_joined_customers`, `silver_mseg_rpt_v_combined`; Write inputs are `gold_<output file name>`; collisions get `_2`, `_3`. `--naming` overrides by tool id.
- **Variables** use the same words without the layer: `orders_filtered_by_status`, `orders_with_year`, `orders_by_country_year`, `orders_unmatched_left`. A stream read from a table uses the table name. Tool ids are not in the code - `coverage.json` maps every tool id to its variable.
- What happened: filter -> `filtered_by_<first field in the condition>`, formula -> `with_<first new field>`, join -> `joined_<right source>` / `unmatched_left`, summarize -> `by_<group fields>` or `totals`, union -> `combined`, unique -> `unique` / `duplicates`, select -> `selected`, others by tool kind.

## Code style
- **No comment lines** inside cells; review detail lives in the report. Headings are one line of Markdown per table.
- **Plain PySpark.** Every tool is written inline with the DataFrame API (`filter`, `withColumns`, `select`/`alias`, `join` on `F.col('l.A') == F.col('r.B')`, `groupBy().agg()`, `unionByName`, `Window`); no helper module is imported. Independent formulas share one `withColumns({...})`; casts are skipped when the expression already has the declared type; literals are plain Python values; long expressions wrap inside their brackets and long conditions get a `<name>_condition` variable.
- **Work that is not executed.** A Sort that does not feed a final table is omitted (Unique, Record ID, Running Total and Multi-Row sort explicitly where they need order) and a Select that keeps every field unchanged is omitted. Cheap row-level steps (filter, formula, select, sort) that several tables need are recomputed in each rather than getting a table of their own; joins, unions, unique, aggregations and window steps that are shared get one table.
- **Only needed tables are read** by a unit; the validator checks every `T['x']` is declared in `00_config` and written before it is read.
- **No placeholders.** A blocked tool (and everything downstream of it) is not in the notebooks at all - no `unsupported(...)`, no `TODO`. The last convertible step before it is materialised as a table so the upstream work is usable now; the notebooks and job carry a PARTIAL CONVERSION banner until nothing is waiting (`references/09-preflight.md`).

## Write behaviour
`write_table(df, table)` (defined in `00_config`): `overwrite` only when `allow_overwrite=true` (default **false**) and never with `overwriteSchema`; Delta history keeps the previous version. Full refresh by default (`WRITE_MODE`). Streaming APIs, SDP decorators, `toPandas`, UDFs and `collect()` on data are never generated (`validate_output.py` enforces it).

## Overrides (human decisions) - `--overrides overrides.json`
```json
{ "88":  {"join_keys": [{"left_field": "Region", "right_field": "KommuneNr"}]},
  "692": {"output_field": "Endring_pst", "order_by": [{"field_name": "Region", "ascending": true}]},
  "806": {"custom_python": "{out} = cleanse({in0})"},
  "1601":{"header_field": "Hyttetype", "value_field": "Antall"} }
```
Keys are IR field names merged over the node; overridden tools stay *Requires review*. `custom_python` placeholders: `{in0}`, `{in1}`, ... (inputs in edge order) and `{out}`; define the called function in a notebook cell or helper module.

## Input tools without a converter
An unsupported tool with no inputs whose name says input/reader (for example `big_query_input`) is treated as a **Bronze source**: its query (`inputCustomFilter`, `query`, `sql`) is documented in `01_bronze`, its table name is taken from the query, and the tool stays *Requires review* ("confirm the table holds this tool's query result"). Its original definition is kept in `unsupported_tools.xml`. Output tools without a converter mark their input as a Gold output.

## Review checklist before handing code to the user
1. `validate_output.py` clean (syntax, banned APIs, undefined variables, table flow). 2. Blocked/unsupported list triaged with the user. 3. Bronze tables exist and hold what `01_bronze` says. 4. No claim of equivalence beyond what `coverage.json` shows. 5. Report narrative added.
