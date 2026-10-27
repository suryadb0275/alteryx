# Pre-flight check, blocked tools, answers and partial conversion

Implemented in `scripts/preflight.py` (+ `scripts/xml_recover.py`); run automatically by `generate_notebooks.py` before anything is written.

## Step 1 - classification of every tool
`BLOCKED` means no code is generated for the tool; its downstream tools are `WAITING`. Rules and where each is detected:

| # | Blocked when | Detected by | Typical question |
|---|---|---|---|
| 1 | macro (.yxmc) / custom / third-party tool and the macro file is not provided | `UnsupportedNode`/predictive node with `macro_file` (read from the XML `EngineSettings@Macro`) | send `Cleanse.yxmc` (or name a PySpark replacement in `answers.macros`) |
| 2 | a required setting is empty or unreadable | `wfmodel.config_issues` **after** XML recovery | the value, or the original .yxmd |
| 3 | a function cannot be translated with certainty | translator `Unsupported function X()` | what it must return / Alteryx sample |
| 4 | depends on row order and no order key exists | Multi-Row, Running Total, Unique, Record ID without an upstream Sort / `order_by` | the order field(s); for Unique "any row" (`accept.order_any`) |
| 5 | external source with no schema or sample | Read tool without `field_info` (database inputs carry `MetaInfo` in the XML; BigQuery inputs do not) | columns + types, or "resolve at runtime" (`accept.source_schema_at_runtime`) |
| 6 | a helper module/library is not provided | `custom_python` that calls functions not listed in `answers.helpers` (the generated notebooks need no helper module) | the module that defines them |
| 7 | (flag, RECOMMENDED) one tool's two outputs feed the same Union | Union with two edges from one tool | confirm both are meant to be stacked |

Also blocked with a specific reason: Join by record position, Join Multiple (colliding-field naming unverified), Cross Tab with several methods, Dynamic Rename modes other than Formula, Multi-Field Formula copy-output, recursive Multi-Row formulas, spatial and forecasting tools, Generate Rows outside the supported pattern.
Output tools without a converter (a BigQuery writer) are **not** blockers: they are a CONFIRM item (default: the data lands in a Delta Gold table).

## Read before you ask (`xml_recover.py`)
With `--yxmd` these are filled from the XML and never asked: Join keys (+ by-position flag), Join Multiple inputs, Cross Tab group/header/data/methods, Running Total fields, Transpose key/data fields (+ `*Unknown` flag), Multi-Row output field/type/group/other-rows, Dynamic Rename mode/formula/fields, DateTime settings, Generate Rows expressions, Formula fields (an empty `FormulaFields` is confirmed as a pass-through), input schemas (`MetaInfo`), macro file names.
Values already in the JSON are kept unless known to be corrupt (e.g. Multi-Row `output_field = "False"`, Transpose data fields).

## Step 2 - project items (always CONFIRM, each with a proposed default)
Run date and time zone (default UTC, current Databricks run date, report covers the current month); catalog/schema/table names (`main`, `bronze|silver|gold`, generated names); write mode and overwrite permission (overwrite, **not** allowed until `allow_overwrite=true`); Spark runtime and ANSI (DBR 16.x, ANSI off; code is ANSI-safe); numeric rules (decimal cast rounds half up, divide-by-zero NULL, NULL propagates); text matching (Contains/StartsWith/EndsWith case-insensitive as in the Alteryx reference; `=` case-sensitive); validation data (none -> "converted, not yet validated").
These settings flow into `00_config` (time zone, ANSI, catalog/schemas, write mode, overwrite guard) and the translator (text matching).

## Step 3 - the message
One message, three groups, each item `[tool ids - label]` + what is missing, why it matters, what to send, and **what happens if there is no answer**:
`1. BLOCKING - needed before I can start`, `2. RECOMMENDED - I can start without these, but accuracy is at risk`, `3. CONFIRM - project settings with my proposed default` ("reply ok to accept").
Written to `PREFLIGHT.md` (+ `preflight.json`, `answers.template.json`). With blocking items and no `--answers`, the generator prints it, writes nothing else and exits 3.

## answers.json
```json
{ "overrides": {"<tool id>": {"order_by": [{"field_name": "Date", "ascending": true}], "join_keys": [...]}},
  "macros":    {"Cleanse.yxmc": {"custom_python": "{out} = my_cleanse({in0})"},
                "GsheetReader.yxmc": {"as_source": true, "table": "optional_bronze_table"}},   # a macro with no inputs that only reads a source (Google Sheet, table): becomes an input table, no file needed
  "helpers":   ["my_cleanse"],
  "accept":    {"order_any": [19, 53] , "source_schema_at_runtime": true, "defaults": true},
  "project":   {"timezone": "Europe/Oslo", "catalog": "main", "bronze_schema": "bronze", "silver_schema": "silver", "gold_schema": "gold",
                "write_mode": "overwrite", "allow_overwrite": false, "ansi": false, "text_match_case_sensitive": false},
  "validation":{"input_snapshot": "catalog.schema.snapshot_*", "alteryx_output": "path or table"} }
```
`{}` is valid: "convert whatever is not blocked". Overridden tools stay *Requires review*.

## Step 4 - after the answers
Only unblocked tools are converted. The planner materialises the last convertible step before every waiting tool, so everything upstream is usable now. A unit that contains any blocked or waiting tool is not generated. The config notebook, job JSON and report say **PARTIAL CONVERSION - not production-ready** until nothing is blocked or waiting.
The report's **Status** section ends every run: tools converted / total (%), blocked tools with reason and input still needed, assumptions (answers + defaults, marked unconfirmed until `accept.defaults`), known risks (type and rounding, date and time zone, row order, nulls), validation status.

## Step 5 - equivalence
"Output matches Alteryx" is allowed only after `04_quality_reconciliation` compared real data. Until then: **converted, not yet validated**.
