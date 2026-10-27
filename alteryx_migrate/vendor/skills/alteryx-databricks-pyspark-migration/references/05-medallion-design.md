# Medallion design rules (`wfmodel.plan_medallion`)

The layout follows the workflow's structure; tools are **not** mapped one-to-one to tables. Every rule below is deterministic and reversible via options - present the resulting plan to the user as a proposal.

## Zones (per tool)
- **Bronze** - Input tools. One Bronze table per distinct (connection, normalised query) or file source; identical queries share a table (the sample's 30 live Input tools reduce to 19 Bronze tables). Names come from the first table in the query: `bronze_<table>`; a different query on the same table gets `_q2`, `_q3`. Silver/Gold names are derived, not invented: `<layer>_<nearest source tables>_<last step>` for intermediate tables (step noun: enriched, filtered, joined, combined, deduped, summary, ...) and `gold_<output file name>` for Write inputs.
- **Silver** - record-level tools that precede any grain-changing tool (Summarize, Cross Tab): filters, formulas, selects, joins, unions, uniques. Source grain is preserved.
- **Gold** - any tool at or downstream of a grain change (an input of the tool is already Gold, or the tool is Summarize/Cross Tab).
- Dead-end tools (no path to a Write) are reported and not generated. TextInput data is embedded in the unit that uses it (small reference data).

## Where tables are materialised (cut streams)
1. Bronze sources.
2. Silver streams consumed by two or more downstream tools **when the step carries real work** (join, union, unique, append, aggregation, window/order-dependent tools, macros). Cheap row-level steps (filter, formula, select, sort) are recomputed where needed instead of getting a table, which keeps Silver small.
3. The Silver->Gold hand-off - mode `auto` (default): only when the Silver chain behind it contains real cleansing/integration work (join, union, unique, append, aggregation, filter, multi-row, record id, running total); trivial formula/select/sort chains are inlined into the consumer. `persist` keeps all, `inline` drops all.
4. Inputs of Write tools -> Gold tables, named from the output file (`gold_<file_name>`).
5. Nodes with real work that two units would otherwise both compute: promoted at the **frontier** of the shared region only (named like any other table, e.g. `silver_orders_joined_customers`).
6. Optional plan-size splits (`--max-unit-nodes N`): oversized units are cut only at *closed* points (no outside consumer of the upstream part), so nothing is computed twice. If no clean split exists the report says so.

Everything else stays an in-memory DataFrame inside a `build_<table>()` function.

## Per-table documentation (generated into the notebook Markdown and report section D)
Purpose and layer, why materialised, upstream tables, tool mix, **grain/keys only where the workflow states them** (Summarize group-by fields, Unique keys, Cross Tab group fields - found by walking back through row-level tools), refresh = full recompute (Alteryx has no incremental logic), consumer = the original output file for Gold.
Never invent business keys, dimensions/facts or relationships. If the user wants a star schema, that is a separate design step: propose it from the Summarize/Join structure and mark it as a proposal.

## Decisions to confirm with the user
1. Who lands the Bronze tables? The notebooks only read them (see `01_bronze`); the JSON contains only Alteryx connection aliases or BigQuery queries, so the data team must provide tables that hold exactly what each input tool read.
2. Hand-off policy and business table names (`--naming naming.json`: tool id -> name).
3. Unit size / checkpointing for very large workflows (the sample's biggest Gold unit has ~1,100 tools; split at 150 leaves one unit of ~560 with no clean split).
4. Whether outputs previously delivered as xlsx/csv still need file delivery.
5. Catalog/schema names and clustering keys (not derivable from the JSON).

## Refresh strategy
Default full refresh (`overwrite`, guarded) because the Alteryx workflow reads and recomputes everything each run. Incremental/CDC is possible only when sources expose a change column - not in the JSON; propose it as follow-up work, do not generate it.
