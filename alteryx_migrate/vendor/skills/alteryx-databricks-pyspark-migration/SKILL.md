---
name: alteryx-databricks-pyspark-migration
description: Converts an Alteryx workflow (.yxmd plus its parsed JSON from parse_and_map) into Databricks PySpark notebooks (.ipynb) organised as Bronze, Silver and Gold Delta layers, focused on the transformations (inputs are existing tables, nothing is ingested). Runs a mandatory pre-flight first: scans every tool, reads settings the parser lost from the XML, asks ONE grouped question list (blocking / recommended / confirm), then converts only the tools that are not blocked and reports honestly what was and was not converted. Use when the user supplies an Alteryx workflow or mapping.json and wants it migrated, explained, analysed or converted to Databricks notebooks, PySpark, Delta or a Medallion layout. Batch notebooks only; never generates Lakeflow Declarative Pipelines (SDP/DLT) code.
---

# Alteryx workflow -> Databricks PySpark notebooks (Bronze / Silver / Gold)

Generates **standard Databricks notebooks** (`.ipynb`; DataFrame API, Delta, batch) that concentrate on the transformations: Bronze tables are inputs, not ingested. It does **not** produce Lakeflow
Declarative Pipelines / DLT / SDP code - if the user asks for that, use the separate `alteryx-to-sdp` skill instead.

## Phases (in order; never skip the pre-flight)

| Phase | What happens | Tool / reference |
|---|---|---|
| 0 Parse | `.yxmd` -> mapping JSON | `parse_and_map/parse_and_map.py <file.yxmd> --json -o mapping.json` |
| 1 **Pre-flight** | scan every tool, recover lost settings from the XML, classify BLOCKED, ask ONE grouped list of questions, **stop** if anything blocking is missing | `scripts/generate_notebooks.py mapping.json OUT --yxmd file.yxmd` (exit code 3 = stopped; `OUT/PREFLIGHT.md`) |
| 2 Convert | after the user answers: convert only tools that are not blocked; list the rest as waiting for input | same command + `--answers answers.json` |
| 2b Embedded Python | a Python (Jupyter) tool is blocked in phase 2; with the user's approval it is converted cell by cell by a model, under rules checked by code | `references/10-python-code-tools.md` |
| 3 Validate | static checks, local Spark tests, reconciliation plan | `scripts/validate_output.py`, `tests/` |

## Pre-flight rules (non-negotiable)

Full detail: `references/09-preflight.md`. In short:

1. **Scan the whole workflow before any code.** Tools are BLOCKED when: (1) a macro (.yxmc)/custom tool is used and its file is not provided; (2) a required setting is empty/unreadable; (3) a function cannot be translated with certainty; (4) the tool depends on row order (Multi-Row, Running Total, Unique, Record ID) and no order key exists; (5) an external source has no schema/sample; (6) a helper module/library is not provided; plus a flag when one tool's two outputs feed the same Union. Everything downstream of a blocked tool *waits for input*.
2. **Read before you ask.** If a setting exists in the `.yxmd` XML but the parser missed it (join keys, Cross Tab/Running Total/Transpose fields, Multi-Row output field, Dynamic Rename formula, DateTime settings, Generate Rows expressions, input schemas) it is read from the XML (`scripts/xml_recover.py`) and **never asked**. Always pass `--yxmd`.
3. **Ask once, grouped:** `1. BLOCKING`, `2. RECOMMENDED`, `3. CONFIRM` (project settings with a proposed default). Each item names exact tool ids/fields, what to send, and what happens if there is no answer. Project items always confirmed: run date + time zone, catalog/schema/table names, write mode + overwrite permission, Spark runtime + ANSI, numeric rules, text-matching case rules, validation data (frozen snapshot + original Alteryx output).
4. **No placeholders.** Never emit `unsupported(...)`, empty configs or `TODO`; blocked tools and their downstream are simply not in the notebooks (the validator rejects placeholder code). Tools upstream of a blocker *are* converted.
5. **Never present a partial conversion as finished or production-ready.** The notebooks carry a PARTIAL CONVERSION banner; the report ends with: tools converted / total (%), blocked tools + input still needed, assumptions (the user's answers and defaults), known risks (type and rounding, date and time zone, row order, nulls), validation status (not run / matched / mismatched with evidence).
6. **Never claim equivalence without proof.** Say "output matches Alteryx" only after comparing real data; until then "converted, not yet validated".

Other rules: semantic equivalence beats performance (optimisations are *estimated* unless measured); do not trust the JSON's `spark_sql` hints (`scripts/alteryx_expr.py` re-translates the original text); never touch the repository's read-only reference folders `alteryx/`, `pyspark/` and `example/`; writes are guarded (overwrite refused unless `allow_overwrite=true`); no `collect()`/`toPandas()`/UDF/streaming in generated code.

## Procedure

```bash
# 0 parse
python parse_and_map/parse_and_map.py "wf.yxmd" --json -o OUT/mapping.json
# 1 pre-flight (also analysis; prints the grouped questions and exits 3 when blocking items exist)
python scripts/generate_notebooks.py OUT/mapping.json OUT/notebooks --yxmd "wf.yxmd"
#   -> relay OUT/notebooks/PREFLIGHT.md to the user VERBATIM and STOP. Do not answer for them.
# 2 after the user replies (fill OUT/notebooks/answers.template.json; {} = "convert whatever is not blocked")
python scripts/generate_notebooks.py OUT/mapping.json OUT/notebooks --yxmd "wf.yxmd" --answers answers.json \
    [--naming naming.json] [--silver-handoff auto|persist|inline] [--max-unit-nodes N] [--format ipynb|py] [--test-results tests/results/last_run.json]   # last_run.json is written by `pytest tests`; optional
# 3 validate
python scripts/validate_output.py OUT/notebooks
```
Spark SQL form of every table (needs input columns for database inputs): `references/13-spark-sql-output.md`. **What every Alteryx tool does and how to replicate it in PySpark / Spark SQL** (all 323 tools of the Designer tool list: settings, recipe, what the help leaves unstated; recipes are tested against the help pages' own worked examples), plus which tools the skill converts, partly converts, or reports: `references/14-tool-coverage.md` (generated from `scripts/tool_guide.py` + `scripts/tool_coverage.py`). A tool without a converter is reported with this recipe and its original XML settings in the hand-off - never dropped, never guessed.
Optional first look (small output, never open the multi-MB JSON directly): `python scripts/analyze_workflow.py OUT/mapping.json --yxmd "wf.yxmd"`.

Read only what is needed: tool semantics `references/03-tool-mapping.md` (per-tool help notes, gaps and rules for every audited tool: `references/12-tool-help-notes.md`); formulas `references/04-expression-translation.md`; JSON/XML shapes `references/01-input-format.md`;
the existing Alteryx skill on demand (`references/00-reference-skills.md`).

Produced in `OUT/notebooks`: `.ipynb` `00_config`, `01_bronze`, `02_silver_gold` (one section per Alteryx container; `02_silver` + `03_gold` when there are none), `04_quality_reconciliation`, `databricks_job.json`,
`MIGRATION_REPORT.md` (Status first, then sections A-I), `coverage.json`, `PREFLIGHT.md`, `preflight.json`, `answers.template.json`, `unsupported_tools.xml`.
Tools that package the conversion themselves (the datalab bridge) call `scripts/unit_functions.py`: the plain-PySpark body, inputs and result variable of every output table, with the blocked tools as root causes - the same conversion as the notebooks, no notebook wiring. Each table also carries `sql`: the same conversion as Spark SQL (`CREATE TABLE ... AS WITH ...`), made from the PySpark by `scripts/sqlframe.py` and tested against it; a table without SQL says why and keeps its PySpark. Needs the input columns (`answers.schemas`): `references/13-spark-sql-output.md`.
Layout, naming and cell conventions: `references/06-notebook-generation.md`; table design: `references/05-medallion-design.md`; validation: `references/07-validation-and-reconciliation.md`;
performance/operations: `references/08-performance-and-operations.md`.

## What to tell the user

- **Pre-flight stop:** the grouped message exactly as written in `PREFLIGHT.md`, plus where `answers.template.json` is. Nothing else.
- **After a conversion:** tools converted / total as a %; blocked/waiting tools each with reason and input still needed; assumptions (copy the user's answers and the defaults);
  known risks; validation status; files created. Do not paste notebook code into the chat.

## Folder map (progressive disclosure - open only what the phase needs)
```
SKILL.md                               this file
scripts/                               generate_notebooks.py (entry point), analyze_workflow.py, validate_output.py
                                       and its modules: preflight, xml_recover, wfmodel, codegen, notebooks, report,
                                       alteryx_expr, pretty, unsupported_xml
references/00-reference-skills.md      which parts of the sibling alteryx / pyspark skills to open, and how
references/01-input-format.md          JSON schema, anchors, converter defects, what the XML recovers
references/02-analysis-and-model.md    analysis procedure, status rules
references/03-tool-mapping.md          per-tool PySpark mapping, semantic differences, tests
references/04-expression-translation.md operators, precedence, functions, pitfalls
references/11-alteryx-function-reference.md every Alteryx function/operator and what it means (the source of truth for semantics)
references/12-tool-help-notes.md       per-tool notes from the Alteryx help: what it states, gaps, rule for each audited tool and category
references/05-medallion-design.md      zone and table rules, decisions to confirm
references/06-notebook-generation.md   file layout, naming, conventions, overrides
references/07-validation-and-reconciliation.md evidence levels, reconciliation procedure, tests
references/08-performance-and-operations.md optimisation checklist, logging, deployment
references/09-preflight.md             blocked rules, question format, answers.json, partial conversion
references/10-python-code-tools.md     embedded Python tools: briefing, JSON reply contract, rules, checks
assets/                                answers / naming / overrides examples, report addendum checklist,
                                       mini_workflow.json + mini_answers.json (small workflow used by the tests)
tests/                                 pytest suite on local Spark (see tests/README.md); not needed to use the skill
```

## Extending
New tool kind: add `e_<kind>` in `scripts/codegen.py` (it writes plain PySpark inline), an XML extractor in `scripts/xml_recover.py` if its settings can be lost, a test in `tests/`, a row in
`references/03-tool-mapping.md` and `KIND_TESTS` in `scripts/notebooks.py`. New Alteryx function: add `f_<name>` in `scripts/alteryx_expr.py` with a case in
`tests/test_expr_translation.py`. Unknown behaviour must stay blocked until its Alteryx semantics are confirmed.
