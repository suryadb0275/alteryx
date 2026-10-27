# Embedded Python / Jupyter code tools -> PySpark (cell by cell)

Applies when a workflow contains a Python (Jupyter) tool. `scripts/generate_notebooks.py` never converts these: it lists the
tool as BLOCKED ("embedded Python code") because the script is hand-written business logic. A caller that has the user's approval
(the orchestrator, or you in a conversation) converts the script **with the rules below**, one batch of cells at a time, and the
result is checked by code before it is accepted. Everything here is judgement work for a model; everything that can be checked
by code is checked by code (`scripts/validate_output.py` rules plus the checks listed at the end).

## What the caller must give you (the briefing)

1. **Workflow map** - outputs, sources, the Python inputs table (`#n -> DataFrame -> upstream tool -> source table`). If the
   table says `UNVERIFIED`, the `#n` numbers came from file order and may be wrong: say so in `notes` and raise a question
   instead of relying on a number.
2. **Decisions** - target DataFrame API (default: PySpark DataFrame API), medallion layer and table name for every input and
   every output, catalog and schemas, write mode and whether overwrite is allowed.
3. **Registry entries** for every DataFrame name the cells use that an earlier cell or tool defined: name, columns (or
   `null` if unknown) and origin. Columns you are not given are **unknown**: never invent them.
4. **Interface of already converted cells** (names and columns only, not code). Do not convert them again.
5. **The cells to convert**, each with its index and original line range.

If something you need is not in the briefing, ask a question (below). Never fill the gap yourself.

## Reply contract (exactly one JSON object, nothing else)

```json
{"cells": [{"cell": 7, "code": "df_x = spark.table(\"main.silver.x\")", "defines": [{"name": "df_x", "columns": ["A", "B"], "origin": "cell 7"}], "notes": ""}],
 "questions": [{"id": "q1", "text": "...", "options": ["a", "b"], "default": "a", "blocking": true, "cells": [9]}]}
```

- **One entry in `cells` for every input cell, in the same order.** A cell with nothing to convert (comment, `.head()`, display
  only) returns `"code": ""` and a short note saying why; it is never silently dropped.
- `defines` lists every DataFrame or variable the cell creates or replaces, with its columns when they can be determined from the
  code and the registry (`null` otherwise).
- A cell you cannot convert without guessing goes into `questions` and its `code` stays `""`. A question needs a recommended
  default and says whether it blocks. Do not write `TODO`, `pass`, `...`, `raise NotImplementedError` or comments such as "convert
  later": the validator rejects them.

## Conversion rules

- Keep the original variable names so that later cells and the registry stay valid.
- `Alteryx.read("#n")` becomes `spark.table("<table from the decisions for input n>")`. `Alteryx.write(df, k)` becomes
  `df.write.format("delta").mode("<mode from the decisions>").saveAsTable("<table for output k>")`; use `overwrite` only when the
  decisions allow it. Plain PySpark only: no helper module is imported.
- Spark DataFrame API only. Forbidden as in the rest of this skill: `collect()`, `toPandas()`, UDFs and `pandas_udf`, `.rdd`,
  streaming, SDP/DLT. `import pandas` is not allowed in converted code; if a cell genuinely needs a pandas-only feature, that is a
  question.
- Pandas semantics differ from Spark: row order is undefined without an explicit `orderBy`; `NaN` and `NULL` differ; the pandas
  index does not exist; `merge` suffixes, `groupby` null handling, `sum` of all-null groups, string-to-date parsing and integer
  overflow all behave differently. State the choice you made in `notes` whenever one of these applies.
- Cells that read a name only (inspection) do not need conversion.

## Never translate mechanically - ask

`iterrows` / `itertuples` / row loops, `apply(axis=1)`, `interpolate`, positional `iloc` / `loc` in loops, `shift` or `cumsum`
without a clear order key, chained in-place mutation whose order matters, `scipy`, `sklearn` and any statistics or model fitting,
reading local files, network calls, randomness without a seed. Each becomes a question with at least two options (for example
"window function over this order key" / "keep as a reviewed manual step") and a recommended default.

## What is checked by code before the result is accepted

1. Every input cell has exactly one output entry, in order.
2. The converted code compiles, uses no name before a cell defines it (registry and earlier cells count as defined), and contains
   no remaining `Alteryx.` call, `ayx` import, `toPandas`, `collect`, UDF, `TODO` or placeholder.
3. A reviewer with a fresh context compares each cell with its original and reports differences; unresolved differences fail the
   batch and the exact issues are sent back for a retry.
4. Passing these checks means "converted, not yet validated": equivalence with Alteryx output is only claimed after real data was
   compared.


## Using the converted script in the workflow (overrides)

The ported program replaces the Python tool so that the tools AFTER it convert by rule. Give it to the skill as an override on the Python tool's id:

```json
{"overrides": {"17": {"custom_python": "{out_Output1}, {out_Output2} = python_tool_17.run(spark, {in0})", "ported_python": true}}}
```

* `{in0}`, `{in1}` ... are the DataFrames of the tool's input connections in connection order; `{out}` is the first output and `{out_Output<k>}` the output anchor `Output<k>`
  (only anchors some tool reads exist); any other brace in the text must be doubled (`{{` `}}`).
* `kept_python: true` marks code that is the script's ORIGINAL pandas code in a component file (`run(spark, input_1, ...)` converts with `toPandas()`, runs the cells unchanged, returns Spark
  DataFrames): flagged `requires_review` (runs on the driver; NaN becomes NULL), columns unknown, lines not re-wrapped, no helper check. This is the exact-logic route when the data fits one machine.
* `ported_python: true` marks the code as written by a model: it is flagged `requires_review`, its output columns are treated as unknown, the generator does not re-wrap its lines, and
  pre-flight does not apply its helper-function check (the code is checked cell by cell instead). Without the flag the text is a human-supplied snippet.
* A component file (`python_tool_17.py` with `run(spark, input_1, ...)`) keeps the notebook short; inline code works the same way.
