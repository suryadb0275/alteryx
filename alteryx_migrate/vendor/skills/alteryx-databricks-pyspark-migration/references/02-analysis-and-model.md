# Phase 1-2: analysis procedure and the internal transformation model

Run `scripts/analyze_workflow.py`; it builds the model below. Read its summary, then decide what to open next.

## The model (one entry per tool; see `wfmodel.py`, `codegen.py::NodeCode`)
| Field | Source |
|---|---|
| tool id, type, kind | IR node (`NODE_SPECS` maps IR node types to logical kinds: read, filter, join, ...) |
| inputs / outputs by anchor | edges, ordered by position in the file |
| configuration + expressions | IR node fields; expressions re-parsed from original text |
| schema (partial) | propagated type categories (string/number/bool/date) - only where the workflow declares types |
| record order provenance | nearest upstream Sort through order-preserving tools; `None` after joins/unions/summaries |
| layer / unit / table | `plan_medallion` |
| generated code | `Emitter.e_<kind>` |
| status, notes, reviews | emitter; see "Status rules" |

## Procedure
1. **Reconstruct**: sources (Read + TextInput), sinks (Write), containers (visual), dead-end tools (no path to a Write), topological order, cycles.
   *If no Write tool exists* the dangling tools are treated as outputs and the report says so (assumption to confirm).
2. **Detect lost/corrupt configuration** per tool kind (`REQUIRED_CONFIG`, `config_issues`). These become *blocked* tools.
3. **Audit expressions**: every Formula/Filter/Multi-Field/Multi-Row expression goes through `alteryx_expr.translate`; failures become *blocked*; semantic caveats become *notes* or *reviews*.
4. **Order sensitivity**: tools whose result depends on record order (Multi-Row Formula, Unique, Record ID, Running Total, First/Last/Concat) need an explicit order. Spark has no inherent order, and
   Alteryx order after a Join/Summarize/CrossTab is itself implementation-defined. The emitter uses an upstream Sort when one exists; otherwise the tool is *blocked* (Unique may accept "any row").
5. **Dependencies**: Join without Left/Right input, Append without Source, dangling edges -> reported as unresolved dependencies.
6. **Medallion plan** (`05-medallion-design.md`).

## Status rules (applied by `NodeCode.need`)
- `blocked`: required configuration empty/corrupt (after XML recovery), an expression cannot be translated, or a row-order key is missing -> the tool is **not converted** and no code is written for it.
- `waiting`: a tool downstream of a blocked tool -> not converted until the blocker is resolved.
- `unsupported`: no verified PySpark equivalent exists (macros, spatial, predictive/forecast, Summarize `Mode`, recursive Multi-Row formulas) -> treated like `blocked`.
- `requires_review`: code is generated but a semantic difference or ambiguity exists (string collation of Sort, Unique/RecordID without explicit order, `StdDev` sample/population, mixed AND/OR, human-supplied overrides, ...).
- `high_confidence`: semantics understood and implemented, nothing ambiguous. **Never promoted to Verified automatically.**
- `not_applicable`: Browse, comments.
- *Verified* requires a passing reconciliation against an Alteryx baseline (`07-validation-and-reconciliation.md`); record it by editing `coverage.json`/the report after the comparison, naming the baseline used.

## What to extract for the user-facing explanation (section A/B of the report)
Comment/TextBox text (business prerequisites - quote verbatim), output file names, source tables from queries, the filters that encode business rules (e.g. status = 'OK'),
the grain-changing tools (Summarize/CrossTab group-by lists) and the final Sort/Select that define the output shape. Do not state business meaning the file does not support.
