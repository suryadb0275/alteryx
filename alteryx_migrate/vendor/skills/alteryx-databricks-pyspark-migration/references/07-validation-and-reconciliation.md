# Validation and reconciliation

Generation and validation are separate. State at each handover what was executed.

## Evidence levels
| Level | Meaning | Who/what assigns it |
|---|---|---|
| Static | code parses, obeys hard rules (`validate_output.py`) | automatic |
| Skill tests | the PySpark written per tool and the expression translator behave as the documented Alteryx semantics on synthetic data (`tests/`, local Spark, both ANSI modes) | pytest; recorded in `tests/results/last_run.json`; passed to the generator with `--test-results` to fill "verified by skill tests" (tool-kind level, only for tools without review flags) |
| **Verified** | the migrated output equals the output of the original Alteryx workflow on the same input | **only** a reconciliation run against an Alteryx baseline; never assigned automatically |

## Reconciliation procedure (needs the user's data)
1. Run the original workflow in Designer on a frozen input snapshot; export each output (csv/parquet) - or use `AlteryxEngineCmd.exe` (see the Alteryx skill's `Invoke-AlteryxWorkflow.ps1`).
2. Load the exports as baseline tables; load the same snapshot into the Bronze tables.
3. Set `EXPECTED_BASELINES = {"gold_<name>": "catalog.schema.baseline_table"}` in `04_quality_reconciliation` and run it. The notebook compares the common columns as multisets (`exceptAll` both ways) and lists missing columns.
4. Intermediate checks when an output differs: compare `silver_*` tables with Browse-tool exports at the same point (record counts first, then keys, then values) to bisect to the first diverging tool.
5. Record outcomes: tool ids covered by passing reconciliations may be marked *Verified* in the report, naming the baseline snapshot and tolerance.

## Checks generated or recommended
| Check | Where |
|---|---|
| Schema and types | compare `df.schema` with the baseline's schema; Select/Formula casts documented per tool |
| Row counts per table and per rejected stream (Filter False, Join L/R, Unique Duplicates) | `04` (counts); compare with Alteryx record counts from the run log |
| Duplicate-key checks | generated for grains the workflow states (Summarize group-by, Unique keys) |
| NULL checks | `df.filter(F.col(..).isNull()).count()` per business-required column |
| Join validation | counts of J/L/R must add up: `J_left_rows + L == left_rows` when right keys are unique; investigate duplicate right keys (rows multiply) |
| Aggregates / KPIs | reconcile sums/counts of Gold vs baseline (`reconcile` covers every column) |
| Boundary / NULL behaviour | synthetic cases in `tests/` (NULL join keys, NULL filter conditions, empty groups, ties) - extend with cases from real data issues |
| Numeric precision and dates | set `float_tol` explicitly; compare Alteryx FixedDecimal columns as `decimal`; compare date strings vs date types after normalising |
| Ordering | only where the Alteryx output order matters: sort both sides explicitly before comparing; otherwise compare as multisets |

## Running the skill's own tests
```powershell
$env:ALTERYX_MIG_JAVA_HOME = "<path to JDK 17 or 21>"     # Spark needs a JVM; tests skip (not pass) without it
python -m pytest tests -q
```
Windows note: tests use global temp views (`MIG_TABLE_FORMAT=tempview`) because a local Hadoop warehouse needs winutils. On Linux/macOS or Databricks use Delta tables.
What the suite covers: ~130 expression cases x 2 ANSI modes, the code written for every tool (filter, select, sort, join J/L/R, union, summarize, unique, append, text-to-columns, transpose, record id, running total, cross tab, multi-row, dynamic rename, generate rows, odd field names), the write guard and the baseline reconciliation, an
end-to-end run of generated notebooks from `assets/mini_workflow.json` compared with hand-derived rows, fail-safe behaviour for lost configuration/macros, and generation on the real 1,533-tool sample.
What it does not cover: Databricks/Delta runtime behaviour, JDBC ingestion, any equivalence on real workflow data, performance.
