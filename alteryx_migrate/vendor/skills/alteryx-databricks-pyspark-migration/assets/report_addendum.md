# Narrative to add to MIGRATION_REPORT.md after generation

`generate_notebooks.py` fills every table and count from the data. These paragraphs need judgement and must only use facts from the file or from the user. Delete any heading you cannot support.

## A. Purpose (3-5 sentences)
- What the workflow produces (output names, grain, audience) - cite the comment tool text and output file names.
- The business rules encoded in its filters (quote the expressions that implement them).
- What it explicitly does *not* state (say "not stated").

## Decisions taken / needed
| # | Decision | Proposed | Owner | Needed before |
|---|---|---|---|---|
| 1 | Bronze: existing tables or ingest? | | | first run |
| 2 | Values for tools listed as blocked (keys, field names, macros) | | | any run |
| 3 | Table / schema / catalog names | | | deploy |
| 4 | File delivery of former Excel/CSV outputs | | | go-live |
| 5 | Alteryx baseline outputs for reconciliation | | | sign-off |

## Execution record (fill with facts only)
- Generated with: `generate_notebooks.py` options used.
- Static validation: result of `validate_output.py`.
- Skill tests: command, date, passed/failed/skipped counts.
- Databricks run: **not executed** / executed on <workspace> at <date> with result.
- Reconciliation: **not run** / baseline <name>: passed / failed (counts).
