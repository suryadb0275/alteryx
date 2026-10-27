# Performance optimisation and production readiness

Principle: **correctness first; measure before optimising.** The skill has no cluster, so every item is labelled *estimated* unless the user supplies run evidence (Spark UI / query profile / `DESCRIBE HISTORY`). Do not cache every DataFrame, repartition every table or add tables for their own sake.

## Review checklist applied to generated code (state the finding and its basis)
| Topic | What to look at | Typical action | Basis |
|---|---|---|---|
| Shuffles | Joins, Summarize, Unique, Sort, windows | Remove Sorts that no later tool depends on (Alteryx habit); keep the Sort when a window/Unique needs the order | structure (estimated) |
| Join strategy | one side small? | AQE broadcast handles most; add `F.broadcast(small)` only after seeing sizes; Append Fields source is already guarded | needs table sizes |
| Skew / high-cardinality | group-by or join keys with hot values (e.g. NULL or a default code) | AQE skew join; salting only if the UI shows skew | needs data |
| Window tools | Record ID, Running Total, Multi-Row without group fields | partition by a business key where the logic allows; a global window runs on one task | structure |
| Pushdown | JDBC `query` in Bronze | keep filters in the source query; add `partitionColumn/lowerBound/upperBound/numPartitions` via `dbtable` subquery for large tables | needs table stats |
| Repeated computation | stream consumed by several tools | cut-point tables (planner) or `.cache()` inside a unit **only** if reused by several actions | measured only |
| Plan size | units with hundreds of tools | `--max-unit-nodes`, checkpoint tables at closed cut points; avoid very long `withColumn` loops (the Formula emitter uses one per formula by necessity) | structure |
| Small files | many tiny Gold tables, repeated overwrite | enable auto-optimize / predictive optimisation; `OPTIMIZE` after bulk loads | Databricks feature |
| Layout | filter/join keys on large tables | liquid clustering on the keys users filter by; avoid partitioning below ~1 TB/table | needs access patterns |
| Incremental | sources with a change column | MERGE/`replaceWhere` loads for Silver; not derivable from the JSON | proposal only |
| Columnar pruning | `select` early | Select tools already prune; a Select keeps unknown fields - set `select_all_unknown=false` upstream if the source is wide | structure |

## Operations
- **Logging**: a failed step fails its notebook cell and the job task with the Spark error; Databricks job runs keep status and duration. Table writes carry Delta history (user, operation metrics).
- **Failure handling**: blocked/unsupported tools raise `NotImplementedError` naming the tool id; missing columns raise `KeyError` with the column list (strict Select/Join); the job stops rather than writing partial Gold data. Re-runs are safe: Silver/Gold are full refreshes with guarded overwrite.
- **Deployment**: parameterise via job parameters/widgets; no credentials are needed by these notebooks (they only read Bronze tables); one job task per notebook (`databricks_job.json`); use a service principal; Unity Catalog grants on the three schemas (Bronze read-restricted, Gold read for consumers).
- **Monitoring**: alert on task failure and on `04_quality_reconciliation` failures; track row counts of Gold tables per run against the previous run (use `DESCRIBE HISTORY` operation metrics); add freshness checks on the Bronze tables.
- **Lineage/audit**: Bronze is owned by the ingestion process (add `_ingested_at`/`_batch_id` there); Silver/Gold outputs intentionally keep the exact Alteryx output schema (no extra columns) - lineage comes from Unity Catalog and Delta history.
- **Cost**: the sample's ~65 minute Alteryx run suggests heavy joins; do not infer Spark runtime - measure on representative data.
