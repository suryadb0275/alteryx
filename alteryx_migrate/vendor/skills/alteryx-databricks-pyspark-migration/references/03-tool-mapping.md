# Alteryx tool -> PySpark mapping (what the generator implements)

Columns: original behaviour | PySpark implementation (plain PySpark written inline; no helper module) | semantic differences / caveats | default status | test (`tests/`).
Anchor names are confirmed in `designer-tool-reference.yaml`. "Review" = *Requires review*. Performance notes are in `08-performance-and-operations.md`.

## Sources and sinks
| Tool (IR node) | Behaviour | PySpark | Caveats | Status | Test |
|---|---|---|---|---|---|
| Input Data (`ReadNode`) | reads a DB query or file | an existing Bronze table (listed in `01_bronze` with the query); read with `spark.table(T[...])`. No ingestion code is generated | Alteryx connection alias must be mapped; T-SQL passed through unchanged (JDBC dialect = SQL Server); no schema in the JSON; Excel sources are not read by Spark natively | Review (DB) | e2e |
| Text Input (`LiteralDataNode`) | static rows | `spark.createDataFrame(rows, ddl)` + typed select | values are text in the JSON, cast by declared type | High | `test_literal_data_types` |
| Output Data (`WriteNode`) | writes xlsx/csv/db | guarded Delta Gold table (`write_table` from `00_config`) | file delivery (xlsx on a drive) is **not** reproduced; add an export job. Excel via pandas would be driver-side - deliberately not generated | Review (file outputs) | e2e |
| Browse (`BrowseNode`), Text Box (`CommentNode`) | inspection/comment | omitted / Markdown | none | n/a | - |
| Tool Container | visual | not migrated, counted | none | n/a | - |

## Row-level tools
| Tool | Behaviour | PySpark | Caveats | Status | Test |
|---|---|---|---|---|---|
| Filter (`True`/`False` outputs) | rows meeting the expression -> True, rest -> False | `df.filter(cond)`; with a False output: `cond = F.coalesce(cond, F.lit(False))`, True = `filter(cond)`, False = `filter(~cond)` | NULL conditions go to **False** (no row is lost) | High if expression clean | `test_filter_null_goes_to_false_output` |
| Formula | one or more formulas evaluated **in order**; new or replaced fields typed per `data_type` | sequential `withColumn(name, expr.cast(type))` (`try_cast` when the project states ANSI on) | later formulas may use earlier outputs; ToString/`+` typing see `04`; cast failures give NULL (`try_cast`) | High unless the expression carries a review | `test_expression_semantics` |
| Multi-Field Formula | same expression for each listed field (`[_CurrentField_]`) | one `select` with `_mf(c)` per listed field, unlisted columns unchanged | each field sees **incoming** values; `copy_output` mode unsupported (blocked); `output_type` "Numeric" assumed Double (Review) | High / Review | `test_expression_semantics` |
| Multi-Row Formula | `[Row-N:Field]` refer to previous/next records, in record order, optionally per group | `F.lag/F.lead(...).over(Window.partitionBy(group).orderBy(order))`; new or replaced field | needs explicit order (upstream Sort) else Review; **recursive** reference to its own output -> Unsupported; no `group_fields` = single-partition window; first/last rows get NULL | Review / High with Sort | `test_multirow_lag_over_window` |
| Select | choose, reorder, rename, retype, resize | `df.select(F.col(..)[.alias(new)][.cast(type)], ..., *[the other fields])`; only removals = `df.drop(..)` | **strict**: a selected field that is missing fails the run; sized strings cut with `substr(1, size)`; unknown fields follow the configured ones; a field is named as configured | High | `test_select_rename_retype_order` |
| Sort | ascending/descending, NULLs first when ascending | `df.orderBy(F.col(..).asc_nulls_first(), F.col(..).desc_nulls_last())` **only when the sort feeds a final table**; otherwise omitted, because order-dependent consumers apply the sort keys explicitly | Spark compares strings by UTF-8 bytes (A-Z before a-z), Alteryx is locale-aware | **Review** (always, string collation) | `test_sort_null_ordering` |
| Unique | first record per key -> `Unique`, the rest -> `Duplicates` | `row_number()` over (partition by keys order by record order), rank 1 = Unique, rank > 1 = Duplicates; `dropDuplicates(keys)` when any row is accepted and no Duplicates output is used | "first" = first in incoming order; without an explicit Sort the kept row is arbitrary -> Review | Review / High with Sort | `test_unique_split_keeps_first_in_order` |
| Record ID | sequential id, first column | `row_number()` over order, offset by start, selected first | global window (single partition); needs explicit order else Review | Review / High with Sort | `test_record_id_ordered` |
| Running Total | cumulative sum per group in record order | `F.sum(..).over(Window.partitionBy(groups).orderBy(order).rowsBetween(unboundedPreceding, currentRow))`, output `RunTot_<field>` | naming and order assumptions; blocked when `running_fields` lost | Review / blocked | `test_running_total` |
| Text To Columns | split a field | `F.split` + `F.get(parts, i)` -> `<root>1..N` (original kept); rows mode = `explode` | multi-character delimiter treated as a character set (assumption -> Review); fewer parts -> NULL | High (1-char) | `test_text_to_columns` |
| Date Time | text <-> date-time with a format | `F.date_format(F.try_to_timestamp(col[, fmt]), ...)`; settings from the XML | format letters passed to Spark unchanged; Language ignored | High | e2e / translator |
| Dynamic Rename (Formula mode) | rename fields by a formula over the field name | `toDF(*names)` where the new names are computed once from the formula (`spark.range(1).select(formula(F.lit(name)))`) | other modes (FirstRow, right input) are blocked | High | `test_rename_fields_applies_a_formula_to_field_names` |
| Generate Rows | init / condition / loop expressions create rows | `F.explode(F.sequence(..))` for `init; [f] <= bound; DateTimeAdd([f],n,unit)` or `[f]+n` | other loop shapes are blocked; confirm boundary/empty-range behaviour | Review | `test_generate_rows_dates_and_numbers` |
| Block Until Done (WorkflowControl) | forces execution order | pass-through | data unchanged | High | - |
| Data Cleansing macro | null rows/columns, case, blanks / 0 for NULL, unwanted characters - in the macro's order | **converted** | questions decoded by their names in Cleanse.yxmc (`wfmodel.CLEANSE_QUESTIONS`); string steps on string fields, 0 on numeric fields; unknown field types are resolved at run time (PySpark) or asked (SQL, `answers.field_types`) | converted | - |

## Multi-input and grain-changing tools
| Tool | Behaviour | PySpark | Caveats | Status | Test |
|---|---|---|---|---|---|
| Join (`Join`/`Left`/`Right`) | inner join -> J; unmatched left -> L; unmatched right -> R; select/rename per side | `L.alias('l').join(R.alias('r'), F.col('l.A') == F.col('r.B'), 'inner')` + `select`; L/R = `left_anti` on the same condition (the aliases keep a self-join unambiguous) | **keys required** (empty = blocked); NULL keys never match unless `project.null_safe_join` (the Alteryx help is silent - asked as a CONFIRM item); duplicate keys multiply rows (as SQL); case-sensitive string keys; key types must match (mismatch -> Review); see "Join - rules from the Alteryx help" below; right key columns dropped unless kept in `select_right`; remaining collisions get `Right_`; L/R outputs are the raw inputs | High with keys | `test_join_outputs_and_null_keys`, `test_join_collision_prefix_and_selection` |
| Join Multiple | n-way join | **not generated**: colliding-field naming (`Input_#k_`) and join type unverified | send an Alteryx sample or the rule | blocked | - |
| Union | stack inputs (auto by name / by position / manual) | `a.unionByName(b, allowMissingColumns=True)` chained / `a.union(b.toDF(*a.columns))` positional | input order = connection order in the file (for a Left / Right / Full outer join the Alteryx help connects **J first** so it sets the schema; the parser JSON keeps one connection per pair of tools, so the missing ones are restored from the `.yxmd`); type coercion across inputs left to Spark; manual mapping blocked | High (auto) | `test_union_by_name_allow_missing` |
| Append Fields | every target record gets the source record(s) | `target.crossJoin(source)` (Spark broadcasts a small source itself) | name collisions raise | High | `test_append_fields_crossjoin` |
| Summarize | group-by + actions | `groupBy(..).agg(F.sum(..).alias(..), ..)`; no group fields = `df.agg(..)`; no aggregates = `select(..).distinct()` | supported: GroupBy, Sum, Avg, Min, Max, Count (all records), CountNonNull, CountDistinct, Median, StdDev/Var (**sample**, Review), First/Last (order, Review), Concat (order, Review), Percentile (Review); other actions unsupported; Alteryx returns groups sorted, Spark does not | High (basic) / Review | `test_summarize_actions` |
| Cross Tab | pivot rows to columns | `groupBy(..).pivot(header).agg(F.sum(value))` | output naming and pivot values must be confirmed; blocked when header/value fields lost (always in the sample) | Review / blocked | `test_crosstab_pivot` |
| Transpose | columns -> name/value rows | `explode(array(struct(lit(name), col.cast('string'))))` | Value is text; key/data overlap blocked | High | `test_transpose` |

## Not implemented (unsupported by design until semantics can be verified)
Spatial: Create Points, Distance, Spatial Match (need Databricks `ST_*`, H3 or Mosaic; geodesic vs haversine differences) - Predictive: ETS, TS Forecast, models (re-implement and compare) -
macros and analytic apps (supply the `.yxmc`) - iterative/batch macros and loops (cycles are reported) - Sample, Random % Sample, Tile, Rank, Select Records, Find Replace, Imputation, Regex tool, Dynamic Select / Replace / Input and any other tool without an emitter
(`no emitter for tool kind`) - run-command, R/Python tools (no safe automatic equivalent).

**Full per-tool notes from the Alteryx help pages** (what the help states and does not state, gaps, the planned PySpark rule for every tool in the audit list, and the specialised categories) are in `12-tool-help-notes.md`. Where that file lists a **Gap**, the generator blocks or reviews the tool; it never approximates silently.

## Adding or changing a rule
Implement `e_<kind>` in `codegen.py` so it writes plain PySpark (no helper module); add a hand-derived test; update this table and `KIND_TESTS`. Keep the rule: when Alteryx behaviour is not certain, emit code **and** a review message, or raise - never approximate silently.

## Join - rules from the Alteryx help (help.alteryx.com, Join tool)

| Rule in the help | What the generator does |
|---|---|
| Outputs: **J** = joined records, **L** = L records with no match, **R** = R records with no match | inner join / `left_anti` with the left frame first / `left_anti` with the right frame first, all on the same condition |
| Left outer = J + L into a Union, **J connected first** (sets the schema); right outer = J + R; full outer = J + L + R | the Union takes its inputs in the order of the connections in the `.yxmd`; the J connection must come first there |
| Join by Record Position needs the same field structure | not implemented (blocked) - needs an explicit row order on both inputs |
| Join by Specific Field: one row per key pair; picking a field on one side picks the same name on the other | `join_keys` pairs, left field = right field by position |
| Key types: string joins string, number joins number, Double only Double, Boolean only Boolean, date-time only its exact type; Double/Float keys not recommended (rounding); spatial and blob fields cannot be joined | when both key types are known and differ -> *Requires review* (Spark would convert one side silently); a numeric key adds a note |
| Each column has a checkbox; deselected columns are left out; **`*Unknown` is selected by default** (new columns pass through) | the listed fields come first in the configured order, then every other field unless `*Unknown` is deselected |
| *Deselect Duplicate Columns* deselects the second column when names repeat | a right-side name that already exists on the left is output as `Right_<name>` (Alteryx names the field `Right_X`) |
| Rename, Type, Size per column | `.alias()`, `.cast()`; sized text is cut with `substr(1, size)` |
| A collided field is configured as `Left_X` / `Right_X` with `input="Left_"` / `"Right_"` | the prefix is removed to find the input column `X`; the verifier maps `Left_X` to `X` too |
| NOT said in the help: how NULL keys match, whether string keys are case-sensitive, how duplicate keys behave | NULL keys do not match by default (`null_safe_join: true` makes them match); keys compared case-sensitively; duplicates multiply rows as in SQL - all listed as assumptions in the report |
