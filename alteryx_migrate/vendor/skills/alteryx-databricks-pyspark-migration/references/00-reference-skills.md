# How to use the two existing reference skills (read-only, load on demand)

Never load whole files. Open only the section the current workflow needs; most tool semantics are already distilled in
`03-tool-mapping.md` and `04-expression-translation.md`.

## Alteryx skill - `C:\Git\skills2\alteryx\alteryx\skills\alteryx-designer\`
| File | Size | Use it for | How to read it |
|---|---|---|---|
| `references/formula-syntax.md` | 16 KB | authoritative list of Alteryx formula functions/operators and the **operator-precedence table** | grep the function name (`Grep "^- `Contains"`); read the Operators section once when a workflow mixes `AND`/`OR` |
| `references/designer-tool-reference.yaml` | 75 KB, 258 tools | tool names, plugin names, **input/output anchor names** (Unique -> `Unique`/`Duplicates`; Join -> `Left`/`Right`/`Join`; Filter -> `True`/`False`; Data Cleansing -> `Input2` / `Output26`) | grep `tool_name: <Tool>` with `-A12`; never read fully |
| `references/workflow-xml.md` | 18 KB | `.yxmd` XML shape (Nodes, Connections, MetaInfo, Containers) when the user can supply the raw workflow to recover configuration the JSON lost | read the "Nodes" and "Connections" sections only |
| `SKILL.md`, `building-with-*.md`, `scripts/*.ps1` | - | building/running workflows in Designer; **not needed** for migration | skip |
| `../alteryx-asset-discovery`, `../alteryx-insights` | - | unrelated to code migration | skip |

Facts confirmed from these files and used by the translator: `==` and `=` are both equality; `&&`/`AND`/`||`/`OR` share one precedence level;
`/` always returns a double; `Contains/StartsWith/EndsWith` list `CaseInsensitive=1` as the default; `Round(x, mult)` rounds to a multiple.

## PySpark skill - `C:\Git\skills2\pyspark\pyspark.md` (`pyspark2.md` is an identical copy)
A single SKILL.md (Spark 3.5+ with Delta). It points to a `references/PATTERNS.md` **that does not exist in the folder**, so only its
principles are available. Adopted by the generator:
- built-in functions over UDFs (the translator emits only `pyspark.sql.functions`; a UDF is never generated);
- explicit column lists / aliases (`select`, `alias`), DDL strings for schemas (`spark.createDataFrame(rows, "a string, b int")`);
- pure DataFrame -> DataFrame functions separated from I/O (each unit = a `build_*` function plus a separate write cell).

Deliberate deviations: `withColumn` chains are kept in the Formula tool because Alteryx evaluates formulas sequentially (a later formula may use an
earlier output); its project layout (`src/`, `tests/unit_tests`) is not applied because the deliverable is notebooks - this skill's own `tests/` follow its
"real SparkSession, no mocks" rule.

## Other skills in the environment
`migration:alteryx-to-sdp` / `alteryx-sdp-migrate` target Lakeflow SDP and parse raw XML. They are **not** used here (this skill emits plain notebooks).
Their XML parsing approach is the right route if the user can provide `.yxmd` files to recover tools the JSON lost.
