# Tool notes from the Alteryx help (one section per tool)

Source: the official Alteryx Designer help pages (`help.alteryx.com/current/en/designer/tools/...`), read on 2026-10-22. Each tool lists what the help states, what it does **not** state (never guessed - ask or mark *Requires review*), and the PySpark rule. "Gen" says what the generator does today (see `03-tool-mapping.md` for tests); **Gap** = documented behaviour the generator does not implement yet (the tool is then blocked or reviewed, never approximated silently).

Status words: `supported` (emitter + test), `requires_review` (emitted with a review message), `unsupported` (blocked; reported).
Page-level notes that apply to every tool: role/edition remarks (Full vs Basic user) do not affect conversion; pages never specify output row order unless stated below, so row order is **never** assumed.

## Cross-cutting rules

- **Identify tools by XML plugin name and output anchors** (`Join`/`Left`/`Right`, `True`/`False`, `Unique`/`Duplicates`, ...), not by caption.
- **Recover settings from the `.yxmd`** before asking the user (`xml_recover.py`, `wfmodel._recover_edges`).
- **Schema fidelity:** keep field names, types, order and sizes where declared; a name is spelled as configured.
- **Row order:** only Sort, Record ID, Running Total, Multi-Row Formula, Unique (first), Sample (first/last/skip), Select Records, Rank, Tile, First/Last, Make Columns and Union with a specific order depend on record order. Each needs an explicit order key in Spark or is reviewed.
- **Dictionary vs code-point order:** Alteryx Sort/Rank can use the language dictionary (lowercase before uppercase, numeric strings by value); Spark compares bytes. Reviewed wherever text order matters.

## High-priority tools

### Join (`Join`; outputs J / L / R) - supported
Help: Join by Specific Fields or by Record Position; J = matched, L / R = unmatched; per-column select, rename, type, size; `*Unknown` selected by default; *Deselect Duplicate Columns*; key types must match (Double/Float keys discouraged; blob/spatial cannot join).
Not stated: NULL keys, string case, duplicate keys. Gen: see "Join - rules from the Alteryx help" in `03`. **Gap:** Join by Record Position (blocked).
Output names (from the XML): every field is written `Left_X` / `Right_X` with `input="Left_"`/`"Right_"`; the output name is the `rename` attribute when there is one, else the plain `X`. Alteryx itself writes `rename="Right_X"` on a field whose name collided, so a rename equal to the field keeps the prefix. `*Unknown` (no `input`) applies to both sides. A listed field the input does not have is left out, as Alteryx does (with a warning).
Open question (BUG-12): the generator drops the right input's join keys unless the configuration lists them; CF_MasterData [91] deselects only the `Left_` keys with `*Unknown` on, which suggests Alteryx keeps the right keys. Not changed until one Alteryx output row confirms it.
NULL text (BUG-16, risk): Alteryx is said to compare a NULL string as empty (`null != 'x'` is true); Spark gives NULL, so the record goes to the False side. The option `project.null_string_as_empty` (off by default) writes `coalesce(col, '')` in `=` / `!=` on text fields; confirm with one sample before turning it on.

### Join Multiple - supported (from the XML)
Help: joins **two or more** inputs; default is a **full outer** join; by Specific Fields (a key per input; same-name key auto-selected) or by Record Position; checkbox *Only Output Records that Join from All Inputs* (off = all records, NULLs where no match); Cartesian handling Allow / Warn above 16 records / Error above 16; per-column select, type, size, rename, `*Unknown`, *Deselect Duplicate Columns*, prefix/suffix actions.
Not stated: output row order, anchor layout, the exact name given to colliding columns across inputs.
Gen: chain `join` over the key list per input, `full` unless the checkbox is on (then `inner`). Fields are named `Input_#k_<field>` in the settings; the output name is the rename, else the field's own name without `Input_#k_` (the tools after it use the plain names, e.g. WalujGlass [72] reads `SRC`). A name an earlier output field already has keeps its `Input_#k_` name and is reviewed.

### Cross Tab - requires_review
Help: group fields, header field (each unique value becomes a column), value field, aggregation chosen **by the data type of the value field** - string: Concatenate (separator, field size), First, Last, Count Distinct (with / without nulls); number: Sum, Average, Count (with / without nulls), Count Distinct, Percent Row, Percent Column, Total Column, Total Row. A prefix (`AVG_`, `SUM_`, `COUNT_`...) is added to the header unless the method is Sum, First or Last alone. Header values that differ only in case are **one** column. Special characters in new names are replaced by `_` unless *Retain Special Characters* (AMP). Up to 32,000 columns.
Not stated: NULL handling beyond the count methods, output order, output types.
Gen: `groupBy(..).pivot(header).agg(F.sum(value))`; the pivot values are listed explicitly. **Gap:** Concatenate / Percent / Total / Count variants (blocked when used), prefix naming, case-folding of header values - each is reviewed.

### Transpose - supported
Help: key columns repeat; data columns become rows with `Name` (column name) and `Value`; *Missing Columns*: Error / Warn / Ignore; rows out = rows in x data columns.
Not stated: output field order, type handling, NULLs. Gen: `explode(array(struct(lit(name), col.cast('string'))))`, so Value is text; `*Unknown` data fields are expanded from the run-time columns. Key/data overlap blocked. **Gap:** the *Missing Columns* action (Error / Warn / Ignore) is not implemented - a missing data column fails the run in Spark (= Error); Warn / Ignore need a column-existence filter.

### Running Total - requires_review
Help: running sum of numeric fields in record order, restarts per Group By; new column prefix `RunTot_` (the help's own example tables show other names - confirm from the `.yxmd`/results).
Not stated: ordering option, NULL handling. Gen: window sum `unboundedPreceding..currentRow`, partitioned by the group, needs an explicit order (else Review). Only numeric fields.

### Multi-Row Formula - requires_review
Help: updates **one field per instance** (existing or new, with type and size); *Num Rows* sets how many rows above / below are available (Row-1, Row+0, Row+1, ...); rows that do not exist return **Null**, **0 or Empty (default)**, or **the closest valid row**; optional Group By.
Not stated: row order, behaviour at the end of data (assume symmetric to the start).
Gen: `F.lag/F.lead` over `partitionBy(group).orderBy(order)`, with 0 / '' for a missing row when the setting is Empty. A reference to its own output (recursive) has two rules: the forward fill `IF IsNull([f]) THEN [Row-1:f] ELSE [f]` (also `[f] = Null()`), and in general an IF / IIF with exactly one result `[Row-1:f]` and no other row reference: a row where that branch applies repeats the value of the last row (in the group, in order) where it did not - `last(when(not carry, struct(value)), ignorenulls)` over the running frame; before the first such row 'other rows' applies. Any other recursive form is reported with its expression.

### Multi-Field Formula - requires_review
Help: field-type filter (Numeric, Text, SpatialObj, Bool, DateTime, Time, All); the same expression for each selected field; `_CurrentField_`, `_CurrentFieldName_`, `_CurrentFieldType_`; *Copy Output Fields* creates new columns with a prefix or suffix, otherwise the originals are updated; optional output type and size.
Gen: one `select`; `_CurrentFieldName_` is a string literal per field. **Gap:** copy-output mode (blocked), `_CurrentFieldType_` (blocked).

### Dynamic Rename - requires_review
Help modes: Formula (incl. fields unknown until runtime), Add Prefix/Suffix, Remove Prefix/Suffix, Take names from First Row, from Right-input metadata, from Right-input rows (by name or **Positional Rename**), descriptions from the right input; notification Warn / Error / Ignore. Null first-row values become `Field_n+1`. Row-based modes depend on record order (a Join upstream can scramble it).
Gen: Formula mode only (names computed once). **Gap:** all other modes blocked; first-row mode is the most common - a Spark conversion needs the first row read explicitly with a defined order.

### Dynamic Select - unsupported
Help: choose fields by type or by formula over field metadata (`Name`, `Type`, `Size`, `Scale`, `Source`, `Description`, `IsNumeric`, `IsString`, `IsFloat`, `IsInteger`, `IsStringOrDate`, `IsDateOrTime`, `IsBinary`, `IsSpatialObj`, `FieldNumber` where 1 is first); fields unknown until runtime also qualify; output is the selected fields only.
Rule when implemented: resolve the field list from `df.schema` in Python at run time (a plain list comprehension over `df.schema.fields`), then `df.select(*names)`. Blocked until added.

### Dynamic Replace - unsupported
Help: D = data, R = replacement rows with a **field-name field**, a **Boolean expression field** (non-zero = match; `_CurrentField_`) and an **output value field**; values are static or expressions; expressions are tried **in order, first match wins** (so ranges are listed ascending); unmatched: Allow (NULL) / Warn / Error; field type Autofield / Keep Original / Change to; second output **S** summarises replacements.
Rule when implemented: build one `F.when(cond1, v1).when(cond2, v2)` chain per field in R-row order (the rules table is read at conversion time, not at run time). Blocked until added.

### Dynamic Input - unsupported
Help: reads sources chosen at run time: *Read a List of Data Sources* (change path / file / table name from a field) or *Modify SQL Query* (pass a field, replace a string, update WHERE / IN, spatial filter, stored procedure). All files must share a schema.
Rule: ingestion is out of scope (inputs are existing Bronze tables). A list of sources becomes one Bronze table per source read by name; reviewed.

### Generate Rows - requires_review
Help: optional input; update existing field (rows added per incoming record) or create a new field (acts as an input); initialization, condition, loop expressions; generation stops when the condition is false; error when the loop expression does not change the field.
Gen: `F.explode(F.sequence(..))` for `init; [f] <= bound; [f]+n` or `DateTimeAdd([f], n, unit)`. Other loop shapes blocked; confirm empty-range behaviour (Spark `sequence` raises when step has the wrong sign).

### Unique - requires_review
Help: compares the selected columns (all columns = whole record); **U** = the first record of each group, **D** = the rest; output is sorted on the unique columns (use a Sort first for a specific order).
Not stated: case sensitivity, NULL handling. Gen: `row_number()` over the keys in record order; text keys compared case-sensitively and NULL = NULL as a group (assumptions in the report). Without an explicit Sort the kept row is arbitrary -> Review.

### Record ID - requires_review
Help: new column with name, **starting value**, type (for a String the size is the length, **padded with leading zeros**), position first or last, optional Group By (restarts per group).
Gen: one global `row_number()` over the order plus start - 1, cast to int / smallint / bigint, selected first (or last). When every path from the tool removes the new field (through Filters, Formulas, Sorts that do not read it, up to a Select that deselects it) the number is never seen: the rows pass through and no order is asked (ONBItaly [160]). The CReW `GroupByRecordID` macro without grouping fields is the same tool (FieldName, StartingValue, LastColumn). **Gap:** Group By (reported), String type with zero padding.

### DateTime Now - supported
The tool is the macro `DTNEngine.yxmc` (installed with Designer): one record, one text field `DateTimeNow` = the run's date-time in the chosen `OutputFormat` (anchor `DTN`). Gen: `spark.range(1).select(date_format(current_timestamp(), <format>))`; depends on the session time zone.

### Sort - requires_review
Help: dictionary order (default for English, French, German, Italian, Portuguese, Spanish: lowercase before uppercase, numeric strings by value, punctuation first) or Unicode code point (uppercase first; "10" before "2"); per-column Ascending / Descending, top to bottom; **stable**.
Not stated: NULL order, dates, booleans. Gen: ascending NULLs first, descending NULLs last (kept from earlier verification); Spark compares bytes and its sort is not guaranteed stable, so ties keep no promised order - Review when a downstream tool depends on tied rows. The sort is only emitted when it feeds a final table (see `03`).

### Sample - requires_review
Help modes: First N, Last N, Skip first N, 1 of every N (first of each block), 1-in-N chance per row (random, approximate), First N% (two passes). Optional Group By applies the mode per group. No seed.
Rule: position modes need an explicit order (`row_number()` over order, per group); Last N uses a descending number; random modes use `F.rand()` (not reproducible - Review); First N% uses `ntile`/`percent_rank` with a count. **Gap:** no emitter yet (blocked).

### Random % Sample - requires_review
Help: Random N Records or Random N% of Records, expected (not exact) count, optional **Random Seed** for repeatable output.
Rule: `df.sample(fraction, seed)` for a percentage; N records needs `orderBy(F.rand(seed)).limit(n)`; a different engine gives a different sample, so equality with Alteryx can only be checked by distribution. No emitter yet (blocked).

### Select - supported
Help: per-column checkbox, rename, description, type, size, `Forced`; `*Unknown` selected by default and positioned; *Deselect Duplicate Columns* (multiple inputs only); comma-decimal option for string <-> number; displayed names up to 40 characters.
Gen: see the Select row in `03`. **Gap:** the comma-decimal option (reviewed when set).

### Union - supported
Help: Auto by name (default), Auto by position, Manual; when columns differ: Error / Warning / Ignore, then *Output All Fields* (NULL fill) or *Output Common Subset*; names come from the input that sorts first alphabetically (inputs are #1, #2 by connection order); output order roughly follows the connection order (not guaranteed) unless *Set a Specific Output Order*; Manual and Non Blocking are fragile.
Not stated: type reconciliation across inputs. Gen: `unionByName(allowMissingColumns=True)`; **Gap:** Common Subset (select the intersection first), Error / Warning on differing columns (add a schema check), specific output order (ignored; ordering is never assumed).

### Summarize - requires_review
Help actions: Group By (case-sensitive, not blob/spatial), Sum, Count (all records), Count Non Null (empty strings and zeros count), Count Distinct, Count Distinct Non Null, Count Null, Min, Max, First, Last (record position); numeric: Average (nulls excluded), Product, Percentile (default 50), Median (even count = mean of the two middle values), Mode (ties -> smallest), Standard Deviation, Variance, *Ignore 0's*; string: Count Blank, Count Non Blank, Concatenate (start / separator / end; `\n \t \r \s`), Concatenate Distinct (case-sensitive), Longest, Shortest, Mode; finance NPV, XNPV, IRR, XIRR, MIRR, MXIRR; spatial and report actions. No Group By = one row over the whole input. Output holds only the action results.
Not stated: default output names (the XML carries them), group order.
Gen: GroupBy, Sum, Avg, Min, Max, Count, CountNonNull, CountDistinct, Median, StdDev / Var (**sample**, Review), First / Last, Concat, Percentile. **Gap (blocked):** Count Null, Count Blank / Non Blank, Product, Mode, Longest / Shortest, Concatenate Distinct, Ignore 0's, finance, spatial, report. Simple rules for later: Count Null = `F.sum(F.col(x).isNull().cast('int'))`; Product = `F.product`; Mode = most frequent with ties -> smallest (a ranked sub-aggregation); Longest / Shortest = `max_by` / `min_by` on `F.length`.

### Text To Columns - requires_review
Help: each character of the delimiter field is a separate delimiter (`\t`, `\n`, `\s`, `\s\t`); split to columns (number of columns, extra characters: leave in the last column / drop with warning / drop silently / error; root name + serial number) or to rows (a key column tracks the record); options to ignore delimiters in quotes, single quotes, parentheses, brackets; skip empty columns.
Settings in the XML: `<Delimeters value="..."/>` (Alteryx's spelling - the parser missed it and fell back to ',', BUG-03), `<NumFields value>`, `<RootName>`, `<ErrorHandling>Last|Drop</ErrorHandling>`, `<Flags value>`. A missing delimiter is asked, never assumed.
Gen: `F.split(col, delim, N)` + `F.get` for 'leave extra in last column' (the limit keeps the rest in the last part), `F.split` + `F.get` for Drop; rows mode by `explode`. **Gap:** Flags other than 0 (quotes / brackets / skip empty) are reported.

### RegEx - unsupported
Help: Boost / Perl syntax; **Case Insensitive on by default**; methods Replace (with marked groups, optional copy unmatched), Parse (named output columns with type and size), Tokenize (one marked group; to columns or rows), Match (1 / 0 into a named column; *Error if not Matched*).
Rule when implemented: `regexp_replace`, `regexp_extract` per group, `rlike`, `regexp_extract_all` + posexplode; Java regex differs from Boost on some constructs (POSIX classes, `\<`) -> Review. Case insensitivity is added as `(?i)` when the flag is on (the Alteryx default). Blocked until added.

### DateTime - requires_review
Help: date-time -> string or string -> date-time; presets per language (8 languages) or custom; specifiers `yyyy yy MM M dd d HH hh mm ss ffff day dy Month Mon`; percent style (`%d %m %Y`) cannot be mixed with pattern style; separators `-` and `/` interchangeable on input, whitespace ignored; **two-digit years map into 66 years before to 33 years after the current year**; unseparated `yymmdd` reads as a 4-digit year; default output name `DateTime_Out`.
Gen: `F.try_to_timestamp` with the format letters passed through. **Gap:** the two-digit-year window, `ffff` sub-seconds precision, language-specific names - reviewed.

### Filter - supported
Simple mode: the operand is typed text compared as the field's type - a number is written unquoted only for a numeric field, else as text (`KPI != '0'`, ONBItaly [29]; a quoted literal also compares correctly with a numeric column).
Help: Basic (one column, type-dependent operator) or Custom (expression); **T** / **F** outputs; string *Is empty* matches NULL **and** `""`, *Is null* matches only NULL; *Does not contain* is case sensitive; date ranges and today / yesterday / tomorrow are dynamic; numbers over 15 digits lose precision.
Gen: NULL conditions go to F (see `03`). Rules to hold for Basic operators: `Is empty` = `isNull() | == ''`, `Is null` = `isNull()`, string `contains` case-sensitive for "Does not contain". **Gap/verify:** the date operators (today, yesterday, Range, periods after / before) must use the run date from the project settings, never the clock; check the translation in the report.

### Formula - supported
Help: expressions run in listed order and may use earlier outputs; existing column overwritten or new one added; optional type and size; a **disabled expression is not run** (a later use of its column fails); Fixed Decimal is converted to Double inside the tool.
Gen: sequential `withColumn`. **Rules to hold (verify in the generator):** a disabled expression is skipped, and a later expression that uses its column is reported, not guessed; Fixed Decimal arithmetic runs in Double (Review).

## Additional tools

| Tool | Help says | Status / rule |
|---|---|---|
| Find Replace | F input updated from R lookup; find at start / any part / entire field; case-insensitive and whole-word options; replace found text or append R fields; multi-replace only for "any part" | **unsupported** (no emitter). Entire field = left join; any part = `regexp_replace` / `contains` join; whole word needs a space-boundary rule |
| Fuzzy Match | purge or merge, match styles, default threshold 80%, needs a unique record id | **unsupported** (scores are Alteryx-specific; Review if a library is chosen) |
| Make Group | transitive grouping of two key columns into groups | **unsupported**; connected components (GraphFrames or iterative join) - needs a decision |
| Append Fields | T (target) x S (source) Cartesian; warn / error above 16 source records; select / rename like Join | supported (`crossJoin`); **Gap:** the 16-record warn / error option |
| Arrange | manual transposition: key fields, new columns built from named description rows | **unsupported**; equals Transpose with a custom name column |
| Make Columns | rows into N columns (horizontal fill left-right, vertical top-bottom), group optional, `Column_#` suffix, `[Null]` fill | **unsupported**; `row_number` + pivot; needs explicit order |
| Count Records | single count; **returns 0 on empty input** (Summarize does not) | **unsupported**; `df.agg(F.count(F.lit(1)))` returns one row with 0 on empty input, as the help requires; the output field name must be read from the XML |
| Weighted Average | value, weight, output name, optional group | **unsupported**; formula not given in the help: use `sum(v*w)/sum(w)` and mark Review |
| Select Records | ranges `3`, `17-20`, `-2` (rows 1..2), `50+`, one per line | **unsupported**; `row_number()` over the record order, 1-based |
| Auto Field | smallest type that fits; a number-like field that starts with 0 stays string | **unsupported**; requires a data scan - type depends on data, not code: Review |
| Imputation | replace Null() or a value by average / median / mode / value; optional `_Indicator` field; separate output field `_ImputedValue` | **unsupported**; `F.when(cond, stat).otherwise(col)` with the statistic computed over all rows; the average excludes the replaced value |
| Rank | Ordinal, Dense, Standard, Modified Competition, Fractional; per-column order; group by; AMP only | **unsupported**; `row_number`, `dense_rank`, `rank`, `percent_rank`; Modified Competition and Fractional need a custom expression |
| Tile | Equal Sum, Equal Records, Smart Tile (std-dev), Unique Value, Manual; adds `Tile_Num` and `Tile_SequenceNum` | **unsupported**; Equal Records = `ntile` (does not keep ties together; reviewed) |
| Create Samples | E / V / H random split, seed 1-1000 | **unsupported**; `randomSplit` seeds differ from Alteryx; distribution check only |
| Oversample Field | keep all positives, sample negatives to reach a target share; no seed | **unsupported** |
| Multi-Field Binning | Equal Records / Equal Intervals on numeric fields; `<Field>_Tile_Num`; max 1000 tiles; Int64 output | **unsupported** |
| Date Filter | deprecated -> Filter; start and end dates **inclusive** | covered by Filter |
| XML Parse | child values / outer XML; `_OuterXML` suffix; auto-detect reads the first row | **unsupported** (`from_xml` / `xpath`) |
| Field Info | metadata rows: Name, Type, Size, Scale, Source, Description; output even for empty input | **unsupported**; derive from `df.schema` (Source / Description do not exist in Spark) |
| JSON Parse | `JSON_Name` (dot path, `phoneNumber.0.type`) + `JSON_ValueString` (or typed values); AMP unnest / flatten | **unsupported**; `from_json` + `explode`, names must be rebuilt |
| JSON Schema | builds a schema column for LLM prompts | **unsupported** (AI tool, out of scope) |
| Base64 Encoder | adds `Base64_Encoded` | **unsupported**; `F.base64(F.col(x).cast('binary'))` |
| Blob Convert / Input / Output | blob <-> base64 / hex / text / MD5 / SHA256; chunking with `BlobNumber` | **unsupported**; files are not ingested; hashes map to `md5`, `sha2(.., 256)` |
| Join In-DB, Summarize In-DB, Union In-DB | same semantics as the in-memory tools, run in the database; In-DB Join: inner / left / right / full | treat as Join / Summarize / Union over tables once the In-DB connection is a Bronze table |
| Dynamic Input In-DB, Dynamic Output In-DB | pass connection name and query between streams; query with a `WITH` clause rejected | **unsupported** (connection plumbing) |
| Macro Input / Macro Output | macro interface anchors | **blocked** until the `.yxmc` is provided; then convert the macro body |
| Block Until Done | releases outputs one at a time to avoid read / write conflicts; lowest Tool ID runs first | pass-through; order is notebook order (document it) |
| Detour / Detour End | bypass a section in apps / macros; the unselected side is not run | **blocked** (needs an app answer); choose a branch and convert it |
| Control Container | activates when its control input receives >= 1 record; AMP only | **blocked**; becomes a conditional cell (`if df.limit(1).count()`) after a decision |
| Throttle | records per minute (default 60) | omit (no data effect); note in the report |
| Test / Expect Equal | assertions on record counts / expressions / equality of two streams (first 4 errors only; float exact; strings case-sensitive) | map to checks in `04_quality_reconciliation`, not to transformations |
| Run Command | Write Source -> program -> Read Results | **unsupported** (no safe equivalent) |
| Python | `Alteryx.read` / `Alteryx.write` pandas | converted with approval under `10-python-code-tools.md` |
| R | `read.Alteryx` / `write.Alteryx`; needs Predictive Tools | **unsupported** (no automatic equivalent) |

## Specialized categories

Classify each tool in these as `supported`, `requires_review` or `unsupported`; none has a native PySpark equivalent assumed.

| Category | Rule |
|---|---|
| Spatial | `unsupported`; Databricks `ST_*` / H3 / Mosaic differ (geodesic vs haversine) - ask, then Review |
| Predictive, Time series, Predictive grouping, Prescriptive | `unsupported`; model re-implementation (Spark ML / statsmodels) and a result comparison are needed |
| Data investigation | `unsupported` for output; Browse is omitted; statistical tools are re-created as `describe()`/`summary()` only if asked |
| Reporting | `unsupported`; layout / rendering is not migrated |
| Connectors | out of scope (ingestion is not generated); the source becomes a Bronze table |
| Alteryx Intelligence Suite, AI tools | `unsupported`; model calls are not reproducible |

## Validation fixtures (per supported tool)

Every supported tool has a hand-derived test in `tests/` (see `KIND_TESTS`). When a row above is added to the generator: write a fixture with NULLs, ties and duplicate keys, state the expected output from the help text, run it on local Spark, and only then move the tool's status to `supported`.
