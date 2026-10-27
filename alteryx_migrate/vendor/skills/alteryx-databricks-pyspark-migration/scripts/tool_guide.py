"""What each Alteryx tool does and exactly how to replicate it in PySpark / Spark SQL.

Written from the official Alteryx Designer help pages (help.alteryx.com/current/en/designer/tools/..., read 2026-10-27) plus the behaviour the skill's own emitters implement.
Every entry has the same parts:

  does      what the tool does to the data (plain words)
  settings  the options the tool has, and what each means
  pyspark   a recipe that replicates the DOCUMENTED behaviour (`df` is the incoming DataFrame, `ORDER` the explicit record order the workflow defines)
  sql       the same in Spark SQL
  confirm   what the help page does NOT say (null handling, ties, output field names, order ...). The recipe makes a stated choice for each; the choice is
            written in the notebook as a comment and must be confirmed against real Alteryx output. NOTHING in `confirm` is silently assumed.

The guide is data: references/14-tool-coverage.md is generated from it, hand-off comments quote it, and a model that writes a table the skill cannot convert is given the
entries of the tools in that table. Spark has no inherent row order, so every "first / last / previous / every Nth" recipe needs an explicit ORDER; where the workflow
defines none the question is asked, never guessed.
"""
from __future__ import annotations

GUIDE: dict = {}


def g(name, does, settings="", pyspark="", sql="", confirm=()):
    GUIDE[name] = {"does": does.strip(), "settings": settings.strip(), "pyspark": pyspark.strip("\n"), "sql": sql.strip("\n"), "confirm": list(confirm)}


# =================================================================================================================== Preparation
g("Sample",
  does="Limits the stream to a number, a percentage or a random set of rows, optionally per group.",
  settings="""Sample method: First N rows | Last N rows | Skip first N rows | 1 of every N rows (the first row of each block of N) | 1 in N chance to include each row (random, N is approximate) |
First N% of rows (the help: the data passes through twice, once to count, once to return the percentage). N = the value. Group by column(s) (optional): the method applies per group.""",
  pyspark=r"""
from pyspark.sql import functions as F, Window
w = Window.partitionBy(*GROUPS).orderBy(*ORDER)                    # GROUPS = [] when there is no group-by
rn = F.row_number().over(w)
n_in_group = F.count(F.lit(1)).over(Window.partitionBy(*GROUPS))
first_n   = df.withColumn("_rn", rn).filter(F.col("_rn") <= N).drop("_rn")
last_n    = df.withColumn("_rn", F.row_number().over(Window.partitionBy(*GROUPS).orderBy(*[c.desc() for c in ORDER]))).filter(F.col("_rn") <= N).drop("_rn")   # then re-sort ascending if the order matters
skip_n    = df.withColumn("_rn", rn).filter(F.col("_rn") > N).drop("_rn")
one_of_n  = df.withColumn("_rn", rn).filter((F.col("_rn") - 1) % N == 0).drop("_rn")
chance    = df.filter(F.rand() < 1.0 / N)                          # approximate count; not reproducible unless F.rand(seed) is used
first_pct = df.withColumn("_rn", rn).withColumn("_n", n_in_group).filter(F.col("_rn") <= F.ceil(F.col("_n") * N / 100.0)).drop("_rn", "_n")
""",
  sql="""
-- first N (per group): SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY g ORDER BY o) rn FROM t) WHERE rn <= N
-- last N: ... ORDER BY o DESC ... WHERE rn <= N          skip N: WHERE rn > N          1 of every N: WHERE (rn - 1) % N = 0
-- 1 in N chance: SELECT * FROM t WHERE rand() < 1.0 / N          first N%: add count(*) OVER (PARTITION BY g) AS n and WHERE rn <= ceil(n * N / 100.0)
""",
  confirm=["The order that defines 'first/last/every Nth' is not stated (Alteryx uses the incoming record order): an explicit ORDER is required.",
           "First N% rounding (ceil here) and N larger than the row count or 0.", "NULL group keys: treated as one group here.",
           "1-in-N chance has no seed, so equality with Alteryx can only be checked by distribution."])

g("Random % Sample",
  does="Returns an EXPECTED number of random records (not exact), either N records or N% of the incoming records.",
  settings="Mode: Random N Records (Number of Records) | Random N% of Records (Percent of Records). Deterministic output: Random Seed (the same seed gives the same sample at run time).",
  pyspark=r"""
pct  = df.sample(withReplacement=False, fraction=P / 100.0, seed=SEED)             # about P% of the rows
n    = df.orderBy(F.rand(SEED)).limit(N)                                           # exactly N rows, a random N
""",
  sql="SELECT * FROM t WHERE rand(SEED) < P / 100.0   -- about P%;  exactly N: SELECT * FROM t ORDER BY rand(SEED) LIMIT N",
  confirm=["A different engine and seed give a different sample: equality with Alteryx can only be checked by distribution, never row by row.",
           "Valid percentage range, N larger than the input, and the seed when none is set."])

g("Create Samples",
  does="Splits the records into 2 or 3 random, disjoint samples: Estimation (E), Validation (V) and Holdout (H = what is left).",
  settings="Estimation percent (1-99), Validation percent (1-99), Random seed (1-1000, default 1). Outputs E, V, H. If E% + V% < 100 the rest goes to H; an odd count with 50/50 gives E one more row than V.",
  pyspark=r"""
w = Window.orderBy(F.rand(SEED))
d = df.withColumn("_rn", F.row_number().over(w)).withColumn("_n", F.count(F.lit(1)).over(Window.partitionBy()))
e_cut = F.ceil(F.col("_n") * E / 100.0)
v_cut = e_cut + F.floor(F.col("_n") * V / 100.0)
E_df = d.filter(F.col("_rn") <= e_cut).drop("_rn", "_n")
V_df = d.filter((F.col("_rn") > e_cut) & (F.col("_rn") <= v_cut)).drop("_rn", "_n")
H_df = d.filter(F.col("_rn") > v_cut).drop("_rn", "_n")
""",
  sql="-- WITH d AS (SELECT *, row_number() OVER (ORDER BY rand(SEED)) rn, count(*) OVER () n FROM t) then three SELECTs on rn against ceil(n*E/100) and ceil(n*E/100)+floor(n*V/100)",
  confirm=["The help gives the proportions and the odd-count rule but not the assignment algorithm: row-by-row equality with Alteryx is not achievable, only the proportions.",
           "Rounding (ceil for E, floor for V here); E% + V% above 100; whether a 2-sample mode exists."])

g("Oversample Field",
  does="Rebalances a binary target: keeps ALL records with the chosen value and a random sample of the others so that the chosen value makes up the target percentage.",
  settings="Field to base oversampling on; Field value to oversample (typically the positive 'yes'); Target percentage 1-100 (should not be below the value's share in the data).",
  pyspark=r"""
pos = df.filter(F.col(FIELD) == VALUE)
neg = df.filter(~(F.col(FIELD) == VALUE) | F.col(FIELD).isNull())
P = pos.count()
k = min(neg.count(), round(P * (100 - PCT) / PCT))                                # negatives to keep so that positives are PCT% of the output
out = pos.unionByName(neg.orderBy(F.rand(SEED)).limit(k))
""",
  sql="-- WITH pos AS (SELECT * FROM t WHERE f = v), neg AS (SELECT * FROM t WHERE NOT (f <=> v) ORDER BY rand(SEED) LIMIT k) SELECT * FROM pos UNION ALL SELECT * FROM neg  (k computed from count(*) of pos)",
  confirm=["The exact negative sample size and its rounding; with or without replacement; seed.", "What happens when the target is below the original share.", "NULLs in the field."])

g("Rank",
  does="Adds a rank for one or more fields, optionally per group, using one of five ranking types.",
  settings="""Ranking type (one or more): Ordinal (default; every record a distinct number) | Dense (ties share a number, no gaps) | Standard (ties share the LOWEST position of the tie, gaps follow) |
Modified Competition (ties share the HIGHEST position of the tie) | Fractional (ties share the mean of their positions). Use Dictionary Order (optional). Select Columns: one or more, Ascending/Descending,
in order. Group by column(s) (optional).""",
  pyspark=r"""
w = Window.partitionBy(*GROUPS).orderBy(*SORT_COLS)                               # ONLY the ranked columns, e.g. F.col("Units Sold").desc(): they define the ties
ordinal   = F.row_number().over(w)                                                # ties get distinct numbers in an arbitrary order; add a tie-break column to the ORDER BY for a reproducible result
dense     = F.dense_rank().over(w)
standard  = F.rank().over(w)                                                      # 1,1,1,4  (the help's Standard)
modified  = F.count(F.lit(1)).over(w.rangeBetween(Window.unboundedPreceding, 0))  # highest position of the tie: 3,3,3,4
fractional = (standard + modified) / 2.0                                          # mean of the tied positions (the help's table: 22 x7 -> 4 = (1+7)/2)
""",
  sql="row_number() OVER (PARTITION BY g ORDER BY x DESC), dense_rank() OVER (...), rank() OVER (...), count(*) OVER (PARTITION BY g ORDER BY x DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), (rank + that) / 2.0",
  confirm=["Output field names and types are not stated.", "Which tied record gets which Ordinal number (not stated: a tie-break column is required for reproducibility).",
           "NULL ordering; dictionary order vs Spark's byte order for text.",
           "The help's prose for Fractional ('1 + count above + half the count equal') disagrees with its own example table; the table (mean of positions) is used."])

g("Select Records",
  does="Returns the records at given positions: single rows, ranges, from the start, or to the end.",
  settings="Ranges, one per line, 1-based over the incoming order: `3` (row 3) | `-2` (rows 1 to 2) | `17-20` (rows 17 to 20) | `50+` (row 50 to the last). Entries combine.",
  pyspark=r"""
d = df.withColumn("_rn", F.row_number().over(Window.orderBy(*ORDER)))
keep = (F.col("_rn") == 3) | (F.col("_rn") <= 2) | F.col("_rn").between(17, 20) | (F.col("_rn") >= 50)     # one term per Ranges line
out = d.filter(keep).drop("_rn")
""",
  sql="SELECT * EXCEPT (rn) FROM (SELECT *, row_number() OVER (ORDER BY o) rn FROM t) WHERE rn = 3 OR rn <= 2 OR rn BETWEEN 17 AND 20 OR rn >= 50",
  confirm=["Output order (input order or the order entered) and a record that two ranges both select (once or twice).", "The row order that defines the numbering: an explicit ORDER is required.",
           "Out-of-range, zero or malformed entries.", "Output field names/anchors."])

g("Tile",
  does="Assigns each record to a tile (a numbered bucket) by one of five methods; appends the tile number and the record's sequence number within its tile.",
  settings="""Method: Equal Records (N tiles, same record count; optional 'do not split a tile on column'; optional sort column - descending for numbers, ascending otherwise) |
Equal Sum (N tiles with sums as equal as possible without breaking a record; Sum column; optional sort column) | Smart Tile (numeric column; tile by standard deviations: 0 = -0.5..+0.5 sd,
1 = 0.5..1.5, -1 = -1.5..-0.5, ...; optional name column Average / Above Average / High / Extremely High / Below Average / Low / Extremely Low, verbose adds the range) |
Unique Value (one tile per distinct value or combination of the Unique columns; output sorted unless 'Leave Unsorted') | Manual (numeric column; one upper limit per tile, unsorted).
Group by columns (optional): tile within each group. Appended fields: tile number, tile sequence number.""",
  pyspark=r"""
w = Window.partitionBy(*GROUPS).orderBy(*SORT)                                    # SORT = the sort column, else the incoming order
equal_records = df.withColumn("Tile_Num", F.ntile(N).over(w))                      # remainder rows go to the FIRST tiles in Spark
seq = F.row_number().over(Window.partitionBy(*GROUPS, "Tile_Num").orderBy(*SORT))   # Tile_SequenceNum
unique_value = df.withColumn("Tile_Num", F.dense_rank().over(Window.orderBy(*UNIQUE_COLS)))
z = (F.col(X) - F.avg(X).over(Window.partitionBy(*GROUPS))) / F.stddev_samp(X).over(Window.partitionBy(*GROUPS))
smart = df.withColumn("Tile_Num", F.floor(z + 0.5).cast("int"))                    # tile 0 = -0.5..0.5 sd; names: 0 Average, 1 Above Average, 2 High, 3 Extremely High, -1 Below Average, -2 Low, -3 Extremely Low
manual = df.withColumn("Tile_Num", F.when(F.col(X) <= C1, 1).when(F.col(X) <= C2, 2).otherwise(None))     # one WHEN per upper limit
total = F.sum(S).over(Window.partitionBy(*GROUPS)); before = F.sum(S).over(w.rowsBetween(Window.unboundedPreceding, -1))
equal_sum = df.withColumn("Tile_Num", F.least(F.lit(N), F.floor(F.coalesce(before, F.lit(0)) / (total / N)).cast("int") + 1))   # greedy by cumulative sum
""",
  sql="ntile(N) OVER (PARTITION BY g ORDER BY o) | dense_rank() OVER (ORDER BY u1, u2) | floor((x - avg(x) OVER ()) / stddev_samp(x) OVER () + 0.5) | CASE WHEN x <= c1 THEN 1 WHEN x <= c2 THEN 2 END",
  confirm=["Output field names (Tile_Num / Tile_SequenceNum) and types are not stated on the help page.", "Which tiles get the extra records when the count is not divisible (Spark ntile: the first ones).",
           "Equal Sum: the exact placement rule at a boundary (the help only says 'without breaking a record').", "Smart Tile: sample or population standard deviation; boundary inclusivity.",
           "Manual: whether a cutoff is inclusive, and values above the last / below the first cutoff.", "NULL values; row order when no sort column is set; whether Group by applies to every method."])

g("Multi-Field Binning",
  does="Bins one or more NUMERIC fields into N tiles each and appends `<Field>_Tile_Num` (Int64) per field. Built for the predictive tools.",
  settings="Fields to bin (numeric only); Tile method: Equal Records (same number of records per bin, by position) | Equal Intervals (min..max of each field split into N equal ranges); Number of tiles (max 1000).",
  pyspark=r"""
for f in FIELDS:
    equal_records = df.withColumn(f + "_Tile_Num", F.ntile(N).over(Window.orderBy(F.col(f))).cast("long"))
    lo, hi = df.agg(F.min(f), F.max(f)).first()
    equal_interval = df.withColumn(f + "_Tile_Num", F.least(F.lit(N), F.floor((F.col(f) - lo) / ((hi - lo) / N)).cast("long") + 1))
""",
  sql="ntile(N) OVER (ORDER BY f) | least(N, floor((f - min(f) OVER ()) / ((max(f) OVER () - min(f) OVER ()) / N)) + 1)",
  confirm=["Tile numbering start (0 or 1), the remainder rule, interval boundary inclusivity, the maximum value's bin, and min = max.", "The help: 'if too many records have the same group value, a tile is skipped' - what 'skipped' means in the output.",
           "NULL handling; independent binning per field (assumed)."])

g("Find Replace",
  does="Looks up a string of one column (F input) in a column of another dataset (R input) and either replaces the found text with an R value or appends R fields to the record.",
  settings="""Find: Beginning of Field | Any Part of Field | Entire Field; Find Within Field (F column); Find Value (R column); options Case Insensitive, Match Whole Word Only (spaces around the word).
Replace: Replace Found Text With Value (an R column; 'Replace Multiple Found Items' for Any Part) | Append Field(s) to Record (selected R fields).""",
  pyspark=r"""
cond = {"entire": F.col("F") == F.col("R_find"),
        "begin":  F.col("F").startswith(F.col("R_find")),
        "any":    F.col("F").contains(F.col("R_find"))}[MODE]                    # wrap both sides in F.lower() when Case Insensitive
j = f_df.alias("f").join(r_df.alias("r"), cond, "left")
replaced = j.withColumn("F", F.when(F.col("R_find").isNotNull(), F.expr("replace(F, R_find, R_replace)")).otherwise(F.col("F")))   # "any part": replaces the found text; "entire": R_replace
appended = j                                                                       # Append mode: the R fields come with the left join
""",
  sql="SELECT f.*, r.val FROM f LEFT JOIN r ON contains(f.x, r.find)  -- startswith(...) / f.x = r.find; replace(f.x, r.find, r.val)",
  confirm=["Several R rows matching one F row (rows multiply here; Alteryx behaviour not stated).", "No match: the F row passes unchanged (assumed).", "Case-insensitive replacement keeps the original casing of unmatched parts (not stated).",
           "Whole-word rule for punctuation and tabs; output field names on append collisions; NULL keys."])

g("Imputation",
  does="Replaces a given value (Null, or a value you type) in the selected numeric fields with the average, median, mode or a value you type.",
  settings="""Fields to impute; Incoming value to replace: Null() | a user value; Replace with: Average (of all values EXCEPT the incoming value) | Median | Mode | a user value;
optional 'Include imputed value indicator field' (`<Field>_Indicator`); optional 'Output imputed values as a separate field' (original kept, `<Field>_ImputedValue` added).""",
  pyspark=r"""
src_is_null = F.col(f).isNull()                                                   # or F.col(f) == USER_VALUE
valid = F.when(~src_is_null, F.col(f))
fill = {"average": F.avg(valid).over(Window.partitionBy()), "median": F.median(valid).over(Window.partitionBy()),
        "mode": F.mode(valid).over(Window.partitionBy())}[METHOD]                  # F.mode needs Spark 3.4+/DBR 12+
out = (df.withColumn(f + "_Indicator", src_is_null.cast("int"))                    # only when the indicator option is on
         .withColumn(f, F.when(src_is_null, fill).otherwise(F.col(f))))
""",
  sql="CASE WHEN x IS NULL THEN avg(x) OVER () ELSE x END   |  median(x) OVER () |  mode(x) OVER ()",
  confirm=["Mode ties; rounding of the average/median for integer fields; the type of the indicator field.", "All-NULL field; non-numeric input.", "Original field replaced in place vs separate field (option)."])

g("Data Cleansing",
  does="Cleans data: removes null rows/columns, replaces nulls, strips unwanted characters, changes case. Original tool = a macro (cleanse.yxmc) whose options are anonymous check boxes.",
  settings="""Fields to cleanse. Remove Null Data: Null Rows | Null Columns (run last; counts reported). Replace Nulls: with blanks (string fields, default on) | with 0 (numeric fields, default on).
Remove Unwanted Characters (string fields): Leading and trailing whitespace (default on) | Tabs, line breaks and duplicate whitespace (each run becomes ONE space) | All whitespace | Letters | Numbers |
Punctuation (! " # $ % & ' ( ) * + , \\ - . / : ; < = > ? @ [ ] ^ _ ` { | } ~). Modify Case: Upper | Lower | Title.""",
  pyspark=r"""
s = F.col(c)
s = F.coalesce(s, F.lit(""))                                                      # Replace nulls with blanks (string)   |   numeric: F.coalesce(F.col(n), F.lit(0))
s = F.trim(s)                                                                     # leading and trailing whitespace (use regexp_replace(s, r'^\s+|\s+$', '') to include tabs/line breaks)
s = F.regexp_replace(s, r"\s+", " ")                                              # tabs, line breaks and duplicate whitespace -> one space
# all whitespace: regexp_replace(s, r"\s", "")   letters: r"\p{L}"   numbers: r"\p{N}"   punctuation: r"[!\"#$%&'()*+,\\\-./:;<=>?@\[\]^_`{|}~]"
s = F.upper(s)   # or F.lower(s) / F.initcap(s)
rows_not_all_null = df.filter(~F.expr(" AND ".join(f"{c} IS NULL" for c in df.columns)))        # Remove Null Rows
""",
  sql="initcap(regexp_replace(trim(coalesce(c, '')), '\\\\s+', ' ')) AS c",
  confirm=["The ORDER in which the options run is not stated (the help only says null removal is last).", "'Check Box (n)' ids in the workflow belong to the macro: the .yxmc (or the tool in Designer) is needed to know which option each id is.",
           "Title Case for apostrophes/hyphens; the exact punctuation set; whether non-string fields are touched.", "Remove Null Columns needs a first pass to find all-NULL columns."])

g("Data Cleanse Pro",
  does="AMP-engine successor of Data Cleansing with more options (HTML tags, invisible characters, 'except' lists, blanks<->nulls both ways).",
  settings="""Remove Null Rows/Columns (all-NULL only; empty strings are kept; run after the other steps). Columns to cleanse. Remove Characters: leading/trailing whitespace (default), tabs/line breaks/duplicate
whitespace, all whitespace, HTML tags, invisible characters (zero-width space U+200B, soft hyphen U+00AD), letters / numbers / punctuation and special characters (ICU symbols incl. emoji) each with an 'Except' list.
Replace String: nulls with blanks | blanks with nulls. Replace Numeric: nulls with 0 | 0 with nulls. Modify Case: Upper | Lower | Title.""",
  pyspark=r"""
s = F.regexp_replace(F.col(c), r"<[^>]*>", "")                                    # HTML tags (text in angle brackets is removed too)
s = F.regexp_replace(s, "[​­]", "")                                     # invisible characters
s = F.regexp_replace(s, r"[^\P{L}" + EXCEPT_LETTERS + r"]", "")                    # letters except the listed ones
s = F.when(s == "", None).otherwise(s)                                            # blanks -> nulls
""",
  sql="nullif(regexp_replace(regexp_replace(c, '<[^>]*>', ''), '[\\\\u200B\\\\u00AD]', ''), '')",
  confirm=["Order of the options, 'Except' list syntax, ICU punctuation/symbol set, Title Case rules, and non-string columns."])

g("Auto Field",
  does="Reads all records and sets each selected string field to the smallest type/size that fits its data (a numeric-looking field stays a string if any value starts with 0 and is not a plain number).",
  settings="Select the string fields to auto-change (All / Clear).",
  pyspark="out = df     # values do not change; Alteryx only narrows the declared field types.  To narrow types in Spark: df.select(F.col(c).cast('int'), ...) after checking all values fit",
  sql="SELECT * FROM t",
  confirm=["Which type is chosen for each data pattern is not stated; the data itself is unchanged, so rows pass through and the type narrowing is noted, not guessed."])

g("Generate Rows",
  does="Creates new rows by an iterative expression: initialise a field, repeat while a condition holds, update by a loop expression.",
  settings="Create new field or update an existing one; Initialization expression; Condition expression; Loop expression (e.g. DateTimeAdd([f],1,'days') or [f]+1).",
  pyspark=r"""
out = df.withColumn("f", F.explode(F.sequence(F.to_date(F.lit(START)), F.to_date(F.lit(END)), F.expr("interval 1 day"))))     # dates
out = df.withColumn("f", F.explode(F.sequence(F.lit(1).cast("long"), F.lit(N).cast("long"), F.lit(1))))                        # numbers
""",
  sql="SELECT *, explode(sequence(to_date(start), to_date(end), interval 1 day)) AS f FROM t",
  confirm=["Only the supported pattern (see status); other loops (state kept between iterations) have no sequence() form and are reported.", "Empty range and boundary behaviour."])

# =================================================================================================================== Join
g("Join Multiple",
  does="Joins two or more inputs on key fields (or by record position); default is a FULL OUTER join.",
  settings="Join by Specific Fields (a key per input) | Join by Record Position; 'Only output records that join from all inputs' (turns it into an inner join); cartesian guard (allow / warn / error above 16 records);"
           " per-column select, rename, type, 'Deselect duplicate columns'.",
  pyspark=r"""
from functools import reduce
joined = reduce(lambda a, b: a.join(b, on=KEYS, how="full"), inputs)              # "inner" when 'only output records that join from all inputs' is on
joined = joined.select(*[F.coalesce(*[d[k] for d in inputs]).alias(k) for k in KEYS], *OTHER_COLUMNS)    # a full join leaves one key per side: coalesce them
# by record position: add F.row_number().over(Window.orderBy(<incoming order>)) to every input and join on it
""",
  sql="SELECT coalesce(a.k, b.k, c.k) AS k, a.x, b.y, c.z FROM a FULL OUTER JOIN b ON a.k = b.k FULL OUTER JOIN c ON coalesce(a.k, b.k) = c.k",
  confirm=["Names given to colliding non-key columns of different inputs (not stated).", "Many-to-many matches and key type rules.", "Output row order.", "Record-position join with unequal row counts."])

g("Make Group",
  does="Groups values that are connected through pairs: if A relates to X and X to B, A, X and B form one group (transitive closure).",
  settings="1st Key and 2nd Key columns (each row is one relationship). Output: Group, Key.",
  pyspark=r"""
edges = df.select(F.col(K1).alias("a"), F.col(K2).alias("b")).unionByName(df.select(F.col(K2).alias("a"), F.col(K1).alias("b")))
label = edges.select("a").distinct().withColumn("g", F.col("a"))                   # start: every key its own label
while True:                                                                        # min-label propagation until nothing changes
    nxt = (edges.join(label.withColumnRenamed("a", "b").withColumnRenamed("g", "gb"), "b")
                .groupBy("a").agg(F.min("gb").alias("m"))
                .join(label, "a").select("a", F.least("g", "m").alias("g")))
    if nxt.exceptAll(label).count() == 0: break
    label = nxt
out = label.select(F.col("g").alias("Group"), F.col("a").alias("Key"))              # group named by its smallest key (the help's example: A and L)
""",
  sql="Spark SQL has no recursion on this engine; use the PySpark loop, GraphFrames connectedComponents(), or a recursive CTE where the runtime supports it",
  confirm=["How groups are named (the example's A and L are the smallest key of each group - assumed).", "NULL keys, self-links, duplicate rows, and whether other input columns pass through."])

g("Fuzzy Match",
  does="Finds non-identical duplicates by comparing fields with similarity styles (name, address, company, phone, ZIP ...) against a threshold (default 80%).",
  settings="Purge (compare all records) | Merge (only records from different sources, needs a Source ID field); Record ID field; Match threshold; Match fields with a style, weight and options; output score / keys / unmatched.",
  pyspark=r"""
# No exact equivalent: Alteryx's styles use Double Metaphone, word-frequency analysis and nickname tables.  An approximation:
a, b = df.alias("a"), df.alias("b")
pairs = a.join(b, (F.col("a.id") < F.col("b.id")) & (F.soundex("a.name") == F.soundex("b.name")))          # blocking key
scored = pairs.withColumn("score", 1 - F.levenshtein("a.name", "b.name") / F.greatest(F.length("a.name"), F.length("b.name")))
out = scored.filter(F.col("score") >= 0.80)
""",
  sql="levenshtein(a.n, b.n), soundex(a.n); Double Metaphone is not built in",
  confirm=["The scores differ from Alteryx's (different algorithms): the matched set can differ, so equivalence cannot be claimed. Treat as a re-implementation to be reviewed with the business."])

# =================================================================================================================== Parse
g("RegEx",
  does="Applies a regular expression to one column: Replace, Parse (marked groups into columns), Tokenize (split to columns/rows) or Match (1/0).",
  settings="""Column to parse; Regular Expression; Case Insensitive (ON by default); Output method: Replace (Replacement Text with marked groups; Copy Unmatched Text) | Tokenize (the regex matches what you KEEP; one marked
group allowed; to columns: Number of Columns, Extra Columns = drop with/without warning or error, Output Root Name; to rows) | Parse (Output Columns: name, type, size, expression per marked group) |
Match (column name for match status; Error if not Matched). Syntax: Boost/Perl.""",
  pyspark=r"""
pat = "(?i)" + PATTERN                                                             # Case Insensitive default on
replace  = df.withColumn(C, F.regexp_replace(F.col(C), pat, REPLACEMENT))          # marked group n = $n in Java regex
parse    = df.select("*", *[F.regexp_extract(C, pat, i).alias(NAMES[i - 1]) for i in range(1, GROUPS + 1)])
tokens   = F.regexp_extract_all(F.col(C), F.lit(pat), F.lit(1))                    # group 1 of every match
to_cols  = df.select("*", *[F.get(tokens, i).alias(f"{ROOT}{i + 1}") for i in range(NCOLS)])
to_rows  = df.withColumn(C, F.explode(tokens))
match    = df.withColumn(NAME, F.when(F.col(C).rlike(pat), 1).otherwise(0))
""",
  sql="regexp_replace(c, '(?i)pat', '$1') | regexp_extract(c, '(?i)pat', 1) | regexp_extract_all(c, '(?i)pat', 1) | CASE WHEN c RLIKE '(?i)pat' THEN 1 ELSE 0 END",
  confirm=["Boost vs Java regex differences (POSIX classes, \\< \\>, possessive/atomic groups): every non-trivial pattern must be re-tested.", "Match: does the pattern have to match the whole value or any part (not stated; RLIKE = any part).",
           "Copy Unmatched Text, Parse output for non-matching rows (NULL here), replacement group syntax, default types/sizes."])

g("XML Parse",
  does="Parses an XML string column into fields: the root, an auto-detected child, or a named child element.",
  settings="Field with XML data; Include in output; XML element to parse: Root | Auto Detect Child (the element occurring most often under ONE parent in the FIRST row; ties/none -> first child of root) | Specific Child Name (case-sensitive);"
           " Return Child Values; Return Outer XML (adds `<field>_OuterXML`); Ignore XML errors.",
  pyspark=r"""
rows  = df.withColumn("el", F.explode(F.expr("xpath(xml, '//record')")))             # the parsed child element, one row per occurrence
cols  = rows.select("*", F.expr("xpath_string(el, 'field1')").alias("field1"), F.expr("xpath_string(el, 'field2')").alias("field2"))
parsed = df.withColumn("p", F.from_xml("xml", "struct<record:array<struct<field1:string,field2:string>>>"))   # Spark 4 / DBR 14.3+: schema_of_xml(sample) can infer the schema
""",
  sql="SELECT *, explode(xpath(xml, '//record')) AS el FROM t  |  xpath_string(el, 'field1')  |  from_xml(xml, 'struct<...>')",
  confirm=["Auto Detect looks only at the first input row: in Spark the element must be fixed at conversion time (ask).", "Output field names, types, attributes, namespaces, CDATA, and the rows produced per input row."])

g("DateTime",
  does="Converts between a date-time text in a custom format and Alteryx's standard date-time text.",
  settings="Direction: Date/Time to string (format) | String to Date/Time (format); input field; output field; format (Alteryx tokens); language.",
  pyspark=r"""
to_text = df.withColumn(OUT, F.date_format(F.try_to_timestamp(F.col(IN).cast("string")), "yyyy/MM/dd"))            # Java pattern letters
from_text = df.withColumn(OUT, F.date_format(F.try_to_timestamp(F.col(IN).cast("string"), F.lit("dd/MM/yyyy")), "yyyy-MM-dd HH:mm:ss"))
""",
  sql="date_format(try_to_timestamp(c), 'yyyy/MM/dd') | date_format(try_to_timestamp(c, 'dd/MM/yyyy'), 'yyyy-MM-dd HH:mm:ss')",
  confirm=["Alteryx tokens (yyyy, MM, dd, hh) vs Java letters (hh = 12-hour; HH = 24-hour) and the language."])

# =================================================================================================================== Transform
g("Arrange",
  does="Manually transposes fields for presentation: each input record becomes several records, one per row of the Output Fields table; columns come from the selected fields; optional description column.",
  settings="Key Fields (carried on every output row); Output Fields table: columns (header, fields supplying its data) and rows (each row = one set of fields, with a description taken from field descriptions, field names or left empty).",
  pyspark=r"""
rows = [("Sales", {"Q1": "Q1_Sales", "Q2": "Q2_Sales"}), ("Cost", {"Q1": "Q1_Cost", "Q2": "Q2_Cost"})]       # one tuple per output-row definition: (description, {output column: source field})
out = reduce(DataFrame.unionByName, [df.select(*KEYS, F.lit(desc).alias("Description"), *[F.col(src).alias(col) for col, src in m.items()]) for desc, m in rows])
""",
  sql="SELECT k, 'Sales' AS Description, Q1_Sales AS Q1, Q2_Sales AS Q2 FROM t UNION ALL SELECT k, 'Cost', Q1_Cost, Q2_Cost FROM t",
  confirm=["The help does not give the algorithm: the recipe is the straightforward reading (one select per defined row) - verify the row count and order against Alteryx.", "Output types for mixed-type columns; NULL / 'Leave empty' representation; row order."])

g("Make Columns",
  does="Arranges rows into several columns (e.g. 50 records into 5 columns x 10 rows), optionally per group.",
  settings="Number of Columns (C); Arrange Horizontally (fill left to right, then wrap) | Vertically (fill column by column); Group by fields (records sorted by group, each group laid out alone). Output names `Column_#_<Field>`; short cells are NULL.",
  pyspark=r"""
w = Window.partitionBy(*GROUPS).orderBy(*ORDER)                                    # incoming position within the group
d = df.withColumn("_i", F.row_number().over(w) - 1).withColumn("_n", F.count(F.lit(1)).over(Window.partitionBy(*GROUPS)))
if ARRANGE == "horizontal":                                                        # fill left to right, then wrap
    placed = d.withColumn("_row", F.floor(F.col("_i") / C)).withColumn("_col", F.col("_i") % C)
else:                                                                              # "vertical": fill column by column (the help's examples)
    rpb = F.ceil(F.col("_n") / C)                                                  # rows per block
    placed = d.withColumn("_row", F.col("_i") % rpb).withColumn("_col", F.floor(F.col("_i") / rpb))
out = (placed.groupBy(*GROUPS, "_row")
       .agg(*[F.max(F.when(F.col("_col") == k, F.col(FIELD))).alias(f"Column_{k + 1}_{FIELD}") for k in range(C)]).drop("_row"))      # short cells are NULL
""",
  sql="max(CASE WHEN col = 0 THEN food END) AS Column_1_Food ... GROUP BY g, row   (row = floor(i / C), col = i % C; vertical: row = i % ceil(n / C), col = floor(i / ceil(n / C)))",
  confirm=["The help's prose and its vertical examples disagree; the examples (column-major) are used.", "Group sort direction (ascending assumed), column order for several fields, output types, multiple group fields."])

g("Weighted Average",
  does="Weighted average of a numeric value field using a numeric weight field, optionally per group.",
  settings="Value field; Weight field; Output field name; Grouping fields (optional). Equal weights give the ordinary average.",
  pyspark=r"""
out = df.groupBy(*GROUPS).agg((F.sum(F.col(V) * F.col(W)) / F.sum(F.col(W))).alias(OUT_NAME))     # no groups: df.agg(...) -> one row
""",
  sql="SELECT g, try_divide(sum(v * w), sum(w)) AS Weighted_Average FROM t GROUP BY g",
  confirm=["The formula is not printed on the page (sum(w*v)/sum(w) is the standard definition).", "Output shape (one row per group assumed; original records not kept), output type, NULL / zero / negative weights, zero total weight (NULL here via try_divide)."])

g("Count Records",
  does="Returns the number of records that pass through; unlike Summarize it returns a count of 0 for empty input.",
  settings="None.",
  pyspark='out = df.agg(F.count(F.lit(1)).alias("Count"))                          # always one row, 0 for empty input',
  sql="SELECT count(*) AS Count FROM t",
  confirm=["The output field name ('Count') and type are not stated on the help page."])

g("Cross Tab",
  does="Pivots rows to columns: each distinct value of the header field becomes a column; groups stay as rows.",
  settings="Group fields; Header field; Value field; aggregation by value type (string: Concatenate, First, Last, Count Distinct ...; numeric: Sum, Average, ...).",
  pyspark='out = df.groupBy(*GROUPS).pivot(HEADER, VALUES).agg(F.sum(VALUE))              # VALUES listed explicitly: fixed schema, one pass',
  sql="SELECT * FROM t PIVOT (sum(v) FOR h IN ('x', 'y'))      -- the value list must be known when the SQL is written",
  confirm=["Pivot values are run-time data: SQL needs them listed (read them once with a SELECT DISTINCT)", "Column naming/case folding of header values, NULL handling, output order."])

g("Transpose",
  does="Key columns repeat; the data columns become rows with Name (the column name) and Value (as text).",
  settings="Key fields; Data fields; Missing columns: Error / Warn / Ignore.",
  pyspark='out = df.select(*KEYS, F.explode(F.array(*[F.struct(F.lit(c).alias("Name"), F.col(c).cast("string").alias("Value")) for c in DATA])).alias("kv")).select(*KEYS, "kv.Name", "kv.Value")',
  sql="SELECT k, kv.Name, kv.Value FROM (SELECT k, explode(array(struct('a' AS Name, CAST(a AS string) AS Value), struct('b' AS Name, CAST(b AS string) AS Value))) AS kv FROM t)",
  confirm=["Output field order; the type of Value is text."])

g("Summarize",
  does="Groups and aggregates: group by, sum, count, average, min, max, median, concatenate, first/last, percentile, mode ...",
  settings="Field + action (+ output name); group-by fields; separators for Concat; percentile value.",
  pyspark='out = df.groupBy(*GROUPS).agg(F.sum("x").alias("Sum_x"), F.count(F.lit(1)).alias("Count"), F.avg("x"), F.min("x"), F.max("x"), F.median("x"), F.concat_ws(",", F.collect_list(F.col("t").cast("string"))))',
  sql="SELECT g, sum(x), count(1), avg(x), min(x), max(x), median(x), concat_ws(',', collect_list(CAST(t AS string))) FROM t GROUP BY g",
  confirm=["Alteryx returns groups sorted; Spark does not.", "Count counts NULL records, Sum/Avg ignore NULLs; Concat order; String Min/Max byte order vs dictionary order."])

g("Running Total",
  does="Running sum of numeric fields in record order, restarting per group.",
  settings="Fields to total; Group by; output fields prefixed `RunTot_`.",
  pyspark='out = df.withColumn("RunTot_x", F.sum("x").over(Window.partitionBy(*GROUPS).orderBy(*ORDER).rowsBetween(Window.unboundedPreceding, 0)))',
  sql="sum(x) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunTot_x",
  confirm=["The record order (explicit ORDER required) and NULL handling."])

# =================================================================================================================== Developer / dynamic
g("Dynamic Select",
  does="Selects (or deselects) fields by field type or by a formula over the field metadata, including fields only known at run time.",
  settings="Select Field Types (check boxes) | Select via a Formula over: Name, Type, Size, Scale, Source, Description, IsNumeric, IsString, IsFloat, IsInteger, IsStringOrDate, IsDateOrTime, IsBinary, IsSpatialObj, FieldNumber.",
  pyspark=r"""
names = [f.name for f in df.schema.fields if "ID" in f.name]                       # FINDSTRING([Name],"ID")>-1
numeric = [f.name for f in df.schema.fields if isinstance(f.dataType, (T.IntegerType, T.LongType, T.DoubleType, T.FloatType, T.DecimalType, T.ShortType, T.ByteType))]
out = df.select(*names)                                                            # deselect: df.drop(*names);  FieldNumber==1 -> df.columns[0]
""",
  sql="Resolve the column list from the table schema at conversion time (information_schema.columns), then SELECT those columns.",
  confirm=["Outputs/anchors, behaviour when nothing matches, and how a deselect is expressed."])

g("Dynamic Replace",
  does="Replaces values across a series of fields according to rules in a second input (R): per field name, a Boolean expression (first match wins) and the replacement value or expression.",
  settings="D input (data), R input (3 columns): Field Name field, Boolean Expression field (non-zero = true; `_CurrentField_` = the field being processed), Output Value field; values are expressions or static;"
           " field types: autofield / keep original / change to; unmatched: allow / warn / error. Outputs O (data) and S (summary of replacements).",
  pyspark=r"""
rules = [("Field1", "Field1 < 100", "'A'"), ("Field1", "Field1 < 200", "'B'"), ("Field1", "Field1 >= 200", "'C'")]    # R rows in order (read once at conversion time)
for field in FIELDS:
    chain = F.expr("CASE " + " ".join(f"WHEN {cond.replace('_CurrentField_', field)} THEN {val}" for f, cond, val in rules if f == field) + " ELSE NULL END")
    df = df.withColumn(field, chain)                                               # first matching rule wins; no match -> NULL
""",
  sql="CASE WHEN Field1 < 100 THEN 'A' WHEN Field1 < 200 THEN 'B' WHEN Field1 >= 200 THEN 'C' END AS Field1",
  confirm=["The expression syntax is Alteryx's (translate it with the expression rules), NULL handling, case-sensitivity of the field-name match, the S summary schema."])

g("Dynamic Input",
  does="Reads from a list of files/tables (or a modified SQL query) at run time and joins the result to the incoming stream.",
  settings="Input template; Read a List of Data Sources (append suffix / prepend prefix / change file or table name / change entire path from a field) | Modify SQL Query (pass a field, replace a string, update WHERE, spatial filter, stored procedure).",
  pyspark=r"""
paths = [r["path"] for r in control_df.select("path").distinct().collect()]       # the control list is small
out = reduce(DataFrame.unionByName, [spark.read.format("delta").table(p) for p in paths])      # or spark.read.load(p) for files
""",
  sql="SELECT * FROM identifier(:table_name)  -- a parameterised table name; one query per entry, UNION ALL",
  confirm=["Ingestion: out of scope for the skill - land the sources as tables and read them. Join type/keys to the incoming stream, schema mismatch, NULLs."])

g("JSON Parse",
  does="Parses a JSON text column. Four modes: single string field, data-type-specific fields, Unnest (one level), Flatten array.",
  settings="""JSON field; Input in output. Mode 1: JSON_Name (dot path, arrays 0-based) + JSON_ValueString, one row per leaf. Mode 2: JSON_Name + JSON_ValueString / ValueInt / ValueFloat / ValueBool (the typed field holds the value, others NULL).
Mode 3 Unnest (AMP): one level of an object into columns `<col>.<key>` holding the nested JSON text. Mode 4 Flatten (AMP): one row per array element, `<col>_flatten` and 1-based `<col>_idx`.""",
  pyspark=r"""
v = F.parse_json(F.col("j"))                                                       # VARIANT (DBR 15.3+ / Spark 4)
df.createOrReplaceTempView("_t")
level1 = spark.sql("SELECT _t.*, v.pos, v.key, to_json(v.value) AS value FROM _t, LATERAL variant_explode(parse_json(j)) AS v")      # top level; repeat on object/array values for deeper levels
flatten = (df.select("*", F.posexplode(F.from_json(COL, "array<string>")).alias("pos", COL + "_flatten"))
             .withColumn(COL + "_idx", F.col("pos") + 1).drop("pos"))                                          # COL = the array column; the index is 1-based (the help)
unnest = df.select("*", *[F.get_json_object("j", f"$.{k}").alias(f"j.{k}") for k in KEYS])        # KEYS read once from a sample or given
known_schema = df.withColumn("p", F.from_json("j", SCHEMA)).select("*", "p.*")    # when the structure is fixed this is simplest and exact
""",
  sql="from_json(j, 'struct<...>') | get_json_object(j, '$.a.b') | variant_explode(parse_json(j)) | posexplode(from_json(j, 'array<string>'))",
  confirm=["Dot-path flattening of arbitrary depth has no single Spark call: the depth/keys must be known or looped.", "JSON null vs [Null], invalid JSON handling, key order, default mode, dotted keys."])

g("Field Info",
  does="Returns one row per incoming field: Name, Type, Size, Scale, Source, Description (also for an empty stream). Used with Test / Block Until Done to validate a schema.",
  settings="None.",
  pyspark='out = spark.createDataFrame([(f.name, f.dataType.simpleString()) for f in df.schema.fields], "Name string, Type string")',
  sql="DESCRIBE TABLE t   |   SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 't'",
  confirm=["Type names (Alteryx V_WString vs Spark string), Size and Scale have no Spark counterpart."])

g("Date Time Now",
  does="Returns ONE record with the date-time at workflow run time as a string in the chosen language and format.",
  settings="DateTime Language (8 languages); Output Format (e.g. yyyy-MM-dd hh:mm:ss, dd/MM/yyyy ...). Time zone: system or the workflow runtime setting.",
  pyspark='out = spark.range(1).select(F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("DateTime_Out"))     # Java letters; set spark.sql.session.timeZone',
  sql="SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS DateTime_Out",
  confirm=["The output field name and type are not stated; the help's 'hh' formats are 12-hour in Java (HH = 24-hour).", "Localised month/day names, time zone."])

g("Directory",
  does="Lists files of a folder (optionally subfolders) with FullPath, Directory, FileName, ShortFileName, times, Size and file attributes.",
  settings="Directory; File specification (wildcard, default *.*); Include subdirectories.",
  pyspark=r"""
files = (spark.read.format("binaryFile").option("recursiveFileLookup", "true").option("pathGlobFilter", "*.csv").load("/Volumes/c/s/v/folder")
         .select("path", "modificationTime", "length"))                              # path, modificationTime, length (bytes)
""",
  sql="LIST '/Volumes/c/s/v/folder'      -- path, name, size, modification_time",
  confirm=["Only path/name/size/modification time exist in Spark; creation time and the attribute flags do not.", "Ingestion-side tool: usually replaced by Auto Loader."])

g("Run Command",
  does="Runs an external program/command with files in and out.", settings="Command, arguments, working directory, read/write results.",
  pyspark="# No equivalent inside a Spark transformation: use a Databricks job task (notebook/script/shell) before or after the notebook; keep its input/output as tables.",
  sql="-- not applicable", confirm=["What the command does must be re-implemented or scheduled separately."])

g("Block Until Done",
  does="Runs its inputs in order: each output waits until the previous one has finished (control only; the data is unchanged).", settings="None.",
  pyspark="out = df     # order is given by the notebook's cell order / job task dependencies", sql="SELECT * FROM t", confirm=[])

g("Python",
  does="Embedded Python (Jupyter) code that reads the incoming data and writes a result.",
  settings="The notebook code; input and output through the Alteryx.read/write API.",
  pyspark="# Port cell by cell: replace Alteryx.read() with spark.table(...)/the incoming DataFrame, pandas code with DataFrame API where possible (see references/10-python-code-tools.md).",
  sql="-- only when the Python is a pure transformation expressible in SQL", confirm=["Behaviour of arbitrary Python cannot be proven equivalent: review and test the ported cells."])

# =================================================================================================================== In/Out
g("Input Data",
  does="Reads a file or a database table/query and gives the records.", settings="Source (file/format/sheet or database connection + table/query), field list.",
  pyspark='df = spark.table("catalog.schema.table")           # the source is landed as a table elsewhere; the converted notebooks read it, they never ingest',
  sql="SELECT * FROM catalog.schema.table", confirm=["The SQL query in a database input is shown in 01_bronze; if it filters or joins in the database, that logic must be in the Bronze table or added."])

g("Output Data",
  does="Writes the stream to a file or database.", settings="Destination, format, write mode.",
  pyspark='df.write.format("delta").mode("errorifexists").saveAsTable("catalog.schema.table")      # guarded: overwrite only when ALLOW_OVERWRITE is True',
  sql="CREATE TABLE catalog.schema.table AS SELECT ...", confirm=["An export to the original file/database destination is added by the user."])

g("Text Input",
  does="Embedded rows typed into the workflow.", settings="Field names, types, rows.",
  pyspark='df = spark.createDataFrame([("99", "Zed")], "`CustId` string, `Name` string").select(F.col("CustId").cast("int").alias("CustId"), "Name")',
  sql="SELECT CAST(CustId AS int) AS CustId, Name FROM VALUES ('99', 'Zed') AS t(CustId, Name)", confirm=[])

# the other converted tools are documented where the emitters live
for _name, _does, _py, _sql in [
    ("Filter", "Splits records into True and False outputs by a Boolean expression (a NULL condition goes to False).", 'true_df = df.filter(F.coalesce(cond, F.lit(False)))\nfalse_df = df.filter(~F.coalesce(cond, F.lit(False)))', "WHERE coalesce(cond, false)  /  WHERE NOT coalesce(cond, false)"),
    ("Formula", "Creates or updates fields from expressions (translated by alteryx_expr.py: IF, Switch, string, math, date functions).", 'out = df.withColumn("New", expr).withColumn("Old", expr2)', "SELECT *, expr AS New FROM t"),
    ("Select", "Selects, renames, retypes, resizes and reorders fields; *Unknown controls the rest.", 'out = df.select(F.col("a").alias("A"), F.col("b").cast("int"), *rest)', "SELECT a AS A, CAST(b AS int) AS b, <rest> FROM t"),
    ("Sort", "Orders records (NULLs first ascending, last descending).", 'out = df.orderBy(F.col("a").asc_nulls_first(), F.col("b").desc_nulls_last())', "ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST"),
    ("Unique", "Keeps the first record per key (Unique output) and the others on the Duplicates output; the order decides which is first.", 'w = Window.partitionBy(*KEYS).orderBy(*ORDER)\nranked = df.withColumn("_rn", F.row_number().over(w))\nunique = ranked.filter("_rn = 1").drop("_rn")\ndups = ranked.filter("_rn > 1").drop("_rn")', "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY k ORDER BY o) rn FROM t) WHERE rn = 1"),
    ("Record ID", "Adds a sequential id (start value, type) in record order.", 'out = df.select((F.row_number().over(Window.orderBy(*ORDER)) + (START - 1)).cast("bigint").alias("RecordID"), "*")', "SELECT row_number() OVER (ORDER BY o) + START - 1 AS RecordID, * FROM t"),
    ("Join", "Joins two inputs on keys; outputs J (matched), L (left unmatched), R (right unmatched).", 'j = l.alias("l").join(r.alias("r"), F.col("l.k") == F.col("r.k"), "inner")\nleft_only = l.alias("l").join(r.alias("r"), F.col("l.k") == F.col("r.k"), "left_anti")', "SELECT ... FROM l JOIN r ON l.k = r.k;  SELECT l.* FROM l LEFT ANTI JOIN r ON l.k = r.k"),
    ("Union", "Stacks inputs by field name (missing fields NULL) or by position.", 'out = a.unionByName(b, allowMissingColumns=True)', "SELECT a, NULL AS b FROM x UNION ALL SELECT a, b FROM y"),
    ("Append Fields", "Adds every Source record's fields to every Target record (cartesian product).", 'out = targets.crossJoin(source)', "SELECT * FROM t CROSS JOIN s"),
    ("Text To Columns", "Splits a field into N columns or into rows at a delimiter.", 'cols = df.select("*", *[F.get(F.split("t", ","), i).alias(f"Part{i + 1}") for i in range(N)])\nrows = df.withColumn("t", F.explode(F.split("t", ",")))', "SELECT *, split(t, ',')[0] AS Part1 FROM t"),
    ("Multi-Field Formula", "Applies one expression to many fields (_CurrentField_).", 'out = df.withColumns({c: F.col(c) * 2 for c in FIELDS})', "SELECT a * 2 AS a, b * 2 AS b FROM t"),
    ("Multi-Row Formula", "Expression using other rows ([Row-1:f]), per group, in record order.", 'out = df.withColumn("Delta", F.col("x") - F.lag("x", 1).over(Window.partitionBy(*GROUPS).orderBy(*ORDER)))', "x - lag(x, 1) OVER (PARTITION BY g ORDER BY o) AS Delta"),
    ("Dynamic Rename", "Renames fields by formula/prefix/suffix/first row/right input.", 'out = df.toDF(*[NEW.get(c, c) for c in df.columns])', "SELECT a AS A_NEW, b AS B_NEW FROM t"),
]:
    if _name not in GUIDE:
        g(_name, _does, "", _py, _sql, [])


# =================================================================================================================== everything else: what it is and the Databricks route
ROUTE = {
    # name -> (what it does, Databricks route)
    "Browse": ("Shows the data for inspection.", "display(df) in the notebook; nothing to convert."),
    "Map Input": ("Interactive map-drawn spatial input.", "Pass the polygon/point as a parameter or table."),
    "Auto Insights Uploader": ("Sends data to Alteryx Auto Insights.", "Out of scope."),
    "Email": ("Sends emails with data/reports.", "Databricks job notifications or a notebook email step."),
    "Tool Container": ("Groups tools visually.", "Its name becomes a section of the notebook."),
    "Comment": ("Text annotation.", "Markdown cell."), "Explorer Box": ("Annotation.", "Markdown cell."),
    "Create Points": ("Builds a point from longitude/latitude fields.", "st_point(lon, lat) (Databricks ST functions); check SRID."),
    "Distance": ("Distance between two spatial objects (units: miles/km).", "st_distancesphere(a, b) in metres, converted to the tool's units; verify vs Alteryx."),
    "Spatial Match": ("Matches points/polygons by relation (contains, intersects, touches ...).", "Join on st_contains / st_intersects / st_touches; H3 index for scale."),
    "Buffer": ("Polygon around a spatial object at a distance.", "st_buffer (units follow the SRID; reproject for metres)."),
    "Find Nearest": ("Nearest spatial objects within distance/count.", "Cross join with st_distancesphere + row_number() per origin; H3 for scale."),
    "Make Grid": ("Builds a grid of cells over an extent.", "Generate cell polygons with sequence() + st_geomfromwkt, or H3 cells."),
    "Trade Area": ("Radius / drive-time polygons around points.", "st_buffer for radius; drive-time needs a routing service."),
    "Poly-Build": ("Builds polygons from points.", "st_convexhull(st_collect_agg(point)) / st_makepolygon."),
    "Poly-Split": ("Splits polygons into points.", "explode(st_dump(geom)) / st_pointn."),
    "Spatial Info": ("Area, centroid, bounds ... of a spatial object.", "st_area, st_centroid, st_envelope."),
    "Generalize": ("Simplifies polygons.", "st_simplify."), "Smooth": ("Smooths polygons.", "No direct equivalent; st_simplify + st_buffer approximations."),
    "Heat Map": ("Raster heat map of points.", "Aggregate by H3 cell and visualise in a dashboard."),
    "Spatial Process": ("Combine / cut / merge spatial objects.", "st_union_agg / st_difference / st_intersection."),
    "Non Overlapping Drivetime": ("Drive-time polygons without overlaps.", "Needs a routing service; not a Spark transformation."),
    "Interactive Chart": ("Chart output.", "A Databricks AI/BI dashboard or display()."),
    "Layout": ("Report layout.", "Dashboard."), "Render": ("Renders reports to files.", "Dashboard / export job."), "Table": ("Report table.", "Dashboard table."),
    "Image": ("Report image.", "Dashboard."), "Overlay": ("Report overlay.", "Dashboard."), "Report Text": ("Report text.", "Dashboard text."),
    "Report Header": ("Report header.", "Dashboard."), "Report Footer": ("Report footer.", "Dashboard."), "Report Map": ("Report map.", "Dashboard map."),
    "Map Legend Builder": ("Map legend.", "Dashboard."), "Map Legend Splitter": ("Map legend.", "Dashboard."),
    "Base64 Encoder": ("Base64-encodes a field.", "base64(c) / unbase64(c) in Spark SQL."),
    "Blob Input": ("Reads binary files into a blob field.", "spark.read.format('binaryFile')."), "Blob Output": ("Writes blob fields to files.", "Write bytes with a Volume path in a job."),
    "Blob Convert": ("Converts between blob and text.", "cast(c AS string) / encode / decode."),
    "Download": ("HTTP request per record.", "Out of scope (ingestion); a job or http_request() on Databricks."),
    "Message": ("Writes a message to the log.", "print()/logging."), "Test": ("Assertion on the data.", "Delta constraints / expectations / assert in the notebook."),
    "Expect Equal": ("Asserts two streams are equal.", "exceptAll both ways must be empty."), "Throttle": ("Slows the stream.", "Not needed."),
    "Control Container": ("Conditional execution of tools.", "Notebook/job conditional logic."), "Detour": ("Route data conditionally.", "Notebook/job conditional logic."),
    "Detour End": ("Ends a detour.", "Notebook/job conditional logic."),
    "Apache Spark Code": ("Already Spark code.", "Paste it into a notebook cell."), "R": ("Embedded R.", "Port to PySpark/sparklyr by hand."),
    "Predict": ("Scores data with a model.", "mlflow.pyfunc.spark_udf / ai_query on a registered model."),
    "Score": ("Scores data with a model.", "mlflow.pyfunc.spark_udf on a registered model."),
    "Decision Tree": ("Fits a tree model.", "pyspark.ml.classification.DecisionTreeClassifier."), "Forest Model": ("Random forest.", "RandomForestClassifier/Regressor."),
    "Boosted Model": ("Gradient boosting.", "GBTClassifier/GBTRegressor or xgboost."), "Linear Regression": ("OLS regression.", "pyspark.ml.regression.LinearRegression."),
    "Logistic Regression": ("Logistic model.", "pyspark.ml.classification.LogisticRegression."), "Neural Network": ("Neural net.", "pyspark.ml MultilayerPerceptron / PyTorch."),
    "Naive Bayes Classifier": ("Naive Bayes.", "pyspark.ml.classification.NaiveBayes."), "Support Vector Machine": ("SVM.", "LinearSVC."),
    "Gamma Regression": ("GLM gamma.", "GeneralizedLinearRegression(family='gamma')."), "Count Regression": ("Poisson/NB regression.", "GeneralizedLinearRegression(family='poisson')."),
    "Spline Model": ("Spline regression.", "Custom (patsy/statsmodels)."), "Stepwise": ("Stepwise selection.", "Custom."),
    "K-Centroids Cluster Analysis": ("K-means style clustering.", "pyspark.ml.clustering.KMeans."), "Principal Components": ("PCA.", "pyspark.ml.feature.PCA."),
    "Find Nearest Neighbors": ("Nearest neighbours.", "BucketedRandomProjectionLSH / approxNearestNeighbors."), "MB Affinity": ("Market basket.", "pyspark.ml.fpm.FPGrowth."),
    "ARIMA": ("Time series model.", "statsmodels/Prophet on groups with applyInPandas."), "ETS": ("Time series model.", "statsmodels ExponentialSmoothing with applyInPandas."),
    "TS Forecast": ("Forecast.", "Databricks forecasting / applyInPandas."), "Test of Means": ("Statistical test.", "scipy via applyInPandas or Spark SQL aggregates."),
    "Optimization": ("Linear/integer optimisation.", "scipy.optimize / PuLP in a job."), "Simulation Sampling": ("Monte Carlo sampling.", "rand()/randn() with seeds."),
    "Field Summary": ("Summary statistics.", "df.summary() / Databricks data profiling."), "Basic Data Profile": ("Profile.", "dbutils.data.summarize / profiling."),
    "Frequency Table": ("Counts per value.", "groupBy().count()."), "Histogram": ("Histogram.", "Bucketed counts / display()."), "Scatterplot": ("Plot.", "display()."),
    "Pearson Correlation": ("Correlation.", "df.stat.corr / pyspark.ml.stat.Correlation."), "Spearman Correlation": ("Rank correlation.", "Correlation.corr(df, col, 'spearman')."),
    "Contingency Table": ("Cross-tab counts.", "df.stat.crosstab."),
    "Parse Address": ("Splits addresses into parts.", "A geocoding/address service or regex rules; no built-in."),
    "US Geocoder": ("Geocodes US addresses.", "External geocoding provider."), "Street Geocoder": ("Geocodes addresses.", "External geocoding provider."),
    "Reverse Geocoder": ("Coordinates to address.", "External provider."), "CASS": ("USPS address standardisation.", "External provider."), "US ZIP9 Coder": ("ZIP+4 lookup.", "External provider."),
    "Google BigQuery Input": ("Reads BigQuery.", "Lakehouse Federation / the BigQuery connector; land as a table."), "Google BigQuery Output": ("Writes BigQuery.", "BigQuery connector."),
    "Google Sheets Input": ("Reads a Google Sheet.", "Land the sheet as a table (connector/Volume)."), "Amazon S3 Download": ("Reads S3 files.", "Volumes/external locations, Auto Loader."),
    "Salesforce Input": ("Reads Salesforce.", "Lakeflow Connect Salesforce."), "SharePoint Files Input": ("Reads SharePoint.", "Lakeflow Connect SharePoint."),
    "MongoDB Input": ("Reads MongoDB.", "Spark MongoDB connector."),
}


def entry(name: str) -> dict | None:
    return GUIDE.get(name)


def route(name: str) -> tuple | None:
    return ROUTE.get(name)


def render(name: str, with_code: bool = True) -> str:
    """The guide entry of one tool as Markdown (used by the reference file, the hand-off comments and the model prompt)."""
    e = GUIDE.get(name)
    if not e:
        r = ROUTE.get(name)
        return f"**{name}** - {r[0]} Databricks: {r[1]}" if r else ""
    parts = [f"**What it does:** {e['does']}"]
    if e["settings"]:
        parts.append(f"**Settings:** {' '.join(e['settings'].split())}")
    if with_code and e["pyspark"]:
        parts.append("**PySpark:**\n```python\n" + e["pyspark"] + "\n```")
    if with_code and e["sql"]:
        parts.append("**Spark SQL:**\n```sql\n" + e["sql"] + "\n```")
    if e["confirm"]:
        parts.append("**Not stated by Alteryx - confirm against real output:**\n" + "\n".join(f"- {c}" for c in e["confirm"]))
    return "\n\n".join(parts)
