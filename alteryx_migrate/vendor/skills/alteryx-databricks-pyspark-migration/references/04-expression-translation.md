# Alteryx expressions -> PySpark Column code (`scripts/alteryx_expr.py`)

The translator is a tokenizer + recursive-descent parser + code generator. It emits Python source that builds a `pyspark.sql.Column` using `F` (`pyspark.sql.functions`) and, for the unknown-type `+`, `StringType`. Plain PySpark only: no helper module.
What each Alteryx function means is in `11-alteryx-function-reference.md`.

Use it through `alteryx_expr.translate(expr, field_types=..., current_field=..., row_ref=..., df_var=...)`. Unknown constructs raise `TranslationError` - the owning tool becomes *blocked*.

## Syntax handled
- Field refs `[Name With Spaces]`, `[Row-1:Field]` / `[Row+2:Field]` (Multi-Row only), `[_CurrentField_]` (Multi-Field only), bare names (`Left(Dato,4)`).
- Strings in `'..'` or `".."` (literal; only `\"` and `\'` are escapes, so regex backslashes survive), numbers, `TRUE/FALSE`, `Null()`, `//` and `/* */` comments.
- `IF .. THEN .. ELSEIF .. ELSE .. ENDIF` (missing ELSE = NULL), `IIF`, `Switch`.
- Operators: `= == != <>  < <= > >=  + - * / %  && AND || OR  ! NOT  IN (..)  NOT IN (..)`.
- **Precedence** (from the Alteryx reference): `* /` > `+ -` > relational/`IN`/`NOT` > `= !=` > `&& AND || OR` (the last four share one level, left to right).
  A chain mixing AND and OR without parentheses is translated left-to-right and flagged *Requires review* - do not "fix" it to SQL precedence without the user's confirmation.

## Function coverage
| Group | Implemented |
|---|---|
| Conditional / null | `IIF, Switch, IsNull, IsEmpty, Null` |
| String | `Left, Right, Length, Substring (0-based), Trim/TrimLeft/TrimRight (whitespace or listed chars), Uppercase, Lowercase, TitleCase, Contains, StartsWith, EndsWith (case-insensitive by default), Replace, FindString (0-based, -1 if absent), PadLeft/PadRight (literal length), ReverseString, GetWord (0-based), CountWords, REGEX_Match (full match, case-insensitive by default), REGEX_Replace, REGEX_CountMatches` |
| Conversion | `ToString` (numbers: integral doubles print without `.0`; `ToString(x, n)` fixed decimals), `ToNumber` (NULL on failure), `ToInteger`, `ToDate`, `ToDateTime` |
| Math | `Abs, Sqrt, Log, Log10, Exp, Pow, Mod, Round(x, mult), Ceil(x[,mult]), Floor(x[,mult]), Min, Max (row-wise; NULL handling Review)` |
| Date/time | `DateTimeToday, DateTimeNow, DateTimeAdd (years/months/weeks/days/hours/minutes/seconds), DateTimeDiff (days/hours/minutes/seconds), DateTimeFormat/Parse (strftime tokens -> Java patterns), DateTimeYear/Month/Day/Hour/Minutes/Seconds` |
| **Not implemented -> blocked** | `GetPart/GetLeft/GetRight` (index base unverified), `FindNth`, `MD5_*`, `UuidCreate`, `DateTimeDiff` months/years, `DateTimeTrim/FirstOfMonth/LastOfMonth/WeekNum/WorkDays/ToLocal/ToUTC`, `Average/Median/AverageNonNull`, finance functions, file functions, `Rand/RandInt`, spatial, `IsNumber/IsInteger`, any function not listed |

To add a function: implement `f_<lowername>` in `_Gen`, add a hand-derived case to `tests/test_expression... CASES`, update this table. Confirm the Alteryx semantics in `formula-syntax.md` first; if it is not documented there, keep it unsupported.

## Semantics deliberately handled (each has a test)
| Alteryx | Spark pitfall | Handling |
|---|---|---|
| `==` and `=` equality | converter parser rejected `==` | both accepted |
| `/` | integer division, divide-by-zero error under ANSI (Spark 4 / DBR 17+ default) | `F.try_divide` -> double / NULL on zero |
| string `+` string, number `+` number | Spark `+` casts strings to numbers (NULL/error) | typed concat via `F.concat` when a side is a string; otherwise an inline `F.concat(..) if isinstance(df.select(a).schema[0].dataType, StringType) ... else a + b` decides from the schema when types are unknown; chains flatten into one `F.concat` |
| `ToString(2026.0)` -> `2026` | Spark gives `2026.0`, silently breaking comparisons such as `[Year]==ToString(ToNumber(...))` | `F.when(F.typeof(x).isin('double', 'float') & (x == F.floor(x)), F.floor(x).cast('string')).otherwise(x.cast('string'))` |
| `ToNumber('abc')` -> NULL | cast error under ANSI | `x.try_cast('double')` when the project says ANSI is on (Spark 4 / DBR 17+), `x.cast('double')` when it is off (NULL on failure) |
| `Substring(s, 1, 3)` 0-based | Spark 1-based | `+1` |
| `Round(7.3, 0.5)` -> 7.5 | `F.round` rounds decimals | `F.round(F.try_divide(x, m)) * m` (half away from zero) |
| `Contains(s, t)` case-insensitive, NULL -> false | `contains` is case-sensitive and NULL-propagating | `lower()` both sides, `coalesce(..., False)` |
| `REGEX_Match` matches the whole string | `rlike` is a partial match | wrapped in `^(?:..)$` |
| Dates are strings (`DateTimeToday()` -> `'2026-10-12'`) | Spark date type | functions return formatted strings; string functions cast date inputs |

## Known unresolved semantics (always surfaced as notes/reviews)
Three-valued logic (`!=`/`NOT` on NULL), NULL in string concatenation, Regex flavour (Boost/PCRE vs Java), `Min/Max` of several values with NULLs, `PadLeft` truncation, `TitleCase` word rules,
`DateTimeAdd` output format when the input has a time part. Resolve these by comparing against an Alteryx run (`07-validation-and-reconciliation.md`), then record the finding in the report.
