# Alteryx Formula Function Reference

Source of truth for what an Alteryx function or operator means. `04-expression-translation.md` says how the translator maps them to PySpark; a function listed here but not handled by `scripts/alteryx_expr.py` makes its tool *blocked* (never guessed). Spatial and Finance functions are not translated.

## Conditional

- `IF c THEN t ELSE f ENDIF`: Returns t if the condition c is true, else returns f.
- `IF c THEN t ELSEIF c2 THEN t2 ELSE f ENDIF`: Returns t if c is true, else t2 if c2 is true, else f.
- `IIF(bool, x, y)`: Returns x if bool is true, else returns y.
- `Switch(Value,Default,Case1,Result1,...,CaseN,ResultN)`: Compares a value against a list of cases and returns the corresponding result.

## String

- `Contains(String, Target, CaseInsensitive=1)`: Checks if String contains Target.
- `CountWords(string)`: Counts words separated by space.
- `DecomposeUnicodeForMatch(String)`: Removes accents and converts to lowercase narrow form.
- `EndsWith(String, Target, CaseInsensitive=1)`: Checks if String ends with Target.
- `FindNth(Initial String, Target, Instance)`: Finds nth occurrence of Target.
- `FindString(String,Target)`: Returns position of Target in String.
- `GetLeft(String, Delimiter)`: Returns left side before Delimiter.
- `GetPart(String, Delimiter, Index)`: Returns substring at Index.
- `GetRight(String, Delimiter)`: Returns right side after Delimiter.
- `GetWord(string, n)`: Returns nth word (0-based).
- `Left(String, len)`: Returns first len characters.
- `Length(String)`: Returns length of string.
- `LowerCase(String)`: Converts to lowercase.
- `MD5_ASCII(String)`: MD5 hash of ASCII string.
- `MD5_UNICODE(String)`: MD5 hash of UTF-16 string.
- `MD5_UTF8(String)`: MD5 hash of UTF-8 string.
- `PadLeft(String, len, char)`: Pads left to len.
- `PadRight(String, len, char)`: Pads right to len.
- `REGEX_CountMatches(String,pattern,icase)`: Count regex matches.
- `REGEX_Match(String,pattern,icase)`: Tests full regex match.
- `REGEX_Replace(String, pattern, replace, icase)`: Regex find/replace.
- `Replace(String, Target, Replacement)`: Replaces Target with Replacement.
- `ReplaceChar(String, y, z)`: Replaces characters y with z.
- `ReplaceFirst(String, Target, Replacement)`: Replaces first occurrence.
- `ReverseString(String)`: Reverses string.
- `Right(String, len)`: Returns last len characters.
- `StartsWith(String, Target, CaseInsensitive=1)`: Checks if String starts with Target.
- `STRCSPN(String, y)`: Returns length until first occurrence of y chars.
- `StripQuotes(String)`: Removes surrounding quotes.
- `STRSPN(String, y)`: Returns length of initial segment of chars in y.
- `Substring(String, start, length)`: Returns substring.
- `TitleCase(String)`: Converts to title case.
- `Trim(String, y)`: Trims y chars from both ends (default whitespace).
- `TrimLeft(String, y)`: Trims from start.
- `TrimRight(String, y)`: Trims from end.
- `Uppercase(String)`: Converts to uppercase.
- `UuidCreate()`: Creates a unique identifier.

## DateTime

- `DateTimeAdd(dt,i,u)`: Adds a specific interval to a date-time value.
- `DateTimeDay(dt)`: Returns the numeric value for the day of the month.
- `DateTimeDiff(dt1,dt2,u)`: Returns the difference between two dates as an integer.
- `DateTimeFirstOfMonth()`: Returns the first day of the month, at midnight.
- `DateTimeFormat(dt,f,[l],[tz])`: Converts date-time data from ISO format to another specified format.
- `DateTimeHour(dt)`: Returns the hour portion of the time.
- `DateTimeLastOfMonth()`: Returns the last day of the current month (23:59:59).
- `DateTimeMinutes(dt)`: Returns the minutes portion of the time.
- `DateTimeMonth(dt)`: Returns the numeric value for the month.
- `DateTimeNow([tz])`: Returns the current date and time, including seconds.
- `DateTimeNowPrecise(digits,[tz])`: Returns the current date and time with fractional seconds.
- `DateTimeParse(string,f,[l],[tzName])`: Converts a date string to standard format.
- `DateTimeQuarter(dt, [Q1 Start])`: Returns the quarter of the year.
- `DateTimeSeconds(dt)`: Returns the seconds portion of the time.
- `DateTimeStart()`: Returns the date/time when the workflow started.
- `DateTimeToday()`: Returns today's date.
- `DateTimeToLocal(dt,[tz])`: Converts UTC date-time to local time zone.
- `DateTimeToUTC(dt,[tz])`: Converts date-time to UTC.
- `DateTimeTrim(dt,t)`: Removes unwanted portions of a date-time.
  - Every part is a **truncation**, per the help: `firstofmonth` = `month` = midnight of the 1st of the month; `year` = midnight of 1 January; `day` = midnight; `hour` / `minute` / `second` = the start of it; `lastofmonth` = one second before the end of the month.
  - The result is the date-time text `yyyy-MM-dd HH:mm:ss` (the help's examples: `2016-01-01 00:00:00`); a plural part name (`months`, used in the workflows) is the same part.
  - Spark: `date_format(trunc(ts,'month'|'year'), 'yyyy-MM-dd HH:mm:ss')`, `date_trunc('day'|'hour'|'minute'|'second', ts)`, `last_day(ts)` + ` 23:59:59`. An output field of type Date keeps the date part (`cast('date')`).
  - Earlier versions returned the part as text (`'MM'` -> `03`), which a Date field turned into NULL (BUG-02).
- `DateTimeWeekNum(dt, [StartOfWeek])`: Returns the week number of the year.
- `DateTimeWorkDays(dt1, dt2, [StartofWeek])`: Returns number of working days between two dates.
- `DateTimeYear(dt)`: Returns the numeric value for the year.
- `ToDate(x)`: Converts a string, number, or date-time to a date.
- `ToDateTime(x)`: Converts a string, number, or date to a date-time.

## Operators

- `/* Comment */`: Block comment.
- `// Comment`: Single-line comment.
- `&&`, `AND`: Boolean AND.
- `||`, `OR`: Boolean OR.
- `!`, `NOT`: Boolean NOT.
- `=`, `==`: Equal to.
- `>`, `>=`, `<`, `<=`: Comparisons.
- `!=`: Not equal to.
- `+`: Adds numbers, concatenates strings, or unions spatial objects.
- `-`: Subtracts numbers or removes one spatial object from another.
- `*`: Multiplies numbers.
- `/`: Divides numbers; always returns a double.
- `value IN (...)`: Returns True if value is in list.
- `value NOT IN (...)`: Returns True if value not in list.

### Order of Precedence

Operations within a group bind left to right.

| Order | Operators |
|--------|------------|
| 1 | `*`, `/` |
| 2 | `+`, `-` |
| 3 | `<=`, `<`, `>=`, `>`, `IN`, `NOT` |
| 4 | `=`, `!=` |
| 5 | `&&`, `AND`, `||`, `OR` |

## Conversion

- `BinToInt(s)`: Converts the binary string s to an integer (limited to 53 bits).
- `CharFromInt(x)`: Returns the Unicode character that matches the input number x.
- `CharToInt(s)`: Returns the number that matches the input Unicode character s.
- `ConvertFromCodePage(s, codePage)`: Translates text from a code page to Unicode.
- `ConvertToCodePage(s, codePage)`: Translates text from Unicode encoding to a specific code page.
- `HexToNumber(x)`: Converts a HEX string to a number (limited to 53 bits).
- `IntToBin(x)`: Converts x to a binary string.
- `IntToHex(x)`: Converts x to a hexadecimal string.
- `ToDegrees(x)`: Converts a numeric radian value (x) to degrees.
- `ToNumber(x, [bIgnoreErrors], [keepNulls], [decimalSeparator])`: Converts a string (x) to a number.
- `ToRadians(x)`: Converts a numeric degree value (x) to radians.
- `ToString(x, numDec, [addThousandsSeparator], [decimalSeparator])`: Converts a numeric parameter (x) to a string using numDec decimal places.
- `UnicodeNormalize(String, Form)`: Converts text data into a standardized Unicode form.

## Math

- `ABS(x)`: Absolute value.
- `ACOS(x)`, `ASIN(x)`, `ATAN(x)`, `ATAN2(y, x)`: Inverse trigonometric functions.
- `Average(n1, ...)`: Average of a list of numbers.
- `AverageNonNull(n1, ...)`: Average excluding nulls.
- `CEIL(x, [mult])`: Rounds up to nearest multiple.
- `COS(x)`, `COSH(x)`, `SIN(x)`, `SINH(x)`, `TAN(x)`, `TANH(x)`: Trigonometric and hyperbolic functions.
- `DISTANCE(from_Lat,from_Lon,to_Lat,to_Lon)`: Distance between coordinates.
- `EXP(x)`: e^x.
- `FACTORIAL(x)`: Factorial of x.
- `FLOOR(x, [mult])`: Rounds down to nearest multiple.
- `LOG(x)`: Natural logarithm.
- `LOG10(x)`: Base-10 logarithm.
- `Median(...)`: Median of values.
- `Mod(n,d)`: Modulo operation.
- `PI()`: Constant pi.
- `POW(x,e)`: x raised to e.
- `RAND()`: Random number [0,1).
- `RandInt(n)`: Random integer [0,n].
- `Round(x,mult)`: Rounds x to nearest multiple.
- `SmartRound(x)`: Dynamic rounding based on size.
- `SQRT(x)`: Square root.

## File

- `FileAddPaths(Path1, Path2)`: Adds two file path parts, ensuring one backslash between them.
- `FileExists(Path)`: Returns True if the file exists, else False.
- `FileGetDir(Path)`: Returns the directory portion of the path.
- `FileGetExt(Path)`: Returns the file extension.
- `FileGetFileName(Path)`: Returns the file name without extension.

## Finance

- `FinanceCAGR(BeginningValue, EndingValue, NumYears)`: Compound Annual Growth Rate.
- `FinanceEffectiveRate(NominalRate, PaymentsPerYear)`: Effective Annual Interest Rate.
- `FinanceFV(Rate, NumPayments, PaymentAmount, PresentValue, PayAtPeriodBegin)`: Future Value.
- `FinanceFVSchedule(Principle, Year1Rate, Year2Rate)`: Future Value Schedule.
- `FinanceIRR(Value1, Value2)`: Internal Rate of Return.
- `FinanceMIRR(FinanceRate, ReinvestRate, Value1, Value2)`: Modified Internal Rate of Return.
- `FinanceMXIRR(FinanceRate, ReinvestRate, Value1, Date1, Value2, Date2)`: Modified IRR with dates.
- `FinanceNominalRate(EffectiveRate, PaymentsPerYear)`: Nominal Annual Interest Rate.
- `FinanceNPER(Rate, PaymentAmount, PresentValue, FutureValue, PayAtPeriodBegin)`: Number of periods.
- `FinanceNPV(Rate, Value1, Value2)`: Net Present Value.
- `FinancePMT(Rate, NumPayments, PresentValue, FutureValue, PayAtPeriodBegin)`: Loan payment amount.
- `FinancePV(Rate, NumPayments, PaymentAmount, FutureValue, PayAtPeriodBegin)`: Present Value.
- `FinanceRate(NumPayments, PaymentAmount, PresentValue, FutureValue, PayAtPeriodBegin)`: Interest rate per period.
- `FinanceXIRR(Value1, Date1, Value2, Date2)`: IRR with dates.
- `FinanceXNPV(Rate, Value1, Date1, Value2, Date2)`: NPV with dates.

## Bitwise

- `BinaryAnd(n,m)`, `BinaryOr(n,m)`, `BinaryXOr(n,m)`, `BinaryNot(n)`: Bitwise AND / OR / XOR / NOT.
- `ShiftLeft(n,b)`, `ShiftRight(n,b)`: Shift by b bits.

## Min/Max

- `BETWEEN(x, min, max)`: Tests if x is between min and max.
- `Bound(x, min, max)`: Clamps x to [min,max].
- `Max(v0, v1, ..., vn)`: Maximum value.
- `MaxIDX(v0, v1,..., vn)`: Index of maximum value.
- `Min(v0, v1,..., vn)`: Minimum value.
- `MinIDX(v0, v1,..., vn)`: Index of minimum value.

## Spatial

- `ST_Area`, `ST_Boundary`, `ST_BoundingRectangle`, `ST_Centroid`, `ST_CentroidX`, `ST_CentroidY`, `ST_Combine`, `ST_Contains`, `ST_ConvexHull`, `ST_CreateLine`, `ST_CreatePoint`, `ST_CreatePolygon`, `ST_Cut`, `ST_Dimension`, `ST_Distance`, `ST_EndPoint`, `ST_Intersection`, `ST_Intersects`, `ST_InverseIntersection`, `ST_Length`, `ST_MD5`, `ST_MaxX`, `ST_MaxY`, `ST_MinX`, `ST_MinY`, `ST_NumParts`, `ST_NumPoints`, `ST_ObjectType`, `ST_PointN`, `ST_RandomPoint`, `ST_Relate`, `ST_StartPoint`, `ST_Touches`, `ST_TouchesOrIntersects`, `ST_Within`: build spatial objects, analyze spatial data and return metrics from spatial fields (area, length, distance, containment, intersection, centroid, bounds).

## Specialized

- `Coalesce(v1,v2,v3,...,vn)`: Returns first non-null value.
- `EscapeXMLMetacharacters(String)`: Escapes XML metacharacters.
- `GetVal(index, v0,...vn)`: Returns value by 0-based index.
- `GetEnvironmentVariable(Name)`: Returns environment variable value.
- `Message(messageType, message, returnValue)`: Outputs message and value when condition met.
- `NULL()`: Returns Null.
- `RangeMedian(...)`: Median from aggregated ranges.
- `ReadRegistryString(Key, ValueName, DefaultValue="")`: Reads registry value.
- `Soundex(String)`: Returns Soundex code of string.
- `Soundex_Digits(String)`: Returns first 4 digits or Soundex code.
- `TOPNIDX(N, v0, v1, ..., vn)`: Index of Nth from max value.
- `UrlEncode(String)`: Legacy UTF-16 percent-encoding (use UrlEncodeUTF8 instead).
- `UrlEncodeUTF8(String)`: RFC 3986-compliant percent-encoding.

## Test

- `CompareDictionary(a,b)`: Case-insensitive compare with numeric sort.
- `CompareDigits(a,b,nNumDigits)`: Compares numbers to given precision.
- `CompareEpsilon(a,b,epsilon)`: Compares floats within epsilon.
- `EqualStrings(a,b)`: Tests if strings are identical.
- `IsEmpty(v)`: True if v is NULL or empty string.
- `IsInteger(v)`: True if v can be converted to integer.
- `IsLowerCase(String)`: True if all alphabetic chars lowercase.
- `IsNull(v)`: True if v is NULL.
- `IsNumber(v)`: True if v is numeric type.
- `IsSpatialObj(v)`: True if v is spatial object.
- `IsString(v)`: True if v is string type.
- `IsUpperCase(String)`: True if all alphabetic chars uppercase.

## Null Handling

| Data1 | Data2 | `>` | `<` | `==` | `!=` |
|:------|:------|:---:|:---:|:----:|:----:|
| 1 | Null | False | False | False | True |
| 0 | Null | False | False | False | True |
| Null | Null | False | False | True | False |

- `1 + Null() == Null()`: Adding a number to Null returns Null.
- `"ABC" + Null() == "ABC"`: Adding a string to Null returns the string.
- `Null() < Null()` and `Null() > Null()`: Always return False.
- `Null() <= Null()` and `Null() >= Null()`: Return True only when both sides are Null.
- Comparisons (`<`, `>`, `=`, `!=`) with Null generally return False, except `Null() == Null()` is True.
