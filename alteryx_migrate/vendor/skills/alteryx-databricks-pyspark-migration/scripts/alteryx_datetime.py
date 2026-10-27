"""The Alteryx DateTime tool's formats, written for Spark.

Source: the Alteryx Designer help page of the DateTime tool (help.alteryx.com/current/en/designer/tools/parse/datetime-tool.html) and of Date Time Now. The tool has two directions:

  Date/Time -> string   the incoming field is a date-time; the output is TEXT written in the chosen format (separators exactly as written)
  string -> Date/Time   the incoming field is text in the chosen format; the output is a date-time

The format uses Alteryx's own pattern tokens, which are NOT Java's: ``hh`` is the 24-hour clock (Java's ``hh`` is 12-hour), ``Month``/``Mon`` are the full/abbreviated month name, ``day``/``dy`` the
weekday name, ``ffff`` sub-seconds, and a letter that is not a token is literal text. Reading text is lenient in Alteryx: ``-`` and ``/`` are the same, whitespace is ignored, leading zeros are optional,
``Mon`` accepts a full name and ``Month`` an abbreviation, and a two-digit year is mapped into the window (current year - 66) .. (current year + 33).

``format_code`` and ``parse_code`` return PySpark Column source for those two directions. Whatever the help does not say is listed in ``NOT_STATED`` and flagged for review; a format that cannot be
replicated raises ``FormatError`` (the tool is then reported, never guessed).
"""
from __future__ import annotations

import re

NOT_STATED = [
    "the type of the output of string -> Date/Time (written here as the text 'yyyy-MM-dd HH:mm:ss', the skill's convention for Alteryx date-times)",
    "what an unparseable or empty value gives (NULL here)",
    "time zones (none are applied)",
    "a format that holds only a time of day on input (the date part is the Spark default, 1970-01-01)",
]
NAME_TOKENS = {"Month", "Mon", "day", "dy", "MMM", "MMMM", "EEEE", "EEE"}


class FormatError(Exception):
    """The format cannot be replicated in Spark with certainty."""


# longest first, so `Month` wins over `Mon`, `yyyy` over `yy`, `MMMM` over `MM`
TOKENS = ["ahh", "Month", "MMMM", "EEEE", "yyyy", "MMM", "day", "Mon", "HH", "hh", "MM", "dd", "yy", "mm", "ss", "dy", "M", "d", "H"]


def tokenize(fmt: str) -> list:
    """[(kind, text)] with kind = 'token' | 'literal'. Quoted text ('de') is literal; a run of f is the sub-second token; any other letter is literal text."""
    out, i = [], 0
    while i < len(fmt):
        ch = fmt[i]
        if ch == "'":
            j = fmt.find("'", i + 1)
            if j < 0:
                raise FormatError("an opening quote in the format is never closed")
            out.append(("literal", fmt[i + 1:j]))
            i = j + 1
            continue
        if ch == "*":
            raise FormatError("the wildcard * of a custom format has no documented behaviour on input or output")
        if ch == "f":
            j = i
            while j < len(fmt) and fmt[j] == "f":
                j += 1
            out.append(("token", fmt[i:j]))
            i = j
            continue
        for tok in TOKENS:
            if fmt.startswith(tok, i):
                out.append(("token", tok))
                i += len(tok)
                break
        else:
            out.append(("literal", ch))
            i += 1
    return out


def _quote(text: str) -> str:
    """Java pattern text for literal characters: letters must be quoted, quotes doubled."""
    if not text:
        return ""
    if text == "'":
        return "''"
    if any(c.isalpha() for c in text):
        return "'" + text.replace("'", "''") + "'"
    return text


def has_percent_style(fmt: str) -> bool:
    return "%" in fmt


def percent_to_java(fmt: str) -> str:
    from alteryx_expr import TranslationError, strftime_to_java
    try:
        return strftime_to_java(fmt)
    except TranslationError as exc:
        raise FormatError(str(exc)) from exc


def to_java(fmt: str, language: str = "English", forbid_names_in_other_languages: bool = True) -> str:
    """The Java/Spark pattern that WRITES a date-time as this Alteryx format."""
    if has_percent_style(fmt):
        if any(t in fmt for t in ("yyyy", "MM", "dd", "Month", "HH", "hh")):
            raise FormatError("percent-style (%d %m %Y) and pattern-style (dd MM yyyy) tokens cannot be mixed in one format")
        return percent_to_java(fmt)
    parts = tokenize(fmt)
    if forbid_names_in_other_languages and (language or "English").lower() not in ("english", "") and any(k == "token" and t in NAME_TOKENS for k, t in parts):
        raise FormatError(f"month and day names in {language} are not available from Spark's date_format (it writes English names)")
    out = []
    for kind, text in parts:
        if kind == "literal":
            out.append(_quote(text))
        elif text == "ahh":
            raise FormatError("the token ahh (AM/PM, Simplified Chinese) is not replicated")
        elif text in ("Month", "MMMM"):
            out.append("MMMM")
        elif text in ("Mon", "MMM"):
            out.append("MMM")
        elif text in ("day", "EEEE"):
            out.append("EEEE")
        elif text == "dy":
            out.append("EEE")
        elif text in ("HH", "hh"):
            out.append("HH")                                    # the Alteryx help: HH and hh are both the 24-hour clock
        elif text.startswith("f"):
            if len(text) > 6:
                raise FormatError("Spark writes at most 6 sub-second digits (ffffff)")
            out.append("S" * len(text))
        else:
            out.append(text)
    return "".join(out)


def _neighbour(parts: list, index: int, step: int):
    """The part next to `index` that is not just whitespace (whitespace is ignored when text is read, so it separates nothing)."""
    j = index + step
    while 0 <= j < len(parts) and parts[j][0] == "literal" and not parts[j][1].strip():
        j += step
    return parts[j] if 0 <= j < len(parts) else None


def parse_patterns(fmt: str, language: str = "English") -> tuple:
    """-> (list of Java patterns to try in order, whether a two-digit year needs the Alteryx window, notes).

    The patterns read the NORMALISED text (whitespace removed, '/' written as '-') leniently: single d/M when a separator follows, both month-name widths for Mon/Month."""
    if has_percent_style(fmt):
        if any(t in fmt for t in ("yyyy", "MM", "dd", "Month", "HH", "hh")):
            raise FormatError("percent-style (%d %m %Y) and pattern-style (dd MM yyyy) tokens cannot be mixed in one format")
        return [percent_to_java(fmt)], False, ["a percent-style format is read exactly as written (the leniency of the pattern style - optional zeros, '-' = '/' - is not applied)"]
    parts = tokenize(fmt)
    notes: list = []
    if (language or "English").lower() not in ("english", "") and any(k == "token" and t in NAME_TOKENS for k, t in parts):
        raise FormatError(f"month and day names in {language} cannot be read by Spark's parser (English names only)")
    if any(k == "token" and t in ("day", "dy", "EEEE", "EEE") for k, t in parts):
        raise FormatError("Alteryx does not check a weekday name against the date when reading text, but Spark's parser does: decide how weekday names are to be read")
    if any(k == "token" and t == "ahh" for k, t in parts):
        raise FormatError("the token ahh (AM/PM, Simplified Chinese) is not replicated")
    two_digit_year = any(k == "token" and t == "yy" for k, t in parts)
    variants = [[]]
    for index, (kind, text) in enumerate(parts):
        nxt, prev = _neighbour(parts, index, 1), _neighbour(parts, index, -1)
        names = ("Month", "Mon", "MMM", "MMMM")                                   # letters bound a number as well as a separator does
        ends = nxt is None or (nxt[0] == "literal" and not nxt[1].strip().isdigit() and nxt[1].strip() != "") or (nxt[0] == "token" and nxt[1] in names)
        starts = prev is None or (prev[0] == "literal" and not prev[1].strip().isdigit() and prev[1].strip() != "") or (prev[0] == "token" and prev[1] in names)
        separated = ends and starts                 # a one-digit day/month is only read where separators bound it; next to another number the width stays fixed
        if kind == "literal":
            cleaned = text.replace("/", "-").replace(" ", "").replace("\t", "")
            for v in variants:
                v.append(_quote(cleaned))
            continue
        if text in ("Month", "MMMM", "Mon", "MMM"):
            wide, abbr = ("MMMM", "MMM") if text in ("Month", "MMMM") else ("MMM", "MMMM")      # Mon accepts a full name too, Month an abbreviation
            variants = [v + [wide] for v in variants] + [v + [abbr] for v in variants]
            continue
        if text in ("HH", "hh"):
            piece = "HH"
        elif text.startswith("f"):
            if len(text) > 6:
                raise FormatError("Spark reads at most 6 sub-second digits (ffffff)")
            piece = "S" * len(text)
        elif text == "dd" and separated:
            piece = "d"
        elif text == "MM" and separated:
            piece = "M"
        elif text == "yyyy":
            piece = "yyyy"
        else:
            piece = text
        for v in variants:
            v.append(piece)
    patterns = ["".join(v) for v in variants]
    if two_digit_year:
        notes.append("a two-digit year is mapped into (current year - 66) .. (current year + 33), as the help describes")
    if any(t in ("yyyy",) for k, t in parts if k == "token"):
        notes.append("a four-digit year is read as four digits; the help also accepts two digits there (not replicated)")
    return patterns, two_digit_year, notes


def format_code(col: str, fmt: str, language: str = "English") -> str:
    """Date/Time -> string: PySpark Column source. `col` is a Column expression source."""
    pattern = to_java(fmt, language)
    return f"F.date_format(F.try_to_timestamp({col}.cast('string')), {pattern!r})"


def parse_code(col: str, fmt: str, language: str = "English") -> str:
    """string -> Date/Time: PySpark Column source giving the text 'yyyy-MM-dd HH:mm:ss' (NULL when the text does not fit the format)."""
    patterns, two_digit_year, _ = parse_patterns(fmt, language)
    text = f"F.regexp_replace(F.regexp_replace({col}.cast('string'), r'\\s+', ''), '/', '-')"
    tries = [f"F.try_to_timestamp({text}, F.lit({p!r}))" for p in patterns]
    ts = tries[0] if len(tries) == 1 else "F.coalesce(" + ", ".join(tries) + ")"
    if two_digit_year:
        yy = f"(F.year({ts}) - 2000)"
        low = "(F.year(F.current_date()) - 66)"
        year = f"({yy} + 100 * F.ceil(F.try_divide({low} - {yy}, F.lit(100))))"
        ts = f"F.add_months({ts}, 12 * ({year} - F.year({ts})))"
    return f"F.date_format({ts}, 'yyyy-MM-dd HH:mm:ss')"
