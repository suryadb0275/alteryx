"""Alteryx formula expression -> PySpark Column-expression source code.

Why this exists: the parsed workflow JSON carries a converter-produced ``spark_sql`` hint, but
that hint is missing or wrong for whole classes of expressions (e.g. every ``==`` comparison
fails in the sample input).  This module re-parses the original ``alteryx_expression`` text
itself and emits *Python source* that builds a ``pyspark.sql.Column`` using plain PySpark only (``F`` = pyspark.sql.functions
and ``StringType`` from pyspark.sql.types are assumed to be imported in the generated notebook; no helper module).

Design rules
  * Never guess.  Unknown functions / unsupported syntax raise ``TranslationError`` so the caller
    can mark the tool "Requires review" and keep the original text in the generated code.
  * ANSI-mode safe (Spark 4 / recent Databricks runtimes enable ANSI by default): division uses
    ``F.try_divide`` and, when the project states ANSI is on, conversions use ``Column.try_cast`` so a bad value
    gives NULL instead of an error, as in Alteryx.
  * Operator precedence follows the Alteryx reference (``&&``/``AND``/``||``/``OR`` share one
    left-to-right level).  A mixed AND/OR chain without parentheses is flagged for review.

Public API: ``translate(expr, ...) -> Result`` and ``TranslationError``.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Optional

STRING, NUMBER, BOOL, DATE, DATETIME, UNKNOWN = "string", "number", "bool", "date", "datetime", "unknown"


class TranslationError(Exception):
    """Expression cannot be translated with confidence."""


@dataclass
class Result:
    code: str
    type: str = UNKNOWN
    notes: list = field(default_factory=list)      # informational semantic caveats
    reviews: list = field(default_factory=list)    # items that force "Requires review"
    fields: list = field(default_factory=list)     # input field names referenced


# ----------------------------------------------------------------------------- lexer
_TOKEN_RE = re.compile(
    r"""
    (?P<WS>\s+)
  | (?P<LCOMMENT>//[^\n]*)
  | (?P<BCOMMENT>/\*.*?\*/)
  | (?P<FIELD>\[[^\]]*\])
  | (?P<STR>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')
  | (?P<NUM>\d+\.\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?|\d+(?:[eE][+-]?\d+)?)
  | (?P<ID>[A-Za-z_][A-Za-z_0-9]*)
  | (?P<OP>&&|\|\||==|!=|<>|<=|>=|[=<>+\-*/%!])
  | (?P<LP>\()
  | (?P<RP>\))
  | (?P<COMMA>,)
    """,
    re.X | re.S,
)


def _lex(text: str):
    pos, out = 0, []
    while pos < len(text):
        m = _TOKEN_RE.match(text, pos)
        if not m:
            raise TranslationError(f"Unrecognised character {text[pos]!r} at position {pos}")
        pos = m.end()
        kind = m.lastgroup
        if kind in ("WS", "LCOMMENT", "BCOMMENT"):
            continue
        out.append((kind, m.group(), m.start()))
    out.append(("EOF", "", len(text)))
    return out


def _unescape(raw: str) -> str:
    body = raw[1:-1]
    # Alteryx strings are literal (file paths, regex): only \" and \' are escapes
    return re.sub(r"\\([\"'])", r"\1", body)


# ----------------------------------------------------------------------------- parser -> AST
class _Parser:
    def __init__(self, text):
        self.t = _lex(text)
        self.i = 0
        self.mixed_bool_chain = False

    # token helpers
    def peek(self, k=0):
        return self.t[min(self.i + k, len(self.t) - 1)]

    def next(self):
        tok = self.t[self.i]
        self.i += 1
        return tok

    def kw(self, word, k=0):
        tok = self.peek(k)
        return tok[0] == "ID" and tok[1].upper() == word

    def expect_kw(self, word):
        if not self.kw(word):
            raise TranslationError(f"Expected {word} at position {self.peek()[2]}, got {self.peek()[1]!r}")
        self.next()

    def parse(self):
        node = self.expr()
        if self.peek()[0] != "EOF":
            raise TranslationError(f"Unexpected token {self.peek()[1]!r} at position {self.peek()[2]}")
        return node

    # grammar (lowest -> highest): bool-chain, equality, relational, additive, multiplicative, unary
    def expr(self):
        left = self.equality()
        ops = set()
        while True:
            tok = self.peek()
            if tok[0] == "OP" and tok[1] in ("&&", "||") or self.kw("AND") or self.kw("OR"):
                op = "and" if (tok[1] == "&&" or tok[1].upper() == "AND") else "or"
                self.next()
                right = self.equality()
                ops.add(op)
                left = ("bool", op, left, right)
            else:
                break
        if len(ops) > 1:
            self.mixed_bool_chain = True
        return left

    def equality(self):
        left = self.relational()
        while self.peek()[0] == "OP" and self.peek()[1] in ("=", "==", "!=", "<>"):
            op = self.next()[1]
            right = self.relational()
            left = ("cmp", "==" if op in ("=", "==") else "!=", left, right)
        return left

    def relational(self):
        if (self.peek()[0] == "OP" and self.peek()[1] == "!") or (self.kw("NOT") and not self.kw("IN", 1)):
            self.next()
            return ("not", self.relational())
        left = self.additive()
        while True:
            tok = self.peek()
            if tok[0] == "OP" and tok[1] in ("<", "<=", ">", ">="):
                self.next()
                left = ("cmp", tok[1], left, self.additive())
            elif self.kw("IN") or (self.kw("NOT") and self.kw("IN", 1)):
                neg = self.kw("NOT")
                if neg:
                    self.next()
                self.next()
                if self.next()[0] != "LP":
                    raise TranslationError("IN must be followed by '('")
                items = [self.expr()]
                while self.peek()[0] == "COMMA":
                    self.next()
                    items.append(self.expr())
                if self.next()[0] != "RP":
                    raise TranslationError("Unclosed IN list")
                left = ("in", left, items, neg)
            else:
                return left

    def additive(self):
        left = self.multiplicative()
        while self.peek()[0] == "OP" and self.peek()[1] in ("+", "-"):
            op = self.next()[1]
            left = ("arith", op, left, self.multiplicative())
        return left

    def multiplicative(self):
        left = self.unary()
        while self.peek()[0] == "OP" and self.peek()[1] in ("*", "/", "%"):
            op = self.next()[1]
            left = ("arith", op, left, self.unary())
        return left

    def unary(self):
        tok = self.peek()
        if tok[0] == "OP" and tok[1] in ("-", "+"):
            self.next()
            return ("neg" if tok[1] == "-" else "pos", self.unary())
        return self.primary()

    def primary(self):
        kind, text, pos = self.next()
        if kind == "NUM":
            return ("num", float(text) if re.search(r"[.eE]", text) else int(text))
        if kind == "STR":
            return ("str", _unescape(text))
        if kind == "FIELD":
            name = text[1:-1]
            m = re.match(r"^Row\s*([+-]\s*\d+)\s*:(.*)$", name, re.S)
            if m:
                return ("field", m.group(2), int(m.group(1).replace(" ", "")))
            return ("field", name, 0)
        if kind == "LP":
            node = self.expr()
            if self.next()[0] != "RP":
                raise TranslationError("Unclosed parenthesis")
            return ("paren", node)
        if kind == "ID":
            up = text.upper()
            if up == "IF":
                return self.if_expr()
            if up in ("TRUE", "FALSE") and self.peek()[0] != "LP":
                return ("bool_lit", up == "TRUE")
            if self.peek()[0] == "LP":
                self.next()
                args = []
                if self.peek()[0] != "RP":
                    args.append(self.expr())
                    while self.peek()[0] == "COMMA":
                        self.next()
                        args.append(self.expr())
                if self.next()[0] != "RP":
                    raise TranslationError(f"Unclosed call to {text}")
                return ("call", text, args)
            if up in ("THEN", "ELSE", "ELSEIF", "ENDIF", "AND", "OR", "NOT", "IN"):
                raise TranslationError(f"Unexpected keyword {text!r} at position {pos}")
            return ("field", text, 0)      # Alteryx allows bare field names (no spaces/special characters)
        raise TranslationError(f"Unexpected token {text!r} at position {pos}")

    def if_expr(self):
        branches = []
        cond = self.expr()
        self.expect_kw("THEN")
        branches.append((cond, self.expr()))
        otherwise = None
        while True:
            if self.kw("ELSEIF"):
                self.next()
                cond = self.expr()
                self.expect_kw("THEN")
                branches.append((cond, self.expr()))
            elif self.kw("ELSE"):
                self.next()
                otherwise = self.expr()
            elif self.kw("ENDIF"):
                self.next()
                break
            else:
                raise TranslationError(f"Expected ELSEIF/ELSE/ENDIF, got {self.peek()[1]!r}")
        return ("if", branches, otherwise)


# ----------------------------------------------------------------------------- code generation
def col_code(name: str) -> str:
    """Python source for a column reference; backticks protect dots in names."""
    n = name.replace("`", "``")
    if "." in n:
        n = f"`{n}`"
    return f"F.col({n!r})"


_STRFTIME = {"%Y": "yyyy", "%y": "yy", "%m": "MM", "%d": "dd", "%H": "HH", "%I": "hh", "%M": "mm",
             "%S": "ss", "%B": "MMMM", "%b": "MMM", "%A": "EEEE", "%a": "EEE", "%p": "a", "%j": "DDD",
             "%f": "SSS"}


def strftime_to_java(fmt: str) -> str:
    out, i = "", 0
    while i < len(fmt):
        if fmt[i] == "%" and i + 1 < len(fmt):
            tok = fmt[i:i + 2]
            if tok == "%%":
                out += "%"
            elif tok in _STRFTIME:
                out += _STRFTIME[tok]
            else:
                raise TranslationError(f"Unsupported date format token {tok!r}")
            i += 2
        else:
            ch = fmt[i]
            out += f"'{ch}'" if ch.isalpha() else ch
            i += 1
    return out


_ALTERYX_TYPES = {
    "v_wstring": "string", "v_string": "string", "string": "string", "wstring": "string", "time": "string",
    "bool": "boolean", "byte": "smallint", "int16": "smallint", "int32": "int", "int64": "bigint",
    "float": "float", "double": "double", "date": "date", "datetime": "timestamp",
}


def spark_type(alteryx_type: str, size=None) -> str:
    """Spark SQL type name of an Alteryx field type (FixedDecimal keeps its 'precision.scale' size)."""
    t = (alteryx_type or "").strip().lower()
    if t == "fixeddecimal":
        if size and re.match(r"^\d+(\.\d+)?$", str(size)):
            p, _, s = str(size).partition(".")
            return f"decimal({int(p)},{int(s or 0)})"
        return "decimal(38,10)"
    if t in _ALTERYX_TYPES:
        return _ALTERYX_TYPES[t]
    raise TranslationError(f"Unmapped Alteryx type {alteryx_type!r}")


def cast_code(code: str, dtype: str, ansi: bool = False) -> str:
    """A conversion that gives NULL instead of failing: Column.try_cast when ANSI is on (Spark 4 / DBR 17+), cast when it is off."""
    return f"{code}.{'try_cast' if ansi else 'cast'}({dtype!r})"


class _Gen:
    def __init__(self, field_types, current_field, row_ref, df_var="df", ci_default=True, ansi=False):
        self.ci_default_value = ci_default
        self.ansi = bool(ansi)
        self.df_var = df_var
        self._concat = {}      # code -> flattened operand list, so a+b+c becomes one F.concat
        self._bool = {}        # code -> (operator, operands) for flattening boolean chains
        self._lits = {}        # F.lit(..) code -> bare Python literal, used where Spark accepts plain values
        self.ft = {k.lower(): v for k, v in (field_types or {}).items()}
        self.current = current_field
        self.row_ref = row_ref
        self.notes, self.reviews, self.fields = [], [], []

    def note(self, msg):
        if msg not in self.notes:
            self.notes.append(msg)

    def review(self, msg):
        if msg not in self.reviews:
            self.reviews.append(msg)

    # --- node dispatch -> (code, type)
    def gen(self, n):
        return getattr(self, "g_" + n[0])(n)

    def _lit_code(self, value, typ):
        code = f"F.lit({value!r})"
        self._lits[code] = repr(value)
        return code, typ

    def B(self, code):
        """Bare Python literal for an operand where a plain value is accepted (comparisons, arithmetic, when/otherwise)."""
        return self._lits.get(code, code)

    def g_num(self, n):
        return self._lit_code(n[1], NUMBER)

    def g_str(self, n):
        return self._lit_code(n[1], STRING)

    def g_bool_lit(self, n):
        return self._lit_code(n[1], BOOL)

    def g_paren(self, n):
        return self.gen(n[1])

    def g_field(self, n):
        name, off = n[1], n[2]
        if name == "_CurrentField_":
            if self.current is None:
                raise TranslationError("[_CurrentField_] used outside a Multi-Field Formula")
            name = self.current
        if off:
            if self.row_ref is None:
                raise TranslationError(f"Row reference [Row{off:+d}:{name}] outside a Multi-Row Formula")
            self.fields.append(name)
            return self.row_ref(name, off), self.ft.get(name.lower(), UNKNOWN)
        self.fields.append(name)
        return col_code(name), self.ft.get(name.lower(), UNKNOWN)

    def g_neg(self, n):
        c, _ = self.gen(n[1])
        return f"(-{c})", NUMBER

    def g_pos(self, n):
        return self.gen(n[1])

    def g_not(self, n):
        c, _ = self.gen(n[1])
        self.note("NOT/!= on nullable values: Spark three-valued logic yields NULL where Alteryx yields a "
                  "Boolean; verify null handling (Filter output coalesces NULL to False).")
        return f"(~{c})", BOOL

    def g_bool(self, n):
        l, _ = self.gen(n[2])
        r, _ = self.gen(n[3])
        op = "&" if n[1] == "and" else "|"
        prev_op, prev_parts = self._bool.get(l, (None, None))
        parts = prev_parts if prev_op == op else [l]       # a | b | c stays one flat chain (same-operator, left to right)
        code = "(" + f" {op} ".join(parts + [r]) + ")"
        self._bool[code] = (op, parts + [r])
        return code, BOOL

    @staticmethod
    def _is_null_literal(node):
        return isinstance(node, tuple) and node[0] == "call" and str(node[1]).lower() == "null" and not node[2]

    def g_cmp(self, n):
        op = n[1]
        lnull, rnull = self._is_null_literal(n[2]), self._is_null_literal(n[3])
        if lnull or rnull:      # Alteryx: Null() = Null() is True and any other comparison with Null is False, never NULL (see 11-alteryx-function-reference.md)
            if lnull and rnull:
                return ("F.lit(True)" if op == "==" else "F.lit(False)"), BOOL
            other, ot = self.gen(n[3] if lnull else n[2])
            return f"{self.B(other)}.{'isNull' if op == '==' else 'isNotNull'}()", BOOL
        l, lt = self.gen(n[2])
        r, rt = self.gen(n[3])
        if op == "!=":
            self.note("NOT/!= on nullable values: Spark three-valued logic yields NULL where Alteryx yields a "
                      "Boolean; verify null handling (Filter output coalesces NULL to False).")
        if STRING in (lt, rt) and NUMBER in (lt, rt):
            self.review("String/number comparison relies on implicit casting; Alteryx and Spark may differ")
        return f"({l} {op} {self.B(r)})", BOOL

    def g_in(self, n):
        x, _ = self.gen(n[1])
        items = [self.B(self.gen(i)[0]) for i in n[2]]
        code = f"{x}.isin({', '.join(items)})"
        return (f"(~{code})" if n[3] else f"({code})"), BOOL

    def g_arith(self, n):
        op = n[1]
        l, lt = self.gen(n[2])
        r, rt = self.gen(n[3])
        if op == "+":
            if STRING in (lt, rt):
                self.note("String concatenation with NULL yields NULL in Spark; confirm Alteryx behaviour.")
                lp = self._concat.get(l, [l if lt == STRING else f"{l}.cast('string')"])
                rp = [r if rt == STRING else f"{r}.cast('string')"]
                code = f"F.concat({', '.join(lp + rp)})"
                self._concat[code] = lp + rp
                return code, STRING
            if lt == NUMBER and rt == NUMBER:
                return f"({l} + {r})", NUMBER
            self.note("'+' operand types unknown at generation time: concat vs add is decided from the DataFrame "
                      "schema when the plan is built.")
            is_text = " or ".join(f"isinstance({self.df_var}.select({x}).schema[0].dataType, StringType)" for x in (l, r))
            return (f"(F.concat({l}.cast('string'), {r}.cast('string')) if {is_text} else {l} + {r})"), UNKNOWN
        if op == "/":
            return f"F.try_divide({l}, {r})", NUMBER
        return f"({l} {op} {self.B(r)})", NUMBER

    def g_if(self, n):
        branches, otherwise = n[1], n[2]
        code, types = None, set()
        parts = []
        for cond, then in branches:
            c, _ = self.gen(cond)
            t, tt = self.gen(then)
            parts.append((c, t))
            types.add(tt)
        if otherwise is not None:
            e, et = self.gen(otherwise)
            types.add(et)
        else:
            e = "F.lit(None)"
        known = {t for t in types if t != UNKNOWN}
        rtype = known.pop() if len(known) == 1 and UNKNOWN not in types else UNKNOWN
        code = f"F.when({parts[0][0]}, {self.B(parts[0][1])})"
        for c, t in parts[1:]:
            code += f".when({c}, {self.B(t)})"
        code += f".otherwise({self.B(e)})"
        return code, rtype

    # --- functions
    def g_call(self, n):
        name, args = n[1], n[2]
        fn = getattr(self, "f_" + name.lower(), None)
        if fn is None:
            raise TranslationError(f"Unsupported function {name}()")
        return fn(args)

    def _a(self, args, count, name, ret=None):
        lo, hi = (count if isinstance(count, tuple) else (count, count))
        if not lo <= len(args) <= hi:
            raise TranslationError(f"{name}() expects {count} argument(s), got {len(args)}")
        return [self.gen(a) for a in args]

    def _lit(self, node, what):
        while node[0] == "paren":
            node = node[1]
        if node[0] in ("num", "str"):
            return node[1]
        raise TranslationError(f"{what} must be a literal")

    def _s(self, c, t):
        return c if t == STRING else f"{c}.cast('string')"

    # conditional / null
    def f_iif(self, a):
        (c, _), (t, tt), (e, et) = self._a(a, 3, "IIF")
        return f"F.when({c}, {self.B(t)}).otherwise({self.B(e)})", tt if tt == et else UNKNOWN

    def f_isnull(self, a):
        (x, _), = self._a(a, 1, "IsNull")
        return f"{x}.isNull()", BOOL

    def f_isempty(self, a):
        (x, _), = self._a(a, 1, "IsEmpty")
        return f"({x}.isNull() | ({x}.cast('string') == ''))", BOOL

    def f_null(self, a):
        self._a(a, 0, "Null")
        return "F.lit(None)", UNKNOWN

    def f_switch(self, a):
        if len(a) < 4 or len(a) % 2:
            raise TranslationError("Switch() expects value, default and case/result pairs")
        gens = [self.gen(x) for x in a]
        v, default = gens[0][0], gens[1][0]
        code = None
        for i in range(2, len(gens), 2):
            piece = f"({v} == {self.B(gens[i][0])}), {self.B(gens[i + 1][0])}"
            code = f"F.when({piece})" if code is None else code + f".when({piece})"
        return code + f".otherwise({default})", UNKNOWN

    # strings
    def f_left(self, a):
        (s, st), (n, _) = self._a(a, 2, "Left")
        return f"F.left({self._s(s, st)}, {n})", STRING

    def f_right(self, a):
        (s, st), (n, _) = self._a(a, 2, "Right")
        return f"F.right({self._s(s, st)}, {n})", STRING

    def f_length(self, a):
        (s, st), = self._a(a, 1, "Length")
        return f"F.length({self._s(s, st)})", NUMBER

    def f_substring(self, a):
        g = self._a(a, (2, 3), "Substring")
        (s, st), (start, _) = g[0], g[1]
        self.note("Substring is 0-based in Alteryx; start+1 applied for Spark.")
        if len(g) == 3:
            return f"{self._s(s, st)}.substr({start} + F.lit(1), {g[2][0]})", STRING
        return f"{self._s(s, st)}.substr({start} + F.lit(1), F.lit(2147483647))", STRING

    def _trim(self, a, name, left, right):
        g = self._a(a, (1, 2), name)
        s = self._s(*g[0])
        if len(a) == 2:
            chars = re.escape(str(self._lit(a[1], f"{name} characters")))
            pats = ([f"^[{chars}]+"] if left else []) + ([f"[{chars}]+$"] if right else [])
        else:
            pats = ([r"^\s+"] if left else []) + ([r"\s+$"] if right else [])
        return f"F.regexp_replace({s}, {'|'.join(pats)!r}, '')", STRING

    def f_trim(self, a):
        return self._trim(a, "Trim", True, True)

    def f_trimleft(self, a):
        return self._trim(a, "TrimLeft", True, False)

    def f_trimright(self, a):
        return self._trim(a, "TrimRight", False, True)

    def f_uppercase(self, a):
        (s, st), = self._a(a, 1, "Uppercase")
        return f"F.upper({self._s(s, st)})", STRING

    def f_lowercase(self, a):
        (s, st), = self._a(a, 1, "Lowercase")
        return f"F.lower({self._s(s, st)})", STRING

    def f_titlecase(self, a):
        (s, st), = self._a(a, 1, "TitleCase")
        self.note("TitleCase mapped to initcap(); word-boundary rules may differ for punctuation.")
        return f"F.initcap({self._s(s, st)})", STRING

    def _ci_default(self, a, name):
        ci = self.ci_default_value
        if len(a) == 3:
            ci = bool(self._lit(a[2], f"{name} CaseInsensitive"))
        self.note(f"{name}: " + ("case-insensitive by default (Alteryx reference: CaseInsensitive=1)" if self.ci_default_value
                                 else "case-sensitive by default (project setting)"))
        return ci

    def f_contains(self, a):
        g = self._a(a, (2, 3), "Contains")
        ci = self._ci_default(a, "Contains")
        s, t = self._s(*g[0]), self._s(*g[1])
        if ci:
            s, t = f"F.lower({s})", f"F.lower({t})"
        return f"F.coalesce({s}.contains({t}), F.lit(False))", BOOL

    def f_startswith(self, a):
        g = self._a(a, (2, 3), "StartsWith")
        ci = self._ci_default(a, "StartsWith")
        s, t = self._s(*g[0]), self._s(*g[1])
        if ci:
            s, t = f"F.lower({s})", f"F.lower({t})"
        return f"F.coalesce({s}.startswith({t}), F.lit(False))", BOOL

    def f_endswith(self, a):
        g = self._a(a, (2, 3), "EndsWith")
        ci = self._ci_default(a, "EndsWith")
        s, t = self._s(*g[0]), self._s(*g[1])
        if ci:
            s, t = f"F.lower({s})", f"F.lower({t})"
        return f"F.coalesce({s}.endswith({t}), F.lit(False))", BOOL

    def f_replace(self, a):
        g = self._a(a, 3, "Replace")
        return f"F.replace({self._s(*g[0])}, {g[1][0]}, {g[2][0]})", STRING

    def f_findstring(self, a):
        g = self._a(a, 2, "FindString")
        self.note("FindString is 0-based (-1 when absent): position()-1.")
        return f"(F.position({self._s(*g[1])}, {self._s(*g[0])}) - F.lit(1))", NUMBER

    def f_padleft(self, a):
        g = self._a(a, 3, "PadLeft")
        n = self._lit(a[1], "PadLeft length")
        s = self._s(*g[0])
        self.review("PadLeft: Alteryx truncation behaviour for over-long input is unverified")
        return f"F.when(F.length({s}) >= {n}, {s}).otherwise(F.lpad({s}, {n}, {g[2][0]}))", STRING

    def f_padright(self, a):
        g = self._a(a, 3, "PadRight")
        n = self._lit(a[1], "PadRight length")
        s = self._s(*g[0])
        self.review("PadRight: Alteryx truncation behaviour for over-long input is unverified")
        return f"F.when(F.length({s}) >= {n}, {s}).otherwise(F.rpad({s}, {n}, {g[2][0]}))", STRING

    def f_reversestring(self, a):
        (s, st), = self._a(a, 1, "ReverseString")
        return f"F.reverse({self._s(s, st)})", STRING

    def f_getword(self, a):
        g = self._a(a, 2, "GetWord")
        return (f"F.get(F.split(F.regexp_replace({self._s(*g[0])}, r'^\\s+|\\s+$', ''), r'\\s+'), {g[1][0]})",
                STRING)

    def f_countwords(self, a):
        (s, st), = self._a(a, 1, "CountWords")
        return f"F.size(F.split(F.regexp_replace({self._s(s, st)}, r'^\\s+|\\s+$', ''), r'\\s+'))", NUMBER

    def _pattern(self, node, ci_node):
        pat = self._lit(node, "Regex pattern")
        ci = True if ci_node is None else bool(self._lit(ci_node, "Regex icase"))
        self.note("Regex flavour: Alteryx (Boost/PCRE) vs Spark (Java); validate non-trivial patterns.")
        return ("(?i)" if ci else "") + str(pat)

    def f_regex_match(self, a):
        g = self._a(a, (2, 3), "REGEX_Match")
        pat = self._pattern(a[1], a[2] if len(a) == 3 else None)
        return f"{self._s(*g[0])}.rlike({('^(?:' + pat + ')$')!r})", BOOL

    def f_regex_replace(self, a):
        g = self._a(a, (3, 4), "REGEX_Replace")
        pat = self._pattern(a[1], a[3] if len(a) == 4 else None)
        return f"F.regexp_replace({self._s(*g[0])}, {pat!r}, {g[2][0]})", STRING

    def f_regex_countmatches(self, a):
        g = self._a(a, (2, 3), "REGEX_CountMatches")
        pat = self._pattern(a[1], a[2] if len(a) == 3 else None)
        return f"F.size(F.regexp_extract_all({self._s(*g[0])}, F.lit({pat!r}), F.lit(0)))", NUMBER

    # conversion
    def f_tostring(self, a):
        g = self._a(a, (1, 4), "ToString")
        x, xt = g[0]
        if len(a) >= 2:
            nd = self._lit(a[1], "ToString decimals")
            if len(a) > 2:
                self.review("ToString thousands/decimal separator arguments are not translated")
            return f"F.format_string('%.{int(nd)}f', {x})", STRING
        if xt == STRING:
            return x, STRING
        if xt == UNKNOWN or xt == NUMBER:
            self.note("ToString(number): integral doubles render without '.0' (Alteryx style).")
            return (f"F.when(F.typeof({x}).isin('double', 'float') & ({x} == F.floor({x})), F.floor({x}).cast('string'))"
                    f".otherwise({x}.cast('string'))"), STRING
        return f"{x}.cast('string')", STRING

    def f_tonumber(self, a):
        g = self._a(a, (1, 4), "ToNumber")
        if len(a) > 1:
            self.review("ToNumber extra arguments (ignore errors/keep nulls/separator) are not translated")
        return cast_code(g[0][0], "double", self.ansi), NUMBER

    def f_toint(self, a):
        return self._toint(a)

    def f_tointeger(self, a):
        return self._toint(a)

    def _toint(self, a):
        (x, _), = self._a(a, 1, "ToInteger")
        return cast_code(x, "bigint", self.ansi), NUMBER

    def f_todate(self, a):
        (x, _), = self._a(a, 1, "ToDate")
        return f"F.date_format(F.try_to_timestamp({self._s(x, UNKNOWN)}), 'yyyy-MM-dd')", STRING

    def f_todatetime(self, a):
        (x, _), = self._a(a, 1, "ToDateTime")
        return f"F.date_format(F.try_to_timestamp({self._s(x, UNKNOWN)}), 'yyyy-MM-dd HH:mm:ss')", STRING

    # math
    def _m1(self, a, name, fn):
        (x, _), = self._a(a, 1, name)
        return f"{fn}({x})", NUMBER

    def f_abs(self, a):
        return self._m1(a, "Abs", "F.abs")

    def f_sqrt(self, a):
        return self._m1(a, "Sqrt", "F.sqrt")

    def f_log(self, a):
        return self._m1(a, "Log", "F.log")

    def f_log10(self, a):
        return self._m1(a, "Log10", "F.log10")

    def f_exp(self, a):
        return self._m1(a, "Exp", "F.exp")

    def f_pow(self, a):
        g = self._a(a, 2, "Pow")
        return f"F.pow({g[0][0]}, {g[1][0]})", NUMBER

    def f_mod(self, a):
        g = self._a(a, 2, "Mod")
        return f"({g[0][0]} % {g[1][0]})", NUMBER

    def _rnd(self, a, name, fn, mode):
        g = self._a(a, (1, 2), name)
        if len(g) == 1:
            return f"{fn}({g[0][0]})", NUMBER
        self.note(f"{name}(x, mult) rounds to a multiple of mult, not to decimal places.")
        return f"({fn}(F.try_divide({g[0][0]}, {g[1][0]})) * {g[1][0]})", NUMBER

    def f_round(self, a):
        g = self._a(a, 2, "Round")
        self.note("Round(x, mult) rounds to the nearest multiple of mult (half away from zero).")
        return f"(F.round(F.try_divide({g[0][0]}, {g[1][0]})) * {g[1][0]})", NUMBER

    def f_ceil(self, a):
        return self._rnd(a, "Ceil", "F.ceil", "ceil")

    def f_floor(self, a):
        return self._rnd(a, "Floor", "F.floor", "floor")

    def f_min(self, a):
        g = self._a(a, (2, 99), "Min")
        self.review("Min(a,b,...) NULL handling differs: Spark least() ignores NULLs")
        return f"F.least({', '.join(x[0] for x in g)})", NUMBER

    def f_max(self, a):
        g = self._a(a, (2, 99), "Max")
        self.review("Max(a,b,...) NULL handling differs: Spark greatest() ignores NULLs")
        return f"F.greatest({', '.join(x[0] for x in g)})", NUMBER

    # date / time (Alteryx dates travel as 'yyyy-MM-dd' / 'yyyy-MM-dd HH:mm:ss' strings)
    def f_datetimetoday(self, a):
        self._a(a, 0, "DateTimeToday")
        return "F.date_format(F.current_date(), 'yyyy-MM-dd')", STRING

    def f_datetimefirstofmonth(self, a):
        self._a(a, 0, "DateTimeFirstOfMonth")
        self.note("DateTimeFirstOfMonth() depends on the run date and Spark session time zone.")
        return "F.date_format(F.trunc(F.current_date(), 'month'), 'yyyy-MM-dd 00:00:00')", STRING

    def f_datetimelastofmonth(self, a):
        self._a(a, 0, "DateTimeLastOfMonth")
        self.note("DateTimeLastOfMonth() depends on the run date and Spark session time zone.")
        return "F.date_format(F.last_day(F.current_date()), 'yyyy-MM-dd 23:59:59')", STRING

    def f_datetimenow(self, a):
        if a:
            self.review("DateTimeNow timezone argument is not translated")
        return "F.date_format(F.current_timestamp(), 'yyyy-MM-dd HH:mm:ss')", STRING

    def _ts(self, x):
        return f"F.try_to_timestamp({self._s(x, UNKNOWN)})"

    def _dt_part(self, a, name, fn):
        (x, _), = self._a(a, 1, name)
        return f"{fn}({self._ts(x)})", NUMBER

    def f_datetimeyear(self, a):
        return self._dt_part(a, "DateTimeYear", "F.year")

    def f_datetimemonth(self, a):
        return self._dt_part(a, "DateTimeMonth", "F.month")

    def f_datetimeday(self, a):
        return self._dt_part(a, "DateTimeDay", "F.dayofmonth")

    def f_datetimehour(self, a):
        return self._dt_part(a, "DateTimeHour", "F.hour")

    def f_datetimeminutes(self, a):
        return self._dt_part(a, "DateTimeMinutes", "F.minute")

    def f_datetimeseconds(self, a):
        return self._dt_part(a, "DateTimeSeconds", "F.second")

    def f_datetimetrim(self, a):
        g = self._a(a, 2, "DateTimeTrim")
        part = str(self._lit(a[1], "DateTimeTrim part")).lower()
        ts = self._ts(g[0][0])
        fmt = {"year": "yyyy", "month": "MM", "day": "dd", "hour": "HH", "minute": "mm", "second": "ss"}.get(part)
        self.review("DateTimeTrim: the returned text for each part (year '2024', month '03', ..., firstofmonth/lastofmonth as 'yyyy-MM-dd') is "
                    "implemented from the Alteryx help text - confirm with a sample input/output from Alteryx")
        if fmt:
            return f"F.date_format({ts}, {fmt!r})", STRING
        if part == "firstofmonth":
            return f"F.date_format(F.trunc({ts}, 'month'), 'yyyy-MM-dd')", STRING
        if part == "lastofmonth":
            return f"F.date_format(F.last_day({ts}), 'yyyy-MM-dd')", STRING
        raise TranslationError(f"DateTimeTrim part {part!r} not supported")

    def f_datetimeadd(self, a):
        g = self._a(a, 3, "DateTimeAdd")
        unit = str(self._lit(a[2], "DateTimeAdd unit")).lower().rstrip("s")
        x, n, ts = g[0][0], g[1][0], self._ts(g[0][0])
        date_fmt = "'yyyy-MM-dd'"
        if unit == "year":
            return f"F.date_format(F.add_months({ts}, {n} * F.lit(12)), {date_fmt})", STRING
        if unit == "month":
            return f"F.date_format(F.add_months({ts}, {n}), {date_fmt})", STRING
        if unit == "week":
            return f"F.date_format(F.date_add({ts}, {n} * F.lit(7)), {date_fmt})", STRING
        if unit == "day":
            return f"F.date_format(F.date_add({ts}, {n}), {date_fmt})", STRING
        secs = {"hour": 3600, "minute": 60, "second": 1}.get(unit)
        if secs is None:
            raise TranslationError(f"DateTimeAdd unit {unit!r} not supported")
        self.note("DateTimeAdd on year/month/day returns 'yyyy-MM-dd'; time units return 'yyyy-MM-dd HH:mm:ss'.")
        return (f"F.date_format(F.timestamp_seconds(F.unix_timestamp({ts}) + {n} * F.lit({secs})), "
                f"'yyyy-MM-dd HH:mm:ss')"), STRING

    def f_datetimediff(self, a):
        g = self._a(a, 3, "DateTimeDiff")
        unit = str(self._lit(a[2], "DateTimeDiff unit")).lower().rstrip("s")
        t1, t2 = self._ts(g[0][0]), self._ts(g[1][0])
        if unit == "day":
            return f"F.datediff({t1}, {t2})", NUMBER
        secs = {"hour": 3600, "minute": 60, "second": 1}.get(unit)
        if secs is None:
            raise TranslationError(f"DateTimeDiff unit {unit!r} not supported (month/year boundary rules unverified)")
        return f"((F.unix_timestamp({t1}) - F.unix_timestamp({t2})) / F.lit({secs})).cast('bigint')", NUMBER

    def f_datetimeformat(self, a):
        g = self._a(a, (2, 4), "DateTimeFormat")
        fmt = strftime_to_java(str(self._lit(a[1], "DateTimeFormat format")))
        if len(a) > 2:
            self.review("DateTimeFormat locale/timezone arguments are not translated")
        return f"F.date_format({self._ts(g[0][0])}, {fmt!r})", STRING

    def f_datetimeparse(self, a):
        g = self._a(a, (2, 4), "DateTimeParse")
        fmt = strftime_to_java(str(self._lit(a[1], "DateTimeParse format")))
        if len(a) > 2:
            self.review("DateTimeParse locale/timezone arguments are not translated")
        return (f"F.date_format(F.try_to_timestamp({self._s(*g[0])}, F.lit({fmt!r})), "
                f"'yyyy-MM-dd HH:mm:ss')"), STRING


# ----------------------------------------------------------------------------- public API
def translate(expr: str, *, field_types: Optional[dict] = None, current_field: Optional[str] = None,
              row_ref: Optional[Callable[[str, int], str]] = None, df_var: str = "df", ci_default: bool = True,
              ansi: bool = False) -> Result:
    """Translate one Alteryx expression.  Raises TranslationError when it cannot do so reliably."""
    if expr is None or not str(expr).strip():
        raise TranslationError("Empty expression")
    parser = _Parser(str(expr))
    ast = parser.parse()
    gen = _Gen(field_types, current_field, row_ref, df_var, ci_default, ansi)
    code, typ = gen.gen(ast)
    if parser.mixed_bool_chain:
        gen.review("Unparenthesised mix of AND and OR: Alteryx applies equal precedence left-to-right; "
                   "emitted grouping follows that rule - confirm intent")
    return Result(code=code, type=typ, notes=gen.notes, reviews=gen.reviews, fields=gen.fields)


def referenced_fields(expr: str) -> list:
    """All [Field] names (excluding Row-N prefixes) mentioned in an expression; never raises."""
    return [m.group(1) for m in re.finditer(r"\[(?:Row\s*[+-]\s*\d+\s*:)?([^\]]*)\]", expr or "")]
