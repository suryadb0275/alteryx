"""Tiny, dependency-free line wrapper for generated PySpark expressions.

Only inserts line breaks inside existing brackets (always valid Python): boolean chains, `.when()/.otherwise()` chains
and long call arguments.  Short expressions are returned untouched.
"""
from __future__ import annotations

WIDTH = 100


def _scan(s):
    """Yield (index, char, depth) for characters outside string literals."""
    depth, quote, i = 0, None, 0
    while i < len(s):
        c = s[i]
        if quote:
            if c == "\\":
                i += 2
                continue
            if c == quote:
                quote = None
        elif c in "'\"":
            quote = c
        else:
            yield i, c, depth
            if c in "([{":
                depth += 1
            elif c in ")]}":
                depth -= 1
        i += 1


def _match_whole(s):
    """True if s starts with '(' and that bracket closes at the very end."""
    if not s.startswith("("):
        return False
    for i, c, d in _scan(s):
        if c == ")" and d == 1:
            return i == len(s) - 1
    return False


def _split(s, seps):
    """Split s at depth-0 occurrences of any separator; return (parts, separators-used-before-each-part)."""
    cuts = []
    for i, c, d in _scan(s):
        if d == 0:
            for sep in seps:
                if s.startswith(sep, i):
                    cuts.append((i, sep))
                    break
    parts, ops, prev = [], [""], 0
    for i, sep in cuts:
        parts.append(s[prev:i])
        ops.append(sep)
        prev = i + len(sep)
    parts.append(s[prev:])
    return parts, ops


def strip_outer(s):
    s = s.strip()
    while _match_whole(s) and not _split(s[1:-1], [", "])[0][1:]:      # redundant wrapping parentheses
        s = s[1:-1].strip()
    return s


def operand(part, indent, width):
    """An operand of & / | : its own parentheses are part of its meaning (`(a == b) & (c > d)`), so they are kept."""
    part = part.strip()
    if not _match_whole(part):
        return fmt(part, indent, width)
    inner = fmt(part[1:-1], indent, width)
    return inner if _match_whole(inner) else "(" + inner + ")"


def fmt(code, indent=0, width=WIDTH, chain=True):
    s = strip_outer(code)
    if indent + len(s) <= width:
        return s
    pad = " " * (indent + 4)
    end = " " * indent
    # 1. .when()/.otherwise() chains
    parts, ops = _split(s, [".when(", ".otherwise("]) if chain else ([s], [""])
    if len(parts) > 1:
        head = parts[0] + ops[1] + parts[1]                     # keep 'F.when(...)' together on the first line
        lines = [fmt(head, indent + 4, width, chain=False)] + [
            ops[i] + fmt(parts[i], indent + 4, width) for i in range(2, len(parts))]
        return "(\n" + "\n".join(pad + ln for ln in lines) + "\n" + end + ")"
    # 2. boolean chain  a & b & c   (operators lead continuation lines)
    parts, ops = _split(s, [" & ", " | "])
    if len(parts) > 1:
        lines = [operand(parts[0], indent + 4, width)] + [
            ops[i].strip() + " " + operand(parts[i], indent + 6, width) for i in range(1, len(parts))]
        return "(\n" + "\n".join(pad + ln for ln in lines) + "\n" + end + ")"
    # 3. call with several arguments
    head = s.split("(", 1)
    if (len(head) == 2 and s.endswith(")") and head[0].replace(".", "").replace("_", "").replace("=", "").isalnum()
            and _match_whole(s[len(head[0]):])):                       # the first '(' must close at the very end
        inner = s[len(head[0]) + 1:-1]
        args, _ = _split(inner, [", "])
        if len(args) > 1 and "\n" not in inner and len(pad) + len(", ".join(args)) + 1 <= width:
            return head[0] + "(\n" + pad + ", ".join(args) + ",\n" + end + ")"     # all arguments fit on one line
        if len(args) > 1:
            return (head[0] + "(\n" + ",\n".join(pad + fmt(a, indent + 4, width) for a in args)
                    + ",\n" + end + ")")
        if len(args) == 1:
            return head[0] + "(\n" + pad + fmt(args[0], indent + 4, width) + "\n" + end + ")"
    # 4. long list / dict literal (optionally `name=`): pack several items per line
    for k, c in enumerate(s):
        if c in "[{":
            prefix, body = s[:k], s[k:]
            closer = "]" if c == "[" else "}"
            if prefix.replace("=", "").replace("_", "").replace(".", "").isalnum() or prefix == "":
                if body.endswith(closer) and _match_whole(("(" + body[1:-1] + ")")):
                    items, _ = _split(body[1:-1], [", "])
                    if len(items) > 1:
                        lines, cur = [], ""
                        for it in items:
                            piece = it + ","
                            if cur and len(pad) + len(cur) + 1 + len(piece) > width:
                                lines.append(cur)
                                cur = piece
                            else:
                                cur = f"{cur} {piece}" if cur else piece
                        lines.append(cur)
                        return prefix + c + "\n" + "\n".join(pad + ln for ln in lines) + "\n" + end + closer
            break
    return s
