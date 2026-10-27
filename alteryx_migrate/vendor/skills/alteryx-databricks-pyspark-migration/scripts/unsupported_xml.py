"""Keep unsupported tools as XML, unchanged.

Every tool the skill cannot migrate is written, with its configuration exactly as the parsed JSON holds it, to
``unsupported_tools.xml`` next to the notebooks, so nothing is lost and a human (or a later skill version) can implement it.

Secrets (keys/names containing secret, token, password, credential, private, api key) are replaced by "[REDACTED: ...]" and values
longer than 500,000 characters by "[OMITTED: ...]" - the export keeps every setting except credentials and embedded blobs.

Conversion rules (xmltodict convention, which is how the converter produced ``original_configuration``):
  * key ``@name``  -> XML attribute ``name``;   key ``#text`` -> element text
  * list value     -> repeated child elements with the same tag
  * a key that is not a valid XML name (spaces, digits first, ...) -> ``<Item key="...">``
  * if the node carries the raw tool XML as a string (``original_xml`` / ``xml`` / ``raw_xml``) that string is embedded
    verbatim in a CDATA section instead of being re-serialised.
"""
from __future__ import annotations

import re
from xml.sax.saxutils import escape, quoteattr

SECRET = re.compile(r"secret|token|passw|cred|private|api[_-]?key|authorization", re.I)       # 'cred' also catches serviceCredsJSONStr
MAX_TEXT = 500_000                     # longer values (embedded blobs) are replaced by a marker
HARMLESS = {"", "{}", "[]", "false", "true", "none", "null", "0", "1"}


def clean(name: str, text) -> str:
    """Secrets are never copied into the export; huge embedded values are summarised."""
    s = _s(text)
    if SECRET.search(name or "") and s.strip().lower() not in HARMLESS:
        return f"[REDACTED: secret value, {len(s)} chars]"
    if len(s) > MAX_TEXT:
        return f"[OMITTED: {len(s)} chars of embedded data]"
    return s

_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
RAW_KEYS = ("original_xml", "raw_xml", "xml")
SKIP = {"position"}          # canvas coordinates are layout, not configuration


def _el(tag, value, ind):
    pad = "  " * ind
    attrs, text, children = {}, None, []
    if isinstance(value, dict):
        for k, v in value.items():
            if k.startswith("@"):
                attrs[k[1:]] = v
            elif k == "#text":
                text = v
            else:
                children.append((k, v))
    elif isinstance(value, list):
        return "\n".join(_el(tag, v, ind) for v in value) if value else f"{pad}<{_name(tag)[0]}{_name(tag)[1]}/>"
    else:
        text = value
    name, extra = _name(tag)
    if isinstance(text, str):
        text = clean(str(attrs.get("name") or tag), text)
    a = extra + "".join(f" {k}={quoteattr(str(v))}" for k, v in attrs.items() if _NAME.match(k))
    if not children:
        if text is None:
            return f"{pad}<{name}{a}/>"
        return f"{pad}<{name}{a}>{escape(_s(text))}</{name}>"
    inner = "\n".join(_el(k, v, ind + 1) for k, v in children)
    body = f"\n{pad}  {escape(_s(text))}" if text not in (None, "") else ""
    return f"{pad}<{name}{a}>{body}\n{inner}\n{pad}</{name}>"


def _name(tag):
    return (tag, "") if _NAME.match(tag) else ("Item", f" key={quoteattr(tag)}")


def _s(v):
    return "true" if v is True else "false" if v is False else str(v)


def tool_xml(node: dict, reason: str = "") -> str:
    """One <Tool> element for an unsupported tool."""
    head = (f'  <Tool id="{node["node_id"]}" type={quoteattr(str(node.get("original_tool_type") or node["node_type"]))} '
            f'plugin={quoteattr(str(node.get("original_plugin_name") or ""))} irNodeType={quoteattr(node["node_type"])} '
            f'reason={quoteattr(reason)}>')
    raw = next((node[k] for k in RAW_KEYS if isinstance(node.get(k), str) and node[k].strip()), None)
    if raw is not None:
        body = "    <OriginalXml><![CDATA[" + raw.replace("]]>", "]]]]><![CDATA[>") + "]]></OriginalXml>"
    elif isinstance(node.get("original_configuration"), (dict, list)) and node["original_configuration"]:
        body = _el("OriginalConfiguration", node["original_configuration"], 2)
    else:                               # tools without a captured configuration: keep every IR field as it is
        cfg = {k: v for k, v in node.items() if k not in SKIP and not k.startswith("_") and k not in
               ("node_type", "node_id", "original_tool_type", "original_plugin_name")}
        body = _el("IrNode", cfg, 2)
    return f"{head}\n{body}\n  </Tool>"


def tools_xml(items) -> str:
    """items: [(node dict, reason)] -> a complete XML document."""
    parts = ['<?xml version="1.0" encoding="utf-8"?>', "<UnsupportedTools>"]
    parts += [tool_xml(n, r) for n, r in items]
    parts.append("</UnsupportedTools>")
    return "\n".join(parts) + "\n"
