"""Small helpers shared by several modules."""
from __future__ import annotations

import json
import re


def size_of(value) -> int:
    """Size in characters: the string length, or the length of the JSON form."""
    return len(value) if isinstance(value, str) else len(json.dumps(value, ensure_ascii=False, default=str))


def is_empty(value) -> bool:
    return value is None or value == "" or value == [] or value == {}


def strip_empty(obj):
    """Recursively remove None, '', [] and {} (False and 0 are real values and are kept)."""
    if isinstance(obj, dict):
        cleaned = {k: strip_empty(v) for k, v in obj.items()}
        return {k: v for k, v in cleaned.items() if not is_empty(v)}
    if isinstance(obj, list):
        return [v for v in (strip_empty(i) for i in obj) if not is_empty(v)]
    return obj


def snake(text, limit: int = 60) -> str:
    """File-name-safe version of a label."""
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", str(text)).strip("_")
    return cleaned[:limit].rstrip("_") or "unnamed"


def sql_tables(sql: str) -> list[str]:
    """Table names after FROM / JOIN in a SQL string."""
    return [t.strip("`") for t in re.findall(r"(?is)\b(?:from|join)\s+(`?[\w\-\.\[\]#]+`?)", sql or "")]


def named_items(config) -> dict:
    """Google-Sheets style config {'Value': [{'@name': k, '#text': v}, ...]} -> {k: v}."""
    items = config.get("Value") if isinstance(config, dict) else None
    if not isinstance(items, list):
        return {}
    return {i.get("@name"): i.get("#text") for i in items if isinstance(i, dict)}


def config_value(config, name: str):
    """A setting from a configuration given either as a plain dict or as a named-item list."""
    if not isinstance(config, dict):
        return None
    return config[name] if name in config else named_items(config).get(name)


def plugin_of(node: dict) -> str:
    """Lower-case 'plugin tool-type' text used to recognise a plugin."""
    return f"{node.get('original_plugin_name') or ''} {node.get('original_tool_type') or ''}".lower()


def tool_label(node: dict) -> str:
    return node.get("original_tool_type") or node.get("node_type") or "?"


def format_size(num_bytes: int) -> str:
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:,.1f} KB"
    return f"{num_bytes / 1024 / 1024:,.2f} MB"
