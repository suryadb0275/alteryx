"""Slim a tool's configuration: drop secrets, picker dumps and large blobs; record every omission (key and size, never the value)."""
from __future__ import annotations

from .common import plugin_of, size_of, strip_empty
from .config import MAX_STRING_CHARS, PLUGIN_WHITELISTS, SCRIPT_KEYS, SECRET_KEY, TREE_KEY


def _record(omitted: list, key: str, value, reason: str) -> None:
    omitted.append({"key": key, "chars": size_of(value), "why": reason})


def _reason_for_key(name: str) -> str | None:
    if SECRET_KEY.search(name):
        return "secret-named"
    if TREE_KEY.search(name):
        return "tree/picker dump"
    return None


def _slim_unknown(value, path: str, omitted: list):
    """Safe fallback for unknown plugins: keep small values, drop secret-named, tree/picker and large ones."""
    if isinstance(value, dict):
        if "@name" in value and ("#text" in value or len(value) == 1):          # {"@name": k, "#text": v} item
            name, text = str(value["@name"]), value.get("#text", "")
            reason = _reason_for_key(name) or (f"larger than {MAX_STRING_CHARS} chars" if size_of(text) > MAX_STRING_CHARS else None)
            if reason:
                _record(omitted, f"{path}[{name}]", text, reason)
                return None
            return value
        kept = {}
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            reason = _reason_for_key(str(key))
            if reason:
                _record(omitted, child_path, child, reason)
                continue
            slimmed = _slim_unknown(child, child_path, omitted)
            if slimmed is not None:
                kept[key] = slimmed
        return kept
    if isinstance(value, list):
        return [s for s in (_slim_unknown(v, path, omitted) for v in value) if s is not None]
    if isinstance(value, str) and len(value) > MAX_STRING_CHARS:
        _record(omitted, path, value, f"larger than {MAX_STRING_CHARS} chars")
        return None
    return value


def _slim_whitelisted(config: dict, keep: set, omitted: list) -> dict:
    """Known plugin: only whitelisted settings survive (named-item lists and plain dicts are both handled)."""
    if isinstance(config.get("Value"), list):
        items = []
        for item in config["Value"]:
            name = str(item.get("@name")) if isinstance(item, dict) else ""
            if name in keep and not SECRET_KEY.search(name):
                items.append(item)
            elif isinstance(item, dict):
                _record(omitted, f"Value[{name}]", item.get("#text", ""), _reason_for_key(name) or "not whitelisted")
        for key, value in config.items():
            if key != "Value":
                _record(omitted, key, value, "not whitelisted")
        return {"Value": items}
    kept = {}
    for key, value in config.items():
        if key in keep and not SECRET_KEY.search(key):
            kept[key] = value
        else:
            _record(omitted, key, value, _reason_for_key(key) or "not whitelisted")
    return kept


def slim_configuration(config, plugin: str):
    """-> (kept configuration, list of omitted keys with their size)."""
    omitted: list = []
    keep = next((keys for fragment, keys in PLUGIN_WHITELISTS.items() if fragment in plugin), None)
    if keep is None:
        return _slim_unknown(config, "", omitted), omitted
    return _slim_whitelisted(config, keep, omitted), omitted


def slim_node(node: dict, python_reference: dict | None = None) -> dict:
    """The node without canvas position and with a slimmed configuration; logic fields are untouched."""
    slim = {k: v for k, v in node.items() if k not in ("position", "original_configuration")}
    config = node.get("original_configuration")
    omitted: list = []
    if python_reference and isinstance(config, dict):         # the script lives in its own file, not in the node
        config = dict(config)
        for key in SCRIPT_KEYS:
            if key in config:
                _record(omitted, key, config.pop(key), f"script saved to {python_reference['file']}")
    if isinstance(config, (dict, list)) and config:
        kept, dropped = slim_configuration(config, plugin_of(node))
        omitted += dropped
        if kept:
            slim["original_configuration"] = kept
    if omitted:
        slim["omitted_config_keys"] = omitted
    if python_reference:
        slim["python_script"] = python_reference
    return strip_empty(slim)
