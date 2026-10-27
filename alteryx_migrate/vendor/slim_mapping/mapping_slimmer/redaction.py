"""Secret handling for the leak check: collect every secret VALUE in the source (memory only) and search the outputs for them."""
from __future__ import annotations

import json
import os

from .config import HARMLESS_VALUES, LONG_SECRET_FINGERPRINT, LONG_SECRET_THRESHOLD, MIN_SECRET_LENGTH, SECRET_KEY


def collect_secret_values(obj, found: set, key: str = "") -> None:
    """Add every secret value under a secret-named key (also inside JSON-in-a-string) to `found`."""
    if isinstance(obj, dict):
        if "@name" in obj and "#text" in obj and SECRET_KEY.search(str(obj["@name"])):
            collect_secret_values(obj["#text"], found, str(obj["@name"]))
        for child_key, value in obj.items():
            collect_secret_values(value, found, str(child_key))
    elif isinstance(obj, list):
        for value in obj:
            collect_secret_values(value, found, key)
    elif isinstance(obj, str) and SECRET_KEY.search(key) and len(obj) >= MIN_SECRET_LENGTH \
            and obj.strip().lower() not in HARMLESS_VALUES:
        found.add(obj)
        try:                                              # service-account keys are JSON stored as a string
            inner = json.loads(obj)
        except ValueError:
            return
        if isinstance(inner, (dict, list)):
            collect_secret_values(inner, found, key)


def fingerprint(secret: str) -> str:
    """What is searched for: the whole secret, or just its head when it is huge."""
    return secret if len(secret) <= LONG_SECRET_THRESHOLD else secret[:LONG_SECRET_FINGERPRINT]


def find_leaks(secrets: set, directory: str, skip: tuple = ("coverage_report.txt",)):
    """-> (leaks [(relative path, secret length)], number of files scanned)."""
    leaks, scanned = [], 0
    for root, _, files in os.walk(directory):
        for name in files:
            if name in skip:
                continue
            path = os.path.join(root, name)
            with open(path, encoding="utf-8", errors="replace") as handle:
                text = handle.read()
            scanned += 1
            leaks += [(os.path.relpath(path, directory), len(s)) for s in secrets if fingerprint(s) in text]
    return leaks, scanned
