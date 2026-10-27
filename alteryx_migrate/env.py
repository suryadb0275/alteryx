"""Read a `.env` file (KEY=VALUE lines) into the environment, without any extra package. Real environment variables are never overridden."""
from __future__ import annotations

import os
from pathlib import Path


def load(path: str | Path = ".env") -> list:
    """Set the variables of `path` that are not set yet; returns their names. A missing file is not an error. Values are never printed."""
    p = Path(path)
    if not p.is_file():
        return []
    done = []
    for raw in p.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip().removeprefix("export ").strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        if key and value and key not in os.environ:
            os.environ[key] = value
            done.append(key)
    return done
