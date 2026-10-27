"""Where the existing tools live. This package calls them as they are; it re-implements none of them.

Resolution order for the folder that holds parse_and_map/, slim_mapping/ and skills/:
  1. env ALTERYX_TOOLS_ROOT
  2. alteryx_migrate/vendor/   (committed copy, refreshed by scripts/vendor.py; a plain clone works on Databricks)
  3. the folder above the repository (local development inside the larger skills repo)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

PKG_DIR = Path(__file__).resolve().parent
REPO = PKG_DIR.parent
SKILL_NAME = "alteryx-databricks-pyspark-migration"


def tools_root() -> Path:
    env = os.environ.get("ALTERYX_TOOLS_ROOT")
    for candidate in ([Path(env)] if env else []) + [PKG_DIR / "vendor", REPO.parent]:
        if (candidate / "parse_and_map" / "parse_and_map.py").exists():
            return candidate
    raise FileNotFoundError("parse_and_map/ not found: run `python scripts/vendor.py` or set ALTERYX_TOOLS_ROOT")


def parse_script() -> Path:
    return tools_root() / "parse_and_map" / "parse_and_map.py"


def slim_script() -> Path:
    return tools_root() / "slim_mapping" / "slim_mapping.py"


def skill_dir() -> Path:
    return tools_root() / "skills" / SKILL_NAME


def use_skill() -> None:
    """Make the migration skill's scripts importable (the conversion always goes through the skill)."""
    scripts = str(skill_dir() / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
