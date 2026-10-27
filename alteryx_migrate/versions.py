"""Skill version = the git SHA of the skill folder (falls back to a content hash where there is no git, for example on Databricks)."""
from __future__ import annotations

import hashlib
import subprocess

from . import paths


def skill_version() -> str:
    skill = paths.skill_dir()
    stamp = skill / ".skill_version"                              # written by scripts/vendor.py next to the copied skill
    if stamp.exists():
        return stamp.read_text(encoding="utf-8").strip()
    try:
        done = subprocess.run(["git", "-C", str(skill), "log", "-1", "--format=%h", "--", "SKILL.md"], capture_output=True, text=True, timeout=10)
        sha = done.stdout.strip()
        if done.returncode == 0 and sha:
            dirty = subprocess.run(["git", "-C", str(skill), "status", "--porcelain", "--", "."], capture_output=True, text=True, timeout=10).stdout.strip()
            return sha + ("+dirty" if dirty else "")
    except (OSError, subprocess.SubprocessError):
        pass
    return "sha256:" + hashlib.sha256((skill / "SKILL.md").read_bytes()).hexdigest()[:12]


def skill_text() -> str:
    return (paths.skill_dir() / "SKILL.md").read_text(encoding="utf-8")
