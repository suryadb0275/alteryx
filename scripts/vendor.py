"""Refresh alteryx_migrate/vendor/ from the original tools. alteryx_migrate/vendor/ is COMMITTED, so a plain clone deploys without this step; run it only to update the snapshot.

    python scripts/vendor.py [--tools-root <folder with parse_and_map/, slim_mapping/ and skills/>]

Copies parse_and_map/, slim_mapping/ and the migration skill into alteryx_migrate/vendor/ .
The skill's git SHA is written to alteryx_migrate/vendor/skills/<skill>/.skill_version, which the conversion report records.
Nothing under the source folders is modified.
"""
from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
SKILL = "alteryx-databricks-pyspark-migration"
IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc", "tests", "output", "*.yxmd", "spark-warehouse", ".pytest_cache")


def skill_sha(skill: Path) -> str:
    try:
        done = subprocess.run(["git", "-C", str(skill), "log", "-1", "--format=%h", "--", "SKILL.md"], capture_output=True, text=True, timeout=15)
        if done.returncode == 0 and done.stdout.strip():
            dirty = subprocess.run(["git", "-C", str(skill), "status", "--porcelain", "--", "."], capture_output=True, text=True, timeout=15).stdout.strip()
            return done.stdout.strip() + ("+dirty" if dirty else "")      # uncommitted changes in the skill folder are part of what ran
    except (OSError, subprocess.SubprocessError):
        pass
    return "sha256:" + hashlib.sha256((skill / "SKILL.md").read_bytes()).hexdigest()[:12]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tools-root", type=Path, default=HERE.parent, help="folder with parse_and_map/, slim_mapping/ and skills/ (default: the parent of this folder)")
    args = parser.parse_args()
    root, out = args.tools_root.resolve(), HERE / "alteryx_migrate" / "vendor"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    for folder in ("parse_and_map", "slim_mapping"):
        if not (root / folder).is_dir():
            raise SystemExit(f"{root / folder} not found: pass --tools-root")
        shutil.copytree(root / folder, out / folder, ignore=IGNORE)
    skill = root / "skills" / SKILL
    if not skill.is_dir():
        raise SystemExit(f"{skill} not found: pass --tools-root")
    shutil.copytree(skill, out / "skills" / SKILL, ignore=IGNORE)
    (out / "skills" / SKILL / ".skill_version").write_text(skill_sha(skill), encoding="utf-8")
    print(f"vendored into {out} (skill version {skill_sha(skill)})")


if __name__ == "__main__":
    main()
