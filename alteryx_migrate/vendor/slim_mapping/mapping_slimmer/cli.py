"""Command line: python -m mapping_slimmer mapping.json --out OUT_DIR [--yxmd workflow.yxmd]"""
from __future__ import annotations

import argparse
import sys

from .pipeline import run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="slim_mapping",
        description="Slim a parsed Alteryx workflow (mapping.json) and split it into per-output chunks. "
                    "Exit code: 0 ok, 1 secret value leaked into an output, 2 another check failed.")
    parser.add_argument("mapping", help="parser output (keys: workflow, tool_mapping, ir, expressions)")
    parser.add_argument("--out", required=True, help="output directory (created if missing)")
    parser.add_argument("--yxmd", help="original workflow; makes Python input numbers (#n) and container names authoritative")
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    return run(args.mapping, args.out, args.yxmd)


if __name__ == "__main__":
    sys.exit(main())
