#!/usr/bin/env python3
"""slim_mapping.py - launcher. Usage: python slim_mapping.py mapping.json --out OUT_DIR [--yxmd workflow.yxmd]

Shrinks a parsed Alteryx mapping.json (keeps all logic, drops secrets and embedded blobs) and splits it into one chunk per
output table. The implementation is the `mapping_slimmer` package next to this file; see README.md.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mapping_slimmer.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
