# parse_and_map.py

Standalone parser + tool-mapping report for an Alteryx `.yxmd` workflow.
Runs only the first two phases of the a2d pipeline — **Parse** (XML ->
`ParsedWorkflow`) and **Convert** (`ParsedNode` -> `IRNode`) — and prints,
for every tool in the workflow, which IR node / converter it mapped to (or
why it didn't), the resulting IR graph (nodes + edges), and every
formula/filter-condition expression found, parsed and translated to Spark
SQL. It does **not** generate any PySpark/DLT/SQL/Lakeflow/Designer code.

## Self-contained and movable

This whole folder is standalone: `a2d/` next to this file is a vendored
copy of exactly the a2d modules this script needs (parser, converter
registry, IR, expression engine — no code generators, no CLI, no server).
It has no dependency on the rest of this repo. Copy or move this folder
anywhere and it keeps working.

## Setup

```bash
pip install -r requirements.txt
```

Installs only `lxml` and `networkx` — the only third-party packages the
vendored a2d subset needs underneath it (see the comment in
`requirements.txt` for how that was confirmed).

## Usage

```bash
python parse_and_map.py path/to/workflow.yxmd
python parse_and_map.py path/to/workflow.yxmd --json
python parse_and_map.py path/to/workflow.yxmd --json -o mapping.json
```

Or from the repo root:
```bash
python scripts/parse_and_map/parse_and_map.py path/to/workflow.yxmd
```

## Keeping the vendored copy in sync

The `a2d/` subdirectory here is a point-in-time copy, not a symlink or
package reference. If the main repo's `src/a2d` parser/converter/IR/
expression code changes, re-copy the same module set into this folder to
pick up the changes.
