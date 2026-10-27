# slim_mapping

Shrinks a parsed Alteryx workflow (`mapping.json` from `parse_and_map`) and splits it into one self-contained chunk per output
table. Deterministic, standard library only (Python 3.10+), no AI and no network.

```
python slim_mapping.py mapping.json --out OUT_DIR [--yxmd workflow.yxmd]
python -m mapping_slimmer mapping.json --out OUT_DIR [--yxmd workflow.yxmd]     # same thing
python -m unittest discover -s tests -t .                                       # tests
```

`--yxmd` is optional but strongly recommended: the `#n` numbers of an embedded Python tool (`Alteryx.read("#3")`) exist only on
the `<Connection name="#n">` elements of the original file. Without it the file order is used and every output says UNVERIFIED.

## Output (in OUT_DIR)

| File | Content |
|---|---|
| `slim.json` | whole workflow, slimmed, tools in dependency order |
| `workflow_map.md` | outputs, sources, one row per tool, the Python inputs table (`#n -> DataFrame -> upstream tool -> source table`) |
| `chunks/NN_<output>.md` | one per output: map + only the tools feeding it + specs + formula expressions + that output's Python slice |
| `jupyter/full_script.py`, `jupyter/cells.json` | the embedded script and its per-cell analysis |
| `coverage_report.txt` | the built-in checks |

Exit code: `0` all checks pass, `1` a secret value leaked into an output, `2` another check failed.

## Modules

| Module | Responsibility |
|---|---|
| `config.py` | all policy constants: secret/picker patterns, plugin whitelists, size limit, exit codes |
| `common.py` | small shared helpers (sizes, empty-stripping, config lookup) |
| `slimming.py` | drop secrets / picker dumps / large blobs from a tool's configuration and record each omission |
| `redaction.py` | collect secret values from the source and search the outputs for them |
| `script_cells.py` | extract the embedded script, split it into cells |
| `name_scan.py` | `ast` analysis of one cell: names loaded/stored, `Alteryx.read/write` |
| `slicing.py` | minimal cells per `Alteryx.write(df, k)` (backward walk) and the compile/undefined-name check |
| `yxmd.py` | connection numbers and container names from the original XML |
| `graph.py` | nodes, edges, dependency order, sinks and sources |
| `describe.py` | one-line tool descriptions and source-table lookup |
| `python_tool.py` | per-tool Python analysis and input numbering |
| `render_map.py`, `render_chunk.py` | the markdown files |
| `report.py` | the checks and the coverage report |
| `pipeline.py`, `cli.py` | orchestration and command line |

## Rules it follows

* Keys named like `token|secret|cred|passw|api_key|private`, `tree*`/`picker*` dumps and strings over 3000 characters are dropped
  from `original_configuration`; BigQuery input/output and Google Sheets keep only a whitelist. Each dropped key is listed under
  `omitted_config_keys` with its size in characters - never its value.
* Logic fields (`field_operations`, `formulas`, `aggregations`, `key_fields`, expressions) are kept; only empty values and canvas
  `position` are removed.
* An output's Python slice contains an earlier cell only if that cell **stores** a name the slice needs (assignment,
  `df['x'] = ...`, augmented assignment, import, loop target, in-place call such as `.drop(inplace=True)` or `list.append()`).
  Limitation: a mutation hidden inside a user-defined function is not seen. Cells that cannot be parsed are included everywhere.

## Extending

* New known plugin: add a fragment and its kept keys to `PLUGIN_WHITELISTS` in `config.py`.
* New tool description: add a branch in `describe.describe`.
* Unknown tool types never crash the run; they appear as their tool type with their slimmed configuration.
