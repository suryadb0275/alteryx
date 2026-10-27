# Input format: parsed Alteryx workflow JSON

Derived from the real file `example/mapping.json` (4.4 MB, 1,533 tools). The loader (`scripts/wfmodel.py::load_workflow`) requires only `ir.nodes` and `ir.edges`.

```
{ "workflow":     {file_path, alteryx_version, tool_count, connection_count},
  "tool_mapping": [ {tool_id, tool_type, plugin_name, category, status: mapped|skipped|unsupported, ir_node_type, converter_class, reason} ],
  "ir": { "nodes": [ {node_type, node_id, original_tool_type, original_plugin_name, annotation, position, conversion_confidence,
                      conversion_notes[], conversion_method: deterministic|mapping|stub, ...tool-specific fields} ],
          "edges": [ {source, target, origin_anchor, destination_anchor, is_wireless} ] },
  "expressions":  [ {node_id, tool_type, label, alteryx_expression, status: parsed|failed, ast, spark_sql, error} ] }
```
- `tool_mapping` includes visual `ToolContainer`s (`status: skipped`, no IR node, no edges) - they carry no data logic.
- Anchors: `origin_anchor` names the output (`Output`, `True`/`False`, `Join`/`Left`/`Right`, `Unique`/`Duplicates`, `Output26` for macros);
  `destination_anchor` names the input (`Input`, `Left`/`Right`, `Targets`/`Source`, `Input2`). Several edges into the `Input` anchor of a Union are its inputs, in file order.
- There are **no field schemas** for sources (`field_info` empty) and no statistics. Columns and types are resolved at runtime; table names come from the `query`.
- Wireless connections (`is_wireless: true`) are ordinary dependencies.

## Tool-specific IR fields used by the generator
- Read/Input: `source_type, connection_string (an Alteryx alias, not a URL), query, file_path, file_format, has_header, delimiter, encoding`
- Filter: `expression, mode` - Formula: `formulas[{output_field, expression, data_type, size}]` - MultiFieldFormula: `expression, fields[], output_type, copy_output`
- MultiRowFormula: `expression, output_field, group_fields` - Select: `field_operations[{field_name, action, selected, rename_to, new_type, new_size}], select_all_unknown`
- Join: `join_keys[{left_field,right_field}], select_left, select_right` - Union: `mode, allow_missing` - Summarize: `aggregations[{field_name, action, output_field_name, separator, percentile_value}]`
- Sort: `sort_fields[{field_name, ascending, nulls_first}]` - Unique: `key_fields` - CrossTab: `group_fields, header_field, value_field, aggregation, separator`
- Transpose: `key_fields, data_fields, header_name, value_name` - TextToColumns: `field_name, delimiter, split_to, num_columns, output_root_name, skip_empty`
- RecordID: `output_field, starting_value, output_type` - TextInput (LiteralData): `field_names, field_types, data_rows` - Output/Write: `file_path, file_format, destination_type, write_mode`
- Macro (`UnsupportedNode`): `original_configuration, unsupported_reason`

## Known converter defects (detected generically by `wfmodel.config_issues`, not by tool id)
| Symptom in the sample | Meaning | Handling |
|---|---|---|
| `join_keys: []` on 108 of 158 joins (and all Join Multiple) | key configuration not captured (could also be join-by-position; undeterminable) | **blocked**; supply keys via `--overrides` |
| `MultiRowFormula.output_field == "False"` (all 32) | a boolean was parsed as the field name | **blocked** |
| `CrossTab.header_field/value_field` empty (all 83), `DynamicRename.fields_to_rename` empty (all 86), `RunningTotal.running_fields` empty (all 12), `DateTime` fields empty | settings lost | **blocked** |
| `Transpose.data_fields` contains the key fields | overlap would duplicate keys | **blocked** |
| `expressions[].status == "failed"` (61) for every `==` | converter parser has no `==` | ignored: expressions are re-translated from the original text |
| `spark_sql` hints present | converter output, unverified | **not used** |
| Macro `cleanse.yxmc` x75 (Alteryx Data Cleansing tool: anchors `Input2` -> `Output26`) | `<Value name="Check Box (n)">` questions with fixed names in the macro | **converted** (`DataCleansingMacroNode`, see `wfmodel.CLEANSE_QUESTIONS`) |
| `ReadNode.connection_string = "aka:D1"` | Alteryx connection alias | placeholder in `00_config` (`<SET: ...>`) |

## Reading the original XML (`--yxmd`)
The parsed JSON is lossy; the `.yxmd` is the source of truth. `scripts/xml_recover.py` fills what the converter dropped (join keys, Cross Tab/Running Total/Transpose/Multi-Row/Dynamic Rename/DateTime/Generate Rows settings, input `MetaInfo` schemas, macro file names) - see `references/09-preflight.md`. The defects listed above only block a tool when no `.yxmd` is supplied or the XML itself lacks the value.

## Other shapes
- Raw `.yxmd` XML is not parsed by this skill. Either run the converter that produced the JSON, or read `workflow-xml.md` and the `migration:alteryx-to-sdp` parser to recover configuration, then re-create the IR.
- A JSON with top-level `nodes`/`edges` (no `ir`) is accepted. Anything else raises an error that points here; adapt `load_workflow` rather than guessing field names.
- Different converter versions may rename keys: if `analyze_workflow.py` reports zero formulas/joins where the file clearly has them, compare node keys with the lists above.
