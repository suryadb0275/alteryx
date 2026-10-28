"""conversion_report.md ends with the full conversion context: every source, every table with its tools and code, every missing transformation with its settings,
and the tools left out on purpose - enough for a person or another AI to finish or re-target the conversion without opening the workflow."""
import json

from conftest import FIXTURES
from alteryx_migrate import pipeline

FIXES = FIXTURES / "analysis_fixes_flow.yxmd"


def test_a_missing_transformation_comes_with_its_settings_place_and_recipe(tmp_path):
    pipeline.convert_workflow(FIXES, tmp_path, "main", "s")                    # no answer for the Multi-Row Formula [21]'s order: it stays missing
    report = (tmp_path / "conversion_report.md").read_text(encoding="utf-8")
    ctx = json.loads((tmp_path / "conversion_context.json").read_text(encoding="utf-8"))
    assert ctx["missing"] == [21, 22, 23, 24] and "# Conversion context" in report                           # [22]-[24]: their table waits for [21]
    assert {t["status"] for t in ctx["tools"] if t["id"] in (22, 23, 24)} == {"NOT CONVERTED - waits for [21] MultiRowFormula"}
    section = report.split("## 1. Missing transformations")[1].split("## 2. Sources")[0]
    assert "### [21] MultiRowFormula - NOT CONVERTED - cannot be converted" in section
    assert "Comes after: [20] TextInput (Output -> Input)" in section and "Feeds: [22]" in section
    assert "the order of the records" in section and "<UpdateField_Name>Plan</UpdateField_Name>" in section     # why, and the original XML
    assert "How to replicate it" in section
    tool = next(t for t in ctx["tools"] if t["id"] == 21)
    assert tool["tables"] == ["gold_g_seq_lookup_selected"] and "Row-1:Plan" in tool["settings_xml"]


def test_converted_tools_carry_their_code_and_nothing_is_reported_missing_when_all_is_converted(tmp_path):
    pipeline.convert_workflow(FIXES, tmp_path, "main", "s", {"21": {"mode": "order_by", "fields": ["Seq"]}})
    report = (tmp_path / "conversion_report.md").read_text(encoding="utf-8")
    ctx = json.loads((tmp_path / "conversion_context.json").read_text(encoding="utf-8"))
    assert ctx["missing"] == [] and "Missing transformations: **0**" in report
    tables = report.split("## 3. Output tables, in build order")[1]
    assert "### `gold_k_x_lookup_selected`" in tables and "- [12] Join: converted by rule" in tables and "k_x_lookup_joined_on = " in tables
    assert [s["table_key"] for s in ctx["sources"]] == [] or all("unity_catalog_name" in s for s in ctx["sources"])
    statuses = {t["id"]: t["status"] for t in ctx["tools"]}
    assert statuses[1] == "converted by rule" and statuses[22].startswith("converted by rule")
    assert ctx["decisions"] == {"21": {"mode": "order_by", "fields": ["Seq"]}}


def test_a_tool_without_a_converter_and_a_kept_python_tool(tmp_path):
    pipeline.convert_workflow(FIXTURES / "sample_flow.yxmd", tmp_path / "s", "main", "s")
    report = (tmp_path / "s" / "conversion_report.md").read_text(encoding="utf-8")
    assert "### [20] Sample - NOT CONVERTED" in report and "<Mode>Random</Mode>" in report
    assert "Query of `bronze_customer_master_v` (original):" in report and "select * from sap_data.Customer_Master_V" in report       # the source query, in full
    assert "- [42] Select: NOT CONVERTED - waits for [20] Sample" in report
    pipeline.convert_workflow(FIXTURES / "python_flow.yxmd", tmp_path / "p", "main", "s", python_mode=lambda tool, info: "keep")
    report = (tmp_path / "p" / "conversion_report.md").read_text(encoding="utf-8")
    assert "## 3b. Code outside the notebooks" in report and "### [17] JupyterCode - `utils/python_tool_17.py`" in report and "df['Amount2'] = df['Amount'] * 2" in report
