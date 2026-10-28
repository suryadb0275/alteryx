"""Tools that depend on the order of the rows (Sample, Multi-Row Formula), Generate Rows that updates a field, and a macro that only reads data."""
import json

from conftest import FIXTURES
from alteryx_migrate import cli, engine, pipeline
from test_interactive import Script, run

FLOW = FIXTURES / "hr_patterns_flow.yxmd"
ORDER = {"mode": "order_by", "fields": ["EmployeeID", "EffectiveDate"]}
ANSWERS = {"2": {"mode": "any"}, "3": ORDER, "4": ORDER, "_macros": {"hr_googlesheet_macro.yxmc": {"as_source": True, "table": "google.sheets.raw"}}}


def code_of(folder):
    return "\n".join("".join(c["source"]) for f in sorted(folder.glob("0*.ipynb")) if "_sql" not in f.name
                     for c in json.loads(f.read_text(encoding="utf-8"))["cells"] if c["cell_type"] == "code")


def test_every_order_dependent_tool_is_asked_for_its_order_with_the_real_reason(tmp_path):
    analysis, _ = engine.analyze(FLOW, tmp_path / "work")
    asked = {u["id"]: u for u in analysis.unsupported}
    assert asked[2]["decision"] == "order" and asked[3]["decision"] == "order" and asked[4]["decision"] == "order"
    assert "record order" in asked[4]["reason"]                                    # not the review note that came before it
    assert 5 not in asked                                                          # Generate Rows updating a field converts by rule


def test_the_answers_convert_every_transformation_by_rule(tmp_path):
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s", ANSWERS)
    assert result.unsupported == [] and not result.open_items
    code = code_of(tmp_path)
    # Sample: first record of each group, any record accepted
    assert "dropDuplicates(['EmployeeID', 'EffectiveDate'])" in code
    # forward fill: the last non-NULL value before the row; 'other rows' = Empty gives '' for a text field
    assert "F.coalesce(F.last(F.col('Currency'), ignorenulls=True).over(" in code and "rowsBetween(Window.unboundedPreceding, Window.currentRow)), F.lit(''))" in code
    assert "orderBy(F.col('EmployeeID').asc_nulls_first(), F.col('EffectiveDate').asc_nulls_first())" in code
    # Row+1 with 'other rows' = NULL: lead() without a default
    assert "F.lead(F.col('EffectiveDate'), 1).over(" in code
    # Generate Rows overwrites EffectiveDate; a start past the end gives no rows
    assert "'EffectiveDate',\n    F.explode(F.when(" in code and ".otherwise(F.array())" in code and "interval 1 month" in code
    # the macro reads the table the user named
    config = (tmp_path / "00_config.ipynb").read_text(encoding="utf-8")
    assert "google.sheets.raw" in config
    report = (tmp_path / "conversion_report.md").read_text(encoding="utf-8")
    assert "only reads data, which is in the table google.sheets.raw" in report and "Tool [3]: rows are ordered by EmployeeID, EffectiveDate." in report


def test_a_descending_field_is_kept_per_field(tmp_path):
    answers = {**ANSWERS, "3": {"mode": "order_by", "fields": ["EmployeeID", "EffectiveDate"], "desc": ["EffectiveDate"]}}
    pipeline.convert_workflow(FLOW, tmp_path, "main", "s", answers)
    assert "orderBy(F.col('EmployeeID').asc_nulls_first(), F.col('EffectiveDate').desc_nulls_last())" in code_of(tmp_path)


def test_the_terminal_asks_the_order_and_what_the_macro_reads(tmp_path):
    script = Script("", "2", "main", "s", "", "",           # model: none; PySpark; catalog; schema; source catalog / schema
                    "1", "google.sheets.raw",               # the macro only reads data, from this table
                    "1",                                    # Sample [2]: any record
                    "EmployeeID, EffectiveDate",            # Multi-Row [3]: the order
                    "")                                     # Multi-Row [4]: Enter = the same order as before
    assert run([str(FLOW), "--out", str(tmp_path / "out")], script) == cli.OK and not script.answers
    shown = "\n".join(script.prompts + script.lines)
    assert "It only reads data" in shown and "reads the previous / next row" in shown and "[EmployeeID, EffectiveDate]" in shown
    assert "google.sheets.raw" in (tmp_path / "out" / "hr_patterns_flow" / "00_config.ipynb").read_text(encoding="utf-8")
