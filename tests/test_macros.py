"""Alteryx's own Imputation macro and the Join Multiple tool are converted by rule, from the names and settings the workflow carries."""
import json
import re

from conftest import FIXTURES
from alteryx_migrate import cli, engine, paths, pipeline
from test_interactive import Script, run

IMPUTE = FIXTURES / "impute_flow.yxmd"
JOINMULTI = FIXTURES / "joinmulti_flow.yxmd"


def cells(path):
    return "\n".join("".join(c["source"]) for c in json.loads(path.read_text(encoding="utf-8"))["cells"])


def test_the_imputation_options_are_read_by_the_names_the_macro_gives_them():
    paths.use_skill()
    import wfmodel as M
    base = {"listbox Select Incoming Fields": '"A","B"', "radio Null Value": "True", "radio User Specified Replace From Value": "False", "updown User Replace Value": "0.0",
            "radio Mean": "False", "radio Median": "True", "radio Mode": "False", "radio User Specified Replace With Value": "False", "updown User Replace With Value": "0.0",
            "checkbox Impute Indicator": "True", "checkbox Imputed Values Separate Field": "False"}

    def cfg(**changes):
        return {"Value": [{"@name": k, "#text": v} for k, v in {**base, **changes}.items()]}

    opts = M.imputation_options(cfg())
    assert opts["fields"] == ["A", "B"] and opts["from_null"] and opts["to_median"] and opts["indicator"] and not opts["separate"]
    assert M.imputation_options(cfg(**{"radio Mean": "True"})) is None                 # two replacements chosen: not guessed
    assert M.imputation_options({"Value": [{"@name": "radio Something Else", "#text": "True"}]}) is None


def test_imputation_is_converted_by_rule_in_pyspark_and_sql(tmp_path):
    result = pipeline.convert_workflow(IMPUTE, tmp_path, "main", "s")
    assert result.unsupported == [] and not result.open_items
    code = cells(tmp_path / "03_gold.ipynb")
    assert "F.avg(F.when(F.col('Price').isNotNull(), F.col('Price'))).alias('_imp_Price')" in code and "crossJoin" in code
    assert "'Price': F.when(F.col('Price').isNull(), F.col('_imp_Price')).otherwise(F.col('Price'))" in code
    assert "'Price_Indicator': F.when(F.col('Price').isNull(), F.lit(1)).otherwise(F.lit(0))" in code and "TODO" not in code
    sql = cells(tmp_path / "03_gold_sql.ipynb")
    assert "NO SQL" not in sql and "avg(" in sql.lower() and "Price_Indicator" in sql


def test_join_multiple_names_the_inputs_by_the_fields_they_list_not_by_file_order(tmp_path):
    result = pipeline.convert_workflow(JOINMULTI, tmp_path, "main", "s")
    assert result.unsupported == [] and not result.open_items
    code = cells(tmp_path / "03_gold.ipynb")
    left, right = re.search(r"(\w+)\s*=\s*(\w+)\.select\(\*\[F\.col\(f'`\{c\}`'\)\.alias\(f'Input_#1_", code), re.search(r"alias\(f'Input_#3_", code)
    assert left and right and "bronze_left_table" in code.split("Input_#1_")[0].split("\n")[-1]       # #1 lists K and A: the Left table, although it is the second connection
    assert "'full'" in code and "F.col('`Input_#1_K`') == F.col('`Input_#3_K`')" in code            # Output Join Only is off: a full outer join on the keys
    assert ".alias('Key')" in code and "Input_#3_K`'" in code and "Input_#3_B" in code               # the tool's own select: rename, deselect, keep
    sql = cells(tmp_path / "03_gold_sql.ipynb")
    assert "NO SQL" not in sql and "FULL" in sql.upper() and "`Input_#1_K` AS `Key`" in sql.replace("\n", " ")


def test_an_unclear_join_multiple_input_is_asked_and_the_answer_applies(tmp_path):
    flow = tmp_path / "joinmulti_untyped.yxmd"                           # without the inputs' <MetaInfo> nothing says which tool carries which fields
    text = re.sub(r"\s*<MetaInfo .*?</MetaInfo>", "", JOINMULTI.read_text(encoding="utf-8"), flags=re.S)
    flow.write_text(re.sub(r"select K, \w from", "select * from", text), encoding="utf-8")        # and `select *` lists no columns either
    analysis, _ = engine.analyze(flow, tmp_path / "work")
    question, = analysis.join_questions
    assert question["id"] == 3 and question["slots"] == ["#1", "#3"] and sorted(t["id"] for t in question["tools"]) == [1, 2]
    assert [u["id"] for u in pipeline.convert_workflow(flow, tmp_path / "a", "main", "s").unsupported] == [3]      # open until it is answered
    done = pipeline.convert_workflow(flow, tmp_path / "b", "main", "s", {"_join_inputs": {"3": {"#1": 1, "#3": 2}}})
    assert done.unsupported == [] and "Input_#1_" in cells(tmp_path / "b" / "03_gold.ipynb")
    assert "input #1 comes from tool [1]" in (tmp_path / "b" / "conversion_report.md").read_text(encoding="utf-8")
    first = [t["id"] for t in question["tools"]].index(1) + 1               # the position of tool [1] in the list the terminal shows
    script = Script("2", "main", "s", "", "", str(first), "1")           # PySpark; catalog; schema; source catalog / schema; #1 from tool [1]; #3 from the one left
    assert run([str(flow), "--out", str(tmp_path / "out")], script) == cli.OK and not script.answers
    assert any("Which tool feeds input #1?" in p for p in script.prompts + script.lines)


def test_an_imputation_with_no_field_selected_passes_the_records_through(tmp_path):
    flow = tmp_path / "impute_none.yxmd"
    flow.write_text(IMPUTE.read_text(encoding="utf-8").replace('"Price","Qty"', '""'), encoding="utf-8")
    result = pipeline.convert_workflow(flow, tmp_path / "out", "main", "s")
    assert result.unsupported == [] and "F.when(" not in cells(tmp_path / "out" / "03_gold.ipynb")
