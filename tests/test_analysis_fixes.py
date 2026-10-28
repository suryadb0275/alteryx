"""The converter bugs listed in alteryx_conversion_master.csv (BUG-01 ... BUG-16), each with the real settings of the tool it was found in.

analysis_fixes_flow.yxmd holds one small branch per rule; its sources are Text Inputs, so the generated notebooks - PySpark and Spark SQL - run as they are on a
local Spark and the rows are checked, not only the code text."""
import json
import re

import pytest

from conftest import FIXTURES
from alteryx_migrate import paths, pipeline, sqlsrc

paths.use_skill()
import alteryx_expr as X  # noqa: E402 - the skill's expression translator

FLOW = FIXTURES / "analysis_fixes_flow.yxmd"
ANSWERS = {"21": {"mode": "order_by", "fields": ["Seq"]}}          # the Multi-Row Formula's order (the workflow sets none)


def cells(path, kind="code"):
    return ["".join(c["source"]) for c in json.loads(path.read_text(encoding="utf-8"))["cells"] if c["cell_type"] == kind]


def block(text, tool):
    m = re.search(rf"^# \[{tool}\] .*?(?=^# \[\d+\] |^write_output|\Z)", text, re.M | re.S)
    return m.group(0) if m else ""


@pytest.fixture(scope="module")
def converted(tmp_path_factory):
    out = tmp_path_factory.mktemp("fixes")
    result = pipeline.convert_workflow(FLOW, out, "", "", ANSWERS)
    return out, result


def run_pyspark(spark, folder):
    """Run 03_gold as it is; write_output keeps each result instead of writing a Delta table."""
    written = {}
    ns = {"spark": spark, "OUTPUTS": {}, "INPUTS": {}, "write_output": lambda df, key: written.__setitem__(key, df)}
    for code in cells(folder / "03_gold.ipynb"):
        if not code.startswith("%"):
            exec(code, ns)                                           # noqa: S102 - the converter's own output
    return written


def run_sql(spark, folder):
    out = {}
    for code in cells(folder / "03_gold_sql.ipynb"):
        m = re.search(r"CREATE TABLE (\w+) AS\n", code)
        if m:
            out[m.group(1)] = spark.sql(code[m.end():])
    return out


def rows(df, *cols):
    return sorted(tuple(r[c] for c in cols) for r in df.collect())


def test_every_tool_of_the_fixture_converts_by_rule(converted):
    out, result = converted
    assert result.unsupported == [] and not result.open_items
    assert not any("NO SQL" in c for c in cells(out / "03_gold_sql.ipynb"))           # the new forms all have a Spark SQL form


def test_missing_fields_are_skipped_as_alteryx_does_bug_01(converted):
    gold = "\n".join(cells(converted[0] / "03_gold.ipynb"))
    assert "*([F.col('BSEG_WRBTR')] if 'bseg_wrbtr' in" in block(gold, 5)                  # CF_MasterData [10]
    assert "next((c for k in (n, n.removeprefix('Right_')) for c in right_cols if c.lower() == k.lower()), None)" in block(gold, 12)   # ONB_PL [23]


def test_datetimetrim_month_truncates_and_accepts_the_plural_bug_02():
    for part, code in (("month", "F.trunc("), ("months", "F.trunc("), ("year", "'year')"), ("day", "F.date_trunc('day'"), ("hours", "F.date_trunc('hour'"),
                       ("lastofmonth", "23:59:59")):
        res = X.translate(f'DateTimeTrim([D], "{part}")', field_types={"D": "date"})
        assert code in res.code, (part, res.code)
    assert "'MM')" not in X.translate('DateTimeTrim([D], "month")').code


def test_text_to_columns_uses_the_workflow_delimiter_bug_03(converted, tmp_path):
    gold = "\n".join(cells(converted[0] / "03_gold.ipynb"))
    assert "F.split(F.col('Code').cast('string'), '\\\\-', 2)" in block(gold, 2)           # HR_CTC_MART [16]: '-', extra parts stay in the last column
    no_delim = tmp_path / "no_delimiter.yxmd"
    no_delim.write_text(FLOW.read_text(encoding="utf-8").replace('<Delimeters value="-" />', ""), encoding="utf-8")
    result = pipeline.convert_workflow(no_delim, tmp_path / "out", "", "", ANSWERS)
    assert [u["id"] for u in result.unsupported] == [2] and "delimiter" in result.unsupported[0]["reason"]      # asked, never ','


def test_join_outputs_plain_names_and_honours_unknown_off_bug_05(converted):
    gold = "\n".join(cells(converted[0] / "03_gold.ipynb"))
    assert "('Left_K', 'K'), ('Left_X', 'X')" in block(gold, 12)                          # HR_CTC_MART [7]: no Left_ prefix
    assert "('Right_X', 'Right_X')" in block(gold, 12)                                    # WalujGlass [46]: Alteryx wrote rename="Right_X"
    assert "[(c, c) for c in left_cols" not in block(gold, 12)                            # '*Unknown' off: no other field


def test_simple_filter_compares_a_text_field_with_text_bug_13(converted):
    assert "F.col('KPI') != '0'" in block("\n".join(cells(converted[0] / "03_gold.ipynb")), 4)       # ONBItaly [29]


def test_the_notebook_imports_stringtype_when_a_plus_needs_it_bug_08(tmp_path):
    flow = tmp_path / "plus.yxmd"
    flow.write_text(FLOW.read_text(encoding="utf-8").replace('expression="&quot;first&quot;"', 'expression="[K] + [X]"'), encoding="utf-8")
    pipeline.convert_workflow(flow, tmp_path / "out", "", "", ANSWERS)
    gold = "\n".join(cells(tmp_path / "out" / "03_gold.ipynb"))
    assert "StringType" not in gold or "from pyspark.sql.types import StringType" in gold


def test_block_until_done_second_output_bug_11(converted):
    gold = "\n".join(cells(converted[0] / "03_gold.ipynb"))
    assert "k_x_lookup.withColumn('Branch', F.lit('second'))" in gold                    # Output2 is the same data as Output


def test_record_id_macro_removed_downstream_and_datetime_now_bug_14(converted):
    gold = "\n".join(cells(converted[0] / "03_gold.ipynb"))
    assert "row_number" not in gold.split("# [21]")[1].split("write_output")[0]           # ONBItaly [160]: removed after a Filter, so never computed
    assert "spark.range(1).select(F.date_format(F.current_timestamp(), 'yyyy-MM-dd HH:mm:ss').alias('DateTimeNow'))" in block(gold, 30)
    assert "_carry = " in block(gold, 21)                                                 # Production Data [46]


def test_the_generated_pyspark_and_sql_give_the_alteryx_rows(spark, converted):
    out = converted[0]
    for result in (run_pyspark(spark, out), run_sql(spark, out)):
        a = result["gold_id_code_lookup_selected"]
        assert a.columns == ["ID", "Code1", "Code2", "Month"]                             # BSEG_WRBTR is not in the data: left out
        assert rows(a, "ID", "Code1", "Code2", "Month") == [("1", "a", "b-c", __import__("datetime").date(2024, 3, 1)),
                                                             ("3", "p", None, __import__("datetime").date(2023, 1, 1))]   # KPI '0' filtered out
        j = result["gold_k_x_lookup_selected"]
        assert j.columns == ["K", "X", "Right_X", "Y"] and rows(j, "K", "X", "Right_X", "Y") == [("k1", "left1", "right1", "y1")]
        c = result["gold_g_seq_lookup_selected"]
        assert "RecordID" not in c.columns
        # IF Last > Day THEN Plan ELSEIF Last = Day THEN [Row-1:Plan] ELSE 0: a carried row repeats the last row that was not carried; the first row has
        # no previous row, so 'other rows' = Empty gives 0
        assert rows(c, "G", "Seq", "Plan") == [("a", 1, 0.0), ("a", 2, 5.0), ("a", 3, 5.0), ("a", 4, 5.0), ("a", 5, 0.0), ("b", 1, 6.0)]
        now = result["gold_datetimenow_lookup_selected"].collect()
        assert len(now) == 1 and re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", now[0]["DateTimeNow"])
        assert rows(result["gold_k_x_lookup_with_branch_2"], "K", "Branch") == [("k1", "second"), ("k2", "second")]


# ---------------------------------------------------------------------------------------------------------------- input queries (sqlsrc)
MAP = sqlsrc.table_mapper(catalog="c", schema="s")


def test_recursive_cte_keeps_recursive_bug_06():
    t = sqlsrc.translate("WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT * FROM c", "bigquery", MAP)
    assert t.sql.startswith("WITH RECURSIVE c AS") and any("Runtime 17" in n for n in t.notes)


def test_a_repeated_column_is_renamed_like_bigquery_bug_07():
    t = sqlsrc.translate("SELECT X.*, c.VBELN FROM (SELECT b.VBELN, b.MATNR FROM d.B b) X JOIN d.C c ON X.VBELN = c.VBELN", "bigquery", MAP)      # GM_Landing [32]
    assert "c.VBELN AS VBELN_1" in t.sql and t.columns == ["VBELN", "MATNR", "VBELN_1"]


def test_unnamed_columns_get_bigquerys_names_bug_10():
    t = sqlsrc.translate("SELECT ROUND(SUM(PREFWT)/1000,5), DATE_TRUNC(date(EDATE), month), WERKS FROM p.d.T GROUP BY 2, 3", "bigquery", MAP)   # WalujGlass [76]
    assert t.columns == ["f0_", "f1_", "WERKS"] and "AS f0_" in t.sql and "AS f1_" in t.sql


def test_the_translated_names_run_on_spark(spark):
    t = sqlsrc.translate("SELECT a.k, b.k, ROUND(a.v / 2, 1) FROM (SELECT 1 AS k, 3.0 AS v) a JOIN (SELECT 1 AS k) b ON a.k = b.k", "bigquery", MAP)
    assert spark.sql(t.sql).columns == ["k", "k_1", "f0_"]


# ---------------------------------------------------------------------------------------------------------------- macro answers (BUG-15) and the option for BUG-16
def test_a_macro_answer_applies_only_to_the_tools_it_was_given_for_bug_15():
    lines = pipeline.decision_lines({"_macros": {"hr_googlesheet_macro.yxmc": {"as_source": True, "table": "google.sheets.raw", "tools": [136]}}})
    assert lines == ["Macro hr_googlesheet_macro.yxmc (tool [136]): only reads data, which is in the table google.sheets.raw."]


def test_null_string_as_empty_is_an_option_off_by_default_bug_16():
    assert X.translate("[A] != 'x'", field_types={"A": "string"}).code == "(F.col('A') != 'x')"
    assert X.translate("[A] != 'x'", field_types={"A": "string"}, null_as_empty=True).code == "(F.coalesce(F.col('A'), F.lit('')) != 'x')"


def test_a_macro_with_a_data_input_is_not_offered_as_a_data_source_bug_15(tmp_path):
    from test_interactive import Script, run
    from alteryx_migrate import cli
    flow = tmp_path / "custom_macro.yxmd"                         # the Record ID macro renamed: a macro the converter has no rule for, with data flowing into it
    flow.write_text(FLOW.read_text(encoding="utf-8").replace("CReW GroupByRecordID.yxmc", "custom_thing.yxmc"), encoding="utf-8")
    script = Script("", "1", "", "2", "Seq")                      # model: none; PySpark; no catalog; the macro: [1] describe [2] skip -> skip; the Multi-Row order
    assert run([str(flow), "--out", str(tmp_path / "out")], script) == cli.OPEN and not script.answers          # the skipped macro stays open
    shown = "\n".join(script.prompts + script.lines)
    assert "custom_thing.yxmc" in shown and "It only reads data" not in shown


def test_the_python_component_gets_dates_as_alteryx_read_gives_them_bug_04(spark, tmp_path):
    import datetime
    import importlib.util
    pipeline.convert_workflow(FIXTURES / "python_flow.yxmd", tmp_path, python_mode=lambda tool, i: "keep")
    spec = importlib.util.spec_from_file_location("python_tool_17", tmp_path / "utils" / "python_tool_17.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    df = spark.createDataFrame([("a", datetime.date(2024, 3, 1), "2024-03-02 10:00:00")], "Region string, D date, T string")
    pdf = module._to_pandas(df, ["T"])                             # T: a DateTime field the workflow carries as text
    assert str(pdf["D"].dtype).startswith("datetime64") and str(pdf["T"].dtype).startswith("datetime64")    # RM_Workflow [17]: compares GRN_Date with dt.today()
