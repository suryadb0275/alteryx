"""The Data Cleansing tool (Alteryx's macro Cleanse.yxmc) is converted by rule: its check boxes are decoded from the macro's own question names."""
import json
import re

from conftest import FIXTURES
from alteryx_migrate import cli, engine, paths, pipeline
from test_interactive import Script, run

FLOW = FIXTURES / "cleanse_flow.yxmd"


def cells(path):
    return "\n".join("".join(c["source"]) for c in json.loads(path.read_text(encoding="utf-8"))["cells"])


def untyped_copy(tmp_path):
    """The fixture without its <MetaInfo>: the input query lists the columns but nothing says their types (as with most database inputs)."""
    text = re.sub(r"\s*<MetaInfo .*?</MetaInfo>", "", FLOW.read_text(encoding="utf-8"), flags=re.S)
    path = tmp_path / "cleanse_untyped.yxmd"
    path.write_text(text, encoding="utf-8")
    return path


def test_the_check_boxes_are_read_by_the_names_the_macro_gives_them():
    paths.use_skill()
    import wfmodel as M
    cfg = {"Value": [{"@name": "List Box (11)", "#text": '"A","B ""x"""'}, {"@name": "Check Box (15)", "#text": "True"},
                     {"@name": "Check Box (77)", "#text": "True"}, {"@name": "Drop Down (81)", "#text": "title"}, {"@name": "Check Box (84)", "#text": "False"}]}
    opts = M.cleanse_options(cfg)
    assert opts["fields"] == ["A", 'B "x"'] and opts["trim"] and opts["modify_case"] and opts["case"] == "title"
    assert not opts["blanks_for_null_strings"] and not opts["remove_null_rows"]
    assert M.cleanse_options({"Value": [{"@name": "Check Box (999)", "#text": "True"}]}) is None          # a setting the macro does not define: not guessed


def test_data_cleansing_is_converted_by_rule_in_pyspark_and_sql(tmp_path):
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s")
    assert result.unsupported == [] and not result.open_items
    code = cells(tmp_path / "03_gold.ipynb")
    assert "# [2] Data Cleansing" in code and "F.upper(F.col(_c)) for _c in ['Name', 'City']" in code
    assert "F.coalesce(F.col(_c), F.lit('')) for _c in ['Name', 'City']" in code and "F.coalesce(F.col(_c), F.lit(0)) for _c in ['Amount']" in code
    assert code.index("F.upper") < code.index("p{Punct}") < code.index("(?U)\\\\s+") < code.index("x{00A0}") < code.index("F.lit(0)")    # the macro's order
    assert "isNotNull()" in code and "remove_letters" not in code and "TODO" not in code
    sql = cells(tmp_path / "03_gold_sql.ipynb")
    assert "NO SQL" not in sql and "upper(`Name`)" in sql and "coalesce(`Amount`, 0)" in sql and "`Amount` IS NOT NULL" in sql


def test_without_types_pyspark_decides_at_run_time_and_the_answer_gives_sql(tmp_path):
    flow = untyped_copy(tmp_path)
    analysis, _ = engine.analyze(flow, tmp_path / "work")
    assert analysis.type_questions == [{"id": 2, "name": "Data Cleansing", "container": "(no container)", "fields": ["Name", "City", "Amount"]}]
    pipeline.convert_workflow(flow, tmp_path / "a", "main", "s")
    assert "{n.lower(): t for n, t in customers_cleansed.dtypes}" in cells(tmp_path / "a" / "03_gold.ipynb")                  # like the macro: by the real type, at run time
    assert "NO SQL" in cells(tmp_path / "a" / "03_gold_sql.ipynb")
    pipeline.convert_workflow(flow, tmp_path / "b", "main", "s", {"_field_types": {"Name": "string", "City": "string", "Amount": "number"}})
    assert "dtypes" not in cells(tmp_path / "b" / "03_gold.ipynb") and "upper(`Name`)" in cells(tmp_path / "b" / "03_gold_sql.ipynb")
    assert "Name = string" in (tmp_path / "b" / "conversion_report.md").read_text(encoding="utf-8")


def test_the_terminal_asks_the_missing_types_when_sql_is_wanted(tmp_path):
    flow = untyped_copy(tmp_path)
    script = Script("3", "main", "s", "", "", "1", "1", "2")     # both codes; catalog; schema; source catalog / schema; Name text, City text, Amount number
    assert run([str(flow), "--out", str(tmp_path / "out")], script) == cli.OK and not script.answers
    assert "What type is Amount?" in "\n".join(script.prompts + script.lines)
    assert "upper(`Name`)" in cells(tmp_path / "out" / "cleanse_untyped" / "03_gold_sql.ipynb")
