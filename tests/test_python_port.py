import json
import re


from conftest import FIXTURES
from alteryx_migrate import engine, pipeline, pytool

FLOW = FIXTURES / "python_flow.yxmd"


class FakeModel:
    """A stand-in for the Databricks model: answers each batch of cells from the cell text, and may raise a question first."""
    model = "fake-model"

    def __init__(self, bad_first=False, question=False):
        self.prompts, self.bad_first, self.question, self.calls = [], bad_first, question, 0

    def complete(self, system, user):
        self.calls += 1
        self.prompts.append(user)
        body = json.loads(re.search(r"```json\n(.*)\n```", user, flags=re.S).group(1))
        if "answers_given_by_the_user" not in body:                  # the model is also asked to write whole tables: it has nothing usable to say
            return "sorry, I cannot write that"
        answered = body["answers_given_by_the_user"]
        cells, questions = [], []
        for c in body["cells"]:
            t = c["text"]
            note = ""
            if "import pandas" in t:
                code, note = "", "imports are not needed in PySpark"
            elif 'Alteryx.read("#1")' in t:
                code = "df = <<IN:1>>"
            elif "['Amount2']" in t:
                code = "df = df.withColumn('Amount2', F.col('Amount') * 2)"
            elif "df_big =" in t:
                if self.question and "q-threshold" not in answered:
                    questions.append({"id": "q-threshold", "text": "Should the threshold 10 be inclusive (>=) as in pandas '>'?", "options": ["exclusive", "inclusive"], "default": "", "blocking": True, "cells": [c["cell"]]})
                    code = ""
                else:
                    code = "df_big = df.filter(F.col('Amount') > 10)"
            elif "Alteryx.write(df, 1)" in t:
                code = "<<OUT:1>> = df"
            elif "Alteryx.write(df_big, 2)" in t:
                code = "<<OUT:2>> = df_big"
            else:
                code = ""
            if self.bad_first and self.calls == 1 and "Amount2" in t:
                code = "import pandas as pd\ndf = df.toPandas()"
            cells.append({"cell": c["cell"], "code": code, "defines": [{"name": "df", "columns": None}], "notes": note})
        return json.dumps({"cells": cells, "questions": questions})


def info():
    a, h = engine.analyze(FLOW, __import__("pathlib").Path(__import__("tempfile").mkdtemp()))
    return pytool.load(h["slim_dir"], h["wf"])[17]


def test_the_script_is_split_into_cells_with_a_slice_per_output():
    i = info()
    assert [c.index for c in i.cells] == [1, 2, 3, 4, 5, 6] and i.slices[1] == [1, 2, 3, 5] and i.slices[2] == [1, 2, 3, 4, 6]
    assert i.inputs[1]["index"] == 0 and i.inputs[1]["verified"] and i.anchors == {1: "Output1", 2: "Output2"}


def test_cells_are_ported_checked_and_assembled_with_placeholders_resolved():
    model = FakeModel()
    out = pytool.port(model, info(), "sales doubled")
    assert out.ok and out.calls == 1 and sorted(out.cells) == [1, 2, 3, 4, 5, 6]
    assert "def run(spark, input_1):" in out.module and "df = input_1" in out.module and "output_1 = df" in out.module and "return output_1, output_2" in out.module and "<<" not in out.module
    assert out.module_name == "utils/python_tool_17.py" and "{out_Output1}, {out_Output2} = python_tool_17.run(spark, {in0})" in out.code      # the notebook only calls the component
    inline = pytool.port(FakeModel(), info(), components=False)
    assert "{out_Output1} = df" in inline.code and "{in0}" in inline.code and "{out_Output2} = df_big" in inline.code and "<<" not in inline.code and not inline.module
    assert "sales doubled" in model.prompts[0] and "references/10" not in model.prompts[0]


def test_code_that_breaks_the_rules_is_sent_back_once_then_accepted():
    model = FakeModel(bad_first=True)
    out = pytool.port(model, info())
    assert out.ok and out.calls == 2 and "pandas/numpy are not allowed" in model.prompts[1] and "toPandas" not in out.code


def test_a_question_is_put_to_the_user_and_the_answer_goes_back_to_the_model():
    model, asked = FakeModel(question=True), []
    out = pytool.port(model, info(), ask=lambda q: asked.append(q["text"]) or "inclusive")
    assert out.ok and asked == ["Should the threshold 10 be inclusive (>=) as in pandas '>'?"] and out.calls == 2
    assert "q-threshold" in model.prompts[1] and "inclusive" in model.prompts[1]


def test_an_unanswered_blocking_question_leaves_the_tool_unported():
    out = pytool.port(FakeModel(question=True), info(), ask=None)
    assert not out.ok and not out.code and out.unanswered and 4 in out.failed and "waits for an answer" in out.failed[4]


def test_the_pipeline_uses_the_port_so_the_tools_after_the_python_tool_convert_by_rule(tmp_path):
    result = pipeline.convert_workflow(FLOW, tmp_path, model=FakeModel(), python_port=lambda tool, i: True)
    gold = "\n".join("".join(c["source"]) for c in json.loads((tmp_path / "02_silver.ipynb").read_text(encoding="utf-8"))["cells"])
    gold += "\n".join("".join(c["source"]) for c in json.loads((tmp_path / "03_gold.ipynb").read_text(encoding="utf-8"))["cells"])
    module = (tmp_path / "utils" / "python_tool_17.py").read_text(encoding="utf-8")                                          # the ported script is a component file next to the notebooks
    assert "df = df.withColumn('Amount2', F.col('Amount') * 2)" in module and "F.col('Amount') > 10" in module and "utils/python_tool_17.py" in result.files and "utils/__init__.py" in result.files
    assert "AI-WRITTEN" in module and "by the model `fake-model`" in module                                           # flagged for review
    assert "from utils import python_tool_17" in gold and "python_tool_17.run(spark, bronze_sales_v)" in gold and "df_big" not in gold      # the notebook only calls it
    assert "The steps are the same as in the cell for `silver_sales_v_jupytercode` above" in gold                       # the call is shown once; the second output only repeats its write
    assert "Python tool [17]" in (tmp_path / "conversion_report.md").read_text(encoding="utf-8")
    assert "UPDATE REQUIRED" not in gold                                                                              # nothing left waiting
    assert not result.unsupported or all("output" in u["reason"].lower() for u in result.unsupported)
    assert json.loads((tmp_path / "python_port.json").read_text(encoding="utf-8"))["17"]["ok"] is True


def test_without_approval_or_when_the_port_fails_the_hand_off_stays(tmp_path):
    result = pipeline.convert_workflow(FLOW, tmp_path / "a", model=FakeModel(), python_port=lambda tool, i: False)
    assert result.unsupported and not (tmp_path / "a" / "python_port.json").exists()
    bad = FakeModel(question=True)
    result = pipeline.convert_workflow(FLOW, tmp_path / "b", model=bad, python_port=lambda tool, i: True)          # a blocking question nobody answers
    report = json.loads((tmp_path / "b" / "python_port.json").read_text(encoding="utf-8"))["17"]
    assert report["ok"] is False and report["questions_without_answer"] and result.unsupported


def test_reads_and_writes_must_go_through_the_placeholders():
    i = info()
    assert any("write <<IN:1>>" in p for p in pytool.check_cell('df = spark.table("output_of_tool_25")', i.cell(2)))
    assert pytool.check_cell("df = <<IN:1>>", i.cell(2)) == []
    assert any("saveAsTable" in p or "<<OUT:1>>" in p for p in pytool.check_cell('df.write.format("delta").saveAsTable("out1")', i.cell(5)))
    assert pytool.check_cell("<<OUT:1>> = df", i.cell(5)) == []


def test_the_component_file_runs_and_returns_the_outputs_the_next_tools_read(spark, tmp_path):
    import importlib.util
    pipeline.convert_workflow(FLOW, tmp_path, model=FakeModel(), python_port=lambda tool, i: True)
    spec = importlib.util.spec_from_file_location("python_tool_17", tmp_path / "utils" / "python_tool_17.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    df = spark.createDataFrame([("a", 5.0), ("b", 20.0)], "Region string, Amount double")
    out1, out2 = module.run(spark, df)
    assert sorted(tuple(r) for r in out1.collect()) == [("a", 5.0, 10.0), ("b", 20.0, 40.0)]            # Output1: with Amount2 = Amount * 2
    assert sorted(tuple(r) for r in out2.collect()) == [("b", 20.0, 40.0)]                              # Output2: Amount > 10


def test_an_output_nobody_reads_does_not_break_the_inline_form():
    i = info()
    i.consumed = [1]                                                                                   # nothing reads output 2
    text = pytool.assemble(i, {2: "df = <<IN:1>>", 6: "<<OUT:2>> = df"}, {}, "m")
    assert "_unused_output_2 = df" in text and "{out_Output2}" not in text


# ---------------------------------------------------------------------------------------------------------------- keeping the original pandas code
def test_only_the_alteryx_plumbing_is_rewritten():
    packages = []
    text, problems = pytool.rewrite_cell('from ayx import Alteryx, Package\nPackage.installPackages(["scipy"])\nnaam = "æøå"\ndf = Alteryx.read("#2")\nAlteryx.write(df.head(3), 4)', packages)
    assert problems == [] and packages == ["scipy"]
    assert "input_2.copy()" in text and "output_4 = df.head(3)" in text and "naam = \"æøå\"" in text and "Alteryx" not in text.replace("Alteryx plumbing", "")
    assert pytool.rewrite_cell("m = Alteryx.readMetadata('x')", [])[1] and pytool.rewrite_cell("df = Alteryx.read(name)", [])[1]


def test_keeping_the_pandas_script_needs_no_model_and_the_component_gives_the_same_rows_as_pandas(spark, tmp_path):
    result = pipeline.convert_workflow(FLOW, tmp_path, python_mode=lambda tool, i: "keep")
    assert not result.unsupported and (tmp_path / "utils" / "python_tool_17.py").exists()
    report = json.loads((tmp_path / "python_port.json").read_text(encoding="utf-8"))["17"]
    assert report["mode"] == "keep" and report["ok"] is True
    import importlib.util
    spec = importlib.util.spec_from_file_location("python_tool_17", tmp_path / "utils" / "python_tool_17.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    df = spark.createDataFrame([("a", 5.0), ("b", 20.0), ("c", None)], "Region string, Amount double")
    out1, out2 = module.run(spark, df)
    rows1 = {r["Region"]: (r["Amount"], r["Amount2"]) for r in out1.collect()}
    assert rows1["a"] == (5.0, 10.0) and rows1["b"] == (20.0, 40.0) and rows1["c"] == (None, None)       # pandas NaN comes back as NULL
    assert [tuple(r) for r in out2.collect()] == [("b", 20.0, 40.0)]
    notebook = "\n".join("".join(c["source"]) for c in json.loads((tmp_path / "02_silver.ipynb").read_text(encoding="utf-8"))["cells"])
    assert "python_tool_17.run(spark, bronze_sales_v)" in notebook and "toPandas" not in notebook             # the notebook stays plain PySpark; the pandas lives in the component


def test_a_script_that_uses_alteryx_features_with_no_counterpart_is_not_kept(tmp_path):
    flow = (FIXTURES / "python_flow.yxmd").read_bytes().replace(b'df = Alteryx.read("#1")', b'df = Alteryx.read("#1")\nmeta = Alteryx.readMetadata("#1")')
    (tmp_path / "w.yxmd").write_bytes(flow)
    result = pipeline.convert_workflow(tmp_path / "w.yxmd", tmp_path / "o", python_mode=lambda tool, i: "keep")
    report = json.loads((tmp_path / "o" / "python_port.json").read_text(encoding="utf-8"))["17"]
    assert report["ok"] is False and "readMetadata" in json.dumps(report["failed"]) and result.unsupported
