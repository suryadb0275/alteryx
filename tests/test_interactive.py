import json
from types import SimpleNamespace


from conftest import FIXTURES
from alteryx_migrate import ai, cli, interactive
from test_python_port import FakeModel


class Script:
    """Scripted terminal: answers are returned in order; every prompt and message is kept."""

    def __init__(self, *answers):
        self.answers, self.lines, self.prompts = list(answers), [], []

    def console(self):
        return interactive.Console(self.input, lambda text, **kw: self.lines.append(text))

    def input(self, prompt):
        self.prompts.append(prompt)
        if not self.answers:
            raise AssertionError(f"unexpected question: {prompt}")
        return self.answers.pop(0)


def run(args_list, script):
    args = cli.build_parser().parse_args(["convert", *args_list, "--interactive"])
    return cli.cmd_convert(args, script.console())


def cells(path):
    return "\n".join("".join(c["source"]) for c in json.loads(path.read_text(encoding="utf-8"))["cells"])


def test_the_converter_asks_which_code_where_and_which_row_then_converts(tmp_path):
    script = Script("1", "main", "s", "", "", "1")          # PySpark only; catalog main; schema s; source tables: same catalog, dataset as schema; Unique: any record
    code = run([str(FIXTURES / "unique_flow.yxmd"), "--out", str(tmp_path)], script)
    assert code == cli.OK and not script.answers
    asked = "\n".join(script.prompts + script.lines)
    assert "Which code do you need?" in asked and "Which catalog holds the tables" in asked and "which one is first" in asked and "In which Databricks catalog are those source tables?" in asked
    assert (tmp_path / "03_gold.ipynb").exists() and not (tmp_path / "03_gold_sql.ipynb").exists()                    # only the code that was asked for
    assert "CREATE OR REPLACE VIEW" in cells(tmp_path / "01_bronze.ipynb") and "main.sap_data.Customer_Master_V" in cells(tmp_path / "01_bronze.ipynb")
    assert "main.s.bronze_customer_master_v" in cells(tmp_path / "00_config.ipynb") and "dropDuplicates" in cells(tmp_path / "03_gold.ipynb")


def test_a_tool_without_a_converter_is_described_in_english_and_the_words_reach_the_model(tmp_path, monkeypatch):
    prompts = []

    class Writer:
        model = "fake-model"

        def complete(self, system, user):
            prompts.append(user)
            return "```python\ndf = spark.table(INPUTS['bronze_customer_master_v']).limit(10)\nwrite_output(df, 'gold_customer_master_v_selected')\n```"
    monkeypatch.setattr(ai, "client_from", lambda *a, **k: Writer())
    script = Script("3", "", "", "", "keep only the first ten rows of each customer", "fake-model")      # both codes; no catalog; the Sample tool; the model
    run([str(FIXTURES / "sample_flow.yxmd"), "--out", str(tmp_path)], script)
    assert "What should this tool do in this workflow?" in "\n".join(script.prompts)
    assert prompts and "keep only the first ten rows of each customer" in prompts[0] and "what_the_user_says_each_tool_must_do" in prompts[0]
    gold = cells(tmp_path / "03_gold.ipynb")
    assert "AI-WRITTEN by the model `fake-model`" in gold and ".limit(10)" in gold


def test_without_a_model_the_words_stay_in_the_hand_off_for_a_person_or_another_ai(tmp_path):
    script = Script("1", "", "", "", "keep only the first ten rows of each customer", "")                 # no model
    run([str(FIXTURES / "sample_flow.yxmd"), "--out", str(tmp_path)], script)
    gold = cells(tmp_path / "03_gold.ipynb")
    assert "UPDATE REQUIRED - tool [20] Sample" in gold and "What the user asked for:" in gold and "keep only the first ten rows of each customer" in gold


def test_an_embedded_python_script_is_ported_after_the_user_agrees_and_describes_it(tmp_path, monkeypatch):
    fake = FakeModel(question=True)
    monkeypatch.setattr(ai, "client_from", lambda *a, **k: fake)
    script = Script("3", "", "", "", "2", "sales are doubled", "fake-model", "2")                        # both; no catalog; python: option 2 (port with the model); description; model; the model's question: option 2
    code = run([str(FIXTURES / "python_flow.yxmd"), "--out", str(tmp_path)], script)
    text = "\n".join(script.prompts + script.lines)
    assert "What should I do with it?" in text and "Keep the pandas code as it is" in text and "Should the threshold 10 be inclusive" in text
    assert "sales are doubled" in fake.prompts[0] and "inclusive" in fake.prompts[1]
    silver = cells(tmp_path / "02_silver.ipynb")
    assert "from utils import python_tool_17" in silver and "UPDATE REQUIRED" not in silver
    component = (tmp_path / "utils" / "python_tool_17.py").read_text(encoding="utf-8")
    assert "AI-WRITTEN" in component and "df.withColumn('Amount2'" in component
    assert code == cli.OK or code == cli.OPEN


def test_yes_asks_nothing(tmp_path):
    def never(prompt):
        raise AssertionError("no question expected")
    args = cli.build_parser().parse_args(["convert", str(FIXTURES / "unique_flow.yxmd"), "--out", str(tmp_path), "--yes", "--decisions", str(tmp_path / "d.json")])
    (tmp_path / "d.json").write_text('{"20": {"mode": "any"}}', encoding="utf-8")
    assert cli.cmd_convert(args, interactive.Console(never, lambda *a, **k: None)) == cli.OK and (tmp_path / "03_gold.ipynb").exists()


def test_a_missing_macro_is_asked_about_and_its_description_goes_to_the_tool():
    analysis = SimpleNamespace(file_name="w.yxmd", tool_count=3, chunks=[], macros_missing=["cleanse.yxmc"], inputs={},
                               unsupported=[{"id": 5, "name": "Macro", "plugin": "macro:cleanse.yxmc", "container": "c", "status": "cannot be converted", "reason": "No converter for tool type: Macro", "decision": "",
                                             "after": "[1]", "before": "[2]"}])
    script = Script("3", "main", "s", "it trims text and upper-cases the Name column", "")        # code; catalog; schema; macro description; model: none
    args = SimpleNamespace(format=None, catalog="", schema="", model=None)
    plan = interactive.gather(script.console(), analysis, {}, args)
    assert plan.tool_notes == {5: "it trims text and upper-cases the Name column"} and "The workflow uses the macro 'cleanse.yxmc'" in "\n".join(script.lines)
    assert plan.catalog == "main" and plan.schema == "s" and plan.formats == ("pyspark", "sql") and plan.model == ""


def test_the_original_pandas_code_is_kept_in_a_component_file_without_any_model(tmp_path):
    script = Script("3", "", "", "", "1")                                      # both; no catalog; python: option 1 (keep the pandas code)
    code = run([str(FIXTURES / "python_flow.yxmd"), "--out", str(tmp_path)], script)
    assert not script.answers and code in (cli.OK, cli.OPEN)
    component = (tmp_path / "utils" / "python_tool_17.py").read_text(encoding="utf-8")
    assert "df['Amount2'] = df['Amount'] * 2" in component and "df_big = df[df['Amount'] > 10]" in component and "def run(spark, input_1):" in component   # the author's lines, unchanged
    silver = cells(tmp_path / "02_silver.ipynb")
    assert "from utils import python_tool_17" in silver and "python_tool_17.run(spark, bronze_sales_v)" in silver and "UPDATE REQUIRED" not in silver
