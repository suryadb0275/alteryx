import json

import pytest

from conftest import FIXTURES, ROOT
from alteryx_migrate import cli, pipeline, templates

FLOW = FIXTURES / "unique_flow.yxmd"
REVENUE = ROOT.parent / "Revenue.yxmd"


def test_a_workflow_becomes_notebooks_and_a_report(tmp_path):
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s")
    assert "00_config.ipynb" in result.files and "conversion_report.md" in result.files
    config = json.loads((tmp_path / "00_config.ipynb").read_text(encoding="utf-8"))
    assert any("main.s." in "".join(c["source"]) for c in config["cells"])           # the tables are named in the chosen catalog.schema
    report = (tmp_path / "conversion_report.md").read_text(encoding="utf-8")
    assert "converted, not yet validated" in report.lower()


def test_unconverted_tools_get_a_hand_off_not_code(tmp_path):
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s")
    assert result.unsupported, "the fixture has a Unique tool whose order is not defined"
    text = "".join("".join(c["source"]) for f in ("02_silver.ipynb", "03_gold.ipynb") if (tmp_path / f).exists()
                   for c in json.loads((tmp_path / f).read_text(encoding="utf-8"))["cells"])
    assert "UPDATE REQUIRED" in text and "Hand-off" in text and "TODO" not in text
    assert "key_fields" in text and "Place in the workflow" in text and "How to finish" in text      # the settings and the place are spelled out
    assert json.loads((tmp_path / "unconverted_tools.json").read_text(encoding="utf-8"))[0]["id"] == result.unsupported[0]["id"]


def test_a_decision_converts_the_waiting_tool(tmp_path):
    before = pipeline.convert_workflow(FLOW, tmp_path / "a", "main", "s")
    tool = str(before.unsupported[0]["id"])
    after = pipeline.convert_workflow(FLOW, tmp_path / "b", "main", "s", {tool: {"mode": "any"}})
    assert after.converted > before.converted and len(after.unsupported) < len(before.unsupported)
    assert "any row may be kept" in (tmp_path / "b" / "conversion_report.md").read_text(encoding="utf-8")


def test_the_cli_converts_a_folder_and_reports_what_is_open(tmp_path, capsys):
    folder = tmp_path / "volume"
    folder.mkdir()
    (folder / "flow.yxmd").write_bytes(FLOW.read_bytes())
    (folder / "notes.txt").write_text("ignored")
    code = cli.main(["convert", str(folder), "--out", str(tmp_path / "out"), "--catalog", "main", "--schema", "s"])
    assert code == cli.OPEN and (tmp_path / "out" / "flow" / "00_config.ipynb").exists()
    assert "need an update" in capsys.readouterr().out
    assert cli.main(["convert", str(tmp_path / "missing"), "--out", str(tmp_path), "--catalog", "c", "--schema", "s"]) == cli.FAILED
    assert cli.main(["convert", str(tmp_path / "out"), "--out", str(tmp_path), "--catalog", "c", "--schema", "s"]) == cli.FAILED     # no workflow in it


def test_notebook_text_round_trips():
    nb = templates.ipynb([templates.md("# T\n\nhi"), templates.code("%run ./00_config"), templates.code("x = 1\ny = 2")])
    assert templates.text_to_ipynb(templates.ipynb_to_text(nb)) == nb


@pytest.mark.skipif(not REVENUE.exists(), reason="Revenue.yxmd is not next to the repository")
def test_the_revenue_workflow_converts_without_error(tmp_path):
    result = pipeline.convert_workflow(REVENUE, tmp_path, "main", "s")
    assert result.chunks >= 15 and result.converted >= 15


def _cells(path):
    return "\n".join("".join(c["source"]) for c in json.loads(path.read_text(encoding="utf-8"))["cells"])


def test_spark_sql_notebooks_are_written_when_the_input_columns_are_known(tmp_path):
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s", {"20": {"mode": "any"}}, {"bronze_customer_master_v": {"Customer_Name": "string", "Region": "string", "Business": "string", "Amount": "double"}})
    assert "03_gold_sql.ipynb" in result.files
    text = _cells(tmp_path / "03_gold_sql.ipynb")
    assert "%sql" in text and "CREATE TABLE main.s.gold_customer_master_v_selected AS" in text and "main.s.bronze_customer_master_v" in text
    assert "@@" not in text and "-- [20] Unique" in text                                # real table names, one common table expression per Alteryx tool


def test_without_input_columns_the_sql_cell_says_why_and_points_to_the_pyspark(tmp_path):
    pipeline.convert_workflow(FLOW, tmp_path, "main", "s", {"20": {"mode": "any"}})
    text = _cells(tmp_path / "03_gold_sql.ipynb")
    assert "NO SQL FOR THIS TABLE" in text and "bronze_customer_master_v" in text and "03_gold.ipynb" in text and "schemas" in text


def test_input_columns_are_read_from_the_tables_when_a_lookup_is_given(tmp_path):
    asked = []

    def lookup(name):
        asked.append(name)
        return ["Customer_Name", "Region", "Business"]
    pipeline.convert_workflow(FLOW, tmp_path, "main", "s", {"20": {"mode": "any"}}, None, lookup)
    assert asked == ["main.s.bronze_customer_master_v"] and "CREATE TABLE" in _cells(tmp_path / "03_gold_sql.ipynb")


def test_a_waiting_table_gets_a_sql_hand_off_as_comments(tmp_path):
    pipeline.convert_workflow(FLOW, tmp_path, "main", "s")
    text = _cells(tmp_path / "03_gold_sql.ipynb")
    assert "-- UPDATE REQUIRED - tool [20] Unique" in text and "-- Hand-off" in text and "#" not in text.split("%sql", 1)[1]


def test_the_hand_off_carries_the_original_xml_and_the_replication_recipe(tmp_path):
    pipeline.convert_workflow(FLOW, tmp_path, "main", "s")
    text = _cells(tmp_path / "03_gold.ipynb")
    assert "Original Alteryx settings (XML from the workflow file" in text and "<Configuration" in text and "plugin: AlteryxBasePluginsGui.Unique.Unique" in text
    assert "How to replicate it (from the Alteryx help" in text and "What Unique does:" in text and "Window.partitionBy(*KEYS).orderBy(*ORDER)" in text
    sql = _cells(tmp_path / "03_gold_sql.ipynb")
    assert "Original Alteryx settings (XML" in sql and "How to replicate it" in sql and "--     What Unique does" in sql


def test_without_a_catalog_or_schema_the_table_names_are_not_qualified(tmp_path, capsys):
    schemas = {"bronze_customer_master_v": ["Customer_Name", "Region", "Business"]}
    pipeline.convert_workflow(FLOW, tmp_path, decisions={"20": {"mode": "any"}}, schemas=schemas)
    config = _cells(tmp_path / "00_config.ipynb")
    assert "'bronze_customer_master_v': 'bronze_customer_master_v'" in config and "main." not in config
    sql = _cells(tmp_path / "03_gold_sql.ipynb")
    assert "CREATE TABLE gold_customer_master_v_selected AS" in sql and "FROM bronze_customer_master_v" in sql
    assert cli.main(["convert", str(FLOW), "--out", str(tmp_path / "o2")]) in (cli.OK, cli.OPEN)         # no --catalog / --schema needed on the command line
    assert (tmp_path / "o2" / "00_config.ipynb").exists()
