"""The SQL inside the workflow's input tools is a transformation: it is parsed, translated to Spark SQL and run as the Bronze view."""
import json
from decimal import Decimal

from conftest import FIXTURES
from alteryx_migrate import pipeline, sqlsrc

BARE = lambda catalog, db, name: ("", "", name)               # noqa: E731 - tests run on temp views, which have no catalog or schema

# the three kinds of query the RM workflow's inputs carry: rename + filter, sub-select with IN, aggregate with a typed cast
RENAME = """SELECT LIFNR as Vendor_Number,
NAME1 as Vendor_Name,
LAND1 as Country
FROM `stl-staging-data.sap_data.LFA1_V` where NAME1 is not null"""
SUBSELECT = """select a.* from
(SELECT CPUDT_MKPF as GRN_Date, MENGE as Quantity, CONCAT(MBLNR,MJAHR) as Temp2
FROM stl-staging-data.sap_data.MSEG_RPT_V
where WERKS in ('1100','1300') and EBELN IS NOT NULL and CPUDT_MKPF>'20210401'
and CPUDT_MKPF < replace(cast(current_date as string),"-","")) as a
where a.Temp2 in
(SELECT CONCAT(MBLNR,MJAHR) as Temp1 FROM stl-staging-data.sap_data.MKPF_VIEW where BLART='WE')"""
AGGREGATE = """SELECT SPTAG, MATNR, SUM(cast(MZUBB as decimal)) as MZUBB_, sum(MAGBB) as MAGBB_
FROM stl-staging-data.sap_data.S033_V where SPTAG>='20220801' GROUP BY MATNR, SPTAG"""


def text_of(path):
    return "\n".join("".join(c["source"]) for c in json.loads(path.read_text(encoding="utf-8"))["cells"])


def test_a_bigquery_query_becomes_spark_sql_with_the_tables_pointed_at_databricks():
    t = sqlsrc.translate(RENAME, "bigquery", sqlsrc.table_mapper(catalog="main"))
    assert t.ok and "FROM main.sap_data.LFA1_V" in t.sql and "NOT NAME1 IS NULL" in t.sql and t.tables == ["main.sap_data.LFA1_V"]
    assert t.columns == ["Vendor_Number", "Vendor_Name", "Country"]                                   # the columns the query lists, in the case it wrote them
    other = sqlsrc.translate(RENAME, "bigquery", sqlsrc.table_mapper(source_catalog="raw", source_schema="sap", catalog="main"))
    assert "FROM raw.sap.LFA1_V" in other.sql


def test_columns_are_found_through_a_subselect_and_a_star_without_a_schema_stays_unknown():
    assert sqlsrc.translate(SUBSELECT, "bigquery", sqlsrc.table_mapper(catalog="main")).columns == ["GRN_Date", "Quantity", "Temp2"]
    assert sqlsrc.translate("select * from `p.d.t`", "bigquery", sqlsrc.table_mapper(catalog="main")).columns is None


def test_a_decimal_without_precision_keeps_the_source_engines_meaning():
    t = sqlsrc.translate(AGGREGATE, "bigquery", sqlsrc.table_mapper(catalog="main"))
    assert "CAST(MZUBB AS DECIMAL(38, 9))" in t.sql and any("DECIMAL(38, 9)" in n for n in t.notes)      # Spark's own default DECIMAL(10,0) would cut the fraction
    assert "DECIMAL(18, 0)" in sqlsrc.translate("select cast(a as decimal) as x from dbo.t", "tsql", BARE).sql


def test_a_query_that_cannot_be_translated_is_reported_not_guessed():
    assert "does not parse" in sqlsrc.translate("select [x] from %Question.a% bad", "tsql", BARE).error
    assert "dialect" in sqlsrc.translate("select 1", "", BARE).error
    assert "no Databricks table is known" in sqlsrc.translate("select 1 from a.b", "spark", lambda c, d, n: None).error
    assert sqlsrc.dialect_of("big_query_input_v3.0.0") == "bigquery" and sqlsrc.dialect_of("DbFileInput", "SQL Server Native Client", "") == "tsql" and sqlsrc.dialect_of("DbFileInput") == ""


def test_the_translated_queries_give_the_right_rows_on_spark(spark):
    spark.createDataFrame([("V1", "Acme", "DK"), ("V2", None, "SE")], "LIFNR string, NAME1 string, LAND1 string").createOrReplaceTempView("LFA1_V")
    spark.createDataFrame([("20230101", 5.0, "100", "2023", "1100", "E1"), ("20200101", 1.0, "101", "2023", "1100", "E2"), ("20230202", 2.0, "102", "2023", "9999", "E3"),
                           ("20230303", 3.0, "103", "2023", "1300", None), ("20230404", 4.0, "104", "2023", "1300", "E5")],
                          "CPUDT_MKPF string, MENGE double, MBLNR string, MJAHR string, WERKS string, EBELN string").createOrReplaceTempView("MSEG_RPT_V")
    spark.createDataFrame([("100", "2023", "WE"), ("101", "2023", "WE"), ("102", "2023", "WE"), ("103", "2023", "WE"), ("104", "2023", "XX")],
                          "MBLNR string, MJAHR string, BLART string").createOrReplaceTempView("MKPF_VIEW")
    spark.createDataFrame([("20230101", "M1", "1.5", 2.0), ("20230101", "M1", "2.5", 3.0), ("20200101", "M2", "9", 9.0)],
                          "SPTAG string, MATNR string, MZUBB string, MAGBB double").createOrReplaceTempView("S033_V")
    rename = spark.sql(sqlsrc.translate(RENAME, "bigquery", BARE).sql)
    assert rename.columns == ["Vendor_Number", "Vendor_Name", "Country"] and [tuple(r) for r in rename.collect()] == [("V1", "Acme", "DK")]
    sub = spark.sql(sqlsrc.translate(SUBSELECT, "bigquery", BARE).sql)
    assert sub.columns == ["GRN_Date", "Quantity", "Temp2"] and [tuple(r) for r in sub.collect()] == [("20230101", 5.0, "1002023")]       # WERKS, NOT NULL, date and the WE-document rule all apply
    agg = spark.sql(sqlsrc.translate(AGGREGATE, "bigquery", BARE).sql)
    assert [tuple(r) for r in agg.collect()] == [("20230101", "M1", Decimal("4.000000000"), 5.0)]                                         # decimals keep their fraction


def test_the_bronze_notebook_builds_a_view_from_the_input_query_and_checks_the_source_tables(tmp_path):
    result = pipeline.convert_workflow(FIXTURES / "unique_flow.yxmd", tmp_path, "main", "s", {"20": {"mode": "any"}})
    text = text_of(tmp_path / "01_bronze.ipynb")
    assert 'spark.sql("CREATE OR REPLACE VIEW " + INPUTS[\'bronze_customer_master_v\']' in text and "FROM main.sap_data.Customer_Master_V" in text
    assert "SOURCES = ['main.sap_data.Customer_Master_V']" in text and "BigQuery to Spark SQL" in text
    assert result.converted == 1


def test_the_columns_of_the_query_let_the_spark_sql_notebooks_work_without_any_schema_file(tmp_path):
    flow = (FIXTURES / "unique_flow.yxmd").read_bytes().replace(b"select * from sap_data.Customer_Master_V", b"select a as Customer_Name, b as Region, c as Business from sap_data.Customer_Master_V")
    (tmp_path / "x.yxmd").write_bytes(flow)
    pipeline.convert_workflow(tmp_path / "x.yxmd", tmp_path / "p", "main", "s", {"20": {"mode": "any"}})
    sql = text_of(tmp_path / "p" / "03_gold_sql.ipynb")
    assert "CREATE TABLE main.s.gold_customer_master_v_selected AS" in sql and "FROM main.s.bronze_customer_master_v" in sql          # SQL exists although no --schemas was given
