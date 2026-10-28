"""Find Replace is converted from the settings in the workflow XML (the parser's own model of the tool is wrong), in every mode."""
import json
import re

from conftest import FIXTURES
from alteryx_migrate import pipeline

FLOW = FIXTURES / "findreplace_flow.yxmd"       # Left_Table (K, A) is searched (Targets); Right_Table (K, B) is the lookup (Source)


def cells(path):
    return "\n".join("".join(c["source"]) for c in json.loads(path.read_text(encoding="utf-8"))["cells"])


def variant(tmp_path, **settings):
    text = FLOW.read_text(encoding="utf-8")
    for tag, value in settings.items():
        if tag in ("NoCase", "MatchWholeWord"):
            text = text.replace(f'<{tag} value="False" />', f'<{tag} value="{value}" />')
        else:
            text = re.sub(rf"<{tag}>[^<]*</{tag}>", f"<{tag}>{value}</{tag}>", text)
    path = tmp_path / "findreplace_variant.yxmd"
    path.write_text(text, encoding="utf-8")
    return path


def test_append_when_the_lookup_value_is_any_part_of_the_field(tmp_path):
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s")
    assert result.unsupported == [] and not result.open_items
    code = cells(tmp_path / "03_gold.ipynb")
    assert "bronze_right_table.select(F.col('K').alias('__fr_find'), F.col('B').alias('__fr_B')" in code               # Source = the lookup
    assert "bronze_left_table.withColumn('__fr_row'" in code                                                          # Targets = the data searched
    assert ".join(" in code and "F.col('K').contains(F.col('__fr_find')), 'left')" in code                           # unmatched rows are kept
    assert "F.col('__rn') == 1" in code and "F.col('__fr_B').alias('B')" in code                                      # first match, B appended
    sql = cells(tmp_path / "03_gold_sql.ipynb")
    assert "NO SQL" not in sql and "LEFT OUTER JOIN" in sql and "contains(`K`, `__fr_find`)" in sql


def test_entire_field_any_case_and_the_beginning_of_the_field(tmp_path):
    pipeline.convert_workflow(variant(tmp_path, FindMode="FindEntire", NoCase="True"), tmp_path / "a", "main", "s")
    assert "(F.lower(F.col('K')) == F.lower(F.col('__fr_find')))" in cells(tmp_path / "a" / "03_gold.ipynb")
    pipeline.convert_workflow(variant(tmp_path, FindMode="FindBegin"), tmp_path / "b", "main", "s")
    assert "F.col('K').startswith(F.col('__fr_find'))" in cells(tmp_path / "b" / "03_gold.ipynb")


def test_whole_word_matches_between_word_boundaries(tmp_path):
    pipeline.convert_workflow(variant(tmp_path, MatchWholeWord="True"), tmp_path, "main", "s")
    assert r"rlike(F.concat(F.lit('(?<!\\w)\\Q'), F.col('__fr_find'), F.lit('\\E(?!\\w)')))" in cells(tmp_path / "03_gold.ipynb")


def test_replace_writes_the_lookup_value_in_place_of_the_found_text(tmp_path):
    flow = variant(tmp_path, ReplaceMode="Replace", ReplaceFoundField="B")
    result = pipeline.convert_workflow(flow, tmp_path, "main", "s")
    assert result.unsupported == []
    code = cells(tmp_path / "03_gold.ipynb")
    assert "F.replace(F.col('K'), F.col('__fr_find'), F.col('__fr_with'))" in code and "F.when(F.col('__fr_find').isNotNull()" in code
    assert "F.col('__fr_B')" not in code                                                                               # nothing appended


def test_replacing_several_found_items_in_turn_is_reported_not_guessed(tmp_path):
    text = variant(tmp_path, ReplaceMode="Replace").read_text(encoding="utf-8").replace('<ReplaceMultipleFound value="False" />', '<ReplaceMultipleFound value="True" />')
    flow = tmp_path / "multiple.yxmd"
    flow.write_text(text, encoding="utf-8")
    result = pipeline.convert_workflow(flow, tmp_path / "out", "main", "s")
    assert [u["id"] for u in result.unsupported] == [3] and "replace multiple found items" in result.unsupported[0]["reason"]
