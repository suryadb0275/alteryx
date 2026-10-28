"""The converter answers the routine questions (the order of the records) itself - from the workflow, or from the model checked in code - and remembers answers.
It still asks what only a person can say. Nothing is applied below high confidence, and the automatic order must give the same code as the order typed by hand."""
import json
import re
from types import SimpleNamespace

from conftest import FIXTURES
from alteryx_migrate import ai, autodecide, cli, engine, interactive, pipeline
from test_interactive import Script, run

HR = FIXTURES / "hr_patterns_flow.yxmd"          # BigQuery input -> Sample [2] -> Multi-Row [3] (forward fill) -> Multi-Row [4] -> ...; a Google Sheet macro
UNIQUE = FIXTURES / "unique_flow.yxmd"           # BigQuery input -> Unique [20] (Customer_Name, Region, Business) -> Select [42]
MACRO = {"_macros": {"hr_googlesheet_macro.yxmc": {"as_source": True, "table": "google.sheets.raw"}}}
ORDER = {"mode": "order_by", "fields": ["EmployeeID", "EffectiveDate"], "desc": []}


def copy(src, folder, *replacements, name=None):
    text = src.read_text(encoding="utf-8")
    for a, b in replacements:
        assert a in text
        text = text.replace(a, b)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (name or src.name)
    path.write_text(text, encoding="utf-8")
    return path


def ordered_hr(folder):
    """The same workflow whose input query ends with ORDER BY: the order the rows arrive in Alteryx."""
    return copy(HR, folder, ("from hr.Pay</inputCustomFilter>", "from hr.Pay order by EmployeeID, EffectiveDate</inputCustomFilter>"))


class OrderModel:
    """A stand-in for the model: answers the order question of each tool with the reply given for its id (a dict -> JSON, a string as is, an exception raised)."""
    model = "fake-model"

    def __init__(self, replies):
        self.replies, self.asked = replies, []

    def complete(self, system, user):
        m = re.search(r'"tool": \{\s*"id": (\d+)', user)
        if not m:
            return "no table code from this model"                     # the conversion also asks it to write whole tables: nothing usable
        tool = int(m.group(1))
        self.asked.append(tool)
        reply = self.replies.get(tool, {"confidence": "low", "mode": "order_by", "fields": []})
        if isinstance(reply, Exception):
            raise reply
        return reply if isinstance(reply, str) else json.dumps(reply)


def text(path):
    return "\n".join("".join(c["source"]) for c in json.loads(path.read_text(encoding="utf-8"))["cells"])


def blocks(folder, ids):
    code = "\n".join(text(p) for p in sorted(folder.glob("0[23]_*.ipynb")) if "_sql" not in p.name)
    return {i: (re.search(rf"^# \[{i}\] .*?(?=^# \[\d+\] |^write_output|\Z)", code, re.M | re.S) or [""])[0] for i in ids}


# ---------------------------------------------------------------------------------------------------------------- A. rules
def test_a_source_query_order_by_answers_every_order_question(tmp_path):
    flow = ordered_hr(tmp_path / "in")
    script = Script("", "1", "main", "s", "", "", "1", "google.sheets.raw")     # model: none; PySpark; catalog; schema; source catalog / schema; the macro reads data
    assert run([str(flow), "--out", str(tmp_path / "out")], script) == cli.OK and not script.answers
    shown = "\n".join(script.prompts + script.lines)
    assert "Decided 3 ordering question(s) from the workflow" in shown and "in which order the rows come" not in shown
    assert "The workflow uses the macro" in shown                              # a missing macro is still asked
    out = tmp_path / "out" / "hr_patterns_flow"
    assert "orderBy(F.col('EmployeeID').asc_nulls_first(), F.col('EffectiveDate').asc_nulls_first())" in "\n".join(blocks(out, [3, 4]).values())
    report = (out / "conversion_report.md").read_text(encoding="utf-8")
    assert "Decisions made for you (please review)" in report and "source query ORDER BY" in report
    assert '`{"3": {"mode": "order_by", "fields": ["EmployeeID", "EffectiveDate"], "desc": []}}`' in report     # the line that overrides it


def test_a_descending_order_by_and_one_the_tools_rename(tmp_path):
    flow = copy(HR, tmp_path, ("from hr.Pay</inputCustomFilter>", "from hr.Pay order by EmployeeID, 2 desc</inputCustomFilter>"))
    analysis, handles = engine.analyze(flow, tmp_path / "work")
    found = autodecide.source_order(handles["wf"], 3)
    assert found[0] == {"mode": "order_by", "fields": ["EmployeeID", "EffectiveDate"], "desc": ["EffectiveDate"]}
    nulls_last = copy(HR, tmp_path / "b", ("from hr.Pay</inputCustomFilter>", "from hr.Pay order by EffectiveDate nulls last</inputCustomFilter>"))
    analysis, handles = engine.analyze(nulls_last, tmp_path / "work_b")
    assert autodecide.source_order(handles["wf"], 3) is None                  # NULLs last ascending is not Alteryx's order: not used


def test_unique_keeps_any_record_only_when_what_follows_uses_only_its_keys(tmp_path):
    keys_only = copy(UNIQUE, tmp_path, ('<SelectField field="*Unknown" selected="True" />', '<SelectField field="*Unknown" selected="False" />'))
    analysis, handles = engine.analyze(keys_only, tmp_path / "a")
    auto = autodecide.decide(analysis, handles)
    assert auto.decisions == {"20": {"mode": "any"}} and auto.evidence["20"]["source"] == "downstream uses only key fields"
    analysis, handles = engine.analyze(UNIQUE, tmp_path / "b")                # the Select also keeps every other field: which record is kept matters
    auto = autodecide.decide(analysis, handles)
    assert auto.decisions == {} and [u["id"] for u in auto.unresolved] == [20]


# ---------------------------------------------------------------------------------------------------------------- B. the model, checked in code
def hr_decide(tmp_path, replies):
    analysis, handles = engine.analyze(HR, tmp_path / "work", decisions=engine.decisions_from_choices(MACRO))
    model = OrderModel(replies)
    return autodecide.decide(analysis, handles, model), model


def test_a_high_confidence_answer_with_a_known_field_is_applied(tmp_path):
    auto, model = hr_decide(tmp_path, {3: {"mode": "order_by", "fields": ["EmployeeID", "EffectiveDate"], "confidence": "high", "evidence": "GroupByFields EmployeeID"}})
    assert auto.decisions["3"]["fields"] == ["EmployeeID", "EffectiveDate"] and auto.evidence["3"]["source"] == "model"
    assert 3 in model.asked and {u["id"] for u in auto.unresolved} == {2, 4}


def test_a_field_that_is_not_in_the_evidence_is_rejected_and_asked(tmp_path):
    auto, _ = hr_decide(tmp_path, {3: {"mode": "order_by", "fields": ["PDATE"], "confidence": "high", "evidence": "?"},
                                   4: {"mode": "order_by", "fields": ["EmployeeID"], "confidence": "high", "evidence": "group"}})
    assert auto.decisions == {} and auto.suggestions == {} and {u["id"] for u in auto.unresolved} == {2, 3, 4}   # [4]: the group field alone is no order


def test_any_record_from_the_model_needs_high_confidence_and_a_unique_or_sample(tmp_path):
    auto, _ = hr_decide(tmp_path, {2: {"mode": "any", "confidence": "high", "evidence": "First 1 per EmployeeID, EffectiveDate"},
                                   3: {"mode": "any", "confidence": "high", "evidence": "?"}})
    assert auto.decisions == {"2": {"mode": "any"}}                           # a Multi-Row Formula never gets 'any'


def test_a_model_error_or_invalid_json_means_the_question_is_asked(tmp_path):
    auto, _ = hr_decide(tmp_path, {2: RuntimeError("endpoint down"), 3: "I think EffectiveDate", 4: '{"mode": "order_by"'})
    assert auto.decisions == {} and auto.suggestions == {} and len(auto.unresolved) == 3


def test_medium_confidence_is_asked_with_the_proposal_as_the_default_and_high_is_reported(tmp_path, monkeypatch):
    model = OrderModel({2: {"mode": "any", "confidence": "low", "evidence": ""},
                        3: {"mode": "order_by", "fields": ["EmployeeID", "EffectiveDate"], "confidence": "medium", "evidence": "dates per employee"},
                        4: {"mode": "order_by", "fields": ["EmployeeID", "EffectiveDate"], "confidence": "high", "evidence": "Row+1:EffectiveDate per EmployeeID"}})
    monkeypatch.setattr(ai, "client_from", lambda *a, **k: model)
    script = Script("fake-model", "2", "main", "s", "", "", "1", "google.sheets.raw",
                    "1",                                                       # Sample [2] (low): asked as before - any record
                    "")                                                        # Multi-Row [3] (medium): Enter accepts the model's proposal
    run([str(HR), "--out", str(tmp_path / "out")], script)
    assert not script.answers
    shown = "\n".join(script.prompts + script.lines)
    assert "The model suggests: rows ordered by EmployeeID, EffectiveDate" in shown and "[EmployeeID, EffectiveDate]" in shown
    assert "Decided 1 of 3 ordering question(s) from the workflow" in shown
    report = (tmp_path / "out" / "hr_patterns_flow" / "conversion_report.md").read_text(encoding="utf-8")
    assert re.search(r"\| \[4\] .*\| model \| high \| Row\+1:EffectiveDate per EmployeeID", report)
    assert re.search(r"\| \[3\] .*\| you \| - \| your answer \(the model's proposal\)", report)


def test_without_a_model_and_without_a_source_order_the_question_is_asked_as_before(tmp_path):
    analysis, handles = engine.analyze(HR, tmp_path / "work", decisions=engine.decisions_from_choices(MACRO))
    auto = autodecide.decide(analysis, handles)
    assert auto.decisions == {} and {u["id"] for u in auto.unresolved} == {2, 3, 4}


# ---------------------------------------------------------------------------------------------------------------- tools without a converter
def test_a_tool_the_skills_describe_is_left_to_the_model_without_a_question(tmp_path):
    script = Script("", "3", "", "", "")                                       # model: none; both; no catalog; source catalog / schema
    run([str(FIXTURES / "sample_flow.yxmd"), "--out", str(tmp_path)], script)
    assert not script.answers and "What should this tool do in this workflow?" not in "\n".join(script.prompts)
    report = (tmp_path / "sample_flow" / "conversion_report.md").read_text(encoding="utf-8")
    assert "Left to the model without a question" in report and "[20] Sample" in report


def test_a_tool_the_skills_do_not_cover_is_still_asked():
    analysis = SimpleNamespace(file_name="w.yxmd", tool_count=2, chunks=[], inputs={}, unsupported=[
        {"id": 7, "name": "Mystery", "plugin": "Acme.Mystery.Tool", "container": "c", "status": "cannot be converted", "reason": "No converter for tool type: Mystery",
         "decision": "", "after": "[1]", "before": "[2]"}])
    script = Script("", "1", "", "it flags rows with a negative amount")
    plan = interactive.gather(script.console(), analysis, {}, SimpleNamespace(format=None, catalog="", schema="", model=None))
    assert plan.tool_notes == {7: "it flags rows with a negative amount"} and not script.answers


# ---------------------------------------------------------------------------------------------------------------- remembered answers
def test_a_second_run_asks_nothing_forget_answers_asks_again_and_a_changed_tool_is_asked_again(tmp_path):
    first = Script("", "1", "main", "s", "", "", "1")                          # model: none; PySpark; catalog; schema; source catalog / schema; Unique: any record
    assert run([str(UNIQUE), "--out", str(tmp_path / "1")], first) == cli.OK and not first.answers
    again = Script()
    assert run([str(UNIQUE), "--out", str(tmp_path / "2")], again) == cli.OK and not again.prompts               # nothing is asked
    assert "Using the settings of your earlier run" in "\n".join(again.lines)
    assert "dropDuplicates" in text(tmp_path / "2" / "unique_flow" / "03_gold.ipynb")                            # the remembered answer is applied
    forgot = Script("", "1", "main", "s", "", "", "1")
    assert run([str(UNIQUE), "--out", str(tmp_path / "3"), "--forget-answers"], forgot) == cli.OK and not forgot.answers
    changed = copy(UNIQUE, tmp_path / "edited", ('<Field field="Business" />', ""))                           # the Unique's keys changed: asked again, nothing else
    script = Script("1")
    assert run([str(changed), "--out", str(tmp_path / "4")], script) == cli.OK and not script.answers
    assert "Which record should it keep?" in "\n".join(script.lines) and len(script.prompts) == 1


def test_ask_all_asks_every_question_as_before(tmp_path):
    flow = ordered_hr(tmp_path / "in")
    script = Script("2", "main", "s", "", "", "1", "google.sheets.raw", "1", "EmployeeID, EffectiveDate", "")
    assert run([str(flow), "--out", str(tmp_path / "out"), "--ask-all"], script) == cli.OK and not script.answers
    assert "\n".join(script.lines).count("in which order the rows come") == 3


def test_yes_uses_the_rules_and_never_invents_an_order(tmp_path):
    def never(prompt):
        raise AssertionError("no question expected")
    flow = ordered_hr(tmp_path / "in")
    (tmp_path / "d.json").write_text(json.dumps(MACRO), encoding="utf-8")
    args = cli.build_parser().parse_args(["convert", str(flow), "--out", str(tmp_path / "out"), "--yes", "--decisions", str(tmp_path / "d.json")])
    assert cli.cmd_convert(args, interactive.Console(never, lambda *a, **k: None)) == cli.OK
    plain = cli.build_parser().parse_args(["convert", str(HR), "--out", str(tmp_path / "plain"), "--yes", "--decisions", str(tmp_path / "d.json")])
    assert cli.cmd_convert(plain, interactive.Console(never, lambda *a, **k: None)) == cli.OPEN                  # no source order, no model: hand-offs


# ---------------------------------------------------------------------------------------------------------------- accuracy guard
def test_the_automatic_order_gives_the_same_code_as_the_order_typed_by_hand(tmp_path, monkeypatch):
    # order tools: the source query's ORDER BY against the same order typed by hand
    pipeline.convert_workflow(HR, tmp_path / "hand", "main", "s", {"2": ORDER, "3": ORDER, "4": ORDER, **MACRO})
    flow = ordered_hr(tmp_path / "in")
    analysis, handles = engine.analyze(flow, tmp_path / "work", decisions=engine.decisions_from_choices(MACRO))
    new, evidence, _, _ = autodecide.settle(analysis, handles, None, None, dict(MACRO))
    assert set(new) == {"2", "3", "4"}
    pipeline.convert_workflow(flow, tmp_path / "auto", "main", "s", {**new, **MACRO})
    assert blocks(tmp_path / "hand", [2, 3, 4, 5]) == blocks(tmp_path / "auto", [2, 3, 4, 5])
    # Unique: a high-confidence model answer against the same order typed by hand
    pick = {"mode": "order_by", "fields": ["Updated_On"], "desc": ["Updated_On"]}
    pipeline.convert_workflow(UNIQUE, tmp_path / "u_hand", "main", "s", {"20": pick})
    analysis, handles = engine.analyze(UNIQUE, tmp_path / "u_work")
    model = OrderModel({20: {**pick, "confidence": "high", "evidence": "Updated_On is the change date"}})
    new, _, _, _ = autodecide.settle(analysis, handles, model, None, {})
    assert new == {"20": pick}
    pipeline.convert_workflow(UNIQUE, tmp_path / "u_auto", "main", "s", new)
    assert blocks(tmp_path / "u_hand", [20]) == blocks(tmp_path / "u_auto", [20]) and "Updated_On" in blocks(tmp_path / "u_auto", [20])[20]
    # Sample (Random: no order question at all): the automatic step changes nothing
    pipeline.convert_workflow(FIXTURES / "sample_flow.yxmd", tmp_path / "s_hand", "main", "s", {})
    analysis, handles = engine.analyze(FIXTURES / "sample_flow.yxmd", tmp_path / "s_work")
    assert autodecide.settle(analysis, handles, OrderModel({}), None, {})[0] == {}
