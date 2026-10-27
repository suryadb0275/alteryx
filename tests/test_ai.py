import json

import pytest

from conftest import FIXTURES
from alteryx_migrate import ai, pipeline

FLOW = FIXTURES / "unique_flow.yxmd"
GOOD = "```python\ndf = spark.table(INPUTS['bronze_customer_master_v']).dropDuplicates(['Customer_Name'])  # [20] Unique\nwrite_output(df, 'gold_customer_master_v_selected')\n```"


class Fake:
    """A stand-in for the HTTP call: replies are consumed in order; every request is kept."""

    def __init__(self, *replies):
        self.replies, self.seen = list(replies), []

    def __call__(self, url, headers, body, timeout):
        self.seen.append((url, headers, body))
        return self.replies.pop(0) if self.replies else self.seen_last()

    def seen_last(self):
        return 500, {}


def responses(text):
    return 200, {"output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}]}


def chat(text):
    return 200, {"choices": [{"message": {"content": text}}]}


def client(post, **kw):
    return ai.Client("https://w.example.com/ai-gateway/mlflow/v1", "alteryx.default.test", lambda: {"Authorization": "Bearer dapi0123456789abcdef"}, post=post, sleep=lambda s: None, **kw)


def test_the_responses_api_is_used_first_with_the_pasted_request_shape():
    post = Fake(responses("hello"))
    assert client(post).complete("be brief", "Hello!") == "hello"
    url, headers, body = post.seen[0]
    assert url.endswith("/ai-gateway/mlflow/v1/responses") and body["model"] == "alteryx.default.test" and body["max_output_tokens"] == 16000
    assert body["input"][-1] == {"role": "user", "content": [{"type": "input_text", "text": "Hello!"}]} and headers["Authorization"].startswith("Bearer ")


def test_a_chat_only_endpoint_is_used_through_chat_completions_and_remembered():
    post = Fake((404, {"error": "no responses"}), chat("from chat"), chat("again"))
    c = client(post)
    assert c.complete("s", "u") == "from chat" and c.route[1] == "chat"
    assert c.complete("s", "u") == "again" and post.seen[-1][0].endswith("/chat/completions") and len(post.seen) == 3      # the second call goes straight to chat


def test_a_foundation_model_endpoint_is_reached_through_serving_endpoints_when_the_gateway_does_not_know_it():
    gone = (403, {"error_code": "PERMISSION_DENIED", "message": "'databricks-x' is no longer available. Use Unity Catalog model services."})
    no_responses = (400, {"error_code": "BAD_REQUEST", "message": "Responses API passthrough is not supported for model databricks-x."})
    post = Fake(gone, gone, no_responses, chat("from serving"), chat("again"))          # gateway responses, gateway chat, serving responses, then serving chat
    c = ai.Client("https://w/ai-gateway/mlflow/v1", "databricks-x", lambda: {}, alt_bases=("https://w/serving-endpoints",), post=post, sleep=lambda s: None)
    assert c.complete("s", "u") == "from serving" and post.seen[-1][0] == "https://w/serving-endpoints/chat/completions"
    assert c.route == ("https://w/serving-endpoints", "chat") and c.complete("s", "u") and len(post.seen) == 5          # remembered: one call, not three


def test_transient_errors_are_retried_and_refusals_say_so_without_the_token():
    c = client(Fake((429, {}), responses("ok")))
    assert c.complete("s", "u") == "ok"
    with pytest.raises(ai.AIError) as exc:
        client(Fake((403, {"error": "dapi0123456789abcdef"}), (403, {"error": "dapi0123456789abcdef"}))).complete("s", "u")
    assert "refused the credentials" in str(exc.value) and "dapi0123456789abcdef" not in str(exc.value)
    with pytest.raises(ai.AIError, match="no usable answer"):
        client(Fake((400, {"x": 1}), (400, {"x": 1}))).complete("s", "u")


def test_any_model_name_works_and_credentials_come_from_the_token_or_the_sdk(monkeypatch):
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)

    class Config:
        host = "dbc-1.cloud.databricks.com"

        @staticmethod
        def authenticate():
            return {"Authorization": "Bearer sdk-token"}

    class Sdk:
        config = Config()
    c = ai.client_from("databricks-meta-llama-3-3-70b-instruct", sdk=Sdk())
    assert c.model == "databricks-meta-llama-3-3-70b-instruct" and c.base_url == "https://dbc-1.cloud.databricks.com/ai-gateway/mlflow/v1"         and c.alt_bases == ("https://dbc-1.cloud.databricks.com/serving-endpoints",) and c.headers()["Authorization"] == "Bearer sdk-token"
    monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
    assert ai.client_from("m", base_url="https://x/y", sdk=Sdk()).headers() == {"Authorization": "Bearer tok"}          # an explicit token wins
    monkeypatch.delenv("DATABRICKS_TOKEN")
    with pytest.raises(ai.AIError):
        ai.client_from("m", base_url="https://x/y")
    with pytest.raises(ai.AIError):
        ai.client_from("")
    assert "dapi0123456789abcdef" not in repr(client(Fake()))                                            # the credentials are never in the object's repr


def test_code_that_breaks_a_rule_is_repaired_once_and_a_second_failure_is_reported():
    chunk = {"id": "gold_x", "container": "c", "chain": "Unique [20]", "reasons": [], "unconverted": [], "tools": []}
    bad = "```python\ndf = spark.table(INPUTS['a']).collect()\nwrite_output(df, 'gold_x')\n```"
    good = "```python\ndf = spark.table(INPUTS['a'])\nwrite_output(df, 'gold_x')\n```"
    post = Fake(responses(bad), responses(good))
    out = ai.write_chunk(client(post), "sys", chunk, [], {})
    assert out.code.startswith("df = spark.table") and out.attempts == 2 and "collect() pulls" in post.seen[1][2]["input"][-1]["content"][0]["text"]
    out = ai.write_chunk(client(Fake(responses(bad), responses(bad))), "sys", chunk, [], {})
    assert not out.code and "broke the rules" in out.error
    out = ai.write_chunk(client(Fake(responses("no code here"))), "sys", chunk, [], {})
    assert not out.code and "no code block" in out.error
    literal_table = "spark.table('main.s.t')" + chr(10) + "write_output(d, 't')"
    assert ai.problems("x = (", "t") and ai.problems(literal_table, "t") and ai.problems("write_output(df, 't')  # TODO", "t") and ai.problems("df = 1", "t")           # syntax, literal table, placeholder, no write


def _cells(path):
    return "\n".join("".join(c["source"]) for c in json.loads(path.read_text(encoding="utf-8"))["cells"])


@pytest.fixture
def gap_for_the_model(monkeypatch):
    """The fixture's only gap is a decision (the order Unique keeps), which never goes to a model; pretend it is a conversion the skill lacks."""
    monkeypatch.setattr(ai, "eligible", lambda chunk: True)


def test_a_decision_is_never_given_to_the_model(tmp_path):
    post = Fake(responses(GOOD))
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s", None, None, None, client(post))
    assert post.seen == [] and result.ai_tables == [] and "UPDATE REQUIRED - tool [20] Unique" in _cells(tmp_path / "03_gold.ipynb")
    assert ai.eligible({"unconverted": [{"id": 3, "decision": "order"}]}) is False and ai.eligible({"unconverted": [{"id": 3, "decision": ""}]}) is True


def test_the_model_writes_only_the_table_the_skill_could_not_convert(tmp_path, gap_for_the_model):
    post = Fake(responses(GOOD))
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s", None, None, None, client(post))
    assert result.ai_tables == ["gold_customer_master_v_selected"] and not result.ai_failed and len(post.seen) == 1
    prompt = post.seen[0][2]["input"][-1]["content"][0]["text"]
    assert "gold_customer_master_v_selected" in prompt and "key_fields" in prompt and "tools_without_converter" in prompt             # the slimmed settings and the gap go with it
    text = _cells(tmp_path / "03_gold.ipynb")
    assert "AI-WRITTEN by the model `alteryx.default.test`" in text and "dropDuplicates" in text and "UPDATE REQUIRED" not in text.split("AI-WRITTEN", 1)[1].split("###", 1)[0]
    assert "NO SQL FOR THIS TABLE: it was written by the model" in _cells(tmp_path / "03_gold_sql.ipynb")
    report = (tmp_path / "conversion_report.md").read_text(encoding="utf-8")
    assert "Written by a model" in report and "`gold_customer_master_v_selected` by `alteryx.default.test`" in report
    assert "dapi" not in report and "Bearer" not in text


def test_when_the_model_fails_the_hand_off_stays_and_says_so(tmp_path, gap_for_the_model):
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s", None, None, None, client(Fake(responses("sorry"), responses("sorry"))))
    assert not result.ai_tables and "gold_customer_master_v_selected" in result.ai_failed
    text = _cells(tmp_path / "03_gold.ipynb")
    assert "A model was asked to write this table and could not" in text and "UPDATE REQUIRED - tool [20] Unique" in text and "Hand-off" in text


def test_tables_the_skill_converts_never_go_to_the_model(tmp_path):
    post = Fake(responses(GOOD))
    result = pipeline.convert_workflow(FLOW, tmp_path, "main", "s", {"20": {"mode": "any"}}, None, None, client(post))
    assert post.seen == [] and result.ai_tables == []                                                  # nothing left that needs a model


def test_the_model_gets_the_original_xml_and_the_replication_recipe(tmp_path, gap_for_the_model):
    post = Fake(responses(GOOD))
    pipeline.convert_workflow(FLOW, tmp_path, "main", "s", None, None, None, client(post))
    prompt = post.seen[0][2]["input"][-1]["content"][0]["text"]
    assert "original_xml_settings_of_the_tools_without_converter" in prompt and "AlteryxBasePluginsGui.Unique.Unique" in prompt
    assert "how_to_replicate_them_from_the_alteryx_help" in prompt and "What Unique does" in prompt
