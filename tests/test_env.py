import os

from alteryx_migrate import env


def test_dotenv_is_read_without_overriding_real_variables(tmp_path, monkeypatch):
    monkeypatch.delenv("A_HOST", raising=False)
    monkeypatch.delenv("A_TOKEN", raising=False)
    monkeypatch.delenv("A_EMPTY", raising=False)
    monkeypatch.setenv("A_KEEP", "real")
    f = tmp_path / ".env"
    f.write_text('# comment\nA_HOST=https://h.example.com  # trailing\nexport A_TOKEN="tok en"\nA_EMPTY=\nA_KEEP=file\n\nnot a pair\n', encoding="utf-8")
    assert sorted(env.load(f)) == ["A_HOST", "A_TOKEN"]
    assert os.environ["A_HOST"] == "https://h.example.com" and os.environ["A_TOKEN"] == "tok en"
    assert os.environ["A_KEEP"] == "real" and "A_EMPTY" not in os.environ                 # real variables win; an empty value sets nothing
    assert env.load(tmp_path / "missing.env") == []


def test_the_example_file_lists_what_the_model_needs():
    text = open(os.path.join(os.path.dirname(__file__), "..", ".env.example"), encoding="utf-8").read()
    assert "DATABRICKS_HOST=" in text and "DATABRICKS_TOKEN=" in text and "dapi" not in text          # a template: no real token
