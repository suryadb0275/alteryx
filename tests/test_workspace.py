import base64

import pytest

from conftest import FIXTURES
from alteryx_migrate import cli, pipeline, workspace


class FakeWorkspaceApi:
    def __init__(self):
        self.made, self.imports = [], []

    def mkdirs(self, path):
        self.made.append(path)

    def import_(self, path, content, format, overwrite):
        self.imports.append((path, format.value, overwrite, base64.b64decode(content)))


class FakeClient:
    def __init__(self):
        self.workspace = FakeWorkspaceApi()


def test_notebooks_are_written_into_the_workspace_with_the_right_formats(tmp_path):
    result = pipeline.convert_workflow(FIXTURES / "unique_flow.yxmd", tmp_path, "main", "s")
    ws = FakeClient()
    paths = workspace.write(ws, "/Workspace/Users/me@x.com/migrations/flow", tmp_path, result.files)
    assert ws.workspace.made == ["/Users/me@x.com/migrations/flow"]
    by_path = {p: f for p, f, *_ in ws.workspace.imports}
    assert by_path["/Users/me@x.com/migrations/flow/00_config"] == "JUPYTER"                 # a notebook, without the .ipynb suffix
    assert by_path["/Users/me@x.com/migrations/flow/conversion_report.md"] == "AUTO"
    assert all(overwrite for _, _, overwrite, _ in ws.workspace.imports) and len(paths) == len(result.files)


def test_a_path_that_is_not_in_the_workspace_is_refused(tmp_path):
    with pytest.raises(ValueError, match="not a workspace folder"):
        workspace.write(FakeClient(), "/Volumes/main/s/v/x", tmp_path, [])


def test_the_cli_writes_a_local_file_to_the_workspace(monkeypatch, capsys, tmp_path):
    fake = FakeClient()
    monkeypatch.setattr(workspace, "client", lambda profile=None: fake)
    flow = str(FIXTURES / "unique_flow.yxmd")
    code = cli.main(["convert", flow, "--workspace-dir", "/Workspace/Users/me@x.com/m", "--catalog", "main", "--schema", "s"])
    assert code == cli.OPEN and any(p.endswith("/m/00_config") for p, *_ in fake.workspace.imports)
    assert "-> /Workspace/Users/me@x.com/m" in capsys.readouterr().out
    monkeypatch.chdir(tmp_path)                                                                                              # neither --out nor --workspace-dir: ./output/<workflow>/ of the folder it runs from
    assert cli.main(["convert", flow, "--catalog", "main", "--schema", "s", "--yes"]) == cli.OPEN
    assert (tmp_path / "output" / "unique_flow" / "00_config.ipynb").exists() and "output" in capsys.readouterr().out
    assert cli.main(["convert", flow, "--workspace-dir", "/tmp/x", "--catalog", "main", "--schema", "s"]) == cli.FAILED   # not a workspace folder
