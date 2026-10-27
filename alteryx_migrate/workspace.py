"""Write the converted notebooks into a Databricks workspace folder, with the credentials of whoever runs it.

Inside a Databricks notebook or job the client authenticates by itself; on a laptop it uses the Databricks CLI profile or DATABRICKS_HOST / DATABRICKS_TOKEN.
"""
from __future__ import annotations

import base64
from pathlib import Path


def client(profile: str | None = None):
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def api_folder(folder: str) -> str:
    folder = folder.rstrip("/")
    if folder.startswith("/Workspace/"):
        return folder[len("/Workspace"):]
    if folder.startswith(("/Users/", "/Shared/")):
        return folder
    raise ValueError(f"{folder!r} is not a workspace folder: use /Workspace/Users/<you>/... (or /Users/<you>/...)")


def write(ws, folder: str, source_dir: Path, files: list) -> list:
    """Upload the .ipynb files as notebooks and the rest as workspace files into `folder` (created, existing files replaced). Returns the paths written."""
    from databricks.sdk.service.workspace import ImportFormat
    target = api_folder(folder)
    ws.workspace.mkdirs(target)
    written = []
    for name in files:
        content = base64.b64encode((Path(source_dir) / name).read_bytes()).decode()
        if "/" in name:
            ws.workspace.mkdirs(f"{target}/{name.rsplit('/', 1)[0]}")                # util files live in a sub-folder (utils/)
        if name.endswith(".ipynb"):
            path = f"{target}/{name[:-len('.ipynb')]}"
            ws.workspace.import_(path, content=content, format=ImportFormat.JUPYTER, overwrite=True)
        else:
            path = f"{target}/{name}"
            ws.workspace.import_(path, content=content, format=ImportFormat.AUTO, overwrite=True)
        written.append(path)
    return written


def table_columns(ws):
    """schema_lookup for pipeline.convert_workflow: the columns and Spark types of a Unity Catalog table, or None when it is missing or not readable."""
    def lookup(full_name: str):
        try:
            table = ws.tables.get(full_name)
        except Exception:                                               # noqa: BLE001 - a missing or forbidden table just has no schema here
            return None
        return {c.name: (c.type_text or "") for c in (table.columns or [])} or None
    return lookup
