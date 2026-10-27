"""alteryx-migrate: convert Alteryx workflows (a file, or a folder such as a Unity Catalog Volume) into Databricks notebooks.

    python -m alteryx_migrate convert <workflow.yxmd | folder> [--out <folder> | --workspace-dir /Workspace/Users/<you>/<folder>] [--catalog C --schema S] [--decisions decisions.json]

Without --out the notebooks are written to ./output/<workflow name>/ inside the folder you run the command from.

--workspace-dir writes the notebooks straight into your Databricks workspace with your credentials (CLI profile, or DATABRICKS_HOST and DATABRICKS_TOKEN),
so a workflow on your own computer needs no Volume and no upload step.

Each workflow of a folder gets its own sub-folder. Exit codes: 0 everything converted, 2 converted but something needs an update (see
conversion_report.md), 1 the command itself failed.

In a terminal the converter first reads the workflow and then asks, in English: which code you need (PySpark, Spark SQL or both), what any missing macro does, which row a Unique tool
keeps, what each tool without a converter must do, whether to port an embedded Python script with the model - and only then converts. `--yes` skips the questions.
A `.env` file in the folder you run from (copy `.env.example`) can hold DATABRICKS_HOST / DATABRICKS_TOKEN.
--model NAME lets any Databricks-served model (an AI Gateway / Model Serving endpoint, see `models`) write the tables the skill's rules cannot convert; those are marked AI-WRITTEN
to be reviewed. Without --model nothing is sent anywhere. `python -m alteryx_migrate models` lists the models you can use.

Besides the PySpark notebooks (00_config, 01_bronze, 02_silver, 03_gold) it writes the Spark SQL of the same tables (02_silver_sql, 03_gold_sql).
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

from . import ai, engine, env, interactive, pipeline, sqlsrc, workspace

OK, FAILED, OPEN = 0, 1, 2


def say(text: str = "") -> None:
    print(text, flush=True)


def cmd_convert(args, console: interactive.Console | None = None) -> int:
    source = Path(args.input)
    if not source.exists():
        say(f"error: {source} does not exist")
        return FAILED
    workflows = pipeline.find_workflows(source)
    if not workflows:
        say(f"no .yxmd or .yxzp file in {source}")
        return FAILED
    default_out = Path.cwd() / "output"                             # no --out and no --workspace-dir: the notebooks go into ./output/<workflow name>/ of the folder you run from
    catalog, schema = args.catalog or os.environ.get("CATALOG", ""), args.schema or os.environ.get("SCHEMA", "")
    if schema and not catalog:
        say("note: a schema without a catalog gives two-part names (schema.table)")
    decisions = pipeline.load_decisions(args.decisions)
    schemas = pipeline.load_decisions(args.schemas)
    asking = bool(getattr(args, "interactive", False)) or (sys.stdin.isatty() and not getattr(args, "yes", False))
    console = console or interactive.Console()
    ws = None
    if args.workspace_dir:
        ws = workspace.client(args.profile)
        workspace.api_folder(args.workspace_dir)                    # refuse a wrong folder before converting anything
    many = len(workflows) > 1 or source.is_dir()
    status = OK
    for wf in workflows:
        if args.out:
            target = Path(args.out) / wf.stem
        elif args.workspace_dir:
            target = Path(tempfile.mkdtemp(prefix="alteryx_out_"))                  # only a staging folder: the notebooks are uploaded to the workspace
        else:
            target = default_out / wf.stem
        plan, prepared = None, None
        formats = {"pyspark": ("pyspark",), "sql": ("sql",), "both": ("pyspark", "sql"), None: ("pyspark", "sql")}[getattr(args, "format", None)]
        wf_decisions, model_name = dict(decisions), args.model or os.environ.get("MODEL", "")
        chosen = "port" if getattr(args, "port_python", False) else (getattr(args, "python", None) or "keep")
        notes, mode_of, describe = {}, (lambda tool, info: None if chosen == "none" else chosen), {}
        wf_catalog, wf_schema = catalog, schema
        sql_options = {"sql_dialect": getattr(args, "sql_dialect", "") or os.environ.get("SQL_DIALECT", ""), "source_catalog": getattr(args, "source_catalog", "") or os.environ.get("SOURCE_CATALOG", ""),
                       "source_schema": getattr(args, "source_schema", "") or os.environ.get("SOURCE_SCHEMA", "")}
        if asking:
            say(f"Reading {wf.name} (this can take a minute for a large workflow) ...")
            prepared = engine.analyze(wf, Path(tempfile.mkdtemp(prefix="alteryx_migrate_")))
            plan = interactive.gather(console, prepared[0], prepared[1], args)
            formats, wf_decisions, model_name = plan.formats, {**decisions, **plan.decisions}, plan.model or model_name
            notes, describe, mode_of = plan.tool_notes, plan.describe, (lambda tool, info: plan.port.get(tool))
            wf_catalog, wf_schema = plan.catalog or catalog, plan.schema or schema
            sql_options = {"sql_dialect": plan.sql_dialect or sql_options["sql_dialect"], "source_catalog": plan.source_catalog or sql_options["source_catalog"],
                           "source_schema": plan.source_schema or sql_options["source_schema"]}
        model = None
        if model_name:
            if ws is None and not os.environ.get("DATABRICKS_TOKEN"):
                ws = workspace.client(args.profile)
            model = ai.client_from(model_name, args.base_url or "", sdk=ws, max_output_tokens=args.max_output_tokens)
        result = pipeline.convert_workflow(
            wf, target, wf_catalog, wf_schema, wf_decisions, schemas, workspace.table_columns(ws) if ws is not None else None, model, python_mode=mode_of,
            describe=describe, ask=console.answer_question if asking else None, progress=lambda i, n: say(f"   ported group {i} of {n} of the script"),
            formats=formats, prepared=prepared, tool_notes=notes, components=not getattr(args, "inline_python", False), **sql_options)
        place = str(target)
        if args.workspace_dir:
            folder = f"{args.workspace_dir.rstrip('/')}/{wf.stem}" if many else args.workspace_dir
            workspace.write(ws, folder, target, result.files)
            place = folder
        say("")
        say(f"{result.workflow}: {result.chunks} output table(s), {result.converted} converted by rule" + (f", {len(result.ai_tables)} written by the model" if result.ai_tables else "")
            + (f"; Spark SQL for {result.sql_tables} of {result.chunks} table(s)" if "sql" in formats else "") + f"; {len(result.unsupported)} tool(s) still need an update -> {place}")
        if model is not None:
            for table, why in result.ai_failed.items():
                say(f"  the model could not write {table}: {why}")
        for u in result.unsupported:
            say(f"  still open: [{u['id']}] {u['name']} in {u['container']}: {u['reason']}")
        if result.open_items:
            status = OPEN
    say("Converted, not yet validated: no output was compared with the Alteryx result. Anything marked AI-WRITTEN in a notebook must be reviewed first.")
    return status


def cmd_models(args) -> int:
    for name, state in ai.list_models(workspace.client(args.profile)):
        say(f"{name}  {state}")
    return OK


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="alteryx-migrate", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    c = sub.add_parser("convert", help="convert workflows into notebooks (00_config, 01_bronze, 02_silver, 03_gold)")
    c.add_argument("input", help="a .yxmd / .yxzp file or a folder that holds them: on your computer, /Volumes/<catalog>/<schema>/<volume>/..., or /Workspace/...")
    c.add_argument("--out", help="parent folder for the notebooks (a local folder or a Volume folder); each workflow gets its own <workflow name> subfolder. Default: ./output/<workflow name>/ inside the folder you run the command from")
    c.add_argument("--workspace-dir", dest="workspace_dir", help="write the notebooks into this Databricks workspace folder, e.g. /Workspace/Users/<you>/migrations")
    c.add_argument("--profile", help="Databricks CLI profile for --workspace-dir (default: environment authentication)")
    c.add_argument("--catalog", default="", help="catalog of the tables the notebooks read and write (optional; env CATALOG). Without it the names are not qualified and the notebook uses its current catalog")
    c.add_argument("--schema", default="", help="schema of those tables (optional; env SCHEMA). Without it the notebook uses its current schema")
    c.add_argument("--schemas", help="JSON: {input table: [columns] | {column: type}} for inputs the workflow gives no columns for (needed for Spark SQL; "
                                     "with --workspace-dir they are read from the tables in Unity Catalog)")
    c.add_argument("--model", help="a Databricks-served model (endpoint name, for example alteryx.default.test) for the tables the skill cannot convert by rule")
    c.add_argument("--base-url", dest="base_url", help="AI Gateway base URL (default https://<workspace>/ai-gateway/mlflow/v1); the token is DATABRICKS_TOKEN or your CLI sign-in")
    c.add_argument("--max-output-tokens", dest="max_output_tokens", type=int, default=16000)
    c.add_argument("--decisions", help="JSON: {tool id: {mode: any | order_by, fields: [...], ascending: true}, \"_field_types\": {field: string | number | date | datetime | bool}, \"_join_inputs\": {join tool id: {\"#1\": feeding tool id}}} (answers the converter asks)")
    c.add_argument("--sql-dialect", dest="sql_dialect", choices=sorted(sqlsrc.DIALECTS), help="SQL dialect of the queries inside the workflow's input tools (BigQuery inputs are recognised by themselves)")
    c.add_argument("--source-catalog", dest="source_catalog", default="", help="Databricks catalog of the tables those queries read (default: --catalog)")
    c.add_argument("--source-schema", dest="source_schema", default="", help="Databricks schema of those tables (default: the dataset written in the query, e.g. sap_data)")
    c.add_argument("--python", choices=("keep", "port", "none"), help="what to do with an embedded Python (pandas) tool: keep its original code in a component file (default), port it to PySpark with --model, or leave it")
    c.add_argument("--inline-python", dest="inline_python", action="store_true", help="put a ported Python tool's code in the notebook instead of a component file (utils/python_tool_<id>.py)")
    c.add_argument("--format", choices=("pyspark", "sql", "both"), help="which code to write (default: ask when running in a terminal, else both)")
    c.add_argument("--yes", action="store_true", help="do not ask questions in the terminal; use the options given and the defaults")
    c.add_argument("--interactive", action="store_true", help="ask the questions even when the input is not a terminal")
    c.add_argument("--port-python", dest="port_python", action="store_true", help="same as --python port")
    c.set_defaults(fn=cmd_convert)
    m = sub.add_parser("models", help="list the Databricks serving endpoints you can use as --model")
    m.add_argument("--profile", help="Databricks CLI profile (default: environment authentication)")
    m.set_defaults(fn=cmd_models)
    return p


def main(argv: list | None = None) -> int:
    env.load()                                                  # a .env in the current folder (see .env.example); real environment variables win
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args)
    except Exception as exc:                                    # noqa: BLE001 - a command line tool reports, it does not dump a traceback
        say(f"error: {type(exc).__name__}: {exc}")
        return FAILED


if __name__ == "__main__":
    sys.exit(main())
