# Databricks notebook source
# MAGIC %md
# MAGIC # Alteryx workflow -> Databricks notebooks
# MAGIC
# MAGIC 1. Put the `.yxmd` (or `.yxzp`) where this notebook can read it: a Unity Catalog **Volume** (Catalog > your volume > Upload to this volume) or a workspace folder.
# MAGIC    Give the file path (or the folder that holds several files) in **workflow_path**.
# MAGIC 2. **catalog** and **schema** are optional: they name the tables the notebooks read and write (`<catalog>.<schema>.<table>`). Leave them empty and the table names are not
# MAGIC    qualified: the notebooks use the catalog and schema that are current when they run (`USE CATALOG ...; USE SCHEMA ...`).
# MAGIC 3. **output_folder** is the workspace folder that receives `00_config`, `01_bronze`, `02_silver`, `03_gold` and `conversion_report.md`.
# MAGIC 4. Optional **decisions** (JSON), for example `{"20": {"mode": "any"}}` or `{"20": {"mode": "order_by", "fields": ["Date"], "ascending": false}}`.
# MAGIC    Run once without it; the output below lists the tools that wait for an answer.
# MAGIC 5. The Spark SQL notebooks (`02_silver_sql`, `03_gold_sql`) need the columns of the input tables. They are read from `<catalog>.<schema>.<table>` when it exists;
# MAGIC    otherwise give them in **schemas** as JSON: `{"bronze_orders": ["OrderId", "Amount"]}` (or `{column: Spark type}`).
# MAGIC 6. **model** (optional): the name of any Databricks serving endpoint (see Serving in the sidebar). The tables the skill cannot convert by rule are then written by that
# MAGIC    model and marked `AI-WRITTEN` in the notebook: review them. Leave it empty to send nothing anywhere.
# MAGIC
# MAGIC The notebooks are written with the credentials of the user who runs this notebook. Converted, not yet validated.

# COMMAND ----------

dbutils.widgets.text("workflow_path", "", "Workflow file or folder (Volume or workspace path)")
dbutils.widgets.text("output_folder", "", "Output workspace folder (/Workspace/Users/<you>/migrations)")
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("decisions", "", "Decisions (JSON, optional)")
dbutils.widgets.text("model", "", "AI model (a Databricks serving endpoint; empty = no AI)")
dbutils.widgets.text("schemas", "", "Input columns (JSON, optional; read from the tables when empty)")

# COMMAND ----------

import json
import os
import sys
import tempfile
from pathlib import Path

repo = Path(os.getcwd()).resolve().parent                 # this notebook lives in <bundle files>/notebooks
sys.path.insert(0, str(repo))

from alteryx_migrate import ai, pipeline, workspace

# COMMAND ----------

source, folder = dbutils.widgets.get("workflow_path").strip(), dbutils.widgets.get("output_folder").strip()
catalog, schema = dbutils.widgets.get("catalog").strip(), dbutils.widgets.get("schema").strip()
missing = [n for n, v in (("workflow_path", source), ("output_folder", folder)) if not v]
if missing:
    raise ValueError("Fill in: " + ", ".join(missing))
workspace.api_folder(folder)                               # refuse a wrong folder before converting anything
decisions = json.loads(dbutils.widgets.get("decisions") or "{}")
schemas = json.loads(dbutils.widgets.get("schemas") or "{}")
model_name = dbutils.widgets.get("model").strip()

ws = workspace.client()                                    # signs in as the user who runs the notebook
model = ai.client_from(model_name, sdk=ws) if model_name else None     # any Databricks-served model; the notebook's own sign-in is the credential
workflows = pipeline.find_workflows(Path(source))
if not workflows:
    raise FileNotFoundError(f"No .yxmd or .yxzp found at {source}")
many = len(workflows) > 1 or Path(source).is_dir()

for wf in workflows:
    local = Path(tempfile.mkdtemp(prefix="alteryx_out_"))
    result = pipeline.convert_workflow(wf, local, catalog, schema, decisions, schemas, workspace.table_columns(ws), model)
    target = f"{folder.rstrip('/')}/{wf.stem}" if many else folder
    workspace.write(ws, target, local, result.files)
    print(f"{result.workflow}: {result.chunks} output table(s), {result.converted} converted, {len(result.unsupported)} tool(s) need an update -> {target}")
    for u in result.unsupported:
        print(f"  [{u['id']}] {u['name']} in {u['container']}: {u['reason']}")
print("Converted, not yet validated: no output was compared with the Alteryx result.")
