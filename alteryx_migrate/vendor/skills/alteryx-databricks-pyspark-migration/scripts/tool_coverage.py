"""What the skill does with every tool on the Alteryx Designer tool list (help.alteryx.com/current/en/designer/tools/designer-tools-list.html).

One row per tool of that page: its category, the status below, and a note. ``python scripts/tool_coverage.py`` prints the matrix as Markdown
(references/14-tool-coverage.md is that output); tests/test_tool_coverage.py keeps it true against the code (a tool marked converted must have an emitter).

Status words
  converted   an emitter writes PySpark (and the Spark SQL made from it); tested on local Spark. "review" = also flagged for confirmation against real data.
  partial     only some modes/options are converted; the rest of the tool is reported, never guessed.
  reported    no emitter yet: the tool is NAMED in the report and in an UPDATE REQUIRED hand-off (settings, position, input/output, task) - never dropped, never stubbed.
              `why` says what is missing (usually: the settings are not verified against the real .yxmd XML).
  not data    no effect on the data (reports, interface widgets, documentation): omitted from the notebooks, listed in the report.
  other       outside a PySpark transformation by nature (ingestion connectors, models, geocoding, ...): reported with the Databricks route to use instead.
"""
from __future__ import annotations

import re

CONVERTED, PARTIAL, REPORTED, NOT_DATA, OTHER = "converted", "partial", "reported", "not data", "other"

# category -> tool names, exactly as listed on the help page
TOOLS = {
    "In/Out": ["Auto Insights Uploader", "Browse", "Date Time Now", "Directory", "Input Data", "Map Input", "Output Data", "Text Input"],
    "Preparation": ["Auto Field", "Create Samples", "Data Cleanse Pro", "Data Cleansing", "Filter", "Formula", "Generate Rows", "Imputation", "Multi-Field Binning",
                    "Multi-Field Formula", "Multi-Row Formula", "Oversample Field", "Random % Sample", "Rank", "Record ID", "Sample", "Select", "Select Records",
                    "Sort", "Tile", "Unique"],
    "Join": ["Append Fields", "Find Replace", "Fuzzy Match", "Join", "Join Multiple", "Make Group", "Union"],
    "Parse": ["DateTime", "RegEx", "Text To Columns", "XML Parse"],
    "Transform": ["Arrange", "Count Records", "Cross Tab", "Make Columns", "Running Total", "Summarize", "Transpose", "Weighted Average"],
    "In-Database": ["Browse In-DB", "Connect In-DB", "Data Stream In", "Data Stream Out", "Dynamic Input In-DB", "Dynamic Output In-DB", "Filter In-DB", "Formula In-DB",
                    "Join In-DB", "Macro Input In-DB", "Macro Output In-DB", "Sample In-DB", "Select In-DB", "Summarize In-DB", "Union In-DB", "Write Data In-DB"],
    "Reporting": ["Email", "Image", "Interactive Chart", "Layout", "Map Legend Builder", "Map Legend Splitter", "Overlay", "Render", "Report Footer", "Report Header",
                  "Report Map", "Report Text", "Table"],
    "Documentation": ["Comment", "Explorer Box", "Tool Container"],
    "Spatial": ["Buffer", "Create Points", "Distance", "Find Nearest", "Generalize", "Heat Map", "Make Grid", "Non Overlapping Drivetime", "Poly-Build", "Poly-Split",
                "Smooth", "Spatial Info", "Spatial Match", "Spatial Process", "Trade Area"],
    "Interface": ["Action", "Check Box", "Condition", "Control Parameter", "Date", "DCM Connection", "Drop Down", "Error Message", "File Browse", "Folder Browse", "List Box",
                  "Macro Input", "Macro Output", "Map", "Numeric Up Down", "Radio Button", "Text Box", "Tree"],
    "Data Investigation": ["Association Analysis", "Basic Data Profile", "Contingency Table", "Distribution Analysis", "Field Summary", "Frequency Table", "Heat Plot",
                           "Histogram", "Importance Weights", "Pearson Correlation", "Plot of Means", "Scatterplot", "Spearman Correlation", "Violin Plot"],
    "Predictive": ["Boosted Model", "Count Regression", "Cross-Validation", "DataRobot Automodel", "DataRobot Predict", "Decision Tree", "Forest Model", "Gamma Regression",
                   "Lift Chart", "Linear Regression", "Logistic Regression", "Model Coefficients", "Model Comparison", "Naive Bayes Classifier", "Nested Test",
                   "Network Analysis", "Neural Network", "Score", "Spline Model", "Stepwise", "Support Vector Machine", "Survival Analysis", "Survival Score",
                   "Test of Means", "Variance Inflation Factors"],
    "AB Testing": ["AB Analysis", "AB Controls", "AB Treatments", "AB Trend"],
    "Time Series": ["ARIMA", "ETS", "TS Compare", "TS Covariate Forecast", "TS Filler", "TS Forecast", "TS Forecast Factory", "TS Model Factory", "TS Plot"],
    "Predictive Grouping": ["Append Cluster", "Find Nearest Neighbors", "K-Centroids Cluster Analysis", "K-Centroids Diagnostics", "MB Affinity", "MB Inspect", "MB Rules",
                            "Multidimensional Scaling", "Principal Components"],
    "Prescriptive": ["Optimization", "Simulation Sampling", "Simulation Scoring", "Simulation Summary"],
    "Connectors": ["Azure Data Lake File Input", "Azure Data Lake File Output", "Adobe Analytics", "Amazon S3 Download", "Amazon S3 Upload", "Anaplan Input", "Anaplan Output",
                   "Automation Anywhere 360", "Box Input", "Box Output", "Cognitive Services Text Analytics", "Dataverse Input", "Dataverse Output", "Dropbox Input",
                   "Dropbox Output", "Dynamics CRM Input", "Dynamics CRM Output", "Google Analytics", "Google Analytics 4 Input", "Google BigQuery Input",
                   "Google BigQuery Output", "Google Drive Input", "Google Drive Output", "Google Sheets Input", "Google Sheets Output", "Marketo Append", "Marketo Input",
                   "Marketo Output", "Microsoft Power BI Output", "MongoDB Input", "MongoDB Output", "OneDrive Input", "OneDrive Output", "Outlook 365 Input",
                   "Power Automate", "Publish to Tableau Server", "Salesforce Input", "Salesforce Output (New)", "ServiceNow Input", "SharePoint Files Input",
                   "SharePoint Files Output", "SharePoint List Input", "SharePoint List Output", "Tableau Output"],
    "Address": ["CASS", "Parse Address", "Reverse Geocoder", "Street Geocoder", "US Geocoder", "US ZIP9 Coder"],
    "Demographic Analysis": ["Allocate Append", "Allocate Input", "Allocate Metainfo", "Allocate Report"],
    "Behavior Analysis (Deprecated)": ["Behavior Metainfo", "Cluster Code", "Compare Behavior", "Create Profile", "Detail Fields", "Profile Comparison Report",
                                       "Profile Detail Report", "Profile Input", "Profile Output", "Profile Rank Report"],
    "Calgary": ["Calgary Input", "Calgary Join", "Calgary Cross Count", "Calgary Cross Count Append", "Calgary Loader"],
    "Developer": ["Apache Spark Code", "API Output", "Base64 Encoder", "Blob Convert", "Blob Input", "Blob Output", "Block Until Done", "Control Container", "Detour",
                  "Detour End", "Download", "Dynamic Input", "Dynamic Rename", "Dynamic Replace", "Dynamic Select", "Expect Equal", "Field Info", "GitHub Commit",
                  "GitHub Get Last Commit", "JSON Parse", "JSON Schema", "License API", "Message", "Python", "R", "Run Command", "Runner", "Server API", "Test", "Throttle"],
    "Laboratory": ["Connect JSON Builder", "Connect Output", "JSON Build", "Jupyter Flow", "Key-Value Pair Extraction", "Transpose In-DB", "Visual Layout"],
    "Machine Learning": ["Assisted Modeling", "Build Features", "Classification", "Data Health", "Feature Types", "Fit", "Machine Learning Predict", "Machine Learning Send",
                         "Predict", "Regression", "Transformation"],
    "Text Mining": ["Named Entity Recognition", "Part-of-Speech Tagger", "Sentiment Analysis", "Text Classification", "Text Pre-processing", "Text Summary", "Topic Modeling",
                    "Word Cloud", "Zero-shot Text Classification"],
    "Computer Vision": ["Barcode Tool", "Image Input", "Image Output", "Image Processing", "Image Profile", "Image Recognition", "Image Template", "Image to Text", "PDF to Text"],
    "AI Tools": ["Invoice Extractor", "LLM Override", "Precision Match", "Prompt", "Schema Fit", "Synthetic Input"],
    "Connect": ["Connect JSON Builder", "Connect Output", "ODBC Loader"],
    "SDK Examples": ["HTML - GUI Library"],
}

# what a category means for tools that have no entry of their own below
CATEGORY_DEFAULT = {
    "In/Out": (REPORTED, "No converter yet; reported with a hand-off."),
    "Preparation": (REPORTED, "No converter yet; reported with a hand-off."),
    "Join": (REPORTED, "No converter yet; reported with a hand-off."),
    "Parse": (REPORTED, "No converter yet; reported with a hand-off."),
    "Transform": (REPORTED, "No converter yet; reported with a hand-off."),
    "Developer": (OTHER, "Automation / integration utility (API, GitHub, blob, license, runner ...) with no data transformation or no Spark equivalent; reported with a hand-off."),
    "In-Database": (REPORTED, "In-Database tools push the work into the database engine; the parser has no converter for them. Rebuild the step on the Databricks table with the "
                              "regular tool of the same name (Filter, Formula, Join, ...), which is converted."),
    "Reporting": (NOT_DATA, "Report layout and delivery; no effect on the data. Rebuild reports in a Databricks dashboard or AI/BI."),
    "Documentation": (NOT_DATA, "Annotation only."),
    "Spatial": (REPORTED, "Spatial objects need Databricks ST_* functions / H3 / Mosaic and a check against Alteryx results; reported with a hand-off."),
    "Interface": (NOT_DATA, "Macro / analytic-app interface widget: its value is a workflow parameter. Replace with a notebook widget or job parameter."),
    "Data Investigation": (OTHER, "Profiling / statistics output for people to read, not a table the workflow continues with. Use Databricks data profiling or SQL summary statistics."),
    "Predictive": (OTHER, "A fitted model: re-implement with Spark ML / MLflow and compare; no equivalence is assumed. Reported with a hand-off."),
    "AB Testing": (OTHER, "Statistical test: re-implement and compare; reported with a hand-off."),
    "Time Series": (OTHER, "Forecasting model: re-implement (for example Databricks forecasting / statsmodels) and compare; reported with a hand-off."),
    "Predictive Grouping": (OTHER, "Clustering / association model: re-implement with Spark ML and compare; reported with a hand-off."),
    "Prescriptive": (OTHER, "Optimisation / simulation: re-implement and compare; reported with a hand-off."),
    "Connectors": (OTHER, "Ingestion from or delivery to an external system. This skill is transformation only: land the data with Lakeflow Connect / Auto Loader / a partner "
                          "connector into the Bronze table, or deliver from the Gold table."),
    "Address": (OTHER, "Address cleansing / geocoding service: use a geocoding provider on Databricks; reported with a hand-off."),
    "Demographic Analysis": (OTHER, "Alteryx data product (Allocate): no Spark equivalent; reported."),
    "Behavior Analysis (Deprecated)": (OTHER, "Deprecated Alteryx data product: no Spark equivalent; reported."),
    "Calgary": (OTHER, "Alteryx Calgary database engine: no Spark equivalent; reported."),
    "Laboratory": (REPORTED, "Experimental tool: not converted; reported with a hand-off."),
    "Machine Learning": (OTHER, "Alteryx Machine Learning model step: re-implement with Spark ML / AutoML and compare; reported with a hand-off."),
    "Text Mining": (OTHER, "Text-analytics model step: re-implement (Spark NLP / AI Functions) and compare; reported with a hand-off."),
    "Computer Vision": (OTHER, "Image / document model step: use Databricks AI Functions (ai_parse_document ...) and compare; reported with a hand-off."),
    "AI Tools": (OTHER, "LLM-based step: re-implement with Databricks AI Functions / Model Serving; reported with a hand-off."),
    "Connect": (OTHER, "Alteryx Connect integration; no Spark equivalent; reported."),
    "SDK Examples": (NOT_DATA, "Developer sample of the SDK; no data."),
}

NO_XML = "the settings are not verified against the real .yxmd XML (the parser's model of this tool is approximate), so converting would guess"

# tools with their own status: name -> (status, note)
OWN = {
    # In/Out
    "Input Data": (CONVERTED, "Read as an existing table (Bronze); nothing is ingested. A database query or file path is shown in 01_bronze."),
    "Text Input": (CONVERTED, "Embedded rows as a small DataFrame (VALUES in SQL)."),
    "Browse": (NOT_DATA, "Viewing aid; omitted."),
    "Output Data": (OTHER, "The converted table is written as a Delta table; an export to the original file / database destination is added by the user (the report says where)."),
    "Date Time Now": (REPORTED, "Run-time clock input: add `current_timestamp()` where needed; reported."),
    "Directory": (REPORTED, "Lists files of a folder; use `dbutils.fs.ls` / `list_files` and a hand-off."),
    "Map Input": (NOT_DATA, "Interactive map selection input; replace with a parameter."),
    "Auto Insights Uploader": (OTHER, "Delivery to Alteryx Auto Insights."),
    # Preparation
    "Filter": (CONVERTED, "True and False outputs; a NULL condition goes to False (as Alteryx)."),
    "Formula": (CONVERTED, "All translated functions (see 11-alteryx-function-reference.md); an untranslatable function blocks the tool, never guessed."),
    "Multi-Field Formula": (PARTIAL, "Review. Copy-output mode and _CurrentFieldType_ are reported."),
    "Multi-Row Formula": (CONVERTED, "Review. Needs an explicit order; the help's default for a missing row (0 / empty) is NULL here - confirm."),
    "Select": (CONVERTED, "Rename, retype, size, deselect, order, *Unknown."),
    "Sort": (CONVERTED, "Review. Spark sorts by bytes, Alteryx by the language dictionary."),
    "Unique": (CONVERTED, "Review. Which row is kept needs an order (asked, never guessed)."),
    "Record ID": (CONVERTED, "Review. Needs an order."),
    "Generate Rows": (PARTIAL, "Review. Only 'init; [f] <= bound; DateTimeAdd | [f]+n' creating a new field."),
    "Sample": (REPORTED, NO_XML + ". Rule from the help: position modes need an order; random modes are not reproducible."),
    "Random % Sample": (REPORTED, NO_XML + ". Rule: sample(fraction, seed) / orderBy(rand(seed)).limit(n); equality with Alteryx can only be checked by distribution."),
    "Rank": (REPORTED, "No parser converter, so the settings are not available."),
    "Select Records": (REPORTED, "No parser converter, so the settings are not available."),
    "Tile": (REPORTED, NO_XML + " (the tile method - equal records, equal sum, smart, unique value, manual - is not captured)."),
    "Imputation": (REPORTED, NO_XML + "."),
    "Data Cleansing": (CONVERTED, "The macro Cleanse.yxmc: its questions are decoded by the names the macro gives them (Check Box (15) = leading/trailing whitespace ...); "
                                  "the steps run in the macro's order with its own expressions. A field whose type the workflow does not state is typed at run time, or by the user for SQL."),
    "Data Cleanse Pro": (REPORTED, "Macro-based; supply the .yxmc."),
    "Auto Field": (CONVERTED, "Alteryx only shrinks the field types to the smallest that fit; the data does not change, so the rows pass through (the type change is noted)."),
    "Multi-Field Binning": (REPORTED, "No parser converter."),
    "Oversample Field": (REPORTED, "No parser converter."),
    "Create Samples": (REPORTED, "No parser converter."),
    # Join
    "Join": (CONVERTED, "J / L / R outputs, select/rename/retype, collisions prefixed. Join by Record Position is reported."),
    "Join Multiple": (REPORTED, "Output naming for colliding fields across inputs is not specified; send a sample of the Alteryx output."),
    "Union": (CONVERTED, "By name (missing columns allowed) and by position."),
    "Append Fields": (CONVERTED, "Cross join; field names must not collide."),
    "Find Replace": (REPORTED, NO_XML + "."),
    "Fuzzy Match": (REPORTED, "Fuzzy matching has no exact Spark equivalent (levenshtein / soundex differ); reported."),
    "Make Group": (REPORTED, "No parser converter."),
    # Parse
    "DateTime": (CONVERTED, "Review. Java-style format letters pass through; Language ignored."),
    "Text To Columns": (CONVERTED, "Review. To columns (N given) or to rows."),
    "RegEx": (REPORTED, "Parse / Match / Replace / Tokenize rules are documented (12-tool-help-notes.md) but " + NO_XML + "."),
    "XML Parse": (REPORTED, NO_XML + "."),
    # Transform
    "Summarize": (CONVERTED, "Review. Group by, Sum, Count, Avg, Min, Max, Median, Concat, Percentile ...; actions without a verified equivalent are reported."),
    "Cross Tab": (PARTIAL, "Review. One aggregation with explicit pivot values; Concatenate / Percent variants reported. No Spark SQL form (pivot values are run-time data)."),
    "Transpose": (CONVERTED, "Key columns repeat; Name / Value (text) rows."),
    "Running Total": (CONVERTED, "Review. Needs an order; prefix RunTot_."),
    "Count Records": (CONVERTED, "One row, one field `Count` (a count of all records, NULLs included)."),
    "Weighted Average": (REPORTED, NO_XML + "."),
    "Arrange": (REPORTED, NO_XML + "."),
    "Make Columns": (REPORTED, "No parser converter."),
    # Spatial that have a parser node but no converter here
    "Create Points": (REPORTED, "Spatial; needs ST_Point and a check against Alteryx."),
    "Distance": (REPORTED, "Spatial; needs ST_Distance and a check."),
    "Spatial Match": (REPORTED, "Spatial; needs ST_Contains / ST_Intersects and a check."),
    # Developer
    "Block Until Done": (CONVERTED, "Control tool: data passes through; the ordering is given by the notebook order."),
    "Dynamic Rename": (PARTIAL, "Review. Formula mode only; the first-row / prefix / suffix / right-input modes are reported."),
    "Dynamic Select": (REPORTED, "Rule documented (select by field metadata from df.schema); no emitter yet."),
    "Dynamic Replace": (REPORTED, "Rule documented; no emitter yet."),
    "Dynamic Input": (REPORTED, "Reads a list of sources chosen at run time; ingestion, out of scope (use Auto Loader / a parameterised notebook)."),
    "Python": (PARTIAL, "Embedded Python: converted cell by cell with the user's approval under rules checked by code (references/10-python-code-tools.md); otherwise reported."),
    "R": (REPORTED, "R code has no automatic translation; reported with the script as the hand-off."),
    "Run Command": (REPORTED, "External program; not part of a Spark transformation."),
    "Download": (OTHER, "HTTP download; ingestion, out of scope."),
    "JSON Parse": (REPORTED, NO_XML + " (use from_json with an explicit schema)."),
    "Apache Spark Code": (OTHER, "Already Spark code: paste it into the notebook; reported."),
    "Field Info": (REPORTED, "Schema metadata as data; use df.schema / DESCRIBE."),
    "Message": (NOT_DATA, "Workflow log message."),
    "Test": (REPORTED, "Data assertion; re-implement as a check / expectation."),
    "Expect Equal": (REPORTED, "Data assertion; re-implement as a check / expectation."),
    "Throttle": (NOT_DATA, "Rate control between records; no effect on the data."),
    "Control Container": (NOT_DATA, "Conditional execution container; replace with notebook / job logic."),
    "Detour": (NOT_DATA, "Conditional routing in a workflow; replace with notebook / job logic."),
    "Detour End": (NOT_DATA, "Conditional routing in a workflow; replace with notebook / job logic."),
    "Tool Container": (NOT_DATA, "Container: its name becomes a section of the notebook."),
    "Comment": (NOT_DATA, "Annotation only."),
    "Explorer Box": (NOT_DATA, "Annotation only."),
    "Field Summary": (OTHER, "Profiling output; use Databricks data profiling / summary(). Reported."),
    "Macro Input": (REPORTED, "Macro interface: supply the .yxmc (and the workflow that calls it); reported."),
    "Macro Output": (REPORTED, "Macro interface: supply the .yxmc; reported."),
    "Google BigQuery Input": (OTHER, "Ingestion: the converted notebooks read the table as it is in Bronze. An input tool is shown in 01_bronze with its query."),
    "Google BigQuery Output": (OTHER, "Delivery; the Gold table is written as Delta. Add the export where needed."),
    "Google Sheets Input": (OTHER, "Ingestion; land the sheet in a Bronze table."),
}


def status_of(category: str, tool: str) -> tuple:
    if tool in OWN:
        return OWN[tool]
    return CATEGORY_DEFAULT[category]


def rows() -> list:
    """[(category, tool, status, note)] for every tool of the list (a tool listed under two categories appears under both)."""
    return [(cat, tool, *status_of(cat, tool)) for cat, names in TOOLS.items() for tool in names]


def counts() -> dict:
    out: dict = {}
    for _, _, status, _ in rows():
        out[status] = out.get(status, 0) + 1
    return out


def _key(text: str) -> str:
    return "".join(ch for ch in str(text).lower() if ch.isalnum())


# the names the parser / workflow files use for tools whose help name differs
ALIASES = {"input": "Input Data", "output": "Output Data", "pythontool": "Python", "jupytercode": "Python", "fileinput": "File Browse", "scoremodel": "Score",
           "naivebayes": "Naive Bayes Classifier", "kcentroids": "K-Centroids Cluster Analysis", "kcentroidsdiagnostics": "K-Centroids Diagnostics",
           "geocoder": "Street Geocoder", "emailoutput": "Email", "chart": "Interactive Chart", "controlparam": "Control Parameter", "automl": "Assisted Modeling",
           "azureblobinput": "Azure Data Lake File Input", "azurebloboutput": "Azure Data Lake File Output", "sharepointinput": "SharePoint Files Input",
           "modeling": "Fit", "insights": "Auto Insights Uploader", "dynamicoutput": "Dynamic Output In-DB", "dbfileinput": "Input Data", "dbfileoutput": "Output Data",
           "datetimenow": "Date Time Now", "bigqueryinput": "Google BigQuery Input", "bigqueryoutput": "Google BigQuery Output", "googlesheetsinput": "Google Sheets Input",
           "browsev2": "Browse", "alteryxselect": "Select", "textbox": "Comment", "toolcontainer": "Tool Container", "randomrecords": "Random % Sample",
           "randompercentsample": "Random % Sample", "multifieldbinning": "Multi-Field Binning", "recordid": "Record ID", "texttocolumns": "Text To Columns",
           "joinmultiple": "Join Multiple", "appendfields": "Append Fields", "countrecords": "Count Records", "autofield": "Auto Field", "runningtotal": "Running Total"}


def lookup(*names) -> tuple | None:
    """(category, help name) of a tool, found by its type or plugin name ('FindReplace', 'Find Replace', 'AlteryxBasePluginsGui.Tile.Tile' ...), or None."""
    index = {_key(tool): (cat, tool) for cat, tools in TOOLS.items() for tool in tools}
    for alias, tool in ALIASES.items():
        index.setdefault(alias, next((cat, t) for cat, tools in TOOLS.items() for t in tools if t == tool))
    for raw in names:
        for piece in str(raw or "").replace("\\", ".").split("."):
            key = _key(piece)
            hit = index.get(key) or index.get(re.sub(r"v\d+$", "", key))          # big_query_output_v3.0.0 -> bigqueryoutput
            if hit:
                return hit
    return None


def explain(*names) -> str | None:
    """The coverage note of a tool, or None."""
    hit = lookup(*names)
    if not hit:
        return None
    status, note = status_of(*hit)
    return f"{hit[1]} - {status}: {note}"


def recipe_lines(*names, width: int = 150) -> list:
    """How to replicate the tool, from the guide, as plain lines (for a comment block or a prompt): what it does, settings, PySpark, Spark SQL, what to confirm."""
    import tool_guide as TG
    hit = lookup(*names)
    if not hit:
        return []
    e = TG.entry(hit[1])
    if not e:
        r = TG.route(hit[1])
        return [f"{hit[1]}: {r[0]} Databricks: {r[1]}"] if r else []
    out = [f"What {hit[1]} does: {e['does']}"]
    if e["settings"]:
        out.append("Settings: " + " ".join(e["settings"].split()))
    if e["pyspark"]:
        out += ["PySpark recipe (replicates the documented behaviour; ORDER = the explicit record order):"] + ["    " + ln for ln in e["pyspark"].splitlines()]
    if e["sql"]:
        out += ["Spark SQL recipe:"] + ["    " + ln for ln in e["sql"].splitlines()]
    if e["confirm"]:
        out += ["Not stated by Alteryx - confirm against real output before relying on it:"] + ["    - " + c for c in e["confirm"]]
    return out


def markdown() -> str:
    import tool_guide as TG
    all_rows = rows()
    total = len(all_rows)
    c = counts()
    lines = ["# Alteryx tools - what each does and how to replicate it in PySpark / Spark SQL", "",
             "Generated by `python scripts/tool_coverage.py` from `scripts/tool_coverage.py` (status) and `scripts/tool_guide.py` (the recipes); `tests/test_tool_coverage.py` checks it against the code. "
             "Source of the tool list: https://help.alteryx.com/current/en/designer/tools/designer-tools-list.html (read 2026-10-27; " + f"{total} entries). "
             "The recipes are written from the official help pages of each tool.", "",
             "**How to read this.** *Status*: **converted** = the skill writes PySpark (and the Spark SQL made from it) and it is tested on local Spark; **partial** = some modes only; **reported** = the skill has no converter yet, "
             "the tool is named in the report and in an `UPDATE REQUIRED` hand-off that carries the recipe below and the tool's original XML settings, so a person or a model can replicate it; "
             "**not data** = no effect on the data; **other** = outside a PySpark transformation by nature (ingestion, models, geocoding ...), with the Databricks route to use. "
             "*Not stated by Alteryx*: the help pages leave many behaviours open (null handling, ties, output field names, row order). A recipe makes a stated choice for each; nothing is silently assumed, "
             "and **no result may be called equal to Alteryx until it has been compared with real Alteryx output**.", "",
             "| Status | Tools |", "|---|---|"]
    for key in (CONVERTED, PARTIAL, REPORTED, NOT_DATA, OTHER):
        lines.append(f"| {key} | {c.get(key, 0)} |")
    lines += ["", "**Where the gaps are:** the data-preparation core is converted (inputs, filter, formula, select, sort, join, union, summarize, cross tab, transpose, unique, record id, running total, multi-row / multi-field formula, "
              "text to columns, datetime, dynamic rename, generate rows, append fields, count records, auto field). Sample, Random % Sample, Create Samples, Oversample Field, Rank, Select Records, Tile, "
              "Multi-Field Binning, Find Replace, Imputation, Data Cleansing, RegEx, Weighted Average, Arrange, Make Columns, XML / JSON Parse, Dynamic Select / Replace / Input, Join Multiple, Make Group and Fuzzy Match "
              "have a full recipe below but no automatic converter: the parser's model of their settings is approximate, so the skill reports them with the recipe and the original XML instead of guessing a setting.", ""]
    for cat, names in TOOLS.items():
        lines += [f"## {cat}", ""]
        brief = [t for t in names if t not in TG.GUIDE]
        if brief:
            lines += ["| Tool | Status | What it does / Databricks route |", "|---|---|---|"]
            for tool in brief:
                status, note = status_of(cat, tool)
                r = TG.route(tool)
                text = f"{r[0]} {r[1]}" if r else note
                lines.append(f"| {tool} | {status} | {text.replace(chr(124), chr(92) + chr(124))} |")
            lines.append("")
        for tool in names:
            if tool in TG.GUIDE:
                status, note = status_of(cat, tool)
                lines += [f"### {tool}", "", f"**Status in this skill:** {status} - {note}", "", TG.render(tool), ""]
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    print(markdown())
