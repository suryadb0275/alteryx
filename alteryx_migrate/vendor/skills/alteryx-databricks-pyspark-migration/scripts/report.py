"""Markdown migration report (sections A-I of the required output) built from the model, plan and emission results."""
from __future__ import annotations

from collections import Counter, defaultdict

import notebooks as NB
import wfmodel as M

MAPPING_ROWS = {
    "read": ("spark.read (JDBC / file) into a Bronze Delta table, then spark.table()", "Query dialect (T-SQL) passed through; no schema in the JSON, types come from the source"),
    "literal": ("spark.createDataFrame + typed select", "Embedded static data"),
    "filter": ("DataFrame.filter on coalesce(cond, False); False output = ~cond", "NULL condition -> False output (as Alteryx)"),
    "formula": ("sequential withColumn(expr) + cast to declared Alteryx type", "Later formulas may use earlier outputs; '+' string/number resolved from schema"),
    "multifield": ("select with one expression per listed field ([_CurrentField_] substituted)", "Fields evaluated from incoming values"),
    "multirow": ("F.lag/F.lead over Window(partitionBy group, orderBy record order)", "Needs an explicit order; recursive row references unsupported"),
    "select": ("DataFrame.select of the configured fields (alias for renames, cast for retypes); fields the configuration does not name follow, unless *Unknown is off", "Strict: a selected field that is missing raises"),
    "sort": ("orderBy with Alteryx null ordering", "String collation differs from Alteryx"),
    "unique": ("row_number() over (partition by keys order by record order); Duplicates = rn > 1", "Keep-first needs explicit order"),
    "join": ("inner join + left_anti / right anti-join for L/R outputs", "NULL keys never match; case-sensitive keys; Right_ prefix on collisions"),
    "joinmulti": ("chained joins (not generated: key mapping not in JSON)", "Needs human-supplied definition"),
    "union": ("unionByName(allowMissingColumns)", "Type coercion across inputs; input order = connection order"),
    "append": ("crossJoin(source)", "Cartesian product"),
    "summarize": ("groupBy().agg() per action", "Alteryx returns groups sorted; several actions need review"),
    "crosstab": ("groupBy().pivot().agg()", "Output naming/pivot values need confirmation"),
    "transpose": ("explode(array(struct(name, value)))", "Value column is text"),
    "recordid": ("row_number() over order, offset by start", "Single-partition window"),
    "runningtotal": ("sum over unbounded-preceding window", "Needs explicit order"),
    "countrecords": ("agg(F.count(F.lit(1)).alias(Count))", "One row; counts every record, NULLs included"),
    "autofield": ("rows pass through unchanged", "Alteryx only shrinks the field types; Spark types are not changed"),
    "text2cols": ("split + F.get(i) / explode", "Columns named <root>N; multi-char delimiter rule assumed"),
    "dynrename": ("not generated (rule not in JSON)", "Needs human-supplied mapping/formula"),
    "datetime": ("not generated (field/mode not in JSON)", "Needs human-supplied configuration"),
    "write": ("guarded Delta write to a Gold table", "Original file output (e.g. xlsx) is not reproduced"),
    "browse": ("omitted - inspection only", "No data effect"),
    "comment": ("Markdown cell", "n/a"),
    "generaterows": ("not implemented", "Loop-based row generation needs a human-written equivalent (sequence/explode)"),
    "spatial": ("not implemented", "Needs Databricks spatial functions/H3/Mosaic and validation"),
    "predictive": ("not implemented", "Needs model re-implementation and comparison"),
    "unsupported": ("not implemented (macro / unknown tool)", "Provide macro definition or custom_python override"),
}


def _grp(rows, key):
    d = defaultdict(list)
    for r in rows:
        d[key(r)].append(r["tool_id"])
    return d


def _ids(ids, n=8):
    ids = sorted(ids)
    return ", ".join(map(str, ids[:n])) + (f", ... (+{len(ids) - n})" if len(ids) > n else "")


def _status_section(wf, cov, pre, project, answers, w):
    """Run report required after every conversion: converted %, blocked, assumptions, risks, validation status."""
    S, rows = cov["summary"], cov["tools"]
    w("## Status\n")
    partial = S["tools_blocked"] or S["tools_waiting"]
    w(("**PARTIAL CONVERSION - not production-ready.** " if partial else "**Converted, not yet validated.** ")
      + f"Tools converted: **{S['tools_converted']} / {S['tools_live_data']} ({S['converted_pct']}%)** tools on a path to an output"
      + (f"; {S['tools_blocked']} blocked, {S['tools_waiting']} waiting on them." if partial else "."))
    w(f"\nTransformation only (inputs and outputs left out): **{S['tools_transformation_converted']} / {S['tools_transformation']} "
      f"({S['transformation_converted_pct']}%)** transformation tools converted.")
    w(f"\nSettings read from the workflow XML instead of asked: {S['tools_recovered_from_xml']} tools"
      + (f" ({', '.join(f'{k.replace('Node', '')} {v}' for k, v in sorted(wf.recovered.items()))})" if wf.recovered else
         " (no .yxmd supplied - nothing could be recovered)") + ".\n")
    blocked = [i for i in pre["items"] if i["kind"] == "BLOCKING"]
    w("### Tools blocked - input still needed\n")
    if blocked:
        w("| Tools | Reason | Input still needed |\n|---|---|---|")
        for i in blocked:
            w(f"| {_ids(i['tools'], 12)} ({len(i['tools'])}) | {i['label']}: {i['why']} | {i['send']} |")
        waiting = [r for r in rows if r["status"] == "waiting"]
        w(f"\n{len(waiting)} further tools are **waiting for input** because they depend on a blocked tool (first ids: "
          f"{_ids([r['tool_id'] for r in waiting], 12)}). Tables not generated yet: "
          f"{S['tables_waiting']} (see `PREFLIGHT.md`).\n")
    else:
        w("None.\n")
    w("### Assumptions made\n")
    confirmed = project.get("defaults_confirmed")
    w(f"- Project defaults ({'confirmed by the user' if confirmed else '**not yet confirmed** - proposed defaults used'}): "
      f"time zone `{project['timezone']}`, catalog `{project['catalog']}`, schemas `{project['bronze_schema']}`/"
      f"`{project['silver_schema']}`/`{project['gold_schema']}`, write mode `{project['write_mode']}` "
      f"(overwriting existing tables {'allowed' if project['allow_overwrite'] else 'NOT allowed'}), ANSI "
      f"{'on' if project.get('ansi') else 'cluster setting' if project.get('ansi') is None else 'off'}, text matching "
      f"{'case-insensitive' if project['text_match_case_insensitive'] else 'case-sensitive'}.")
    for i in [i for i in pre["items"] if i["kind"] == "CONFIRM"]:
        w(f"- {i['label']}: {i['default']}")
    for i in [i for i in pre["items"] if i["kind"] == "RECOMMENDED"]:
        w(f"- {i['label']} ({_ids(i['tools'], 6)}): {i['default']}")
    if answers:
        w(f"- User answers applied: {sorted(k for k in answers if not k.startswith('_'))}")
    w("\n### Known risks\n")
    risks = defaultdict(list)
    for r in rows:
        if not r["converted"]:
            continue
        for n in r["notes"] + r["reviews"]:
            low = n.lower()
            for key, label in (("decimal", "type and rounding"), ("round", "type and rounding"), ("tostring", "type and rounding"),
                               ("time zone", "date and time zone"), ("datetime", "date and time zone"), ("run date", "date and time zone"),
                               ("order", "row order"), ("sort", "row order"), ("null", "nulls"), ("three-valued", "nulls")):
                if key in low:
                    risks[label].append(r["tool_id"])
    for label in ("type and rounding", "date and time zone", "row order", "nulls"):
        ids_ = sorted(set(risks.get(label, [])))
        w(f"- **{label}**: " + (f"{len(ids_)} converted tools carry a note ({_ids(ids_, 8)}); details per tool in `coverage.json`." if ids_
                              else "no converted tool carries a specific note."))
    w("\n### Validation status\n")
    w("**not run** - converted, not yet validated. No Alteryx output or frozen input snapshot was compared; the skill's own tests "
      "cover the helper library and expression translator on synthetic data only. Say \"output matches Alteryx\" only after "
      "`04_quality_reconciliation` has compared real data.\n")


def build_report(wf, plan, em, info, cov, comments, pre=None, project=None, kept=None, dropped=None, answers=None):
    S = cov["summary"]
    rows = cov["tools"]
    out = []
    w = out.append
    name = wf.meta.get("file_path") or wf.path
    w(f"# Migration report - {name}\n")
    w("> Generated by `alteryx-databricks-pyspark-migration`. Code generation and validation are separate: nothing below "
      "has been executed against workflow data or in Databricks unless a section says so.\n")
    if pre is not None:
        _status_section(wf, cov, pre, project, answers, w)

    # ---------------------------------------------------------------- A
    w("## A. Workflow overview\n")
    w(f"- Source file: `{name}` (Alteryx {wf.meta.get('alteryx_version')}); {S['tools_identified']} tools, "
      f"{len(wf.edges)} connections, {S['visual_containers_not_migrated']} visual containers.")
    w(f"- Inputs: {len(info['sources'])} ({len(plan.bronze)} distinct Bronze sources in the output path); "
      f"outputs: {len(info['sinks'])} ({info['sinks_source']}).")
    for s in info["sinks"]:
        nd = wf.nodes[s]
        w(f"  - output tool #{s}: `{(nd.get('file_path') or nd.get('table_name') or '')[:150]}` ({nd.get('file_format') or nd.get('destination_type')})")
    w("- Tool mix: " + ", ".join(f"{k} x{v}" for k, v in sorted(info["kinds"].items(), key=lambda kv: -kv[1])[:12]))
    if comments:
        w("\nBusiness context stated in the workflow itself (comment tool, verbatim):\n")
        for c in comments:
            w("```text\n" + str(c).strip() + "\n```")
    w("\n_Reviewer: add a short plain-language purpose statement here; the file does not state one beyond the comment above._\n")

    # ---------------------------------------------------------------- B
    w("## B. Source and dependency analysis\n")
    w("| Bronze key | Source | Tables referenced | Alteryx Input tools |\n|---|---|---|---|")
    for k, v in sorted(plan.bronze.items()):
        w(f"| `{k}` | `{v['connection'] or v['file_path']}` | {', '.join(v['source_tables'])[:90]} | {_ids(v['nodes'], 6)} |")
    lit = [n for n in info["sources"] if wf.kind(n) == "literal"]
    if lit:
        w(f"\nEmbedded reference data (TextInput): tools {lit}.")
    w("\n**Execution flow.** Topological order computed over "
      f"{len(wf.nodes)} IR tools with {len(wf.edges)} connections ({sum(1 for e in wf.edges if e.wireless)} wireless). "
      f"{len(info['live'])} tools lie on a path to an output; {len(info['dead_end_tools'])} do not.")
    dead = Counter(wf.nodes[n]["node_type"] for n in info["dead_end_tools"])
    if dead:
        w("\nTools that feed no output (dead-end / Browse-only branches) - **not generated**, listed so nothing is silently dropped: "
          + ", ".join(f"{k} x{v}" for k, v in dead.most_common()) + f". IDs: {_ids(info['dead_end_tools'], 20)}.")
    if info["unresolved"] or info["missing_inputs"]:
        w("\nUnresolved dependencies:")
        for u in info["unresolved"]:
            w(f"- {u}")
        for n, a in info["missing_inputs"]:
            w(f"- tool #{n} ({wf.nodes[n]['node_type']}) has no connection on required input anchor '{a}'")
    w("\n**Schemas.** The JSON carries no field schemas for sources (`field_info` is empty); column names and types are "
      "discovered at runtime from the Bronze tables. Type-dependent expressions (`+`) are resolved from the DataFrame schema when the plan is built.\n")

    # ---------------------------------------------------------------- C
    w("## C. Transformation mapping\n")
    w("| Alteryx tool kind | Instances | PySpark equivalent | Semantic differences / notes | high conf. | review | blocked | waiting |\n|---|---|---|---|---|---|---|---|")
    bykind = defaultdict(list)
    for r in rows:
        bykind[wf.kind(r["tool_id"])].append(r)
    for k, rs in sorted(bykind.items(), key=lambda kv: -len(kv[1])):
        c = Counter(r["status"] for r in rs)
        eq, diff = MAPPING_ROWS.get(k, ("-", "-"))
        w(f"| {k} | {len(rs)} | {eq} | {diff} | {c['high_confidence']} | {c['requires_review']} | {c['blocked'] + c['unsupported']} | {c['waiting']} |")
    w("\nPer-tool results (status, unit, reviews, notes) are in `coverage.json`; each tool's code is in the unit function named in its `unit` field.\n")
    w("### Items requiring a human decision (grouped)\n")
    groups = defaultdict(list)
    for r in rows:
        for m in r["reviews"]:
            groups[(r["status"], m.split("::")[0][:150])].append(r["tool_id"])
    w("| Status | Issue | Tools | Count |\n|---|---|---|---|")
    for (st, msg), ids in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        w(f"| {st} | {msg.replace('|', '/')} | {_ids(ids, 6)} | {len(ids)} |")

    # ---------------------------------------------------------------- D
    w("\n## D. Medallion architecture\n")
    w("Layer rule (derived from structure, not from tool names): **Bronze** = Input tools; **Silver** = record-level tools "
      "that precede any grain-changing tool (Summarize / Cross Tab); **Gold** = everything downstream of a grain change. "
      "Tables are materialised only at: Bronze sources, Silver nodes shared by >= 2 consumers, the Silver->Gold hand-off, "
      "Write-tool inputs, and nodes that would otherwise be computed twice. Everything else stays an in-memory DataFrame.\n")
    w("| Table | Layer | Why materialised | Tools | Grain / keys (only if stated by workflow) |\n|---|---|---|---|---|")
    for u in plan.units:
        w(f"| `{u.key}` | {u.layer} | {u.reason[:70]} | {len(u.nodes)} | {NB.grain_of(em, u)[:80]} |")
    w(f"\nBronze tables: {len(plan.bronze)}; Silver tables: {sum(1 for u in plan.units if u.layer == 'silver')}; "
      f"Gold tables: {sum(1 for u in plan.units if u.layer == 'gold')}.")
    w("\nDesign decisions needing sign-off: (1) Silver->Gold hand-off tables (re-run with `--silver-handoff inline` to drop them); "
      "(2) table names are derived from sources and steps - `--naming` supplies business names; (3) the Bronze tables "
      "listed in `01_bronze` must exist and hold what each Alteryx input tool read (ingestion is not generated); "
      "(4) shared intermediate tables are created only for nodes used by several outputs.")
    for n in plan.notes:
        w(f"- NOTE: {n}")

    # ---------------------------------------------------------------- E
    w("\n## E. Notebook implementation\n")
    w("Notebooks (`.ipynb`, import into a Databricks workspace folder - plain PySpark, no helper module): `00_config` (catalog, "
      "schemas, table names, write settings) -> `01_bronze` (the input tables, checked - **not ingested**) -> `02_silver_gold` (one section per Alteryx container, in flow order; "
      "without containers `02_silver` -> `03_gold`) -> `04_quality_reconciliation`. `databricks_job.json` chains them as job tasks. Variable names say what the "
      "data is (`orders_filtered_by_status`), tables are `<layer>_<source>_<step>` or `gold_<output file>`; the tool id of every "
      "step is in `coverage.json` (`variable` column).\n")

    # ---------------------------------------------------------------- F
    w("## F. Data quality and reconciliation\n")
    w("`04_quality_reconciliation` checks row counts and duplicate keys (where the workflow defines a grain) for every table and "
      "compares Gold outputs row-by-row (`exceptAll` both ways, numeric tolerance) with baselines exported from the original "
      "Alteryx run. **No baselines were available, so equivalence on workflow data is unproven (NOT RUN).**\n")

    # ---------------------------------------------------------------- G
    w("## G. Optimization and production readiness\n")
    kinds = Counter(wf.kind(r["tool_id"]) for r in rows if r["live"])
    big = [u for u in plan.units if len(u.nodes) > 200]
    w(f"- **Measured:** nothing - no cluster was available. All items below are estimates from workflow structure.")
    w(f"- Joins: {kinds['join']} (no size information in the JSON; rely on AQE broadcast; add `F.broadcast` only after measuring the small side).")
    w(f"- Order-dependent tools: {len(info['order_sensitive'])}; each needs an explicit order (see review table) - windows without partitions run on one task.")
    if kinds["recordid"]:
        w(f"- Record ID x{kinds['recordid']} use a global window (single partition): prefer a business key where possible.")
    if kinds["crosstab"]:
        w(f"- Cross Tab x{kinds['crosstab']}: pass the pivot values explicitly to avoid an extra distinct job and unstable schemas.")
    if big:
        w(f"- {len(big)} unit(s) exceed 200 tools (largest {max(len(u.nodes) for u in big)}): deep logical plans slow Spark analysis; "
          "re-run with `--max-unit-nodes` to add checkpoint tables at clean split points.")
    w("- Layout: no access-pattern information in the JSON, so no partitioning/clustering is applied. After the first runs choose "
      "liquid clustering on frequent filter/join keys; enable predictive optimisation; avoid partitioning small tables.")
    w("- Incremental processing: Alteryx recomputes everything; the generated notebooks full-refresh Silver/Gold. Incremental "
      "loads need a change-tracking column on the sources (not present in the JSON).")
    w("- Write safety: overwrite is refused unless `allow_overwrite=true`; schema changes are refused (no `overwriteSchema`); Delta time travel keeps the previous version.")
    w("- Logging/failure handling: a failed step fails its notebook cell and the job task with the Spark error; blocked or unsupported tools raise "
      "`NotImplementedError` naming the tool, so a run can never silently skip logic.")
    w("- Deployment: parameters via widgets/job parameters; secrets via Databricks secret scopes; compute and schedule are deployment decisions.\n")

    # ---------------------------------------------------------------- H
    w("## H. Assumptions and limitations\n")
    w("- Input is a converter IR, not the original `.yxmd`: anything the converter dropped is invisible here. "
      f"{len(info['config_issues'])} tools have empty/corrupt required configuration and are **blocked** rather than guessed.")
    w("- Converter-produced `spark_sql` hints were **not** used; expressions were re-translated from the original Alteryx text.")
    w(f"- Converter expression failures: {sum(1 for es in wf.expressions.values() for e in es if e.get('status') == 'failed')}; "
      "this skill's translator is the source of truth, and any expression it cannot translate blocks its tool.")
    w("- Unsupported: macros (provide the `.yxmc` or a `custom_python` override), spatial tools, predictive/forecast tools. "
      f"All {S['tools_unsupported']} unsupported tools are preserved unchanged as XML in `unsupported_tools.xml`.")
    w("- Alteryx semantics assumed and flagged where relevant: null comparison, string case/collation, `Contains` case-insensitivity "
      "default, `Round(x, mult)` multiples, `ToString` of integral doubles, Summarize group ordering, Union input order.")
    w("- Not executed in Databricks. Local PySpark tests cover the helper library and expression translator only.\n")

    # ---------------------------------------------------------------- I
    w("## I. Migration coverage report\n")
    w("| Metric | Count |\n|---|---|")
    labels = [
        ("Tools identified (incl. visual containers)", "tools_identified"),
        ("Visual containers (no data effect, not migrated)", "visual_containers_not_migrated"),
        ("Data tools on a path to an output", "tools_live_data"),
        ("Tools converted", "tools_converted"),
        ("  as % of data tools on a path to an output", "converted_pct"),
        ("Transformation tools (no inputs / outputs)", "tools_transformation"),
        ("  converted", "tools_transformation_converted"),
        ("  transformation migration %", "transformation_converted_pct"),
        ("  of which high confidence", "tools_high_confidence"),
        ("  of which requiring review", "tools_requires_review"),
        ("Tools blocked (input still needed)", "tools_blocked"),
        ("Tools waiting on a blocked tool", "tools_waiting"),
        ("Tables converted / waiting", "tables_converted"),
        ("Settings recovered from the workflow XML (tools)", "tools_recovered_from_xml"),
        ("Tools verified by the skill's synthetic tests (tool-kind level)", "tools_verified_by_skill_tests"),
        ("Tools verified on workflow data (needs Alteryx baselines)", "tools_verified_on_workflow_data"),
        ("Not applicable (Browse/Comment)", "tools_not_applicable"),
        ("Dead-end tools not generated", "tools_not_generated_dead_end"),
        ("Unresolved dependencies", "unresolved_dependencies"),
        ("Validation status", "validation_status"),
        ("Validation failures (skill test run)", "validation_failures"),
    ]
    for lab, key in labels:
        v = S[key]
        w(f"| {lab} | {'not executed' if v is None else (str(v) + ' / ' + str(S['tables_waiting']) if key == 'tables_converted' else v)} |")
    w("\nCounts are over data tools on a path to an output. 'Converted' means code was generated for the tool; it does not mean "
      "the result equals Alteryx's.")
    return "\n".join(out) + "\n"
