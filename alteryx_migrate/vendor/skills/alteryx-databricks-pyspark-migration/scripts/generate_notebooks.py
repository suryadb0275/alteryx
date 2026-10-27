#!/usr/bin/env python3
"""Pre-flight check, then Databricks PySpark notebooks (Bronze/Silver/Gold) from parsed Alteryx workflow JSON.

    python generate_notebooks.py WORKFLOW.json OUTDIR --yxmd WORKFLOW.yxmd [--answers answers.json]
                                 [--overrides overrides.json] [--naming naming.json] [--silver-handoff auto|persist|inline]
                                 [--max-unit-nodes N] [--format ipynb|py] [--preflight-only]
                                 [--test-results tests/results/last_run.json]

Stage 1 (always): pre-flight. Every tool is scanned; settings missing from the JSON are read from --yxmd. If something
blocking remains and no --answers file was given, NOTHING is generated: PREFLIGHT.md (one grouped list of questions) and
answers.template.json are written to OUTDIR, the questions are printed, and the exit code is 3.
Stage 2: with --answers (even an empty {} to say "convert what you can") the non-blocked tools are converted; blocked tools and
everything downstream are listed as waiting for input. No placeholder code is written for them.

answers.json : {"overrides": {"<tool id>": {...}}, "macros": {"<File.yxmc>": {"custom_python": "..."}},
                "accept": {"order_any": [ids] | "all", "source_schema_at_runtime": true, "defaults": true},
                "project": {"timezone": "UTC", "catalog": "main", "write_mode": "overwrite", "allow_overwrite": false, ...}}
overrides.json: {"<tool id>": {<IR field>: value}}  - human-supplied tool configuration (same shape as answers.overrides)
naming.json   : {"<tool id>": "table_name"}         - business names for materialised streams
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import notebooks as NB  # noqa: E402
import preflight as PF  # noqa: E402
import wfmodel as M     # noqa: E402


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("workflow")
    ap.add_argument("outdir")
    ap.add_argument("--yxmd", help="the original workflow XML; settings the parser dropped are read from it")
    ap.add_argument("--answers", help="the user's answers to PREFLIGHT.md (use {} to convert whatever is not blocked)")
    ap.add_argument("--overrides")
    ap.add_argument("--naming")
    ap.add_argument("--silver-handoff", choices=["auto", "persist", "inline"], default="auto")
    ap.add_argument("--max-unit-nodes", type=int, default=0)
    ap.add_argument("--layout", choices=["auto", "units"], default="auto",
                    help="auto: one section per Alteryx container when the .yxmd has containers; units: one function per output table")
    ap.add_argument("--format", choices=["ipynb", "py"], default="ipynb", help="notebook format (default ipynb)")
    ap.add_argument("--preflight-only", action="store_true")
    ap.add_argument("--test-results")
    a = ap.parse_args()
    load = lambda p: json.load(open(p, encoding="utf-8")) if p else None  # noqa: E731
    answers = load(a.answers)
    overrides = {**(load(a.overrides) or {}), **((answers or {}).get("overrides") or {})}
    wf = M.load_workflow(a.workflow, overrides, a.yxmd)
    PF.apply_answers(wf, answers)
    plan = M.plan_medallion(wf, a.silver_handoff, a.max_unit_nodes, load(a.naming))
    project = PF.project_settings(answers)
    options = {"text_match_case_insensitive": project["text_match_case_insensitive"], "ansi": project.get("ansi"), "null_safe_join": project.get("null_safe_join")}
    os.makedirs(a.outdir, exist_ok=True)
    pre, message, template = NB.run_preflight(wf, plan, options, answers)
    for name, text in (("PREFLIGHT.md", message), ("answers.template.json", json.dumps(template, indent=2, ensure_ascii=False)),
                       ("preflight.json", json.dumps(pre, indent=1, ensure_ascii=False, default=str))):
        with open(os.path.join(a.outdir, name), "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
    if a.preflight_only or (pre["blocking"] and answers is None):
        print(message)
        if pre["blocking"]:
            print(f"STOPPED before generating code: {pre['blocking']} blocking question group(s). Answer in one reply "
                  f"(template: {os.path.join(a.outdir, 'answers.template.json')}), then re-run with --answers.")
            sys.exit(3)
        return
    em, cov, files = NB.generate(wf, plan, a.outdir, {"format": a.format, "answers": answers, "layout": a.layout}, load(a.test_results))
    print(f"Wrote {len(files)} files to {a.outdir}: {', '.join(files)}")
    for k, v in cov["summary"].items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
