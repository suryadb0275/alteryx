#!/usr/bin/env python3
"""Phase 1-3 analysis of a parsed Alteryx workflow JSON.  Prints a compact summary (never the raw JSON) and can
write the full machine-readable model.  Usage:
    python analyze_workflow.py WORKFLOW.json [--overrides overrides.json] [--json out.json] [--max-unit-nodes N]
"""
import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import alteryx_expr as X          # noqa: E402
import wfmodel as M               # noqa: E402


def expression_audit(wf):
    """Run the independent translator over every expression in the file; compare with converter status."""
    stats = Counter()
    failures = Counter()
    for nid, n in wf.nodes.items():
        kind = wf.kind(nid)
        items = []
        if kind == "formula":
            items = [f.get("expression") for f in n.get("formulas") or []]
        elif kind in ("filter", "multifield"):
            items = [n.get("expression")]
        elif kind == "multirow":
            items = [n.get("expression")]
        for ex in items:
            if not ex:
                continue
            try:
                r = X.translate(ex, current_field="x" if kind == "multifield" else None,
                                row_ref=(lambda name, off: "F.lag(F.col('x'), 1).over(w)") if kind == "multirow" else None)
                stats["translated_with_review" if r.reviews else "translated"] += 1
            except X.TranslationError as exc:
                stats["failed"] += 1
                failures[str(exc)[:90]] += 1
    conv = Counter(e["status"] for es in wf.expressions.values() for e in es)
    return dict(stats), failures.most_common(8), dict(conv)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("workflow")
    ap.add_argument("--yxmd", help="original workflow XML: settings the parser dropped are read from it")
    ap.add_argument("--overrides")
    ap.add_argument("--naming")
    ap.add_argument("--max-unit-nodes", type=int, default=0)
    ap.add_argument("--silver-handoff", choices=["auto", "persist", "inline"], default="auto")
    ap.add_argument("--json")
    a = ap.parse_args()
    ov = json.load(open(a.overrides, encoding="utf-8")) if a.overrides else None
    nm = json.load(open(a.naming, encoding="utf-8")) if a.naming else None
    wf = M.load_workflow(a.workflow, ov, a.yxmd)
    info = M.analyse(wf)
    plan = M.plan_medallion(wf, a.silver_handoff, a.max_unit_nodes, nm)
    ex_stats, ex_fail, conv = expression_audit(wf)

    print(f"Workflow: {wf.meta.get('file_path')}  (Alteryx {wf.meta.get('alteryx_version')})")
    print(f"Tools in file: {info['tools']} | IR nodes: {info['ir_nodes']} | containers (visual only): {info['containers']} | edges: {info['edges']}")
    print("\nTool types (IR):")
    for k, v in sorted(info["kinds"].items(), key=lambda kv: -kv[1]):
        print(f"  {k:24s}{v:5d}")
    print(f"\nSources: {len(info['sources'])} | Outputs ({info['sinks_source']}): {len(info['sinks'])} -> {info['sinks'][:8]}")
    print(f"Tools needed for outputs (live): {len(info['live'])} | not needed (inspection/dead-end): {len(info['dead_end_tools'])}")
    print(f"Order-sensitive tools: {len(info['order_sensitive'])}")
    print(f"Tools with converter confidence < 1: {len(info['converter_confidence_lt_1'])}")
    by = Counter()
    for n, iss in info["config_issues"].items():
        by[(wf.nodes[n]["node_type"], iss[0].split(" - ")[0])] += 1
    print(f"\nTools with lost/corrupt configuration: {len(info['config_issues'])}")
    for (t, msg), c in by.most_common(15):
        print(f"  {t:24s}{c:5d}  {msg}")
    print(f"\nMissing required inputs: {len(info['missing_inputs'])} | structural problems: {len(info['unresolved'])}")
    for u in info["unresolved"][:10]:
        print("  -", u)
    print(f"\nExpressions: converter status {conv}; independent translator {ex_stats}")
    for msg, c in ex_fail:
        print(f"  translator failure x{c}: {msg}")
    print(f"\nMedallion plan: bronze tables {len(plan.bronze)} (from {sum(len(v['nodes']) for v in plan.bronze.values())} read tools)")
    lc = Counter(u.layer for u in plan.units)
    print(f"  silver units {lc.get('silver', 0)} | gold units {lc.get('gold', 0)} | largest unit {max((len(u.nodes) for u in plan.units), default=0)} tools")
    for n in plan.notes:
        print("  NOTE:", n)
    if a.json:
        out = dict(info=dict(info, live=sorted(info["live"])), plan=dict(
            bronze=plan.bronze, units=[dict(key=u.key, layer=u.layer, node=u.node, nodes=u.nodes, reason=u.reason,
                                            tables={f"{k[0]}:{k[1]}": v for k, v in u.table.items()}) for u in plan.units]))
        json.dump(out, open(a.json, "w", encoding="utf-8"), indent=1, ensure_ascii=False, default=str)


if __name__ == "__main__":
    main()
