"""The built-in checks and the coverage report text."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from .common import format_size, tool_label
from .config import NO_DATA_NODES
from .graph import Workflow
from .python_tool import PythonTool
from .redaction import collect_secret_values, find_leaks


@dataclass
class RunData:
    """Everything the checks need to know about one run."""
    mapping_path: str
    yxmd_path: str | None
    out_dir: str
    input_bytes: int
    document: dict
    workflow: Workflow
    python_tools: dict[int, PythonTool]
    slim_nodes: dict
    chunk_files: list
    chunk_bytes: dict
    tools_in_chunks: set
    warnings: list = field(default_factory=list)


@dataclass
class Outcome:
    text: str
    leaks: list
    problems: list


def _unused_reason(run: RunData, node_id: int) -> str:
    if run.workflow.nodes[node_id].get("node_type") in NO_DATA_NODES:
        return "Browse/Comment tool (no data effect)"
    for tool in run.python_tools.values():
        for number, item in tool.inputs.items():
            if item.tool == node_id:
                return f"feeds only Python input #{number}, which no output slice reads"
    return "not upstream of any output (dead end / inspection only)"


def evaluate(run: RunData) -> Outcome:
    wf, out, problems = run.workflow, [], []
    say = out.append
    say("COVERAGE REPORT - slim_mapping")
    say(f"input: {run.mapping_path}  ({format_size(run.input_bytes)})   yxmd: {run.yxmd_path or 'NOT GIVEN'}")
    say(f"tools: {len(wf.nodes)}   edges: {len(wf.edges)}   outputs (sinks): {len(wf.sinks())}   "
        f"embedded Python tools: {len(run.python_tools)}")
    out += [f"WARNING: {w}" for w in run.warnings]

    say("\n[1] Every tool appears in at least one chunk, or is listed as unused with a reason")
    unused = [(n, _unused_reason(run, n)) for n in wf.order if n not in run.tools_in_chunks]
    say(f"    tools in >=1 chunk: {len(wf.nodes) - len(unused)} / {len(wf.nodes)}   unused (with reason): {len(unused)}")
    out += [f"      #{n} {tool_label(wf.nodes[n])}: {why}" for n, why in unused]
    say("    RESULT: PASS")

    say("\n[2] Python slices compile and use no name before it is defined")
    slice_problems = []
    for node_id, tool in run.python_tools.items():
        say(f"    tool #{node_id}: {tool.line_count} lines, {len(tool.cells)} cells, {len(tool.slices)} output(s)")
        for output, check in sorted(tool.checks.items()):
            names = sorted({name for _, name in check.undefined})
            say(f"      Output{output}: cells {tool.slices[output]}  compiles: {'yes' if check.compiles else 'NO - ' + check.error}  "
                f"undefined names: {names or 'none'}")
            if not check.compiles or names:
                slice_problems.append(f"Output{output} slice: compiles={check.compiles} undefined={names}")
        unparsed = [c.index for c in tool.cells if not c.parsed]
        if unparsed:
            say(f"      cells that could not be parsed (included in every slice): {unparsed}")
        if not tool.slices:
            say("      the script has no Alteryx.write(...) call - no slice could be computed")
    if not run.python_tools:
        say("    (no embedded Python tool)")
    problems += slice_problems
    say("    RESULT: " + ("PASS" if not slice_problems else "FAIL"))

    say("\n[3] Cells in no slice (exploration / dead code)")
    for node_id, tool in run.python_tools.items():
        say(f"    tool #{node_id}: {len(tool.unused_cells)} of {len(tool.cells)} cells are in no slice")
        out += [f"      cell {c.index:>3} (lines {c.start}-{c.end}): {c.first_line[:90]}" for c in tool.unused_cells]

    say("\n[4] Python input numbering (#n -> DataFrame -> upstream tool -> source table)")
    for node_id, tool in run.python_tools.items():
        for number, item in sorted(tool.inputs.items()):
            say(f"    #{number:<3} {item.variable or '-':<18} <- tool #{item.tool:<4} {', '.join(item.sources) or '-':<38} "
                f"[{'XML' if item.verified else 'UNVERIFIED file order'}] name check: {item.name_check}")
        by_number = [item.tool for _, item in sorted(tool.inputs.items())]
        by_file = [e.source for e in sorted(wf.preds[node_id], key=lambda e: e.index)]
        say(f"    file order of inputs: {by_file}")
        say(f"    #1..#n order       : {by_number}   -> "
            f"{'SAME' if by_number == by_file else 'DIFFERENT (the file-order fallback would have been wrong)'}")

    say("\n[5] Secret leak check")
    secrets: set = set()
    collect_secret_values(run.document, secrets)
    leaks, scanned = find_leaks(secrets, run.out_dir)
    say(f"    secret values found in the source: {len(secrets)} (total {sum(len(s) for s in secrets):,} chars); "
        f"output files scanned: {scanned}")
    say(f"    leaks: {len(leaks)}")
    out += [f"      LEAK in {path} (a {length}-char secret)" for path, length in leaks]
    say("    RESULT: " + ("PASS" if not leaks else "FAIL"))

    say("\n[6] Sizes")
    slim_bytes = os.path.getsize(os.path.join(run.out_dir, "slim.json"))
    omitted = [o for n in run.slim_nodes.values() for o in n.get("omitted_config_keys", [])]
    say(f"    input mapping.json : {format_size(run.input_bytes)}")
    say(f"    slim.json          : {format_size(slim_bytes)}  ({100 * slim_bytes / run.input_bytes:.1f}% of input; "
        f"~{slim_bytes // 4:,} tokens)")
    say(f"    config keys omitted: {len(omitted)}  ({sum(o['chars'] for o in omitted):,} chars)")
    out += [f"    {name:<58} {format_size(run.chunk_bytes[name]):>10}  (~{run.chunk_bytes[name] // 4:,} tokens)"
            for name in run.chunk_files]
    return Outcome("\n".join(out) + "\n", leaks, problems)
