"""Databricks AI models for the part of a workflow the migration skill cannot convert by rule.

Only tables that contain a tool without a converter go to a model; everything the skill converts is untouched. Any model served by Databricks can be used: give its name
(an AI Gateway / Model Serving endpoint such as ``alteryx.default.test``, a Foundation Model endpoint, a custom endpoint). The call is OpenAI-compatible:

    POST <base_url>/responses            (Responses API; what the Databricks AI Gateway shows for an endpoint)
    POST <base_url>/chat/completions     (used instead when an endpoint only speaks chat)

``base_url`` is ``https://<workspace>/ai-gateway/mlflow/v1`` (Unity Catalog model services such as ``alteryx.default.test``) and, when that door does not know the model,
``https://<workspace>/serving-endpoints`` (Foundation Model / custom endpoints such as ``databricks-meta-llama-3-3-70b-instruct``). The credentials are the caller's: a personal access token in DATABRICKS_TOKEN, or whatever the
Databricks SDK is signed in with (notebook, CLI profile, OAuth). Nothing here stores or prints a token.

The model's code is never trusted: it is parsed, checked against the project rules, repaired once if it breaks them, and written into the notebook marked as
AI-written, to be reviewed. If it still fails, the table keeps its UPDATE REQUIRED hand-off.
"""
from __future__ import annotations

import ast
import json
import re
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

DEFAULT_PATH = "/ai-gateway/mlflow/v1"


class AIError(Exception):
    """A model call failed in a way the user must read (one message, no token in it)."""


def base_url_for(host: str) -> str:
    host = (host or "").strip().rstrip("/")
    if not host:
        raise AIError("No Databricks workspace URL: set DATABRICKS_HOST, sign in with the Databricks CLI, or pass --base-url.")
    if not host.startswith("http"):
        host = "https://" + host
    return host + DEFAULT_PATH


def redact(text: str, secrets=()) -> str:
    text = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._\-]+", r"\1***", str(text))
    text = re.sub(r"\bdapi[0-9a-f]{8,}\b", "***", text)
    for s in secrets:
        if s:
            text = text.replace(s, "***")
    return text


def _http_post(url: str, headers: dict, body: dict, timeout: int) -> tuple:
    request = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"), headers={**headers, "Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as reply:                     # noqa: S310 - the URL is the user's own workspace
            return reply.status, json.loads(reply.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode("utf-8") or "{}")
        except Exception:                                                                    # noqa: BLE001
            detail = {}
        return exc.code, detail
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return 0, {"error": str(getattr(exc, "reason", exc))}


def text_of_responses(data: dict) -> str:
    """The text of an OpenAI Responses reply (the SDK's output_text, computed from the raw JSON)."""
    if isinstance(data.get("output_text"), str):
        return data["output_text"]
    parts = []
    for item in data.get("output") or []:
        for part in item.get("content") or []:
            if isinstance(part, dict) and part.get("type") in ("output_text", "text") and part.get("text"):
                parts.append(part["text"])
    return "".join(parts)


def text_of_chat(data: dict) -> str:
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return ""
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return content or ""


@dataclass
class Client:
    """One Databricks-served model. `headers` is a callable returning the authorization headers (so tokens can refresh and are never kept in the object's repr).

    Databricks serves models through more than one door and the name decides which one answers: a Unity Catalog model service (``catalog.schema.name``, for example
    ``alteryx.default.test``) is reached through the AI Gateway, a Foundation Model or custom endpoint (``databricks-meta-llama-3-3-70b-instruct``) through
    ``/serving-endpoints``. The client tries each door and each API (Responses, then chat completions) and remembers the first that answers, so any model name works."""
    base_url: str
    model: str
    headers: object = field(repr=False)
    max_output_tokens: int = 16000
    timeout: int = 120
    retries: int = 3
    alt_bases: tuple = ()                            # further doors to try when the first does not know the model
    post: object = field(default=_http_post, repr=False)
    sleep: object = field(default=time.sleep, repr=False)
    route: tuple = ()                                # (base url, "responses" | "chat"), learned from the first answer

    def _call(self, base: str, path: str, body: dict) -> tuple:
        status, data = 0, {}
        for attempt in range(self.retries):
            status, data = self.post(base.rstrip("/") + path, dict(self.headers()), body, self.timeout)
            if status in (0, 429, 500, 502, 503, 504):
                self.sleep(2 ** attempt)
                continue
            break
        return status, data

    def _body(self, api: str, system: str, user: str) -> tuple:
        if api == "responses":
            return "/responses", {"model": self.model, "max_output_tokens": self.max_output_tokens,
                                  "input": [{"role": "system", "content": [{"type": "input_text", "text": system}]},
                                            {"role": "user", "content": [{"type": "input_text", "text": user}]}]}
        return "/chat/completions", {"model": self.model, "max_tokens": self.max_output_tokens,
                                     "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]}

    def complete(self, system: str, user: str) -> str:
        """The model's text for one system + user message. Raises AIError with a readable reason."""
        routes = [self.route] if self.route else [(b, a) for b in (self.base_url, *self.alt_bases) for a in ("responses", "chat")]
        tried = []
        for base, api in routes:
            path, body = self._body(api, system, user)
            status, data = self._call(base, path, body)
            text = (text_of_responses(data) if api == "responses" else text_of_chat(data)) if status == 200 else ""
            if status == 200 and text.strip():
                self.route = (base, api)
                return text
            tried.append(f"{base.rsplit('//', 1)[-1].split('/', 1)[-1]}{path}: HTTP {status} {redact(json.dumps(data)[:160])}")
            if status == 401:
                raise AIError("The workspace refused the credentials (HTTP 401). Check the token / sign-in.")
        denied = [t for t in tried if "HTTP 403" in t]
        if denied and len(denied) == len(tried):
            raise AIError(f"The model {self.model!r} refused the credentials (HTTP 403). Check the token / sign-in and that you may query this endpoint. ({denied[0]})")
        raise AIError(f"The model {self.model!r} gave no usable answer on any route: " + "; ".join(tried[:4]))


def client_from(model: str, base_url: str = "", token: str = "", sdk=None, **options) -> Client:
    """A Client for any Databricks-served model. Credentials: `token`, else the Databricks SDK client `sdk` (notebook / CLI profile / OAuth), else DATABRICKS_TOKEN."""
    import os
    if not model:
        raise AIError("Give the name of the model (a Databricks serving or AI Gateway endpoint).")
    token = token or os.environ.get("DATABRICKS_TOKEN", "")
    alt: tuple = ()
    if not base_url:
        host = (sdk.config.host if sdk is not None else "") or os.environ.get("DATABRICKS_HOST", "")
        base_url = base_url_for(host)
        alt = (base_url.rsplit(DEFAULT_PATH, 1)[0] + "/serving-endpoints",)
    if token:
        def headers():
            return {"Authorization": f"Bearer {token}"}
    elif sdk is not None:
        def headers():
            return dict(sdk.config.authenticate())
    else:
        raise AIError("No credentials for the model: set DATABRICKS_TOKEN or sign in with the Databricks CLI / notebook.")
    return Client(base_url, model, headers, alt_bases=alt, **options)


def list_models(sdk) -> list:
    """Names of the serving endpoints the signed-in user can see (any of them can be used as the model)."""
    out = []
    for e in sdk.serving_endpoints.list():
        state = getattr(getattr(e, "state", None), "ready", None)
        out.append((e.name, getattr(state, "value", state) or ""))
    return sorted(out)


# ---------------------------------------------------------------------------------------------------------------- prompts and checks
FORBIDDEN = [
    (r"\.collect\(\)", "collect() pulls all rows to the driver"),
    (r"\.toPandas\(\)", "toPandas() pulls all rows to the driver"),
    (r"\budf\(|\bpandas_udf\(", "UDFs are not allowed"),
    (r"\bimport\s+dlt\b|\bfrom\s+pyspark\.pipelines\b", "Lakeflow pipeline code is not generated here"),
    (r"\bTODO\b|\bFIXME\b|NotImplementedError", "placeholder code is not allowed"),
    (r"\bimport\s+alteryx_helpers\b", "no helper module: plain PySpark only"),
    (r"(?i)\b(password|secret|token|api[_-]?key)\s*=\s*['\"][^'\"]{6,}", "a secret value is written in the code"),
]


def eligible(chunk: dict) -> bool:
    """A model may write a table only when the gap is a conversion the skill lacks. A gap that is a DECISION (which row Unique keeps, the order of a Record ID ...) is the
    user's to make, never the model's: those tables wait for the answer."""
    return not any(u.get("decision") for u in chunk.get("unconverted") or [])


def system_prompt(skill_text: str, mappings: dict) -> str:
    lines = [skill_text.strip(), "", "## This task",
             "The migration skill above converts Alteryx tools by rule. You are asked ONLY for a table that contains a tool it could not convert. Convert that table's tools",
             "from the Alteryx settings you are given, with the same care and the same rules, and do not invent a setting that is not in the settings: never choose a record order,",
             "a join type, a sort key or a default the settings do not state - write a MANUAL STEP comment for that part instead.",
             "Table mappings (Alteryx input -> Unity Catalog table):"]
    lines += [f"- {k} -> {v}" for k, v in sorted(mappings.items())] or ["- (none)"]
    lines += ["", "## Output rules",
              "Reply with ONE ```python code block: plain PySpark (F, Window, the DataFrame API); F and Window are already imported; no helper module, no placeholders, no TODO,",
              "no collect()/toPandas()/UDF. Read an input with spark.table(INPUTS[\"<key>\"]) (the keys are the first names in the mappings) and finish with",
              "write_output(<DataFrame>, \"<output table>\"). Comment every step with `# [tool id] Tool name`.",
              "If a part cannot be written from the settings, do not guess: write a comment block starting `# MANUAL STEP - tool [<id>] <name>` that says what the tool receives,",
              "what the next tools expect, the Alteryx settings that matter, why you could not write it and the exact PySpark task, and keep the rest working."]
    return "\n".join(lines)


def recipes(chunk: dict) -> dict:
    """{tool: replication guide text} for the tools of the table that have no converter, from the skill's tool guide."""
    try:
        import tool_coverage as TC
    except ImportError:
        return {}
    out = {}
    for u in chunk.get("unconverted") or []:
        if not str(u.get("status", "")).startswith("waits"):
            lines = TC.recipe_lines(u.get("plugin"), u.get("name"))
            if lines:
                out[f"[{u['id']}] {u['name']}"] = "\n".join(lines)
    return out


def user_prompt(chunk: dict, slim_nodes: list, details: dict) -> str:
    body = {"output_table": chunk["id"], "container": chunk.get("container"), "tools_in_order": chunk.get("chain"),
            "why_the_skill_did_not_convert": chunk.get("reasons"),
            "tools_without_converter": [{k: u.get(k) for k in ("id", "name", "container", "after", "before", "reason")} for u in chunk.get("unconverted") or []],
            "tools": chunk.get("tools"), "slim_tool_settings": slim_nodes,
            "original_xml_settings_of_the_tools_without_converter": {str(u["id"]): (details.get(u["id"]) or {}).get("_xml") for u in chunk.get("unconverted") or []},
            "what_the_user_says_each_tool_must_do": {str(u["id"]): (details.get(u["id"]) or {}).get("_asked") for u in chunk.get("unconverted") or [] if (details.get(u["id"]) or {}).get("_asked")},
            "how_to_replicate_them_from_the_alteryx_help": recipes(chunk)}
    return "Convert this part of the Alteryx workflow.\n```json\n" + json.dumps(body, indent=1, default=str) + "\n```"


def extract_code(text: str) -> str:
    blocks = re.findall(r"```(?:python|py)?\s*\n(.*?)```", text, flags=re.S)
    if blocks:
        return max(blocks, key=len).strip("\n") + "\n"
    open_fence = re.search(r"```(?:python|py)?\s*\n(.*)", text, flags=re.S)         # a reply cut off before its closing fence: the repair round reports the syntax error
    if open_fence:
        return open_fence.group(1).strip("\n") + "\n"
    try:                                                                            # the whole reply is code without a fence
        ast.parse(text)
        if text.strip():
            return text.strip("\n") + "\n"
    except SyntaxError:
        pass
    start = redact((text or "").strip().replace("\n", " "))[:200]
    raise AIError(f"The model reply had no code block (it may have been cut off: raise --max-output-tokens). The reply began: {start!r}")


def problems(code: str, table: str) -> list:
    found = []
    try:
        ast.parse(code)
    except SyntaxError as exc:
        found.append(f"syntax error at line {exc.lineno}: {exc.msg}")
        return found
    found += [why for pattern, why in FORBIDDEN if re.search(pattern, code)]
    if re.search(r"spark\.table\(\s*[\"']", code):
        found.append("read tables through INPUTS[<key>] / OUTPUTS[<table>], not a literal table name")
    if "write_output(" not in code and "MANUAL STEP" not in code:
        found.append(f"the code must end with write_output(<DataFrame>, {table!r}) (or contain a MANUAL STEP comment)")
    return found


@dataclass
class Written:
    code: str = ""
    error: str = ""
    attempts: int = 0


def write_chunk(client: Client, system: str, chunk: dict, slim_nodes: list, details: dict, repairs: int = 1) -> Written:
    """Ask the model for one table; check the code; ask once more with the problems when it breaks a rule."""
    user = user_prompt(chunk, slim_nodes, details)
    out = Written()
    try:
        reply = client.complete(system, user)
        out.attempts = 1
        for _ in range(repairs + 1):
            code = extract_code(reply)
            bad = problems(code, chunk["id"])
            if not bad:
                out.code = code
                return out
            if out.attempts > repairs:
                out.error = "the model's code broke the rules: " + "; ".join(bad)
                return out
            reply = client.complete(system, user + "\n\nYour previous answer:\n```python\n" + code + "\n```\nIt does not pass the checks (" + "; ".join(bad) + "). Fix only that; keep the behaviour.")
            out.attempts += 1
    except AIError as exc:
        out.error = str(exc)
    return out


def write_chunks(client: Client, skill_text: str, mappings: dict, chunks: list, slim_by_chunk: dict, details: dict, concurrency: int = 4) -> dict:
    """{chunk id: Written} for the given chunks, a few at a time."""
    system = system_prompt(skill_text, mappings)
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = {c["id"]: pool.submit(write_chunk, client, system, c, slim_by_chunk.get(c["id"], []), details) for c in chunks}
        return {cid: f.result() for cid, f in futures.items()}
