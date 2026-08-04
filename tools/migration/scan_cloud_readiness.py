"""Cloud-migration fragility gauge — how exposed is the pipeline before a move?

This does NOT fix anything. It measures three axes of cloud-migration risk and
grades them, so "how fragile are we?" has a reproducible answer instead of a
vibe. Run it before any migration decision and again after any hardening pass.

Axis 1 — SOURCE EXPOSURE (from data/_meta/source_registry.generated.json)
    Every source host classified by how it behaves from a datacenter IP:
      WAF_GOVIE  — assets.gov.ie / *.gov.ie: 403s bot UAs AND may block by
                   datacenter ASN even with a browser-UA spoof. The confirmed
                   2026-07-07 incident. HIGH risk on GitHub-hosted / cloud runners.
      COUNCIL    — *.ie council sites: TLS cert quirks (curl -k), possible geo
                   blocking. MEDIUM.
      ARCHIVE    — web.archive.org: rate-limits + latency, not a hard block. MEDIUM.
      API_SAFE   — Oireachtas / CKAN / open-data APIs: not gated. LOW.

Axis 2 — POLLING / CANARY COVERAGE (registry + source_health.json)
    What fraction of sources can we even tell has gone stale? A source with no
    poller and no canary fails silently in the cloud.

Axis 3 — RUNTIME RESILIENCE (AST over the executable pipeline surface)
    Migration blockers baked into the code that runs during pipeline.py:
      * hardcoded c:/tmp or Windows paths (won't exist on Linux)
      * bare requests.get / urlopen (no retry, no curl fallback, no WAF spoof)
      * gov.ie fetches with no browser-UA in the module

Usage
-----
    python tools/scan_cloud_readiness.py                 # graded report
    python tools/scan_cloud_readiness.py -o doc/CLOUD_READINESS.md
    python tools/scan_cloud_readiness.py --json out.json

Exit code is always 0 — a gauge, not a gate.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
import tokenize
from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REGISTRY = PROJECT_ROOT / "data" / "_meta" / "source_registry.generated.json"
HEALTH = PROJECT_ROOT / "data" / "_meta" / "source_health.json"
RUNTIME_DIR_NAMES = (
    "attendance",
    "charity",
    "committees",
    "corporate",
    "debates",
    "extractors",
    "iris",
    "legal",
    "legislation",
    "lobbying",
    "members",
    "payments",
    "pdf_infra",
    "planning/civic/extractors",
    "reference",
    "services",
    "shared",
    "votes",
    "wikidata",
)
RUNTIME_DIRS = tuple(PROJECT_ROOT / name for name in RUNTIME_DIR_NAMES)
PIPELINE_TOOL_FILES = (
    "tools/build_fact_cards.py",
    "tools/build_source_health.py",
    "tools/build_source_registry.py",
    "tools/check_extraction_quality.py",
    "tools/check_freshness.py",
    "tools/check_output_regressions.py",
    "tools/lobbying_freshness_check.py",
    "tools/procurement_source_poller.py",
)

# Direct transports are exceptions only when the HTTP exchange itself carries
# state that the generic helpers intentionally do not model.  Every exception is
# reported with this rationale; adding a filename here is therefore a reviewable
# architecture decision, not a hidden scanner suppression.
DIRECT_HTTP_EXEMPTIONS = {
    "services/http_engine.py": "canonical shared retry, validation, streaming and curl-fallback transport",
    "services/ted_search.py": (
        "stateful TED iteration-token paginator with declared-total completeness checks and endpoint-specific retries"
    ),
    "pdf_infra/pdf_endpoint_check.py": (
        "diagnostic probe must retain exact HEAD status and requests exception classes instead of downloading content"
    ),
    "pdf_infra/pdf_downloader.py": "dedicated streamed PDF transport owns response streaming and per-file diagnostics",
    "pdf_infra/oireachtas_pdf_poller.py": (
        "stateful manifest poller compares remote HEAD lengths before resumable streamed downloads"
    ),
    "tools/build_source_health.py": (
        "source-health probe intentionally observes HEAD/Range status and headers without fetching full files"
    ),
    "tools/procurement_source_poller.py": (
        "freshness probe requires response status/headers and a source-specific permissive TLS context"
    ),
    "extractors/_gnews_resolve.py": (
        "cookie-bearing Google News resolver performs a stateful landing-page plus batchexecute RPC exchange"
    ),
}

HTTP_METHODS = frozenset({"get", "post", "head", "put", "patch", "delete", "request"})
HTTP_ENGINE_FUNCTIONS = frozenset(
    {
        "download_file",
        "fetch_all",
        "fetch_all_text",
        "fetch_bytes",
        "fetch_json",
        "fetch_text",
        "new_session",
        "polite_headers",
        "post_bytes",
        "post_json",
    }
)

# Host → (risk band, class). Order matters: first match wins.
HOST_RULES: list[tuple[str, str, str]] = [
    ("assets.gov.ie", "HIGH", "WAF_GOVIE"),
    (".gov.ie", "HIGH", "WAF_GOVIE"),
    ("web.archive.org", "MEDIUM", "ARCHIVE"),
    ("oireachtas.ie", "LOW", "API_SAFE"),
    ("opendata.cro.ie", "LOW", "API_SAFE"),
    ("data.gov.ie", "LOW", "API_SAFE"),
    ("lobbying.ie", "LOW", "API_SAFE"),
    ("wikidata.org", "LOW", "API_SAFE"),
    ("ted.europa.eu", "LOW", "API_SAFE"),
]
# Anything ending .ie that didn't match above is treated as a council/public host.
COUNCIL_SUFFIX = (".ie",)

LOCAL_PATH_RE = re.compile(r"(?:[cC]:[\\/]tmp|/mnt/c|[cC]:[\\/]Users)[^\r\n\"']*")


def _docstring_nodes(tree: ast.AST) -> set[int]:
    """Identify structural docstrings so prose never counts as runtime code."""
    found: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) or not node.body:
            continue
        first = node.body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
            found.add(id(first.value))
    return found


def _attribute_parts(node: ast.AST) -> tuple[str, ...]:
    """Return the dotted name represented by a Name/Attribute expression."""
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return tuple(reversed(parts))
    return ()


def _is_session_annotation(node: ast.AST | None, request_modules: set[str], session_types: set[str]) -> bool:
    if node is None:
        return False
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return _is_session_annotation(node.left, request_modules, session_types) or _is_session_annotation(
            node.right, request_modules, session_types
        )
    if isinstance(node, ast.Subscript):
        return _is_session_annotation(node.value, request_modules, session_types)
    parts = _attribute_parts(node)
    return bool(
        (len(parts) == 1 and parts[0] in session_types)
        or (len(parts) >= 2 and parts[-1] == "Session" and parts[0] in request_modules)
    )


def _direct_http_calls(tree: ast.AST) -> list[dict[str, object]]:
    """Resolve direct requests/urllib calls, including aliases and bound sessions."""
    request_modules: set[str] = set()
    request_functions: dict[str, str] = {}
    session_types: set[str] = set()
    urllib_roots: set[str] = set()
    urllib_request_modules: set[str] = set()
    urlopen_functions: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".")[0]
                if alias.name == "requests":
                    request_modules.add(bound)
                elif alias.name == "urllib":
                    urllib_roots.add(bound)
                elif alias.name == "urllib.request":
                    if alias.asname:
                        urllib_request_modules.add(bound)
                    else:
                        urllib_roots.add(bound)
        elif isinstance(node, ast.ImportFrom):
            if node.module == "requests":
                for alias in node.names:
                    bound = alias.asname or alias.name
                    if alias.name in HTTP_METHODS:
                        request_functions[bound] = alias.name
                    elif alias.name == "Session":
                        session_types.add(bound)
            elif node.module == "requests.sessions":
                for alias in node.names:
                    if alias.name == "Session":
                        session_types.add(alias.asname or alias.name)
            elif node.module == "urllib.request":
                for alias in node.names:
                    if alias.name == "urlopen":
                        urlopen_functions.add(alias.asname or alias.name)
            elif node.module == "urllib":
                for alias in node.names:
                    if alias.name == "request":
                        urllib_request_modules.add(alias.asname or alias.name)

    # Support local module aliases (``http = requests``) and session factories
    # whose return annotation makes the transport ownership explicit.
    session_factories = set(session_types)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _is_session_annotation(
            node.returns, request_modules, session_types
        ):
            session_factories.add(node.name)
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if isinstance(value, ast.Name) and value.id in request_modules:
                request_modules.update(target.id for target in targets if isinstance(target, ast.Name))

    def is_session_factory(call: ast.Call) -> bool:
        parts = _attribute_parts(call.func)
        return bool(
            (len(parts) == 1 and parts[0] in session_factories)
            or (len(parts) == 2 and parts[0] in request_modules and parts[1] in {"Session", "session"})
        )

    session_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
            session_names.update(
                arg.arg for arg in args if _is_session_annotation(arg.annotation, request_modules, session_types)
            )

    # Fixed point covers ``session = make_session()`` and simple aliases.
    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            targets: list[ast.expr] = []
            value: ast.AST | None = None
            if isinstance(node, ast.Assign):
                targets, value = node.targets, node.value
            elif isinstance(node, ast.AnnAssign):
                targets, value = [node.target], node.value
            if value is not None:
                is_session = (isinstance(value, ast.Call) and is_session_factory(value)) or (
                    isinstance(value, ast.Name) and value.id in session_names
                )
                if is_session:
                    for target in targets:
                        if isinstance(target, ast.Name) and target.id not in session_names:
                            session_names.add(target.id)
                            changed = True
            if isinstance(node, (ast.With, ast.AsyncWith)):
                for item in node.items:
                    if (
                        isinstance(item.context_expr, ast.Call)
                        and is_session_factory(item.context_expr)
                        and isinstance(item.optional_vars, ast.Name)
                        and item.optional_vars.id not in session_names
                    ):
                        session_names.add(item.optional_vars.id)
                        changed = True

    found: list[dict[str, object]] = []
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        parts = _attribute_parts(call.func)
        kind: str | None = None
        if isinstance(call.func, ast.Name) and call.func.id in request_functions:
            kind = f"requests.{request_functions[call.func.id]} (imported as {call.func.id})"
        elif isinstance(call.func, ast.Name) and call.func.id in urlopen_functions:
            kind = f"urllib.request.urlopen (imported as {call.func.id})"
        elif len(parts) == 2 and parts[0] in request_modules and parts[1] in HTTP_METHODS:
            kind = f"requests.{parts[1]}"
        elif len(parts) == 2 and parts[0] in session_names and parts[1] in HTTP_METHODS:
            kind = f"requests.Session.{parts[1]}"
        elif (
            len(parts) == 2
            and parts[0] in urllib_request_modules
            and parts[1] == "urlopen"
            or len(parts) == 3
            and parts[0] in urllib_roots
            and parts[1:] == ("request", "urlopen")
        ):
            kind = "urllib.request.urlopen"
        elif (
            isinstance(call.func, ast.Attribute)
            and call.func.attr in HTTP_METHODS
            and isinstance(call.func.value, ast.Call)
            and is_session_factory(call.func.value)
        ):
            kind = f"requests.Session.{call.func.attr}"
        if kind is not None:
            found.append({"line": call.lineno, "kind": kind})
    return sorted(found, key=lambda item: (int(item["line"]), str(item["kind"])))


def _uses_http_engine(tree: ast.AST) -> bool:
    """Return whether a module imports a public shared-engine helper."""
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "services.http_engine"
            and any(alias.name in HTTP_ENGINE_FUNCTIONS for alias in node.names)
        ):
            return True
        if isinstance(node, ast.Import) and any(alias.name == "services.http_engine" for alias in node.names):
            return True
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "services"
            and any(alias.name == "http_engine" for alias in node.names)
        ):
            return True
    return False


def _runtime_signals(source: str, filename: str) -> dict[str, object]:
    """Extract executable path/fetch signals from one parsed Python module."""
    tree = ast.parse(source, filename=filename)
    docs = _docstring_nodes(tree)
    live_strings = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docs
    ]
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    direct_http_calls = _direct_http_calls(tree)
    uses_engine = _uses_http_engine(tree)
    browser_ua = any("Mozilla/5.0" in value for value in live_strings) or any(
        isinstance(call.func, ast.Name)
        and call.func.id == "polite_headers"
        and any(
            keyword.arg == "browser" and isinstance(keyword.value, ast.Constant) and keyword.value.value is True
            for keyword in call.keywords
        )
        for call in calls
    )
    return {
        "local_paths": sorted({match.group(0) for value in live_strings for match in LOCAL_PATH_RE.finditer(value)})[
            :4
        ],
        "bare_requests": bool(direct_http_calls),
        "direct_http_calls": direct_http_calls,
        "uses_engine": uses_engine,
        "gov_ie": any("gov.ie" in value for value in live_strings),
        "browser_ua": browser_ua,
    }


def discover_runtime_files() -> list[Path]:
    """Return the deterministic Python surface exercised by refresh pipelines."""
    discovered: set[Path] = set()
    for directory in RUNTIME_DIRS:
        if directory.exists():
            discovered.update(path for path in directory.rglob("*.py") if "__pycache__" not in path.parts)
    discovered.update(PROJECT_ROOT.glob("*_refresh.py"))
    discovered.add(PROJECT_ROOT / "pipeline.py")
    discovered.update(PROJECT_ROOT / rel for rel in PIPELINE_TOOL_FILES)
    return sorted(path for path in discovered if path.is_file())


def classify_host(host: str) -> tuple[str, str]:
    h = host.lower()
    for needle, band, klass in HOST_RULES:
        if needle in h:
            return band, klass
    if h.endswith(COUNCIL_SUFFIX):
        return "MEDIUM", "COUNCIL"
    return "LOW", "OTHER"


def load_registry() -> list[dict]:
    if not REGISTRY.exists():
        print(f"registry not found: {REGISTRY} — run tools/build_source_registry.py", file=sys.stderr)
        return []
    return json.loads(REGISTRY.read_text(encoding="utf-8")).get("sources", [])


def source_exposure(sources: list[dict]) -> dict:
    host_band: dict[str, str] = {}
    host_class: dict[str, str] = {}
    host_count: Counter = Counter()
    src_worst: list[tuple[str, str, str, str]] = []  # (source_id, band, class, host)

    for s in sources:
        urls = [s.get("listing_url")] + (s.get("direct_files") or [])
        worst = ("LOW", "OTHER", "")
        order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
        for u in urls:
            if not (u and str(u).startswith("http")):
                continue
            host = urlparse(u).netloc
            band, klass = classify_host(host)
            host_band[host], host_class[host] = band, klass
            host_count[host] += 1
            if order[band] > order[worst[0]]:
                worst = (band, klass, host)
        src_worst.append((s.get("source_id", "?"), *worst))

    band_totals = Counter(b for _, b, _, _ in src_worst)
    class_totals = Counter(k for _, _, k, _ in src_worst)
    return {
        "host_count": host_count,
        "host_band": host_band,
        "host_class": host_class,
        "src_worst": src_worst,
        "band_totals": band_totals,
        "class_totals": class_totals,
    }


def polling_coverage(sources: list[dict]) -> dict:
    pollable = sum(1 for s in sources if s.get("pollable"))
    wired = sum(1 for s in sources if s.get("parser_wired"))
    by_check = Counter(s.get("check_type", "none") for s in sources)
    health_status: Counter = Counter()
    if HEALTH.exists():
        h = json.loads(HEALTH.read_text(encoding="utf-8"))
        for rec in h.get("sources", []):
            health_status[rec.get("status", "unknown")] += 1
    return {
        "total": len(sources),
        "pollable": pollable,
        "wired": wired,
        "by_check": by_check,
        "health_status": health_status,
    }


def runtime_resilience(files: Iterable[Path] | None = None) -> dict:
    if files is None:
        files = discover_runtime_files()
    files = list(files)

    local_paths: dict[str, list[str]] = {}
    bare_requests: list[str] = []
    direct_http: dict[str, list[dict[str, object]]] = {}
    transport_exemptions: dict[str, dict[str, object]] = {}
    govie_no_ua: list[str] = []
    scan_errors: dict[str, str] = {}
    uses_engine = 0

    for path in sorted(files):
        try:
            rel = str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
        except ValueError:
            rel = str(path)
        try:
            with tokenize.open(path) as source_file:
                src = source_file.read()
            signals = _runtime_signals(src, rel)
        except (OSError, UnicodeError, SyntaxError) as exc:
            scan_errors[rel] = f"{type(exc).__name__}: {exc}"
            continue

        hits = signals["local_paths"]
        if hits:
            local_paths[rel] = hits
        if signals["bare_requests"]:
            calls = signals["direct_http_calls"]
            direct_http[rel] = calls
            rationale = DIRECT_HTTP_EXEMPTIONS.get(rel)
            if rationale is not None:
                transport_exemptions[rel] = {"reason": rationale, "calls": calls}
            else:
                bare_requests.append(rel)
        if signals["uses_engine"]:
            uses_engine += 1
        if signals["gov_ie"] and not signals["browser_ua"] and (signals["bare_requests"] or signals["uses_engine"]):
            govie_no_ua.append(rel)

    return {
        "n_files": len(files),
        "local_paths": local_paths,
        "bare_requests": bare_requests,
        "direct_http": direct_http,
        "transport_exemptions": transport_exemptions,
        "govie_no_ua": govie_no_ua,
        "uses_engine": uses_engine,
        "scan_errors": scan_errors,
    }


def render(sources: list[dict]) -> str:
    exp = source_exposure(sources)
    pol = polling_coverage(sources)
    res = runtime_resilience()

    out: list[str] = []
    w = out.append

    w("# Cloud-migration readiness — fragility gauge\n")
    w(
        "> Generated by `python tools/scan_cloud_readiness.py`. A measurement, not a "
        "gate. Re-run after any hardening pass to watch the numbers move.\n"
    )

    # ---- headline ----
    high = exp["band_totals"].get("HIGH", 0)
    med = exp["band_totals"].get("MEDIUM", 0)
    blockers = len(res["local_paths"])
    w("## Headline\n")
    w(
        f"- **{high} sources** sit behind a HIGH-risk host (gov.ie WAF) — these can "
        f"403 from a datacenter IP even with a browser-UA spoof."
    )
    w(f"- **{med} sources** on MEDIUM-risk hosts (council TLS quirks / archive.org).")
    w(f"- **{blockers} runtime modules** hardcode a `c:/tmp` or Windows path — hard blockers on Linux.")
    w(
        f"- **{len(res['bare_requests'])} runtime modules** bypass the resilient "
        f"HTTP engine without an approved stateful-transport rationale."
    )
    w(f"- **{len(res['scan_errors'])} runtime modules** could not be parsed; any such error fails the scan.")
    w(
        f"- Polling: **{pol['pollable']}/{pol['total']}** sources pollable; but only "
        f"**{pol['wired']}** have a wired parser.\n"
    )

    # ---- axis 1 ----
    w("## Axis 1 — source host exposure\n")
    w(
        "Each source graded by its worst host. WAF_GOVIE is the axis that actually "
        "decides which runner a source can live on.\n"
    )
    w("| Risk | Sources | Meaning |")
    w("|---|---:|---|")
    w(
        f"| HIGH (gov.ie WAF) | {exp['band_totals'].get('HIGH', 0)} | datacenter ASN may block even spoofed → residential/self-hosted runner only |"
    )
    w(f"| MEDIUM (council/archive) | {exp['band_totals'].get('MEDIUM', 0)} | flaky; needs curl fallback + retries |")
    w(f"| LOW (open APIs) | {exp['band_totals'].get('LOW', 0)} | cloud-safe |")
    w("")
    w("Top exposed hosts by source-URL count:\n")
    w("| Host | URLs | Risk | Class |")
    w("|---|---:|---|---|")
    for host, n in exp["host_count"].most_common(15):
        w(f"| `{host}` | {n} | {exp['host_band'][host]} | {exp['host_class'][host]} |")
    w("")

    # ---- axis 2 ----
    w("## Axis 2 — polling / canary coverage\n")
    w("A source with no poller and no canary goes stale silently once nobody is watching the laptop.\n")
    w("| Measure | Count |")
    w("|---|---:|")
    w(f"| Sources total | {pol['total']} |")
    w(f"| Pollable (freshness checkable) | {pol['pollable']} |")
    w(f"| Parser wired into pipeline | {pol['wired']} |")
    w("")
    w("By check type:\n")
    w("| check_type | Sources |")
    w("|---|---:|")
    for ct, n in pol["by_check"].most_common():
        w(f"| {ct} | {n} |")
    if pol["health_status"]:
        w("\nLast recorded `source_health.json` status:\n")
        w("| Status | Sources |")
        w("|---|---:|")
        for st, n in pol["health_status"].most_common():
            w(f"| {st} | {n} |")
    w("")

    # ---- axis 3 ----
    w("## Axis 3 — runtime resilience\n")
    w(
        f"Scanned {res['n_files']} modules across runtime packages, root refresh "
        f"orchestrators and pipeline tools. "
        f"{res['uses_engine']} use the `fetch_bytes` engine (retry + curl fallback + "
        f"WAF-spoof option).\n"
    )

    w(f"### Hard blockers — {len(res['local_paths'])} modules with hardcoded local paths\n")
    w(
        "Each of these writes or reads a Windows/`c:/tmp` path that does not exist on "
        "a Linux cloud runner. Parameterise via env/config before migrating.\n"
    )
    w("| Module | Path literal(s) |")
    w("|---|---|")
    for rel, hits in sorted(res["local_paths"].items()):
        w(f"| `{rel}` | {', '.join(f'`{h.strip(chr(34))}`' for h in hits)} |")
    w("")

    if res["scan_errors"]:
        w(f"### Scanner errors â€” {len(res['scan_errors'])} modules were not inspected\n")
        for rel, error in sorted(res["scan_errors"].items()):
            w(f"- `{rel}`: {error}")
        w("")

    w(f"### Fragile fetches — {len(res['bare_requests'])} modules bypass the shared HTTP engine\n")
    w(
        "Bare `requests`/`urlopen`: no shared-session retry, no curl fallback, no "
        "WAF-interstitial validation. Fine for a CKAN API resolve; risky for a file "
        "download from a WAF'd host.\n"
    )
    for rel in sorted(res["bare_requests"]):
        calls = ", ".join(f"{call['kind']}:{call['line']}" for call in res["direct_http"][rel])
        w(f"- `{rel}` — {calls}")
    w("")

    if res["transport_exemptions"]:
        w(f"### Reviewed stateful transports — {len(res['transport_exemptions'])}\n")
        w(
            "These modules intentionally own response/session state that the generic helpers "
            "do not expose. Their rationale is part of the generated report so suppressions "
            "cannot become invisible.\n"
        )
        for rel, exemption in sorted(res["transport_exemptions"].items()):
            calls = ", ".join(f"{call['kind']}:{call['line']}" for call in exemption["calls"])
            w(f"- `{rel}` — {exemption['reason']} ({calls})")
        w("")

    if res["govie_no_ua"]:
        w(f"### gov.ie fetches with no browser-UA in the module — {len(res['govie_no_ua'])}\n")
        w(
            "These touch a gov.ie host but the module shows no browser-UA spoof. Verify "
            "each threads `polite_headers(browser=True)` or it will 403.\n"
        )
        for rel in sorted(res["govie_no_ua"]):
            w(f"- `{rel}`")
        w("")

    # ---- how to read ----
    w("## How to read this before migrating\n")
    w(
        "1. **HIGH-risk sources decide the runner.** As long as any gov.ie-WAF source "
        "must refresh, part of the pipeline needs a residential or self-hosted runner "
        "— a pure GitHub-hosted / cloud-datacenter refresh cannot cover them. Splitting "
        "the pipeline by risk band is the real decision, not a code fix.\n"
    )
    w(
        "2. **The c:/tmp blockers are the migration checklist.** Every module above must "
        "take its path from config/env before a headless Linux run will work end-to-end. "
        "This is the concrete, finite list behind the 'can pipeline.py run headless?' "
        "question that is still unproven for a write-run.\n"
    )
    w(
        "3. **The bare-requests list is the resilience backlog.** Route file downloads "
        "through `fetch_bytes` so a WAF 403 degrades to a curl fallback instead of "
        "crashing the chain.\n"
    )
    w(
        "4. **Polling coverage is your only cloud early-warning.** Sources that are not "
        "pollable will fail silently once the laptop is not watched daily.\n"
    )

    return "\n".join(out)


def main() -> int:
    # Windows PowerShell commonly exposes a cp1252 stream; the report contains
    # civic names and arrows that are not representable there. Configure the
    # existing streams instead of requiring every caller to set environment
    # variables before a scanner can run.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path)
    ap.add_argument("--json", type=Path)
    args = ap.parse_args()

    sources = load_registry()
    if not sources:
        return 0

    report = render(sources)

    json_path = (PROJECT_ROOT / args.json).resolve() if args.json and not args.json.is_absolute() else args.json
    out_path = (PROJECT_ROOT / args.out).resolve() if args.out and not args.out.is_absolute() else args.out

    if json_path:
        exp = source_exposure(sources)
        payload = {
            "band_totals": dict(exp["band_totals"]),
            "class_totals": dict(exp["class_totals"]),
            "host_count": dict(exp["host_count"]),
            "polling": {k: (dict(v) if isinstance(v, Counter) else v) for k, v in polling_coverage(sources).items()},
            "resilience": {k: (v if isinstance(v, (int, list)) else v) for k, v in runtime_resilience().items()},
        }
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print(f"[wrote {json_path}]", file=sys.stderr)

    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print(f"[wrote {out_path}]", file=sys.stderr)
    else:
        sys.stdout.write(report)
    return 2 if runtime_resilience()["scan_errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
