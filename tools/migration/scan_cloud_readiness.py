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

Axis 3 — RUNTIME RESILIENCE (AST/grep over extractors/ + iris/)
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
import json
import re
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REGISTRY = PROJECT_ROOT / "data" / "_meta" / "source_registry.generated.json"
HEALTH = PROJECT_ROOT / "data" / "_meta" / "source_health.json"
RUNTIME_DIRS = [PROJECT_ROOT / "extractors", PROJECT_ROOT / "iris"]

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

LOCAL_PATH_RE = re.compile(r"""["'](?:[cC]:[\\/]tmp|/mnt/c|[cC]:[\\/]Users)[^"']*["']""")
GOVIE_IN_CODE_RE = re.compile(r"gov\.ie")
BROWSER_UA_RE = re.compile(r"browser\s*=\s*True|GOVIE_HEADERS|BROWSER_UA|Mozilla/5\.0")
BARE_REQUEST_RE = re.compile(r"\brequests\.(get|post|head)\s*\(|\burlopen\s*\(")
FETCH_ENGINE_RE = re.compile(r"fetch_bytes|http_engine")


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


def runtime_resilience() -> dict:
    files: list[Path] = []
    for d in RUNTIME_DIRS:
        if d.exists():
            files += [p for p in d.rglob("*.py") if "__pycache__" not in p.parts]

    local_paths: dict[str, list[str]] = {}
    bare_requests: list[str] = []
    govie_no_ua: list[str] = []
    uses_engine = 0

    for path in sorted(files):
        rel = str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
        try:
            src = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue

        hits = LOCAL_PATH_RE.findall(src)
        if hits:
            local_paths[rel] = sorted(set(hits))[:4]
        if BARE_REQUEST_RE.search(src):
            bare_requests.append(rel)
        if FETCH_ENGINE_RE.search(src):
            uses_engine += 1
        # a module that fetches gov.ie but never sets a browser UA
        if (
            GOVIE_IN_CODE_RE.search(src)
            and not BROWSER_UA_RE.search(src)
            and (BARE_REQUEST_RE.search(src) or FETCH_ENGINE_RE.search(src))
        ):
            govie_no_ua.append(rel)

    return {
        "n_files": len(files),
        "local_paths": local_paths,
        "bare_requests": bare_requests,
        "govie_no_ua": govie_no_ua,
        "uses_engine": uses_engine,
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
        f"`fetch_bytes` engine with bare `requests`."
    )
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
        f"Scanned {res['n_files']} modules in `extractors/` + `iris/`. "
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

    w(f"### Fragile fetches — {len(res['bare_requests'])} modules bypass `fetch_bytes`\n")
    w(
        "Bare `requests`/`urlopen`: no shared-session retry, no curl fallback, no "
        "WAF-interstitial validation. Fine for a CKAN API resolve; risky for a file "
        "download from a WAF'd host.\n"
    )
    for rel in sorted(res["bare_requests"]):
        w(f"- `{rel}`")
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
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path)
    ap.add_argument("--json", type=Path)
    args = ap.parse_args()

    sources = load_registry()
    if not sources:
        return 0

    report = render(sources)

    if args.json:
        exp = source_exposure(sources)
        payload = {
            "band_totals": dict(exp["band_totals"]),
            "class_totals": dict(exp["class_totals"]),
            "host_count": dict(exp["host_count"]),
            "polling": {k: (dict(v) if isinstance(v, Counter) else v) for k, v in polling_coverage(sources).items()},
            "resilience": {k: (v if isinstance(v, (int, list)) else v) for k, v in runtime_resilience().items()},
        }
        args.json.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print(f"[wrote {args.json}]", file=sys.stderr)

    if args.out:
        args.out.write_text(report, encoding="utf-8")
        print(f"[wrote {args.out}]", file=sys.stderr)
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
