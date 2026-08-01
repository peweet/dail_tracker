#!/usr/bin/env python
"""One capture harness for the Dáil Tracker UI — replaces the 88 ad-hoc
`audit_screenshots/_*.py` probes (archived 2026-08-01 to
`c:\\tmp\\audit_screenshots_pre_consolidation_2026-08-01.zip`).

Why one script: every retired probe re-implemented the same four things —
launch chromium, `goto`, `time.sleep(9)`, `screenshot` — against a hardcoded
port and a hardcoded route list. Both drifted. Ports ranged over eight values
(8501/8631/8534/8533/8599/8596/8590/8536) and no probe knew when the app had
actually finished rendering, so captures raced Streamlit's rerun.

Two things here are not in any of the originals:

1. **Routes are derived from `utility/app.py`, never restated.** `discover_routes()`
   AST-parses the `st.Page(...)` calls, so a renamed `url_path` can't leave a
   stale route behind. `routes --check` fails loudly if a route stops resolving.
2. **Readiness is measured, not slept through.** `settle()` polls Streamlit's
   status widget, skeleton count, and DOM size until they hold steady across
   consecutive polls, then waits on `document.fonts.ready`. Fixed sleeps both
   flaked and wasted ~9s per page; this returns as soon as the frame is stable
   and reports honestly when it times out instead of screenshotting a half-page.

    python tools/ui_capture.py routes                      # list discovered routes
    python tools/ui_capture.py capture --label wave1       # every route, both viewports
    python tools/ui_capture.py capture --route rankings-votes --viewport desktop
    python tools/ui_capture.py capture --baseline          # (re)write the diff baseline
    python tools/ui_capture.py diff --label wave1          # vs baseline, pixel-ratio report
    python tools/ui_capture.py a11y --label wave1          # axe-core violations per route
    python tools/ui_capture.py probe --route rankings-votes --js "..."

Assumes a Streamlit server is already up (`streamlit run utility/app.py`); pass
`--serve` to have the harness start and stop one itself. Booting the app costs
~1 GB — see the memory note in CLAUDE.md before running a full sweep.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import services.runtime_env  # noqa: E402, F401  isort:skip  (BLAS cap; first project import at every entry point)

import argparse  # noqa: E402
import ast  # noqa: E402
import contextlib  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import socket  # noqa: E402
import subprocess  # noqa: E402
import time  # noqa: E402
from dataclasses import dataclass  # noqa: E402

OUT_ROOT = REPO / "audit_screenshots"
BASELINE_DIR = OUT_ROOT / "baseline"
RUNS_DIR = OUT_ROOT / "runs"
APP_PY = REPO / "utility" / "app.py"

DEFAULT_BASE = os.environ.get("DT_UI_BASE", "http://localhost:8501")

# Shaped as `browser.new_context(**kwargs)`. device_scale_factor 2 matches the
# retina captures the originals used most often; mobile width 390 is the
# iPhone 14/15 logical viewport.
VIEWPORTS: dict[str, dict[str, object]] = {
    "desktop": {
        "viewport": {"width": 1440, "height": 900},
        "device_scale_factor": 2,
    },
    "mobile": {
        "viewport": {"width": 390, "height": 844},
        "device_scale_factor": 2,
        "is_mobile": True,
        "has_touch": True,
    },
}


@dataclass(frozen=True)
class Route:
    url_path: str
    title: str
    hidden: bool
    default: bool


# ── route discovery ───────────────────────────────────────────────────────────


def discover_routes(app_py: Path = APP_PY) -> list[Route]:
    """AST-parse `st.Page(...)` calls out of app.py.

    Parsed rather than imported: importing app.py would execute
    `st.set_page_config` / `st.navigation` outside a Streamlit runtime and drag
    in every page module (and their data deps) just to read a route table.
    """
    tree = ast.parse(app_py.read_text(encoding="utf-8"))
    routes: list[Route] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name != "Page":
            continue
        kwargs = {k.arg: k.value for k in node.keywords if k.arg}

        def literal(key: str, kw=kwargs):
            value = kw.get(key)
            return value.value if isinstance(value, ast.Constant) else None

        url_path = literal("url_path")
        if url_path is None:
            continue
        routes.append(
            Route(
                url_path=url_path,
                title=literal("title") or url_path,
                hidden=literal("visibility") == "hidden",
                default=literal("default") is True,
            )
        )
    return sorted(routes, key=lambda r: r.url_path)


def select_routes(routes: list[Route], *, only: list[str] | None, include_hidden: bool) -> list[Route]:
    if only:
        wanted = {r.strip("/") for r in only}
        chosen = [r for r in routes if r.url_path in wanted]
        missing = wanted - {r.url_path for r in chosen}
        if missing:
            raise SystemExit(f"unknown route(s): {', '.join(sorted(missing))}")
        return chosen
    return [r for r in routes if include_hidden or not r.hidden]


# ── server ────────────────────────────────────────────────────────────────────


def _port_open(base: str) -> bool:
    host, _, port = base.removeprefix("http://").removeprefix("https://").partition(":")
    try:
        with socket.create_connection((host, int(port or 80)), timeout=1.5):
            return True
    except OSError:
        return False


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class Server:
    """Attach to a running app, or start one for the duration of the run."""

    def __init__(self, base: str, *, serve: bool) -> None:
        self.base = base
        self._serve = serve
        self._proc: subprocess.Popen | None = None

    def __enter__(self) -> str:
        if not self._serve:
            if not _port_open(self.base):
                raise SystemExit(
                    f"no Streamlit server at {self.base}.\n"
                    f"  start one:  .venv/Scripts/streamlit run utility/app.py\n"
                    f"  or re-run with --serve to have the harness start it."
                )
            return self.base
        port = _free_port()
        self.base = f"http://localhost:{port}"
        env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
        self._proc = subprocess.Popen(
            [
                str(REPO / ".venv" / "Scripts" / "streamlit"), "run", str(APP_PY),
                "--server.port", str(port), "--server.headless", "true",
            ],
            cwd=REPO, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if _port_open(self.base):
                time.sleep(2)  # the socket opens slightly before the app serves
                return self.base
            if self._proc.poll() is not None:
                raise SystemExit("streamlit exited while starting; run it manually to see the error")
            time.sleep(1)
        raise SystemExit(f"streamlit did not come up on {self.base} within 120s")

    def __exit__(self, *exc) -> None:
        if self._proc is not None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self._proc.kill()


# ── readiness ─────────────────────────────────────────────────────────────────

_STATE_JS = """() => {
  const main = document.querySelector('[data-testid="stAppViewContainer"]');
  const status = document.querySelector('[data-testid="stStatusWidget"]');
  return {
    busy: !!status && status.offsetParent !== null,
    skeletons: document.querySelectorAll('[data-testid="stSkeleton"], .stSkeleton').length,
    spinners: document.querySelectorAll('[data-testid="stSpinner"]').length,
    nodes: main ? main.querySelectorAll('*').length : 0,
    chars: main ? main.innerText.length : 0,
  };
}"""

_DISMISS_JS = """() => {
  // Streamlit's "Page not found" / rerun dialogs intercept clicks and sit on
  // top of the content, so they poison both screenshots and DOM probes.
  let closed = 0;
  for (const btn of document.querySelectorAll('[data-testid="stDialog"] button[aria-label="Close"]')) {
    btn.click(); closed++;
  }
  for (const t of document.querySelectorAll('[data-testid="stToast"]')) { t.remove(); closed++; }
  return closed;
}"""


def settle(page, *, timeout: float = 45.0, stable_polls: int = 3, interval: float = 0.4) -> dict:
    """Block until the frame stops changing. Returns the final state plus `settled`.

    Streamlit streams a page in over the websocket, so `load` fires long before
    the content exists. Stability across consecutive polls is the only honest
    signal available without app-side instrumentation.
    """
    deadline = time.monotonic() + timeout
    previous: tuple | None = None
    stable = 0
    state: dict = {}
    while time.monotonic() < deadline:
        try:
            state = page.evaluate(_STATE_JS)
        except Exception:  # navigation mid-poll
            time.sleep(interval)
            continue
        signature = (state["nodes"], state["chars"])
        quiet = not state["busy"] and not state["skeletons"] and not state["spinners"] and state["nodes"] > 0
        if quiet and signature == previous:
            stable += 1
            if stable >= stable_polls:
                page.evaluate(_DISMISS_JS)
                with contextlib.suppress(Exception):
                    page.evaluate("() => document.fonts.ready")
                return {**state, "settled": True}
        else:
            stable = 0
        previous = signature
        time.sleep(interval)
    return {**state, "settled": False}


def open_route(page, base: str, route: Route, *, timeout: float) -> dict:
    url = f"{base}/{route.url_path}"
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    state = settle(page, timeout=timeout)
    state["url"] = url
    return state


# ── commands ──────────────────────────────────────────────────────────────────


def cmd_routes(args) -> int:
    routes = discover_routes()
    if not args.check:
        for r in routes:
            flags = " ".join(f for f in ("hidden" if r.hidden else "", "default" if r.default else "") if f)
            print(f"  /{r.url_path:<34} {r.title:<28} {flags}")
        print(f"\n{len(routes)} route(s) — {sum(1 for r in routes if r.hidden)} hidden")
        return 0

    from playwright.sync_api import sync_playwright

    selected = select_routes(routes, only=None, include_hidden=True)
    broken: list[str] = []
    with Server(args.base, serve=args.serve) as base, sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(viewport={"width": 1440, "height": 900}).new_page()
        for route in selected:
            state = open_route(page, base, route, timeout=args.timeout)
            body = page.evaluate("() => document.body.innerText.slice(0, 400)")
            ok = state["settled"] and "Page not found" not in body
            print(f"  {'ok  ' if ok else 'FAIL'} /{route.url_path}")
            if not ok:
                broken.append(route.url_path)
        browser.close()
    if broken:
        print(f"\n{len(broken)} route(s) did not resolve: {', '.join(broken)}")
        return 1
    print(f"\nall {len(selected)} routes resolve")
    return 0


def _capture_dir(args) -> Path:
    return BASELINE_DIR if args.baseline else RUNS_DIR / args.label


def cmd_capture(args) -> int:
    from playwright.sync_api import sync_playwright

    routes = select_routes(discover_routes(), only=args.route, include_hidden=args.include_hidden)
    viewports = [args.viewport] if args.viewport else list(VIEWPORTS)
    out_dir = _capture_dir(args)
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    with Server(args.base, serve=args.serve) as base, sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for vp_name in viewports:
            ctx = browser.new_context(**VIEWPORTS[vp_name])
            page = ctx.new_page()
            for route in routes:
                started = time.monotonic()
                state = open_route(page, base, route, timeout=args.timeout)
                path = out_dir / f"{route.url_path}__{vp_name}.png"
                page.screenshot(path=str(path), full_page=args.full_page)
                results.append(
                    {
                        "route": route.url_path, "viewport": vp_name, "settled": state["settled"],
                        "nodes": state.get("nodes"), "chars": state.get("chars"),
                        "seconds": round(time.monotonic() - started, 1), "path": str(path.relative_to(REPO)),
                    }
                )
                flag = "" if state["settled"] else "  [TIMED OUT — capture may be mid-render]"
                print(f"  {vp_name:<8} /{route.url_path:<34} {results[-1]['seconds']:>5.1f}s{flag}")
            ctx.close()
        browser.close()

    (out_dir / "report.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    stalled = [r for r in results if not r["settled"]]
    print(f"\n{len(results)} capture(s) → {out_dir.relative_to(REPO)}")
    if stalled:
        print(f"{len(stalled)} did not settle: {', '.join(sorted({r['route'] for r in stalled}))}")
    return 0


def cmd_diff(args) -> int:
    try:
        from PIL import Image, ImageChops
    except ImportError:
        raise SystemExit("diff needs Pillow: .venv/Scripts/python -m pip install Pillow") from None

    run_dir = RUNS_DIR / args.label
    if not run_dir.is_dir():
        raise SystemExit(f"no such run: {run_dir.relative_to(REPO)}")
    if not BASELINE_DIR.is_dir():
        raise SystemExit("no baseline yet — run: python tools/ui_capture.py capture --baseline")

    rows: list[dict] = []
    for shot in sorted(run_dir.glob("*.png")):
        base_shot = BASELINE_DIR / shot.name
        if not base_shot.exists():
            rows.append({"image": shot.name, "status": "new", "ratio": None})
            continue
        a, b = Image.open(base_shot).convert("RGB"), Image.open(shot).convert("RGB")
        if a.size != b.size:
            rows.append({"image": shot.name, "status": "size-changed", "ratio": None,
                         "detail": f"{a.size} → {b.size}"})
            continue
        diff = ImageChops.difference(a, b).convert("L")
        changed = sum(count for value, count in enumerate(diff.histogram()) if value > args.threshold)
        ratio = changed / (a.size[0] * a.size[1])
        rows.append({"image": shot.name, "status": "changed" if ratio > args.min_ratio else "same",
                     "ratio": round(ratio, 5)})

    for row in sorted(rows, key=lambda r: -(r["ratio"] or 1)):
        ratio = "  —  " if row["ratio"] is None else f"{row['ratio']:>7.3%}"
        print(f"  {row['status']:<13} {ratio}  {row['image']}{'  ' + row.get('detail', '') if row.get('detail') else ''}")
    (run_dir / "diff.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    moved = [r for r in rows if r["status"] in {"changed", "size-changed", "new"}]
    print(f"\n{len(moved)} of {len(rows)} image(s) differ from baseline")
    return 0


_AXE_PATHS = (
    REPO / "node_modules" / "axe-core" / "axe.min.js",
    REPO / "node_modules" / "axe-core" / "axe.js",
)


def cmd_a11y(args) -> int:
    from playwright.sync_api import sync_playwright

    axe_js = next((p for p in _AXE_PATHS if p.exists()), None)
    if axe_js is None:
        raise SystemExit("axe-core not installed. Run:  npm install --no-save axe-core")

    routes = select_routes(discover_routes(), only=args.route, include_hidden=args.include_hidden)
    out_dir = RUNS_DIR / args.label
    out_dir.mkdir(parents=True, exist_ok=True)
    source = axe_js.read_text(encoding="utf-8")

    findings: list[dict] = []
    with Server(args.base, serve=args.serve) as base, sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(**VIEWPORTS["desktop"]).new_page()
        for route in routes:
            open_route(page, base, route, timeout=args.timeout)
            page.evaluate(source)
            report = page.evaluate(
                """() => axe.run(document, {resultTypes: ['violations']})
                        .then(r => r.violations.map(v => ({
                            id: v.id, impact: v.impact, help: v.help,
                            nodes: v.nodes.length,
                            target: v.nodes.length ? String(v.nodes[0].target) : null})))"""
            )
            for violation in report:
                findings.append({"route": route.url_path, **violation})
            counts: dict[str, int] = {}
            for v in report:
                counts[v["impact"] or "unknown"] = counts.get(v["impact"] or "unknown", 0) + 1
            summary = ", ".join(f"{n} {impact}" for impact, n in sorted(counts.items())) or "clean"
            print(f"  /{route.url_path:<34} {summary}")
        browser.close()

    (out_dir / "a11y.json").write_text(json.dumps(findings, indent=2), encoding="utf-8")
    by_rule: dict[str, int] = {}
    for f in findings:
        by_rule[f["id"]] = by_rule.get(f["id"], 0) + f["nodes"]
    print(f"\n{len(findings)} violation instance(s) across {len(routes)} route(s)")
    for rule, n in sorted(by_rule.items(), key=lambda kv: -kv[1])[:12]:
        print(f"  {n:>5}  {rule}")
    print("\nNote: automated rules catch ~30-40% of WCAG issues; the rest needs a manual pass.")
    return 0


def cmd_probe(args) -> int:
    from playwright.sync_api import sync_playwright

    routes = select_routes(discover_routes(), only=args.route, include_hidden=True)
    expression = Path(args.js_file).read_text(encoding="utf-8") if args.js_file else args.js
    if not expression:
        raise SystemExit("probe needs --js or --js-file")

    with Server(args.base, serve=args.serve) as base, sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(**VIEWPORTS[args.viewport or "desktop"]).new_page()
        for route in routes:
            state = open_route(page, base, route, timeout=args.timeout)
            try:
                result = page.evaluate(expression)
            except Exception as exc:
                result = {"error": str(exc)}
            print(f"/{route.url_path}  settled={state['settled']}")
            print(f"  {json.dumps(result, ensure_ascii=False)}")
        browser.close()
    return 0


# ── cli ───────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def shared(sp, *, routes=True):
        sp.add_argument("--base", default=DEFAULT_BASE, help=f"app URL (default {DEFAULT_BASE})")
        sp.add_argument("--serve", action="store_true", help="start a Streamlit server for this run")
        sp.add_argument("--timeout", type=float, default=45.0, help="per-route settle timeout, seconds")
        if routes:
            sp.add_argument("--route", action="append", help="url_path (repeatable); default = all visible")
            sp.add_argument("--include-hidden", action="store_true", help="also capture visibility='hidden' routes")

    p_routes = sub.add_parser("routes", help="list routes discovered from app.py")
    p_routes.add_argument("--check", action="store_true", help="load each route and fail if any 404s")
    shared(p_routes, routes=False)
    p_routes.set_defaults(func=cmd_routes)

    p_cap = sub.add_parser("capture", help="screenshot routes")
    shared(p_cap)
    p_cap.add_argument("--label", default="latest", help="run name under audit_screenshots/runs/")
    p_cap.add_argument("--baseline", action="store_true", help="write to baseline/ instead of runs/<label>/")
    p_cap.add_argument("--viewport", choices=sorted(VIEWPORTS), help="default: every viewport")
    p_cap.add_argument("--full-page", action="store_true", help="capture the full scroll height")
    p_cap.set_defaults(func=cmd_capture)

    p_diff = sub.add_parser("diff", help="compare a run against the baseline")
    p_diff.add_argument("--label", default="latest")
    p_diff.add_argument("--threshold", type=int, default=12, help="per-channel delta counted as changed (0-255)")
    p_diff.add_argument("--min-ratio", type=float, default=0.001, help="changed-pixel ratio reported as a diff")
    p_diff.set_defaults(func=cmd_diff)

    p_a11y = sub.add_parser("a11y", help="run axe-core against each route")
    shared(p_a11y)
    p_a11y.add_argument("--label", default="latest")
    p_a11y.set_defaults(func=cmd_a11y)

    p_probe = sub.add_parser("probe", help="evaluate a JS expression on each route")
    shared(p_probe)
    p_probe.add_argument("--js", help="JS expression, e.g. \"() => document.querySelectorAll('.dt-card').length\"")
    p_probe.add_argument("--js-file", help="file holding the JS expression")
    p_probe.add_argument("--viewport", choices=sorted(VIEWPORTS))
    p_probe.set_defaults(func=cmd_probe)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
