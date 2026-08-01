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
import re  # noqa: E402
import socket  # noqa: E402
import subprocess  # noqa: E402
import time  # noqa: E402
from dataclasses import dataclass, replace  # noqa: E402

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


# Excluded by default on a whole-app sweep: these two boot the siting geospatial
# engine (PointScopedLayerStore, ~1.2 GB RSS) inside the Streamlit process. On a
# 16 GB box already carrying several Claude sessions that is the difference
# between a sweep and an OOM. Name them with --route to review them deliberately.
HEAVY_ROUTES = frozenset({"planning-siting-check", "planning-siting-assistant"})


def _slug_route(url_path: str) -> str:
    """Filename-safe form of a route, keeping any query string legible.
    "your-council?council=Cork%20City" -> "your-council__council-Cork-City"."""
    path, _, query = url_path.partition("?")
    if not query:
        return path
    from urllib.parse import unquote

    return f"{path}__{re.sub(r'[^A-Za-z0-9._-]+', '-', unquote(query)).strip('-')}"


def select_routes(
    routes: list[Route],
    *,
    only: list[str] | None,
    include_hidden: bool,
    exclude: list[str] | None = None,
    skip_heavy: bool = True,
) -> list[Route]:
    if only:
        # A route may carry a query string ("your-council?council=Cork City"). Several pages
        # render almost nothing until a selection is made — Your Council shows only its picker —
        # so without this the harness could screenshot the empty state and never the page under
        # review. The PATH is still validated against app.py; only the query rides along, and it
        # becomes part of the capture's name so two states of one route don't overwrite
        # each other. Added 2026-08-01.
        chosen: list[Route] = []
        missing: list[str] = []
        by_path = {r.url_path: r for r in routes}
        for raw in only:
            path, _, query = raw.strip("/").partition("?")
            base = by_path.get(path)
            if base is None:
                missing.append(path)
            elif query:
                chosen.append(replace(base, url_path=f"{path}?{query}"))
            else:
                chosen.append(base)
        if missing:
            raise SystemExit(f"unknown route(s): {', '.join(sorted(set(missing)))}")
        return chosen
    dropped = {r.strip("/") for r in (exclude or [])}
    if skip_heavy:
        dropped |= HEAVY_ROUTES
    return [r for r in routes if (include_hidden or not r.hidden) and r.url_path not in dropped]


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


# Density. NAVIGATION_GRAPH.md scores whether the entity travels; this scores
# whether the page is worth arriving at. Both are structural — neither judges
# taste — but "too dense / too sparse" stops being an impression once you can
# say how much information a screen actually carries.
#
# `screens` is the page's scroll height in viewports. `chars_per_screen` and
# `interactive_per_screen` normalise by it, so a long page is not punished for
# being long — only for being thin. `above_fold_*` answers rubric dimension 1
# (does the first screen carry data, or only navigation?).
_DENSITY_JS = """() => {
  const vh = window.innerHeight, doc = document.documentElement;
  const main = document.querySelector('[data-testid="stMain"]') || document.body;
  const inFold = (el) => { const r = el.getBoundingClientRect();
                           return r.top < vh && r.bottom > 0; };
  // A container div that merely INTERSECTS the fold contributes its whole
  // innerText, including everything below it — first cut reported 384 figures
  // on a legislation fold showing ~30. Count only leaf elements whose own top
  // is on screen.
  const foldLeaf = (el) => el.childElementCount === 0 &&
                           el.getBoundingClientRect().top < vh &&
                           el.getBoundingClientRect().bottom > 0;
  const interactive = [...main.querySelectorAll('a,button,input,select,textarea,[role="button"]')];
  const scrollH = Math.max(doc.scrollHeight, main.scrollHeight);
  const text = (main.innerText || '').trim();
  // Digit runs are the cheapest proxy for "a figure is on screen": this app's
  // job is showing numbers, so a first screen with none is navigation-only.
  const foldText = [...main.querySelectorAll('p,span,td,div,h1,h2,h3,li,a,strong,b')]
      .filter(foldLeaf).map(e => e.textContent || '').join(' ');
  return {
    screens: +(scrollH / vh).toFixed(2),
    chars: text.length,
    interactive: interactive.length,
    above_fold_interactive: interactive.filter(inFold).length,
    above_fold_chars: foldText.trim().length,
    above_fold_figures: (foldText.match(/\\d[\\d,.]*/g) || []).length,
  };
}"""


def measure_density(page) -> dict:
    """Structural density for one rendered page. Never raises."""
    try:
        d = page.evaluate(_DENSITY_JS)
    except Exception:
        return {}
    screens = d.get("screens") or 1
    d["chars_per_screen"] = int(d.get("chars", 0) / screens)
    d["interactive_per_screen"] = round(d.get("interactive", 0) / screens, 1)
    return d


def open_route(page, base: str, route: Route, *, timeout: float, click: str | None = None) -> dict:
    url = f"{base}/{route.url_path}"
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    state = settle(page, timeout=timeout)
    if click:
        # st.tabs is CLIENT-side: only the first panel has a layout box, so a screenshot of a
        # tabbed page has never shown anything past tab 1 — the IPAS map and the entitlements
        # section were unreviewable for that reason alone (found 2026-08-01). A ?query cannot
        # reach them either, because st.tabs keeps no URL state. Clicking the tab by its
        # visible text is the only handle the DOM offers.
        try:
            page.get_by_text(click, exact=False).first.click(timeout=8_000)
            state = settle(page, timeout=timeout)
        except Exception as exc:  # noqa: BLE001 — report it, never fail the whole sweep
            state["click_error"] = f"{type(exc).__name__}: {exc}"[:160]
        state["clicked"] = click
    state["url"] = url
    return state


def scroll_to(page, text: str, *, timeout: float = 8_000) -> str:
    """Bring the element whose text matches into view before a screenshot.

    Streamlit renders into a scroll CONTAINER, not the document, so Playwright's
    `full_page=True` still yields one viewport — a component below the fold cannot be seen at
    all (the IPAS cartogram sits under three cards and a stat strip). Scrolling to the thing
    under review is the only way to photograph it. Returns '' on success, else the error.
    """
    try:
        page.get_by_text(text, exact=False).first.scroll_into_view_if_needed(timeout=timeout)
        page.wait_for_timeout(350)  # let the smooth-scroll settle before the shutter
        return ""
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"[:160]


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

    routes = select_routes(
        discover_routes(), only=args.route, include_hidden=args.include_hidden,
        exclude=args.exclude, skip_heavy=not args.include_heavy,
    )
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
                state = open_route(page, base, route, timeout=args.timeout, click=args.click)
                if args.scroll_to:
                    err = scroll_to(page, args.scroll_to)
                    if err:
                        state["scroll_error"] = err
                # A route carrying a query string ("your-council?council=Cork City") cannot go
                # into a filename as-is — '?' is illegal on Windows and '=' / '%' read badly.
                path = out_dir / f"{_slug_route(route.url_path)}__{vp_name}.png"
                page.screenshot(path=str(path), full_page=args.full_page)
                density = measure_density(page)
                results.append(
                    {
                        "route": route.url_path, "viewport": vp_name, "settled": state["settled"],
                        "nodes": state.get("nodes"), "chars": state.get("chars"),
                        "seconds": round(time.monotonic() - started, 1), "path": str(path.relative_to(REPO)),
                        **density,
                    }
                )
                flag = "" if state["settled"] else "  [TIMED OUT — capture may be mid-render]"
                shape = (
                    f"  {density['screens']:>5.1f} screens  {density['chars_per_screen']:>5} ch/scr"
                    f"  fold: {density['above_fold_figures']:>3} figures"
                    if density else ""
                )
                print(f"  {vp_name:<8} /{route.url_path:<34} {results[-1]['seconds']:>5.1f}s{shape}{flag}")
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

# Attribution matters more than the raw count. The first real run on
# /rankings-lobbying returned 80 violation instances, of which 71 targeted
# `.st-emotion-cache-*` (Streamlit's generated DOM) and 8 targeted
# `.rc-overflow-item` (the React library behind st.multiselect) — neither is
# fixable without patching Streamlit. Exactly one, `.site-banner-sub`, was ours.
# An unattributed report reads as "80 accessibility problems" and buries the one
# that can actually be fixed.
OUR_CLASS_PREFIXES = (
    "dt-", "site-", "section-", "stat-", "lob-", "lp3-", "att-", "part-", "pay-", "int-",
    "mo-", "leg-", "q-", "vt-", "cmt-", "don-", "e24-", "jud-", "pr-", "mf-", "pp-",
    "con-", "hou-", "lg-", "sup-", "td-",
)
VENDOR_MARKERS = (
    "st-emotion-cache", 'data-testid="st', "stApp", "rc-overflow", "rc-virtual", "glide-", "katex",
    # Streamlit's short-form emotion classes (.st-ag, .st-c5) and its generated
    # tab ids (#tabs-bui11-tab-0). Without these they land in `unknown` and
    # inflate the "check by hand" bucket with things we cannot fix anyway.
    "#tabs-bui",
)
RE_VENDOR_SHORT_CLASS = re.compile(r"^\.st-[a-z0-9]{1,3}\b")


def leaf(target: str) -> str:
    """The element axe actually flagged, not its ancestor chain.

    axe reports a full descendant path. Matching anywhere in that string
    misattributes every violation on a Streamlit container that happens to
    contain one of our cards — the whole point is to know which element must
    change, and that is the last segment.
    """
    tip = (target or "").split(">")[-1].strip()
    # Strip per-instance qualifiers so 31 council cards group as ONE finding
    # rather than 31 — `.dt-card-link[href="?council=Donegal"]` and
    # `:nth-child(4)` are the same element class with different data in it.
    tip = re.sub(r"\[[^\]]*\]|:nth-child\([^)]*\)|:nth-of-type\([^)]*\)", "", tip)
    return tip.strip()


def attribute(target: str) -> str:
    """'ours' | 'vendor' | 'unknown' for one axe target selector."""
    tip = leaf(target)
    if tip:
        if any(f".{prefix}" in tip for prefix in OUR_CLASS_PREFIXES):
            return "ours"
        if any(marker in tip for marker in VENDOR_MARKERS) or RE_VENDOR_SHORT_CLASS.match(tip):
            return "vendor"
    # The leaf can be a bare, unclassed element (a plain <div>) whose only
    # identifying wrapper is an ANCESTOR segment axe already trimmed off by
    # leaf() — e.g. `.rc-overflow-item:nth-child(3) > div[aria-haspopup="true"]`
    # leaves just `div`. Fall back to the full chain for vendor markers only:
    # "ours" stays leaf-scoped (a vendor container that happens to wrap one of
    # our cards must not over-attribute blame to us — see the docstring above).
    if any(marker in target for marker in VENDOR_MARKERS):
        return "vendor"
    return "unknown"


def cmd_a11y(args) -> int:
    from playwright.sync_api import sync_playwright

    axe_js = next((p for p in _AXE_PATHS if p.exists()), None)
    if axe_js is None:
        raise SystemExit("axe-core not installed. Run:  npm install --no-save axe-core")

    routes = select_routes(
        discover_routes(), only=args.route, include_hidden=args.include_hidden,
        exclude=args.exclude, skip_heavy=not args.include_heavy,
    )
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
                            targets: v.nodes.map(n => String(n.target))})))"""
            )
            ours = 0
            for violation in report:
                for target in violation["targets"]:
                    findings.append(
                        {
                            "route": route.url_path, "id": violation["id"],
                            "impact": violation["impact"], "help": violation["help"],
                            "target": target, "owner": attribute(target),
                        }
                    )
                    ours += attribute(target) == "ours"
            total = sum(len(v["targets"]) for v in report)
            print(f"  /{route.url_path:<34} {ours} ours / {total} total")
        browser.close()

    (out_dir / "a11y.json").write_text(json.dumps(findings, indent=2), encoding="utf-8")
    mine = [f for f in findings if f["owner"] == "ours"]
    print(f"\n{len(mine)} actionable of {len(findings)} instance(s) across {len(routes)} route(s)")
    for owner in ("vendor", "unknown"):
        n = sum(1 for f in findings if f["owner"] == owner)
        if n:
            print(f"  ({n} {owner} — Streamlit's own DOM, not fixable here)" if owner == "vendor"
                  else f"  ({n} unattributed — check the selector by hand)")
    if mine:
        # Grouped by rule + element: 71 instances of one rule on one card class is
        # ONE thing to fix, and printing it 71 times hides the other findings.
        grouped: dict[tuple[str, str, str], int] = {}
        for f in mine:
            key = (f["impact"] or "unknown", f["id"], leaf(f["target"]))
            grouped[key] = grouped.get(key, 0) + 1
        print("\nOURS (grouped by rule + element):")
        for (impact, rule, element), n in sorted(grouped.items(), key=lambda kv: -kv[1]):
            print(f"  [{impact}] {rule:<18} x{n:<4} {element}")
    print("\nNote: automated rules catch ~30-40% of WCAG issues; the rest needs a manual pass.")
    return 0


def cmd_probe(args) -> int:
    from playwright.sync_api import sync_playwright

    routes = select_routes(
        discover_routes(), only=args.route, include_hidden=True,
        exclude=args.exclude, skip_heavy=not args.include_heavy,
    )
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
            sp.add_argument("--exclude", action="append", help="url_path to skip (repeatable)")
            sp.add_argument(
                "--include-heavy", action="store_true",
                help=f"also sweep the siting routes ({', '.join(sorted(HEAVY_ROUTES))}) — ~1.2 GB each",
            )

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
    p_cap.add_argument(
        "--click",
        help="visible text to click before capturing, e.g. a st.tabs label "
        '("Where people are housed") — those panels have no URL state and are otherwise unreachable',
    )
    p_cap.add_argument(
        "--scroll-to",
        help="visible text to scroll into view before the shutter — Streamlit scrolls a "
        "CONTAINER, so --full-page cannot reach a component below the fold",
    )
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
