"""Verify each claimed edge in doc/NAVIGATION_GRAPH.md on its CORRECT detail
state (auto-seeded), since contextual edges are state-indexed.

Self-contained: launches its OWN streamlit server as a child process (the box
idle-reaps detached servers between tool calls), waits for health, runs the
checks, and tears the server down in finally. Run: python _nav_graph_edges.py
"""

from __future__ import annotations

import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import sync_playwright

REPO = Path(__file__).resolve().parents[1]
PORT = 8646
BASE = f"http://127.0.0.1:{PORT}"
HOST = f"127.0.0.1:{PORT}"


def wait_health(timeout=150) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{BASE}/_stcore/health", timeout=3) as r:
                if r.status == 200:
                    return True
        except Exception:  # noqa: BLE001
            time.sleep(2)
    return False


def harvest(page):
    return page.evaluate(
        "() => [...document.querySelectorAll('a')].map(a => a.getAttribute('href')||'')"
    )


def goto(page, url, settle=10):
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    time.sleep(settle)


def edges_to(hrefs, slug, param):
    hits = []
    for h in hrefs:
        if not h or h.startswith(("mailto:", "#")):
            continue
        if h.startswith("?"):
            tslug, q = slug, parse_qs(h[1:])
        else:
            pr = urlparse(h)
            if pr.scheme in ("http", "https") and pr.netloc != HOST:
                continue
            tslug, q = pr.path.lstrip("/").rstrip("/"), parse_qs(pr.query)
        if tslug == slug and param in q:
            hits.append(h)
    return hits


def first_param_val(hrefs, param):
    for h in hrefs:
        q = parse_qs(urlparse(h).query) or (parse_qs(h[1:]) if h.startswith("?") else {})
        if param in q:
            return q[param][0]
    return None


def run_checks():
    results = []
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        page = b.new_context(viewport={"width": 1440, "height": 4200}).new_page()

        goto(page, f"{BASE}/member-overview", 11)
        mcode = first_param_val(harvest(page), "member")
        goto(page, f"{BASE}/rankings-public-payments", 11)
        psup = first_param_val(harvest(page), "supplier")
        goto(page, f"{BASE}/rankings-committees", 9)
        comm = first_param_val(harvest(page), "committee")

        def check(label, url, slug, param, expect):
            goto(page, url, 11)
            hits = edges_to(harvest(page), slug, param)
            present = len(hits) > 0
            verdict = "OK" if present == expect else "!! MISMATCH"
            results.append((label, "PRESENT" if present else "ABSENT",
                            "present" if expect else "absent", verdict, hits[:1]))
            return present

        mh = []
        if mcode:
            goto(page, f"{BASE}/member-overview?member={mcode}", 12)
            mh = harvest(page)
            for slug, param, lbl, exp in [
                ("rankings-legislation", "bill", "member -> legislation(bill) [FIX #2]", True),
                ("rankings-statutory-instruments", "si", "member -> statutory-instruments(si)", None),
                ("rankings-votes", "vote", "member -> votes(vote)", None),
            ]:
                hits = edges_to(mh, slug, param)
                present = bool(hits)
                verdict = "OK" if (exp is None or present == exp) else "!! MISMATCH"
                results.append((lbl, "PRESENT" if present else "ABSENT",
                                "present" if exp else ("info" if exp is None else "absent"),
                                verdict, hits[:1]))
            bill = first_param_val([h for h in mh if "bill=" in h], "bill")
            if bill:
                check("legislation(bill) -> member(member) [sponsor back-edge]",
                      f"{BASE}/rankings-legislation?bill={bill}",
                      "member-overview", "member", True)
            si = first_param_val([h for h in mh if "si=" in h], "si")
            if si:
                check("statutory-instruments(si) -> member(member) [minister back-edge]",
                      f"{BASE}/rankings-statutory-instruments?si={si}",
                      "member-overview", "member", True)

        if psup:
            check("public-payments(supplier) -> company(supplier) [FIX #1]",
                  f"{BASE}/rankings-public-payments?supplier={psup}",
                  "company", "supplier", True)

        if comm:
            check("committees(committee) -> member(member) [#4 claim=MISSING]",
                  f"{BASE}/rankings-committees?committee={comm}",
                  "member-overview", "member", False)

        b.close()

    print("\n" + "=" * 80)
    print("NAV GRAPH — per-edge verification on correct detail states")
    print("=" * 80)
    print(f"seeds: member={mcode!r} pp_supplier={psup!r} committee={comm!r}\n")
    mism = 0
    for label, state, exp, verdict, sample in results:
        if verdict.startswith("!!"):
            mism += 1
        print(f"  [{state:7}] {verdict:11} exp={exp:8} {label}")
        if sample:
            print(f"            sample={sample}")
    print("-" * 80)
    print(f"  mismatches vs doc claims: {mism}")
    print("=" * 80)


def main():
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "utility/app.py",
         "--server.port", str(PORT), "--server.headless", "true",
         "--server.fileWatcherType", "none"],
        cwd=str(REPO), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        if not wait_health():
            print("SERVER FAILED TO START")
            return
        print(f"server up on {BASE}")
        run_checks()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:  # noqa: BLE001
            proc.kill()
        # belt-and-braces: kill any child uvicorn left on the port
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                       capture_output=True)


if __name__ == "__main__":
    main()
