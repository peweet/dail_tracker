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


def harvest_after(page, url, settle):
    goto(page, url, settle)
    return harvest(page)


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


def all_param_vals(hrefs, param, limit=12):
    """Ordered, de-duplicated list of values a param takes across hrefs.

    Several edges are CONDITIONAL on the seeded entity (a bill needs a resolvable
    TD sponsor; the payments->company CTA is gated on awards-register membership,
    public_payments.py:523). Seeding the first candidate blind mis-reports the edge
    as broken when the entity is simply ineligible — so callers iterate candidates
    until one exercises the edge.
    """
    out = []
    for h in hrefs:
        q = parse_qs(urlparse(h).query) or (parse_qs(h[1:]) if h.startswith("?") else {})
        v = q.get(param, [None])[0]
        if v and v not in out:
            out.append(v)
            if len(out) >= limit:
                break
    return out


def run_checks():
    results = []
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        page = b.new_context(viewport={"width": 1440, "height": 4200}).new_page()

        # ── Seeds from landing pages ──────────────────────────────────────────
        # Single-value seeds where any candidate exercises the edge.
        goto(page, f"{BASE}/rankings-committees", 9)
        comm = first_param_val(harvest(page), "committee")
        goto(page, f"{BASE}/rankings-statutory-instruments", 11)
        si = first_param_val(harvest(page), "si")
        # Candidate lists for the two CONDITIONAL edges (see all_param_vals):
        #  - bills: many older enacted bills have a NULL/unresolvable sponsor, so
        #    the bill->member link only renders for some. Iterate until one resolves.
        #  - suppliers: the payments->company CTA is awards-register gated, and most
        #    paid suppliers are payments-only. Seed candidates from the PROCUREMENT
        #    landing (whose suppliers ARE awards-registered) so an eligible one exists.
        goto(page, f"{BASE}/rankings-legislation", 11)
        bill_candidates = all_param_vals(harvest(page), "bill")
        goto(page, f"{BASE}/rankings-procurement", 11)
        sup_candidates = all_param_vals(harvest(page), "supplier")

        def check(label, url, slug, param, expect, settle=11):
            """expect True/False = strict edge assertion; None = info-only (tolerated)."""
            goto(page, url, settle)
            hits = edges_to(harvest(page), slug, param)
            present = len(hits) > 0
            if expect is None:
                verdict, expstr = "OK", "info"
            else:
                verdict = "OK" if present == expect else "!! MISMATCH"
                expstr = "present" if expect else "absent"
            results.append((label, "PRESENT" if present else "ABSENT",
                            expstr, verdict, hits[:1]))
            return hits[:1]

        def probe_first(candidates, url_of, slug, param, settle):
            """Navigate each candidate until one shows the target edge; return
            (candidate, first-hit) or (None, None) if no candidate exercised it."""
            for c in candidates:
                hits = edges_to(harvest_after(page, url_of(c), settle), slug, param)
                if hits:
                    return c, hits[:1]
            return None, None

        # #8 loop-closer: legislation(bill) -> member(sponsor). Probe bills for the
        # back-edge AND collect several resolved sponsors: the sponsor of one
        # (possibly old) bill may itself have no bills in their OWN overview
        # legislation section (recency window), so the forward test needs choices.
        bill_used, first_hit, sponsors = None, None, []
        for c in bill_candidates:
            hits = edges_to(
                harvest_after(page, f"{BASE}/rankings-legislation?bill={c}", 8),
                "member-overview", "member")
            if not hits:
                continue
            if bill_used is None:
                bill_used, first_hit = c, hits[:1]
            m = first_param_val(hits, "member")
            if m and m not in sponsors:
                sponsors.append(m)
            if len(sponsors) >= 6:
                break
        sponsor = first_param_val(first_hit, "member") if first_hit else None
        results.append((
            f"legislation(bill) -> member(sponsor) [back-edge #8]  (bill={bill_used})",
            "PRESENT" if sponsor else "ABSENT",
            "present", "OK" if sponsor else "!! MISMATCH",
            first_hit or [],
        ))

        # #2 forward: member(?section=legislation) -> legislation(bill), driven on
        # the section state where bill links render. Iterate sponsors until one's
        # section actually links a bill (closes the member<->bill loop).
        fwd_member, fwd_hit = None, None
        for m in sponsors:
            hits = edges_to(
                harvest_after(page, f"{BASE}/member-overview?member={m}&section=legislation", 10),
                "rankings-legislation", "bill")
            if hits:
                fwd_member, fwd_hit = m, hits[:1]
                break
        results.append((
            f"member(section=legislation) -> legislation(bill) [FIX #2]  (member={fwd_member})",
            "PRESENT" if fwd_member else "ABSENT",
            "present", "OK" if fwd_member else "!! MISMATCH",
            fwd_hit or [],
        ))

        # SI minister back-edge — info-only: not every SI has an identifiable
        # signing minister rendered (nested-anchor cards skip the link per the doc).
        if si:
            check("statutory-instruments(si) -> member(minister) [back-edge]",
                  f"{BASE}/rankings-statutory-instruments?si={si}",
                  "member-overview", "member", None)

        # #1: public-payments(supplier) -> company. Registry-gated CTA
        # (public_payments.py:523) — iterate awards-registered suppliers until one
        # with payment lines shows the /company hand-off.
        sup_used, hit = probe_first(
            sup_candidates,
            lambda c: f"{BASE}/rankings-public-payments?supplier={c}",
            "company", "supplier", settle=9,
        )
        results.append((
            f"public-payments(supplier) -> company [FIX #1, gated]  (supplier={sup_used})",
            "PRESENT" if sup_used else "ABSENT",
            "present", "OK" if sup_used else "!! MISMATCH",
            hit or [],
        ))

        # #4: committees(committee) -> member. FIXED 2026-06-20 (roster LinkColumn),
        # so the edge is now expected PRESENT (the old harness still expected absent).
        if comm:
            check("committees(committee) -> member(member) [FIX #4]",
                  f"{BASE}/rankings-committees?committee={comm}",
                  "member-overview", "member", True)

        b.close()

    print("\n" + "=" * 80)
    print("NAV GRAPH — per-edge verification on correct detail states")
    print("=" * 80)
    print(f"seeds: bill_used={bill_used!r} sponsor={sponsor!r} sup_used={sup_used!r}")
    print(f"       si={si!r} committee={comm!r}")
    print(f"       (bill candidates={len(bill_candidates)}, supplier candidates={len(sup_candidates)})\n")
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
