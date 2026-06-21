"""Empirically validate the 2026-06-21 clutter/nav-graph audit claims against
the LIVE app (real clicks + live DOM), not code reading.

Run with a fresh server up:  python _clutter_validate.py http://127.0.0.1:8645

Each check prints  PASS (claim confirmed) / FAIL (claim wrong) / INCONCLUSIVE,
with the concrete evidence it used (actual href, testid presence, text count).
"""

from __future__ import annotations

import json
import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"
RESULTS: list[dict] = []


def rec(check, page, claim, verdict, evidence):
    RESULTS.append({"check": check, "page": page, "claim": claim, "verdict": verdict, "evidence": evidence})
    print(f"[{verdict:12}] {check}: {evidence}")


def settle(page, t=6.0):
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    time.sleep(t)


def goto(page, path, t=6.0):
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=60000)
    settle(page, t)


def anchors(page):
    return page.evaluate(
        """() => [...document.querySelectorAll('a')].map(a => ({
            href: a.getAttribute('href') || '',
            text: (a.textContent || '').trim().slice(0, 80)
        }))"""
    )


def body_text(page):
    return page.evaluate("() => document.body.innerText")


def n_dataframes(page):
    return page.evaluate("() => document.querySelectorAll('[data-testid=\"stDataFrame\"]').length")


def count_sub(haystack, needle):
    return haystack.lower().count(needle.lower())


# ── element vertical position (to prove "X renders before Y") ──
def first_y(page, selector):
    return page.evaluate(
        """(sel) => { const el = document.querySelector(sel);
            if (!el) return null; const r = el.getBoundingClientRect();
            return Math.round(r.top + window.scrollY); }""",
        selector,
    )


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 2600})
        page = ctx.new_page()

        # ───────────────────────────────────────────────────────────────────
        # FUNCTIONAL BUG 1 — statutory-instruments enabling-Act "View Bill
        # detail" link uses unregistered slug /legislation?bill= (should be
        # /rankings-legislation). Drill into SI details until one has the link.
        # ───────────────────────────────────────────────────────────────────
        try:
            goto(page, "/rankings-statutory-instruments", 7)
            si_hrefs = [a["href"] for a in anchors(page) if "si=" in a["href"]]
            si_hrefs = list(dict.fromkeys(si_hrefs))[:12]
            found = None
            for h in si_hrefs:
                dest = h if h.startswith("http") else f"{BASE}{h if h.startswith('/') else '/rankings-statutory-instruments' + h}"
                page.goto(dest, wait_until="domcontentloaded", timeout=60000)
                settle(page, 4)
                bill_links = [a for a in anchors(page) if "bill=" in a["href"] or "View Bill" in a["text"]]
                if bill_links:
                    found = bill_links[0]
                    break
            if found is None:
                rec("SI_bill_link", "statutory-instruments",
                    "enabling-Act link uses dead slug /legislation?bill=",
                    "INCONCLUSIVE", f"no SI among {len(si_hrefs)} had a 'View Bill detail' link to test")
            else:
                href = found["href"]
                slug = href.lstrip("/").split("?")[0].rstrip("/")
                broken = slug == "legislation"  # unregistered; registered is rankings-legislation
                rec("SI_bill_link", "statutory-instruments",
                    "enabling-Act link uses dead slug /legislation?bill=",
                    "PASS" if broken else "FAIL",
                    f"href={href!r} -> slug={slug!r} ({'DEAD: legislation not registered' if broken else 'resolves OK'})")
        except Exception as e:
            rec("SI_bill_link", "statutory-instruments", "dead bill slug", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # FUNCTIONAL BUG 2 — forbidden st.dataframe on PRIMARY views.
        # ───────────────────────────────────────────────────────────────────
        # 2a accommodation-spend: provider ranking is a dataframe on first paint
        try:
            goto(page, "/accommodation-spend", 7)
            n = n_dataframes(page)
            rec("dataframe_accommodation", "accommodation-spend",
                "providers/year rendered as st.dataframe on primary view",
                "PASS" if n >= 1 else "FAIL", f"{n} stDataFrame element(s) on first paint")
        except Exception as e:
            rec("dataframe_accommodation", "accommodation-spend", "primary dataframe", "ERROR", repr(e))

        # 2b housing 'By county' section table
        try:
            goto(page, "/housing", 6)
            # try to switch to the By county lane via a segmented control / pill
            for label in ("By county", "By county "):
                try:
                    el = page.get_by_text(label, exact=False).first
                    if el and el.is_visible():
                        el.click()
                        settle(page, 5)
                        break
                except Exception:
                    pass
            n = n_dataframes(page)
            rec("dataframe_housing", "housing",
                "By-county league table is st.dataframe on primary view",
                "PASS" if n >= 1 else "INCONCLUSIVE",
                f"{n} stDataFrame element(s) after selecting By county")
        except Exception as e:
            rec("dataframe_housing", "housing", "primary dataframe", "ERROR", repr(e))

        # 2c lobbying per-politician primary view (?lp3_pol)
        try:
            goto(page, "/rankings-lobbying", 7)
            pol = [a["href"] for a in anchors(page) if "lp3_pol=" in a["href"] or "lp3_result_pol=" in a["href"]]
            if pol:
                dest = pol[0]
                dest = dest if dest.startswith("http") else f"{BASE}{dest if dest.startswith('/') else '/rankings-lobbying' + dest}"
                page.goto(dest, wait_until="domcontentloaded", timeout=60000)
                settle(page, 6)
                n = n_dataframes(page)
                rec("dataframe_lobbying", "rankings-lobbying",
                    "per-politician primary view uses st.dataframe x2",
                    "PASS" if n >= 1 else "FAIL", f"followed {dest.split(BASE)[-1]} -> {n} stDataFrame element(s)")
            else:
                rec("dataframe_lobbying", "rankings-lobbying", "per-politician dataframe", "INCONCLUSIVE",
                    "no lp3_pol/lp3_result_pol link found on landing to drill into")
        except Exception as e:
            rec("dataframe_lobbying", "rankings-lobbying", "per-politician dataframe", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # FUNCTIONAL BUG 3 — follow-the-money 'sum sum-safe' duplicated word.
        # In _render_group (corporate-group node). Follow the group start point.
        # ───────────────────────────────────────────────────────────────────
        try:
            goto(page, "/follow-the-money", 6)
            txt = body_text(page)
            grp = [a["href"] for a in anchors(page) if "group" in a["href"].lower()]
            if "sum sum" not in txt.lower() and grp:
                dest = grp[0]
                dest = dest if dest.startswith("http") else f"{BASE}{dest if dest.startswith('/') else '/follow-the-money' + dest}"
                page.goto(dest, wait_until="domcontentloaded", timeout=60000)
                settle(page, 6)
                txt = body_text(page)
            has_typo = "sum sum" in txt.lower()
            rec("ftm_typo", "follow-the-money", "'sum sum-safe' duplicated word in group view",
                "PASS" if has_typo else "INCONCLUSIVE",
                f"'sum sum' present={has_typo} (followed group node={bool(grp)})")
        except Exception as e:
            rec("ftm_typo", "follow-the-money", "duplicated word", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # DEAD-END A — council-spending supplier card does NOT link to /company
        # ───────────────────────────────────────────────────────────────────
        try:
            goto(page, "/rankings-council-spending", 7)
            # click first council card link (carries paid_publisher)
            councils = [a["href"] for a in anchors(page) if "paid_publisher=" in a["href"]]
            if councils:
                dest = councils[0]
                dest = dest if dest.startswith("http") else f"{BASE}{dest if dest.startswith('/') else '/rankings-council-spending' + dest}"
                page.goto(dest, wait_until="domcontentloaded", timeout=60000)
                settle(page, 7)
                hs = anchors(page)
                to_company = [a["href"] for a in hs if "company?supplier=" in a["href"]]
                supplier_leaf = [a["href"] for a in hs if "paid_supplier=" in a["href"] or "paid_pair=" in a["href"]]
                dead = (len(to_company) == 0) and (len(supplier_leaf) > 0)
                rec("deadend_council_supplier", "rankings-council-spending",
                    "supplier card not linked to /company (leaf-only)",
                    "PASS" if dead else ("FAIL" if to_company else "INCONCLUSIVE"),
                    f"company?supplier links={len(to_company)}, paid_supplier/pair leaf links={len(supplier_leaf)}")
            else:
                rec("deadend_council_supplier", "rankings-council-spending", "supplier dead-end",
                    "INCONCLUSIVE", "no paid_publisher council link on index to drill into")
        except Exception as e:
            rec("deadend_council_supplier", "rankings-council-spending", "supplier dead-end", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # DEAD-END B — votes Mode C: division detail bill NOT linked onward
        # (no /rankings-legislation?bill= anchor in the division view).
        # ───────────────────────────────────────────────────────────────────
        try:
            goto(page, "/rankings-votes", 7)
            votes = [a["href"] for a in anchors(page) if "vote=" in a["href"]]
            if votes:
                dest = votes[0]
                dest = dest if dest.startswith("http") else f"{BASE}{dest if dest.startswith('/') else '/rankings-votes' + dest}"
                page.goto(dest, wait_until="domcontentloaded", timeout=60000)
                settle(page, 7)
                bill_links = [a["href"] for a in anchors(page) if "bill=" in a["href"]]
                rec("deadend_votes_bill", "rankings-votes",
                    "division detail does not link the bill via bill_detail_url",
                    "PASS" if not bill_links else "FAIL",
                    f"followed {dest.split(BASE)[-1]} -> bill= links in division view = {len(bill_links)}")
            else:
                rec("deadend_votes_bill", "rankings-votes", "bill dead-end", "INCONCLUSIVE",
                    "no vote= division link on landing")
        except Exception as e:
            rec("deadend_votes_bill", "rankings-votes", "bill dead-end", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # DEAD-END C — ministerial-diaries: minister names not linked to
        # member-overview (no member= anchors on the diaries pages).
        # ───────────────────────────────────────────────────────────────────
        try:
            goto(page, "/rankings-ministerial-diaries", 7)
            hs = anchors(page)
            member_links = [a["href"] for a in hs if "member-overview?member=" in a["href"] or "member=" in a["href"]]
            org_links = [a["href"] for a in hs if "lp3_org=" in a["href"] or "company?supplier=" in a["href"] or "rankings-corporate" in a["href"]]
            rec("deadend_diaries", "rankings-ministerial-diaries",
                "ministers/orgs not linked onward (member_profile/lobbying/company)",
                "PASS" if (not member_links and not org_links) else "FAIL",
                f"member= links={len(member_links)}, org onward links={len(org_links)}")
        except Exception as e:
            rec("deadend_diaries", "rankings-ministerial-diaries", "dead-end", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # DEAD-END D — appointments detail view: only Back + external Iris.
        # ───────────────────────────────────────────────────────────────────
        try:
            goto(page, "/rankings-appointments", 7)
            refs = [a["href"] for a in anchors(page) if "ref=" in a["href"]]
            if refs:
                dest = refs[0]
                dest = dest if dest.startswith("http") else f"{BASE}{dest if dest.startswith('/') else '/rankings-appointments' + dest}"
                page.goto(dest, wait_until="domcontentloaded", timeout=60000)
                settle(page, 6)
                hs = anchors(page)
                onward = [a["href"] for a in hs if "member=" in a["href"] or "company?supplier=" in a["href"]
                          or "authority=" in a["href"]]
                ext = [a["href"] for a in hs if a["href"].startswith("http")]
                rec("deadend_appointments", "rankings-appointments",
                    "detail view has no onward entity edge (Back + Iris only)",
                    "PASS" if not onward else "FAIL",
                    f"onward entity links={len(onward)}, external links={len(ext)}")
            else:
                rec("deadend_appointments", "rankings-appointments", "detail dead-end", "INCONCLUSIVE",
                    "no ref= detail link on landing")
        except Exception as e:
            rec("deadend_appointments", "rankings-appointments", "detail dead-end", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # COUNTER-CHECK — known-GOOD forward edges should be present.
        # public-payments supplier detail -> /company ; what-they-own -> member.
        # ───────────────────────────────────────────────────────────────────
        try:
            goto(page, "/what-they-own", 7)
            mem = [a["href"] for a in anchors(page) if "member-overview?member=" in a["href"]]
            rec("good_edge_wto", "what-they-own", "member cards DO link onward (sanity check)",
                "PASS" if mem else "FAIL", f"member-overview?member= links on landing = {len(mem)}")
        except Exception as e:
            rec("good_edge_wto", "what-they-own", "good edge", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # CLUTTER A — legislation: pipeline-strip counts duplicated in the
        # segmented-control labels (same numbers, two chromes).
        # ───────────────────────────────────────────────────────────────────
        try:
            goto(page, "/rankings-legislation", 7)
            txt = body_text(page)
            import re
            seg = re.findall(r"(Dáil Stages|Seanad Stages|Enacted)\s*\((\d+)\)", txt)
            rec("clutter_leg_counts", "rankings-legislation",
                "phase counts shown twice (pipeline strip + segmented labels)",
                "PASS" if len(seg) >= 2 else "INCONCLUSIVE",
                f"segmented labels with counts found: {seg}")
        except Exception as e:
            rec("clutter_leg_counts", "rankings-legislation", "dup counts", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # CLUTTER B — choropleth/map renders BEFORE the searchable card grid
        # on local-government and constituencies (decorative block first).
        # ───────────────────────────────────────────────────────────────────
        for slug in ("/local-government", "/constituencies"):
            try:
                goto(page, slug, 8)
                map_y = None
                for sel in ('[data-testid="stPlotlyChart"]', "iframe", '[data-testid="stDeckGlJsonChart"]',
                            "canvas", ".js-plotly-plot"):
                    map_y = first_y(page, sel)
                    if map_y is not None:
                        used = sel
                        break
                search_y = first_y(page, 'input[type="text"]') or first_y(page, '[data-testid="stTextInput"]')
                if map_y is not None and search_y is not None:
                    before = map_y < search_y
                    rec("clutter_map_first", slug,
                        "map/choropleth renders before the search/card grid",
                        "PASS" if before else "FAIL",
                        f"map({used})_y={map_y} vs search_y={search_y} -> map first={before}")
                else:
                    rec("clutter_map_first", slug, "map before grid", "INCONCLUSIVE",
                        f"map_y={map_y}, search_y={search_y} (selector miss)")
            except Exception as e:
                rec("clutter_map_first", slug, "map before grid", "ERROR", repr(e))

        # ───────────────────────────────────────────────────────────────────
        # CLUTTER C — caveat repetition (same disclaimer phrase >=3x in DOM).
        # ───────────────────────────────────────────────────────────────────
        caveat_pages = [
            ("/rankings-public-payments", "20,000", 2),
            ("/election-2024", "never", 3),
            ("/in-the-news", "not an", 2),
        ]
        for slug, phrase, thresh in caveat_pages:
            try:
                goto(page, slug, 7)
                txt = body_text(page)
                c = count_sub(txt, phrase)
                rec("clutter_caveat", slug,
                    f"caveat phrase repeated on first paint ('{phrase}')",
                    "PASS" if c >= thresh else "INCONCLUSIVE",
                    f"'{phrase}' appears {c}x (threshold {thresh})")
            except Exception as e:
                rec("clutter_caveat", slug, "caveat repetition", "ERROR", repr(e))

        browser.close()

    print("\n" + "=" * 78)
    print("SUMMARY")
    print("=" * 78)
    by_v = {}
    for r in RESULTS:
        by_v.setdefault(r["verdict"], []).append(r["check"])
    for v in ("PASS", "FAIL", "INCONCLUSIVE", "ERROR"):
        if by_v.get(v):
            print(f"  {v}: {len(by_v[v])}  -> {', '.join(by_v[v])}")
    print("\nJSON:")
    print(json.dumps(RESULTS, ensure_ascii=False))


if __name__ == "__main__":
    main()
