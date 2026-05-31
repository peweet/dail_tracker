"""Post-fix smoke: confirm Member Overview payments now render for known TDs.

Five TDs hand-picked to span match-vs-miss surface:
  Michael Healy-Rae  - expected ~€279k (Minister of State at Agriculture)
  Pearse Doherty     - expected ~€312k (Senate→Dail; opposition heavyweight)
  Michael Lowry      - expected ~€300k (long-serving Independent)
  Simon Harris       - expected ~€95k  (cabinet member; reduced TAA)
  Frankie Feighan    - expected "Not on file" (nickname mismatch, known-unmatched)

Confirms both the hero stat strip AND the Payments tab body render values.
"""
from __future__ import annotations
import re, time
from pathlib import Path
from urllib.parse import quote
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "_payments_smoke"
OUT.mkdir(parents=True, exist_ok=True)

COHORT = [
    ("healyrae",  "Michael-Healy-Rae.D.2011-03-09", True),
    ("doherty",   "Pearse-Doherty.S.2007-07-23",    True),
    ("lowry",     "Michael-Lowry.D.1987-03-10",     True),
    ("harris",    "Simon-Harris.D.2011-03-09",      True),
    ("feighan",   "Frankie-Feighan.D.2007-06-14",   False),
]


def main():
    findings = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for name, code, expect_value in COHORT:
            ctx = browser.new_context(viewport={"width": 1440, "height": 1000})
            page = ctx.new_page()
            url = f"{BASE}/member-overview?member={quote(code, safe='')}"
            page.goto(url, wait_until="networkidle")
            time.sleep(8)
            # dismiss page-not-found if any
            try:
                btn = page.locator('[data-testid="stDialog"] button').first
                if btn.is_visible(timeout=500):
                    btn.click()
                    time.sleep(0.5)
            except Exception:
                pass
            # open all sections so the payments body renders
            try:
                page.get_by_text(re.compile(r"Open all", re.I)).first.click()
                time.sleep(2)
            except Exception:
                pass
            body = page.locator('[data-testid="stApp"]').inner_text()
            (OUT / f"{name}_body.txt").write_text(body, encoding="utf-8")

            # hero stat: look for €<digits> in PAYMENTS RECEIVED context, or "Not on file"
            hero_value = None
            for line in body.splitlines():
                if line.strip().startswith("€") or "Not on file" in line:
                    hero_value = line.strip()
                    break

            # payments section body: any € total reported?
            body_total = None
            for m in re.finditer(r"Total received[:\s]*[€]?(\d[\d,]*)", body):
                body_total = m.group(1)
                break

            page.screenshot(path=str(OUT / f"{name}.png"), full_page=True)
            ctx.close()
            findings.append({
                "name": name, "code": code,
                "expected_value": expect_value,
                "hero": hero_value,
                "body_total": body_total,
                "body_chars": len(body),
            })
        browser.close()

    print("\n=== POST-FIX SMOKE TEST ===")
    for f in findings:
        flag = "OK" if (f["expected_value"] and f["body_total"]) or (not f["expected_value"] and not f["body_total"]) else "FAIL"
        print(f"  [{flag}] {f['name']:<10} hero={f['hero']!r:<30} body_total={f['body_total']!r}")


if __name__ == "__main__":
    main()
