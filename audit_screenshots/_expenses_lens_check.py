"""Verify the new Election-Expenses lens on the Payments page (/rankings-payments).

Drives the real app: click the 'Election expenses' segment -> screenshot the party
grid -> click the Fianna Fáil card -> screenshot the candidate drill. Captures page
text as evidence (FF total = €374,778 not €3.44M; over-limit candidates show
'verify · SIPO p.N' not a euro figure).
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.stdout.reconfigure(encoding="utf-8")
BASE = "http://localhost:8501"
ROUTE = "/rankings-payments"
OUT = Path(__file__).resolve().parent / "_expenses_check"
OUT.mkdir(parents=True, exist_ok=True)


def main() -> None:
    findings: list[str] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(viewport={"width": 1440, "height": 1000}).new_page()
        page.goto(f"{BASE}{ROUTE}", wait_until="networkidle")
        time.sleep(10)  # cold-start hydrate

        # (1) the three-way View control exists
        body = page.inner_text("body")
        for opt in ("Member payments", "Party donations", "Election expenses"):
            findings.append(f"segment '{opt}' present: {opt in body}")

        # (2) select Election expenses
        try:
            page.get_by_text("Election expenses", exact=True).first.click()
        except Exception as e:
            findings.append(f"!! could not click 'Election expenses': {e}")
        time.sleep(4)
        page.screenshot(path=str(OUT / "01_expenses_grid.png"), full_page=True)
        grid = page.inner_text("body")
        findings.append(f"hero 'Election Expenses' shown: {'Election Expenses' in grid}")
        findings.append(f"FF €374,778 shown: {'374,778' in grid or '374,777' in grid}")
        findings.append(f"FF €3.44M NOT shown: {'3,442,4' not in grid}")
        findings.append(f"'to verify' mark present: {'to verify' in grid}")
        for label in ("Fianna F", "Fine Gael", "Sinn F", "spent on candidates"):
            findings.append(f"grid has '{label}': {label in grid}")

        # (3) drill into Fianna Fáil
        try:
            page.locator("a.don-card", has_text="Fianna F").first.click()
        except Exception as e:
            findings.append(f"!! could not click FF card: {e}")
        time.sleep(4)
        page.screenshot(path=str(OUT / "02_ff_candidate_drill.png"), full_page=True)
        drill = page.inner_text("body")
        findings.append(f"drill heading present: {'spend on candidates' in drill}")
        for nm in ("Fitzpatrick", "Lawless", "McSharry", "Meara"):
            findings.append(f"drill lists '{nm}': {nm in drill}")
        findings.append(f"'verify · SIPO p.' marks present: {'verify' in drill and 'SIPO p.' in drill}")
        # the inflated magnitudes must NOT appear as figures
        bad = [m for m in ("709,513", "625,118", "1,028,616", "704,388") if m in drill]
        findings.append(f"inflated FF magnitudes suppressed (none shown): {not bad} {('LEAK:'+str(bad)) if bad else ''}")
        # a known-good FF candidate amount should still show
        findings.append(f"clean FF amount shown (Ardagh 17,844): {'17,844' in drill}")

        browser.close()

    print("\n=== FINDINGS ===")
    for f in findings:
        print(" -", f)
    print(f"\nscreenshots -> {OUT}")


if __name__ == "__main__":
    main()
