"""Quick render check on the three new routes."""
from __future__ import annotations

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-lobbying"


def hero(page) -> str:
    h = page.eval_on_selector_all("h1", "ns => ns.map(n => n.textContent.trim())")
    return " | ".join(h)[:200]


def check(page, url: str, label: str) -> None:
    page.goto(f"{BASE}{ROUTE}{url}", wait_until="domcontentloaded")
    page.wait_for_timeout(7000)
    cards = page.eval_on_selector_all("article.lp3-return-card", "ns => ns.length")
    # any text rendered at all?
    has_empty_state = page.eval_on_selector_all(
        '[data-testid="stAlert"], .lp3-empty-state', "ns => ns.length",
    )
    h2s = page.eval_on_selector_all("h2", "ns => ns.map(n => n.textContent.trim())")
    df_rows = page.eval_on_selector_all(
        '[data-testid="stDataFrame"] [role="row"]', "ns => ns.length",
    )
    print(f"\n=== {label} ===")
    print(f"  hero: {hero(page)}")
    print(f"  lp3-return-card count: {cards}")
    print(f"  empty-state nodes: {has_empty_state}")
    print(f"  h2 headings: {h2s[:8]}")
    print(f"  dataframe rows (sum across frames): {df_rows}")


def main() -> None:
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        ctx = b.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()
        check(page, "?lp3_pol=Paschal%20Donohoe", "Politician-only: Paschal Donohoe")
        check(page, "?lp3_dpo=Lorraine%20Higgins&lp3_result_pol=Aidan%20Davitt",
              "Stage 3 DPO x pol: Higgins x Aidan Davitt")
        check(page, "?lp3_dpo=Lorraine%20Higgins&lp3_result_pol=Paschal%20Donohoe",
              "Stage 3 DPO x pol: Higgins x Paschal Donohoe")
        b.close()


if __name__ == "__main__":
    main()
