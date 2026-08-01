"""Playwright-rendered fetch fallback for WAF'd / JS-rendered council + semi-state sites.

Plain requests gets 403'd by WAFs (Louth/Meath/Sligo seeds) or an empty JS shell
(semi-state corporate sites). Chromium fixes both: page render for HTML, and the
browser-context request API for PDFs (page.request gets 200 where fetch 403s —
project_siting_gap_closure_2026_07_31). One browser per process; call close() when done.
"""
from __future__ import annotations

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


class PWFetch:
    def __init__(self) -> None:
        from playwright.sync_api import sync_playwright

        self._p = sync_playwright().start()
        self._b = self._p.chromium.launch(headless=True)
        self._ctx = self._b.new_context(user_agent=UA, ignore_https_errors=True)

    def html(self, url: str, settle_ms: int = 4000) -> str | None:
        page = self._ctx.new_page()
        try:
            page.goto(url, timeout=45000, wait_until="domcontentloaded")
            page.wait_for_timeout(settle_ms)
            return page.content()
        except Exception:  # noqa: BLE001
            return None
        finally:
            page.close()

    def bytes(self, url: str) -> bytes | None:
        try:
            r = self._ctx.request.get(url, timeout=90000)
            return r.body() if r.ok else None
        except Exception:  # noqa: BLE001
            return None

    def close(self) -> None:
        for closer in (self._ctx.close, self._b.close, self._p.stop):
            try:
                closer()
            except Exception:  # noqa: BLE001
                pass
