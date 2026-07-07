"""Resolve Google-News RSS redirect URLs to the real publisher article URL.

WHY THIS EXISTS
---------------
Google-News RSS ``<link>`` values are opaque ``news.google.com/rss/articles/<id>``
redirects. They are NOT usable as-is:
  * for an EU session they land on a ``consent.google.com`` cookie wall, not the
    article (the link "works" — HTTP 200 — but never reaches the publisher);
  * the post-2022 ``<id>`` format is obfuscated, not base64-decodable to the URL.

The reliable resolution is Google's own internal RPC: fetch the article page (with a
consent cookie so the wall is skipped) to read its per-article signature + timestamp,
then POST those to ``/_/DotsSplashUi/data/batchexecute``, which returns the real URL.

This is undocumented Google internals and will break if Google changes the format —
so every call is defensive and returns ``None`` on any failure, leaving the caller to
fall back to the original redirect URL (no worse than before).
"""

from __future__ import annotations

import json
import logging
import re
import time

import requests

logger = logging.getLogger(__name__)

_GN_HOST = "news.google.com"
# A pre-consented cookie skips the consent.google.com interstitial that otherwise
# blocks every request from an EU IP.
_CONSENT_COOKIE = "CAESEwgDEgk0ODE3Nzk3MjQaAmVuIAEaBgiA_LyaBg"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

_SIG = re.compile(r'data-n-a-sg="([^"]+)"')
_TS = re.compile(r'data-n-a-ts="([^"]+)"')
# First http(s) URL in the RPC response that is NOT a google.com host = the publisher.
_REAL_URL = re.compile(r'(https?://(?!news\.google|consent\.google|www\.google)[^"\\]+)')


def is_gn_redirect(url: str | None) -> bool:
    """True for a Google-News /articles/ redirect URL (the only kind we resolve)."""
    return bool(url) and _GN_HOST in url and "/articles/" in url


def make_session() -> requests.Session:
    """A session pre-seeded with the consent cookie + a browser UA."""
    s = requests.Session()
    s.headers.update({"User-Agent": _UA})
    s.cookies.set("SOCS", _CONSENT_COOKIE, domain=".google.com")
    return s


def resolve_url(url: str, session: requests.Session, timeout: int = 25) -> str | None:
    """Resolve one Google-News redirect URL to the real publisher URL, or None.

    Two requests: GET the article page for its signature/timestamp, then POST the
    batchexecute RPC. None on any failure (caller keeps the original URL).
    """
    try:
        art_id = url.split("/articles/")[1].split("?")[0]
    except IndexError:
        return None
    try:
        page = session.get(f"https://{_GN_HOST}/rss/articles/{art_id}", timeout=timeout)
        sg, ts = _SIG.search(page.text), _TS.search(page.text)
        if not (sg and ts):
            return None
        inner = [
            "garturlreq",
            [
                ["X", "X", ["X", "X"], None, None, 1, 1, "US:en", None, 1, None, None, None, None, None, 0, 1],
                "X",
                "X",
                1,
                [1, 1, 1],
                1,
                1,
                None,
                0,
                0,
                None,
                0,
            ],
            art_id,
            ts.group(1),
            sg.group(1),
        ]
        req = [[["Fbv4je", json.dumps(inner), None, "generic"]]]
        rpc = session.post(
            f"https://{_GN_HOST}/_/DotsSplashUi/data/batchexecute",
            data={"f.req": json.dumps(req)},
            timeout=timeout,
        )
        m = _REAL_URL.search(rpc.text)
        return m.group(1) if m else None
    except requests.RequestException as exc:
        logger.debug("gnews resolve failed for %s: %s", art_id, exc)
        return None


def resolve_many(
    urls: list[str],
    session: requests.Session | None = None,
    *,
    delay: float = 0.0,
    max_resolve: int = 0,
    progress_every: int = 100,
) -> dict[str, str]:
    """Resolve a list of Google-News redirect URLs → {redirect_url: real_url}.

    Only successfully-resolved URLs appear in the result (failures are omitted so the
    caller keeps the original). ``max_resolve`` (>0) caps how many are attempted and
    logs the remainder so a partial pass is never mistaken for a complete one.
    """
    session = session or make_session()
    todo = [u for u in dict.fromkeys(urls) if is_gn_redirect(u)]
    capped = todo[:max_resolve] if max_resolve else todo
    out: dict[str, str] = {}
    for i, u in enumerate(capped, 1):
        real = resolve_url(u, session)
        if real:
            out[u] = real
        if progress_every and i % progress_every == 0:
            logger.info("  resolved %d/%d Google-News URLs (%d ok)", i, len(capped), len(out))
        if delay:
            time.sleep(delay)
    if max_resolve and len(todo) > max_resolve:
        logger.warning(
            "gnews resolve cap hit: %d of %d redirect URLs left unresolved this pass",
            len(todo) - max_resolve,
            len(todo),
        )
    return out
