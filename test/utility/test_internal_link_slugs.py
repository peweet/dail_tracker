"""Dead-link guard — static lint, no server needed.

"Dead links are an increasing problem": every hand-rolled internal ``href="/slug"``
in a page must point at a slug that is actually registered in
``utility/app.py`` (``st.Page(url_path=...)``). The classic failure (shipped on
the Statutory Instruments page) was ``href="/legislation?bill="`` — the
registered route is ``rankings-legislation``, so ``/legislation`` rendered a
Streamlit "page not found" modal.

Two checks:
  1. every literal internal href in utility/pages_code/*.py resolves to a
     registered slug;
  2. every slug exposed by ``utility/ui/entity_links.PAGES`` is registered too
     (the helper layer can't point at a dead route either).

Keep links going through the ``entity_links`` builders; this lint is the
backstop for the hand-rolled ones that slip past review.
"""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_UTIL = _ROOT / "utility"
_APP = _UTIL / "app.py"
_PAGES_DIR = _UTIL / "pages_code"
_ENTITY_LINKS = _UTIL / "ui" / "entity_links.py"

# Literal internal href: href="/<slug>..."  (single or double quoted).
# Only matches when the path begins with a lowercase letter — i.e. a literal
# slug, not an f-string interpolation like href="{url}" (helper output, trusted)
# and not an in-page href="?param" (no leading slash).
_HREF_RE = re.compile(r"""href=["']/([a-z][a-z0-9-]*)""")
_URLPATH_RE = re.compile(r"""url_path\s*=\s*["']([a-z0-9-]+)["']""")
# PAGES dict values in entity_links.py:  "key": "slug-value",
_PAGES_VALUE_RE = re.compile(r""":\s*["']([a-z0-9-]+)["']""")


def _registered_slugs() -> set[str]:
    text = _APP.read_text(encoding="utf-8")
    slugs = set(_URLPATH_RE.findall(text))
    # The default page resolves to the bare root path "" in addition to its
    # declared url_path (Streamlit drops url_path on the default page).
    slugs.add("")
    assert "rankings-legislation" in slugs, "slug parse failed — app.py changed shape?"
    return slugs


def _entity_links_pages_slugs() -> set[str]:
    """Slugs on the right-hand side of the PAGES dict in entity_links.py."""
    text = _ENTITY_LINKS.read_text(encoding="utf-8")
    # Restrict to the PAGES = { ... } block so we don't pick up unrelated dicts.
    m = re.search(r"PAGES\s*:\s*dict\[str,\s*str\]\s*=\s*\{(.*?)\n\}", text, re.DOTALL)
    assert m, "could not locate PAGES dict in entity_links.py"
    return set(_PAGES_VALUE_RE.findall(m.group(1)))


def test_hand_rolled_internal_hrefs_point_to_registered_slugs():
    registered = _registered_slugs()
    offenders: list[str] = []
    for py in sorted(_PAGES_DIR.glob("*.py")):
        text = py.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            for slug in _HREF_RE.findall(line):
                if slug not in registered:
                    offenders.append(f'{py.name}:{lineno}  href="/{slug}"  (not a registered url_path)')
    assert not offenders, "Dead internal links (slug not registered in app.py):\n  " + "\n  ".join(offenders)


def test_entity_links_helpers_resolve_to_registered_slugs():
    registered = _registered_slugs()
    bad = sorted(s for s in _entity_links_pages_slugs() if s not in registered)
    assert not bad, f"entity_links.PAGES points at unregistered slug(s): {bad}"
