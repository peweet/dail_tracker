"""Member avatar lookup for Dáil Tracker.

Reads avatar/wikidata/manifest.json once, normalises member names to the
project's standard join key (matching normalise_df_td_name), and exposes:

    avatar_data_url(name)    -> str | None
        Base64 data URL for use inside HTML cards / <img src=...>.

    avatar_credit_html(name) -> str | None
        Inline attribution caption ("Photo: <author> · <license> ·
        Wikimedia Commons"). Required under CC BY / CC BY-SA when the
        photo is shown.

    initials(name)           -> str
        1–2 letter initials for use as a no-photo fallback inside a
        circular chip.

Pure presentation: no business logic, no aggregation. The manifest is
keyed by name because the interests view does not yet expose
unique_member_code (TODO_PIPELINE_VIEW_REQUIRED on
v_member_interests_*).
"""
from __future__ import annotations

import base64
import json
import re
import unicodedata
from html import escape as _h
from pathlib import Path

# project_root / avatar / wikidata / manifest.json
_MANIFEST_PATH = (
    Path(__file__).resolve().parents[2] / "avatar" / "wikidata" / "manifest.json"
)

_lookup: dict[str, dict] | None = None
_b64_cache: dict[str, str] = {}

_HONORIFIC_RE  = re.compile(r"^\s*(dr|prof|rev|fr|sr|mr|mrs|ms|miss|br)\s+")
_APOSTROPHE_RE = re.compile(r"['‘’ʼʹ`´＇]")
_NON_ALPHA_RE  = re.compile(r"[^a-z\s]")
_WS_RE         = re.compile(r"\s+")
_SPLIT_RE      = re.compile(r"[\s,]+")


def _normalise_name(name: str) -> str:
    """Mirror normalise_df_td_name.normalise_df_td_name (pure Python)."""
    if not name:
        return ""
    s = name.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = _APOSTROPHE_RE.sub("", s)
    s = _NON_ALPHA_RE.sub("", s)
    s = _HONORIFIC_RE.sub("", s)
    s = _WS_RE.sub("", s)
    return "".join(sorted(s))


def _load_lookup() -> dict[str, dict]:
    global _lookup
    if _lookup is not None:
        return _lookup
    _lookup = {}
    if not _MANIFEST_PATH.exists():
        return _lookup
    try:
        records = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _lookup
    if not isinstance(records, list):
        return _lookup
    for rec in records:
        key = _normalise_name(rec.get("name", ""))
        if key and key not in _lookup:
            _lookup[key] = rec
    return _lookup


def _record_for(name: str) -> dict | None:
    return _load_lookup().get(_normalise_name(name))


def _data_url_for(filename: str) -> str | None:
    if not filename:
        return None
    cached = _b64_cache.get(filename)
    if cached is not None:
        return cached
    path = _MANIFEST_PATH.parent / filename
    if not path.exists():
        return None
    mime = {
        ".jpg":  "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png":  "image/png",
        ".webp": "image/webp",
    }.get(path.suffix.lower(), "image/jpeg")
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    url = f"data:{mime};base64,{encoded}"
    _b64_cache[filename] = url
    return url


def initials(name: str) -> str:
    parts = [p for p in _SPLIT_RE.split(name.strip()) if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def avatar_data_url(name: str) -> str | None:
    rec = _record_for(name)
    if not rec:
        return None
    return _data_url_for(rec.get("local_file") or "")


def has_photo(name: str) -> bool:
    return avatar_data_url(name) is not None


def avatar_credit_html(name: str) -> str | None:
    """Inline attribution for the photo, or None when no photo."""
    rec = _record_for(name)
    if not rec or not rec.get("local_file"):
        return None
    artist        = rec.get("artist") or ""
    license_name  = rec.get("license_name") or ""
    license_url   = rec.get("license_url") or ""
    file_page_url = rec.get("file_page_url") or rec.get("image_commons_url") or ""

    parts: list[str] = []
    if artist:
        parts.append(f"Photo: {_h(artist)}")
    if license_name and license_url:
        parts.append(
            f'<a href="{_h(license_url)}" target="_blank" rel="noopener">{_h(license_name)}</a>'
        )
    elif license_name:
        parts.append(_h(license_name))
    if file_page_url:
        parts.append(
            f'<a href="{_h(file_page_url)}" target="_blank" rel="noopener">Wikimedia Commons</a>'
        )

    if not parts:
        # Manifest hasn't been backfilled with license metadata yet — generic
        # attribution. Re-run test_wiki_data.py to populate per-photo credits.
        return (
            'Photo via '
            '<a href="https://commons.wikimedia.org/" target="_blank" rel="noopener">'
            'Wikimedia Commons</a>'
        )
    return " · ".join(parts)
