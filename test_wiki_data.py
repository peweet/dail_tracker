"""
test_wiki_data.py - Wikidata SPARQL avatar downloader (exploratory)

Step 1 (this script): pull current-Dáil TDs from Wikidata via the position-held
statement (P39 = TD, P2937 = 34th Dáil), download portrait thumbnails, and
fetch per-image licensing metadata from the Commons API so the app can show
required CC BY / CC BY-SA attribution.

Wikidata properties used:
  P39   = "position held" (statement)
  P2937 = parliamentary term qualifier (Q131309742 = 34th Dáil)
  P768  = electoral district qualifier (constituency)
  P4100 = parliamentary group qualifier (party)
  P2336 = "Houses of the Oireachtas member ID" (our join key, OPTIONAL)
  P18   = "image" (Commons file, OPTIONAL)

Outputs:
  avatar/wikidata/<oireachtas_id_or_qid>.<ext>  -- one image per member
  avatar/wikidata/manifest.json                  -- list of records (see below)

Re-runs are idempotent: existing image files are not re-downloaded, and
existing license metadata is not re-fetched. To force a refresh, delete
the avatar/wikidata/ directory.

Run: python test_wiki_data.py
"""
import html as _html
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

SPARQL_URL = "https://query.wikidata.org/sparql"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"

HEADERS = {
    "User-Agent": "DailTracker/0.1 (civic transparency research; contact p.glynn18@gmail.com)",
    "Accept": "application/sparql-results+json",
}

PROJECT_ROOT = Path(__file__).resolve().parent
PHOTO_DIR = PROJECT_ROOT / "avatar" / "wikidata"
MANIFEST_PATH = PHOTO_DIR / "manifest.json"

# 300px is small enough to commit to the repo, large enough for an avatar
# circle and a slightly bigger profile-page render.
THUMB_WIDTH = 300

# Free-license whitelist used for the `usable` flag on each manifest record.
# Any LicenseShortName that contains one of these (case-insensitive) is
# treated as free-to-reuse-with-attribution. Anything else is recorded but
# flagged usable=False so the UI can fall back to the placeholder.
FREE_LICENSE_PATTERNS = (
    "cc0", "public domain", "pd-",
    "cc by 4", "cc by 3", "cc by 2",
    "cc by-sa 4", "cc by-sa 3", "cc by-sa 2",
    "ogl",
)

QUERY = """
SELECT DISTINCT
  ?person
  ?personLabel
  ?oireachtasId
  ?image
  ?constituencyLabel
  ?groupLabel
WHERE {
  ?person p:P39 ?tdStatement.

  ?tdStatement ps:P39 wd:Q654291 ;
               pq:P2937 wd:Q131309742 .

  OPTIONAL { ?person wdt:P2336 ?oireachtasId. }
  OPTIONAL { ?person wdt:P18   ?image. }
  OPTIONAL { ?tdStatement pq:P768  ?constituency. }
  OPTIONAL { ?tdStatement pq:P4100 ?group. }

  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en,ga" .
  }
}
ORDER BY ?constituencyLabel ?personLabel
"""


def fetch_sparql(query: str) -> list[dict]:
    params = urllib.parse.urlencode({"query": query, "format": "json"})
    url = f"{SPARQL_URL}?{params}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    return data["results"]["bindings"]


def image_url_to_thumb(commons_url: str, width: int) -> str:
    return f"{commons_url}?width={width}"


def safe_filename(value: str) -> str:
    keep = "-_."
    return "".join(c if c.isalnum() or c in keep else "_" for c in value)


def qid_from_uri(uri: str) -> str:
    return uri.rsplit("/", 1)[-1] if uri else ""


def commons_filename_from_url(url: str) -> str:
    """Extract the Commons filename from a Special:FilePath URL."""
    marker = "Special:FilePath/"
    idx = url.find(marker)
    if idx < 0:
        return ""
    return urllib.parse.unquote(url[idx + len(marker):])


def strip_html(s: str) -> str:
    """Plain-text version of HTML extmetadata fields (artist, credit, ...)."""
    if not s:
        return ""
    no_tags = re.sub(r"<[^>]+>", "", s)
    return _html.unescape(no_tags).strip()


def is_license_free(license_name: str) -> bool:
    if not license_name:
        return False
    lower = license_name.lower()
    return any(pat in lower for pat in FREE_LICENSE_PATTERNS)


def ext_from_content_type(content_type: str) -> str:
    ct = (content_type or "").lower()
    if "jpeg" in ct or "jpg" in ct:
        return ".jpg"
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    return ".jpg"


def download_image(url: str, save_dir: Path, base_name: str) -> tuple[Path | None, str]:
    """Download to save_dir/<base_name><ext>. Ext is derived from Content-Type."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": HEADERS["User-Agent"]})
        with urllib.request.urlopen(req, timeout=15) as resp:
            content_type = resp.headers.get("Content-Type", "")
            data = resp.read()
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}"
    except Exception as e:
        return None, str(e)

    save_path = save_dir / (base_name + ext_from_content_type(content_type))
    save_path.write_bytes(data)
    return save_path, content_type


def fetch_commons_metadata(filename: str) -> dict:
    """Fetch licensing metadata for a single Commons file via the API.

    Returns a dict with: artist, artist_html, license_name, license_url,
    credit, usage_terms, file_page_url, usable. Empty strings on missing
    fields. On HTTP/parse failure returns {"_error": ...}.
    """
    if not filename:
        return {"_error": "empty filename"}
    api_url = (
        f"{COMMONS_API}?"
        + urllib.parse.urlencode({
            "action": "query",
            "prop": "imageinfo",
            "iiprop": "extmetadata",
            "titles": f"File:{filename}",
            "format": "json",
            "formatversion": "2",
        })
    )
    try:
        req = urllib.request.Request(api_url, headers={"User-Agent": HEADERS["User-Agent"]})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return {"_error": str(e)}

    pages = data.get("query", {}).get("pages", [])
    if not pages:
        return {"_error": "no pages in response"}
    page = pages[0]
    imageinfo = page.get("imageinfo", [])
    if not imageinfo:
        return {"_error": "no imageinfo block"}

    em = imageinfo[0].get("extmetadata", {})
    artist_html  = em.get("Artist",           {}).get("value", "") or ""
    license_name = em.get("LicenseShortName", {}).get("value", "") or ""
    license_url  = em.get("LicenseUrl",       {}).get("value", "") or ""
    credit_html  = em.get("Credit",           {}).get("value", "") or ""
    usage_terms  = em.get("UsageTerms",       {}).get("value", "") or ""

    return {
        "artist":        strip_html(artist_html),
        "artist_html":   artist_html,
        "license_name":  strip_html(license_name),
        "license_url":   license_url,
        "credit":        strip_html(credit_html),
        "usage_terms":   strip_html(usage_terms),
        "file_page_url": f"https://commons.wikimedia.org/wiki/File:{urllib.parse.quote(filename)}",
        "usable":        is_license_free(license_name),
    }


def load_existing_manifest() -> dict[str, dict]:
    """Return {wikidata_qid: record} for previously-saved entries."""
    if not MANIFEST_PATH.exists():
        return {}
    try:
        prev = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(prev, list):
        return {}
    out: dict[str, dict] = {}
    for entry in prev:
        qid = entry.get("wikidata_qid")
        if qid:
            out[qid] = entry
    return out


def main() -> None:
    PHOTO_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Photo dir : {PHOTO_DIR}")
    print(f"Manifest  : {MANIFEST_PATH}")
    print()

    rows = fetch_sparql(QUERY)
    print(f"SPARQL results: {len(rows)}")
    if not rows:
        print("No results -- check the query or your connection.")
        return

    cache = load_existing_manifest()

    n_dl = n_dl_skip = n_dl_fail = n_no_image = 0
    n_meta_fetch = n_meta_skip = n_meta_fail = 0
    seen_persons: set[str] = set()
    out: list[dict] = []

    for r in rows:
        person_uri = r.get("person",            {}).get("value", "")
        name       = r.get("personLabel",       {}).get("value", "")
        oid        = r.get("oireachtasId",      {}).get("value")
        image      = r.get("image",             {}).get("value")
        constit    = r.get("constituencyLabel", {}).get("value", "")
        group      = r.get("groupLabel",        {}).get("value", "")

        if person_uri in seen_persons:
            continue
        seen_persons.add(person_uri)

        qid = qid_from_uri(person_uri)
        cached = cache.get(qid, {})

        # Refresh canonical fields from the latest SPARQL row, but keep
        # cached download + licence metadata if present.
        record: dict = {
            **cached,
            "wikidata_qid":      qid,
            "oireachtas_id":     oid,
            "name":              name,
            "constituency":      constit,
            "group":             group,
            "image_commons_url": image,
            "thumb_url":         image_url_to_thumb(image, THUMB_WIDTH) if image else None,
        }
        record.setdefault("local_file", None)

        safe_name = name.encode("ascii", "replace").decode("ascii")

        if not image:
            n_no_image += 1
            record["local_file"]  = None
            record["license_name"] = record.get("license_name") or ""
            out.append(record)
            print(f"NONE {qid:<10} {safe_name:<30} (no P18 on Wikidata)")
            continue

        # ── Image download (idempotent) ─────────────────────────────────────
        base = safe_filename(oid) if oid else qid
        existing = next(
            (p for p in PHOTO_DIR.glob(f"{base}.*") if p.name != "manifest.json"),
            None,
        )
        if existing is not None:
            record["local_file"] = existing.name
            n_dl_skip += 1
        else:
            local_path, info = download_image(
                image_url_to_thumb(image, THUMB_WIDTH), PHOTO_DIR, base,
            )
            if local_path is None:
                n_dl_fail += 1
                print(f"FAIL DL {qid:<10} {safe_name:<30} {info}")
                out.append(record)
                continue
            record["local_file"] = local_path.name
            n_dl += 1
            print(f"OK DL   {qid:<10} {safe_name:<30} -> {local_path.name}")

        # ── License metadata (idempotent) ───────────────────────────────────
        if record.get("license_name"):
            n_meta_skip += 1
        else:
            filename = commons_filename_from_url(image)
            meta = fetch_commons_metadata(filename)
            if meta.get("_error"):
                n_meta_fail += 1
                print(f"FAIL META {qid:<10} {safe_name:<30} {meta['_error']}")
            else:
                record.update(meta)
                n_meta_fetch += 1
                lic = (meta.get("license_name") or "?")[:24]
                usable = "usable" if meta.get("usable") else "BLOCKED"
                print(f"OK META {qid:<10} {safe_name:<30} {lic:<24} {usable}")

        out.append(record)

    MANIFEST_PATH.write_text(
        json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8",
    )

    # ── Summary ────────────────────────────────────────────────────────────
    print()
    print("DOWNLOADS")
    print("-" * 40)
    print(
        f"  new: {n_dl}    cached: {n_dl_skip}    failed: {n_dl_fail}    no P18: {n_no_image}"
    )
    print()
    print("LICENSE METADATA")
    print("-" * 40)
    print(
        f"  new: {n_meta_fetch}    cached: {n_meta_skip}    failed: {n_meta_fail}"
    )
    usable_count   = sum(1 for e in out if e.get("usable"))
    blocked_count  = sum(1 for e in out if e.get("license_name") and not e.get("usable"))
    unknown_count  = sum(1 for e in out if e.get("local_file") and not e.get("license_name"))
    print(f"  usable (free license): {usable_count}")
    print(f"  blocked (non-free)   : {blocked_count}")
    print(f"  unknown (no metadata): {unknown_count}")

    # License breakdown
    licenses: dict[str, int] = {}
    for e in out:
        lic = e.get("license_name") or "(none)"
        licenses[lic] = licenses.get(lic, 0) + 1
    print()
    print("LICENSE BREAKDOWN")
    print("-" * 40)
    for lic, count in sorted(licenses.items(), key=lambda kv: -kv[1]):
        print(f"  {count:>4}  {lic}")

    # Join-key coverage
    print()
    print("JOIN KEY COVERAGE")
    print("-" * 40)
    with_oid = sum(1 for e in out if e.get("oireachtas_id"))
    print(f"  with P2336 (Oireachtas ID): {with_oid} / {len(out)}")
    print(f"  unique_member_code looks like : Simon-Harris.D.2011-03-09")
    sample = next((e["oireachtas_id"] for e in out if e.get("oireachtas_id")), "(none)")
    print(f"  Wikidata P2336 sample         : {sample}")


if __name__ == "__main__":
    main()
