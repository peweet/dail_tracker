"""
test_wiki_data.py - Wikidata SPARQL photo exploration

Wikidata property P2336 = "Houses of the Oireachtas member ID"
Wikidata property P18   = "image" (Wikimedia Commons filename)

This script is a hands-on exploration -- run it, read the output, understand
what comes back before committing to a full pipeline enrichment script.

Run: python test_wiki_data.py
"""
import json
import urllib.parse
import urllib.request

# Wikidata SPARQL endpoint
SPARQL_URL = "https://query.wikidata.org/sparql"

# Wikidata blocks requests with no User-Agent -- this header is required
HEADERS = {
    "User-Agent": "DailTracker/0.1 (civic transparency research; contact p.glynn18@gmail.com)",
    "Accept": "application/sparql-results+json",
}

# SPARQL query
# Get all Wikidata items that have:
#   P2336 -- a Houses of the Oireachtas member ID (the bridge to our data)
#   P18   -- an image on Wikimedia Commons
QUERY = """
SELECT ?item ?itemLabel ?oireachtasId ?image WHERE {
  ?item wdt:P2336 ?oireachtasId .
  ?item wdt:P18   ?image .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
ORDER BY ?itemLabel
LIMIT 20
"""


def fetch_sparql(query: str) -> list[dict]:
    """Run a SPARQL query against Wikidata, return list of result rows."""
    params = urllib.parse.urlencode({"query": query, "format": "json"})
    url = f"{SPARQL_URL}?{params}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    return data["results"]["bindings"]


def image_url_to_thumb(commons_url: str, width: int = 200) -> str:
    """
    Convert a Wikidata P18 value to a Wikimedia Commons thumbnail URL.

    P18 values look like:
      http://commons.wikimedia.org/wiki/Special:FilePath/Simon_Harris.jpg

    Appending ?width=N gives a resized version from the Commons CDN.
    No authentication needed, no JavaScript needed.
    """
    return f"{commons_url}?width={width}"


def check_url_accessible(url: str) -> tuple[int, str]:
    """Return (HTTP status, content-type). 0 = connection error."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": HEADERS["User-Agent"]})
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.status, resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as e:
        return e.code, ""
    except Exception as e:
        return 0, str(e)


def main() -> None:
    print("Querying Wikidata SPARQL for Irish TDs with photos")
    print(f"Endpoint: {SPARQL_URL}")
    print()

    rows = fetch_sparql(QUERY)
    print(f"Results returned: {len(rows)}")
    print()

    if not rows:
        print("No results. Check the SPARQL query or your internet connection.")
        return

    # Print raw results
    print(f"{'Name':<30} {'Oireachtas ID':<40} Image (truncated)")
    print("-" * 110)
    for r in rows:
        name  = r.get("itemLabel",    {}).get("value", "?")
        oid   = r.get("oireachtasId", {}).get("value", "?")
        image = r.get("image",        {}).get("value", "?")
        # encode for safe printing on Windows terminal
        safe_name  = name.encode("ascii", "replace").decode("ascii")
        safe_image = image[:50].encode("ascii", "replace").decode("ascii")
        print(f"{safe_name:<30} {oid:<40} {safe_image}")

    print()

    # Test thumbnail URL for the first result
    first = rows[0]
    name  = first.get("itemLabel", {}).get("value", "?").encode("ascii", "replace").decode("ascii")
    image = first.get("image", {}).get("value", "")

    if image:
        thumb = image_url_to_thumb(image, width=200)
        print(f"Testing thumbnail URL for: {name}")
        print(f"Commons URL  : {image}")
        print(f"Thumb URL    : {thumb}")
        status, ct = check_url_accessible(thumb)
        if status == 200:
            print(f"Status       : {status} OK - content-type: {ct}")
            print("SUCCESS: thumbnail is publicly accessible, no auth needed.")
        else:
            print(f"Status       : {status} - may need a different URL pattern")
    else:
        print("No image URL in first result.")

    print()

    # Licensing summary
    print("LICENSING")
    print("-" * 40)
    print("Wikimedia Commons only hosts freely licensed content.")
    print("Most politician photos are CC BY-SA 4.0 or CC BY 4.0.")
    print("Attribution required: photographer name + license link shown under the photo.")
    print()
    print("To fetch per-image license + photographer at pipeline time:")
    print("  https://commons.wikimedia.org/w/api.php")
    print("  ?action=query&prop=imageinfo&iiprop=extmetadata&titles=File:filename.jpg")
    print("Store photo_url + photo_credit + photo_license in member reference parquet.")
    print()

    # Oireachtas ID format check -- the critical join key
    print("OIREACHTAS ID FORMAT CHECK")
    print("-" * 40)
    print("Our unique_member_code format : Simon-Harris.D.2011-03-09")
    oid_sample = rows[0].get("oireachtasId", {}).get("value", "?")
    print(f"Wikidata P2336 returns        : {oid_sample}")
    print()
    print("If these match -> direct join, no fuzzy matching needed.")
    print("If they differ -> we fall back to normalise_df_td_name as with other domains.")


if __name__ == "__main__":
    main()
