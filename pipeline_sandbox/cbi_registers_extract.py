"""CBI Registers — SANDBOX extract + cross-reference experiment.

Source : https://registers.centralbank.ie/downloadspage.aspx (ASP.NET postbacks)
Outputs (all under data/sandbox/, never gold/silver):
    data/sandbox/_cbi_raw/*.pdf                              cached source PDFs
    data/sandbox/parquet/cbi_authorised_firms.parquet        flattened firm rows
    data/sandbox/parquet/cbi_xref_member_interests.parquet   matches to TDs/Senators interests
    data/sandbox/parquet/cbi_xref_lobbying_entities.parquet  matches to lobbying return entities
    data/sandbox/_cbi_meta.json                              extraction stats

Why sandbox:
  * Source PDFs are SSRS-rendered tables — column inference is heuristic.
  * Cross-ref is a noise/signal experiment, not a production join.
  * Two registers (#57 CIT Providers, #58 Designated Entities) fail on direct
    postback today; left out of scope.

Honest framing:
  * NO write to data/gold or data/silver.
  * NO touch of pipeline.py / enrich.py / normalise_join_key.py.
  * Match tiers are conservative (exact normalised substring + dual signal),
    not fuzzy — bias to under-call rather than over-call.

CLI:
    python pipeline_sandbox/cbi_registers_extract.py --download
    python pipeline_sandbox/cbi_registers_extract.py --extract
    python pipeline_sandbox/cbi_registers_extract.py --xref
    python pipeline_sandbox/cbi_registers_extract.py            # all three (skips download if PDFs cached)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup
import fitz

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

URL = "https://registers.centralbank.ie/downloadspage.aspx"
H = {"User-Agent": "Mozilla/5.0 dail_extractor sandbox"}

_RAW = _ROOT / "data" / "sandbox" / "_cbi_raw"
_OUT = _ROOT / "data" / "sandbox" / "parquet"
_META = _ROOT / "data" / "sandbox" / "_cbi_meta.json"

# CBI reference number patterns observed across registers.
#   C153, C70109, C160182, C496731    — most common
#   F\d+, NU\d+                       — older schemes
#   306CU, 337CU                      — Credit Union register
_REF_RE = re.compile(r"\b(C\d{3,7}|F\d{3,6}|NU\d{3,6}|\d{2,4}CU)\b")

# Header/template lines to never treat as firm names.
_HEADER_TERMS = {
    "name and address", "name of undertaking", "credit union name",
    "applicant name", "business name", "business address",
    "reference number", "ref no", "ref no.", "ref. no.",
    "address", "authorisation date", "auth date", "auth date revocation",
    "registered office", "registered number", "registration date",
    "classes of business", "status", "country", "member state of origin",
    "head office", "regulated as", "registered on", "registered as",
    "tied to", "persons responsible", "passporting into",
    "service provided", "services", "business services",
    "financial instruments", "other  jurisdictions", "other jurisdictions",
    "page", "run date", "deposit-taking and other services",
    "client  money", "client money", "revocation",
    "revocation  reason", "revocation reason", "tied agents",
    "date of authorisation", "date authorised", "intermediary",
    "intermediary*", "reference  number", "intermediary**",
    "name", "trading name",
}

# Address / boilerplate prefixes (rejection heuristic).
_ADDRESS_RE = re.compile(
    r"^(?:\d+\s+|unit\s+|suite\s+|block\s+|floor\s+|po box|p\.o\.|"
    r"first floor|second floor|third floor|ground floor|"
    r"co\.\s|county\s|dublin\s\d|cork\s|galway\s|d\d{2}\s|"
    r"the\s.+(?:road|street|avenue|terrace|park|square|lane|place))",
    re.I,
)

# Country names (registers list these as their own column for cross-border firms).
_COUNTRY_ONLY = {
    "ireland", "spain", "netherlands", "denmark", "germany", "luxembourg",
    "france", "italy", "belgium", "austria", "portugal", "greece", "poland",
    "sweden", "finland", "lithuania", "latvia", "estonia", "czech republic",
    "slovakia", "slovenia", "hungary", "bulgaria", "romania", "croatia",
    "cyprus", "malta", "iceland", "norway", "liechtenstein",
    "united kingdom", "uk", "isle of man", "guernsey", "jersey",
    "united states", "usa", "switzerland", "canada", "japan", "australia",
}

# Suffixes to strip when normalising firm names. Deliberately CONSERVATIVE —
# we keep "dac", "icav", "ucits", "company", "co" as discriminating tokens
# (stripping them turned short fund names like "HORIZON ICAV" into the
# noise-bait "horizon", and triggered postcode/address false positives).
_SUFFIX_RE = re.compile(
    r"\b("
    r"public limited company|"
    r"limited liability partnership|"
    r"trading as|t/?a|"
    r"limited|ltd\.?|"
    r"plc|llp|"
    r"sa|nv|s\.r\.l|gmbh|"
    r"inc\.?"
    r")\b\.?",
    re.I,
)

# Generic words that, when alone, make a firm name too noisy to match on.
_STOPWORDS_FIRM = {
    "ireland", "europe", "international", "global", "group", "holdings",
    "services", "management", "capital", "investments", "investment",
    "financial", "finance", "bank", "insurance", "fund", "funds",
    "asset", "the", "of", "and", "a", "an",
}

# Irish placenames / common street types that surface as extraction-artifact
# "firm names" (e.g. "Co Limerick", "Church Street", "Kanturk"). Any firm name
# whose non-stopword tokens are ENTIRELY drawn from this set is rejected.
_IRISH_PLACES_AND_STREETS = {
    # Counties + city pieces
    "co", "county", "ireland",
    "dublin", "cork", "galway", "limerick", "waterford", "kilkenny", "wexford",
    "wicklow", "carlow", "kildare", "meath", "louth", "longford", "westmeath",
    "offaly", "laois", "kerry", "tipperary", "clare", "mayo", "sligo", "leitrim",
    "donegal", "roscommon", "cavan", "monaghan",
    # Common Dublin / city locales that appear bare in addresses
    "kanturk", "donnybrook", "ballsbridge", "rathmines", "ranelagh", "killarney",
    "tralee", "ennis", "navan", "drogheda", "dundalk", "bray", "wexford",
    "athlone", "mullingar", "tullamore", "letterkenny", "killaloe",
    "blackrock", "blackrock", "dun laoghaire", "dunlaoghaire",
    # Street/road words
    "street", "st", "road", "rd", "avenue", "ave", "place", "park", "lane",
    "square", "terrace", "drive", "close", "court", "way", "row", "quay",
    "hill", "view", "grove", "crescent", "garden", "gardens",
    "church", "high", "main", "upper", "lower", "north", "south", "east", "west",
    "old", "new",
}

# Corporate-form keywords whose presence in the RAW firm name is strong
# evidence the row is a real authorised entity (not an address fragment).
_CORP_KEYWORD_RE = re.compile(
    r"\b(limited|ltd\.?|plc|dac|llp|company|co\.|"
    r"bank|banking|"
    r"insurance|assurance|reinsurance|"
    r"credit union|cu\b|"
    r"icav|ucits|fund|funds|"
    r"asset management|investments?|"
    r"trust|partners?|holdings|group|"
    r"capital|financial|finance|securities|brokers?|"
    r"gmbh|s\.a|n\.v|sas|spa|spc|"
    r"plc\.?|inc\.?"
    r")\b",
    re.I,
)


def _slug(s: str, n: int = 80) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_")
    return s[:n]


def _norm_firm(name: str) -> str:
    """Lowercase, strip accents, drop legal suffixes, collapse whitespace."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    # Drop "t/a XYZ" trading-as portions (everything after t/a)
    s = re.sub(r"\bt/?a\b.*$", "", s)
    # Strip legal suffixes
    s = _SUFFIX_RE.sub(" ", s)
    # Strip punctuation
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_meaningful_firm_token(norm: str) -> bool:
    """Reject single-stopword or near-empty normalised names that would noise-match."""
    if not norm:
        return False
    tokens = [t for t in norm.split() if t not in _STOPWORDS_FIRM]
    # need at least one non-stopword token of length >=4 OR a multi-token phrase
    if len(tokens) == 0:
        return False
    if len(tokens) == 1 and len(tokens[0]) < 5:
        return False
    return len(norm) >= 6


def _is_strong_firm_match_candidate(raw: str, norm: str) -> bool:
    """Stricter filter used for member-interests substring matching.

    The substring strategy is precision-sensitive — short generic strings like
    "Co Limerick" or "Church Street" hit every address. Require either:
      * a corporate keyword in the raw form (Limited, Plc, DAC, Bank, etc.), or
      * a normalised name >= 14 chars AND at least 2 non-place tokens.
    Always reject if every non-stopword token is an Irish place/street word.
    """
    if not norm or not _is_meaningful_firm_token(norm):
        return False
    tokens = norm.split()
    # All tokens are stopwords + place words?
    non_place = [t for t in tokens if t not in _STOPWORDS_FIRM and t not in _IRISH_PLACES_AND_STREETS]
    if not non_place:
        return False
    if _CORP_KEYWORD_RE.search(raw or ""):
        return True
    if len(norm) >= 14 and len(non_place) >= 2:
        return True
    return False


# ─── 1. DOWNLOAD ──────────────────────────────────────────────────────────────

def list_postback_targets() -> list[dict]:
    s = requests.Session()
    r = s.get(URL, headers=H, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    items = []
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        text = " ".join(a.get_text(" ", strip=True).split())
        m = re.search(r"__doPostBack\('([^']+)'", href)
        if m and text:
            items.append({"title": text, "target": m.group(1)})
    return items


def download_all(force: bool = False) -> list[dict]:
    items = list_postback_targets()
    _RAW.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    results = []
    for i, it in enumerate(items):
        fname = f"{i:02d}_{_slug(it['title'])}.pdf"
        path = _RAW / fname
        if path.exists() and not force:
            results.append({"i": i, "title": it["title"], "file": str(path), "bytes": path.stat().st_size, "cached": True})
            continue
        rg = s.get(URL, headers=H, timeout=30)
        sp = BeautifulSoup(rg.text, "html.parser")
        def _hidden(name):
            el = sp.find("input", {"name": name})
            return el.get("value", "") if el else ""
        data = {
            "__EVENTTARGET": it["target"],
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": _hidden("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": _hidden("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION": _hidden("__EVENTVALIDATION"),
        }
        try:
            rp = s.post(URL, headers={**H, "Content-Type": "application/x-www-form-urlencoded"},
                        data=data, timeout=120)
        except Exception as e:
            results.append({"i": i, "title": it["title"], "error": str(e)})
            continue
        body = rp.content
        if body[:4] != b"%PDF":
            results.append({"i": i, "title": it["title"], "error": "not_pdf", "bytes": len(body)})
            continue
        path.write_bytes(body)
        results.append({"i": i, "title": it["title"], "file": str(path), "bytes": len(body), "cached": False})
        print(f"  [{i:2d}] {len(body):>10,}b  {it['title'][:80]}")
    return results


# ─── 2. EXTRACT ───────────────────────────────────────────────────────────────

def _looks_like_firm_name(line: str) -> bool:
    """Heuristic — accept lines that plausibly are an organisation name."""
    if not line or len(line) < 4 or len(line) > 200:
        return False
    low = line.lower().strip(".:,;-* ")
    if low in _HEADER_TERMS:
        return False
    if low in _COUNTRY_ONLY:
        return False
    # Skip address-looking lines
    if _ADDRESS_RE.match(line):
        return False
    # Skip pagination / metadata
    if re.match(r"^(page\s+\d|run date|total number|the (firms|undertakings|societies|register|registers|persons|companies)\b|listed (here|below)\b|please note|under (the|article|section)|all\s+credit|section\s+\d|undertakings with their head offices|in accordance|pursuant to|firms? on this register|name (and address|of undertaking)|insurance distribution|the register)", line, re.I):
        return False
    # Skip pure boilerplate that mentions Central Bank of Ireland in narrative
    if "central bank of ireland" in low and len(line) > 50:
        return False
    # Must contain at least one alphabetic char
    if not re.search(r"[A-Za-z]", line):
        return False
    # If it's a single word and looks like a header (all-caps short word), reject
    if len(line.split()) == 1 and (line.isupper() or line.islower()):
        return False
    # If too many digits, looks like an Eircode/phone/date
    digits = sum(c.isdigit() for c in line)
    if digits > 0 and digits / len(line) > 0.4:
        return False
    return True


def extract_firms_from_pdf(pdf_path: Path, register_title: str) -> list[dict]:
    """Per-PDF firm row extraction (heuristic, multi-layout).

    Strategy: read every text block on every page. For each block:
      1. If the block contains a CBI reference token (Cxxxxx, NUxxxxx, NNNCU),
         attribute the first plausible firm-name line in that block to the ref.
      2. Otherwise, treat the first non-empty line as a *candidate* firm name
         and accept it if it passes _looks_like_firm_name() — this lets us
         catch registers that have no per-row ref number (banks, insurers,
         credit unions with country-only columns).

    Sandbox-grade: there will be false positives (header strings the heuristic
    missed). Cross-ref filters most of them out, since real TD interests /
    lobbying entities won't match noise.
    """
    rows = []
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        return [{"_error": f"open: {e}", "register": register_title}]

    for page_no in range(doc.page_count):
        page = doc.load_page(page_no)
        blocks = page.get_text("blocks") or []
        # Sort blocks top-to-bottom, then left-to-right
        blocks_sorted = sorted(blocks, key=lambda b: (round(b[1] / 5) * 5, b[0]))
        # Discover the leftmost-column x0 (the "firm column" cluster)
        x0s = sorted({round(b[0]) for b in blocks_sorted})
        leftmost = x0s[0] if x0s else 0

        for blk in blocks_sorted:
            x0, _, _, _, text, *_ = blk
            text = (text or "").strip()
            if not text:
                continue

            # PATH 1: ref-anchored
            ref_match = _REF_RE.search(text)
            ref = ref_match.group(0) if ref_match else ""
            firm_name = ""

            if ref:
                # Try first line after the ref
                tail = text[ref_match.end():].strip("\n :\t-")
                for ln in tail.splitlines():
                    ln = ln.strip()
                    if ln and not _REF_RE.fullmatch(ln) and _looks_like_firm_name(ln):
                        firm_name = ln
                        break
                if not firm_name:
                    head = text[:ref_match.start()].strip(" :\t-")
                    head_lines = [ln.strip() for ln in head.splitlines() if ln.strip()]
                    if head_lines and _looks_like_firm_name(head_lines[-1]):
                        firm_name = head_lines[-1]
            else:
                # PATH 2: leftmost-column block, first line as candidate name.
                # Only attempt if this block is roughly in the leftmost column
                # (tolerance ~15px) to avoid mistaking address/value columns
                # for firm names on rows where ref-anchoring would normally work.
                if round(x0) - leftmost <= 15:
                    first_line = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
                    if _looks_like_firm_name(first_line):
                        firm_name = first_line

            if not firm_name:
                continue

            rows.append({
                "register": register_title,
                "page": page_no + 1,
                "ref_no": ref or "",
                "firm_name_raw": firm_name,
                "firm_name_norm": _norm_firm(firm_name),
                "block_text": text[:500],
            })
    doc.close()
    return rows


def extract_all() -> pl.DataFrame:
    pdf_paths = sorted(_RAW.glob("*.pdf"))
    all_rows = []
    print(f"Extracting firms from {len(pdf_paths)} PDFs ...")
    for p in pdf_paths:
        # Reconstruct register title from filename prefix
        title = re.sub(r"^\d+_", "", p.stem).replace("_", " ")
        rows = extract_firms_from_pdf(p, title)
        all_rows.extend(rows)
        n_real = sum(1 for r in rows if "_error" not in r)
        print(f"  {p.name:80s}  rows={n_real}")
    df = pl.DataFrame(
        [r for r in all_rows if "_error" not in r],
        schema={"register": pl.Utf8, "page": pl.Int32, "ref_no": pl.Utf8,
                "firm_name_raw": pl.Utf8, "firm_name_norm": pl.Utf8, "block_text": pl.Utf8}
    )
    # Deduplicate exact (register, ref_no, firm_name_raw)
    df = df.unique(subset=["register", "ref_no", "firm_name_raw"], keep="first")
    return df


# ─── 3. CROSS-REFERENCE ───────────────────────────────────────────────────────

def _norm_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def xref_member_interests(firms: pl.DataFrame) -> pl.DataFrame:
    """For each declared interest, find CBI firms whose normalised name appears as a substring."""
    paths = [
        ("Dáil",   _ROOT / "data" / "silver" / "parquet" / "dail_member_interests_combined.parquet"),
        ("Seanad", _ROOT / "data" / "silver" / "parquet" / "seanad_member_interests_combined.parquet"),
    ]
    parts = []
    for house, p in paths:
        if not p.exists():
            continue
        df = pl.read_parquet(p).with_columns(pl.lit(house).alias("house"))
        parts.append(df)
    interests = pl.concat(parts, how="diagonal") if parts else None
    if interests is None or interests.is_empty():
        return pl.DataFrame()

    # Pre-normalise interest text
    interests = interests.with_columns(
        pl.col("interest_description_cleaned").map_elements(_norm_text, return_dtype=pl.Utf8).alias("interest_norm")
    ).filter(pl.col("interest_norm").str.len_chars() >= 6)

    # Build deduplicated firm-name index — STRICT filter for substring matching
    # (every char in interests is a substring-attack-surface, so under-call > over-call).
    candidates = (
        firms
        .filter(pl.col("firm_name_norm").is_not_null() & (pl.col("firm_name_norm").str.len_chars() >= 6))
    )
    # Apply strict filter row-by-row in Python — small dataset, simple logic.
    strict_mask = [
        _is_strong_firm_match_candidate(raw or "", norm or "")
        for raw, norm in zip(candidates["firm_name_raw"].to_list(), candidates["firm_name_norm"].to_list())
    ]
    candidates = candidates.filter(pl.Series(strict_mask))

    firm_idx = (
        candidates
        .group_by("firm_name_norm")
        .agg(
            pl.col("register").n_unique().alias("n_registers"),
            pl.col("register").unique().alias("registers"),
            pl.col("ref_no").unique().alias("ref_nos"),
            pl.col("firm_name_raw").first().alias("firm_name_raw_first"),
        )
    )
    print(f"  firm-name index: {firm_idx.height} unique normalised names (after strict filter)")
    print(f"  interest rows  : {interests.height} (post-norm)")

    # Substring match — pure python loop. n_firms ~ a few thousand; n_interests ~ 7k.
    # Worst case ~ tens of millions; we keep it simple by also requiring the firm token
    # appear as a delimited substring (start-of-line, space-surrounded, or end-of-line).
    firm_rows = firm_idx.iter_rows(named=True)
    firm_list = list(firm_rows)
    int_rows = list(interests.select(
        "house", "full_name", "unique_member_code", "interest_category",
        "interest_description_cleaned", "interest_norm", "year_declared"
    ).iter_rows(named=True))

    hits = []
    for f in firm_list:
        fn = f["firm_name_norm"]
        # Use word-boundary-ish surrounded match to avoid 'data' matching 'database'
        token = f" {fn} "
        for ir in int_rows:
            t = f" {ir['interest_norm']} "
            if token in t:
                hits.append({
                    "house": ir["house"],
                    "full_name": ir["full_name"],
                    "unique_member_code": ir["unique_member_code"],
                    "year_declared": ir.get("year_declared"),
                    "interest_category": ir["interest_category"],
                    "firm_name_norm": fn,
                    "firm_name_raw": f["firm_name_raw_first"],
                    "registers": f["registers"],
                    "n_registers": f["n_registers"],
                    "ref_nos": f["ref_nos"],
                    "interest_excerpt": (ir["interest_description_cleaned"] or "")[:200],
                })
    return pl.DataFrame(hits)


def xref_corporate_notices(firms: pl.DataFrame) -> pl.DataFrame:
    """Cross-ref CBI authorised firms against Iris corporate notices entity_name.

    Civic frame: when an entity wound up / in receivership / in examinership
    in Iris Oifigiúil is itself a CBI-authorised firm, that is a regulatory
    distress signal — much stronger than the standalone register or notice
    alone. Match is EXACT on normalised entity name (no substrings, no fuzz).
    """
    p = _ROOT / "data" / "gold" / "parquet" / "corporate_notices.parquet"
    if not p.exists():
        return pl.DataFrame()
    cn = pl.read_parquet(p).with_columns(
        pl.col("entity_name").map_elements(_norm_firm, return_dtype=pl.Utf8).alias("entity_norm")
    ).filter(pl.col("entity_norm").str.len_chars() >= 6)

    firm_idx = (
        firms.filter(pl.col("firm_name_norm").str.len_chars() >= 6)
             .group_by("firm_name_norm")
             .agg(
                 pl.col("register").unique().alias("registers"),
                 pl.col("ref_no").unique().alias("ref_nos"),
                 pl.col("firm_name_raw").first().alias("firm_name_raw_first"),
             )
    )
    return cn.join(firm_idx, left_on="entity_norm", right_on="firm_name_norm", how="inner")


def xref_lobbying_entities(firms: pl.DataFrame) -> pl.DataFrame:
    p = _ROOT / "data" / "silver" / "parquet" / "lobbying_return_documents.parquet"
    if not p.exists():
        return pl.DataFrame()
    lob = pl.read_parquet(p).select("primary_key", "lobbyist_name")
    lob = lob.with_columns(
        pl.col("lobbyist_name").map_elements(_norm_firm, return_dtype=pl.Utf8).alias("lobbyist_norm")
    )

    firm_idx = (
        firms
        .filter(pl.col("firm_name_norm").is_not_null() & (pl.col("firm_name_norm").str.len_chars() >= 6))
        .filter(pl.col("firm_name_norm").map_elements(_is_meaningful_firm_token, return_dtype=pl.Boolean))
        .group_by("firm_name_norm")
        .agg(
            pl.col("register").unique().alias("registers"),
            pl.col("ref_no").unique().alias("ref_nos"),
            pl.col("firm_name_raw").first().alias("firm_name_raw_first"),
        )
    )
    # Exact normalised-name equality (lobbyist names are short, so substring would over-match)
    join = lob.join(firm_idx, left_on="lobbyist_norm", right_on="firm_name_norm", how="inner")
    return join.rename({"lobbyist_norm": "firm_name_norm"})


# ─── 4. WRITE ─────────────────────────────────────────────────────────────────

def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)
    print(f"  wrote {path.name:50s}  rows={df.height:>6}  cols={df.width}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--download", action="store_true", help="re-fetch all PDFs (else uses cache)")
    ap.add_argument("--force-download", action="store_true", help="ignore cache, re-fetch")
    ap.add_argument("--extract", action="store_true", help="rebuild firm table from cached PDFs")
    ap.add_argument("--xref", action="store_true", help="build cross-ref tables (member interests + lobbying)")
    args = ap.parse_args()

    if not (args.download or args.extract or args.xref):
        # Default: ensure cache, extract, xref
        args.download = True
        args.extract = True
        args.xref = True

    meta = {}

    if args.download or args.force_download:
        print("[1/3] Download (cached -> skip)")
        results = download_all(force=args.force_download)
        meta["download"] = {
            "n_attempted": len(results),
            "n_cached": sum(1 for r in results if r.get("cached")),
            "n_fetched": sum(1 for r in results if "file" in r and not r.get("cached")),
            "n_failed":  sum(1 for r in results if "error" in r),
            "failed":    [r for r in results if "error" in r],
        }

    firms_path = _OUT / "cbi_authorised_firms.parquet"
    if args.extract:
        print("\n[2/3] Extract firm rows from PDFs")
        firms = extract_all()
        _write_parquet(firms, firms_path)
        meta["extract"] = {
            "n_firm_rows": firms.height,
            "n_unique_firm_names": firms.select(pl.col("firm_name_norm").n_unique()).item(),
            "n_registers": firms.select(pl.col("register").n_unique()).item(),
            "rows_per_register": (
                firms.group_by("register")
                     .agg(pl.len().alias("n"))
                     .sort("n", descending=True)
                     .head(15)
                     .to_dicts()
            ),
        }

    if args.xref:
        print("\n[3/3] Cross-reference")
        firms = pl.read_parquet(firms_path)

        print(" - Member interests")
        xref_mi = xref_member_interests(firms)
        _write_parquet(xref_mi, _OUT / "cbi_xref_member_interests.parquet")

        print(" - Lobbying entities")
        xref_lob = xref_lobbying_entities(firms)
        _write_parquet(xref_lob, _OUT / "cbi_xref_lobbying_entities.parquet")

        print(" - Corporate notices")
        xref_cn = xref_corporate_notices(firms)
        _write_parquet(xref_cn, _OUT / "cbi_xref_corporate_notices.parquet")

        meta["xref"] = {
            "member_interests": {
                "n_matches": xref_mi.height,
                "n_distinct_members": xref_mi.select(pl.col("unique_member_code").n_unique()).item() if xref_mi.height else 0,
                "n_distinct_firms":   xref_mi.select(pl.col("firm_name_norm").n_unique()).item() if xref_mi.height else 0,
            },
            "lobbying": {
                "n_matches": xref_lob.height,
                "n_distinct_lobbyists": xref_lob.select(pl.col("lobbyist_name").n_unique()).item() if xref_lob.height else 0,
                "n_distinct_firms":     xref_lob.select(pl.col("firm_name_norm").n_unique()).item() if xref_lob.height else 0,
            },
            "corporate_notices": {
                "n_matches": xref_cn.height,
                "n_distinct_firms":     xref_cn.select(pl.col("entity_norm").n_unique()).item() if xref_cn.height else 0,
                "by_category": (
                    xref_cn.group_by("notice_category")
                           .agg(pl.len().alias("n"))
                           .sort("n", descending=True)
                           .to_dicts()
                    if xref_cn.height else []
                ),
            },
        }

    _META.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"\nmeta -> {_META}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
