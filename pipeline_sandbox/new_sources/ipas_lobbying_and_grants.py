"""IPAS influence + integration-grant layer (SANDBOX ONLY).

Three things:
1. `ipas_lobbying.parquet`  — lobbying returns touching international protection /
   asylum / refugee / temporary protection (from silver lobbying returns_master).
2. `ip_integration_fund_2022.parquet` — the International Protection Integration Fund
   2022 grants (NGO recipients, amounts, counties) parsed from the user's PDF.
3. Two cross-references, each with an explicit identity caveat:
   a. Do the COMMERCIAL ACCOMMODATION PROVIDERS (who receive the money) appear in the
      lobbying register at all?
   b. Do INTEGRATION-FUND GRANT RECIPIENTS appear as lobbyists ("funded-then-lobbying")?

CO-OCCURRENCE IS NOT CAUSATION. Advocacy lobbying by a grant-funded NGO is lawful and
normal; this is a disclosure surface, never an allegation. Never sum grants with
procurement/payments (third money channel).
"""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import fitz
import polars as pl

from _common import SILVER, now_iso

LOBBY = "c:/Users/pglyn/PycharmProjects/dail_extractor/data/silver/lobbying/parquet/returns_master.parquet"
FUND_PDF = Path("c:/Users/pglyn/PycharmProjects/dail_extractor/the-international-protection-integration-fund-2022.pdf")
FUND_URL = "https://www.gov.ie/en/publication/international-protection-integration-fund-2022/"

KW = (r"(?i)international protection|asylum|refugee|direct provision|\bIPAS\b|"
      r"reception condition|accommodation centre|protection applicant|"
      r"temporary protection|Ukrain")


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\b(ltd|limited|clg|dac|clg|teoranta|company|the)\b", " ", s, flags=re.I)
    return re.sub(r"[^a-z0-9 ]", " ", s.lower()).strip()


def norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", fold(s))


def main() -> None:
    # ---------- 1. lobbying on IP/asylum ----------
    lf = pl.scan_parquet(LOBBY)
    cols = lf.collect_schema()
    txt = [c for c, d in cols.items() if d == pl.Utf8]
    expr = None
    for c in txt:
        e = pl.col(c).str.contains(KW)
        expr = e if expr is None else (expr | e)
    lob = lf.filter(expr).collect().with_columns([
        pl.lit("lobbying.ie returns_master").alias("source_name"),
        pl.lit(now_iso()).alias("derived_at"),
        pl.lit("co-occurrence disclosure only — advocacy lobbying is lawful; NEVER causation")
          .alias("caveat"),
    ])
    lob.write_parquet(SILVER / "ipas_lobbying.parquet", compression="zstd", statistics=True)
    print(f"ipas_lobbying: {lob.height} returns, "
          f"{lob['lobbyist_name'].n_unique()} distinct lobbyists")

    # ---------- 2. Integration Fund 2022 grants ----------
    doc = fitz.open(FUND_PDF)
    lines = [l.strip() for pg in doc for l in pg.get_text("text").splitlines() if l.strip()]
    EUR = re.compile(r"^€([\d,]+)$")
    rows, i = [], 0
    # layout: Organisation / Project Name / €Grant / County  (county may wrap)
    while i < len(lines):
        m = EUR.match(lines[i])
        if m and i >= 2:
            amount = int(m.group(1).replace(",", ""))
            project = lines[i - 1]
            org = lines[i - 2]
            county_parts = []
            j = i + 1
            while j < len(lines) and not EUR.match(lines[j]) and j < i + 4:
                nxt = lines[j]
                # stop if this line starts a new org (i.e. the line after is a project + €)
                if j + 2 < len(lines) and EUR.match(lines[j + 2]):
                    break
                county_parts.append(nxt)
                j += 1
            rows.append({"organisation": org, "project_name": project,
                         "grant_awarded_eur": amount,
                         "county": " ".join(county_parts).strip(" ,") or None})
        i += 1
    fund = pl.DataFrame(rows).unique(subset=["organisation", "project_name"], keep="first")
    fund = fund.with_columns([
        pl.lit(2022).alias("year"),
        pl.lit("International Protection Integration Fund").alias("scheme"),
        pl.lit("grant_awarded").alias("grant_basis"),
        pl.lit(False).alias("value_safe_to_sum"),
        pl.lit(FUND_URL).alias("source_url"),
        pl.lit(now_iso()).alias("derived_at"),
        pl.lit("pdf_table_parse").alias("extraction_method"),
        pl.lit("public_bodies_and_ngos").alias("privacy_tier"),
        pl.col("organisation").map_elements(norm_key, return_dtype=pl.Utf8).alias("org_key"),
    ])
    fund.write_parquet(SILVER / "ip_integration_fund_2022.parquet",
                       compression="zstd", statistics=True)
    tot = fund["grant_awarded_eur"].sum()
    print(f"\nip_integration_fund_2022: {fund.height} grants, total EUR {tot:,}")
    with pl.Config(tbl_rows=40, fmt_str_lengths=45):
        print(fund.select("organisation", "project_name", "grant_awarded_eur", "county")
                  .sort("grant_awarded_eur", descending=True).head(20))

    # ---------- 3a. do the ACCOMMODATION PROVIDERS lobby? ----------
    # Restrict to REAL commercial accommodation providers paid by Dept of Justice.
    # (The keyword scan that built provider_candidates matched 'centre', which pulls in
    #  NGOs like 'Mercy Law Resource Centre' — exclude the advocacy/legal/crisis false
    #  positives explicitly rather than silently.)
    NOT_A_PROVIDER = re.compile(r"(?i)rape crisis|law resource|resource centre|volunteer|"
                                r"family resource|refugee council|advocacy|citizens")
    prov = pl.read_parquet(SILVER / "cag_ipas_provider_candidates.parquet")
    real_prov = (prov.filter((pl.col("spend_stream") == "justice_ipas_2025plus") &
                             (~pl.col("provider_root").str.contains(NOT_A_PROVIDER.pattern)))
                     ["provider_root"].unique().to_list())
    lob_keys = {norm_key(n): n for n in lob["lobbyist_name"].unique().to_list()}
    prov_hits = {p: lob_keys[norm_key(p)] for p in real_prov if norm_key(p) in lob_keys}
    print(f"\n--- (a) COMMERCIAL ACCOMMODATION PROVIDERS ({len(real_prov)} paid by DoJ) "
          f"appearing as lobbyists on IP matters: {len(prov_hits)} ---")
    print("   ", prov_hits if prov_hits else
          "NONE. The providers receiving the money do NOT appear in the IP lobbying "
          "register. (Excluded 2 false positives from the 'centre' keyword: Mercy Law "
          "Resource Centre, Dublin Rape Crisis Centre — advocacy NGOs, not providers.)")

    # ---------- 3b. funded-then-lobbying (Integration Fund NGOs) ----------
    # Exact key match FAILS here (e.g. fund 'Doras' vs lobbyist 'Doras Luimni';
    # fund 'Nasc, the Migrant and Refugee Rights Centre' vs lobbyist 'Nasc Ireland').
    # Use distinctive-token overlap + record match_confidence — an identity GATE, not a guess.
    STOP = {"the", "of", "and", "for", "ireland", "irish", "centre", "center", "project",
            "group", "service", "services", "national", "support", "network", "migrant",
            "refugee", "rights", "community", "council", "association", "trust"}

    def tokens(s: str) -> set[str]:
        return {t for t in norm_key(s).split() if t not in STOP and len(t) > 3}

    fund_orgs = fund.select("organisation", "org_key", "grant_awarded_eur").to_dicts()

    # A shared token only identifies an org if it is DISTINCTIVE. Generic words
    # ('family', 'youth', 'city', 'foundation', 'international') appear across many
    # bodies and produce nonsense links (e.g. 'Twitter International Company' vs
    # 'Tralee International Resource Centre'). Gate on document-frequency: a token
    # must name at most 2 organisations in the lobbying corpus to be usable as a key.
    # ...and it must not be an ordinary English word: a rare-but-generic token
    # ('family', 'vision', 'work', 'cork') still mislinks unrelated bodies
    # (e.g. 'One Family' vs 'Happy Muslim Family of Ireland'). Only PROPER-NOUN-like
    # tokens identify an organisation.
    GENERIC = {
        "family", "vision", "work", "cork", "dublin", "galway", "limerick", "waterford",
        "kerry", "mayo", "sligo", "louth", "meath", "laois", "wicklow", "donegal",
        "initiative", "partnership", "resource", "information", "supporting", "people",
        "with", "young", "sports", "chamber", "alliance", "africa", "african", "muslim",
        "coast", "east", "west", "north", "south", "northside", "area", "county", "city",
        "connect", "voices", "solidarity", "sanctuary", "horizons", "doors", "open",
        "culture", "cultural", "integration", "employment", "education", "health",
        "wellbeing", "women", "youth", "foundation", "international", "welcome",
    }
    from collections import Counter
    df = Counter()
    for lname in lob_keys.values():
        for t in tokens(lname):
            df[t] += 1
    DISTINCTIVE = {t for t, n in df.items() if n <= 2 and t not in GENERIC}

    matches = []
    for f in fund_orgs:
        ft = tokens(f["organisation"])
        if not ft:
            continue
        for lk, lname in lob_keys.items():
            lt = tokens(lname)
            inter = ft & lt
            if not inter:
                continue
            key_tokens = inter & DISTINCTIVE
            exact = norm_key(f["organisation"]) == lk
            if not exact and not key_tokens:
                continue  # only generic tokens shared -> NOT an identity link
            conf = ("exact" if exact
                    else "high" if (inter == ft or inter == lt) and key_tokens
                    else "review")
            matches.append({"fund_organisation": f["organisation"], "lobbyist_name": lname,
                            "grant_awarded_eur": f["grant_awarded_eur"],
                            "shared_tokens": " ".join(sorted(inter)),
                            "distinctive_tokens": " ".join(sorted(key_tokens)) or None,
                            "match_confidence": conf,
                            "lobbying_returns": lob.filter(
                                pl.col("lobbyist_name") == lname).height})
    xref = pl.DataFrame(matches).unique(subset=["fund_organisation", "lobbyist_name"]) \
        if matches else pl.DataFrame()
    print(f"\n--- (b) INTEGRATION-FUND recipients that ALSO lobbied on IP matters: "
          f"{xref.height if xref.height else 0} candidate links ---")
    if xref.height:
        xref = xref.sort(["match_confidence", "lobbying_returns"], descending=[False, True])
        with pl.Config(tbl_rows=30, fmt_str_lengths=42, tbl_width_chars=150):
            print(xref)
        xref.write_parquet(SILVER / "ip_fund_lobbying_xref.parquet",
                           compression="zstd", statistics=True)
        xref.write_csv(SILVER / "_eyeball" / "ip_fund_lobbying_xref.csv")
    print("    NOTE: match_confidence is an IDENTITY GATE — 'medium' links share a "
          "distinctive token but need human confirmation before any publication.")
    print("    Advocacy lobbying by a grant-funded NGO is LAWFUL AND EXPECTED — this is a "
          "disclosure surface, never an allegation.")

    (SILVER / "_eyeball").mkdir(exist_ok=True)
    fund.write_csv(SILVER / "_eyeball" / "ip_integration_fund_2022.csv")
    (lob.group_by("lobbyist_name").len().sort("len", descending=True)
        .write_csv(SILVER / "_eyeball" / "ipas_lobbyists.csv"))


if __name__ == "__main__":
    main()
