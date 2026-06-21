# Ministerial-diary gazetteer — company add-list (candidates)

> **APPLIED 2026-06-21.** 11 added to `CURATED_ORGS` + chain re-run (company_influence 568→600):
> Merck, Amgen, Cisco (boundary guard blocks "San Francisco"), Oracle, Coca-Cola, Kerry Group,
> Teva, Becton Dickinson, Alexion, Thermo Fisher, Anthropic. **DROPPED on verification (false
> positives):** Roche ("Stephen Roche" / "Rochestown Park Hotel"), Baxter ("Peter Baxter" person),
> Bayer (OCR noise), bare Kerry (the county / Radio Kerry). The table below is the original scan.


Read-only scan (2026-06-21) of the diary engagements for pharma / tech / multinational brands
that ministers met but the diary org gazetteer does NOT yet capture — so they're invisible to
`v_ministerial_diary_org_overlap` and the `company_influence` cross-reference.

**How to apply (when the active diary-pipeline context is done — DO NOT run mid-flight):**
add canonical names to `CURATED_ORGS` in `extractors/diary_org_match.py` (Tier ⑥), then re-run
`extract→classify→match→overlap→promote_gold` + `diary_company_influence.py`. Each curated brand
key needs a substring-collision guard (cf. the "Microsoft Teams" / "Central Bank vs Bank of
Ireland" traps already in that file). Co-occurrence framing unchanged — access, not influence.

## MISSING — real misses worth adding (count = diary mentions, not in gazetteer)

| Add (canonical) | Mentions | In `other` | Ministers (sample) | Guard note |
|---|---|---|---|---|
| **Roche** (Roche Products (Ireland) Ltd) | 31 | 3 | Breen, Browne, Chambers, Coveney | biggest pharma miss; guard vs person "Roche" |
| **Cisco** (Cisco Systems) | 26 | 3 | Breen, Burke, Calleary, Chambers | biggest tech miss |
| **Merck** (Merck Sharp & Dohme / MSD) | 4 | 2 | Calleary, Martin, McGrath | ⚠ reconcile with existing `MSD` — same group in IE; decide one canonical |
| **Amgen** (Amgen Ireland) | 4 | 1 | Burke, Fitzgerald, Harris | |
| **West Pharmaceutical Services** | 4 | 0 | Chambers, Halligan, Higgins, Martin | match on "west pharma" not bare "west" |
| **Coca-Cola** (Coca-Cola HBC / The Coca-Cola Co.) | 4 | 0 | Calleary, Coveney, Varadkar | |
| **Becton Dickinson** | 3 | 0 | Breen, Richmond, Varadkar | match "becton" |
| **Oracle** (Oracle EMEA) | 3 | 1 | Burke, Humphreys, Richmond | slightly generic word — guard |
| **Baxter** (Baxter Healthcare) | 2 | 0 | Martin | |
| **Kerry Group** | 2 | 0 | Harris, Varadkar | guard vs "Kerry" county |
| **Anthropic** | 2 | 0 | Calleary | |
| **Bayer** | 1 | 0 | Harris | |
| **Teva** (Teva Pharmaceuticals Ireland) | 1 | 0 | Varadkar | short key — exact-ish guard |
| **Alexion** (Alexion Pharma) | 1 | 1 | Chambers | |
| **Thermo Fisher Scientific** | 1 | 0 | Coveney | |

## Spelling/variant of an ALREADY-captured org (just widen the alias, not a new entity)
- `open ai` → already have **OpenAI** (1 stray spaced spelling)
- `tirlan` → already have **Tirlán** (matched as "tirl")
- `intel ireland` → already have **Intel**

## Already captured (no action — listed so we don't re-add)
Pharma: MSD, Medtronic, Pfizer, Abbott, Stryker, Johnson & Johnson, Eli Lilly, Janssen, AbbVie,
Novo Nordisk, Sanofi, Gilead, Edwards Lifesciences, AstraZeneca, Takeda, Novartis, Regeneron,
Boston Scientific, Viatris, Jazz Pharma, Horizon Therapeutics.
Tech: Microsoft, Google, Intel, Amazon, Stripe, Mastercard, Facebook, Workday, PayPal, Analog
Devices, TikTok, Salesforce, LinkedIn, HPE, Accenture, Qualcomm, OpenAI.
Other: Virgin Media, Ryanair, Vodafone, Aldi, Tesco, KPMG, Musgrave, Deloitte, Lidl, Glanbia,
Grant Thornton, Tirlán, Diageo, Kingspan, Smurfit, Heineken, PepsiCo, Nestlé, Glen Dimplex.

## Notes
- **Pharma is a public-money blind spot**: even captured pharma show €0 awards/payments because
  state pharma spend flows via HSE drug reimbursement/pricing, not the procurement-award or
  PO-payment registers. "Met ministers, no contracts" ≠ "no public money" for this sector.
- Many of these are FDI multinationals whose access is at Taoiseach / Enterprise-minister level
  (e.g. "Announcement of Merck expansion" → Taoiseach Martin; Roche across 4+ ministers).
- The scan keyed on a fixed brand list; a broader pass over the `other` bucket (residents'
  associations, schools, GAA clubs, food hubs) would surface non-multinational local bodies too,
  but those are lower-value for the influence cross-reference.
