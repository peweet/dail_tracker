# Disclosed PO/payments — CROSS-CORPUS LEVERAGE (WHAT-CAN-BE-DETERMINED #4)

Question: can the suppliers named in the disclosed EUR-20k file be JOINED to what we
already hold (lobbying register, ministerial diaries, eTenders/TED awards, CRO), and
what features/stories does that unlock?

Method: rough name-normaliser (uppercase, strip trading-as tail, drop `&`/punctuation,
drop corporate suffixes LTD/LIMITED/PLC/DAC/GROUP/IRELAND/…), then set-membership join
of disclosed supplier_norm against the normalised key of each gold corpus. Candidate set
= union of top-50-by-gross + top-40-by-distinct-bodies = **79 distinct suppliers**.
Script: `pipeline_sandbox/disclosed_po_spend/cross_corpus_leverage.py` (read-only).

> CAVEAT carried from prior workflow: disclosed "gross" mixes PO-commitment bodies with
> payment-list bodies and MUST NOT be read as spend. Every euro figure here is
> gross-line-value-within-the-disclosed-file, used only to rank/size suppliers — not a
> spend claim. Award EUR are gold `value_eur` (framework/DPS-inflated, also not spend).

## Headline match rates (candidate set, n=79)

| Corpus | Hits | Rate |
|---|---|---|
| (c) procurement AWARDS (eTenders/TED winner) | 48/79 | **60.8%** |
| (d) CRO via `procurement_supplier_cro_match` | 25/79 | 31.6% |
| (a) lobbying register (lobbyist OR client org) | 24/79 | 30.4% |
|     …lobbyist-org only | 16/79 | 20.3% |
|     …lobby-client only | 12/79 | 15.2% |
| (b) ministerial diaries (orgs ministers met) | 15/79 | 19.0% |
| our existing fact suppliers (already parsed) | 69/79 | 87.3% |
| our fact rows that already carry a `cro_company_num` | 31/79 | 39.2% |
| charities register | 1/79 | 1.3% |

Read-across: the top suppliers are **highly joinable**. ~6 in 10 of the biggest disclosed
payees are already eTenders/TED award winners we hold — so a disclosed payment line can be
hung off a tendered contract. ~1 in 3 already carry a resolvable CRO number, and ~3 in 10
appear in the lobbying register. Charities is near-zero (these are commercial suppliers).
Match rate is a FLOOR — it is a strict normalised-string equality; fuzzy/CRO-bridged
matching would lift every cell (e.g. "SISK"→"John Sisk & Son", "LAGAN", "SIAC" miss only
because the disclosed string is a short trade name the gold key spells out in full).

## (a) Lobbying register — concrete linkages

- **Roadstone** — disclosed gross EUR 638.4m across **36 bodies**; appears as a lobbying
  **client** (`top_client_companies`: 2 returns, 3 politicians targeted, 1 policy area).
- **An Post** — gross EUR 470.4m / 51 bodies; lobbying client (1 return) AND lobbyist org.
- **KPMG / Grant Thornton / Mazars / Deloitte / EY / PwC** — all Big-4/advisory firms appear
  both as paid suppliers (74–87 bodies each) and as lobbyist organisations.
- **Pfizer** — gross EUR 341.9m / 2 bodies; lobbyist + client.

## (b) Ministerial diaries — concrete linkages (org ministers actually met)

- **Accenture PLC** — met Min. **Bruton** (2017-04-27, 2017-09-05) and Min. **Coveney**
  (2024-01-09, "Visit Accenture R&D and Innovation…"); disclosed gross EUR 637.1m / 20 bodies.
- **Deloitte Ireland** — met Min. **McGrath** (2020-11-08 call; 2023-09-22 Cork leadership)
  and Min. **Chambers** (2025-03-02 Infrastructure Event); disclosed EUR 446.0m / 82 bodies.
- **Vodafone Ireland** — met Min. **Donohoe** (2020-02-14, 2023-05-21 Joakim Reiter);
  disclosed EUR 274.9m / 93 bodies.
- **IBM** — met Min. **Donohoe** (2019), **Harris** (2022 MOU announcement), **Chambers** (2025);
  disclosed EUR 253.4m / 16 bodies.
- **Pfizer** — met Min. **Donohoe** repeatedly (2017–2018, incl. CEO Ian Read).

## (c) Procurement AWARDS — payment <-> tendered-contract bridge

48/79 candidate firms are eTenders/TED award winners we already hold. Award `value_eur` is
framework/DPS-inflated (NOT spend) but the link itself is the asset — it ties a disclosed
payment line to a named tender / contracting authority. Examples (award rows / authorities):

| Firm | Award rows / authorities | Disclosed gross / bodies |
|---|---|---|
| Roadstone | 125 / 26 | EUR 638.4m / 36 |
| Accenture | 24 / 7 | EUR 637.1m / 20 |
| PFH Technology | 281 / 49 | EUR 626.8m / 116 |
| Deloitte | 368 / 60 | EUR 446.0m / 82 |
| Version 1 | 25 / 5 | EUR 419.1m / 46 |
| Micromail | 195 / 41 | EUR 349.4m / 117 |
| Wills Bros | 33 / 15 | EUR 501.2m / 24 |

Note CHC Ireland and Roscommon CC show award **rows but EUR 0** (award value blank / pre-2024
TED value gap) — flag, do not treat as zero contract.

## (d) CRO — supplier -> legal entity

`procurement_supplier_cro_match` resolves top disclosed firms to a CRO number with
`exact_unique` confidence 0.9:

| Firm | CRO no. | status | method |
|---|---|---|---|
| Roadstone | 11035 | Normal | exact_unique |
| Accenture | 340745 | Normal | exact_unique |
| Version 1 | 532721 | Normal | exact_unique |
| Micromail | 215566 | Normal | exact_unique |
| Tetra Ireland Communications | 406355 | Normal | exact_unique |
| Abtran | 260018 | Normal | exact_**ambiguous** (0.5 — verify) |

So the disclosed file can be lifted from free-text supplier strings to legal entities, which
then chains to the CRO register (status / distress / group structure) and to our corporate
notices corpus. Our own `procurement_payments_fact.cro_company_num` already carries a CRO
number for 39.2% of these candidate firms — i.e. the resolution is partly pre-computed.

## Firms appearing in lobby + diary + disclosed-payment (the triple)

These lobbied a politician, met a minister AND are large disclosed payees — the strongest
accountability cross-refs:

Roadstone (EUR 638.4m/36 bodies), Accenture (637.1m/20), An Post (470.4m/51),
Deloitte (446.0m/82), Kerry CC (346.0m/16), Pfizer (341.9m/2), Vodafone (274.9m/93),
IBM (253.4m/16), KPMG (131.4m/87), Mason Hayes Curran (93.1m/43),
McCann FitzGerald (78.2m/49), Grant Thornton (70.9m/74), Mazars (26.3m/85).

## Whole-of-government footprint (top cross-body suppliers)

The disclosed file's superpower vs our 72-publisher parse is **breadth** — it exposes one
firm's spread across the entire state. Top by distinct bodies:
Datapac 123, Micromail 117, PFH Technology 116, Dell 109, Vodafone 93, KPMG 87, Mazars 85,
Deloitte 82, Eir 78, PwC 77, C&AG 77, EY 75, Grant Thornton 74. The IT-reseller layer
(Datapac/Micromail/PFH/Dell/SoftwareOne/Ergo/Ekco/Softcat/Sord) and the advisory layer
(Big-4 + Mazars/Grant Thornton) are the most pervasive — exactly the firms most useful for a
"who supplies the whole state" lens, and all CRO-resolvable + mostly award-linked.

## Feature / story opportunities unlocked

1. **Lobbied-then-paid** — firms that met a minister / filed a lobbying return AND are large
   disclosed payees of (ideally) that minister's department. 13 triple-match firms ready.
2. **Award-to-payment realisation bridge** — link a disclosed payment line to a tendered
   contract (60.8% of top firms are award winners). Powers a "did the tender turn into
   payments, and how much" trail; caveat both sides as not-directly-summable.
3. **Whole-of-government footprint of firm X** — one page per supplier showing every body
   that names them (Datapac across 123 bodies, PFH across 116), CRO identity, awards held,
   ministers met. The disclosed file is the only source giving this breadth.
4. **IT-reseller / Big-4 concentration** — a small ring of resellers + advisory firms touch
   ~half of all public bodies; quantifiable concentration story, all entity-resolved.
5. **Entity-resolve the disclosed file** — push disclosed suppliers through
   `procurement_supplier_cro_match` to attach CRO numbers, then chain to corporate-notices
   distress / group rollup (e.g. BAM-style group aggregation already in gold).
6. **Recover suppliers in NEW bodies** — for the 141 genuinely-new bodies (Irish Water, DCC,
   Garda, etc.) the disclosed supplier strings are joinable the same way, extending every
   cross-ref above to bodies we never parsed.

## Limits / honesty

- Match rate is a strict-equality FLOOR; short trade names (SISK, LAGAN, SIAC, BAM) miss only
  on spelling and would join via fuzzy/CRO bridge. CRO-bridged join is the right long-term key.
- Disclosed gross and award value_eur are BOTH not-spend; never sum across PO/payment
  semantics or across framework call-offs. Use linkages for narrative, not euro totals.
- Many top "suppliers" are themselves public bodies (Cork CC, Mayo CC, HEA, OPW, C&AG, ESB)
  — inter-body transfers, not market purchases; exclude from a private-vendor lens.
- `exact_ambiguous` CRO matches (e.g. Abtran 0.5) must be verified before publishing identity.
