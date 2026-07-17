---
tier: CONTEXT
status: LIVE
domain: data
updated: 2026-07-14
supersedes: []
read_when: before attempting any cross-register association/join between datasets (ORG name-norm or PERSON anagram key)
key: CONTEXT|LIVE|data
---

# JOIN MAP — how to cross-reference this data (and how NOT to)

**Read this before attempting ANY cross-register association.** It is the one place that records
the canonical keys, the join graph, the measured yields, the structural blind spots, and the
never-join rules. Everything here is **measured from the data**, not asserted — figures dated
2026-07-14; re-measure with `tools/`-style queries if the data has moved.

Siblings: `doc/DATA_MAP.md` (maturity tiers + the 3-money-grain rule) · `doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md`
(the dossier design — **note it still says "design-only"; the `organisation_dossier` tool is in fact LIVE**).

---

## 1. There are TWO canonical keys. Never conflate them.

The code says this explicitly, and it is load-bearing.

### ORG key — `shared/name_norm.py`
`name_norm_expr(col)` (Polars) and `name_norm_str(s)` (Python row-loops). The two are pinned
**byte-identical** by `test/shared/test_name_norm.py` — if you add a third normaliser you WILL
reintroduce the "distress join = 0" class of bug.

Rule: NFD accent-fold → drop combining marks → UPPERCASE → strip `. , & ' "` → **drop legal
suffixes/fillers** (`THE|AND|LIMITED|LTD|DAC|PLC|CLG|UC|COMPANY|DESIGNATED ACTIVITY COMPANY|
COMPANY LIMITED BY GUARANTEE|UNLIMITED COMPANY|GROUP|HOLDINGS|IRELAND|IRL|OF`) → drop non-alnum →
collapse whitespace.

So `ACME HOLDINGS LIMITED` → `ACME`, and `Tirlán` → `TIRLAN` (the accent-fold is why: without it
`Tirlán` → `TIRL N` and silently fails to meet its CRO twin).

Used by: CRO companies · procurement suppliers · lobbying registrants & clients · TED winners ·
ministerial-diary orgs · CBI registers. ~18 call sites.

### PERSON key (TD/member) — `shared/normalise_join_key.py`
`normalise_df_td_name(df, col)` → a `join_key` column. Rule: lowercase → NFD accent-fold → strip
apostrophes → strip non-alpha → strip honorifics (`dr|prof|rev|fr|sr|mr|mrs|ms|miss|br`) → remove
ALL whitespace → **sort the characters alphabetically**.

⚠️ **This is an ANAGRAM key.** It is order-insensitive *by design* (so `Richard Boyd Barrett` ==
`Barrett Richard Boyd`, which is the point — datasets disagree on name order). The cost: any two
names that are character-anagrams of each other **collide**. Treat a member match as a strong
lead, not proof, where the name is short or common; verify against the member register.

**Do not use the ORG key on people or the PERSON key on organisations.** They strip different
things and only one is an anagram key.

---

## 2. The entity spine — and its one big blind spot

`v_supplier_entity_xref` ← `data/gold/parquet/supplier_entity_xref.parquet`
Join key: **`supplier_norm`** (the ORG key above).

Columns: `supplier_norm, display_name, company_num, has_cro, procurement_award_rows,
awarded_value_safe_eur, on_lobbying_register, lobby_returns, has_corporate_notice,
corporate_notices, is_charity, has_epa_licence, cross_register_count`.

### 🚨 THE SPINE IS ANCHORED ON PROCUREMENT SUPPLIERS
It contains **10,017 organisations — and every one of them won public procurement.**
**An organisation that never won a public contract is ABSENT FROM THE SPINE ENTIRELY.**
You cannot ask "what else does the State know about org X" via the spine if X is a lobbying-only
body, a charity that holds no contracts, or a company that appears only in Iris notices.

### Measured yields (2026-07-14) — and what they do NOT mean

| Register | In spine | Register universe | Read this as |
|---|---|---|---|
| CRO company match | **6,469** (64.6%) | 819,429 companies | Most suppliers resolve to a CRO number |
| Lobbying register | **239** (2.4%) | ~2,557 lobbyist orgs + 1,992 clients | **~91% of lobbying orgs are NOT in the spine** |
| Corporate notices (Iris) | **246** (2.5%) | — | |
| Charity register | **72** (0.7%) | 14,448 charities | Only 0.5% of charities are also suppliers |
| EPA licence | **38** (0.4%) | — | |

These low numbers are **two effects mixed together**, and the data cannot separate them:
1. a genuinely small overlap (most lobbyists don't win contracts), and
2. **exact-name match failure** (subsidiaries, trading names, spelling variants).

**Therefore: a 0 or a low count is a FLOOR, never proof of absence.**
`on_lobbying_register = False` means **"not matched"**, NOT "did not lobby". This distinction is
already enforced in the MCP docstrings — keep it.

---

## 3. NEVER-JOIN / NEVER-SUM (the hard rules)

1. **Three money grains never sum or union across each other:**
   **BUDGET** (LA adopted budgets, AFS by-division) · **AWARD/ceiling** (eTenders, TED) ·
   **PAYMENT/supplier** (LA payments, public_payments). A 4th grain — LA *adopted budgets* — is
   BUDGETED, also never unioned. See `doc/DATA_MAP.md`.
2. **NEVER sum TED with national awards** — TED overlaps eTenders; summing double-counts.
3. **procurement × lobbying — NEVER sum `awarded_value` across the overlap.** One lobbying return
   attaches to its registrant **and** to each of its clients, so any org-level sum double-counts.
4. **Lobbying return counts are not additive across organisations** — same reason.
5. **votes × member interests** — only the **landlord/property** cross-reference is substantively
   real; the others are noise. Do not build a general "voted on their own interest" claim.
6. **Co-occurrence is NOT causation.** No key links a lobbying return to a contract award. An
   organisation appearing on two registers is a **research lead**, never evidence that one
   explains the other. Say so in any output.

---

## 4. How to actually run an association

1. Resolve the entity with the **ORG key** (or `organisation_dossier`, which does it for you and
   returns a `disambiguation` list rather than guessing).
2. Read the returned **`caveat`** field — 13 MCP tools return one in-band. It is not decoration.
3. Treat every cross-register hit as a **lead requiring verification**, and state the match tier.
4. If you need coverage beyond exact-match, that is a **tiered-matching problem**: keep EXACT as
   the only assertable tier and label fuzzy/embedding candidates explicitly as unverified leads.
   **Never silently fuse a fuzzy match into the data.**

## 5. Useful MCP entry points
`organisation_dossier` (one-call cross-register) · `cross_register_watchlist` (orgs on the most
registers) · `procurement_lobbying_overlap` (⚠ never sum awarded_value) · `company_influence`
(meeting-level access) · `data_coverage` (year ranges / scope guard) · `join_map` (this document,
in-band).
