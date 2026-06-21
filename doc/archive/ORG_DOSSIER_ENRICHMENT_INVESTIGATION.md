# Org dossier enrichment — investigation (2026-06-20)

**Context.** The Company dossier (`utility/pages_code/company.py`) was a finished
entity-first page but hidden from the nav. Step 1 (done) surfaced it as a visible
**"Companies"** tab under *The Money* (`utility/app.py`). This investigation assesses
step 2: turning the procurement-scoped *supplier* dossier into a genuine
*"everything we hold on this org"* hub.

## The join key

The dossier row already carries **`company_num`** (CRO number) plus `company_status`
and `cro_match_method`, sourced from `procurement_supplier_cro_match.parquet`
(cols: `supplier, supplier_norm, n_cro, company_num, company_status, match_method,
match_confidence`). `company_num` is a **hard identifier** — anything else keyed on the
same CRO number joins cleanly. That single fact decides feasibility per candidate.

## Candidate additions, ranked by feasibility × value

### Tier 1 — easy + high value: Corporate distress notices  ✅ RECOMMEND
- **Source:** `data/gold/parquet/cro_xref_corporate_notices.parquet` — already
  **CRO-keyed**: `notice_ref, entity_name, entity_norm, issue_date, notice_category,
  notice_subtype, company_num, company_status, company_reg_date, comp_dissolved_date,
  status_pill_value`.
- **Join:** direct on `company_num` → the dossier key. No matching work.
- **Plumbing that exists:** `dail_tracker_core/queries/corporate.py::corporate_notices`,
  `utility/data_access/corporate_data.py`, the `corporate_*` registered views, the
  Corporate Notices page, and the `corporate_distress_notices` MCP tool.
- **Build:** one per-company query (`WHERE company_num = ?`) + a render panel mirroring
  `_render_epa_credentials_panel` (conditional, silent absence). ~half a day.
- **Why it fits:** strong **accountability tension** — examinership / liquidation /
  strike-off on a firm taking public money. Same civic register as EPA, not a positive
  badge. This is the one to do first.

### Tier 1 — easy + medium value: Charity financials  ✅ RECOMMEND (conditional)
- **Source:** `data/gold/parquet/charities_enriched.parquet` (very rich: `cro_number`,
  `gov_funded_share_latest`, `gross_income_latest_eur`, `state_adjacent_flag`,
  `funding_profile`, deficit/insolvency flags, …).
- **Join:** **already built** — `v_procurement_charity_overlap` +
  `dail_tracker_core/queries/procurement.py::charity_overlap`, joined on `company_num`.
- **Build:** filter the existing view by `company_num`; render only when the firm is a
  charity (small subset of suppliers → mostly silent). ~quarter day.
- **Why it fits:** for state-funded charities winning public contracts, showing their own
  gov-funded share + financial health beside the awards is genuinely additive and honest
  (co-occurrence framing already written into the view header).

### Tier 2 — partial: CRO identity / "contact details"  ⚠️ PARTIAL
- **Have today on the dossier:** `company_num`, `company_status` (+ a status pill).
- **Cheaply addable:** `company_reg_date`, `comp_dissolved_date`, `status_pill_value`
  come free from `cro_xref_corporate_notices.parquet` (so an identity strip:
  *Registered <date> · Status <live/dissolved>* is trivial once Tier 1 is wired).
- **GAP — registered address / "contact details":** no gold table carries a company's
  registered **address**. `procurement_supplier_cro_match` has none; addresses only exist
  for *members (TDs)* (`v_member_contact_details`) and partially for charities
  (`eircode`, `county`). A true company contact card needs a new CRO-master ingest
  (registered office address) — **out of scope for a quick win; flag as a follow-up source.**

### Tier 3 — hard, defer: Members' interests in this firm  ❌ DEFER
- **Problem:** member interest declarations in `what_they_own` are **free-text company
  names, not CRO-keyed** (grep confirms no `company_num`/`cro` linkage on that page).
- **Cost:** would require a name→CRO fuzzy match of declarations (precision-sensitive —
  a wrong join here implies a false conflict-of-interest), plus the base rate is low
  (few suppliers have a TD-declared stake).
- **Verdict:** high effort, low yield, reputational downside. Defer until/unless a
  declaration→CRO crosswalk is built deliberately.

## Recommendation

Sequence for a real "org hub" without scope creep:
1. **Corporate distress panel** (Tier 1, CRO-keyed, accountability) — do first.
2. **Charity panel** (Tier 1, view already exists) — cheap add, conditional render.
3. **CRO identity strip** (reg date + status) — free rider on step 1's data.
4. **Defer**: registered-address contact card (needs new source) and member-interest
   linkage (needs name→CRO crosswalk).

NSAI stays parked — it's a positive credential with no accountability tension; the
Tier-1 additions above carry the civic value NSAI lacks.

All four use the existing `company_num` key and follow the EPA panel's pattern
(registered view → per-company query → conditional `_render_*` panel, silent absence,
no-inference copy). Nothing here needs new matching infrastructure except the two
deferred items.
