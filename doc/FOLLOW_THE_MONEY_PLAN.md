# Follow the Money — feature plan

Status: **PLANNED** (2026-06-20). Not yet built. Scope agreed: Phase 1 + Phase 2.
Placement agreed: **dedicated page + shared render module**, procurement feeds it.

## 0. Origin

Question that started this: *"can we see anything to the children's hospital or other
BAM contracts in the data, and can we dig into where the money is going (subcontractors /
suppliers)?"*

Finding: the **payments fact** captures **€130.7m committed by the National Paediatric
Hospital Development Board (NPHDB) to "BAM Building"** — and all 5 line items are
**dispute settlements** (Conciliator's Recommendations 22/25/29, two Adjudicator's
Decisions), sourced from the NPHDB quarterly PO listing PDF. The base construction
contract (awarded 2017) is NOT in eTenders/TED. BAM also appears across Education schools
PPP bundles (€352m, mixes one-off + repeating PPP charges), Waterford, Donegal, NTA, etc.

## 1. The data wall (must be honoured in the UI)

- **Public records are one tier deep:** public body → direct contractor. BAM → its own
  subcontractors is **not published anywhere** in Irish open data. There is no node below
  the prime contractor. Confirmed by probe: payments_fact has no sub-tier column; zero
  description rows mention "subcontract".
- The money graph that DOES exist is **bipartite**: `bodies ⇄ suppliers`, with the
  **ledger** (individual payment lines) on each `supplier × body` edge.
- **"BAM" is ~18 legal-entity strings** across a few CRO numbers (Building 199001, Civil
  17543, FM 459966, Glasgiven JV 680048, Bamford Bus 688028) + PPP SPVs with **no CRO**
  (Schools Bundle 3/4, Courts Bundle). Consolidating them is the only genuinely new
  modelling.

## 2. Navigation model (the crux)

Bipartite graphs loop forever (body → supplier → other bodies → …). Three rules kill the
"endless breadcrumb" risk:

1. **The ledger is a terminus.** From a `supplier × body` pair you see the line items and
   the trail stops. No pivot from a leaf.
2. **Bounded trail rail, not a growing breadcrumb.** One compact top strip, capped at 4
   nodes; deeper paths collapse the middle to `…` keeping origin + last two. Each prior
   node is click-to-jump-back (truncates the trail to that node).
3. **"Two columns and a ledger" fixed mental model.** At any moment you're on ONE node
   (a body OR a supplier OR a group), seeing counterparties ranked by €. One click pivots
   to a counterparty; one more opens the ledger. Useful depth is always 3. Pivoting to
   "follow this firm's other income" is an EXPLICIT labelled action — never accidental.

```
💶 Following the money
 NPHDB  ›  BAM Building  ›  Ledger (5 payments · €130.7m committed)
 └ click any step to jump back ┘                      [ Start over ]
```

### Back / forward without losing analysis
- **URL carries only the current node** (`?flow_body=`, `?flow_supplier=`, `?flow_group=`,
  `?flow_pair=`). Each click is a soft pushState via the existing `utility/ui/spa_links.py`
  interceptor → browser Back/Forward already walk the trail node-by-node, no reload.
- **Trail + per-node filters (year pills, SPENT/COMMITTED tier) live in `st.session_state`,
  keyed by node.** Re-visiting a node restores its filter state. This REPLACES today's
  `_return_to_browse()` (procurement.py ~L340) which clears everything to the section root.
- **One global "Start over"** returns to the landing — the only full reset.

## 3. Node / URL scheme

| Node | URL | Renders | Reuses (data_access) |
|---|---|---|---|
| Body | `?flow_body=<name>` | suppliers it pays, ranked by € | `fetch_payments_publisher_profile_result` |
| Supplier (entity) | `?flow_supplier=<norm>` | bodies paying it, ranked by € | `fetch_payments_publishers_for_supplier_result` |
| Group (BAM) | `?flow_group=<slug>` | member entities + combined body breakdown | **new view (Phase 2)** |
| Ledger (terminus) | `?flow_pair=<sup>~<body>` | line items + source PDF link | `fetch_payment_lines_for_pair_result` |

The four existing payment renderers become the four node renderers. New work = the trail
rail wrapper + the group node, NOT new data fetching (except group).

## 4. Corporate-group rollup (Phase 2)

- Curated `data/_meta/supplier_groups.csv`:
  `supplier_norm | cro_company_num | group_slug | group_label`.
  Matches the project's hand-curated `_meta` pattern (stateboards, rates). Honest approach:
  auto-grouping on the string "bam" wrongly sweeps in **Bamford Bus, Bammedia, D&S Bamford**
  and PPP SPVs have no CRO to join on.
- New `sql_views/procurement/` view rolls entities → group, exposes the group node + a
  "spans N legal entities incl. 3 PPP SPVs" disclosure line. All aggregation in the view
  (logic firewall); page stays thin.
- Remember the gitignore `*.csv` trap — the new `_meta` CSV needs a gitignore negation +
  `git add` (see project_curated_meta_reference_files).

## 5. Placement / architecture

- **New dedicated page** ("Follow the money") — the feature is big enough to warrant it,
  and the real infra violation would be bloating the already-3512-line procurement.py.
- **Shared module `utility/pages_code/_money_flow.py`** holds the trail rail + 4 node
  renderers ONCE. Both the new page and procurement's "Who actually gets paid?" tab call it.
- **Procurement stays the rankings/overview**; its paid-supplier / paid-body cards become
  cross-page links that hand off into the trail (existing `*_profile_url` pattern). One
  canonical home for "following," fed from the overview.
- Respects the Streamlit-uncoupling goal: shared module thin-over-core, aggregation in views.

## 6. Honest framing baked in
- NPHDB→BAM is `po_committed` and overwhelmingly **dispute settlements**, not base
  construction — ledger labels it so.
- Education BAM rows mix one-off spend with **repeating PPP availability charges**
  (€7.37m ×7 yrs) — ledger flags PPP rows so totals don't mislead.
- Persistent footer: *"Public records stop at the direct contractor. Payments below this
  firm to its own subcontractors are not published."*

## 7. Build order
1. Phase 1a — extract the 4 payment renderers from procurement.py into `_money_flow.py`
   unchanged (pure refactor, keep procurement tab green).
2. Phase 1b — add the trail rail + session-state trail + per-node filter memory; wire the
   `?flow_*` routing on the new page.
3. Phase 1c — convert procurement paid-tab cards to hand off into the trail page.
4. Phase 2a — curate `supplier_groups.csv` (BAM first), add gitignore negation.
5. Phase 2b — group rollup view + `?flow_group=` node + disclosure line.
6. Tests: routing/back-forward, trail truncation cap, group rollup sums (sum-safe only),
   PPP-flag correctness.
```
