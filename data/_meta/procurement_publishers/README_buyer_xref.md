# buyer_xref.csv — curated buyer crosswalk

One row per public body, reconciling the SAME buyer across five registers that key it
differently: eTenders awards (`etenders_name`), TED (`ted_buyer_name`), public-body
payments (`payments_publisher_name` — e.g. "Limerick" for "Limerick City and County
Council"), council AFS (`afs_council`), and the live-tender feed (`live_buyer`).

- **Resolver:** `dail_tracker_core/buyer_xref.py::resolve_buyer()` — exact lookup on a
  conservative normalised key, **fail-closed** (unknown → `None`; callers render an honest
  gap note, never a fuzzy guess). Tests: `test/dail_tracker_core/test_core_buyer_xref.py`
  (includes a cross-row key-collision guard — Cork City vs Cork County, Meath vs Westmeath).
- **Provenance:** anchored on the payment publishers + OGP/EPS (build 2026-06-29,
  `c:/tmp/buyer_xref_draft/build_buyer_xref.py`); the 30 `needs_review` rows were repaired
  against the live registers on 2026-07-13 (`repair_buyer_xref.py` — council rows had
  latched onto defunct pre-2014 entities, e.g. "Limerick City Council" n=3 vs the merged
  council n=874) and three rows hand-adjudicated with evidence in `notes` (BIM name
  variants, the DPER department renames, ESB Networks DAC).
- **match_tier:** `curated_exact` = cross-register fusion allowed; anything else (e.g.
  `single_register`) = fusion suppressed. `needs_review=yes` rows: none at promotion.
- **Editing:** hand-edit this CSV (it is the curated source of truth); keep `buyer_id`
  stable; run the tests after any change.
