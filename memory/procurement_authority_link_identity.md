# Procurement buyer links must preserve identity through the detail query

Confirmed 2026-10-04 by public CI run 37224289794 and real query inspection.
The supplier-dependency view groups raw contracting authorities. The authority
summary instead attributes awards to a named client, falling back to the raw
authority after guarding blank and literal `NULL` client values.

Tender 5928223 illustrates the mismatch: raw authority
`Presentation Secondary School (Warrenmount)`, client buyer
`Presentation Primary School Warrenmount`. An unstable tied top-authority row
exposed an unreachable link. Adding a tie-break alone would hide the identity
defect. The source also contains one-to-many raw-to-client mappings, so choosing
an arbitrary client would invent a destination.

Keep the dependency grain, raw labels and counts intact. Expose a separate
nullable canonical profile key only for an unambiguous mapping across the full
source population. Ambiguous mappings render plain escaped text. Stabilise
ties separately. Summary and itemised-award queries must use the same canonical
buyer expression and valid source population; a summary-only reachability test
can pass while the destination silently shows no originating awards.

Evidence and reusable checks:

- `sql_views/procurement/procurement_supplier_dependency.sql`: raw grouping and
  guarded, unambiguous profile mapping.
- `sql_views/procurement/procurement_awards.sql`: canonical buyer projection with
  raw contracting/client columns retained.
- `dail_tracker_core/queries/procurement/awards.py::awards_for_authority`: detail
  identity matches the authority summary.
- `test/sql_views/test_supplier_dependency_authority_links.py`: real SQL and
  query fixtures cover renamed clients, dirty-value fallback, ambiguity, ties,
  summary/detail counts and year filtering.
- `test/utility/test_link_reachability.py`: clickable keys resolve; null keys
  render without a fabricated href.
