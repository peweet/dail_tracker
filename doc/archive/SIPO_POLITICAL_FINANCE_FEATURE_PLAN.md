# Political Finance feature — planning notes (pre-brief)

> **Status: planning only.** No formal design brief yet (user: *wait for data +
> gold promotion* before `/shape`). No code, no backend changes. This captures the
> concept, the tie-in with Payments/Attendance, and the sequencing so the brief is
> fast to write once the data lands.

## What the data is (extracted, sandbox)
GE2024 SIPO political finance, in `pipeline_sandbox/_sipo_output/`:
- **Election expenses — candidate summary** (`sipo_expenses_fact.parquet`): per
  candidate → constituency, amount assigned + national-agent spend. 8 parties +
  National Party. (Part-3.)
- **Election expenses — itemised** (`sipo_expense_items_fact.parquet` +
  `..._categories.parquet`): the 8 headings (4A Advertising incl. **Meta ads**, 4B
  Publicity, 4C Posters … 4H Campaign Workers) + line items. FF+SF done; 6 pending.
- **Donations** (`sipo_donations_fact.parquet`): 74 donations, €161,578, 7 parties;
  donor → value → party → date. ([[project_sipo_donations_extracted]])

## The tie-in (the user's instinct, confirmed by the architecture)
`utility/pages_code/member_overview.py` is a **per-TD hub** that renders a compact
panel from each domain via `render_member_<domain>(conn, join_key)`
(`ui/payments_panel.py`, `ui/attendance_panel.py`, votes, interests, lobbying,
committees). Payments and Attendance *also* exist as standalone, year-segmented
pages (cards + rankings + provenance footer) — and they're **real-estate-heavy for
simple, one-number-per-TD data.**

So political finance slots into the SAME two-surface pattern:
1. **Standalone "Election Money" page** — the PARTY-level story that only makes
   sense in aggregate (party total spend, the category/**Meta** breakdown,
   donations received).
2. **`render_member_election_finance` panel** on member_overview — the PER-TD
   slice (their 2024 campaign spend + category breakdown; any donation they
   personally made to their party).

### The real-estate lever (don't build a 4th heavy page)
Payments + Attendance show the cost of a heavy standalone page for simple data.
**Lean into the hub:** keep the standalone Election-Money page tight (party /
category / Meta / donations-received — the genuinely aggregate story), and push the
per-TD facts into the member_overview panel where they **consolidate into one
coherent money picture per TD**:
- **income** — payments as a TD (existing payments panel)
- **campaign cost** — election spend (new)
- **funding** — donations their party received / they personally gave (new)
A future tidy: these three could share one "Money" section on member_overview
rather than three separate panels — addresses the real-estate sprawl directly.

## Surface → data → view map (logic firewall: UI reads `v_` views only)
| Surface | Data | SQL view (to build) |
|---|---|---|
| Election-Money page: party totals + category/Meta | expense_categories + items | `v_sipo_expense_categories`, `v_sipo_expense_items` |
| Election-Money page: candidate spend ranking | expenses_fact | `v_sipo_expenses_candidates` |
| Election-Money page: donations received | donations_fact | `v_sipo_donations` |
| member panel: this TD's campaign spend | expenses_fact (candidate→member join) | `v_sipo_expenses_candidates` |
| member panel: donation this TD gave | donations_fact (donor→member join) | `v_sipo_donations` |

## Guardrails (non-negotiable — political finance)
- **No-inference** ([[feedback_no_inference_in_app]]): figures + source link only. A
  spend or donation is never framed as influence. The over-cap / under-threshold
  rows surface as *"verify vs the official SIPO PDF p.N"*, never "exceeded the
  limit". No judgmental ordering.
- **Privacy** ([[feedback_personal_insolvency_privacy]]): donor name + value are the
  public record; **home address (`donor_address_raw`) is NEVER displayed.**
- **Provenance**: OCR-derived → confidence/flag column + verify-vs-PDF caption;
  flagged rows handled per the gold-quarantine pattern ([[feedback_gold_layer_quarantine]]).

## Prerequisites before a brief/build (sequence)
1. **Finish Part-4 items** for the remaining 6 parties (other context).
2. **SIPO consolidation** 22→7 files ([[project_sipo_consolidation_plan]]) — cleaner
   before promotion.
3. **Promote** sandbox facts → gold (+ provenance columns) and build the `v_sipo_*`
   views (pipeline-owned).
4. **Normalize**: party-name spellings differ between donations and expenses facts;
   join candidate_name + donor_name → member registry (`unique_member_code` via
   `normalise_df_td_name`) for the member-panel surfaces. Only *winning* candidates
   become TDs — losers have spend but no member page (party-page only).
5. **Then** `/shape` the formal brief → build page + panel (contract → build).

## Decisions locked
- **Member-overview surface = a SEPARATE `Election finance · GE2024` panel**
  (`render_member_election_finance`), rendered below the existing payments panel —
  NOT folded into a unified "Money" section. Rationale (user, 2026-06-04): campaign
  spend + donations-given are GE-specific one-offs, not ongoing pay; keep them
  distinct and leave the payments panel untouched. Panel content: campaign spend
  (of assigned) + category chips (Advertising/Posters/Publicity…) + any donation the
  TD personally gave to their party + source/verify caption.
- **Standalone "Election Money" page** stays the party-level home (two lenses: spend
  with the category/Meta breakdown · donations received), mirroring the Payments
  page idiom (lens/year pills, cards, provenance footer).

## Open design questions for the brief (later)
- Year model: 2024-only now, but build year-segmented like Payments/Attendance for
  future elections (GE2020/2016 are available but out of current 2024 scope).
- Candidate spend without a member page (losing candidates) — party-page list only.
