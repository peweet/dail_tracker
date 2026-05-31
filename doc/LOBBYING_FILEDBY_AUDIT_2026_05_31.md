# Lobbying page — focused audit on "FILED BY" addition (2026-05-31)

Audit run via Playwright against the running Streamlit instance on
`localhost:8520` after adding a `person_primarily_responsible` byline
("FILED BY <name>") to the return-card pattern. Five card surfaces
now carry it: org profile, area profile, area×politician Stage 3,
org×politician Stage 3, and topic Stage 2. DPO surfaces deliberately
omit it (silver doesn't carry the field; the DPO is the filer).

Scope: the addition itself and its second-order effects (hierarchy,
contrast, density, wrap). The broader page audit from
[LOBBYING_AUDIT.md](LOBBYING_AUDIT.md) (15/20, 2026-05-26) is not
re-graded here; open P0/P1 items from that pass remain open.

Screenshots: `audit_screenshots/_lobbying_post_filedby_*.png`
(8 full-page + 4 single-card zooms across desktop 1400px and
mobile 390px).

---

## Health score

| # | Dimension       | Score | Headline finding                                                                                                  |
|---|-----------------|-------|--------------------------------------------------------------------------------------------------------------------|
| 1 | Accessibility   | 3/4   | New `.lp3-return-filed-by` inherits the page-wide `--text-meta` colour at 4.29:1 on white (just under WCAG AA 4.5:1) |
| 2 | Performance     | 4/4   | +1 column SELECT per fetch, +1 short string concat per card; negligible (DuckDB column-store, 25-card pagination)   |
| 3 | Theming         | 4/4   | New class uses tokens only (`var(--text-meta)`); zero new hard-coded values                                          |
| 4 | Responsive      | 4/4   | No horizontal overflow at 360px; long bylines wrap inside the card; cards still fit a one-thumb scan               |
| 5 | Anti-patterns   | 4/4   | Editorial uppercase-label byline pattern is in-spirit with the civic register; no new AI tells, gradients, glass    |
| **Total** |       | **19/20** | **Excellent — one inherited a11y polish item; otherwise a clean, on-register addition.**                       |

### Anti-pattern verdict

**Pass.** The new line reads as a newspaper byline — small uppercase
label + body text in the meta colour — which is exactly the register
PRODUCT.md asks for ("editorial accountability journalism"). It does
not invent a new affordance, does not break the card vocabulary,
and does not pull visual weight from the bolded politician/org title
sitting directly above it.

---

## Executive summary

The addition lands well. The card grew by one short line; the line
sits below the bold title without competing with it; the data is
real and dense (100% coverage on the silver parquet — 85,826/85,826
rows). On the org/Ibec page the byline often surfaces a useful "Conor
Mulvihill, Ibec Dairy Industry Ireland" — name + the contextual
role inside the org, which is the kind of fact a journalist scanning
returns will actually use.

- **0 P0 blocking** — nothing prevents task completion.
- **1 P1 major** — inherited contrast gap on `--text-meta` (4.29:1 on
  white). Pre-existing across `.lp3-return-sub` / `.lp3-return-snippet`;
  the new `.lp3-return-filed-by` inherits it. WCAG AA boundary case.
- **2 P2 minor** — long bylines (p99 = 66 chars on Ibec, max 334 on
  Immigration topic) can wrap to 2–3 lines on mobile and disrupt
  card-scan rhythm; the "Filed by" label-span has no
  `aria-label="Filed by"` (it's plain text, so SR reads "Filed by
  Conor Mulvihill" correctly — flagged for completeness only).
- **1 P3 polish** — the byline reads with adequate density on desktop
  but could earn a thinner divider rule (1px subtle border-top) on
  desktop to mark the meta band visually distinct from the snippet
  paragraph below it. Optional.

---

## Detailed findings

### [P1] Inherited contrast gap on `.lp3-return-filed-by`

**Location:** [utility/shared_css.py](../utility/shared_css.py) lines 4646–4661 (new class), inherits same colour token used by `.lp3-return-sub` (line 4634) and `.lp3-return-snippet` (line 4640).

**Category:** Accessibility

**Impact:** The "FILED BY" label (0.7rem, weight 600) and the byline value (0.78rem, weight 400) both render in `oklch(58% 0.010 75)` = `rgb(125, 121, 115)` on a `#ffffff` card. Computed contrast ratio: **4.29 : 1**. WCAG AA requires 4.5 : 1 for normal text. The label-span at 11.2px is unambiguously normal-sized text, so the AA-failure is real, not a large-text exception.

This is a **page-wide pattern** — every "lobbied by …" subtitle and every snippet paragraph on the page renders in the same colour, so we're not introducing the gap, only paying for it on a new line. But it's worth surfacing now because adding a third meta line per card increases the total `--text-meta` surface area on the page roughly +25% for users on the org / area / topic profiles.

**WCAG:** 1.4.3 (Contrast — Minimum). Boundary failure — 4.29 : 1 vs the 4.5 : 1 requirement.

**Recommendation:** Darken `--text-meta` from `oklch(58% 0.010 75)` to `oklch(52% 0.012 75)` (≈ `rgb(108, 104, 99)`, contrast 5.42 : 1) globally. This passes AA across all four current consumers (sub, snippet, filed-by, sidebar-label) and keeps the warm-grey character of the editorial palette. Verify the change doesn't cool too far against `--bg` warm-beige on the few callsites that render meta text on the page background instead of on white cards.

**Suggested command:** `/impeccable polish` (token-level tweak, page-wide effect).

---

### [P2] Long bylines wrap to 2–3 lines on mobile and break card-scan rhythm

**Location:** [utility/pages_code/lobbying_3.py](../utility/pages_code/lobbying_3.py) line 599 (`_return_card_html` `filed_by` kwarg, no truncation).

**Category:** Anti-Pattern / Responsive

**Impact:** Distribution of `person_primarily_responsible` length on the live data: median 42 chars, p90 56, p99 66, max **334**. On a 360px-wide mobile viewport the byline column has ~290px of usable width — enough for ~55 chars per line. Entries at p99 (66) wrap to 2 lines; the 334-char outlier (a real entry on Immigration topic: "Professor Doctor Joseph Chikelue Obi GKB (State Counsellor of Biafraland…") would wrap to 5–6 lines and visually dominate the card. The remaining card metadata (snippet, link) gets pushed down inconsistently, breaking the user's predicted scroll pattern when paging through 1,174 returns.

**Recommendation:** Truncate `filed_by` to ~80 chars + ellipsis in `_return_card_html`, mirroring the existing snippet truncation (`(text[:260] + "…") if len(text) > 260 else text`). Users who want the full string can open the lobbying.ie return via the existing "View on lobbying.ie ↗" link. Alternatively, render long bylines with `text-overflow: ellipsis; overflow: hidden; white-space: nowrap` — but truncate-at-source is more honest because copy/paste won't grab a misleading partial.

**Suggested command:** `/impeccable harden` (edge-case truncation).

---

### [P2] "Filed by" label is a `<span>` with no semantic role

**Location:** [utility/pages_code/lobbying_3.py](../utility/pages_code/lobbying_3.py) `_return_card_html` — `<p class="lp3-return-filed-by"><span>Filed by</span> {name}</p>`.

**Category:** Accessibility

**Impact:** A screen reader reads "Filed by Conor Mulvihill" as one continuous phrase — which is the desired user experience. So in practice this is fine. The flag is structural: if a future visual treatment hides the label-span behind a `::before` pseudo-element (a common refactor), the meaning is lost from the accessibility tree. Marking it now with a `aria-hidden="false"` and giving the parent `<p>` a more explicit role would future-proof.

**Recommendation:** Either leave as-is (current behaviour is correct), or change the markup to `<p class="lp3-return-filed-by"><strong>Filed by</strong> {name}</p>` — `<strong>` carries semantic weight, the label-span styling can hook on `.lp3-return-filed-by strong` and you keep current rendering. Defer until the next round of card-vocabulary work.

**Suggested command:** `/impeccable polish` (markup-level micro-fix, bundle with other a11y improvements).

---

### [P3] No visual divider between meta-band (title + filed-by) and snippet

**Location:** All five card-rendering callsites in [utility/pages_code/lobbying_3.py](../utility/pages_code/lobbying_3.py).

**Category:** Anti-Pattern / Layout (very mild)

**Impact:** On desktop at 1400px the card now reads as five vertical bands: period chip / title / FILED BY / snippet / action link. Without a divider, the FILED BY line and the snippet paragraph have very similar typographic colour (both `--text-meta`) and sit close together — the eye doesn't have a strong cue separating "WHO" from "WHAT". On the area page (where there's no snippet, only "lobbied by X" + FILED BY), this is invisible. On the org and topic pages, where snippets are routinely present, it's faintly worth doing.

**Recommendation:** Optional — add `border-top: 1px solid var(--border); padding-top: 0.45rem; margin-top: 0.2rem;` to `.lp3-return-snippet` only on cards that also have a `.lp3-return-filed-by` (use `.lp3-return-filed-by + .lp3-return-snippet` adjacency selector). Don't over-engineer; consider skipping unless you're already touching this CSS.

**Suggested command:** `/impeccable polish` if combined with the P1 token tweak; otherwise skip.

---

## Patterns & systemic observations

- **Meta-text colour is now a high-traffic token.** Five card surfaces × 25 cards/page × three usages per card (subtitle, filed-by, snippet) = up to 375 instances of `--text-meta` on a single deep-link page. Any improvement to the token's contrast pays dividends everywhere. This justifies treating P1 as the highest leverage move.

- **The card vocabulary has grown to five lines without losing legibility.** Before: header / title / subtitle? / snippet? / link. After: header / title / subtitle? / **filed-by?** / snippet? / link. The card pattern handled the addition because it was inserted in the meta tier, not the headline tier — a signal that future "small fact" additions (a "DPO present?" dot, a "grassroots?" pill) can also live in this tier without restructuring.

- **The five-callsite wiring is a sign of a missing helper.** `for _, row in slice.iterrows(): cards.append(_return_card_html(period=…, title=…, filed_by=row.get("person_primarily_responsible", "") or "", …))` is now repeated four times with near-identical row-extraction logic. A `_return_card_from_row(row, *, title_field, subtitle_field=None)` helper would make the next field addition a one-line change. Not urgent.

---

## Positive findings

- **Editorial register held.** The uppercase "FILED BY" + body name reads exactly like a newspaper byline. PRODUCT.md asks for "editorial accountability journalism" — this is the most literal interpretation of that brief on the page so far.
- **Data plumbing is purely additive.** One column added to one SQL view + five fetch SELECTs + one kwarg default-empty. No callsite broke. No tests rewritten. No pipeline rebuild.
- **Coverage is total.** 85,826/85,826 silver rows carry `person_primarily_responsible`. The "—" / "" empty-state fall-through wasn't needed on any real card during testing.
- **The Ibec page tells a story now.** "Filed by Danny McCoy, Ibec CEO" surfaces a fact you couldn't see before — that the lobbyist firm's CEO personally signed certain returns. That's a journalism-grade detail unlocked by a 6-character SQL change.
- **Mobile renders cleanly.** 360px viewport: no horizontal overflow, no card overflow, long bylines wrap inside the card container. The mobile zoom shows the byline still reads as the third visual tier (period → name → byline → snippet) without crowding.
- **Topic page parity.** The topic-page rendering, captured at `/rankings-lobbying?lp3_topic=Immigration%20%26%20asylum`, picked up the field via a separate view path (`v_lobbying_topic_search`) and rendered consistently — no visual drift between the contact-detail path and the topic-search path.

---

## Recommended actions

1. **[P1] `/impeccable polish`** — darken `--text-meta` from `oklch(58% 0.010 75)` to `oklch(52% 0.012 75)` to clear WCAG AA 4.5:1 on `.lp3-return-sub` / `.lp3-return-snippet` / `.lp3-return-filed-by` / sidebar labels. Verify against the warm-beige `--bg` on the few off-card callsites.
2. **[P2] `/impeccable harden`** — truncate `filed_by` at ~80 chars + ellipsis in `_return_card_html` to absorb the 334-char outliers without breaking card-scan rhythm on mobile.
3. **[P2] `/impeccable polish`** — convert the `<span>Filed by</span>` to `<strong>Filed by</strong>` for semantic weight (cheap, bundle with #1).
4. **[P3] `/impeccable polish`** — optional `.lp3-return-filed-by + .lp3-return-snippet` adjacency divider on desktop. Defer if not already touching this CSS.

> You can ask me to run these one at a time, all at once, or in any order you prefer.
>
> Re-run `/impeccable audit` after fixes to see your score improve.
