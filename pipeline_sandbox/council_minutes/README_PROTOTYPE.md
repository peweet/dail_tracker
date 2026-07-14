# Your Councillors — sandbox prototype (BUILT 2026-06-22)

Runnable prototype of the feature designed in `doc/archive/YOUR_COUNCILLORS_UI_BRIEF.md`.
**Sandbox only — NOT wired into the prod app, nav, or gold.** Reads sandbox data directly.

## Run it
```
streamlit run pipeline_sandbox/council_minutes/your_councillors_prototype.py
```
Then: pick a **County → Local Electoral Area** → see the roster → click a councillor for the dossier.
**Try Carlow** (the one council with a validated named-voting record).

## The 3 screens
1. **Picker** — County → LEA dropdowns (no Eircode; it's licensed).
2. **LEA roster** — councillor cards + party-composition stripe + a per-council coverage badge.
3. **Councillor dossier** — Voting record (tier-gated), Pay & allowances (DHLGH schedule),
   reserved-vs-executive explainer, provenance.

## The honest degradation (the point of the design)
The Voting-record card renders by the council's data tier (`council_coverage.csv`):
- **roll_call** (Carlow): real named votes — For/Against/Abstain + motion + date.
- **proposer_seconder** (14 councils): "decides by agreement, individual votes not published".
- **scanned_pending** (Galway ×2, Louth): "minutes scanned, OCR pending".
- **cmis_pending** (Dublin City, DLR): "ModernGov portal, not yet processed".
- **unseeded** (11 councils): "minutes not yet harvested".
No tier ever implies zero activity falsely.

## Data it reads (all sandbox)
- `councillors_roster.csv` — 763 councillors / 31 councils (Wikipedia). **~80% complete** — some
  councils undercounted where the article table format varies (Galway City 5/18, Cork City 10/31,
  Sligo 6/18, Limerick 9/40…). Parser refinement is the first follow-up.
- `council_coverage.csv` — per-council tier (drives the voting card).
- `member_votes.jsonl` — 185 Carlow per-member votes (the only validated named-vote council).

## What's real vs stubbed
- **Real**: roster (31 councils), Carlow voting record, coverage tiers, pay schedule, all flow/UX,
  honest empty states.
- **Static**: pay schedule is the DHLGH national figures (not per-person actuals).
- **Pending** (per the brief / QUALITY_ASSESSMENT_ULTRA.md): roster parser refinement to ~100%;
  off-box OCR (Galway/Louth); ModernGov scraper (Dublin City/DLR); corrected seeds (11 councils);
  vote extraction for other roll-call councils (Kilkenny/Laois/South Dublin/Fingal).

## Promotion path (when you sign off — needs gold, not done)
The production build needs the pipeline views in `doc/archive/YOUR_COUNCILLORS_UI_BRIEF.md` §10
(`v_la_councillors`, `v_la_councillor_votes`, coverage tier-flag, …), a real page in
`utility/pages_code/`, and an `app.py` nav entry. None touched — this prototype is for assessment.
"""
