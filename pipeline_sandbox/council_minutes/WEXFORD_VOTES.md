# Wexford named votes — adapter record (2026-08-01)

Wexford became the 7th roll-call council on 2026-08-01. It is written up separately because
the previous adapter sweep had recorded a **wrong** verdict for it, and the reason matters more
than the row count.

## Why the sweep missed it

The sweep tried the three PROSE parsers (Cork, Galway, Fingal) against Wexford's corpus and
got zero divisions, which was logged as "no free wins — minutes mention roll-calls without
printing name lists". That conclusion was wrong. Wexford **does** print named lists, in a
TABLE, which fitz flattens to one token per line:

```
The outcome was that the proposal to abolish slipway charges
was defeated with 6 in favour and 22 against. The voting was as follows:
BARDEN PAT
F
DONOHOE
ANTHONY
A
```

A prose parser cannot see that shape, so "the prose parsers found nothing" was never evidence
about whether named votes exist — only about whether they are prose. The check that settled it
was reading the documents, not re-running the sweep.

## What the format gives us

- The outcome sentence carries a **printed tally** ("6 in favour and 22 against"), so the same
  reconcile gate every other adapter uses applies unchanged: a division is emitted only when
  each side's parsed names count exactly to its printed number.
- Names appear in two layouts — `SURNAME FORENAME` on one line, or surname and forename on
  consecutive lines — both terminated by a bare code line.
- Codes are `F` (for) and `A` (against) consistently.

## ⚠ The code vocabulary is per-document

In-document legends gloss `AS` as **Absent** in 2 documents and **Abstain** in 2 others. A
hardcoded meaning would silently mislabel one of the two groups, so the parser reads the legend
from each document and, where a document does not gloss `AS`, records those members **nowhere**
rather than guessing. Neither reading can affect the gate, which counts only for/against.

## Completeness

- Corpus denominator: **98 Wexford documents** (all clean, all born-digital `text`).
- Documents carrying the name+code grid: **18** (372 name-code triples).
- Divisions found by the tally pattern: **13**. Kept: **4**. Dropped by the reconcile gate: **9**.
- Rows emitted: **119** (67 for, 52 against), across 4 meetings, **119/119 dated**,
  spanning 2022-11-28 → 2023-12-11.
- The 9 drops are not silent: they are counted in the run line, and are the honest cost of the
  no-guess rule. They are the next place to look for yield — the likely cause is that the grid
  for those divisions carries members the entry pattern does not match (a third name layout, or
  a code the document does not gloss), NOT that the tally is wrong. **Unverified** — nobody has
  yet read those 9 blocks one by one.

## Recall

- Upper bound on what this adapter can ever reach: the 18 grid documents, i.e. **18.4% of the
  Wexford corpus**. The other 80 documents record decisions by proposer/seconder without a named
  division, which is the normal Irish local-government pattern and not a parsing failure.
- Against the grid documents themselves, recall is **4/13 divisions (31%)** — the gap is the 9
  gate drops above, not undetected divisions: the tally regex found every division the grid
  documents announce.
- Kerry was inspected the same day and earns **no adapter**: of 29 documents, its vote-language
  markers are attendance roll-calls ("took a roll call") and budget "Division A/B" headings —
  both false positives — with exactly **one** printed tally and no accompanying named list.

## Provenance

Every row ships `source_status='text'` (born-digital) and `join_status` from the gold roster.
50 of the 119 rows are `printed_form`: Wexford's divisions are 2022-2023, i.e. the **previous**
council, and the roster holds sitting members — the same structural limit as Galway City. A
roster refresh cannot fix those; a term-scoped historic roster would.

Method: [[reference_extraction_hybrid_recipe_2026_08_01]]. Code: `extractors/council_votes_extract.py`
(`parse_wexford_grid`), tests in `test/extractors/test_council_votes_extract.py`.
