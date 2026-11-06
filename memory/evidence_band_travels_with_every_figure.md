# Evidence band travels with every figure, in every paragraph

Date: 2026-11-06. Scope: public.

## Lesson

A figure grounded once does not stay grounded. The Rule 2 linter in the Stop hook reads one
paragraph at a time, and so does a reader who copies one paragraph into UI copy or an audit.
Every run of digits either carries a band in its own paragraph or is not written.

The shapes that keep recurring, all of them restatements of a number banded earlier:

- a closing recap, "still owed" or "next step" line;
- a bold lead-in that carries a count, above bare bullets;
- bullets under a banded lead-in (each bullet is its own paragraph to the linter);
- an offer or question sentence ("want me to re-ingest the N...?");
- a figure derived from my own parameters (`--pages 6 --page-size 100` described as "~600");
- a bare URL in brackets used in place of the band word.

The mechanism is familiarity. A number repeated across a long thread stops feeling like a data
claim and starts feeling like background, and background does not feel like it needs a citation.
Position, register and grammatical role were symptoms of that one cause.

What works:

1. On a second mention, write words, not digits: "those layers", "the same count", "the rest".
   Pronouning the number leaves nothing to ground and nothing to forget.
2. When the digits are needed again, paste the band inline in that paragraph, even when the same
   figure was banded one screen up or in the previous turn. A turn boundary and a surface
   boundary (artifact to chat, file to chat) both reset grounding.
3. Before sending, make a separate pass that scans the draft for digit characters, not for
   "claims". Read the last paragraph and any owed-items list first; that is where the misses sit.

## Evidence

- Session 671d9180 (2026-11-06): the hook fired twice. First on "100%", scp progress text inside a
  handover command that the linter read as a data claim; second on "23 Celtic shadow layers", a
  count grounded two turns earlier against `rules/source_seed_registry.csv` and restated bare in
  a status recap. Both are the second-mention shape.
- Earlier recurrences, from the workstation-local card `feedback_band_must_travel_into_closing_recaps`
  [Reported — local card, not tracked]: 4 hits on 2026-08-30, 5 on 2026-09-05, 15 on 2026-09-28
  and again on 09-29, 2 on 2026-10-23 and 2 on 10-25. The diagnosis changed three times before
  settling on familiarity; the digit-scan pass is the only countermeasure that held.
- The rule: `.claude/rules/evidence.md` and `.claude/rules/communication.md` Rule 2. The linter is
  `tools/hooks/style_lint.py`.
