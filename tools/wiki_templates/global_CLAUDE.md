# Personal rules (global tier)

Personal, cross-project rules live as small pages in `~/.claude/rules/`, one topic per file,
each under 120 lines. They load in every project on this machine, before project rules, and
neither tier overrides the other, so a project rule that conflicts must be reconciled, not
stacked. Project knowledge stays in the project's own wiki (`.claude/rules/`, `memory/`,
`doc/`), never here.

To add a page: `python tools/wiki.py page global <slug> --title "..."` from the dail_tracker
checkout, or write the file by hand with a one-line `description:` front matter. To check
both tiers: `python tools/wiki.py status`.
