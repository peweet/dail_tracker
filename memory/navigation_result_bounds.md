# Navigation limits must bound returned work

Trigger: MCP directory outlines, navigation result limits, oversized context, or repeated broad searches.

The 2026-10-04 efficiency review found that directory `code_outline(limit=N)` ignored N:
it parsed and returned up to 80 modules. File mode already honoured a definition limit.
`search_project` also returned top-k metadata hits without saying that more matches existed.

Directory mode now caps parsed modules at `max(1, min(limit, 80))`, reports total/returned
modules and preserves its explicit omission message. Default directory output keeps the
80-module ceiling. Search adds total/returned/truncated metadata fields without conflating
lexical metadata matches with FTS spans. Paths, parse errors and public tracked-source policy
remain visible.

Red evidence: `test_outline_directory_limit_is_module_bound_and_skips_omitted_parses`
returned and parsed all five fixture modules for limit 3. The default-cap and lexical-search
count regressions also failed because the new coverage fields were absent.

Green evidence: all three focused nodes passed, then
`uv run --locked --group dev --extra pipeline --extra api --extra mcp pytest -q test/mcp_server/test_code_index.py`
passed 27 tests. The catalog check retained 74 read-only tools and six always-loaded
navigation tools. Independent verification and the final gate are recorded at task closeout.

Lesson: check result limits at every alternate execution branch and prove that omitted
items are not processed. Preserve an explicit completeness marker so an agent can choose
whether to refine its query or retrieve more evidence.

Limits: FTS independence uses a fake at the search transport seam; ranking runs against
a small metadata fixture. Payload/parse-count improvements do not prove lower model token
use, fewer model rounds, client-side deferral or billing savings. Those require a live trial.

Related skill boundary: admitting a portable `.agents/skills` file also admits it
to the benchmark's shared cleanroom. Codex's OFF condition previously disabled
AGENTS instructions, MCP and execpolicy rules without disabling repository skills.
It now emits per-folder `skills.config` overrides with `enabled=false` for immediate
checkout skill folders; ON discovery is preserved. The paired parameterized
`test_codex_project_skill_loading_follows_project_settings` failed for OFF before
the repair and passed in both modes afterwards; all 29 adapter tests passed.
This controls automatic project discovery only. Manual reads and host-scoped
skills remain outside the shared-checkout boundary; no paid/live activation was tested.
