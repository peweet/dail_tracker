"""
Private-IP / secret leak guard.

Usage:
    python tools/check_no_private_ip.py            # scan all tracked files
    python tools/check_no_private_ip.py --staged   # scan only staged files (pre-commit)

Returns exit code 0 when clean, 1 when any tracked/staged path matches a
sensitive pattern. Designed for CI (.github/workflows/ci.yml) and the versioned
`.githooks/pre-push` hook.

WHY THIS EXISTS
    The siting planning engine, commercial product apps, their app-level tests,
    and council rulebooks are commercial IP deliberately removed from the public
    repo (see the siting-made-private note).
    `.gitignore` is not enough on its own: it is bypassable with `git add -f`, and
    a newly-named sensitive file (e.g. a fresh `doc/private/*` strategy doc) slips
    straight through. This guard is the belt to that suspenders — it blocks the
    *push*, not just the accidental `add`. It only ever BLOCKS; it never modifies
    files, and it cannot recover a leak already pushed (that needs git-filter-repo).

    A real incident this guard would have caught: `doc/private/SITING_COMMERCIALISATION_REVIEW.md`
    was staged by a plain `git add -A` and only spotted by eye.

WHAT IS SENSITIVE (deny)  vs  PUBLIC (allow)
    Deny  = the siting engine, county rulebooks, layer/precedent data, private
            strategy docs, and obvious secrets.
    Allow = the CIVIC planning-stats lane (appeal outcomes, decision profiles,
            applications ingest, CPO, the LA planning-overturn view, Iris probes,
            the Part V probe) stays PUBLIC and must never be flagged. These are
            listed explicitly so a broadening of the deny rules can't catch them.

Maintenance: add a pattern when a new IP area appears; add an ALLOW entry if a
genuinely-public file ever trips a rule. Keep the deny rules narrow enough that
no civic-lane path matches (none contain "siting").
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

_SCRIPT_PATH = Path(__file__).resolve()
_PROJECT_ROOT = _SCRIPT_PATH.parents[1]

# planning/product/ is its own nested private repo (separate remote, see CLAUDE.md's
# multi-root layout) — this guard protects the PUBLIC root's tracked tree only. If a
# copy of this script is ever invoked from inside that private repo (e.g. its own
# tooling or hooks), it has nothing to check there and must not scan or error.
_PRIVATE_NESTED_REPO_MARKER = "planning/product"


def _running_inside_private_nested_repo() -> bool:
    return f"/{_PRIVATE_NESTED_REPO_MARKER}/" in _SCRIPT_PATH.as_posix()


# ── Directories whose entire subtree is private IP ───────────────────────────
DENY_DIR_PREFIXES: tuple[str, ...] = (
    # The consolidation (2026-07-31) moved the engine, rulebook and private tests here —
    # one prefix instead of the ~30 hand-maintained globs that produced every leak recorded
    # to date. planning/civic/ is the deliberately-public sibling and must never appear here.
    "planning/product/",
    "apps/planspec-demo/",
    "apps/public-signal/",
    "test/apps/",
    "doc/private/",
    "siting_reports/",
    "data/silver/parquet/planning_layers/",
    # Raw scrape cache AND the operator's own tender-response working files — nine .docx
    # tender documents were nearly pushed inside commit 7259967a (2026-08-13). The subtree
    # is regenerable or commercial. CORRECTION (2026-08-29): ida/ files WERE git-tracked
    # once — commit 87616200 (2026-08-01) added three scrape-cache .htm files, removed same
    # day by 230db850, but both commits are reachable from origin/main today; this guard
    # only scans the CURRENT tree, so an add-then-revert within one push is invisible to it
    # (see the --all-branches mode below, which doesn't close that gap either — only a
    # commit-range scan over the outgoing push would).
    "ida/",
)

# ── Any path containing this substring (case-insensitive) is private IP.
#    Verified: NO civic-lane path contains "siting", so this cannot false-positive
#    on the public planning-stats files. ─────────────────────────────────────
DENY_SUBSTRINGS: tuple[str, ...] = ("siting",)

# ── Filenames matching the siting-report generator's own output convention
#    (site_<lat>_<lon>_<dev_type>.<ext>) are private-IP even when they land outside
#    a deny directory and don't contain "siting" — confirmed 2026-08-02 by a disclosure
#    audit that found exactly this shape sitting untracked at the repo root, unflagged
#    by every rule above it. Matched on the basename only, case-insensitive. ───────────
_SITE_REPORT_RE = re.compile(
    r"^site_-?\d+(\.\d+)?_-?\d+(\.\d+)?_[a-z0-9_]+\.(html|pdf|docx|xlsx|json|md|geojson)$",
    re.IGNORECASE,
)

# ── write_gis_package (core/gis.py) drops the coordinates into a DIRECTORY name —
#    <stem>_gis/ — and writes plain-named files (site.geojson, constraints_points.geojson,
#    README.txt, "OPEN ME - site map.html") underneath it. None of those basenames match
#    _SITE_REPORT_RE, so the leak was in the parent path segment, not the file's own name.
#    Extension-less variant of the same stem, checked against every path segment. ────────
_SITE_STEM_RE = re.compile(
    r"^site_-?\d+(\.\d+)?_-?\d+(\.\d+)?_[a-z0-9_]+$",
    re.IGNORECASE,
)

# ── Individual private-IP files whose names carry neither a deny-dir nor
#    the "siting" substring (so they need naming explicitly). ────────────────
DENY_EXACT: frozenset[str] = frozenset(
    {
        "planning/product/mcp/precedent_fts.py",
        "planning/product/tools/build_point_scoped_layers.py",
        "doc/PLAN_ACP_GEOMETRY_AND_PRECEDENT.md",
        "doc/PLANNING_PERMISSION_SCOPING.md",
        "doc/archive/PLANNING_PERMISSION_SCOPING.md",
        "data/silver/parquet/planning_acp_cases.parquet",
        "planning/product/ingest/planning_layers_ingest.py",
        "planning/product/ingest/planning_layers_freshness.py",
        "planning/product/ingest/planning_acp_precedents.py",
        # PublicSignal's composition modules moved under apps/public-signal/ on 2026-08-08 (a
        # deny-directory above), so these three paths are now empty. They are kept so a file
        # recreated at one of them is still caught. The lesson that motivated the move: this list
        # and .gitignore had to be updated in lockstep, and market_intel.py was added to one and
        # not the other, leaving it guarded by a single layer.
        "dail_tracker_core/queries/procurement/opportunities.py",
        "dail_tracker_core/queries/procurement/market_intel.py",
        "test/api/test_api_publicsignal_feed.py",
        "test/dail_tracker_core/test_publicsignal_opportunities.py",
        "test/dail_tracker_core/test_publicsignal_market_intel.py",
        # The CE-leads parquet deliberately stays at data/gold/parquet/: a PUBLIC extractor
        # (extractors/pre_tender_work_packages.py) reads it, so it is a shared path rather than
        # product-private code. It is gitignored, and naming it here gives it a second guard.
        "data/gold/parquet/council_ce_report_leads.parquet",
        "pipeline_sandbox/planning_layers_wfs.py",
        "pipeline_sandbox/planning_scale_gated_triggers.py",
        "pipeline_sandbox/planning_areaofsite_normalise.py",
        "pipeline_sandbox/planning_plausibility_layers.py",
        "pipeline_sandbox/planning_osm_roads.py",
        "pipeline_sandbox/planning_osm_roads_geofabrik.py",
        "pipeline_sandbox/planning_osm_extras_geofabrik.py",
        "pipeline_sandbox/test_planning_layers_gate.py",
    }
)

# ── Obvious secrets (defensive; these are usually gitignored already). ───────
SECRET_BASENAMES: frozenset[str] = frozenset({".env", "secrets.toml", "id_rsa", "id_rsa.pub", "credentials.json"})
SECRET_SUFFIXES: tuple[str, ...] = (".pem",)

# ── CIVIC planning-stats lane — PUBLIC by deliberate decision. Never flag,
#    even if a future rule broadens. (None of these contain "siting".) ────────
ALLOWLIST: frozenset[str] = frozenset(
    {
        "planning/civic/extractors/planning_appeal_outcomes.py",
        "planning/civic/extractors/planning_applications_ingest.py",
        "planning/civic/extractors/planning_cpo_compensation.py",
        "planning/civic/extractors/planning_decision_profiles.py",
        "sql_views/constituency/constituency_la_planning_overturn.sql",
        "test/sql_views/test_la_planning_overturn.py",
        "planning/civic/sandbox/planning_partv_liability_probe.py",
        "planning/civic/sandbox/iris_planning_notices_audit.py",
        "pipeline_sandbox/_archive/cpo_planning_prospect_probe.py",
        "pipeline_sandbox/planning_appeal_outcomes.py",
        "pipeline_sandbox/planning_applications_ingest.py",
        "pipeline_sandbox/planning_cpo_compensation.py",
        "pipeline_sandbox/planning_decision_profiles.py",
        "pipeline_sandbox/test_planning_appeal_outcomes.py",
        "pipeline_sandbox/test_planning_cpo_compensation.py",
        "pipeline_sandbox/test_planning_decision_profiles.py",
        "doc/archive/PLANNING_DEVELOPMENT_CONTRIBUTIONS.md",
        "data/_meta/planning_appeal_outcomes_coverage.json",
        "data/_meta/planning_decision_profiles_coverage.json",
        "data/silver/parquet/planning_appeal_outcomes.parquet",
        "test/mcp_server/test_mcp_siting_vocabulary.py",
    }
)


def classify(path: str) -> str | None:
    """Return a reason string if ``path`` is private/sensitive, else ``None``.

    ``path`` is a repo-relative POSIX path (forward slashes). The allow-list wins
    over every deny rule so the civic lane is never flagged.
    """
    p = path.replace("\\", "/").strip()
    if not p or p in ALLOWLIST:
        return None

    lower = p.lower()
    base = p.rsplit("/", 1)[-1]

    if p in DENY_EXACT:
        return "private siting-engine file"
    for prefix in DENY_DIR_PREFIXES:
        if p.startswith(prefix):
            return f"under private directory {prefix!r}"
    for sub in DENY_SUBSTRINGS:
        if sub in lower:
            return f"path contains {sub!r} (siting IP)"
    if _SITE_REPORT_RE.match(base):
        return "generated site-report filename (site_<lat>_<lon>_<type>), regardless of location"
    for segment in p.split("/")[:-1]:
        if _SITE_STEM_RE.match(segment):
            return "under generated site-report directory (site_<lat>_<lon>_<type>_gis), regardless of filename"
    if base in SECRET_BASENAMES or any(base.endswith(sfx) for sfx in SECRET_SUFFIXES):
        return "looks like a secret / credential file"
    return None


def find_offenders(paths: list[str]) -> list[tuple[str, str]]:
    """Pure function: map a list of repo-relative paths to (path, reason) offenders."""
    out: list[tuple[str, str]] = []
    for p in paths:
        reason = classify(p)
        if reason:
            out.append((p, reason))
    return out


def _git_paths(staged: bool) -> list[str]:
    cmd = ["git", "diff", "--cached", "--name-only", "--diff-filter=AM", "-z"] if staged else ["git", "ls-files", "-z"]
    res = subprocess.run(cmd, cwd=_PROJECT_ROOT, capture_output=True, text=True, check=True)
    return [p for p in res.stdout.split("\0") if p]


def _remote_branches() -> list[str]:
    res = subprocess.run(
        ["git", "for-each-ref", "refs/remotes/origin", "--format=%(refname:short)"],
        cwd=_PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [b for b in res.stdout.splitlines() if b and not b.endswith("/HEAD")]


def _branch_paths(branch: str) -> list[str]:
    res = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", "-z", branch],
        cwd=_PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [p for p in res.stdout.split("\0") if p]


def find_all_branch_offenders() -> dict[str, list[tuple[str, str]]]:
    """Scan every remote branch's CURRENT tree, not just the one being pushed.

    Deny rules only apply going forward: a branch pushed before a path was classified
    private (e.g. `dail_tracker_core/siting/` before the 2026-07-31 consolidation) is never
    re-checked once new rules land, so it can sit exposed indefinitely. Confirmed 2026-08-29:
    `origin/bq_hse_enrich`, last pushed 2026-06-25, still carried 20 pre-consolidation engine
    files at its tip. Run this periodically (see the scheduled workflow), not on every push —
    it scans every branch, not just the one changing.
    """
    out: dict[str, list[tuple[str, str]]] = {}
    for branch in _remote_branches():
        offenders = find_offenders(_branch_paths(branch))
        if offenders:
            out[branch] = offenders
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Block private siting IP / secrets from the tracked tree.")
    ap.add_argument("--staged", action="store_true", help="scan only staged files (pre-commit use)")
    ap.add_argument(
        "--all-branches",
        action="store_true",
        help="scan every remote branch's current tree instead of the local tracked/staged tree "
        "(requires 'git fetch origin' first for a full view; run periodically, not per-push)",
    )
    args = ap.parse_args()

    if _running_inside_private_nested_repo():
        print(f"SKIPPED — running inside the private {_PRIVATE_NESTED_REPO_MARKER}/ repo, not the public root.")
        return 0

    if args.all_branches:
        by_branch = find_all_branch_offenders()
        if not by_branch:
            branches = _remote_branches()
            print(f"OK — no private siting IP or secrets on any of {len(branches)} remote branches.")
            return 0
        print("BLOCKED — sensitive paths found on remote branches:\n")
        for branch, offenders in sorted(by_branch.items()):
            print(f"  branch {branch!r}:")
            for path, reason in sorted(offenders):
                print(f"    ✗ {path}  ({reason})")
        print(
            "\nDelete or rewrite these branches — a stale branch pushed before a path was "
            "classified private stays exposed until someone re-checks it by hand.\n"
            "Note: deleting a remote branch does not purge the commit objects; GitHub can "
            "retain them and any existing clone/fork already has them. Full removal needs "
            "git-filter-repo across every affected ref, then a coordinated force-push."
        )
        return 1

    paths = _git_paths(args.staged)
    offenders = find_offenders(paths)
    if not offenders:
        scope = "staged" if args.staged else "tracked"
        print(f"OK — no private siting IP or secrets in the {scope} tree ({len(paths)} paths scanned).")
        return 0

    print("BLOCKED — sensitive paths must not be committed/pushed to the public repo:\n")
    for path, reason in sorted(offenders):
        print(f"  ✗ {path}  ({reason})")
    print(
        "\nThese belong in the private overlay (`git siting add -f ...`), not the public repo.\n"
        "If a path is genuinely public, add it to ALLOWLIST in tools/check_no_private_ip.py.\n"
        "Note: this guard cannot un-publish an already-pushed leak — that needs git-filter-repo."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
