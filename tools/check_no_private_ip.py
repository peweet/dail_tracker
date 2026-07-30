"""
Private-IP / secret leak guard.

Usage:
    python tools/check_no_private_ip.py            # scan all tracked files
    python tools/check_no_private_ip.py --staged   # scan only staged files (pre-commit)

Returns exit code 0 when clean, 1 when any tracked/staged path matches a
sensitive pattern. Designed for CI (.github/workflows/ci.yml) and the versioned
`.githooks/pre-push` hook.

WHY THIS EXISTS
    The siting planning engine + council rulebooks are commercial IP that was
    deliberately removed from the public repo (see the siting-made-private note).
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
import subprocess
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ── Directories whose entire subtree is private IP ───────────────────────────
DENY_DIR_PREFIXES: tuple[str, ...] = (
    "dail_tracker_core/siting/",
    "planning_rules/",
    "test/siting/",
    "doc/private/",
    "siting_reports/",
    "data/silver/parquet/planning_layers/",
)

# ── Any path containing this substring (case-insensitive) is private IP.
#    Verified: NO civic-lane path contains "siting", so this cannot false-positive
#    on the public planning-stats files. ─────────────────────────────────────
DENY_SUBSTRINGS: tuple[str, ...] = ("siting",)

# ── Individual private-IP files whose names carry neither a deny-dir nor
#    the "siting" substring (so they need naming explicitly). ────────────────
DENY_EXACT: frozenset[str] = frozenset(
    {
        "mcp_server/precedent_fts.py",
        "tools/build_point_scoped_layers.py",
        "doc/PLAN_ACP_GEOMETRY_AND_PRECEDENT.md",
        "doc/PLANNING_PERMISSION_SCOPING.md",
        "doc/archive/PLANNING_PERMISSION_SCOPING.md",
        "data/silver/parquet/planning_acp_cases.parquet",
        "extractors/planning_layers_ingest.py",
        "extractors/planning_layers_freshness.py",
        "extractors/planning_acp_precedents.py",
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
        "extractors/planning_appeal_outcomes.py",
        "extractors/planning_applications_ingest.py",
        "extractors/planning_cpo_compensation.py",
        "extractors/planning_decision_profiles.py",
        "sql_views/constituency/constituency_la_planning_overturn.sql",
        "test/sql_views/test_la_planning_overturn.py",
        "pipeline_sandbox/planning_partv_liability_probe.py",
        "pipeline_sandbox/iris_planning_notices_audit.py",
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


def main() -> int:
    ap = argparse.ArgumentParser(description="Block private siting IP / secrets from the tracked tree.")
    ap.add_argument("--staged", action="store_true", help="scan only staged files (pre-commit use)")
    args = ap.parse_args()

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
