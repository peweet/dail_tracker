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
import hashlib
import os
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

# Reviewed public vocabulary collisions. Each exception is bound to exact blob bytes:
# changing a document requires a new review, not a permanent whole-path exemption.
# Never store private module names in this public mapping.
CONTENT_EXCEPTIONS: dict[str, tuple[str, str]] = {
    "doc/SOURCE_CADENCE_PROCEDURE.md": (
        "fd9edc3a9e22331c10aabdd8d6e9cce644f868c7cb589b1f53386d18affc7793",
        "Names the public experimental target-list CSV.",
    ),
    "doc/archive/BUYER_DOSSIER_DESIGN.md": (
        "0d299a525c995ad3e76c594d3161a009a7eebe541d9b99e667962e27b290880c",
        "Names a public coverage column and its proposed JSON field.",
    ),
    "doc/archive/fastapi_query_core_uncoupling_plan.md": (
        "dafe2bb74641a538aadc84f1d83f5eedcc6acae98b98c4367f08998c764f2e88",
        "Names a public coverage field in a historical API sketch.",
    ),
    "pipeline_sandbox/council_minutes/README_PROTOTYPE.md": (
        "8b908707f3be862a6235a89c5989f0d2adfcc2c7ce4d9d6ecf997e066b62afe3",
        "Names the public council coverage CSV.",
    ),
    "pipeline_sandbox/public_decisions/README.md": (
        "e1615a6106fc8f291c0e53aaef95c02ca17c23f4fce8b61a02813367368f0bda",
        "Names the public candidate-corpus registry CSV.",
    ),
}
_MODULE_STEM = re.compile(r"[a-z]+_[a-z_]{6,}")


def _snapshot_git(root: Path, *args: str, input_bytes: bytes | None = None) -> bytes:
    # Hooks export GIT_DIR/GIT_INDEX_FILE. A nested repository must not inherit them.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    result = subprocess.run(
        ["git", "-C", str(root), *args], input=input_bytes, capture_output=True, env=env, check=False
    )
    if result.returncode:
        raise ValueError("Git snapshot unavailable")
    return result.stdout


def _commit(root: Path, ref: str) -> str:
    return _snapshot_git(root, "rev-parse", "--verify", "--end-of-options", ref + "^{commit}").decode().strip()


def _private_repository() -> Path:
    # A linked public worktree may lack the nested repository. Resolve its shared
    # primary checkout without selecting the first entry in a worktree listing.
    common = _snapshot_git(_PROJECT_ROOT, "rev-parse", "--path-format=absolute", "--git-common-dir")
    return Path(os.fsdecode(common).strip()).parent / _PRIVATE_NESTED_REPO_MARKER


def scan_content(ref: str, private_repo: Path | None = None, private_ref: str = "main") -> dict:
    """Check committed Markdown for distinctive private Python module names.

    This is a partial name-disclosure check, not detection of copied source or
    paraphrased architecture. The working tree and private token list are never published.
    """
    try:
        private_repo = (private_repo or _private_repository()).resolve()
        actual = Path(os.fsdecode(_snapshot_git(private_repo, "rev-parse", "--show-toplevel")).strip())
        if actual.resolve() != private_repo:
            raise ValueError("Private source must be a separate repository root")
        public_sha = _commit(_PROJECT_ROOT, ref)
        private_sha = _commit(private_repo, private_ref)
        public_paths = _snapshot_git(_PROJECT_ROOT, "ls-tree", "-rz", "--name-only", public_sha).split(b"\0")
        private_paths = _snapshot_git(private_repo, "ls-tree", "-rz", "--name-only", private_sha).split(b"\0")
        public_stems = {Path(os.fsdecode(p)).stem for p in public_paths if p.endswith(b".py")}
        tokens = sorted(
            {
                Path(os.fsdecode(p)).stem
                for p in private_paths
                if p.endswith(b".py") and _MODULE_STEM.fullmatch(Path(os.fsdecode(p)).stem)
            }
            - public_stems
        )
        if not tokens:
            raise ValueError("Private source has no eligible module names")
        env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        result = subprocess.run(
            [
                "git",
                "-C",
                str(_PROJECT_ROOT),
                "grep",
                "--no-color",
                "--full-name",
                "-n",
                "-z",
                "-I",
                "-w",
                "-F",
                "-f",
                "-",
                public_sha,
                "--",
                "*.md",
            ],
            input=("\n".join(tokens) + "\n").encode(),
            capture_output=True,
            env=env,
            check=False,
        )
        if result.returncode not in (0, 1):
            raise ValueError("Public document scan failed")
        findings = []
        exempt = set()
        for match in re.finditer(rb"([^\0]+)\0([0-9]+)\0[^\n]*\n", result.stdout):
            path = os.fsdecode(match[1])[len(public_sha) + 1 :]
            if path not in exempt and path in CONTENT_EXCEPTIONS:
                digest, reason = CONTENT_EXCEPTIONS[path]
                blob = _snapshot_git(_PROJECT_ROOT, "show", public_sha + ":" + path)
                if reason.strip() and hashlib.sha256(blob).hexdigest() == digest:
                    exempt.add(path)
            if path not in exempt:
                findings.append({"path": path, "line": int(match[2])})
        if result.returncode == 0 and not findings and not exempt:
            raise ValueError("Unrecognised Git scan output")
        return {
            "status": "FAIL" if findings else "PASS",
            "public_commit": public_sha,
            "private_commit": private_sha,
            "findings": findings,
            "exceptions": sorted(exempt),
        }
    except (OSError, ValueError, UnicodeError):
        return {
            "status": "NOT_RUN",
            "findings": [],
            "reason": "Cannot verify committed Markdown; check the public ref and nested private repository/ref.",
        }


def _print_content_result(result: dict) -> int:
    print(f"{result['status']} - committed Markdown private-module name check")
    if result["status"] == "NOT_RUN":
        print(result["reason"])
        return 2
    print(f"public={result['public_commit']} private={result['private_commit']}")
    for hit in result["findings"]:
        print(f"  {hit['path']}:{hit['line']} - review private-module reference (content withheld)")
    for path in result["exceptions"]:
        print(f"  reviewed public vocabulary: {path}")
    return 1 if result["findings"] else 0


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
    ap.add_argument("--content", action="store_true", help="check committed Markdown; requires the nested private repo")
    ap.add_argument("--ref", default="HEAD", help="public commit/ref to inspect with --content")
    ap.add_argument("--private-repo", type=Path, help="explicit nested private checkout (default: primary checkout)")
    ap.add_argument(
        "--private-ref", default="main", help="private source revision (default: local main; never fetched)"
    )
    ap.add_argument(
        "--all-branches",
        action="store_true",
        help="scan every remote branch's current tree instead of the local tracked/staged tree "
        "(requires 'git fetch origin' first for a full view; run periodically, not per-push)",
    )
    args = ap.parse_args()

    if args.content:
        if args.staged or args.all_branches:
            ap.error("--content cannot be combined with --staged or --all-branches")
        return _print_content_result(scan_content(args.ref, args.private_repo, args.private_ref))

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
