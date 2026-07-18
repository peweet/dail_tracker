"""Registration-graph invariants — the sql_views ordering conventions, machine-checked.

Registration order is encoded in filename alphabetics (sorted-glob loading,
``zz_`` prefixes, sort-first naming) plus hand-ordered lists in
``dail_tracker_core/connections.py``, and most production registration runs with
``swallow_errors=True`` — so an ordering mistake or an orphaned view file ships
as a silently half-empty page, not a crash. These tests make the three
conventions fail CI instead (DuckDB-layer audit 2026-07-17):

1. EVERY ``sql_views/**/*.sql`` file is reachable by at least one production
   registration pattern (closes the ship-dark gap: ``test_view_group_registers``
   iterates a hand-maintained group list, so a misnamed file matched by no glob
   was previously invisible).
2. Same-directory dependency edges satisfy sorted-glob order — or the consumer
   is registered ONLY via an explicit ordered list that places its dependency
   first (the constituency set is the proven example).
3. Cross-directory edges are satisfied by the api_conn unit sequence — the union
   connection registers the dependency's unit no later than the consumer's.

The dependency edges come from REAL SQL ASTs (``mcp_server/sql_index`` — DuckDB's
own parser), not from naming conventions. No parquet is read; runs in the fast suite.
"""

from __future__ import annotations

import sys
from fnmatch import fnmatch
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from dail_tracker_core import connections as C  # noqa: E402
from dail_tracker_core.db import SQL_VIEWS_DIR  # noqa: E402
from mcp_server import sql_index  # noqa: E402

# ── The production registration surface ────────────────────────────────────────
# Ordered UNITS as api_conn() registers them: the member set first, then the
# derived domain globs, then the attendance/housing/publicfinance globs, then the
# constituency list. Mirrors connections.api_conn — update BOTH when changing it.
_MEMBER_SET: list[str] = [
    *C.DOMAIN_FILES,
    *C.REGISTRY_FILES,
    *C.EXTERNAL_LINKS_FILES,
    *C.CONTACT_DETAILS_FILES,
    *C.NEWS_MENTIONS_FILES,
    *C.VOTE_FILES,
    *C.SPEECH_FILES,
]


def _api_units() -> list[list[str]]:
    units: list[list[str]] = [_MEMBER_SET]
    units.extend([g] for g in C._api_domain_globs())
    units.append(["attendance_*.sql"])
    units.append(["housing_*.sql"])
    units.append(["publicfinance_*.sql"])
    units.append(list(C.CONSTITUENCY_FILES))
    return units


def _all_production_patterns() -> list[str]:
    """Every pattern any production builder registers (api units ⊇ the per-domain
    Streamlit conns after the 2026-07-18 DOMAIN_REGISTRATIONS consolidation)."""
    pats: list[str] = list(_MEMBER_SET)
    for phases in C.DOMAIN_REGISTRATIONS.values():
        for globs, _swallow in phases:
            pats.extend(globs)
    for unit in _api_units():
        pats.extend(unit)
    return pats


def _files() -> list[Path]:
    return sorted(SQL_VIEWS_DIR.rglob("*.sql"))


def _matches(filename: str, pattern: str) -> bool:
    # register_views globs sql_views/**/{pattern}; patterns carry no separators,
    # so filename-level fnmatch is equivalent.
    return fnmatch(filename, pattern)


def test_every_view_file_is_reachable():
    pats = set(_all_production_patterns())
    orphans = [
        f.relative_to(SQL_VIEWS_DIR).as_posix()
        for f in _files()
        if not any(_matches(f.name, p) for p in pats)
    ]
    assert not orphans, (
        "sql_views files matched by NO production registration pattern — they will "
        f"silently never load (swallow_errors hides it): {orphans}. Either name the "
        "file with its domain prefix or add it to a DOMAIN_REGISTRATIONS/connections list."
    )


def test_same_directory_order_holds():
    views = sql_index.graph(REPO)
    glob_pats = [p for p in _all_production_patterns() if "*" in p]
    lists = [_MEMBER_SET, list(C.CONSTITUENCY_FILES)]

    failures = []
    for risk in sql_index.order_risks(views):
        consumer_file = Path(risk["file"]).name
        dep_file = Path(risk["needs_file"]).name
        # (a) a sorted glob matching the consumer would register it before its dep
        bad_globs = [p for p in glob_pats if _matches(consumer_file, p)]
        # (b) an explicit ordered list must carry dep before consumer
        ordered_ok = any(
            dep_file in lst and consumer_file in lst and lst.index(dep_file) < lst.index(consumer_file)
            for lst in lists
        )
        if bad_globs or not ordered_ok:
            failures.append({**risk, "matched_by_globs": bad_globs, "explicit_list_ok": ordered_ok})
    assert not failures, (
        "Same-directory dependency sorts AFTER its consumer and no explicit ordered "
        f"list saves it — sorted-glob registration would break: {failures}. Rename the "
        "dependency to sort first (the lobbying_base trick) or add both, ordered, to an "
        "explicit list in connections.py."
    )


def test_cross_directory_edges_satisfied_by_api_order():
    views = sql_index.graph(REPO)
    units = _api_units()

    first_unit: dict[str, int] = {}
    for f in _files():
        for i, unit in enumerate(units):
            if any(_matches(f.name, p) for p in unit):
                first_unit[f.name] = i
                break

    failures = []
    for name, v in views.items():
        for dep in v["reads"]:
            d = views[dep]
            cf, df = Path(v["file"]).name, Path(d["file"]).name
            if Path(v["file"]).parent == Path(d["file"]).parent:
                continue  # same-dir edges covered above
            ci, di = first_unit.get(cf), first_unit.get(df)
            if ci is None or di is None or di > ci:
                failures.append(
                    {"view": name, "consumer_unit": ci, "needs": dep, "dep_unit": di, "files": (cf, df)}
                )
    assert not failures, (
        "Cross-directory dependency registers AFTER its consumer in the api_conn unit "
        f"sequence (or never): {failures}. Reorder the units in connections.api_conn / "
        "_api_domain_globs, and mirror the change in _api_units() here."
    )


def test_api_glob_derivation_covers_old_hand_list():
    """The DOMAIN_REGISTRATIONS-derived API glob list must register AT LEAST every
    file the pre-derivation hand list did (parity floor; additions are fine)."""
    old = [
        "legislation_*.sql", "lobbying_*.sql", "charity_*.sql", "payments_*.sql",
        "committees_*.sql", "procurement_*.sql", "member_interests_*.sql",
        "member_zz_interests_*.sql", "vote_*.sql", "speech_*.sql", "sipo_*.sql",
        "judiciary_*.sql", "appointments_*.sql", "ministerial_diary_*.sql", "corporate_*.sql",
    ]
    new = C._api_domain_globs()
    files = _files()
    old_matched = {f.name for f in files if any(_matches(f.name, p) for p in old)}
    new_matched = {f.name for f in files if any(_matches(f.name, p) for p in new)}
    missing = old_matched - new_matched
    assert not missing, f"derived API glob list dropped files the old hand list registered: {sorted(missing)}"
