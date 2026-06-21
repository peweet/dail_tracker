"""Structural tests for the pipeline.py chain registry (CHAINS + _CHAIN_BLURBS).

The orchestrator launches each chain as ``python <script>`` with cwd=repo root.
A typo'd or moved script path is invisible until a run reaches that chain and
fails — late, and on the cloud runner. These cheap checks catch it at CI time
for ALL chains at once: every registered script must exist, names must be unique,
and the --list display's blurb map must stay in sync with the chain set.
"""

from __future__ import annotations

from paths import PROJECT_ROOT
from pipeline import _CHAIN_BLURBS, CHAINS


def test_every_chain_script_exists():
    missing = [(name, script) for name, script in CHAINS if not (PROJECT_ROOT / script).is_file()]
    assert not missing, f"CHAINS entries point at non-existent scripts: {missing}"


def test_chain_names_are_unique():
    names = [name for name, _ in CHAINS]
    dupes = sorted({n for n in names if names.count(n) > 1})
    assert not dupes, f"duplicate chain names in CHAINS: {dupes}"


def test_every_chain_has_a_blurb():
    # _print_chain_list() falls back to "" for a missing blurb, but every chain
    # should carry a one-line description so --list stays useful.
    missing = [name for name, _ in CHAINS if name not in _CHAIN_BLURBS]
    assert not missing, f"chains without a _CHAIN_BLURBS entry: {missing}"


def test_no_orphan_blurbs():
    names = {name for name, _ in CHAINS}
    orphans = sorted(set(_CHAIN_BLURBS) - names)
    assert not orphans, f"_CHAIN_BLURBS keys with no matching chain: {orphans}"


def test_promoted_chains_are_registered():
    # The 2026-06-21 promotion sweep — guard against an accidental revert.
    names = {name for name, _ in CHAINS}
    for expected in ("news_mentions", "member_contact", "ministerial_diaries"):
        assert expected in names, f"{expected!r} missing from CHAINS"
