"""Tests for tools/build_runtime_manifest.py and the committed runtime_data_manifest.json.

The manifest is the source of truth for which tracked parquet a deployment needs (retention:
runtime) and why. These tests are the tripwires that keep the on-disk manifest honest:

* the DRIFT GUARD — the committed manifest must equal a fresh build, so adding a view (or an export)
  that reads a new parquet without regenerating the manifest fails CI;
* PUBLISH_PATHS parity — every parquet the git-lane publisher ships must be marked runtime;
* schema/enum/r2_key invariants;
* the placeholder map covers every {KEY} in the SQL;
* no NEW accidental ship gap (a runtime read of an untracked, non-optional file) creeps in.
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from tools import build_runtime_manifest as brm  # noqa: E402

_VALID_RETENTION = {"runtime", "lineage", "dead"}

# Ship gaps: runtime-read parquet that isn't git-tracked, so a fresh clone can't serve the view.
# Each must be fixed by tracking the file (or removing the view). Currently NONE — stateboards_boards
# was tracked 2026-06-19. Any entry the manifest reports beyond this set fails test_no_new_ship_gaps.
KNOWN_SHIP_GAPS: set[str] = set()


@pytest.fixture(scope="module")
def on_disk() -> dict:
    """The committed manifest."""
    return json.loads(brm.MANIFEST_PATH.read_text(encoding="utf-8"))


def test_manifest_exists():
    assert brm.MANIFEST_PATH.exists(), "run `python tools/build_runtime_manifest.py`"


def test_manifest_not_stale():
    """THE drift guard: committed manifest == fresh build (ignoring the timestamp).

    Fails when views/exports/placeholders changed but the manifest wasn't regenerated.
    """
    assert brm.main(["--check"]) == 0, "manifest is STALE — re-run `python tools/build_runtime_manifest.py`"


def test_schema_and_envelope(on_disk):
    for key in ("generated_at", "note", "summary", "files", "referenced_but_untracked", "unresolved_placeholders"):
        assert key in on_disk, f"missing envelope key: {key}"
    s = on_disk["summary"]
    assert s["runtime"] + s["lineage"] + s["dead"] == s["tracked_total"] == len(on_disk["files"])


def test_every_entry_well_formed(on_disk):
    for f in on_disk["files"]:
        assert f["retention"] in _VALID_RETENTION, f"bad retention for {f['path']}"
        assert f["path"].startswith("data/")
        assert f["read_at_runtime"] == (f["retention"] == "runtime")
        # runtime + lineage are real committed files → must carry a hash; dead may too but isn't required.
        if f["retention"] in {"runtime", "lineage"}:
            assert f["sha256"], f"missing sha256 for {f['path']}"
            assert f["size_bytes"] > 0, f"zero-byte {f['path']}"


def test_r2_key_reconstruction(on_disk):
    for f in on_disk["files"]:
        assert f["r2_key"] == "runtime/" + f["path"][len("data/") :]


def test_placeholder_map_covers_all_sql_placeholders(on_disk):
    """Every {KEY} read in sql_views resolves through PLACEHOLDER_TO_PATH (else it'd be silently
    dropped from the runtime set)."""
    assert on_disk["unresolved_placeholders"] == [], (
        f"add to PLACEHOLDER_TO_PATH in build_runtime_manifest.py: {on_disk['unresolved_placeholders']}"
    )


def test_no_new_ship_gaps(on_disk):
    """A runtime read of an untracked, non-optional file is a ship gap. Catch NEW ones; tolerate the
    documented known ones (which still need fixing)."""
    gaps = {e["path"] for e in on_disk["referenced_but_untracked"]}
    new_gaps = gaps - KNOWN_SHIP_GAPS
    assert not new_gaps, f"NEW ship gap(s) — runtime-read but not git-tracked: {sorted(new_gaps)}"


def test_publish_paths_parity(on_disk):
    """Every parquet the git-lane publisher (tools/publish_data.py PUBLISH_PATHS) ships must be a
    runtime file in the manifest — the two declarations of 'what the app reads' must agree."""
    publish_paths = _read_publish_paths()
    runtime = {f["path"] for f in on_disk["files"] if f["retention"] == "runtime"}
    tracked_parquet = set(brm.tracked_parquet())

    for entry in publish_paths:
        if entry.endswith(".parquet"):
            assert entry in runtime, f"PUBLISH_PATHS ships {entry} but manifest doesn't mark it runtime"
        else:
            # A directory (e.g. data/gold/parquet): every tracked parquet under it that is committed
            # for the app should be runtime (lineage pre-union copies are the documented exception).
            under = [p for p in tracked_parquet if p.startswith(entry.rstrip("/") + "/")]
            non_runtime = [p for p in under if p not in runtime]
            # Allow the known lineage/dead tail under gold (pre-union copies, unsurfaced precompute).
            lineage_or_dead = {f["path"] for f in on_disk["files"] if f["retention"] in {"lineage", "dead"}}
            unexpected = [p for p in non_runtime if p not in lineage_or_dead]
            assert not unexpected, f"under {entry}, unclassified non-runtime parquet: {unexpected}"


def _read_publish_paths() -> list[str]:
    """Statically read PUBLISH_PATHS from tools/publish_data.py without importing it (it pulls git
    + pyarrow at import-light but the list is a plain literal we can parse)."""
    src = (_ROOT / "tools" / "publish_data.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "PUBLISH_PATHS":
            return [el.value for el in node.value.elts if isinstance(el, ast.Constant)]
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "PUBLISH_PATHS" for t in node.targets
        ):
            return [el.value for el in node.value.elts if isinstance(el, ast.Constant)]
    raise AssertionError("PUBLISH_PATHS not found in tools/publish_data.py")


def test_lineage_files_are_not_runtime_read(on_disk):
    """A file hand-listed as lineage must genuinely have no runtime reader — else it should be
    runtime, not lineage."""
    readers, _ = brm.runtime_reads()
    for f in on_disk["files"]:
        if f["retention"] == "lineage":
            assert f["path"] not in readers, f"{f['path']} is read at runtime — reclassify as runtime, not lineage"
