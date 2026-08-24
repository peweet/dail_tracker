"""Tests for the tracked-import boundary guard (tools/check_no_untracked_imports.py).

Covers: a tracked file importing an untracked local module is flagged; a `try`/`except`-
guarded optional import is not; an ordinary tracked-to-tracked import is not; the real
tracked tree is clean — the live regression that fails the moment a commit ships a file
depending on something git doesn't have.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "check_no_untracked_imports", _REPO / "tools" / "check_no_untracked_imports.py"
)
guard = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(guard)


def test_finds_import_of_untracked_local_module() -> None:
    offenders = guard.find_offenders(
        ["test/dail_tracker_core/test_core_connections.py"],
        tracked=set(),  # nothing tracked -> every local import it makes is "untracked"
    )
    assert offenders, "expected at least one offender when nothing is tracked"


def test_does_not_flag_when_resolved_module_is_tracked() -> None:
    real_tracked = set(guard._git_paths(staged=False))
    offenders = guard.find_offenders(["test/dail_tracker_core/test_core_connections.py"], real_tracked)
    assert offenders == []


def test_try_except_guarded_import_is_not_flagged(tmp_path: Path) -> None:
    src = tmp_path / "guarded.py"
    src.write_text(
        "try:\n    from planning.product.core import engine as _engine\nexcept Exception:\n    _engine = None\n",
        encoding="utf-8",
    )
    imports = guard._imports_in(src)
    assert imports == [], "an import inside a guarded try/except must not be reported"


def test_contextlib_suppress_guarded_import_is_not_flagged(tmp_path: Path) -> None:
    """`with contextlib.suppress(Exception)` is the same guard as try/except and degrades
    identically on a public clone -- the form mcp_server/resource_policy.py uses for its
    best-effort precedent_fts cache drop."""
    src = tmp_path / "suppressed.py"
    src.write_text(
        "import contextlib\n"
        "with contextlib.suppress(Exception):\n"
        "    from planning.product.mcp import precedent_fts\n",
        encoding="utf-8",
    )
    assert guard._imports_in(src) == [("contextlib", 1)], (
        "an import inside contextlib.suppress(Exception) must not be reported"
    )


def test_bare_suppress_import_error_is_not_flagged(tmp_path: Path) -> None:
    """`from contextlib import suppress` then bare `suppress(ImportError)` is the same guard."""
    src = tmp_path / "bare.py"
    src.write_text(
        "from contextlib import suppress\nwith suppress(ImportError):\n    from planning.product import core\n",
        encoding="utf-8",
    )
    assert ("planning.product", 3) not in guard._imports_in(src)


def test_suppress_of_an_unrelated_exception_is_still_flagged(tmp_path: Path) -> None:
    """The exemption must not widen into 'any with-statement'. suppress(ValueError) does
    NOT catch a missing module, so the import still breaks a clean checkout and must be
    reported -- otherwise the guard could be silenced by an irrelevant suppress()."""
    src = tmp_path / "wrong_exc.py"
    src.write_text(
        "import contextlib\n"
        "with contextlib.suppress(ValueError):\n"
        "    from planning.product.mcp import precedent_fts\n",
        encoding="utf-8",
    )
    assert ("planning.product.mcp", 3) in guard._imports_in(src)


def test_plain_with_block_does_not_exempt_an_import(tmp_path: Path) -> None:
    """A non-suppress context manager protects nothing and must not earn the exemption."""
    src = tmp_path / "plain_with.py"
    src.write_text(
        "with open('x') as fh:\n    from planning.product.mcp import precedent_fts\n",
        encoding="utf-8",
    )
    assert ("planning.product.mcp", 2) in guard._imports_in(src)


def test_unguarded_import_of_a_real_but_untracked_module(tmp_path: Path, monkeypatch) -> None:
    """A bare (unguarded) import that resolves to a real-but-untracked file IS flagged."""
    monkeypatch.setattr(guard, "PROJECT_ROOT", tmp_path)
    (tmp_path / "private_pkg").mkdir()
    (tmp_path / "private_pkg" / "secret.py").write_text("X = 1\n", encoding="utf-8")
    importer = tmp_path / "public.py"
    importer.write_text("from private_pkg.secret import X\n", encoding="utf-8")

    offenders = guard.find_offenders(["public.py"], tracked=set())
    assert len(offenders) == 1
    assert offenders[0][2] == "private_pkg.secret"
    assert offenders[0][3] == "private_pkg/secret.py"


def test_real_tracked_tree_is_clean() -> None:
    """Fails the instant a tracked file ships depending on something git doesn't have."""
    result = subprocess.run(
        [sys.executable, str(_REPO / "tools" / "check_no_untracked_imports.py")],
        cwd=_REPO,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"import guard flagged tracked files:\n{result.stdout}\n{result.stderr}"
