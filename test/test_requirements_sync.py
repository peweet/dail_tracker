"""
Deploy-drift guard: requirements.txt must stay in sync with pyproject.toml.

`requirements.txt` is what Streamlit Community Cloud installs (the cloud image
does not read pyproject extras). It is hand-curated, not lock-exported, so it is
easy to bump a version bound in pyproject's `[ui]` extra (or a core dep) and
forget the mirror — leaving the deployed app on different versions than CI
tested against. Silent divergence between "what CI proves" and "what users run".

This test makes that divergence a red CI check instead of a production surprise.

Contract enforced:
  - Every package that appears in BOTH files must carry an *identical* version
    specifier (e.g. `pandas>=2.2,<3.1` in both, character-for-character after
    normalisation).
  - The set of packages that live ONLY in requirements.txt must be exactly the
    known cloud-only pins (currently {"pyarrow"} — a transitive of pandas/
    streamlit that the cloud image needs pinned explicitly). Adding a new
    untracked dep to requirements.txt trips this test on purpose, forcing a
    conscious decision about whether it also belongs in pyproject.

This is a pure unit test — no network, no pipeline output — so it runs in the
default CI lane (no marker).
"""

import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement

_ROOT = Path(__file__).parent.parent
_REQUIREMENTS = _ROOT / "requirements.txt"
_PYPROJECT = _ROOT / "pyproject.toml"

# Packages allowed to exist in requirements.txt without a pyproject counterpart.
# Keep this list tiny and commented — each entry is a deliberate cloud-only pin.
_CLOUD_ONLY = {
    "pyarrow",  # transitive of pandas/streamlit; cloud image needs it pinned explicitly
}


def _canonical(name: str) -> str:
    """PEP 503 name normalisation so 'Flatten-JSON' == 'flatten_json'."""
    return name.lower().replace("_", "-").replace(".", "-")


def _parse_requirements_txt() -> dict[str, str]:
    """Map canonical package name -> specifier string from requirements.txt."""
    out: dict[str, str] = {}
    for raw in _REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        req = Requirement(line)
        out[_canonical(req.name)] = str(req.specifier)
    return out


def _parse_pyproject() -> dict[str, str]:
    """Map canonical package name -> specifier from core deps + the [ui] extra.

    requirements.txt is the Streamlit-Cloud runtime, so the relevant pyproject
    sources are the core `[project.dependencies]` (pandas lives there) and the
    `[ui]` optional-dependency extra (streamlit/altair/plotly/duckdb).
    """
    data = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    project = data["project"]
    specs = list(project.get("dependencies", []))
    specs += project.get("optional-dependencies", {}).get("ui", [])

    out: dict[str, str] = {}
    for spec in specs:
        req = Requirement(spec)
        # Skip self-referential extras like "dail-tracker[ocr,ui,dev]".
        if _canonical(req.name) == "dail-tracker":
            continue
        out[_canonical(req.name)] = str(req.specifier)
    return out


def test_requirements_specifiers_match_pyproject():
    """Shared packages must carry identical version specifiers in both files."""
    reqs = _parse_requirements_txt()
    pyproj = _parse_pyproject()

    mismatches = {
        name: (reqs[name], pyproj[name]) for name in reqs.keys() & pyproj.keys() if reqs[name] != pyproj[name]
    }

    assert not mismatches, (
        "requirements.txt and pyproject.toml disagree on version bounds for "
        f"{sorted(mismatches)}. Streamlit Cloud installs requirements.txt, so "
        "this drift means the deployed app runs different versions than CI tested.\n"
        + "\n".join(
            f"  {name}: requirements.txt has '{r}', pyproject has '{p}'" for name, (r, p) in sorted(mismatches.items())
        )
    )


def test_requirements_only_known_cloud_only_extras():
    """requirements.txt must not introduce deps absent from pyproject, except
    the explicitly allow-listed cloud-only pins."""
    reqs = _parse_requirements_txt()
    pyproj = _parse_pyproject()

    unexpected = set(reqs) - set(pyproj) - _CLOUD_ONLY
    assert not unexpected, (
        f"requirements.txt lists {sorted(unexpected)} which are not in "
        "pyproject.toml's core deps or [ui] extra. Either add them to pyproject "
        "(so pipeline installs and CI cover them) or, if they are deliberate "
        "Streamlit-Cloud-only pins, add them to _CLOUD_ONLY in this test with a "
        "comment explaining why."
    )


@pytest.mark.parametrize("missing", sorted(_CLOUD_ONLY))
def test_cloud_only_pins_are_actually_present(missing):
    """Guard against the allow-list going stale: every _CLOUD_ONLY entry must
    still be in requirements.txt (otherwise drop it from the list)."""
    assert missing in _parse_requirements_txt(), (
        f"'{missing}' is allow-listed as cloud-only but no longer appears in "
        "requirements.txt. Remove it from _CLOUD_ONLY."
    )
