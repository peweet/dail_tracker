"""Unit tests for the buffer(0)-repair Ireland-bbox guard in the two locator-map
boundary extractors (local_authority_boundaries_extract, constituency_boundaries_extract).

Added 2026-08-01 after a GIS bug-hardening scan (doc/LLM_GEO_EU_RESEARCH_SCAN_2026_07_31.md
Part 4) found the two extractors repaired invalid polygons with a bare `buffer(0)` and no
sanity check — unlike `planning_decision_profiles.py`'s make_valid()+Ireland-bbox pattern,
which is the established convention here. The guard added prints a warning (not a hard
drop/raise) when a repair leaves bounds outside Ireland's envelope, because these are
locator-map builders with their own "all N canonical entries present" integrity check that
would fail loudly on a silent drop.

Three behaviours locked in:
  * a valid input polygon never touches the repair/guard path at all
  * an invalid (self-intersecting) polygon whose buffer(0) repair lands back inside Ireland
    is silently accepted, no warning
  * an invalid polygon whose repair lands outside Ireland prints a WARNING but does not raise
    (build_outlines still returns a result)
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from reference.constituency_boundaries_extract import (  # noqa: E402
    _CONSTITUENCIES,
)
from reference.constituency_boundaries_extract import (
    build_outlines as build_constituency_outlines,
)
from reference.local_authority_boundaries_extract import (  # noqa: E402
    build_outlines as build_la_outlines,
)

# A valid, non-self-intersecting rectangle well inside Ireland.
_VALID_IN_IRELAND = {"type": "Polygon", "coordinates": [[[-8, 53], [-7, 53], [-7, 54], [-8, 54], [-8, 53]]]}
# A self-intersecting bowtie (vertices crossed) whose buffer(0) repair stays inside Ireland.
_BOWTIE_IN_IRELAND = {"type": "Polygon", "coordinates": [[[-8, 53], [-7, 54], [-7, 53], [-8, 54], [-8, 53]]]}
# Same bowtie shape, translated to lon=100 so the repair's bounds land outside the Ireland envelope.
_BOWTIE_OUT_OF_IRELAND = {"type": "Polygon", "coordinates": [[[100, 53], [101, 54], [101, 53], [100, 54], [100, 53]]]}


def _write_fc(tmp_path: Path, name_value: str, geometry: dict) -> Path:
    fc = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "properties": {"ENG_NAME_VALUE": name_value}, "geometry": geometry}],
    }
    dest = tmp_path / "fixture.geojson"
    dest.write_text(json.dumps(fc), encoding="utf-8")
    return dest


def test_la_valid_geometry_no_warning(tmp_path, capsys):
    path = _write_fc(tmp_path, "GALWAY CITY COUNCIL", _VALID_IN_IRELAND)
    result = build_la_outlines(path)
    assert "Galway City" in result["local_authorities"]
    assert "WARNING" not in capsys.readouterr().out


def test_la_invalid_repair_in_bounds_no_warning(tmp_path, capsys):
    path = _write_fc(tmp_path, "GALWAY CITY COUNCIL", _BOWTIE_IN_IRELAND)
    result = build_la_outlines(path)
    assert "Galway City" in result["local_authorities"]
    assert "WARNING" not in capsys.readouterr().out


def test_la_invalid_repair_out_of_bounds_warns_without_raising(tmp_path, capsys):
    path = _write_fc(tmp_path, "GALWAY CITY COUNCIL", _BOWTIE_OUT_OF_IRELAND)
    result = build_la_outlines(path)  # must not raise
    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "Galway City" in out
    assert "outside Ireland envelope" in out
    assert "local_authorities" in result


def test_constituency_valid_geometry_no_warning(tmp_path, capsys):
    name = sorted(_CONSTITUENCIES)[0]
    path = _write_fc(tmp_path, f"{name} (5)", _VALID_IN_IRELAND)
    result = build_constituency_outlines(path)
    assert name in result["constituencies"]
    assert "WARNING" not in capsys.readouterr().out


def test_constituency_invalid_repair_in_bounds_no_warning(tmp_path, capsys):
    name = sorted(_CONSTITUENCIES)[0]
    path = _write_fc(tmp_path, f"{name} (5)", _BOWTIE_IN_IRELAND)
    result = build_constituency_outlines(path)
    assert name in result["constituencies"]
    assert "WARNING" not in capsys.readouterr().out


def test_constituency_invalid_repair_out_of_bounds_warns_without_raising(tmp_path, capsys):
    name = sorted(_CONSTITUENCIES)[0]
    path = _write_fc(tmp_path, f"{name} (5)", _BOWTIE_OUT_OF_IRELAND)
    result = build_constituency_outlines(path)  # must not raise
    out = capsys.readouterr().out
    assert "WARNING" in out
    assert name in out
    assert "outside Ireland envelope" in out
    assert "constituencies" in result
