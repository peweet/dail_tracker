"""The public MCP `siting_check` must reject an off-vocabulary dev_type/use_class, not degrade.

Measured 2026-08-08 against the shipped catalogue via `Node.applies(dev_type, use_class)`:

    dev_type   one_off_house 25 nodes · multi_unit 42 · commercial 37 · extension 17
               ANY other string (typo, wrong case, empty) -> 15, the `applies_to: [all]` floor
    use_class  commercial + pharma_chemical -> 45 · + solar_farm 38 · + data_centre 41
               commercial + "Pharma_Chemical" -> 37, IDENTICAL to passing no use_class at all

Neither raised. The tool returned a complete, confident triage built off the reduced node set, and
for use_class the eight nodes that vanish on a capitalisation slip are Seveso COMAH, the EPA IE
licence and mandatory EIA screening — the highest-consequence items in the catalogue. The tool's
own docstring already instructed callers to "pass one of the canonical values exactly"; these tests
make that a contract instead of a hope.

An MCP tool is called by a model, which is exactly the caller most likely to pass a plausible
near-miss ("pharma_chemical" vs "pharma"), and least likely to notice that a shorter report is a
degraded one rather than a cleaner site.

The guard runs BEFORE make_store(), so these tests need no layer store and stay cheap.
"""

from __future__ import annotations

import pytest

from mcp_server.server import siting_check

# Ringaskiddy — the pharma stress-test pin. Any coordinate works; the guard rejects before it is used.
LAT, LON = 51.8256, -8.3137


def _call(**kw):
    fn = getattr(siting_check, "fn", siting_check)  # unwrap the FastMCP tool if it is wrapped
    return fn(lat=LAT, lon=LON, **kw)


@pytest.mark.parametrize(
    "bogus",
    ["One_Off_House", "one-off-house", "one_off_home", "house", "dwelling"],
)
def test_unknown_dev_type_is_refused_with_the_valid_list(bogus):
    out = _call(dev_type=bogus)
    assert "error" in out, f"dev_type {bogus!r} was accepted — it would silently run 15 of 67 nodes"
    assert bogus in out["error"], "the error must name the rejected value so a caller can self-correct"
    assert "one_off_house" in out["error"], "the error must list the valid dev_types"


@pytest.mark.parametrize(
    "bogus",
    ["Pharma_Chemical", "pharma-chemical", "pharma", "chemical", "solar"],
)
def test_unknown_use_class_is_refused_rather_than_silently_ungated(bogus):
    out = _call(dev_type="commercial", use_class=bogus)
    assert "error" in out, (
        f"use_class {bogus!r} was accepted — it gates nothing, which is indistinguishable from "
        "omitting it, so the Seveso/IE-licence/EIA bundle would stay silent on a pharma site"
    )
    assert bogus in out["error"]
    assert "pharma_chemical" in out["error"], "the error must list the canonical use classes"


def test_empty_use_class_stays_legal():
    """Unset genuinely means "do not gate on use" — the documented safe default, not an error.

    Asserted by what the guard does NOT reject: the call may still fail downstream for an unrelated
    reason (no layers ingested in this environment), so this checks only that the refusal is not the
    vocabulary one.
    """
    for empty in ("", "   "):
        out = _call(dev_type="one_off_house", use_class=empty)
        assert "unknown use_class" not in out.get("error", "")


def test_valid_vocabulary_is_not_refused_by_the_guard():
    """Every canonical value must pass the gate; a guard that rejects a real class is worse than none.

    planning/product is the private overlay (not in the public repo), so this import is guarded the
    same way mcp_server/server.py guards its own — tools/check_no_untracked_imports.py exempts a
    try/except ImportError, which keeps the public tree pushable without moving the test.
    """
    try:
        from planning.product.core.assistant import DEV_TYPES
        from planning.product.core.engine import USE_CLASSES
    except ImportError:
        pytest.skip("planning/product is private and not installed in this checkout")

    for dev_type in DEV_TYPES:
        out = _call(dev_type=dev_type)
        assert "unknown dev_type" not in out.get("error", ""), f"{dev_type} wrongly refused"
    for use_class in USE_CLASSES:
        out = _call(dev_type="commercial", use_class=use_class)
        assert "unknown use_class" not in out.get("error", ""), f"{use_class} wrongly refused"
