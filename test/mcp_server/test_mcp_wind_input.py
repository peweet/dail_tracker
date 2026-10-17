"""Transport contract tests for the optional wind-generation siting input."""

from __future__ import annotations

import builtins
from types import ModuleType, SimpleNamespace

import pytest

from mcp_server.server import siting_check

LAT, LON = 51.8256, -8.3137


def _call(**kw):
    fn = getattr(siting_check, "fn", siting_check)
    return fn(lat=LAT, lon=LON, **kw)


@pytest.fixture
def fake_private(monkeypatch):
    state = {"make_store_calls": 0, "evaluate_kwargs": None}

    class FakeWindGeneration:
        def __init__(self, *, proposal_scope, turbine_count=None, total_generating_output_mw=None):
            if proposal_scope == "reject":
                raise ValueError("fake private contract rejection")
            self.proposal_scope = proposal_scope
            self.turbine_count = turbine_count
            self.total_generating_output_mw = total_generating_output_mw

    class FakeProcessCapacity:
        def __init__(self, **kwargs):
            self.values = kwargs

    class FakeInventoryLine:
        def __init__(self, **kwargs):
            self.values = kwargs

    class FakeStore:
        def available(self):
            return ("fake",)

    def make_store():
        state["make_store_calls"] += 1
        return FakeStore()

    def evaluate(*args, **kwargs):
        state["evaluate_kwargs"] = kwargs
        return SimpleNamespace()

    serialize_module = ModuleType("planning.product.api.serialize")
    serialize_module.serialize_result = lambda result, dev_type, include_context: {"ok": True}
    engine_module = ModuleType("planning.product.core.evaluation.engine")
    engine_module.evaluate = evaluate
    process_capacity_module = ModuleType("planning.product.core.evaluation.process_capacity")
    process_capacity_module.ProcessCapacity = FakeProcessCapacity
    inventory_module = ModuleType("planning.product.core.evaluation.seveso_inventory")
    inventory_module.InventoryLine = FakeInventoryLine
    layers_module = ModuleType("planning.product.core.layer_store.layers")
    layers_module.make_store = make_store
    wind_module = ModuleType("planning.product.core.evaluation.wind_generation")
    wind_module.WindGeneration = FakeWindGeneration
    evaluation_module = ModuleType("planning.product.core.evaluation")
    evaluation_module.engine = engine_module
    modules = {
        "planning.product.api.serialize": serialize_module,
        "planning.product.core.evaluation": evaluation_module,
        "planning.product.core.evaluation.engine": engine_module,
        "planning.product.core.evaluation.process_capacity": process_capacity_module,
        "planning.product.core.evaluation.seveso_inventory": inventory_module,
        "planning.product.core.layer_store.layers": layers_module,
        "planning.product.core.evaluation.wind_generation": wind_module,
    }
    original_import = builtins.__import__

    def import_private(name, globals=None, locals=None, fromlist=(), level=0):
        if name in modules:
            return modules[name]
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", import_private)
    return state, FakeWindGeneration


@pytest.mark.parametrize("total_generating_output_mw", [None, 0])
def test_wind_mapping_is_constructed_and_forwarded_as_typed_value(fake_private, total_generating_output_mw):
    state, wind_type = fake_private

    out = _call(
        wind_generation={
            "proposal_scope": "new_generation",
            "turbine_count": 0,
            "total_generating_output_mw": total_generating_output_mw,
        }
    )

    assert out == {"ok": True}
    forwarded = state["evaluate_kwargs"]["wind_generation"]
    assert type(forwarded) is wind_type
    assert forwarded.proposal_scope == "new_generation"
    assert forwarded.turbine_count == 0
    assert forwarded.total_generating_output_mw == total_generating_output_mw


def test_omitted_wind_mapping_forwards_none(fake_private):
    state, _ = fake_private

    _call()

    assert state["evaluate_kwargs"]["wind_generation"] is None


def test_missing_wind_quantities_keep_private_defaults(fake_private):
    state, _ = fake_private

    _call(wind_generation={"proposal_scope": "new_generation"})

    forwarded = state["evaluate_kwargs"]["wind_generation"]
    assert forwarded.turbine_count is None
    assert forwarded.total_generating_output_mw is None


def test_existing_positional_arguments_keep_their_order(fake_private):
    state, _ = fake_private
    fn = getattr(siting_check, "fn", siting_check)

    out = fn(LAT, LON, "one_off_house", 0, 0.0, "", 0.0, None, "", None, None, None, "footprint", 3, 1)

    assert out == {"ok": True}
    assert state["evaluate_kwargs"]["floor_area_basis"] == "footprint"
    assert state["evaluate_kwargs"]["storeys"] == 3
    assert state["evaluate_kwargs"]["basement_levels"] == 1
    assert state["evaluate_kwargs"]["wind_generation"] is None


@pytest.mark.parametrize(
    "wind_generation",
    [
        ["not", "a", "mapping"],
        {},
        {"proposal_scope": "new_generation", "unknown": 1},
        {"proposal_scope": "reject"},
    ],
)
def test_invalid_wind_mapping_is_bounded_before_store(fake_private, wind_generation):
    state, _ = fake_private

    out = _call(wind_generation=wind_generation)

    assert out == {"error": "The siting request is invalid."}
    assert state["make_store_calls"] == 0
