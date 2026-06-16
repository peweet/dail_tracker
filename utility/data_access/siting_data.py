"""Siting-check data access — thin Streamlit cache wrapper over the core engine.

NO business logic here (logic firewall): all spatial joins, rule resolution and DEM reads
live in dail_tracker_core.siting (Streamlit-free core). This module only caches the result.
"""

from __future__ import annotations

import os

import streamlit as st

from dail_tracker_core.siting.dem import terrain as _terrain
from dail_tracker_core.siting.engine import SitingResult, evaluate as _evaluate
from dail_tracker_core.siting.layers import LayerStore

# ONE shared store, built once and reused for every evaluation. Without this, engine.evaluate()
# does `store = store or LayerStore()` and rebuilds every layer's STRtree per call. Set
# SITING_LAYERS_DIR to point at a lighter simplified layer set (e.g. c:/tmp/siting_simplify_final).
_LAYERS_DIR = os.environ.get("SITING_LAYERS_DIR")
_STORE = LayerStore(_LAYERS_DIR) if _LAYERS_DIR else LayerStore()


@st.cache_data(ttl=3600, show_spinner=False)
def evaluate_site(
    lon: float, lat: float, dev_type: str,
    num_units: int | None = None, floor_area_m2: float | None = None,
) -> SitingResult:
    return _evaluate(
        lon, lat, dev_type, num_units=num_units, floor_area_m2=floor_area_m2, store=_STORE
    )


@st.cache_data(ttl=3600, show_spinner=False)
def site_terrain(lon: float, lat: float):
    return _terrain(lon, lat)
