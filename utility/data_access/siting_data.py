"""Siting-check data access — thin Streamlit cache wrapper over the core engine.

NO business logic here (logic firewall): all spatial joins, rule resolution and DEM reads
live in dail_tracker_core.siting (Streamlit-free core). This module only caches the result.
"""

from __future__ import annotations

import streamlit as st

from dail_tracker_core.siting.dem import terrain as _terrain
from dail_tracker_core.siting.engine import SitingResult, evaluate as _evaluate


@st.cache_data(ttl=3600, show_spinner=False)
def evaluate_site(
    lon: float, lat: float, dev_type: str,
    num_units: int | None = None, floor_area_m2: float | None = None,
) -> SitingResult:
    return _evaluate(lon, lat, dev_type, num_units=num_units, floor_area_m2=floor_area_m2)


@st.cache_data(ttl=3600, show_spinner=False)
def site_terrain(lon: float, lat: float):
    return _terrain(lon, lat)
