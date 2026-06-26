"""Gaeltacht (Irish-speaking area) linguistic-planning node.

In a statutory Gaeltacht the Irish language is a material planning consideration (Planning and
Development Act s.10(2)): a Linguistic Impact Statement is typically required and, in stronger
sub-areas, an Irish-language occupancy condition. The node fires on containment of the national
Tailte Eireann Gaeltacht boundary (7 county areas); the council-specific verbatim rule resolves
via the rulebook. Trigger-level tests (no DEM / no rulebook) so they are fast + deterministic.
"""

from dail_tracker_core.siting.catalogue import load_catalogue
from dail_tracker_core.siting.engine import TRIGGERS
from dail_tracker_core.siting.layers import LayerStore

_STORE = LayerStore()


def test_gaeltacht_layer_ingested():
    assert "gaeltacht" in _STORE.available()


def test_node_validates_and_flag_states_the_statutory_rule():
    cat = load_catalogue()  # _validate() runs — source_layer glossary + class must resolve
    n = cat.node("gaeltacht")
    assert "gaeltacht" in n.source_layers
    assert n.mitigation_classes == frozenset({"D"})
    assert "Linguistic Impact Statement" in n.flag_template
    assert "s.10(2)" in n.flag_template


def test_fires_in_connemara_spiddal():
    fired, detail, status = TRIGGERS["gaeltacht"](_STORE, -9.3050, 53.2460, "one_off_house", None)
    assert fired and status == "ok"
    assert detail.get("gt_name")  # names the Gaeltacht (e.g. "Gaillimh")


def test_fires_in_kerry_gaeltacht_dingle():
    fired, detail, _ = TRIGGERS["gaeltacht"](_STORE, -10.2670, 52.1408, "one_off_house", None)
    assert fired and detail.get("gt_name")


def test_does_not_fire_outside_gaeltacht():
    fired, _d, status = TRIGGERS["gaeltacht"](_STORE, -6.2603, 53.3498, "one_off_house", None)  # Dublin
    assert not fired and status == "ok"  # honest 'ok' (national layer, point simply not inside)
