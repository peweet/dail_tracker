"""Page-layer tests for the Companies landing EPA filter (PR 4).

The EPA membership join was graduated out of company.py into the
``has_epa_licence`` flag on v_procurement_supplier_summary. These tests lock the
PAGE side — that _landing() counts and filters on that column correctly. They run
the real _landing() in Streamlit bare mode with monkeypatched fetchers/widgets and
capture the emitted HTML, so a broken count or filter fails (verified by mutation).

Run:  pytest test/utility/test_company_page_smoke.py -v
"""

import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "utility"))
sys.path.insert(0, str(_ROOT / "utility" / "pages_code"))

import company  # noqa: E402

from dail_tracker_core.results import QueryResult  # noqa: E402


def _supplier_frame() -> pd.DataFrame:
    base = dict(
        n_awards=3,
        n_authorities=1,
        awarded_value_safe_eur=100_000.0,
        company_num="111",
        company_status="Normal",
        cro_match_method="exact",
        on_lobbying_register=False,
        lobbying_returns=0,
        is_lobbying_registrant=False,
        is_lobbying_client=False,
    )
    return pd.DataFrame(
        [
            {
                **base,
                "supplier": "Acme Ltd",
                "supplier_norm": "acme ltd",
                "company_num": "111",
                "has_epa_licence": True,
            },
            {
                **base,
                "supplier": "Beta Ltd",
                "supplier_norm": "beta ltd",
                "company_num": "222",
                "has_epa_licence": False,
            },
            {
                **base,
                "supplier": "Gamma Ltd",
                "supplier_norm": "gamma ltd",
                "company_num": "333",
                "has_epa_licence": True,
            },
        ]
    )


def _patch_landing(monkeypatch, *, frame, epa_checked: bool):
    """Wire _landing's data + widgets; return (html_sink, checkbox_labels)."""
    html_sink: list[str] = []
    checkbox_labels: list[str] = []

    monkeypatch.setattr(company, "fetch_supplier_summary_result", lambda *a, **k: QueryResult.success(frame))
    monkeypatch.setattr(company, "paginate", lambda *a, **k: 0)
    monkeypatch.setattr(company.st, "session_state", {})
    monkeypatch.setattr(company.st, "text_input", lambda *a, **k: "")

    def _checkbox(label, *a, **k):
        checkbox_labels.append(label)
        return epa_checked

    monkeypatch.setattr(company.st, "checkbox", _checkbox)
    monkeypatch.setattr(company.st, "caption", lambda *a, **k: html_sink.append(str(a[0]) if a else ""))
    monkeypatch.setattr(company.st, "html", lambda *a, **k: html_sink.append(str(a[0]) if a else ""))
    return html_sink, checkbox_labels


def test_landing_epa_count_reflects_flag(monkeypatch):
    # 2 of 3 suppliers carry has_epa_licence -> the filter label must show (2).
    _, labels = _patch_landing(monkeypatch, frame=_supplier_frame(), epa_checked=False)
    company._landing()
    epa_labels = [lbl for lbl in labels if "EPA licence" in lbl]
    assert epa_labels, "the EPA filter checkbox must be offered when EPA firms exist"
    assert "(2)" in epa_labels[0], f"count must equal the has_epa_licence sum (2); got {epa_labels[0]!r}"


def test_landing_epa_filter_keeps_only_licensed(monkeypatch):
    html_sink, _ = _patch_landing(monkeypatch, frame=_supplier_frame(), epa_checked=True)
    company._landing()
    grid = " ".join(html_sink)
    assert "Acme Ltd" in grid and "Gamma Ltd" in grid, "EPA-licensed firms must remain after filtering"
    assert "Beta Ltd" not in grid, "non-EPA firm must be filtered out when 'Only EPA licence' is checked"


def test_landing_unfiltered_shows_all(monkeypatch):
    html_sink, _ = _patch_landing(monkeypatch, frame=_supplier_frame(), epa_checked=False)
    company._landing()
    grid = " ".join(html_sink)
    assert "Acme Ltd" in grid and "Beta Ltd" in grid and "Gamma Ltd" in grid, "unfiltered landing shows every firm"


def test_landing_no_epa_firms_hides_filter(monkeypatch):
    frame = _supplier_frame()
    frame["has_epa_licence"] = False
    _, labels = _patch_landing(monkeypatch, frame=frame, epa_checked=False)
    company._landing()
    assert not [lbl for lbl in labels if "EPA licence" in lbl], "no EPA firms -> no EPA filter checkbox"
