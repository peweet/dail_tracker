"""Awards-register gate on Procurement's SHARED supplier-dossier renderer (2026-07-23).

``_render_payments_supplier_profile`` is reused by Follow the Money and Council Spending
for the paid-supplier node. It now offers the ``/company`` hand-off ONLY when the supplier
resolves on the awards register: ``/company`` is awards-built (``company.py::_dossier``
shows "Company not found" otherwise), so a payments-only firm would land on a dead end —
the nav-graph never-a-false-hand-off rule ([[feedback_entity_links_seamless_navigation]]).
These pin the gate both ways. Bare-mode + monkeypatched fetchers (same harness idiom as
test_money_declutter_phase2.py — the repo avoids AppTest).

Run:  pytest test/utility/test_procurement_company_handoff.py -v
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT), str(_ROOT / "utility"), str(_ROOT / "utility" / "pages_code")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import procurement as pr  # noqa: E402

from dail_tracker_core.results import QueryResult  # noqa: E402

_HDR = pd.DataFrame(
    [
        {
            "supplier_class": "company",
            "supplier": "Acme Ltd",
            "n_paid_lines": 5,
            "n_ordered_lines": 0,
            "n_publishers": 3,
            "min_year": 2015,
            "max_year": 2024,
            "ordered_safe_eur": 0.0,
            "paid_safe_eur": 120_000.0,
            "vat_mixed": False,
            "cro_company_num": None,
            "cro_company_status": None,
        }
    ]
)


def _drive_supplier_dossier(monkeypatch, *, awarded: bool) -> str:
    """Render pr._render_payments_supplier_profile in bare mode; return the html sink.

    Bodies list returns empty so the render stops just past the gated CTA (which sits
    above that early-return) — exactly the fragment under test.
    """
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    sink: list[str] = []
    monkeypatch.setattr(pr, "fetch_payments_supplier_header_result", lambda *a, **k: QueryResult.success(_HDR))
    monkeypatch.setattr(
        pr, "fetch_payments_publishers_for_supplier_result", lambda *a, **k: QueryResult.success(pd.DataFrame())
    )
    monkeypatch.setattr(pr, "awards_register_norms", lambda: frozenset({"ACME"}) if awarded else frozenset())
    monkeypatch.setattr(pr, "back_button", lambda *a, **k: False)
    monkeypatch.setattr(pr, "empty_state", lambda *a, **k: None)
    monkeypatch.setattr(pr.st, "html", lambda *a, **k: sink.append(str(a[0]) if a else ""))
    monkeypatch.setattr(pr.st, "caption", lambda *a, **k: sink.append(str(a[0]) if a else ""))
    pr._render_payments_supplier_profile("ACME")
    return "".join(sink)


def test_supplier_dossier_company_link_gated_in(monkeypatch):
    # On the awards register -> /company resolves -> the hand-off is offered.
    assert "/company?supplier=" in _drive_supplier_dossier(monkeypatch, awarded=True)


def test_supplier_dossier_company_link_gated_out(monkeypatch):
    # Payments-only -> /company is "Company not found" -> NO link (never a false hand-off).
    assert "/company?supplier=" not in _drive_supplier_dossier(monkeypatch, awarded=False)
