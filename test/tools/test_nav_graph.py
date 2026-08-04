"""Runs the nav-graph cul-de-sac ratchet (tools/check_nav_graph.py) in the fast suite.

The ratchet flags a known entity column rendered in a page with no entity_links
builder carrying that entity — a contextual cul-de-sac ([[feedback_entity_links_
seamless_navigation]]). The unit tests lock the property that makes the check
trustworthy: detection is AST-level, so a column named only in a comment or
docstring never counts as a rendered entity (the false positive that made a naive
substring scan flag ministerial_diaries).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools import check_nav_graph  # noqa: E402


def test_nav_graph_ratchet_holds(capsys):
    rc = check_nav_graph.main()
    out = capsys.readouterr().out
    assert rc == 0, f"nav-graph cul-de-sac(s) or stale baseline:\n{out}"


def test_scan_flags_uncarried_entity_column(tmp_path):
    f = tmp_path / "fake_page.py"
    f.write_text("x = row.supplier_normalised\n", encoding="utf-8")
    carried, cols = check_nav_graph.scan(f)
    assert "supplier_normalised" in cols
    assert not carried


def test_scan_detects_builder_call(tmp_path):
    f = tmp_path / "fake_page.py"
    f.write_text(
        "from ui.entity_links import company_profile_url\nu = company_profile_url(s)\n",
        encoding="utf-8",
    )
    carried, _ = check_nav_graph.scan(f)
    assert "COMPANY" in carried


def test_scan_recognises_shared_procurement_entity_wrappers(tmp_path):
    f = tmp_path / "fake_page.py"
    f.write_text(
        "\n".join(
            (
                "supplier = _supplier_href(row.supplier_norm)",
                "paid = _paid_supplier_href(row.supplier_normalised)",
                "authority = _authority_href(row.contracting_authority)",
                "authority_name = _authority_link(row.contracting_authority)",
            )
        ),
        encoding="utf-8",
    )

    carried, cols = check_nav_graph.scan(f)

    assert carried == {"AUTHORITY", "COMPANY"}
    assert cols == {"contracting_authority", "supplier_norm", "supplier_normalised"}


def test_scan_ignores_comment_only_reference(tmp_path):
    # A column named in a COMMENT (not rendered) must NOT count as an entity on
    # screen — this is the exact false positive AST-level detection eliminates.
    f = tmp_path / "fake_page.py"
    f.write_text("# resolve a minister member_code from the pipeline\nx = 1\n", encoding="utf-8")
    carried, cols = check_nav_graph.scan(f)
    assert "member_code" not in cols


def test_scan_ignores_module_class_and_function_docstrings(tmp_path):
    f = tmp_path / "fake_page.py"
    f.write_text(
        '''"""supplier_norm"""

class Example:
    """member_code"""

    def render(self):
        """bill_id"""
        return "ordinary text"
''',
        encoding="utf-8",
    )
    _, cols = check_nav_graph.scan(f)
    assert cols == set()


def test_main_scans_nested_page_modules(tmp_path, capsys, monkeypatch):
    nested = tmp_path / "reports" / "detail.py"
    nested.parent.mkdir()
    nested.write_text('column = "supplier_norm"\n', encoding="utf-8")
    monkeypatch.setattr(check_nav_graph, "PAGES", tmp_path)
    monkeypatch.setattr(check_nav_graph, "BASELINE", {})

    assert check_nav_graph.main() == 1
    assert "reports/detail.py" in capsys.readouterr().out


def test_nested_canonical_basename_does_not_inherit_top_level_ownership(tmp_path, monkeypatch):
    nested = tmp_path / "reports" / "member_overview.py"
    nested.parent.mkdir()
    nested.write_text('column = "member_code"\n', encoding="utf-8")
    monkeypatch.setattr(check_nav_graph, "PAGES", tmp_path)
    monkeypatch.setattr(check_nav_graph, "BASELINE", {})

    assert check_nav_graph.main() == 1


def test_main_reports_parse_errors_and_fails_closed(tmp_path, capsys, monkeypatch):
    broken = tmp_path / "broken.py"
    broken.write_text("def incomplete(:\n", encoding="utf-8")
    monkeypatch.setattr(check_nav_graph, "PAGES", tmp_path)
    monkeypatch.setattr(check_nav_graph, "BASELINE", {})

    assert check_nav_graph.main() == 1
    captured = capsys.readouterr()
    assert "broken.py" in captured.err
    assert "failing closed" in captured.err
