from __future__ import annotations

import sys

from tools.migration import extract_class_contract, extract_url_contract, scan_framework_coupling


def test_class_contract_ignores_docstrings_but_keeps_live_markup(tmp_path):
    module = tmp_path / "component.py"
    module.write_text(
        '''"""<div class="module-fake">example</div>"""

class Component:
    """<span class="class-fake">example</span>"""

    def render(self):
        """<p class="function-fake">example</p>"""
        return '<div class="real-card">live</div>'
''',
        encoding="utf-8",
    )

    static, dynamic = extract_class_contract.emitted_classes(module)

    assert static == {"real-card"}
    assert dynamic == set()


def test_class_contract_scans_nested_style_modules_without_docstrings(tmp_path, monkeypatch):
    pages = tmp_path / "utility" / "pages_code"
    ui = tmp_path / "utility" / "ui"
    nested = pages / "reports"
    nested.mkdir(parents=True)
    ui.mkdir(parents=True)
    css = tmp_path / "utility" / "shared_css.py"
    css.write_text('CSS = "<style>.base-rule { color: red; }</style>"\n', encoding="utf-8")
    (nested / "detail.py").write_text(
        '''"""<style>.docstring-fake { color: red; }</style>"""
CSS = "<style>.nested-rule { color: blue; }</style>"
''',
        encoding="utf-8",
    )
    monkeypatch.setattr(extract_class_contract, "PAGES_DIR", pages)
    monkeypatch.setattr(extract_class_contract, "UI_DIR", ui)
    monkeypatch.setattr(extract_class_contract, "CSS_FILE", css)

    selectors = extract_class_contract.defined_selectors()

    assert {"base-rule", "nested-rule"} <= selectors
    assert "docstring-fake" not in selectors


def test_class_contract_main_reports_parse_errors(tmp_path, capsys, monkeypatch):
    pages = tmp_path / "utility" / "pages_code"
    ui = tmp_path / "utility" / "ui"
    pages.mkdir(parents=True)
    ui.mkdir(parents=True)
    (pages / "broken.py").write_text("def incomplete(:\n", encoding="utf-8")
    app = tmp_path / "utility" / "app.py"
    css = tmp_path / "utility" / "shared_css.py"
    app.write_text("", encoding="utf-8")
    css.write_text("", encoding="utf-8")
    monkeypatch.setattr(extract_class_contract, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(extract_class_contract, "PAGES_DIR", pages)
    monkeypatch.setattr(extract_class_contract, "UI_DIR", ui)
    monkeypatch.setattr(extract_class_contract, "APP_FILE", app)
    monkeypatch.setattr(extract_class_contract, "CSS_FILE", css)
    monkeypatch.setattr(extract_class_contract, "BASELINE", tmp_path / "baseline.txt")
    monkeypatch.setattr(sys, "argv", ["extract_class_contract.py", "--check"])

    assert extract_class_contract.main() == 1
    captured = capsys.readouterr()
    assert "broken.py" in captured.err
    assert "failed closed" in captured.err


def test_url_contract_ignores_docstring_link_examples(tmp_path):
    module = tmp_path / "page.py"
    module.write_text(
        '''"""Example: /?module_fake=1"""

class Page:
    """Example: /?class_fake=1"""

    def render(self):
        """Example: /?function_fake=1"""
        selected = st.query_params["live_key"]
        return f"/?live_link={selected}"
''',
        encoding="utf-8",
    )

    keys, links = extract_url_contract.keys_in_module(module)

    assert keys == {"live_key"}
    assert links == {"live_link"}


def test_url_contract_scans_nested_modules(tmp_path, monkeypatch):
    pages = tmp_path / "utility" / "pages_code"
    ui = tmp_path / "utility" / "ui"
    nested = pages / "reports"
    nested.mkdir(parents=True)
    ui.mkdir(parents=True)
    (nested / "detail.py").write_text('value = st.query_params["nested_key"]\n', encoding="utf-8")
    app = tmp_path / "utility" / "app.py"
    app.write_text("", encoding="utf-8")
    monkeypatch.setattr(extract_url_contract, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(extract_url_contract, "PAGES_DIR", pages)
    monkeypatch.setattr(extract_url_contract, "UI_DIR", ui)
    monkeypatch.setattr(extract_url_contract, "APP_FILE", app)

    report = extract_url_contract.build_report()

    assert "`nested_key`" in report
    assert "utility/pages_code/reports/detail.py" in report


def test_url_contract_main_reports_nested_parse_errors(tmp_path, capsys, monkeypatch):
    pages = tmp_path / "utility" / "pages_code"
    ui = tmp_path / "utility" / "ui"
    nested = pages / "reports"
    nested.mkdir(parents=True)
    ui.mkdir(parents=True)
    (nested / "broken.py").write_text("def incomplete(:\n", encoding="utf-8")
    app = tmp_path / "utility" / "app.py"
    app.write_text("", encoding="utf-8")
    monkeypatch.setattr(extract_url_contract, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(extract_url_contract, "PAGES_DIR", pages)
    monkeypatch.setattr(extract_url_contract, "UI_DIR", ui)
    monkeypatch.setattr(extract_url_contract, "APP_FILE", app)
    monkeypatch.setattr(sys, "argv", ["extract_url_contract.py"])

    assert extract_url_contract.main() == 1
    captured = capsys.readouterr()
    assert "broken.py" in captured.err
    assert "failed closed" in captured.err


def test_framework_scan_ignores_docstring_markup(tmp_path):
    module = tmp_path / "page.py"
    module.write_text(
        '''"""<div>module example</div>"""

class Page:
    """<span>class example</span>"""

    def render(self):
        """<p>function example</p>"""
        return "<section>live markup</section>"
''',
        encoding="utf-8",
    )

    info = scan_framework_coupling.scan_module(module, tmp_path)

    assert info.html_literals == 1


def test_framework_file_walk_excludes_cache_trees(tmp_path):
    cache = tmp_path / ".cache" / "vendor"
    cache.mkdir(parents=True)
    (cache / "dependency.py").write_text("import streamlit\n", encoding="utf-8")
    live = tmp_path / "app.py"
    live.write_text("import streamlit\n", encoding="utf-8")

    assert scan_framework_coupling.iter_python_files(tmp_path) == [live]


def test_framework_markup_ratchet_scans_nested_modules(tmp_path, monkeypatch):
    pages = tmp_path / "utility" / "pages_code" / "reports"
    ui = tmp_path / "utility" / "ui"
    pages.mkdir(parents=True)
    ui.mkdir(parents=True)
    (pages / "detail.py").write_text(
        'import streamlit as st\nst.html("<div>inline</div>")\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(scan_framework_coupling, "PROJECT_ROOT", tmp_path)

    counts = scan_framework_coupling.markup_counts()

    assert counts == {"utility/pages_code/reports/detail.py": 1}


def test_framework_main_reports_parse_errors(tmp_path, capsys, monkeypatch):
    nested = tmp_path / "package"
    nested.mkdir()
    (nested / "broken.py").write_text("def incomplete(:\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["scan_framework_coupling.py", "--root", str(tmp_path)])

    assert scan_framework_coupling.main() == 1
    captured = capsys.readouterr()
    assert "broken.py" in captured.err
    assert "failed closed" in captured.err
