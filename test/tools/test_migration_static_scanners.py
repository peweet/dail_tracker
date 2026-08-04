from __future__ import annotations

import json
import sys

import pytest

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


def test_url_contract_route_records_ignore_lines_but_keep_public_fields():
    first = """## Routes (1)
| Route (`url_path`) | Title | Page module | app.py line |
|---|---|---|---:|
| `?page=members` | Members | `utility.pages_code.members` | 10 |

## Query parameters (0 distinct)
"""
    moved = first.replace("| 10 |", "| 999 |")
    renamed = first.replace("| Members |", "| Representatives |")

    assert extract_url_contract.extract_routes_from_doc(first) == extract_url_contract.extract_routes_from_doc(moved)
    assert extract_url_contract.extract_routes_from_doc(first) != extract_url_contract.extract_routes_from_doc(renamed)


def test_url_contract_check_fails_when_route_changes_but_parameters_do_not(tmp_path, capsys, monkeypatch):
    committed = """## Routes (1)
| Route (`url_path`) | Title | Page module | app.py line |
|---|---|---|---:|
| `?page=members` | Members | `utility.pages_code.members` | 10 |

## Query parameters (1 distinct)
| Parameter | Modules | Shared? |
|---|---|---|
| `member` | `utility/pages_code/members.py` | no |
"""
    current = committed.replace("`?page=members`", "`?page=representatives`")
    contract = tmp_path / "URL_CONTRACT.md"
    contract.write_text(committed, encoding="utf-8")
    monkeypatch.setattr(extract_url_contract, "DEFAULT_OUT", contract)
    monkeypatch.setattr(extract_url_contract, "build_report", lambda: current)
    monkeypatch.setattr(sys, "argv", ["extract_url_contract.py", "--check"])

    assert extract_url_contract.main() == 1
    assert "route records" in capsys.readouterr().err


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


def test_framework_markup_ratchet_excludes_plain_copy_but_finds_html_fragments(tmp_path):
    module = tmp_path / "page.py"
    module.write_text(
        """import streamlit as st

name = "Ada"

def component():
    return "<article>named</article>"

st.caption("An ordinary explanatory sentence.")
st.write(f"An ordinary sentence about {name}.")
st.markdown("**Formatted editorial copy.**")
st.caption("<strong>Raw HTML caption</strong>")
st.markdown(f"<div>A rendered fragment for {name}</div>")
st.html("Text-only content in the dedicated HTML sink")
st.html("<section>" + component())
st.html(body="<p>Keyword-form fragment</p>")
st.html(component())
fragment = "<aside>Named fragment</aside>"
st.html(fragment)
""",
        encoding="utf-8",
    )

    assert scan_framework_coupling.count_inline_markup(module) == 5


def test_framework_markup_ratchet_supports_explicit_split_and_rename_lineage(tmp_path, monkeypatch, capsys):
    baseline = tmp_path / "markup.json"
    baseline.write_text(
        json.dumps(
            {
                "version": 2,
                "counts": {
                    "utility/pages_code/legacy.py": 3,
                    "utility/ui/old_name.py": 2,
                    "utility/ui/stable.py": 1,
                },
                "lineage": [
                    {
                        "name": "legacy page split",
                        "baseline_paths": ["utility/pages_code/legacy.py"],
                        "current_counts": {
                            "utility/pages_code/legacy/header.py": 1,
                            "utility/pages_code/legacy/detail.py": 2,
                        },
                    },
                    {
                        "name": "component rename",
                        "baseline_paths": ["utility/ui/old_name.py"],
                        "current_counts": {"utility/ui/new_name.py": 2},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(scan_framework_coupling, "MARKUP_BASELINE", baseline)
    monkeypatch.setattr(
        scan_framework_coupling,
        "markup_counts",
        lambda: {
            "utility/pages_code/legacy/header.py": 1,
            "utility/pages_code/legacy/detail.py": 2,
            "utility/ui/new_name.py": 2,
            "utility/ui/stable.py": 1,
        },
    )

    assert scan_framework_coupling.run_markup_ratchet(update=False) == 0
    assert "OK — no new anonymous markup" in capsys.readouterr().out

    monkeypatch.setattr(
        scan_framework_coupling,
        "markup_counts",
        lambda: {
            "utility/pages_code/legacy/header.py": 2,
            "utility/pages_code/legacy/detail.py": 1,
            "utility/ui/new_name.py": 2,
            "utility/ui/stable.py": 1,
        },
    )
    assert scan_framework_coupling.run_markup_ratchet(update=False) == 1
    captured = capsys.readouterr()
    assert "utility/pages_code/legacy/header.py [legacy page split]: 1 -> 2 (+1)" in captured.err
    assert "legacy/detail.py" not in captured.err

    monkeypatch.setattr(
        scan_framework_coupling,
        "markup_counts",
        lambda: {
            "utility/pages_code/legacy/header.py": 1,
            "utility/pages_code/legacy/detail.py": 2,
            "utility/pages_code/legacy/new_child.py": 1,
            "utility/ui/new_name.py": 2,
            "utility/ui/stable.py": 1,
        },
    )
    assert scan_framework_coupling.run_markup_ratchet(update=False) == 1
    assert "utility/pages_code/legacy/new_child.py: 0 -> 1 (+1)" in capsys.readouterr().err


def test_framework_markup_ratchet_rejects_lineage_that_increases_the_budget(tmp_path, monkeypatch):
    baseline = tmp_path / "markup.json"
    baseline.write_text(
        json.dumps(
            {
                "version": 2,
                "counts": {"utility/pages_code/legacy.py": 1},
                "lineage": [
                    {
                        "name": "inflated split",
                        "baseline_paths": ["utility/pages_code/legacy.py"],
                        "current_counts": {
                            "utility/pages_code/legacy/one.py": 1,
                            "utility/pages_code/legacy/two.py": 1,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(scan_framework_coupling, "MARKUP_BASELINE", baseline)

    with pytest.raises(scan_framework_coupling.MarkupBaselineError, match="reallocates 1 source sites to 2"):
        scan_framework_coupling._load_markup_baseline()


def test_framework_main_reports_parse_errors(tmp_path, capsys, monkeypatch):
    nested = tmp_path / "package"
    nested.mkdir()
    (nested / "broken.py").write_text("def incomplete(:\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["scan_framework_coupling.py", "--root", str(tmp_path)])

    assert scan_framework_coupling.main() == 1
    captured = capsys.readouterr()
    assert "broken.py" in captured.err
    assert "failed closed" in captured.err
