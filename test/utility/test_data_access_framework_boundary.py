"""Keep the page retrieval layer reusable without a Streamlit runtime."""

from __future__ import annotations

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ACCESS = PROJECT_ROOT / "utility" / "data_access"
STREAMLIT_ADAPTER = DATA_ACCESS / "_cache.py"


def test_data_access_imports_streamlit_only_through_cache_adapter() -> None:
    offenders: list[str] = []
    for path in sorted(DATA_ACCESS.glob("*.py")):
        if path == STREAMLIT_ADAPTER:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            imports_streamlit = isinstance(node, ast.Import) and any(alias.name == "streamlit" for alias in node.names)
            imports_from_streamlit = isinstance(node, ast.ImportFrom) and node.module == "streamlit"
            if imports_streamlit or imports_from_streamlit:
                offenders.append(path.name)

    assert offenders == [], f"data-access modules importing Streamlit directly: {sorted(set(offenders))}"


def test_quarantine_renderer_lives_in_ui_layer() -> None:
    data_source = (DATA_ACCESS / "quarantine_data.py").read_text(encoding="utf-8")
    ui_source = (PROJECT_ROOT / "utility" / "ui" / "data_integrity_panel.py").read_text(encoding="utf-8")

    assert "render_data_integrity_panel" not in data_source
    assert "def render_data_integrity_panel" in ui_source
