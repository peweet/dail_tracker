"""Security and rendering contracts for official source links."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "utility"))

from ui import source_links  # noqa: E402


def test_source_links_escape_data_and_reject_non_http_urls(monkeypatch) -> None:
    rendered: list[str] = []
    monkeypatch.setattr(
        source_links,
        "st",
        SimpleNamespace(
            caption=lambda *_args, **_kwargs: None,
            markdown=lambda body, **_kwargs: rendered.append(body),
        ),
    )
    monkeypatch.setattr(source_links, "todo_callout", lambda *_args, **_kwargs: None)

    source_links.render_source_links(
        pd.DataFrame(
            [
                {
                    "source_url": 'https://example.ie/doc?q="unsafe"&page=1',
                    "source_label": '<img src=x onerror="alert(1)">',
                },
                {
                    "source_url": "javascript:alert(1)",
                    "source_label": "Unsafe scheme",
                },
            ]
        )
    )

    html = "".join(rendered)
    assert 'href="https://example.ie/doc?q=&quot;unsafe&quot;&amp;page=1"' in html
    assert "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;" in html
    assert "javascript:" not in html
    assert "<img" not in html
