"""PDF word-geometry helpers shared by the procurement PDF parsers.

``cluster_word_rows`` groups PyMuPDF ``page.get_text("words")`` tuples into
visual rows by y-proximity — the layout-agnostic first stage proven in
probe_procurement_pdf_counties. It was previously copy-pasted (byte-identical)
into three extractors; keeping it here means a clustering fix reaches the LA
payments, public-body payments and HSE/Tusla parsers together instead of
needing the same edit three times.
"""

from __future__ import annotations


def cluster_word_rows(page, ytol: float = 3.0) -> list[list]:
    words = page.get_text("words")
    words.sort(key=lambda w: (round(w[1] / ytol), w[0]))
    rows, cur, cur_y = [], [], None
    for w in words:
        y = w[1]
        if cur_y is None or abs(y - cur_y) <= ytol:
            cur.append(w)
            cur_y = y if cur_y is None else cur_y
        else:
            rows.append(cur)
            cur, cur_y = [w], y
    if cur:
        rows.append(cur)
    return rows
