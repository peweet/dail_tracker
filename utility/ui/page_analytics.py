"""Cookieless page-view counter.

Records ONLY which page was loaded and when — nothing per-person. No session
id, no IP, no User-Agent, no cookie, no localStorage. It cannot recognise a
returning visitor and is not meant to: it answers exactly one question, "which
pages get loaded most", with zero PII and therefore zero consent obligations.

Each page load appends one JSON line to ``logs/page_views.jsonl``:

    {"ts": "2026-06-18T10:31:04.812Z", "page": "rankings-procurement"}

JSON Lines is the storage on purpose — a single ``open(..., "a")`` append is
the cheapest concurrency-tolerant write primitive (no read-modify-write, no
file lock, no DB), so logging never blocks a page render and parallel Streamlit
sessions can't corrupt each other's rows.

Caveat for Streamlit Cloud: the container filesystem is ephemeral, so this log
resets on redeploy/sleep. That's fine for local "what do I look at most" use;
for durable cross-deploy counts the log would need flushing to external storage
(R2 — see memory/project_data_backup_r2). Not built here by design.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from config import PROJECT_ROOT

PAGE_VIEWS_LOG = PROJECT_ROOT / "logs" / "page_views.jsonl"


def log_page_view(url_path: str) -> None:
    """Append one page-load record. Never raises — analytics must not be able
    to take the app down, so any I/O failure is swallowed silently."""
    try:
        PAGE_VIEWS_LOG.parent.mkdir(parents=True, exist_ok=True)
        # Streamlit's hidden default Home page has an empty url_path; label it.
        page = url_path or "home"
        ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        line = json.dumps({"ts": ts, "page": page}, ensure_ascii=False)
        with PAGE_VIEWS_LOG.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:  # noqa: BLE001 — telemetry is best-effort, never fatal
        pass


def read_page_views(path: Path | None = None):
    """Load the raw event log as a DataFrame (``ts``, ``page``). Returns an
    empty frame if nothing has been logged yet. For aggregation/inspection,
    not used by the live app render path."""
    import pandas as pd

    src = path or PAGE_VIEWS_LOG
    if not src.exists():
        return pd.DataFrame(columns=["ts", "page"])
    rows = []
    with src.open(encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                rows.append(json.loads(raw))
            except json.JSONDecodeError:
                continue  # skip a partially-written/corrupt line
    df = pd.DataFrame(rows, columns=["ts", "page"])
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    return df


def top_pages(path: Path | None = None):
    """Return page-load counts, most-loaded first (``page``, ``views``)."""
    import pandas as pd

    df = read_page_views(path)
    if df.empty:
        return pd.DataFrame(columns=["page", "views"])
    counts = df["page"].value_counts().reset_index()
    counts.columns = ["page", "views"]
    return counts
