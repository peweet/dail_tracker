#!/usr/bin/env python
"""Statusline command — live token spend for the CURRENT session.

Registered in .claude/settings.json (statusLine). Receives the statusline JSON on
stdin (session_id, transcript_path, model...) and prints one line:

    est $4.20 | out 412k | turns 249 | fable-5

Fires on every UI refresh, so unlike tools/hooks/session_token_ledger.py (one full
scan at SessionEnd) it parses INCREMENTALLY: a per-session cache in %TEMP% stores the
transcript byte offset + running totals, and each call reads only appended lines.
Usage-field parsing matches token_ledger.scan(). Est $ is API-equivalent (notional on
a subscription); unknown models (incl. fable) are priced at the sonnet tier.
Fails open: any error prints a bare fallback and exits 0.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

# $/Mtok: (input, output, cache_write, cache_read) — same table as tools/token_trend.py
RATES = {
    "opus": (15, 75, 18.75, 1.5),
    "sonnet": (3, 15, 3.75, 0.3),
    "haiku": (0.8, 4, 1, 0.08),
}


def _rate(model: str):
    m = (model or "").lower()
    for k, v in RATES.items():
        if k in m:
            return v
    return RATES["sonnet"]


def _fmt_tok(n: float) -> str:
    return f"{n / 1e6:.1f}M" if n >= 1e6 else f"{n / 1e3:.0f}k"


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read().lstrip('\ufeff') or "{}")
        tp = payload.get("transcript_path") or ""
        sid = str(payload.get("session_id") or "unknown")[:36]
        model_label = ((payload.get("model") or {}).get("id") or "").replace("claude-", "")
        path = Path(tp)
        if not path.is_file():
            print(model_label or "no transcript")
            return 0

        cache_file = Path(tempfile.gettempdir()) / f"claude_statusline_{sid}.json"
        state = {"offset": 0, "out": 0.0, "fresh": 0.0, "cache_read": 0.0, "cost": 0.0, "turns": 0}
        try:
            cached = json.loads(cache_file.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and cached.get("offset", 0) <= path.stat().st_size:
                state.update(cached)
        except Exception:
            pass  # first call, or the transcript was truncated/rotated — rescan from 0

        with path.open("rb") as fh:
            fh.seek(int(state["offset"]))
            buf = fh.read()
        # Only consume complete lines; a partial trailing line is re-read next call.
        last_nl = buf.rfind(b"\n")
        if last_nl >= 0:
            for raw in buf[: last_nl + 1].splitlines():
                try:
                    rec = json.loads(raw)
                except Exception:
                    continue
                if rec.get("type") != "assistant":
                    continue
                msg = rec.get("message") or {}
                u = msg.get("usage") or {}
                if not u:
                    continue
                inp = u.get("input_tokens", 0) or 0
                out = u.get("output_tokens", 0) or 0
                cw = u.get("cache_creation_input_tokens", 0) or 0
                cr = u.get("cache_read_input_tokens", 0) or 0
                ri, ro, rcw, rcr = _rate(msg.get("model", ""))
                state["turns"] += 1
                state["out"] += out
                state["fresh"] += inp + cw
                state["cache_read"] += cr
                state["cost"] += (inp * ri + out * ro + cw * rcw + cr * rcr) / 1e6
            state["offset"] = int(state["offset"]) + last_nl + 1
            try:
                cache_file.write_text(json.dumps(state), encoding="utf-8")
            except Exception:
                pass

        print(
            f"est ${state['cost']:.2f} | out {_fmt_tok(state['out'])} | "
            f"turns {state['turns']} | {model_label or '?'}"
        )
        return 0
    except Exception:
        import os
        import traceback
        if os.environ.get("STATUSLINE_DEBUG"):
            traceback.print_exc()
        print("statusline n/a")
        return 0


if __name__ == "__main__":
    sys.exit(main())
