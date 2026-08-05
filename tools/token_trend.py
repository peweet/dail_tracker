"""Weekly token-spend + adoption trend from session transcripts. Prints aggregates only."""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

TDIR = Path.home() / ".claude" / "projects" / "c--Users-pglyn-PycharmProjects-dail-extractor"

# $/Mtok: (input, output, cache_write, cache_read)
RATES = {
    "opus": (15, 75, 18.75, 1.5),
    "sonnet": (3, 15, 3.75, 0.3),
    "haiku": (0.8, 4, 1, 0.08),
}


def rate(model: str):
    m = (model or "").lower()
    for k, v in RATES.items():
        if k in m:
            return v
    return RATES["sonnet"]  # unknown (incl. fable) priced at sonnet tier as a floor


wk = defaultdict(lambda: defaultdict(float))
unknown_models = defaultdict(int)

for f in TDIR.rglob("*.jsonl"):  # rglob: subagent transcripts live in per-session subdirs (agent-*.jsonl)
    for line in f.open(encoding="utf-8", errors="replace"):
        try:
            rec = json.loads(line)
        except Exception:
            continue
        ts = rec.get("timestamp")
        if not ts:
            continue
        try:
            week = datetime.fromisoformat(ts.replace("Z", "+00:00")).strftime("%G-W%V")
        except Exception:
            continue
        msg = rec.get("message") or {}
        if rec.get("type") == "assistant":
            u = msg.get("usage") or {}
            if u:
                w = wk[week]
                w["turns"] += 1
                inp = u.get("input_tokens", 0)
                out = u.get("output_tokens", 0)
                cw = u.get("cache_creation_input_tokens", 0)
                cr = u.get("cache_read_input_tokens", 0)
                w["fresh_in"] += inp + cw
                w["out"] += out
                w["cache_read"] += cr
                model = msg.get("model", "")
                if not any(k in (model or "").lower() for k in RATES):
                    unknown_models[model or "?"] += 1
                ri, ro, rcw, rcr = rate(model)
                w["cost"] += (inp * ri + out * ro + cw * rcw + cr * rcr) / 1e6
            for blk in msg.get("content") or []:
                if isinstance(blk, dict) and blk.get("type") == "tool_use":
                    n = blk.get("name", "")
                    if n in ("Read", "Grep", "Glob"):
                        wk[week]["raw_nav"] += 1
                    elif n.startswith("mcp__dail-tracker"):
                        wk[week]["mcp"] += 1
                    elif n in ("Agent", "Task"):
                        wk[week]["agents"] += 1

print(
    f"{'week':>9} {'turns':>6} {'fresh/turn':>10} {'out(M)':>7} {'cacheRd(M)':>10} "
    f"{'est$':>7} {'raw':>5} {'mcp':>4} {'agents':>6}"
)
for week in sorted(wk):
    w = wk[week]
    t = max(w["turns"], 1)
    print(
        f"{week:>9} {int(w['turns']):>6} {w['fresh_in'] / t:>10,.0f} {w['out'] / 1e6:>7.2f} "
        f"{w['cache_read'] / 1e6:>10.0f} {w['cost']:>7.0f} {int(w['raw_nav']):>5} "
        f"{int(w['mcp']):>4} {int(w['agents']):>6}"
    )

if unknown_models:
    tops = sorted(unknown_models.items(), key=lambda kv: -kv[1])[:4]
    print("\nmodels priced at sonnet tier (no known rate):", ", ".join(f"{m} ({c}t)" for m, c in tops))
