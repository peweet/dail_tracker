"""Merge the fan-out workflow results into meeting_history.jsonl (the failed merge agent's job).
Deterministic, sandbox only."""
import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = r"C:\Users\pglyn\AppData\Local\Temp\claude\c--Users-pglyn-PycharmProjects-dail-extractor\0fb85024-4363-455e-a756-6e7c7eb88cd4\tasks\w2dlauwhv.output"

obj = json.load(open(OUT, encoding="utf-8"))
res = None
def walk(o):
    global res
    if isinstance(o, dict):
        if "results" in o and isinstance(o["results"], list):
            res = o["results"]
        for v in o.values():
            walk(v)
    elif isinstance(o, list):
        for v in o:
            walk(v)
walk(obj)
print("councils in fan-out results:", len(res) if res else 0)

_mh = HERE / "meeting_history.jsonl"
existing = [json.loads(l) for l in _mh.read_text(encoding="utf-8").splitlines() if l.strip()] if _mh.exists() else []
seen = {(r["council"], r.get("date", ""), r.get("file", "")) for r in existing}
out = list(existing)
added = 0
for c in (res or []):
    la = c.get("council")
    for m in c.get("meetings", []):
        items = [i for i in m.get("agenda_items", []) if i and i.strip()]
        if not items:
            continue
        date = m.get("date", "")
        url = m.get("source_url", "") or ""
        file = url.split("/")[-1] if url else date
        key = (la, date, file)
        if key in seen:
            continue
        seen.add(key)
        out.append({"council": la, "file": file, "date": date, "agenda_items": items[:15], "source_url": url})
        added += 1
(HERE / "meeting_history.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in out), encoding="utf-8")
print(f"added {added}; total {len(out)} meetings across {len({r['council'] for r in out})} councils")
print("by council:", dict(Counter(r["council"] for r in out)))
zero = [c["council"] for c in (res or []) if not any(m.get("agenda_items") for m in c.get("meetings", []))]
print("fan-out councils with 0 agendas:", zero)
