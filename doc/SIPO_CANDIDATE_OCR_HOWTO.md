# How to run the SIPO per-candidate OCR yourself

This is the operator's guide for OCR'ing the **per-candidate GE2024 election-finance
PDFs** (the 43-constituency tier — distinct from the 18 party-level National-Agent
returns). It is a long, machine-heavy job (~8k pages) that must be run by **exactly one
owner on a clean machine**. Read the "Hard rules" section before you start — every one of
them is there because ignoring it cost a whole working session.

Status board, code map, and the data model live in
[SIPO_PIPELINE.md](SIPO_PIPELINE.md) / [SIPO_EXTRACTION_BACKLOG.md](SIPO_EXTRACTION_BACKLOG.md).
This file is purely "how do I push the OCR forward."

---

## What's left

| doc_type | docs | pages (approx) |
|---|---|---|
| `expense_statement` | 607 | ~7,840 |
| `donation_statement` | 428 | ~570 |

As of 2026-06-08 the **GPU fast path is running the expense tier** (~0.71 s/page, ~1.5 h to
finish). The page count moves quickly — don't trust a hardcoded snapshot; **get a live count
any time** with the progress check below (the GPU and CPU runs share the same `_ckpt`, so the
count covers both).

---

## ⚡ GPU fast path (RECOMMENDED — ~100× faster, validated 2026-06-08)

This machine has an **NVIDIA RTX 3060** (CUDA 12.x). PaddleOCR on the GPU runs at
**~0.76 s/page vs ~55 s/page on CPU — ~100× faster**, with **equivalent fidelity**
(validated: single page byte-identical to CPU; a 25-doc / 318-page smoke had **0 errors**,
**100 % identical text-region detection**, and the only text diffs were cosmetic
whitespace/cell-order — in one case the GPU was *more* correct, reading `N/A` where CPU
read `N/4`). The whole corpus (expenses + donations) finishes in **~3 hours** instead of
~2 weeks of nights.

It runs from an **isolated sandbox venv** so the main `.venv` is never touched, and it
writes to the **same production `_ckpt`** in the same format as the CPU driver — so the two
are interchangeable and the parser reads either identically.

### One-time setup (the sandbox GPU venv)
```powershell
# 1. create the isolated venv (base interpreter, NOT the repo .venv)
C:\Users\pglyn\AppData\Local\Programs\Python\Python312\python.exe -m venv C:\tmp\paddle_gpu_venv
# 2. paddle GPU build — from paddle's CUDA index (NOT PyPI)
C:\tmp\paddle_gpu_venv\Scripts\python.exe -m pip install paddlepaddle-gpu==3.3.1 -i https://www.paddlepaddle.org.cn/packages/stable/cu126/
# 3. paddleocr + pymupdf — from PyPI (a separate command; the -i above can't find them)
C:\tmp\paddle_gpu_venv\Scripts\python.exe -m pip install paddleocr==3.6.0 pymupdf
# 4. match cuDNN to what paddle was compiled against (9.9), else a "may cause serious bug" warning
C:\tmp\paddle_gpu_venv\Scripts\python.exe -m pip install "nvidia-cudnn-cu12>=9.9,<9.10"
```

### Run it (after the clean-machine step 0 below)
The runner is **`c:/tmp/gpu_ocr_runner.py`** — self-contained (paddleocr + pymupdf + stdlib
only, no repo deps), resumable (skips cached pages), with the same 300→300→200→skip attempt
ladder as the CPU driver, writing to `data/silver/sipo_candidate/_ckpt/`.
```powershell
taskkill /F /IM python.exe   # rule 0 — exactly one OCR process
# expenses (default):
C:\tmp\paddle_gpu_venv\Scripts\python.exe C:\tmp\gpu_ocr_runner.py
# donations afterwards:
C:\tmp\paddle_gpu_venv\Scripts\python.exe C:\tmp\gpu_ocr_runner.py --doc-types donation_statement
```
Logs to `data/silver/sipo_candidate/_log_gpu_runner.txt`. Progress check is the same as the
CPU path (below). The hard rules still apply — **one OCR process, clean machine** — but the
GPU run is fast enough (~1.5 h for expenses) that it's a single attended session, not an
overnight grind. **Graduation note:** the runner + GPU venv live in `c:/tmp` (sandbox, kept
isolated per "don't destabilise the main env"); promote `gpu_ocr_runner.py` into `extractors/`
and the GPU deps into a pipeline extra if this becomes the standing method.

---

## The two pieces (CPU path — fallback / no-GPU machines)

- **`extractors/sipo_candidate_ocr.py`** — the OCR driver. Renders each scanned page
  @300 DPI, runs PaddleOCR, and caches the raw cells to
  `data/silver/sipo_candidate/_ckpt/<candidate_slug>__<media_id>/cNNN.json` (one JSON per
  page). It is **resumable**: an already-cached page is skipped, so re-running just
  continues. It does **not** parse — parsing is a separate, cheap, re-runnable pass over
  the cached cells.
- **`extractors/_sipo_candidate_watchdog.py`** — the thing you actually run. PaddleOCR on
  this build both *segfaults* and *hangs*. The watchdog launches the driver in chunks of
  25 docs (a fresh paddle process each chunk), watches the checkpoint tree, and if no new
  page appears for `STALL = 600s` it kills the process tree and resumes from the cache.
  It stops on its own when the corpus is complete, and **aborts** (rather than spin-loops)
  after 6 consecutive fast crashes that OCR'd nothing — the signature of a contended
  machine or a second OCR process.

You almost never run the driver directly — always go through the watchdog.

---

## Hard rules (do not skip)

1. **Exactly ONE OCR process at a time.** Two PaddleOCR processes thrash memory, crash the
   machine (PowerShell `0x800705af`), and can wedge the GPU driver. If you're running OCR,
   close any other context/terminal that might launch it. The watchdog already runs one
   driver at a time — your job is to not start a *second* watchdog.
2. **Start from a clean machine.** Before launching, kill every stray python. The classic
   failure here was `import polars` taking **27 minutes** because dozens of duplicate
   python processes were all importing the heavy native lib at once — the watchdog then
   kills every driver mid-import, forever. One clean owner → polars imports in <1s and OCR
   runs at ~40–50 s/page.
   ```powershell
   taskkill /F /IM python.exe
   ```
3. **Don't background it blindly and walk away from a dirty box.** The watchdog is built
   for unattended runs, but only after rule 2. If a run "swarms," `taskkill /F /IM
   python.exe` resets everything (also kills Streamlit/MCP — restart those after).
4. **A venv python launch spawns a harmless base-interpreter twin.** You'll see paired
   processes. The base interp has no paddleocr, so its child crashes at import and writes
   nothing — it can't corrupt OCR data. Ignore it. (The one exception, the base-python
   *watchdog* twin, only matters if you launched the watchdog itself from base python — so
   always launch with the `.venv` python explicitly.)

---

## Run it

All commands from the repo root (`c:\Users\pglyn\PycharmProjects\dail_extractor`).

### 0. Clean the machine first (always)
```powershell
taskkill /F /IM python.exe
```

### 1. (Recommended) smoke-test on one document
Confirms paddle initialises and a page actually lands before you commit to an overnight run.
```powershell
.venv\Scripts\python.exe -m extractors.sipo_candidate_ocr --limit 1
```
Expect it to render → OCR → write `cNNN.json` files and exit 0. If it crashes at paddle
init, the machine is still contended (go back to step 0) — **do not** wrap a crashing
init in the watchdog.

### 2. Run the expense tier overnight (resumable, chunked)
```powershell
.venv\Scripts\python.exe extractors\_sipo_candidate_watchdog.py
```
Default scope is `expense_statement`. It will churn through ~25 docs per chunk, logging to
`data/silver/sipo_candidate/_log_candidate_ocr.txt`, and resume after any crash/hang. This
is **multi-night** for the full expense tier — just relaunch it each evening; it picks up
exactly where it left off.

### 3. Then the donation tier
Same driver, different doc type (pass-through arg). Run this **after** expenses, or instead
of — never a second watchdog alongside the first.
```powershell
.venv\Scripts\python.exe extractors\_sipo_candidate_watchdog.py --doc-types donation_statement
```
⚠️ Donation statements name **private donors** — that data is PII. Keep the
no-inference / privacy posture (see `feedback_personal_insolvency_privacy`,
`feedback_no_inference_in_app`) when it eventually reaches the parser/UI. Two donation
PDFs (O'Flynn Ken, Kennedy Pat) are HTTP 403 on SIPO's own server and were never
downloaded — they'll simply be skipped.

---

## Watch progress

**Live page count** (run in a *second* terminal — it's read-only, safe):
```powershell
.venv\Scripts\python.exe -c "from pathlib import Path; r=Path('data/silver/sipo_candidate/_ckpt'); print(sum(1 for _ in r.glob('*/c*.json')), 'pages cached across', len(list(r.glob('*'))), 'docs')"
```

**Tail the log:**
```powershell
Get-Content data\silver\sipo_candidate\_log_candidate_ocr.txt -Tail 30 -Wait
```

**Full per-tier breakdown** (complete / partial / untouched docs, pages done):
```powershell
.venv\Scripts\python.exe -c "
import csv, fitz
from pathlib import Path
from config import BRONZE_DIR, DATA_DIR
MANIFEST = BRONZE_DIR / 'sipo_candidate_expenses' / '_manifest.csv'
CKPT = DATA_DIR / 'silver' / 'sipo_candidate' / '_ckpt'
rows = list(csv.DictReader(MANIFEST.open(encoding='utf-8')))
key = lambda r: f\"{r['candidate_slug']}__{r['media_id']}\"
for dt in ['expense_statement','donation_statement']:
    jobs=[r for r in rows if r['doc_type']==dt and r['status'] in ('DOWNLOADED','CACHED') and r.get('local_path')]
    done=part=todo=pd=pt=0
    for r in jobs:
        p=Path(r['local_path']); pdf=p if p.is_absolute() else (BRONZE_DIR.parent/r['local_path']).resolve()
        if not pdf.exists(): continue
        d=fitz.open(pdf); n=d.page_count; d.close()
        ck=CKPT/key(r); c=sum(1 for i in range(1,n+1) if (ck/f'c{i:03}.json').exists()) if ck.exists() else 0
        pd+=c; pt+=n
        done+=c==n; part+=0<c<n; todo+=c==0
    print(f'{dt}: {len(jobs)} docs | complete={done} partial={part} untouched={todo} | pages {pd}/{pt}')
"
```

---

## When it stops

- **"corpus COMPLETE — nothing left to OCR"** in the log → the tier is done. Move to the
  next `--doc-types`, or to the parser pass.
- **"ABORT: N consecutive fast crashes"** → paddle can't start. The machine is contended or
  another OCR process is alive. Run `taskkill /F /IM python.exe`, confirm with the live
  count that nothing's moving, then relaunch.
- **Hang killed every ~10 min** but pages still advance slowly → that's the watchdog doing
  its job on individual bad pages; leave it. If *no* page ever lands, it's the
  contended-machine death spiral — clean and relaunch (step 0).

---

## After OCR: the parser pass (separate, cheap, no re-OCR)

Once cells are cached, the per-candidate **form parser** reads the `cNNN.json` files and
builds the structured rows — it never re-runs OCR, so it can iterate freely. Inspect any
document's cached OCR text to design/debug the parser:
```powershell
.venv\Scripts\python.exe -m extractors.sipo_candidate_ocr --dump <candidate_slug>__<media_id>
```
The party-tier parser (`sipo_expenses_paddle_etl.py`) and its geometry/anchor/cap layer are
the reuse target; the per-candidate single-form layout differs and needs its own parser —
that's tracked as the next stage in [SIPO_EXTRACTION_BACKLOG.md](SIPO_EXTRACTION_BACKLOG.md).
