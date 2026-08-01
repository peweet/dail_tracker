"""Overnight driver, phase 2: close the gaps phase 1 exposed.

Stages:
  1. moderngov_harvest.py --years 2024,2025,2026  (Fingal + Dublin City CMIS portals)
  2. merge_orphan_corpus.py  (wire the ModernGov corpus files into meetings_clean.jsonl)
  3. NIGHT_PW=1 night_harvest.py --deep on the gap councils (WAF'd seeds + zero-yield:
     Louth, Meath, Sligo, Limerick, Mayo, Roscommon, Cork County, Kerry, Kildare,
     Offaly, Leitrim, South Dublin, Longford, Cavan, Fingal)
  4. council_votes_extract.py  (re-sweep: Cork/Kilkenny/Laois/Fingal parsers over the
     3x corpus)
  5. decisions_extract.py  (final decisions + steering pass)
  6. NIGHT_PW=1 semistate_probe.py  (Playwright-rendered; phase 1 got 0 docs off JS shells)
  7. classifier exercise (reuses night_run.classifier_exercise)

Same deadline contract as night_run.py (NIGHT_DEADLINE_TS exported).
Usage: python pipeline_sandbox/night_run2.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
CM = HERE / "council_minutes"
SS = HERE / "semistate_minutes"
DEADLINE_H = 6.5
PY = sys.executable
START = time.time()
DEADLINE_TS = START + DEADLINE_H * 3600
GAP_COUNCILS = ("Louth,Meath,Sligo,Limerick,Mayo,Roscommon,Cork County,Kerry,Kildare,"
                "Offaly,Leitrim,South Dublin,Longford,Cavan,Fingal")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run_stage(name: str, cmd: list[str], cwd: Path, pw: bool = False) -> bool:
    if time.time() > DEADLINE_TS:
        log(f"SKIP {name} — global deadline passed")
        return False
    log(f"=== STAGE {name} ===")
    env = {**os.environ, "NIGHT_DEADLINE_TS": str(DEADLINE_TS), "PYTHONUNBUFFERED": "1",
           "PYTHONIOENCODING": "utf-8"}
    if pw:
        env["NIGHT_PW"] = "1"
    try:
        rc = subprocess.run(cmd, cwd=str(cwd), env=env,
                            timeout=DEADLINE_TS - time.time() + 600).returncode
        log(f"=== STAGE {name} exit={rc} ===")
        return rc == 0
    except subprocess.TimeoutExpired:
        log(f"=== STAGE {name} TIMEOUT ===")
        return False
    except Exception as e:  # noqa: BLE001
        log(f"=== STAGE {name} ERROR {type(e).__name__}: {e} ===")
        return False


def main() -> int:
    log(f"night phase 2 start; deadline in {DEADLINE_H}h")
    run_stage("1-moderngov", [PY, "moderngov_harvest.py", "--years", "2024,2025,2026"], CM)
    run_stage("2-orphan-merge", [PY, "merge_orphan_corpus.py"], CM)
    run_stage("3-deep-harvest", [PY, "night_harvest.py", "--deep",
                                 "--council", GAP_COUNCILS], CM, pw=True)
    run_stage("4-votes", [PY, "council_votes_extract.py"], CM)
    run_stage("5-decisions", [PY, "decisions_extract.py"], CM)
    run_stage("6-semistate", [PY, "semistate_probe.py"], SS, pw=True)
    sys.path.insert(0, str(HERE))
    from night_run import classifier_exercise

    classifier_exercise()
    log("NIGHT PHASE 2 COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
