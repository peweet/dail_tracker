"""Customization validation backlog — list, run the eval gate, mark validated.

Counterpart to tools/hooks/customization_edit_marker.py, which records every
Edit/Write to a reusable customization (CLAUDE.md, .claude/, tools/hooks/,
tools/evals/) as UNVALIDATED in validation_ledger.json. This CLI is where the
backlog gets cleared:

    python tools/evals/validate_customizations.py                 # list backlog
    python tools/evals/validate_customizations.py --run           # run promptfoo gate
    python tools/evals/validate_customizations.py --mark-validated <path> [...]
    python tools/evals/validate_customizations.py --mark-validated --all

Two things the automation deliberately does NOT do:
  * Run evals from a hook or agent session — each promptfoo eval spends real API
    money (~$0.60-$1.50 per bench arm) and the probes are blocked by the
    auto-mode classifier inside agent sessions. Run --run from a plain terminal.
  * Auto-clear entries after --run — not every edit is covered by the current
    probes (a comment fix, a new hook with no bench). Clearing is an explicit
    human judgment via --mark-validated, so "validated" always means someone
    decided the gate covered the change.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LEDGER = REPO / "tools" / "evals" / "validation_ledger.json"
PROMPTFOO_CFG = REPO / "tools" / "evals" / "promptfooconfig.yaml"


def _load() -> dict:
    if not LEDGER.exists():
        return {}
    try:
        return json.loads(LEDGER.read_text(encoding="utf-8"))
    except Exception:
        print(f"WARN: {LEDGER} unreadable — treating as empty.")
        return {}


def _save(ledger: dict) -> None:
    LEDGER.write_text(json.dumps(ledger, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="List/clear the customization validation backlog.")
    ap.add_argument("--run", action="store_true", help="run the promptfoo regression gate (spends API money)")
    ap.add_argument(
        "--mark-validated", nargs="*", metavar="PATH", help="mark path(s) validated; with --all, everything"
    )
    ap.add_argument("--all", action="store_true", help="with --mark-validated: clear the whole backlog")
    args = ap.parse_args()

    ledger = _load()
    backlog = {p: e for p, e in ledger.items() if e.get("status") == "UNVALIDATED"}

    if args.mark_validated is not None:
        targets = list(backlog) if args.all else args.mark_validated
        if not targets:
            print("nothing to mark — pass path(s) or --all.")
            return 1
        stamp = datetime.now().isoformat(timespec="seconds")
        missing = [t for t in targets if t not in ledger]
        for t in targets:
            if t in ledger:
                ledger[t]["status"] = "VALIDATED"
                ledger[t]["validated"] = stamp
        _save(ledger)
        print(f"marked {len(targets) - len(missing)} entr(ies) VALIDATED.")
        for m in missing:
            print(f"  ⚠ not in ledger: {m}")
        return 0

    if args.run:
        print(f"running promptfoo gate ({PROMPTFOO_CFG.name}) — this spends real API money...")
        r = subprocess.run(
            ["promptfoo", "eval", "-c", str(PROMPTFOO_CFG)], cwd=str(REPO), shell=sys.platform == "win32"
        )
        print(
            "gate finished with exit code "
            f"{r.returncode}. Entries are NOT auto-cleared — review the results, then "
            "--mark-validated the paths the gate actually covered."
        )
        return r.returncode

    if not backlog:
        print("validation backlog empty — every recorded customization edit is validated.")
        return 0
    print(f"{len(backlog)} customization edit(s) UNVALIDATED:")
    for p, e in sorted(backlog.items(), key=lambda kv: kv[1].get("edited", "")):
        print(f"  {p}  (edited {e.get('edited', '?')}, {e.get('edits', 1)} edit(s))")
    print("run the gate from a PLAIN terminal: python tools/evals/validate_customizations.py --run")
    return 0


if __name__ == "__main__":
    sys.exit(main())
