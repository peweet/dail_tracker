"""Docker disk garbage collection — report reclaimable space; prune on request.

WHY THIS EXISTS (2026-08-24): local Docker had grown to 80 images / 104.3GB reclaimable
(97%) and a 152.6GB docker_data.vhdx on a drive with 102GB free, entirely from repeat
`redline-review-engine` / `siting-native-verify` dev builds that were never cleaned up —
every rebuild left the prior layers behind as a dangling `<none>` image. Nobody was
watching disk usage between sessions, so it grew silently for weeks. This tool is the
fix: a report-only default (matches memory_gc.py's convention) plus an explicit
`--reclaim` action safe enough to run unattended on a schedule (see
register_docker_gc_task.ps1), and a separate `--compact` action that is NOT part of
the schedule because it calls `wsl --shutdown`, which stops every WSL distro on the
box, not just Docker's — that step stays a deliberate, confirmed action.

Safety boundary for `--reclaim` (the part that runs unattended):
  * `docker image prune -a -f --filter until=24h` — removes any image, dangling or
    tagged, not referenced by a container and untouched for 24h. Docker itself refuses
    to remove an image any container (even a stopped one) still references, and the
    24h filter protects a build from earlier today. Images are reproducible from
    source, so deleting one is never data loss.
  * `docker builder prune -f --filter until=168h` — build cache older than a week.
  * `docker volume prune -f` (no `-a`) — anonymous volumes only, Docker's own default.
    Named-but-unused volumes are left alone deliberately: unlike an image, a volume can
    hold real data with no container currently pointing at it (a torn-down compose
    stack), and that is not something to delete without a human looking at the list.

Usage:
  python tools/docker_gc.py                 # report only, no changes
  python tools/docker_gc.py --reclaim        # prune images/cache/anonymous volumes
  python tools/docker_gc.py --reclaim --dry-run   # print the commands, run nothing
  python tools/docker_gc.py --compact        # wsl --shutdown + vhdx compact (asks first)
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LOG = REPO / "logs" / "docker_gc_log.jsonl"

#: Docker Desktop's WSL2 backend stores the whole image/volume/cache store in this one
#: dynamically-growing file. Confirmed present on this box 2026-08-24 (Get-ChildItem);
#: not a documented Docker API, so treat a miss as "can't check", never as "0 bytes".
VHDX_CANDIDATES = (
    Path.home() / "AppData" / "Local" / "Docker" / "wsl" / "disk" / "docker_data.vhdx",
    Path.home() / "AppData" / "Local" / "Docker" / "wsl" / "data" / "ext4.vhdx",
)

#: Named only so the elevation message can warn AGAINST the obvious-looking fix.
#: `wsl --manage docker-desktop --set-sparse true` acts on the DISTRO's own root VHD
#: (ext4.vhdx, 0.14GB here), NOT on docker_data.vhdx — verified 2026-08-24 inside the
#: distro: root is /dev/sdd while the Docker data disk is /dev/sde, a separate disk
#: Docker Desktop attaches. Running it leaves the 158GB file untouched, which is exactly
#: what happened when it was tried. `diskpart compact vdisk` on the file is the real route.
SPARSE_DISTRO = "docker-desktop"

_ROW_RE = re.compile(
    r"^(Images|Containers|Local Volumes|Build Cache)\s+(\d+)\s+(\d+)\s+"
    r"([\d.]+\s?\w+)\s+([\d.]+\s?\w+)(?:\s*\((\d+)%\))?",
    re.MULTILINE,
)
_UNIT_MB = {"B": 1 / (1024 * 1024), "KB": 1 / 1024, "KIB": 1 / 1024, "MB": 1, "MIB": 1, "GB": 1024, "GIB": 1024}


def _to_mb(size: str) -> float:
    m = re.match(r"([\d.]+)\s?(\w+)", size.strip())
    if not m:
        return 0.0
    value, unit = float(m.group(1)), m.group(2).upper()
    return value * _UNIT_MB.get(unit, 0.0)


def df_report(timeout: float = 8.0) -> dict[str, dict] | None:
    """Parsed `docker system df`. None if Docker isn't reachable (not installed, not
    running, cold-starting) — every caller must treat that as "can't tell", not zero."""
    try:
        out = subprocess.run(["docker", "system", "df"], capture_output=True, text=True, timeout=timeout)
    except Exception:
        return None
    if out.returncode != 0:
        return None
    rows: dict[str, dict] = {}
    for m in _ROW_RE.finditer(out.stdout):
        kind, total, active, size, reclaim, pct = m.groups()
        rows[kind] = {
            "total": int(total),
            "active": int(active),
            "size_mb": _to_mb(size),
            "reclaimable_mb": _to_mb(reclaim),
            "reclaimable_pct": int(pct) if pct else None,
        }
    return rows or None


def vhdx_size_mb() -> float | None:
    for path in VHDX_CANDIDATES:
        if path.exists():
            return path.stat().st_size / (1024 * 1024)
    return None


def _fmt_gb(mb: float) -> str:
    return f"{mb / 1024:.1f}GB"


def print_report(rows: dict[str, dict] | None) -> None:
    if rows is None:
        print("docker system df: unreachable (Docker not running or not installed)")
        return
    for kind in ("Images", "Local Volumes", "Build Cache", "Containers"):
        r = rows.get(kind)
        if not r:
            continue
        pct = f" ({r['reclaimable_pct']}%)" if r["reclaimable_pct"] is not None else ""
        print(
            f"{kind:14s} {r['total']:>4d} total  {_fmt_gb(r['size_mb']):>8s}  reclaimable {_fmt_gb(r['reclaimable_mb'])}{pct}"
        )
    vhdx = vhdx_size_mb()
    if vhdx is not None:
        accounted = sum(r["size_mb"] for r in rows.values())
        bloat = vhdx - accounted
        print(f"docker_data.vhdx on disk: {_fmt_gb(vhdx)} ({_fmt_gb(bloat)} beyond what Docker itself accounts for)")
        if bloat > 20 * 1024:
            print(
                "vhdx is >20GB larger than Docker's own accounting — pruning alone won't shrink "
                "this file; run `python tools/docker_gc.py --compact` to reclaim it from Windows."
            )


#: Deletes nothing a running (or recently-stopped) container references, and the age
#: filters protect anything touched in the lookback window — see module docstring.
RECLAIM_COMMANDS = (
    ("image prune", ("docker", "image", "prune", "-a", "-f", "--filter", "until=24h")),
    ("builder prune", ("docker", "builder", "prune", "-f", "--filter", "until=168h")),
    ("volume prune", ("docker", "volume", "prune", "-f")),
)


def _log(entry: dict) -> None:
    try:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        with LOG.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def reclaim(dry_run: bool) -> int:
    before = df_report()
    for label, cmd in RECLAIM_COMMANDS:
        if dry_run:
            print(f"[dry-run] would run: {' '.join(cmd)}")
            continue
        print(f"running: {' '.join(cmd)}")
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        except Exception as exc:
            print(f"  {label} failed to start: {exc}")
            continue
        tail = out.stdout.strip().splitlines()[-1:] or [""]
        print(f"  {tail[0]}")
    if dry_run:
        return 0
    after = df_report()
    _log(
        {
            "ts": datetime.now(UTC).isoformat(timespec="seconds"),
            "before": before,
            "after": after,
        }
    )
    print("\nafter:")
    print_report(after)
    return 0


def trim(distro: str = "docker-desktop") -> int:
    """fstrim inside the Docker distro. This is a PREREQUISITE for compaction, not an
    optional extra: `compact vdisk` can only return blocks the guest has TRIMmed, so
    compacting without it reclaims freed-but-un-TRIMmed ext4 blocks as nothing. Safe to
    run any time — it changes no data and needs no shutdown."""
    try:
        out = subprocess.run(
            ["wsl", "-d", distro, "-e", "sh", "-c", "fstrim -av"],
            capture_output=True,
            text=True,
            timeout=300,
        )
    except Exception as exc:
        print(f"fstrim could not run ({exc}) — compaction will reclaim little or nothing")
        return 1
    print(out.stdout.strip() or out.stderr.strip())
    return out.returncode


def compact(assume_yes: bool = False) -> int:
    """fstrim, then wsl --shutdown, then compact docker_data.vhdx via diskpart.

    Never call this from the scheduled task: `wsl --shutdown` stops every WSL distro on
    the box, not just Docker's, so a distro doing unrelated work would be killed. That is
    why it prompts, and why --yes is an explicit opt-out rather than the default."""
    vhdx = None
    for path in VHDX_CANDIDATES:
        if path.exists():
            vhdx = path
            break
    if vhdx is None:
        print("no docker vhdx found under the known Docker Desktop locations — nothing to compact")
        return 1
    before_mb = vhdx.stat().st_size / (1024 * 1024)
    print(f"{vhdx} is {_fmt_gb(before_mb)}. This will run `wsl --shutdown` (stops ALL WSL distros, not just Docker's).")
    if not assume_yes:
        reply = input("Continue? [y/N] ").strip().lower()
        if reply != "y":
            print("aborted")
            return 1
    print("TRIMming freed blocks first (compaction can only reclaim TRIMmed space)...")
    trim()
    subprocess.run(["wsl", "--shutdown"], check=False, timeout=30)
    time.sleep(2)
    script = f'select vdisk file="{vhdx}"\nattach vdisk readonly\ncompact vdisk\ndetach vdisk\nexit\n'
    script_path = REPO / "logs" / "_docker_compact_diskpart.txt"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text(script, encoding="utf-8")
    try:
        out = subprocess.run(["diskpart", "/s", str(script_path)], capture_output=True, text=True, timeout=300)
    except OSError as exc:
        # WinError 740: diskpart always requires elevation, and a Claude session cannot
        # elevate. Say so plainly with the exact command rather than raising -- the
        # traceback reads as a tool bug when it is a fixed OS requirement.
        if getattr(exc, "winerror", None) == 740:
            print(
                f"\ndiskpart requires an ELEVATED prompt, which this process is not.\n"
                f"Quit Docker Desktop, then from an Administrator PowerShell:\n"
                f"  wsl --shutdown\n"
                f'  diskpart /s "{script_path}"\n'
                f"\nNOTE: `wsl --manage {SPARSE_DISTRO} --set-sparse true` does NOT fix this file.\n"
                f"It applies to the distro's own root VHD; docker_data.vhdx is a SEPARATE data\n"
                f"disk Docker Desktop attaches (verified 2026-08-24: root is /dev/sdd, the data\n"
                f"disk is /dev/sde). Compaction above is the route that acts on the right file."
            )
            return 2
        raise
    print(out.stdout)
    if out.returncode != 0:
        print(out.stderr)
        return out.returncode
    after_mb = vhdx.stat().st_size / (1024 * 1024)
    print(f"vhdx: {_fmt_gb(before_mb)} -> {_fmt_gb(after_mb)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reclaim", action="store_true", help="prune images/build cache/anonymous volumes")
    ap.add_argument("--dry-run", action="store_true", help="with --reclaim, print commands without running them")
    ap.add_argument("--compact", action="store_true", help="fstrim + wsl --shutdown + vhdx compact (asks first)")
    ap.add_argument("--trim", action="store_true", help="fstrim inside the Docker distro only; no shutdown, no compact")
    ap.add_argument("--yes", action="store_true", help="with --compact, skip the prompt (stops ALL WSL distros)")
    args = ap.parse_args()

    if args.trim:
        return trim()
    if args.compact:
        return compact(assume_yes=args.yes)
    if args.reclaim:
        return reclaim(dry_run=args.dry_run)
    print_report(df_report())
    return 0


if __name__ == "__main__":
    sys.exit(main())
