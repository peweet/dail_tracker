from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    paths = [Path(p) for p in sys.argv[1:]]
    if not paths:
        paths = list(Path(".").glob("prompts/*.md")) + list(Path(".").glob("page_runbooks/*.md"))

    failed = False
    for path in paths:
        if not path.exists():
            continue
        words = path.read_text(encoding="utf-8", errors="ignore").split()
        if len(words) > 2500:
            print(f"WARN {path}: {len(words)} words. Consider splitting.")
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
