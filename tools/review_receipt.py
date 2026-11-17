#!/usr/bin/env python
"""Issue a content-bound reviewer receipt for the hygiene stop hook.

Manual path (after an independent reviewer has returned PASS)::

    python -m tools.review_receipt --session-id SESSION --checkout CHECKOUT \
      --acceptance "MATCHES BRIEF" --result PASS --path path/to/file.py

The command only records the supplied result and SHA-256 values; it does not
run or impersonate a reviewer. Set ``DAIL_REVIEW_RECEIPT`` to the output JSON
path consumed by ``tools/hooks/code_hygiene_gate.py`` when the default temp
location is not suitable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_receipt(
    *, session_id: str, checkout: Path, paths: list[Path], acceptance: str, result: str
) -> dict[str, object]:
    root = checkout.resolve(strict=False)
    reviewed: dict[str, str] = {}
    for path in paths:
        resolved = (path if path.is_absolute() else root / path).resolve(strict=True)
        resolved.relative_to(root)
        reviewed[str(resolved).replace("\\", "/")] = sha256(resolved)
    return {
        "schema_version": 1,
        "session_id": session_id,
        "checkout": str(root),
        "reviewed_paths": reviewed,
        "acceptance": acceptance,
        "result": result.upper(),
        "reviewer_role": "reviewer",
        "issued_utc": datetime.now(UTC).isoformat(),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--path", type=Path, action="append", required=True, dest="paths")
    parser.add_argument("--acceptance", required=True)
    parser.add_argument("--result", choices=("PASS", "FAIL"), required=True)
    parser.add_argument("--reviewer-role", choices=("reviewer",), default="reviewer")
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        receipt = build_receipt(
            session_id=args.session_id,
            checkout=args.checkout,
            paths=args.paths,
            acceptance=args.acceptance,
            result=args.result,
        )
        configured = os.environ.get("DAIL_REVIEW_RECEIPT", "").strip()
        output = args.output or (Path(configured) if configured else None)
        if output is None:
            output = Path(tempfile.gettempdir()) / f"dail_review_receipt_{args.session_id[:12]}.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f"{output.name}.{os.getpid()}.tmp")
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    except (OSError, ValueError) as exc:
        print(f"review receipt: {exc}")
        return 2
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
