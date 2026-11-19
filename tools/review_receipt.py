#!/usr/bin/env python
"""Issue a content-bound reviewer receipt for the hygiene stop hook.

Manual path (after an independent reviewer has returned PASS)::

    python -m tools.review_receipt --session-id SESSION --checkout CHECKOUT \
      --acceptance "MATCHES BRIEF" --result PASS --path path/to/file.py

The command only records the supplied result and SHA-256 values; it does not
run or impersonate a reviewer. By default each checkout gets its own receipt in
the temp directory, so a session that edits two repositories issues one for
each and ``tools/hooks/code_hygiene_gate.py`` reads them all. Set
``DAIL_REVIEW_RECEIPT`` to one output JSON path when the default temp location
is not suitable; the hook then reads only that file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

_RECEIPT_DIR = Path(tempfile.gettempdir())


def default_output(session_id: str, checkout: Path) -> Path:
    """One receipt per session and checkout; the hook globs ``dail_review_receipt_<session[:12]>*.json``."""
    root = os.path.normcase(str(checkout.resolve(strict=False)))
    key = hashlib.sha256(root.encode("utf-8")).hexdigest()[:10]
    return _RECEIPT_DIR / f"dail_review_receipt_{session_id[:12]}_{key}.json"


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


def carry_forward(receipt: dict[str, object], previous: object) -> dict[str, object]:
    """Keep the files an earlier PASS for this session and checkout covered, while their content is unchanged.

    One receipt per checkout used to be replaced outright, so a second review dropped the cover of files the
    first had passed (2026-11-19). A file edited since its review drops out; a FAIL carries nothing and is not
    carried from, so a failed review still withdraws every earlier PASS.
    """
    if receipt["result"] != "PASS" or not isinstance(previous, dict) or previous.get("result") != "PASS":
        return receipt
    if previous.get("session_id") != receipt["session_id"] or os.path.normcase(
        str(previous.get("checkout", ""))
    ) != os.path.normcase(str(receipt["checkout"])):
        return receipt
    reviewed = receipt["reviewed_paths"]
    assert isinstance(reviewed, dict)
    kept: set[str] = set()
    for name, digest in dict(previous.get("reviewed_paths") or {}).items():
        if name in reviewed:
            continue
        path = Path(name)
        try:
            unchanged = path.is_file() and sha256(path) == digest
        except OSError:
            unchanged = False
        if unchanged:
            reviewed[name] = digest
            kept.add(name)
    earlier = [row for row in previous.get("carried") or [] if isinstance(row, dict)]
    earlier_names = {name for row in earlier for name in row.get("paths", [])}
    own = [name for name in dict(previous.get("reviewed_paths") or {}) if name not in earlier_names]
    generations = [
        *earlier,
        {"acceptance": previous.get("acceptance"), "issued_utc": previous.get("issued_utc"), "paths": own},
    ]
    carried = [
        {**row, "paths": [name for name in row.get("paths", []) if name in kept]}
        for row in generations
        if any(name in kept for name in row.get("paths", []))
    ]
    return {**receipt, "carried": carried} if carried else receipt


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
            output = default_output(args.session_id, args.checkout)
        try:
            previous = json.loads(output.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            previous = None
        receipt = carry_forward(receipt, previous)
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
