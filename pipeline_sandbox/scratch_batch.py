"""Download named PDFs and write their extracted text to a transient directory.

Inputs use ``NAME::URL``. ``DAIL_SCRATCH_DIR`` may relocate the output; otherwise
the project-wide OS-native runtime directory is used. Names are restricted to a
single safe path component so an input cannot escape that directory.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import fitz

from paths import configured_path, runtime_path
from services.http_engine import fetch_bytes, polite_headers

SCRATCH = configured_path("DAIL_SCRATCH_DIR", runtime_path("scratch_batch"))
_SAFE_NAME = re.compile(r"[A-Za-z0-9_.-]+")


def _parse_pair(value: str) -> tuple[str, str]:
    """Parse and validate one ``NAME::URL`` command-line value."""
    name, separator, url = value.partition("::")
    if not separator or not url:
        raise argparse.ArgumentTypeError("expected NAME::URL")
    if not _SAFE_NAME.fullmatch(name) or name in {".", ".."}:
        raise argparse.ArgumentTypeError("NAME must be one safe filename component")
    if not url.startswith(("https://", "http://")):
        raise argparse.ArgumentTypeError("URL must use http or https")
    return name, url


def _extract_pdf_text(url: str) -> str:
    """Fetch one PDF through the shared retry path and return its page text."""
    payload = fetch_bytes(
        url,
        headers=polite_headers(browser=True, extra={"Accept": "application/pdf,*/*"}),
        timeout=90,
        validate=lambda body: body.lstrip().startswith(b"%PDF"),
    )
    if payload is None:
        raise RuntimeError("download failed or response was not a PDF")
    with fitz.open(stream=payload, filetype="pdf") as document:
        return "\n".join(page.get_text() for page in document)


def main(argv: list[str] | None = None) -> int:
    """Extract every requested PDF, returning non-zero when any item fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pairs", nargs="+", type=_parse_pair, metavar="NAME::URL")
    args = parser.parse_args(argv)

    SCRATCH.mkdir(parents=True, exist_ok=True)
    failures = 0
    for name, url in args.pairs:
        try:
            text = _extract_pdf_text(url)
            destination: Path = SCRATCH / f"{name}.txt"
            destination.write_text(text, encoding="utf-8")
            print(f"OK {name} chars={len(text)} path={destination}")
        except Exception as exc:  # noqa: BLE001 - independent batch items must continue
            failures += 1
            print(f"ERR {name} {url} :: {type(exc).__name__}: {exc}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
