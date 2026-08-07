"""Build the machine-readable frontend acceptance contract.

The Markdown URL and class reports are useful to reviewers. A second frontend
needs the same facts as deterministic data: routes, query-parameter ownership,
the styled class vocabulary, and hashes for every CSS source. This generator
composes the existing AST scanners rather than creating another parser.

Usage:
    python tools/migration/build_frontend_contract.py --check
    python tools/migration/build_frontend_contract.py -o utility/static/frontend_contract.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.migration import extract_class_contract as class_contract  # noqa: E402
from tools.migration import extract_url_contract as url_contract  # noqa: E402

DEFAULT_OUT = PROJECT_ROOT / "utility" / "static" / "frontend_contract.json"
SCHEMA_VERSION = 1


def _relative(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _routing_contract() -> dict[str, Any]:
    routes = [
        {
            "path": f"/{route['url_path']}",
            "title": route["title"],
            "module": route["module"],
        }
        for route in url_contract.parse_routes()
    ]

    owners: dict[str, set[str]] = defaultdict(set)
    emitters: dict[str, set[str]] = defaultdict(set)
    sources = [
        *url_contract.PAGES_DIR.rglob("*.py"),
        *url_contract.UI_DIR.rglob("*.py"),
        url_contract.APP_FILE,
    ]
    for path in sorted(set(sources)):
        if "__pycache__" in path.parts:
            continue
        keys, link_keys = url_contract.keys_in_module(path)
        relative = _relative(path)
        for key in keys:
            owners[key].add(relative)
        for key in link_keys:
            emitters[key].add(relative)

    parameters = [
        {
            "name": key,
            "owners": sorted(paths),
            "emitters": sorted(emitters.get(key, set())),
        }
        for key, paths in sorted(owners.items())
    ]
    return {
        "routes": sorted(routes, key=lambda route: (route["path"], route["module"], route["title"])),
        "query_parameters": parameters,
    }


def _styling_contract() -> dict[str, Any]:
    shared_bytes = class_contract.CSS_FILE.read_bytes()
    per_module, emitted, dynamic, defined = class_contract.collect()
    styled = emitted & defined
    unstyled = sorted(name for name in emitted - defined if not class_contract.is_framework(name))

    local_stylesheets: list[dict[str, Any]] = []
    sources = [
        *class_contract.PAGES_DIR.rglob("*.py"),
        *class_contract.UI_DIR.rglob("*.py"),
        class_contract.APP_FILE,
    ]
    for path in sorted(set(sources)):
        if "__pycache__" in path.parts:
            continue
        blocks = class_contract._style_blocks(path)
        if not blocks:
            continue
        css = "\n\n".join(blocks).encode("utf-8")
        local_stylesheets.append(
            {
                "module": _relative(path),
                "sha256": _sha256(css),
                "bytes": len(css),
            }
        )

    return {
        "shared_stylesheet": {
            "path": _relative(class_contract.CSS_FILE),
            "sha256": _sha256(shared_bytes),
            "bytes": len(shared_bytes),
        },
        "page_local_stylesheets": local_stylesheets,
        "styled_classes": sorted(styled),
        "dynamic_class_stems": sorted(dynamic),
        "unstyled_classes": unstyled,
        "emitter_modules": sorted(per_module),
    }


def build_manifest() -> dict[str, Any]:
    """Return deterministic, framework-neutral acceptance data."""
    return {
        "schema_version": SCHEMA_VERSION,
        "sources": {
            "routes": _relative(url_contract.APP_FILE),
            "shared_stylesheet": _relative(class_contract.CSS_FILE),
            "url_report": _relative(url_contract.DEFAULT_OUT),
            "class_report": _relative(class_contract.DEFAULT_OUT),
        },
        "routing": _routing_contract(),
        "styling": _styling_contract(),
        "acceptance_rules": [
            "preserve_existing_route_paths",
            "preserve_existing_query_parameter_names",
            "preserve_styled_class_names_or_change_markup_and_css_together",
            "match_shared_stylesheet_sha256_for_byte_identical_reuse",
            "account_for_every_page_local_stylesheet",
        ],
    }


def render_manifest() -> str:
    return json.dumps(build_manifest(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _output_path(path: Path) -> Path:
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-o", "--out", type=Path, help="write the generated JSON contract")
    parser.add_argument("--check", action="store_true", help="fail if the committed contract has drifted")
    args = parser.parse_args()

    try:
        rendered = render_manifest()
    except (class_contract.AnalysisError, url_contract.AnalysisError, OSError, UnicodeError) as exc:
        print(f"Frontend contract analysis failed closed: {exc}", file=sys.stderr)
        return 1

    if args.check:
        if not DEFAULT_OUT.exists():
            print(f"Frontend contract missing: {DEFAULT_OUT}", file=sys.stderr)
            return 1
        committed = DEFAULT_OUT.read_text(encoding="utf-8")
        if committed != rendered:
            print(
                "Frontend contract DRIFT — regenerate with "
                "python tools/migration/build_frontend_contract.py "
                "-o utility/static/frontend_contract.json",
                file=sys.stderr,
            )
            return 1
        manifest = json.loads(rendered)
        print(
            "Frontend contract OK — "
            f"{len(manifest['routing']['routes'])} routes, "
            f"{len(manifest['routing']['query_parameters'])} parameters, "
            f"{len(manifest['styling']['styled_classes'])} styled classes."
        )
        return 0

    if args.out:
        destination = _output_path(args.out)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(rendered, encoding="utf-8", newline="\n")
        print(f"[wrote {destination}]", file=sys.stderr)
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
