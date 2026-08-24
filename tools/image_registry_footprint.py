"""Record and bound compressed OCI image layers without loading image contents.

The command is suitable for CI after a candidate image is pushed. It queries the
registry manifest through Docker Buildx, writes a small JSON receipt, and fails
when the configured compressed-byte ceiling is exceeded.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class RegistryFootprintError(RuntimeError):
    """The OCI manifest could not be read or did not meet its policy."""


def manifest_metrics(raw_manifest: str) -> dict[str, int | str]:
    payload = json.loads(raw_manifest)
    layers = payload.get("layers")
    if not isinstance(layers, list):
        raise RegistryFootprintError("expected a single-platform OCI manifest with layers")
    try:
        compressed_layer_bytes = sum(int(layer["size"]) for layer in layers)
    except (KeyError, TypeError, ValueError) as exc:
        raise RegistryFootprintError("manifest layer has no valid size") from exc
    return {
        "manifest_sha256": hashlib.sha256(raw_manifest.encode("utf-8")).hexdigest(),
        "manifest_bytes": len(raw_manifest.encode("utf-8")),
        "compressed_layer_bytes": compressed_layer_bytes,
        "layer_count": len(layers),
    }


def _run(command: Sequence[str]) -> str:
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no diagnostic"
        raise RegistryFootprintError(detail)
    return completed.stdout


def _repository(image: str) -> str:
    """Return the registry repository without a tag or digest."""

    name = image.split("@", maxsplit=1)[0]
    last_slash = name.rfind("/")
    last_colon = name.rfind(":")
    return name[:last_colon] if last_colon > last_slash else name


def select_platform_manifest(payload: dict[str, Any], platform: str) -> str:
    """Return the sole OCI index child matching ``os/architecture``."""

    try:
        operating_system, architecture = platform.split("/", maxsplit=1)
    except ValueError as exc:
        raise RegistryFootprintError("platform must use os/architecture form") from exc
    manifests = payload.get("manifests")
    if not isinstance(manifests, list):
        raise RegistryFootprintError("expected an OCI image index with platform manifests")
    matches = [
        manifest
        for manifest in manifests
        if isinstance(manifest, dict)
        and manifest.get("platform", {}).get("os") == operating_system
        and manifest.get("platform", {}).get("architecture") == architecture
    ]
    if len(matches) != 1 or not isinstance(matches[0].get("digest"), str):
        raise RegistryFootprintError(f"expected exactly one {platform} manifest in OCI index")
    return matches[0]["digest"]


def resolve_platform_manifest(image: str, platform: str) -> tuple[str, str, str | None]:
    """Read a layer manifest, selecting one platform when ``image`` is an index."""

    root_raw = _run(["docker", "buildx", "imagetools", "inspect", image, "--raw"])
    root_payload = json.loads(root_raw)
    if isinstance(root_payload.get("layers"), list):
        return root_raw, image, None
    child_digest = select_platform_manifest(root_payload, platform)
    resolved_image = f"{_repository(image)}@{child_digest}"
    selected_raw = _run(["docker", "buildx", "imagetools", "inspect", resolved_image, "--raw"])
    return selected_raw, resolved_image, root_raw


def _write_receipt(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(rendered)
        temporary = Path(handle.name)
    temporary.replace(path)


def inspect_registry_image(*, image: str, max_compressed_bytes: int, receipt: Path, platform: str) -> int:
    raw_manifest, resolved_image, index_manifest = resolve_platform_manifest(image, platform)
    metrics = manifest_metrics(raw_manifest)
    compressed = int(metrics["compressed_layer_bytes"])
    payload = {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "image_ref": image,
        "resolved_image_ref": resolved_image,
        "platform": platform,
        "max_compressed_bytes": max_compressed_bytes,
        "result": "pass" if compressed <= max_compressed_bytes else "fail",
        **metrics,
    }
    if index_manifest is not None:
        payload["index_manifest_sha256"] = hashlib.sha256(index_manifest.encode("utf-8")).hexdigest()
    _write_receipt(receipt, payload)
    if compressed > max_compressed_bytes:
        raise RegistryFootprintError(
            f"compressed image layers are {compressed} bytes; limit is {max_compressed_bytes} bytes"
        )
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--max-compressed-bytes", required=True, type=int)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--platform", default="linux/amd64")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.max_compressed_bytes <= 0:
        print("IMAGE_FOOTPRINT_ERROR --max-compressed-bytes must be positive", file=sys.stderr)
        return 2
    try:
        result = inspect_registry_image(
            image=args.image,
            max_compressed_bytes=args.max_compressed_bytes,
            receipt=args.receipt,
            platform=args.platform,
        )
    except (RegistryFootprintError, OSError, json.JSONDecodeError, subprocess.SubprocessError) as exc:
        print(f"IMAGE_FOOTPRINT_ERROR {exc}", file=sys.stderr)
        return 1
    print(f"IMAGE_FOOTPRINT_OK receipt={args.receipt}")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
