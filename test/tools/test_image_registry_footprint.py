"""Pure contracts for registry-image footprint accounting."""

from __future__ import annotations

import json

from tools.image_registry_footprint import (
    RegistryFootprintError,
    manifest_metrics,
    select_platform_manifest,
)


def test_manifest_metrics_sums_compressed_layer_sizes() -> None:
    raw = json.dumps({"schemaVersion": 2, "layers": [{"size": 7}, {"size": 11}]})

    metrics = manifest_metrics(raw)

    assert metrics["compressed_layer_bytes"] == 18
    assert metrics["layer_count"] == 2
    assert metrics["manifest_bytes"] == len(raw.encode("utf-8"))


def test_manifest_metrics_rejects_an_image_index_without_platform_layers() -> None:
    raw = json.dumps({"schemaVersion": 2, "manifests": [{"size": 99}]})

    try:
        manifest_metrics(raw)
    except RegistryFootprintError as exc:
        assert "single-platform" in str(exc)
    else:
        raise AssertionError("image index must not be accepted as a layer-byte measurement")


def test_index_selection_uses_the_linux_amd64_image_not_its_attestation() -> None:
    payload = {
        "schemaVersion": 2,
        "manifests": [
            {
                "digest": "sha256:image",
                "platform": {"os": "linux", "architecture": "amd64"},
            },
            {
                "digest": "sha256:attestation",
                "platform": {"os": "unknown", "architecture": "unknown"},
            },
        ],
    }

    assert select_platform_manifest(payload, "linux/amd64") == "sha256:image"
