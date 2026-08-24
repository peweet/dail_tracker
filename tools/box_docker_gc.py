"""Retention-based Docker garbage collection for the Linux deployment host.

This is the Linux counterpart to ``tools/docker_gc.py``. That module targets the
Windows/WSL2 development laptop, where the dominant problem is a ``docker_data.vhdx``
that never shrinks. This module targets a plain Linux Docker host on ext4, where
deleted bytes *are* returned to the filesystem and the dominant problem is instead an
unbounded accumulation of superseded release tags.

Why a retention policy rather than ``docker image prune -a``: on the deployment host the
release image tags are **local-only** — they exist in no registry, so a blanket prune
destroys the ability to roll back to a previous release. ``prune -a`` keeps only images
backing a running container, which is exactly the wrong retention set. This tool instead
keeps the newest ``--keep`` tags per repository plus everything any container references,
and only then removes what is left.

Safety properties, in order of importance:

1. Dry-run is the default. Deleting requires an explicit ``--apply``.
2. An image referenced by any container -- running or stopped -- is never removed.
3. Images newer than ``--min-age-days`` are never removed, regardless of policy.
4. The newest ``--keep`` tags of every repository are never removed, so a rollback
   target always survives.
5. Every run writes a JSON receipt so an unattended invocation can be proven to have
   happened, and to have done what it claimed.

Build cache is handled separately from images because cache shared with a live image
cannot be reclaimed until that image goes; pruning cache first and images second would
under-report what the image pass actually released.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

#: Separator used in ``docker images --format``. Chosen because it cannot occur in a
#: repository name, a tag, an image ID or a Docker timestamp.
FIELD_SEP = "\x1f"

#: Repository/tag pairs that must never be removed even when policy would allow it.
#: ``latest`` is protected because compose files and humans both reach for it by habit.
DEFAULT_PROTECTED = (r":latest$",)

#: Tag shapes that never earn a keep-N rollback slot. These are experiment, rehearsal and
#: dry-run builds; a real release is ``git-<sha>`` or a registry digest. Matching here only
#: disqualifies an image from keep-N -- an image that is in use or pinned by a deployment
#: env file is still protected absolutely.
DEFAULT_DISPOSABLE = (r":pilot-", r":rehearsal-", r":remote-dryrun-", r":hotfix-")


class DockerGCError(RuntimeError):
    """Docker could not be queried, or a policy argument was invalid."""


def _run(command: Sequence[str], *, check: bool = True) -> str:
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if check and completed.returncode:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no diagnostic"
        raise DockerGCError(f"{' '.join(command)}: {detail}")
    return completed.stdout


def _parse_docker_time(raw: str) -> datetime:
    """Parse Docker's ``CreatedAt`` form, e.g. ``2026-08-11 03:33:12 +0000 UTC``."""

    cleaned = re.sub(r"\s+[A-Z]{2,4}$", "", raw.strip())
    try:
        return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S %z")
    except ValueError as exc:
        raise DockerGCError(f"unparseable image timestamp {raw!r}") from exc


def in_use_image_ids() -> set[str]:
    """Return the image ID of every container on the host, running or not.

    Stopped containers matter: a stopped container still pins its image, and removing
    that image would make the container unrestartable.
    """

    raw = _run(["docker", "ps", "-aq"]).split()
    if not raw:
        return set()
    inspected = _run(["docker", "inspect", "--format", "{{.Image}}", *raw], check=False)
    return {line.strip() for line in inspected.splitlines() if line.strip()}


#: Deployment env files consulted for pinned image references. A ref named here is a
#: deploy-time dependency even when no container currently runs it.
DEFAULT_ENV_FILES = (
    Path("/srv/redline-review/config/pilot.env"),
    Path("/srv/redline-review/config/pilot.previous.env"),
)

#: Matches ``SITING_ENGINE_IMAGE=repo:tag`` and every sibling ``*IMAGE=`` assignment.
_IMAGE_ASSIGNMENT = re.compile(r"^[A-Z0-9_]*IMAGE=(.+)$")


def pinned_image_refs(env_files: Iterable[Path]) -> dict[str, list[str]]:
    """Return ``{image_ref: [env files that pin it]}`` for deployment env files.

    A retention policy that ignores these deletes the image a deploy is *configured*
    to use, which is invisible until the next deploy fails. That is not hypothetical:
    on 2026-08-24 an earlier version of this tool removed ``redline-review-engine:
    git-952965b`` while both ``pilot.env`` and ``pilot.previous.env`` still pinned it.
    Nothing broke at the time, because a running container holds its image by ID -- the
    breakage was latent, and would have surfaced only at the next ``compose up``, with
    no registry copy to re-pull from.

    Only the assignment is read. Values are never logged by callers, because these files
    also carry API keys and tunnel tokens on adjacent lines.
    """

    pinned: dict[str, list[str]] = {}
    for env_file in env_files:
        try:
            content = env_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in content.splitlines():
            match = _IMAGE_ASSIGNMENT.match(line.strip())
            if not match:
                continue
            ref = match.group(1).strip().strip('"').strip("'")
            if ref:
                pinned.setdefault(ref, []).append(str(env_file))
    return pinned


def list_images() -> list[dict[str, Any]]:
    """Return every tagged image as a dict. Untagged images are handled by prune."""

    template = FIELD_SEP.join(("{{.Repository}}", "{{.Tag}}", "{{.ID}}", "{{.CreatedAt}}", "{{.Size}}"))
    raw = _run(["docker", "images", "--format", template])
    images: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        parts = line.split(FIELD_SEP)
        if len(parts) != 5:
            continue
        repository, tag, image_id, created, size = parts
        # A fully untagged image (<none>:<none>) is genuinely dangling and is left to
        # `docker image prune`. An image that has a repository but no tag is a different
        # animal: a digest-pinned registry pull, which `docker image prune` deliberately
        # SKIPS because it is not dangling. Those accumulate forever unless the retention
        # policy reaches them by ID -- and they are the safest images to remove, because
        # unlike a local-only release tag they can always be re-pulled from the registry.
        if repository == "<none>":
            continue
        tagged = tag != "<none>"
        images.append(
            {
                "repository": repository,
                "tag": tag,
                "ref": f"{repository}:{tag}" if tagged else image_id,
                "tagged": tagged,
                "id": image_id,
                "created": _parse_docker_time(created),
                "size": size,
            }
        )
    return images


def select_removable(
    images: Iterable[dict[str, Any]],
    *,
    in_use: set[str],
    pinned: dict[str, list[str]] | None = None,
    keep: int,
    keep_max_age_days: int = 30,
    disposable: Sequence[str] = DEFAULT_DISPOSABLE,
    min_age_days: int,
    protected: Sequence[str],
    now: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split images into (removable, retained) under the retention policy.

    The policy is deliberately conservative: an image survives if ANY rule protects it.
    """

    protected_patterns = [re.compile(pattern) for pattern in protected]
    disposable_patterns = [re.compile(pattern) for pattern in disposable]
    cutoff = now - timedelta(days=min_age_days)
    keep_horizon = now - timedelta(days=keep_max_age_days)

    by_repository: dict[str, list[dict[str, Any]]] = {}
    for image in images:
        by_repository.setdefault(image["repository"], []).append(image)

    removable: list[dict[str, Any]] = []
    retained: list[dict[str, Any]] = []

    for repository_images in by_repository.values():
        ordered = sorted(repository_images, key=lambda item: item["created"], reverse=True)
        # A keep-N slot exists to preserve a plausible ROLLBACK TARGET, so it is not
        # granted unconditionally. Two things disqualify an image from taking one: an
        # experiment-shaped tag, which nobody rolls back to, and age past the rollback
        # horizon. Without this, a repository whose newest images are all ancient junk
        # keeps three pieces of junk forever -- observed on the deployment host, where
        # 12-13 day old `pilot-2026...` tags survived purely by being newest in their
        # repository while the only real release sat below them.
        #
        # Disqualification removes ONLY the keep-N protection. An image that is in use or
        # pinned by a deployment env file is still retained by the rules below, so this
        # cannot delete something a deploy depends on.
        eligible = [
            image
            for image in ordered
            if image["created"] > keep_horizon
            and not any(pattern.search(image["ref"]) for pattern in disposable_patterns)
        ]
        newest_refs = {image["ref"] for image in eligible[:keep]}
        for image in ordered:
            reason = None
            pin_sources = (pinned or {}).get(image["ref"]) or (pinned or {}).get(image["id"])
            if pin_sources:
                reason = "pinned by " + ", ".join(Path(source).name for source in pin_sources)
            elif _id_in_use(image["id"], in_use):
                reason = "in use by a container"
            elif image["ref"] in newest_refs:
                reason = f"among newest {keep} of {image['repository']}"
            elif image["created"] > cutoff:
                reason = f"newer than {min_age_days}d"
            elif any(pattern.search(image["ref"]) for pattern in protected_patterns):
                reason = "protected pattern"
            if reason:
                retained.append({**image, "retained_because": reason})
            else:
                removable.append(image)

    removable.sort(key=lambda item: item["created"])
    return removable, retained


def _id_in_use(image_id: str, in_use: set[str]) -> bool:
    """Whether ``image_id`` names an image some container holds.

    The two sides arrive in different shapes and CANNOT be compared directly:
    ``docker images --format {{.ID}}`` yields a 12-character hex ID (``670ee99a955d``)
    while a container's ``.Image`` is the full digest
    (``sha256:670ee99a955d9361a221680add...``). A raw ``in`` test therefore never matches
    and silently disables safety property 2 -- images held by containers become removable.
    That was live for the run of 2026-08-24; `docker rmi` refusing to delete a referenced
    image was the only thing standing between the policy and an unrestartable container,
    and it would have surfaced as an opaque failure rather than a retention decision.

    Matching is anchored with ``startswith`` on the hex, mirroring ``_pin_resolves``: the
    short ID is a genuine prefix of the digest, so a prefix test is exact here, whereas a
    substring test would match an unrelated ID appearing anywhere inside a digest.
    """

    bare = image_id.removeprefix("sha256:")
    if not bare:
        return False
    return any(used.removeprefix("sha256:").startswith(bare) for used in in_use if used)


def _pin_resolves(ref: str, known_refs: set[str], known_id_hexes: set[str]) -> bool:
    """Whether a pinned reference matches an image actually present on this host.

    Pins appear in three shapes: ``repo:tag``, a bare image ID, and ``repo@sha256:<hex>``.
    Docker reports image IDs truncated to twelve characters, so the digest form never
    matches textually and a naive membership test reports every digest-pinned image as
    missing. That precision matters more than it looks: a warning that fires on four
    healthy refs for every real one is a warning nobody reads, which is the same outcome
    as having no check at all.
    """

    if ref in known_refs:
        return True
    _, separator, digest = ref.partition("@sha256:")
    if not separator:
        return False
    return any(digest.startswith(id_hex) for id_hex in known_id_hexes if id_hex)


def _disk_free_bytes(path: str) -> int | None:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return None


def _write_receipt(path: Path, payload: dict[str, Any]) -> bool:
    """Write the receipt atomically. Returns False when the path is not writable."""

    rendered = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            handle.write(rendered)
            temporary = Path(handle.name)
        temporary.replace(path)
    except OSError as exc:
        print(f"DOCKER_GC_WARN could not write receipt to {path}: {exc}", file=sys.stderr)
        return False
    return True


def reclaim(
    *,
    apply_changes: bool,
    keep: int,
    keep_max_age_days: int,
    disposable: Sequence[str],
    min_age_days: int,
    protected: Sequence[str],
    prune_cache: bool,
    cache_keep_hours: int,
    receipt: Path,
    disk_path: str,
    env_files: Sequence[Path] = DEFAULT_ENV_FILES,
) -> int:
    if keep < 1:
        raise DockerGCError("--keep must be at least 1; keeping zero destroys the rollback target")
    if min_age_days < 0:
        raise DockerGCError("--min-age-days cannot be negative")

    now = datetime.now(UTC)
    before_free = _disk_free_bytes(disk_path)
    in_use = in_use_image_ids()
    pinned = pinned_image_refs(env_files)
    images = list_images()
    removable, retained = select_removable(
        images,
        in_use=in_use,
        pinned=pinned,
        keep=keep,
        keep_max_age_days=keep_max_age_days,
        disposable=disposable,
        min_age_days=min_age_days,
        protected=protected,
        now=now,
    )

    # A pin that already resolves to nothing is a broken deploy waiting to happen, and
    # it is invisible from the running stack because a live container holds its image by
    # ID. Surface it whether or not this run removes anything.
    known_refs = {image["ref"] for image in images} | {image["id"] for image in images}
    known_id_hexes = {image["id"].removeprefix("sha256:") for image in images}
    dangling_pins = sorted(ref for ref in pinned if not _pin_resolves(ref, known_refs, known_id_hexes))
    for ref in dangling_pins:
        sources = ", ".join(Path(source).name for source in pinned[ref])
        print(f"DOCKER_GC_WARN pinned image {ref} ({sources}) does not exist on this host", file=sys.stderr)

    mode = "APPLY" if apply_changes else "DRY-RUN"
    print(
        f"DOCKER_GC {mode} images={len(images)} in_use={len(in_use)} pinned={len(pinned)} "
        f"removable={len(removable)} retained={len(retained)} dangling_pins={len(dangling_pins)}"
    )

    for image in removable:
        age_days = (now - image["created"]).days
        print(f"  remove  {image['ref']:<62} {image['size']:>8}  {age_days}d")

    removed: list[str] = []
    failed: list[dict[str, str]] = []
    if apply_changes and removable:
        for image in removable:
            completed = subprocess.run(["docker", "rmi", image["ref"]], capture_output=True, text=True, check=False)
            if completed.returncode:
                failed.append({"ref": image["ref"], "error": completed.stderr.strip()})
            else:
                removed.append(image["ref"])

    dangling_removed = False
    if apply_changes:
        # Untagged layers left behind by rebuilds. Safe: prune without -a never touches
        # a tagged image, and Docker refuses to remove anything a container references.
        _run(["docker", "image", "prune", "-f"], check=False)
        dangling_removed = True

    cache_pruned = False
    if apply_changes and prune_cache:
        _run(["docker", "builder", "prune", "-f", "--filter", f"until={cache_keep_hours}h"], check=False)
        cache_pruned = True

    after_free = _disk_free_bytes(disk_path)
    payload = {
        "schema_version": 1,
        "ran_at": now.isoformat(),
        "mode": mode,
        "policy": {
            "keep_per_repository": keep,
            "keep_max_age_days": keep_max_age_days,
            "disposable": list(disposable),
            "min_age_days": min_age_days,
            "protected": list(protected),
            "cache_keep_hours": cache_keep_hours if prune_cache else None,
        },
        "images_total": len(images),
        "images_in_use": len(in_use),
        "pinned_refs": sorted(pinned),
        "dangling_pins": dangling_pins,
        "candidates": [image["ref"] for image in removable],
        "removed": removed,
        "failed": failed,
        "dangling_pruned": dangling_removed,
        "cache_pruned": cache_pruned,
        "disk_free_before": before_free,
        "disk_free_after": after_free,
        "disk_freed": (after_free - before_free) if (before_free is not None and after_free is not None) else None,
    }
    _write_receipt(receipt, payload)

    if failed:
        for failure in failed:
            print(f"DOCKER_GC_ERROR {failure['ref']}: {failure['error']}", file=sys.stderr)
        return 1

    if apply_changes and before_free is not None and after_free is not None:
        print(f"DOCKER_GC_OK removed={len(removed)} freed={(after_free - before_free) / 1e9:.2f}GB receipt={receipt}")
    else:
        print(f"DOCKER_GC_OK mode={mode} receipt={receipt}")
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually remove images; without it the run only reports what it would do",
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=3,
        help="tags to keep per repository, newest first (default 3: the live one plus two rollback targets)",
    )
    parser.add_argument(
        "--keep-max-age-days",
        type=int,
        default=30,
        help="a keep slot expires past this age, so a repository of nothing but ancient "
        "images stops hoarding them (default 30)",
    )
    parser.add_argument(
        "--disposable",
        action="append",
        metavar="REGEX",
        help="regex for tag shapes that never earn a keep slot -- experiment and rehearsal "
        "builds (repeatable; defaults to pilot-/rehearsal-/remote-dryrun-/hotfix-)",
    )
    parser.add_argument(
        "--min-age-days",
        type=int,
        default=3,
        help="never remove an image younger than this, whatever the policy says (default 3)",
    )
    parser.add_argument(
        "--protect",
        action="append",
        default=list(DEFAULT_PROTECTED),
        metavar="REGEX",
        help="regex matched against repository:tag; matches are never removed (repeatable)",
    )
    parser.add_argument("--no-cache-prune", action="store_true", help="leave the build cache alone")
    parser.add_argument(
        "--cache-keep-hours",
        type=int,
        default=168,
        help="prune build cache unused for longer than this many hours (default 168 = 7 days)",
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=Path("/srv/redline-review/monitoring/docker-gc-status.json"),
        help="where to write the JSON receipt",
    )
    parser.add_argument("--disk-path", default="/", help="filesystem to measure free space on")
    parser.add_argument(
        "--env-file",
        action="append",
        type=Path,
        metavar="PATH",
        help="deployment env file whose *IMAGE= pins must never be deleted "
        "(repeatable; defaults to the pilot env pair)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return reclaim(
            apply_changes=args.apply,
            keep=args.keep,
            keep_max_age_days=args.keep_max_age_days,
            disposable=args.disposable or list(DEFAULT_DISPOSABLE),
            min_age_days=args.min_age_days,
            protected=args.protect,
            prune_cache=not args.no_cache_prune,
            cache_keep_hours=args.cache_keep_hours,
            receipt=args.receipt,
            disk_path=args.disk_path,
            env_files=args.env_file or list(DEFAULT_ENV_FILES),
        )
    except (DockerGCError, OSError, subprocess.SubprocessError) as exc:
        print(f"DOCKER_GC_ERROR {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
