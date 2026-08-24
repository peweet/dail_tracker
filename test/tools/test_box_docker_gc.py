"""Contract tests for tools/box_docker_gc.py -- the retention GC that runs on the LIVE
deployment host.

Why this file exists: the tool had already been run with --apply against
deploy@ubuntu-8gb-hel1-1 (removing 15 images) while carrying no tests at all. Everything
asserted here is a safety property its own docstring claims; each is pinned so widening
it breaks a test rather than a production rollback.

The retention rules are pure functions over dicts, so nearly all of this runs without a
Docker daemon. Only the ID-normalisation helpers touch Docker's actual output shapes, and
those are reproduced verbatim from real output captured 2026-08-24.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "tools"))

import box_docker_gc as gc  # noqa: E402

NOW = datetime(2026, 8, 24, 12, 0, 0, tzinfo=UTC)

# Real shapes, captured 2026-08-24 on the deployment host:
#   docker images  --format '{{.ID}}'    -> 670ee99a955d          (12 hex, no prefix)
#   docker rmi                            -> Deleted: sha256:670ee99a955d9361a221680add...
# A container's .Image is the FULL digest, which is why the two cannot be compared raw.
SHORT_ID = "670ee99a955d"
FULL_ID = "sha256:670ee99a955d9361a221680add498b3f73ce48e64ef344051a662f600933d12f"


def _image(repository: str, tag: str, *, age_days: float, image_id: str = "aaaaaaaaaaaa") -> dict:
    return {
        "repository": repository,
        "tag": tag,
        "ref": f"{repository}:{tag}",
        "tagged": True,
        "id": image_id,
        "created": NOW - timedelta(days=age_days),
        "size": "9.4GB",
    }


def _select(images, **overrides):
    kwargs = {
        "in_use": set(),
        "pinned": {},
        "keep": 1,
        "min_age_days": 0,
        "protected": (),
        "now": NOW,
    }
    kwargs.update(overrides)
    return gc.select_removable(images, **kwargs)


def _reclaim_kwargs(**overrides) -> dict:
    """A full, valid reclaim() call, so the argument-guard tests below assert the guard
    rather than a TypeError.

    Centralised deliberately: reclaim() takes every policy knob as a REQUIRED keyword, so
    adding one breaks every guard test with a TypeError at once -- which is how
    keep_max_age_days and disposable broke this file. One place to update instead of N.
    Values mirror the module defaults; both guards raise before in_use_image_ids(), so
    nothing here reaches a Docker daemon."""
    kwargs = {
        "apply_changes": False,
        "keep": 1,
        "keep_max_age_days": 30,
        "disposable": gc.DEFAULT_DISPOSABLE,
        "min_age_days": 3,
        "protected": (),
        "prune_cache": False,
        "cache_keep_hours": 168,
        "receipt": Path("unused.json"),
        "disk_path": "/",
    }
    kwargs.update(overrides)
    return kwargs


# ---------------------------------------------------------------- safety property 2


def test_image_used_by_a_container_is_never_removable():
    """Docstring safety property 2. The container's .Image is the full sha256 digest
    while docker images reports 12 chars, so this only holds if the comparison
    normalises. An old image under a long-running container is the real-world case:
    it is outside the newest --keep and past --min-age-days, so NOTHING else protects it."""
    old = _image("redline-review-engine", "git-ancient", age_days=90, image_id=SHORT_ID)
    newer = _image("redline-review-engine", "git-current", age_days=1)
    removable, retained = _select([old, newer], in_use={FULL_ID}, keep=1, min_age_days=3)

    assert old["ref"] not in [image["ref"] for image in removable], (
        "an image held by a container was selected for removal -- safety property 2 is not enforced"
    )
    assert any(r["ref"] == old["ref"] and "in use" in r["retained_because"] for r in retained)


def test_in_use_match_is_not_a_substring_coincidence():
    """Normalising must not go so far that an unrelated image whose short ID merely
    appears somewhere inside a digest is treated as in use."""
    unrelated = _image("repo", "tag", age_days=90, image_id="ffffffffffff")
    # A second, newer image so `unrelated` is NOT retained merely for being newest-of-repo;
    # that leaves the in-use rule as the only thing that could protect it.
    newer = _image("repo", "newer", age_days=0)
    removable, _ = _select([unrelated, newer], in_use={FULL_ID}, keep=1, min_age_days=3)
    assert unrelated["ref"] in [image["ref"] for image in removable], (
        "an unrelated ID was treated as in-use -- the digest match is too loose"
    )


# ---------------------------------------------------------------- other retention rules


def test_pinned_env_refs_are_retained():
    """A ref pinned in pilot.env is a deploy-time dependency even with no container on it.
    Removing one is latent breakage: it surfaces only at the next compose up."""
    pinned_image = _image("redline-review-engine", "git-952965b", age_days=90)
    other = _image("redline-review-engine", "git-new", age_days=0)
    _, retained = _select(
        [pinned_image, other],
        pinned={"redline-review-engine:git-952965b": ["/srv/redline-review/config/pilot.env"]},
        keep=1,
        min_age_days=3,
    )
    reasons = {r["ref"]: r["retained_because"] for r in retained}
    assert "pinned by pilot.env" in reasons["redline-review-engine:git-952965b"]


def test_newest_keep_per_repository_survive():
    """Rollback target always survives: --keep newest tags per repository.

    Ages stay INSIDE the --keep-max-age-days horizon on purpose. A keep-N slot is only
    granted to an image still young enough to be a plausible rollback target, so a
    fixture built from 30-day-old images would exercise the horizon rule below rather
    than this one and pass or fail for the wrong reason."""
    images = [_image("engine", f"git-{i}", age_days=1 + i) for i in range(5)]
    removable, retained = _select(images, keep=3, min_age_days=0)
    assert len(retained) == 3
    assert len(removable) == 2
    # The two oldest go.
    assert {image["tag"] for image in removable} == {"git-3", "git-4"}


def test_keep_slot_is_not_granted_past_the_rollback_horizon():
    """An image older than --keep-max-age-days does not earn a keep-N slot.

    Without this, a repository whose newest tags are all ancient keeps them forever
    purely for being newest -- the observed deployment-host case where 12-13 day old
    experiment tags survived while the only real release sat below them."""
    ancient = [_image("engine", f"git-{i}", age_days=60 + i) for i in range(3)]
    removable, retained = _select(ancient, keep=3, min_age_days=3, keep_max_age_days=30)
    assert retained == [], "an image past the rollback horizon took a keep-N slot"
    assert len(removable) == 3


def test_disposable_tag_shapes_never_earn_a_keep_slot():
    """Experiment/rehearsal builds are not rollback targets, so being newest-of-repo
    must not protect them -- while a real git-<sha> release in the same repository does
    take the slot."""
    rehearsal = _image("engine", "rehearsal-879b268", age_days=1)
    release = _image("engine", "git-8889022", age_days=5)
    removable, retained = _select([rehearsal, release], keep=1, min_age_days=0)
    assert [image["ref"] for image in removable] == [rehearsal["ref"]]
    assert [r["ref"] for r in retained] == [release["ref"]]


def test_min_age_days_floor_beats_policy():
    """An image younger than --min-age-days is never removed even when outside --keep."""
    images = [_image("engine", f"git-{i}", age_days=1) for i in range(4)]
    removable, _ = _select(images, keep=1, min_age_days=3)
    assert removable == []


def test_latest_tag_is_protected_by_default():
    assert gc.DEFAULT_PROTECTED == (r":latest$",)
    old_latest = _image("dailtracker-web", "latest", age_days=90)
    newer = _image("dailtracker-web", "git-new", age_days=0)
    _, retained = _select([old_latest, newer], keep=1, min_age_days=3, protected=gc.DEFAULT_PROTECTED)
    reasons = {r["ref"]: r["retained_because"] for r in retained}
    assert reasons["dailtracker-web:latest"] == "protected pattern"


# ---------------------------------------------------------------- argument guards


def test_keep_below_one_is_refused():
    """keep=0 would delete every tag of a repository, destroying the rollback target."""
    with pytest.raises(gc.DockerGCError, match="rollback target"):
        gc.reclaim(**_reclaim_kwargs(keep=0, min_age_days=3))


def test_negative_min_age_is_refused():
    with pytest.raises(gc.DockerGCError):
        gc.reclaim(**_reclaim_kwargs(keep=1, min_age_days=-1))


def test_apply_defaults_to_false():
    """Dry-run is safety property 1: deleting must require an explicit --apply."""
    assert gc.parse_args([]).apply is False
    assert gc.parse_args(["--apply"]).apply is True


# ---------------------------------------------------------------- parsing helpers


def test_parses_docker_created_at_format():
    parsed = gc._parse_docker_time("2026-08-11 03:33:12 +0000 UTC")
    assert parsed == datetime(2026, 8, 11, 3, 33, 12, tzinfo=UTC)


def test_unparseable_timestamp_raises_rather_than_defaulting():
    """A timestamp that silently became 'now' or 'epoch' would corrupt the age policy."""
    with pytest.raises(gc.DockerGCError):
        gc._parse_docker_time("not a timestamp")


def test_digest_pinned_ref_resolves_against_truncated_id():
    """repo@sha256:<64hex> never matches a 12-char ID textually; prefix matching is what
    stops the warning firing on healthy digest-pinned images."""
    known_hexes = {SHORT_ID}
    assert gc._pin_resolves(f"ghcr.io/peweet/web@sha256:{FULL_ID.removeprefix('sha256:')}", set(), known_hexes)
    assert not gc._pin_resolves("ghcr.io/peweet/web@sha256:deadbeef" + "0" * 56, set(), known_hexes)


def test_receipt_is_written_atomically(tmp_path):
    receipt = tmp_path / "nested" / "docker-gc-status.json"
    assert gc._write_receipt(receipt, {"mode": "DRY-RUN"}) is True
    assert json.loads(receipt.read_text(encoding="utf-8"))["mode"] == "DRY-RUN"


def test_receipt_failure_is_not_fatal(tmp_path):
    """An unwritable receipt path must not abort a GC run."""
    blocker = tmp_path / "afile"
    blocker.write_text("x", encoding="utf-8")
    assert gc._write_receipt(blocker / "cannot" / "exist.json", {"mode": "DRY-RUN"}) is False
