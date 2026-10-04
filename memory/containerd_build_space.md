# Containerd image export and retained build cache

Trigger: a Docker build passes its runtime checks but exhausts disk during image export or unpacking.

An image's displayed unique size is not the space released by retiring a group of related images.
Two unused images can share large layers with each other while sharing little with retained images.
After their tags are removed, BuildKit records can still retain the unpacked snapshots. Image and
cache accounting overlap, and containerd reclamation can be delayed.

Before a build, identify the live image and the usable rollback image, their matching configuration
and data identity, every container reference, and the current/previous deployment pins. Retiring a
protected recovery image needs an explicit owner decision. Do not infer authorization from its age.

For an approved cleanup, remove exact unused tags without force. Inspect the remaining cache again;
bound any further pruning to reviewed, reclaimable, unshared records. Record actual filesystem free
bytes before and after each operation. Never add overlapping image/cache estimates or temporary
build cleanup to persistent storage savings.

Measure free disk throughout the exact build and retain a reserve for the running application.
Passing compilation and runtime checks does not mean image export or unpacking has completed.
A Docker tar export may still need the same expensive layer-compression work before writing the
archive, so it is not evidence of a lower storage peak. Do not repeat an unchanged failed build.

Evidence: the 2026-10-04 operation's ignored `cleanup-retired-image-cache.log`,
`approved-historical-rollback-removal.log`, and `engine-build-a52fe952-retry3-status.log` under
`.cache/deploy-20261004/` separate approved retirement, remaining cache, and completed export.
The reusable protection rules live in `tools/box_docker_gc.py`; its dry run and filesystem
observations remain authoritative for each later operation. No universal free-space threshold or
reclaim amount is established by one successful build.
