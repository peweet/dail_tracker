---
tier: RUNBOOK
status: LIVE
domain: infra
updated: 2026-08-24
supersedes: []
read_when: setting up Python, diagnosing dependency drift, using Docker, or changing a runtime profile
key: RUNBOOK|LIVE|infra
---

# Environment and Docker runbook

The repository uses a hybrid model. Python development runs in named `uv`
environments; reproducible verification runs in an isolated environment; Docker
is reserved for deployable services and Linux-runtime parity. One shared `.venv`
is not an environment contract.

## Environment ownership

| Environment | Owner and contents | Mutation rule |
| --- | --- | --- |
| `.uv-envs/public` | Public app, pipeline, API, MCP, and developer tools | Only `tools/dev_env.py sync public` may repair it |
| `.uv-envs/siting` | Public profile plus the private Siting runtime | Only `tools/dev_env.py sync siting` may repair it |
| `.uv-envs/siting-ai` | Siting profile plus optional OpenAI SDK, tracing and local tokenizer | Only `tools/dev_env.py sync siting-ai` may repair it |
| Isolated verification | Temporary locked public profile | Created by `uv run --isolated`; never changes a persistent environment |
| Dev container | Linux public profile in `/opt/dail-tracker-env` | Rebuilt from `.devcontainer/`; never reuses the host `.venv` |
| Scheduled job | A job-specific `UV_PROJECT_ENVIRONMENT` | The job owns its environment; it must not target an editor environment |

`pyproject.toml` expresses dependency intent and `uv.lock` contains the resolved
versions. `requirements.txt` is only the generated app-runtime export. Do not
install project packages manually with `pip`, and do not use bare `python` on
Windows: this machine may resolve it to a 32-bit interpreter.

## Bootstrap and normal use

```powershell
# Deliberate, exact mutations:
py -3.12 tools/dev_env.py sync public
py -3.12 tools/dev_env.py sync siting
py -3.12 tools/dev_env.py sync siting-ai

# Read-only state checks:
py -3.12 tools/dev_env.py check public
py -3.12 tools/dev_env.py doctor public

# Refuses to run when the selected environment is stale:
py -3.12 tools/dev_env.py run public python tools/dev.py verify
```

Invoke PowerShell automation with `pwsh -NoProfile -File ...`. Repository jobs
must not depend on a personal profile, and a profile execution-policy warning
must not be confused with a Docker or application failure.

Use the Siting profiles only for private work. `siting` keeps deterministic
planning development free of model transport dependencies; select `siting-ai`
only for the OpenAI/Claude edge and its local token diagnostics. Switching
profiles means changing the profile argument, not exact-syncing a different
package set into the same directory.

## Verification

Agent and handoff gates use an ephemeral environment:

```powershell
uv run --isolated --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py verify
uv run --isolated --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py check
```

If the user-profile cache is unavailable in a managed execution sandbox, add
`--no-cache`. A cache/permission error means dependency state was **not
evaluated**; it is not evidence that the lockfile is stale.

## Docker boundary

- The root `Dockerfile` is the public read-only API delivery artifact.
- `apps/public-signal/private-api/Dockerfile` is a separate tracked service with
  an independent small runtime and CI smoke test.
- `planning/product/Dockerfile` is private Siting delivery configuration and
  belongs in the `.git-siting` overlay/standalone private repository.
- The dev container is a development convenience, not a production image.
- ETL and large-layer interactive development stay on the host. Containerising
  those workflows would copy or bind large mutable data without improving their
  dependency contract.

Application images pin the Python base and `uv` image digests. Updating a digest
is a reviewed dependency change: rebuild the image, assert its non-root user,
start it, and wait for its health/readiness endpoint.

## Docker disk management

Four tools bound Docker's on-disk footprint in the three places it accumulates:
the Windows/WSL2 dev laptop, the Linux deployment host, and the image CI pushes.
Every one of them reports by default and mutates only when told to.

| Tool | Host | Default | Mutating flag |
| --- | --- | --- | --- |
| `tools/docker_gc.py` | Windows/WSL2 dev laptop | Report only | `--reclaim`; `--compact` (asks first) |
| `tools/register_docker_gc_task.ps1` | Windows dev laptop | Registers the weekly task | `-RunNow` starts it immediately |
| `tools/box_docker_gc.py` | Linux deployment host | Dry run | `--apply` |
| `tools/image_registry_footprint.py` | CI, after image push | Inspects and reports | Exits non-zero over `--max-compressed-bytes` |

**`tools/docker_gc.py`** exists because local Docker reached 80 images (104.3GB
reclaimable) and a 152.6GB `docker_data.vhdx` on a drive with 102GB free, entirely
from rebuilds leaving prior layers as dangling `<none>` images with nobody watching
between sessions. `--reclaim` runs three commands judged safe to run unattended:

- `docker image prune -a -f --filter until=24h` — any image, dangling or tagged, that
  no container references and nothing has touched for 24h.
- `docker builder prune -f --filter until=168h` — build cache older than a week.
- `docker volume prune -f` (no `-a`) — anonymous volumes only. A named-but-unused
  volume can hold real data with no container pointing at it, so it is left alone.

Add `--dry-run` to print those commands without running them. `--trim` runs `fstrim`
inside the Docker distro alone; it changes no data, needs no shutdown, and is a
prerequisite for compaction rather than an optional extra, because `compact vdisk`
can only return blocks the guest has already TRIMmed.

`--compact` is excluded from the schedule and prompts before acting (`--yes` skips
the prompt): it calls `wsl --shutdown`, which stops every WSL distro on the box, not
just Docker's. Keep it a deliberate, confirmed action.

**`tools/register_docker_gc_task.ps1`** registers `DailTracker-DockerGC`, a Scheduled
Task running `docker_gc.py --reclaim` on Sundays at 04:00 local, catching up after
sleep. It never registers `--compact`. Re-run it to update the registration, pass
`-RunNow` to start the task immediately, and remove it with:

```powershell
Unregister-ScheduledTask -TaskName 'DailTracker-DockerGC' -Confirm:$false
```

**`tools/box_docker_gc.py`** is the Linux deployment-host counterpart. Deleted bytes
return to the filesystem there, so the problem is not a vhdx that never shrinks but
unbounded accumulation of superseded release tags. It applies a retention policy
rather than `docker image prune -a` because those release tags are local-only — they
exist in no registry, so a blanket prune destroys the ability to roll back, and
`prune -a` keeps only images backing a running container, which is the wrong
retention set.

Dry run is the default; `--apply` is required to delete anything. An image any
container references, running or stopped, is never removed. `--min-age-days`
(default 3) floors how young an image can be, `--keep` (default 3) holds the newest
tags per repository as rollback targets, and `--protect` (default `:latest$`) is
absolute. `--env-file` reads deployment env files and treats every `*IMAGE=` pin as a
deploy-time dependency — a ref pinned there survives even with no container running
it, which is the guard against deleting a pinned image mid-rollback. Every run writes
a JSON receipt to `--receipt`, so an unattended invocation can be proven to have run
and to have done what it claimed. `--help` lists the remaining policy flags.

**`tools/image_registry_footprint.py`** runs in CI after a candidate image is pushed.
It reads the pushed manifest with `docker buildx imagetools inspect --raw`, sums the
compressed layer bytes, and fails when the total exceeds `--max-compressed-bytes`. It
never pulls the image or reads its contents. `--image`, `--max-compressed-bytes` and
`--receipt` are required; `--platform` defaults to `linux/amd64`.

## Failure classification

Diagnose the layer that failed before changing dependencies:

1. **Host/bootstrap:** wrong interpreter, 32-bit Python, PowerShell profile error,
   Docker/WSL unavailable.
2. **Tool infrastructure:** inaccessible `uv` cache, locked executable, network
   or registry failure.
3. **Dependency intent:** `pyproject.toml` and `uv.lock` disagree.
4. **Installed environment:** `uv sync --check` or `pip check` reports drift.
5. **Application:** imports, tests, health checks, or runtime contracts fail in a
   known-good environment.

Never convert failures in layers 1-2 into a claim about layers 3-5.

## Recovery

1. Run `tools/dev_env.py doctor <profile>`.
2. If it reports only environment drift, run the explicit matching `sync` once.
3. If cache access failed, repair permissions or retry with `--no-cache`; do not
   delete unrelated processes or environments.
4. If an isolated gate passes but the named environment fails, recreate only
   that named directory.
5. If Docker fails, first verify `docker context show`, `docker version`, WSL
   state, and container health. A managed sandbox's pipe denial is not proof that
   Docker Desktop is down.
