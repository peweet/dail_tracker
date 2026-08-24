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
