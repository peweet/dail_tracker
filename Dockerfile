# Dáil Tracker API — read-only JSON API + bulk exports over committed parquet.
#
# The image bakes the data in (the API is a read-only serve layer over the
# git-committed gold/silver files — same contract as Streamlit Cloud), so a
# data refresh ships as a new image build, never a runtime mutation. Build from
# a CLEAN CHECKOUT (CI / git archive): a local working tree carries multi-GB
# untracked bronze/silver artefacts that .dockerignore excludes by whitelist —
# if a view 503s in the container, check the whitelist first.
#
#   docker build -t dailtracker-api .
#   docker run -p 8080:8080 dailtracker-api
#
# Note: `uv sync` installs the core project deps (which include Streamlit — the
# app and API share one lockfile) plus the api extra. That costs ~150MB of
# unused app deps in exchange for lockfile fidelity; revisit only if image size
# ever matters.

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

# Dependency layer first so code/data changes don't bust the cache.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --extra api

# Code + the SQL view firewall + the committed data the views read.
COPY paths.py config.py ./
COPY api ./api
COPY dail_tracker_core ./dail_tracker_core
COPY sql_views ./sql_views
COPY data ./data

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s \
    CMD python -c "import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8080/v1/health', timeout=5); sys.exit(0 if r.status==200 else 1)"

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
