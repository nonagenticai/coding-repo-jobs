ARG BASE_REGISTRY=docker.io

# --- Stage 1: Builder ---
FROM ${BASE_REGISTRY}/library/python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /uvx /bin/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./

# git is required because coding-client is a pinned GIT dependency (a public repository, not a
# package on an index). uv shells out to the git CLI for those; the slim base does not ship it.
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN uv sync --frozen --no-dev --no-install-project

# --- Stage 2: Production ---
FROM ${BASE_REGISTRY}/library/python:3.14-slim AS base
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /uvx /bin/

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    useradd -m -u 1000 appuser

WORKDIR /app
COPY --chown=1000:1000 --from=builder /app/uv.lock        /app/uv.lock
COPY --chown=1000:1000 --from=builder /app/pyproject.toml /app/pyproject.toml
COPY --chown=1000:1000 --from=builder /app/.venv          /app/.venv
COPY --chown=1000:1000 src/      ./src/
COPY --chown=1000:1000 alembic/  ./alembic/
COPY --chown=1000:1000 alembic.ini ./

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"
USER appuser

EXPOSE 8695

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8695/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8695"]
