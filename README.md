# coding-repo-jobs

L2 **bulk dependency-upgrade spine**: a durable `repo_jobs` queue + a stateless
worker that drives the *already-proven* `coding-containers` / `coding-security`
direct-push PR path (the `coding-github` #27/#29 routing) — **bypassing
n8n/Planka** so thousands of owned/org repos can be upgraded without a board per
job.

## What it does

One row in `repo_jobs` = one `(repo_url, package, target)` upgrade intent.
`UNIQUE (repo_url, package, target)` makes enqueue idempotent + resumable.

Worker per-job lifecycle:

```
queued → claimed → upgrading → verifying → pr_open    (oracle green: draft PR promoted to ready)
                                          → done       (no manifest change: already at target)
                              ↘ dead                   (upgrade unresolvable / oracle red / attempts exhausted)
```

The worker:
1. Provisions an ephemeral container from the repo (`coding-containers`).
2. Upgrades the package + commits + opens a **draft** PR via
   `coding-security /api/dependencies/{c}/upgrade[-major]` (which calls
   `coding-github`'s direct-push routing — no fork on owned/org repos).
3. Runs the **oracle**: in-container tests (`/exec`) + security scan
   (`/api/security/scan/{c}/quick`). The PR stays a draft until both pass.
4. Green → promotes the draft PR to ready (`mark-pull-request-ready`).
   Red → leaves the gated draft and marks the job `dead` with a reason.
5. Tears the container down (best-effort).

Claims use `FOR UPDATE SKIP LOCKED` so a worker fleet never double-processes a
row and never blocks on a peer.

## API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/repo-jobs` | Enqueue one intent (idempotent) |
| `POST` | `/api/repo-jobs/bulk` | Enqueue many (deduped) |
| `GET`  | `/api/repo-jobs` | List (`?status=&limit=&offset=`) |
| `GET`  | `/api/repo-jobs/stats` | Dashboard `{status: count}` |
| `GET`  | `/api/repo-jobs/{id}` | Inspect one row |
| `GET`  | `/health` | Liveness |

## Local dev

```bash
uv sync --all-extras
uv run ruff check . && uv run ruff format --check . && uv run ty check src
# With DATABASE_URL unset the suite starts ONE ephemeral docker postgres:18 on a
# random loopback port and removes it at session end -- no host Postgres needed.
uv run pytest                       # unit suite (-m 'not pg')
uv run pytest tests/integration -m pg   # Postgres-backed queue tests

# alembic has no default DSN; point it at a throwaway container:
cid=$(docker run --rm -d --tmpfs /var/lib/postgresql -p 127.0.0.1::5432 \
  -e POSTGRES_PASSWORD=postgres postgres:18)  # pragma: allowlist secret
export DATABASE_URL="postgresql+psycopg://postgres:postgres@$(docker port "$cid" 5432/tcp)/postgres"  # pragma: allowlist secret
uv run alembic upgrade head
docker rm -f "$cid"
```

Port: **8695**. Stack: FastAPI · SQLAlchemy async · psycopg v3 · alembic · uv.
