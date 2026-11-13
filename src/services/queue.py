"""Durable-queue operations over the ``repo_jobs`` table.

All functions take an ``AsyncSession`` and do NOT commit — the caller owns the
transaction boundary (FastAPI ``get_db`` commits on success; the worker commits
explicitly after each claim/transition). This keeps claim-and-update atomic.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from models.database import RepoJob


async def enqueue(
    session: AsyncSession,
    *,
    tenant_id: str,
    repo_url: str,
    package: str,
    target: str,
    ecosystem: str = "python",
    mode: str = "in-range",
    owner: str | None = None,
    correlation_id: str | None = None,
    max_attempts: int = 3,
) -> tuple[RepoJob, bool]:
    """Idempotently enqueue one intent for ``tenant_id``.

    Returns ``(job, created)``. ``created`` is False when an identical
    ``(tenant_id, repo_url, package, target)`` row already existed (dedup via
    the UNIQUE constraint + ``ON CONFLICT DO NOTHING``). Dedup is PER-TENANT:
    two tenants enqueuing the same triple get separate rows.
    """
    stmt = (
        pg_insert(RepoJob)
        .values(
            tenant_id=tenant_id,
            repo_url=repo_url,
            package=package,
            target=target,
            ecosystem=ecosystem,
            mode=mode,
            owner=owner,
            correlation_id=correlation_id,
            max_attempts=max_attempts,
        )
        .on_conflict_do_nothing(constraint="uq_repo_job_intent")
        .returning(RepoJob.id)
    )
    result = await session.execute(stmt)
    new_id = result.scalar_one_or_none()

    if new_id is not None:
        job = await session.get(RepoJob, new_id)
        if job is None:  # pragma: no cover - row just inserted in this txn
            raise RuntimeError(f"enqueue: inserted row {new_id} vanished")
        return job, True

    # Conflict: the row already exists FOR THIS TENANT — fetch and report it.
    existing = await session.scalar(
        select(RepoJob).where(
            RepoJob.tenant_id == tenant_id,
            RepoJob.repo_url == repo_url,
            RepoJob.package == package,
            RepoJob.target == target,
        )
    )
    if existing is None:  # pragma: no cover - conflict implies the row exists
        raise RuntimeError("enqueue: conflict but conflicting row not found")
    return existing, False


async def claim_next(session: AsyncSession) -> RepoJob | None:
    """Atomically claim the oldest queued job.

    Uses ``FOR UPDATE SKIP LOCKED`` so concurrent workers never hand the same
    row to two workers and never block on each other. Marks the row ``claimed``,
    increments ``attempts`` and stamps ``claimed_at``. The caller must commit.
    """
    claim_sql = text(
        """
        UPDATE repo_jobs
        SET status = 'claimed',
            attempts = attempts + 1,
            claimed_at = now(),
            updated_at = now()
        WHERE id = (
            SELECT id FROM repo_jobs
            WHERE status = 'queued' AND attempts < max_attempts
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id
        """
    )
    claimed_id = (await session.execute(claim_sql)).scalar_one_or_none()
    if claimed_id is None:
        return None
    return await session.get(RepoJob, claimed_id)


async def mark(
    session: AsyncSession,
    job_id: int,
    *,
    status: str,
    last_error: str | None = None,
    container_name: str | None = None,
    branch_name: str | None = None,
    pr_url: str | None = None,
    pr_number: int | None = None,
    is_cross_repository: bool | None = None,
    tenant_id: str | None = None,
) -> None:
    """Update mutable job fields. Only non-None kwargs are written.

    ``status`` is always written; the rest patch in place so a transition that
    only learns the container name doesn't clobber a previously-set pr_url.
    """
    values: dict[str, object] = {"status": status, "updated_at": datetime.now().astimezone()}
    for field, value in (
        ("last_error", last_error),
        ("container_name", container_name),
        ("branch_name", branch_name),
        ("pr_url", pr_url),
        ("pr_number", pr_number),
        ("is_cross_repository", is_cross_repository),
        ("tenant_id", tenant_id),
    ):
        if value is not None:
            values[field] = value
    await session.execute(update(RepoJob).where(RepoJob.id == job_id).values(**values))


async def requeue_or_kill(
    session: AsyncSession,
    job: RepoJob,
    *,
    error: str,
) -> str:
    """Transient-failure handler.

    Re-queues the job if attempts remain (so it becomes claimable again), else
    marks it ``dead``. Returns the resulting status. ``attempts`` was already
    incremented at claim time, so the comparison is against the value now in the
    row.
    """
    if job.attempts < job.max_attempts:
        new_status = "queued"
    else:
        new_status = "dead"
    await mark(session, job.id, status=new_status, last_error=error)
    return new_status


async def get_job(
    session: AsyncSession, job_id: int, *, tenant_id: str | None = None
) -> RepoJob | None:
    """Fetch one job by id.

    When ``tenant_id`` is given (the API path), a row owned by a DIFFERENT
    tenant returns ``None`` so the caller 404s instead of leaking it. The
    worker calls this with ``tenant_id=None`` to operate on rows it has already
    claimed globally.
    """
    if tenant_id is None:
        return await session.get(RepoJob, job_id)
    return await session.scalar(
        select(RepoJob).where(RepoJob.id == job_id, RepoJob.tenant_id == tenant_id)
    )


async def list_jobs(
    session: AsyncSession,
    *,
    tenant_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[RepoJob], int]:
    """Return ``(rows, total)`` for the dashboard, newest first.

    When ``tenant_id`` is given, results are scoped to that tenant only.
    """
    base = select(RepoJob)
    count_q = select(func.count()).select_from(RepoJob)
    if tenant_id is not None:
        base = base.where(RepoJob.tenant_id == tenant_id)
        count_q = count_q.where(RepoJob.tenant_id == tenant_id)
    if status is not None:
        base = base.where(RepoJob.status == status)
        count_q = count_q.where(RepoJob.status == status)
    total = (await session.execute(count_q)).scalar_one()
    rows = list(
        (
            await session.execute(
                base.order_by(RepoJob.created_at.desc()).limit(limit).offset(offset)
            )
        )
        .scalars()
        .all()
    )
    return rows, total


async def status_counts(session: AsyncSession, *, tenant_id: str | None = None) -> dict[str, int]:
    """Return a ``{status: count}`` map. Scoped to ``tenant_id`` when given."""
    stmt = select(RepoJob.status, func.count()).group_by(RepoJob.status)
    if tenant_id is not None:
        stmt = stmt.where(RepoJob.tenant_id == tenant_id)
    rows = await session.execute(stmt)
    return {status: count for status, count in rows.all()}
