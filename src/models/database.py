"""SQLAlchemy ORM models for coding-repo-jobs.

A single table, ``repo_jobs``, is the durable bulk-upgrade queue. One row =
one (tenant, repo, package, target) upgrade intent. ``UNIQUE (tenant_id,
repo_url, package, target)`` gives per-tenant idempotent enqueue +
resumability: re-enqueuing the same intent for the same tenant is a no-op, and
a worker that dies mid-job leaves a claimable row behind. The dedup key is
tenant-scoped so two tenants enqueuing the same triple get SEPARATE rows and
never read or collide on each other's jobs.

The migrations ``alembic/versions/0001_repo_jobs_baseline.py``,
``0002_tenant_scoped_dedup_and_notnull.py`` and
``0003_tenant_id_is_a_uuid.py`` mirror this module exactly.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


# ── Lifecycle states ──────────────────────────────────────────────────────
# queued     -> ready to be claimed
# claimed    -> a worker has reserved it (attempts incremented, claimed_at set)
# upgrading  -> container provisioned, dependency upgrade running
# verifying  -> upgrade applied + draft PR opened, oracle gate running
# pr_open    -> oracle passed, draft PR flipped to ready-for-review (terminal-success)
# done       -> alias terminal-success for jobs that need no PR (reserved)
# dead       -> permanently failed (attempts exhausted or non-retryable) + last_error
TERMINAL_STATUSES = ("pr_open", "done", "dead")
CLAIMABLE_STATUSES = ("queued",)


class RepoJob(Base):
    """A single bulk dependency-upgrade job."""

    __tablename__ = "repo_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # ── Intent (the dedup key) ────────────────────────────────────────────
    repo_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    package: Mapped[str] = mapped_column(String(256), nullable=False)
    target: Mapped[str] = mapped_column(String(128), nullable=False)
    ecosystem: Mapped[str] = mapped_column(String(32), nullable=False, default="python")
    # 'in-range' (POST coding-security /upgrade) or 'cross-major' (/upgrade-major)
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="in-range")

    # ── Routing / identity ────────────────────────────────────────────────
    owner: Mapped[str | None] = mapped_column(String(256), nullable=True)
    # Owning tenant — part of the dedup key and every read filter. NOT NULL:
    # an un-attributed job is a cross-tenant leak/collision hazard (S5).
    #
    # Real postgres ``uuid`` (fleet standard: a tenant id IS a uuid, so the
    # database — not a convention — refuses a malformed one). ``as_uuid=False``
    # is load-bearing and deliberate: it keeps the PYTHON side a ``str``, so the
    # ``Mapped[str]`` annotation above, ``JobResponse.tenant_id: str | None``,
    # the ``RepoJob.tenant_id == tenant_id`` filters against the request-scoped
    # header string, and every JSON body all stay exactly as they were.
    # ``as_uuid=True`` would hand callers ``uuid.UUID`` objects and silently
    # break string comparison and serialisation at the edges.
    tenant_id: Mapped[str] = mapped_column(PG_UUID(as_uuid=False), nullable=False)
    # Per-job GitHub token id (resolved from the worker's token source). The
    # plaintext token is never persisted — only a reference/label.
    token_ref: Mapped[str | None] = mapped_column(String(256), nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # ── Lifecycle ─────────────────────────────────────────────────────────
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Work products ─────────────────────────────────────────────────────
    container_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    branch_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    pr_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    pr_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # The DIRECT-vs-fork proof: False == same-repo PR (the #27/#29 routing).
    is_cross_repository: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # ── Timestamps ────────────────────────────────────────────────────────
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        # Tenant-scoped dedup: the same (repo_url, package, target) is a distinct
        # intent for a different tenant, so tenant_id leads the unique key.
        UniqueConstraint("tenant_id", "repo_url", "package", "target", name="uq_repo_job_intent"),
        Index("idx_repo_job_status", "status"),
        Index("idx_repo_job_status_created", "status", "created_at"),
        Index("idx_repo_job_owner", "owner"),
        # Every API read is tenant-scoped — index the filter column.
        Index("idx_repo_job_tenant", "tenant_id"),
    )
