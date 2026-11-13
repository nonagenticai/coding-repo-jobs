"""Pydantic request/response schemas for the repo-jobs API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from models.database import RepoJob


class EnqueueJobRequest(BaseModel):
    """Enqueue one bulk dependency-upgrade intent."""

    repo_url: str = Field(..., min_length=1, max_length=1024)
    package: str = Field(..., min_length=1, max_length=256)
    target: str = Field(..., min_length=1, max_length=128)
    ecosystem: str = Field(default="python", max_length=32)
    mode: Literal["in-range", "cross-major"] = Field(default="in-range")
    owner: str | None = Field(default=None, max_length=256)
    correlation_id: str | None = Field(default=None, max_length=128)
    max_attempts: int = Field(default=3, ge=1, le=10)


class BulkEnqueueRequest(BaseModel):
    """Enqueue many intents in one call (the bulk spine entrypoint)."""

    jobs: list[EnqueueJobRequest] = Field(..., min_length=1, max_length=1000)


class JobResponse(BaseModel):
    """A repo_jobs row as returned by the API."""

    id: int
    repo_url: str
    package: str
    target: str
    ecosystem: str
    mode: str
    owner: str | None
    tenant_id: str | None
    correlation_id: str | None
    status: str
    attempts: int
    max_attempts: int
    last_error: str | None
    container_name: str | None
    branch_name: str | None
    pr_url: str | None
    pr_number: int | None
    is_cross_repository: bool | None
    claimed_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_orm_job(cls, job: RepoJob) -> JobResponse:
        return cls(
            id=job.id,
            repo_url=job.repo_url,
            package=job.package,
            target=job.target,
            ecosystem=job.ecosystem,
            mode=job.mode,
            owner=job.owner,
            tenant_id=job.tenant_id,
            correlation_id=job.correlation_id,
            status=job.status,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            last_error=job.last_error,
            container_name=job.container_name,
            branch_name=job.branch_name,
            pr_url=job.pr_url,
            pr_number=job.pr_number,
            is_cross_repository=job.is_cross_repository,
            claimed_at=job.claimed_at,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )


class EnqueueResult(BaseModel):
    """Per-intent enqueue outcome (idempotent: existing rows are reported, not re-created)."""

    id: int
    repo_url: str
    package: str
    target: str
    status: str
    created: bool = Field(
        description="True if a new row was inserted; False if it already existed."
    )


class BulkEnqueueResponse(BaseModel):
    enqueued: int = Field(description="Count of newly-created rows.")
    existing: int = Field(description="Count of intents that already existed (deduped).")
    results: list[EnqueueResult]


class JobListResponse(BaseModel):
    total: int
    jobs: list[JobResponse]


class StatusCounts(BaseModel):
    """Aggregate dashboard view over the queue."""

    counts: dict[str, int]
    total: int
