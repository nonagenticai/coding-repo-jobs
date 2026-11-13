"""Repo-jobs queue API: enqueue (idempotent), list, inspect, dashboard counts."""

from __future__ import annotations

from coding_client.auth.tenant import require_tenant
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from db.session import get_db
from models.schemas import (
    BulkEnqueueRequest,
    BulkEnqueueResponse,
    EnqueueJobRequest,
    EnqueueResult,
    JobListResponse,
    JobResponse,
    StatusCounts,
)
from services import queue

router = APIRouter(prefix="/api/repo-jobs", tags=["repo-jobs"])


async def _enqueue_one(
    session: AsyncSession, req: EnqueueJobRequest, tenant_id: str
) -> EnqueueResult:
    job, created = await queue.enqueue(
        session,
        tenant_id=tenant_id,
        repo_url=req.repo_url,
        package=req.package,
        target=req.target,
        ecosystem=req.ecosystem,
        mode=req.mode,
        owner=req.owner,
        correlation_id=req.correlation_id,
        max_attempts=req.max_attempts,
    )
    return EnqueueResult(
        id=job.id,
        repo_url=job.repo_url,
        package=job.package,
        target=job.target,
        status=job.status,
        created=created,
    )


@router.post("", status_code=status.HTTP_201_CREATED, response_model=EnqueueResult)
async def enqueue_job(
    req: EnqueueJobRequest,
    session: AsyncSession = Depends(get_db),
    tenant_id: str = Depends(require_tenant),
) -> EnqueueResult:
    """Enqueue a single intent. Idempotent on (tenant_id, repo_url, package, target)."""
    return await _enqueue_one(session, req, tenant_id)


@router.post("/bulk", response_model=BulkEnqueueResponse)
async def enqueue_bulk(
    req: BulkEnqueueRequest,
    session: AsyncSession = Depends(get_db),
    tenant_id: str = Depends(require_tenant),
) -> BulkEnqueueResponse:
    """Enqueue many intents. Existing intents are deduped per-tenant, not re-created."""
    results = [await _enqueue_one(session, j, tenant_id) for j in req.jobs]
    enqueued = sum(1 for r in results if r.created)
    return BulkEnqueueResponse(
        enqueued=enqueued,
        existing=len(results) - enqueued,
        results=results,
    )


@router.get("", response_model=JobListResponse)
async def list_jobs(
    session: AsyncSession = Depends(get_db),
    tenant_id: str = Depends(require_tenant),
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> JobListResponse:
    rows, total = await queue.list_jobs(
        session, tenant_id=tenant_id, status=status_filter, limit=limit, offset=offset
    )
    return JobListResponse(total=total, jobs=[JobResponse.from_orm_job(r) for r in rows])


@router.get("/stats", response_model=StatusCounts)
async def stats(
    session: AsyncSession = Depends(get_db),
    tenant_id: str = Depends(require_tenant),
) -> StatusCounts:
    """Dashboard: a {status: count} map over the caller's tenant queue."""
    counts = await queue.status_counts(session, tenant_id=tenant_id)
    return StatusCounts(counts=counts, total=sum(counts.values()))


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: int,
    session: AsyncSession = Depends(get_db),
    tenant_id: str = Depends(require_tenant),
) -> JobResponse:
    job = await queue.get_job(session, job_id, tenant_id=tenant_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"job {job_id} not found")
    return JobResponse.from_orm_job(job)
