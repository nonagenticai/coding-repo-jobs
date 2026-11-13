"""The stateless bulk-upgrade worker.

Runs ``worker_concurrency`` claim loops as asyncio tasks. Each loop atomically
claims one queued job (``FOR UPDATE SKIP LOCKED``), drives it through
:class:`UpgradeOrchestrator`, and persists the terminal outcome. Transient
failures re-queue (bounded by ``max_attempts``); permanent failures and the
oracle-red verdict mark the job ``dead`` with a reason.

It is "stateless" in that all durable state lives in ``repo_jobs`` — restarting
the process resumes cleanly from whatever rows are ``queued``/re-queued.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

import httpx
from coding_client.clients import CodingWorkspacesClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config.settings import Settings
from models.database import RepoJob
from services import queue
from services.orchestrator import (
    PermanentError,
    TransientError,
    UpgradeOrchestrator,
)

logger = logging.getLogger(__name__)


class Worker:
    """Owns the claim loops + a shared HTTP client."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        settings: Settings,
        client: httpx.AsyncClient | None = None,
        workspaces_client: CodingWorkspacesClient | None = None,
    ) -> None:
        self._sm = sessionmaker
        self._settings = settings
        self._client = client or httpx.AsyncClient(timeout=settings.http_timeout_seconds)
        self._owns_client = client is None
        # ``client`` still carries the coding-security / coding-github calls. The
        # /api/workspaces calls now go through the typed client, which builds its
        # own transport — so it needs its own injection seam or tests would dial
        # the real in-cluster host. ``None`` in production: the orchestrator then
        # constructs (and closes) per-budget clients itself.
        self._workspaces_client = workspaces_client
        self._tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()

    # ── lifecycle ─────────────────────────────────────────────────────────
    def start(self) -> None:
        if self._tasks:
            return
        self._stop.clear()
        for i in range(max(1, self._settings.worker_concurrency)):
            self._tasks.append(asyncio.create_task(self._loop(i), name=f"repo-jobs-worker-{i}"))
        logger.info("started %d worker loop(s)", len(self._tasks))

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        if self._owns_client:
            await self._client.aclose()

    # ── claim loop ────────────────────────────────────────────────────────
    async def _loop(self, idx: int) -> None:
        while not self._stop.is_set():
            try:
                claimed = await self._claim_one()
            except Exception:  # pragma: no cover - defensive; DB blip
                logger.exception("worker %d: claim failed", idx)
                await self._sleep(self._settings.worker_poll_interval_seconds)
                continue
            if claimed is None:
                await self._sleep(self._settings.worker_poll_interval_seconds)
                continue
            await self.process_job(claimed)

    async def _claim_one(self) -> RepoJob | None:
        async with self._sm() as session:
            job = await queue.claim_next(session)
            await session.commit()
            return job

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    # ── per-job processing ────────────────────────────────────────────────
    async def process_job(self, job: RepoJob) -> str:
        """Drive one claimed job to a terminal state. Returns the final status."""
        orch = UpgradeOrchestrator(
            self._client,
            containers_url=self._settings.containers_url,
            security_url=self._settings.security_url,
            github_url=self._settings.github_url,
            tenant_id=self._settings.worker_tenant_id,
            # coding-github gates platform-owned repositories to the platform
            # tenants; the worker's operator tenant is not one of them.
            platform_repo_owner=self._settings.platform_repo_owner,
            platform_tenant_id=self._settings.platform_tenant_id,
            upgrade_timeout_seconds=self._settings.upgrade_timeout_seconds,
            test_timeout_seconds=self._settings.test_timeout_seconds,
            container_ready_timeout_seconds=self._settings.container_ready_timeout_seconds,
            container_ready_poll_seconds=self._settings.container_ready_poll_seconds,
            # The workspace calls now go through coding_client's typed
            # CodingWorkspacesClient, which builds its own httpx client with a
            # timeout fixed at construction (library default: 30s — far below
            # the test/upgrade budgets). Hand it the same transport budget the
            # shared httpx client carried so those calls are unchanged.
            http_timeout_seconds=self._settings.http_timeout_seconds,
            workspaces_client=self._workspaces_client,
        )

        async def on_step(status: str, **fields: object) -> None:
            async with self._sm() as session:
                await queue.mark(session, job.id, status=status, **fields)
                await session.commit()

        try:
            outcome = await orch.run(
                repo_url=job.repo_url,
                package=job.package,
                target=job.target,
                ecosystem=job.ecosystem,
                mode=job.mode,
                job_id=job.id,
                on_step=on_step,
            )
        except TransientError as exc:
            return await self._handle_transient(job, str(exc))
        except PermanentError as exc:
            return await self._terminal(job.id, "dead", last_error=str(exc))
        except Exception as exc:  # unexpected → dead with reason (never lose the job)
            logger.exception("worker: unexpected failure on job %d", job.id)
            return await self._terminal(job.id, "dead", last_error=f"unexpected: {exc}")
        finally:
            # One orchestrator per job, and it owns the workspaces clients' own
            # httpx connection pools — leaking one pool per job would exhaust
            # sockets long before the process restarts.
            await orch.aclose()

        return await self._terminal(
            job.id,
            outcome.status,
            container_name=outcome.container_name,
            branch_name=outcome.branch_name,
            pr_url=outcome.pr_url,
            pr_number=outcome.pr_number,
            is_cross_repository=outcome.is_cross_repository,
        )

    async def _terminal(self, job_id: int, status: str, **fields: object) -> str:
        async with self._sm() as session:
            await queue.mark(session, job_id, status=status, **fields)
            await session.commit()
        return status

    async def _handle_transient(self, job: RepoJob, error: str) -> str:
        async with self._sm() as session:
            fresh = await queue.get_job(session, job.id)
            if fresh is None:  # pragma: no cover - row vanished
                return "dead"
            status = await queue.requeue_or_kill(session, fresh, error=error)
            await session.commit()
        await self._sleep(self._settings.retry_backoff_seconds)
        return status
