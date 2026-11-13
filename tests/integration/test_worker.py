"""End-to-end worker tests: real DB queue + mocked backend HTTP."""

from __future__ import annotations

import httpx

from services import queue
from services.worker import Worker
from tests.conftest import make_backend_transport, make_workspaces_client


async def _enqueue(sm, **kw) -> int:
    defaults = dict(
        tenant_id="00000000-0000-0000-0000-000000000003",
        repo_url="https://github.com/o/r",
        package="fastmcp",
        target="3.4.1",
    )
    defaults.update(kw)
    async with sm() as s:
        job, _ = await queue.enqueue(s, **defaults)
        await s.commit()
        return job.id


def _worker(sm, settings, transport: httpx.MockTransport) -> Worker:
    # Two seams now: the raw httpx client still carries coding-security /
    # coding-github, and the typed workspaces client carries /api/workspaces.
    # Both are pointed at the SAME mock transport so one handler still simulates
    # the whole backend sequence.
    client = httpx.AsyncClient(transport=transport)
    return Worker(
        sm,
        settings,
        client=client,
        workspaces_client=make_workspaces_client(transport),
    )


async def test_process_job_success_to_pr_open(sm, settings):
    jid = await _enqueue(sm)
    async with sm() as s:
        job = await queue.claim_next(s)
        await s.commit()
    worker = _worker(sm, settings, make_backend_transport())
    final = await worker.process_job(job)
    assert final == "pr_open"
    async with sm() as s:
        row = await queue.get_job(s, jid)
        assert row.status == "pr_open"
        assert row.pr_url == "https://github.com/o/r/pull/7"
        assert row.pr_number == 7
        assert row.container_name == "bulk-c-1"
        assert row.tenant_id == settings.worker_tenant_id
    await worker.stop()


async def test_process_job_oracle_red_to_dead(sm, settings):
    await _enqueue(sm)
    async with sm() as s:
        job = await queue.claim_next(s)
        await s.commit()
    worker = _worker(sm, settings, make_backend_transport(fail="tests"))
    final = await worker.process_job(job)
    assert final == "dead"
    async with sm() as s:
        row = await queue.get_job(s, job.id)
        assert row.status == "dead"
        assert "tests failed" in (row.last_error or "")
    await worker.stop()


async def test_process_job_transient_requeues(sm, settings):
    await _enqueue(sm, max_attempts=3)
    async with sm() as s:
        job = await queue.claim_next(s)  # attempts -> 1
        await s.commit()
    worker = _worker(sm, settings, make_backend_transport(fail="provision"))
    final = await worker.process_job(job)
    assert final == "queued"  # re-claimable
    async with sm() as s:
        row = await queue.get_job(s, job.id)
        assert row.status == "queued"
        assert "503" in (row.last_error or "")
    await worker.stop()


async def test_process_job_permanent_provision_4xx_to_dead(sm, settings):
    await _enqueue(sm)
    async with sm() as s:
        job = await queue.claim_next(s)
        await s.commit()
    worker = _worker(sm, settings, make_backend_transport(fail="provision-4xx"))
    final = await worker.process_job(job)
    assert final == "dead"
    await worker.stop()


async def test_no_change_marks_done(sm, settings):
    await _enqueue(sm)
    async with sm() as s:
        job = await queue.claim_next(s)
        await s.commit()
    transport = make_backend_transport(
        upgrade={"exit_code": 0, "results": [{"success": True}], "pr_url": None}
    )
    worker = _worker(sm, settings, transport)
    final = await worker.process_job(job)
    assert final == "done"
    await worker.stop()


async def test_worker_loop_drains_queue(sm, settings):
    # Drive the actual claim loop (not process_job directly) over 3 jobs.
    for i in range(3):
        await _enqueue(sm, package=f"pkg{i}", target="1.0")
    worker = _worker(sm, settings, make_backend_transport())
    worker.start()
    # Poll until all reach a terminal state or timeout.
    import asyncio

    for _ in range(100):
        async with sm() as s:
            counts = await queue.status_counts(s)
        terminal = counts.get("pr_open", 0) + counts.get("dead", 0) + counts.get("done", 0)
        if terminal == 3:
            break
        await asyncio.sleep(0.05)
    await worker.stop()
    async with sm() as s:
        counts = await queue.status_counts(s)
    assert counts.get("pr_open", 0) == 3
