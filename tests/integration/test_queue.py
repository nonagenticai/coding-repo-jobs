"""DB-backed tests for the durable queue (dedup, SKIP LOCKED claim, transitions).

Requires a live Postgres via DATABASE_URL (CI provisions it; locally use pg18).
"""

from __future__ import annotations

import asyncio

from services import queue

# ``repo_jobs.tenant_id`` is a real postgres ``uuid`` (migration 0003), so the
# short slug literals these tests used to pass can no longer be stored at all.
TENANT_1 = "33333333-3333-4333-8333-333333333333"
TENANT_A = "11111111-1111-4111-8111-111111111111"
TENANT_B = "22222222-2222-4222-8222-222222222222"


async def test_enqueue_is_idempotent(sm):
    async with sm() as s:
        job1, created1 = await queue.enqueue(
            s, tenant_id=TENANT_1, repo_url="o/r", package="p", target="1.0"
        )
        await s.commit()
    async with sm() as s:
        job2, created2 = await queue.enqueue(
            s, tenant_id=TENANT_1, repo_url="o/r", package="p", target="1.0"
        )
        await s.commit()
    assert created1 is True
    assert created2 is False
    assert job1.id == job2.id


async def test_enqueue_distinct_targets_create_rows(sm):
    async with sm() as s:
        _, c1 = await queue.enqueue(
            s, tenant_id=TENANT_1, repo_url="o/r", package="p", target="1.0"
        )
        _, c2 = await queue.enqueue(
            s, tenant_id=TENANT_1, repo_url="o/r", package="p", target="2.0"
        )
        await s.commit()
    assert c1 and c2


async def test_claim_marks_and_increments(sm):
    async with sm() as s:
        await queue.enqueue(s, tenant_id=TENANT_1, repo_url="o/r", package="p", target="1.0")
        await s.commit()
    async with sm() as s:
        job = await queue.claim_next(s)
        await s.commit()
        assert job is not None
        assert job.status == "claimed"
        assert job.attempts == 1
        assert job.claimed_at is not None


async def test_claim_returns_none_when_empty(sm):
    async with sm() as s:
        assert await queue.claim_next(s) is None


async def test_claim_respects_fifo_order(sm):
    async with sm() as s:
        await queue.enqueue(s, tenant_id=TENANT_1, repo_url="o/r", package="a", target="1")
        await s.commit()
    async with sm() as s:
        await queue.enqueue(s, tenant_id=TENANT_1, repo_url="o/r", package="b", target="1")
        await s.commit()
    async with sm() as s:
        first = await queue.claim_next(s)
        await s.commit()
    assert first is not None
    assert first.package == "a"


async def test_skip_locked_no_double_claim(sm):
    # Two concurrent claims against a single queued row: exactly one wins, the
    # other gets None (SKIP LOCKED — no block, no double-hand-off).
    async with sm() as s:
        await queue.enqueue(s, tenant_id=TENANT_1, repo_url="o/r", package="p", target="1.0")
        await s.commit()

    async def claim() -> int | None:
        async with sm() as s:
            job = await queue.claim_next(s)
            await s.commit()
            return job.id if job else None

    a, b = await asyncio.gather(claim(), claim())
    got = {x for x in (a, b) if x is not None}
    assert len(got) == 1


async def test_requeue_when_attempts_remain(sm):
    async with sm() as s:
        await queue.enqueue(
            s, tenant_id=TENANT_1, repo_url="o/r", package="p", target="1.0", max_attempts=3
        )
        await s.commit()
    async with sm() as s:
        job = await queue.claim_next(s)  # attempts -> 1
        await s.commit()
    async with sm() as s:
        fresh = await queue.get_job(s, job.id)
        status = await queue.requeue_or_kill(s, fresh, error="boom")
        await s.commit()
    assert status == "queued"
    async with sm() as s:
        again = await queue.get_job(s, job.id)
        assert again.status == "queued"
        assert again.last_error == "boom"


async def test_kill_when_attempts_exhausted(sm):
    async with sm() as s:
        await queue.enqueue(
            s, tenant_id=TENANT_1, repo_url="o/r", package="p", target="1.0", max_attempts=1
        )
        await s.commit()
    async with sm() as s:
        job = await queue.claim_next(s)  # attempts -> 1 == max
        await s.commit()
    async with sm() as s:
        fresh = await queue.get_job(s, job.id)
        status = await queue.requeue_or_kill(s, fresh, error="gave up")
        await s.commit()
    assert status == "dead"


async def test_mark_patches_only_given_fields(sm):
    async with sm() as s:
        job, _ = await queue.enqueue(
            s, tenant_id=TENANT_1, repo_url="o/r", package="p", target="1.0"
        )
        await s.commit()
        jid = job.id
    async with sm() as s:
        await queue.mark(s, jid, status="verifying", pr_url="https://x/pull/1", pr_number=1)
        await s.commit()
    async with sm() as s:
        await queue.mark(s, jid, status="pr_open")  # must NOT clobber pr_url
        await s.commit()
    async with sm() as s:
        j = await queue.get_job(s, jid)
        assert j.status == "pr_open"
        assert j.pr_url == "https://x/pull/1"
        assert j.pr_number == 1


async def test_list_and_status_counts(sm):
    async with sm() as s:
        await queue.enqueue(s, tenant_id=TENANT_1, repo_url="o/r", package="a", target="1")
        await queue.enqueue(s, tenant_id=TENANT_1, repo_url="o/r", package="b", target="1")
        await s.commit()
    async with sm() as s:
        await queue.mark(s, 1, status="dead", last_error="x")
        await s.commit()
    async with sm() as s:
        rows, total = await queue.list_jobs(s)
        assert total == 2
        dead_rows, dead_total = await queue.list_jobs(s, status="dead")
        assert dead_total == 1 and dead_rows[0].status == "dead"
        counts = await queue.status_counts(s)
        assert counts.get("dead") == 1
        assert counts.get("queued") == 1


async def test_dedup_is_per_tenant(sm):
    # S5 regression: two tenants enqueuing the SAME (repo_url, package, target)
    # must get SEPARATE rows — the old tenant-blind unique key deduped tenant B
    # onto tenant A's row and handed B tenant A's job id.
    async with sm() as s:
        job_a, created_a = await queue.enqueue(
            s, tenant_id=TENANT_A, repo_url="o/r", package="p", target="1.0"
        )
        job_b, created_b = await queue.enqueue(
            s, tenant_id=TENANT_B, repo_url="o/r", package="p", target="1.0"
        )
        await s.commit()
    assert created_a is True
    assert created_b is True
    assert job_a.id != job_b.id
    assert job_a.tenant_id == TENANT_A
    assert job_b.tenant_id == TENANT_B


async def test_reads_are_tenant_scoped(sm):
    # list/get/status_counts scoped to a tenant never see another tenant's rows.
    async with sm() as s:
        a, _ = await queue.enqueue(s, tenant_id=TENANT_A, repo_url="o/r", package="a", target="1")
        b, _ = await queue.enqueue(s, tenant_id=TENANT_B, repo_url="o/r", package="b", target="1")
        await s.commit()
        a_id, b_id = a.id, b.id
    async with sm() as s:
        rows, total = await queue.list_jobs(s, tenant_id=TENANT_A)
        assert total == 1 and rows[0].tenant_id == TENANT_A
        counts = await queue.status_counts(s, tenant_id=TENANT_A)
        assert sum(counts.values()) == 1
        # tenant-a cannot fetch tenant-b's row → None (the API turns this into 404)
        assert await queue.get_job(s, b_id, tenant_id=TENANT_A) is None
        assert await queue.get_job(s, a_id, tenant_id=TENANT_A) is not None
