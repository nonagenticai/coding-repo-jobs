"""API tests for the enqueue/list/inspect endpoints (real DB via dependency override).

Auth: every /api/repo-jobs route is tenant-scoped via coding_client require_tenant.
Tests drive the API from an *untrusted* client host (so the trusted-network
bypass never masks a missing tenant) and pass tenant identity via the
``X-Tenant-Id`` header, exactly as Istio ext_authz would in cluster.
"""

from __future__ import annotations

import httpx
import pytest
from httpx import ASGITransport

from app_factory import create_app
from db.session import get_db

# TEST-NET-3 (RFC 5737) — deliberately NOT an RFC1918/loopback trusted network,
# so AuthMiddleware/require_tenant do not fall back to DEFAULT_TENANT_ID.
_UNTRUSTED_HOST = ("203.0.113.10", 5555)

# ``repo_jobs.tenant_id`` is a real postgres ``uuid`` (migration 0003), so a
# tenant identity that is not a uuid can no longer be stored at all. These
# fixtures used to carry hyphenated slugs; the column now refuses them, which
# is exactly the guarantee the retype buys.
TENANT_A = "11111111-1111-4111-8111-111111111111"
TENANT_B = "22222222-2222-4222-8222-222222222222"


@pytest.fixture
def app_with_db(sm):
    app = create_app()

    async def _override_db():
        async with sm() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = _override_db
    return app


def _client(app, tenant: str | None = None) -> httpx.AsyncClient:
    headers = {"X-Tenant-Id": tenant} if tenant else {}
    transport = ASGITransport(app=app, client=_UNTRUSTED_HOST)
    return httpx.AsyncClient(transport=transport, base_url="http://t", headers=headers)


@pytest.fixture
async def client(app_with_db):
    """Default client authenticated as TENANT_A."""
    async with _client(app_with_db, tenant=TENANT_A) as c:
        yield c


async def test_enqueue_then_get(client):
    resp = await client.post(
        "/api/repo-jobs",
        json={"repo_url": "https://github.com/o/r", "package": "fastmcp", "target": "3.4.1"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["created"] is True
    jid = body["id"]

    got = await client.get(f"/api/repo-jobs/{jid}")
    assert got.status_code == 200
    assert got.json()["package"] == "fastmcp"
    assert got.json()["tenant_id"] == TENANT_A


async def test_enqueue_idempotent_via_api(client):
    payload = {"repo_url": "o/r", "package": "p", "target": "1.0"}
    first = await client.post("/api/repo-jobs", json=payload)
    second = await client.post("/api/repo-jobs", json=payload)
    assert first.json()["created"] is True
    assert second.json()["created"] is False
    assert first.json()["id"] == second.json()["id"]


async def test_bulk_enqueue_dedups(client):
    payload = {
        "jobs": [
            {"repo_url": "o/r", "package": "a", "target": "1"},
            {"repo_url": "o/r", "package": "b", "target": "1"},
            {"repo_url": "o/r", "package": "a", "target": "1"},  # dup of first
        ]
    }
    resp = await client.post("/api/repo-jobs/bulk", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["enqueued"] == 2
    assert body["existing"] == 1


async def test_list_and_stats(client):
    await client.post("/api/repo-jobs", json={"repo_url": "o/r", "package": "a", "target": "1"})
    await client.post("/api/repo-jobs", json={"repo_url": "o/r", "package": "b", "target": "1"})

    listed = await client.get("/api/repo-jobs")
    assert listed.json()["total"] == 2

    stats = await client.get("/api/repo-jobs/stats")
    assert stats.json()["total"] == 2
    assert stats.json()["counts"]["queued"] == 2


async def test_get_missing_job_404(client):
    resp = await client.get("/api/repo-jobs/999999")
    assert resp.status_code == 404


# ── S5 security regression: auth + tenant isolation ──────────────────────────
async def test_request_without_tenant_is_rejected(app_with_db):
    # No X-Tenant-Id header, untrusted network → AuthMiddleware rejects (401).
    async with _client(app_with_db, tenant=None) as c:
        resp = await c.post(
            "/api/repo-jobs", json={"repo_url": "o/r", "package": "p", "target": "1"}
        )
    assert resp.status_code == 401


async def test_tenant_cannot_list_or_stat_other_tenants_jobs(app_with_db):
    async with _client(app_with_db, tenant=TENANT_A) as a:
        await a.post("/api/repo-jobs", json={"repo_url": "o/r", "package": "a", "target": "1"})
    async with _client(app_with_db, tenant=TENANT_B) as b:
        # B's queue is empty — it must NOT see A's job via list or stats.
        listed = await b.get("/api/repo-jobs")
        assert listed.json()["total"] == 0
        assert listed.json()["jobs"] == []
        stats = await b.get("/api/repo-jobs/stats")
        assert stats.json()["total"] == 0


async def test_tenant_cannot_get_other_tenants_job_by_id(app_with_db):
    async with _client(app_with_db, tenant=TENANT_A) as a:
        created = await a.post(
            "/api/repo-jobs", json={"repo_url": "o/r", "package": "p", "target": "1"}
        )
        jid = created.json()["id"]
    async with _client(app_with_db, tenant=TENANT_B) as b:
        # Fetching A's job id as tenant B must 404 (not leak the row).
        resp = await b.get(f"/api/repo-jobs/{jid}")
    assert resp.status_code == 404


async def test_two_tenants_same_intent_get_separate_jobs(app_with_db):
    payload = {"repo_url": "o/r", "package": "p", "target": "1.0"}
    async with _client(app_with_db, tenant=TENANT_A) as a:
        ra = await a.post("/api/repo-jobs", json=payload)
    async with _client(app_with_db, tenant=TENANT_B) as b:
        rb = await b.post("/api/repo-jobs", json=payload)
    assert ra.json()["created"] is True
    # The old tenant-blind dedup would return created=False + tenant A's id here.
    assert rb.json()["created"] is True
    assert ra.json()["id"] != rb.json()["id"]
