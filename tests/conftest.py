"""Shared test fixtures for coding-repo-jobs.

Two kinds of tests:
- pure unit (orchestrator/parse/schemas/settings/health) — no DB.
- DB-backed (queue/worker) — connect via DATABASE_URL to a live Postgres.
  CI provisions a migrated DB and exports DATABASE_URL. There is NO host:5432
  default (it silently hits whatever runs on that port on a dev box): when
  DATABASE_URL is unset an ephemeral docker postgres:18 is started for the
  session (tests/ephemeral_postgres.py); without docker the session stops.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from coding_client.clients import CodingWorkspacesClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from tests.ephemeral_postgres import (
    resolve_test_database_url,
    start_session_database,
    stop_session_database,
)


def _normalize(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def pytest_configure(config: pytest.Config) -> None:
    """Start the ONE session database in the xdist controller (no-op in workers), before
    workers spawn, so they inherit the exported DSN instead of starting their own."""
    start_session_database(
        env_vars=("DATABASE_URL",),
        export_var="DATABASE_URL",
        dbname="repo_jobs",
        config=config,
    )


def pytest_unconfigure(config: pytest.Config) -> None:
    """Controller only: remove the session container by its own ID."""
    stop_session_database(config)


@pytest.fixture(scope="session")
def database_url(request: pytest.FixtureRequest) -> str:
    """DATABASE_URL, else an ephemeral docker postgres:18, else stop the session."""
    return _normalize(
        resolve_test_database_url(
            env_vars=("DATABASE_URL",),
            export_var="DATABASE_URL",
            dbname="repo_jobs",
            request=request,
        )
    )


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    """Configure settings for tests; keep the worker loop dormant."""
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.setenv("WORKER_ENABLED", "false")
    monkeypatch.setenv("CONTAINERS_URL", "http://coding-containers:8691")
    monkeypatch.setenv("SECURITY_URL", "http://coding-security:8693")
    monkeypatch.setenv("GITHUB_URL", "http://coding-github:8692")
    monkeypatch.setenv("WORKER_TENANT_ID", "00000000-0000-0000-0000-000000000003")
    monkeypatch.setenv("RETRY_BACKOFF_SECONDS", "0")
    monkeypatch.setenv("WORKER_POLL_INTERVAL_SECONDS", "0.05")
    from config.settings import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def settings():
    from config.settings import get_settings

    return get_settings()


@pytest.fixture
async def sm(database_url):
    """A clean, migrated repo_jobs table + a session factory bound to it."""
    from models.database import Base

    engine = create_async_engine(database_url, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("TRUNCATE repo_jobs RESTART IDENTITY"))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.fixture
async def db_session(sm):
    async with sm() as session:
        yield session


# ── Mock backend (coding-containers / coding-security / coding-github) ────────
def make_backend_transport(
    *,
    container: str = "bulk-c-1",
    provision_status: int = 201,
    upgrade: dict | None = None,
    exec_result: dict | None = None,
    scan: dict | None = None,
    fail: str | None = None,
) -> httpx.MockTransport:
    """Build an httpx MockTransport simulating the full backend sequence.

    ``fail`` injects a failure at one step: 'provision' (503), 'provision-4xx'
    (400), 'upgrade-exit' (exit_code != 0), 'tests' (exec exit 1), 'scan'
    (critical CVE).
    """
    up = upgrade or {
        "exit_code": 0,
        "results": [{"name": "pkg", "success": True}],
        "pr_url": "https://github.com/o/r/pull/7",
        "pr_number": 7,
    }
    ex = exec_result or {"exit_code": 0, "stdout": "1 passed"}
    sc = scan or {"result": {"critical_count": 0, "security_grade": "A"}}

    if fail == "upgrade-exit":
        up = {"exit_code": 1, "results": [{"success": False}], "stderr": "resolver conflict"}
    if fail == "tests":
        ex = {"exit_code": 1, "stdout": "1 failed", "stderr": "AssertionError"}
    if fail == "scan":
        sc = {"result": {"critical_count": 2, "security_grade": "F"}}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method
        if method == "GET" and url.endswith("/status"):
            return httpx.Response(200, json={"container_status": "running"})
        if method == "POST" and url.endswith("/api/workspaces/"):
            if fail == "provision":
                return httpx.Response(503, json={"detail": "docker down"})
            if fail == "provision-4xx":
                return httpx.Response(400, json={"detail": "bad repo url"})
            return httpx.Response(provision_status, json={"container_name": container})
        if method == "POST" and "/api/dependencies/" in url and "/upgrade" in url:
            return httpx.Response(200, json=up)
        if method == "POST" and "/exec" in url:
            return httpx.Response(200, json=ex)
        if method == "POST" and "/api/security/scan/" in url:
            return httpx.Response(200, json=sc)
        if method == "POST" and url.endswith("/mark-pull-request-ready"):
            return httpx.Response(200, json={"success": True, "pr_status": "open"})
        if method == "DELETE" and "/api/workspaces/" in url:
            return httpx.Response(200, json={"message": "deleted"})
        return httpx.Response(404, json={"detail": f"unmocked {method} {url}"})

    return httpx.MockTransport(handler)


@pytest.fixture
def backend_factory() -> Callable[..., httpx.MockTransport]:
    return make_backend_transport


DEFAULT_CONTAINERS_URL = "http://coding-containers:8691"


def make_workspaces_client(
    transport: httpx.MockTransport,
    *,
    base_url: str = DEFAULT_CONTAINERS_URL,
    timeout: float = 900.0,
    max_retries: int = 1,
) -> CodingWorkspacesClient:
    """A REAL ``CodingWorkspacesClient`` wired to the mocked backend transport.

    ``ServiceClient`` takes no ``client=``/``transport=`` argument — it builds its
    own ``httpx.AsyncClient`` lazily in ``_ensure_client`` and reuses it while it
    is open. Pre-seeding ``_client`` is therefore the seam: it keeps the client's
    REAL behaviour under test (route construction, ``params`` vs ``json``, the
    ``ServiceError``-on-4xx/5xx contract, the 204→``None`` decode) instead of a
    hand-rolled fake that would re-verify only itself — which is the whole point
    of adopting the typed client.

    ``max_retries=1`` keeps tests from waiting on tenacity's exponential backoff.
    """
    client = CodingWorkspacesClient(base_url=base_url, timeout=timeout, max_retries=max_retries)
    client._client = httpx.AsyncClient(
        base_url=base_url, transport=transport, timeout=httpx.Timeout(timeout)
    )
    return client
