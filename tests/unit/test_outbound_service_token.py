"""The worker must prove WHO it is on every call to a backend service.

``UpgradeOrchestrator._headers`` used to be ``{"X-Tenant-Id": self.tenant_id}``
and nothing else. That is an unverified claim — any pod in the mesh can assert
any tenant — honoured today only because coding-containers, coding-security and
coding-github still trust the header. ``TRUST_TENANT_HEADER`` exists so they can
stop, and the moment one of them does, this worker goes anonymous.

That failure is expensive here in a way it is not elsewhere. A 401/403 lands in
``_classify_http_error``, which maps every non-5xx status to ``PermanentError``
— so a claimed job does not retry, it *dies*. And ``run()``'s ``finally`` tears
the container down on the way out, after coding-security has already opened a
DRAFT pull request that only the oracle gate promotes. The observable result on
a real repo would be: job killed, container gone, and a stranded draft PR nobody
promotes. Not an outage that looks like one.

So these tests assert on the WIRE, via ``httpx.MockTransport``, and they assert
BOTH halves of the identity on EVERY request the full ``run()`` emits — POST,
GET-poll and the best-effort DELETE teardown alike, since each builds its
headers through a separate method:

* ``Authorization: Bearer <token>`` — this service's own license;
* ``X-Tenant-Id: <operator tenant>`` — unchanged, still the job's tenant.

Checking only ``Authorization`` would not be enough. The tempting shortcut of
overloading ``X-Tenant-Id`` with ``Bearer <token>`` puts the credential on the
wire while making every operator's containers resolve to coding-repo-jobs' own
tenant — a mutant that a one-sided assertion happily passes.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from coding_client.auth.outbound import TenantMissingError
from coding_client.clients import ServiceError

from services.orchestrator import (
    OrchestratorError,
    UpgradeOrchestrator,
)
from tests.conftest import make_backend_transport, make_workspaces_client

TOKEN = "svc-coding-repo-jobs-token-abc"
TENANT = "00000000-0000-0000-0000-000000000003"


def _orch(transport: httpx.MockTransport, *, tenant_id: str = TENANT) -> UpgradeOrchestrator:
    # The four /api/workspaces calls now build their headers inside
    # CodingWorkspacesClient calls (``headers=self._headers``) rather than in
    # ``_post``/``_get``. Wiring the typed client to the SAME recording transport
    # is what keeps this file's "every request on the wire" claim honest — drop
    # it and the provision/poll/exec/teardown requests would silently vanish from
    # ``seen`` and the assertions would pass while proving nothing.
    return UpgradeOrchestrator(
        httpx.AsyncClient(transport=transport),
        containers_url="http://coding-containers:8691",
        security_url="http://coding-security:8693",
        github_url="http://coding-github:8692",
        tenant_id=tenant_id,
        container_ready_poll_seconds=0.0,
        workspaces_client=make_workspaces_client(transport),
    )


def _recording_transport(seen: list[httpx.Request]) -> httpx.MockTransport:
    """Wrap the shared backend mock so every real request is captured."""
    inner = make_backend_transport()

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return inner.handle_request(request)

    return httpx.MockTransport(handler)


async def _drive_full_job(seen: list[httpx.Request], **orch_kwargs) -> None:
    """Run one complete job so provision/poll/upgrade/exec/scan/promote/teardown fire."""
    orch = _orch(_recording_transport(seen), **orch_kwargs)
    try:
        await orch.run(repo_url="o/r", package="p", target="1.0", job_id=1)
    finally:
        await orch.client.aclose()
        await orch.aclose()


# ─────────────────────────────────────────────────────────────────────────────
# The header builder
# ─────────────────────────────────────────────────────────────────────────────


class TestHeaderBuilder:
    def test_credential_and_tenant_are_separate_headers(self, monkeypatch):
        monkeypatch.setenv("CODING_SERVICE_TOKEN", TOKEN)
        headers = _orch(make_backend_transport())._headers

        assert headers["Authorization"] == f"Bearer {TOKEN}"
        # Moving the token into X-Tenant-Id would make the backend resolve the
        # tenant from the token and attribute every operator's containers to us.
        assert headers["X-Tenant-Id"] == TENANT

    def test_no_token_means_no_authorization_header(self, monkeypatch):
        monkeypatch.delenv("CODING_SERVICE_TOKEN", raising=False)
        headers = _orch(make_backend_transport())._headers

        assert "Authorization" not in headers
        assert headers["X-Tenant-Id"] == TENANT

    def test_empty_token_is_not_sent_as_a_bare_bearer(self, monkeypatch):
        """A present-but-blank Secret key is the realistic misconfiguration."""
        monkeypatch.setenv("CODING_SERVICE_TOKEN", "")

        assert "Authorization" not in _orch(make_backend_transport())._headers

    def test_token_is_read_per_call_not_pinned_at_construction(self, monkeypatch):
        """The worker is long-lived; a rotated Secret must not need a restart."""
        orch = _orch(make_backend_transport())

        monkeypatch.setenv("CODING_SERVICE_TOKEN", "first")
        assert orch._headers["Authorization"] == "Bearer first"
        monkeypatch.setenv("CODING_SERVICE_TOKEN", "second")
        assert orch._headers["Authorization"] == "Bearer second"

    def test_a_blank_tenant_refuses_the_call_instead_of_asserting_nothing(self, monkeypatch):
        """The tenant scopes the resource, so a blank one is not a lost log line.

        ``WORKER_TENANT_ID`` defaults to empty; before this change that produced
        ``{"X-Tenant-Id": ""}`` and the call went out anyway, exec'ing into
        whatever container the backend's fallback tenant resolved to.
        """
        monkeypatch.setenv("CODING_SERVICE_TOKEN", TOKEN)

        with pytest.raises(TenantMissingError):
            _ = _orch(make_backend_transport(), tenant_id="")._headers

    def test_tenant_missing_cannot_be_reclassified_as_a_retryable_failure(self):
        """It must escape the orchestrator's own error taxonomy, not be absorbed.

        ``_post`` catches ``httpx.HTTPError`` and ``_ws_call`` catches that plus
        ``ServiceError``; both funnel into an ``OrchestratorError``. A missing
        tenant is a bug in the caller, and re-queueing it forever would hide that
        — so it must sit outside BOTH hierarchies, not just httpx's.
        """
        assert not issubclass(TenantMissingError, OrchestratorError)
        assert not issubclass(TenantMissingError, httpx.HTTPError)
        assert not issubclass(TenantMissingError, ServiceError)


# ─────────────────────────────────────────────────────────────────────────────
# The wire, across a full job
# ─────────────────────────────────────────────────────────────────────────────


class TestEveryBackendRequestCarriesTheIdentity:
    async def test_license_and_tenant_on_every_request_of_a_full_job(self, monkeypatch):
        monkeypatch.setenv("CODING_SERVICE_TOKEN", TOKEN)
        seen: list[httpx.Request] = []

        await _drive_full_job(seen)

        # Provision, ready-poll, upgrade, exec, scan, promote, teardown.
        assert len(seen) >= 7, [f"{r.method} {r.url}" for r in seen]
        for request in seen:
            assert request.headers["authorization"] == f"Bearer {TOKEN}", request.url
            assert request.headers["x-tenant-id"] == TENANT, request.url

    async def test_the_teardown_delete_is_not_left_credential_less(self, monkeypatch):
        """``_delete_workspace`` builds its own headers and swallows every error.

        An unauthenticated teardown fails silently by design ("best-effort"), so
        it would leak a container per job with nothing louder than a WARNING.
        """
        monkeypatch.setenv("CODING_SERVICE_TOKEN", TOKEN)
        seen: list[httpx.Request] = []

        await _drive_full_job(seen)

        deletes = [r for r in seen if r.method == "DELETE"]
        assert deletes, "teardown never fired — this test proves nothing"
        assert deletes[0].headers["authorization"] == f"Bearer {TOKEN}"
        assert deletes[0].headers["x-tenant-id"] == TENANT

    async def test_the_status_poll_get_carries_the_identity(self, monkeypatch):
        """The readiness poll is a third header-building path (now via the
        typed client's ``get_status(name, headers=...)``)."""
        monkeypatch.setenv("CODING_SERVICE_TOKEN", TOKEN)
        seen: list[httpx.Request] = []

        await _drive_full_job(seen)

        gets = [r for r in seen if r.method == "GET"]
        assert gets, "status poll never fired — this test proves nothing"
        assert gets[0].headers["authorization"] == f"Bearer {TOKEN}"
        assert gets[0].headers["x-tenant-id"] == TENANT

    async def test_no_authorization_header_anywhere_when_no_token_configured(self, monkeypatch):
        """Absent is absent — never fabricated, never an empty ``Bearer ``."""
        monkeypatch.delenv("CODING_SERVICE_TOKEN", raising=False)
        seen: list[httpx.Request] = []

        await _drive_full_job(seen)

        assert seen
        for request in seen:
            assert "authorization" not in request.headers, request.url
            assert request.headers["x-tenant-id"] == TENANT, request.url


# ─────────────────────────────────────────────────────────────────────────────
# Census guard
# ─────────────────────────────────────────────────────────────────────────────


def test_the_orchestrator_does_not_hand_roll_the_tenant_header():
    """A future call site must not reintroduce a bare ``{"X-Tenant-Id": ...}``.

    ``_headers`` is the single seam through which all three backends are called;
    a hand-rolled dict anywhere in this module silently opts that call out of the
    credential.
    """
    source = (Path(__file__).resolve().parents[2] / "src/services/orchestrator.py").read_text()

    assert "tenant_headers(" in source
    assert "service_token=" in source
    assert '"X-Tenant-Id"' not in source
    assert '"x-tenant-id"' not in source
