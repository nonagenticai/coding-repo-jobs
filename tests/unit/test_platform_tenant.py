"""coding-github gates platform-owned repositories to the platform tenants.

A repository owned by ``PLATFORM_REPO_OWNER`` is only addressable by one of a
small set of platform tenant ids. The bulk spine runs as a single *operator*
tenant, which is not one of them — so ``POST /api/repositories/
mark-pull-request-ready`` against such a repo is refused with 403
``platform_repo_forbidden`` the moment that gate is enforced. 403 is not
retryable here (:func:`_classify_http_error` maps every non-5xx to
``PermanentError``), so the job would *die* with its DRAFT pull request stranded
un-promoted on the repo — a silent, permanent failure on exactly the repos this
service exists to upgrade.

These tests assert on the WIRE, and they assert both directions, because the
cheap wrong fix — re-tenanting every backend call — passes a one-sided test
while mis-attributing the job's container to the platform tenant and letting the
worker exec into a container the operator does not own. So each test checks the
promote call AND the coding-containers / coding-security calls of the same run.
"""

from __future__ import annotations

import httpx
import pytest
from coding_client.auth.outbound import TenantMissingError

from config.settings import PLATFORM_REPO_OWNER, PLATFORM_TENANT_ID, Settings
from services.orchestrator import UpgradeOrchestrator
from tests.conftest import make_backend_transport, make_workspaces_client

JOB_TENANT = "00000000-0000-0000-0000-000000000003"
PLATFORM_REPO = f"https://github.com/{PLATFORM_REPO_OWNER}/enterprise_mcp_server"
CUSTOMER_REPO = "https://github.com/acme-customer/widget"


def _recording_transport(seen: list[httpx.Request]) -> httpx.MockTransport:
    inner = make_backend_transport()

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return inner.handle_request(request)

    return httpx.MockTransport(handler)


def _orch(
    transport: httpx.MockTransport,
    *,
    tenant_id: str = JOB_TENANT,
    **kwargs,
) -> UpgradeOrchestrator:
    return UpgradeOrchestrator(
        httpx.AsyncClient(transport=transport),
        containers_url="http://coding-containers:8691",
        security_url="http://coding-security:8693",
        github_url="http://coding-github:8692",
        tenant_id=tenant_id,
        container_ready_poll_seconds=0.0,
        workspaces_client=make_workspaces_client(transport),
        **kwargs,
    )


async def _run(repo_url: str, seen: list[httpx.Request], **kwargs) -> None:
    orch = _orch(_recording_transport(seen), **kwargs)
    try:
        await orch.run(repo_url=repo_url, package="p", target="1.0", job_id=1)
    finally:
        await orch.client.aclose()
        await orch.aclose()


def _promote(seen: list[httpx.Request]) -> httpx.Request | None:
    hits = [r for r in seen if str(r.url).endswith("/mark-pull-request-ready")]
    return hits[0] if hits else None


def _other_backend_requests(seen: list[httpx.Request]) -> list[httpx.Request]:
    """Every request of the run that is NOT the coding-github promote."""
    return [r for r in seen if not str(r.url).endswith("/mark-pull-request-ready")]


class TestTenantOnTheWire:
    async def test_a_platform_owned_target_sends_the_platform_tenant(self):
        seen: list[httpx.Request] = []

        await _run(PLATFORM_REPO, seen)

        promote = _promote(seen)
        assert promote is not None, "promote never fired — this test proves nothing"
        assert promote.headers["x-tenant-id"] == PLATFORM_TENANT_ID
        assert promote.headers["x-tenant-id"] != JOB_TENANT

    async def test_the_platform_swap_does_not_re_tenant_the_other_backends(self):
        """Negative control: containers/security still get the operator tenant.

        They scope the job's OWN container. Asserting the platform tenant there
        would mis-attribute it and let the job exec into a container it does not
        own — a blanket re-tenanting passes the test above and fails this one.
        """
        seen: list[httpx.Request] = []

        await _run(PLATFORM_REPO, seen)

        others = _other_backend_requests(seen)
        assert len(others) >= 6, [f"{r.method} {r.url}" for r in others]
        for request in others:
            assert request.headers["x-tenant-id"] == JOB_TENANT, request.url

    async def test_a_customer_owned_target_keeps_the_jobs_own_tenant(self):
        seen: list[httpx.Request] = []

        await _run(CUSTOMER_REPO, seen)

        promote = _promote(seen)
        assert promote is not None, "promote never fired — this test proves nothing"
        assert promote.headers["x-tenant-id"] == JOB_TENANT
        for request in seen:
            assert request.headers["x-tenant-id"] == JOB_TENANT, request.url

    async def test_the_owner_match_is_case_insensitive(self):
        """GitHub owners are case-insensitive; ``NonAgenticAI/x`` is the same org.

        A case-sensitive compare would send the operator tenant into the gate and
        earn the same permanent 403 this change exists to prevent.
        """
        seen: list[httpx.Request] = []

        await _run(f"https://github.com/{PLATFORM_REPO_OWNER.upper()}/repo", seen)

        promote = _promote(seen)
        assert promote is not None
        assert promote.headers["x-tenant-id"] == PLATFORM_TENANT_ID

    async def test_the_setting_side_of_the_owner_match_is_folded_too(self):
        """The CONFIGURED owner is folded as well, not only the parsed one.

        ``PLATFORM_REPO_OWNER`` is env-overridable, and ``NonAgenticAI`` is a
        legal spelling of the same org — GitHub owners are case-insensitive, so
        gitops may carry it in any case. If only the repo_url side were folded,
        that spelling would make the platform swap fire for NO repository at
        all: every platform-owned target would take the operator tenant into
        the gate and earn the permanent 403 this change exists to prevent —
        reached by configuration instead of by a URL, and therefore invisible
        to every test that pins the setting to its lowercase default.

        The test above varies the URL; this one varies the SETTING, which is
        the half the mutation ``(platform_repo_owner or "")`` survives.
        """
        seen: list[httpx.Request] = []

        await _run(PLATFORM_REPO, seen, platform_repo_owner="NonAgenticAI")

        promote = _promote(seen)
        assert promote is not None, "promote never fired — this test proves nothing"
        assert promote.headers["x-tenant-id"] == PLATFORM_TENANT_ID
        assert promote.headers["x-tenant-id"] != JOB_TENANT

    async def test_a_blank_platform_repo_owner_is_not_a_wildcard(self):
        """An empty owner SETTING must match nothing — not "an empty owner".

        Both sides of the compare normalise an absent value to ``""``, so a bare
        ``==`` makes ``PLATFORM_REPO_OWNER=""`` fire on any repo_url whose owner
        parses empty (``parse_repo_url("/widget")`` → ``("", "widget")``). That
        is the inverse failure of the one above: a repository the platform does
        not own, re-tenanted onto the platform tenant by a setting that is
        switched OFF.
        """
        seen: list[httpx.Request] = []

        await _run("/widget", seen, platform_repo_owner="")

        promote = _promote(seen)
        assert promote is not None, "promote never fired — this test proves nothing"
        assert promote.headers["x-tenant-id"] == JOB_TENANT
        assert promote.headers["x-tenant-id"] != PLATFORM_TENANT_ID


class TestABlankTenantRefusesInsteadOfSendingAnEmptyHeader:
    async def test_a_blank_worker_tenant_raises_and_names_its_setting(self):
        """An empty ``WORKER_TENANT_ID`` must not go on the wire as ``""``.

        The receiving gate reads a blank ``X-Tenant-Id`` as "no tenant" and
        refuses — surfacing as an opaque 403 rather than as the misconfiguration
        it is. Nothing may reach the network at all.
        """
        seen: list[httpx.Request] = []

        with pytest.raises(TenantMissingError) as excinfo:
            await _run(CUSTOMER_REPO, seen, tenant_id="")

        assert "WORKER_TENANT_ID" in str(excinfo.value)
        assert seen == [], [f"{r.method} {r.url}" for r in seen]

    async def test_a_blank_platform_tenant_raises_and_names_its_setting(self):
        """The platform half fails the same way: refuse, never send a blank."""
        seen: list[httpx.Request] = []

        with pytest.raises(TenantMissingError) as excinfo:
            await _run(PLATFORM_REPO, seen, platform_tenant_id="")

        assert "PLATFORM_TENANT_ID" in str(excinfo.value)
        assert _promote(seen) is None
        # The earlier backend calls did happen, so the teardown must have run —
        # otherwise this failure would leak the job's container.
        assert [r for r in seen if r.method == "DELETE"], "teardown never fired"

    def test_tenant_missing_is_not_absorbed_by_the_orchestrators_error_taxonomy(self):
        """``run()``'s callers re-queue ``TransientError`` forever; a
        misconfigured tenant must not be able to look like one."""
        from services.orchestrator import OrchestratorError

        assert not issubclass(TenantMissingError, OrchestratorError)
        assert not issubclass(TenantMissingError, httpx.HTTPError)


class TestTheSettingIsTheSingleSource:
    def test_platform_tenant_is_one_named_setting_and_env_overridable(self, monkeypatch):
        assert Settings(environment="test").platform_tenant_id == PLATFORM_TENANT_ID
        monkeypatch.setenv("PLATFORM_TENANT_ID", "0195f48b-d000-7000-8000-000000000005")
        assert (
            Settings(environment="test").platform_tenant_id
            == "0195f48b-d000-7000-8000-000000000005"
        )

    def test_the_orchestrator_holds_no_platform_literal_of_its_own(self):
        """The owner and the tenant id come from the settings module, so there is
        exactly one place to change when the gate's allowlist changes."""
        from pathlib import Path

        source = (Path(__file__).resolve().parents[2] / "src/services/orchestrator.py").read_text()
        code = "\n".join(line for line in source.splitlines() if not line.lstrip().startswith("#"))

        assert f'"{PLATFORM_REPO_OWNER}"' not in code
        assert f'"{PLATFORM_TENANT_ID}"' not in code
        assert "PLATFORM_TENANT_ID" in code and "PLATFORM_REPO_OWNER" in code


class TestABlankPlatformSettingFailsAtStartupNotAfterSideEffects:
    """Both platform settings are in ``_REQUIRED_IN_PRODUCTION``.

    They carry real defaults, so this can only fire on an EXPLICIT empty value
    in the Deployment env — which is exactly the case that must not boot. The
    call-site refusal in ``_tenant_for`` is a backstop, not the gate: by the
    time it raises, the job has provisioned a container, run the upgrade and
    opened a draft PR that nothing will now promote.
    """

    PROD = {
        "environment": "production",
        "database_url": "postgresql+psycopg://u:p@h:5432/d",
        "containers_url": "http://coding-containers:8691",
        "security_url": "http://coding-security:8693",
        "github_url": "http://coding-github:8692",
        "worker_tenant_id": JOB_TENANT,
    }

    def test_the_defaults_boot_in_production(self):
        """Positive control: without it, the two tests below prove nothing."""
        settings = Settings(**self.PROD)
        assert settings.platform_tenant_id == PLATFORM_TENANT_ID
        assert settings.platform_repo_owner == PLATFORM_REPO_OWNER

    def test_a_blank_platform_tenant_refuses_to_boot(self):
        with pytest.raises(RuntimeError) as excinfo:
            Settings(**self.PROD, platform_tenant_id="")
        assert "platform_tenant_id" in str(excinfo.value)

    def test_a_blank_platform_repo_owner_refuses_to_boot(self):
        """A blank owner matches nothing, so EVERY platform repo would take the
        operator tenant into the gate — a silent 403 on exactly the repos this
        service exists to upgrade, not a loud misconfiguration."""
        with pytest.raises(RuntimeError) as excinfo:
            Settings(**self.PROD, platform_repo_owner="")
        assert "platform_repo_owner" in str(excinfo.value)

    def test_the_production_check_names_every_blank_at_once(self):
        """The file's convention is ONE combined error, not first-failure."""
        with pytest.raises(RuntimeError) as excinfo:
            Settings(**self.PROD, platform_repo_owner="", platform_tenant_id="")
        message = str(excinfo.value)
        assert "platform_repo_owner" in message and "platform_tenant_id" in message
