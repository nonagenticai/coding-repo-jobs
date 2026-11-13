"""Unit tests for the HTTP orchestrator (no DB; httpx MockTransport)."""

from __future__ import annotations

import re

import httpx
import pytest

from services.orchestrator import (
    PermanentError,
    TransientError,
    UpgradeOrchestrator,
    _branch_name,
    build_container_name,
    parse_repo_url,
)
from tests.conftest import make_backend_transport, make_workspaces_client

_CONTAINER_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9\-_]{0,62}$")


def _orch(transport: httpx.MockTransport) -> UpgradeOrchestrator:
    # The /api/workspaces calls go through the typed CodingWorkspacesClient,
    # which builds its OWN transport — injecting one wired to the same mock keeps
    # a single handler simulating the whole backend.
    client = httpx.AsyncClient(transport=transport)
    return UpgradeOrchestrator(
        client,
        containers_url="http://coding-containers:8691",
        security_url="http://coding-security:8693",
        github_url="http://coding-github:8692",
        tenant_id="t-1",
        workspaces_client=make_workspaces_client(transport),
    )


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "https://github.com/nonagenticai/enterprise_mcp_server",
            ("nonagenticai", "enterprise_mcp_server"),
        ),
        ("https://github.com/owner/repo.git", ("owner", "repo")),
        ("git@github.com:owner/repo.git", ("owner", "repo")),
        ("owner/repo", ("owner", "repo")),
    ],
)
def test_parse_repo_url(url, expected):
    assert parse_repo_url(url) == expected


def test_parse_repo_url_invalid():
    with pytest.raises(PermanentError):
        parse_repo_url("not-a-repo")


def test_branch_name_sanitizes():
    b = _branch_name("fast api", "3.4.1+local")
    assert b.startswith("bulk-upgrade/")
    assert " " not in b


@pytest.mark.parametrize(
    ("job_id", "repo"),
    [
        (1, "enterprise_mcp_server"),
        (123456789, "repo.with.dots"),
        (7, "UPPER_and-mixed"),
        (42, "x" * 200),  # absurdly long repo → must still fit the pattern
        (99, ""),  # empty slug → bare bulk-<id>
    ],
)
def test_build_container_name_is_dns_safe(job_id, repo):
    name = build_container_name(job_id, repo)
    assert _CONTAINER_NAME_RE.match(name), name
    assert len(name) <= 63
    # job id (the uniqueness guarantee) survives truncation of a long slug.
    assert f"bulk-{job_id}" in name


def test_build_container_name_is_unique_per_job():
    a = build_container_name(1, "repo")
    b = build_container_name(2, "repo")
    assert a != b


async def test_run_pins_isolated_container_name_when_job_id_given():
    import json as _json

    seen_provision: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.method == "GET" and url.endswith("/status"):
            return httpx.Response(200, json={"container_status": "running"})
        if request.method == "POST" and url.endswith("/api/workspaces/"):
            body = _json.loads(request.content)
            seen_provision.update(body)
            return httpx.Response(201, json={"container_name": body.get("container_name", "c")})
        if "/upgrade" in url:
            return httpx.Response(
                200,
                json={
                    "exit_code": 0,
                    "results": [{"success": True}],
                    "pr_url": "https://github.com/o/r/pull/3",
                    "pr_number": 3,
                },
            )
        if "/exec" in url:
            return httpx.Response(200, json={"exit_code": 0})
        if "/api/security/scan/" in url:
            return httpx.Response(
                200, json={"result": {"critical_count": 0, "security_grade": "A"}}
            )
        if url.endswith("/mark-pull-request-ready"):
            return httpx.Response(200, json={"success": True})
        if request.method == "DELETE":
            return httpx.Response(200, json={})
        return httpx.Response(404, json={})

    orch = _orch(httpx.MockTransport(handler))
    outcome = await orch.run(
        repo_url="https://github.com/o/enterprise_mcp_server",
        package="fastmcp",
        target="3.4.1",
        job_id=77,
    )
    assert outcome.status == "pr_open"
    # The worker pinned an isolated, job-scoped name (not a bare repo-dedup).
    assert seen_provision.get("container_name") == "bulk-77-enterprise-mcp-server"


async def test_scan_404_is_transient_not_permanent():
    # Container vanished mid-flight (concurrent teardown / pod redeploy) → the
    # scan 404s. Must re-queue (transient), not die with the draft PR stuck.
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.method == "GET" and url.endswith("/status"):
            return httpx.Response(200, json={"container_status": "running"})
        if request.method == "POST" and url.endswith("/api/workspaces/"):
            return httpx.Response(201, json={"container_name": "c"})
        if "/upgrade" in url:
            return httpx.Response(
                200,
                json={
                    "exit_code": 0,
                    "results": [{"success": True}],
                    "pr_url": "https://github.com/o/r/pull/4",
                    "pr_number": 4,
                },
            )
        if "/exec" in url:
            return httpx.Response(200, json={"exit_code": 0})
        if "/api/security/scan/" in url:
            return httpx.Response(404, json={"detail": "Container 'c' not found"})
        if request.method == "DELETE":
            return httpx.Response(200, json={})
        return httpx.Response(404, json={})

    orch = _orch(httpx.MockTransport(handler))
    with pytest.raises(TransientError, match="vanished mid-flight"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_exec_404_is_transient_not_permanent():
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.method == "GET" and url.endswith("/status"):
            return httpx.Response(200, json={"container_status": "running"})
        if request.method == "POST" and url.endswith("/api/workspaces/"):
            return httpx.Response(201, json={"container_name": "c"})
        if "/upgrade" in url:
            return httpx.Response(
                200,
                json={
                    "exit_code": 0,
                    "results": [{"success": True}],
                    "pr_url": "https://github.com/o/r/pull/5",
                    "pr_number": 5,
                },
            )
        if "/exec" in url:
            return httpx.Response(404, json={"detail": "Container 'c' not found"})
        if request.method == "DELETE":
            return httpx.Response(200, json={})
        return httpx.Response(404, json={})

    orch = _orch(httpx.MockTransport(handler))
    with pytest.raises(TransientError, match="vanished mid-flight"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


def _orch_fast_ready(
    transport: httpx.MockTransport, *, timeout: float = 1.0
) -> UpgradeOrchestrator:
    client = httpx.AsyncClient(transport=transport)
    return UpgradeOrchestrator(
        client,
        containers_url="http://coding-containers:8691",
        security_url="http://coding-security:8693",
        github_url="http://coding-github:8692",
        tenant_id="t-1",
        container_ready_timeout_seconds=timeout,
        container_ready_poll_seconds=0.0,
        workspaces_client=make_workspaces_client(transport),
    )


async def test_waits_for_container_ready_before_upgrade():
    # provision returns immediately; the pod reports 'creating' twice then
    # 'running'. The upgrade must not fire until 'running'.
    statuses = iter(["creating", "creating", "running"])
    order: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.method == "GET" and url.endswith("/status"):
            s = next(statuses, "running")
            order.append(f"status:{s}")
            return httpx.Response(200, json={"container_status": s})
        if request.method == "POST" and url.endswith("/api/workspaces/"):
            return httpx.Response(201, json={"container_name": "c"})
        if "/upgrade" in url:
            order.append("upgrade")
            return httpx.Response(
                200,
                json={
                    "exit_code": 0,
                    "results": [{"success": True}],
                    "pr_url": "https://github.com/o/r/pull/6",
                    "pr_number": 6,
                },
            )
        if "/exec" in url:
            return httpx.Response(200, json={"exit_code": 0})
        if "/api/security/scan/" in url:
            return httpx.Response(
                200, json={"result": {"critical_count": 0, "security_grade": "A"}}
            )
        if url.endswith("/mark-pull-request-ready"):
            return httpx.Response(200, json={"success": True})
        if request.method == "DELETE":
            return httpx.Response(200, json={})
        return httpx.Response(404, json={})

    orch = _orch_fast_ready(httpx.MockTransport(handler))
    outcome = await orch.run(repo_url="o/r", package="p", target="1.0")
    assert outcome.status == "pr_open"
    # The upgrade fired only after the pod went 'running' (after two 'creating').
    assert order == ["status:creating", "status:creating", "status:running", "upgrade"]


async def test_container_ready_status_error_is_transient():
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.method == "GET" and url.endswith("/status"):
            return httpx.Response(200, json={"container_status": "error"})
        if request.method == "POST" and url.endswith("/api/workspaces/"):
            return httpx.Response(201, json={"container_name": "c"})
        if request.method == "DELETE":
            return httpx.Response(200, json={})
        return httpx.Response(404, json={})

    orch = _orch_fast_ready(httpx.MockTransport(handler))
    with pytest.raises(TransientError, match="failed to start"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_container_ready_timeout_is_transient():
    # Never reaches 'running' within the timeout → transient (re-queue), not dead.
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.method == "GET" and url.endswith("/status"):
            return httpx.Response(200, json={"container_status": "creating"})
        if request.method == "POST" and url.endswith("/api/workspaces/"):
            return httpx.Response(201, json={"container_name": "c"})
        if request.method == "DELETE":
            return httpx.Response(200, json={})
        return httpx.Response(404, json={})

    orch = _orch_fast_ready(httpx.MockTransport(handler), timeout=0.05)
    with pytest.raises(TransientError, match="not ready within"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_run_success_promotes_draft_to_ready():
    steps: list[str] = []

    async def on_step(status, **fields):
        steps.append(status)

    orch = _orch(make_backend_transport())
    outcome = await orch.run(
        repo_url="https://github.com/o/r",
        package="fastmcp",
        target="3.4.1",
        on_step=on_step,
    )
    assert outcome.status == "pr_open"
    assert outcome.pr_url == "https://github.com/o/r/pull/7"
    assert outcome.pr_number == 7
    assert outcome.container_name == "bulk-c-1"
    assert steps == ["upgrading", "verifying"]


async def test_run_no_change_is_done():
    # upgrade succeeds but yields no PR (already at target) → terminal 'done'.
    transport = make_backend_transport(
        upgrade={"exit_code": 0, "results": [{"success": True}], "pr_url": None}
    )
    orch = _orch(transport)
    outcome = await orch.run(repo_url="o/r", package="p", target="1.0")
    assert outcome.status == "done"
    assert outcome.pr_url is None


async def test_provision_5xx_is_transient():
    orch = _orch(make_backend_transport(fail="provision"))
    with pytest.raises(TransientError):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_provision_4xx_is_permanent():
    orch = _orch(make_backend_transport(fail="provision-4xx"))
    with pytest.raises(PermanentError):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_upgrade_nonzero_exit_is_permanent():
    orch = _orch(make_backend_transport(fail="upgrade-exit"))
    with pytest.raises(PermanentError):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_tests_failure_is_permanent_oracle_red():
    orch = _orch(make_backend_transport(fail="tests"))
    with pytest.raises(PermanentError, match="tests failed"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_scan_failure_is_permanent_oracle_red():
    orch = _orch(make_backend_transport(fail="scan"))
    with pytest.raises(PermanentError, match="security scan failed"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


# --- _gate_scan fails closed on a non-trustworthy verdict -------------------


def test_gate_scan_passes_on_clean_verdict():
    # Both keys present, not degraded, no criticals → no raise.
    UpgradeOrchestrator._gate_scan({"critical_count": 0, "security_grade": "A"})


def test_gate_scan_blocks_when_degraded():
    with pytest.raises(PermanentError, match="degraded"):
        UpgradeOrchestrator._gate_scan(
            {"critical_count": 0, "security_grade": "A", "scan_degraded": True}
        )


def test_gate_scan_blocks_on_missing_keys():
    # An error envelope without the verdict keys previously defaulted to 0/None
    # and silently passed; it must now fail closed.
    with pytest.raises(PermanentError, match="unexpected shape"):
        UpgradeOrchestrator._gate_scan({"detail": "scan service unavailable"})


def test_gate_scan_blocks_on_partial_shape():
    with pytest.raises(PermanentError, match="unexpected shape"):
        UpgradeOrchestrator._gate_scan({"critical_count": 0})  # no grade


def test_gate_scan_still_blocks_critical_and_grade_f():
    with pytest.raises(PermanentError, match="security scan failed"):
        UpgradeOrchestrator._gate_scan({"critical_count": 1, "security_grade": "A"})
    with pytest.raises(PermanentError, match="security scan failed"):
        UpgradeOrchestrator._gate_scan({"critical_count": 0, "security_grade": "F"})


def test_gate_scan_blocks_on_grade_d():
    # Full tightening: D is no longer a passable grade (previously only F failed).
    with pytest.raises(PermanentError, match="security scan failed"):
        UpgradeOrchestrator._gate_scan({"critical_count": 0, "security_grade": "D"})


def test_gate_scan_passes_on_grade_c():
    # C is still passable — the gate only fails at D and below.
    UpgradeOrchestrator._gate_scan({"critical_count": 0, "security_grade": "C"})


# --- _gate_tests: no tests is not a pass -----------------------------------


def test_gate_tests_passes_on_clean_exit():
    UpgradeOrchestrator._gate_tests({"exit_code": 0}, "python")


def test_gate_tests_blocks_on_no_tests_collected():
    # pytest exit 5 == "no tests collected"; no longer tolerated.
    with pytest.raises(PermanentError, match="tests failed"):
        UpgradeOrchestrator._gate_tests({"exit_code": 5}, "python")


def test_gate_tests_blocks_on_failure():
    with pytest.raises(PermanentError, match="tests failed"):
        UpgradeOrchestrator._gate_tests({"exit_code": 1, "stderr": "boom"}, "node")


# cargo/go exit 0 on an empty suite — the gate must inspect stdout for them.


def test_gate_tests_blocks_rust_zero_tests():
    # `cargo test` exits 0 but ran nothing.
    out = "   Compiling foo v0.1.0\n    Finished test\n\nrunning 0 tests\n\ntest result: ok. 0 passed; 0 failed; 0 ignored\n"
    with pytest.raises(PermanentError, match="no tests were run"):
        UpgradeOrchestrator._gate_tests({"exit_code": 0, "stdout": out}, "rust")


def test_gate_tests_passes_rust_with_tests():
    out = "running 3 tests\ntest a ... ok\ntest b ... ok\ntest c ... ok\n\ntest result: ok. 3 passed; 0 failed\n"
    UpgradeOrchestrator._gate_tests({"exit_code": 0, "stdout": out}, "rust")


def test_gate_tests_blocks_rust_with_no_running_line():
    # An empty crate prints no `running N tests` line at all → still no tests.
    with pytest.raises(PermanentError, match="no tests were run"):
        UpgradeOrchestrator._gate_tests({"exit_code": 0, "stdout": "    Finished test\n"}, "rust")


def test_gate_tests_blocks_go_all_no_test_files():
    out = "?   \texample.com/foo\t[no test files]\nok  \texample.com/bar\t[no test files]\n"
    with pytest.raises(PermanentError, match="no tests were run"):
        UpgradeOrchestrator._gate_tests({"exit_code": 0, "stdout": out}, "go")


def test_gate_tests_passes_go_with_real_package():
    # One package ran tests (ok with a duration, no `[no test files]` marker).
    out = "?   \texample.com/foo\t[no test files]\nok  \texample.com/bar\t0.512s\n"
    UpgradeOrchestrator._gate_tests({"exit_code": 0, "stdout": out}, "go")


async def test_run_tests_fails_closed_on_unknown_ecosystem():
    # An unrecognised ecosystem has no wired runner → the test gate fails closed
    # (early return, no container exec), so the transport is never hit.
    orch = _orch(httpx.MockTransport(lambda req: httpx.Response(500)))
    result = await orch._run_tests("ctr", "haskell")
    assert result["exit_code"] != 0


async def test_cross_major_uses_upgrade_major_endpoint():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        seen.append(f"{request.method} {url}")
        if request.method == "GET" and url.endswith("/status"):
            return httpx.Response(200, json={"container_status": "running"})
        if request.method == "POST" and url.endswith("/api/workspaces/"):
            return httpx.Response(201, json={"container_name": "c"})
        if "/upgrade-major" in url:
            return httpx.Response(
                200,
                json={
                    "exit_code": 0,
                    "results": [{"success": True}],
                    "pr_url": "https://github.com/o/r/pull/9",
                    "pr_number": 9,
                },
            )
        if "/exec" in url:
            return httpx.Response(200, json={"exit_code": 0})
        if "/api/security/scan/" in url:
            return httpx.Response(
                200, json={"result": {"critical_count": 0, "security_grade": "A"}}
            )
        if url.endswith("/mark-pull-request-ready"):
            return httpx.Response(200, json={"success": True})
        if request.method == "DELETE":
            return httpx.Response(200, json={})
        return httpx.Response(404, json={})

    orch = _orch(httpx.MockTransport(handler))
    outcome = await orch.run(repo_url="o/r", package="p", target="4.0.0", mode="cross-major")
    assert outcome.status == "pr_open"
    assert any("/upgrade-major" in s for s in seen)


async def test_teardown_called_even_on_oracle_failure():
    deleted: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.method == "GET" and url.endswith("/status"):
            return httpx.Response(200, json={"container_status": "running"})
        if request.method == "DELETE":
            deleted.append(url)
            return httpx.Response(200, json={})
        if url.endswith("/api/workspaces/"):
            return httpx.Response(201, json={"container_name": "c-teardown"})
        if "/upgrade" in url:
            return httpx.Response(
                200,
                json={
                    "exit_code": 0,
                    "results": [{"success": True}],
                    "pr_url": "https://github.com/o/r/pull/1",
                    "pr_number": 1,
                },
            )
        if "/exec" in url:
            return httpx.Response(200, json={"exit_code": 1, "stderr": "boom"})
        return httpx.Response(404, json={})

    orch = _orch(httpx.MockTransport(handler))
    with pytest.raises(PermanentError):
        await orch.run(repo_url="o/r", package="p", target="1.0")
    assert deleted and "c-teardown" in deleted[0]


# ─────────────────────────────────────────────────────────────────────────────
# Typed CodingWorkspacesClient adoption
#
# The client RAISES ServiceError on every status >= 400 instead of handing back a
# response, and fixes its transport timeout at construction. These tests pin the
# three places where that difference could silently change a job's verdict.
# ─────────────────────────────────────────────────────────────────────────────


def _ws_handler(
    *,
    status_responses=None,
    exec_response=None,
    delete_status: int = 200,
    on_request=None,
):
    """A full-sequence handler with the workspace steps individually steerable."""
    statuses = iter(status_responses or [])

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if on_request is not None:
            on_request(request)
        if request.method == "GET" and "/status" in url:
            return next(statuses, httpx.Response(200, json={"container_status": "running"}))
        if request.method == "POST" and url.endswith("/api/workspaces/"):
            return httpx.Response(201, json={"container_name": "c"})
        if request.method == "DELETE" and "/api/workspaces/" in url:
            return httpx.Response(delete_status, json={"detail": "teardown exploded"})
        if "/api/dependencies/" in url:
            return httpx.Response(
                200,
                json={
                    "exit_code": 0,
                    "results": [{"success": True}],
                    "pr_url": "https://github.com/o/r/pull/11",
                    "pr_number": 11,
                },
            )
        if "/exec" in url:
            return exec_response or httpx.Response(200, json={"exit_code": 0})
        if "/api/security/scan/" in url:
            return httpx.Response(
                200, json={"result": {"critical_count": 0, "security_grade": "A"}}
            )
        if url.endswith("/mark-pull-request-ready"):
            return httpx.Response(200, json={"success": True})
        return httpx.Response(404, json={"detail": f"unmocked {request.method} {url}"})

    return httpx.MockTransport(handler)


# --- 1. the readiness poll -------------------------------------------------


async def test_readiness_poll_keeps_waiting_while_the_pod_is_not_running():
    """A non-terminal status is "keep waiting", NOT a failure.

    This is the loop behaviour the typed client could most easily destroy: the
    client raises on every non-2xx, so the only thing keeping the loop alive is
    that a 200-with-a-pending-status still returns normally. An unrecognised
    status string must be treated the same as ``creating`` — keep polling — and
    never be mistaken for readiness.
    """
    polls: list[str] = []
    seq = [
        httpx.Response(200, json={"container_status": "creating"}),
        httpx.Response(200, json={"status": "pending"}),  # alternate key, still waiting
        httpx.Response(200, json={}),  # empty verdict → still waiting, never "ready"
        httpx.Response(200, json={"container_status": "running"}),
    ]
    transport = _ws_handler(
        status_responses=seq,
        on_request=lambda r: polls.append(str(r.url)) if "/status" in str(r.url) else None,
    )
    orch = _orch_fast_ready(transport)
    outcome = await orch.run(repo_url="o/r", package="p", target="1.0")
    assert outcome.status == "pr_open"
    assert len(polls) == 4, "the loop must have kept waiting through all three pending polls"


async def test_readiness_poll_404_requeues_instead_of_crashing_the_job():
    """A 404 while polling is the container vanishing, and stays TRANSIENT.

    Behaviour note, deliberately pinned: this is NOT "keep polling". The status
    route is only ever polled for a container ``provision`` just returned, so a
    404 means it was torn down under us — the pre-existing ``container_op=True``
    contract mapped that to ``TransientError`` (re-queue + re-provision) and the
    typed client must not change it. What must NOT happen is the raised
    ``ServiceError`` escaping unclassified and killing the job as an
    ``unexpected`` failure, which is what a naive adoption produces.
    """
    transport = _ws_handler(status_responses=[httpx.Response(404, json={"detail": "gone"})])
    orch = _orch_fast_ready(transport)
    with pytest.raises(TransientError, match="vanished mid-flight"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_readiness_poll_5xx_is_transient_not_permanent():
    transport = _ws_handler(status_responses=[httpx.Response(503, json={"detail": "down"})])
    orch = _orch_fast_ready(transport)
    with pytest.raises(TransientError, match="503"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


# --- 2. the best-effort teardown in `finally:` -----------------------------


async def test_teardown_service_error_does_not_mask_the_real_verdict():
    """The oracle's PermanentError must survive a teardown that 500s.

    The delete lives in ``run``'s ``finally``. With raw httpx it returned a
    response; the typed client RAISES, and an exception raised in a ``finally``
    REPLACES the exception in flight — the job would be recorded as an
    infrastructure failure and the real "tests failed" verdict lost.
    """
    transport = _ws_handler(
        exec_response=httpx.Response(200, json={"exit_code": 1, "stderr": "boom"}),
        delete_status=500,
    )
    orch = _orch(transport)
    with pytest.raises(PermanentError, match="tests failed"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_teardown_service_error_does_not_fail_a_green_job():
    """A 500 on teardown must not turn a promoted PR into a failed job."""
    transport = _ws_handler(delete_status=500)
    orch = _orch(transport)
    outcome = await orch.run(repo_url="o/r", package="p", target="1.0")
    assert outcome.status == "pr_open"


async def test_teardown_connection_error_is_swallowed():
    """Connect errors survive the client's retries and reach the ``finally``."""

    inner = _ws_handler()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            raise httpx.ConnectError("containers service unreachable", request=request)
        return inner.handle_request(request)

    orch = _orch(httpx.MockTransport(handler))
    outcome = await orch.run(repo_url="o/r", package="p", target="1.0")
    assert outcome.status == "pr_open"


# --- 3./4. the exec call: timeout budget + verdict preservation ------------


def _budget_orch(*, test_timeout_seconds: int, http_timeout_seconds: float):
    return UpgradeOrchestrator(
        httpx.AsyncClient(),
        containers_url="http://coding-containers:8691",
        security_url="http://coding-security:8693",
        github_url="http://coding-github:8692",
        tenant_id="t-1",
        test_timeout_seconds=test_timeout_seconds,
        http_timeout_seconds=http_timeout_seconds,
    )


def test_exec_transport_budget_never_truncates_the_configured_test_budget():
    """``ServiceClient`` fixes its timeout at CONSTRUCTION — 30s by default.

    The exec body carries the job's in-container budget; the transport must
    outlive it or the client aborts before coding-containers can report the
    result, converting a legitimate test verdict into an infrastructure error.
    """
    # Default settings: unchanged from the pre-adoption shared httpx client.
    default = _budget_orch(test_timeout_seconds=300, http_timeout_seconds=900.0)
    assert default._exec_http_timeout_seconds == 900.0
    assert default._ws(timeout=default._exec_http_timeout_seconds).timeout == 900.0

    # A test budget LONGER than the transport timeout must stretch the transport,
    # not be silently truncated by it.
    long_tests = _budget_orch(test_timeout_seconds=1200, http_timeout_seconds=900.0)
    assert long_tests._exec_http_timeout_seconds == 1260.0  # budget + headroom
    assert long_tests._ws(timeout=long_tests._exec_http_timeout_seconds).timeout == 1260.0

    # Never the library's 30s default.
    assert default._exec_http_timeout_seconds > 30.0
    assert long_tests._exec_http_timeout_seconds > 30.0


def test_workspace_clients_are_cached_per_budget_and_closed():
    orch = _budget_orch(test_timeout_seconds=300, http_timeout_seconds=900.0)
    a = orch._ws(timeout=900.0)
    b = orch._ws(timeout=900.0)
    c = orch._ws(timeout=900.0, max_retries=1)
    assert a is b, "same budget must reuse one connection pool"
    assert c is not a, "a different retry policy is a different client"
    assert len(orch._ws_owned) == 2


async def test_aclose_releases_every_owned_client():
    orch = _budget_orch(test_timeout_seconds=300, http_timeout_seconds=900.0)
    orch._ws(timeout=900.0)
    orch._ws(timeout=360.0)
    assert len(orch._ws_owned) == 2
    await orch.aclose()
    assert orch._ws_owned == {}


async def test_exec_body_still_carries_the_jobs_in_container_timeout():
    """``timeout`` is a BODY field, not an httpx kwarg — ``exec(**body)`` is JSON."""
    import json as _json

    bodies: list[dict] = []

    def capture(request: httpx.Request) -> None:
        if "/exec" in str(request.url):
            bodies.append(_json.loads(request.content))

    orch = UpgradeOrchestrator(
        httpx.AsyncClient(transport=_ws_handler()),
        containers_url="http://coding-containers:8691",
        security_url="http://coding-security:8693",
        github_url="http://coding-github:8692",
        tenant_id="t-1",
        test_timeout_seconds=42,
        workspaces_client=make_workspaces_client(_ws_handler(on_request=capture)),
    )
    await orch.run(repo_url="o/r", package="p", target="1.0")
    assert bodies, "exec never fired — this test proves nothing"
    assert bodies[0]["timeout"] == 42
    assert bodies[0]["workdir"] == "/workspace"
    assert bodies[0]["command"] == ["/workspace/.venv/bin/python", "-m", "pytest", "-q"]


async def test_a_red_test_run_stays_a_test_failure_not_an_infra_error():
    """HTTP 200 + a non-zero exit code is a VERDICT, and must stay in-band.

    The typed client only raises on >= 400, so a failing suite still returns a
    body and still reaches ``_gate_tests`` — the job dies as "tests failed", not
    as a transport failure that would re-queue and burn attempts.
    """
    transport = _ws_handler(
        exec_response=httpx.Response(200, json={"exit_code": 1, "stdout": "1 failed"})
    )
    orch = _orch(transport)
    with pytest.raises(PermanentError, match="tests failed"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_exec_5xx_is_transient_and_exec_4xx_is_permanent():
    """The ServiceError→transient/permanent split must match the old httpx one."""
    orch = _orch(_ws_handler(exec_response=httpx.Response(503, json={"detail": "docker down"})))
    with pytest.raises(TransientError, match="503"):
        await orch.run(repo_url="o/r", package="p", target="1.0")

    orch = _orch(_ws_handler(exec_response=httpx.Response(400, json={"detail": "bad command"})))
    with pytest.raises(PermanentError, match="400"):
        await orch.run(repo_url="o/r", package="p", target="1.0")


async def test_provision_empty_body_fails_closed_instead_of_crashing():
    """A 204/empty provision decodes to ``None`` through the typed client.

    Normalising it to ``{}`` keeps the existing "no container_name" verdict
    instead of an ``AttributeError`` recorded as an unexpected failure.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and str(request.url).endswith("/api/workspaces/"):
            return httpx.Response(204)
        return httpx.Response(200, json={})

    orch = _orch(httpx.MockTransport(handler))
    with pytest.raises(PermanentError, match="no container_name"):
        await orch.run(repo_url="o/r", package="p", target="1.0")
