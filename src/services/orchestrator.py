"""Per-job HTTP orchestrator.

Drives one ``repo_jobs`` row through the *already-proven* backend path —
coding-containers (provision/exec/teardown) + coding-security (upgrade + draft
PR via coding-github's #27/#29 direct-push routing) — bypassing n8n/Planka.

The orchestrator owns NO database state. It reports progress through an
optional ``on_step`` async callback (the worker wires it to a DB write+commit)
and signals the terminal outcome by returning an :class:`UpgradeOutcome` or
raising :class:`TransientError` / :class:`PermanentError`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx
from coding_client.auth.outbound import TenantMissingError, tenant_headers
from coding_client.clients import CodingWorkspacesClient, ServiceError

from config.settings import PLATFORM_REPO_OWNER, PLATFORM_TENANT_ID

logger = logging.getLogger(__name__)

# How much longer the HTTP call may take than the in-container budget it carries.
# ``POST /exec`` passes ``timeout`` in the BODY (the container-side command
# budget). The HTTP read timeout must outlive it, otherwise the client aborts
# before coding-containers can report the timed-out result and a legitimate
# "tests timed out" verdict is downgraded to an infrastructure error.
_EXEC_HTTP_HEADROOM_SECONDS = 60.0

# Manifest manager per ecosystem (coding-security/containers `manager` enum).
_ECOSYSTEM_MANAGER = {
    "python": "uv",
    "node": "npm",
    "rust": "cargo",
    "go": "go",
}

# Per-ecosystem in-container test command. Python uses the upgraded venv python
# (the exec whitelist strips the path → 'python' is allowed). node/rust/go run
# through the coding-containers exec whitelist (npm/cargo/go are whitelisted).
_ECOSYSTEM_TEST_CMD = {
    "python": ["/workspace/.venv/bin/python", "-m", "pytest", "-q"],
    "node": ["npm", "test"],
    "rust": ["cargo", "test"],
    "go": ["go", "test", "./..."],
}
# Only a clean exit passes the test gate. pytest exit 5 ("no tests collected")
# is NO LONGER tolerated: a submission that runs zero tests is not verified and
# must not pass the oracle (tournament policy: no tests is not a pass).
_TOLERATED_TEST_EXIT = {0}
# Ecosystems whose test runner EXITS 0 even when it ran zero tests. python
# (pytest exit 5) and node (`npm test` with no script exits non-zero) already
# fail on no-tests via the exit code; cargo/go return 0 on an empty suite, so we
# must inspect their stdout to honour the no-tests=fail policy for them.
_ZERO_TEST_EXIT_ZERO = {"rust", "go"}
# cargo prints a `running N tests` line per section (unit/integration/doc).
_CARGO_RUNNING_RE = re.compile(r"^\s*running (\d+) tests?\b", re.MULTILINE)
# `go test ./...` prints one package line each; a package with no tests reads
# `?   pkg   [no test files]` (or, when built, `ok  pkg  [no test files]`).
_GO_NO_TEST_FILES = "[no test files]"
# A go package whose tests actually ran shows `ok  pkg  0.5s` (no marker) or a
# FAIL line; `--- PASS`/`--- FAIL` appear with `-v`.
_GO_RAN_RE = re.compile(r"^(?:ok|FAIL|---\s+(?:PASS|FAIL))\b", re.MULTILINE)
# Security grades that fail the oracle scan gate (D and F are not passable).
_FAILING_GRADES = {"D", "F"}

StepCallback = Callable[..., Awaitable[None]]


class OrchestratorError(Exception):
    """Base for orchestrator failures."""


class TransientError(OrchestratorError):
    """A retryable failure (network blip, 5xx, timeout) — re-queue with backoff."""


class PermanentError(OrchestratorError):
    """A non-retryable failure (bad repo, upgrade unresolvable, oracle red) — kill."""


@dataclass
class UpgradeOutcome:
    """Terminal-success result of a job."""

    status: str  # 'pr_open' (PR opened+ready) or 'done' (no change needed)
    container_name: str | None = None
    branch_name: str | None = None
    pr_url: str | None = None
    pr_number: int | None = None
    is_cross_repository: bool | None = None


def parse_repo_url(repo_url: str) -> tuple[str, str]:
    """Return ``(owner, repo)`` from a GitHub URL or ``owner/repo`` shorthand."""
    cleaned = repo_url.strip().removesuffix(".git").rstrip("/")
    m = re.search(r"github\.com[:/]+([^/]+)/([^/]+)$", cleaned)
    if m:
        return m.group(1), m.group(2)
    parts = cleaned.split("/")
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    raise PermanentError(f"cannot parse owner/repo from repo_url: {repo_url!r}")


def _branch_name(package: str, target: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", f"{package}-{target}").strip("-")
    return f"bulk-upgrade/{slug}"[:240]


def build_container_name(job_id: int, repo: str) -> str:
    """Derive a per-job, caller-isolated container name.

    coding-containers dedups an *un-named* provision by ``repo_url`` — so a bare
    ``{repo_url}`` request can latch onto a container another caller (e.g. the
    n8n IT-flow1 flow) created and is about to tear down, yielding a mid-flight
    ``404 Container not found`` on the oracle scan. Pinning a name unique to this
    job row (``job.id`` is the immutable PK) gives the worker a container it owns
    exclusively for the job's lifetime, and makes re-attempts idempotent (same
    name → same container, re-provisioned fresh if a prior attempt tore it down).

    Must satisfy coding-containers' ``GitHubRepoRequest.container_name`` pattern
    ``^[a-zA-Z][a-zA-Z0-9\\-_]{0,62}$``: starts with a letter, ≤63 chars, only
    ``[A-Za-z0-9-_]``. The job id sits right after the ``bulk-`` prefix so it
    survives truncation of a long repo slug (uniqueness is never lost).
    """
    slug = re.sub(r"[^A-Za-z0-9]+", "-", repo).strip("-").lower()
    name = f"bulk-{job_id}-{slug}" if slug else f"bulk-{job_id}"
    return name[:63].rstrip("-")


def _service_token() -> str | None:
    """This service's own license, read from the environment per call.

    Read here rather than through ``Settings`` on purpose: ``Settings`` is
    constructed once at startup and the worker is a long-lived process, so a
    rotated Secret would need a pod restart to take effect. The ``or None``
    normalises a present-but-blank variable, which is the realistic
    misconfiguration — an empty ``Bearer `` on the wire is a malformed
    credential, not "no credential", and would burn a license-introspection
    round-trip before being rejected.
    """
    return os.getenv("CODING_SERVICE_TOKEN") or None


def _classify_http_error(exc: Exception) -> OrchestratorError:
    """Map an httpx error to transient/permanent."""
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        body = exc.response.text[:500]
        if code >= 500 or code == 429:
            return TransientError(f"{code} from {exc.request.url}: {body}")
        return PermanentError(f"{code} from {exc.request.url}: {body}")
    if isinstance(exc, httpx.TimeoutException | httpx.TransportError):
        return TransientError(f"{type(exc).__name__}: {exc}")
    return PermanentError(f"{type(exc).__name__}: {exc}")


def _classify_service_error(exc: ServiceError, what: str) -> OrchestratorError:
    """Map a :class:`ServiceError` to transient/permanent.

    ``CodingWorkspacesClient`` never returns a response — it raises on every
    status >= 400 — so this is the exact counterpart of
    :func:`_classify_http_error`'s ``HTTPStatusError`` arm and MUST keep the
    same split (5xx/429 retryable, everything else fatal). Anything else would
    silently change which failures burn a job's attempts.
    """
    code = exc.status_code
    body = "" if exc.body is None else str(exc.body)[:500]
    if code >= 500 or code == 429:
        return TransientError(f"{code} from {what}: {body}")
    return PermanentError(f"{code} from {what}: {body}")


class UpgradeOrchestrator:
    """Runs the provision → upgrade(+draft PR) → oracle → promote → teardown path."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        containers_url: str,
        security_url: str,
        github_url: str,
        tenant_id: str,
        platform_repo_owner: str = PLATFORM_REPO_OWNER,
        platform_tenant_id: str = PLATFORM_TENANT_ID,
        upgrade_timeout_seconds: int = 600,
        test_timeout_seconds: int = 300,
        container_ready_timeout_seconds: float = 180.0,
        container_ready_poll_seconds: float = 3.0,
        http_timeout_seconds: float = 900.0,
        workspaces_client: CodingWorkspacesClient | None = None,
    ) -> None:
        self.client = client
        self.containers_url = containers_url.rstrip("/")
        self.security_url = security_url.rstrip("/")
        self.github_url = github_url.rstrip("/")
        self.tenant_id = tenant_id
        # GitHub owners are case-insensitive, and BOTH sides of the compare in
        # ``_tenant_for`` must be folded: this one because the setting is
        # env-overridable, so ``PLATFORM_REPO_OWNER=NonAgenticAI`` is a legal
        # spelling of the same org, and dropping the fold here would mean the
        # platform swap never fires for ANY repo — the permanent 403 this change
        # exists to prevent, arrived at by configuration rather than by a URL.
        # ``or ""`` normalises None, and the empty result is NOT a wildcard:
        # ``_tenant_for`` refuses to match on a blank owner (see there).
        self.platform_repo_owner = (platform_repo_owner or "").strip().casefold()
        self.platform_tenant_id = platform_tenant_id
        self.upgrade_timeout_seconds = upgrade_timeout_seconds
        self.test_timeout_seconds = test_timeout_seconds
        self.container_ready_timeout_seconds = container_ready_timeout_seconds
        self.container_ready_poll_seconds = container_ready_poll_seconds
        # The workspace calls no longer ride the injected httpx client; they go
        # through the typed CodingWorkspacesClient, which builds its OWN
        # httpx.AsyncClient with a timeout FIXED AT CONSTRUCTION. This value is
        # the timeout the injected client used to carry for those calls, so the
        # short-budget workspace calls behave exactly as before.
        self.http_timeout_seconds = float(http_timeout_seconds)
        # Injected (tests / a caller that owns the client's lifecycle). When set
        # it serves every budget and is never closed by ``aclose``.
        self._ws_injected = workspaces_client
        # Clients this orchestrator constructed, keyed by (timeout, max_retries).
        self._ws_owned: dict[tuple[float, int], CodingWorkspacesClient] = {}

    @property
    def _headers(self) -> dict[str, str]:
        """Identity headers for every backend call this orchestrator makes.

        Two separate headers, and the separation is the whole mechanism:

        * ``X-Tenant-Id`` — the operator tenant the job belongs to. This is a
          claim about *whose resource* we are touching, and it is what
          coding-containers uses to decide which container we may exec into.
        * ``Authorization: Bearer`` — this service's OWN license, proving *who
          is making the claim*. The backend honours the asserted tenant only
          when coding-repo-jobs' ``subject_id`` is in its
          ``CODING_ONBEHALF_SUBJECTS`` allowlist.

        Until now only the first was sent, which is an unverified assertion that
        any pod in the mesh could make about any tenant. It worked solely because
        the three backends still trust the header; the moment one of them sets
        ``TRUST_TENANT_HEADER=false`` this worker goes anonymous. That is not a
        cosmetic failure here — the orchestrator's ``finally`` tears the
        container down, and `_classify_http_error` maps a 401/403 to
        ``PermanentError``, so every claimed job would burn its attempts and die
        with a draft PR stranded un-promoted on the repo.

        Putting the token into ``X-Tenant-Id`` instead would be worse than
        omitting it: the backend would resolve the tenant from the token and
        attribute every operator's containers to coding-repo-jobs itself.

        The tenant is REQUIRED (:func:`tenant_headers` raises rather than
        defaulting). It scopes the resource, so a guessed or blank tenant is a
        cross-tenant authorisation bug, not a lost log line — and
        ``TenantMissingError`` is deliberately outside the ``httpx.HTTPError``
        hierarchy these call sites catch, so it cannot be reclassified into a
        retryable-looking transient failure.

        This property is the JOB-tenant path — coding-containers and
        coding-security, whose resources belong to the operator. A call that
        names a repository goes through :meth:`_headers_for`, which may assert
        the platform tenant instead; see :meth:`_tenant_for`.
        """
        return self._headers_for(repo_owner=None)

    def _tenant_for(self, repo_owner: str | None) -> str:
        """The tenant to assert on a call, given the repository it targets.

        coding-github refuses any call against a repository owned by
        ``platform_repo_owner`` unless the asserted tenant is one of the platform
        tenants — 403 ``platform_repo_forbidden``. The bulk spine runs as a single
        operator tenant, which is NOT one of them, so without this every
        platform-repo job would die PERMANENTLY (``_classify_http_error`` maps a
        403 to :class:`PermanentError`) with its draft PR stranded un-promoted on
        the repo, exactly the not-an-outage-that-looks-like-one failure the
        credential tests already describe.

        The swap is deliberately narrow: only for repositories that owner owns,
        and only on calls that name a repository at all (today: coding-github).
        coding-containers and coding-security scope a real per-operator resource —
        the job's own container — and asserting the platform tenant there would
        both mis-attribute it and let the job exec into a container it does not
        own. So ``repo_owner=None`` always keeps the job's tenant.

        Both empty cases raise :class:`TenantMissingError` NAMING THE SETTING.
        An empty tenant must never reach the wire: the receiving gate reads a
        blank ``X-Tenant-Id`` as "no tenant" and refuses, which surfaces as an
        opaque 403 rather than as the misconfiguration it is.

        A BLANK ``platform_repo_owner`` matches nothing. Both operands are
        normalised to ``""`` when absent, so a bare ``==`` would make an empty
        setting a wildcard that fires on any repo_url whose owner parsed empty
        (``parse_repo_url("/widget")`` → ``("", "widget")``) — re-tenanting a
        repository the platform does not own onto the platform tenant. The
        guard is truthiness on BOTH sides, not just ``is not None``.
        """
        owner = (repo_owner or "").strip().casefold()
        if owner and self.platform_repo_owner and owner == self.platform_repo_owner:
            if not self.platform_tenant_id:
                raise TenantMissingError(
                    f"Refusing to call coding-github about the {repo_owner}-owned "
                    f"repository with no platform tenant: PLATFORM_TENANT_ID "
                    f"(settings.platform_tenant_id) is empty. That owner's repos are "
                    f"restricted to the platform tenants; an operator tenant — and a "
                    f"blank one even more so — is refused with 403 "
                    f"platform_repo_forbidden."
                )
            return self.platform_tenant_id
        if not self.tenant_id:
            raise TenantMissingError(
                "Refusing to call a backend with no tenant: WORKER_TENANT_ID "
                "(settings.worker_tenant_id) is empty. The tenant scopes the "
                "resource; it is never defaulted and must never be sent blank."
            )
        return self.tenant_id

    def _headers_for(self, *, repo_owner: str | None) -> dict[str, str]:
        """:attr:`_headers`, for a call that targets ``repo_owner``'s repository."""
        return tenant_headers(
            self._tenant_for(repo_owner),
            operation="coding-repo-jobs backend call",
            service_token=_service_token(),
        )

    # ── typed workspaces client ───────────────────────────────────────────
    @property
    def _exec_http_timeout_seconds(self) -> float:
        """HTTP budget for ``POST /exec``.

        ``ServiceClient`` fixes its timeout at CONSTRUCTION — there is no
        per-request override — while the exec body carries the job's own
        in-container ``timeout``. Taking the max of the two keeps today's
        behaviour byte-for-byte at the default settings (900s transport vs a
        300s test budget) and, unlike a bare 30s default or a bare 900s
        constant, cannot truncate a job that configures a LONGER test budget
        than the transport timeout.
        """
        return max(
            self.http_timeout_seconds,
            float(self.test_timeout_seconds) + _EXEC_HTTP_HEADROOM_SECONDS,
        )

    def _ws(self, *, timeout: float, max_retries: int = 3) -> CodingWorkspacesClient:
        """Return a workspaces client for this timeout budget (cached)."""
        if self._ws_injected is not None:
            return self._ws_injected
        key = (float(timeout), max_retries)
        client = self._ws_owned.get(key)
        if client is None:
            client = CodingWorkspacesClient(
                base_url=self.containers_url,
                timeout=float(timeout),
                max_retries=max_retries,
            )
            self._ws_owned[key] = client
        return client

    async def aclose(self) -> None:
        """Close every workspaces client this orchestrator constructed.

        Each holds its own ``httpx.AsyncClient``; an orchestrator is built per
        job, so not closing them leaks a connection pool per job.
        """
        for client in self._ws_owned.values():
            try:
                await client.close()
            except Exception:  # pragma: no cover - teardown must never raise
                logger.debug("closing workspaces client failed (ignored)", exc_info=True)
        self._ws_owned.clear()

    @staticmethod
    async def _ws_call(coro: Awaitable[object], *, what: str, container_op: bool = False) -> dict:
        """Await a workspaces-client call and translate its errors.

        The client RAISES ``ServiceError`` on any status >= 400 (it never hands
        back a response), so the transient/permanent split that ``_post``
        derives from ``raise_for_status`` has to be reproduced here — including
        the ``container_op`` 404 arm, which means "the container we just
        provisioned vanished mid-flight", never a caller error.

        A 204/empty body decodes to ``None``; it is normalised to ``{}`` so the
        callers' ``.get(...)`` defaults produce their fail-closed verdict rather
        than an ``AttributeError``.
        """
        try:
            result = await coro
        except ServiceError as exc:
            if container_op and exc.status_code == 404:
                raise TransientError(f"container vanished mid-flight (404) at {what}") from exc
            raise _classify_service_error(exc, what) from exc
        except httpx.HTTPError as exc:
            # Connect/read/pool errors survive the client's own retries and are
            # re-raised as httpx exceptions.
            raise _classify_http_error(exc) from exc
        return result if isinstance(result, dict) else {}

    async def _post(
        self,
        url: str,
        json: dict,
        *,
        container_op: bool = False,
        headers: dict[str, str] | None = None,
    ) -> dict:
        """POST to a backend. ``headers`` defaults to the job-tenant identity;
        a repo-scoped caller passes :meth:`_headers_for` instead."""
        try:
            resp = await self.client.post(
                url, json=json, headers=self._headers if headers is None else headers
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            # A 404 on a call that targets the container we *just provisioned*
            # (exec / scan) means the container vanished mid-flight — a pod
            # redeploy, GC, or a concurrent teardown — never a caller error. Treat
            # it as transient so the job re-queues and re-provisions, rather than
            # dying with the draft PR stuck un-promoted.
            if (
                container_op
                and isinstance(exc, httpx.HTTPStatusError)
                and exc.response.status_code == 404
            ):
                raise TransientError(
                    f"container vanished mid-flight (404) at {exc.request.url}"
                ) from exc
            raise _classify_http_error(exc) from exc
        return resp.json()

    # NB: the raw-httpx ``_get`` helper is gone — the only GET this orchestrator
    # made was the workspace status poll, which now goes through the typed
    # client. ``_post`` stays for the coding-security / coding-github calls,
    # which are not workspace operations.

    async def _wait_container_ready(self, container_name: str) -> None:
        """Poll the container status until its k8s pod is RUNNING.

        ``provision`` returns as soon as the Deployment is *created*, not when its
        pod is Ready. Exec/upgrade against a not-yet-Running pod fails inside
        coding-containers with a misleading ``Neither Docker nor Kubernetes is
        available`` (the swallowed "no Running pod for deployment" error). We poll
        ``GET /api/workspaces/{name}/status`` for ``container_status == "running"``
        before driving the upgrade. ``error`` or a timeout is transient — re-queue
        and re-provision a fresh container rather than killing the job.

        A 404 here is NOT "not created yet" and never has been: this route is
        only polled for a container ``provision`` already returned, so a 404
        means it was torn down under us. It keeps ``container_op=True`` →
        ``TransientError`` → re-queue + re-provision, exactly as before the
        typed-client adoption. Only a non-terminal status keeps the loop
        spinning.
        """
        deadline = time.monotonic() + self.container_ready_timeout_seconds
        last = "creating"
        while time.monotonic() < deadline:
            info = await self._ws_call(
                self._ws(timeout=self.http_timeout_seconds).get_status(
                    container_name, headers=self._headers
                ),
                what=f"GET /api/workspaces/{container_name}/status",
                container_op=True,
            )
            last = (info.get("container_status") or info.get("status") or "").lower()
            if last == "running":
                return
            if last == "error":
                raise TransientError(f"container {container_name} failed to start (status=error)")
            await asyncio.sleep(self.container_ready_poll_seconds)
        raise TransientError(
            f"container {container_name} not ready within "
            f"{self.container_ready_timeout_seconds:.0f}s (last status={last!r})"
        )

    async def _delete_workspace(self, container_name: str) -> None:
        """Best-effort teardown. Runs inside ``run``'s ``finally`` — MUST NOT raise.

        ``force=true`` is a QUERY parameter (``delete_workspace(**options)``
        forwards options as params), matching the old hand-built
        ``?force=true`` URL.

        The catch is deliberately broad. Today's ``except httpx.HTTPError`` was
        wide enough because the call was raw httpx; the typed client adds two
        more ways to blow up inside this ``finally`` — ``ServiceError`` on any
        4xx/5xx, and a ``RuntimeError`` from the client constructor when
        ``containers_url`` is empty — and ANY exception escaping here would mask
        the job's real exception (an oracle ``PermanentError``, say) and lose the
        verdict. A leaked container is reclaimed by the containers service's own
        GC; a swallowed verdict is not recoverable. ``BaseException``
        (``CancelledError``) still propagates.
        """
        try:
            await self._ws(timeout=self.http_timeout_seconds).delete_workspace(
                container_name, headers=self._headers, force=True
            )
        except Exception as exc:
            logger.warning("teardown failed for workspace %s (ignored): %s", container_name, exc)

    async def run(
        self,
        *,
        repo_url: str,
        package: str,
        target: str,
        ecosystem: str = "python",
        mode: str = "in-range",
        job_id: int | None = None,
        on_step: StepCallback | None = None,
    ) -> UpgradeOutcome:
        owner, repo = parse_repo_url(repo_url)
        manager = _ECOSYSTEM_MANAGER.get(ecosystem, "uv")
        branch = _branch_name(package, target)
        # Pin a job-isolated container name so we never reuse (and get torn down
        # under) another caller's container. Falls back to repo_url-dedup when no
        # job id is supplied (ad-hoc callers / unit tests).
        pinned_name = build_container_name(job_id, repo) if job_id is not None else None
        container_name: str | None = None

        async def step(status: str, **fields: object) -> None:
            if on_step is not None:
                await on_step(status=status, **fields)

        try:
            # 1. Provision an ephemeral container with the repo cloned to /workspace.
            provision_body: dict[str, object] = {"repo_url": repo_url, "branch": "main"}
            if pinned_name:
                provision_body["container_name"] = pinned_name
            # No access_token: coding-containers mints a GitHub App clone token
            # via coding-github. There is no PAT fallback.
            info = await self._ws_call(
                self._ws(timeout=self.http_timeout_seconds).create_workspace(
                    headers=self._headers, **provision_body
                ),
                what="POST /api/workspaces/",
            )
            container_name = info.get("container_name")
            if not container_name:
                raise PermanentError(f"provision returned no container_name: {info}")
            await step("upgrading", container_name=container_name, tenant_id=self.tenant_id)

            # 1b. Wait for the container's pod to be Ready before exec/upgrade.
            await self._wait_container_ready(container_name)

            # 2. Upgrade + commit + open a DRAFT PR (gated until the oracle passes).
            pkg_spec: dict[str, object] = {"name": package}
            if mode == "cross-major":
                pkg_spec["target_version"] = target
                endpoint = f"{self.security_url}/api/dependencies/{container_name}/upgrade-major"
            else:
                endpoint = f"{self.security_url}/api/dependencies/{container_name}/upgrade"
            upgrade_body = {
                "packages": [pkg_spec],
                "manager": manager,
                "commit": True,
                "open_pr": True,
                "draft": True,
                "branch_name": branch,
                "pr_title": f"Upgrade {package} to {target}",
                "pr_body": (
                    f"Automated dependency upgrade of `{package}` to `{target}` "
                    f"({mode}) by the coding-repo-jobs bulk spine. "
                    f"Opened as draft; promoted to ready only after the in-container "
                    f"test + security oracle passes."
                ),
                "timeout_seconds": self.upgrade_timeout_seconds,
            }
            up = await self._post(endpoint, upgrade_body)

            exit_code = up.get("exit_code", 1)
            results = up.get("results") or []
            first_ok = bool(results) and results[0].get("success") is True
            if exit_code != 0 or not first_ok:
                stderr = (up.get("stderr") or "")[:800]
                raise PermanentError(f"upgrade failed (exit={exit_code}, ok={first_ok}): {stderr}")

            pr_url = up.get("pr_url")
            pr_number = up.get("pr_number")
            if not pr_url:
                # Upgrade succeeded but produced no manifest change (already at
                # target) → terminal success, no PR.
                return UpgradeOutcome(status="done", container_name=container_name)
            await step(
                "verifying",
                container_name=container_name,
                branch_name=branch,
                pr_url=pr_url,
                pr_number=pr_number,
            )

            # 3. Oracle gate: tests + security scan in the same container.
            self._gate_tests(await self._run_tests(container_name, ecosystem), ecosystem)
            self._gate_scan(await self._scan(container_name))

            # 4. Promote the draft PR to ready-for-review.
            # The ONLY coding-github call this orchestrator makes. It names a
            # repository, so it asserts the platform tenant when that repository
            # is platform-owned (see ``_tenant_for``) and the job's own tenant
            # otherwise. Any future coding-github call must do the same.
            await self._post(
                f"{self.github_url}/api/repositories/mark-pull-request-ready",
                {"repo_owner": owner, "repo_name": repo, "pr_number": pr_number},
                headers=self._headers_for(repo_owner=owner),
            )
            return UpgradeOutcome(
                status="pr_open",
                container_name=container_name,
                branch_name=branch,
                pr_url=pr_url,
                pr_number=pr_number,
            )
        finally:
            if container_name:
                await self._delete_workspace(container_name)

    async def _run_tests(self, container_name: str, ecosystem: str) -> dict:
        cmd = _ECOSYSTEM_TEST_CMD.get(ecosystem)
        if cmd is None:
            # Every supported ecosystem (python/node/rust/go) has a wired test
            # command. An unrecognised ecosystem means we cannot verify the
            # submission, so fail closed rather than waving it through.
            logger.warning("no test command for ecosystem %s; failing the test gate", ecosystem)
            return {"exit_code": 1, "skipped": True, "stderr": f"no test runner for {ecosystem}"}
        # ``timeout`` is a BODY field (coding-containers' in-container command
        # budget), not the HTTP timeout — ``exec(**body)`` forwards it as JSON.
        # The transport budget comes from the client this call is made on.
        #
        # ``max_retries=1``: the base client retries ReadTimeout, and a retried
        # exec RE-RUNS the whole suite. Today a read timeout raises straight
        # through to a TransientError and the worker re-queues with backoff;
        # silently running the tests up to 3x inside one job would triple the
        # worst-case job latency with no bound. Connect-level blips are still
        # covered by that re-queue.
        return await self._ws_call(
            self._ws(timeout=self._exec_http_timeout_seconds, max_retries=1).exec(
                container_name,
                headers=self._headers,
                command=cmd,
                workdir="/workspace",
                timeout=self.test_timeout_seconds,
            ),
            what=f"POST /api/workspaces/{container_name}/exec",
            container_op=True,
        )

    @staticmethod
    def _ran_zero_tests(ecosystem: str, stdout: str) -> bool:
        """True when a clean-exit run executed ZERO tests for an ecosystem whose
        runner exits 0 on an empty suite (rust/go). Python/node fail via the exit
        code, so they are never inspected here and always return False."""
        if ecosystem not in _ZERO_TEST_EXIT_ZERO:
            return False
        text = stdout or ""
        if ecosystem == "rust":
            counts = _CARGO_RUNNING_RE.findall(text)
            # No `running N tests` line at all is itself a no-test signal.
            return sum(int(n) for n in counts) == 0
        # go: tests ran iff at least one package line reports a real result
        # (ok/FAIL/--- PASS/--- FAIL) that is NOT a `[no test files]` line.
        for line in text.splitlines():
            if _GO_RAN_RE.match(line) and _GO_NO_TEST_FILES not in line:
                return False
        return True

    @staticmethod
    def _gate_tests(exec_result: dict, ecosystem: str) -> None:
        exit_code = exec_result.get("exit_code", 1)
        if exit_code not in _TOLERATED_TEST_EXIT:
            stderr = (exec_result.get("stderr") or exec_result.get("stdout") or "")[:800]
            raise PermanentError(f"oracle: tests failed (exit={exit_code}): {stderr}")
        # Clean exit, but cargo/go exit 0 on an empty suite. A submission that ran
        # zero tests is not verified and must not pass the oracle.
        if UpgradeOrchestrator._ran_zero_tests(ecosystem, exec_result.get("stdout") or ""):
            raise PermanentError(f"oracle: no tests were run (ecosystem={ecosystem}); not a pass")

    async def _scan(self, container_name: str) -> dict:
        resp = await self._post(
            f"{self.security_url}/api/security/scan/{container_name}/quick", {}, container_op=True
        )
        # Comprehensive-scan shape wraps the verdict under "result".
        return resp.get("result", resp)

    @staticmethod
    def _gate_scan(scan_result: dict) -> None:
        # Fail CLOSED on anything that isn't a trustworthy clean verdict. A scan
        # that flagged itself degraded (a scanner failed / was unavailable, so a
        # clean-looking result is not trustworthy) or that did not return the
        # expected verdict shape (an error envelope without the count/grade keys)
        # must block the oracle, never wave the job through. Previously a missing
        # key defaulted to 0 / None and silently passed.
        if scan_result.get("scan_degraded"):
            raise PermanentError(
                "oracle: security scan degraded (a scanner failed/was unavailable); failing closed"
            )
        if "critical_count" not in scan_result or "security_grade" not in scan_result:
            raise PermanentError(
                "oracle: security scan returned an unexpected shape "
                f"(keys={sorted(scan_result)}); failing closed"
            )
        critical = scan_result.get("critical_count", 0) or 0
        grade = scan_result.get("security_grade")
        # Full tightening: block on any critical finding, and on a security grade
        # of D or F (previously only F). A grade at or below D is not a passable
        # security posture for promoting a PR on the live tournament.
        if critical > 0 or grade in _FAILING_GRADES:
            raise PermanentError(
                f"oracle: security scan failed (critical={critical}, grade={grade})"
            )
