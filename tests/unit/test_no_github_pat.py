"""No GitHub PAT fallback (owner ruling 2026-09-30).

GitHub access is ONLY via the GitHub Apps behind coding-github: coding-containers
mints an App clone token per provision when the caller sends no ``access_token``.
This service must never read a GitHub token or forward one.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import httpx

from config.settings import Settings
from services.orchestrator import UpgradeOrchestrator
from tests.conftest import make_backend_transport, make_workspaces_client

SRC = Path(__file__).resolve().parents[2] / "src"
_PAT_READ = re.compile(r"github_token|GITHUB_TOKEN|access_token")


def test_src_reads_no_github_token():
    files = sorted(SRC.rglob("*.py"))
    assert len(files) > 5, f"scanned {len(files)} files under {SRC} — wrong root"
    hits = [
        f"{p.relative_to(SRC)}:{n}: {line.strip()}"
        for p in files
        for n, line in enumerate(p.read_text().splitlines(), 1)
        if _PAT_READ.search(line) and not line.lstrip().startswith("#")
    ]
    assert hits == [], hits


def test_settings_ignore_a_stale_github_token_env(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_stale")
    settings = Settings()
    assert not hasattr(settings, "github_token")


async def test_provision_sends_no_access_token():
    seen: list[httpx.Request] = []
    inner = make_backend_transport()

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return inner.handle_request(request)

    transport = httpx.MockTransport(handler)
    orch = UpgradeOrchestrator(
        httpx.AsyncClient(transport=transport),
        containers_url="http://coding-containers:8691",
        security_url="http://coding-security:8693",
        github_url="http://coding-github:8692",
        tenant_id="t-1",
        container_ready_poll_seconds=0.0,
        workspaces_client=make_workspaces_client(transport),
    )
    try:
        await orch.run(
            repo_url="https://github.com/acme/widget", package="p", target="1.0", job_id=1
        )
    finally:
        await orch.client.aclose()
        await orch.aclose()

    provisions = [
        r for r in seen if r.method == "POST" and r.url.path.rstrip("/") == "/api/workspaces"
    ]
    assert len(provisions) == 1, [f"{r.method} {r.url}" for r in seen]
    body = json.loads(provisions[0].content)
    assert body["repo_url"] == "https://github.com/acme/widget"
    assert "access_token" not in body
