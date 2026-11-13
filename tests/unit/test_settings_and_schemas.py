"""Unit tests for settings validation + schema mapping (no DB)."""

from __future__ import annotations

import pytest

from config.settings import Settings


def test_db_driver_normalized():
    s = Settings(environment="test", database_url="postgresql://u:p@h:5432/d")
    assert s.database_url.startswith("postgresql+psycopg://")


def test_invalid_environment_rejected():
    with pytest.raises(ValueError):
        Settings(environment="prod-typo")


def test_required_in_production_raises_when_missing():
    with pytest.raises(RuntimeError, match="missing required production env"):
        Settings(environment="production", database_url="", containers_url="")


def test_production_ok_when_all_present():
    s = Settings(
        environment="production",
        database_url="postgresql+psycopg://u:p@h/db",
        containers_url="http://c:8691",
        security_url="http://s:8693",
        github_url="http://g:8692",
        worker_tenant_id="t-1",
    )
    assert s.environment == "production"


def test_job_response_from_orm():
    from datetime import UTC, datetime

    from models.database import RepoJob
    from models.schemas import JobResponse

    now = datetime.now(UTC)
    job = RepoJob(
        id=5,
        repo_url="o/r",
        package="p",
        target="1.0",
        ecosystem="python",
        mode="in-range",
        owner="o",
        tenant_id=None,
        correlation_id=None,
        status="queued",
        attempts=0,
        max_attempts=3,
        last_error=None,
        container_name=None,
        branch_name=None,
        pr_url=None,
        pr_number=None,
        is_cross_repository=None,
        claimed_at=None,
        created_at=now,
        updated_at=now,
    )
    resp = JobResponse.from_orm_job(job)
    assert resp.id == 5
    assert resp.status == "queued"
    assert resp.repo_url == "o/r"
