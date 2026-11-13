"""coding-repo-jobs configuration using pydantic-settings.

Per the fleet config-externalization policy, service addresses MUST come from
gitops env. Backend URLs default to empty strings; ``_check_required`` raises a
single combined error at startup when ``ENVIRONMENT=production`` and any are
missing, so a misconfigured prod boot fails loudly instead of routing bulk
upgrades at a dead in-cluster service name.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# ── Platform-owned repositories ───────────────────────────────────────────────
# coding-github restricts every repository owned by ``PLATFORM_REPO_OWNER`` to a
# small set of platform tenant ids: a call that asserts an ordinary operator
# tenant against one of those repos is refused with 403
# ``platform_repo_forbidden``. These two are protocol constants of that gate — an
# identity, not a service address — so unlike the backend URLs above they carry a
# real default rather than defaulting empty and failing the production check. The
# env vars exist so a change to the gate's allowlist does not need a code release.
#
# WHICH platform tenant, and why it is not the first of the five
# ----------------------------------------------------------------------------
# The gate's allowlist is ``CODING_GITHUB_PLATFORM_TENANTS``, whose default is
# ``0195f48b-d000-7000-8000-00000000000{1..5}``
# (coding-github ``src/config/settings.py``: *"fr's five INTERNAL tenants (…0003
# is EXECUTION, the conveyor)"*). All five pass the gate, so the 403 does not
# decide this — attribution does, and this change is entirely about attribution:
# the tenant it picks is the identity stamped on every promoted pull request.
#
#   …0001 is the workspace fr's migration 062
#          (``fr/backend/migrations/062_super_admin_workspaces_namespace_realign.sql``)
#          inserts under the name ``system`` — the canonical uuid7 of the legacy
#          ``n8n-dev`` text tenant. It is the fleet's *default/system* identity,
#          the one coding-containers and coding-realtime carry as
#          ``DEFAULT_TENANT_ID``. Asserting it here would file this service's
#          PR promotions under the platform's catch-all tenant.
#   …0003 is that same migration's ``george-dev → nonagentic-exec`` rename
#          (step 6) — fr's EXECUTION workspace, which coding-github's allowlist
#          comment names as *the conveyor*.
#
# coding-repo-jobs IS that execution spine: a bulk upgrade conveyor whose own
# operator identity is already the execution one — gitops
# ``clusters/strato/services/coding-repo-jobs/configmap.yaml`` sets
# ``WORKER_TENANT_ID: 00000000-0000-0000-0000-000000000003`` with the comment
# *"Matches the IT-Flow1 system-execution-operator tenant"*. So …0003 keeps both
# halves of this orchestrator's identity — the operator tenant on the job's own
# container and the platform tenant on the repository — naming the same thing,
# EXECUTION. …0001 would split them and attribute conveyor work to ``system``.
PLATFORM_REPO_OWNER = "nonagenticai"
PLATFORM_TENANT_ID = "0195f48b-d000-7000-8000-000000000003"


class Settings(BaseSettings):
    """coding-repo-jobs configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Identity ──────────────────────────────────────────────────────────
    service_name: str = Field(default="coding-repo-jobs")
    version: str = Field(default="0.1.0")
    environment: str = Field(default="production")
    debug: bool = Field(default=False)
    port: int = Field(default=8695)
    # Tenant-isolation auth (coding_client AuthMiddleware + require_tenant).
    # Only disable for isolated local runs — never in cluster.
    enable_auth: bool = Field(default=True)

    # ── Database ──────────────────────────────────────────────────────────
    database_url: str = Field(default="")
    db_statement_timeout: int = Field(default=30000)

    # ── Backend micro-service URLs (sourced from the Deployment env) ──────
    containers_url: str = Field(default="")
    security_url: str = Field(default="")
    github_url: str = Field(default="")

    # ── Tenant the worker uses when calling the backends ──────────────────
    # The backends require X-Tenant-Id; the bulk engine acts as a single
    # service-execution operator tenant.
    worker_tenant_id: str = Field(default="")

    # ── Platform repo targeting (coding-github's platform-tenant gate) ────
    # The owner whose repositories coding-github gates, and the single platform
    # tenant the worker asserts when it calls coding-github about one of them.
    # Every other target keeps the job's own operator tenant — this is not a
    # blanket re-tenanting.
    platform_repo_owner: str = Field(default=PLATFORM_REPO_OWNER)
    platform_tenant_id: str = Field(default=PLATFORM_TENANT_ID)

    # No GitHub credential lives here. GitHub access is ONLY via the GitHub
    # Apps behind coding-github (owner ruling 2026-09-30, no PAT fallbacks):
    # coding-containers mints an App clone token per provision. A stale
    # GITHUB_TOKEN env var is ignored (``extra="ignore"``), never read.

    # ── Worker loop tuning ────────────────────────────────────────────────
    worker_enabled: bool = Field(default=True)
    worker_poll_interval_seconds: float = Field(default=5.0)
    worker_concurrency: int = Field(default=2)
    # Per-job step timeouts handed to the backends.
    upgrade_timeout_seconds: int = Field(default=600)
    test_timeout_seconds: int = Field(default=300)
    http_timeout_seconds: float = Field(default=900.0)
    # A freshly-provisioned container's k8s pod is not Ready the instant provision
    # returns — exec/upgrade before then fails with a misleading "Neither Docker
    # nor Kubernetes is available". Poll GET /status until RUNNING before upgrading.
    container_ready_timeout_seconds: float = Field(default=180.0)
    container_ready_poll_seconds: float = Field(default=3.0)
    # Backoff (seconds) before a transient failure becomes re-claimable.
    retry_backoff_seconds: float = Field(default=30.0)

    # ── Observability ─────────────────────────────────────────────────────
    log_level: str = Field(default="INFO")

    # Every key here must be non-empty when ``ENVIRONMENT=production``. The
    # first five default empty, so listing them is what supplies their value at
    # all. The last two carry real defaults, so this entry can only ever fire on
    # an EXPLICIT ``PLATFORM_TENANT_ID=""`` / ``PLATFORM_REPO_OWNER=""`` in the
    # Deployment env — and that is exactly the case worth catching: a blank
    # platform tenant does not surface until ``_tenant_for`` refuses, which is
    # AFTER the job has provisioned a container, run the upgrade and opened the
    # draft PR, and a blank owner silently matches nothing so every
    # platform-owned repo takes the operator tenant into the gate and earns the
    # permanent 403 this change exists to prevent. Both are side effects bought
    # before the error; the check belongs at startup.
    _REQUIRED_IN_PRODUCTION: tuple[str, ...] = (
        "database_url",
        "containers_url",
        "security_url",
        "github_url",
        "worker_tenant_id",
        "platform_repo_owner",
        "platform_tenant_id",
    )

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        allowed = {"development", "staging", "production", "test", "testing"}
        if v not in allowed:
            raise ValueError(f"environment must be one of {allowed}")
        return v

    @field_validator("database_url")
    @classmethod
    def _normalize_db_driver(cls, v: str) -> str:
        # Fleet standard: psycopg v3. Force the v3 driver for bare postgresql://.
        if v.startswith("postgresql://"):
            return v.replace("postgresql://", "postgresql+psycopg://", 1)
        return v

    @model_validator(mode="after")
    def _check_required(self) -> Settings:
        if self.environment == "production":
            missing = [name for name in self._REQUIRED_IN_PRODUCTION if not getattr(self, name)]
            if missing:
                raise RuntimeError(
                    "coding-repo-jobs: missing required production env: " + ", ".join(missing)
                )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton of the service settings."""
    return Settings()
