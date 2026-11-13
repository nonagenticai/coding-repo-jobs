"""FastAPI application factory for coding-repo-jobs."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from config.settings import get_settings
from db.session import dispose_engine, get_sessionmaker
from routers.health import router as health_router
from routers.jobs import router as jobs_router
from services.worker import Worker

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))

    worker: Worker | None = None
    if settings.worker_enabled and settings.environment not in ("test", "testing"):
        worker = Worker(get_sessionmaker(), settings)
        worker.start()
    app.state.worker = worker

    try:
        yield
    finally:
        if worker is not None:
            await worker.stop()
        await dispose_engine()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="coding-repo-jobs",
        description=(
            "L2 bulk dependency-upgrade spine — durable queue + stateless worker.\n\n"
            "**Auth:** Istio ext_authz via coding_client AuthMiddleware. Tenant "
            "isolation via `X-Tenant-Id` header / Bearer license token. Every "
            "`/api/repo-jobs` route is tenant-scoped (`require_tenant`); reads and "
            "the enqueue dedup key are per-tenant."
        ),
        version=settings.version,
        lifespan=lifespan,
    )

    # Auth middleware sets request.state.user from Istio-forwarded headers; the
    # route-level require_tenant dependency then resolves + enforces the tenant.
    if settings.enable_auth:
        try:
            from coding_client.auth.middleware import AuthMiddleware
        except ImportError as exc:
            # A failed import must fail CLOSED: silently skipping the middleware
            # would run the whole service with no authentication at all — the
            # exact defect this branch used to have (it logged a warning and
            # continued). Mirrors the production-fail-fast contract already used
            # for required settings (Settings._check_required / coding_client's
            # BaseMicroserviceSettings._enforce_required_in_production): raise in
            # production, warn otherwise so a partially-installed dev checkout can
            # still boot. Matches coding-testing's app_factory._add_middlewares.
            if settings.environment == "production":
                raise RuntimeError(
                    "coding-repo-jobs cannot import coding_client.auth.middleware."
                    "AuthMiddleware; refusing to start in production without "
                    f"authentication. Original import error: {exc}"
                ) from exc
            logger.warning(
                "AuthMiddleware not available (%s); starting WITHOUT authentication "
                "because environment=%s. This would be a hard failure in production.",
                exc,
                settings.environment,
            )
        else:
            app.add_middleware(AuthMiddleware)  # type: ignore[arg-type]
            logger.info("Auth middleware enabled")
    else:
        logger.warning("Auth middleware DISABLED (ENABLE_AUTH=false)")

    app.include_router(health_router)
    app.include_router(jobs_router)

    # Mark the protected prefix in the OpenAPI schema (KAG standard).
    try:
        from coding_client.api.openapi_config import configure_openapi

        configure_openapi(
            app,
            protected_prefixes={"/api/repo-jobs"},
            public_endpoints={"/health", "/health/ready", "/health/live"},
        )
    except ImportError:  # pragma: no cover
        logger.warning("configure_openapi not available, skipping")

    return app
