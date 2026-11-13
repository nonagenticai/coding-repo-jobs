"""App-factory + health endpoint (no DB; ASGITransport does not run lifespan)."""

from __future__ import annotations

import sys
from unittest.mock import patch

import httpx
import pytest
from httpx import ASGITransport

from app_factory import create_app


def _route_paths(routes) -> set[str]:
    """Every EFFECTIVE (fully-prefixed) path reachable from ``routes``.

    fastapi>=0.137 stopped flattening ``include_router()`` sub-routes into
    ``app.routes``: each inclusion is a private ``_IncludedRouter`` wrapper
    (https://github.com/fastapi/fastapi/releases/tag/0.137.0), so the old
    ``{route.path for route in app.routes}`` raises
    ``AttributeError: '_IncludedRouter' object has no attribute 'path'``.

    Neither a ``hasattr(r, "path")`` filter nor a hand-rolled recursion into
    ``r.routes`` is a fix: the wrapper exposes NEITHER attribute, so both
    quietly reduce the route table to the four default docs routes and the
    assertions below fail (or, elsewhere, pass vacuously).
    ``fastapi.routing.iter_route_contexts`` (public, added 0.137.2) is the
    accessor that resolves either shape to contexts carrying the real
    prefixed path.
    """
    try:
        from fastapi.routing import iter_route_contexts
    except ImportError:  # fastapi < 0.137.2: app.routes is already flat
        return {r.path for r in routes}  # type: ignore[attr-defined]
    return {ctx.path for ctx in iter_route_contexts(routes) if ctx.path is not None}


def test_create_app_registers_routes():
    app = create_app()
    paths = _route_paths(app.routes)
    assert "/health" in paths
    assert "/api/repo-jobs" in paths


async def test_health_endpoint():
    app = create_app()
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service"] == "coding-repo-jobs"


def test_create_app_registers_auth_middleware_when_import_succeeds():
    """Positive control: the normal path really does register AuthMiddleware."""
    app = create_app()
    assert any(m.cls.__name__ == "AuthMiddleware" for m in app.user_middleware)


# ── AuthMiddleware import failure must fail CLOSED ─────────────────────────
#
# Regression guard: create_app() used to swallow the ImportError with
# `logger.warning("AuthMiddleware not available, skipping")`, which meant a
# broken coding_client install silently ran the whole service with NO
# authentication at all.


def _no_auth_middleware_module():
    """Context manager making `from coding_client.auth.middleware import ...`
    raise ImportError.

    A None entry in sys.modules is the documented way to make an import of
    that name fail without touching the real module on disk.
    """
    return patch.dict(sys.modules, {"coding_client.auth.middleware": None})


def test_create_app_import_error_raises_in_production(monkeypatch):
    """In production an unimportable AuthMiddleware must abort startup."""
    from config.settings import get_settings

    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@localhost:5432/d")
    get_settings.cache_clear()
    try:
        with _no_auth_middleware_module():
            with pytest.raises(RuntimeError, match="refusing to start in production"):
                create_app()
    finally:
        get_settings.cache_clear()


def test_create_app_import_error_warns_outside_production(monkeypatch, caplog):
    """Outside production the app still boots, but says loudly that auth is off."""
    import logging

    from config.settings import get_settings

    # The autouse _test_env fixture already sets ENVIRONMENT=test.
    get_settings.cache_clear()
    try:
        with caplog.at_level(logging.WARNING, logger="app_factory"), _no_auth_middleware_module():
            app = create_app()  # must not raise
    finally:
        get_settings.cache_clear()

    assert "starting WITHOUT authentication" in caplog.text
    assert not any(m.cls.__name__ == "AuthMiddleware" for m in app.user_middleware)
