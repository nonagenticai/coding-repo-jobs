"""Health + readiness endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from config.settings import get_settings

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness — returns 200 without touching the DB (matches fleet convention)."""
    settings = get_settings()
    return {"status": "ok", "service": settings.service_name, "version": settings.version}
