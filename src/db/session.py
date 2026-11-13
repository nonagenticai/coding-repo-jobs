"""Async SQLAlchemy engine + session factory.

Built lazily so importing this module (e.g. at test collection) does not require
a configured ``DATABASE_URL``. Call ``get_engine()`` / ``get_sessionmaker()``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from config.settings import get_settings

logger = logging.getLogger(__name__)

_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Return the process-wide async engine, creating it on first use."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.database_url,
            echo=settings.debug,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            connect_args={
                "options": f"-c statement_timeout={settings.db_statement_timeout}",
                # PgBouncer runs in transaction pool mode with no
                # max_prepared_statements, so psycopg3 server-side prepared
                # statements break when a pooled server connection rotates.
                # The worker's claim loop reissues the same FOR UPDATE SKIP
                # LOCKED query every poll, crossing the default prepare_threshold
                # within seconds, so disable prepared statements entirely.
                "prepare_threshold": None,
            },
        )
    return _engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    """Return the process-wide async session factory."""
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = async_sessionmaker(
            bind=get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _sessionmaker


async def get_db() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: yield a session, commit on success, roll back on error."""
    async with get_sessionmaker()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def dispose_engine() -> None:
    """Dispose the engine on shutdown (no-op if never created)."""
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _sessionmaker = None
