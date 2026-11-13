"""Alembic environment configuration."""

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool

from alembic import context

# Ensure src/ is on the path so ORM models can be imported
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from models.database import Base  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# DATABASE_URL is required: alembic.ini carries no DSN (a localhost default
# silently hits whatever runs on that port on a dev box).
_db_url = os.getenv("DATABASE_URL")
if not _db_url:
    raise RuntimeError(
        "alembic: set DATABASE_URL; there is no default DSN. For a local run start an "
        "ephemeral docker postgres:18 and point DATABASE_URL at it."
    )
# Fleet standard: psycopg v3. SQLAlchemy picks psycopg2 for bare
# postgresql:// URLs; force the v3 driver since psycopg2 isn't installed.
if _db_url.startswith("postgresql://"):
    _db_url = _db_url.replace("postgresql://", "postgresql+psycopg://", 1)
# Escape % for configparser interpolation (e.g. %40 in passwords)
config.set_main_option("sqlalchemy.url", _db_url.replace("%", "%%"))


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
