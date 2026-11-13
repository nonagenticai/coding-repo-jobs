"""``repo_jobs.tenant_id`` is a real postgres ``uuid`` — and the census proves it.

Two things are under test here and they fail in opposite directions:

1. The COLUMN. After migration 0003 the database itself refuses a non-uuid
   tenant, and — because ``PG_UUID(as_uuid=False)`` — hands Python back a
   ``str``, so every ``Mapped[str]`` annotation, ``==`` filter against the
   request-scoped ``X-Tenant-Id`` header and JSON body stays true. A test that
   only checked "the insert fails" would pass just as happily with
   ``as_uuid=True``, which silently breaks all three.

2. The CENSUS in the migration. It must refuse the two conditions that make the
   retype unsafe (a non-uuid value, a case-only collision) and it must stay
   GREEN on a healthy table with MANY rows per tenant. That last one is the
   CONTROL case: the obvious-looking ``count(*) <> count(DISTINCT lower(...))``
   collision test counts ROWS, so it would refuse every real queue on the
   fleet. ``test_control_many_rows_per_tenant_migrates_green`` is what stops
   anyone "simplifying" the query back into that shape.

Requires a live Postgres via DATABASE_URL (CI provisions it; unset locally => the
conftest starts an ephemeral docker postgres:18).
"""

from __future__ import annotations

import importlib.util
import pathlib
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from models.database import Base, RepoJob
from services import queue

TENANT = "44444444-4444-4444-8444-444444444444"


def _load_migration():
    """Import the 0003 version file directly (alembic/versions is not a package)."""
    path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "alembic"
        / "versions"
        / "0003_tenant_id_is_a_uuid.py"
    )
    spec = importlib.util.spec_from_file_location("_m0003", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


m0003 = _load_migration()


@pytest.fixture
async def engine(database_url):
    eng = create_async_engine(database_url, poolclass=NullPool)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest.fixture
async def scratch(engine):
    """A throwaway VARCHAR-tenant table to exercise the census against."""
    name = f"census_probe_{uuid.uuid4().hex[:12]}"
    async with engine.begin() as conn:
        await conn.execute(
            sa.text(f"CREATE TABLE {name} (id serial PRIMARY KEY, tenant_id varchar(128))")
        )
    try:
        yield name
    finally:
        async with engine.begin() as conn:
            await conn.execute(sa.text(f"DROP TABLE IF EXISTS {name}"))


async def _census(engine, table: str):
    async with engine.begin() as conn:
        return await conn.run_sync(lambda sync: m0003.census(sync, ((table, "tenant_id"),)))


# ── 1. The column ────────────────────────────────────────────────────────────
async def test_the_column_is_a_real_uuid_in_postgres(sm, engine):
    """Not varchar, not text — ``uuid``. This is the whole point of 0003."""
    async with engine.connect() as conn:
        data_type = (
            await conn.execute(
                sa.text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_name = 'repo_jobs' AND column_name = 'tenant_id'"
                )
            )
        ).scalar_one()
    assert data_type == "uuid"


async def test_a_non_uuid_tenant_is_refused_by_the_database(sm):
    """``'tenant-a'`` used to be storable. The DATABASE now says no."""
    with pytest.raises(DBAPIError):
        async with sm() as s:
            await queue.enqueue(s, tenant_id="tenant-a", repo_url="o/r", package="p", target="1.0")
            await s.commit()


async def test_the_value_round_trips_as_str_not_a_uuid_object(sm):
    """``as_uuid=False`` is load-bearing: the Python side must stay ``str``.

    With ``as_uuid=True`` the reads below return ``uuid.UUID``, ``type(...) is
    str`` goes red, and — the reason it matters in production — the ``==``
    comparison against the request-scoped header string silently stops
    matching.
    """
    async with sm() as s:
        job, created = await queue.enqueue(
            s, tenant_id=TENANT, repo_url="o/r", package="p", target="1.0"
        )
        await s.commit()
    assert created is True

    async with sm() as s:
        fetched = await queue.get_job(s, job.id, tenant_id=TENANT)
    assert fetched is not None
    assert type(fetched.tenant_id) is str, f"got {type(fetched.tenant_id)!r}, not str"
    assert fetched.tenant_id == TENANT

    # The tenant-scoped read filter compares the column to a plain header string.
    async with sm() as s:
        found = await s.scalar(sa.select(RepoJob).where(RepoJob.tenant_id == TENANT))
    assert found is not None and found.id == job.id


async def test_the_orm_and_the_migration_agree_on_the_type():
    """The ORM declares the same postgres type the migration ALTERs to."""
    column = Base.metadata.tables["repo_jobs"].c.tenant_id
    assert column.type.__class__.__name__ == "UUID"
    assert column.type.as_uuid is False
    assert m0003._TENANT_COLUMNS == (("repo_jobs", "tenant_id"),)


# ── 2. The census ────────────────────────────────────────────────────────────
async def test_control_many_rows_per_tenant_migrates_green(engine, scratch):
    """CONTROL: a healthy table with MANY rows per tenant must NOT be refused.

    500 rows, 2 tenants, zero collisions. The naive
    ``count(*) <> count(DISTINCT lower(tenant_id))`` form would see 500 <> 2
    and refuse — i.e. it would refuse every real queue on the fleet. The
    ``count(DISTINCT ...) > 1`` per case-folded group used by the migration
    counts SPELLINGS, so this stays green, and the retype that follows the
    census succeeds.
    """
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    async with engine.begin() as conn:
        await conn.execute(
            sa.text(f"INSERT INTO {scratch} (tenant_id) SELECT :a FROM generate_series(1, 300)"),
            {"a": a},
        )
        await conn.execute(
            sa.text(f"INSERT INTO {scratch} (tenant_id) SELECT :b FROM generate_series(1, 200)"),
            {"b": b},
        )

    counts = await _census(engine, scratch)
    assert counts == {scratch: 500}

    # ...and the ALTER the census gates actually goes through on this table.
    async with engine.begin() as conn:
        await conn.execute(
            sa.text(f"ALTER TABLE {scratch} ALTER COLUMN tenant_id TYPE uuid USING tenant_id::uuid")
        )
        after = (await conn.execute(sa.text(f"SELECT count(*) FROM {scratch}"))).scalar_one()
        dtype = (
            await conn.execute(
                sa.text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_name = :t AND column_name = 'tenant_id'"
                ),
                {"t": scratch},
            )
        ).scalar_one()
    assert after == 500, "the retype must not drop rows"
    assert dtype == "uuid"


async def test_census_refuses_a_non_uuid_value(engine, scratch):
    async with engine.begin() as conn:
        await conn.execute(
            sa.text(f"INSERT INTO {scratch} (tenant_id) VALUES (:a), (:b)"),
            {"a": str(uuid.uuid4()), "b": "n8n-dev"},
        )
    with pytest.raises(m0003.TenantIdCensusError, match="non-uuid"):
        await _census(engine, scratch)


async def test_census_refuses_a_case_only_collision(engine, scratch):
    """Two spellings of one uuid are two tenants under varchar, one under uuid."""
    value = str(uuid.uuid4())
    async with engine.begin() as conn:
        await conn.execute(
            sa.text(f"INSERT INTO {scratch} (tenant_id) VALUES (:lo), (:up)"),
            {"lo": value, "up": value.upper()},
        )
    with pytest.raises(m0003.TenantIdCensusError, match="case-only collision"):
        await _census(engine, scratch)


async def test_census_ignores_nulls_and_an_empty_table(engine, scratch):
    assert await _census(engine, scratch) == {scratch: 0}
    async with engine.begin() as conn:
        await conn.execute(sa.text(f"INSERT INTO {scratch} (tenant_id) VALUES (NULL)"))
    assert await _census(engine, scratch) == {scratch: 1}
