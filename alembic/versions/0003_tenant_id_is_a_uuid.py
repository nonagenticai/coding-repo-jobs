"""Make ``repo_jobs.tenant_id`` a real postgres ``uuid``.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-02

Fleet standard: a tenant id IS a uuid. Storing it as ``character varying``
means the DATABASE cannot refuse ``'tenant-a'``, ``'n8n-dev'`` or ``''`` — only
convention can, and convention is not a constraint. This migration retypes the
column so the storage layer enforces what the fleet already assumes.

``uuid`` is also case-INSENSITIVE on input and canonicalises to lower case:
``'AB...'::uuid`` and ``'ab...'::uuid`` are the SAME value. ``varchar`` is
case-SENSITIVE, so two rows that differ only in case are two distinct tenants
today and would COLLAPSE into one after the ALTER. In ``repo_jobs`` that is not
merely a data-quality problem: ``tenant_id`` leads ``UNIQUE (tenant_id,
repo_url, package, target)``, so a collapse would make the ALTER fail with a
duplicate-key error mid-migration — or, worse in a table without that key,
silently merge two tenants' rows. The census below therefore refuses BOTH
non-uuid values and case-only collisions BEFORE touching the column.

Census semantics — read this before "simplifying" the collision query:

    GROUP BY lower(tenant_id::text) HAVING count(DISTINCT tenant_id::text) > 1

``count(DISTINCT ...)`` counts SPELLINGS within one case-folded key. It is >1
only when the same tenant is written two different ways. The tempting
``count(*) <> count(DISTINCT lower(...))`` is WRONG: ``count(*)`` counts ROWS,
and a perfectly healthy queue has many rows per tenant, so that form would
refuse to migrate every non-trivial table on the fleet. See
``tests/integration/test_tenant_id_uuid_migration.py::test_control_*`` for the
control case that pins this.

The ALTER is driven from the literal ``_TENANT_COLUMNS`` tuple below, NOT from
live ``Base.metadata``. A migration that reads live metadata changes its output
whenever the ORM changes, which is how ``alembic upgrade head`` from revision
zero started failing with ``operator does not exist: character varying = uuid``
in two sibling repos: invisible in an already-migrated production database,
fatal in CI and in every fresh environment.

Postgres-only (fleet standard: postgres18 + psycopg v3). No SQLite path.
"""

from __future__ import annotations

import logging

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")

# EXPLICIT literal (table, column) pairs. Never derived from Base.metadata —
# see the module docstring for the failure that rule exists to prevent.
_TENANT_COLUMNS: tuple[tuple[str, str], ...] = (("repo_jobs", "tenant_id"),)

# The canonical 8-4-4-4-12 hex form. Anchored, case-insensitive, because
# ``uuid`` accepts either case on input.
_UUID_RE = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"


class TenantIdCensusError(RuntimeError):
    """Raised when a tenant column cannot be safely retyped to ``uuid``."""


def census(
    connection: sa.engine.Connection,
    tables: tuple[tuple[str, str], ...] = _TENANT_COLUMNS,
) -> dict[str, int]:
    """Refuse the migration unless every listed column is safely castable.

    Returns ``{table: row_count}`` so the caller can log before/after counts.
    Raises :class:`TenantIdCensusError` on either failure mode:

    1. a value that is not a canonical uuid (``uuid`` cannot store it);
    2. a case-only collision, which ``uuid`` would silently merge.

    Works on both ``varchar`` and ``uuid`` columns (``::text`` normalises), so
    it is safe to call on an already-migrated database.
    """
    counts: dict[str, int] = {}
    for table, column in tables:
        total = connection.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one()  # noqa: S608
        counts[table] = int(total)

        non_uuid = (
            connection.execute(
                sa.text(  # noqa: S608
                    f"SELECT DISTINCT {column}::text FROM {table} "
                    f"WHERE {column} IS NOT NULL AND {column}::text !~ :pattern "
                    "ORDER BY 1 LIMIT 20"
                ),
                {"pattern": _UUID_RE},
            )
            .scalars()
            .all()
        )
        if non_uuid:
            raise TenantIdCensusError(
                f"{table}.{column}: {len(non_uuid)} non-uuid value(s) present, "
                f"a uuid column cannot store them: {non_uuid!r}. "
                "Fix or delete these rows, then re-run the migration."
            )

        # Case-only collisions. count(DISTINCT) counts SPELLINGS, not rows —
        # many rows per tenant is HEALTHY and must stay green.
        collisions = connection.execute(
            sa.text(  # noqa: S608
                f"""
                SELECT lower({column}::text) AS folded,
                       count(DISTINCT {column}::text) AS spellings
                FROM {table}
                WHERE {column} IS NOT NULL
                GROUP BY lower({column}::text)
                HAVING count(DISTINCT {column}::text) > 1
                ORDER BY 1
                LIMIT 20
                """
            )
        ).all()
        if collisions:
            raise TenantIdCensusError(
                f"{table}.{column}: {len(collisions)} case-only collision(s) — "
                "these are distinct tenants under varchar and would COLLAPSE "
                f"into one under uuid: {[(r.folded, r.spellings) for r in collisions]!r}. "
                "Reconcile them before retyping."
            )

        logger.info("census %s.%s: %d row(s), 0 non-uuid, 0 case collisions", table, column, total)
    return counts


def upgrade() -> None:
    connection = op.get_bind()
    before = census(connection)

    for table, column in _TENANT_COLUMNS:
        op.alter_column(
            table,
            column,
            existing_type=sa.String(128),
            type_=PG_UUID(as_uuid=False),
            existing_nullable=False,
            postgresql_using=f"{column}::uuid",
        )

    after = {
        table: int(connection.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one())  # noqa: S608
        for table, _ in _TENANT_COLUMNS
    }
    for table in before:
        logger.info("upgrade %s: %d row(s) before, %d after", table, before[table], after[table])
        if before[table] != after[table]:  # pragma: no cover - ALTER TYPE never drops rows
            raise TenantIdCensusError(
                f"{table}: row count changed during retype "
                f"({before[table]} -> {after[table]}) — aborting."
            )


def downgrade() -> None:
    """Genuinely reversible: uuid -> varchar(128) is total and lossless.

    Every uuid renders as a 36-character canonical string, which fits
    ``varchar(128)``. The values come back lower-cased (postgres canonicalises
    on the way in), so an upper-case spelling that existed before the FIRST
    upgrade is not restored — but the census guarantees such a spelling can
    only have existed alone in its case-folded group, so no row is merged,
    split, or lost.
    """
    connection = op.get_bind()
    before = {
        table: int(connection.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one())  # noqa: S608
        for table, _ in _TENANT_COLUMNS
    }

    for table, column in _TENANT_COLUMNS:
        op.alter_column(
            table,
            column,
            existing_type=PG_UUID(as_uuid=False),
            type_=sa.String(128),
            existing_nullable=False,
            postgresql_using=f"{column}::text",
        )

    for table, _ in _TENANT_COLUMNS:
        after = int(connection.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one())  # noqa: S608
        logger.info("downgrade %s: %d row(s) before, %d after", table, before[table], after)
