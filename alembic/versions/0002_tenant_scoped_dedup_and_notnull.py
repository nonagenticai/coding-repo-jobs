"""Tenant-scope the repo_jobs dedup key + make tenant_id NOT NULL (S5 fix).

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-18

Security fix (S5): the baseline unique constraint ``uq_repo_job_intent`` was
``(repo_url, package, target)`` — it OMITTED the tenant, so tenant B enqueuing
the same triple silently deduped onto tenant A's row and received A's job id
(cross-tenant read leak + write collision). ``tenant_id`` was also nullable, so
un-attributed rows were written and every read aggregated across tenants.

This migration:
  1. Deletes existing NULL-tenant rows. Decision: pre-prod, these rows were
     written with ``tenant_id=NULL`` (the queue never set it) and are therefore
     un-attributable junk. Forward-only, no backfill to a sentinel — a sentinel
     tenant would be a fabricated owner and could re-collide. Deleting is the
     safe, honest option (the queue is durable state, not a system of record).
  2. Rebuilds ``uq_repo_job_intent`` to lead with ``tenant_id`` so dedup is
     per-tenant.
  3. Sets ``tenant_id`` NOT NULL.
  4. Adds ``idx_repo_job_tenant`` (every API read now filters on tenant_id).

Postgres-only (fleet standard: postgres18 + psycopg v3). No SQLite path.
"""

import sqlalchemy as sa

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Drop the tenant-blind unique constraint before touching the column.
    op.drop_constraint("uq_repo_job_intent", "repo_jobs", type_="unique")

    # 2. Remove un-attributable NULL-tenant rows (pre-prod junk — see module doc).
    op.execute(sa.text("DELETE FROM repo_jobs WHERE tenant_id IS NULL"))

    # 3. Now that no NULLs remain, enforce NOT NULL.
    op.alter_column("repo_jobs", "tenant_id", existing_type=sa.String(128), nullable=False)

    # 4. Recreate the dedup key, tenant-scoped.
    op.create_unique_constraint(
        "uq_repo_job_intent",
        "repo_jobs",
        ["tenant_id", "repo_url", "package", "target"],
    )

    # 5. Index the tenant filter used by every API read.
    op.create_index("idx_repo_job_tenant", "repo_jobs", ["tenant_id"])


def downgrade() -> None:
    # Reversible schema-wise; the deleted NULL-tenant rows are NOT restored.
    op.drop_index("idx_repo_job_tenant", table_name="repo_jobs")
    op.drop_constraint("uq_repo_job_intent", "repo_jobs", type_="unique")
    op.alter_column("repo_jobs", "tenant_id", existing_type=sa.String(128), nullable=True)
    op.create_unique_constraint(
        "uq_repo_job_intent",
        "repo_jobs",
        ["repo_url", "package", "target"],
    )
