"""Baseline - create the repo_jobs queue table.

Revision ID: 0001
Revises: None
Create Date: 2026-06-06

Matches the ORM in src/models/database.py exactly.
"""

import sqlalchemy as sa

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "repo_jobs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        # Intent (dedup key)
        sa.Column("repo_url", sa.String(1024), nullable=False),
        sa.Column("package", sa.String(256), nullable=False),
        sa.Column("target", sa.String(128), nullable=False),
        sa.Column("ecosystem", sa.String(32), nullable=False, server_default="python"),
        sa.Column("mode", sa.String(16), nullable=False, server_default="in-range"),
        # Routing / identity
        sa.Column("owner", sa.String(256), nullable=True),
        sa.Column("tenant_id", sa.String(128), nullable=True),
        sa.Column("token_ref", sa.String(256), nullable=True),
        sa.Column("correlation_id", sa.String(128), nullable=True),
        # Lifecycle
        sa.Column("status", sa.String(16), nullable=False, server_default="queued"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("last_error", sa.Text(), nullable=True),
        # Work products
        sa.Column("container_name", sa.String(256), nullable=True),
        sa.Column("branch_name", sa.String(256), nullable=True),
        sa.Column("pr_url", sa.String(1024), nullable=True),
        sa.Column("pr_number", sa.Integer(), nullable=True),
        sa.Column("is_cross_repository", sa.Boolean(), nullable=True),
        # Timestamps
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("repo_url", "package", "target", name="uq_repo_job_intent"),
    )
    op.create_index("idx_repo_job_status", "repo_jobs", ["status"])
    op.create_index("idx_repo_job_status_created", "repo_jobs", ["status", "created_at"])
    op.create_index("idx_repo_job_owner", "repo_jobs", ["owner"])


def downgrade() -> None:
    op.drop_index("idx_repo_job_owner", table_name="repo_jobs")
    op.drop_index("idx_repo_job_status_created", table_name="repo_jobs")
    op.drop_index("idx_repo_job_status", table_name="repo_jobs")
    op.drop_table("repo_jobs")
