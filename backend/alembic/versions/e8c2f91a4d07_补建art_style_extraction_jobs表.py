"""补建 art_style_extraction_jobs 表

Revision ID: e8c2f91a4d07
Revises: d4b7e1c9a362
Create Date: 2026-08-13 18:00:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "e8c2f91a4d07"
down_revision: str | None = "d4b7e1c9a362"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 线上数据库曾被 stamp 到基线 d4b7e1c9a362，但 ``art_style_extraction_jobs``
    # 表实际并未创建（schema 漂移）。此处用 ``if_not_exists`` 幂等补建，
    # 使实际 schema 与 alembic_version 声明的基线一致。
    # - 全新数据库：基线迁移已创建该表，本迁移为 no-op。
    # - 当前线上：表缺失，本迁移按基线定义补建表、约束与索引。
    op.create_table(
        "art_style_extraction_jobs",
        sa.Column("script_id", sa.String(length=36), nullable=False),
        sa.Column("actor_user_id", sa.String(length=36), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("source_asset_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_hashes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("input_snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("input_version_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_token", sa.String(length=128), nullable=True),
        sa.Column("leased_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("extraction_provider", sa.String(length=64), nullable=True),
        sa.Column("extraction_model", sa.String(length=255), nullable=True),
        sa.Column("art_style_id", sa.String(length=36), nullable=True),
        sa.Column("billing_mode", sa.String(length=32), server_default=sa.text("'unmetered'"), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("billing_mode = 'unmetered'", name="ck_art_style_extraction_jobs_unmetered"),
        sa.CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed')", name="ck_art_style_extraction_jobs_status"),
        sa.CheckConstraint("attempt_count >= 1", name="ck_art_style_extraction_jobs_attempt_positive"),
        sa.ForeignKeyConstraint(["art_style_id"], ["art_styles.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["script_id"], ["scripts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("script_id", "actor_user_id", "idempotency_key", name="uq_art_style_extraction_jobs_actor_idempotency"),
        if_not_exists=True,
    )
    op.create_index(op.f("ix_art_style_extraction_jobs_actor_user_id"), "art_style_extraction_jobs", ["actor_user_id"], unique=False, if_not_exists=True)
    op.create_index(op.f("ix_art_style_extraction_jobs_art_style_id"), "art_style_extraction_jobs", ["art_style_id"], unique=False, if_not_exists=True)
    op.create_index(op.f("ix_art_style_extraction_jobs_lease_owner"), "art_style_extraction_jobs", ["lease_owner"], unique=False, if_not_exists=True)
    op.create_index(op.f("ix_art_style_extraction_jobs_lease_token"), "art_style_extraction_jobs", ["lease_token"], unique=False, if_not_exists=True)
    op.create_index(op.f("ix_art_style_extraction_jobs_leased_until"), "art_style_extraction_jobs", ["leased_until"], unique=False, if_not_exists=True)
    op.create_index(op.f("ix_art_style_extraction_jobs_script_id"), "art_style_extraction_jobs", ["script_id"], unique=False, if_not_exists=True)
    op.create_index("ix_art_style_extraction_jobs_script_status", "art_style_extraction_jobs", ["script_id", "status"], unique=False, if_not_exists=True)
    op.create_index(op.f("ix_art_style_extraction_jobs_status"), "art_style_extraction_jobs", ["status"], unique=False, if_not_exists=True)
    op.create_index(
        "uq_art_style_extraction_jobs_active_name",
        "art_style_extraction_jobs",
        ["script_id", "name"],
        unique=True,
        postgresql_where=sa.text("status IN ('queued', 'running')"),
        sqlite_where=sa.text("status IN ('queued', 'running')"),
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index("uq_art_style_extraction_jobs_active_name", table_name="art_style_extraction_jobs", if_exists=True)
    op.drop_index(op.f("ix_art_style_extraction_jobs_status"), table_name="art_style_extraction_jobs", if_exists=True)
    op.drop_index("ix_art_style_extraction_jobs_script_status", table_name="art_style_extraction_jobs", if_exists=True)
    op.drop_index(op.f("ix_art_style_extraction_jobs_script_id"), table_name="art_style_extraction_jobs", if_exists=True)
    op.drop_index(op.f("ix_art_style_extraction_jobs_leased_until"), table_name="art_style_extraction_jobs", if_exists=True)
    op.drop_index(op.f("ix_art_style_extraction_jobs_lease_token"), table_name="art_style_extraction_jobs", if_exists=True)
    op.drop_index(op.f("ix_art_style_extraction_jobs_lease_owner"), table_name="art_style_extraction_jobs", if_exists=True)
    op.drop_index(op.f("ix_art_style_extraction_jobs_art_style_id"), table_name="art_style_extraction_jobs", if_exists=True)
    op.drop_index(op.f("ix_art_style_extraction_jobs_actor_user_id"), table_name="art_style_extraction_jobs", if_exists=True)
    op.drop_table("art_style_extraction_jobs", if_exists=True)
