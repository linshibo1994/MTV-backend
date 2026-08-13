"""补建 scene_auditions 与 subject_candidates 缺失列

Revision ID: f9a3c2187e5c
Revises: e8c2f91a4d07
Create Date: 2026-08-13 19:00:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "f9a3c2187e5c"
down_revision: str | None = "e8c2f91a4d07"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 线上 scene_auditions 与 subject_candidates 表曾被 stamp 到基线，但实际缺少
    # 部分列（基线 create_table 已包含，prod 旧版表没有）。用 if_not_exists 幂等补齐：
    # - 全新数据库：基线已建表含这些列，本迁移为 no-op。
    # - 当前线上：列缺失，按基线定义补列与 check 约束。
    op.add_column(
        "scene_auditions",
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        if_not_exists=True,
    )
    op.add_column(
        "scene_auditions",
        sa.Column("error_code", sa.String(length=64), nullable=True),
        if_not_exists=True,
    )
    op.add_column(
        "scene_auditions",
        sa.Column("error_message", sa.String(length=255), nullable=True),
        if_not_exists=True,
    )
    op.add_column(
        "scene_auditions",
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        if_not_exists=True,
    )
    op.add_column(
        "scene_auditions",
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        if_not_exists=True,
    )
    op.add_column(
        "subject_candidates",
        sa.Column("resolution_action", sa.String(length=32), nullable=True),
        if_not_exists=True,
    )
    op.add_column(
        "subject_candidates",
        sa.Column("resolution_request_hash", sa.String(length=64), nullable=True),
        if_not_exists=True,
    )

    # 补建与基线一致的 check 约束（幂等：已存在则跳过）。
    op.execute(
        "DO $do$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = "
        "'ck_scene_auditions_attempt_count_non_negative') THEN "
        "ALTER TABLE scene_auditions ADD CONSTRAINT ck_scene_auditions_attempt_count_non_negative "
        "CHECK (attempt_count >= 0); END IF; END $do$"
    )
    op.execute(
        "DO $do$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = "
        "'ck_subject_candidates_resolution_action') THEN "
        "ALTER TABLE subject_candidates ADD CONSTRAINT ck_subject_candidates_resolution_action "
        "CHECK (resolution_action IS NULL OR resolution_action IN ('link_existing', 'create_subject', 'ignore')); "
        "END IF; END $do$"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE subject_candidates DROP CONSTRAINT IF EXISTS ck_subject_candidates_resolution_action")
    op.execute("ALTER TABLE scene_auditions DROP CONSTRAINT IF EXISTS ck_scene_auditions_attempt_count_non_negative")
    op.drop_column("subject_candidates", "resolution_request_hash", if_exists=True)
    op.drop_column("subject_candidates", "resolution_action", if_exists=True)
    op.drop_column("scene_auditions", "finished_at", if_exists=True)
    op.drop_column("scene_auditions", "started_at", if_exists=True)
    op.drop_column("scene_auditions", "error_message", if_exists=True)
    op.drop_column("scene_auditions", "error_code", if_exists=True)
    op.drop_column("scene_auditions", "attempt_count", if_exists=True)
