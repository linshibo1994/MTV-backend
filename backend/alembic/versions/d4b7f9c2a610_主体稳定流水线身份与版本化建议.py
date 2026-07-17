"""主体稳定流水线身份与版本化建议

Revision ID: d4b7f9c2a610
Revises: e91b4f6a2c73
Create Date: 2026-07-18 22:20:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d4b7f9c2a610"
down_revision: str | None = "e91b4f6a2c73"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "subjects",
        sa.Column("pipeline_name", sa.String(length=255), nullable=True),
    )
    # 存量名称来源不可可靠区分人工或流水线，优先保护人工值：全部锁定。
    # 同名同类的异常存量仅让最早一条保留原身份，其余附加 ID，避免唯一约束失败。
    op.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT id,
                       row_number() OVER (
                           PARTITION BY script_id, type, name
                           ORDER BY created_at ASC, id ASC
                       ) AS ordinal
                FROM subjects
            )
            UPDATE subjects AS subject
            SET pipeline_name = CASE
                    WHEN ranked.ordinal = 1 THEN subject.name
                    ELSE left(subject.name, 210) || ' #' || subject.id
                END,
                name_locked = true,
                description_locked = true
            FROM ranked
            WHERE ranked.id = subject.id
            """
        )
    )
    op.alter_column("subjects", "pipeline_name", nullable=False)
    op.create_unique_constraint(
        "uq_subjects_script_type_pipeline_name",
        "subjects",
        ["script_id", "type", "pipeline_name"],
    )

    # 每个主体字段在同一基础版本最多保留一条 pending；迁移前重复项保留最新一条。
    op.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT id,
                       row_number() OVER (
                           PARTITION BY subject_id, field, base_subject_version
                           ORDER BY created_at DESC, id DESC
                       ) AS ordinal
                FROM subject_change_suggestions
                WHERE status = 'pending'
            )
            UPDATE subject_change_suggestions AS suggestion
            SET status = 'ignored',
                resolved_at = COALESCE(suggestion.resolved_at, now())
            FROM ranked
            WHERE ranked.id = suggestion.id
              AND ranked.ordinal > 1
            """
        )
    )
    op.create_index(
        "uq_subject_change_suggestions_pending_version",
        "subject_change_suggestions",
        ["subject_id", "field", "base_subject_version"],
        unique=True,
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_subject_change_suggestions_pending_version",
        table_name="subject_change_suggestions",
    )
    op.drop_constraint(
        "uq_subjects_script_type_pipeline_name",
        "subjects",
        type_="unique",
    )
    op.drop_column("subjects", "pipeline_name")
