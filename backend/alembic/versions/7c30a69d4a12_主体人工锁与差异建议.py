"""主体人工锁与差异建议

Revision ID: 7c30a69d4a12
Revises: c03ed92c2a4f
Create Date: 2026-07-18 20:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "7c30a69d4a12"
down_revision: str | None = "c03ed92c2a4f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "subjects",
        sa.Column(
            "name_locked",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "subjects",
        sa.Column(
            "description_locked",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.alter_column("subjects", "name_locked", server_default=None)
    op.alter_column("subjects", "description_locked", server_default=None)

    op.create_table(
        "subject_change_suggestions",
        sa.Column("script_id", sa.String(length=36), nullable=False),
        sa.Column("subject_id", sa.String(length=36), nullable=False),
        sa.Column("field", sa.String(length=32), nullable=False),
        sa.Column("current_value", sa.Text(), nullable=True),
        sa.Column("suggested_value", sa.Text(), nullable=True),
        sa.Column("base_subject_version", sa.Integer(), nullable=False),
        sa.Column("source_run_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("resolved_by_user_id", sa.String(length=36), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "field IN ('name', 'description')",
            name="ck_subject_change_suggestions_field",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'accepted', 'ignored')",
            name="ck_subject_change_suggestions_status",
        ),
        sa.ForeignKeyConstraint(
            ["script_id"], ["scripts.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["subject_id"], ["subjects.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_subject_change_suggestions_script_id"),
        "subject_change_suggestions",
        ["script_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_subject_change_suggestions_subject_id"),
        "subject_change_suggestions",
        ["subject_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_subject_change_suggestions_source_run_id"),
        "subject_change_suggestions",
        ["source_run_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_subject_change_suggestions_status"),
        "subject_change_suggestions",
        ["status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_subject_change_suggestions_resolved_by_user_id"),
        "subject_change_suggestions",
        ["resolved_by_user_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_subject_change_suggestions_resolved_by_user_id"),
        table_name="subject_change_suggestions",
    )
    op.drop_index(
        op.f("ix_subject_change_suggestions_status"),
        table_name="subject_change_suggestions",
    )
    op.drop_index(
        op.f("ix_subject_change_suggestions_source_run_id"),
        table_name="subject_change_suggestions",
    )
    op.drop_index(
        op.f("ix_subject_change_suggestions_subject_id"),
        table_name="subject_change_suggestions",
    )
    op.drop_index(
        op.f("ix_subject_change_suggestions_script_id"),
        table_name="subject_change_suggestions",
    )
    op.drop_table("subject_change_suggestions")
    op.drop_column("subjects", "description_locked")
    op.drop_column("subjects", "name_locked")
