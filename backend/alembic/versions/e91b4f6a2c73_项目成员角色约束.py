"""项目成员角色约束

Revision ID: e91b4f6a2c73
Revises: 7c30a69d4a12
Create Date: 2026-07-18 21:15:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "e91b4f6a2c73"
down_revision: str | None = "7c30a69d4a12"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_project_members_role",
        "project_members",
        "project_role IN ('owner', 'manager', 'editor', 'reviewer', 'viewer')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_project_members_role",
        "project_members",
        type_="check",
    )
