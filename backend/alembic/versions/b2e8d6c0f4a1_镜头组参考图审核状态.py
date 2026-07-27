"""镜头组参考图审核状态

Revision ID: b2e8d6c0f4a1
Revises: c8e7d6f5a4b3
Create Date: 2026-07-24 12:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b2e8d6c0f4a1"
down_revision: str | None = "c8e7d6f5a4b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "shot_groups",
        sa.Column(
            "reference_image_audit", postgresql.JSONB(astext_type=sa.Text()), nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column("shot_groups", "reference_image_audit", server_default=None)


def downgrade() -> None:
    op.drop_column("shot_groups", "reference_image_audit")
