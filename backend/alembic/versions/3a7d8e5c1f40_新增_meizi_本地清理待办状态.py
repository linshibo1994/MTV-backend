"""新增 MeiZi 本地清理待办状态

Revision ID: 3a7d8e5c1f40
Revises: 9d5e1c7a4b20
Create Date: 2026-07-30 11:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "3a7d8e5c1f40"
down_revision: str | None = "9d5e1c7a4b20"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "ck_meizi_storage_objects_status",
        "meizi_storage_objects",
        type_="check",
    )
    op.create_check_constraint(
        "ck_meizi_storage_objects_status",
        "meizi_storage_objects",
        "status IN ('uploading', 'active', 'deleting', 'cleanup_pending', 'deleted')",
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                pending_count bigint;
            BEGIN
                SELECT count(*) INTO pending_count
                FROM meizi_storage_objects
                WHERE status = 'cleanup_pending';
                IF pending_count > 0 THEN
                    RAISE EXCEPTION
                        '存在 MeiZi 本地清理待办，拒绝移除状态: objects=%',
                        pending_count;
                END IF;
            END
            $$
            """
        )
    )
    op.drop_constraint(
        "ck_meizi_storage_objects_status",
        "meizi_storage_objects",
        type_="check",
    )
    op.create_check_constraint(
        "ck_meizi_storage_objects_status",
        "meizi_storage_objects",
        "status IN ('uploading', 'active', 'deleting', 'deleted')",
    )
