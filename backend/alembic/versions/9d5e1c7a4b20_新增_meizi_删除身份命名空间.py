"""新增 MeiZi 删除身份命名空间

Revision ID: 9d5e1c7a4b20
Revises: 6f3c2a1b9d80
Create Date: 2026-07-30 18:20:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9d5e1c7a4b20"
down_revision: str | None = "6f3c2a1b9d80"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 旧映射无法安全推断其 frontupload 身份，使用哨兵值使删除 fail closed。
    op.add_column(
        "meizi_storage_objects",
        sa.Column(
            "delete_namespace",
            sa.String(length=64),
            server_default="legacy-unassigned",
            nullable=False,
        ),
    )
    op.alter_column(
        "meizi_storage_objects",
        "delete_namespace",
        server_default=None,
    )


def downgrade() -> None:
    # 删除该列会丢失防误删边界；只允许空表降级。
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                object_count bigint;
            BEGIN
                SELECT count(*) INTO object_count FROM meizi_storage_objects;
                IF object_count > 0 THEN
                    RAISE EXCEPTION
                        '存在 MeiZi 媒资映射，拒绝移除删除身份命名空间: objects=%',
                        object_count;
                END IF;
            END
            $$
            """
        )
    )
    op.drop_column("meizi_storage_objects", "delete_namespace")
