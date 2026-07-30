"""新增 MeiZi 媒资对象映射

Revision ID: 6f3c2a1b9d80
Revises: b2e8d6c0f4a1
Create Date: 2026-07-29 15:20:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "6f3c2a1b9d80"
down_revision: str | None = "b2e8d6c0f4a1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "meizi_storage_objects",
        sa.Column("storage_key", sa.String(length=512), nullable=False),
        sa.Column("media_id", sa.BigInteger(), nullable=False),
        sa.Column("info_id", sa.BigInteger(), nullable=False),
        sa.Column("lib_type", sa.Integer(), nullable=False),
        sa.Column("media_kind", sa.String(length=16), nullable=False),
        sa.Column("request_id", sa.String(length=64), nullable=False),
        sa.Column("public_url", sa.Text(), nullable=True),
        sa.Column("mime_type", sa.String(length=128), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
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
            "media_kind IN ('img', 'audio', 'video', 'file')",
            name="ck_meizi_storage_objects_media_kind",
        ),
        sa.CheckConstraint(
            "status IN ('uploading', 'active', 'deleting', 'deleted')",
            name="ck_meizi_storage_objects_status",
        ),
        sa.CheckConstraint("media_id > 0", name="ck_meizi_storage_objects_media_id"),
        sa.CheckConstraint("info_id > 0", name="ck_meizi_storage_objects_info_id"),
        sa.CheckConstraint("lib_type > 0", name="ck_meizi_storage_objects_lib_type"),
        sa.CheckConstraint(
            "size_bytes >= 0",
            name="ck_meizi_storage_objects_size_bytes",
        ),
        sa.PrimaryKeyConstraint("storage_key"),
        sa.UniqueConstraint(
            "media_id",
            "lib_type",
            name="uq_meizi_storage_objects_media_id_lib_type",
        ),
    )
    op.create_index(
        op.f("ix_meizi_storage_objects_status"),
        "meizi_storage_objects",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    # 映射一旦存在就承载远端删除身份，直接降级会使 MeiZi 对象永久失联。
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
                        '存在 MeiZi 媒资映射，拒绝有损降级: objects=%',
                        object_count;
                END IF;
            END
            $$
            """
        )
    )
    op.drop_index(
        op.f("ix_meizi_storage_objects_status"),
        table_name="meizi_storage_objects",
    )
    op.drop_table("meizi_storage_objects")
