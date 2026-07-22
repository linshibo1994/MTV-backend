"""扩展画布节点与边标识长度

Revision ID: c8e7d6f5a4b3
Revises: f4a8c2e1d907
Create Date: 2026-07-22 12:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c8e7d6f5a4b3"
down_revision: str | None = "f4a8c2e1d907"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SOURCE_FK = "fk_canvas_edges_source_same_material"
_TARGET_FK = "fk_canvas_edges_target_same_material"


def _drop_endpoint_foreign_keys() -> None:
    op.drop_constraint(_SOURCE_FK, "canvas_edges", type_="foreignkey")
    op.drop_constraint(_TARGET_FK, "canvas_edges", type_="foreignkey")


def _create_endpoint_foreign_keys() -> None:
    op.create_foreign_key(
        _SOURCE_FK,
        "canvas_edges",
        "canvas_nodes",
        ["material_id", "source"],
        ["material_id", "id"],
    )
    op.create_foreign_key(
        _TARGET_FK,
        "canvas_edges",
        "canvas_nodes",
        ["material_id", "target"],
        ["material_id", "id"],
    )


def upgrade() -> None:
    _drop_endpoint_foreign_keys()
    op.alter_column(
        "canvas_nodes",
        "id",
        existing_type=sa.String(length=36),
        type_=sa.String(length=255),
        existing_nullable=False,
    )
    for column in ("id", "source", "target"):
        op.alter_column(
            "canvas_edges",
            column,
            existing_type=sa.String(length=36),
            type_=sa.String(length=255),
            existing_nullable=False,
        )
    _create_endpoint_foreign_keys()


def downgrade() -> None:
    # 回滚前显式拒绝会被截断的数据，避免 VARCHAR(36) 转换造成不可恢复损坏。
    op.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM canvas_nodes WHERE length(id) > 36)
                    OR EXISTS (
                        SELECT 1
                        FROM canvas_edges
                        WHERE length(id) > 36
                           OR length(source) > 36
                           OR length(target) > 36
                    )
                THEN
                    RAISE EXCEPTION '画布中存在超过 36 字符的标识，拒绝不安全降级';
                END IF;
            END
            $$
            """
        )
    )

    _drop_endpoint_foreign_keys()
    for column in ("id", "source", "target"):
        op.alter_column(
            "canvas_edges",
            column,
            existing_type=sa.String(length=255),
            type_=sa.String(length=36),
            existing_nullable=False,
        )
    op.alter_column(
        "canvas_nodes",
        "id",
        existing_type=sa.String(length=255),
        type_=sa.String(length=36),
        existing_nullable=False,
    )
    _create_endpoint_foreign_keys()
