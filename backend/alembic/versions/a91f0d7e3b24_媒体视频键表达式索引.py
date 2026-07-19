"""媒体视频键表达式索引

Revision ID: a91f0d7e3b24
Revises: f6c9a31e8b42
Create Date: 2026-07-18 23:55:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a91f0d7e3b24"
down_revision: str | None = "f6c9a31e8b42"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_video_generation_jobs_output_video_storage_key",
        "video_generation_jobs",
        [sa.text("(CAST(output_json ->> 'video_storage_key' AS VARCHAR))")],
    )
    op.create_index(
        "ix_shot_groups_reference_frames_first_frame",
        "shot_groups",
        [sa.text("(CAST(reference_frames ->> 'first_frame' AS VARCHAR))")],
    )
    op.create_index(
        "ix_shot_groups_reference_frames_last_frame",
        "shot_groups",
        [sa.text("(CAST(reference_frames ->> 'last_frame' AS VARCHAR))")],
    )
    op.create_index(
        "ix_shot_groups_reference_frames_reference_frame",
        "shot_groups",
        [sa.text("(CAST(reference_frames ->> 'reference_frame' AS VARCHAR))")],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_shot_groups_reference_frames_reference_frame",
        table_name="shot_groups",
    )
    op.drop_index(
        "ix_shot_groups_reference_frames_last_frame",
        table_name="shot_groups",
    )
    op.drop_index(
        "ix_shot_groups_reference_frames_first_frame",
        table_name="shot_groups",
    )
    op.drop_index(
        "ix_video_generation_jobs_output_video_storage_key",
        table_name="video_generation_jobs",
    )
