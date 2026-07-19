"""启用 Gemini Veo 视频供应商

Revision ID: b31d8f4a6c20
Revises: a91f0d7e3b24
Create Date: 2026-07-18 12:55:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b31d8f4a6c20"
down_revision: str | None = "a91f0d7e3b24"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PROVIDER_CONFIG_TABLE = "video_provider_configs"
_PROVIDER_CONFIG_CONSTRAINT = "ck_video_provider_configs_provider_key"
_PROVIDER_CONFIG_TEMP_CONSTRAINT = f"{_PROVIDER_CONFIG_CONSTRAINT}_replacement"
_VIDEO_JOB_TABLE = "video_generation_jobs"
_VIDEO_JOB_CONSTRAINT = "ck_video_jobs_provider_key"
_VIDEO_JOB_TEMP_CONSTRAINT = f"{_VIDEO_JOB_CONSTRAINT}_replacement"

_LEGACY_PROVIDER_CHECK = (
    "provider_key IN "
    "('jianyu', 'modelsai', 'volcengine', 'doubao', 'bailian', 'openai')"
)
_GEMINI_PROVIDER_CHECK = (
    "provider_key IN "
    "('jianyu', 'modelsai', 'volcengine', 'doubao', 'bailian', 'openai', 'gemini')"
)


def _replace_check_constraint(
    *,
    table_name: str,
    current_name: str,
    temporary_name: str,
    condition: str,
) -> None:
    # NOT VALID 先建立新写入防线再验证存量；DDL 锁在事务提交前不释放，需维护窗口执行。
    op.execute(
        sa.text(
            f"ALTER TABLE {table_name} "
            f"ADD CONSTRAINT {temporary_name} CHECK ({condition}) NOT VALID"
        )
    )
    op.execute(
        sa.text(f"ALTER TABLE {table_name} VALIDATE CONSTRAINT {temporary_name}")
    )
    op.drop_constraint(current_name, table_name, type_="check")
    op.execute(
        sa.text(
            f"ALTER TABLE {table_name} "
            f"RENAME CONSTRAINT {temporary_name} TO {current_name}"
        )
    )


def upgrade() -> None:
    _replace_check_constraint(
        table_name=_PROVIDER_CONFIG_TABLE,
        current_name=_PROVIDER_CONFIG_CONSTRAINT,
        temporary_name=_PROVIDER_CONFIG_TEMP_CONSTRAINT,
        condition=_GEMINI_PROVIDER_CHECK,
    )
    _replace_check_constraint(
        table_name=_VIDEO_JOB_TABLE,
        current_name=_VIDEO_JOB_CONSTRAINT,
        temporary_name=_VIDEO_JOB_TEMP_CONSTRAINT,
        condition=_GEMINI_PROVIDER_CHECK,
    )


def downgrade() -> None:
    # 锁住写入后检查两类业务数据，避免检查通过后又并发写入 gemini。
    op.execute(
        sa.text(
            "LOCK TABLE video_provider_configs, video_generation_jobs "
            "IN SHARE ROW EXCLUSIVE MODE"
        )
    )
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                gemini_config_count bigint;
                gemini_job_count bigint;
            BEGIN
                SELECT count(*) INTO gemini_config_count
                FROM video_provider_configs
                WHERE provider_key = 'gemini';

                SELECT count(*) INTO gemini_job_count
                FROM video_generation_jobs
                WHERE provider_key = 'gemini';

                IF gemini_config_count > 0 OR gemini_job_count > 0 THEN
                    RAISE EXCEPTION
                        '存在 Gemini 视频供应商数据，拒绝降级以避免数据丢失: configs=%, jobs=%',
                        gemini_config_count,
                        gemini_job_count;
                END IF;
            END
            $$
            """
        )
    )
    _replace_check_constraint(
        table_name=_PROVIDER_CONFIG_TABLE,
        current_name=_PROVIDER_CONFIG_CONSTRAINT,
        temporary_name=_PROVIDER_CONFIG_TEMP_CONSTRAINT,
        condition=_LEGACY_PROVIDER_CHECK,
    )
    _replace_check_constraint(
        table_name=_VIDEO_JOB_TABLE,
        current_name=_VIDEO_JOB_CONSTRAINT,
        temporary_name=_VIDEO_JOB_TEMP_CONSTRAINT,
        condition=_LEGACY_PROVIDER_CHECK,
    )
