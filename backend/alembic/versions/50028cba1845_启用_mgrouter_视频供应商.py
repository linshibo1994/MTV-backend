"""启用 mgrouter 视频供应商

Revision ID: 50028cba1845
Revises: 3a7d8e5c1f40
Create Date: 2026-08-05 16:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "50028cba1845"
down_revision: str | None = "3a7d8e5c1f40"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PROVIDER_CONFIG_TABLE = "video_provider_configs"
_PROVIDER_CONFIG_CONSTRAINT = "ck_video_provider_configs_provider_key"
_PROVIDER_CONFIG_TEMP_CONSTRAINT = f"{_PROVIDER_CONFIG_CONSTRAINT}_replacement"
_VIDEO_JOB_TABLE = "video_generation_jobs"
_VIDEO_JOB_CONSTRAINT = "ck_video_jobs_provider_key"
_VIDEO_JOB_TEMP_CONSTRAINT = f"{_VIDEO_JOB_CONSTRAINT}_replacement"

_PREVIOUS_PROVIDER_CHECK = (
    "provider_key IN "
    "('jianyu', 'modelsai', 'volcengine', 'doubao', 'bailian', 'openai', 'gemini')"
)
_MGROUTER_PROVIDER_CHECK = (
    "provider_key IN "
    "('jianyu', 'modelsai', 'volcengine', 'doubao', 'bailian', 'openai', 'gemini', "
    "'mgrouter')"
)


def _replace_check_constraint(
    *,
    table_name: str,
    current_name: str,
    temporary_name: str,
    condition: str,
) -> None:
    # 新约束从创建起约束新写入，验证完成前保留旧约束，避免出现无保护窗口。
    op.execute(
        sa.text(
            f"ALTER TABLE {table_name} "
            f"ADD CONSTRAINT {temporary_name} CHECK ({condition}) NOT VALID"
        )
    )
    op.execute(sa.text(f"ALTER TABLE {table_name} VALIDATE CONSTRAINT {temporary_name}"))
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
        condition=_MGROUTER_PROVIDER_CHECK,
    )
    _replace_check_constraint(
        table_name=_VIDEO_JOB_TABLE,
        current_name=_VIDEO_JOB_CONSTRAINT,
        temporary_name=_VIDEO_JOB_TEMP_CONSTRAINT,
        condition=_MGROUTER_PROVIDER_CHECK,
    )


def downgrade() -> None:
    # 锁在 Alembic 事务结束前持续持有，阻止检查后并发写入无法由旧约束表达的数据。
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
                mgrouter_config_count bigint;
                mgrouter_job_count bigint;
            BEGIN
                SELECT count(*) INTO mgrouter_config_count
                FROM video_provider_configs
                WHERE provider_key = 'mgrouter';

                SELECT count(*) INTO mgrouter_job_count
                FROM video_generation_jobs
                WHERE provider_key = 'mgrouter';

                IF mgrouter_config_count > 0 OR mgrouter_job_count > 0 THEN
                    RAISE EXCEPTION
                        '存在 mgrouter 视频供应商数据，拒绝降级以避免数据丢失: configs=%, jobs=%',
                        mgrouter_config_count,
                        mgrouter_job_count;
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
        condition=_PREVIOUS_PROVIDER_CHECK,
    )
    _replace_check_constraint(
        table_name=_VIDEO_JOB_TABLE,
        current_name=_VIDEO_JOB_CONSTRAINT,
        temporary_name=_VIDEO_JOB_TEMP_CONSTRAINT,
        condition=_PREVIOUS_PROVIDER_CHECK,
    )
