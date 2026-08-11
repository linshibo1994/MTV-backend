"""启用 neolink 视频供应商

Revision ID: 8c1f4a2e7b90
Revises: 50028cba1845
Create Date: 2026-08-10 16:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "8c1f4a2e7b90"
down_revision: str | None = "50028cba1845"
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
    "('jianyu', 'modelsai', 'volcengine', 'doubao', 'bailian', 'openai', 'gemini', "
    "'mgrouter')"
)
_NEOLINK_PROVIDER_CHECK = (
    "provider_key IN "
    "('jianyu', 'modelsai', 'volcengine', 'doubao', 'bailian', 'openai', 'gemini', "
    "'mgrouter', 'neolink')"
)


def _replace_check_constraint(
    *,
    table_name: str,
    current_name: str,
    temporary_name: str,
    condition: str,
) -> None:
    # 新约束先保护新写入，再验证存量；旧约束在验证完成前持续生效。
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
        condition=_NEOLINK_PROVIDER_CHECK,
    )
    _replace_check_constraint(
        table_name=_VIDEO_JOB_TABLE,
        current_name=_VIDEO_JOB_CONSTRAINT,
        temporary_name=_VIDEO_JOB_TEMP_CONSTRAINT,
        condition=_NEOLINK_PROVIDER_CHECK,
    )


def downgrade() -> None:
    # 锁在事务结束前持续持有，阻止检查通过后并发写入旧约束无法表达的数据。
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
                neolink_config_count bigint;
                neolink_job_count bigint;
            BEGIN
                SELECT count(*) INTO neolink_config_count
                FROM video_provider_configs
                WHERE provider_key = 'neolink';

                SELECT count(*) INTO neolink_job_count
                FROM video_generation_jobs
                WHERE provider_key = 'neolink';

                IF neolink_config_count > 0 OR neolink_job_count > 0 THEN
                    RAISE EXCEPTION
                        '存在 neolink 视频供应商数据，拒绝降级以避免数据丢失: configs=%, jobs=%',
                        neolink_config_count,
                        neolink_job_count;
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
