"""视频生成幂等请求映射

Revision ID: e2a7c4d9b615
Revises: b31d8f4a6c20
Create Date: 2026-07-19 00:35:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e2a7c4d9b615"
down_revision: str | None = "b31d8f4a6c20"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "video_generation_idempotency_requests"
_JOB_INDEX = "ix_video_generation_idempotency_requests_job_id"


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM video_generation_jobs
                    WHERE idempotency_key IS NOT NULL
                      AND btrim(idempotency_key) = ''
                ) THEN
                    RAISE EXCEPTION '存在空白视频生成幂等键，拒绝迁移';
                END IF;

                IF EXISTS (
                    SELECT 1
                    FROM video_generation_jobs
                    WHERE idempotency_key IS NOT NULL
                    GROUP BY script_id, idempotency_key
                    HAVING count(*) > 1
                ) THEN
                    RAISE EXCEPTION '存在重复视频生成幂等键，拒绝迁移';
                END IF;
            END
            $$
            """
        )
    )

    # 当前请求哈希可由三个不可变归属字段安全重建；写回后旧版本降级路径也可稳定重放。
    op.execute(
        sa.text(
            """
            UPDATE video_generation_jobs AS job
            SET request_hash = encode(
                sha256(
                    convert_to(
                        '{"episode_id":' || to_json(job.episode_id)::text ||
                        ',"script_id":' || to_json(job.script_id)::text ||
                        ',"shot_group_id":' || to_json(job.shot_group_id)::text ||
                        '}',
                        'UTF8'
                    )
                ),
                'hex'
            )
            WHERE job.idempotency_key IS NOT NULL
              AND length(job.request_hash) IS DISTINCT FROM 64
            """
        )
    )

    op.create_table(
        _TABLE,
        sa.Column("script_id", sa.String(length=36), nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=False),
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
            "trim(idempotency_key) <> ''",
            name="ck_video_generation_idempotency_requests_key_not_blank",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["video_generation_jobs.job_id"],
            name="fk_video_idem_request_job",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["script_id"],
            ["scripts.id"],
            name="fk_video_idem_request_script",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "script_id",
            "idempotency_key",
            name="pk_video_generation_idempotency_requests",
        ),
    )
    op.create_index(_JOB_INDEX, _TABLE, ["job_id"], unique=False)

    # 旧版本将主 key 保存在 job 上；迁移表后续还可为同一 job 绑定多个别名 key。
    op.execute(
        sa.text(
            """
            INSERT INTO video_generation_idempotency_requests (
                script_id,
                idempotency_key,
                request_hash,
                job_id,
                created_at,
                updated_at
            )
            SELECT
                job.script_id,
                job.idempotency_key,
                job.request_hash,
                job.job_id,
                job.created_at,
                job.updated_at
            FROM video_generation_jobs AS job
            WHERE job.idempotency_key IS NOT NULL
            """
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM video_generation_idempotency_requests AS request
                    JOIN video_generation_jobs AS job ON job.job_id = request.job_id
                    WHERE request.script_id IS DISTINCT FROM job.script_id
                       OR request.idempotency_key IS DISTINCT FROM job.idempotency_key
                       OR request.request_hash IS DISTINCT FROM job.request_hash
                ) THEN
                    RAISE EXCEPTION
                        '存在视频生成幂等别名或映射漂移，拒绝降级以避免丢失稳定幂等语义';
                END IF;
            END
            $$
            """
        )
    )
    op.drop_index(_JOB_INDEX, table_name=_TABLE)
    op.drop_table(_TABLE)
