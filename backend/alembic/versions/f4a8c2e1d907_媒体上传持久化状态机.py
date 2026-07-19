"""媒体上传持久化状态机

Revision ID: f4a8c2e1d907
Revises: e2a7c4d9b615
Create Date: 2026-07-19 01:20:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f4a8c2e1d907"
down_revision: str | None = "e2a7c4d9b615"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_STATUS_CHECK_CONSTRAINT = "ck_media_uploads_status"


def upgrade() -> None:
    op.create_table(
        "media_uploads",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("owner_script_id", sa.String(length=36), nullable=False),
        sa.Column("script_id", sa.String(length=36), nullable=True),
        sa.Column("material_id", sa.String(length=36), nullable=True),
        sa.Column("subject_id", sa.String(length=36), nullable=True),
        sa.Column("asset_id", sa.String(length=36), nullable=True),
        sa.Column("temp_storage_key", sa.String(length=512), nullable=False),
        sa.Column("final_storage_key", sa.String(length=512), nullable=False),
        sa.Column("original_filename", sa.String(length=512), nullable=False),
        sa.Column("mime_type", sa.String(length=128), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("last_error_type", sa.String(length=128), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "status IN ('staging', 'ready', 'failed', 'deleted')",
            name=_STATUS_CHECK_CONSTRAINT,
        ),
        sa.ForeignKeyConstraint(["asset_id"], ["asset_entries.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["material_id"], ["materials.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["script_id"], ["scripts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", name="uq_media_uploads_asset_id"),
        sa.UniqueConstraint("final_storage_key", name="uq_media_uploads_final_storage_key"),
        sa.UniqueConstraint("temp_storage_key", name="uq_media_uploads_temp_storage_key"),
    )
    for column in (
        "asset_id",
        "material_id",
        "owner_script_id",
        "script_id",
        "status",
        "subject_id",
    ):
        op.create_index(op.f(f"ix_media_uploads_{column}"), "media_uploads", [column], unique=False)

    # 检查和回填共用事务内表锁，避免检查通过后来源候选发生变化。
    op.execute(
        sa.text(
            "LOCK TABLE asset_entries, materials IN SHARE ROW EXCLUSIVE MODE"
        )
    )
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                duplicate_asset_id_groups bigint;
                duplicate_final_key_groups bigint;
                duplicate_temp_key_groups bigint;
            BEGIN
                WITH candidates AS (
                    SELECT
                        asset.id AS asset_id,
                        ltrim(asset.storage_key, '/') AS final_storage_key,
                        ltrim(asset.storage_key, '/') AS temp_storage_key
                    FROM asset_entries AS asset
                    JOIN materials AS material
                      ON material.script_id = asset.script_id
                     AND material.id = split_part(ltrim(asset.storage_key, '/'), '/', 2)
                    WHERE asset.source = 'upload_images'
                      AND ltrim(asset.storage_key, '/') LIKE 'upload_images/%'
                )
                SELECT
                    (
                        SELECT count(*)
                        FROM (
                            SELECT asset_id
                            FROM candidates
                            GROUP BY asset_id
                            HAVING count(*) > 1
                        ) AS duplicate_asset_ids
                    ),
                    (
                        SELECT count(*)
                        FROM (
                            SELECT final_storage_key
                            FROM candidates
                            GROUP BY final_storage_key
                            HAVING count(*) > 1
                        ) AS duplicate_final_keys
                    ),
                    (
                        SELECT count(*)
                        FROM (
                            SELECT temp_storage_key
                            FROM candidates
                            GROUP BY temp_storage_key
                            HAVING count(*) > 1
                        ) AS duplicate_temp_keys
                    )
                INTO
                    duplicate_asset_id_groups,
                    duplicate_final_key_groups,
                    duplicate_temp_key_groups;

                IF duplicate_asset_id_groups > 0
                    OR duplicate_final_key_groups > 0
                    OR duplicate_temp_key_groups > 0
                THEN
                    RAISE EXCEPTION
                        '存在媒体上传回填唯一键歧义，拒绝迁移: asset_id=%, final_storage_key=%, temp_storage_key=%',
                        duplicate_asset_id_groups,
                        duplicate_final_key_groups,
                        duplicate_temp_key_groups;
                END IF;
            END
            $$
            """
        )
    )

    # 旧上传资产已经可访问，按其 final key 回填 ready 记录；无法绑定真实 materials 的坏数据不猜测。
    op.execute(
        sa.text(
            """
            INSERT INTO media_uploads (
                id, owner_script_id, script_id, material_id, subject_id, asset_id,
                temp_storage_key, final_storage_key, original_filename,
                mime_type, size_bytes, sha256, status, completed_at,
                created_at, updated_at
            )
            SELECT
                md5('media-upload:' || asset.id),
                asset.script_id,
                asset.script_id,
                material.id,
                asset.subject_id,
                asset.id,
                ltrim(asset.storage_key, '/'),
                ltrim(asset.storage_key, '/'),
                'legacy-' || asset.id,
                asset.mime_type,
                asset.size_bytes,
                CASE
                    WHEN asset.sha256 ~ '^[0-9a-fA-F]{64}$' THEN lower(asset.sha256)
                    ELSE repeat('0', 64)
                END,
                CASE WHEN asset.status = 'deleted' THEN 'deleted' ELSE 'ready' END,
                asset.updated_at,
                asset.created_at,
                asset.updated_at
            FROM asset_entries AS asset
            JOIN materials AS material
              ON material.script_id = asset.script_id
             AND material.id = split_part(ltrim(asset.storage_key, '/'), '/', 2)
            WHERE asset.source = 'upload_images'
              AND ltrim(asset.storage_key, '/') LIKE 'upload_images/%'
            """
        )
    )

    op.add_column("media_storage_jobs", sa.Column("upload_id", sa.String(length=36), nullable=True))
    op.add_column("media_storage_jobs", sa.Column("expected_sha256", sa.String(length=64), nullable=True))
    op.add_column("media_storage_jobs", sa.Column("expected_size_bytes", sa.Integer(), nullable=True))
    op.add_column(
        "media_storage_jobs",
        sa.Column("max_attempts", sa.Integer(), server_default="8", nullable=False),
    )
    op.create_foreign_key(
        "fk_media_storage_jobs_upload_id_media_uploads",
        "media_storage_jobs",
        "media_uploads",
        ["upload_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(op.f("ix_media_storage_jobs_upload_id"), "media_storage_jobs", ["upload_id"])
    op.execute(
        sa.text(
            """
            UPDATE media_storage_jobs AS job
            SET upload_id = upload.id,
                expected_sha256 = NULLIF(upload.sha256, repeat('0', 64)),
                expected_size_bytes = CASE
                    WHEN upload.sha256 = repeat('0', 64) THEN NULL
                    ELSE upload.size_bytes
                END
            FROM media_uploads AS upload
            WHERE upload.asset_id = job.asset_id
            """
        )
    )
    # 默认值仅用于存量行回填；后续写入由 ORM/服务显式采用同一重试预算。
    op.alter_column("media_storage_jobs", "max_attempts", server_default=None)


def downgrade() -> None:
    # 旧版本无法表达上传状态源及新增的任务完整性/重试语义，存在数据时禁止有损降级。
    op.execute(
        sa.text(
            "LOCK TABLE media_uploads, media_storage_jobs IN SHARE ROW EXCLUSIVE MODE"
        )
    )
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                upload_count bigint;
                incompatible_job_count bigint;
            BEGIN
                SELECT count(*) INTO upload_count
                FROM media_uploads;

                SELECT count(*) INTO incompatible_job_count
                FROM media_storage_jobs
                WHERE upload_id IS NOT NULL
                   OR expected_sha256 IS NOT NULL
                   OR expected_size_bytes IS NOT NULL
                   OR max_attempts IS DISTINCT FROM 8;

                IF upload_count > 0 OR incompatible_job_count > 0 THEN
                    RAISE EXCEPTION
                        '存在无法由旧版本表达的媒体上传数据，拒绝降级: uploads=%, incompatible_jobs=%',
                        upload_count,
                        incompatible_job_count;
                END IF;
            END
            $$
            """
        )
    )

    op.drop_index(op.f("ix_media_storage_jobs_upload_id"), table_name="media_storage_jobs")
    op.drop_constraint(
        "fk_media_storage_jobs_upload_id_media_uploads",
        "media_storage_jobs",
        type_="foreignkey",
    )
    op.drop_column("media_storage_jobs", "max_attempts")
    op.drop_column("media_storage_jobs", "expected_size_bytes")
    op.drop_column("media_storage_jobs", "expected_sha256")
    op.drop_column("media_storage_jobs", "upload_id")
    for column in (
        "subject_id",
        "status",
        "script_id",
        "owner_script_id",
        "material_id",
        "asset_id",
    ):
        op.drop_index(
            op.f(f"ix_media_uploads_{column}"),
            table_name="media_uploads",
            if_exists=True,
        )
    op.drop_table("media_uploads")
