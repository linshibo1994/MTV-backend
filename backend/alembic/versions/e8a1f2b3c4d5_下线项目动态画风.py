"""下线项目动态画风与生成提示词模板。

Revision ID: e8a1f2b3c4d5
Revises: d4b7e1c9a362

升级会永久删除动态画风、提取任务、已下线的生成提示词快照及 enrich 版本数据；
降级只恢复 schema，无法恢复升级时删除的历史值。部署前如需保留审计/排障信息，
应先单独备份相关数据。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "e8a1f2b3c4d5"
down_revision: str | None = "d4b7e1c9a362"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """清理遗留投递、画风、生成模板及历史任务快照。"""
    op.execute(sa.text("DELETE FROM task_dispatch_outbox WHERE task_type = 'art_style.extract'"))
    # 视频约束模板已不再参与生成；只删除该键，保留 storyboard.* 通用模板。
    op.execute(sa.text("DELETE FROM prompt_templates WHERE key = 'video.constraint_lock'"))
    # 历史任务快照可能保存已下线的画风请求字段，避免重试或审计继续暴露废弃配置。
    op.execute(
        sa.text(
            "UPDATE generation_jobs "
            "SET input_json = input_json - 'style_id' - 'art_style_id' - 'visual_style' - 'unify_style' "
            "- 'resolved_prompts' - 'enrich_version' "
            "WHERE input_json ?| ARRAY['style_id', 'art_style_id', 'visual_style', 'unify_style', "
            "'resolved_prompts', 'enrich_version']"
        )
    )
    op.execute(
        sa.text(
            "UPDATE video_generation_jobs "
            "SET input_json = input_json - 'style_id' - 'art_style_id' - 'visual_style' - 'unify_style' "
            "- 'constraint_prompt_template' "
            "WHERE input_json ?| ARRAY['style_id', 'art_style_id', 'visual_style', 'unify_style', "
            "'constraint_prompt_template']"
        )
    )
    # enrich 阶段 detail.prompts 是服务端生成的主体设计提示词，删除而不触碰其他阶段详情。
    op.execute(
        sa.text(
            "UPDATE pipeline_stages "
            "SET detail = detail - 'prompts' "
            "WHERE stage = 'enrich' AND detail ? 'prompts'"
        )
    )
    op.execute(
        sa.text(
            "UPDATE pipeline_jobs "
            "SET output_json = output_json - 'prompts' "
            "WHERE stage = 'enrich' AND output_json ? 'prompts'"
        )
    )
    # merge 产物中的 enrich 版本只服务于已下线的设计提示词缓存，保留主体其他快照字段。
    op.execute(
        sa.text(
            "UPDATE pipeline_jobs "
            "SET output_json = jsonb_set("
            "output_json, "
            "'{subjects}', "
            "(SELECT jsonb_agg(item - 'base_enrich_version' - 'enrich_version' ORDER BY ordinal) "
            "FROM jsonb_array_elements(output_json -> 'subjects') WITH ORDINALITY AS subjects(item, ordinal))"
            ") "
            "WHERE stage = 'merge' "
            "AND jsonb_typeof(output_json -> 'subjects') = 'array' "
            "AND EXISTS ("
            "SELECT 1 FROM jsonb_array_elements(output_json -> 'subjects') AS subjects(item) "
            "WHERE item ?| ARRAY['base_enrich_version', 'enrich_version']"
            ")"
        )
    )
    # extraction_jobs.art_style_id 引用 art_styles，必须先删子表。
    op.execute(sa.text("DELETE FROM art_style_extraction_jobs"))
    op.execute(sa.text("DELETE FROM art_styles"))
    op.drop_table("art_style_extraction_jobs")
    op.drop_table("art_styles")
    op.drop_column("subjects", "enrich_version")
    op.drop_column("subjects", "visual_style")
    op.drop_column("scripts", "unify_style")
    op.drop_column("scripts", "visual_style")


def downgrade() -> None:
    """恢复空的历史 schema；升级删除的数据无法无损恢复。"""
    op.add_column("scripts", sa.Column("visual_style", sa.String(length=32), nullable=True))
    op.add_column(
        "scripts",
        sa.Column("unify_style", sa.Boolean(), server_default=sa.text("true"), nullable=False),
    )
    op.add_column("subjects", sa.Column("visual_style", sa.String(length=32), nullable=True))
    op.add_column(
        "subjects",
        sa.Column("enrich_version", sa.Integer(), server_default=sa.text("1"), nullable=False),
    )
    op.alter_column("subjects", "enrich_version", server_default=None)

    op.create_table(
        "art_styles",
        sa.Column("script_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("prompt_zh", sa.Text(), nullable=False),
        sa.Column("prompt_en", sa.Text(), nullable=False),
        sa.Column("negative_prompt", sa.Text(), nullable=False),
        sa.Column("visual_style", sa.String(length=32), nullable=True),
        sa.Column("source_asset_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_hashes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("thumbnail_storage_key", sa.String(length=512), nullable=True),
        sa.Column("extraction_provider", sa.String(length=64), nullable=True),
        sa.Column("extraction_model", sa.String(length=255), nullable=True),
        sa.Column("extraction_version", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_by_user_id", sa.String(length=36), nullable=False),
        sa.Column("updated_by_user_id", sa.String(length=36), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("extraction_version >= 1", name="ck_art_styles_extraction_version_positive"),
        sa.ForeignKeyConstraint(["script_id"], ["scripts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("script_id", "name", name="uq_art_styles_script_name"),
    )
    op.create_index(op.f("ix_art_styles_created_by_user_id"), "art_styles", ["created_by_user_id"])
    op.create_index(op.f("ix_art_styles_is_active"), "art_styles", ["is_active"])
    op.create_index("ix_art_styles_script_active", "art_styles", ["script_id", "is_active"])
    op.create_index(op.f("ix_art_styles_script_id"), "art_styles", ["script_id"])
    op.create_index(op.f("ix_art_styles_updated_by_user_id"), "art_styles", ["updated_by_user_id"])

    op.create_table(
        "art_style_extraction_jobs",
        sa.Column("script_id", sa.String(length=36), nullable=False),
        sa.Column("actor_user_id", sa.String(length=36), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("source_asset_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_hashes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("input_snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("input_version_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_token", sa.String(length=128), nullable=True),
        sa.Column("leased_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("extraction_provider", sa.String(length=64), nullable=True),
        sa.Column("extraction_model", sa.String(length=255), nullable=True),
        sa.Column("art_style_id", sa.String(length=36), nullable=True),
        sa.Column("billing_mode", sa.String(length=32), server_default=sa.text("'unmetered'"), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("billing_mode = 'unmetered'", name="ck_art_style_extraction_jobs_unmetered"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed')",
            name="ck_art_style_extraction_jobs_status",
        ),
        sa.CheckConstraint("attempt_count >= 1", name="ck_art_style_extraction_jobs_attempt_positive"),
        sa.ForeignKeyConstraint(["art_style_id"], ["art_styles.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["script_id"], ["scripts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "script_id",
            "actor_user_id",
            "idempotency_key",
            name="uq_art_style_extraction_jobs_actor_idempotency",
        ),
    )
    op.create_index(
        op.f("ix_art_style_extraction_jobs_actor_user_id"),
        "art_style_extraction_jobs",
        ["actor_user_id"],
    )
    op.create_index(
        op.f("ix_art_style_extraction_jobs_art_style_id"),
        "art_style_extraction_jobs",
        ["art_style_id"],
    )
    op.create_index(op.f("ix_art_style_extraction_jobs_lease_owner"), "art_style_extraction_jobs", ["lease_owner"])
    op.create_index(op.f("ix_art_style_extraction_jobs_lease_token"), "art_style_extraction_jobs", ["lease_token"])
    op.create_index(op.f("ix_art_style_extraction_jobs_leased_until"), "art_style_extraction_jobs", ["leased_until"])
    op.create_index(op.f("ix_art_style_extraction_jobs_script_id"), "art_style_extraction_jobs", ["script_id"])
    op.create_index("ix_art_style_extraction_jobs_script_status", "art_style_extraction_jobs", ["script_id", "status"])
    op.create_index(op.f("ix_art_style_extraction_jobs_status"), "art_style_extraction_jobs", ["status"])
    op.create_index(
        "uq_art_style_extraction_jobs_active_name",
        "art_style_extraction_jobs",
        ["script_id", "name"],
        unique=True,
        postgresql_where=sa.text("status IN ('queued', 'running')"),
    )
