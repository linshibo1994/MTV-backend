"""补齐当前模型迁移

Revision ID: 99ac501e725c
Revises: 8c1f4a2e7b90
Create Date: 2026-08-12 20:19:31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "99ac501e725c"
down_revision: str | None = "8c1f4a2e7b90"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
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
    op.create_index(op.f("ix_art_styles_created_by_user_id"), "art_styles", ["created_by_user_id"], unique=False)
    op.create_index(op.f("ix_art_styles_is_active"), "art_styles", ["is_active"], unique=False)
    op.create_index("ix_art_styles_script_active", "art_styles", ["script_id", "is_active"], unique=False)
    op.create_index(op.f("ix_art_styles_script_id"), "art_styles", ["script_id"], unique=False)
    op.create_index(op.f("ix_art_styles_updated_by_user_id"), "art_styles", ["updated_by_user_id"], unique=False)

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
        sa.CheckConstraint("attempt_count >= 1", name="ck_art_style_extraction_jobs_attempt_positive"),
        sa.CheckConstraint("billing_mode = 'unmetered'", name="ck_art_style_extraction_jobs_unmetered"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed')",
            name="ck_art_style_extraction_jobs_status",
        ),
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
        unique=False,
    )
    op.create_index(
        op.f("ix_art_style_extraction_jobs_art_style_id"),
        "art_style_extraction_jobs",
        ["art_style_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_art_style_extraction_jobs_leased_until"),
        "art_style_extraction_jobs",
        ["leased_until"],
        unique=False,
    )
    op.create_index(
        op.f("ix_art_style_extraction_jobs_lease_owner"),
        "art_style_extraction_jobs",
        ["lease_owner"],
        unique=False,
    )
    op.create_index(
        op.f("ix_art_style_extraction_jobs_lease_token"),
        "art_style_extraction_jobs",
        ["lease_token"],
        unique=False,
    )
    op.create_index(
        op.f("ix_art_style_extraction_jobs_script_id"),
        "art_style_extraction_jobs",
        ["script_id"],
        unique=False,
    )
    op.create_index(
        "ix_art_style_extraction_jobs_script_status",
        "art_style_extraction_jobs",
        ["script_id", "status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_art_style_extraction_jobs_status"),
        "art_style_extraction_jobs",
        ["status"],
        unique=False,
    )
    op.create_index(
        "uq_art_style_extraction_jobs_active_name",
        "art_style_extraction_jobs",
        ["script_id", "name"],
        unique=True,
        postgresql_where=sa.text("status IN ('queued', 'running')"),
        sqlite_where=sa.text("status IN ('queued', 'running')"),
    )

    op.create_table(
        "scene_auditions",
        sa.Column("script_id", sa.String(length=36), nullable=False),
        sa.Column("episode_id", sa.String(length=36), nullable=False),
        sa.Column("scene_no", sa.Integer(), nullable=False),
        sa.Column("input_version", sa.Integer(), nullable=False),
        sa.Column("scene_type", sa.String(length=64), nullable=False),
        sa.Column("source_hash", sa.String(length=64), nullable=False),
        sa.Column("plans", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("selected_plan_id", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.String(length=255), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("selected_by_user_id", sa.String(length=36), nullable=True),
        sa.Column("selected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "status IN ('draft', 'running', 'ready', 'selected', 'stale', 'failed')",
            name="ck_scene_auditions_status",
        ),
        sa.CheckConstraint("attempt_count >= 0", name="ck_scene_auditions_attempt_count_non_negative"),
        sa.CheckConstraint("input_version >= 1", name="ck_scene_auditions_input_version_positive"),
        sa.CheckConstraint(
            "scene_type IN ('character_introduction', 'identity_reveal', "
            "'relationship_reversal', 'climax_ending')",
            name="ck_scene_auditions_key_scene_type",
        ),
        sa.CheckConstraint("scene_no >= 1", name="ck_scene_auditions_scene_no_positive"),
        sa.ForeignKeyConstraint(["episode_id"], ["episodes.episode_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["script_id"], ["scripts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("episode_id", "scene_no", "input_version", name="uq_scene_auditions_episode_scene_version"),
    )
    op.create_index(op.f("ix_scene_auditions_episode_id"), "scene_auditions", ["episode_id"], unique=False)
    op.create_index(op.f("ix_scene_auditions_script_id"), "scene_auditions", ["script_id"], unique=False)
    op.create_index(op.f("ix_scene_auditions_status"), "scene_auditions", ["status"], unique=False)

    op.create_table(
        "subject_candidates",
        sa.Column("script_id", sa.String(length=36), nullable=False),
        sa.Column("source_run_id", sa.String(length=36), nullable=True),
        sa.Column("subject_type", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("pipeline_name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("aliases", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("episode_nos", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("evidence", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("linked_subject_id", sa.String(length=36), nullable=True),
        sa.Column("resolved_by_user_id", sa.String(length=36), nullable=True),
        sa.Column("resolution_action", sa.String(length=32), nullable=True),
        sa.Column("resolution_request_hash", sa.String(length=64), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("status IN ('pending', 'linked', 'ignored')", name="ck_subject_candidates_status"),
        sa.CheckConstraint(
            "resolution_action IS NULL OR resolution_action IN "
            "('link_existing', 'create_subject', 'ignore')",
            name="ck_subject_candidates_resolution_action",
        ),
        sa.CheckConstraint("subject_type IN ('character', 'scene', 'prop', 'sound')", name="ck_subject_candidates_type"),
        sa.ForeignKeyConstraint(["linked_subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["script_id"], ["scripts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_subject_candidates_linked_subject_id"), "subject_candidates", ["linked_subject_id"], unique=False)
    op.create_index(op.f("ix_subject_candidates_resolved_by_user_id"), "subject_candidates", ["resolved_by_user_id"], unique=False)
    op.create_index(op.f("ix_subject_candidates_script_id"), "subject_candidates", ["script_id"], unique=False)
    op.create_index(op.f("ix_subject_candidates_source_run_id"), "subject_candidates", ["source_run_id"], unique=False)
    op.create_index(op.f("ix_subject_candidates_status"), "subject_candidates", ["status"], unique=False)
    op.create_index(
        "uq_subject_candidates_pending_identity",
        "subject_candidates",
        ["script_id", "subject_type", "pipeline_name"],
        unique=True,
        postgresql_where=sa.text("status = 'pending'"),
        sqlite_where=sa.text("status = 'pending'"),
    )

    op.add_column("scripts", sa.Column("visual_style", sa.String(length=32), nullable=True))
    op.add_column(
        "scripts",
        sa.Column("unify_style", sa.Boolean(), nullable=False, server_default=sa.true()),
    )

    op.add_column(
        "shot_groups",
        sa.Column(
            "quality_summary",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column("shot_groups", sa.Column("prompt_context", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column(
        "shot_groups",
        sa.Column("is_continuation", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column("shot_groups", sa.Column("source_storyboard_job_id", sa.String(length=36), nullable=True))
    op.create_index(op.f("ix_shot_groups_source_storyboard_job_id"), "shot_groups", ["source_storyboard_job_id"], unique=False)
    op.create_foreign_key(
        "fk_shot_groups_source_storyboard_job_id_storyboard_jobs",
        "shot_groups",
        "storyboard_jobs",
        ["source_storyboard_job_id"],
        ["job_id"],
        ondelete="SET NULL",
    )

    op.add_column("shots", sa.Column("structured_data", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("storyboard_jobs", sa.Column("input_provenance", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("subjects", sa.Column("visual_style", sa.String(length=32), nullable=True))


def downgrade() -> None:
    op.drop_column("subjects", "visual_style")
    op.drop_column("storyboard_jobs", "input_provenance")
    op.drop_column("shots", "structured_data")
    op.drop_constraint("fk_shot_groups_source_storyboard_job_id_storyboard_jobs", "shot_groups", type_="foreignkey")
    op.drop_index(op.f("ix_shot_groups_source_storyboard_job_id"), table_name="shot_groups")
    op.drop_column("shot_groups", "source_storyboard_job_id")
    op.drop_column("shot_groups", "is_continuation")
    op.drop_column("shot_groups", "prompt_context")
    op.drop_column("shot_groups", "quality_summary")
    op.drop_column("scripts", "unify_style")
    op.drop_column("scripts", "visual_style")

    op.drop_index("uq_subject_candidates_pending_identity", table_name="subject_candidates", postgresql_where=sa.text("status = 'pending'"), sqlite_where=sa.text("status = 'pending'"))
    op.drop_index(op.f("ix_subject_candidates_status"), table_name="subject_candidates")
    op.drop_index(op.f("ix_subject_candidates_source_run_id"), table_name="subject_candidates")
    op.drop_index(op.f("ix_subject_candidates_script_id"), table_name="subject_candidates")
    op.drop_index(op.f("ix_subject_candidates_resolved_by_user_id"), table_name="subject_candidates")
    op.drop_index(op.f("ix_subject_candidates_linked_subject_id"), table_name="subject_candidates")
    op.drop_table("subject_candidates")

    op.drop_index(op.f("ix_scene_auditions_status"), table_name="scene_auditions")
    op.drop_index(op.f("ix_scene_auditions_script_id"), table_name="scene_auditions")
    op.drop_index(op.f("ix_scene_auditions_episode_id"), table_name="scene_auditions")
    op.drop_table("scene_auditions")

    op.drop_index(
        "uq_art_style_extraction_jobs_active_name",
        table_name="art_style_extraction_jobs",
        postgresql_where=sa.text("status IN ('queued', 'running')"),
        sqlite_where=sa.text("status IN ('queued', 'running')"),
    )
    op.drop_index(op.f("ix_art_style_extraction_jobs_status"), table_name="art_style_extraction_jobs")
    op.drop_index("ix_art_style_extraction_jobs_script_status", table_name="art_style_extraction_jobs")
    op.drop_index(op.f("ix_art_style_extraction_jobs_script_id"), table_name="art_style_extraction_jobs")
    op.drop_index(op.f("ix_art_style_extraction_jobs_lease_token"), table_name="art_style_extraction_jobs")
    op.drop_index(op.f("ix_art_style_extraction_jobs_lease_owner"), table_name="art_style_extraction_jobs")
    op.drop_index(op.f("ix_art_style_extraction_jobs_leased_until"), table_name="art_style_extraction_jobs")
    op.drop_index(op.f("ix_art_style_extraction_jobs_art_style_id"), table_name="art_style_extraction_jobs")
    op.drop_index(op.f("ix_art_style_extraction_jobs_actor_user_id"), table_name="art_style_extraction_jobs")
    op.drop_table("art_style_extraction_jobs")

    op.drop_index(op.f("ix_art_styles_updated_by_user_id"), table_name="art_styles")
    op.drop_index(op.f("ix_art_styles_script_id"), table_name="art_styles")
    op.drop_index("ix_art_styles_script_active", table_name="art_styles")
    op.drop_index(op.f("ix_art_styles_is_active"), table_name="art_styles")
    op.drop_index(op.f("ix_art_styles_created_by_user_id"), table_name="art_styles")
    op.drop_table("art_styles")
