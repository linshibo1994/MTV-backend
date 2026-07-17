"""GenerationJob（图片生成任务）ORM 模型。"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.models.base import Base, TimestampMixin


class GenerationJob(TimestampMixin, Base):
    """图片生成任务 — 异步，前端轮询 status/progress。"""

    __tablename__ = "generation_jobs"
    __table_args__ = (
        UniqueConstraint(
            "owner_user_id",
            "idempotency_key",
            name="uq_generation_jobs_owner_idempotency_key",
        ),
        Index("ix_generation_jobs_status_created", "status", "created_at"),
        Index("ix_generation_jobs_material_created", "material_id", "created_at"),
        CheckConstraint(
            "status in ('starting','processing','succeeded','failed','canceled')",
            name="ck_generation_jobs_status",
        ),
        CheckConstraint(
            "progress >= 0 and progress <= 100",
            name="ck_generation_jobs_progress",
        ),
        CheckConstraint("attempt >= 1", name="ck_generation_jobs_attempt"),
        CheckConstraint(
            "input_version > 0",
            name="ck_generation_jobs_input_version_positive",
        ),
        CheckConstraint(
            "trim(idempotency_key) <> ''",
            name="ck_generation_jobs_idempotency_key_not_blank",
        ),
        CheckConstraint("job_type = 'generate_images'", name="ck_generation_jobs_job_type"),
        ForeignKeyConstraint(
            ["script_id", "material_id"],
            ["materials.script_id", "materials.id"],
            name="fk_generation_jobs_material_same_script",
        ).ddl_if(dialect="postgresql"),
        ForeignKeyConstraint(
            ["script_id", "subject_id"],
            ["subjects.script_id", "subjects.id"],
            name="fk_generation_jobs_subject_same_script",
        ).ddl_if(dialect="postgresql"),
        ForeignKeyConstraint(
            ["script_id", "episode_id"],
            ["episodes.script_id", "episodes.episode_id"],
            name="fk_generation_jobs_episode_same_script",
        ).ddl_if(dialect="postgresql"),
    )

    job_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=new_id
    )
    material_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("materials.id"), nullable=False, index=True
    )
    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    subject_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("subjects.id"), nullable=False, index=True
    )
    owner_user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.user_id"), nullable=False, index=True
    )
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    input_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    episode_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("episodes.episode_id"), nullable=True
    )
    scene_no: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # generate_images（可扩展）
    job_type: Mapped[str] = mapped_column(String(32), nullable=False)
    # starting | processing | succeeded | failed | canceled
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="starting")
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    progress_stage: Mapped[str | None] = mapped_column(String(64), nullable=True)
    progress_message: Mapped[str | None] = mapped_column(String(512), nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    retryable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    canceled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    lease_token: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    input_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    output_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    metrics: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    material = relationship("Materials", lazy="noload", foreign_keys=[material_id])
