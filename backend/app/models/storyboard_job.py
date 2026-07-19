"""StoryboardJob（分镜生成任务）ORM 模型。

用于异步分镜生成任务的状态/进度落库与轮询 (AC-VG01 可观测性)。
每集每次触发创建一条 job 记录，job_id 同时作为积分预扣的 source_id，
保证同集多次重新生成不会撞 CreditReservation 的 unique(source_type, source_id) 约束。
"""

from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.models.base import Base, TimestampMixin

STORYBOARD_IDEMPOTENCY_KEY_MAX_LENGTH = 128


class StoryboardJob(TimestampMixin, Base):
    """分镜生成任务 — LLM-5 异步生成的状态机载体。

    状态机: pending -> running -> succeeded | failed
    任务失败时由 worker 释放冻结积分并写 error_message。
    """

    __tablename__ = "storyboard_jobs"
    __table_args__ = (
        UniqueConstraint("episode_id", "attempt", name="uq_storyboard_jobs_episode_attempt"),
        UniqueConstraint(
            "episode_id",
            "idempotency_key",
            name="uq_storyboard_jobs_episode_idempotency_key",
        ),
        CheckConstraint("job_type = 'storyboard'", name="ck_storyboard_jobs_job_type"),
        CheckConstraint(
            "status in ('pending', 'running', 'succeeded', 'failed', 'canceled', 'retry')",
            name="ck_storyboard_jobs_status",
        ),
        CheckConstraint("progress >= 0 and progress <= 100", name="ck_storyboard_jobs_progress"),
        CheckConstraint("attempt >= 1", name="ck_storyboard_jobs_attempt"),
        CheckConstraint("input_version >= 1", name="ck_storyboard_jobs_input_version"),
    )

    job_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=new_id
    )
    job_type: Mapped[str] = mapped_column(String(32), nullable=False, default="storyboard")
    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    episode_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("episodes.episode_id"), nullable=False, index=True
    )
    # Celery AsyncResult.id (无 Celery 时为本地占位 id)
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    # 触发者，用于积分审计 actor_user_id
    triggered_by_user_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True
    )
    owner_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    episode_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    input_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    input_client_save_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    idempotency_key: Mapped[str] = mapped_column(
        String(STORYBOARD_IDEMPOTENCY_KEY_MAX_LENGTH), nullable=False, index=True
    )
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    lease_token: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    canceled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reservation_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    financial_cleanup_status: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    financial_cleanup_error: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    retryable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # pending | running | succeeded | failed | canceled
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    progress_stage: Mapped[str | None] = mapped_column(String(64), nullable=True)
    progress_message: Mapped[str | None] = mapped_column(String(512), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    # {scenes, shot_groups, shots} 生成计数 / {reservation_id} 积分预约
    detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    script = relationship("Script", lazy="selectin")
