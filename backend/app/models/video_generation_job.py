"""VideoGenerationJob（视频生成任务）ORM 模型。"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.models.base import Base, TimestampMixin


class VideoGenerationJob(TimestampMixin, Base):
    """视频生成任务 — 多参考图图生视频，provider_key 路由 jianyu/modelsai。"""

    __tablename__ = "video_generation_jobs"
    __table_args__ = (
        UniqueConstraint(
            "script_id",
            "idempotency_key",
            name="uq_video_generation_jobs_script_idempotency_key",
        ),
        UniqueConstraint("unit_id", "attempt", name="uq_video_generation_jobs_unit_attempt"),
        CheckConstraint(
            "status IN ('starting', 'processing', 'succeeded', 'failed', 'canceled')",
            name="ck_video_jobs_status",
        ),
        CheckConstraint("progress >= 0 AND progress <= 100", name="ck_video_jobs_progress_range"),
        CheckConstraint("attempt > 0", name="ck_video_jobs_attempt_positive"),
        CheckConstraint(
            "provider_key IN ('jianyu', 'modelsai', 'volcengine', 'doubao', 'bailian', 'openai')",
            name="ck_video_jobs_provider_key",
        ),
        CheckConstraint("job_type IN ('all_reference')", name="ck_video_jobs_job_type"),
    )

    job_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=new_id
    )
    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    episode_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("episodes.episode_id"), nullable=False, index=True
    )
    shot_group_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("shot_groups.shot_group_id"), nullable=False, index=True
    )
    owner_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    request_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    input_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    reservation_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    dispatch_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    scene_no: Mapped[int] = mapped_column(Integer, nullable=False)
    # 形如 group:<shot_group_id>
    unit_id: Mapped[str] = mapped_column(String(64), nullable=False)
    unit_runtime_key: Mapped[str] = mapped_column(String(128), nullable=False, index=True)

    # jianyu | modelsai
    provider_key: Mapped[str] = mapped_column(String(32), nullable=False)
    # all_reference（可扩展）
    job_type: Mapped[str] = mapped_column(String(32), nullable=False)
    # starting | processing | succeeded | failed | canceled
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="starting")
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    progress_stage: Mapped[str | None] = mapped_column(String(64), nullable=True)
    progress_message: Mapped[str | None] = mapped_column(String(512), nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    retryable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    input_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    output_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    metrics: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # ===== Worker 租约/并发保护（doc08 §2.1 乐观锁延伸 + §4.2 租约恢复）=====
    # 乐观锁版本号：每次更新 +1，CAS 防止并发覆写
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # 当前持有租约的 worker 标识（如 celery worker hostname），空闲时为 None
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    lease_token: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    # 租约到期时间，过期后可被其他 worker 抢占恢复
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    canceled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    script = relationship("Script", lazy="noload")
