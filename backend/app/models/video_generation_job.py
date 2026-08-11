"""VideoGenerationJob（视频生成任务）ORM 模型。"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.models.base import Base, TimestampMixin

VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH = 256

# 远端提交或恢复事实无法自动判定时，复用 failed 终态承载“待人工核对”。
# 这类失败不可自动重试、不可释放积分，也不可删除所属剧本。
VIDEO_SUBMISSION_MANUAL_REVIEW_ERROR_TYPE = "provider_submission_uncertain"


def is_video_job_manual_review(status: str | None, error_type: str | None) -> bool:
    """判断视频任务是否已进入保留账务与远端事实的人工核对状态。"""

    return status == "failed" and error_type == VIDEO_SUBMISSION_MANUAL_REVIEW_ERROR_TYPE


class VideoGenerationJob(TimestampMixin, Base):
    """视频生成任务 — 多参考图图生视频，provider_key 由数据库或 env 选择规范供应商。"""

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
            "provider_key IN "
            "('jianyu', 'modelsai', 'volcengine', 'doubao', 'neolink', 'bailian', 'openai', "
            "'gemini', 'mgrouter')",
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
    idempotency_key: Mapped[str | None] = mapped_column(
        String(VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH), nullable=True
    )
    request_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    input_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    reservation_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    dispatch_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    scene_no: Mapped[int] = mapped_column(Integer, nullable=False)
    # 形如 group:<shot_group_id>
    unit_id: Mapped[str] = mapped_column(String(64), nullable=False)
    unit_runtime_key: Mapped[str] = mapped_column(String(128), nullable=False, index=True)

    # jianyu | modelsai | neolink | bailian | openai | gemini | mgrouter
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


class VideoGenerationIdempotencyRequest(TimestampMixin, Base):
    """视频生成幂等请求事实，同一任务可绑定多个请求键。"""

    __tablename__ = "video_generation_idempotency_requests"
    __table_args__ = (
        PrimaryKeyConstraint(
            "script_id",
            "idempotency_key",
            name="pk_video_generation_idempotency_requests",
        ),
        CheckConstraint(
            "trim(idempotency_key) <> ''",
            name="ck_video_generation_idempotency_requests_key_not_blank",
        ),
    )

    script_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "scripts.id", name="fk_video_idem_request_script", ondelete="CASCADE"
        ),
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(VIDEO_IDEMPOTENCY_KEY_MAX_LENGTH), nullable=False
    )
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    job_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey(
            "video_generation_jobs.job_id",
            name="fk_video_idem_request_job",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    )


Index(
    "ix_video_generation_jobs_output_video_storage_key",
    VideoGenerationJob.output_json["video_storage_key"].as_string(),
)
