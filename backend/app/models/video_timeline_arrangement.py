"""VideoTimelineArrangement（时间线编排）ORM 模型。"""

from sqlalchemy import Boolean, CheckConstraint, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class VideoTimelineArrangement(UUIDPKMixin, TimestampMixin, Base):
    """时间线片段 — 按 row_order 排列，trim 控制裁剪，关联视频生成 job。"""

    __tablename__ = "video_timeline_arrangements"
    __table_args__ = (
        UniqueConstraint(
            "script_id",
            "episode_id",
            "row_order",
            name="uq_video_timeline_arrangements_script_episode_row_order",
        ),
        CheckConstraint("row_order >= 0", name="ck_timeline_row_order_non_negative"),
        CheckConstraint(
            "trim_start_seconds >= 0",
            name="ck_timeline_trim_start_non_negative",
        ),
        CheckConstraint(
            "trim_end_seconds IS NULL OR trim_end_seconds > trim_start_seconds",
            name="ck_timeline_trim_end_after_start",
        ),
    )

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    episode_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("episodes.episode_id"), nullable=False, index=True
    )
    unit_runtime_key: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("video_generation_jobs.job_id"), nullable=False, index=True
    )
    row_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_selected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    trim_start_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    trim_end_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    # 乐观锁版本号: 每次 PATCH/reorder +1，客户端可携带 expected_version 校验冲突 (409)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    script = relationship("Script", lazy="noload")
    video_job = relationship("VideoGenerationJob", lazy="noload")
