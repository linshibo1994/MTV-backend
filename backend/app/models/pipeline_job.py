"""PipelineJob（流水线任务）ORM 模型。"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class PipelineJob(UUIDPKMixin, TimestampMixin, Base):
    """流水线任务；每个显式 attempt 独立成行且历史不可改写。"""

    __tablename__ = "pipeline_jobs"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "stage",
            "attempt",
            name="uq_pipeline_jobs_run_stage_attempt",
        ),
    )

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    # split | extract | merge | enrich | parents
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    dispatch_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    episode_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("episodes.episode_id"), nullable=True, index=True
    )
    # pending | running | done | failed
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    input_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    input_client_save_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retryable: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    # 每个 run/attempt 的阶段产物；成功结算前不得发布到业务表或全局阶段详情。
    output_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    script = relationship("Script", lazy="selectin")

    @property
    def lease_expires_at(self) -> datetime | None:
        return self.lease_until

    @lease_expires_at.setter
    def lease_expires_at(self, value: datetime | None) -> None:
        self.lease_until = value
