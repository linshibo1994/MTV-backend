"""流水线新增主体的人在回路候选。"""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class SubjectCandidate(UUIDPKMixin, TimestampMixin, Base):
    """新增主体候选；与资产设计审核状态相互独立。"""

    __tablename__ = "subject_candidates"
    __table_args__ = (
        CheckConstraint(
            "subject_type IN ('character', 'scene', 'prop', 'sound')",
            name="ck_subject_candidates_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'linked', 'ignored')",
            name="ck_subject_candidates_status",
        ),
        CheckConstraint(
            "resolution_action IS NULL OR resolution_action IN "
            "('link_existing', 'create_subject', 'ignore')",
            name="ck_subject_candidates_resolution_action",
        ),
        Index(
            "uq_subject_candidates_pending_identity",
            "script_id",
            "subject_type",
            "pipeline_name",
            unique=True,
            postgresql_where=text("status = 'pending'"),
            sqlite_where=text("status = 'pending'"),
        ),
    )

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id", ondelete="CASCADE"), nullable=False, index=True
    )
    source_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    subject_type: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    aliases: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    episode_nos: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    evidence: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    linked_subject_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("subjects.id", ondelete="SET NULL"), nullable=True, index=True
    )
    resolved_by_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    # 仅保存已执行操作和规范化载荷哈希，用于区分同载荷重放与异负载冲突。
    resolution_action: Mapped[str | None] = mapped_column(String(32), nullable=True)
    resolution_request_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
