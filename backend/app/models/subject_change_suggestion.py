"""主体人工锁字段的流水线差异建议。"""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class SubjectChangeSuggestion(UUIDPKMixin, TimestampMixin, Base):
    """记录流水线对人工锁定字段提出、但未直接写入的候选值。"""

    __tablename__ = "subject_change_suggestions"
    __table_args__ = (
        CheckConstraint(
            "field IN ('name', 'description')",
            name="ck_subject_change_suggestions_field",
        ),
        CheckConstraint(
            "status IN ('pending', 'accepted', 'ignored')",
            name="ck_subject_change_suggestions_status",
        ),
        Index(
            "uq_subject_change_suggestions_pending_version",
            "subject_id",
            "field",
            "base_subject_version",
            unique=True,
            postgresql_where=text("status = 'pending'"),
            sqlite_where=text("status = 'pending'"),
        ),
    )

    script_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("scripts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    subject_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("subjects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    field: Mapped[str] = mapped_column(String(32), nullable=False)
    current_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    suggested_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    base_subject_version: Mapped[int] = mapped_column(Integer, nullable=False)
    source_run_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True
    )
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending", index=True
    )
    resolved_by_user_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
