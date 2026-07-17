"""ParentSubject（父主体）ORM 模型。"""

from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class ParentSubject(UUIDPKMixin, TimestampMixin, Base):
    """父主体 — 同类子主体的跨集去重聚合。"""

    __tablename__ = "parent_subjects"

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # character | background | prop | sound
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    child_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    script = relationship("Script", back_populates="parent_subjects")
    children = relationship("Subject", back_populates="parent_subject", lazy="selectin")
