"""Script（剧本）ORM 模型。"""

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class Script(UUIDPKMixin, TimestampMixin, Base):
    """剧本 — 顶层创作对象，scope 区分 personal/team。"""

    __tablename__ = "scripts"

    title: Mapped[str] = mapped_column(String(512), nullable=False)
    # personal | team
    scope: Mapped[str] = mapped_column(String(32), nullable=False, default="personal")
    owner_user_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    org_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    created_by_user_id: Mapped[str] = mapped_column(String(36), nullable=False)
    episodes = relationship(
        "Episode", back_populates="script", lazy="selectin", cascade="all, delete-orphan"
    )
    subjects = relationship(
        "Subject", back_populates="script", lazy="selectin", cascade="all, delete-orphan"
    )
    parent_subjects = relationship(
        "ParentSubject", back_populates="script", lazy="selectin",
        cascade="all, delete-orphan",
    )
    materials = relationship(
        "Materials", back_populates="script", lazy="selectin", uselist=False,
        cascade="all, delete-orphan",
    )
