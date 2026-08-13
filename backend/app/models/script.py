"""Script（剧本）ORM 模型。"""

from sqlalchemy import Boolean, String
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
    # 渲染媒介维度（如 live_action/anime）；空值表示使用系统默认。
    visual_style: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # True 时主体缺省继承项目媒介；主体仍可显式覆盖。
    unify_style: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
    )

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
