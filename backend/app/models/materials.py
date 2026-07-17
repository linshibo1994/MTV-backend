"""Materials（画布项目）ORM 模型。"""

from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class Materials(UUIDPKMixin, TimestampMixin, Base):
    """画布项目 — 一个剧本对应一个 materials。"""

    __tablename__ = "materials"
    __table_args__ = (
        UniqueConstraint("script_id", "id", name="uq_materials_script_id_id"),
    )

    name: Mapped[str] = mapped_column(String(512), nullable=False)
    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, unique=True
    )
    # 画布增量保存的单调 CAS 版本。
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    def __init__(self, **kwargs):
        kwargs.setdefault("version", 0)
        super().__init__(**kwargs)

    script = relationship("Script", back_populates="materials")
    nodes = relationship(
        "CanvasNode", back_populates="material", lazy="selectin",
        cascade="all, delete-orphan",
    )
    edges = relationship(
        "CanvasEdge", back_populates="material", lazy="selectin",
        cascade="all, delete-orphan",
    )
