"""CanvasNode（画布节点）ORM 模型。"""

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class CanvasNode(UUIDPKMixin, TimestampMixin, Base):
    """画布节点 — 基于 React Flow，type=imageUpload/text/generated/uploaded/storyboard/assets。"""

    __tablename__ = "canvas_nodes"
    __table_args__ = (
        UniqueConstraint("material_id", "id", name="uq_canvas_nodes_material_id_id"),
    )

    material_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("materials.id"), nullable=False, index=True
    )
    # imageUpload | text | generated | uploaded | storyboard | assets
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    position: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    material = relationship("Materials", back_populates="nodes")
