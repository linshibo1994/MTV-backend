"""CanvasEdge（画布连线）ORM 模型。"""

from sqlalchemy import ForeignKey, ForeignKeyConstraint, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.models.base import Base, TimestampMixin, UUIDPKMixin


class CanvasEdge(UUIDPKMixin, TimestampMixin, Base):
    """画布连线 — React Flow 边，连接 source/target 节点。"""

    __tablename__ = "canvas_edges"
    __table_args__ = (
        ForeignKeyConstraint(
            ["material_id", "source"],
            ["canvas_nodes.material_id", "canvas_nodes.id"],
            name="fk_canvas_edges_source_same_material",
        ),
        ForeignKeyConstraint(
            ["material_id", "target"],
            ["canvas_nodes.material_id", "canvas_nodes.id"],
            name="fk_canvas_edges_target_same_material",
        ),
    )

    # React Flow 默认边 ID 会拼接两端节点及 handle，长度可能明显超过 UUID。
    id: Mapped[str] = mapped_column(String(255), primary_key=True, default=new_id)
    material_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("materials.id"), nullable=False, index=True
    )
    source: Mapped[str] = mapped_column(String(255), nullable=False)
    target: Mapped[str] = mapped_column(String(255), nullable=False)
    data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    material = relationship("Materials", back_populates="edges")
