"""PipelineStage（流水线阶段状态）ORM 模型。"""

from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class PipelineStage(UUIDPKMixin, TimestampMixin, Base):
    """流水线阶段状态 — split/extract/merge/enrich/parents 每阶段一行。"""

    __tablename__ = "pipeline_stages"
    __table_args__ = (
        UniqueConstraint("script_id", "stage", name="uq_pipeline_stages_script_stage"),
    )

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    # split | extract | merge | enrich | parents
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    # pending | running | done | failed
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    current: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    script = relationship("Script", lazy="selectin")
