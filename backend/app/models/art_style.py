"""可运营的动态画风模型。"""

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class ArtStyle(UUIDPKMixin, TimestampMixin, Base):
    """从授权参考图提取或人工维护的项目画风。"""

    __tablename__ = "art_styles"
    __table_args__ = (
        Index("ix_art_styles_script_active", "script_id", "is_active"),
        UniqueConstraint("script_id", "name", name="uq_art_styles_script_name"),
        CheckConstraint("extraction_version >= 1", name="ck_art_styles_extraction_version_positive"),
    )

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    prompt_zh: Mapped[str] = mapped_column(Text, nullable=False)
    prompt_en: Mapped[str] = mapped_column(Text, nullable=False)
    negative_prompt: Mapped[str] = mapped_column(Text, nullable=False, default="")
    visual_style: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source_asset_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    source_hashes: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    thumbnail_storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    extraction_provider: Mapped[str | None] = mapped_column(String(64), nullable=True)
    extraction_model: Mapped[str | None] = mapped_column(String(255), nullable=True)
    extraction_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    created_by_user_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    updated_by_user_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
