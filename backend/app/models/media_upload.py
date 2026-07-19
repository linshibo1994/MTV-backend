"""用户上传媒体的持久化归属与存储生命周期。"""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class MediaUpload(UUIDPKMixin, TimestampMixin, Base):
    """跨数据库与对象存储边界的上传状态源。"""

    __tablename__ = "media_uploads"
    __table_args__ = (
        UniqueConstraint("temp_storage_key", name="uq_media_uploads_temp_storage_key"),
        UniqueConstraint("final_storage_key", name="uq_media_uploads_final_storage_key"),
        UniqueConstraint("asset_id", name="uq_media_uploads_asset_id"),
        CheckConstraint(
            "status IN ('staging', 'ready', 'failed', 'deleted')",
            name="ck_media_uploads_status",
        ),
    )

    owner_script_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    script_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("scripts.id", ondelete="SET NULL"), nullable=True, index=True
    )
    material_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("materials.id", ondelete="SET NULL"), nullable=True, index=True
    )
    subject_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("subjects.id", ondelete="SET NULL"), nullable=True, index=True
    )
    asset_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("asset_entries.id", ondelete="SET NULL"), nullable=True, index=True
    )
    temp_storage_key: Mapped[str] = mapped_column(String(512), nullable=False)
    final_storage_key: Mapped[str] = mapped_column(String(512), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(512), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(128), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="staging", index=True
    )
    last_error_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    def __init__(self, **kwargs):
        kwargs.setdefault("owner_script_id", kwargs.get("script_id"))
        super().__init__(**kwargs)
