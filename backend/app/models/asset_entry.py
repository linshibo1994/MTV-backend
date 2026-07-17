"""AssetEntry（资产条目）ORM 模型。"""

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class AssetEntry(UUIDPKMixin, TimestampMixin, Base):
    """资产条目 — image/audio 的对象存储记录，挂载于 Subject。"""

    __tablename__ = "asset_entries"
    __table_args__ = (
        CheckConstraint(
            "(status = 'candidate' AND is_approved = false AND superseded = false "
            "AND superseded_by_id IS NULL) OR "
            "(status = 'approved' AND is_approved = true AND superseded = false "
            "AND superseded_by_id IS NULL) OR "
            "(status = 'superseded' AND is_approved = false AND superseded = true "
            "AND superseded_by_id IS NOT NULL) OR "
            "(status = 'deleted' AND is_approved = false AND superseded = false "
            "AND superseded_by_id IS NULL)",
            name="ck_asset_entries_lifecycle_consistent",
        ),
    )

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    subject_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("subjects.id"), nullable=False, index=True
    )
    # image | audio
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    storage_key: Mapped[str] = mapped_column(String(512), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(128), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    # generate_images | upload_images
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # candidate | approved | superseded | deleted
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="candidate", index=True
    )
    is_approved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    superseded_by_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("asset_entries.id"), nullable=True
    )
    # 兼容旧消费者：superseded 表示历史版本，不再视为当前已批准资产。
    superseded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    def __init__(self, **kwargs):
        """兼容旧调用方：从旧布尔字段推导初始生命周期。"""
        status = kwargs.get("status")
        if status is None:
            if kwargs.get("superseded", False):
                status = "superseded"
            elif kwargs.get("is_approved", False):
                status = "approved"
            else:
                status = "candidate"
            kwargs["status"] = status
        kwargs["is_approved"] = status == "approved"
        kwargs.setdefault("superseded", status == "superseded")
        super().__init__(**kwargs)

    def set_status(self, status: str, *, superseded_by_id: str | None = None) -> None:
        """更新生命周期并同步旧布尔字段。"""
        if status not in {"candidate", "approved", "superseded", "deleted"}:
            raise ValueError(f"不支持的资产状态: {status}")
        self.status = status
        self.is_approved = status == "approved"
        self.superseded = status == "superseded"
        self.superseded_by_id = superseded_by_id

    subject = relationship(
        "Subject",
        back_populates="asset_entries",
        foreign_keys=[subject_id],
    )
