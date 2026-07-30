"""MTV 逻辑对象键与 MeiZi 媒资对象的持久映射。"""

from sqlalchemy import BigInteger, CheckConstraint, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class MeiziStorageObject(TimestampMixin, Base):
    """保存远端媒资身份，维持现有 ``storage_key`` 生命周期语义。"""

    __tablename__ = "meizi_storage_objects"
    __table_args__ = (
        UniqueConstraint(
            "media_id",
            "lib_type",
            name="uq_meizi_storage_objects_media_id_lib_type",
        ),
        CheckConstraint(
            "media_kind IN ('img', 'audio', 'video', 'file')",
            name="ck_meizi_storage_objects_media_kind",
        ),
        CheckConstraint(
            "status IN ('uploading', 'active', 'deleting', 'cleanup_pending', 'deleted')",
            name="ck_meizi_storage_objects_status",
        ),
        CheckConstraint("media_id > 0", name="ck_meizi_storage_objects_media_id"),
        CheckConstraint("info_id > 0", name="ck_meizi_storage_objects_info_id"),
        CheckConstraint("lib_type > 0", name="ck_meizi_storage_objects_lib_type"),
        CheckConstraint("size_bytes >= 0", name="ck_meizi_storage_objects_size_bytes"),
    )

    storage_key: Mapped[str] = mapped_column(String(512), primary_key=True)
    media_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # 视频详情接口历史上使用 videoId；其余类型与 media_id 相同。
    info_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    lib_type: Mapped[int] = mapped_column(Integer, nullable=False)
    media_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    request_id: Mapped[str] = mapped_column(String(64), nullable=False)
    public_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    mime_type: Mapped[str] = mapped_column(String(128), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    # frontupload 的非敏感身份命名空间指纹；不包含 salt 或请求 token。
    delete_namespace: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="uploading",
        index=True,
    )
