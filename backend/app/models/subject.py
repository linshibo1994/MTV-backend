"""Subject（资产主体）ORM 模型。"""

from sqlalchemy import Boolean, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class Subject(UUIDPKMixin, TimestampMixin, Base):
    """资产主体 — character/scene/prop/sound，可成父子层次。"""

    __tablename__ = "subjects"
    __table_args__ = (
        UniqueConstraint("script_id", "id", name="uq_subjects_script_id_id"),
        UniqueConstraint(
            "script_id",
            "type",
            "pipeline_name",
            name="uq_subjects_script_type_pipeline_name",
        ),
        Index("ix_subjects_script_review_type", "script_id", "review_status", "type"),
    )

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    # character | scene | prop | sound
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # 流水线首次创建时的稳定名称身份；人工改名不得修改此字段。
    pipeline_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        default=lambda context: str(context.get_current_parameters().get("name") or ""),
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    subject_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    # 人工修改后锁定；流水线只能生成差异建议，不能静默覆盖。
    name_locked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    description_locked: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )

    is_child: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_parent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    parent_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("parent_subjects.id"), nullable=True, index=True
    )

    episode_nos: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    # active 参与当前流水线；archive 仅保留历史资产引用，不再参与分析写回。
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    # not_designed | pending | approved
    review_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="not_designed"
    )
    image_entry_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey(
            "asset_entries.id",
            name="fk_subjects_image_entry_id_asset_entries",
            use_alter=True,
        ),
        nullable=True,
    )
    whitelist_status: Mapped[str | None] = mapped_column(String(64), nullable=True)

    script = relationship("Script", back_populates="subjects")
    parent_subject = relationship("ParentSubject", back_populates="children", lazy="selectin")
    asset_entries = relationship(
        "AssetEntry",
        back_populates="subject",
        foreign_keys="AssetEntry.subject_id",
        lazy="selectin",
    )
    image_entry = relationship(
        "AssetEntry",
        foreign_keys=[image_entry_id],
        post_update=True,
        lazy="selectin",
    )
