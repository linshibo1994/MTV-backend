"""Shot（镜头）ORM 模型。"""

from sqlalchemy import CheckConstraint, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.models.base import Base


class Shot(Base):
    """镜头 — 分镜最细粒度，含自然语言 content 与结构化 description。"""

    __tablename__ = "shots"
    __table_args__ = (
        UniqueConstraint("shot_group_id", "display_no", name="uq_shots_group_display_no"),
        UniqueConstraint("shot_group_id", "shot_no", name="uq_shots_group_shot_no"),
        CheckConstraint("scene_no >= 1", name="ck_shots_scene_no_positive"),
        CheckConstraint("display_no >= 1", name="ck_shots_display_no_positive"),
        CheckConstraint("start_seconds >= 0", name="ck_shots_start_nonnegative"),
        CheckConstraint("end_seconds > start_seconds", name="ck_shots_time_range"),
    )

    shot_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=new_id
    )
    scene_no: Mapped[int] = mapped_column(Integer, nullable=False)
    shot_group_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("shot_groups.shot_group_id"), nullable=False, index=True
    )
    group_no: Mapped[str] = mapped_column(String(32), nullable=False)
    shot_no: Mapped[str] = mapped_column(String(64), nullable=False)
    display_no: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    shot_line: Mapped[str] = mapped_column(Text, nullable=False)
    start_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    end_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    characters: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    props: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    sounds: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    dialogue_character: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    # storyboard.v2 的结构化单一事实源快照。迁移上线前/历史数据允许为空；
    # 展示字段仍保留 content/description/shot_line 以兼容既有客户端。
    structured_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    shot_group = relationship("ShotGroup", back_populates="shots")
