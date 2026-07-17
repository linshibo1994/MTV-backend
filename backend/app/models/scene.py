"""Scene（场景）ORM 模型。"""

from sqlalchemy import CheckConstraint, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPKMixin


class Scene(UUIDPKMixin, Base):
    """场景 — 分集下的场景分组，聚合多个镜头组。"""

    __tablename__ = "scenes"
    __table_args__ = (
        UniqueConstraint("episode_id", "scene_no", name="uq_scenes_episode_scene_no"),
        CheckConstraint("scene_no >= 1", name="ck_scenes_scene_no_positive"),
        CheckConstraint("shot_group_count >= 0", name="ck_scenes_group_count_nonnegative"),
        CheckConstraint("pending_shot_count >= 0", name="ck_scenes_pending_count_nonnegative"),
    )

    episode_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("episodes.episode_id"), nullable=False, index=True
    )
    scene_no: Mapped[int] = mapped_column(Integer, nullable=False)
    # 形如 bg:<background>
    scene_key: Mapped[str] = mapped_column(String(255), nullable=False)
    background: Mapped[str] = mapped_column(String(255), nullable=False)
    label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    shot_group_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pending_shot_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    episode = relationship("Episode", back_populates="scenes")
