"""Episode（分集）ORM 模型。"""

from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.models.base import Base, TimestampMixin


class Episode(TimestampMixin, Base):
    """分集 — 剧本文本载体，episode_version/client_save_seq 乐观锁。"""

    __tablename__ = "episodes"
    __table_args__ = (
        UniqueConstraint("script_id", "episode_id", name="uq_episodes_script_id_episode_id"),
        UniqueConstraint("script_id", "episode_no", name="uq_episodes_script_no"),
    )

    episode_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=new_id
    )
    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    episode_no: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)

    script_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    script_file_storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    episode_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    client_save_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    script = relationship("Script", back_populates="episodes")
    scenes = relationship(
        "Scene", back_populates="episode", lazy="selectin", cascade="all, delete-orphan"
    )
