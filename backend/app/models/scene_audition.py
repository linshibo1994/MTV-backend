"""关键场次导演方案比较模型。"""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class SceneAudition(UUIDPKMixin, TimestampMixin, Base):
    """绑定分集输入版本的关键场次多方案记录。"""

    __tablename__ = "scene_auditions"
    __table_args__ = (
        UniqueConstraint(
            "episode_id",
            "scene_no",
            "input_version",
            name="uq_scene_auditions_episode_scene_version",
        ),
        CheckConstraint("scene_no >= 1", name="ck_scene_auditions_scene_no_positive"),
        CheckConstraint("input_version >= 1", name="ck_scene_auditions_input_version_positive"),
        CheckConstraint(
            "scene_type IN ('character_introduction', 'identity_reveal', "
            "'relationship_reversal', 'climax_ending')",
            name="ck_scene_auditions_key_scene_type",
        ),
        CheckConstraint(
            "status IN ('draft', 'running', 'ready', 'selected', 'stale', 'failed')",
            name="ck_scene_auditions_status",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_scene_auditions_attempt_count_non_negative"),
    )

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id", ondelete="CASCADE"), nullable=False, index=True
    )
    episode_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("episodes.episode_id", ondelete="CASCADE"), nullable=False, index=True
    )
    scene_no: Mapped[int] = mapped_column(Integer, nullable=False)
    input_version: Mapped[int] = mapped_column(Integer, nullable=False)
    scene_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    plans: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    selected_plan_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="draft", index=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(255), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    selected_by_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    selected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
