"""ShotGroup（镜头组）ORM 模型。"""

import hashlib
import json

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.models.base import Base, TimestampMixin

DRAFT_SHOT_GROUP_UNIT_PREFIX = "draft:"
FINAL_SHOT_GROUP_UNIT_PREFIX = "group:"


def shot_group_content_hash(group: "ShotGroup") -> str:
    """只对会影响视频生成的可编辑字段生成稳定组级指纹。"""

    payload = {
        "audio_elements": group.audio_elements or {},
        "duration_seconds": group.duration_seconds,
        "materials": group.materials or {},
        "prompt": group.prompt or "",
        "video_duration_seconds": group.video_duration_seconds,
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ShotGroup(TimestampMixin, Base):
    """镜头组 — 视频生成单元，多镜头分时段 prompt + 参考帧 + 素材汇总。"""

    __tablename__ = "shot_groups"
    __table_args__ = (
        UniqueConstraint("episode_id", "group_index", name="uq_shot_groups_episode_group_index"),
        UniqueConstraint("episode_id", "scene_no", "group_no", name="uq_shot_groups_episode_scene_group_no"),
        CheckConstraint("scene_no >= 1", name="ck_shot_groups_scene_no_positive"),
        CheckConstraint("group_index >= 1", name="ck_shot_groups_group_index_positive"),
        CheckConstraint("duration_seconds >= 3 and duration_seconds <= 15", name="ck_shot_groups_duration_range"),
        CheckConstraint(
            "video_duration_seconds is null or video_duration_seconds >= 0",
            name="ck_shot_groups_video_duration_nonnegative",
        ),
    )

    shot_group_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=new_id
    )
    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id"), nullable=False, index=True
    )
    episode_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("episodes.episode_id"), nullable=False, index=True
    )
    scene_no: Mapped[int] = mapped_column(Integer, nullable=False)
    group_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    group_no: Mapped[str] = mapped_column(String(32), nullable=False)

    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    # {first_frame, last_frame, reference_frame}
    reference_frames: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # 最终汇聚到视频任务的参考图审核快照；reference_hash 与每张图 sha256 绑定。
    reference_image_audit: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    video_duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # {character[], background[], prop[], other[]}
    materials: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    audio_elements: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    description_text: Mapped[str] = mapped_column(Text, nullable=False)

    # 形如 group:<id>
    unit_id: Mapped[str] = mapped_column(String(64), nullable=False)
    unit_runtime_key: Mapped[str] = mapped_column(String(128), nullable=False, index=True)

    shots = relationship(
        "Shot", back_populates="shot_group", lazy="selectin", cascade="all, delete-orphan"
    )

    def __init__(self, **kwargs):
        shot_group_id = kwargs.get("shot_group_id")
        episode_id = kwargs.get("episode_id")
        scene_no = kwargs.get("scene_no")
        group_no = kwargs.get("group_no")
        if (kwargs.get("group_index") or 0) <= 0:
            kwargs["group_index"] = 1
        if shot_group_id:
            kwargs.setdefault("unit_id", f"{FINAL_SHOT_GROUP_UNIT_PREFIX}{shot_group_id}")
        if shot_group_id and episode_id and scene_no:
            kwargs.setdefault("unit_runtime_key", f"{episode_id}:{scene_no}:group:{shot_group_id}")
        elif shot_group_id and episode_id and group_no:
            kwargs.setdefault("unit_runtime_key", f"{episode_id}:group:{group_no}:{shot_group_id}")
        kwargs.setdefault("prompt", "")
        kwargs.setdefault("duration_seconds", 3)
        kwargs.setdefault("description_text", "")
        kwargs.setdefault("reference_frames", {})
        kwargs.setdefault("reference_image_audit", {})
        kwargs.setdefault("materials", {"character": [], "background": [], "prop": [], "other": []})
        kwargs.setdefault("audio_elements", {"characters": [], "sounds": []})
        super().__init__(**kwargs)


Index(
    "ix_shot_groups_reference_frames_first_frame",
    ShotGroup.reference_frames["first_frame"].as_string(),
)
Index(
    "ix_shot_groups_reference_frames_last_frame",
    ShotGroup.reference_frames["last_frame"].as_string(),
)
Index(
    "ix_shot_groups_reference_frames_reference_frame",
    ShotGroup.reference_frames["reference_frame"].as_string(),
)
