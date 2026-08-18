"""ShotGroup（镜头组）ORM 模型。"""

import hashlib
import json

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.core.reference_media_limits import CONTINUATION_FROM_PREDECESSOR_KEY
from app.models.base import Base, TimestampMixin

DRAFT_SHOT_GROUP_UNIT_PREFIX = "draft:"
FINAL_SHOT_GROUP_UNIT_PREFIX = "group:"


def shot_group_content_hash(group: "ShotGroup") -> str:
    """只对会影响视频生成的可编辑字段生成稳定组级指纹。"""

    payload: dict[str, object] = {
        "audio_elements": group.audio_elements or {},
        "duration_seconds": group.duration_seconds,
        "materials": group.materials or {},
        "prompt": group.prompt or "",
        "video_duration_seconds": group.video_duration_seconds,
    }
    quality_summary = getattr(group, "quality_summary", None) or {}
    if quality_summary:
        payload["quality_summary"] = quality_summary
    prompt_context = getattr(group, "prompt_context", None) or {}
    if prompt_context:
        # 仅纳入脱敏模板/溯源锚点；历史 NULL 组继续保持旧指纹。
        payload["prompt_context"] = prompt_context
    # 新字段仅在产生实际语义时进入指纹：默认空报告 / 非续帧仍与上线前哈希
    # 一致，避免部署后无条件让全部历史视频失效。getattr 同时兼容分步迁移。
    if bool(getattr(group, "is_continuation", False)):
        payload["is_continuation"] = True
    reference_frames = getattr(group, "reference_frames", None)
    if (
        isinstance(reference_frames, dict)
        and reference_frames.get(CONTINUATION_FROM_PREDECESSOR_KEY) is True
    ):
        payload[CONTINUATION_FROM_PREDECESSOR_KEY] = True
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
    # 纯规则质量报告与边界/动作拍点摘要。仅用于告警、展示和生成输入指纹，
    # 不把 warn-only 结果升级成数据库约束。
    quality_summary: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default="{}",
    )
    # 生成时使用的结构化引用、模板与 token 绑定上下文；历史镜头组允许为空。
    prompt_context: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    # True 表示视频应优先承接同场景直接前驱的真实尾帧；默认关闭以兼容历史组。
    is_continuation: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )
    # 生成来源审计锚点；任务删除后保留镜头组并清空引用。
    source_storyboard_job_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("storyboard_jobs.job_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

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
        kwargs.setdefault(
            "materials",
            {"character": [], "background": [], "prop": [], "other": [], "video": []},
        )
        kwargs.setdefault("audio_elements", {"characters": [], "sounds": []})
        kwargs.setdefault("quality_summary", {})
        kwargs.setdefault("is_continuation", False)
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
