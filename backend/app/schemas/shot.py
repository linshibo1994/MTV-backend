"""分镜模块 Pydantic schemas。

严格遵循 04-数据模型与API文档.md 第 1.14~1.18 节与 02-功能规格文档.md 第 5.3~5.6 节。

层级: Episode -> Scenes(背景场景) -> Shot Groups(镜头组) -> Shots(镜头)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# 基础出参
# ---------------------------------------------------------------------------


class StoryboardFieldsOut(BaseModel):
    """由既有 description/shot_line 确定性解析出的分镜展示字段。"""

    model_config = ConfigDict(from_attributes=True)

    scene: str | None = None
    background: str | None = None
    shot_size: str | None = None
    shot_scale: str | None = None
    camera_angle: str | None = None
    camera: str | None = None
    camera_movement: str | None = None
    action: str | None = None
    frame_description: str | None = None
    lighting: str | None = None
    emotion: str | None = None
    narrative_purpose: str | None = None
    dialogue_text: str | None = None
    dialogue_kind: str | None = None
    composition: str | None = None
    sound_design: str | None = None


class ShotOut(StoryboardFieldsOut):
    """镜头 — 分镜最细粒度。"""

    model_config = ConfigDict(from_attributes=True)

    shot_id: str
    scene_no: int
    shot_group_id: str
    group_no: str
    shot_no: str
    display_no: int
    content: str | None = None
    description: str | None = None
    shot_line: str | None = None
    start_seconds: int | None = None
    end_seconds: int | None = None
    characters: list[str] = Field(default_factory=list)
    props: list[str] = Field(default_factory=list)
    sounds: list[str] = Field(default_factory=list)
    dialogue_character: str = ""


class ReferenceFramesOut(BaseModel):
    """首/尾/参考帧 — 相邻镜头组视觉连续性。"""

    model_config = ConfigDict(from_attributes=True)

    first_frame: str | None = None
    last_frame: str | None = None
    reference_frame: str | None = None


class ReferenceImageAuditOut(BaseModel):
    """镜头组最终参考图的火山审核状态。"""

    status: Literal["pending", "approved", "rejected", "review", "not_required", "failed"]
    image_count: int = 0
    audited_at: datetime | None = None
    results: list[dict[str, Any]] = Field(default_factory=list)
    message: str | None = None


class MaterialItemOut(BaseModel):
    """素材主体引用，兼容旧字符串输入在 CRUD 层归一化。"""

    name: str
    subject_id: str | None = None
    asset_entry_id: str | None = None


class MaterialsOut(BaseModel):
    """镜头组引用的主体资产分组。"""

    character: list[MaterialItemOut] = Field(default_factory=list)
    background: list[MaterialItemOut] = Field(default_factory=list)
    prop: list[MaterialItemOut] = Field(default_factory=list)
    other: list[MaterialItemOut] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Shot Hierarchy — GET /scripts/<sid>/episodes/<eid>/shot-hierarchy
# ---------------------------------------------------------------------------


class ShotGroupHierarchyOut(BaseModel):
    """镜头组 (shot-hierarchy 视角)。"""

    model_config = ConfigDict(from_attributes=True)

    shot_group_id: str
    scene_no: int
    group_no: str
    prompt: str | None = None
    duration_seconds: int | None = None
    narrative_purpose: str | None = None
    materials: MaterialsOut | None = None
    audio_elements: list[dict[str, Any]] = Field(default_factory=list)
    reference_frames: ReferenceFramesOut | None = None
    shots: list[ShotOut] = Field(default_factory=list)


class SceneHierarchyOut(BaseModel):
    """场景 (shot-hierarchy 视角) — 按背景聚合。"""

    model_config = ConfigDict(from_attributes=True)

    scene_key: str | None = None
    scene_no: int
    background: str | None = None
    label: str | None = None
    groups: list[ShotGroupHierarchyOut] = Field(default_factory=list)
    ungrouped_shots: list[ShotOut] = Field(default_factory=list)


class ShotHierarchyOut(BaseModel):
    """分镜层级响应。"""

    model_config = ConfigDict(from_attributes=True)

    episode_id: str
    episode_version: int
    scenes: list[SceneHierarchyOut] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Video Workbench — GET /shots/<sid>/episodes/<eid>/video-workbench
# ---------------------------------------------------------------------------


class ShotGroupWorkbenchOut(BaseModel):
    """镜头组 (video-workbench 视角) — 含视频生成单元字段。"""

    model_config = ConfigDict(from_attributes=True)

    shot_group_id: str
    scene_no: int
    background: str | None = None
    scene_key: str | None = None
    group_no: str
    unit_id: str | None = None
    unit_runtime_key: str | None = None
    prompt: str | None = None
    duration_seconds: int | None = None
    video_duration_seconds: int | None = None
    materials: MaterialsOut | None = None
    audio_elements: list[dict[str, Any]] = Field(default_factory=list)
    shot_ids: list[str] = Field(default_factory=list)
    shot_display_nos: list[int] = Field(default_factory=list)
    shot_nos: list[str] = Field(default_factory=list)
    description_text: str | None = None
    reference_frames: ReferenceFramesOut | None = None
    reference_image_audit: ReferenceImageAuditOut
    content_hash: str = Field(min_length=64, max_length=64)
    updated_at: datetime


class SceneWorkbenchOut(BaseModel):
    """场景 (video-workbench 视角)。"""

    model_config = ConfigDict(from_attributes=True)

    scene_key: str | None = None
    scene_no: int
    background: str | None = None
    label: str | None = None
    shot_group_count: int = 0
    pending_shot_count: int = 0
    shot_groups: list[ShotGroupWorkbenchOut] = Field(default_factory=list)
    pending_shots: list[ShotOut] = Field(default_factory=list)


class VideoWorkbenchOut(BaseModel):
    """视频工作台响应。"""

    model_config = ConfigDict(from_attributes=True)

    episode_id: str
    episode_version: int
    scenes: list[SceneWorkbenchOut] = Field(default_factory=list)


class ShotGroupMergeIn(BaseModel):
    """把当前分集的全部小分镜自动组合为正式镜头组。"""

    shot_ids: list[str] = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="当前分集分镜生成结果中的全部镜头 ID；数组顺序不作为叙事顺序",
    )
    expected_episode_version: int = Field(..., ge=1)

    @field_validator("shot_ids")
    @classmethod
    def validate_shot_ids(cls, values: list[str]) -> list[str]:
        normalised = [value.strip() for value in values]
        if any(not value or len(value) > 36 for value in normalised):
            raise ValueError("shot_ids 包含非法镜头 ID")
        if len(set(normalised)) != len(normalised):
            raise ValueError("shot_ids 必须是当前分集全部且唯一的镜头 ID")
        return normalised


class ShotGroupMaterialItemIn(BaseModel):
    """镜头组视觉素材引用；稳定 ID 由服务端校验并补齐。"""

    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=255)
    subject_id: str | None = Field(default=None, min_length=1, max_length=36)
    asset_entry_id: str | None = Field(default=None, min_length=1, max_length=36)

    @model_validator(mode="after")
    def require_identity(self) -> ShotGroupMaterialItemIn:
        if not self.name and not self.subject_id and not self.asset_entry_id:
            raise ValueError("素材至少需要 name、subject_id 或 asset_entry_id")
        return self


class ShotGroupMaterialsIn(BaseModel):
    """镜头组视觉素材分组。"""

    model_config = ConfigDict(extra="forbid")

    character: list[ShotGroupMaterialItemIn] = Field(default_factory=list, max_length=100)
    background: list[ShotGroupMaterialItemIn] = Field(default_factory=list, max_length=100)
    prop: list[ShotGroupMaterialItemIn] = Field(default_factory=list, max_length=100)
    other: list[ShotGroupMaterialItemIn] = Field(default_factory=list, max_length=100)


class ShotGroupAudioElementIn(BaseModel):
    """镜头组音频主体引用。"""

    model_config = ConfigDict(extra="forbid")

    type: Literal["character", "sound"]
    name: str | None = Field(default=None, min_length=1, max_length=255)
    subject_id: str | None = Field(default=None, min_length=1, max_length=36)
    asset_entry_id: str | None = Field(default=None, min_length=1, max_length=36)

    @model_validator(mode="after")
    def require_identity(self) -> ShotGroupAudioElementIn:
        if not self.name and not self.subject_id and not self.asset_entry_id:
            raise ValueError("音频元素至少需要 name、subject_id 或 asset_entry_id")
        return self


class ShotGroupUpdateIn(BaseModel):
    """编辑正式镜头组；除乐观锁版本外至少提交一个可编辑字段。"""

    model_config = ConfigDict(extra="forbid")

    expected_episode_version: int = Field(..., ge=1)
    duration_seconds: int | None = Field(default=None, ge=3, le=15)
    prompt: str | None = Field(default=None, min_length=1, max_length=32000)
    materials: ShotGroupMaterialsIn | None = None
    audio_elements: list[ShotGroupAudioElementIn] | None = Field(
        default=None,
        max_length=200,
    )

    @field_validator("prompt")
    @classmethod
    def validate_prompt(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("prompt 不得为空")
        return value

    @model_validator(mode="after")
    def require_edit_field(self) -> ShotGroupUpdateIn:
        editable_fields = {
            "duration_seconds",
            "prompt",
            "materials",
            "audio_elements",
        }
        if not (self.model_fields_set & editable_fields):
            raise ValueError("至少提交一个镜头组编辑字段")
        if "duration_seconds" in self.model_fields_set and self.duration_seconds is None:
            raise ValueError("duration_seconds 不得为 null")
        if "prompt" in self.model_fields_set and self.prompt is None:
            raise ValueError("prompt 不得为 null")
        if "materials" in self.model_fields_set and self.materials is None:
            raise ValueError("materials 不得为 null")
        if "audio_elements" in self.model_fields_set and self.audio_elements is None:
            raise ValueError("audio_elements 不得为 null")
        return self


# ---------------------------------------------------------------------------
# Cover — GET /shots/<script_id>/cover
# ---------------------------------------------------------------------------


class CoverOut(BaseModel):
    """视频项目封面 — 首个镜头首帧。"""

    model_config = ConfigDict(from_attributes=True)

    script_id: str
    cover: str | None = None
    mime_type: str | None = None


# ---------------------------------------------------------------------------
# Generate Storyboard — POST /scripts/<sid>/episodes/<eid>/generate-storyboard
# ---------------------------------------------------------------------------


class GenerateStoryboardResponse(BaseModel):
    """触发分镜生成的响应。"""

    task_id: str
    job_id: str
    episode_id: str
    script_id: str
    status: str = "pending"
    accepted: bool = True
    message: str = "分镜生成任务已投递"


# ---------------------------------------------------------------------------
# Storyboard Status — GET /scripts/<sid>/episodes/<eid>/storyboard-status
# ---------------------------------------------------------------------------


class StoryboardStatusOut(BaseModel):
    """分镜生成任务状态 (供前端轮询)。"""

    model_config = ConfigDict(from_attributes=True)

    job_id: str
    task_id: str | None = None
    episode_id: str
    script_id: str
    # pending | running | succeeded | failed
    status: str
    progress: int = 0
    progress_stage: str | None = None
    progress_message: str | None = None
    error_message: str | None = None
    detail: dict | None = None


class StoryboardJobListOut(BaseModel):
    """分镜任务列表响应。"""

    items: list[StoryboardStatusOut] = Field(default_factory=list)
    total: int


class StoryboardJobEditIn(BaseModel):
    """分镜任务可编辑展示字段；不允许客户端改写终态事实。"""

    progress_message: str | None = Field(default=None, max_length=512)
    detail: dict | None = None


__all__ = [
    "ShotOut",
    "MaterialItemOut",
    "ReferenceFramesOut",
    "MaterialsOut",
    "ShotGroupHierarchyOut",
    "SceneHierarchyOut",
    "ShotHierarchyOut",
    "ShotGroupWorkbenchOut",
    "SceneWorkbenchOut",
    "VideoWorkbenchOut",
    "ShotGroupMergeIn",
    "ShotGroupMaterialItemIn",
    "ShotGroupMaterialsIn",
    "ShotGroupAudioElementIn",
    "ShotGroupUpdateIn",
    "CoverOut",
    "GenerateStoryboardResponse",
    "StoryboardStatusOut",
    "StoryboardJobEditIn",
    "StoryboardJobListOut",
]
