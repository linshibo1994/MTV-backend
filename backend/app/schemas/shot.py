"""分镜模块 Pydantic schemas。

严格遵循 04-数据模型与API文档.md 第 1.14~1.18 节与 02-功能规格文档.md 第 5.3~5.6 节。

层级: Episode -> Scenes(背景场景) -> Shot Groups(镜头组) -> Shots(镜头)
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# 基础出参
# ---------------------------------------------------------------------------


class ShotOut(BaseModel):
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
    reference_frames: ReferenceFramesOut | None = None
    shots: list[ShotOut] = Field(default_factory=list)


class SceneHierarchyOut(BaseModel):
    """场景 (shot-hierarchy 视角) — 按背景聚合。"""

    model_config = ConfigDict(from_attributes=True)

    scene_no: int
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
    "CoverOut",
    "GenerateStoryboardResponse",
    "StoryboardStatusOut",
    "StoryboardJobEditIn",
    "StoryboardJobListOut",
]
