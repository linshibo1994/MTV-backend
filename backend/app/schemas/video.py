"""视频生成模块 Pydantic 模型 — VideoGenerationJob 出参、激活供应商配置、创建入参。"""

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

MAX_VIDEO_PROMPT_LENGTH = 32_000
VideoPrompt = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=MAX_VIDEO_PROMPT_LENGTH,
    ),
]


class VideoJobOut(BaseModel):
    """视频生成任务出参 — 对应 VideoGenerationJob ORM。"""

    model_config = ConfigDict(from_attributes=True)

    job_id: str
    script_id: str
    episode_id: str
    shot_group_id: str | None = None
    owner_user_id: str | None = None
    idempotency_key: str | None = None
    request_hash: str | None = None
    input_version: int | None = None
    attempt: int | None = None
    reservation_id: str | None = None
    dispatch_id: str | None = None
    scene_no: int
    unit_id: str
    unit_runtime_key: str
    provider_key: str
    job_type: str
    status: str
    progress: int
    progress_stage: str | None = None
    progress_message: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    retryable: bool | None = None
    input_json: dict[str, Any] | None = None
    # 生成结果：含 video_storage_key（结果视频 storage_key，前端据此播放/下载）
    output_json: dict[str, Any] | None = None
    metrics: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ActiveProviderOut(BaseModel):
    """当前激活的视频供应商 + 模型。"""

    model_config = ConfigDict(from_attributes=True)

    provider_key: str
    job_type: str
    model: str


class VideoJobCreateIn(BaseModel):
    """创建视频生成任务入参 — 绑定 shot_group。"""

    script_id: str
    episode_id: str
    shot_group_id: str
    prompt: VideoPrompt | None = Field(
        None,
        description="客户端可编辑的视频提示词；缺省时兼容旧客户端并回退镜头组 prompt",
    )
    style_id: str | None = Field(None, description="风格 ID")


class ReferenceImageAuditIn(BaseModel):
    """手动触发一个镜头组最终参考图审核。"""

    script_id: str
    episode_id: str
    shot_group_id: str


class ReferenceImageAuditOut(BaseModel):
    """参考图审核结果；参考图内容变更后旧结果会自动变为 pending。"""

    shot_group_id: str
    status: Literal["pending", "approved", "rejected", "review", "not_required", "failed"]
    image_count: int = 0
    audited_at: datetime | None = None
    results: list[dict[str, Any]] = Field(default_factory=list)
    message: str | None = None


class VideoJobBatchCreateIn(BaseModel):
    """批量创建视频生成任务入参 — 多个 shot_group_id（doc02 §5.9 工作台批量生成）。"""

    script_id: str
    episode_id: str
    shot_group_ids: list[str] = Field(
        ..., min_length=1, max_length=100, description="镜头组 ID 列表"
    )
    prompts: dict[str, VideoPrompt] | None = Field(
        None,
        description="每个镜头组的客户端可编辑视频提示词，键为 shot_group_id",
    )
    style_id: str | None = Field(None, description="风格 ID")


class ActiveProviderUpdateIn(BaseModel):
    """切换视频供应商（管理员）入参。"""

    provider_key: Literal[
        "jianyu", "modelsai", "neolink", "bailian", "openai", "gemini", "mgrouter"
    ] = Field(..., description="供应商 key")
    job_type: str = Field(default="all_reference", description="任务类型，默认 all_reference")


class VideoJobListOut(BaseModel):
    """视频生成任务列表包装。"""

    items: list[VideoJobOut]
    total: int | None = None


class VideoJobFailedItem(BaseModel):
    """批量创建中单个失败项 — 仅记录失败镜头组与原因（doc02 §5.9）。"""

    shot_group_id: str
    error_message: str


class VideoJobBatchOut(BaseModel):
    """批量创建视频生成任务响应（doc02 §5.9）— 区分 success/failed 列表。

    成功项为完整 VideoJobOut（可继续轮询/取消）；失败项仅含 shot_group_id 与
    error_message，避免用 job_id="" 的伪记录混入 items 导致前端无法分组、
    且空 job_id 无法被后续轮询/取消端点使用。
    """

    success: list[VideoJobOut] = Field(default_factory=list)
    failed: list[VideoJobFailedItem] = Field(default_factory=list)
