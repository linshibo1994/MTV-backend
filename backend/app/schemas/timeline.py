"""时间线编排模块 Pydantic v2 入参/出参。

对应模型: app.models.video_timeline_arrangement.VideoTimelineArrangement
参考: docs/Claude/04-数据模型与API文档.md 1.20 节
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class TimelineItemOut(BaseModel):
    """时间线单个片段出参。"""

    id: str
    unit_runtime_key: str
    job_id: str
    row_order: int
    is_selected: bool
    trim_start_seconds: float
    trim_end_seconds: float | None = None
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TimelineOut(BaseModel):
    """时间线列表出参 — 按 row_order 排列。"""

    script_id: str
    episode_id: str
    items: list[TimelineItemOut] = Field(default_factory=list)


class TimelineItemIn(BaseModel):
    """添加片段入参 — 关联视频生成 job。

    script_id / episode_id 由 POST 查询参数提供（与 GET 列表一致）。
    """

    unit_runtime_key: str = Field(..., min_length=1, max_length=128)
    job_id: str = Field(..., min_length=1, max_length=36, description="关联视频生成 job_id")


class TimelineUpdateIn(BaseModel):
    """更新单个片段入参 — 全字段可选，部分更新。

    expected_version 用于乐观锁: 必传，服务端用条件更新防止并发丢更新。
    """

    row_order: int | None = Field(default=None, ge=0)
    is_selected: bool | None = None
    trim_start_seconds: float | None = Field(default=None, ge=0.0)
    trim_end_seconds: float | None = Field(default=None, ge=0.0)
    expected_version: int = Field(..., ge=0, description="乐观锁版本号")


class ReorderItem(BaseModel):
    """批量重排索单项。"""

    id: str
    row_order: int = Field(..., ge=0)
    expected_version: int = Field(..., ge=0)


class ReorderIn(BaseModel):
    """批量重排序入参。"""

    items: list[ReorderItem] = Field(..., min_length=1)


class RenderRequestIn(BaseModel):
    """合成最终视频请求入参。"""

    script_id: str = Field(..., min_length=1)
    episode_id: str = Field(..., min_length=1)


class RenderStatusOut(BaseModel):
    """合成任务状态出参 — 前端轮询。"""

    job_id: str
    status: Literal["starting", "processing", "succeeded", "failed", "canceled"]
    progress: int = 0
    progress_stage: str | None = None
    progress_message: str | None = None
    error_message: str | None = None
    error_type: str | None = None
    retryable: bool = False
    storage_key: str | None = Field(
        default=None, description="合成产物 storage_key（succeeded 时返回）"
    )
    output_storage_key: str | None = None


class RenderJobOut(BaseModel):
    """合成任务创建出参。"""

    job_id: str
    status: Literal["starting", "processing", "succeeded", "failed", "canceled"] = "starting"
    output_storage_key: str | None = None
