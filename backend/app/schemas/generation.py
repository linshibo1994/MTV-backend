"""生成任务 Pydantic 入参/出参 schema。

- CreateJobIn: 创建生图任务入参（绑定画布项目 + 主体 + 模型 + prompt）。
- BatchCreateJobIn: 一键设计 N 批量生图入参（06 §3.5）。
- GenerationJobOut: 生成任务出参（异步轮询 status/progress）。
- BatchJobOut: 批量生图结果出参。

出参契约对齐 02 §4.7 与 04 §1.13：input_json/output_json 序列化为 JSON 字符串
（前端按字符串解析），由 field_serializer 在响应序列化时统一转换。
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_serializer

# 生图模型枚举：Google / OpenAI / 阿里百炼通义万相。
ImageModelName = Literal[
    "banana-2",
    "gpt-image-2",
    "wan2.1-t2i-plus",
    "wan2.1-t2i-turbo",
    "wanx-v1",
    "qwen-image",
    "qwen-image-2.0",
]

MAX_IMAGE_PROMPT_LENGTH = 32_000
ImagePrompt = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=MAX_IMAGE_PROMPT_LENGTH,
    ),
]


class CreateJobIn(BaseModel):
    """创建生图任务入参。"""

    model_config = ConfigDict(extra="forbid")

    material_id: str = Field(..., description="画布项目 ID")
    subject_id: str = Field(..., description="关联主体 ID")
    prompt: ImagePrompt | None = Field(
        None,
        description="客户端可编辑的最终生图提示词；缺省时兼容旧客户端并回退服务端 enrich",
    )
    style_id: str | None = Field(None, description="风格 ID")
    model: ImageModelName = Field("banana-2", description="生图模型")
    size: str = Field("2k", description="尺寸，如 2k")
    aspect: str = Field("auto", description="比例；auto 按主体类型选择，或显式传入 16:9、3:4 等")
    count: int = Field(1, ge=1, le=4, description="生成张数（多张选一）")


class BatchCreateJobIn(BaseModel):
    """一键设计 N 批量生图入参（06 §3.5）。

    subject_ids 为空时自动选取该剧本下未设计(not_designed)且非声音的主体。
    prompts 按 subject_id 分别覆盖最终生图提示词；缺失项兼容旧客户端并回退服务端 enrich。
    """

    model_config = ConfigDict(extra="forbid")

    material_id: str = Field(..., description="画布项目 ID")
    subject_ids: list[str] | None = Field(
        None, description="显式指定的主体 ID 列表；为空则自动选取未设计主体"
    )
    prompts: dict[str, ImagePrompt] | None = Field(
        None,
        description="每个主体的客户端可编辑最终生图提示词，键为 subject_id",
    )
    style_id: str | None = Field(None, description="风格 ID")
    model: ImageModelName = Field("banana-2", description="生图模型")
    size: str = Field("2k", description="尺寸，如 2k")
    aspect: str = Field("auto", description="比例；auto 按主体类型选择，或显式传入 16:9、3:4 等")
    count: int = Field(1, ge=1, le=4, description="每个主体生成张数（多张选一）")


class GenerationJobOut(BaseModel):
    """生成任务出参 — 前端轮询 status/progress/progress_stage。"""

    job_id: str
    material_id: str | None = None
    script_id: str | None = None
    subject_id: str
    episode_id: str | None = None
    scene_no: int | None = None
    job_type: str | None = None
    status: str
    progress: int
    progress_stage: str | None = None
    progress_message: str | None = None
    error_message: str | None = None
    # ORM 为 JSONB(dict)，响应序列化为 JSON 字符串以对齐 02 §4.7 / 04 §1.13 契约
    input_json: dict[str, Any] | None = None
    output_json: dict[str, Any] | None = None
    metrics: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_serializer("input_json", "output_json")
    def _serialize_json_field(self, v: dict[str, Any] | None) -> str | None:
        """将 JSONB dict 序列化为 JSON 字符串，与契约 string(JSON) 一致。"""
        if v is None:
            return None
        return json.dumps(v, ensure_ascii=False)


class JobListOut(BaseModel):
    """生成任务列表出参。"""

    items: list[GenerationJobOut]


class BatchJobOut(BaseModel):
    """批量生图结果出参。"""

    created: list[GenerationJobOut]
    skipped: list[str] = Field(
        default_factory=list, description="跳过的主体 ID（声音类型/无描述等）"
    )
    failed: list[dict[str, Any]] = Field(
        default_factory=list,
        description="运行期创建/预扣失败的主体（subject_id/subject_name/reason），"
        "部分任务可能已成功投递，客户端据此告知用户部分已运行",
    )
