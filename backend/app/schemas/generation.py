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
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_serializer, model_validator

from app.providers.storage import public_urls_for_payload

# 生图模型标识放开为自由字符串（请求级动态路由）：已知别名（banana-2/
# gpt-image-2/gp-im-2/wan2.1-*/qwen-image* 等）仍按别名路由厂商，其余视为
# 真实模型 ID，由显式 provider 或部署默认厂商决定去向。
ImageModelName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=128),
]
# 请求级路由字段：显式传入 provider（及可选 model）时按请求路由生图厂商。
ProviderOverride = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=128),
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
ImageNegativePrompt = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        max_length=MAX_IMAGE_PROMPT_LENGTH,
    ),
]


class CreateJobIn(BaseModel):
    """创建生图任务入参。"""

    model_config = ConfigDict(extra="forbid")

    material_id: str = Field(..., description="画布项目 ID")
    subject_id: str = Field(..., description="关联主体 ID")
    prompt: ImagePrompt = Field(..., description="客户端确认的最终生图提示词")
    negative_prompt: ImageNegativePrompt = Field(
        "", description="客户端确认的最终负向提示词；为空表示不附加负向约束"
    )
    model: ImageModelName = Field("qwen-image-2.0", description="生图模型")
    provider: ProviderOverride | None = Field(
        None,
        description="请求级生图供应商（openai/gemini/bailian/neolink）；缺省按模型别名或服务端默认路由",
    )
    size: str = Field("2k", description="尺寸，如 2k")
    aspect: str = Field("auto", description="比例；auto 按主体类型选择，或显式传入 16:9、3:4 等")
    count: int = Field(1, ge=1, le=4, description="生成张数（多张选一）")


class BatchCreateJobIn(BaseModel):
    """一键设计 N 批量生图入参（06 §3.5）。

    subject_ids 为空时自动选取该剧本下未设计(not_designed)且非声音的主体。
    prompts 必须按 subject_id 提供每个目标主体的最终生图提示词。
    """

    model_config = ConfigDict(extra="forbid")

    material_id: str = Field(..., description="画布项目 ID")
    subject_ids: list[str] | None = Field(
        None, description="显式指定的主体 ID 列表；为空则自动选取未设计主体"
    )
    prompts: dict[str, ImagePrompt] = Field(
        ...,
        min_length=1,
        description="每个主体的客户端确认最终生图提示词，键为 subject_id",
    )
    negative_prompts: dict[str, ImageNegativePrompt] | None = Field(
        None,
        description="每个主体的客户端确认负向提示词，键为 subject_id；缺失项为空",
    )
    model: ImageModelName = Field("qwen-image-2.0", description="生图模型")
    provider: ProviderOverride | None = Field(
        None,
        description="请求级生图供应商（openai/gemini/bailian/neolink）；缺省按模型别名或服务端默认路由",
    )
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

    @model_validator(mode="after")
    def populate_output_public_urls(self) -> GenerationJobOut:
        self.output_json = public_urls_for_payload(self.output_json)
        return self

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
