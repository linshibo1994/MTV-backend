"""风格目录与提示词优化接口 Schema。"""

from pydantic import BaseModel, ConfigDict, Field


class StyleOut(BaseModel):
    id: str
    name_zh: str
    target: list[str]
    prompt_template: str
    negative_prompt: str
    default_prompt: str
    best_for: list[str]


class StyleListOut(BaseModel):
    styles: list[StyleOut]


class PromptOptimizeIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt: str = Field("", max_length=2000, description="原始提示词，可为空（生成默认提示词）")
    style_id: str | None = Field(None)
    target: str = Field(..., pattern="^(image|video)$")


MAX_PROMPT_OPTIMIZE_BATCH_ITEMS = 50


class PromptOptimizeBatchIn(BaseModel):
    """批量提示词优化入参；每项沿用单条优化契约。"""

    model_config = ConfigDict(extra="forbid")

    items: list[PromptOptimizeIn] = Field(
        ...,
        min_length=1,
        max_length=MAX_PROMPT_OPTIMIZE_BATCH_ITEMS,
        description="待优化提示词列表，按输入顺序返回结果",
    )


class PromptOptimizeOut(BaseModel):
    optimized_prompt: str
    negative_prompt: str
    style_id: str | None
    style_name: str | None
    original_prompt: str


class PromptOptimizeBatchOut(BaseModel):
    """批量提示词优化出参；items 与请求顺序一一对应。"""

    items: list[PromptOptimizeOut]
