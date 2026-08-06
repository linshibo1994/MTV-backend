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


class PromptOptimizeOut(BaseModel):
    optimized_prompt: str
    negative_prompt: str
    style_id: str | None
    style_name: str | None
    original_prompt: str
