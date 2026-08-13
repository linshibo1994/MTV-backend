"""提示词模板管理 API Schema。"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PromptTemplateCreateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_.-]*$")
    content: str = Field(min_length=1, max_length=120_000)
    variables_schema: dict[str, object] = Field(default_factory=dict)
    activate: bool = False

    @field_validator("key")
    @classmethod
    def strip_key(cls, value: str) -> str:
        return value.strip()

    @field_validator("content")
    @classmethod
    def reject_blank_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("模板正文不能为空")
        return value


class PromptTemplateActivateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1)


class PromptTemplateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    key: str
    version: int
    content: str
    content_sha256: str
    variables_schema: dict[str, object]
    is_active: bool
    created_by_user_id: str | None
    created_at: datetime
    updated_at: datetime


class PromptTemplateListOut(BaseModel):
    items: list[PromptTemplateOut]
    total: int
