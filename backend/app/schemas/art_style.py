"""动态 ArtStyle API 契约。"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.providers.storage import public_url_for_output_key


class ArtStyleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    script_id: str
    name: str
    prompt_zh: str
    prompt_en: str
    negative_prompt: str
    visual_style: Literal["live_action", "anime"] | None = None
    source_asset_ids: list[str]
    source_hashes: list[str]
    thumbnail_storage_key: str | None = None
    thumbnail_public_url: str | None = None
    extraction_provider: str | None = None
    extraction_model: str | None = None
    extraction_version: int
    is_active: bool
    created_by_user_id: str
    updated_by_user_id: str
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def populate_thumbnail_public_url(self) -> "ArtStyleOut":
        self.thumbnail_public_url = public_url_for_output_key(self.thumbnail_storage_key)
        return self


class ArtStyleListOut(BaseModel):
    items: list[ArtStyleOut]
    total: int


class ArtStyleExtractionJobOut(BaseModel):
    """客户端轮询的异步画风提取任务。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    script_id: str
    name: str
    source_asset_ids: list[str]
    source_hashes: list[str]
    input_version_hash: str
    status: Literal["queued", "running", "succeeded", "failed"]
    attempt_count: int
    error_code: str | None = None
    error_message: str | None = None
    extraction_provider: str | None = None
    extraction_model: str | None = None
    art_style_id: str | None = None
    billing_mode: Literal["unmetered"]
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class ArtStyleExtractIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=255)
    asset_ids: list[str] = Field(min_length=1, max_length=6)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        return value.strip()


class ArtStyleUpdateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=255)
    prompt_zh: str | None = Field(default=None, min_length=1, max_length=8_000)
    prompt_en: str | None = Field(default=None, min_length=1, max_length=8_000)
    negative_prompt: str | None = Field(default=None, max_length=8_000)
    visual_style: Literal["live_action", "anime"] | None = None
    is_active: bool | None = None
