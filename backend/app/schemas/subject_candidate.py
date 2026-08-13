"""主体候选确认 API 契约。"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.services.subject_candidate_evidence import normalize_candidate_evidence


class SubjectCandidateEvidenceOut(BaseModel):
    """可公开的候选来源证据；兼容旧记录时只暴露白名单字段。"""

    model_config = ConfigDict(extra="ignore")

    episode_no: int | None = Field(default=None, ge=1)
    quote: str = Field(min_length=1, max_length=2_000)


class SubjectCandidateEvidenceIn(BaseModel):
    """新写入必须遵循的候选来源证据契约。"""

    model_config = ConfigDict(extra="forbid")

    episode_no: int | None = Field(default=None, ge=1)
    quote: str = Field(min_length=1, max_length=2_000)

    @field_validator("quote")
    @classmethod
    def strip_quote(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("证据原文不得为空")
        return normalized


class SubjectCandidateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    script_id: str
    source_run_id: str | None = None
    subject_type: Literal["character", "scene", "prop", "sound"]
    name: str
    pipeline_name: str
    description: str | None = None
    aliases: list[str] = Field(default_factory=list)
    episode_nos: list[int] = Field(default_factory=list)
    evidence: list[SubjectCandidateEvidenceOut] = Field(default_factory=list)
    status: Literal["pending", "linked", "ignored"]
    version: int
    linked_subject_id: str | None = None
    resolved_by_user_id: str | None = None
    resolved_at: datetime | None = None
    created_at: datetime
    updated_at: datetime

    @field_validator("evidence", mode="before")
    @classmethod
    def normalize_legacy_evidence(cls, value: object) -> list[dict[str, object]]:
        return normalize_candidate_evidence(value)


class SubjectCandidateListOut(BaseModel):
    items: list[SubjectCandidateOut]
    total: int


class SubjectCandidateCreateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subject_type: Literal["character", "scene", "prop", "sound"]
    name: str = Field(min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=10_000)
    aliases: list[str] = Field(default_factory=list, max_length=50)
    episode_nos: list[int] = Field(default_factory=list, max_length=1_000)
    evidence: list[SubjectCandidateEvidenceIn] = Field(default_factory=list, max_length=100)

    @field_validator("name", "description")
    @classmethod
    def strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("不得为空")
        return normalized


class SubjectCandidateResolveIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["link_existing", "create_subject", "ignore"]
    expected_version: int = Field(ge=1)
    subject_id: str | None = None
    visual_style: Literal["live_action", "anime"] | None = None

    @model_validator(mode="after")
    def validate_action_payload(self) -> "SubjectCandidateResolveIn":
        if self.action == "link_existing":
            if not self.subject_id:
                raise ValueError("关联已有主体时必须提供 subject_id")
            if self.visual_style is not None:
                raise ValueError("关联已有主体时不得提交 visual_style")
        elif self.action == "create_subject":
            if self.subject_id is not None:
                raise ValueError("创建主体时不得提交 subject_id")
        elif self.subject_id is not None or self.visual_style is not None:
            raise ValueError("忽略候选时不得提交主体字段")
        return self
