"""关键场次 Coverage Audition API 契约。"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SceneVisualPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plan_id: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=120)
    audience_knowledge_timing: str = Field(min_length=1, max_length=500)
    alignment_target: str = Field(min_length=1, max_length=500)
    performance_space: str = Field(min_length=1, max_length=500)
    strongest_image: str = Field(min_length=1, max_length=500)
    landing: str = Field(min_length=1, max_length=500)
    tradeoff: str = Field(min_length=1, max_length=500)


class SceneAuditionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    script_id: str
    episode_id: str
    scene_no: int
    input_version: int
    scene_type: str
    source_hash: str
    plans: list[SceneVisualPlan]
    selected_plan_id: str | None = None
    status: str
    attempt_count: int = 0
    error_code: str | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    selected_by_user_id: str | None = None
    selected_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class SceneAuditionListOut(BaseModel):
    items: list[SceneAuditionOut]
    total: int


class SceneAuditionGenerateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scene_no: int = Field(ge=1)
    expected_episode_version: int = Field(ge=1)
    scene_type: Literal[
        "character_introduction",
        "identity_reveal",
        "relationship_reversal",
        "climax_ending",
    ]


class SceneAuditionConfirmIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plan_id: str = Field(min_length=1, max_length=64)
    expected_input_version: int = Field(ge=1)


class SceneAuditionPlanPayload(BaseModel):
    """供分镜 Provider/服务写入严格计划的结构。"""

    model_config = ConfigDict(extra="forbid")

    plans: list[SceneVisualPlan] = Field(min_length=2, max_length=3)

    @model_validator(mode="after")
    def unique_plan_ids(self) -> "SceneAuditionPlanPayload":
        ids = [plan.plan_id for plan in self.plans]
        if len(ids) != len(set(ids)):
            raise ValueError("plan_id 不得重复")
        return self
