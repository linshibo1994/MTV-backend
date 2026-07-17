"""流水线人工运行 API 契约。"""

from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class PipelineRunMode(StrEnum):
    """人工运行类型。"""

    MANUAL_REEXTRACT = "manual_reextract"
    RETRY_FAILED = "retry_failed"


class PipelineRunCreateIn(BaseModel):
    """创建人工流水线运行。"""

    mode: PipelineRunMode
    episode_version: int = Field(ge=1)
    client_save_seq: int = Field(ge=0)
    failed_run_id: str | None = Field(default=None, min_length=1, max_length=36)

    @model_validator(mode="after")
    def validate_failed_run(self) -> "PipelineRunCreateIn":
        if self.mode is PipelineRunMode.RETRY_FAILED and self.failed_run_id is None:
            raise ValueError("失败重试必须提供 failed_run_id")
        if self.mode is PipelineRunMode.MANUAL_REEXTRACT and self.failed_run_id is not None:
            raise ValueError("手动重新提取不能提供 failed_run_id")
        return self


class PipelineRunOut(BaseModel):
    """已受理的人工流水线运行。"""

    script_id: str
    episode_id: str
    run_id: str
    dispatch_id: str
    reservation_id: str
    mode: PipelineRunMode
    retry_of_run_id: str | None
    attempt: int
    input_version: int
    input_client_save_seq: int
    estimated: int
    status: str = "pending"
    idempotent_replay: bool = False
