"""可测试的任务租约、CAS 与流水线错误状态辅助。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from secrets import token_urlsafe

TASK_HARD_TIME_LIMIT_SECONDS = 900
TASK_SOFT_TIME_LIMIT_SECONDS = 810
MEDIA_STORAGE_JOB_LEASE_SECONDS = TASK_HARD_TIME_LIMIT_SECONDS + 60

_SAFE_FAILURES: dict[str, tuple[str, bool]] = {
    "provider_contract_error": ("供应商响应不符合约定", False),
    "provider_timeout": ("上游服务超时", True),
    "infrastructure_unavailable": ("依赖服务暂不可用", True),
    "invalid_pipeline_data": ("流水线数据无效", False),
    "insufficient_credits": ("积分不足", False),
    "internal_error": ("任务执行失败", False),
}


@dataclass
class LeaseState:
    """可由 ORM 行或内存测试替代的最小租约状态。"""

    status: str
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_until: datetime | None = None
    heartbeat_at: datetime | None = None


@dataclass
class StageSnapshot:
    status: str
    detail: dict | None
    current: int | None
    total: int | None
    error: dict | None


@dataclass(frozen=True)
class PipelineFailure:
    error_type: str
    error_message: str
    retryable: bool

    @classmethod
    def from_exception(cls, exc: Exception) -> PipelineFailure:
        declared = str(getattr(exc, "error_type", ""))
        if declared in _SAFE_FAILURES:
            error_type = declared
        elif isinstance(exc, TimeoutError):
            error_type = "provider_timeout"
        elif isinstance(exc, ConnectionError | OSError):
            error_type = "infrastructure_unavailable"
        elif isinstance(exc, ValueError):
            error_type = "invalid_pipeline_data"
        elif getattr(exc, "status_code", None) == 402:
            error_type = "insufficient_credits"
        else:
            error_type = "internal_error"
        message, default_retryable = _SAFE_FAILURES[error_type]
        retryable = bool(getattr(exc, "retryable", default_retryable))
        return cls(error_type, message, retryable)

    def to_dict(self) -> dict[str, object]:
        return {
            "error_type": self.error_type,
            "error_message": self.error_message,
            "retryable": self.retryable,
        }


def claim_lease(
    state: LeaseState, owner: str, now: datetime, duration: timedelta
) -> str | None:
    """CAS 领取 pending/retry 或已过期的 running 项，返回不可伪造的 token。"""
    expired = state.lease_until is not None and state.lease_until <= now
    if state.status not in {"pending", "retry"} and not (
        state.status == "running" and expired
    ):
        return None
    token = token_urlsafe(24)
    state.status = "running"
    state.lease_owner = owner
    state.lease_token = token
    state.lease_until = now + duration
    state.heartbeat_at = now
    return token


def heartbeat_lease(
    state: LeaseState, token: str, now: datetime, duration: timedelta
) -> bool:
    if state.status != "running" or state.lease_token != token:
        return False
    state.lease_until = now + duration
    state.heartbeat_at = now
    return True


def complete_lease(state: LeaseState, token: str, terminal_status: str) -> bool:
    """仅当前租约 token 可以把运行态转换为终态。"""
    if terminal_status not in {"done", "failed", "published", "skipped"}:
        raise ValueError("terminal_status 必须是终态")
    if state.status != "running" or state.lease_token != token:
        return False
    state.status = terminal_status
    state.lease_owner = None
    state.lease_token = None
    state.lease_until = None
    return True


def reset_stage(stage: StageSnapshot) -> None:
    """新轮次必须清空上一轮的展示状态和错误。"""
    stage.status = "pending"
    stage.detail = None
    stage.current = None
    stage.total = None
    stage.error = None


def is_current_input_version(input_version: int, active_version: int) -> bool:
    """只有完全匹配的版本可写回，晚到或异常超前任务都必须放弃结果。"""
    return input_version == active_version
