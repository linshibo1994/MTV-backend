"""视频任务删除前的清理债务判定。

剧本、分集和镜头组的删除都必须使用同一套判定，避免把远端取消、媒体清理或
积分补偿尚未完成的任务直接从本地记录中抹掉。
"""

from __future__ import annotations

from typing import Any

from app.models.video_generation_job import is_video_job_manual_review
from app.services.video_remote_cancellation import PROVIDER_CANCELLATION_KEY

_ATTEMPT_OUTPUT_CLEANUP_KEY = "attempt_output_cleanup"
_RETRYABLE_PROVIDER_CANCELLATION_OUTCOMES = frozenset({"pending", "failed"})


def has_retryable_video_cleanup_debt(
    status: str,
    output_json: dict[str, Any] | None,
    error_type: str | None,
    retryable: bool,
) -> bool:
    """判断删除任务是否会丢失必须保留的清理或账务事实。"""

    if is_video_job_manual_review(status, error_type):
        return True
    if status == "failed" and error_type == "financial_cleanup" and retryable:
        return True
    if not isinstance(output_json, dict):
        return False

    cancellation = output_json.get(PROVIDER_CANCELLATION_KEY)
    if status == "canceled" and isinstance(cancellation, dict):
        provider_task_id = str(
            cancellation.get("provider_task_id")
            or output_json.get("provider_task_id")
            or ""
        ).strip()
        if (
            provider_task_id
            and cancellation.get("retryable") is True
            and str(cancellation.get("outcome") or "")
            in _RETRYABLE_PROVIDER_CANCELLATION_OUTCOMES
        ):
            return True

    attempt_cleanup = output_json.get(_ATTEMPT_OUTPUT_CLEANUP_KEY)
    return isinstance(attempt_cleanup, dict) and any(
        isinstance(record, dict) and record.get("retryable") is True
        for record in attempt_cleanup.values()
    )


__all__ = ["has_retryable_video_cleanup_debt"]
